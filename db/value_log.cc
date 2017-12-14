// changes needed in main Rocks code:
// FileMetaData needs a unique ID number for each SST in the system.  Could be unique per column family if that's easier
// Manifest needs smallest file# referenced by the SST

//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <memory>
#include <algorithm>
#include <math.h>
#include "db/value_log.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "options/cf_options.h"
#include "rocksdb/status.h"
#include<chrono>
#include<thread>

namespace rocksdb {
#if DELAYPROB
static Random rnd(301);
// Throw in occasional long delays to help find timing problems
extern void ProbDelay() {
    if(DELAYPROB > (rnd.Next() % 100))std::this_thread::sleep_for(DELAYTIME);  // give the compactor time to run
}
#endif

// Convert a filename, which is known to be a valid vlg filename, to the ring and filenumber in this VLog
static ParsedFnameRing VlogFnametoRingFname(std::string pathfname) {
    // Isolate the filename from the path as required by the subroutine
    size_t fslashx = pathfname.find_last_of('/');  // index of '/'
    if(fslashx==pathfname.size())fslashx = -1;  // if no /, start at beginning
    size_t bslashx = pathfname.find_last_of('\\');  // index of '\'
    if(bslashx==pathfname.size())bslashx = -1;  // if no \, start at beginning
    if(fslashx<bslashx)fslashx=bslashx;  // fslashx is now the larger index
    std::string fname = pathfname.substr(fslashx+1);  // get the part AFTER the last /\

    // Extract the file number (which includes the ring) from the filename
    uint64_t number;  // return value, giving file number
    FileType type;  // return value, giving type of file
    ParseFileName(fname, &number, &type);  // get file number (can't fail)

    // Parse the file number into file number/ring
    return ParsedFnameRing(number);
}

//
//
// ******************************************** VlogRing ***********************************************
// VLogRing keeps track of VLogRingFiles for a specified set of levels in the database.  Files are entered into
// the ring during compaction.  The file persists until there are no more SSTs that refer to it.  During compaction,
// the oldest values are picked up & copied to new files, which removes references to the oldest files.
//
// We use the VLogRing to retrieve the value of an indirect reference, given the ring number/file number/offset/length
// of the reference.  We also use the VLogRing to see which SSTs have references to the oldest files.
//

// A VLogRing is a set of sequentially-numbered files, with a common prefix and extension .vlg, that contain
// values for a column family.  Each value is pointed to by an SST, which uses a VLogRingRef for the purpose.
// During compaction, bursts of values are written to the VLogRing, lumped into files of approximately equal size.
//
// Each SST remembers the oldest VLogRingRef that it contains.  Whenever an SST is destroyed, we check to
// see if there are VLogRing files that are no longer referred to by any SST, and delete any such that are found.
//
// Each column family has (possibly many) VLogRing.  VLogRingRef entries in an SST implicitly refer to the VLog of the column family.
// Constructor.  Find all the VLogRing files and open them.  Create the VLogRingQueue for the files.
// Delete files that have numbers higher than the last valid one.
VLogRing::VLogRing(
  int ringno,   // ring number of this ring within the CF
  ColumnFamilyData *cfd,  // info for the CF for this ring
  std::vector<std::string> filenames,  // the filenames that might be vlog files for this ring
  VLogRingRefFileno earliest_ref,   // earliest file number referred to in manifest
  const ImmutableDBOptions *immdbopts,   // The current Env
  EnvOptions& file_options  // options to use for all VLog files
)   : 
    ringno_(ringno),
    cfd_(cfd),
    immdbopts_(immdbopts),
    envopts_(file_options)  // must copy into heap storage here
{
#if DEBLEVEL&1
printf("VLogRing cfd_=%p\n",cfd_);
#endif

  VLogRingRefFileno latest_ref = 0;   // highest file number found
  // Find the largest filenumber for this ring
  for(auto pathfname : filenames) {
    ParsedFnameRing fnring = VlogFnametoRingFname(pathfname);

    if(fnring.ringno==ringno) {  // If this file is for our ring...
      if(latest_ref<fnring.fileno)latest_ref = fnring.fileno;
    }
  }

  // If there are no references to the ring, set the limits to empty.  We override the number of
  // the existing files, to handle the case where the database gets deleted down to no files while the
  // filenumbers have existing values; then some files get written with high numbers, and the system crashes
  // before the SSTs are validated.  Then on restart there will be no references but a large existing filenumber.
  // It would be bad to allocate the ring from 0 to this filenumber, because that might be MUCH larger than
  // the ring needs to be.  There are several ways to handle this; we choose the easiest, which is to delete
  // the dangling files
  //
  // Note that file 0 is not allocated.  It is reserved to mean 'file number not given'
  if(earliest_ref==high_value){earliest_ref = 1; latest_ref = 0;}

  // Allocate the rings for files and references.
  VLogRingRefFileno power2 = RingSizeNeeded(earliest_ref,latest_ref);
  while(power2-->0)fd_ring[0].emplace_back();  // mustn't use resize() - if requires copy semantics, incompatible with unique_ptr

  // For each file in this ring, open it (if its number is valid) or delete it (if not)
  for(auto pathfname : filenames) {
    ParsedFnameRing fnring = VlogFnametoRingFname(pathfname);

    if(fnring.ringno==ringno) {  // If this file is for our ring...
      if(earliest_ref<=fnring.fileno && fnring.fileno<=latest_ref) {
        // the file is in the range pointed to by the SSTs (not before the first reference, and not after the last).  Open it
        std::unique_ptr<RandomAccessFile> fileptr;  // pointer to the opened file
        immdbopts_->env->NewRandomAccessFile(pathfname, &fileptr,envopts_);  // open the file if it exists
          // move the file reference into the ring, and publish it to all threads
        fd_ring[0][Ringx(fd_ring[0],fnring.fileno)]=std::move(VLogRingFile(fileptr));
#if DEBLEVEL&2
printf("Opening file %s\n",pathfname.c_str());
#endif
        // if error opening file, we can't do anything useful, so leave file unopened, which will give an error if a value in it is referenced
// scaf error?
      } else {
        // The file is not referenced.  We must have crashed while deleting it, or before it was referenced.  Delete it now
        immdbopts_->env->DeleteFile(pathfname);   // ignore error - what could we do?
  // should log? scaf
      }
    }
  }

  // Set up the pointers for the ring, indicating validity
  // The conversion from file# to slot is fixed given the size of the ring, so the first
  // file does not necessarily go into position 0
  // The tail pointers point to the oldest ref
  atomics.fd_ring_tail_fileno.store(earliest_ref,std::memory_order_release);  // init at 0
  // The head pointers point to the last entry.  But, we always start by
  // creating a new file.  The oldest file may have some junk at the end, which will hang around until
  // the ring recycles.
  atomics.fd_ring_head_fileno.store(latest_ref,std::memory_order_release);
  // Init ring usecount to 0, meaning 'not in use', and set write lock to 'available'
  atomics.usecount.store(0,std::memory_order_release);
  atomics.writelock.store(0,std::memory_order_release);
  // Since nothing starts out relying on the previous buffer, we can safely clear it at any time
  atomics.fd_ring_prevbuffer_clear_fileno.store(0,std::memory_order_release);

  // Start us up using array 0
  atomics.currentarrayx.store(0,std::memory_order_release);
}

  // Delete the file.  Close it as random-access, then delete it
  void VLogRingFileDeletion::DeleteFile(VLogRing& v,   // the current ring
    const ImmutableDBOptions *immdbopts,   // The current Env
    EnvOptions& file_options  // options to use for all VLog files
  ) {
    filepointer = nullptr;  // This closes the file if it was open
    std::string filename = VLogFileName(v.immdbopts_->db_paths,
      VLogRingRef(v.ringno_,(int)fileno).FileNumber(), (uint32_t)v.immdbopts_->db_paths.size()-1, v.cfd_->GetName());
#if DEBLEVEL&2
printf("Deleting file: %s\n",filename.c_str());
#endif

    immdbopts->env->DeleteFile(filename);  // delete the file
// scaf ignore error - what could we do?
  }

// See how many files need to be deleted.  Result is a vector of them, with the ring entries for the old files cleared.
// The tail pointer is moved over the deleted files.
// This whole routine must run under the lock for the ring.
// On entry, we assume that we have just come back from reserving space for the deleted files, and that the tailpointer must be checked again
void VLogRing::CollectDeletions(
  VLogRingRefFileno tailfile,  // up-to-date tail file number.  There must be no files before it.
  VLogRingRefFileno headfile,  // up-to-date head file number
  std::vector<VLogRingFileDeletion>& deleted_files  // result: the files that are ready to be deleted.  The capacity must have been set and will not be exceeded in this routine
) {
  // If the tailpointer has moved since we decided to try to delete, that means that some other thread slipped in when we released the lock,
  // and has freed some files.  We don't need to free any, then
  if(tailfile!=atomics.fd_ring_tail_fileno.load(std::memory_order_acquire)) return;
#if DELAYPROB
ProbDelay();
#endif 
  // Fetch the up-to-date value for the current ring
  uint64_t arrayx = atomics.currentarrayx.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
  // In case the ring was resized, recalculate the slot number to use. 
  size_t slotx = Ringx(fd_ring[arrayx],tailfile);   // point to first file to consider deleting
  if(fd_ring[arrayx][slotx].refcount)return;  // return fast if the first file does not need to be deleted
  while(1) {  // loop till tailfile is right
    if(deleted_files.size()==deleted_files.capacity())break;  // if we have no space for another deletion, stop
    deleted_files.emplace_back(tailfile,fd_ring[arrayx][slotx]);  // mark file for deletion.  This also resets the ring slot to empty
    if(++slotx==fd_ring[arrayx].size())slotx=0;  // Step to next slot; don't use Ringx() lest it perform a slow divide
    ++tailfile;  // Advance file number to match slot pointer
    if(tailfile+deletion_deadband>headfile)break;   // Tail must stop at one past head, and that only if ring is empty; but we enforce the deadband
    if(fd_ring[arrayx][slotx].refcount)break;  // if the current slot has references, it will become the tail
  }
  // If we delete past the queued pointer, move the queued pointer
  if(tailfile>atomics.fd_ring_queued_fileno.load())atomics.fd_ring_queued_fileno.store(tailfile,std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
  // Now tailfile is the lowest file# that has a nonempty queue, unless there are no files, in which case it is one past the head
  // Save this value in the ring.  We do this AFTER we have made the changes to the ring, so that Get() will see them in that order
  atomics.fd_ring_tail_fileno.store(tailfile,std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
#if DEBLEVEL&8
printf("Tail pointer set; pointers=%lld %lld\n",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
#endif

}


// Resize the ring if necessary, updating currentarrayx if it is resized
// If the ring is resized, or if we found that no change was needed, return the new value of currentarrayx
//   (NOTE that in either case tailfile and headfile are NOT modified)
// If someone else took the lock when we laid it down, return 2 to indicate that fact (the resize must be retried)
// Requires holding the ring at start, and guarantees holding it at the end (though it may have been released)
uint64_t VLogRing::ResizeRingIfNecessary(VLogRingRefFileno tailfile, VLogRingRefFileno headfile, uint64_t currentarray,  // current ring pointers
  size_t numaddedfiles  // number of files we plan to add
){
  // If our new files will fit in the current ring, return fast
  if((headfile+numaddedfiles)<(tailfile+fd_ring[currentarray].size()))return currentarray;
  // We have to resize the ring.  See how big the new ring needs to be (a power-of-2 multiple of the current ring)
  VLogRingRefFileno allosize = RingSizeNeeded(tailfile,headfile+numaddedfiles);  // # slots to allocate all active files
  // Allocate a vector for the new ring.  We have to relinquish the lock while we do this lest the request for memory
  // block while we are holding the ring.  Therefore we cannot simply change the reservation in the existing previous ring,
  // because another thread may be trying to resize also, possibly to a different size.
  ReleaseLock();
    std::vector<VLogRingFile> newring;  // allocate ring
    newring.reserve(allosize);   // reserve all the space we need, so it will never be resized
    for(size_t i=0;i<allosize;++i)newring.emplace_back();  // fill ring with harmless empty entries.  Do this outside of lock, to reduce lock time
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();
  // If the ring pointers changed while we relinquished the ring, return with a value that will cause us to retry
  if(tailfile!=atomics.fd_ring_tail_fileno.load(std::memory_order_acquire) || headfile!=atomics.fd_ring_head_fileno.load(std::memory_order_acquire)
      || currentarray!=atomics.currentarrayx.load(std::memory_order_acquire))return 2;  // 2 means 'retry'
  // We are back in control.
  // Copy the old ring's data to the new ring.  This violates the 'no-copy' rule for unique_ptrs, as we temporarily
  // create a second copy of the data.  We will release the original before we exit.
  // NO EXCEPTIONS STARTING HERE
  for(size_t i=tailfile;i<=headfile;++i){  // copy the entire valid part of the ring from the old positions to the new ones
    newring[Ringx(newring,i)]=std::move(VLogRingFile(fd_ring[currentarray][Ringx(fd_ring[currentarray],i)],0));
  }
  // swap the new ring into the ping-pong buffer.  The old one, which is probably but not necessarily of 0 length,
  // will be destroyed when this function exits.  If it is not empty it will contain all nullptrs, because it is always a 'previous buffer' that
  // was cleared after it was swapped out
  uint64_t newcurrent = 1-currentarray;  // index of the new current buffer
  newring.swap(fd_ring[newcurrent]);
  // Set the point at which we deem we can reclaim the storage for the previous array.
  // We use a value of half a ring from where it is now; this will be rounded up since we sample only occasionally
  atomics.fd_ring_prevbuffer_clear_fileno.store(headfile+(allosize>>1),std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
  // switch buffers: set the new value of currentarrayx, thereby publishing the new ring to the system
  atomics.currentarrayx.store(newcurrent,std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
  // Go through the newly-previous ring, releasing all the pointers.  This restores the single ownership of the underlying resource in
  // the new ring
  for(size_t i=0;i<fd_ring[currentarray].size();++i)fd_ring[currentarray][i].filepointer.release();
  // EXCEPTIONS OK NOW
  return newcurrent;  // let operation proceed
  // newring, which now contains the obsolete third-from-last buffer, will be destroyed
}


// Write accumulated bytes to the VLogRing.  First allocate the bytes to files, being
// careful not to split a record, then write them all out.  Create new files as needed, and leave them
// open.  The last file will be open for write; the others are read-only
// The result is a VLogRingRef for the first (of possibly several sequential) file, and a vector indicating the
// number of bytes written to each file
// We housekeep the end-of-VLogRing information
// We use release-acquire ordering for the VLogRing file and offset to avoid needing a Mutex in the reader
// If the circular buffer gets full we have to relocate it, so we use release-acquire
void VLogRing::VLogRingWrite(
std::vector<NoInitChar>& bytes,   // The bytes to be written, jammed together
std::vector<VLogRingRefFileOffset>& rcdend,  // The running length of all records up to and including this one
VLogRingRef& firstdataref,   // result: reference to the first value written
std::vector<VLogRingRefFileOffset>& fileendoffsets,   // result: ending offset of the data written to each file.  The file numbers written are sequential
          // following the one in firstdataref.  The starting offset in the first file is in firstdataref; it is 0 for the others
std::vector<Status>& resultstatus   // result: place to save error status.  For any file that got an error in writing or reopening,
          // we add the error status to resultstatus and change the sign of the file's entry in fileendoffsets.  (no entry in fileendoffsets
          // can be 0).  must be initialized to empty by the caller
)
{
  // In this implementation, we write only complete files, rather than adding on to the last one
  // written by the previous call.  That way we don't have to worry about who is going to open the last file, and
  // whether it gets opened in time for the next batch to be added on.

  // We use spin locks rather than a mutex because we don't keep the lock for long and we never do anything
  // that might block while we hold the lock.  We use CAS to acquire the spinlock, and we code with the assumption that the spinlock
  // is always available, but takes ~50 cycles to complete the read

  // If there is nothing to write, abort early.  We must, because 0-length files are not allowed when memory-mapping is turned on
  // This also avoids errors if there are no references
  if(!bytes.size())return;   // fast exit if no data

  // split the input into file-sized pieces
int maxfilesize = 1000000;  // scaf
  // Loop till we have processed all the inputs
  int64_t prevbytes = 0;  // total bytes written to previous files
  for(size_t rcdx = 0; rcdx<rcdend.size();++rcdx){   // rcdptr is the index of the next input record to process
    // Calculate the output filesize, erring on the side of larger files.  In other words, we round down the number
    // of files needed, and get the target filesize from that.  For each calculation we use the number of bytes remaining
    // in the input, so that if owing to breakage a file gets too long, we will keep trying to prorate the remaining bytes
    // into the proper number of files.  We may end up reducing the number of files created if there is enough breakage.
    double bytesleft = (double)(bytes.size()-prevbytes);  // #unprocessed bytes, which is total - (total processed up to previous record).
    // Because zero-length files are not allowed, we need to check to make sure there is data to put into a file
    // (consider the case of lengths 5 5 5 5 1000000 0 0 0 0  which we would like to split into two files of size 500005).  If there
    // is no data left, stop with the files we have - the zero-length records will add on to the end of the last file
    if(bytesleft==0.0)break;   // stop to avoid 0-length file
    double targetfilect = std::max(1.0,std::floor(bytesleft/(double)maxfilesize));  // number of files expected, rounded down
    int64_t targetfileend = prevbytes + (int64_t)(bytesleft/targetfilect);  // min endpoint for this file (min number of bytes plus number of bytes previously output)
    // Allocate records to this file until the minimum fileend is reached.  Use binary search in case there are a lot of small files (questionable decision, since
    // the files would have been added one by one)
    size_t left = rcdx-1;  // init brackets for search
    // the loop variable rcdx will hold the result of the search, fulfilling its role as pointer to the end of the written block
    rcdx=rcdend.size()-1;
    while(rcdx!=(left+1)) {
      // at top of loop rcdend[left]<targetfileend and rcdend[rcdx]>=targetfileend.  This remains invariant.  Note that left may be before the search region, or even -1
      // Loop terminates when rcdx-left=1; at that point rcdx points to the ending record, i. e. the first record that contains enough data
      // Calculate the middle record position, which is known not to equal an endpoint (since rcdx-left>1)
      size_t mid = left + ((rcdx-left)>>1);   // index of middle value
      // update one or the other input pointer
      if(rcdend[mid]<targetfileend)left=mid; else rcdx=mid;
    }
    // rcdend[rcdx] has the file length.  Call for the output file.
    fileendoffsets.push_back(rcdend[rcdx]-prevbytes);
    prevbytes = rcdend[rcdx];  // update total # bytes written
  }

  std::vector<VLogRingFileDeletion> deleted_files;  // files put here will be deleted after we release the lock
  AcquireLock();
    VLogRingRefFileno headfile, tailfile; uint64_t currentarray;

    // Check for resizing the ring.  When we come out of this loop the value of heasfile, tailfile, currentarray will
    // be up to date, and we will still have the lock (though it may have been given up for a while)
    do{
      tailfile = atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
      headfile = atomics.fd_ring_head_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
      currentarray = atomics.currentarrayx.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
    }while(2==(currentarray = ResizeRingIfNecessary(tailfile,headfile,currentarray,fileendoffsets.size())));  // return of 2 means 'retry required', otherwise new currentarray

    // Allocate file#s for this write.
    VLogRingRefFileno fileno_for_writing = headfile+1;
  
    // Move the reservation pointer in the file.  Does not have to be atomic with the read, since we have acquired writelock
    atomics.fd_ring_head_fileno.store(headfile+fileendoffsets.size(),std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 

    // See if the tail pointer was being held up, for whatever reason
    if(tailfile<=headfile && fd_ring[currentarray][Ringx(fd_ring[currentarray],tailfile)].refcount==0) {  // if tail>head, the ring is empty & refcounts are invalid
      // The file at the tail pointer was deletable, which means the tail pointer was being held up either by proximity to the headpointer or
      // because a deletion operation hit its size limit.  We need to delete files, starting at the tailpointer.
      // This is a very rare case but we have to handle it because otherwise the tail pointer could get stuck
      //
      // We need to make sure we don't perform any large memory allocations that might block while we are holding a lock on the ring, so we reserve space for the
      // deletions - while NOT holding the lock - and then count the number of deletions
      ReleaseLock(); deleted_files.reserve(max_simultaneous_deletions); AcquireLock(); // reserve space to save deletions, outside of lock
      // Go get list of files to delete, updating the tail pointer if there are any.  CollectDeletions will reestablish tailptr, arrayx, slotx
      CollectDeletions(tailfile,headfile+fileendoffsets.size(),deleted_files);  // find deletions
    }

  ReleaseLock();

#if DEBLEVEL&8
printf("Head pointer set; pointers=%lld %lld\n",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
#endif

  // We had to release the lock before we actually deleted the files, if any.  The files to delete are in deleted_files.  Delete them now
  ApplyDeletions(deleted_files);

  // Now create the output files.

  // We do not hold a lock during file I/O, so we must save anything that we will need to install into the rings
  std::vector<std::string> pathnames;   // the names of the files we create
  std::vector<std::unique_ptr<RandomAccessFile>> filepointers;  // pointers to the files, as random-access

  char *startofnextfile = (char *)bytes.data();  // starting position of the next file.  Starts at beginning, advances by file-length

#if DEBLEVEL&2
printf("Writing %zd sequential files, %zd values, %zd bytes\n",fileendoffsets.size(),rcdend.size(), bytes.size());
#endif

  // Loop for each file: create it, reopen as random-access
  for(int i=0;i<fileendoffsets.size();++i) {
    Status iostatus;  // where we save status from the file I/O

    // Pass aligned buffer when use_direct_io() returns true.   scaf ??? what do we do for direct_io?
    // Create filename for the file to write
    pathnames.push_back(VLogFileName(immdbopts_->db_paths,
      VLogRingRef(ringno_,(int)fileno_for_writing+i).FileNumber(), (uint32_t)immdbopts_->db_paths.size()-1, cfd_->GetName()));

    // Create the file as sequential, and write it out
    {
#if DEBLEVEL&2
printf("file %s: %zd bytes\n",pathnames.back().c_str(), fileendoffsets[i]);
#endif

      unique_ptr<WritableFile> writable_file;
      iostatus = immdbopts_->env->NewWritableFile(pathnames.back(),&writable_file,envopts_);  // open the file
      if(iostatus.ok())iostatus = writable_file->Append(Slice(startofnextfile,fileendoffsets[i]));  // write out the data
      startofnextfile += fileendoffsets[i];  // advance write pointer to position for next file

      // Sync the written data.  We must make sure it is synced before the SSTs referring to it are committed to the manifest.
      // We might as well sync it right now
      if(iostatus.ok())iostatus = writable_file->Fsync();
    }  // this closes the writable_file

    // Reopen the file as randomly readable
    std::unique_ptr<RandomAccessFile> fp;  // the open file
    if(iostatus.ok()){
      immdbopts_->env->NewRandomAccessFile(pathnames.back(), &fp, envopts_);
      if(fp.get()==nullptr)iostatus = Status::IOError("unable to reopen file.");
    }
    filepointers.push_back(std::move(fp));  // push one fp, even if null, for every file slot

    // if there was an I/O error on the file, return the error, localized to the file by a negative offset in the return area.
    // (the offset can never be zero)
    if(!iostatus.ok()) {
      resultstatus.push_back(iostatus);  // save the error details
      fileendoffsets[i] = -fileendoffsets[i];  // flag the offending file
    }
  }

  // Files are written and reopened.  Transfer them to the fd_ring under lock.
  // ALSO: clear the unused ping-pong buffer, if the current one has been in place 'long enough'

  // We lock during the initialization of the added block because the ring may be resized at any time & thus all changes to it must be performed
  // under lock to make sure they get resized correctly.  The lock is not needed to make Get() work.
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();
    // Figure out which ring slot the new file(s) will go into
    currentarray = atomics.currentarrayx.load(std::memory_order_acquire);  // fetch current ping-pong side
#if DELAYPROB
ProbDelay();
#endif 
    std::vector<VLogRingFile> *fdring = fd_ring + currentarray;  // the current ring array
    size_t new_ring_slot = Ringx(*fdring,fileno_for_writing);  // position therein

    // Loop to add each file to the ring
    for(int i=0;i<pathnames.size();++i) {
      (*fdring)[new_ring_slot]=std::move(VLogRingFile(filepointers[i]));
      if(++new_ring_slot==(*fdring).size()){
        //  The write wraps around the end of the ring buffer...
        new_ring_slot = 0;   // advance to next slot, and handle wraparound (avoiding divide)
        // ...and at that point we check to see if the head pointer has been incremented through half of its size.
        // Surely every core has by now been updated with the new ping-pong buffer; so we can clear
        // the previous buffer.
        // Almost always, this will be fast because the previous buffer will already be empty.  Only the first time after
        // a resize will this clear a large vector.  And even then, the elements of the vector will already have been
        // invalidated (when they were moved to the current buffer) and thus will take no processing beyond visiting the pointer.
        // Thus, it is safe to do this under lock; it is the completion of the ring resizing.  We will only have trouble if
        // freeing the buffer causes this thread to be preempted by a higher-priority thread, leaving this thread waiting while holding
        // the lock; but nothing could be done to prevent that anyway.
        if(atomics.fd_ring_head_fileno.load(std::memory_order_acquire)>atomics.fd_ring_prevbuffer_clear_fileno.load(std::memory_order_acquire)){
          fd_ring[1-currentarray].clear();  // clear the previous buffer, to save space
        }
#if DELAYPROB
ProbDelay();
#endif 
      }
    }
  ReleaseLock();
 
  // move the file reference into the ring, and publish it to all threads

  // fill in the FileRef for the first value, with its length
  firstdataref.FillVLogRingRef(ringno_,fileno_for_writing,0,rcdend[0]);

  return;

}

// Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
// Returns the bytes.  ?? Should this return to user area to avoid copying?
Status VLogRing::VLogRingGet(
  VLogRingRef& request,  // the values to read
  std::string *response   // return value - the data pointed to by the reference
)
{
  response->resize(request.Len());  // allocate area for return
  Slice resultslice;  // place to get back pointer to data, which may be different from scratch (if cached)
  // Read the reference.  Make sure we have the most recent copy of the ring information, and the most recent
  // copy of the file pointer.

  // We have to get the latest copy of the validity information, and also the latest copy of the memory pointer.
  // We have no way of knowing when these become valid, as they are not protected by any lock.  But they should have
  // been updated long before we need them.  If the current values indicate invalid references or nonexistent pointers
  // wait a little while and retry, before giving up.
  //
  // The tricky part is when the ring has to grow owing to growth in the database.  We don't want Get() to have to acquire a lock,
  // so we keep two rings hanging around: the current one and its predecessor, which is half as big.  As part of request processing
  // we get a pointer to the current ring, which might be replaced before we finish processing.  But that's OK: the old ring will
  // be valid enough to get us to the correct file or to indicate that we need to reacquire the ring pointer.
  //
  // We hold the previous ring around for a long time, long enough to be sure the new ring is in use in all threads.

  // Even though we should never be presented with an invalid request (i. e. a request for a file that does not exist),
  // we must nevertheless verify that the request is between the head and tail pointers, because if the ring has grown and we are
  // still using the previous ring, a valid reference in the new ring to a slot that doesn't exist in the old ring will be aliased
  // to an incorrect slot in the old ring.  This is very unlikely because the ring grows in the middle of compaction and the
  // references to new slots are not available to the user until the compaction is finished and versioned; but on non-Intel machines that
  // have loose memory coherence it is possible that a stale cache might persist for long enough to make trouble.

  RandomAccessFile *selectedfile;  // this will be the pointer to the file, once we have validated everything

  // We use acquire/release memory ordering on all accesses to the ring pointers (acquire for load, release for store).  This memory ordering means
  // that if we load a value X, we can be sure that subsequent reads to ANY memory (not just ring pointers) will see any store by the writer of X that happened before the
  // store to X.  It doesn't mean that the load gets the 'current' value, whatever that would mean; in particular, if a store is in flight, a second load from the same address
  // might return a different value even though no new store has been issued anywhere.
  //
  // The way to think of this is: if I load x=a and y=b with acquire/release ordering, the value of y may be from a later value of b than the one in effect when x was loaded,
  // but it can never be from an earlier value of b.

  // On Intel architectures acquire/release ordering does not take any extra time.

  // loop until we have resolved the pointer:
  int retrystate = 0;   // number of times we have retried
  while(1) {
    // acquire the head pointer
    VLogRingRefFileno headfile = atomics.fd_ring_head_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
    // acquire the tail pointer.  This may be ahead of the status matching the head pointer, but that's OK as any valid reference should be
    // ahead of any valid tailpointer
    VLogRingRefFileno tailfile = atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
    // acquire the current ring.  It is guaranteed to be able to hold the head and tail pointers that we have read, even if we are in the
    // process of switching rings, because we know that the head/tail pointers are valid from a time earlier than the ring
    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // get the filepointer at the slot indicated for this file.  The pointer fetched may be bogus, if the file number is out of range, but
    // it will fetch without error.  The filepointer fetched here will be valid as of the time the head/tail pointers were set, meaning that
    // if the filenumber is valid, the pointer will be, UNLESS the ring was switched, in which case it will be either valid or null.  If null, we
    // will retry.  We don't use an atomic load operation, because we can't have an atomic unique_ptr (at least not in C++17); but loading a pointer will
    // be atomic on any modern CPU anyway
    selectedfile = (*fdring)[Ringx(*fdring,request.Fileno())].filepointer.get();  // fetch the filepointer, unconditionally

    // If the loaded pointer is valid and nonnull, go use it
    if(request.Fileno()<=headfile && request.Fileno()>=tailfile && selectedfile!=nullptr)break;  // could use conditional assignment, but we hope these branches are predicted right

    // Here we did not resolve the file correctly.  The possible causes, in order of severity, are:
    // 1. The head pointer that we fetched was stale and the reference is to a later file.  Since there is no guarantee that the load-for-acquire fetched the
    //  latest value stored in another thread, this may not be an error, and the problem will go away 'soon', where soon depends on the CPU architecture.  On
    //  an Intel architecture with its strict concurrency rules, this problem will never show up because there is a long delay between the time a file is created and
    //  the first time it can be referenced; but on a permissive-concurrency architecture it is possible, especially if a core wakes up after a delay with stale data in caches.
    //  We should wait a very short time and retry.
    // 2. The ring was resized.  This happens very rarely - every few weeks, perhaps.  The ring is locked for a few milliseconds (a few dozen instructions per file),
    //  but the old ring should suffice to handle Get() requests without interruption.  After the new ring is in place, all pointers in the previous one are released,
    //  and we must have fetched one of those.  A very short wait should suffice, then retry
    // 3. The reference is unusable - either it is corrupted or the file did not exist.  Either of those is Major Bad News, and we delay quite some time in hopes it will go away.
    //
    // We just run through delays of increasing length until the problem goes away or we declare it permanent
    if(retrystate<3){}   // first 3 times, just loop fast
    else if(retrystate<4){std::this_thread::sleep_for(std::chrono::microseconds(1));}  // then, 1usec - case 1/2
    else if(retrystate<6){std::this_thread::sleep_for(std::chrono::milliseconds(100));}  // then, 100msec - case 3
    else{selectedfile = nullptr; break;}   // then, permanent error.  Set selectedfile null as the error indicator

    ++retrystate;  // count the number of retries
  }

  // Read the reference, unless there is a permanent error, in which case set the error message
  Status iostatus = selectedfile!=nullptr ? selectedfile->Read(request.Offset(), request.Len(), &resultslice, (char *)response->data())
                                          : Status::Corruption("Indirect reference to nonexistent file")
                                          ;
  // If the result is not in the user's buffer, copy it to there
  if(iostatus.ok()){
    if(response->data()!=resultslice.data())response->assign(resultslice.data(),resultslice.size());  // no error: copy data only if not already in user's buffer
  }else{response->clear();}  // error: return empty string
// scaf decompress and check CRC
  return iostatus;
}

// Install a new SST into the ring, with the given earliest-VLog reference.  Increment refcount
void VLogRing::VLogRingSstInstall(
  FileMetaData& newsst   // the SST that has just been created & filled in
){
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();  // lock at the Ring level to allow multiple compactions on the same CF

    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // Find the slot that this reference maps to
    size_t slotx = Ringx(*fdring,newsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid
// scaf out of bounds?

    // Increment the usecount for the slot
    ++(*fdring)[slotx].refcount;  // starting from now until this SST is deleted, this VLog file is in use

    // Install the SST into the head of the chain for the slot
    newsst.ringfwdchain[ringno_] = (*fdring)[slotx].queue;   // attach old chain after new head
    newsst.ringbwdchain[ringno_] = nullptr;   // there is nothing before the new head
    if((*fdring)[slotx].queue!=nullptr)(*fdring)[slotx].queue->ringbwdchain[ringno_] = &newsst;  // if there is an old chain, point old head back to new head
    (*fdring)[slotx].queue = &newsst;  // point anchor to the new head
    
  ReleaseLock();
#if DEBLEVEL&1
printf("VLogRingSstInstall newsst=%p ref0=%lld refs=%d",&newsst,newsst.indirect_ref_0[ringno_],newsst.refs);
printf("\n");
#endif
#if DEBLEVEL&128
printf("Ring pointers: tail=%zd head=%zd\n   queue=",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){int ct=0; FileMetaData* qp=(*fdring)[i].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("\nrefcount=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[i].refcount);}
printf("\n");
#endif
}

// Remove an SST from the ring when it is no longer current.  Do not adjust refcount.
void VLogRing::VLogRingSstUnCurrent(
  FileMetaData& retiringsst   // the SST that has just been obsoleted
){
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();  // lock at the Ring level to allow multiple compactions on the same CF
#if DELAYPROB
ProbDelay();
#endif 

    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // Find the slot that this reference maps to
    size_t slotx = Ringx(*fdring,retiringsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid

    // Take the SST out of the ring.  It must be there.
    assert(retiringsst.ringbwdchain[ringno_] != &retiringsst);
    if(retiringsst.ringfwdchain[ringno_]!=nullptr)retiringsst.ringfwdchain[ringno_]->ringbwdchain[ringno_] = retiringsst.ringbwdchain[ringno_];  // if there is a next, point it back to the elements before this
    if(retiringsst.ringbwdchain[ringno_]!=nullptr)retiringsst.ringbwdchain[ringno_]->ringfwdchain[ringno_] = retiringsst.ringfwdchain[ringno_];  // if there is a prev, point it to the elements after this
    else (*fdring)[slotx].queue = retiringsst.ringfwdchain[ringno_]; // but if deleting the head, just make the next ele the head
    // Mark the chain fields to show that this block is no longer on the ring.  We use chain-to-self to indicate this
    retiringsst.ringbwdchain[ringno_] = &retiringsst;  // indicate 'block not in ring'

  ReleaseLock();
#if DEBLEVEL&1
printf("VLogRingSstUnCurrent retiringsst=%p ref0=%lld refs=%d",&retiringsst,retiringsst.indirect_ref_0[ringno_],retiringsst.refs);
printf("\n");
#endif
#if DEBLEVEL&128
printf("Ring pointers: tail=%zd head=%zd\n   queue=",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){int ct=0; FileMetaData* qp=(*fdring)[i].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("\nrefcount=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[i].refcount);}
printf("\n");
#endif
}

// Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused.  Advance tail pointer past deleted files
void VLogRing::VLogRingSstDelete(
  FileMetaData& expiringsst   // the SST that is about to be destroyed
){
  std::vector<VLogRingFileDeletion> deleted_files;  // files put here will be deleted at the end of this routine
  // If this file is still on the queue for this ring, we must be closing and releasing resources.  We don't want to delete all the files just because
  // the references went away!  But we can be sure that if this SST is still on the queue for a file, that file is still needed.  So, we detect that
  // case and DO NOT reduce the usecount for the file.  Eventually the rings will be freed, which will close the files but will not delete the data
  if(expiringsst.ringbwdchain[ringno_] != &expiringsst){
#if DEBLEVEL&1
  printf("Deleting file on ring: %p\n",&expiringsst);
#endif
    VLogRingSstUnCurrent(expiringsst);  // If backchain is valid (not looped to self), this block is still on the queue.  Remove from queue but leave (permanently) referenced
    return;
  }
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();  // lock at the Ring level
#if DELAYPROB
ProbDelay();
#endif 

    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // Find the slot that this reference maps to
    size_t slotx = Ringx(*fdring,expiringsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid

    // Decrement the usecount
    if(!--(*fdring)[slotx].refcount) {
      // The usecount went to 0.  We can free the file, if it is the oldest in the tail.
      // NOTE that the tailfile coming in might have a 0 refcount, if the previous deletion was interrupted.  We will never catch that case here, because
      // we are looking only for NEW deletions at the tail.  Interrupted deletions will be resumed the next time we CREATE a file.  That's not really a problem,
      // because whenever we delete SSTs for compaction we are also creating new ones; & if we aren't, there's no hurry to delete anything.
      VLogRingRefFileno tailfile = atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);  // get up-to-date copy of tail file
#if DELAYPROB
ProbDelay();
#endif 
      if(expiringsst.indirect_ref_0[ringno_]==tailfile) {
        // the expiring sst was earliest in the tail.  See how many files in the tail have 0 usecount.  Stop looking
        // after we have checked the head.
        VLogRingRefFileno headfile = atomics.fd_ring_head_fileno.load(std::memory_order_acquire);  // get up-to-date copy of head file
#if DELAYPROB
ProbDelay();
#endif 
        // We need to make sure we don't perform any large memory allocations that might block while we are holding a lock on the ring, so we reserve space for the
        // deletions - while NOT holding the lock - and then count the number of deletions
        ReleaseLock(); deleted_files.reserve(max_simultaneous_deletions); AcquireLock(); // reserve space to save deletions, outside of lock
        // Go get list of files to delete, updating the tail pointer if there are any.  CollectDeletions will reestablish tailptr, arrayx, slotx
        CollectDeletions(tailfile,headfile,deleted_files);  // find deletions
      }
    }

  ReleaseLock();

  ApplyDeletions(deleted_files);  // Now that we have released the lock, delete any files we marked for deletion

#if DEBLEVEL&1
printf("VLogRingSstDelete expiringsst=%p ref0=%lld refs=%d",&expiringsst,expiringsst.indirect_ref_0[ringno_],expiringsst.refs);
printf("\n");
#endif
#if DEBLEVEL&128
printf("Ring pointers: tail=%zd head=%zd\n   queue=",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){int ct=0; FileMetaData* qp=(*fdring)[i].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("\nrefcount=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[i].refcount);}
printf("\n");
#endif

}

// Return a vector of up to n SSTs that have the smallest oldest-reference-filenumbers.  If extend is true, return all SSTs
// whose filenumber does not exceed that of the nth-smallest SST's (in other words, return every SST that is tied with n).
void VLogRing::VLogRingFindLaggingSsts(
  int n,  // number of lagging ssts to return
  std::vector<FileMetaData*>& laggingssts,  // result: vector of SSTs that should be recycled
  int extend   // if 1, report all (default 0)
) {}
// Do this operation under spin lock.  Reheap to close up deleted SSTs whenever we encounter them


//
//
// ******************************************** Vlog ***********************************************
// One per CF, containing one or more VLogRings.  Vlog is created for any CF that uses a table type
// that is compatible with indirect values.
//
//

// A VLog is a set of VLogRings for a single column family.  The VLogRings are distinguished by an index.
VLog::VLog(
  // the info for the column family
  ColumnFamilyData *cfd
) :
  rings_(std::vector<std::unique_ptr<VLogRing>>()),  // start with empty rings
  starting_level_for_ring_(std::vector<int>()),
  cfd_(cfd),
  waiting_sst_queues(std::vector<FileMetaData *>{4})  // init max possible # ring anchors
{  writelock.store(0,std::memory_order_release);
#if DEBLEVEL&1
printf("VLog cfd=%p name=%s\n",cfd,cfd->GetName().data());
#endif
}


// Initialize each ring to make it ready for reading and writing.
// It would be better to do this when the ring is created, but with the existing interfaces there is
// no way to get the required information to the constructor
Status VLog::VLogInit(
    std::vector<std::string> vlg_filenames,    // all the filenames that exist for this database - at least, all the vlg files
    const ImmutableDBOptions *immdbopts,   // The current Env
    EnvOptions& file_options  // options to use for all VLog files
) {
#if DEBLEVEL&1
printf("VLogInit cfd_=%p name=%s\n",cfd_,cfd_->GetName().data());
#endif
  // Save the ring starting levels
    starting_level_for_ring_.push_back(0);  // scaf

  // If there are VlogRings now, it means that somehow the database was reopened without being destroyed.  This is bad form but not necessarily fatal.
  // To cope, we will delete the old rings, which should go through and free up all their resources.  Then we will continue with new rings.
  rings_.clear();  // delete any rings that are hanging around

  // cull the list of files to leave only those that apply to this cf
  std::vector<std::string> existing_vlog_files_for_cf;  // .vlg files for this cf
  std::string columnsuffix = "." + kRocksDbVLogFileExt + cfd_->GetName();  // suffix for this CF's vlog files
  for(auto fname : vlg_filenames){
    if(fname.size()>columnsuffix.size() && 0==fname.substr(fname.size()-columnsuffix.size()).compare(columnsuffix))
      existing_vlog_files_for_cf.emplace_back(fname);  // if suffix matches, keep the filename
  }

  // For each ring, allocate & initialize the ring, and save the resulting object address
  for(int i = 0; i<starting_level_for_ring_.size(); ++i) {
    // Loop through the queued SSTs to find the earliest ref therein.  Every block on chain i has a nonzero earlyref for ring i
    VLogRingRefFileno early_ref = high_value;  // init to no ref
    FileMetaData *sstptr = waiting_sst_queues[i];  // start on the chain for this ring
    while(sstptr!=nullptr){if(early_ref > sstptr->indirect_ref_0[i])early_ref = sstptr->indirect_ref_0[i]; sstptr = sstptr->ringfwdchain[i];}
    
    // Create the ring and save it
    rings_.push_back(std::move(std::make_unique<VLogRing>(i /* ring# */, cfd_ /* ColumnFamilyData */, existing_vlog_files_for_cf /* filenames */,
      early_ref /* earliest_ref */, 
      immdbopts /* immdbopts */, file_options)));

    // Traverse the sst waiting queue, taking each SST off and installing it into the queue according to its reference.
    // Each call will lock the ring, but the ones after the first should be processed very quickly because the lock
    // will still be held by this core
    sstptr = waiting_sst_queues[i];  // start on the chain for this ring
    while(sstptr!=nullptr){FileMetaData *nextsstptr = sstptr->ringfwdchain[i]; rings_[i]->VLogRingSstInstall(*sstptr); sstptr = nextsstptr; }
       // remove new block from chain before installing it into the ring
  }

// status? scaf
  return Status();
}


// Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
// Returns the bytes.  ?? Should this return to user area to avoid copying?
Status VLog::VLogGet(
  const Slice& reference,  // the reference
  std::string *result   // where the result is built
)
{
  assert(reference.size()==16);  // should be a reference
  if(reference.size()!=16){return Status::Corruption("indirect reference is ill-formed.");}
  VLogRingRef ref = VLogRingRef(reference.data());   // analyze the reference

  // Because of the OS kludge that doesn't allow zero-length files to be memory-mapped, we have to check to make
  // sure that the reference doesn't have 0 length: because the 0-length reference might be contained in a
  // (nonexistent) 0-length file, and we'd better not try to read it
  if(!ref.Len()){ result->clear(); return Status(); }   // length 0; return empty string, no error

  // Vector to the appropriate ring to do the read
  return rings_[ref.Ringno()]->VLogRingGet(ref,result);
}

  void VLog::VLogSstInstall(
    FileMetaData& newsst   // the SST that has just been created & filled in
  ) {
#if DEBLEVEL&64
if(!rings_.size() || !newsst.indirect_ref_0.size())printf("VLogSstInstall newsst=%p\n",&newsst);
#endif
    // Initialization of SSTs that have been installed on the ring:
    // put the address of this VLog into the SST, so we can get back to this VLog from a reference to the file
    newsst.vlog=this;   // We have to have this chain field since cfd is not available in most functions
    // Make sure there is one chain-field pair per ring containing a reference
    newsst.ringfwdchain.clear(); newsst.ringfwdchain.resize(newsst.indirect_ref_0.size(),nullptr);
    newsst.ringbwdchain.clear(); newsst.ringbwdchain.resize(newsst.indirect_ref_0.size(),&newsst);
    // We set the backchain pointer to point to the block itself as an indication that the block is not
    // on the queue for a VLog file.  We need to know the queue status so that we don't leave a block on
    // a ring after the block itself has been deleted.  The not-on-queue indication persists throughout for
    // those rings that the block doesn't have a reference for; for the others it changes based on queue status

    if(!rings_.size()){
      // if the rings have not been, or never will be, created, enqueue them here in the Vlog.  In case initialization ever
      // becomes multi-threaded, acquire a lock on the ring headers
      AcquireLock();
        // for each ring that has a reference in the SST, enqueue the SST to that ring's chains.  We know that there
        // are chain-fields for each ring containing a reference.  We use only the forward chains here since this queue
        // is consumed from head to tail
        for (int i=0;i<newsst.indirect_ref_0.size();++i)
          if(newsst.indirect_ref_0[i]){newsst.ringfwdchain[i] = waiting_sst_queues[i]; waiting_sst_queues[i] = &newsst;};
      ReleaseLock();
    } else {
      // Normal case after initial recovery.  Put the SST into each ring for which it has a reference
if(newsst.indirect_ref_0.size()){
  printf("VLogSstInstall newsst=%p level=%d\n",&newsst,newsst.level);  // scaf
}
  if(newsst.level<0)
    printf("installing SST with no level\n");
      for (int i=0;i<newsst.indirect_ref_0.size();++i)if(newsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstInstall(newsst);
    }
  }

  // Remove an SST from the ring when it is no longer current.
  void VLog::VLogSstUnCurrent(
    FileMetaData& retiringsst   // the SST that has just been obsoleted
  ) {
#if DEBLEVEL&64
if(!rings_.size() || !retiringsst.indirect_ref_0.size())printf("VLogSstUnCurrent retiringsst=%p rings_.size()=%zd\n",&retiringsst,rings_.size());
#endif
    if(!rings_.size())return;  // no action if the rings have not, or never will be, created
    for (int i=0;i<retiringsst.indirect_ref_0.size();++i)if(retiringsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstUnCurrent(retiringsst);
    }

  // Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused
  void VLog::VLogSstDelete(
    FileMetaData& expiringsst   // the SST that is about to be destroyed
  ) {
#if DEBLEVEL&64
if(!rings_.size() || !expiringsst.indirect_ref_0.size())printf("VLogSstDelete expiringsst=%p rings_.size()=%zd\n",&expiringsst,rings_.size());
#endif
    if(!rings_.size())return;  // no action if the rings have not, or never will be, created
    for (int i=0;i<expiringsst.indirect_ref_0.size();++i)if(expiringsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstDelete(expiringsst);
    }


}   // namespace rocksdb


	
