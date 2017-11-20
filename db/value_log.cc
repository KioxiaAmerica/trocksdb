// changes needed in main Rocks code:
// FileMetaData needs a unique ID number for each SST in the system.  Could be unique per column family if that's easier
// Manifest needs smallest file# referenced by the SST

//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#define DEBLEVEL 0

#include <atomic>
#include <memory>
#include "db/value_log.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "options/cf_options.h"
#include "rocksdb/status.h"

namespace rocksdb {


static const VLogRingRefFileno high_value = ((VLogRingRefFileno)-1)>>1;  // biggest positive value
static const float expansion_fraction = 0.25;  // fraction of valid files to leave for expansion
static const int expansion_minimum = 1000;  // minimum number of expansion files

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
printf("VLogRing cfd_=%p\n",cfd_); // scaf debug
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
  if(earliest_ref==high_value){earliest_ref = 1; /*latest_ref = 0 scaf */;}

  // Allocate the rings for files and references.  Allocate as many files as we need to hold the
  // valid files, plus some room for expansion (here, the larger of a fraction of the number of valid files
  // and a constant)
  VLogRingRefFileno ringexpansion = (int)((latest_ref-earliest_ref)*expansion_fraction);
  if(ringexpansion<expansion_minimum)ringexpansion = expansion_minimum;
ringexpansion = 16384;  // scaf
  // force the ring to a power-of-2 multiple so that Ringx() can be efficient
  ringexpansion = latest_ref-earliest_ref+ringexpansion;
  VLogRingRefFileno power2; for(power2 = 1;power2<ringexpansion;power2<<=1);
  while(power2-->0)fd_ring.emplace_back();

  // For each file in this ring, open it (if its number is valid) or delete it (if not)
  for(auto pathfname : filenames) {
    ParsedFnameRing fnring = VlogFnametoRingFname(pathfname);

    if(fnring.ringno==ringno) {  // If this file is for our ring...
      if(earliest_ref<=fnring.fileno && fnring.fileno<=latest_ref) {
        // the file is in the range pointed to by the SSTs (not before the first reference, and not after the last).  Open it
#if 0 // scaf delete
        std::unique_ptr<RandomAccessFile> fileptr;  // pointer to the opened file
        if((immdbopts_->env->NewRandomAccessFile(pathfname, &fileptr,envopts_)).ok()){  // open the file if it exists
#endif
          // move the file reference into the ring, and publish it to all threads
        fd_ring[Ringx(fnring.fileno)]=std::move(VLogRingFile(pathfname,immdbopts_,envopts_));
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
  atomics.fd_ring_tail_fileno_shadow.store(earliest_ref,std::memory_order_release);
  atomics.fd_ring_tail_fileno.store(earliest_ref,std::memory_order_release);  // init at 0
  // The head pointers point to the last entry.  But, we always start by
  // creating a new file.  The oldest file may have some junk at the end, which will hang around until
  // the ring recycles.
  atomics.fd_ring_head_fileno.store(latest_ref,std::memory_order_release);
  atomics.fd_ring_head_fileno_shadow.store(latest_ref,std::memory_order_release);
// obsolete  // The index into the last file is always 0, since we are starting on a new file
// obsolete  atomics.fd_ring_head_fileoffset = 0;
  // Init ring usecount to 0, meaning 'not in use', and set write lock to 'available'
  atomics.usecount.store(0,std::memory_order_release);
  atomics.writelock.store(0,std::memory_order_release);

  // install references into queue  scaf

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
std::string& bytes,   // The bytes to be written, jammed together
std::vector<VLogRingRefFileOffset>& rcdend,  // The running length of all records up to and including this one
VLogRingRef& firstdataref,   // result: reference to the first values written
std::vector<VLogRingRefFileLen>& fileendoffsets,   // result: ending offset of the data written to each file.  The file numbers written are sequential
          // following the one in firstdataref.  The starting offset in the first file is in firstdataref; it is 0 for the others
std::vector<Status>& resultstatus   // place to save error status.  For any file that got an error in writing or reopening,
          // we add the error status to resultstatus and change the sign of the file's entry in fileendoffsets.  (no entry in fileendoffsets
          // can be 0).  must be initialized to empty by the caller
)
{
  Status iostatus;  // where we save status from the file I/O

  // In this implementation, we write only complete files, rather than adding on to the last one
  // written by the previous call.  That way we don't have to worry about who is going to open the last file, and
  // whether it gets opened in time for the next batch to be added on.

  // We use spin locks rather than a mutex because we don't keep the lock for long and we never do anything
  // that might block while we hold the lock.  We use CAS to acquire the spinlock, and we code with the assumption that the spinlock
  // is always available, but takes ~50 cycles to complete the read

  // If there is nothing to write, abort early.  We must, because 0-length files are not allowed when memory-mapping is turned on
  // This also avoids errors if there are no references
  if(!bytes.size())return;   // fast exit if no data

  // scaf version for single file

  // Remove any empty files from the tail of file list so we don't allocate file space for them

  do{
    uint32_t expected_atomic_value = 0;
    if(atomics.writelock.compare_exchange_strong(expected_atomic_value,1,std::memory_order_acq_rel))break;
  }while(1);  // get lock on file

  // Allocate a file# for this write.  Must get from the shared copy of the pointers
  VLogRingRefFileno fileno_for_writing = atomics.fd_ring_head_fileno.load(std::memory_order_acquire)+1;
  
  // Move the reservation pointer in the file.  Does not have to be atomic with the read, since we have acquired writelock
  atomics.fd_ring_head_fileno.store(fileno_for_writing,std::memory_order_release);

  // Figure out which ring slot the new file(s) will go into
  size_t new_ring_slot = Ringx(fileno_for_writing);

  // Release the lock
  atomics.writelock.store(0,std::memory_order_release);

  // Write to file
  // Pass aligned buffer when use_direct_io() returns true.   scaf ??? what do we do for direct_io?
  // Create filename for the file to write
  std::string fname = VLogFileName(immdbopts_->db_paths,
    VLogRingRef(ringno_,(int)fileno_for_writing).FileNumber(), (uint32_t)immdbopts_->db_paths.size()-1, cfd_->GetName());

  // Create the file as sequential, and write it out
  {
    unique_ptr<WritableFile> writable_file;
    iostatus = immdbopts_->env->NewWritableFile(fname,&writable_file,envopts_);  // open the file
    if(iostatus.ok())iostatus = writable_file->Append(Slice(bytes));  // write out the data

  // Sync the written data.  We must make sure it is synced before the SSTs referring to it are committed to the manifest.
  // We might as well sync it right now
    if(iostatus.ok())iostatus = writable_file->Fsync();
  }  // this closes the file

  // Reopen the file as randomly readable; install the new file into the ring
  if(iostatus.ok()){
    fd_ring[new_ring_slot]=std::move(VLogRingFile(fname,immdbopts_,envopts_));
    if(fd_ring[new_ring_slot].filepointer.get()==nullptr)iostatus = Status::IOError("unable to reopen file.");
#if 0  // scaf will delete
    iostatus = immdbopts_->env->NewRandomAccessFile(fname, &fd_ring[new_ring_slot], envopts_);  // open the file - it must exist
#endif
  }
  // move the file reference into the ring, and publish it to all threads

  // Advance the shadow file pointer to indicate that the file is ready for reading.  Since other threads may be
  // doing the same thing, make sure the file pointer never goes backwards
// scaf could hop over unfinished writes... not a problem?  no, we should advance when we are writing <= the shadow pointer, but advance over all valid file pointers
  VLogRingRefFileno expected_fileno = atomics.fd_ring_head_fileno_shadow.load(std::memory_order_acquire);
  while(expected_fileno<fileno_for_writing) {
    if(atomics.fd_ring_head_fileno_shadow.compare_exchange_strong(expected_fileno,fileno_for_writing,std::memory_order_acq_rel))break;
  };

  // fill in the FileRef for the first value, with its length
  firstdataref.FillVLogRingRef(ringno_,fileno_for_writing,0,rcdend[0]);  // scaf offset goes away

  // Return all as a single file
  fileendoffsets.clear();

  // if there was an I/O error on the file, return the error, localized to the file by a negative offset in the return area.
  // (the offset can never be zero)
  if(iostatus.ok())fileendoffsets.push_back(bytes.size());
  else {
    resultstatus.push_back(iostatus);  // save the error details
    fileendoffsets.push_back(-(VLogRingRefFileOffset)bytes.size());  // flag the offending file
  }

  return;

// acquire spin lock
//   allocate data to files, create filelengths return value
//   if ring must be reallocated (check shadow head against shadow tail pointer, read atomically)
//     exchange ring usecount with large negative value
//     pause until ring usecount is (large neg)-(original usecount)
//     reallocate, set new values for base & length
//     copy old data, delete old ring
//     atomic store of 0 to ring usecount
//   atomic write of new shadow head/offset
// release spin lock
// open new files, write data, put fd pointer into ring
// atomic write to head/offset
// atomic incr ring usecount by 0 (to force update of new fds)
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
  // wait a little while and retry, before giving up
  RandomAccessFile *selectedfile;
  while(1) {
    if((selectedfile = fd_ring[Ringx(request.Fileno())].filepointer.get()) != nullptr)break;  // read the file number.  This will usually succeed
    // Here the file pointer is not valid.  It is possible that we have an unupdated local copy.  To update it, do an acquire on the
    // shadow pointer, which is updated after the file pointer is modified
    atomics.fd_ring_head_fileno_shadow.load(std::memory_order_acquire);
  }

  Status iostatus = selectedfile->Read(request.Offset(), request.Len(), &resultslice, (char *)response->data());  // read the data
  // If the result is not in the user's buffer, copy it to there
  if(iostatus.ok()){
    if(response->data()!=resultslice.data())response->assign(resultslice.data(),resultslice.size());  // no error: copy data is not already in user's buffer
  }else{resultslice.clear();}  // error: return empty string
  return iostatus;
}

// Install a new SST into the ring, with the given earliest-VLog reference.  Increment refcount
void VLogRing::VLogRingSstInstall(
  FileMetaData& newsst   // the SST that has just been created & filled in
){
  AcquireLock();  // lock at the Ring level to allow multiple compactions on the same CF
    // Find the slot that this reference maps to
    size_t slotx = Ringx(newsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid
// scaf out of bounds?

    // Increment the usecount for the slot
    ++fd_ring[slotx].refcount;  // starting from now until this SST is deleted, this VLog file is in use

    // Install the SST into the head of the chain for the slot
    newsst.ringfwdchain[ringno_] = fd_ring[slotx].queue;   // attach old chain after new head
    newsst.ringbwdchain[ringno_] = nullptr;   // there is nothing before the new head
    if(fd_ring[slotx].queue!=nullptr)fd_ring[slotx].queue->ringbwdchain[ringno_] = &newsst;  // if there is an old chain, point old head back to new head
    fd_ring[slotx].queue = &newsst;  // point anchor to the new head
    
  ReleaseLock();
#if DEBLEVEL&1
printf("VLogRingSstInstall newsst=%p ref0=%lld",&newsst,newsst.indirect_ref_0[ringno_]); // scaf debug
printf("; queue=");for(int i = 1;i<6;++i){int ct=0; FileMetaData* qp=fd_ring[i].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("; refcount=");for(int i = 1;i<6;++i)printf("%d ",fd_ring[i].refcount);
printf("\n");
#endif
}

// Remove an SST from the ring when it is no longer current.  Do not adjust refcount.  Advance shadow tail pointer past files with no queue
void VLogRing::VLogRingSstUnCurrent(
  FileMetaData& retiringsst   // the SST that has just been obsoleted
){
  AcquireLock();  // lock at the Ring level to allow multiple compactions on the same CF

    // Find the slot that this reference maps to
    size_t slotx = Ringx(retiringsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid

    // Take the SST out of the ring.  We assume it's there.
    assert(retiringsst.ringbwdchain[ringno_] != &retiringsst);
    if(retiringsst.ringfwdchain[ringno_]!=nullptr)retiringsst.ringfwdchain[ringno_]->ringbwdchain[ringno_] = retiringsst.ringbwdchain[ringno_];  // if there is a next, point it back to the elements before this
    if(retiringsst.ringbwdchain[ringno_]!=nullptr)retiringsst.ringbwdchain[ringno_]->ringfwdchain[ringno_] = retiringsst.ringfwdchain[ringno_];  // if there is a prev, point it to the elements after this
    else {
      // We are deleting the head
      if((fd_ring[slotx].queue = retiringsst.ringfwdchain[ringno_])==nullptr) {
        // We have emptied the ring.  If it is the shadow tail, update the shadow tail
        VLogRingRefFileno shadowtailfile = atomics.fd_ring_tail_fileno_shadow.load(std::memory_order_acquire);  // get up-to-date copy of tail file
        if(retiringsst.indirect_ref_0[ringno_]==shadowtailfile) {
          // the retiring sst was the beginning of the shadow tail.  See how many files in the shadow tail have empty queues.  Stop looking
          // when we get past the shadow head
          VLogRingRefFileno shadowheadfile = atomics.fd_ring_head_fileno_shadow.load(std::memory_order_acquire);  // get up-to-date copy of shadow head file
          while(1) {  // loop till shadowtailfile is right
            if(++slotx==fd_ring.size())slotx=0;  // Step to next slot; don't use Ringx() lest it perform a slow divide
            ++shadowtailfile;  // Advance file number to match slot pointer
            if(shadowtailfile>shadowheadfile)break;   // The highest tailfile can be is 1 more than shadow head, when ring is empty
            if(fd_ring[slotx].queue!=nullptr)break;  // if the current slot has SSTs, it will be the new shadow tail
          }
          // Now tailfile is the lowest file# that has a nonempty queue, unless there are no files, in which case it is one past the shadow head
          // Save this value in the ring
          atomics.fd_ring_tail_fileno_shadow.store(shadowtailfile,std::memory_order_release); 
        }
      }
    }
    // Mark the chain fields to show that this block is no longer on the ring.  We use chain-to-self to indicate this
    retiringsst.ringbwdchain[ringno_] = &retiringsst;  // indicate 'block not in ring'

  ReleaseLock();
#if DEBLEVEL&1
printf("VLogRingSstUnCurrent retiringsst=%p ref0=%lld",&retiringsst,retiringsst.indirect_ref_0[ringno_]); // scaf debug
printf("; queue=");for(int i = 1;i<6;++i){int ct=0; FileMetaData* qp=fd_ring[i].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("; refcount=");for(int i = 1;i<6;++i)printf("%d ",fd_ring[i].refcount);
printf("\n");
#endif
}

// Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused.  Advance tail pointer past deleted files
void VLogRing::VLogRingSstDelete(
  FileMetaData& expiringsst   // the SST that is about to be destroyed
){
  std::vector<VLogRingFile> deleted_files;  // files put here will be deleted at the end of this routine
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

  AcquireLock();  // lock at the Ring level

    // Find the slot that this reference maps to
    size_t slotx = Ringx(expiringsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid

    // Decrement the usecount
    if(!--fd_ring[slotx].refcount) {
      // The usecount went to 0.  We can free the file, if it is the oldest in the tail
      VLogRingRefFileno tailfile = atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);  // get up-to-date copy of tail file
      if(expiringsst.indirect_ref_0[ringno_]==tailfile) {
        // the expiring sst was earliest in the tail.  See how many files in the tail have 0 usecount.  Stop looking
        // when we get to the shadow tail.
        VLogRingRefFileno shadowtailfile = atomics.fd_ring_tail_fileno_shadow.load(std::memory_order_acquire);  // get up-to-date copy of shadow head file
        while(1) {  // loop till tailfile is right
          deleted_files.push_back(std::move(fd_ring[slotx]));  // mark file for deletion
          if(++slotx==fd_ring.size())slotx=0;  // Step to next slot; don't use Ringx() lest it perform a slow divide
          ++tailfile;  // Advance file number to match slot pointer
          if(tailfile>=shadowtailfile)break;   // Tail must stop at shadow tail
          if(fd_ring[slotx].refcount)break;  // if the current slot has references, it will become the shadow tail
        }
        // Now tailfile is the lowest file# that has a nonempty queue, unless there are no files, in which case it is one past the shadow head
        // Save this value in the ring
        atomics.fd_ring_tail_fileno.store(tailfile,std::memory_order_release); 
      }
    }

  ReleaseLock();
#if DEBLEVEL&1
printf("VLogRingSstDelete expiringsst=%p ref0=%lld",&expiringsst,expiringsst.indirect_ref_0[ringno_]); // scaf debug
printf("; queue=");for(int i = 1;i<6;++i){int ct=0; FileMetaData* qp=fd_ring[i].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("; refcount=");for(int i = 1;i<6;++i)printf("%d ",fd_ring[i].refcount);
printf("\n");
#endif
  // We had to release the lock before we actually deleted the files.  The files to delete are in deleted_files.  Delete them now
  for(size_t i = 0;i<deleted_files.size();++i) {
#if DEBLEVEL&1
  printf("Deleting: %s\n",deleted_files[i].filename.data());
#endif
    deleted_files[i].DeleteFile(immdbopts_,envopts_);
  }

}

// Return a vector of up to n SSTs that have the smallest oldest-reference-filenumbers.  If extend is true, return all SSTs
// whose filenumber does not exceed that of the nth-smallest SST's (in other words, return every SST that is tied with n).
void VLogRing::VLogRingFindLaggingSsts(
  int n,  // number of lagging ssts to return
  std::vector<FileMetaData*>& laggingssts,  // result: vector of SSTs that should be recycled
  int extend   // if 1, report all (default 0)
) {}
// Do this operation under spin lock.  Reheap to close up deleted SSTs whenever we encounter them




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
printf("VLog cfd=%p name=%s\n",cfd,cfd->GetName().data()); // scaf debug
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
printf("VLogInit cfd_=%p name=%s\n",cfd_,cfd_->GetName().data()); // scaf debug
#endif
  // Save the ring starting levels
    starting_level_for_ring_.push_back(1);  // scaf

  // If there are VlogRings now, it means that somehow the database was reopened without being destroyed.  This is bad form but not necessarily fatal.
  // To cope, we will delete the old rings, which should go through and free up all their resources.  Then we will continue with new rings.
  rings_.clear();  // delete any rings that are hanging around

#if 0  // scaf will be deleted
  // Go through all the SSTs and create a vector of filerefs for each ring
  std::vector<VLogRingRefFileno> early_refs (starting_level_for_ring_.size(),high_value) ;  // place to hold earliest refs; init all missing
  std::vector<VLogRingRefFileno> earliest_ref;  // for each ring, the earliest file referred to in the ring
  for(int level = cfd_->NumberLevels()-1;level>0;--level) {  // for each on-disk level in the CF
    VersionStorageInfo* sinfo = cfd_->current()->storage_info();  // factor out storage_info.  Since we are in initialization,
      // it is safe to look at the current version, which will have the initial list of files
    // accumulate the earliest nonzero reference across all SSTs in this CF
    // We have to include all rings for all levels, in case the user changes the
    // starting level for a ring after the initial opening of the ring.
    for(FileMetaData* fmd : sinfo->LevelFiles(level)){
      // In case this SST has never been initalized, resize it to have the correct number of ('invalid') early refs
      fmd->indirect_ref_0.resize(starting_level_for_ring_.size(),0);

      // Loop through all the early-ring-refs in this SST
      for(int i = 0; i < starting_level_for_ring_.size(); ++i) {
        if(fmd->indirect_ref_0[i])if((VLogRingRefFileno)fmd->indirect_ref_0[i]<early_refs[i])early_refs[i]=(VLogRingRefFileno)fmd->indirect_ref_0[i];
      }
    }
  }
#endif

  // cull the list of files to leave only those that apply to this cf
  std::vector<std::string> existing_vlog_files_for_cf;  // .vlg files for this cf
  std::string columnsuffix = "." + kRocksDbVLogFileExt + cfd_->GetName();  // suffix for this CF's vlog files
  for(auto fname : vlg_filenames){
    if(fname.size()>columnsuffix.size() && 0==fname.substr(fname.size()-columnsuffix.size()).compare(columnsuffix))
      existing_vlog_files_for_cf.emplace_back(fname);  // if suffix matches, keep the filename
  }

#if 0 // scaf will be removed
  // Now that we know how many rings there are, resize the ring_end vector in the column family to match.
  // Extend missing elements with 0, indicating 'no valid value'
  cfd_->ResizeRingEnds(early_refs.size());
#endif

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
  Slice& reference,  // the reference
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
#if DEBLEVEL&1
printf("VLogSstInstall newsst=%p\n",&newsst); // scaf debug
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
      for (int i=0;i<newsst.indirect_ref_0.size();++i)if(newsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstInstall(newsst);
    }
  }

  // Remove an SST from the ring when it is no longer current.
  void VLog::VLogSstUnCurrent(
    FileMetaData& retiringsst   // the SST that has just been obsoleted
  ) {
#if DEBLEVEL&1
printf("VLogSstUnCurrent retiringsst=%p rings_.size()=%zd\n",&retiringsst,rings_.size()); // scaf debug
#endif
    if(!rings_.size())return;  // no action if the rings have not, or never will be, created
    for (int i=0;i<retiringsst.indirect_ref_0.size();++i)if(retiringsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstUnCurrent(retiringsst);
    }

  // Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused
  void VLog::VLogSstDelete(
    FileMetaData& expiringsst   // the SST that is about to be destroyed
  ) {
#if DEBLEVEL&1
printf("VLogSstDelete expiringsst=%p rings_.size()=%zd\n",&expiringsst,rings_.size()); // scaf debug
#endif
    if(!rings_.size())return;  // no action if the rings have not, or never will be, created
    for (int i=0;i<expiringsst.indirect_ref_0.size();++i)if(expiringsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstDelete(expiringsst);
    }


}   // namespace rocksdb


	
