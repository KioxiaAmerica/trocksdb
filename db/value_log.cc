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
#include "db/value_log.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "options/cf_options.h"
#include "rocksdb/status.h"

namespace rocksdb {



// We have two occasions to search through the SSTs to find the ones with the oldest VLogRing entries: (1) scheduling
// compactions, where we give priority to the SSTs with the oldest VLogRing; (2) SST destruction, which may remove the
// last reference to a VLogRing file, allowing its deletion.  Searching all SSTs is potentially slow, and SST destruction
// may be asynchronous with compaction (since it can happen when a Range Query completes); so we are moved to implement
// a priority-queue mechanism for efficiently finding the earliest SSTs/VLogRingRefs.  We have a priority queue and a hash table.
// When an SST is created, its entry goes into the hash table and the priority queue.  When the SST is deleted, its entry is removed
// from the hash table.  When the oldest entries are looked up, we ignore any that are marked as deleted in the hashtable
// Constructor.  We need an estimate of the number of SSTs so we can size the hashtable
VLogRingQueue::VLogRingQueue(
  int sstcount  // estimated number of SSTs.  We size the hash table for fast lookup at this size
){}

// Constructor with an initial set of SST information.  Faster to generate the priority queue in bulk
VLogRingQueue::VLogRingQueue(
  int sstcount,  // estimated number of SSTs.  We size the hash table for fast lookup at this size
  std::vector<uint64_t> sstids,  // vector of unique IDs for SSTs
  std::vector<VLogRingRefFileno> earlyfileno   // corresponding earliest reference to VLogRing
){}

// Add an SST to the table, with the given minimum filenumber
// Arguments can be replaced by the metadata for the SST, or the Manifest entry?
void VLogRingQueue::VLogRingQueueAdd(
  uint64_t sstid,  // SST that has been recalculated
  VLogRingRefFileno fileno  // Earliest VLogRing file referred to therein
)
{
// acquire spin lock
// expand table if needed
// add SST to hash
// add SST/fileno to the priority queue
// release spin lock
}

// Delete an SST from the table.  It is possible that some of the SSTs are not in the table.
void VLogRingQueue::VLogRingQueueDeleteSST(
  uint64_t sstid  // SST that has been deleted
)
{
// acquire spin lock
// delete SST from the hashtable
// release spin lock
}

// Return the earliest reference to a VLogRing value
// It is the value of the top of the heap, unless that is stale.  We keep discarding stale elements until we find a good one.
VLogRingRefFileno VLogRingQueueFindOldestFileno(){return 0;}  // scaf

// Return a vector of up to n SSTs that have the smallest filenumbers.  If extend is true, return all SSTs
// whose filenumber does not exceed that of the nth-smallest SST's (in other words, return every SST that is tied with n).
// For each value we encounter, we look up the SST in the hashtable; if the SST is no longer alive, we delete the entry from
// the priority queue
std::vector<uint64_t> VLogRingQueueFindLaggingSSTs(
  int n,  // number of smallest filenumbers to return
  int extend=0   // if 1, report all 
)
{
// Do this operation under spin lock.  Reheap to close up deleted SSTs whenever we encounter them
return std::vector<uint64_t>{};  // scaf
}

static const VLogRingRefFileno high_value = ((VLogRingRefFileno)-1)>>1;  // biggest positive value
static const float expansion_fraction = 0.25;  // fraction of valid files to leave for expansion
static const int expansion_minimum = 1000;  // minimum number of expansion files

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
  VLogRingRefFileno latest_ref,   // last file number referred to in manifest
  const ImmutableDBOptions *immdbopts,   // The current Env
  EnvOptions& file_options  // options to use for all VLog files
)   : 
    ringno_(ringno),
    cfd_(cfd),
    immdbopts_(immdbopts),
    envopts_(file_options)  // must copy into heap storage here
{
  // If there are no references to the ring, set the limits to empty
  if(earliest_ref==high_value){earliest_ref = 1; latest_ref = 0;}
  // Note that file 0 is not allocated.  It is reserved to mean 'file number not given'

  // Allocate the rings for files and references.  Allocate as many files as we need to hold the
  // valid files, plus some room for expansion (here, the larger of a fraction of the number of valid files
  // and a constant)
  VLogRingRefFileno ringexpansion = (int)((latest_ref-earliest_ref)*expansion_fraction);
  if(ringexpansion<expansion_minimum)ringexpansion = expansion_minimum;
ringexpansion = 16384;  // scaf
  // force the ring to a power-of-2 multiple so that Ringx() can be efficient
  ringexpansion = latest_ref-earliest_ref+ringexpansion;
  VLogRingRefFileno power2; for(power2 = 1;power2<ringexpansion;power2<<=1);
  fd_ring.resize(power2);  // establish initial size of ring

  // For each file in this ring, open it (if its number is valid) or delete it (if not)
  for(auto pathfname : filenames) {
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
    ParsedFnameRing fnring(number);

    if(fnring.ringno==ringno) {// If this file is for our ring...
if(latest_ref<fnring.fileno)latest_ref = fnring.fileno;  // scaf
      if(earliest_ref<=fnring.fileno && fnring.fileno<=latest_ref) {
        // the file is referenced by the SSTs.  Open it
        std::unique_ptr<RandomAccessFile> fileptr;  // pointer to the opened file
        immdbopts_->env->NewRandomAccessFile(pathfname, &fileptr,envopts_);  // open the file if it exists
        // move the file reference into the ring, and publish it to all threads
        fd_ring[Ringx(fnring.fileno)]=std::move(fileptr);
// scaf error?
      } else {
        // The file is not referenced.  We must have crashed during deletion.  Delete it now
        immdbopts_->env->DeleteFile(pathfname);   // ignore error - what could we do?
  // should log? scaf
      }
    }
  }

  // Set up the pointers for the ring, indicating validity
  // Set up so ring entry 0 corresponds to the first file
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
// Delete files that are no longer referred to by the SSTs.  Returns error status
Status VLogRing::VLogRingCompact(
)
{
// get smallest file# in SSTs
// if that's greater than the tail pointer
//   compare-and-swap tail pointer; exit if incumbent value >= our new, otherwise retry
//   erase files (if any), set fd to 0
//   atomic incr ring usecount by 0 (to force update of new fds)
//   compare-and-swap shadow tail pointer; exit if incumbent >= new, else retry
return Status();  // scaf
}


// Write accumulated bytes to the VLogRing.  First allocate the bytes to files, being
// careful not to split a record, then write them all out.  Create new files as needed, and leave them
// open.  The last file will be open for write; the others are read-only
// The result is a VLogRingRef for the first (of possibly several sequential) file, and a vector indicating the
// number of bytes written to each file
// We housekeep the end-of-VLogRing information
// We use release-acquire ordering for the VLogRing file and offset to avoid needing a Mutex in the reader
// If the circular buffer gets full we have to relocate it, so we use release-acquire
Status VLogRing::VLogRingWrite(
std::string& bytes,   // The bytes to be written, jammed together
std::vector<size_t>& rcdend,  // The running length of all records up to and including this one
VLogRingRef& firstdataref,   // result: reference to the first values written
std::vector<VLogRingRefFileLen>& fileendoffsets   // result: length of amount written to each file.  The file written are sequential
          // following the one in firstdataref.  The offset in the first file is in firstdataref; it is 0 for the others
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
  if(!bytes.size())return Status();   // fast exit if no data

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
    immdbopts_->env->NewWritableFile(fname,&writable_file,envopts_);  // open the file
// scaf errors
    writable_file->Append(Slice(bytes));  // write out the data

  // Sync the written data.  We must make sure it is synced before the SSTs referring to it are committed to the manifest.
  // We might as well sync it right now
    writable_file->Fsync();
  }  // this closes the file

  // Reopen the file as randomly readable; install the new file into the ring
  immdbopts_->env->NewRandomAccessFile(fname, &fd_ring[new_ring_slot], envopts_);  // open the file - it must exist
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
  fileendoffsets.clear(); fileendoffsets.push_back(bytes.size());

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
return Status();  // scaf
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
  // copy of the file pointer.  scaf avoid race on index_offset_load using %; other ring ptrs

  // We have to get the latest copy of the validity information, and also the latest copy of the memory pointer.
  // We have no way of knowing when these become valid, as they are not protected by any lock.  But they should have
  // been updated long before we need them.  If the current values indicate invalid references or nonexistent pointers
  // wait a little while and retry, before giving up
  RandomAccessFile *selectedfile;
  while(1) {
    if((selectedfile = fd_ring[Ringx(request.Fileno())].get()) != nullptr)break;  // read the file number.  This will usually succeed
    // Here the file pointer is not valid.  It is possible that we have an unupdated local copy.  To update it, do an acquire on the
    // shadow pointer, which is updated after the file pointer is modified
    atomics.fd_ring_head_fileno_shadow.load(std::memory_order_acquire);
  }

  Status iostatus = selectedfile->Read(request.Offset(), request.Len(), &resultslice, (char *)response->data());  // read the data
  // If the result is not in the user's buffer, copy it to there
  if(response->data()!=resultslice.data())response->assign(resultslice.data(),resultslice.size());
  return iostatus;  // scaf

// atomic incr of ring usecount -  if negative, atomic decr & wait for nonnegative
// read ring base & length (protected by usecount)
// (debug only) atomic read ring offset/head & tail, verify in range
//   NOTE that there may be trouble resolving ring position if index is changed - perhaps use mod(length)
// read fd, verify non0, read data (protected by usecount)
// atomic decr of ring usecount
}

// delete one SST from the queue, and delete any files that frees up
void VLogRing::VLogRingDeleteSST(
    uint64_t sstid  // SST that has been deleted
) {}


// A VLog is a set of VLogRings for a single column family.  The VLogRings are distinguished by an index.
VLog::VLog(
  // the info for the column family
  ColumnFamilyData *cfd
) :
  rings_(std::vector<std::unique_ptr<VLogRing>>()),  // start with empty rings
  starting_level_for_ring_(std::vector<int>()),
  cfd_(cfd)
{}


// Initialize each ring to make it ready for reading and writing.
// It would be better to do this when the ring is created, but with the existing interfaces there is
// no way to get the required information to the constructor
Status VLog::VLogInit(
    std::vector<std::string> vlg_filenames,    // all the filenames that exist for this database - at least, all the vlg files
    const ImmutableDBOptions *immdbopts,   // The current Env
    EnvOptions& file_options  // options to use for all VLog files
) {
  // Save the ring starting levels
    starting_level_for_ring_.push_back(1);  // scaf

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

  // cull the list of files to leave only those that apply to this cf
  std::vector<std::string> existing_vlog_files_for_cf;  // .vlg files for this cf
  std::string columnsuffix = "." + kRocksDbVLogFileExt + cfd_->GetName();  // suffix for this CF's vlog files
  for(auto fname : vlg_filenames){
    if(fname.size()>columnsuffix.size() && 0==fname.substr(fname.size()-columnsuffix.size()).compare(columnsuffix))
      existing_vlog_files_for_cf.emplace_back(fname);  // if suffix matches, keep the filename
  }

  // Now that we know how many rings there are, resize the ring_end vector in the column family to match.
  // Extend missing elements with 0, indicating 'no valid value'
  cfd_->ResizeRingEnds(early_refs.size());

  // For each ring, allocte & initialize the ring, and save the resulting object address
  for(int i = 0; i<early_refs.size(); ++i) {
    // Create the ring and save it
    rings_.push_back(std::move(std::make_unique<VLogRing>(i /* ring# */, cfd_ /* ColumnFamilyData */, existing_vlog_files_for_cf /* filenames */,
      (VLogRingRefFileno)early_refs[i] /* earliest_ref */, (VLogRingRefFileno)cfd_->GetRingEnds()[i]/* latest_ref */,
      immdbopts /* immdbopts */, file_options)));
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
  VLogRingRef ref = VLogRingRef(reference.data());   // analyze the reference

  // Because of the OS kludge that doesn't allow zero-length files to be memory-mapped, we have to check to make
  // sure that the reference doesn't have 0 length: because the 0-length reference might be contained in a
  // (nonexistent) 0-length file, and we'd better not try to read it
  if(!ref.Len()){ result->clear(); return Status(); }   // length 0; return empty string, no error

  // Vector to the appropriate ring to do the read
  return rings_[ref.Ringno()]->VLogRingGet(ref,result);
}

}   // namespace rocksdb


	
