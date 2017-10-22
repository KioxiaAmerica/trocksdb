// changes needed in main Rocks code:
// FileMetaData needs a unique ID number for each SST in the system.  Could be unique per column family if that's easier
// Manifest needs smallest file# referenced by the SST

//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <list>
#include <string>
#include <vector>
#include "options/cf_options.h"

namespace rocksdb {

typedef uint32_t VLogRingRefFileno;
typedef uint64_t VLogRingRefFileOffset;
typedef uint64_t VLogRingRefFileLen;


// The VLogRingRef is a fixed-length reference to data in a VLogRing.  The length is fixed so that references in an SST
// can be easily remapped in place.  The reference indicates the file, offset, and length of the reference
class VLogRingRef {
private:
VLogRingRefFileno fileno;
VLogRingRefFileOffset offset;
VLogRingRefFileLen len;
public:
// Constructor
VLogRingRef(VLogRingRefFileno f, VLogRingRefFileOffset o, VLogRingRefFileLen l) : fileno(f), offset(o), len(l) {}

// Add fileoffset/byteoffset to the given ref
void VLogRingAdd(VLogRingRefFileno fileoffset, VLogRingRefFileOffset byteoffset) {fileno+=fileoffset; offset=(fileoffset?0:offset)+byteoffset;}

// we will need functions to move a ref to and from the SST
};


// We have two occasions to search through the SSTs to find the ones with the oldest VLogRing entries: (1) scheduling
// compactions, where we give priority to the SSTs with the oldest VLogRing; (2) SST destruction, which may remove the
// last reference to a VLogRing file, allowing its deletion.  Searching all SSTs is potentially slow, and SST destruction
// may be asynchronous with compaction (since it can happen when a Range Query completes); so we are moved to implement
// a priority-queue mechanism for efficiently finding the earliest SSTs/VLogRingRefs.  We have a priority queue and a hash table.
// When an SST is created, its entry goes into the hash table and the priority queue.  When the SST is deleted, its entry is removed
// from the hash table.  When the oldest entries are looked up, we ignore any that are marked as deleted in the hashtable
class VLogRingQueue {
private: 
public:
// Constructor.  We need an estimate of the number of SSTs so we can size the hashtable
VLogRingQueue(
  int sstcount  // estimated number of SSTs.  We size the hash table for fast lookup at this size
);

// Constructor with an initial set of SST information.  Faster to generate the priority queue in bulk
VLogRingQueue(
  int sstcount,  // estimated number of SSTs.  We size the hash table for fast lookup at this size
  std::vector<uint64_t> sstids,  // vector of unique IDs for SSTs
  std::vector<VLogRingRefFileno> earlyfileno   // corresponding earliest reference to VLogRing
);


// Add an SST to the table, with the given minimum filenumber
// Arguments can be replaced by the metadata for the SST, or the Manifest entry?
void VLogRingQueueAdd(
  uint64_t sstid,  // SST that has been recalculated
  VLogRingRefFileno fileno  // Earliest VLogRing file referred to therein
)
// acquire spin lock
// expand table if needed
// add SST to hash
// add SST/fileno to the priority queue
// release spin lock
;

// Delete an SST from the table.  It is possible that some of the SSTs are not in the table.
void VLogRingQueueDeleteSST(
  uint64_t sstid  // SST that has been deleted
)
// acquire spin lock
// delete SST from the hashtable
// release spin lock
;

// Return the earliest reference to a VLogRing value
// It is the value of the top of the heap, unless that is stale.  We keep discarding stale elements until we find a good one.
VLogRingRefFileno VLogRingQueueFindOldestFileno();

// Return a vector of up to n SSTs that have the smallest filenumbers.  If extend is true, return all SSTs
// whose filenumber does not exceed that of the nth-smallest SST's (in other words, return every SST that is tied with n).
// For each value we encounter, we look up the SST in the hashtable; if the SST is no longer alive, we delete the entry from
// the priority queue
std::vector<uint64_t> VLogRingQueueFindLaggingSSTs(
  int n,  // number of smallest filenumbers to return
  int extend=0   // if 1, report all 
)
// Do this operation under spin lock.  Reheap to close up deleted SSTs whenever we encounter them
;


};


// A VLogRing is a set of sequentially-numbered files, with a common prefix and extension .vlg, that contain
// values for a column family.  Each value is pointed to by an SST, which uses a VLogRingRef for the purpose.
// During compaction, bursts of values are written to the VLogRing, lumped into files of approximately equal size.
//
// Each SST remembers the oldest VLogRingRef that it contains.  Whenever an SST is destroyed, we check to
// see if there are VLogRing files that are no longer referred to by any SST, and delete any such that are found.
//
// Each column family has (possibly many) VLogRing.  VLogRingRef entries in an SST implicitly refer to the VLog of the column family.
class VLogRing {
friend class VLog;
private:
  // The ring:
  std::vector<int> fd_ring;  // the ring of open file descriptors for the VLog files.  -1 means file is not open

  // We group the atomics together so they can be aligned to a cacheline boundary
  struct alignas(64) {
  // The ring head/tail pointers:
  // These must be WRITTEN in the order fileno,fileoffset and READ in the reverse order, using acquire/release memory model.  This will ensure that
  // both variables are valid everywhere
  VLogRingRefFileno fd_ring_head_fileno;   // file number of the last file in the ring.  We are appending to this file
  VLogRingRefFileno fd_ring_head_fileno_shadow;   // we move head_fileno to reserve space, before the files have been created.  Move the shadow after they are valid
  std::atomic<VLogRingRefFileOffset> fd_ring_head_fileoffset;  // the offset that the next write will go to
  std::atomic<VLogRingRefFileOffset> fd_ring_head_fileoffset_shadow;  // shadow, as with fileno
  std::atomic<VLogRingRefFileno> fd_ring_tail_fileno;   // smallest valid file# in ring
  std::atomic<VLogRingRefFileno> fd_ring_tail_fileno_shadow;   // We move tail_fileno to remove files from validity, but before we have erased them.
     // after we erase them we move the shadow pointer.  So tests for ring-full must use the shadow pointer

  // The usecount of the current ring.  Set to 0 initially, incr/decr during Get.  Set to a negative value, which
  // quiesces Get, when we need to resize the ring.  We use this as a sync point for the ringpointer/len so we don't have to use atomic reads on them.
  std::atomic<int> usecount;
  } atomics;


  // Non-ring variables:
  VLogRingQueue& queue;  // how we find out what's coming up

public:
// Constructor.  Find all the VLogRing files and open them.  Create the ring buffer for the files.
// Perform validity checks, which are nothing for the moment.
// Remember the file number/size of the last file that has been marked as synced in the Manifest, and delete any files that
// come after that one (they must have been written just before a crash).  Also allocate the queue/hashtable for the SSTs
VLogRing(
  VLogRingRefFileno lastsyncedfile,  // file number of the last file acked in the Manifest
  VLogRingRefFileOffset lastsyncedoffset,  // Number of bytes successfully written
  int estimatednumssts    // approximate number of SSTs expected
);

// Delete files that are no longer referred to by the SSTs.  Returns error status
Status VLogRingCompact(
)
// get smallest file# in SSTs
// if that's greater than the tail pointer
//   compare-and-swap tail pointer; exit if incumbent value >= our new, otherwise retry
//   erase files (if any), set fd to 0
//   atomic incr ring usecount by 0 (to force update of new fds)
//   compare-and-swap shadow tail pointer; exit if incumbent >= new, else retry
;


// Write accumulated bytes to the VLogRing.  First allocate the bytes to files, being
// careful not to split a record, then write them all out.  Create new files as needed, and leave them
// open.  The last file will be open for write; the others are read-only
// The result is a VLogRingRef for the first (of possibly several sequential) file, and a vector indicating the
// number of bytes written to each file
// We housekeep the end-of-VLogRing information
// We use release-acquire ordering for the VLogRing file and offset to avoid needing a Mutex in the reader
// If the circular buffer gets full we have to relocate it, so we use release-acquire
Status VLogRingWrite(
std::vector<char>& bytes,   // The bytes to be written, jammed together
std::vector<int>& rcdend,  // The running length of all records up to and including this one
VLogRingRef* firstdataref,   // result: reference to the first values written
std::vector<int>* filelengths   // result: length of amount written to each file.  The file written are sequential
          // following the one in firstdataref.  The offset in the first file is in firstdataref; it is 0 for the others
)
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
;

// Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
// Returns the bytes.  ?? Should this return to user area to avoid copying?
Status VLogRingGet(
  VLogRingRef& request,  // the values to read
  std::string& response   // the data pointed to by the reference
)
// atomic incr of ring usecount -  if negative, atomic decr & wait for nonnegative
// read ring base & length (protected by usecount)
// (debug only) atomic read ring offset/head & tail, verify in range
// read fd, verify non0, read data (protected by usecount)
// atomic decr of ring usecount
;

// delete one SST from the queue, and delete any files that frees up
void VLogRingDeleteSST(
    uint64_t sstid  // SST that has been deleted
);

};


// A VLog is a set of VLogRings for a single column family.  The VLogRings are distinguished by an index.
class VLog {
private:
  std::vector<VLogRing*> rings_;  // the VLogRing objects for this CF
  std::vector<int> starting_level_for_ring_;
public:
  VLog(
    // for each ring, the database level at which to start using it.  The ring remains in use until
    // the value passes into a level with a higher ring number.  This list may be shorter than the number of rings defined
    std::vector<int> starting_level_for_ring,
    // for each ring, the number of SSTs expected in the ring.  A ring is defined for each value here
    std::vector<int> ssts_per_ring
  );

  // Return a vector of the end-file-number for each ring.  This is the last file number that has been successfully synced.
  // NOTE that there is no guarantee that data is written to files in sequential order, and thus on a restart the
  // end-file-number may cause some space to be lost.  It will be recovered when the ring recycles.
   void GetRingEnds(std::vector<uint64_t> *result) {
    for(auto ring : rings_){result->push_back(ring->atomics.fd_ring_head_fileno_shadow);}
    return;
  }

// Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
// Returns the bytes.  ?? Should this return to user area to avoid copying?
Status VLogGet(
  Slice& reference,  // the reference
  std::string *result   // where the result is built
)
  // extract the ring# from the reference

  // Call VLogRingGet in the selected ring
;

};

} // namespace rocksdb
	
