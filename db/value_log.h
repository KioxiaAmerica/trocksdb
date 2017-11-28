//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <list>
#include <forward_list>
#include <string>
#include <vector>
#include <atomic>
#include <memory>
#include "options/db_options.h"
#include "rocksdb/env.h"
#include "rocksdb/transaction_log.h"

namespace rocksdb {

#define DEBLEVEL 0x7f  // 1=SST ring ops 2=file ops 4=iterator ops 8=ring pointers 16=deleted_files 32=versions 64=top-level ring ops

struct FileMetaData;
class ColumnFamilyData;
class Status;
class Slice;

// class that can be used to create a vector of chars that's doesn't initialize when you resize
class NoInitChar {
public:
  char data;
  NoInitChar(){};
};

// The Value Log is the place where values are stored separate from their keys.  Values are written to the Value Log
// during compaction.

// There is one VLog per column family, created when the column family is initialized (provided that the
// column family has a table type that supports indirect values).  After options have been vetted and all columns created, the
// Value Log is initialized for operation.

// Each VLog has one or more VLogRings, under programmer control.  Each VLogRing is associated with a set of levels.  Each compaction
// writes to the ring associated with its output level.  The association of output levels to VLogRings can be changed, but it may
// take some time for the values to be migrated to the correct output ring.

// The VLogRing contains the Ring: a list of all the files containing values; and the Queue: for each VLog file, a linked-list of SSTs whose
// earliest reference is to the file.  We use chain fields inside the SST's metadata to hold the chain fields for the Queue.

// Files containing values are sequences of bytes, jammed together with no record marks.  Each value can be compressed and CRCd
// independently.  The filename for a value file is path###.vlg$$$ where
// path is the last db_path for the column family
// ### is an ASCII string of the number of the file
// $$$ is the name of the column family, which must be suitable for inclusion in a file name
//
// With the file number, the ring number occupies the low 2 bits and the upper bits are a sequential number.

// During compaction, each value above a user-specified size is written to a file and replaced by a VLogRingRef, a fixed-length string
// that identifies the ring number, file, offset, and length of the stored value.  Calls to Get() that encounter a VLogRingRef (which
// is identified by a special Value Type) call the VLog to read from disk to replace the indirect reference withthe value data.


static const std::string kRocksDbVLogFileExt = "vlg";   // extension for vlog files is vlgxxx when xxx is CF name

typedef uint64_t VLogRingRefFileno;
typedef int64_t VLogRingRefFileOffset;   // signed because negative values are used to indicate error
typedef uint64_t VLogRingRefFileLen;

// Convert file number to file number and ring
class ParsedFnameRing {
public:
  int ringno;
  uint64_t fileno;
  ParsedFnameRing(VLogRingRefFileno file_ring) :
    fileno(file_ring>>2), ringno((int)file_ring&3) {}
};

// The VLogRingRef is a reference to data in a VLogRing.  It can be converted to an opaque string
// form, whose length is fixed so that references in an SST
// can be easily remapped in place.  The reference indicates the ring, file, offset, and length of the reference
//
// Opaque string holds the 16 bytes from 2 int64s, which are FFFooooo ffrlllll  (file/ring is FFFffr)
class VLogRingRef {
public:
// Constructor
VLogRingRef(int r, VLogRingRefFileno f, VLogRingRefFileOffset o, VLogRingRefFileLen l) : ringno(r), fileno(f), offset(o), len(l) {}  // with data
VLogRingRef() { }   // placeholder only
VLogRingRef(int r) : ringno(r), fileno(0), offset(0), len(0) {}  // ring only
VLogRingRef(int r, VLogRingRefFileno f) : ringno(r), fileno(f), offset(0), len(0) {}  // ring & filenumber only
VLogRingRef(const char *opaqueref) {  // creating ref from OpaqueRef array
  memcpy(workarea.extform,opaqueref,sizeof(workarea.extform));  // move ref to aligned storage
  // extract fields from 64-bit names to be independent of byte order, as long as the machine architecture
  // doesn't change over the life of the database
  offset = workarea.intform[0]&((1LL<<40)-1);
  len = workarea.intform[1]&((1LL<<40)-1);
  ringno = (workarea.intform[1]>>40)&3;
  fileno = ((workarea.intform[0]&(-(1LL<<40)))>>(40-(64-42))) + ((workarea.intform[1]&(-(1LL<<42)))>>(42-0));  // move ff from bit 42 to bit 0; FFF from 40 to 22
}
VLogRingRef(std::string& opaqueref) {  // creating ref from OpaqueRef
  assert(16==opaqueref.size());   // make sure size of ref is right
  VLogRingRef(opaqueref.data());  // make ref from data array
}

// Fill in an existing RingRef
void FillVLogRingRef(int r, VLogRingRefFileno f, VLogRingRefFileOffset o, VLogRingRefFileLen l)
  {ringno=r; fileno=f; offset=o; len=l;}

// Create OpaqueRef (a character array)
void OpaqueRef(char *result) {
  MakeWorkarea();
  memcpy(result,workarea.extform,sizeof(workarea.extform));  // move ref user area
}
void OpaqueRef(std::string &result) {
  MakeWorkarea();
  result.assign(workarea.extform,sizeof(workarea.extform));  // make ref from data array
}

// Create an OpaqueRef in the workarea for this ref and fill in the given slice to point to it
void IndirectSlice(Slice& slice) {
  MakeWorkarea();
  slice.install(workarea.extform,sizeof(workarea.extform));  // point the argument slice to our workarea
}

// Extract portions of the ref
VLogRingRefFileOffset Offset() { return offset; }
VLogRingRefFileLen Len() { return len; }
int Ringno() { return ringno; }
VLogRingRefFileno Fileno() { return fileno; }
// Set portions of the ref
void SetOffset(VLogRingRefFileOffset o) { offset = o; }
void SetFileno(VLogRingRefFileno f) { fileno = f; }
void SetLen(VLogRingRefFileLen l) { len = l; }

// Create Filenumber from RingRef
uint64_t FileNumber() {return (fileno<<2)+ringno;}

private:
VLogRingRefFileno fileno;
VLogRingRefFileOffset offset;
VLogRingRefFileLen len;
int ringno;
union {
  uint64_t intform[2];   // internal form
  char extform[16];   // external form
} workarea;
void MakeWorkarea() {
  assert(offset<=((1LL<<40)-1));  // offset and length should be 5 bytes max
  assert(len<=((1LL<<40)-1));  // offset and length should be 5 bytes max
  assert(fileno<=((1LL<<46)-1));  // fileno should be 46 bits max
  assert(ringno<=3);  // ring should be 2 bits max; fileno+ringno is 48 bits
  workarea.intform[0] = offset + ((((fileno*4)+ringno)&(-(1LL<<(64-40))))<<(40-24));  // isolate FFF starting in bit 24; move to bit 40 of offset
  workarea.intform[1] = len + (((fileno*4)+ringno)<<40);   ///  move ffr to bit 40 of len
}

};

// A VLogRingFile is a random-access file along with a refcount, queue, and filename.
// It is created to add the file to a VLogRing.  When the file is no longer referenced it is deleted; when
// the VLogRingFile object is deleted, the file is simply closed.
class VLogRingFile {
public:
  // The ring:
  std::unique_ptr<RandomAccessFile> filepointer;  // the open file
  // The queue contains one entry for each entry in the fd_ring.  That entry is a linked list of pointers to the SST files whose earliest
  // reference in this ring is in the corresponding file.  When an SST is created, an element is added, and when the SST is finally deleted, one
  // is removed.
  FileMetaData* queue;  // base of forward chain
  int refcount;   // Number of SSTs that hold a reference to this file
  std::string filename;

  VLogRingFile(std::string pathfname,  // fully qualified pathname
    const ImmutableDBOptions *immdbopts,   // The current Env
    EnvOptions& file_options  // options to use for all VLog files
  ) :
    queue(nullptr),
    refcount(0),
    filename(pathfname) {
  // Open the file as random-access and install the pointer.  If error, pointer will be null
#if DEBLEVEL&2
printf("Opening file %s\n",filename.c_str()); // scaf debug
#endif
  immdbopts->env->NewRandomAccessFile(filename, &filepointer, file_options);
}
  VLogRingFile() :   // used to initialize empty slot
    filepointer(nullptr),
    queue(nullptr),
    refcount(0),
    filename("") {
}

  // Delete the file.  Close it as random-access, then delete it
  void DeleteFile(const ImmutableDBOptions *immdbopts,   // The current Env
    EnvOptions& file_options  // options to use for all VLog files
  ) {
    filepointer = nullptr;  // This closes the file if it was open
    immdbopts->env->DeleteFile(filename);  // delete the file
// scaf ignore error - what could we do?
  }

  // Ensure the file is not copyable
  VLogRingFile(VLogRingFile const&) = delete;
  VLogRingFile& operator=(VLogRingFile const&) = delete;

  // But make it movable, needed for emplace_back
    // Move constructor
  VLogRingFile(VLogRingFile&& other) noexcept
  : queue(other.queue), refcount(other.refcount), filename(other.filename)
  {
    filepointer = std::move(other.filepointer);
  }

  // move assignment operator
  VLogRingFile& operator=(VLogRingFile&& rhs) noexcept {
    queue = rhs.queue;
    refcount = rhs.refcount;
    filename = rhs.filename;
    filepointer = std::move(rhs.filepointer);
    return *this;
  }

};

// A VLogRing is a set of sequentially-numbered files, with a common prefix and extension .vlg, that contain
// values for a column family.  Each value is pointed to by an SST, which uses a VLogRingRef for the purpose.
// During compaction, bursts of values are written to the VLogRing, lumped into files of approximately equal size.
//
// Each column family has (possibly many) VLogRing.  VLogRingRef entries in an SST implicitly refer to the VLog of the column family.


class VLogRing {
friend class VLog;
friend class IndirectIterator;
private:

// We have to cross-index the VLog files and the SSTs for two purposes: (1) to see which VLog files can be deleted when they
// are no longer used by an SST; (2) to see which SSTs are pointing to the oldest VLog files, so we can Active Recycle them (or
// give priority to compacting them).
//
// Each SST saves, as part of its metadata, the oldest VLog reference in the SST (there is one of these for each ring in the CF,
// because it is possible though unusual for a single SST to have references to multiple rings).  These VLog references are
// calculated for each compaction and are kept in the Manifest so they can be recovered quickly during recovery.
//
// The SSTs whose earliest reference is to a given VLog file are chained together on a doubly-linked list that is anchored in
// a vector of anchors, one per file.  In addition, each VLog file is assigned a counter indicating how many SSTs hold an earliest-reference
// to it.  An SST is (1) added to the chain when it is created during compaction; (2) removed from the chain
// when it becomes inactive, i. e. no longer in the current view.  Even though the SST is inactive it may be part of a snapshot, so
// the reference counter is not decremented until the SST is finally destroyed (after all snapshots it appears in have been deleted).
//
// When the reference count in the oldest VLog file is zero, that file can be deleted.  The tail pointer in the ring indicates the
// earliest file that has not been inactivated, and the shadow tail pointer indicates the earliest file that has not been deleted.
//
// An SST can be chained to more than one ring.  Because the chain anchors may move, end-of-chain is indicated by a nullptr rather
// than a pointer to the root.
//
// The design of the rings is complicated by two considerations: (1) we want to make the Get() path absolutely as fast as possible,
// without even a read-for-acquire in the normal case; (2) as the database grows, the ring may fill up and have to be relocated.
// In mitigation, reallocation of the ring will be very rare, and any modification, even adding a file, should occur long in advance of the
// first time the file is used as a reference.  We acquire a lock on the ring to make a modification, including reallocation, and we
// double-buffer the ring so that after a reallocation the old ring persists until the next reallocation, which should be days later.

  // The ring:
  std::vector<VLogRingFile> fd_ring;  // the ring of open file descriptors for the VLog files.

#if 0 // scaf will be deleted
  // The queue contains one entry for each entry in the fd_ring.  That entry is a linked list of pointers to the SST files whose earliest
  // reference in this ring is in the corresponding file.  When an SST is created, an element is added, and when the SST is finally deleted, one
  // is removed.
  std::vector<FileMetaData*> queue;  // base of forward chain
  std::vector<int> refcount;   // Number of SSTs that hold a reference to this file
#endif

  // We group the atomics together so they can be aligned to a cacheline boundary
  struct /* alignas(64) */ {   // alignment is desirable, but not available till C++17.  This struct is 1 full cache line
  // The ring head/tail pointers:
  // For the initial implementation we write only entire files and thus don't need offsets.
  // The file numbers are actual disk-file numbers.  The conversion function Ringx converts a file number to a ring index.
  std::atomic<VLogRingRefFileno> fd_ring_head_fileno;   // file number of the last file in the ring.
  std::atomic<VLogRingRefFileno> fd_ring_head_fileno_shadow;   // we move head_fileno to reserve space, before the files have been created.  Move the shadow after they are synced
//   std::atomic<VLogRingRefFileOffset> fd_ring_head_fileoffset;  // the offset that the next write will go to
//  std::atomic<VLogRingRefFileOffset> fd_ring_head_fileoffset_shadow;  // shadow, as with fileno
  std::atomic<VLogRingRefFileno> fd_ring_tail_fileno;   // smallest valid file# in ring
  std::atomic<VLogRingRefFileno> fd_ring_tail_fileno_shadow;   // We move tail_fileno_shadow to remove files from currency, but before we have erased them.
     // after we erase them we move the tail pointer.  So tests for ring-full must use the tail pointer
  // The usecount of the current ring.  Set to 0 initially, incr/decr during Get.  Set to a negative value, which
  // quiesces Get, when we need to resize the ring.  We use this as a sync point for the ringpointer/len so we don't have to use atomic reads on them.
  std::atomic<uint32_t> usecount;
  std::atomic<uint32_t> writelock;  // 0 normally, 1 when the ring is in use for writing
  } atomics;

  // Convert a file number to a ring slot in the current ring.  To avoid the divide we require the ring have power-of-2 size
  size_t Ringx(VLogRingRefFileno f) { return (size_t) f & (fd_ring.size()-1); }

  // Non-ring variables:
  int ringno_;  // The ring number of this ring within its CF
  ColumnFamilyData *cfd_;  // The data for this CF
  const ImmutableDBOptions *immdbopts_;  // Env for this database
  EnvOptions envopts_;  // Options to use for files in this VLog

  // Acquire write lock on the VLogRing.  Won't happen often, and only for a short time
  void AcquireLock() {
    do{
      uint32_t expected_atomic_value = 0;
      if(atomics.writelock.compare_exchange_weak(expected_atomic_value,1,std::memory_order_acq_rel))break;
    }while(1);  // get lock on file
  }

  // Release the lock
  void ReleaseLock() {
  atomics.writelock.store(0,std::memory_order_release);
  }

public:
// Constructor.  Find all the VLogRing files and open them.  Create the VLogRingQueue for the files.
// Delete files that have numbers higher than the last valid one.
VLogRing(
  int ringno,   // ring number of this ring within the CF
  ColumnFamilyData *cfd,  // info for the CF for this ring
  std::vector<std::string> filenames,  // the filenames that might be vlog files for this ring
  VLogRingRefFileno earliest_ref,   // earliest file number referred to in manifest
  const ImmutableDBOptions *immdbopts,   // The current Env
  EnvOptions& file_options  // options to use for all VLog files
);

  // Ensure the ring is not copyable
  VLogRing(VLogRing const&) = delete;
  VLogRing& operator=(VLogRing const&) = delete;



// Write accumulated bytes to the VLogRing.  First allocate the bytes to files, being
// careful not to split a record, then write them all out.  Create new files as needed, and leave them
// open.  The last file will be open for write; the others are read-only
// The result is a VLogRingRef for the first (of possibly several sequential) file, and a vector indicating the
// number of bytes written to each file
// We housekeep the end-of-VLogRing information
void VLogRingWrite(
std::vector<NoInitChar>& bytes,   // The bytes to be written, jammed together
std::vector<VLogRingRefFileOffset>& rcdend,  // The running length of all records up to and including this one
VLogRingRef& firstdataref,   // result: reference to the first value written
std::vector<VLogRingRefFileLen>& fileendoffsets,   // result: ending offset of the data written to each file.  The file numbers written are sequential
          // following the one in firstdataref.  The starting offset in the first file is in firstdataref; it is 0 for the others
std::vector<Status>& resultstatus   // place to save error status.  For any file that got an error in writing or reopening,
          // we add the error status to resultstatus and change the sign of the file's entry in fileendoffsets.  (no entry in fileendoffsets
          // can be 0)
)
;

// Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
// Returns the bytes.  ?? Should this return to user area to avoid copying?
Status VLogRingGet(
  VLogRingRef& request,  // the values to read
  std::string *response   // the data pointed to by the reference
)
;

// Install a new SST into the ring, with the given earliest-VLog reference
void VLogRingSstInstall(
  FileMetaData& newsst   // the SST that has just been created & filled in
)
;

// Remove an SST from the ring when it is no longer current
void VLogRingSstUnCurrent(
  FileMetaData& retiringsst   // the SST that has just been obsoleted
)
;

// Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused
void VLogRingSstDelete(
  FileMetaData& expiringsst   // the SST that is about to be destroyed
);

// Return a vector of up to n SSTs that have the smallest oldest-reference-filenumbers.  If extend is true, return all SSTs
// whose filenumber does not exceed that of the nth-smallest SST's (in other words, return every SST that is tied with n).
void VLogRingFindLaggingSsts(
  int n,  // number of lagging ssts to return
  std::vector<FileMetaData*>& laggingssts,  // result: vector of SSTs that should be recycled
  int extend=0   // if 1, report all 
)
// Do this operation under spin lock.  Reheap to close up deleted SSTs whenever we encounter them
;

};


// A VLog is a set of VLogRings for a single column family.  The VLogRings are distinguished by an index.
class VLog {
private:
  friend class IndirectIterator;
  std::vector<std::unique_ptr<VLogRing>> rings_;  // the VLogRing objects for this CF
  std::vector<int> starting_level_for_ring_;
  ColumnFamilyData *cfd_;
  std::vector<FileMetaData *> waiting_sst_queues;  // queue headers when SSTs are queued awaiting init.  One per possible ring
  std::atomic<uint32_t> writelock;  // 0 normally, 1 when the ring headers are being modified

  // Acquire write lock on the VLog.  Won't happen often, and only for a short time
  void AcquireLock() {
    do{
      uint32_t expected_atomic_value = 0;
      if(writelock.compare_exchange_weak(expected_atomic_value,1,std::memory_order_acq_rel))break;
    }while(1);  // get lock on file
  }

  // Release the lock
  void ReleaseLock() {
  writelock.store(0,std::memory_order_release);
  }

public:
  VLog(
    // the info for the column family
    ColumnFamilyData *cfd
  );
  size_t nrings() { return rings_.size(); }

  // No copying
  VLog(VLog const&) = delete;
  VLog& operator=(VLog const&) = delete;

  // Initialize each ring to make it ready for reading and writing.
  // It would be better to do this when the ring is created, but with the existing interfaces there is
  // no way to get the required information to the constructor
  Status VLogInit(
    std::vector<std::string> vlg_filenames,    // all the filenames that exist for this database - at least, all the vlg files
    const ImmutableDBOptions *immdbopts,  // the currect Env
    EnvOptions& file_options   // options to use for VLog files
  )
    // Go through all the SSTs and create a vector of filerefs for each ring

    // For each ring, initialize the ring and queue
;

#if 0 // scaf will be removed
  // Return a vector of the end-file-number for each ring.  This is the last file number that has been successfully synced.
  // NOTE that there is no guarantee that data is written to files in sequential order, and thus on a restart the
  // end-file-number may cause some space to be lost.  It will be recovered when the ring recycles.
   void GetRingEnds(std::vector<uint64_t> *result) {
    for(int i = 0;i<rings_.size();++i){result->push_back(rings_[i]->atomics.fd_ring_head_fileno_shadow);}
    return;
  }
#endif

  // Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
  // Returns the bytes.  ?? Should this return to user area to avoid copying?
  Status VLogGet(
    Slice& reference,  // the reference
    std::string *result   // where the result is built
  )
  // extract the ring# from the reference
  // Call VLogRingGet in the selected ring
;

  // Given the level of an output file, return the ring number, if any, to write to (-1 if no ring)
  int VLogRingNoForLevelOutput(int level) { int i; for(i=0; i<starting_level_for_ring_.size() && level>=starting_level_for_ring_[i];++i); return i-1;}  // advance if output can go into ring; back up to last such

  // Return the VLogRing for the given level
  VLogRing *VLogRingFromNo(int ringno) { return rings_[ringno].get(); }

  // Install a new SST into the ring, with the given earliest-VLog reference
  // This routine is called whenever a file is added to a column family, which means either
  // during recovery or compaction/ingestion (though note, ingested files have no VLog references and
  // don't need to come through here).  During recovery, the files are encountered before the rings have
  // been created: necessarily, because we don't know how big to make the ring until we have seen what the
  // earliest reference is.  So, if we get called before the rings have been created, we chain them onto
  // a waiting list (one list per eventual ring, using the same chain fields that will normally be used for
  // the doubly-linked list of SSTs per VLog file) and then process them en bloc when the rings are created.
  // Whether this is good design or a contemptible kludge is a matter of opinion.  To be sure, it would
  // be possible to avoid queueing the SSTs by simply using the list of SSTs in each CF as the files to add to the rings
  //
  // We detect pre-initialization by the absence of rings (i. e. rings_.size()==0).  If the CF doesn't turn on
  // indirect values, the number of rings will simply stay at 0.  We will make sure to take no action in removing from the
  // rings in that case.
  //
  // It is OVERWHELMINGLY likely that there will be exactly one nonzero earliest-ref.  The only way to have more
  // is for the ring boundaries to change during operation, and the only way to have less is for an SST to have no
  // indirect values (either too short or all deletes or the like).  Nevertheless we lock most operations at the VLogRing level
  // rather than the VLog level, because it is possible to have compactions going on to the same CF at different levels.
  // In any case all locks are short-lived.
  //
  // We could avoid locks on the chain operations if we were sure that all SST operations held the SST mutex.  They probably do,
  // but I can't guarantee it.  We would still need locks on the queue operations because VLogRingWrite does NOT require the mutex
  // and has to interlock with Get() without locking.
  void VLogSstInstall(
    FileMetaData& newsst   // the SST that has just been created & filled in
  );
  // Remove an SST from the ring when it is no longer current.
  void VLogSstUnCurrent(
    FileMetaData& retiringsst   // the SST that has just been obsoleted
  );

  // Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused
  void VLogSstDelete(
    FileMetaData& expiringsst   // the SST that is about to be destroyed
  );

};


} // namespace rocksdb
	
