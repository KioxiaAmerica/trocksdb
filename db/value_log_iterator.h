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
#include "table/internal_iterator.h"
#include "db/column_family.h"
#include "db/compaction_iterator.h"



namespace rocksdb {

// Iterator class to layer between the compaction_job loop and the compaction_iterator
// We read all the key/values for the compaction and buffer them, write indirect values to disk, and then
// return the possibly modified kvs one by one as the iterator result
class IndirectIterator {
public: 
  static const VLogRingRefFileno high_value = ((VLogRingRefFileno)-1)>>1;  // biggest positive value

  IndirectIterator(
   CompactionIterator* c_iter,   // the input iterator that feeds us kvs
   ColumnFamilyData* cfd,  // the column family we are working on
   int level,   // output level for this iterator - where the files will be written to
   Slice *end,   // the last+1 key to include (i. e. end of open interval), or nullptr if not given
   bool use_indirects   // if false, do not do any indirect processing, just pass through c_iter_
  );

// the following lines are the interface that is shared with CompactionIterator, so these entry points
// must not be modified
  const Slice& key() { return  use_indirects_ ? key_ : c_iter_->key(); }
  const Slice& value() { return use_indirects_ ? value_ : c_iter_->value(); }
  const Status& status() { return use_indirects_ ? status_ : c_iter_->status(); }
  const ParsedInternalKey& ikey() { return use_indirects_ ? ikey_ : c_iter_->ikey(); }
    // If an end key (exclusive) is specified, check if the current key is
    // >= than it and return invalid if it is because the iterator is out of its range
  bool Valid() { return use_indirects_ ? valid_ : (c_iter_->Valid() && 
           !(end_ != nullptr && pcfd->user_comparator()->Compare(c_iter_->user_key(), *end_) >= 0)); }
  void Next();
// end of shared interface
  // Return the vector of earliest-references to each ring within the current file, and clear the value for next file.
  // This should be called AFTER the last key of the current file has been retrieved.
  // We initialize the references to high_value for ease in comparison; but when we return to the user we replace
  // high_value by 0 to indicate 'no reference' to that ring
  // Because of the way this is called from the compaction loop, Next() is called to look ahead one key before
  // closing the output file.  So, ref0_ does not include the last fileref that was returned.  On the very last file, we need to
  // include that key
  void ref0(std::vector<uint64_t>& result, bool include_last) {
    if(!use_indirects_){result = std::vector<uint64_t>(); return; }  // return null value if no indirects
    if(include_last)  // include the last key only for the last file
      if(ref0_[prevringfno.ringno]>prevringfno.fileno)
        ref0_[prevringfno.ringno]=prevringfno.fileno;  // if current > new, switch to new
    result = ref0_; for(size_t i=0;i<ref0_.size();++i){if(result[i]==high_value)result[i]=0; ref0_[i]=high_value;}
    return;
  }

private:
  Slice key_;  // the next key to return, if it is Valid()
  Slice value_;  // the next value to return, if it is Valid()
  Status status_;  // the status to return
  ParsedInternalKey ikey_;  // like key_, but parsed
  std::string npikey;  // string form of ikey_
  bool valid_;  // set when there is another kv to be read
  ColumnFamilyData* pcfd;  // ColumnFamilyData for this run
  CompactionIterator* c_iter_;  // underlying c_iter_, the source for our values
  Slice *end_;   // if given, the key+1 of the end of range
  bool use_indirects_;  // if false, just pass c_iter_ result through
  std::string keys;  // all the keys read from the iterator, jammed together
  std::vector<size_t> keylens;   // length of each string in keys
  size_t keysx_;   // position in keys[] where the next key starts
  std::string passthroughdata;  // data that is passed through unchanged
  std::vector<VLogRingRefFileOffset> passthroughrecl;  // record lengths (NOT running total) of records in passthroughdata
  std::vector<char> valueclass;   // one entry per key.  bit 0 means 'value is a passthrough'; bit 1 means 'value is being converted from direct to indirect'
  std::vector<VLogRingRefFileOffset> diskrecl;  // running total of record lengths in diskdata
  VLogRingRef nextdiskref;  // reference for the first or next first data written to VLog
  std::vector<VLogRingRefFileLen>fileendoffsets;   // end+1 offsets of the data written to successive VLog files
  std::vector<Status> inputerrorstatus;  // error status returned by the iterator
  std::vector<Status> outputerrorstatus;  // error status returned when writing the output files
  std::shared_ptr<VLog> current_vlog;
  std::vector<uint64_t> ref0_;  // for each ring, the earliest reference found into the ring.  Reset when we start each new file
struct RingFno {
  int ringno;
  VLogRingRefFileno fileno;
};
  std::vector<RingFno> diskfileref;   // where we hold the reference values from the input passthroughs
  RingFno prevringfno;  // set to the ring/file for the key we are returning now.  It is not included in the ref0_ value until
    // the NEXT key is returned (this to match the way the compaction job uses the iterator), at which time it is the previous key to use

  int keyno_;  // number of keys processed previously
  int passx_;  // number of passthrough references returned previously
  int diskx_;  // number of disk references returned previously
  int filex_;  // number of files (as returned by RingWrite) that have been completely returned to the user
  int statusx_;  // number of input error statuses returned to user
  int ostatusx_;  // number of output error statuses returned to user
  int passthroughrefx_;  // number of passthrough indirects returned to user
  VLogRingRefFileOffset nextpassthroughref;  // index of next passthrough byte to return

enum valtype : int { vNone=0, vIndirectRemapped=1, vPassthroughDirect=2, vIndirectFirstMap=3, vPassthroughIndirect=4, vHasError=8 };

};

} // namespace rocksdb
	
