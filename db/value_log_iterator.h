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
  IndirectIterator(
   CompactionIterator* c_iter,   // the input iterator that feeds us kvs
   ColumnFamilyData* cfd,  // the column family we are working on
   Slice *end,   // the last+1 key to include (i. e. end of open interval), or nullptr if not given
   bool use_indirects   // if false, do not do any indirect processing, just pass through c_iter_
  );

// the following lines are the interface that is shared with CompactionIterator, so these entry points
// must not be modified
#if 1
  const Slice& key() { return use_indirects_ ? key_ : c_iter_->key(); }
  const Slice& value() { return use_indirects_ ? value_ : c_iter_->value(); }
  const Status& status() { return use_indirects_ ? status_ : c_iter_->status(); }
  const ParsedInternalKey& ikey() { return use_indirects_ ? ikey_ : c_iter_->ikey(); }
    // If an end key (exclusive) is specified, check if the current key is
    // >= than it and return invalid if it is because the iterator is out of its range
  bool Valid() { return use_indirects_ ? valid_ : c_iter_->Valid() && 
           !(end_ != nullptr && pcfd->user_comparator()->Compare(c_iter_->user_key(), *end_) >= 0); }
  void Next() { return c_iter_->Next(); }  // scaf
#else
  const Slice& key() { return c_iter_->key(); }
  const Slice& value() { return c_iter_->value(); }
  const Status& status() { return c_iter_->status(); }
  const Slice& user_key() { return c_iter_->user_key(); }
  const ParsedInternalKey& ikey() { return c_iter_->ikey(); }
  bool Valid() { return c_iter_->Valid(); }
  void Next() { return c_iter_->Next(); }
#endif
// end of shared interface

private:
  Slice key_;  // the next key to return, if it is Valid()
  Slice value_;  // the next value to return, if it is Valid()
  Status status_;  // the status to return
  ParsedInternalKey ikey_;  // like key_, but parsed
  bool valid_;  // set when there is another kv to be read
  ColumnFamilyData* pcfd;  // ColumnFamilyData for this run
  CompactionIterator* c_iter_;  // underlying c_iter_, the source for our values
  Slice *end_;   // if given, the key+1 of the end of range
  bool use_indirects_;  // if false, just pass c_iter_ result through
};

} // namespace rocksdb
	
