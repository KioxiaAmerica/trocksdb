// changes needed in main Rocks code:
// FileMetaData needs a unique ID number for each SST in the system.  Could be unique per column family if that's easier
// Manifest needs smallest file# referenced by the SST

//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/value_log_iterator.h"
#include "db/value_log.h"

namespace rocksdb {


// Code for building the indirect-value files.
// This iterator lies between the compaction-iterator and the builder loop.
// We read all the values, save the key/values, write indirects, and then
// return the indirect kvs one by one to the builder.
  IndirectIterator::IndirectIterator(
   CompactionIterator* c_iter,   // the input iterator that feeds us kvs
   ColumnFamilyData* cfd,  // the column family we are working on
   int level,   // output level for this iterator - where the files will be written to
   Slice *end,  // end+1 key in range, if given
   bool use_indirects  // if false, just pass c_iter through
  ) :
  c_iter_(c_iter),
  pcfd(cfd),
  end_(end),
  use_indirects_(0 && use_indirects),  // scaf
  current_vlog(cfd->vlog())
  {
    // If indirects are disabled, we have nothing to do.  We will just be returning values from c_iter_.
    if(!use_indirects)return;
    int outputringno = current_vlog->VLogRingNoForLevelOutput(level);  // get the ring number we will write output to
    if(outputringno<0)return;  // if no output ring, skip looking for indirects
    VLogRing *outputring = current_vlog->VLogRingFromNo(outputringno);
// scaf handle status;

    // Read all the kvs from c_iter and save them.  We start at the first kv
    // We create:
    // diskdata - the compressed, CRCd, values to write to disk.  Stored in key order.
    // diskrecl - the length of each value in diskdata
    // keys - the keys read from c_iter (Slice)
    // passthroughdata - values from c_iter that should be passed through (Slice)
    // valueclass - bit 0 means 'value is a passthrough'; bit 1 means 'value is being converted from direct to indirect'
    //
    // The Slices are references to pinned tables and we return them unchanged as the result of our iterator.
    // They are immediately passed to Builder which must make a copy of the data.
    std::string diskdata;  // where we accumulate the data to write
    std::vector<size_t> diskrecl;  // length of each record in diskdata
    while(c_iter->Valid() && 
           !(end != nullptr && pcfd->user_comparator()->Compare(c_iter->user_key(), *end) >= 0)) {
      // process this kv.  It is valid and the key is not past the specified ending key

      // Classify the value, as (1) a value to be passed through, either too short to encode or an indirect
      // reference that doesn't need changing; (2) a direct value that needs to be converted to indirect;
      // (0) an indirect reference that needs to be remapped
      //
      // For case 1, the value is copied to passthroughdata
      // For case 2, the value is compressed and CRCd and written to diskdata
      // For case 0, the (compressed & CRCd) value is read from disk into diskdata and not modified
      Slice &key = (Slice &) c_iter->key();  // read the key
      keys.push_back(key);  // put it in our saved list of keys
      Slice &val = (Slice &) c_iter->value();  // read the value
      char vclass = 1;  // init class to 'passthrough'
      if(IsTypeDirect(c_iter->ikey().type) && val.size() > 0 ) {  // scaf include length
        // direct value that should be indirected
        vclass = 2;  // indicate the conversion
        // compress the data scaf
        // CRC the data  scaf
        diskrecl.push_back(val.size());  // scaf use modified size
        diskdata.append(val.data(),val.size());  // scaf
      } else if(IsTypeIndirect(c_iter->ikey().type) && 0) {
        // indirect value being remapped
        vclass = 0;  // indicate remapping
        // read the data, no decompression
        diskrecl.push_back(val.size());  // scaf use modified size
        diskdata.append(val.data(),val.size());  // scaf
      } else {
        // otherwise must be passthrough
        vclass = 1;  // indicate passthrough
        passthroughdata.push_back(val);    // put the slice in our list.  It is pinned so this is safe
      }
      // save the type of record for use in the replay
      valueclass.push_back(vclass);
    }

    // All values have been read from c_iter

    // Allocate space in the Value Log and write the values out, and save the information for assigning references
    status_ = outputring->VLogRingWrite(diskdata,diskrecl,&startingref_,&filelengths_);

    // set up the variables for the first key
    keyno_ = 0;  // we start on the first key
    passx_ = 0;  // initialize data pointers to start of region
    diskx_ = 0;
    SetReturnVariables();
  }

  // set up key_ etc. with the data for the next valid key
  void IndirectIterator::SetReturnVariables() {
// scaf  fill it in
  }


}   // namespace rocksdb


	
