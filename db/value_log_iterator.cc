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
#include "rocksdb/status.h"

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
  use_indirects_(use_indirects),
  current_vlog(cfd->vlog())
  {
    // If indirects are disabled, we have nothing to do.  We will just be returning values from c_iter_.
    if(!use_indirects_)return;
    int outputringno = current_vlog->VLogRingNoForLevelOutput(level);  // get the ring number we will write output to
    if(outputringno<0){use_indirects_ = false; return;}  // if no output ring, skip looking for indirects
    VLogRing *outputring = current_vlog->VLogRingFromNo(outputringno);  // the ring we will write values to

    // Read all the kvs from c_iter and save them.  We start at the first kv
    // We create:
    // diskdata - the compressed, CRCd, values to write to disk.  Stored in key order.
    // diskrecl - the length of each value in diskdata
    // keys - the keys read from c_iter read as a Slice but converted to string
    // passthroughdata - values from c_iter that should be passed through (Slice)
    // valueclass - bit 0 means 'value is a passthrough'; bit 1 means 'value is being converted from direct to indirect'
    //
    // The Slices are references to pinned tables and we return them unchanged as the result of our iterator.
    // They are immediately passed to Builder which must make a copy of the data.
    std::string diskdata;  // where we accumulate the data to write
    while(c_iter->Valid() && 
           !(end != nullptr && pcfd->user_comparator()->Compare(c_iter->user_key(), *end) >= 0)) {
      // process this kv.  It is valid and the key is not past the specified ending key
      char vclass;   // disposition of this value

      // If there is error status, save it.  We save only errors
      if(c_iter->status().ok())vclass = 0;
      else {
        vclass = vHasError;   // indicate error on this key
        inputerrorstatus.push_back(c_iter->status());  // save the full error
      }

      // Classify the value, as (1) a value to be passed through, either too short to encode or an indirect
      // reference that doesn't need changing; (2) a direct value that needs to be converted to indirect;
      // (0) an indirect reference that needs to be remapped
      //
      // For case 1, the value is copied to passthroughdata
      // For case 2, the value is compressed and CRCd and written to diskdata
      // For case 0, the (compressed & CRCd) value is read from disk into diskdata and not modified
      const Slice &key = c_iter->key();  // read the key
      // Because the compaction_iterator builds all its return keys in the same buffer, we have to move the key
      // into an area that won't get overwritten.  To avoid lots of memory allocation we jam all the keys into one vector,
      // and keep a vector of lengths
      keys.append(key.data(),key.size());   // save the key...
      keylens.push_back(key.size());    // ... and its length
      Slice &val = (Slice &) c_iter->value();  // read the value
      if(IsTypeDirect(c_iter->ikey().type) && val.size() > 0 ) {  // scaf include length
        // direct value that should be indirected
        vclass += vIndirectFirstMap;  // indicate the conversion
        // compress the data scaf
        // CRC the data  scaf
        diskdata.append(val.data(),val.size());  // scaf
        diskrecl.push_back(diskdata.size());   // write running sum of record lengths, i. e. current total size of diskdata
      } else if(IsTypeIndirect(c_iter->ikey().type) && 0) {  // scaf
        // indirect value being remapped
        vclass += vIndirectRemapped;  // indicate remapping
        // read the data, no decompression  scaf
        diskdata.append(val.data(),val.size());   // copy the opaque compressed data being remapped
        diskrecl.push_back(diskdata.size());   // write running sum of record lengths, i. e. current total size of diskdata
      } else {
        // otherwise must be passthrough
        vclass += vPassthrough;  // indicate passthrough
        // Regrettably we have to make a copy of the passthrough data.  Even though the original data is pinned in SSTs,
        // anything returned from a merge uses buffers in the compaction_iterator that are overwritten after each merge.
        // Since most passthrough data is short (otherwise why use indirects?), this is probably not a big problem; the
        // solution would be to keep all merge results valid for the life of the compaction_iterator.
         passthroughdata.append(val.data(),val.size());    // copy the data
         passthroughrecl.push_back(val.size());
      }
      // save the type of record for use in the replay
      valueclass.push_back(vclass);

      // We have processed one key from the compaction iterator - get the next one
      c_iter->Next();
    }

    // All values have been read from c_iter

    // Allocate space in the Value Log and write the values out, and save the information for assigning references
    outputring->VLogRingWrite(diskdata,diskrecl,nextdiskref,fileendoffsets,outputerrorstatus);

    // set up the variables for the first key
    keyno_ = 0;  // we start on the first key
    keysx_ = 0;   // it is at position 0 in keys[]
    passx_ = 0;  // initialize data pointers to start of region
    diskx_ = 0;
    nextpassthroughref = 0;  // init offset of first passthrough record
    filex_ = 0;  // indicate that we are creating references to the first file in filelengths
    statusx_ = 0;  // reset input error pointer to first error
    ostatusx_ = 0;  // reset output error pointer to first error

    Next();   // first Next() gets the first key; subsequent ones return later keys
  }

  // set up key_ etc. with the data for the next valid key, whose index in our tables is keyno_
  // We copy all these into temp variables, because the user is allowed to call key() and value() repeatedly and
  // we don't want to repeat any work
  //
  // keyno_ is the index of the key we are about to return, if valid
  // passx_ is the index of the next passthrough record
  // nextpassthroughref is the index of the next passthrough byte to return
  // diskx_ is the index of the next disk data offset (into diskrecl_)
  // nextdiskref_ has the file info for the next disk block
  // diskrecl_ has running total (i. e. record-end points) of each record written en bloc
  // We update all these variables to be ready for the next record
  void IndirectIterator::Next() {
    VLogRingRefFileOffset currendlen;  // the cumulative length of disk records up to the current one

    // If this table type doesn't support indirects, revert to the standard compaction iterator
    if(!use_indirects_){ c_iter_->Next(); return; }
    // Here indirects are supported.  If we have returned all the keys, set this one as invalid
    if(valid_ = (keyno_ < valueclass.size())) {
      // There is another key to return.  Retrieve the key info and parse it

      // If there are errors about, we need to make sure we attach the errors to the correct keys.
      // First we see if there was an input error for the key we are working on
      int vclass = valueclass[keyno_];   // extract key type
      if(vclass<vHasError)status_ = Status();  // if no error on input, init status to good
      else {
        // There was an input error when this key was read.  Set the return status based on the that error.
        // We will go ahead and process the key normally, in case the error was not fatal
        status_ = inputerrorstatus[statusx_++];  // recover input error status, advance to next error
        vclass -= vHasError;  // remove error flag, leaving the value type
      }

      ParseInternalKey(Slice(keys.data()+keysx_,keylens[keyno_]),&ikey_);  // Fill in the parsed result area
      keysx_ += keylens[keyno_];  // advance keysx_ to point to start of next key, for next time
      
      // Create its info based on its class
      switch(vclass) {
      case vPassthrough:
        // passthrough.  Fill in the slice from the buffered data, and advance pointers to next record
        value_.install(passthroughdata.data() + nextpassthroughref,passthroughrecl[passx_++]);  // nextpassthroughref is current data offset
        nextpassthroughref += value_.size_;  // advance data offset for next time
        break;
      case vIndirectFirstMap:
        // first mapping: change the value type to indicate indirect
        ikey_.type = (ikey_.type==kTypeValue) ? kTypeIndirectValue : kTypeIndirectMerge;  // must be one or the other
        // fall through to...
      case vIndirectRemapped:
        // Data taken from disk, either to be written the first time or to be rewritten for remapping
        // Fill in the slice with the current disk reference; then advance the reference to the next record
        nextdiskref.IndirectSlice(value_);  // convert nextdiskref to string form in its workarea, and point value_ to the workarea
        // Advance to the next record - or the next file, getting the new file/offset/length ready in nextdiskref
        // If there is no next indirect value, don't set up for it
        currendlen = diskrecl[diskx_++];  // save end offset of current record, advance to next record
        if(diskx_<diskrecl.size()) {  // if there is going to be a next record...
          nextdiskref.SetOffset(nextdiskref.Offset()+nextdiskref.Len());   // offset of current + len of current = offset of next
          nextdiskref.SetLen(diskrecl[diskx_]-currendlen);    // endpos of next - endpos of current = length of next
          // Subtlety: if the length of the trailing values is 0, we could advance the file pointer past the last file.
          // To ensure that doesn't happen, we advance the output file only when the length of the value is nonzero.
          // It is still possible that a zero-length reference will have a filenumber past the ones we have allocated, so
          // we have to make sure that zero-length indirects (which shouldn't exist!) are not read
          if(nextdiskref.Len()){   // advance to next file only on nonempty value
            // Here, for nonempty current values only, we check to see whether there was an I/O error writing the
            // file.  We haven't advanced to the next file yet.  A negative value in the fileendoffset indicates an error.
            VLogRingRefFileOffset endofst = fileendoffsets[filex_];
            if(endofst<0){
              // error on the output file.  Return the full error information.  If there was an input error AND an output
              // error, keep the input error, since the output error will probably be reported later
              if(status_.ok())status_ = outputerrorstatus[ostatusx_];  // use error for the current file
              endofst = -endofst;  // restore endofst to positive so it can test correctly below
            }
            if(nextdiskref.Offset()==endofst) {   // if start of next rcd = endpoint of current file
              // The next reference is in the next output file.  Advance to the start of the next output file
              //if(fileendoffsets[filex_]<0)++ostatusx_;  // if we are leaving a file with error, point to the next error (if any)
              // Note: fileendoffsets[filex_]<0 is always false because fileendoffsets[x] always returns an unsigned integer.
              nextdiskref.SetFileno(nextdiskref.Fileno()+1);  // next file...
              nextdiskref.SetOffset(0);  // at the beginning...
              ++filex_;   // ... and advance to look at the ending position of the NEXT output file
            }
          }
        }
        break;
      }

      // Now that we know we have the value type right, create the total key to return to the user
      npikey.clear();  // clear the old key
      AppendInternalKey(&npikey, ikey_);
      key_.install(npikey.data(),npikey.size());  // Install data & size into the object buffer, to avoid returning stack variable

      // Advance to next position for next time
      ++keyno_;   // keyno_ always has the key-number to use for the next call to Next()
    }else status_ = Status();  // if key not valid, give good status
  }

}   // namespace rocksdb


	
