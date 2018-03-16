//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/value_log_iterator.h"
#include "rocksdb/status.h"

namespace rocksdb {
// Iterator class to go through the files for Active Recycling, in order
//
// This does essentially the same thing as the TwoLevelIterator, but without a LevelFilesBrief, going forward only, and with the facility for
// determining when an input file is exhausted.  Rather than put switches
// into all the other iterator types, we just do everything here, in a single level.
  RecyclingIterator::RecyclingIterator(Compaction *compaction_,  // pointer to all compaction data including files
    VersionSet *versions_  //  pointer to environmant data
  ) :
    compaction(compaction_),
    file_index(-1),  // init pointing before first file
    file_iterator(nullptr),  // init no iterator for file
    env_options(versions_->env_options_compactions())
  {
    // These read options are copied from MakeInputIterator in version_set.cc
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;
  }

  // Go to the next key.  The current key must be valid, or else we must be at the beginning
  void RecyclingIterator::Next() {
    // If there is a current iterator, see if it has another kv
    if(file_iterator==nullptr || (file_iterator->Next(),!file_iterator->Valid())) {
      // the current iterator, if any, has expired.  Advance to the next one
      do {   // loop in case there is a file with no kvs
        // If there is no next file, we are through
        if(++file_index >= compaction->inputs()->size())break;  // exit is no more files
        // Create the iterator for the next (or first) file
        file_iterator.reset(compaction->column_family_data()->table_cache()->NewIterator(
          read_options, *env_options, compaction->column_family_data()->internal_comparator() /* not used */, (*compaction->inputs())[file_index][0]->fd, nullptr /* range_del_agg */ ,
          nullptr /* don't need reference to table */, nullptr /* no file_read_hist */,
          true /* for_compaction */, nullptr /* arena */, false /* skip_filters */, (*compaction->inputs())[file_index][0]->level));
        // start at the beginning of the next file
        file_iterator->SeekToFirst();             
      } while(!file_iterator->Valid());
    }
  }

// Make sure there are at least reservation spaces in vec.  We don't just use reserve directly,
// because it reserves the minimum requested, which leads to quadratic performance
static void reserveatleast(std::vector<NoInitChar>& vec, size_t reservation) {
  if(vec.size()+reservation <= vec.capacity()) return;  // return fast if no need to expand
  size_t newcap = vec.capacity() & -0x40;   // keep capacity a nice boundary
  if(newcap<1024)newcap=1024;   // start with a minimum above 0 so it can grow by multiplication
  while(newcap < vec.size()+reservation)newcap = (size_t)(newcap*2.0) & -0x40;  // grow in large jumps until big enough
  vec.reserve(newcap);
}

// Append the addend to end of charvec, by reserving enough space and then copying in the data
static void appendtovector(std::vector<NoInitChar> &charvec, const Slice &addend) {
  reserveatleast(charvec,addend.size());  // make sure there is room for the new data
  char *bufend = (char *)charvec.data()+charvec.size();   // address of place to put new data
  charvec.resize(charvec.size()+addend.size());  // advance end pointer past end of new data
  memcpy(bufend,addend.data(),addend.size());  // move in the new data
}


// Code for building the indirect-value files.
// This iterator lies between the compaction-iterator and the builder loop.
// We read all the values, save the key/values, write indirects, and then
// return the indirect kvs one by one to the builder.
  IndirectIterator::IndirectIterator(
   CompactionIterator* c_iter,   // the input iterator that feeds us kvs.
   ColumnFamilyData* cfd,  // the column family we are working on
   const Compaction *compaction,   // various info for this compaction
   Slice *end,  // end+1 key in range, if given
   bool use_indirects,  // if false, just pass c_iter through
   RecyclingIterator *recyciter  // null if not Active Recycling; then, points to the iterator
 ) :
  c_iter_(c_iter),
  pcfd(cfd),
  end_(end),
  use_indirects_(use_indirects),
  current_vlog(cfd->vlog())
  {
    // If indirects are disabled, we have nothing to do.  We will just be returning values from c_iter_.
    if(!use_indirects_)return;
    size_t outputringno;   // The ring# we will be writing VLog files to
    // For Active Recycling, use the ringno chosen earlier; otherwise use the ringno for the output level
    if(recyciter!=nullptr)outputringno = compaction->ringno();   // AR
    else{
      int level = compaction->output_level();  // output level for this run
      outputringno = current_vlog->VLogRingNoForLevelOutput(level);  // get the ring number we will write output to
#if DEBLEVEL&4
printf("Creating iterator for level=%d, earliest_passthrough=",level);
#endif
    }
    if(outputringno+1==0){use_indirects_ = false; return;}  // if no output ring, skip looking for indirects
    VLogRing *outputring = current_vlog->VLogRingFromNo((int)outputringno);  // the ring we will write values to

    // Calculate the remapping threshold.  References earlier than this will be remapped.
    //
    // It is not necessary to lock the ring to calculate the remapping - any valid value is OK - but we do need to do an
    // atomic read to make sure we get a recent one
    //
    // We have to calculate a threshold for our output ring.  A reference to any other ring will automatically be passed through
    // For active Recycling, we stop just a few files above the last remapped file, because any remapping that does not
    // result in an empty file just ADDS to fragmentation.  For normal compaction, we copy a fixed fraction of the ring
    VLogRingRefFileno earliest_passthrough;  // the lowest file# that will remain unmapped in the output ring.  All other rings pass through
    VLogRingRefFileno head = current_vlog->rings_[outputringno]->atomics.fd_ring_head_fileno.load(std::memory_order_acquire);  // last file with data
    VLogRingRefFileno tail = current_vlog->rings_[outputringno]->atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);  // first file with live refs
    if(head>tail)head-=tail; else head=0;  // change 'head' to head-tail.  It would be possible for tail to pass head, on an
           // empty ring.  What happens then doesn't matter, but to keep things polite we check for it rather than overflowing the unsigned subtraction.
    earliest_passthrough = (VLogRingRefFileno)(tail + 
                                                    ((recyciter==nullptr) ? compaction->mutable_cf_options()->fraction_remapped_during_active_recycling
                                                                          : compaction->mutable_cf_options()->fraction_remapped_during_compaction)[outputringno]
                                                    * head);  // calc file# before which we remap
        // scaf bug this creates one passthrough for all rings during AR, whilst they might have different thresholds
    // For Active Recycling, since the aim is to free old files, we make sure we remap everything in the files we are going to delete.  More than that is a tuning parameter.
    if(recyciter!=nullptr)earliest_passthrough = std::max(earliest_passthrough,compaction->lastfileno()+4);   // AR   scaf constant, which is max # data files per compaction
#if DEBLEVEL&4
    for(int i=0;i<earliest_passthrough.size();++i)printf("%lld ",earliest_passthrough[i]);
printf("\n");
#endif
    addedfrag.clear(); addedfrag.resize(current_vlog->rings_.size());   // init the fragmentation count to 0 for each ring
    std::vector<VLogRingRefFileOffset> outputrcdend; outputrcdend.reserve(10000); // each entry here is the running total of the bytecounts that will be sent to the SST from each kv

    // Get the compression information to use for this file
    CompressionType compressiontype = compaction->mutable_cf_options()->ring_compression_style[outputringno];  // scaf need option
    CompressionOptions compressionopts{};  // scaf need option
    std::string compressiondict;  // scaf need initial dict

    // For AR, create the list of number of records in each input file.
    filecumreccnts.clear(); if(recyciter!=nullptr)filecumreccnts.resize((const_cast<Compaction *>(compaction))->inputs()->size());
    // Read all the kvs from c_iter and save them.  We start at the first kv
    // We create:
    // diskdata - the compressed, CRCd, values to write to disk.  Stored in key order.
    // diskrecl - the length of each value in diskdata
    // keys - the keys read from c_iter read as a Slice but converted to string
    // passthroughdata - values from c_iter that should be passed through (Slice)
    // valueclass - bit 0 means 'value is a passthrough'; bit 1 means 'value is being converted from direct to indirect', bit 2='Error'

    // The Slices are references to pinned tables.  We copy them into our buffers.
    // They are immediately passed to Builder which must make a copy of the data.
    std::vector<NoInitChar> diskdata;  // where we accumulate the data to write
    std::string indirectbuffer;   // temp area where we read indirect values that need to be remapped

    // init stats we will keep
    remappeddatalen = 0;  // number of bytes that were read & rewritten to a new VLog position
    bytesintocompression = 0;  // number of bytes split off to go to VLog


    // initialize the vectors to reduce later reallocation and copying
    keys.reserve((uint64_t)(std::min(10000000.0,1.2*compaction->max_compaction_bytes()))); diskdata.reserve(50000000);  // scaf size the diskdata better, depending on level
    passthroughrecl.reserve(10000); passthroughdata.reserve(10000*VLogRingRef::sstrefsize);  // reserve space for passthrough data and lengths
    diskrecl.reserve(10000); keylens.reserve(10000); valueclass.reserve(10000);  // scaf const  number of keys in an ordinary compaction

    size_t totalsstlen=0;  // total length so far that will be written to the SST
    while(c_iter->Valid() && 
           !(recyciter==nullptr && end != nullptr && pcfd->user_comparator()->Compare(c_iter->user_key(), *end) >= 0)) {
      // process this kv.  It is valid and the key is not past the specified ending key
      char vclass;   // disposition of this value
      size_t sstvaluelen;  // length of the value that will be written to the sst for this kv

      // If there is error status, save it.  We save only errors
      if(c_iter->status().ok())vclass = vNone;
      else {
        vclass = vHasError;   // indicate error on this key
        inputerrorstatus.push_back(c_iter->status());  // save the full error
      }

      // Classify the value, as (2) a value to be passed through, not indirect (3) a direct value that needs to be converted to indirect;
      // (1) an indirect reference that needs to be remapped; (4) an indirect value that is passed through
      //
      // For case 2, the value is copied to passthroughdata
      // For case 3, the value is compressed and CRCd and written to diskdata
      // For case 1, the (compressed & CRCd) value is read from disk into diskdata and not modified
      // For case 4, the value (16 bytes) is passed through
      const Slice &key = c_iter->key();  // read the key
      // Because the compaction_iterator builds all its return keys in the same buffer, we have to move the key
      // into an area that won't get overwritten.  To avoid lots of memory allocation we jam all the keys into one vector,
      // and keep a vector of lengths
 
// obsolete       keys.append(key.data(),key.size());   // save the key...
      appendtovector(keys,key);  // collect new data into the written-to-disk area
      keylens.push_back(keys.size());    // ... and its cumulative length
      Slice &val = (Slice &) c_iter->value();  // read the value
      sstvaluelen = val.size();  // if value passes through, its length will go to the SST
      if(IsTypeDirect(c_iter->ikey().type) && val.size() > 0 ) {  // scaf compare against min length
        // direct value that should be indirected
        vclass += vIndirectFirstMap;  // indicate the conversion
        bytesintocompression += val.size();  // count length into compression
        std::string compresseddata;  // place the compressed string will go
        // Compress the data.  This will never fail; if there is an error, we just get uncompressed data
        CompressionType ctype = CompressForVLog(std::string(val.data(),val.size()),compressiontype,compressionopts,compressiondict,&compresseddata);
        // Move the compression type and the compressed data into the output area, jammed together
        size_t ctypeindex = diskdata.size();  // offset to the new record
        reserveatleast(diskdata,1+4+compresseddata.size());  // make sure there's room for header+CRC
        diskdata.push_back((char)ctype);
        appendtovector(diskdata,compresseddata);  // collect new data into the written-to-disk area
        // CRC the type/data and move the CRC to the output record
        uint32_t crcint = crc32c::Value((char *)diskdata.data()+ctypeindex,diskdata.size()-ctypeindex);  // take CRC
        // Append the CRC to the type/data, giving final record format of type/data/CRC.  We don't use a structure for this for fear
        // of compiler/architecture variations.  Instead, we treat everything as bytes.  We put the CRC out littlendian here
        for(int i = 0;i<4;++i){diskdata.push_back((char)crcint); crcint>>=8;}

        // We have built the compressed/CRCd record.  save its length
        diskrecl.push_back(diskdata.size());   // write running sum of record lengths, i. e. current total size of diskdata
        sstvaluelen = VLogRingRef::sstrefsize;  // what we write to the SST will be a reference
      } else if(IsTypeIndirect(c_iter->ikey().type)) {  // is indirect ref?
        // value is indirect; does it need to be remapped?
        assert(val.size()==VLogRingRef::sstrefsize);  // should be a reference
        // If the reference is ill-formed, create an error if there wasn't one already on the key
        if(val.size()!=VLogRingRef::sstrefsize){
          ROCKS_LOG_ERROR(current_vlog->immdbopts_->info_log,
            "During compaction: indirect reference has length that is not %d",VLogRingRef::sstrefsize);
          if(vclass<vHasError){   // Don't create an error for this key if it carries one already
            inputerrorstatus.push_back(Status::Corruption("indirect reference is ill-formed."));
            vclass += vHasError;  // indicate that this key now carries error status...
            val = Slice();   // expunge the errant reference.  This will be treated as a passthrough below
            sstvaluelen = 0;  // the value now has 0 length.  Non-erroneous indirects keep their length from the file
// scaf log it?
          }
        } else {
          // Valid indirect reference.  See if it needs to be remapped: too old, or not in our output ring
          VLogRingRef ref(val.data());   // analyze the reference
          assert(ref.Ringno()<addedfrag.size());  // should be a reference
          if(ref.Ringno()>=addedfrag.size()) {  // If ring does not exist for this CF, that's an error
            ROCKS_LOG_ERROR(current_vlog->immdbopts_->info_log,
              "During compaction: reference is to ring %d, but there are only %zd rings",ref.Ringno(),addedfrag.size());
            if(vclass<vHasError){   // Don't create an error for this key if it carries one already
              inputerrorstatus.push_back(Status::Corruption("indirect reference is ill-formed."));
              vclass += vHasError;  // indicate that this key now carries error status...
              val = Slice();   // expunge the errant reference.  This will be treated as a passthrough below
              sstvaluelen = 0;  // the value now has 0 length.  Non-erroneous indirects keep their length from the file
            }
          } else if(ref.Ringno()!=outputringno || ref.Fileno()<earliest_passthrough) {  // file number is too low to pass through
            // indirect value being remapped.  Replace val with the data from disk
            vclass += vIndirectRemapped;  // indicate remapping
            // read the data of the reference.  We don't decompress it or check CRC; we just pass it on.  We know that
            // the compression/CRC do not depend on anything outside the actual value
            reserveatleast(diskdata,ref.Len());  // make sure there is room for the new data
            char *bufend = (char *)diskdata.data()+diskdata.size();   // address of place to put new data
            diskdata.resize(diskdata.size()+ref.Len());  // advance end pointer past end of new data
            // point to the fdring to use for reading indirect values.  Whatever value is current now will be sufficient to translate any indirect that we find
            // in this compaction; however, the ring may be resized while we are using it, so we have to look out for that.  We could set this at the beginning
            // and change only on a change of ring, but we don't take the trouble
            remappeddatalen += ref.Len();  // add to count of remapped bytes
#if DELAYPROB
ProbDelay();
#endif 
            std::vector<VLogRingFile> *fdring = current_vlog->rings_[ref.Ringno()]->fd_ring + current_vlog->rings_[ref.Ringno()]->atomics.currentarrayx.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
            // Get the pointer to the file
            RandomAccessFile *fileptr = (*fdring)[current_vlog->rings_[ref.Ringno()]->Ringx(*fdring,ref.Fileno())].filepointer.get();
#if DELAYPROB
ProbDelay();
#endif 
            if(fileptr==nullptr){
              // Retry the above with the new ring.  It's ugly to repeat the code, but this is the price we pay for allowing references to the possibly-changing
              // ring from outside a lock
#if DELAYPROB
ProbDelay();
#endif 
              current_vlog->rings_[ref.Ringno()]->AcquireLock();
                fdring = current_vlog->rings_[ref.Ringno()]->fd_ring + current_vlog->rings_[ref.Ringno()]->atomics.currentarrayx.load(std::memory_order_acquire);
                fileptr = (*fdring)[current_vlog->rings_[ref.Ringno()]->Ringx(*fdring,ref.Fileno())].filepointer.get();
              current_vlog->rings_[ref.Ringno()]->ReleaseLock();
            }
            if(fileptr!=nullptr && fileptr->Read(ref.Offset(), ref.Len(), &val, bufend).ok()){  // read the data
              // Here the data was read with no error.  It was probably read straight into the buffer, but in case not, move it there
              if(bufend!=val.data())memcpy(bufend,val.data(),val.size());
            } else {
              if(fileptr==nullptr)ROCKS_LOG_ERROR(current_vlog->immdbopts_->info_log,
                "During compaction: reference to file %zd in ring %d, but that file is not open",ref.Fileno(),ref.Ringno());
              else ROCKS_LOG_ERROR(current_vlog->immdbopts_->info_log,
                "During compaction: error reading indirect reference to file %zd in ring %d",ref.Fileno(),ref.Ringno());  // LOG_ERROR requires constant string
              // Here there was an error reading from the file.  Report the error, and reset the data to empty
              if(vclass<vHasError){   // Don't create an error for this key if it carries one already
                inputerrorstatus.push_back(Status::IOError("error reading indirect reference."));
                vclass += vHasError;  // indicate that this key now carries error status...
              }
              diskdata.resize(diskdata.size()-ref.Len());  // error: just pretend the reference was to an empty string.  Take back the space for the disk record
            }
            // move in the record length of the new data
            diskrecl.push_back(diskdata.size());   // write running sum of record lengths, i. e. current total size of diskdata
          } else {
            // indirect value, passed through (normal case).  Mark it as a passthrough, and install the file number in the
            // reference for the ring so it can contribute to the earliest-ref for this file
            vclass += vPassthroughIndirect;  // indirect data passes through...
            diskfileref.push_back(RingFno{ref.Ringno(),ref.Fileno()});   // ... and we save the ring/file of the reference
            // As described below, we must copy the data that is going to be passed through
// obsolete            passthroughdata.append(val.data(),val.size());    // copy the data
            appendtovector(passthroughdata,val);  // copy the reference as data to be passed back to compaction but not written to disk
            passthroughrecl.push_back(val.size());  // save its length too
            // of all the data referenced in the VLog, this is the only data that DOES NOT turn into fragmentation.  Anything else - deletions or remapping -
            // does produce fragmentation.  We subtract the passthrough data from the frag count.  Later, we will add in the total number of bytes referenced to get
            // the total fragmentation added.
            addedfrag[ref.Ringno()] -= ref.Len();
          }
        }
      }
      if(!(vclass&~vHasError)) {
        // not classified above; must be passthrough, and not indirect
        vclass += vPassthroughDirect;  // indicate passthrough
        // Regrettably we have to make a copy of the passthrough data.  Even though the original data is pinned in SSTs,
        // anything returned from a merge uses buffers in the compaction_iterator that are overwritten after each merge.
        // Since most passthrough data is short (otherwise why use indirects?), this is probably not a big problem; the
        // solution would be to keep all merge results valid for the life of the compaction_iterator.
        appendtovector(passthroughdata,val);  // copy the data
// obsolete        passthroughdata.append(val.data(),val.size());    // copy the data
        passthroughrecl.push_back(val.size());
      }
      // save the type of record for use in the replay
      valueclass.push_back(vclass);

      // save the total length of the kv that will be written to the SST, so we can plan file sizes
// scaf emulate differential key encoding to figure this length
      outputrcdend.push_back(totalsstlen += key.size()+VarintLength(key.size())+sstvaluelen+VarintLength(sstvaluelen));

      // Associate the valid kv with the file it comes from.  We store the current record # into the file# slot it came from, so
      // that when it's all over each slot contains the record# of the last record (except for empty files which contain 0 as the ending record#)
      if(recyciter!=nullptr)filecumreccnts[recyciter->fileindex()] = valueclass.size();

      // We have processed one key from the compaction iterator - get the next one
      c_iter->Next();
    }

    // All values have been read from c_iter

    // See how many bytes were read from each ring.  They will become fragmentation, to the extent they were not passed through
    std::vector<int64_t> refsread(std::vector<int64_t>(addedfrag.size()));  // for each ring, the # bytes referred to
    c_iter->RingBytesRefd(refsread);  // read bytesread from the iterator
    for(uint32_t i=0;i<addedfrag.size();++i)addedfrag[i] += refsread[i];  //  every byte will add to fragmentation, if not passed through

    // TODO: It might be worthwhile to sort the kvs by key.  This would be needed only during Active Recycling, since they are
    // automatically sorted during compaction.  Perhaps we could merge by level.

    // Allocate space in the Value Log and write the values out, and save the information for assigning references
    outputring->VLogRingWrite(diskdata,diskrecl,compaction->mutable_cf_options()->vlogfile_max_size[outputringno],firstdiskref,fileendoffsets,outputerrorstatus);
    nextdiskref = firstdiskref;    // remember where we start, and initialize the running pointer to the disk data

    // save what we need to return to stats
    diskdatalen = diskdata.size();  // save # bytes written for stats report
#if DEBLEVEL&4
printf("%zd keys read, with %zd passthroughs\n",keylens.size(),passthroughrecl.size());
#endif
    if(outputerrorstatus.empty()) {
      // No error reading keys and writing to disk.
      if(recyciter==nullptr) {      // If this is NOT Active Recycling, allocate the kvs to SSTs so as to keep files sizes equal.
        BreakRecordsIntoFiles(filecumreccnts, outputrcdend, compaction->max_output_file_size(),
          &compaction->grandparents(), &keys, &keylens, &compaction->column_family_data()->internal_comparator(),
          compaction->max_compaction_bytes());  // calculate filecumreccnts, including use of grandparent info
      }
      // now filecumreccnts has the length in kvs of each eventual output file.  For AR, we mimic the input; for compaction, we create new files

      // set up to read first key
      // set up the variables for the first key
      keyno_ = 0;  // we start on the first key
      passx_ = 0;  // initialize data pointers to start of region
      diskx_ = 0;
      nextpassthroughref = 0;  // init offset of first passthrough record
      filex_ = 0;  // indicate that we are creating references to the first file in filelengths
      statusx_ = 0;  // reset input error pointer to first error
      ostatusx_ = 0;  // reset output error pointer to first error
      passthroughrefx_ = 0;  // reset pointer to passthrough file/ring
      ref0_ = std::vector<uint64_t>(cfd->vlog()->nrings(),high_value);  // initialize to no refs to each ring
      prevringfno = RingFno{0,high_value};  // init to no previous key
      outputfileno = 0;  // init that the records we are emitting are going into SST 0

      Next();   // first Next() gets the first key; subsequent ones return later keys
    } else {
      // If there is an error(s) writing to the VLog, we don't have any way to give them all.  Until such an
      // interface is created, we will just give initial error status, which will abort the compaction
      // The error log was written when the inital error was found
      status_ = Status::Corruption("error writing to VLog");
    }
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
    if((valid_ = keyno_ < valueclass.size())) {
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

      size_t keyx = keyno_ ? keylens[keyno_-1] : 0;  // starting position of previous key
      ParseInternalKey(Slice((char *)keys.data()+keyx,keylens[keyno_] - keyx),&ikey_);  // Fill in the parsed result area

      // Include the previous key's file/ring in the current result (because the compaction job calls Next() before closing
      // the current output file)
      if(ref0_[prevringfno.ringno]>prevringfno.fileno)
        ref0_[prevringfno.ringno]=prevringfno.fileno;  // if current > new, switch to new

      prevringfno = RingFno{0,high_value};  // set to no indirect ref here
      // Create its info based on its class
      switch(vclass) {
      case vPassthroughIndirect:
        // Indirect passthrough.  We need to retrieve the reference file# and apply it
        prevringfno = diskfileref[passthroughrefx_++];   // copy indirect ref, advance to next indirect ref
        // fall through to...
      case vPassthroughDirect:
       // passthrough, either kind.  Fill in the slice from the buffered data, and advance pointers to next record
        value_.install((char *)passthroughdata.data() + nextpassthroughref,passthroughrecl[passx_++]);  // nextpassthroughref is current data offset
        nextpassthroughref += value_.size_;  // advance data offset for next time
        break;
      case vIndirectFirstMap:
        // first mapping: change the value type to indicate indirect
        ikey_.type = (ikey_.type==kTypeValue) ? kTypeIndirectValue : kTypeIndirectMerge;  // must be one or the other
        // fall through to...
      case vIndirectRemapped:
        // Data taken from disk, either to be written the first time or to be rewritten for remapping
        // nextdiskref contains the next record to return
        // Fill in the slice with the current disk reference; then advance the reference to the next record
        nextdiskref.IndirectSlice(value_);  // convert nextdiskref to string form in its workarea, and point value_ to the workarea
        // Save the file/ring of the record we are returning
        prevringfno = RingFno{nextdiskref.Ringno(),nextdiskref.Fileno()};
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


	
