//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "port/port.h"
#include "rocksdb/experimental.h"
#include "rocksdb/utilities/convenience.h"
#include "util/sync_point.h"
#include<chrono>
#include<thread>
#ifdef INDIRECT_VALUE_SUPPORT
#include "db/value_log.h"
#endif

namespace rocksdb {

// SYNC_POINT is not supported in released Windows mode.
#if !defined(ROCKSDB_LITE)

class DBVLogTest : public DBTestBase {
 public:
  // these copied from CompactionPickerTest
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableCFOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  LevelCompactionPicker level_compaction_picker;
  std::string cf_name_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::unique_ptr<VersionStorageInfo> vstorage_;
  std::vector<std::unique_ptr<FileMetaData>> files_;
  // does not own FileMetaData
  std::unordered_map<uint32_t, std::pair<FileMetaData*, int>> file_map_;
  // input files to compaction process.
  std::vector<CompactionInputFiles> input_files_;
  int compaction_level_start_;

  DBVLogTest() : DBTestBase("/db_compaction_test"), ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        ioptions_(options_),
        mutable_cf_options_(options_),
        level_compaction_picker(ioptions_, &icmp_),
        cf_name_("dummy"),
        file_num_(1),
        vstorage_(nullptr) {
     fifo_options_.max_table_files_size = 1;
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    ioptions_.cf_paths.emplace_back("dummy",
                                    std::numeric_limits<uint64_t>::max());}

  void DeleteVersionStorage() {
    vstorage_.reset();
    files_.clear();
    file_map_.clear();
    input_files_.clear();
  }

  void NewVersionStorage(int num_levels, CompactionStyle style) {
    DeleteVersionStorage();
    options_.num_levels = num_levels;

    vstorage_.reset(new VersionStorageInfo(&icmp_, ucmp_, options_.num_levels,
                                           style, nullptr, false
#ifdef INDIRECT_VALUE_SUPPORT
    , static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd()  // needs to be &c_f_d for AR compactions
#endif
    ));
    vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
  }

  void UpdateVersionStorageInfo() {
    vstorage_->CalculateBaseBytes(ioptions_, mutable_cf_options_);
    vstorage_->UpdateFilesByCompactionPri(ioptions_.compaction_pri);
    vstorage_->UpdateNumNonEmptyLevels();
    vstorage_->GenerateFileIndexer();
    vstorage_->GenerateLevelFilesBrief();
    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
    vstorage_->GenerateLevel0NonOverlapping();
    vstorage_->ComputeFilesMarkedForCompaction();
    vstorage_->SetFinalized();
  }

  void SetUpForActiveRecycleTest();

};

class DBVLogTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  DBVLogTestWithParam() : DBTestBase("/db_compaction_test") {
    max_subcompactions_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t max_subcompactions_;
  bool exclusive_manual_compaction_;
};

class DBCompactionDirectIOTest : public DBVLogTest,
                                 public ::testing::WithParamInterface<bool> {
 public:
  DBCompactionDirectIOTest() : DBVLogTest() {}
};

namespace {

class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() {}
  ~FlushedFileCollector() {}

  virtual void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (auto fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }

  void ClearFlushedFiles() { flushed_files_.clear(); }

 private:
  std::vector<std::string> flushed_files_;
  std::mutex mutex_;
};

static const int kCDTValueSize = 1000;
static const int kCDTKeysPerBuffer = 4;
static const int kCDTNumLevels = 8;
/* Unused Function DeletionTriggerOptions */
/*
Options DeletionTriggerOptions(Options options) {
  options.compression = kNoCompression;
  options.write_buffer_size = kCDTKeysPerBuffer * (kCDTValueSize + 24);
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 0;
  options.num_levels = kCDTNumLevels;
  options.level0_file_num_compaction_trigger = 1;
  options.target_file_size_base = options.write_buffer_size * 2;
  options.target_file_size_multiplier = 2;
  options.max_bytes_for_level_base =
      options.target_file_size_base * options.target_file_size_multiplier;
  options.max_bytes_for_level_multiplier = 2;
  options.disable_auto_compactions = false;
  return options;
}
*/

/* Unused Function HaveOverlappingKeyRanges */
/*
bool HaveOverlappingKeyRanges(
    const Comparator* c,
    const SstFileMetaData& a, const SstFileMetaData& b) {
  if (c->Compare(a.smallestkey, b.smallestkey) >= 0) {
    if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
      // b.smallestkey <= a.smallestkey <= b.largestkey
      return true;
    }
  } else if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
    // a.smallestkey < b.smallestkey <= a.largestkey
    return true;
  }
  if (c->Compare(a.largestkey, b.largestkey) <= 0) {
    if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
      // b.smallestkey <= a.largestkey <= b.largestkey
      return true;
    }
  } else if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
    // a.smallestkey <= b.largestkey < a.largestkey
    return true;
  }
  return false;
}
*/

// Identifies all files between level "min_level" and "max_level"
// which has overlapping key range with "input_file_meta".
/* Unused Function GetOverlappingFileNumbersForLevelCompaction */
/*
void GetOverlappingFileNumbersForLevelCompaction(
    const ColumnFamilyMetaData& cf_meta,
    const Comparator* comparator,
    int min_level, int max_level,
    const SstFileMetaData* input_file_meta,
    std::set<std::string>* overlapping_file_names) {
  std::set<const SstFileMetaData*> overlapping_files;
  overlapping_files.insert(input_file_meta);
  for (int m = min_level; m <= max_level; ++m) {
    for (auto& file : cf_meta.levels[m].files) {
      for (auto* included_file : overlapping_files) {
        if (HaveOverlappingKeyRanges(
                comparator, *included_file, file)) {
          overlapping_files.insert(&file);
          overlapping_file_names->insert(file.name);
          break;
        }
      }
    }
  }
}
*/

/* Unused Function VerifyCompactionResult */
/*
void VerifyCompactionResult(
    const ColumnFamilyMetaData& cf_meta,
    const std::set<std::string>& overlapping_file_numbers) {
#ifndef NDEBUG
  for (auto& level : cf_meta.levels) {
    for (auto& file : level.files) {
      assert(overlapping_file_numbers.find(file.name) ==
             overlapping_file_numbers.end());
    }
  }
#endif
}
*/

/* Unused Function PickFileRandomly */
/*
const SstFileMetaData* PickFileRandomly(
    const ColumnFamilyMetaData& cf_meta,
    Random* rand,
    int* level = nullptr) {
  auto file_id = rand->Uniform(static_cast<int>(
      cf_meta.file_count)) + 1;
  for (auto& level_meta : cf_meta.levels) {
    if (file_id <= level_meta.files.size()) {
      if (level != nullptr) {
        *level = level_meta.level;
      }
      auto result = rand->Uniform(file_id);
      return &(level_meta.files[result]);
    }
    file_id -= static_cast<uint32_t>(level_meta.files.size());
  }
  assert(false);
  return nullptr;
}
*/
}  // anonymous namespace

#if 1  // turn on for long r/w test.  This is the TRocks random-load test
static std::string LongKey(int i, int len) { return DBTestBase::Key(i).append(len,' '); }
TEST_F(DBVLogTest, IndirectTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 1 * (1LL<<20);
  options.max_bytes_for_level_multiplier = 10;

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  DestroyAndReopen(options);
  int32_t value_size = 18;  // 10 KB
  int32_t key_size = 10 * 1024 - value_size;
  int32_t value_size_var = 20;
  int32_t batch_size = 200; // scaf 200000;

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L2
//  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);
    ASSERT_OK(Put(LongKey(i,key_size), values[i]));
  }
  ASSERT_OK(Flush());
  for (int32_t i = 300; i < 300+batch_size; i++) {
    std::string vstg =  RandomString(&rnd, value_size);
    // append compressible suffix
    vstg.append(rnd.Next()%value_size_var,'a');
    values[i] = vstg;
  }


  for(int32_t k=0;k<10;++k) {
    // Many files 4 [300 => 4300)
    for (int32_t i = 0; i <= 5; i++) {
      for (int32_t j = 300; j < batch_size+300; j++) {
//      if (j == 2300) {
//        ASSERT_OK(Flush());
//        dbfull()->TEST_WaitForFlushMemTable();
//      }
        if((rnd.Next()&0x7f)==0)values[j] = RandomString(&rnd, value_size + rnd.Next()%value_size_var);  // replace one value in 100
        Status s = (Put(LongKey(j,key_size), values[j]));
        if(!s.ok())
          printf("Put failed\n");
        if(i|k) {   // if we have filled up all the slots...
          for(int32_t m=0;m<2;++m){
            int32_t randkey = (rnd.Next()) % batch_size;  // make 2 random gets per put
            std::string getresult = Get(LongKey(randkey,key_size));
            if(getresult.compare(values[randkey])!=0) {
              printf("mismatch: Get result=%s len=%zd\n",getresult.c_str(),getresult.size());
              printf("mismatch: Expected=%s len=%zd\n",values[randkey].c_str(),values[randkey].size());
              std::string getresult2 = Get(LongKey(randkey,key_size));
              if(getresult.compare(getresult2)==0)printf("unchanged on reGet\n");
              else if(getresult.compare(values[randkey])==0)printf("correct on reGet\n");
              else printf("after ReGet: Get result=%s len=%zd\n",getresult2.c_str(),getresult2.size());
            }
          }
        }
      
      }
      printf("batch ");
      std::this_thread::sleep_for(std::chrono::seconds(2));  // give the compactor time to run
    }
//  ASSERT_OK(Flush());
//  dbfull()->TEST_WaitForFlushMemTable();
//  dbfull()->TEST_WaitForCompact();

    for (int32_t j = 0; j < batch_size+300; j++) {
      ASSERT_EQ(Get(LongKey(j,key_size)), values[j]);
    }
    printf("...verified.\n");
    TryReopen(options);
    printf("reopened.\n");
  }

}
#endif


static void ListVLogFileSizes(DBVLogTest *db, std::vector<uint64_t>& vlogfilesizes){
  vlogfilesizes.clear();
  // Get the list of file in the DB, cull that down to VLog files
  std::vector<std::string> filenames;
  ASSERT_OK(db->env_->GetChildren(db->db_->GetOptions().db_paths.back().path, &filenames));
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (ParseFileName(filenames[i], &number, &type)) {
      if (type == kVLogFile) {
        // vlog file.  Extract the CF id from the file extension
        size_t dotpos=filenames[i].find_last_of('.');  // find start of extension
        std::string cfname(filenames[i].substr(dotpos+1+kRocksDbVLogFileExt.size()));
        uint64_t file_size; db->env_->GetFileSize(db->db_->GetOptions().db_paths.back().path+"/"+filenames[i], &file_size);
        vlogfilesizes.emplace_back(file_size);
      }
    }
  }
}


TEST_F(DBVLogTest, VlogFileSizeTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size = (1LL<<14);  // k+v=16KB
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB


  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB
  // Write keys 4MB+multiples of 1MB; Compact them to L1; count files & sizes.
  // # files should be correct, and files should be close to the same size
  printf("Stopping at %zd:",10*options.vlogfile_max_size[0]/value_size);
  for(size_t nkeys = options.vlogfile_max_size[0]/value_size; nkeys<10*options.vlogfile_max_size[0]/value_size; nkeys += (1LL<<20)/value_size){
    printf(" %zd",nkeys);
    DestroyAndReopen(options);
    // write the kvs
    for(int key = 0;key<(int)nkeys;++key){
      ASSERT_OK(Put(Key(key), RandomString(&rnd, value_size)));
    }
    size_t vsize=nkeys*(value_size+5);  // bytes written to vlog.  5 bytes for compression header
    // compact them
    ASSERT_OK(Flush());
    // Compact them into L1, which will write the VLog files
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    dbfull()->TEST_WaitForCompact();
    // 1 file in L1, after compaction
    ASSERT_EQ("0,1", FilesPerLevel(0));
    std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
    ListVLogFileSizes(this,vlogfilesizes);
    // verify number of files is correct
    uint64_t expfiles = (uint64_t)std::max(std::ceil(vsize/((double)options.vlogfile_max_size[0]*1.25)),std::floor(vsize/((double)options.vlogfile_max_size[0])));
    ASSERT_EQ(expfiles, vlogfilesizes.size());
    // verify files have almost the same size
    uint64_t minsize=(uint64_t)~0, maxsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     minsize=std::min(minsize,vlogfilesizes[i]); maxsize=std::max(maxsize,vlogfilesizes[i]);
    }
    ASSERT_LT(maxsize-minsize, 2*value_size);
  }
  printf("\n");
}

TEST_F(DBVLogTest, MinIndirectValSizeTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size_incr = 10;  // k+v=16KB
  const int32_t nkeys = 2000;  // k+v=16KB
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB
  // Write keys 4MB+multiples of 1MB; Compact them to L1; count files & sizes.
  // total # bytes in files should go down as the minimum remapping size goes up
  printf("Stopping at %d:",nkeys*value_size_incr);
  for(size_t minsize=0; minsize<nkeys*value_size_incr; minsize+=500){
    printf(" %zd",minsize);
    options.min_indirect_val_size[0]=minsize;
    DestroyAndReopen(options);
    // write the kvs
    int value_size=0;  // we remember the ending value
    for(int key = 0;key<nkeys;++key){
      ASSERT_OK(Put(Key(key), RandomString(&rnd, value_size)));
      value_size += value_size_incr;
    }
    // compact them
    ASSERT_OK(Flush());
    // Compact them into L1, which will write the VLog files
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    dbfull()->TEST_WaitForCompact();
    // 1 file in L1, after compaction
    ASSERT_EQ("0,1", FilesPerLevel(0));
    std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
    ListVLogFileSizes(this,vlogfilesizes);
    // verify total file size is correct for the keys we remapped
    int64_t totalsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     totalsize += vlogfilesizes[i];
    }
    // mapped size = #mapped rcds * average size of rcd
    int64_t nmappedkeys = nkeys * (value_size - minsize) / value_size;
    int64_t expmappedsize = nmappedkeys * ((minsize + value_size)+5) / 2;  // 5 for compression header
    ASSERT_LT(std::abs(expmappedsize-totalsize), 2*value_size);
  }
  printf("\n");
}


TEST_F(DBVLogTest, VLogCompressionTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size_incr = 10;  // k+v=16KB
  const int32_t nkeys = 2000;  // k+v=16KB
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB
  // Write keys 4MB+multiples of 1MB; Compact them to L1; count files & sizes.
  options.min_indirect_val_size[0]=0;
  DestroyAndReopen(options);
  // write the kvs
  int value_size=0;  // we remember the ending value
  for(int key = 0;key<nkeys;++key){
    std::string vstg(RandomString(&rnd, 10));
    vstg.resize(value_size,' ');  // extend string with compressible data
    ASSERT_OK(Put(Key(key), vstg));
    value_size += value_size_incr;
  }
  // compact them
  ASSERT_OK(Flush());
  // Compact them into L1, which will write the VLog files
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForCompact();
  // 1 file in L1, after compaction
  ASSERT_EQ("0,1", FilesPerLevel(0));
  std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
  ListVLogFileSizes(this,vlogfilesizes);
  // verify total file size is correct for the keys we remapped
  int64_t totalsize=0;  // place to build file stats
  for(size_t i=0;i<vlogfilesizes.size();++i){
   totalsize += vlogfilesizes[i];
  }

  // Repeat with VLog compression on
  options.ring_compression_style = std::vector<CompressionType>({kSnappyCompression});

  DestroyAndReopen(options);
  // write the kvs
  value_size=0;  // we remember the ending value
  for(int key = 0;key<nkeys;++key){
    std::string vstg(RandomString(&rnd, 10));
    vstg.resize(value_size,' ');  // extend string with compressible data
    ASSERT_OK(Put(Key(key), vstg));
    value_size += value_size_incr;
  }
  // compact them
  ASSERT_OK(Flush());
  // Compact them into L1, which will write the VLog files
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForCompact();
  // 1 file in L1, after compaction
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ListVLogFileSizes(this,vlogfilesizes);
  // verify total file size is correct for the keys we remapped
  int64_t totalsizecomp=0;  // place to build file stats
  for(size_t i=0;i<vlogfilesizes.size();++i){
   totalsizecomp += vlogfilesizes[i];
  }


  // Verify compression happened
  ASSERT_LT(totalsizecomp, 0.1*totalsize);
}

TEST_F(DBVLogTest, RemappingFractionTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size = 16384;  // k+v=16KB
  const int32_t nkeys = 100;  // number of files
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB


  printf("Stopping at %d:",40);
  for(int32_t mappct=20; mappct<=40; mappct+= 5){
    double mapfrac = mappct * 0.01;
    printf(" %d",mappct);
    // set remapping fraction to n
    options.fraction_remapped_during_compaction = std::vector<int32_t>({mappct});
    DestroyAndReopen(options);
    // write 100 files in key order, flushing each to give it a VLog file
    std::string val0string;
    for(int key = 0;key<nkeys;++key){
      std::string keystring = Key(key);
      Slice keyslice = keystring;  // the key we will add
      std::string valstring = RandomString(&rnd, value_size);
      if(key==0)val0string = valstring;
      Slice valslice = valstring;
      ASSERT_OK(Put(keyslice, valslice));
      ASSERT_EQ(valslice, Get(Key(key)));
      // Flush into L0
      ASSERT_OK(Flush());
      // Compact into L1, which will write the VLog files.  At this point there is nothing to remap
      CompactRangeOptions compact_options;
      compact_options.change_level = true;
      compact_options.target_level = 1;
      ASSERT_OK(db_->CompactRange(compact_options, &keyslice, &keyslice));
      dbfull()->TEST_WaitForCompact();
    }
    // Verify 100 SSTs and 100 VLog files
    ASSERT_EQ("0,100", FilesPerLevel(0));
    std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
    ListVLogFileSizes(this,vlogfilesizes);
    ASSERT_EQ(100, vlogfilesizes.size());
    // Verify total file size is pretty close to right
    int64_t totalsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     totalsize += vlogfilesizes[i];
    }
    ASSERT_GT(1000, std::abs(totalsize-(value_size+5)*nkeys));
    // compact n-5% to n+5%
    CompactRangeOptions remap_options;
    remap_options.change_level = true;
    remap_options.target_level = 1;
    remap_options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    std::string minkeystring(Key((int)(nkeys*(mapfrac-0.05)))), maxkeystring(Key((int)(nkeys*(mapfrac+0.05))));
    Slice minkey(minkeystring), maxkey(maxkeystring);
    ASSERT_OK(db_->CompactRange(remap_options, &minkey, &maxkey));
    dbfull()->TEST_WaitForCompact();
    // verify that the VLog file total has grown by 5%
    vlogfilesizes.clear();  // reinit list of files
    ListVLogFileSizes(this,vlogfilesizes);
    int64_t newtotalsize=0;  // place to build file stats
    for(size_t i=0;i<vlogfilesizes.size();++i){
     newtotalsize += vlogfilesizes[i];
    }
    ASSERT_GT(value_size+500, (int64_t)std::abs(newtotalsize-totalsize*1.05));  // not a tight match, but if it works throughout the range it's OK
  }
  printf("\n");
}

void DBVLogTest::SetUpForActiveRecycleTest() {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  //const int32_t key_size = 100;
  const int32_t value_size = 16384;  // k+v=16KB
  const int32_t nkeys = 100;  // number of files
  options.target_file_size_base = 100LL << 20;  // high limit for compaction result, so we don't subcompact

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({50});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB

  Random rnd(301);
  // Key of 100 bytes, kv of 16KB
  // Set filesize to 4MB

  // set up 100 files in L1
  // set AR level to 10%
  // compact & remap 15% of the data near the beginning of the database
  // AR should happen, freeing the beginning of the db

  int32_t mappct=20;
  // turn off remapping
  options.fraction_remapped_during_compaction = std::vector<int32_t>({mappct});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  options.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});

  DestroyAndReopen(options);
  // write 100 files in key order, flushing each to give it a VLog file
  std::string val0string;
  for(int key = 0;key<nkeys;++key){
    std::string keystring = Key(key);
    Slice keyslice = keystring;  // the key we will add
    std::string valstring = RandomString(&rnd, value_size);
    if(key==0)val0string = valstring;
    Slice valslice = valstring;
    ASSERT_OK(Put(keyslice, valslice));
    ASSERT_EQ(valslice, Get(Key(key)));
    // Flush into L0
    ASSERT_OK(Flush());
    // Compact into L1, which will write the VLog files.  At this point there is nothing to remap
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    ASSERT_OK(db_->CompactRange(compact_options, &keyslice, &keyslice));
    dbfull()->TEST_WaitForCompact();
  }
  // Verify 100 SSTs and 100 VLog files
  ASSERT_EQ("0,100", FilesPerLevel(0));
  std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
  ListVLogFileSizes(this,vlogfilesizes);
  ASSERT_EQ(100, vlogfilesizes.size());
  // Verify total file size is pretty close to right
  int64_t totalsize=0;  // place to build file stats
  for(size_t i=0;i<vlogfilesizes.size();++i){
   totalsize += vlogfilesizes[i];
  }
  ASSERT_GT(1000, std::abs(totalsize-(value_size+5)*nkeys));
  // compact between 5% and 20%.  This should remap all, leaving 15% fragmentation.
  CompactRangeOptions remap_options;
  remap_options.change_level = true;
  remap_options.target_level = 1;
  remap_options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  remap_options.exclusive_manual_compaction = false;
  std::string minkeystring(Key((int)(nkeys*0.05))), maxkeystring(Key((int)(nkeys*0.20)));
  Slice minkey(minkeystring), maxkey(maxkeystring);
  ASSERT_OK(db_->CompactRange(remap_options, &minkey, &maxkey));
  dbfull()->TEST_WaitForCompact();
  // The user compaction and the AR compaction should both run.

  NewVersionStorage(6, kCompactionStyleLevel);
  UpdateVersionStorageInfo();

  // Now we have 5 VLog files, numbered 1-5, holding keys 0-4, referred to by SSTs 0-4
  // then files 6-19, which have no reference
  // then file 20, which holds key 19 and is referred to by the SST holding keys 5-20
  // then file 21, which holds key 20 and is not the earliest key in any SST
  // then files 22-101, holding keys 21-100

}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest1) {
  SetUpForActiveRecycleTest();

  // For some reason mutable_cf_options_ is not initialized at DestroyAndReopen

  // Verify that we don't pick a compaction when the database is too small
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({1LL<<30});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({15});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_TRUE(compaction.get() == nullptr);

  // Verify that we don't pick a compaction with a 20% size requirement
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({20});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({15});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_TRUE(compaction.get() == nullptr);

  // verify that we do pick a compaction at a 10% size requirement
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({15});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(15, compaction->inputs()->size());  // get the max 15 SSTs allowed
  ASSERT_EQ(30, compaction->lastfileno());   // pick up the VLog files for them, plus the 15 orphaned VLog files

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest2) {
  SetUpForActiveRecycleTest();

  // testing minct.  Set vlogfile_freed_min to 0, then verify responsiveness to min.  freed_min=0 is a debug mode that doesn't look for SSTs past the minimum
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({3});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({0});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(3, compaction->inputs()->size());
  ASSERT_EQ(3, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}


TEST_F(DBVLogTest, ActiveRecycleTriggerTest3) {
  SetUpForActiveRecycleTest();

  // testing minct.  Set vlogfile_freed_min to 0, then verify responsiveness to min.  freed_min=0 is a debug mode that doesn't look for SSTs past the minimum
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({4});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({0});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(4, compaction->inputs()->size());
  ASSERT_EQ(4, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest4) {
  SetUpForActiveRecycleTest();

  // testing maxct.  Move vlogfile_freed_min off of 0, then verify responsiveness to maxct.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({3});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({4});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({1});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(4, compaction->inputs()->size());
  ASSERT_EQ(4, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest5) {
  SetUpForActiveRecycleTest();

  // testing maxct.  Move vlogfile_freed_min off of 0, then verify responsiveness to maxct.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({3});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({1});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(5, compaction->inputs()->size());
  ASSERT_EQ(5, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest6) {
  SetUpForActiveRecycleTest();

  // testing freed_min.  set sst_minct to 0 (debugging mode), then verify responsiveness to freed_min.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({0});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({2});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(2, compaction->inputs()->size());
  ASSERT_EQ(2, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, ActiveRecycleTriggerTest7) {
  SetUpForActiveRecycleTest();

  // testing freed_min.  set sst_minct to 0 (debugging mode), then verify responsiveness to freed_min.
  mutable_cf_options_.fragmentation_active_recycling_trigger = std::vector<int32_t>({10});
  mutable_cf_options_.active_recycling_size_trigger = std::vector<int64_t>({armagictestingvalue});
  mutable_cf_options_.active_recycling_sst_minct = std::vector<int32_t>({0});
  mutable_cf_options_.active_recycling_sst_maxct = std::vector<int32_t>({5});
  mutable_cf_options_.active_recycling_vlogfile_freed_min = std::vector<int32_t>({3});

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, vstorage_.get(), nullptr));
  ASSERT_FALSE(compaction.get() == nullptr);
  ASSERT_EQ(3, compaction->inputs()->size());
  ASSERT_EQ(3, compaction->lastfileno());

  // unfortunately, once we pick a compaction the files in it are marked as running the compaction and are not unmarked even if we
  // delete the compaction.  So once we get a compaction we have to move on to the next test
  compaction.reset();
}

TEST_F(DBVLogTest, VLogRefsTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100 * 1024 * 1024;  // 100MB write buffer: hold all keys
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 5;
  options.level0_stop_writes_trigger = 11;
  options.max_background_compactions = 3;
  options.max_bytes_for_level_base = 100 * (1LL<<20);  // keep level1 big too
  options.max_bytes_for_level_multiplier = 10;

  const int32_t key_size = 16384;
  const int32_t value_size = 16;  // k+v=16KB
  const int32_t max_keys_per_file = 3;  // number of keys we want in each file
  options.target_file_size_base = max_keys_per_file * (key_size-100);  // file size limit, to put 3 refs per file (first, middle, last).  Err on the low side since we make files at least the min size

  options.vlogring_activation_level = std::vector<int32_t>({0});
  options.min_indirect_val_size = std::vector<size_t>({0});
  options.fraction_remapped_during_compaction = std::vector<int32_t>({0});
  options.fraction_remapped_during_active_recycling = std::vector<int32_t>({25});
  options.fragmentation_active_recycling_trigger = std::vector<int32_t>({99});
  options.fragmentation_active_recycling_klaxon = std::vector<int32_t>({50});
  options.active_recycling_sst_minct = std::vector<int32_t>({5});
  options.active_recycling_sst_maxct = std::vector<int32_t>({15});
  options.active_recycling_vlogfile_freed_min = std::vector<int32_t>({7});
  options.compaction_picker_age_importance = std::vector<int32_t>({100});
  options.ring_compression_style = std::vector<CompressionType>({kNoCompression});
  options.vlogfile_max_size = std::vector<uint64_t>({4LL << 20});  // 4MB
  options.active_recycling_size_trigger = std::vector<int64_t>({100LL<<30});

  Random rnd(301);

  // Turn off remapping so that each value stays in its starting VLogFile
  // Create SSTs, with 1 key per file.  The key for files 1 and 6 are as long as 3 normal keys
  // flush each file to L0, then compact into L1 to produce 1 VLog file per SST

  // compact all the files into L2
  // this will produce 1 SST per 3 keys (1 key, for SSTs 1 and 6).

  // pick a compaction of everything, to get references to the files
  // go through the earliest-references, verifying that they match the expected

  // repeat total of 6 times, permuting the keys in each block of 3.  Results should not change

  // repeat again, making the very last key a single-key file.  Results should not change

  int keys_per_file[] = {max_keys_per_file,1,max_keys_per_file,max_keys_per_file,max_keys_per_file,1,max_keys_per_file};
  const int32_t num_files = sizeof(keys_per_file)/sizeof(keys_per_file[0]);
 
  // loop twice, once with the size of the last file set to 1
  printf("Running to permutation 2/6:");
  for(int finalkey=0;finalkey<2;finalkey++){
    // loop over each permutation of inputs
    for(int perm=0;perm<6;++perm){
      int permadd[3]; permadd[0]=perm>>1; permadd[1]=1^(permadd[0]&1); if(perm&1)permadd[1]=3-permadd[0]-permadd[1]; permadd[2]=3-permadd[0]-permadd[1];  // adjustment for each slot of 3
      printf(" %d/%d%d%d",finalkey,permadd[0],permadd[1],permadd[2]);

      // Create SSTs, with 1 key per file.  Flush each file to L0, then compact into L1 to produce 1 VLog file per SST
      DestroyAndReopen(options);

      int key=0;  // running key
      for(int kfx=0;kfx<num_files;++kfx){
        int kpf=keys_per_file[kfx];
        for(int slot=0;slot<kpf;++slot){   // for each key
          int permkey = key; if(kpf>1)permkey+=permadd[slot];  // next key to use: key, but permuted if part of a set
          int keylen = key_size; if(kpf==1)keylen = max_keys_per_file*key_size + (max_keys_per_file-1)*value_size ;  // len of key: if just 1 key per file, make it long so it fills the SST, including the missing 2 values
          if(kfx==(num_files-1) && slot==(kpf-1))keylen-=1000;  // shorten the last key so the SSTs end where we expect

          // Write the key
          std::string keystring = KeyBig(permkey,keylen);
          Slice keyslice = keystring;  // the key we will add
          std::string valstring = RandomString(&rnd, value_size);
          Slice valslice = valstring;
          ASSERT_OK(Put(keyslice, valslice));
          // Read it back
          ASSERT_EQ(valslice, Get(KeyBig(permkey,keylen)));
          // Flush into L0
          ASSERT_OK(Flush());
          // Compact into L1, which will write the VLog files.  At this point there is nothing to remap
          CompactRangeOptions compact_options;
          compact_options.change_level = true;
          compact_options.target_level = 1;
          ASSERT_OK(db_->CompactRange(compact_options, &keyslice, &keyslice));
          dbfull()->TEST_WaitForCompact();
        }
        key += kpf;  // skip to next key
      }

      // All files created now.  Verify # VLog files created
      // Verify 100 SSTs and 100 VLog files
      char buf[100]; sprintf(buf,"0,%d",key);
      ASSERT_EQ(buf, FilesPerLevel(0));
      std::vector<uint64_t> vlogfilesizes;  // sizes of all the VLog files
      ListVLogFileSizes(this,vlogfilesizes);
      ASSERT_EQ(key, vlogfilesizes.size());

      // compact all the files into L2
      // Verify correct # SSTs
      CompactRangeOptions remap_options;
      remap_options.change_level = true;
      remap_options.target_level = 2;
      remap_options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      remap_options.exclusive_manual_compaction = false;
      ASSERT_OK(db_->CompactRange(remap_options,nullptr,nullptr));
      dbfull()->TEST_WaitForCompact();
      // The user compaction and the AR compaction should both run.
      sprintf(buf,"0,0,%d",num_files);
      ASSERT_EQ(buf, FilesPerLevel(0));
      ListVLogFileSizes(this,vlogfilesizes);
      ASSERT_EQ(key, vlogfilesizes.size());

      // Find the list of SST files.  There must be a good way to do this, but I can't figure one out.
      // Here we create a new version and comb through it until we get to the vset, where 'obsolete_files'
      // contains the files before the version was created
      NewVersionStorage(6, kCompactionStyleLevel);
      UpdateVersionStorageInfo();
      VersionStorageInfo *vstore(vstorage_.get());
//      vstore->cfd_->current_->vset_->obsolete_files_;
      std::vector<ObsoleteFileInfo>& currssts = vstore->GetCfd()->current()->version_set()->obsolete_files();

      // Go through the file & verify that the ref0 field has the key of the oldest VLog file in the SST.  This is always the same key,
      // but it appears in different positions depending on the permutation.
      int ref0=1;  // the first file number is 1
      for(int kfx=0;kfx<num_files;++kfx){
        ASSERT_EQ(ref0, currssts[kfx].metadata->indirect_ref_0[0]);
        ref0 += keys_per_file[kfx];  // skip to the batch of VLog values in the next file
      }
    }

    keys_per_file[num_files-1] = 1;  // make the last file 1 key long for the second run
  }
  printf("\n");

}
#endif
}  // namespace rocksdb


int main(int argc, char** argv) {
#if !defined(ROCKSDB_LITE)
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  return 0;
#endif
}
