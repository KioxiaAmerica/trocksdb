//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// #include <iostream>
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_builder.h"
#include "test_util/fault_injection_test_env.h"
#if !defined(ROCKSDB_LITE)
#include "test_util/sync_point.h"
#endif

namespace rocksdb {

class DBBasicTest : public DBTestBase {
 public:
  DBBasicTest() : DBTestBase("/db_basic_test") {}
};

TEST_F(DBBasicTest, OpenWhenOpen) {
  Options options = CurrentOptions();
  options.env = env_;
  rocksdb::DB* db2 = nullptr;
  rocksdb::Status s = DB::Open(options, dbname_, &db2);

  ASSERT_EQ(Status::Code::kIOError, s.code());
  ASSERT_EQ(Status::SubCode::kNone, s.subcode());
  ASSERT_TRUE(strstr(s.getState(), "lock ") != nullptr);

  delete db2;
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, ReadOnlyDB) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  auto options = CurrentOptions();
  assert(options.env == env_);
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  Iterator* iter = db_->NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }
  ASSERT_EQ(count, 2);
  delete iter;
  Close();

  // Reopen and flush memtable.
  Reopen(options);
  Flush();
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  ASSERT_TRUE(db_->SyncWAL().IsNotSupported());
}

const uint64_t kFileSize = 1 << 20;
const uint64_t kHalfFileSize = kFileSize >> 1;
TEST_F(DBBasicTest, CompactedDB) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = kFileSize;
  options.target_file_size_base = kFileSize;
  options.max_bytes_for_level_base = 1 << 30;
  options.compression = kNoCompression;
  Reopen(options);
  bool values_are_indirect = false;  // Set if we are using VLogging
#ifndef NO_INDIRECT_VALUE
  values_are_indirect = options.vlogring_activation_level.size()!=0;
#endif
  // 1 L0 file, use CompactedDB if max_open_files = -1
  ASSERT_OK(Put(KeyInvInd(std::string("aaa"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'1'),values_are_indirect)));
  Flush();
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  Status s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'1'),values_are_indirect), Get(KeyInvInd(std::string("aaa"),kHalfFileSize,values_are_indirect)));
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'1'),values_are_indirect), Get(KeyInvInd(std::string("aaa"),kHalfFileSize,values_are_indirect)));
  Close();
  Reopen(options);
  // Add more L0 files
  ASSERT_OK(Put(KeyInvInd(std::string("bbb"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'2'),values_are_indirect)));
  Flush();
  ASSERT_OK(Put(KeyInvInd(std::string("aaa"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'a'),values_are_indirect)));
  Flush();
  ASSERT_OK(Put(KeyInvInd(std::string("bbb"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'b'),values_are_indirect)));
  ASSERT_OK(Put(KeyInvInd(std::string("eee"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'e'),values_are_indirect)));
  Flush();
  Close();

  ASSERT_OK(ReadOnlyReopen(options));
  // Fallback to read-only DB
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
  Close();

  // Full compaction
  Reopen(options);
  // Add more keys
  ASSERT_OK(Put(KeyInvInd(std::string("fff"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'f'),values_are_indirect)));
  ASSERT_OK(Put(KeyInvInd(std::string("hhh"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'h'),values_are_indirect)));
  ASSERT_OK(Put(KeyInvInd(std::string("iii"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'i'),values_are_indirect)));
  ASSERT_OK(Put(KeyInvInd(std::string("jjj"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'j'),values_are_indirect)));
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(3, NumTableFilesAtLevel(1));
  Close();

  // CompactedDB
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ("NOT_FOUND", Get(KeyInvInd(std::string("abc"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'a'),values_are_indirect), Get(KeyInvInd(std::string("aaa"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'b'),values_are_indirect), Get(KeyInvInd(std::string("bbb"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ("NOT_FOUND", Get(KeyInvInd(std::string("ccc"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'e'),values_are_indirect), Get(KeyInvInd(std::string("eee"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'f'),values_are_indirect), Get(KeyInvInd(std::string("fff"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ("NOT_FOUND", Get(KeyInvInd(std::string("ggg"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'h'),values_are_indirect), Get(KeyInvInd(std::string("hhh"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'i'),values_are_indirect), Get(KeyInvInd(std::string("iii"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'j'),values_are_indirect), Get(KeyInvInd(std::string("jjj"),kHalfFileSize,values_are_indirect)));
  ASSERT_EQ("NOT_FOUND", Get(KeyInvInd(std::string("kkk"),kHalfFileSize,values_are_indirect)));

  // MultiGet
  std::vector<std::string> values;
  std::vector<Status> status_list = dbfull()->MultiGet(
      ReadOptions(),
      std::vector<Slice>({Slice(KeyInvInd(std::string("aaa"),kHalfFileSize,values_are_indirect)), Slice(KeyInvInd(std::string("ccc"),kHalfFileSize,values_are_indirect)), Slice(KeyInvInd(std::string("eee"),kHalfFileSize,values_are_indirect)),
                          Slice(KeyInvInd(std::string("ggg"),kHalfFileSize,values_are_indirect)), Slice(KeyInvInd(std::string("iii"),kHalfFileSize,values_are_indirect)), Slice(KeyInvInd(std::string("kkk"),kHalfFileSize,values_are_indirect))}),
      &values);
  ASSERT_EQ(status_list.size(), static_cast<uint64_t>(6));
  ASSERT_EQ(values.size(), static_cast<uint64_t>(6));
  ASSERT_OK(status_list[0]);
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'a'),values_are_indirect), values[0]);
  ASSERT_TRUE(status_list[1].IsNotFound());
  ASSERT_OK(status_list[2]);
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'e'),values_are_indirect), values[2]);
  ASSERT_TRUE(status_list[3].IsNotFound());
  ASSERT_OK(status_list[4]);
  ASSERT_EQ(ValueInvInd(std::string(kHalfFileSize,'i'),values_are_indirect), values[4]);
  ASSERT_TRUE(status_list[5].IsNotFound());

  Reopen(options);
  // Add a key
  ASSERT_OK(Put(KeyInvInd(std::string("fff"),kHalfFileSize,values_are_indirect), ValueInvInd(std::string(kHalfFileSize,'f'),values_are_indirect)));
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
}

TEST_F(DBBasicTest, LevelLimitReopen) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  bool values_are_indirect = false;  // Set if we are using VLogging
#ifndef NO_INDIRECT_VALUE
  values_are_indirect = options.vlogring_activation_level.size()!=0;
#endif

  const std::string value(1024 * 1024, ' ');
  int i = 0;
  while (NumTableFilesAtLevel(2, 1) == 0) {
    ASSERT_OK(Put(1, KeyInvInd(i++,value.size(),values_are_indirect), ValueInvInd(value,values_are_indirect)));
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }

  options.num_levels = 1;
  options.max_bytes_for_level_multiplier_additional.resize(1, 1);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(s.IsInvalidArgument(), true);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: db has more levels than options.num_levels");

  options.num_levels = 10;
  options.max_bytes_for_level_multiplier_additional.resize(10, 1);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}
#endif  // ROCKSDB_LITE

TEST_F(DBBasicTest, PutDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBBasicTest, PutSingleDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo2", "v2"));
    ASSERT_EQ("v2", Get(1, "foo2"));
    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    // Ski FIFO and universal compaction because they do not apply to the test
    // case. Skip MergePut because single delete does not get removed when it
    // encounters a merge.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_F(DBBasicTest, EmptyFlush) {
  // It is possible to produce empty flushes when using single deletes. Tests
  // whether empty flushes cause issues.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "a", Slice());
    SingleDelete(1, "a");
    ASSERT_OK(Flush(1));

    ASSERT_EQ("[ ]", AllEntriesFor("a", 1));
    // Skip FIFO and  universal compaction as they do not apply to the test
    // case. Skip MergePut because merges cannot be combined with single
    // deletions.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_F(DBBasicTest, GetFromVersions) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  } while (ChangeOptions());
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, GetSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_OK(Put(1, key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      ASSERT_OK(Put(1, key, "v2"));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      ASSERT_OK(Flush(1));
      ASSERT_EQ("v2", Get(1, key));
      ASSERT_EQ("v1", Get(1, key, s1));
      db_->ReleaseSnapshot(s1);
    }
  } while (ChangeOptions());
}
#endif  // ROCKSDB_LITE

TEST_F(DBBasicTest, CheckLock) {
  do {
    DB* localdb;
    Options options = CurrentOptions();
    ASSERT_OK(TryReopen(options));

    // second open should fail
    ASSERT_TRUE(!(DB::Open(options, dbname_, &localdb)).ok());
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushMultipleMemtable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    options.max_write_buffer_number_to_maintain = -1;
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));
    ASSERT_OK(Flush(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushEmptyColumnFamily) {
  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  Options options = CurrentOptions();
  // disable compaction
  options.disable_auto_compactions = true;
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_number_to_maintain = 1;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Compaction can still go through even if no thread can flush the
  // mem table.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  // Insert can go through
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[0], "foo", "v1"));
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

  ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(1, "bar"));

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();

  // Flush can still go through.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBBasicTest, FLUSH) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    SetPerfLevel(kEnableTime);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    // this will now also flush the last 2 writes
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    get_perf_context()->Reset();
    Get(1, "foo");
    ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);
    ASSERT_EQ(2, (int)get_perf_context()->get_read_bytes);

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v2", Get(1, "bar"));
    get_perf_context()->Reset();
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // 'foo' should be there because its put
    // has WAL enabled.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, ManifestRollOver) {
  do {
    Options options;
    options.max_manifest_file_size = 10;  // 10 bytes
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    {
      ASSERT_OK(Put(1, "manifest_key1", std::string(1000, '1')));
      ASSERT_OK(Put(1, "manifest_key2", std::string(1000, '2')));
      ASSERT_OK(Put(1, "manifest_key3", std::string(1000, '3')));
      uint64_t manifest_before_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_OK(Flush(1));  // This should trigger LogAndApply.
      uint64_t manifest_after_flush = dbfull()->TEST_Current_Manifest_FileNo();
      ASSERT_GT(manifest_after_flush, manifest_before_flush);
      ReopenWithColumnFamilies({"default", "pikachu"}, options);
      ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_after_flush);
      // check if a new manifest file got inserted or not.
      ASSERT_EQ(std::string(1000, '1'), Get(1, "manifest_key1"));
      ASSERT_EQ(std::string(1000, '2'), Get(1, "manifest_key2"));
      ASSERT_EQ(std::string(1000, '3'), Get(1, "manifest_key3"));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, IdentityAcrossRestarts) {
  do {
    std::string id1;
    ASSERT_OK(db_->GetDbIdentity(id1));

    Options options = CurrentOptions();
    Reopen(options);
    std::string id2;
    ASSERT_OK(db_->GetDbIdentity(id2));
    // id1 should match id2 because identity was not regenerated
    ASSERT_EQ(id1.compare(id2), 0);

    std::string idfilename = IdentityFileName(dbname_);
    ASSERT_OK(env_->DeleteFile(idfilename));
    Reopen(options);
    std::string id3;
    ASSERT_OK(db_->GetDbIdentity(id3));
    // id1 should NOT match id3 because identity was regenerated
    ASSERT_NE(id1.compare(id3), 0);
  } while (ChangeCompactOptions());
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, Snapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    Put(0, "foo", "0v1");
    Put(1, "foo", "1v1");

    const Snapshot* s1 = db_->GetSnapshot();
    ASSERT_EQ(1U, GetNumSnapshots());
    uint64_t time_snap1 = GetTimeOldestSnapshots();
    ASSERT_GT(time_snap1, 0U);
    Put(0, "foo", "0v2");
    Put(1, "foo", "1v2");

    env_->addon_time_.fetch_add(1);

    const Snapshot* s2 = db_->GetSnapshot();
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    Put(0, "foo", "0v3");
    Put(1, "foo", "1v3");

    {
      ManagedSnapshot s3(db_);
      ASSERT_EQ(3U, GetNumSnapshots());
      ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());

      Put(0, "foo", "0v4");
      Put(1, "foo", "1v4");
      ASSERT_EQ("0v1", Get(0, "foo", s1));
      ASSERT_EQ("1v1", Get(1, "foo", s1));
      ASSERT_EQ("0v2", Get(0, "foo", s2));
      ASSERT_EQ("1v2", Get(1, "foo", s2));
      ASSERT_EQ("0v3", Get(0, "foo", s3.snapshot()));
      ASSERT_EQ("1v3", Get(1, "foo", s3.snapshot()));
      ASSERT_EQ("0v4", Get(0, "foo"));
      ASSERT_EQ("1v4", Get(1, "foo"));
    }

    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ("0v1", Get(0, "foo", s1));
    ASSERT_EQ("1v1", Get(1, "foo", s1));
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));

    db_->ReleaseSnapshot(s1);
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
    ASSERT_EQ(1U, GetNumSnapshots());
    ASSERT_LT(time_snap1, GetTimeOldestSnapshots());

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ(0U, GetNumSnapshots());
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
  } while (ChangeOptions());
}

#endif  // ROCKSDB_LITE

TEST_F(DBBasicTest, CompactBetweenSnapshots) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);
    Random rnd(301);
    FillLevels("a", "z", 1);

    Put(1, "foo", "first");
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put(1, "foo", "second");
    Put(1, "foo", "third");
    Put(1, "foo", "fourth");
    const Snapshot* snapshot2 = db_->GetSnapshot();
    Put(1, "foo", "fifth");
    Put(1, "foo", "sixth");

    // All entries (including duplicates) exist
    // before any compaction or flush is triggered.
    ASSERT_EQ(AllEntriesFor("foo", 1),
              "[ sixth, fifth, fourth, third, second, first ]");
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ("first", Get(1, "foo", snapshot1));

    // After a flush, "second", "third" and "fifth" should
    // be removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth, first ]");

    // after we release the snapshot1, only two values left
    db_->ReleaseSnapshot(snapshot1);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);

    // We have only one valid snapshot snapshot2. Since snapshot1 is
    // not valid anymore, "first" should be removed by a compaction.
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth ]");

    // after we release the snapshot2, only one value should be left
    db_->ReleaseSnapshot(snapshot2);
    FillLevels("a", "z", 1);
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ("sixth", Get(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth ]");
  } while (ChangeOptions(kSkipFIFOCompaction));
}

TEST_F(DBBasicTest, DBOpen_Options) {
  Options options = CurrentOptions();
  Close();
  Destroy(options);

  // Does not exist, and create_if_missing == false: error
  DB* db = nullptr;
  options.create_if_missing = false;
  Status s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does not exist, and create_if_missing == true: OK
  options.create_if_missing = true;
  s = DB::Open(options, dbname_, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;

  // Does exist, and error_if_exists == true: error
  options.create_if_missing = false;
  options.error_if_exists = true;
  s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does exist, and error_if_exists == false: OK
  options.create_if_missing = true;
  options.error_if_exists = false;
  s = DB::Open(options, dbname_, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  delete db;
  db = nullptr;
}

TEST_F(DBBasicTest, CompactOnFlush) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    Put(1, "foo", "v1");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v1 ]");

    // Write two new keys
    Put(1, "a", "begin");
    Put(1, "z", "end");
    Flush(1);

    // Case1: Delete followed by a put
    Delete(1, "foo");
    Put(1, "foo", "v2");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");

    // After the current memtable is flushed, the DEL should
    // have been removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");

    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");

    // Case 2: Delete followed by another delete
    Delete(1, "foo");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, DEL, v2 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v2 ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 3: Put followed by a delete
    Put(1, "foo", "v3");
    Delete(1, "foo");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v3 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 4: Put followed by another Put
    Put(1, "foo", "v4");
    Put(1, "foo", "v5");
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5, v4 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");

    // clear database
    Delete(1, "foo");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: Put followed by snapshot followed by another Put
    // Both puts should remain.
    Put(1, "foo", "v6");
    const Snapshot* snapshot = db_->GetSnapshot();
    Put(1, "foo", "v7");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v7, v6 ]");
    db_->ReleaseSnapshot(snapshot);

    // clear database
    Delete(1, "foo");
    dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                           nullptr);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: snapshot followed by a put followed by another Put
    // Only the last put should remain.
    const Snapshot* snapshot1 = db_->GetSnapshot();
    Put(1, "foo", "v8");
    Put(1, "foo", "v9");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v9 ]");
    db_->ReleaseSnapshot(snapshot1);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushOneColumnFamily) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "pikachu", "pikachu"));
  ASSERT_OK(Put(2, "ilya", "ilya"));
  ASSERT_OK(Put(3, "muromec", "muromec"));
  ASSERT_OK(Put(4, "dobrynia", "dobrynia"));
  ASSERT_OK(Put(5, "nikitich", "nikitich"));
  ASSERT_OK(Put(6, "alyosha", "alyosha"));
  ASSERT_OK(Put(7, "popovich", "popovich"));

  for (int i = 0; i < 8; ++i) {
    Flush(i);
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), i + 1U);
  }
}

TEST_F(DBBasicTest, MultiGetSimple) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});

    std::vector<std::string> values(20, "Temporary data to be overwritten");
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);

    get_perf_context()->Reset();
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(values[0], "v1");
    ASSERT_EQ(values[1], "v2");
    ASSERT_EQ(values[2], "v3");
    ASSERT_EQ(values[4], "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());
    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, MultiGetEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Empty Key Set
    std::vector<Slice> keys;
    std::vector<std::string> values;
    std::vector<ColumnFamilyHandle*> cfs;
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Empty Key Set
    Options options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Search for Keys
    keys.resize(2);
    keys[0] = "a";
    keys[1] = "b";
    cfs.push_back(handles_[0]);
    cfs.push_back(handles_[1]);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(static_cast<int>(s.size()), 2);
    ASSERT_TRUE(s[0].IsNotFound() && s[1].IsNotFound());
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, ChecksumTest) {
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  // change when new checksum type added
  int max_checksum = static_cast<int>(kxxHash64);
  const int kNumPerFile = 2;

  // generate one table with each type of checksum
  for (int i = 0; i <= max_checksum; ++i) {
    table_options.checksum = static_cast<ChecksumType>(i);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    for (int j = 0; j < kNumPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumPerFile + j), Key(i * kNumPerFile + j)));
    }
    ASSERT_OK(Flush());
  }

  // verify data with each type of checksum
  for (int i = 0; i <= kxxHash64; ++i) {
    table_options.checksum = static_cast<ChecksumType>(i);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    for (int j = 0; j < (max_checksum + 1) * kNumPerFile; ++j) {
      ASSERT_EQ(Key(j), Get(Key(j)));
    }
  }
}

// On Windows you can have either memory mapped file or a file
// with unbuffered access. So this asserts and does not make
// sense to run
#ifndef OS_WIN
TEST_F(DBBasicTest, MmapAndBufferOptions) {
  if (!IsMemoryMappedAccessSupported()) {
    return;
  }
  Options options = CurrentOptions();

  options.use_direct_reads = true;
  options.allow_mmap_reads = true;
  ASSERT_NOK(TryReopen(options));

  // All other combinations are acceptable
  options.use_direct_reads = false;
  ASSERT_OK(TryReopen(options));

  if (IsDirectIOSupported()) {
    options.use_direct_reads = true;
    options.allow_mmap_reads = false;
    ASSERT_OK(TryReopen(options));
  }

  options.use_direct_reads = false;
  ASSERT_OK(TryReopen(options));
}
#endif

class TestEnv : public EnvWrapper {
  public:
    explicit TestEnv() : EnvWrapper(Env::Default()),
                close_count(0) { }

    class TestLogger : public Logger {
      public:
        using Logger::Logv;
        TestLogger(TestEnv *env_ptr) : Logger() { env = env_ptr; }
        ~TestLogger() override {
          if (!closed_) {
            CloseHelper();
          }
        }
        void Logv(const char* /*format*/, va_list /*ap*/) override{};

       protected:
        Status CloseImpl() override { return CloseHelper(); }

       private:
        Status CloseHelper() {
          env->CloseCountInc();;
          return Status::IOError();
        }
        TestEnv *env;
    };

    void CloseCountInc() { close_count++; }

    int GetCloseCount() { return close_count; }

    Status NewLogger(const std::string& /*fname*/,
                     std::shared_ptr<Logger>* result) override {
      result->reset(new TestLogger(this));
      return Status::OK();
    }

   private:
    int close_count;
};

TEST_F(DBBasicTest, DBClose) {
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("db_close_test");
  ASSERT_OK(DestroyDB(dbname, options));

  DB* db = nullptr;
  TestEnv* env = new TestEnv();
  options.create_if_missing = true;
  options.env = env;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  s = db->Close();
  ASSERT_EQ(env->GetCloseCount(), 1);
  ASSERT_EQ(s, Status::IOError());

  delete db;
  ASSERT_EQ(env->GetCloseCount(), 1);

  // Do not call DB::Close() and ensure our logger Close() still gets called
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  delete db;
  ASSERT_EQ(env->GetCloseCount(), 2);

  // Provide our own logger and ensure DB::Close() does not close it
  options.info_log.reset(new TestEnv::TestLogger(env));
  options.create_if_missing = false;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  s = db->Close();
  ASSERT_EQ(s, Status::OK());
  delete db;
  ASSERT_EQ(env->GetCloseCount(), 2);
  options.info_log.reset();
  ASSERT_EQ(env->GetCloseCount(), 3);

  delete options.env;
}

TEST_F(DBBasicTest, DBCloseFlushError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(Env::Default()));
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.manual_wal_flush = true;
  options.write_buffer_size=100;
  options.env = fault_injection_env.get();

  Reopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key3", "value3"));
  fault_injection_env->SetFilesystemActive(false);
  Status s = dbfull()->Close();
  fault_injection_env->SetFilesystemActive(true);
  ASSERT_NE(s, Status::OK());

  Destroy(options);
}

TEST_F(DBBasicTest, MultiGetMultiCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  rocksdb::DBImpl* db = reinterpret_cast<DBImpl*>(db_);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::AfterRefSV", [&](void* /*arg*/) {
        if (++get_sv_count == 2) {
          // After MultiGet refs a couple of CFs, flush all CFs so MultiGet
          // is forced to repeat the process
          for (int i = 0; i < 8; ++i) {
            ASSERT_OK(Flush(i));
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val2"));
          }
        }
        if (get_sv_count == 11) {
          for (int i = 0; i < 8; ++i) {
            auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                            db->GetColumnFamilyHandle(i))
                            ->cfd();
            ASSERT_EQ(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
          }
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < 8; ++i) {
    cfs.push_back(i);
    keys.push_back("cf" + std::to_string(i) + "_key");
  }

  values = MultiGet(cfs, keys, nullptr);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j], "cf" + std::to_string(j) + "_val2");
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                    reinterpret_cast<DBImpl*>(db_)->GetColumnFamilyHandle(i))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
  }
}

TEST_F(DBBasicTest, MultiGetMultiCFMutex) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  int retries = 0;
  bool last_try = false;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::LastTry", [&](void* /*arg*/) {
        last_try = true;
        rocksdb::SyncPoint::GetInstance()->DisableProcessing();
      });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::AfterRefSV", [&](void* /*arg*/) {
        if (last_try) {
          return;
        }
        if (++get_sv_count == 2) {
          ++retries;
          get_sv_count = 0;
          for (int i = 0; i < 8; ++i) {
            ASSERT_OK(Flush(i));
            ASSERT_OK(Put(
                i, "cf" + std::to_string(i) + "_key",
                "cf" + std::to_string(i) + "_val" + std::to_string(retries)));
          }
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < 8; ++i) {
    cfs.push_back(i);
    keys.push_back("cf" + std::to_string(i) + "_key");
  }

  values = MultiGet(cfs, keys, nullptr);
  ASSERT_TRUE(last_try);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j],
              "cf" + std::to_string(j) + "_val" + std::to_string(retries));
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                    reinterpret_cast<DBImpl*>(db_)->GetColumnFamilyHandle(i))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_F(DBBasicTest, MultiGetMultiCFSnapshot) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  rocksdb::DBImpl* db = reinterpret_cast<DBImpl*>(db_);
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiGet::AfterRefSV", [&](void* /*arg*/) {
        if (++get_sv_count == 2) {
          for (int i = 0; i < 8; ++i) {
            ASSERT_OK(Flush(i));
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val2"));
          }
        }
        if (get_sv_count == 8) {
          for (int i = 0; i < 8; ++i) {
            auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                            db->GetColumnFamilyHandle(i))
                            ->cfd();
            ASSERT_TRUE(
                (cfd->TEST_GetLocalSV()->Get() == SuperVersion::kSVInUse) ||
                (cfd->TEST_GetLocalSV()->Get() == SuperVersion::kSVObsolete));
          }
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < 8; ++i) {
    cfs.push_back(i);
    keys.push_back("cf" + std::to_string(i) + "_key");
  }

  const Snapshot* snapshot = db_->GetSnapshot();
  values = MultiGet(cfs, keys, snapshot);
  db_->ReleaseSnapshot(snapshot);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j], "cf" + std::to_string(j) + "_val");
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(
                    reinterpret_cast<DBImpl*>(db_)->GetColumnFamilyHandle(i))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_F(DBBasicTest, MultiGetBatchedSimpleUnsorted) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"no_key", "k5", "k4", "k3", "k2", "k1"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    db_->MultiGet(ReadOptions(), handles_[1], keys.size(), keys.data(),
                  values.data(), s.data(), false);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[5].data(), values[5].size()), "v1");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v2");
    ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v3");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_TRUE(s[0].IsNotFound());
    ASSERT_OK(s[1]);
    ASSERT_TRUE(s[2].IsNotFound());
    ASSERT_OK(s[3]);
    ASSERT_OK(s[4]);
    ASSERT_OK(s[5]);

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, MultiGetBatchedSimpleSorted) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    db_->MultiGet(ReadOptions(), handles_[1], keys.size(), keys.data(),
                  values.data(), s.data(), true);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[0].data(), values[0].size()), "v1");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v2");
    ASSERT_EQ(std::string(values[2].data(), values[2].size()), "v3");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, MultiGetBatchedMultiLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 128; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 128; i += 3) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 128; i += 5) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      Flush();
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    Flush();
    num_keys = 0;
  }
  ASSERT_EQ(0, num_keys);

  for (int i = 0; i < 128; i += 9) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_mem_" + std::to_string(i)));
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 64; i < 80; ++i) {
    keys.push_back("key_" + std::to_string(i));
  }

  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values.size(), 16);
  for (unsigned int j = 0; j < values.size(); ++j) {
    int key = j + 64;
    if (key % 9 == 0) {
      ASSERT_EQ(values[j], "val_mem_" + std::to_string(key));
    } else if (key % 5 == 0) {
      ASSERT_EQ(values[j], "val_l0_" + std::to_string(key));
    } else if (key % 3 == 0) {
      ASSERT_EQ(values[j], "val_l1_" + std::to_string(key));
    } else {
      ASSERT_EQ(values[j], "val_l2_" + std::to_string(key));
    }
  }
}

#ifndef ROCKSDB_LITE
TEST_F(DBBasicTest, GetAllKeyVersions) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  const size_t kNumInserts = 4;
  const size_t kNumDeletes = 4;
  const size_t kNumUpdates = 4;

  // Check default column family
  for (size_t i = 0; i != kNumInserts; ++i) {
    ASSERT_OK(Put(std::to_string(i), "value"));
  }
  for (size_t i = 0; i != kNumUpdates; ++i) {
    ASSERT_OK(Put(std::to_string(i), "value1"));
  }
  for (size_t i = 0; i != kNumDeletes; ++i) {
    ASSERT_OK(Delete(std::to_string(i)));
  }
  std::vector<KeyVersion> key_versions;
  ASSERT_OK(rocksdb::GetAllKeyVersions(db_, Slice(), Slice(),
                                       std::numeric_limits<size_t>::max(),
                                       &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());
  ASSERT_OK(rocksdb::GetAllKeyVersions(db_, handles_[0], Slice(), Slice(),
                                       std::numeric_limits<size_t>::max(),
                                       &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());

  // Check non-default column family
  for (size_t i = 0; i != kNumInserts - 1; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value"));
  }
  for (size_t i = 0; i != kNumUpdates - 1; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value1"));
  }
  for (size_t i = 0; i != kNumDeletes - 1; ++i) {
    ASSERT_OK(Delete(1, std::to_string(i)));
  }
  ASSERT_OK(rocksdb::GetAllKeyVersions(db_, handles_[1], Slice(), Slice(),
                                       std::numeric_limits<size_t>::max(),
                                       &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates - 3, key_versions.size());
}
#endif  // !ROCKSDB_LITE

class DBBasicTestWithParallelIO
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool,bool,bool,bool>> {
 public:
  DBBasicTestWithParallelIO()
      : DBTestBase("/db_basic_test_with_parallel_io") {
    bool compressed_cache = std::get<0>(GetParam());
    bool uncompressed_cache = std::get<1>(GetParam());
    compression_enabled_ = std::get<2>(GetParam());
    fill_cache_ = std::get<3>(GetParam());

    if (compressed_cache) {
      std::shared_ptr<Cache> cache = NewLRUCache(1048576);
      compressed_cache_ = std::make_shared<MyBlockCache>(cache);
    }
    if (uncompressed_cache) {
      std::shared_ptr<Cache> cache = NewLRUCache(1048576);
      uncompressed_cache_ = std::make_shared<MyBlockCache>(cache);
    }

    env_->count_random_reads_ = true;

    Options options = CurrentOptions();
    Random rnd(301);
    BlockBasedTableOptions table_options;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.block_cache = uncompressed_cache_;
    table_options.block_cache_compressed = compressed_cache_;
    table_options.flush_block_policy_factory.reset(
                      new MyFlushBlockPolicyFactory());
    options.table_factory.reset(new BlockBasedTableFactory(table_options));
    if (!compression_enabled_) {
      options.compression = kNoCompression;
    }
    Reopen(options);

    std::string zero_str(128, '\0');
    for (int i = 0; i < 100; ++i) {
      // Make the value compressible. A purely random string doesn't compress
      // and the resultant data block will not be compressed
      values_.emplace_back(RandomString(&rnd, 128) + zero_str);
      assert(Put(Key(i), values_[i]) == Status::OK());
    }
    Flush();
  }

  bool CheckValue(int i, const std::string& value) {
    if (values_[i].compare(value) == 0) {
      return true;
    }
    return false;
  }

  int num_lookups() { return uncompressed_cache_->num_lookups(); }
  int num_found() { return uncompressed_cache_->num_found(); }
  int num_inserts() { return uncompressed_cache_->num_inserts(); }

  int num_lookups_compressed() {
    return compressed_cache_->num_lookups();
  }
  int num_found_compressed() {
    return compressed_cache_->num_found();
  }
  int num_inserts_compressed() {
    return compressed_cache_->num_inserts();
  }

  bool fill_cache() { return fill_cache_; }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 private:
  class MyFlushBlockPolicyFactory
    : public FlushBlockPolicyFactory {
   public:
    MyFlushBlockPolicyFactory() {}

    virtual const char* Name() const override {
      return "MyFlushBlockPolicyFactory";
    }

    virtual FlushBlockPolicy* NewFlushBlockPolicy(
        const BlockBasedTableOptions& /*table_options*/,
        const BlockBuilder& data_block_builder) const override {
      return new MyFlushBlockPolicy(data_block_builder);
    }
  };

  class MyFlushBlockPolicy
    : public FlushBlockPolicy {
   public:
    explicit MyFlushBlockPolicy(const BlockBuilder& data_block_builder)
      : num_keys_(0), data_block_builder_(data_block_builder) {}

    bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
      if (data_block_builder_.empty()) {
        // First key in this block
        num_keys_ = 1;
        return false;
      }
      // Flush every 10 keys
      if (num_keys_ == 10) {
        num_keys_ = 1;
        return true;
      }
      num_keys_++;
      return false;
    }

   private:
    int num_keys_;
    const BlockBuilder& data_block_builder_;
  };

  class MyBlockCache
    : public Cache {
   public:
    explicit MyBlockCache(std::shared_ptr<Cache>& target)
      : target_(target), num_lookups_(0), num_found_(0), num_inserts_(0) {}

    virtual const char* Name() const override { return "MyBlockCache"; }

    virtual Status Insert(const Slice& key, void* value, size_t charge,
                          void (*deleter)(const Slice& key, void* value),
                          Handle** handle = nullptr,
                          Priority priority = Priority::LOW) override {
      num_inserts_++;
      return target_->Insert(key, value, charge, deleter, handle, priority);
    }

    virtual Handle* Lookup(const Slice& key,
                           Statistics* stats = nullptr) override {
      num_lookups_++;
      Handle* handle = target_->Lookup(key, stats);
      if (handle != nullptr) {
        num_found_++;
      }
      return handle;
    }

    virtual bool Ref(Handle* handle) override {
      return target_->Ref(handle);
    }

    virtual bool Release(Handle* handle, bool force_erase = false) override {
      return target_->Release(handle, force_erase);
    }

    virtual void* Value(Handle* handle) override {
      return target_->Value(handle);
    }

    virtual void Erase(const Slice& key) override {
      target_->Erase(key);
    }
    virtual uint64_t NewId() override {
      return target_->NewId();
    }

    virtual void SetCapacity(size_t capacity) override {
      target_->SetCapacity(capacity);
    }

    virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override {
      target_->SetStrictCapacityLimit(strict_capacity_limit);
    }

    virtual bool HasStrictCapacityLimit() const override {
      return target_->HasStrictCapacityLimit();
    }

    virtual size_t GetCapacity() const override {
      return target_->GetCapacity();
    }

    virtual size_t GetUsage() const override {
      return target_->GetUsage();
    }

    virtual size_t GetUsage(Handle* handle) const override {
      return target_->GetUsage(handle);
    }

    virtual size_t GetPinnedUsage() const override {
      return target_->GetPinnedUsage();
    }

    virtual size_t GetCharge(Handle* /*handle*/) const override { return 0; }

    virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                        bool thread_safe) override {
      return target_->ApplyToAllCacheEntries(callback, thread_safe);
    }

    virtual void EraseUnRefEntries() override {
      return target_->EraseUnRefEntries();
    }

    int num_lookups() { return num_lookups_; }

    int num_found() { return num_found_; }

    int num_inserts() { return num_inserts_; }
   private:
    std::shared_ptr<Cache> target_;
    int num_lookups_;
    int num_found_;
    int num_inserts_;
  };

  std::shared_ptr<MyBlockCache> compressed_cache_;
  std::shared_ptr<MyBlockCache> uncompressed_cache_;
  bool compression_enabled_;
  std::vector<std::string> values_;
  bool fill_cache_;
};

TEST_P(DBBasicTestWithParallelIO, MultiGet) {
  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(Slice(key_data.back()));
  key_data.emplace_back(Key(50));
  keys.emplace_back(Slice(key_data.back()));
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
           keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(0, values[0].ToString()));
  ASSERT_TRUE(CheckValue(50, values[1].ToString()));

  int random_reads = env_->random_read_counter_.Read();
  key_data[0] = Key(1);
  key_data[1] = Key(51);
  keys[0] = Slice(key_data[0]);
  keys[1] = Slice(key_data[1]);
  values[0].Reset();
  values[1].Reset();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
           keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(1, values[0].ToString()));
  ASSERT_TRUE(CheckValue(51, values[1].ToString()));

  int expected_reads = random_reads + (fill_cache() ? 0 : 2);
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);

  keys.resize(10);
  statuses.resize(10);
  std::vector<int> key_ints{1,2,15,16,55,81,82,83,84,85};
  for (size_t i = 0; i < key_ints.size(); ++i) {
    key_data[i] = Key(key_ints[i]);
    keys[i] = Slice(key_data[i]);
    statuses[i] = Status::OK();
    values[i].Reset();
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
           keys.data(), values.data(), statuses.data(), true);
  for (size_t i = 0; i < key_ints.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_TRUE(CheckValue(key_ints[i], values[i].ToString()));
  }
  expected_reads += (fill_cache() ? 2 : 4);
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);
}

INSTANTIATE_TEST_CASE_P(
    ParallelIO, DBBasicTestWithParallelIO,
    // Params are as follows -
    // Param 0 - Compressed cache enabled
    // Param 1 - Uncompressed cache enabled
    // Param 2 - Data compression enabled
    // Param 3 - ReadOptions::fill_cache
    ::testing::Values(std::make_tuple(false, true, true, true),
                      std::make_tuple(true, true, true, true),
                      std::make_tuple(false, true, false, true),
                      std::make_tuple(false, true, true, false),
                      std::make_tuple(true, true, true, false),
                      std::make_tuple(false, true, false, false)));

class DBBasicTestWithTimestampWithParam
    : public DBTestBase,
      public testing::WithParamInterface<bool> {
 public:
  DBBasicTestWithTimestampWithParam()
      : DBTestBase("/db_basic_test_with_timestamp") {}

 protected:
  class TestComparator : public Comparator {
   private:
    const Comparator* cmp_without_ts_;

   public:
    explicit TestComparator(size_t ts_sz)
        : Comparator(ts_sz), cmp_without_ts_(nullptr) {
      cmp_without_ts_ = BytewiseComparator();
    }

    const char* Name() const override { return "TestComparator"; }

    void FindShortSuccessor(std::string*) const override {}

    void FindShortestSeparator(std::string*, const Slice&) const override {}

    int Compare(const Slice& a, const Slice& b) const override {
      int r = CompareWithoutTimestamp(a, b);
      if (r != 0 || 0 == timestamp_size()) {
        return r;
      }
      return CompareTimestamp(
          Slice(a.data() + a.size() - timestamp_size(), timestamp_size()),
          Slice(b.data() + b.size() - timestamp_size(), timestamp_size()));
    }

    int CompareWithoutTimestamp(const Slice& a, const Slice& b) const override {
      assert(a.size() >= timestamp_size());
      assert(b.size() >= timestamp_size());
      Slice k1 = StripTimestampFromUserKey(a, timestamp_size());
      Slice k2 = StripTimestampFromUserKey(b, timestamp_size());

      return cmp_without_ts_->Compare(k1, k2);
    }

    int CompareTimestamp(const Slice& ts1, const Slice& ts2) const override {
      if (!ts1.data() && !ts2.data()) {
        return 0;
      } else if (ts1.data() && !ts2.data()) {
        return 1;
      } else if (!ts1.data() && ts2.data()) {
        return -1;
      }
      assert(ts1.size() == ts2.size());
      uint64_t low1 = 0;
      uint64_t low2 = 0;
      uint64_t high1 = 0;
      uint64_t high2 = 0;
      auto* ptr1 = const_cast<Slice*>(&ts1);
      auto* ptr2 = const_cast<Slice*>(&ts2);
      if (!GetFixed64(ptr1, &low1) || !GetFixed64(ptr1, &high1) ||
          !GetFixed64(ptr2, &low2) || !GetFixed64(ptr2, &high2)) {
        assert(false);
      }
      if (high1 < high2) {
        return 1;
      } else if (high1 > high2) {
        return -1;
      }
      if (low1 < low2) {
        return 1;
      } else if (low1 > low2) {
        return -1;
      }
      return 0;
    }
  };

  Slice EncodeTimestamp(uint64_t low, uint64_t high, std::string* ts) {
    assert(nullptr != ts);
    ts->clear();
    PutFixed64(ts, low);
    PutFixed64(ts, high);
    assert(ts->size() == sizeof(low) + sizeof(high));
    return Slice(*ts);
  }
};

TEST_P(DBBasicTestWithTimestampWithParam, PutAndGet) {
  const int kNumKeysPerFile = 8192;
  const size_t kNumTimestamps = 6;
  bool memtable_only = GetParam();
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.memtable_factory.reset(new SpecialSkipListFactory(kNumKeysPerFile));
  std::string tmp;
  size_t ts_sz = EncodeTimestamp(0, 0, &tmp).size();
  TestComparator test_cmp(ts_sz);
  options.comparator = &test_cmp;
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(
      10 /*bits_per_key*/, false /*use_block_based_builder*/));
  bbto.whole_key_filtering = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  std::vector<std::string> write_ts_strs(kNumTimestamps);
  std::vector<std::string> read_ts_strs(kNumTimestamps);
  std::vector<Slice> write_ts_list;
  std::vector<Slice> read_ts_list;

  for (size_t i = 0; i != kNumTimestamps; ++i) {
    write_ts_list.emplace_back(EncodeTimestamp(i * 2, 0, &write_ts_strs[i]));
    read_ts_list.emplace_back(EncodeTimestamp(1 + i * 2, 0, &read_ts_strs[i]));
    const Slice& write_ts = write_ts_list.back();
    WriteOptions wopts;
    wopts.timestamp = &write_ts;
    for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
      for (size_t j = 0; j != (kNumKeysPerFile - 1) / kNumTimestamps; ++j) {
        ASSERT_OK(Put(cf, "key" + std::to_string(j),
                      "value_" + std::to_string(j) + "_" + std::to_string(i),
                      wopts));
      }
      if (!memtable_only) {
        ASSERT_OK(Flush(cf));
      }
    }
  }
  const auto& verify_db_func = [&]() {
    for (size_t i = 0; i != kNumTimestamps; ++i) {
      ReadOptions ropts;
      ropts.timestamp = &read_ts_list[i];
      for (int cf = 0; cf != static_cast<int>(num_cfs); ++cf) {
        ColumnFamilyHandle* cfh = handles_[cf];
        for (size_t j = 0; j != (kNumKeysPerFile - 1) / kNumTimestamps; ++j) {
          std::string value;
          ASSERT_OK(db_->Get(ropts, cfh, "key" + std::to_string(j), &value));
          ASSERT_EQ("value_" + std::to_string(j) + "_" + std::to_string(i),
                    value);
        }
      }
    }
  };
  verify_db_func();
}

INSTANTIATE_TEST_CASE_P(Timestamp, DBBasicTestWithTimestampWithParam,
                        ::testing::Bool());

}  // namespace rocksdb

#ifdef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
extern "C" {
void RegisterCustomObjects(int argc, char** argv);
}
#else
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif  // !ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
