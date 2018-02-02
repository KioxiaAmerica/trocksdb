//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_test.h"

#include <inttypes.h>
#include <algorithm>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/mock_table.h"
#include "util/fault_injection_test_env.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/transaction_test_util.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

#include "port/port.h"

using std::string;

namespace rocksdb {

using CommitEntry = WritePreparedTxnDB::CommitEntry;
using CommitEntry64b = WritePreparedTxnDB::CommitEntry64b;
using CommitEntry64bFormat = WritePreparedTxnDB::CommitEntry64bFormat;

TEST(PreparedHeap, BasicsTest) {
  WritePreparedTxnDB::PreparedHeap heap;
  heap.push(14l);
  // Test with one element
  ASSERT_EQ(14l, heap.top());
  heap.push(24l);
  heap.push(34l);
  // Test that old min is still on top
  ASSERT_EQ(14l, heap.top());
  heap.push(13l);
  // Test that the new min will be on top
  ASSERT_EQ(13l, heap.top());
  // Test that it is persistent
  ASSERT_EQ(13l, heap.top());
  heap.push(44l);
  heap.push(54l);
  heap.push(64l);
  heap.push(74l);
  heap.push(84l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(24l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(14l);
  // Test that old min is still on top
  ASSERT_EQ(13l, heap.top());
  heap.erase(13l);
  // Test that the new comes to the top after multiple erase
  ASSERT_EQ(34l, heap.top());
  heap.erase(34l);
  // Test that the new comes to the top after single erase
  ASSERT_EQ(44l, heap.top());
  heap.erase(54l);
  ASSERT_EQ(44l, heap.top());
  heap.pop();  // pop 44l
  // Test that the erased items are ignored after pop
  ASSERT_EQ(64l, heap.top());
  heap.erase(44l);
  // Test that erasing an already popped item would work
  ASSERT_EQ(64l, heap.top());
  heap.erase(84l);
  ASSERT_EQ(64l, heap.top());
  heap.push(85l);
  heap.push(86l);
  heap.push(87l);
  heap.push(88l);
  heap.push(89l);
  heap.erase(87l);
  heap.erase(85l);
  heap.erase(89l);
  heap.erase(86l);
  heap.erase(88l);
  // Test top remians the same after a ranodm order of many erases
  ASSERT_EQ(64l, heap.top());
  heap.pop();
  // Test that pop works with a series of random pending erases
  ASSERT_EQ(74l, heap.top());
  ASSERT_FALSE(heap.empty());
  heap.pop();
  // Test that empty works
  ASSERT_TRUE(heap.empty());
}

TEST(CommitEntry64b, BasicTest) {
  const size_t INDEX_BITS = static_cast<size_t>(21);
  const size_t INDEX_SIZE = static_cast<size_t>(1ull << INDEX_BITS);
  const CommitEntry64bFormat FORMAT(static_cast<size_t>(INDEX_BITS));

  // zero-initialized CommitEntry64b should inidcate an empty entry
  CommitEntry64b empty_entry64b;
  uint64_t empty_index = 11ul;
  CommitEntry empty_entry;
  bool ok = empty_entry64b.Parse(empty_index, &empty_entry, FORMAT);
  ASSERT_FALSE(ok);

  // the zero entry is reserved for un-initialized entries
  const size_t MAX_COMMIT = (1 << FORMAT.COMMIT_BITS) - 1 - 1;
  // Samples over the numbers that are covered by that many index bits
  std::array<uint64_t, 4> is = {{0, 1, INDEX_SIZE / 2 + 1, INDEX_SIZE - 1}};
  // Samples over the numbers that are covered by that many commit bits
  std::array<uint64_t, 4> ds = {{0, 1, MAX_COMMIT / 2 + 1, MAX_COMMIT}};
  // Iterate over prepare numbers that have i) cover all bits of a sequence
  // number, and ii) include some bits that fall into the range of index or
  // commit bits
  for (uint64_t base = 1; base < kMaxSequenceNumber; base *= 2) {
    for (uint64_t i : is) {
      for (uint64_t d : ds) {
        uint64_t p = base + i + d;
        for (uint64_t c : {p, p + d / 2, p + d}) {
          uint64_t index = p % INDEX_SIZE;
          CommitEntry before(p, c), after;
          CommitEntry64b entry64b(before, FORMAT);
          ok = entry64b.Parse(index, &after, FORMAT);
          ASSERT_TRUE(ok);
          if (!(before == after)) {
            printf("base %" PRIu64 " i %" PRIu64 " d %" PRIu64 " p %" PRIu64
                   " c %" PRIu64 " index %" PRIu64 "\n",
                   base, i, d, p, c, index);
          }
          ASSERT_EQ(before, after);
        }
      }
    }
  }
}

class WritePreparedTxnDBMock : public WritePreparedTxnDB {
 public:
  WritePreparedTxnDBMock(DBImpl* db_impl, TransactionDBOptions& opt)
      : WritePreparedTxnDB(db_impl, opt) {}
  WritePreparedTxnDBMock(DBImpl* db_impl, TransactionDBOptions& opt,
                         size_t snapshot_cache_size)
      : WritePreparedTxnDB(db_impl, opt, snapshot_cache_size) {}
  WritePreparedTxnDBMock(DBImpl* db_impl, TransactionDBOptions& opt,
                         size_t snapshot_cache_size, size_t commit_cache_size)
      : WritePreparedTxnDB(db_impl, opt, snapshot_cache_size,
                           commit_cache_size) {}
  void SetDBSnapshots(const std::vector<SequenceNumber>& snapshots) {
    snapshots_ = snapshots;
  }
  void TakeSnapshot(SequenceNumber seq) { snapshots_.push_back(seq); }

 protected:
  virtual const std::vector<SequenceNumber> GetSnapshotListFromDB(
      SequenceNumber /* unused */) override {
    return snapshots_;
  }

 private:
  std::vector<SequenceNumber> snapshots_;
};

class WritePreparedTransactionTest : public TransactionTest {
 protected:
  // If expect_update is set, check if it actually updated old_commit_map_. If
  // it did not and yet suggested not to check the next snapshot, do the
  // opposite to check if it was not a bad suggstion.
  void MaybeUpdateOldCommitMapTestWithNext(uint64_t prepare, uint64_t commit,
                                           uint64_t snapshot,
                                           uint64_t next_snapshot,
                                           bool expect_update) {
    WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
    // reset old_commit_map_empty_ so that its value indicate whether
    // old_commit_map_ was updated
    wp_db->old_commit_map_empty_ = true;
    bool check_next = wp_db->MaybeUpdateOldCommitMap(prepare, commit, snapshot,
                                                     snapshot < next_snapshot);
    if (expect_update == wp_db->old_commit_map_empty_) {
      printf("prepare: %" PRIu64 " commit: %" PRIu64 " snapshot: %" PRIu64
             " next: %" PRIu64 "\n",
             prepare, commit, snapshot, next_snapshot);
    }
    EXPECT_EQ(!expect_update, wp_db->old_commit_map_empty_);
    if (!check_next && wp_db->old_commit_map_empty_) {
      // do the oppotisite to make sure it was not a bad suggestion
      const bool dont_care_bool = true;
      wp_db->MaybeUpdateOldCommitMap(prepare, commit, next_snapshot,
                                     dont_care_bool);
      if (!wp_db->old_commit_map_empty_) {
        printf("prepare: %" PRIu64 " commit: %" PRIu64 " snapshot: %" PRIu64
               " next: %" PRIu64 "\n",
               prepare, commit, snapshot, next_snapshot);
      }
      EXPECT_TRUE(wp_db->old_commit_map_empty_);
    }
  }

  // Test that a CheckAgainstSnapshots thread reading old_snapshots will not
  // miss a snapshot because of a concurrent update by UpdateSnapshots that is
  // writing new_snapshots. Both threads are broken at two points. The sync
  // points to enforce them are specified by a1, a2, b1, and b2. CommitEntry
  // entry is expected to be vital for one of the snapshots that is common
  // between the old and new list of snapshots.
  void SnapshotConcurrentAccessTestInternal(
      WritePreparedTxnDB* wp_db,
      const std::vector<SequenceNumber>& old_snapshots,
      const std::vector<SequenceNumber>& new_snapshots, CommitEntry& entry,
      SequenceNumber& version, size_t a1, size_t a2, size_t b1, size_t b2) {
    // First reset the snapshot list
    const std::vector<SequenceNumber> empty_snapshots;
    wp_db->old_commit_map_empty_ = true;
    wp_db->UpdateSnapshots(empty_snapshots, ++version);
    // Then initialize it with the old_snapshots
    wp_db->UpdateSnapshots(old_snapshots, ++version);

    // Starting from the first thread, cut each thread at two points
    rocksdb::SyncPoint::GetInstance()->LoadDependency({
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(a1),
         "WritePreparedTxnDB::UpdateSnapshots:s:start"},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(b1),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(a1)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(a2),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(b1)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(b2),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(a2)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:end",
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(b2)},
    });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    {
      ASSERT_TRUE(wp_db->old_commit_map_empty_);
      rocksdb::port::Thread t1(
          [&]() { wp_db->UpdateSnapshots(new_snapshots, version); });
      rocksdb::port::Thread t2([&]() { wp_db->CheckAgainstSnapshots(entry); });
      t1.join();
      t2.join();
      ASSERT_FALSE(wp_db->old_commit_map_empty_);
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();

    wp_db->old_commit_map_empty_ = true;
    wp_db->UpdateSnapshots(empty_snapshots, ++version);
    wp_db->UpdateSnapshots(old_snapshots, ++version);
    // Starting from the second thread, cut each thread at two points
    rocksdb::SyncPoint::GetInstance()->LoadDependency({
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(a1),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:start"},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(b1),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(a1)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:" + std::to_string(a2),
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(b1)},
        {"WritePreparedTxnDB::CheckAgainstSnapshots:p:" + std::to_string(b2),
         "WritePreparedTxnDB::UpdateSnapshots:s:" + std::to_string(a2)},
        {"WritePreparedTxnDB::UpdateSnapshots:p:end",
         "WritePreparedTxnDB::CheckAgainstSnapshots:s:" + std::to_string(b2)},
    });
    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    {
      ASSERT_TRUE(wp_db->old_commit_map_empty_);
      rocksdb::port::Thread t1(
          [&]() { wp_db->UpdateSnapshots(new_snapshots, version); });
      rocksdb::port::Thread t2([&]() { wp_db->CheckAgainstSnapshots(entry); });
      t1.join();
      t2.join();
      ASSERT_FALSE(wp_db->old_commit_map_empty_);
    }
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  }

  // Verify value of keys.
  void VerifyKeys(const std::unordered_map<std::string, std::string>& data,
                  const Snapshot* snapshot = nullptr) {
    std::string value;
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    for (auto& kv : data) {
      auto s = db->Get(read_options, kv.first, &value);
      ASSERT_TRUE(s.ok() || s.IsNotFound());
      if (s.ok()) {
        if (kv.second != value) {
          printf("key = %s\n", kv.first.c_str());
        }
        ASSERT_EQ(kv.second, value);
      } else {
        ASSERT_EQ(kv.second, "NOT_FOUND");
      }
    }
  }

  // Verify all versions of keys.
  void VerifyInternalKeys(const std::vector<KeyVersion>& expected_versions) {
    std::vector<KeyVersion> versions;
    ASSERT_OK(GetAllKeyVersions(db, expected_versions.front().user_key,
                                expected_versions.back().user_key, &versions));
    ASSERT_EQ(expected_versions.size(), versions.size());
    for (size_t i = 0; i < versions.size(); i++) {
      ASSERT_EQ(expected_versions[i].user_key, versions[i].user_key);
      ASSERT_EQ(expected_versions[i].sequence, versions[i].sequence);
      ASSERT_EQ(expected_versions[i].type, versions[i].type);
      if (versions[i].type != kTypeDeletion &&
#ifdef INDIRECT_VALUE_SUPPORT
          versions[i].type != kTypeIndirectValue &&  // the value has been translated, don't check it
#endif
          versions[i].type != kTypeSingleDeletion) {
        ASSERT_EQ(expected_versions[i].value, versions[i].value);
      }
      // Range delete not supported.
      assert(expected_versions[i].type != kTypeRangeDeletion);
    }
  }
};

// TODO(myabandeh): enable it for concurrent_prepare
INSTANTIATE_TEST_CASE_P(WritePreparedTransactionTest,
                        WritePreparedTransactionTest,
                        ::testing::Values(std::make_tuple(false, false,
                                                          WRITE_PREPARED)));
TEST_P(WritePreparedTransactionTest, CommitMapTest) {
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  assert(wp_db);
  assert(wp_db->db_impl_);
  size_t size = wp_db->COMMIT_CACHE_SIZE;
  CommitEntry c = {5, 12}, e;
  bool evicted = wp_db->AddCommitEntry(c.prep_seq % size, c, &e);
  ASSERT_FALSE(evicted);

  // Should be able to read the same value
  CommitEntry64b dont_care;
  bool found = wp_db->GetCommitEntry(c.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);
  // Should be able to distinguish between overlapping entries
  found = wp_db->GetCommitEntry((c.prep_seq + size) % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_NE(c.prep_seq + size, e.prep_seq);
  // Should be able to detect non-existent entry
  found = wp_db->GetCommitEntry((c.prep_seq + 1) % size, &dont_care, &e);
  ASSERT_FALSE(found);

  // Reject an invalid exchange
  CommitEntry e2 = {c.prep_seq + size, c.commit_seq + size};
  CommitEntry64b e2_64b(e2, wp_db->FORMAT);
  bool exchanged = wp_db->ExchangeCommitEntry(e2.prep_seq % size, e2_64b, e);
  ASSERT_FALSE(exchanged);
  // check whether it did actually reject that
  found = wp_db->GetCommitEntry(e2.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(c, e);

  // Accept a valid exchange
  CommitEntry64b c_64b(c, wp_db->FORMAT);
  CommitEntry e3 = {c.prep_seq + size, c.commit_seq + size + 1};
  exchanged = wp_db->ExchangeCommitEntry(c.prep_seq % size, c_64b, e3);
  ASSERT_TRUE(exchanged);
  // check whether it did actually accepted that
  found = wp_db->GetCommitEntry(c.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(e3, e);

  // Rewrite an entry
  CommitEntry e4 = {e3.prep_seq + size, e3.commit_seq + size + 1};
  evicted = wp_db->AddCommitEntry(e4.prep_seq % size, e4, &e);
  ASSERT_TRUE(evicted);
  ASSERT_EQ(e3, e);
  found = wp_db->GetCommitEntry(e4.prep_seq % size, &dont_care, &e);
  ASSERT_TRUE(found);
  ASSERT_EQ(e4, e);
}

TEST_P(WritePreparedTransactionTest, MaybeUpdateOldCommitMap) {
  // If prepare <= snapshot < commit we should keep the entry around since its
  // nonexistence could be interpreted as committed in the snapshot while it is
  // not true. We keep such entries around by adding them to the
  // old_commit_map_.
  uint64_t p /*prepare*/, c /*commit*/, s /*snapshot*/, ns /*next_snapshot*/;
  p = 10l, c = 15l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  // If we do not expect the old commit map to be updated, try also with a next
  // snapshot that is expected to update the old commit map. This would test
  // that MaybeUpdateOldCommitMap would not prevent us from checking the next
  // snapshot that must be checked.
  p = 10l, c = 15l, s = 20l, ns = 11l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 10l, c = 20l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 10l, c = 20l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 20l, c = 20l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 20l, c = 20l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);

  p = 10l, c = 25l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, true);

  p = 20l, c = 25l, s = 20l, ns = 21l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, true);

  p = 21l, c = 25l, s = 20l, ns = 22l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
  p = 21l, c = 25l, s = 20l, ns = 19l;
  MaybeUpdateOldCommitMapTestWithNext(p, c, s, ns, false);
}

TEST_P(WritePreparedTransactionTest, CheckAgainstSnapshotsTest) {
  std::vector<SequenceNumber> snapshots = {100l, 200l, 300l, 400l, 500l,
                                           600l, 700l, 800l, 900l};
  const size_t snapshot_cache_bits = 2;
  // Safety check to express the intended size in the test. Can be adjusted if
  // the snapshots lists changed.
  assert((1ul << snapshot_cache_bits) * 2 + 1 == snapshots.size());
  DBImpl* mock_db = new DBImpl(options, dbname);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options, snapshot_cache_bits));
  SequenceNumber version = 1000l;
  ASSERT_EQ(0, wp_db->snapshots_total_);
  wp_db->UpdateSnapshots(snapshots, version);
  ASSERT_EQ(snapshots.size(), wp_db->snapshots_total_);
  // seq numbers are chosen so that we have two of them between each two
  // snapshots. If the diff of two consecuitive seq is more than 5, there is a
  // snapshot between them.
  std::vector<SequenceNumber> seqs = {50l,  55l,  150l, 155l, 250l, 255l, 350l,
                                      355l, 450l, 455l, 550l, 555l, 650l, 655l,
                                      750l, 755l, 850l, 855l, 950l, 955l};
  assert(seqs.size() > 1);
  for (size_t i = 0; i < seqs.size() - 1; i++) {
    wp_db->old_commit_map_empty_ = true;  // reset
    CommitEntry commit_entry = {seqs[i], seqs[i + 1]};
    wp_db->CheckAgainstSnapshots(commit_entry);
    // Expect update if there is snapshot in between the prepare and commit
    bool expect_update = commit_entry.commit_seq - commit_entry.prep_seq > 5 &&
                         commit_entry.commit_seq >= snapshots.front() &&
                         commit_entry.prep_seq <= snapshots.back();
    ASSERT_EQ(expect_update, !wp_db->old_commit_map_empty_);
  }
}

// Return true if the ith bit is set in combination represented by comb
bool IsInCombination(size_t i, size_t comb) { return comb & (size_t(1) << i); }

// This test is too slow for travis
#ifndef TRAVIS
// Test that CheckAgainstSnapshots will not miss a live snapshot if it is run in
// parallel with UpdateSnapshots.
TEST_P(WritePreparedTransactionTest, SnapshotConcurrentAccessTest) {
  // We have a sync point in the method under test after checking each snapshot.
  // If you increase the max number of snapshots in this test, more sync points
  // in the methods must also be added.
  const std::vector<SequenceNumber> snapshots = {10l, 20l, 30l, 40l, 50l,
                                                 60l, 70l, 80l, 90l, 100l};
  const size_t snapshot_cache_bits = 2;
  // Safety check to express the intended size in the test. Can be adjusted if
  // the snapshots lists changed.
  assert((1ul << snapshot_cache_bits) * 2 + 2 == snapshots.size());
  SequenceNumber version = 1000l;
  // Choose the cache size so that the new snapshot list could replace all the
  // existing items in the cache and also have some overflow.
  DBImpl* mock_db = new DBImpl(options, dbname);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options, snapshot_cache_bits));
  // Add up to 2 items that do not fit into the cache
  for (size_t old_size = 1; old_size <= wp_db->SNAPSHOT_CACHE_SIZE + 2;
       old_size++) {
    const std::vector<SequenceNumber> old_snapshots(
        snapshots.begin(), snapshots.begin() + old_size);

    // Each member of old snapshot might or might not appear in the new list. We
    // create a common_snapshots for each combination.
    size_t new_comb_cnt = size_t(1) << old_size;
    for (size_t new_comb = 0; new_comb < new_comb_cnt; new_comb++) {
      printf(".");  // To signal progress
      fflush(stdout);
      std::vector<SequenceNumber> common_snapshots;
      for (size_t i = 0; i < old_snapshots.size(); i++) {
        if (IsInCombination(i, new_comb)) {
          common_snapshots.push_back(old_snapshots[i]);
        }
      }
      // And add some new snapshots to the common list
      for (size_t added_snapshots = 0;
           added_snapshots <= snapshots.size() - old_snapshots.size();
           added_snapshots++) {
        std::vector<SequenceNumber> new_snapshots = common_snapshots;
        for (size_t i = 0; i < added_snapshots; i++) {
          new_snapshots.push_back(snapshots[old_snapshots.size() + i]);
        }
        for (auto it = common_snapshots.begin(); it != common_snapshots.end();
             it++) {
          auto snapshot = *it;
          // Create a commit entry that is around the snapshot and thus should
          // be not be discarded
          CommitEntry entry = {static_cast<uint64_t>(snapshot - 1),
                               snapshot + 1};
          // The critical part is when iterating the snapshot cache. Afterwards,
          // we are operating under the lock
          size_t a_range =
              std::min(old_snapshots.size(), wp_db->SNAPSHOT_CACHE_SIZE) + 1;
          size_t b_range =
              std::min(new_snapshots.size(), wp_db->SNAPSHOT_CACHE_SIZE) + 1;
          // Break each thread at two points
          for (size_t a1 = 1; a1 <= a_range; a1++) {
            for (size_t a2 = a1 + 1; a2 <= a_range; a2++) {
              for (size_t b1 = 1; b1 <= b_range; b1++) {
                for (size_t b2 = b1 + 1; b2 <= b_range; b2++) {
                  SnapshotConcurrentAccessTestInternal(
                      wp_db.get(), old_snapshots, new_snapshots, entry, version,
                      a1, a2, b1, b2);
                }
              }
            }
          }
        }
      }
    }
  }
  printf("\n");
}
#endif

// This test clarifies the contract of AdvanceMaxEvictedSeq method
TEST_P(WritePreparedTransactionTest, AdvanceMaxEvictedSeqBasicTest) {
  DBImpl* mock_db = new DBImpl(options, dbname);
  std::unique_ptr<WritePreparedTxnDBMock> wp_db(
      new WritePreparedTxnDBMock(mock_db, txn_db_options));

  // 1. Set the initial values for max, prepared, and snapshots
  SequenceNumber zero_max = 0l;
  // Set the initial list of prepared txns
  const std::vector<SequenceNumber> initial_prepared = {10,  30,  50, 100,
                                                        150, 200, 250};
  for (auto p : initial_prepared) {
    wp_db->AddPrepared(p);
  }
  // This updates the max value and also set old prepared
  SequenceNumber init_max = 100;
  wp_db->AdvanceMaxEvictedSeq(zero_max, init_max);
  const std::vector<SequenceNumber> initial_snapshots = {20, 40};
  wp_db->SetDBSnapshots(initial_snapshots);
  // This will update the internal cache of snapshots from the DB
  wp_db->UpdateSnapshots(initial_snapshots, init_max);

  // 2. Invoke AdvanceMaxEvictedSeq
  const std::vector<SequenceNumber> latest_snapshots = {20, 110, 220, 300};
  wp_db->SetDBSnapshots(latest_snapshots);
  SequenceNumber new_max = 200;
  wp_db->AdvanceMaxEvictedSeq(init_max, new_max);

  // 3. Verify that the state matches with AdvanceMaxEvictedSeq contract
  // a. max should be updated to new_max
  ASSERT_EQ(wp_db->max_evicted_seq_, new_max);
  // b. delayed prepared should contain every txn <= max and prepared should
  // only contian txns > max
  auto it = initial_prepared.begin();
  for (; it != initial_prepared.end() && *it <= new_max; it++) {
    ASSERT_EQ(1, wp_db->delayed_prepared_.erase(*it));
  }
  ASSERT_TRUE(wp_db->delayed_prepared_.empty());
  for (; it != initial_prepared.end() && !wp_db->prepared_txns_.empty();
       it++, wp_db->prepared_txns_.pop()) {
    ASSERT_EQ(*it, wp_db->prepared_txns_.top());
  }
  ASSERT_TRUE(it == initial_prepared.end());
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  // c. snapshots should contain everything below new_max
  auto sit = latest_snapshots.begin();
  for (size_t i = 0; sit != latest_snapshots.end() && *sit <= new_max &&
                     i < wp_db->snapshots_total_;
       sit++, i++) {
    ASSERT_TRUE(i < wp_db->snapshots_total_);
    // This test is in small scale and the list of snapshots are assumed to be
    // within the cache size limit. This is just a safety check to double check
    // that assumption.
    ASSERT_TRUE(i < wp_db->SNAPSHOT_CACHE_SIZE);
    ASSERT_EQ(*sit, wp_db->snapshot_cache_[i]);
  }
}

// TODO(myabandeh): remove this redundant test after transaction_test is enabled
// with WRITE_PREPARED too This test clarifies the existing expectation from the
// sequence number algorithm. It could detect mistakes in updating the code but
// it is not necessarily the one acceptable way. If the algorithm is
// legitimately changed, this unit test should be updated as well.
TEST_P(WritePreparedTransactionTest, SeqAdvanceTest) {
  WriteOptions wopts;
  FlushOptions fopt;

  // Do the test with NUM_BRANCHES branches in it. Each run of a test takes some
  // of the branches. This is the same as counting a binary number where i-th
  // bit represents whether we take branch i in the represented by the number.
  const size_t NUM_BRANCHES = 8;
  // Helper function that shows if the branch is to be taken in the run
  // represented by the number n.
  auto branch_do = [&](size_t n, size_t* branch) {
    assert(*branch < NUM_BRANCHES);
    const size_t filter = static_cast<size_t>(1) << *branch;
    return n & filter;
  };
  const size_t max_n = static_cast<size_t>(1) << NUM_BRANCHES;
  for (size_t n = 0; n < max_n; n++, ReOpen()) {
    DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    size_t branch = 0;
    auto seq = db_impl->GetLatestSequenceNumber();
    exp_seq = seq;
    txn_t0(0);
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      db_impl->Flush(fopt);
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }
    if (branch_do(n, &branch)) {
      db_impl->FlushWAL(true);
      ReOpenNoDelete();
      db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }

    // Doing it twice might detect some bugs
    txn_t0(1);
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    txn_t1(0);
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      db_impl->Flush(fopt);
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }
    if (branch_do(n, &branch)) {
      db_impl->FlushWAL(true);
      ReOpenNoDelete();
      db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }

    txn_t3(0);
    // Since commit marker does not write to memtable, the last seq number is
    // not updated immediately. But the advance should be visible after the next
    // write.

    if (branch_do(n, &branch)) {
      db_impl->Flush(fopt);
    }
    if (branch_do(n, &branch)) {
      db_impl->FlushWAL(true);
      ReOpenNoDelete();
      db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }

    txn_t0(0);
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    txn_t2(0);
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    if (branch_do(n, &branch)) {
      db_impl->Flush(fopt);
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }
    if (branch_do(n, &branch)) {
      db_impl->FlushWAL(true);
      ReOpenNoDelete();
      db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
      seq = db_impl->GetLatestSequenceNumber();
      ASSERT_EQ(exp_seq, seq);
    }
  }
}

TEST_P(WritePreparedTransactionTest, SeqAdvanceConcurrentTest) {
  // Given the sequential run of txns, with this timeout we should never see a
  // deadlock nor a timeout unless we have a key conflict, which should be
  // almost infeasible.
  txn_db_options.transaction_lock_timeout = 1000;
  txn_db_options.default_lock_timeout = 1000;
  ReOpen();
  FlushOptions fopt;

  // Number of different txn types we use in this test
  const size_t type_cnt = 4;
  // The size of the first write group
  // TODO(myabandeh): This should be increase for pre-release tests
  const size_t first_group_size = 2;
  // Total number of txns we run in each test
  const size_t txn_cnt = first_group_size * 2;

  size_t base[txn_cnt + 1] = {
      1,
  };
  for (size_t bi = 1; bi <= txn_cnt; bi++) {
    base[bi] = base[bi - 1] * type_cnt;
  }
  const size_t max_n = static_cast<size_t>(std::pow(type_cnt, txn_cnt));
  printf("Number of cases being tested is %" PRIu64 "\n", max_n);
  for (size_t n = 0; n < max_n; n++, ReOpen()) {
    if (n % 1000 == 0) {
      printf("Tested %" PRIu64 " cases so far\n", n);
    }
    DBImpl* db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    auto seq = db_impl->GetLatestSequenceNumber();
    exp_seq = seq;
    // This is increased before writing the batch for commit
    commit_writes = 0;
    // This is increased before txn starts linking if it expects to do a commit
    // eventually
    expected_commits = 0;
    std::vector<port::Thread> threads;

    linked = 0;
    std::atomic<bool> batch_formed(false);
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "WriteThread::EnterAsBatchGroupLeader:End",
        [&](void* arg) { batch_formed = true; });
    rocksdb::SyncPoint::GetInstance()->SetCallBack(
        "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
          linked++;
          if (linked == 1) {
            // Wait until the others are linked too.
            while (linked < first_group_size) {
            }
          } else if (linked == 1 + first_group_size) {
            // Make the 2nd batch of the rest of writes plus any followup
            // commits from the first batch
            while (linked < txn_cnt + commit_writes) {
            }
          }
          // Then we will have one or more batches consisting of follow-up
          // commits from the 2nd batch. There is a bit of non-determinism here
          // but it should be tolerable.
        });

    rocksdb::SyncPoint::GetInstance()->EnableProcessing();
    for (size_t bi = 0; bi < txn_cnt; bi++) {
      size_t d =
          (n % base[bi + 1]) /
          base[bi];  // get the bi-th digit in number system based on type_cnt
      switch (d) {
        case 0:
          threads.emplace_back(txn_t0, bi);
          break;
        case 1:
          threads.emplace_back(txn_t1, bi);
          break;
        case 2:
          threads.emplace_back(txn_t2, bi);
          break;
        case 3:
          threads.emplace_back(txn_t3, bi);
          break;
        default:
          assert(false);
      }
      // wait to be linked
      while (linked.load() <= bi) {
      }
      if (bi + 1 ==
          first_group_size) {  // after a queue of size first_group_size
        while (!batch_formed) {
        }
        // to make it more deterministic, wait until the commits are linked
        while (linked.load() <= bi + expected_commits) {
        }
      }
    }
    for (auto& t : threads) {
      t.join();
    }
    if (txn_db_options.write_policy == WRITE_PREPARED) {
      // In this case none of the above scheduling tricks to deterministically
      // form merged bactches works because the writes go to saparte queues.
      // This would result in different write groups in each run of the test. We
      // still keep the test since althgouh non-deterministic and hard to debug,
      // it is still useful to have. Since in this case we could finish with
      // commit writes that dont write to memtable, the seq is not advanced in
      // this code path. It will be after the next write. So we do one more
      // write to make the impact of last seq visible.
      txn_t0(0);
    }
    // Check if memtable inserts advanced seq number as expected
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();

    // Check if recovery preserves the last sequence number
    db_impl->FlushWAL(true);
    ReOpenNoDelete();
    db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    // Check if flush preserves the last sequence number
    db_impl->Flush(fopt);
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);

    // Check if recovery after flush preserves the last sequence number
    db_impl->FlushWAL(true);
    ReOpenNoDelete();
    db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
    seq = db_impl->GetLatestSequenceNumber();
    ASSERT_EQ(exp_seq, seq);
  }
}

// Run a couple of differnet txns among them some uncommitted. Restart the db at
// a couple points to check whether the list of uncommitted txns are recovered
// properly.
TEST_P(WritePreparedTransactionTest, BasicRecoveryTest) {
  options.disable_auto_compactions = true;
  ReOpen();
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);

  txn_t0(0);

  TransactionOptions txn_options;
  WriteOptions write_options;
  size_t index = 1000;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  auto istr0 = std::to_string(index);
  auto s = txn0->SetName("xid" + istr0);
  ASSERT_OK(s);
  s = txn0->Put(Slice("foo0" + istr0), Slice("bar0" + istr0));
  ASSERT_OK(s);
  s = txn0->Prepare();
  auto prep_seq_0 = txn0->GetId();

  txn_t1(0);

  index++;
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
  auto istr1 = std::to_string(index);
  s = txn1->SetName("xid" + istr1);
  ASSERT_OK(s);
  s = txn1->Put(Slice("foo1" + istr1), Slice("bar"));
  ASSERT_OK(s);
  s = txn1->Prepare();
  auto prep_seq_1 = txn1->GetId();

  txn_t2(0);

  ReadOptions ropt;
  PinnableSlice pinnable_val;
  // Check the value is not committed before restart
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.IsNotFound());
  pinnable_val.Reset();

  delete txn0;
  delete txn1;
  wp_db->db_impl_->FlushWAL(true);
  ReOpenNoDelete();
  wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  // After recovery, all the uncommitted txns (0 and 1) should be inserted into
  // delayed_prepared_
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  ASSERT_FALSE(wp_db->delayed_prepared_empty_);
  ASSERT_LE(prep_seq_0, wp_db->max_evicted_seq_);
  ASSERT_LE(prep_seq_1, wp_db->max_evicted_seq_);
  {
    ReadLock rl(&wp_db->prepared_mutex_);
    ASSERT_EQ(2, wp_db->delayed_prepared_.size());
    ASSERT_TRUE(wp_db->delayed_prepared_.find(prep_seq_0) !=
                wp_db->delayed_prepared_.end());
    ASSERT_TRUE(wp_db->delayed_prepared_.find(prep_seq_1) !=
                wp_db->delayed_prepared_.end());
  }

  // Check the value is still not committed after restart
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.IsNotFound());
  pinnable_val.Reset();

  txn_t3(0);

  // Test that a recovered txns will be properly marked committed for the next
  // recovery
  txn1 = db->GetTransactionByName("xid" + istr1);
  ASSERT_NE(txn1, nullptr);
  txn1->Commit();
  delete txn1;

  index++;
  Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
  auto istr2 = std::to_string(index);
  s = txn2->SetName("xid" + istr2);
  ASSERT_OK(s);
  s = txn2->Put(Slice("foo2" + istr2), Slice("bar"));
  ASSERT_OK(s);
  s = txn2->Prepare();
  auto prep_seq_2 = txn2->GetId();

  delete txn2;
  wp_db->db_impl_->FlushWAL(true);
  ReOpenNoDelete();
  wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  ASSERT_FALSE(wp_db->delayed_prepared_empty_);

  // 0 and 2 are prepared and 1 is committed
  {
    ReadLock rl(&wp_db->prepared_mutex_);
    ASSERT_EQ(2, wp_db->delayed_prepared_.size());
    const auto& end = wp_db->delayed_prepared_.end();
    ASSERT_NE(wp_db->delayed_prepared_.find(prep_seq_0), end);
    ASSERT_EQ(wp_db->delayed_prepared_.find(prep_seq_1), end);
    ASSERT_NE(wp_db->delayed_prepared_.find(prep_seq_2), end);
  }
  ASSERT_LE(prep_seq_0, wp_db->max_evicted_seq_);
  ASSERT_LE(prep_seq_2, wp_db->max_evicted_seq_);

  // Commit all the remaining txns
  txn0 = db->GetTransactionByName("xid" + istr0);
  ASSERT_NE(txn0, nullptr);
  txn0->Commit();
  txn2 = db->GetTransactionByName("xid" + istr2);
  ASSERT_NE(txn2, nullptr);
  txn2->Commit();

  // Check the value is committed after commit
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(pinnable_val == ("bar0" + istr0));
  pinnable_val.Reset();

  delete txn0;
  delete txn2;
  wp_db->db_impl_->FlushWAL(true);
  ReOpenNoDelete();
  wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  ASSERT_TRUE(wp_db->prepared_txns_.empty());
  ASSERT_TRUE(wp_db->delayed_prepared_empty_);

  // Check the value is still committed after recovery
  s = db->Get(ropt, db->DefaultColumnFamily(), "foo0" + istr0, &pinnable_val);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(pinnable_val == ("bar0" + istr0));
  pinnable_val.Reset();
}

// After recovery the new transactions should still conflict with recovered
// transactions.
TEST_P(WritePreparedTransactionTest, ConflictDetectionAfterRecoveryTest) {
  options.disable_auto_compactions = true;
  ReOpen();

  TransactionOptions txn_options;
  WriteOptions write_options;
  size_t index = 0;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  auto istr0 = std::to_string(index);
  auto s = txn0->SetName("xid" + istr0);
  ASSERT_OK(s);
  s = txn0->Put(Slice("key" + istr0), Slice("bar0" + istr0));
  ASSERT_OK(s);
  s = txn0->Prepare();

  // With the same index 0 and key prefix, txn_t0 should conflict with txn0
  txn_t0_with_status(0, Status::TimedOut());
  delete txn0;

  auto db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  db_impl->FlushWAL(true);
  ReOpenNoDelete();

  // It should still conflict after the recovery
  txn_t0_with_status(0, Status::TimedOut());

  db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  db_impl->FlushWAL(true);
  ReOpenNoDelete();

  // Check that a recovered txn will still cause conflicts after 2nd recovery
  txn_t0_with_status(0, Status::TimedOut());

  txn0 = db->GetTransactionByName("xid" + istr0);
  ASSERT_NE(txn0, nullptr);
  txn0->Commit();
  delete txn0;

  db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
  db_impl->FlushWAL(true);
  ReOpenNoDelete();

  // tnx0 is now committed and should no longer cause a conflict
  txn_t0_with_status(0, Status::OK());
}

// After recovery the commit map is empty while the max is set. The code would
// go through a different path which requires a separate test.
TEST_P(WritePreparedTransactionTest, IsInSnapshotEmptyMapTest) {
  WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
  wp_db->max_evicted_seq_ = 100;
  ASSERT_FALSE(wp_db->IsInSnapshot(50, 40));
  ASSERT_TRUE(wp_db->IsInSnapshot(50, 50));
  ASSERT_TRUE(wp_db->IsInSnapshot(50, 100));
  ASSERT_TRUE(wp_db->IsInSnapshot(50, 150));
  ASSERT_FALSE(wp_db->IsInSnapshot(100, 80));
  ASSERT_TRUE(wp_db->IsInSnapshot(100, 100));
  ASSERT_TRUE(wp_db->IsInSnapshot(100, 150));
}

// Test WritePreparedTxnDB's IsInSnapshot against different ordering of
// snapshot, max_committed_seq_, prepared, and commit entries.
TEST_P(WritePreparedTransactionTest, IsInSnapshotTest) {
  WriteOptions wo;
  // Use small commit cache to trigger lots of eviction and fast advance of
  // max_evicted_seq_
  const size_t commit_cache_bits = 3;
  // Same for snapshot cache size
  const size_t snapshot_cache_bits = 2;

  // Take some preliminary snapshots first. This is to stress the data structure
  // that holds the old snapshots as it will be designed to be efficient when
  // only a few snapshots are below the max_evicted_seq_.
  for (int max_snapshots = 1; max_snapshots < 20; max_snapshots++) {
    // Leave some gap between the preliminary snapshots and the final snapshot
    // that we check. This should test for also different overlapping scnearios
    // between the last snapshot and the commits.
    for (int max_gap = 1; max_gap < 10; max_gap++) {
      // Since we do not actually write to db, we mock the seq as it would be
      // increaased by the db. The only exception is that we need db seq to
      // advance for our snapshots. for which we apply a dummy put each time we
      // increase our mock of seq.
      uint64_t seq = 0;
      // At each step we prepare a txn and then we commit it in the next txn.
      // This emulates the consecuitive transactions that write to the same key
      uint64_t cur_txn = 0;
      // Number of snapshots taken so far
      int num_snapshots = 0;
      // Number of gaps applied so far
      int gap_cnt = 0;
      // The final snapshot that we will inspect
      uint64_t snapshot = 0;
      bool found_committed = false;
      // To stress the data structure that maintain prepared txns, at each cycle
      // we add a new prepare txn. These do not mean to be committed for
      // snapshot inspection.
      std::set<uint64_t> prepared;
      // We keep the list of txns comitted before we take the last snaphot.
      // These should be the only seq numbers that will be found in the snapshot
      std::set<uint64_t> committed_before;
      // The set of commit seq numbers to be excluded from IsInSnapshot queries
      std::set<uint64_t> commit_seqs;
      DBImpl* mock_db = new DBImpl(options, dbname);
      std::unique_ptr<WritePreparedTxnDBMock> wp_db(new WritePreparedTxnDBMock(
          mock_db, txn_db_options, snapshot_cache_bits, commit_cache_bits));
      // We continue until max advances a bit beyond the snapshot.
      while (!snapshot || wp_db->max_evicted_seq_ < snapshot + 100) {
        // do prepare for a transaction
        seq++;
        wp_db->AddPrepared(seq);
        prepared.insert(seq);

        // If cur_txn is not started, do prepare for it.
        if (!cur_txn) {
          seq++;
          cur_txn = seq;
          wp_db->AddPrepared(cur_txn);
        } else {                                     // else commit it
          seq++;
          wp_db->AddCommitted(cur_txn, seq);
          commit_seqs.insert(seq);
          if (!snapshot) {
            committed_before.insert(cur_txn);
          }
          cur_txn = 0;
        }

        if (num_snapshots < max_snapshots - 1) {
          // Take preliminary snapshots
          wp_db->TakeSnapshot(seq);
          num_snapshots++;
        } else if (gap_cnt < max_gap) {
          // Wait for some gap before taking the final snapshot
          gap_cnt++;
        } else if (!snapshot) {
          // Take the final snapshot if it is not already taken
          snapshot = seq;
          wp_db->TakeSnapshot(snapshot);
          num_snapshots++;
        }

        // If the snapshot is taken, verify seq numbers visible to it. We redo
        // it at each cycle to test that the system is still sound when
        // max_evicted_seq_ advances.
        if (snapshot) {
          for (uint64_t s = 1;
               s <= seq && commit_seqs.find(s) == commit_seqs.end(); s++) {
            bool was_committed =
                (committed_before.find(s) != committed_before.end());
            bool is_in_snapshot = wp_db->IsInSnapshot(s, snapshot);
            if (was_committed != is_in_snapshot) {
              printf("max_snapshots %d max_gap %d seq %" PRIu64 " max %" PRIu64
                     " snapshot %" PRIu64
                     " gap_cnt %d num_snapshots %d s %" PRIu64 "\n",
                     max_snapshots, max_gap, seq,
                     wp_db->max_evicted_seq_.load(), snapshot, gap_cnt,
                     num_snapshots, s);
            }
            ASSERT_EQ(was_committed, is_in_snapshot);
            found_committed = found_committed || is_in_snapshot;
          }
        }
      }
      // Safety check to make sure the test actually ran
      ASSERT_TRUE(found_committed);
      // As an extra check, check if prepared set will be properly empty after
      // they are committed.
      if (cur_txn) {
        wp_db->AddCommitted(cur_txn, seq);
      }
      for (auto p : prepared) {
        wp_db->AddCommitted(p, seq);
      }
      ASSERT_TRUE(wp_db->delayed_prepared_.empty());
      ASSERT_TRUE(wp_db->prepared_txns_.empty());
    }
  }
}

void ASSERT_SAME(TransactionDB* db, Status exp_s, PinnableSlice& exp_v,
                 Slice key) {
  Status s;
  PinnableSlice v;
  ReadOptions roptions;
  s = db->Get(roptions, db->DefaultColumnFamily(), key, &v);
  ASSERT_TRUE(exp_s == s);
  ASSERT_TRUE(s.ok() || s.IsNotFound());
  if (s.ok()) {
    ASSERT_TRUE(exp_v == v);
  }
}

TEST_P(WritePreparedTransactionTest, RollbackTest) {
  ReadOptions roptions;
  WriteOptions woptions;
  TransactionOptions txn_options;
  const size_t num_keys = 4;
  const size_t num_values = 5;
  for (size_t ikey = 1; ikey <= num_keys; ikey++) {
    for (size_t ivalue = 0; ivalue < num_values; ivalue++) {
      for (bool crash : {false, true}) {
        ReOpen();
        WritePreparedTxnDB* wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
        std::string key_str = "key" + ToString(ikey);
        switch (ivalue) {
          case 0:
            break;
          case 1:
            ASSERT_OK(db->Put(woptions, key_str, "initvalue1"));
            break;
          case 2:
            ASSERT_OK(db->Merge(woptions, key_str, "initvalue2"));
            break;
          case 3:
            ASSERT_OK(db->Delete(woptions, key_str));
            break;
          case 4:
            ASSERT_OK(db->SingleDelete(woptions, key_str));
            break;
          default:
            assert(0);
        }

        PinnableSlice v1;
        auto s1 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key1"), &v1);
        PinnableSlice v2;
        auto s2 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key2"), &v2);
        PinnableSlice v3;
        auto s3 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key3"), &v3);
        PinnableSlice v4;
        auto s4 =
            db->Get(roptions, db->DefaultColumnFamily(), Slice("key4"), &v4);
        Transaction* txn = db->BeginTransaction(woptions, txn_options);
        auto s = txn->SetName("xid0");
        ASSERT_OK(s);
        s = txn->Put(Slice("key1"), Slice("value1"));
        ASSERT_OK(s);
        s = txn->Merge(Slice("key2"), Slice("value2"));
        ASSERT_OK(s);
        s = txn->Delete(Slice("key3"));
        ASSERT_OK(s);
        s = txn->SingleDelete(Slice("key4"));
        ASSERT_OK(s);
        s = txn->Prepare();
        ASSERT_OK(s);

        {
          ReadLock rl(&wp_db->prepared_mutex_);
          ASSERT_FALSE(wp_db->prepared_txns_.empty());
          ASSERT_EQ(txn->GetId(), wp_db->prepared_txns_.top());
        }

        ASSERT_SAME(db, s1, v1, "key1");
        ASSERT_SAME(db, s2, v2, "key2");
        ASSERT_SAME(db, s3, v3, "key3");
        ASSERT_SAME(db, s4, v4, "key4");

        if (crash) {
          // TODO(myabandeh): replace it with true crash (commented lines below)
          // after compaction PR is landed.
          auto db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
          auto seq = db_impl->GetLatestSequenceNumber();
          wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
          SequenceNumber prev_max = wp_db->max_evicted_seq_;
          wp_db->AdvanceMaxEvictedSeq(prev_max, seq);
          //    delete txn;
          //    auto db_impl = reinterpret_cast<DBImpl*>(db->GetRootDB());
          //    db_impl->FlushWAL(true);
          //    ReOpenNoDelete();
          //    wp_db = dynamic_cast<WritePreparedTxnDB*>(db);
          //    txn = db->GetTransactionByName("xid0");
          //    ASSERT_FALSE(wp_db->delayed_prepared_empty_);
          //    ReadLock rl(&wp_db->prepared_mutex_);
          //    ASSERT_TRUE(wp_db->prepared_txns_.empty());
          //    ASSERT_FALSE(wp_db->delayed_prepared_.empty());
          //    ASSERT_TRUE(wp_db->delayed_prepared_.find(txn->GetId()) !=
          //                wp_db->delayed_prepared_.end());
        }

        ASSERT_SAME(db, s1, v1, "key1");
        ASSERT_SAME(db, s2, v2, "key2");
        ASSERT_SAME(db, s3, v3, "key3");
        ASSERT_SAME(db, s4, v4, "key4");

        s = txn->Rollback();
        ASSERT_OK(s);

        {
          ASSERT_TRUE(wp_db->delayed_prepared_empty_);
          ReadLock rl(&wp_db->prepared_mutex_);
          ASSERT_TRUE(wp_db->prepared_txns_.empty());
          ASSERT_TRUE(wp_db->delayed_prepared_.empty());
        }

        ASSERT_SAME(db, s1, v1, "key1");
        ASSERT_SAME(db, s2, v2, "key2");
        ASSERT_SAME(db, s3, v3, "key3");
        ASSERT_SAME(db, s4, v4, "key4");
        delete txn;
      }
    }
  }
}

// TODO(myabandeh): move it to transaction_test when it is extended to
// WROTE_PREPARED.
// Test that the transactional db can handle duplicate keys in the write batch
TEST_P(WritePreparedTransactionTest, DuplicateKeyTest) {
  for (bool do_prepare : {true, false}) {
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
    auto s = txn0->SetName("xid");
    ASSERT_OK(s);
    s = txn0->Put(Slice("foo0"), Slice("bar0a"));
    ASSERT_OK(s);
    s = txn0->Put(Slice("foo0"), Slice("bar0b"));
    ASSERT_OK(s);
    s = txn0->Put(Slice("foo1"), Slice("bar1"));
    ASSERT_OK(s);
    s = txn0->Merge(Slice("foo2"), Slice("bar2a"));
    ASSERT_OK(s);
    // TODO(myabandeh): enable this after duplicatae merge keys are supported
    // s = txn0->Merge(Slice("foo2"), Slice("bar2a"));
    // ASSERT_OK(s);
    s = txn0->Put(Slice("foo2"), Slice("bar2b"));
    ASSERT_OK(s);
    s = txn0->Put(Slice("foo3"), Slice("bar3"));
    ASSERT_OK(s);
    // TODO(myabandeh): enable this after duplicatae merge keys are supported
    // s = txn0->Merge(Slice("foo3"), Slice("bar3"));
    // ASSERT_OK(s);
    s = txn0->Put(Slice("foo4"), Slice("bar4"));
    ASSERT_OK(s);
    s = txn0->Delete(Slice("foo4"));
    ASSERT_OK(s);
    s = txn0->SingleDelete(Slice("foo4"));
    ASSERT_OK(s);
    if (do_prepare) {
      s = txn0->Prepare();
      ASSERT_OK(s);
    }
    s = txn0->Commit();
    ASSERT_OK(s);
    if (!do_prepare) {
      auto pdb = reinterpret_cast<PessimisticTransactionDB*>(db);
      pdb->UnregisterTransaction(txn0);
    }
    delete txn0;
    ReadOptions ropt;
    PinnableSlice pinnable_val;

    s = db->Get(ropt, db->DefaultColumnFamily(), "foo0", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar0b"));
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo1", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar1"));
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo2", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar2b"));
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo3", &pinnable_val);
    ASSERT_OK(s);
    ASSERT_TRUE(pinnable_val == ("bar3"));
    s = db->Get(ropt, db->DefaultColumnFamily(), "foo4", &pinnable_val);
    ASSERT_TRUE(s.IsNotFound());
  }
}

TEST_P(WritePreparedTransactionTest, SequenceNumberZeroTest) {
  ASSERT_OK(db->Put(WriteOptions(), "foo", "bar"));
  VerifyKeys({{"foo", "bar"}});
  const Snapshot* snapshot = db->GetSnapshot();
  ASSERT_OK(db->Flush(FlushOptions()));
  // Dummy keys to avoid compaction trivially move files and get around actual
  // compaction logic.
  ASSERT_OK(db->Put(WriteOptions(), "a", "dummy"));
  ASSERT_OK(db->Put(WriteOptions(), "z", "dummy"));
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Compaction will output keys with sequence number 0, if it is visible to
  // earliest snapshot. Make sure IsInSnapshot() report sequence number 0 is
  // visible to any snapshot.
  VerifyKeys({{"foo", "bar"}});
  VerifyKeys({{"foo", "bar"}}, snapshot);
#ifdef INDIRECT_VALUE_SUPPORT
  const int exptype = kTypeIndirectValue;
#else
  const int exptype = kTypeValue;
#endif
  VerifyInternalKeys({{"foo", "bar", 0, exptype}});
  db->ReleaseSnapshot(snapshot);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr,
          "SKIPPED as Transactions are not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
