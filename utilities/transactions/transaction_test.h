//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <functional>
#include <string>
#include <thread>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
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

namespace rocksdb {

class TransactionTest : public ::testing::TestWithParam<
                            std::tuple<bool, bool, TxnDBWritePolicy>> {
 public:
  TransactionDB* db;
  FaultInjectionTestEnv* env;
  std::string dbname;
  Options options;

  TransactionDBOptions txn_db_options;

  TransactionTest() {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    options.write_buffer_size = 4 * 1024;
    options.level0_file_num_compaction_trigger = 2;
    options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
    env = new FaultInjectionTestEnv(Env::Default());
    options.env = env;
    options.concurrent_prepare = std::get<1>(GetParam());
    dbname = test::TmpDir() + "/transaction_testdb";

    DestroyDB(dbname, options);
    txn_db_options.transaction_lock_timeout = 0;
    txn_db_options.default_lock_timeout = 0;
    txn_db_options.write_policy = std::get<2>(GetParam());
    Status s;
    if (std::get<0>(GetParam()) == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    assert(s.ok());
  }

  ~TransactionTest() {
    delete db;
    DestroyDB(dbname, options);
    delete env;
  }

  Status ReOpenNoDelete() {
    delete db;
    db = nullptr;
    env->AssertNoOpenFile();
    env->DropUnsyncedFileData();
    env->ResetState();
    Status s;
    if (std::get<0>(GetParam()) == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
      assert(s.ok());
    } else {
      s = OpenWithStackableDB();
      assert(s.ok());
    }
    return s;
  }

  Status ReOpen() {
    delete db;
    DestroyDB(dbname, options);
    Status s;
    if (std::get<0>(GetParam()) == false) {
      s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    } else {
      s = OpenWithStackableDB();
    }
    return s;
  }

  Status OpenWithStackableDB() {
    std::vector<size_t> compaction_enabled_cf_indices;
    std::vector<ColumnFamilyDescriptor> column_families{ColumnFamilyDescriptor(
        kDefaultColumnFamilyName, ColumnFamilyOptions(options))};

    TransactionDB::PrepareWrap(&options, &column_families,
                               &compaction_enabled_cf_indices);
    std::vector<ColumnFamilyHandle*> handles;
    DB* root_db;
    Options options_copy(options);
    Status s =
        DB::Open(options_copy, dbname, column_families, &handles, &root_db);
    if (s.ok()) {
      assert(handles.size() == 1);
      s = TransactionDB::WrapStackableDB(
          new StackableDB(root_db), txn_db_options,
          compaction_enabled_cf_indices, handles, &db);
      delete handles[0];
    }
    return s;
  }

  std::atomic<size_t> linked = {0};
  std::atomic<size_t> exp_seq = {0};
  std::atomic<size_t> commit_writes = {0};
  std::atomic<size_t> expected_commits = {0};
  std::function<void(size_t, Status)> txn_t0_with_status = [&](size_t index,
                                                               Status exp_s) {
    // Test DB's internal txn. It involves no prepare phase nor a commit marker.
    WriteOptions wopts;
    auto s = db->Put(wopts, "key" + std::to_string(index), "value");
    ASSERT_EQ(exp_s, s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq++;
    } else {
      // Consume one seq per batch
      exp_seq++;
    }
  };
  std::function<void(size_t)> txn_t0 = [&](size_t index) {
    return txn_t0_with_status(index, Status::OK());
  };
  std::function<void(size_t)> txn_t1 = [&](size_t index) {
    // Testing directly writing a write batch. Functionality-wise it is
    // equivalent to commit without prepare.
    WriteBatch wb;
    auto istr = std::to_string(index);
    wb.Put("k1" + istr, "v1");
    wb.Put("k2" + istr, "v2");
    wb.Put("k3" + istr, "v3");
    WriteOptions wopts;
    auto s = db->Write(wopts, &wb);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 3;
      ;
    } else {
      // Consume one seq per batch
      exp_seq++;
    }
    ASSERT_OK(s);
  };
  std::function<void(size_t)> txn_t2 = [&](size_t index) {
    // Commit without prepare. It should write to DB without a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
    auto s = txn->SetName("xid" + istr);
    ASSERT_OK(s);
    s = txn->Put(Slice("foo" + istr), Slice("bar"));
    s = txn->Put(Slice("foo2" + istr), Slice("bar2"));
    s = txn->Put(Slice("foo3" + istr), Slice("bar3"));
    s = txn->Put(Slice("foo4" + istr), Slice("bar4"));
    ASSERT_OK(s);
    s = txn->Commit();
    ASSERT_OK(s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 4;
    } else {
      // Consume one seq per batch
      exp_seq++;
    }
    auto pdb = reinterpret_cast<PessimisticTransactionDB*>(db);
    pdb->UnregisterTransaction(txn);
    delete txn;
  };
  std::function<void(size_t)> txn_t3 = [&](size_t index) {
    // A full 2pc txn that also involves a commit marker.
    TransactionOptions txn_options;
    WriteOptions write_options;
    Transaction* txn = db->BeginTransaction(write_options, txn_options);
    auto istr = std::to_string(index);
    auto s = txn->SetName("xid" + istr);
    ASSERT_OK(s);
    s = txn->Put(Slice("foo" + istr), Slice("bar"));
    s = txn->Put(Slice("foo2" + istr), Slice("bar2"));
    s = txn->Put(Slice("foo3" + istr), Slice("bar3"));
    s = txn->Put(Slice("foo4" + istr), Slice("bar4"));
    s = txn->Put(Slice("foo5" + istr), Slice("bar5"));
    ASSERT_OK(s);
    expected_commits++;
    s = txn->Prepare();
    ASSERT_OK(s);
    commit_writes++;
    s = txn->Commit();
    ASSERT_OK(s);
    if (txn_db_options.write_policy == TxnDBWritePolicy::WRITE_COMMITTED) {
      // Consume one seq per key
      exp_seq += 5;
    } else {
      // Consume one seq per batch
      exp_seq++;
      // Consume one seq per commit marker
      exp_seq++;
    }
    delete txn;
  };
};

class MySQLStyleTransactionTest : public TransactionTest {};

}  // namespace rocksdb
