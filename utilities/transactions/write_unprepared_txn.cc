//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_unprepared_txn.h"
#include "db/db_impl/db_impl.h"
#include "util/cast_util.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace rocksdb {

bool WriteUnpreparedTxnReadCallback::IsVisibleFullCheck(SequenceNumber seq) {
  // Since unprep_seqs maps prep_seq => prepare_batch_cnt, to check if seq is
  // in unprep_seqs, we have to check if seq is equal to prep_seq or any of
  // the prepare_batch_cnt seq nums after it.
  //
  // TODO(lth): Can be optimized with std::lower_bound if unprep_seqs is
  // large.
  for (const auto& it : unprep_seqs_) {
    if (it.first <= seq && seq < it.first + it.second) {
      return true;
    }
  }

  return db_->IsInSnapshot(seq, wup_snapshot_, min_uncommitted_);
}

WriteUnpreparedTxn::WriteUnpreparedTxn(WriteUnpreparedTxnDB* txn_db,
                                       const WriteOptions& write_options,
                                       const TransactionOptions& txn_options)
    : WritePreparedTxn(txn_db, write_options, txn_options),
      wupt_db_(txn_db),
      recovered_txn_(false),
      largest_validated_seq_(0) {
  if (txn_options.write_batch_flush_threshold < 0) {
    write_batch_flush_threshold_ =
        txn_db_impl_->GetTxnDBOptions().default_write_batch_flush_threshold;
  } else {
    write_batch_flush_threshold_ = txn_options.write_batch_flush_threshold;
  }
}

WriteUnpreparedTxn::~WriteUnpreparedTxn() {
  if (!unprep_seqs_.empty()) {
    assert(log_number_ > 0);
    assert(GetId() > 0);
    assert(!name_.empty());

    // We should rollback regardless of GetState, but some unit tests that
    // test crash recovery run the destructor assuming that rollback does not
    // happen, so that rollback during recovery can be exercised.
    if (GetState() == STARTED) {
      auto s __attribute__((__unused__)) = RollbackInternal();
      // TODO(lth): Better error handling.
      assert(s.ok());
      dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
          log_number_);
    }
  }

  // Call tracked_keys_.clear() so that ~PessimisticTransaction does not
  // try to unlock keys for recovered transactions.
  if (recovered_txn_) {
    tracked_keys_.clear();
  }
}

void WriteUnpreparedTxn::Initialize(const TransactionOptions& txn_options) {
  PessimisticTransaction::Initialize(txn_options);
  if (txn_options.write_batch_flush_threshold < 0) {
    write_batch_flush_threshold_ =
        txn_db_impl_->GetTxnDBOptions().default_write_batch_flush_threshold;
  } else {
    write_batch_flush_threshold_ = txn_options.write_batch_flush_threshold;
  }

  unprep_seqs_.clear();
  recovered_txn_ = false;
  largest_validated_seq_ = 0;
}

Status WriteUnpreparedTxn::HandleWrite(std::function<Status()> do_write) {
  Status s = MaybeFlushWriteBatchToDB();
  if (!s.ok()) {
    return s;
  }
  s = do_write();
  if (s.ok()) {
    if (snapshot_) {
      largest_validated_seq_ =
          std::max(largest_validated_seq_, snapshot_->GetSequenceNumber());
    } else {
      largest_validated_seq_ = kMaxSequenceNumber;
    }
  }
  return s;
}

Status WriteUnpreparedTxn::Put(ColumnFamilyHandle* column_family,
                               const Slice& key, const Slice& value,
                               const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Put(column_family, key, value, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Put(ColumnFamilyHandle* column_family,
                               const SliceParts& key, const SliceParts& value,
                               const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Put(column_family, key, value, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Merge(ColumnFamilyHandle* column_family,
                                 const Slice& key, const Slice& value,
                                 const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Merge(column_family, key, value,
                                      assume_tracked);
  });
}

Status WriteUnpreparedTxn::Delete(ColumnFamilyHandle* column_family,
                                  const Slice& key, const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Delete(column_family, key, assume_tracked);
  });
}

Status WriteUnpreparedTxn::Delete(ColumnFamilyHandle* column_family,
                                  const SliceParts& key,
                                  const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::Delete(column_family, key, assume_tracked);
  });
}

Status WriteUnpreparedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                        const Slice& key,
                                        const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::SingleDelete(column_family, key,
                                             assume_tracked);
  });
}

Status WriteUnpreparedTxn::SingleDelete(ColumnFamilyHandle* column_family,
                                        const SliceParts& key,
                                        const bool assume_tracked) {
  return HandleWrite([&]() {
    return TransactionBaseImpl::SingleDelete(column_family, key,
                                             assume_tracked);
  });
}

// WriteUnpreparedTxn::RebuildFromWriteBatch is only called on recovery. For
// WriteUnprepared, the write batches have already been written into the
// database during WAL replay, so all we have to do is just to "retrack" the key
// so that rollbacks are possible.
//
// Calling TryLock instead of TrackKey is also possible, but as an optimization,
// recovered transactions do not hold locks on their keys. This follows the
// implementation in PessimisticTransactionDB::Initialize where we set
// skip_concurrency_control to true.
Status WriteUnpreparedTxn::RebuildFromWriteBatch(WriteBatch* wb) {
  struct TrackKeyHandler : public WriteBatch::Handler {
    WriteUnpreparedTxn* txn_;
    bool rollback_merge_operands_;

    TrackKeyHandler(WriteUnpreparedTxn* txn, bool rollback_merge_operands)
        : txn_(txn), rollback_merge_operands_(rollback_merge_operands) {}

    Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status DeleteCF(uint32_t cf, const Slice& key) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
      txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                     false /* read_only */, true /* exclusive */);
      return Status::OK();
    }

    Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
      if (rollback_merge_operands_) {
        txn_->TrackKey(cf, key.ToString(), kMaxSequenceNumber,
                       false /* read_only */, true /* exclusive */);
      }
      return Status::OK();
    }

    // Recovered batches do not contain 2PC markers.
    Status MarkBeginPrepare(bool) override { return Status::InvalidArgument(); }

    Status MarkEndPrepare(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkNoop(bool) override { return Status::InvalidArgument(); }

    Status MarkCommit(const Slice&) override {
      return Status::InvalidArgument();
    }

    Status MarkRollback(const Slice&) override {
      return Status::InvalidArgument();
    }
  };

  TrackKeyHandler handler(this,
                          wupt_db_->txn_db_options_.rollback_merge_operands);
  return wb->Iterate(&handler);
}

Status WriteUnpreparedTxn::MaybeFlushWriteBatchToDB() {
  const bool kPrepared = true;
  Status s;
  if (write_batch_flush_threshold_ > 0 &&
      write_batch_.GetDataSize() >
          static_cast<size_t>(write_batch_flush_threshold_)) {
    assert(GetState() != PREPARED);
    s = FlushWriteBatchToDB(!kPrepared);
  }
  return s;
}

Status WriteUnpreparedTxn::FlushWriteBatchToDB(bool prepared) {
  if (name_.empty()) {
    return Status::InvalidArgument("Cannot write to DB without SetName.");
  }

  // TODO(lth): Reduce duplicate code with WritePrepared prepare logic.
  WriteOptions write_options = write_options_;
  write_options.disableWAL = false;
  const bool WRITE_AFTER_COMMIT = true;
  const bool first_prepare_batch = log_number_ == 0;
  // MarkEndPrepare will change Noop marker to the appropriate marker.
  WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(), name_,
                                     !WRITE_AFTER_COMMIT, !prepared);
  // For each duplicate key we account for a new sub-batch
  prepare_batch_cnt_ = GetWriteBatch()->SubBatchCnt();
  // AddPrepared better to be called in the pre-release callback otherwise there
  // is a non-zero chance of max advancing prepare_seq and readers assume the
  // data as committed.
  // Also having it in the PreReleaseCallback allows in-order addition of
  // prepared entries to PreparedHeap and hence enables an optimization. Refer
  // to SmallestUnCommittedSeq for more details.
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, prepare_batch_cnt_,
      db_impl_->immutable_db_options().two_write_queues, first_prepare_batch);
  const bool DISABLE_MEMTABLE = true;
  uint64_t seq_used = kMaxSequenceNumber;
  // log_number_ should refer to the oldest log containing uncommitted data
  // from the current transaction. This means that if log_number_ is set,
  // WriteImpl should not overwrite that value, so set log_used to nullptr if
  // log_number_ is already set.
  uint64_t* log_used = log_number_ ? nullptr : &log_number_;
  auto s = db_impl_->WriteImpl(write_options, GetWriteBatch()->GetWriteBatch(),
                               /*callback*/ nullptr, log_used, /*log ref*/
                               0, !DISABLE_MEMTABLE, &seq_used,
                               prepare_batch_cnt_, &add_prepared_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  auto prepare_seq = seq_used;

  // Only call SetId if it hasn't been set yet.
  if (GetId() == 0) {
    SetId(prepare_seq);
  }
  // unprep_seqs_ will also contain prepared seqnos since they are treated in
  // the same way in the prepare/commit callbacks. See the comment on the
  // definition of unprep_seqs_.
  unprep_seqs_[prepare_seq] = prepare_batch_cnt_;

  // Reset transaction state.
  if (!prepared) {
    prepare_batch_cnt_ = 0;
    write_batch_.Clear();
    WriteBatchInternal::InsertNoop(write_batch_.GetWriteBatch());
  }

  return s;
}

Status WriteUnpreparedTxn::PrepareInternal() {
  const bool kPrepared = true;
  return FlushWriteBatchToDB(kPrepared);
}

Status WriteUnpreparedTxn::CommitWithoutPrepareInternal() {
  if (unprep_seqs_.empty()) {
    assert(log_number_ == 0);
    assert(GetId() == 0);
    return WritePreparedTxn::CommitWithoutPrepareInternal();
  }

  // TODO(lth): We should optimize commit without prepare to not perform
  // a prepare under the hood.
  auto s = PrepareInternal();
  if (!s.ok()) {
    return s;
  }
  return CommitInternal();
}

Status WriteUnpreparedTxn::CommitInternal() {
  // TODO(lth): Reduce duplicate code with WritePrepared commit logic.

  // We take the commit-time batch and append the Commit marker.  The Memtable
  // will ignore the Commit marker in non-recovery mode
  WriteBatch* working_batch = GetCommitTimeWriteBatch();
  const bool empty = working_batch->Count() == 0;
  WriteBatchInternal::MarkCommit(working_batch, name_);

  const bool for_recovery = use_only_the_last_commit_time_batch_for_recovery_;
  if (!empty && for_recovery) {
    // When not writing to memtable, we can still cache the latest write batch.
    // The cached batch will be written to memtable in WriteRecoverableState
    // during FlushMemTable
    WriteBatchInternal::SetAsLastestPersistentState(working_batch);
  }

  const bool includes_data = !empty && !for_recovery;
  size_t commit_batch_cnt = 0;
  if (UNLIKELY(includes_data)) {
    ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                   "Duplicate key overhead");
    SubBatchCounter counter(*wpt_db_->GetCFComparatorMap());
    auto s = working_batch->Iterate(&counter);
    assert(s.ok());
    commit_batch_cnt = counter.BatchCount();
  }
  const bool disable_memtable = !includes_data;
  const bool do_one_write =
      !db_impl_->immutable_db_options().two_write_queues || disable_memtable;

  WriteUnpreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, unprep_seqs_, commit_batch_cnt);
  const bool kFirstPrepareBatch = true;
  AddPreparedCallback add_prepared_callback(
      wpt_db_, db_impl_, commit_batch_cnt,
      db_impl_->immutable_db_options().two_write_queues, !kFirstPrepareBatch);
  PreReleaseCallback* pre_release_callback;
  if (do_one_write) {
    pre_release_callback = &update_commit_map;
  } else {
    pre_release_callback = &add_prepared_callback;
  }
  uint64_t seq_used = kMaxSequenceNumber;
  // Since the prepared batch is directly written to memtable, there is
  // already a connection between the memtable and its WAL, so there is no
  // need to redundantly reference the log that contains the prepared data.
  const uint64_t zero_log_number = 0ull;
  size_t batch_cnt = UNLIKELY(commit_batch_cnt) ? commit_batch_cnt : 1;
  auto s = db_impl_->WriteImpl(write_options_, working_batch, nullptr, nullptr,
                               zero_log_number, disable_memtable, &seq_used,
                               batch_cnt, pre_release_callback);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  const SequenceNumber commit_batch_seq = seq_used;
  if (LIKELY(do_one_write || !s.ok())) {
    if (LIKELY(s.ok())) {
      // Note RemovePrepared should be called after WriteImpl that publishsed
      // the seq. Otherwise SmallestUnCommittedSeq optimization breaks.
      for (const auto& seq : unprep_seqs_) {
        wpt_db_->RemovePrepared(seq.first, seq.second);
      }
    }
    if (UNLIKELY(!do_one_write)) {
      wpt_db_->RemovePrepared(commit_batch_seq, commit_batch_cnt);
    }
    unprep_seqs_.clear();
    return s;
  }  // else do the 2nd write to publish seq

  // Populate unprep_seqs_ with commit_batch_seq, since we treat data in the
  // commit write batch as just another "unprepared" batch. This will also
  // update the unprep_seqs_ in the update_commit_map callback.
  unprep_seqs_[commit_batch_seq] = commit_batch_cnt;

  // Note: the 2nd write comes with a performance penality. So if we have too
  // many of commits accompanied with ComitTimeWriteBatch and yet we cannot
  // enable use_only_the_last_commit_time_batch_for_recovery_ optimization,
  // two_write_queues should be disabled to avoid many additional writes here.

  // Update commit map only from the 2nd queue
  WriteBatch empty_batch;
  empty_batch.PutLogData(Slice());
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(&empty_batch);
  const bool DISABLE_MEMTABLE = true;
  const size_t ONE_BATCH = 1;
  const uint64_t NO_REF_LOG = 0;
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          NO_REF_LOG, DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          &update_commit_map);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Note RemovePrepared should be called after WriteImpl that publishsed the
  // seq. Otherwise SmallestUnCommittedSeq optimization breaks.
  for (const auto& seq : unprep_seqs_) {
    wpt_db_->RemovePrepared(seq.first, seq.second);
  }
  unprep_seqs_.clear();
  return s;
}

Status WriteUnpreparedTxn::RollbackInternal() {
  // TODO(lth): Reduce duplicate code with WritePrepared rollback logic.
  WriteBatchWithIndex rollback_batch(
      wpt_db_->DefaultColumnFamily()->GetComparator(), 0, true, 0);
  assert(GetId() != kMaxSequenceNumber);
  assert(GetId() > 0);
  Status s;
  const auto& cf_map = *wupt_db_->GetCFHandleMap();
  auto read_at_seq = kMaxSequenceNumber;

  ReadOptions roptions;
  // Note that we do not use WriteUnpreparedTxnReadCallback because we do not
  // need to read our own writes when reading prior versions of the key for
  // rollback.
  const auto& tracked_keys = GetTrackedKeys();
  WritePreparedTxnReadCallback callback(wpt_db_, read_at_seq);
  for (const auto& cfkey : tracked_keys) {
    const auto cfid = cfkey.first;
    const auto& keys = cfkey.second;
    for (const auto& pair : keys) {
      const auto& key = pair.first;
      const auto& cf_handle = cf_map.at(cfid);
      PinnableSlice pinnable_val;
      bool not_used;
      s = db_impl_->GetImpl(roptions, cf_handle, key, &pinnable_val, &not_used,
                            &callback);

      if (s.ok()) {
        s = rollback_batch.Put(cf_handle, key, pinnable_val);
        assert(s.ok());
      } else if (s.IsNotFound()) {
        s = rollback_batch.Delete(cf_handle, key);
        assert(s.ok());
      } else {
        return s;
      }
    }
  }

  // The Rollback marker will be used as a batch separator
  WriteBatchInternal::MarkRollback(rollback_batch.GetWriteBatch(), name_);
  bool do_one_write = !db_impl_->immutable_db_options().two_write_queues;
  const bool DISABLE_MEMTABLE = true;
  const uint64_t NO_REF_LOG = 0;
  uint64_t seq_used = kMaxSequenceNumber;
  // TODO(lth): We write rollback batch all in a single batch here, but this
  // should be subdivded into multiple batches as well. In phase 2, when key
  // sets are read from WAL, this will happen naturally.
  const size_t ONE_BATCH = 1;
  // We commit the rolled back prepared batches. ALthough this is
  // counter-intuitive, i) it is safe to do so, since the prepared batches are
  // already canceled out by the rollback batch, ii) adding the commit entry to
  // CommitCache will allow us to benefit from the existing mechanism in
  // CommitCache that keeps an entry evicted due to max advance and yet overlaps
  // with a live snapshot around so that the live snapshot properly skips the
  // entry even if its prepare seq is lower than max_evicted_seq_.
  WriteUnpreparedCommitEntryPreReleaseCallback update_commit_map(
      wpt_db_, db_impl_, unprep_seqs_, ONE_BATCH);
  // Note: the rollback batch does not need AddPrepared since it is written to
  // DB in one shot. min_uncommitted still works since it requires capturing
  // data that is written to DB but not yet committed, while the roolback
  // batch commits with PreReleaseCallback.
  s = db_impl_->WriteImpl(write_options_, rollback_batch.GetWriteBatch(),
                          nullptr, nullptr, NO_REF_LOG, !DISABLE_MEMTABLE,
                          &seq_used, rollback_batch.SubBatchCnt(),
                          do_one_write ? &update_commit_map : nullptr);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  if (!s.ok()) {
    return s;
  }
  if (do_one_write) {
    for (const auto& seq : unprep_seqs_) {
      wpt_db_->RemovePrepared(seq.first, seq.second);
    }
    unprep_seqs_.clear();
    return s;
  }  // else do the 2nd write for commit
  uint64_t& prepare_seq = seq_used;
  ROCKS_LOG_DETAILS(db_impl_->immutable_db_options().info_log,
                    "RollbackInternal 2nd write prepare_seq: %" PRIu64,
                    prepare_seq);
  // Commit the batch by writing an empty batch to the queue that will release
  // the commit sequence number to readers.
  WriteUnpreparedRollbackPreReleaseCallback update_commit_map_with_prepare(
      wpt_db_, db_impl_, unprep_seqs_, prepare_seq);
  WriteBatch empty_batch;
  empty_batch.PutLogData(Slice());
  // In the absence of Prepare markers, use Noop as a batch separator
  WriteBatchInternal::InsertNoop(&empty_batch);
  s = db_impl_->WriteImpl(write_options_, &empty_batch, nullptr, nullptr,
                          NO_REF_LOG, DISABLE_MEMTABLE, &seq_used, ONE_BATCH,
                          &update_commit_map_with_prepare);
  assert(!s.ok() || seq_used != kMaxSequenceNumber);
  // Mark the txn as rolled back
  if (s.ok()) {
    for (const auto& seq : unprep_seqs_) {
      wpt_db_->RemovePrepared(seq.first, seq.second);
    }
  }

  unprep_seqs_.clear();
  return s;
}

void WriteUnpreparedTxn::Clear() {
  if (!recovered_txn_) {
    txn_db_impl_->UnLock(this, &GetTrackedKeys());
  }
  TransactionBaseImpl::Clear();
}

void WriteUnpreparedTxn::MultiGet(const ReadOptions& options,
                                  ColumnFamilyHandle* column_family,
                                  const size_t num_keys, const Slice* keys,
                                  PinnableSlice* values, Status* statuses,
                                  bool sorted_input) {
  SequenceNumber min_uncommitted, snap_seq;
  const bool backed_by_snapshot =
      wupt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_);
  write_batch_.MultiGetFromBatchAndDB(db_, options, column_family, num_keys,
                                      keys, values, statuses, sorted_input,
                                      &callback);
  if (UNLIKELY(!wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    for (size_t i = 0; i < num_keys; i++) {
      statuses[i] = Status::TryAgain();
    }
  }
}

Status WriteUnpreparedTxn::Get(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableSlice* value) {
  SequenceNumber min_uncommitted, snap_seq;
  const bool backed_by_snapshot =
      wupt_db_->AssignMinMaxSeqs(options.snapshot, &min_uncommitted, &snap_seq);
  WriteUnpreparedTxnReadCallback callback(wupt_db_, snap_seq, min_uncommitted,
                                          unprep_seqs_);
  auto res = write_batch_.GetFromBatchAndDB(db_, options, column_family, key,
                                            value, &callback);
  if (LIKELY(wupt_db_->ValidateSnapshot(snap_seq, backed_by_snapshot))) {
    return res;
  } else {
    return Status::TryAgain();
  }
}

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options) {
  return GetIterator(options, wupt_db_->DefaultColumnFamily());
}

Iterator* WriteUnpreparedTxn::GetIterator(const ReadOptions& options,
                                          ColumnFamilyHandle* column_family) {
  // Make sure to get iterator from WriteUnprepareTxnDB, not the root db.
  Iterator* db_iter = wupt_db_->NewIterator(options, column_family, this);
  assert(db_iter);

  return write_batch_.NewIteratorWithBase(column_family, db_iter);
}

const std::map<SequenceNumber, size_t>&
WriteUnpreparedTxn::GetUnpreparedSequenceNumbers() {
  return unprep_seqs_;
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
