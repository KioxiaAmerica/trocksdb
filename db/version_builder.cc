//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"

namespace rocksdb {

bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->fd.largest_seqno != b->fd.largest_seqno) {
    return a->fd.largest_seqno > b->fd.largest_seqno;
  }
  if (a->fd.smallest_seqno != b->fd.smallest_seqno) {
    return a->fd.smallest_seqno > b->fd.smallest_seqno;
  }
  // Break ties by file number
  return a->fd.GetNumber() > b->fd.GetNumber();
}

namespace {
bool BySmallestKey(FileMetaData* a, FileMetaData* b,
                   const InternalKeyComparator* cmp) {
  int r = cmp->Compare(a->smallest, b->smallest);
  if (r != 0) {
    return (r < 0);
  }
  // Break ties by file number
  return (a->fd.GetNumber() < b->fd.GetNumber());
}
}  // namespace

class VersionBuilder::Rep {
  friend class VersionBuilder;
 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod { kLevel0 = 0, kLevelNon0 = 1, } sort_method;
    const InternalKeyComparator* internal_comparator;

    FileComparator() : internal_comparator(nullptr) {}

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      switch (sort_method) {
        case kLevel0:
          return NewestFirstBySeqNo(f1, f2);
        case kLevelNon0:
          return BySmallestKey(f1, f2, internal_comparator);
      }
      assert(false);
      return false;
    }
  };

  struct LevelState {
    std::unordered_set<uint64_t> deleted_files;
    // Map from file number to file meta data.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
  };

  const EnvOptions& env_options_;
  Logger* info_log_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  int num_levels_;
  LevelState* levels_;
#ifdef INDIRECT_VALUE_SUPPORT
  // We accumulate all the deletions owing to compaction/copy (which deleted using FileMetaData rather than a file number)
  // here, to be applied in SaveTo
  std::vector<FileMetaData*> retiredfiles;
  std::vector<VLogRingRestartInfo> vlog_additions;   // files and bytes added/deleted
#endif
  // Store states of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::map<int, std::unordered_set<uint64_t>> invalid_levels_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;
#ifdef INDIRECT_VALUE_SUPPORT
  friend void VersionBuilder::SaveTo(VersionStorageInfo*, ColumnFamilyData *);
#endif

 public:
  Rep(const EnvOptions& env_options, Logger* info_log, TableCache* table_cache,
      VersionStorageInfo* base_vstorage)
      : env_options_(env_options),
        info_log_(info_log),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        num_levels_(base_vstorage->num_levels()),
        has_invalid_levels_(false) {
    levels_ = new LevelState[num_levels_];
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
    level_nonzero_cmp_.internal_comparator =
        base_vstorage_->InternalComparator();
  }

  ~Rep() {
    for (int level = 0; level < num_levels_; level++) {
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        UnrefFile(pair.second);
      }
    }

    delete[] levels_;
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        table_cache_->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
#ifdef INDIRECT_VALUE_SUPPORT
      // The SST is about to be deleted.  Remove it from any VLog queues it is attached to.
      // We have to do this explicitly rather than in a destructor because FileMetaData blocks get copied & put on queues
      // with no regard for ownership.  Rather than try to enforce no-copy semantics everywhere we root out all the delete calls and put this there
      if(f->vlog)f->vlog->VLogSstDelete(*f);
      // it is possible that files on the added list were never actually added to the rings.  Those files will
      // not have a vlog pointer so we won't try to take them off the rings.
#endif
      delete f;
    }
  }

  void CheckConsistency(VersionStorageInfo* vstorage) {
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // make sure the files are sorted correctly
    for (int level = 0; level < num_levels_; level++) {
      auto& level_files = vstorage->LevelFiles(level);
      for (size_t i = 1; i < level_files.size(); i++) {
        auto f1 = level_files[i - 1];
        auto f2 = level_files[i];
        if (level == 0) {
          if (!level_zero_cmp_(f1, f2)) {
            fprintf(stderr, "L0 files are not sorted properly");
            abort();
          }

          if (f2->fd.smallest_seqno == f2->fd.largest_seqno) {
            // This is an external file that we ingested
            SequenceNumber external_file_seqno = f2->fd.smallest_seqno;
            if (!(external_file_seqno < f1->fd.largest_seqno ||
                  external_file_seqno == 0)) {
              fprintf(stderr,
                      "L0 file with seqno %" PRIu64 " %" PRIu64
                              " vs. file with global_seqno %" PRIu64 "\n",
                      f1->fd.smallest_seqno, f1->fd.largest_seqno,
                      external_file_seqno);
              abort();
            }
          } else if (f1->fd.smallest_seqno <= f2->fd.smallest_seqno) {
            fprintf(stderr,
                    "L0 files seqno %" PRIu64 " %" PRIu64 " vs. %" PRIu64
                    " %" PRIu64 "\n",
                    f1->fd.smallest_seqno, f1->fd.largest_seqno,
                    f2->fd.smallest_seqno, f2->fd.largest_seqno);
            abort();
          }
        } else {
          if (!level_nonzero_cmp_(f1, f2)) {
            fprintf(stderr, "L%d files are not sorted properly", level);
            abort();
          }

          // Make sure there is no overlap in levels > 0
          if (vstorage->InternalComparator()->Compare(f1->largest,
                                                      f2->smallest) >= 0) {
            fprintf(stderr, "L%d have overlapping ranges %s vs. %s\n", level,
                    (f1->largest).DebugString(true).c_str(),
                    (f2->smallest).DebugString(true).c_str());
            abort();
          }
        }
      }
    }
  }

  void CheckConsistencyForDeletes(VersionEdit* /*edit*/, uint64_t number,
                                  int level) {
#ifdef NDEBUG
    if (!base_vstorage_->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // a file to be deleted better exist in the previous version
    bool found = false;
    for (int l = 0; !found && l < num_levels_; l++) {
      const std::vector<FileMetaData*>& base_files =
          base_vstorage_->LevelFiles(l);
      for (size_t i = 0; i < base_files.size(); i++) {
        FileMetaData* f = base_files[i];
        if (f->fd.GetNumber() == number) {
          found = true;
          break;
        }
      }
    }
    // if the file did not exist in the previous version, then it
    // is possibly moved from lower level to higher level in current
    // version
    for (int l = level + 1; !found && l < num_levels_; l++) {
      auto& level_added = levels_[l].added_files;
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
        break;
      }
    }

    // maybe this file was added in a previous edit that was Applied
    if (!found) {
      auto& level_added = levels_[level].added_files;
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
      }
    }
    if (!found) {
      fprintf(stderr, "not found %" PRIu64 "\n", number);
      abort();
    }
  }

  bool CheckConsistencyForNumLevels() {
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }
    for (auto& level : invalid_levels_) {
      if (level.second.size() > 0) {
        return false;
      }
    }
    return true;
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    CheckConsistency(base_vstorage_);
#if DEBLEVEL&32
printf("Apply:");
#endif

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->GetDeletedFiles();
    for (const auto& del_file : del) {
      const auto level = del_file.first;
      const auto number = del_file.second;
      if (level < num_levels_) {
#if DEBLEVEL&32
printf(" %zd",number);
#endif
        levels_[level].deleted_files.insert(number);
        CheckConsistencyForDeletes(edit, number, level);

        auto exising = levels_[level].added_files.find(number);
        if (exising != levels_[level].added_files.end()) {
          UnrefFile(exising->second);
          levels_[level].added_files.erase(exising);
        }
      } else {
        auto exising = invalid_levels_[level].find(number);
        if (exising != invalid_levels_[level].end()) {
          invalid_levels_[level].erase(exising);
        } else {
          // Deleting an non-existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }
#ifdef INDIRECT_VALUE_SUPPORT
    // Move the files marked for deletion by compaction/copy (which were deleted with a pointer to the FileMetaData) to the deletion
    // list for this rep.  We will delete them during SaveTo, where we have access to the CF
    const std::vector<FileMetaData*>& retiring = edit->GetRetiringFiles();
    for (const auto& retiringsst : retiring) {
      retiredfiles.push_back(retiringsst);
    }

    // Accumulate the VLog changes from the edits.  These are all added files and stats changes
    Coalesce(vlog_additions,edit->VLogAdditions(),true);
#endif
#if DEBLEVEL&32
printf("\n");
#endif

    // Add new files
    for (const auto& new_file : edit->GetNewFiles()) {
      const int level = new_file.first;
      if (level < num_levels_) {
        FileMetaData* f = new FileMetaData(new_file.second);
        f->refs = 1;
#if DEBLEVEL&512
if(level>1&&f->indirect_ref_0.size()==0)
  printf("file with no ref0\n");
#endif
        assert(levels_[level].added_files.find(f->fd.GetNumber()) ==
               levels_[level].added_files.end());
        levels_[level].deleted_files.erase(f->fd.GetNumber());
        levels_[level].added_files[f->fd.GetNumber()] = f;
      } else {
        uint64_t number = new_file.second.fd.GetNumber();
        if (invalid_levels_[level].count(number) == 0) {
          invalid_levels_[level].insert(number);
        } else {
          // Creating an already existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }
  }

  // Save the current state in *v.
  void SaveTo(VersionStorageInfo* vstorage) {
#if DEBLEVEL&32
printf("SaveTo: starting files:");
    for (int level = 0; level < num_levels_; level++) {
      const auto& base_files = base_vstorage_->LevelFiles(level);
      auto base_iter = base_files.begin();
      auto base_end = base_files.end();
      for (; base_iter != base_end; ++base_iter) {
        printf(" %p",*base_iter);
      }
    }
printf("\nfiles after editing:");
#endif
    CheckConsistency(base_vstorage_);
    CheckConsistency(vstorage);

    for (int level = 0; level < num_levels_; level++) {
      const auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const auto& base_files = base_vstorage_->LevelFiles(level);
      const auto& unordered_added_files = levels_[level].added_files;
      vstorage->Reserve(level,
                        base_files.size() + unordered_added_files.size());

      // Sort added files for the level.
      std::vector<FileMetaData*> added_files;
      added_files.reserve(unordered_added_files.size());
      for (const auto& pair : unordered_added_files) {
        added_files.push_back(pair.second);
      }
      std::sort(added_files.begin(), added_files.end(), cmp);

#ifndef NDEBUG
      FileMetaData* prev_added_file = nullptr;
      for (const auto& added : added_files) {
        if (level > 0 && prev_added_file != nullptr) {
          assert(base_vstorage_->InternalComparator()->Compare(
                     prev_added_file->smallest, added->smallest) <= 0);
        }
        prev_added_file = added;
      }
#endif

      auto base_iter = base_files.begin();
      auto base_end = base_files.end();
      auto added_iter = added_files.begin();
      auto added_end = added_files.end();
      while (added_iter != added_end || base_iter != base_end) {
        if (base_iter == base_end ||
                (added_iter != added_end && cmp(*added_iter, *base_iter))) {
          MaybeAddFile(vstorage, level, *added_iter++);
        } else {
          MaybeAddFile(vstorage, level, *base_iter++);
        }
      }
      }

    CheckConsistency(vstorage);
#if DEBLEVEL&32
printf("\n");
#endif
  }

  void LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                         bool prefetch_index_and_filter_in_cache,
                         const SliceTransform* prefix_extractor) {
    assert(table_cache_ != nullptr);
    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level].added_files) {
        auto* file_meta = file_meta_pair.second;
        assert(!file_meta->table_reader_handle);
        files_meta.emplace_back(file_meta, level);
      }
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> load_handlers_func([&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx].first;
        int level = files_meta[file_idx].second;
        table_cache_->FindTable(
            env_options_, *(base_vstorage_->InternalComparator()),
            file_meta->fd, &file_meta->table_reader_handle, prefix_extractor,
                                false /*no_io */, true /* record_read_stats */,
            internal_stats->GetFileReadHist(level), false, level,
            prefetch_index_and_filter_in_cache);
        if (file_meta->table_reader_handle != nullptr) {
          // Load table_reader
          file_meta->fd.table_reader = table_cache_->GetTableReaderFromHandle(
              file_meta->table_reader_handle);
        }
      }
    });

      std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
        threads.emplace_back(load_handlers_func);
      }
    load_handlers_func();
      for (auto& t : threads) {
        t.join();
      }
    }

  void MaybeAddFile(VersionStorageInfo* vstorage, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->fd.GetNumber()) > 0) {
      // f is to-be-deleted table file
#if DEBLEVEL&32
printf(" ~%p(%zd)",f,f->fd.GetNumber());
#endif
      vstorage->RemoveCurrentStats(f);
    } else {
#if DEBLEVEL&32
printf(" %p",f);
#endif
      vstorage->AddFile(level, f, info_log_);
    }
  }
};

VersionBuilder::VersionBuilder(const EnvOptions& env_options,
                               TableCache* table_cache,
                               VersionStorageInfo* base_vstorage,
                               Logger* info_log)
    : rep_(new Rep(env_options, info_log, table_cache, base_vstorage)) {}

VersionBuilder::~VersionBuilder() { delete rep_; }

void VersionBuilder::CheckConsistency(VersionStorageInfo* vstorage) {
  rep_->CheckConsistency(vstorage);
}

void VersionBuilder::CheckConsistencyForDeletes(VersionEdit* edit,
                                                uint64_t number, int level) {
  rep_->CheckConsistencyForDeletes(edit, number, level);
}

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

void VersionBuilder::Apply(VersionEdit* edit) { rep_->Apply(edit); }

#ifdef INDIRECT_VALUE_SUPPORT
std::vector<VLogRingRestartInfo> VersionBuilder::VLogAdditions() { return rep_->vlog_additions; }
#endif

#ifdef INDIRECT_VALUE_SUPPORT
void VersionBuilder::SaveTo(VersionStorageInfo* vstorage, ColumnFamilyData *cfd) {
#else
void VersionBuilder::SaveTo(VersionStorageInfo* vstorage, ColumnFamilyData* /*cfd*/) {
#endif
#ifdef INDIRECT_VALUE_SUPPORT
  // We are about to commit the added_files in rep_ to the new version vstorage.  This is a safe time
  // to enter their info into the Value Log.  We could come here either from initial recovery or from finishing a
  // compaction/flush.  SaveTo() is also called from DumpManifest to build a faux version for dumping, and in tests; in that case we
  // make cfd null so we skip the value log work.  We also skip the Value Log work if there is no VLog for the cfd
  if(cfd!=nullptr && cfd->vlog()){
    VLog *vlog = cfd->vlog().get();
    // Go through each added file.  They have been split by levels, which makes work here
    for (int level = 0; level < rep_->num_levels_; level++) {  // for each level...
      for (auto& pair : rep_->levels_[level].added_files) {  // for each number/FileMetaData pair, representing 1 file...
        vlog->VLogSstInstall(*pair.second);  // register the new file with the VLogRings
      }
    }
    // Likewise, we need to ratify the files that are about to be removed from the current version.  We don't need or want to do anything here during
    // recovery.  By good fortune, recovery, which plays back the manifest, refers to deletions by file namber, while all deletions during the running
    // system use a pointer to the FileMetaData.  So, we can easily distinguish deletes from the manifest (ignored) from deletes from compaction/copy (processed)
    // 'Deleting' has no immediate effect on the file, which is still active in the
    // current version.  It will eventually be deleted when its usecount goes to 0; at that point we will destroy the
    // FileMetaData and decrement the usecount for it in the queue.
    // We have to UnCurrent the file so that we treat its eventual deletion as a signal to delete any VLog files that its
    // removal makes unnecessary; without this mark the VLog files will hang around, as they must if the database is merely closed.
    for(auto retiringsst : rep_->retiredfiles) { 
      vlog->VLogSstUnCurrent(*retiringsst);  // mark the file as dormant
    }

  }
#endif
  rep_->SaveTo(vstorage);
}

void VersionBuilder::LoadTableHandlers(InternalStats* internal_stats,
                                       int max_threads,
                                       bool prefetch_index_and_filter_in_cache,
                                       const SliceTransform* prefix_extractor) {
  rep_->LoadTableHandlers(internal_stats, max_threads,
                          prefetch_index_and_filter_in_cache, prefix_extractor);
}

void VersionBuilder::MaybeAddFile(VersionStorageInfo* vstorage, int level,
                                  FileMetaData* f) {
  rep_->MaybeAddFile(vstorage, level, f);
}

}  // namespace rocksdb
