//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction_picker.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include <vector>
#include "db/column_family.h"
#include "monitoring/statistics.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

namespace {
uint64_t TotalCompensatedFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->compensated_file_size;
  }
  return sum;
}
}  // anonymous namespace

bool FindIntraL0Compaction(const std::vector<FileMetaData*>& level_files,
                           size_t min_files_to_compact,
                           uint64_t max_compact_bytes_per_del_file,
                           CompactionInputFiles* comp_inputs) {
  size_t compact_bytes = static_cast<size_t>(level_files[0]->fd.file_size);
  size_t compact_bytes_per_del_file = port::kMaxSizet;
  // compaction range will be [0, span_len).
  size_t span_len;
  // pull in files until the amount of compaction work per deleted file begins
  // increasing.
  size_t new_compact_bytes_per_del_file = 0;
  for (span_len = 1; span_len < level_files.size(); ++span_len) {
    compact_bytes += static_cast<size_t>(level_files[span_len]->fd.file_size);
    new_compact_bytes_per_del_file = compact_bytes / span_len;
    if (level_files[span_len]->being_compacted ||
        new_compact_bytes_per_del_file > compact_bytes_per_del_file) {
      break;
    }
    compact_bytes_per_del_file = new_compact_bytes_per_del_file;
  }

  if (span_len >= min_files_to_compact &&
      compact_bytes_per_del_file < max_compact_bytes_per_del_file) {
    assert(comp_inputs != nullptr);
    comp_inputs->level = 0;
    for (size_t i = 0; i < span_len; ++i) {
      comp_inputs->files.push_back(level_files[i]);
    }
    return true;
  }
  return false;
}

// Determine compression type, based on user options, level of the output
// file and whether compression is disabled.
// If enable_compression is false, then compression is always disabled no
// matter what the values of the other two parameters are.
// Otherwise, the compression type is determined based on options and level.
CompressionType GetCompressionType(const ImmutableCFOptions& ioptions,
                                   const VersionStorageInfo* vstorage,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level, int base_level,
                                   const bool enable_compression) {
  if (!enable_compression) {
    // disable compression
    return kNoCompression;
  }

  // If bottommost_compression is set and we are compacting to the
  // bottommost level then we should use it.
  if (ioptions.bottommost_compression != kDisableCompressionOption &&
      level >= (vstorage->num_non_empty_levels() - 1)) {
    return ioptions.bottommost_compression;
  }
  // If the user has specified a different compression level for each level,
  // then pick the compression for that level.
  if (!ioptions.compression_per_level.empty()) {
    assert(level == 0 || level >= base_level);
    int idx = (level == 0) ? 0 : level - base_level + 1;

    const int n = static_cast<int>(ioptions.compression_per_level.size()) - 1;
    // It is possible for level_ to be -1; in that case, we use level
    // 0's compression.  This occurs mostly in backwards compatibility
    // situations when the builder doesn't know what level the file
    // belongs to.  Likewise, if level is beyond the end of the
    // specified compression levels, use the last value.
    return ioptions.compression_per_level[std::max(0, std::min(idx, n))];
  } else {
    return mutable_cf_options.compression;
  }
}

CompressionOptions GetCompressionOptions(const ImmutableCFOptions& ioptions,
                                         const VersionStorageInfo* vstorage,
                                         int level,
                                         const bool enable_compression) {
  if (!enable_compression) {
    return ioptions.compression_opts;
  }
  // If bottommost_compression is set and we are compacting to the
  // bottommost level then we should use the specified compression options
  // for the bottmomost_compression.
  if (ioptions.bottommost_compression != kDisableCompressionOption &&
      level >= (vstorage->num_non_empty_levels() - 1) &&
      ioptions.bottommost_compression_opts.enabled) {
    return ioptions.bottommost_compression_opts;
  }
  return ioptions.compression_opts;
}

CompactionPicker::CompactionPicker(const ImmutableCFOptions& ioptions,
                                   const InternalKeyComparator* icmp)
    : ioptions_(ioptions), icmp_(icmp) {}

CompactionPicker::~CompactionPicker() {}

// Delete this compaction from the list of running compactions.
void CompactionPicker::ReleaseCompactionFiles(Compaction* c, Status status) {
  UnregisterCompaction(c);
  if (!status.ok()) {
    c->ResetNextCompactionIndex();
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  const int level = inputs.level;
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();

  if (level == 0) {
    for (size_t i = 0; i < inputs.size(); i++) {
      FileMetaData* f = inputs[i];
      if (i == 0) {
        *smallest = f->smallest;
        *largest = f->largest;
      } else {
        if (icmp_->Compare(f->smallest, *smallest) < 0) {
          *smallest = f->smallest;
        }
        if (icmp_->Compare(f->largest, *largest) > 0) {
          *largest = f->largest;
        }
      }
    }
  } else {
    *smallest = inputs[0]->smallest;
    *largest = inputs[inputs.size() - 1]->largest;
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs1,
                                const CompactionInputFiles& inputs2,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  assert(!inputs1.empty() || !inputs2.empty());
  if (inputs1.empty()) {
    GetRange(inputs2, smallest, largest);
  } else if (inputs2.empty()) {
    GetRange(inputs1, smallest, largest);
  } else {
    InternalKey smallest1, smallest2, largest1, largest2;
    GetRange(inputs1, &smallest1, &largest1);
    GetRange(inputs2, &smallest2, &largest2);
    *smallest =
        icmp_->Compare(smallest1, smallest2) < 0 ? smallest1 : smallest2;
    *largest = icmp_->Compare(largest1, largest2) < 0 ? largest2 : largest1;
  }
}

void CompactionPicker::GetRange(const std::vector<CompactionInputFiles>& inputs,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  InternalKey current_smallest;
  InternalKey current_largest;
  bool initialized = false;
  for (const auto& in : inputs) {
    if (in.empty()) {
      continue;
    }
    GetRange(in, &current_smallest, &current_largest);
    if (!initialized) {
      *smallest = current_smallest;
      *largest = current_largest;
      initialized = true;
    } else {
      if (icmp_->Compare(current_smallest, *smallest) < 0) {
        *smallest = current_smallest;
      }
      if (icmp_->Compare(current_largest, *largest) > 0) {
        *largest = current_largest;
      }
    }
  }
  assert(initialized);
}

bool CompactionPicker::ExpandInputsToCleanCut(const std::string& /*cf_name*/,
                                              VersionStorageInfo* vstorage,
                                              CompactionInputFiles* inputs,
                                              InternalKey** next_smallest) {
  // This isn't good compaction
  assert(!inputs->empty());

  const int level = inputs->level;
  // GetOverlappingInputs will always do the right thing for level-0.
  // So we don't need to do any expansion if level == 0.
  if (level == 0) {
    return true;
  }

  InternalKey smallest, largest;

  // Keep expanding inputs until we are sure that there is a "clean cut"
  // boundary between the files in input and the surrounding files.
  // This will ensure that no parts of a key are lost during compaction.
  int hint_index = -1;
  size_t old_size;
  do {
    old_size = inputs->size();
    GetRange(*inputs, &smallest, &largest);
    inputs->clear();
    vstorage->GetOverlappingInputs(level, &smallest, &largest, &inputs->files,
                                   hint_index, &hint_index, true,
                                   next_smallest);
  } while (inputs->size() > old_size);

  // we started off with inputs non-empty and the previous loop only grew
  // inputs. thus, inputs should be non-empty here
  assert(!inputs->empty());

  // If, after the expansion, there are files that are already under
  // compaction, then we must drop/cancel this compaction.
  if (AreFilesInCompaction(inputs->files)) {
    return false;
  }
  return true;
}

bool CompactionPicker::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int level) const {
  const Comparator* ucmp = icmp_->user_comparator();
  for (Compaction* c : compactions_in_progress_) {
    if (c->output_level() == level &&
        ucmp->Compare(smallest_user_key, c->GetLargestUserKey()) <= 0 &&
        ucmp->Compare(largest_user_key, c->GetSmallestUserKey()) >= 0) {
      // Overlap
      return true;
    }
  }
  // Did not overlap with any running compaction in level `level`
  return false;
}

bool CompactionPicker::FilesRangeOverlapWithCompaction(
    const std::vector<CompactionInputFiles>& inputs, int level) const {
  bool is_empty = true;
  for (auto& in : inputs) {
    if (!in.empty()) {
      is_empty = false;
      break;
    }
  }
  if (is_empty) {
    // No files in inputs
    return false;
  }

  InternalKey smallest, largest;
  GetRange(inputs, &smallest, &largest);
  return RangeOverlapWithCompaction(smallest.user_key(), largest.user_key(),
                                    level);
}

// Returns true if any one of specified files are being compacted
bool CompactionPicker::AreFilesInCompaction(
    const std::vector<FileMetaData*>& files) {
  for (size_t i = 0; i < files.size(); i++) {
    if (files[i]->being_compacted) {
      return true;
    }
  }
  return false;
}

Compaction* CompactionPicker::CompactFiles(
    const CompactionOptions& compact_options,
    const std::vector<CompactionInputFiles>& input_files, int output_level,
    VersionStorageInfo* vstorage, const MutableCFOptions& mutable_cf_options,
    uint32_t output_path_id) {
  assert(input_files.size());
  // This compaction output should not overlap with a running compaction as
  // `SanitizeCompactionInputFiles` should've checked earlier and db mutex
  // shouldn't have been released since.
  assert(!FilesRangeOverlapWithCompaction(input_files, output_level));

  CompressionType compression_type;
  if (compact_options.compression == kDisableCompressionOption) {
    int base_level;
    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      base_level = vstorage->base_level();
    } else {
      base_level = 1;
    }
    compression_type =
        GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                           output_level, base_level);
  } else {
    // TODO(ajkr): `CompactionOptions` offers configurable `CompressionType`
    // without configurable `CompressionOptions`, which is inconsistent.
    compression_type = compact_options.compression;
  }
  auto c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, input_files, output_level,
      compact_options.output_file_size_limit,
      mutable_cf_options.max_compaction_bytes, output_path_id, compression_type,
      GetCompressionOptions(ioptions_, vstorage, output_level),
      compact_options.max_subcompactions,
      /* grandparents */ {}, true);
  RegisterCompaction(c);
  return c;
}

Status CompactionPicker::GetCompactionInputsFromFileNumbers(
    std::vector<CompactionInputFiles>* input_files,
    std::unordered_set<uint64_t>* input_set, const VersionStorageInfo* vstorage,
    const CompactionOptions& /*compact_options*/) const {
  if (input_set->size() == 0U) {
    return Status::InvalidArgument(
        "Compaction must include at least one file.");
  }
  assert(input_files);

  std::vector<CompactionInputFiles> matched_input_files;
  matched_input_files.resize(vstorage->num_levels());
  int first_non_empty_level = -1;
  int last_non_empty_level = -1;
  // TODO(yhchiang): use a lazy-initialized mapping from
  //                 file_number to FileMetaData in Version.
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    for (auto file : vstorage->LevelFiles(level)) {
      auto iter = input_set->find(file->fd.GetNumber());
      if (iter != input_set->end()) {
        matched_input_files[level].files.push_back(file);
        input_set->erase(iter);
        last_non_empty_level = level;
        if (first_non_empty_level == -1) {
          first_non_empty_level = level;
        }
      }
    }
  }

  if (!input_set->empty()) {
    std::string message(
        "Cannot find matched SST files for the following file numbers:");
    for (auto fn : *input_set) {
      message += " ";
      message += ToString(fn);
    }
    return Status::InvalidArgument(message);
  }

  for (int level = first_non_empty_level; level <= last_non_empty_level;
       ++level) {
    matched_input_files[level].level = level;
    input_files->emplace_back(std::move(matched_input_files[level]));
  }

  return Status::OK();
}

// Returns true if any one of the parent files are being compacted
bool CompactionPicker::IsRangeInCompaction(VersionStorageInfo* vstorage,
                                           const InternalKey* smallest,
                                           const InternalKey* largest,
                                           int level, int* level_index) {
  std::vector<FileMetaData*> inputs;
  assert(level < NumberLevels());

  vstorage->GetOverlappingInputs(level, smallest, largest, &inputs,
                                 level_index ? *level_index : 0, level_index);
  return AreFilesInCompaction(inputs);
}

// Populates the set of inputs of all other levels that overlap with the
// start level.
// Now we assume all levels except start level and output level are empty.
// Will also attempt to expand "start level" if that doesn't expand
// "output level" or cause "level" to include a file for compaction that has an
// overlapping user-key with another file.
// REQUIRES: input_level and output_level are different
// REQUIRES: inputs->empty() == false
// Returns false if files on parent level are currently in compaction, which
// means that we can't compact them
bool CompactionPicker::SetupOtherInputs(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, CompactionInputFiles* inputs,
    CompactionInputFiles* output_level_inputs, int* parent_index,
    int base_index) {
  assert(!inputs->empty());
  assert(output_level_inputs->empty());
  const int input_level = inputs->level;
  const int output_level = output_level_inputs->level;
  if (input_level == output_level) {
    // no possibility of conflict
    return true;
  }

  // For now, we only support merging two levels, start level and output level.
  // We need to assert other levels are empty.
  for (int l = input_level + 1; l < output_level; l++) {
    assert(vstorage->NumLevelFiles(l) == 0);
  }

  InternalKey smallest, largest;

  // Get the range one last time.
  GetRange(*inputs, &smallest, &largest);

  // Populate the set of next-level files (inputs_GetOutputLevelInputs()) to
  // include in compaction
  vstorage->GetOverlappingInputs(output_level, &smallest, &largest,
                                 &output_level_inputs->files, *parent_index,
                                 parent_index);
  if (AreFilesInCompaction(output_level_inputs->files)) {
    return false;
  }
  if (!output_level_inputs->empty()) {
    if (!ExpandInputsToCleanCut(cf_name, vstorage, output_level_inputs)) {
      return false;
    }
  }

  // See if we can further grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up. We also choose NOT
  // to expand if this would cause "level" to include some entries for some
  // user key, while excluding other entries for the same user key. This
  // can happen when one user key spans multiple files.
  if (!output_level_inputs->empty()) {
    const uint64_t limit = mutable_cf_options.max_compaction_bytes;
    const uint64_t output_level_inputs_size =
        TotalCompensatedFileSize(output_level_inputs->files);
    const uint64_t inputs_size = TotalCompensatedFileSize(inputs->files);
    bool expand_inputs = false;

    CompactionInputFiles expanded_inputs;
    expanded_inputs.level = input_level;
    // Get closed interval of output level
    InternalKey all_start, all_limit;
    GetRange(*inputs, *output_level_inputs, &all_start, &all_limit);
    bool try_overlapping_inputs = true;
    vstorage->GetOverlappingInputs(input_level, &all_start, &all_limit,
                                   &expanded_inputs.files, base_index, nullptr);
    uint64_t expanded_inputs_size =
        TotalCompensatedFileSize(expanded_inputs.files);
    if (!ExpandInputsToCleanCut(cf_name, vstorage, &expanded_inputs)) {
      try_overlapping_inputs = false;
    }
    if (try_overlapping_inputs && expanded_inputs.size() > inputs->size() &&
        output_level_inputs_size + expanded_inputs_size < limit &&
        !AreFilesInCompaction(expanded_inputs.files)) {
      InternalKey new_start, new_limit;
      GetRange(expanded_inputs, &new_start, &new_limit);
      CompactionInputFiles expanded_output_level_inputs;
      expanded_output_level_inputs.level = output_level;
      vstorage->GetOverlappingInputs(output_level, &new_start, &new_limit,
                                     &expanded_output_level_inputs.files,
                                     *parent_index, parent_index);
      assert(!expanded_output_level_inputs.empty());
      if (!AreFilesInCompaction(expanded_output_level_inputs.files) &&
          ExpandInputsToCleanCut(cf_name, vstorage,
                                 &expanded_output_level_inputs) &&
          expanded_output_level_inputs.size() == output_level_inputs->size()) {
        expand_inputs = true;
      }
    }
    if (!expand_inputs) {
      vstorage->GetCleanInputsWithinInterval(input_level, &all_start,
                                             &all_limit, &expanded_inputs.files,
                                             base_index, nullptr);
      expanded_inputs_size = TotalCompensatedFileSize(expanded_inputs.files);
      if (expanded_inputs.size() > inputs->size() &&
          output_level_inputs_size + expanded_inputs_size < limit &&
          !AreFilesInCompaction(expanded_inputs.files)) {
        expand_inputs = true;
      }
    }
    if (expand_inputs) {
      ROCKS_LOG_INFO(ioptions_.info_log,
                     "[%s] Expanding@%d %" ROCKSDB_PRIszt "+%" ROCKSDB_PRIszt
                     "(%" PRIu64 "+%" PRIu64 " bytes) to %" ROCKSDB_PRIszt
                     "+%" ROCKSDB_PRIszt " (%" PRIu64 "+%" PRIu64 " bytes)\n",
                     cf_name.c_str(), input_level, inputs->size(),
                     output_level_inputs->size(), inputs_size,
                     output_level_inputs_size, expanded_inputs.size(),
                     output_level_inputs->size(), expanded_inputs_size,
                     output_level_inputs_size);
      inputs->files = expanded_inputs.files;
    }
  }
  return true;
}

void CompactionPicker::GetGrandparents(
    VersionStorageInfo* vstorage, const CompactionInputFiles& inputs,
    const CompactionInputFiles& output_level_inputs,
    std::vector<FileMetaData*>* grandparents) {
  InternalKey start, limit;
  GetRange(inputs, output_level_inputs, &start, &limit);
  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (output_level_inputs.level + 1 < NumberLevels()) {
    vstorage->GetOverlappingInputs(output_level_inputs.level + 1, &start,
                                   &limit, grandparents);
  }
}

Compaction* CompactionPicker::CompactRange(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, int input_level, int output_level,
    uint32_t output_path_id, uint32_t max_subcompactions,
    const InternalKey* begin, const InternalKey* end,
    InternalKey** compaction_end, bool* manual_conflict) {
  // CompactionPickerFIFO has its own implementation of compact range
  assert(ioptions_.compaction_style != kCompactionStyleFIFO);

  if (input_level == ColumnFamilyData::kCompactAllLevels) {
    assert(ioptions_.compaction_style == kCompactionStyleUniversal);

    // Universal compaction with more than one level always compacts all the
    // files together to the last level.
    assert(vstorage->num_levels() > 1);
    // DBImpl::CompactRange() set output level to be the last level
    if (ioptions_.allow_ingest_behind) {
      assert(output_level == vstorage->num_levels() - 2);
    } else {
      assert(output_level == vstorage->num_levels() - 1);
    }
    // DBImpl::RunManualCompaction will make full range for universal compaction
    assert(begin == nullptr);
    assert(end == nullptr);
    *compaction_end = nullptr;

    int start_level = 0;
    for (; start_level < vstorage->num_levels() &&
           vstorage->NumLevelFiles(start_level) == 0;
         start_level++) {
    }
    if (start_level == vstorage->num_levels()) {
      return nullptr;
    }

    if ((start_level == 0) && (!level0_compactions_in_progress_.empty())) {
      *manual_conflict = true;
      // Only one level 0 compaction allowed
      return nullptr;
    }

    std::vector<CompactionInputFiles> inputs(vstorage->num_levels() -
                                             start_level);
    for (int level = start_level; level < vstorage->num_levels(); level++) {
      inputs[level - start_level].level = level;
      auto& files = inputs[level - start_level].files;
      for (FileMetaData* f : vstorage->LevelFiles(level)) {
        files.push_back(f);
      }
      if (AreFilesInCompaction(files)) {
        *manual_conflict = true;
        return nullptr;
      }
    }

    // 2 non-exclusive manual compactions could run at the same time producing
    // overlaping outputs in the same level.
    if (FilesRangeOverlapWithCompaction(inputs, output_level)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      *manual_conflict = true;
      return nullptr;
    }

    Compaction* c = new Compaction(
        vstorage, ioptions_, mutable_cf_options, std::move(inputs),
        output_level,
        MaxFileSizeForLevel(mutable_cf_options, output_level,
                            ioptions_.compaction_style),
        /* max_compaction_bytes */ LLONG_MAX, output_path_id,
        GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                           output_level, 1),
        GetCompressionOptions(ioptions_, vstorage, output_level),
        max_subcompactions, /* grandparents */ {}, /* is manual */ true);
    RegisterCompaction(c);
    return c;
  }

  CompactionInputFiles inputs;
  inputs.level = input_level;
  bool covering_the_whole_range = true;

  // All files are 'overlapping' in universal style compaction.
  // We have to compact the entire range in one shot.
  if (ioptions_.compaction_style == kCompactionStyleUniversal) {
    begin = nullptr;
    end = nullptr;
  }

  vstorage->GetOverlappingInputs(input_level, begin, end, &inputs.files);
  if (inputs.empty()) {
    return nullptr;
  }

  if ((input_level == 0) && (!level0_compactions_in_progress_.empty())) {
    // Only one level 0 compaction allowed
    TEST_SYNC_POINT("CompactionPicker::CompactRange:Conflict");
    *manual_conflict = true;
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (input_level > 0) {
    const uint64_t limit = mutable_cf_options.max_compaction_bytes;
    uint64_t total = 0;
    for (size_t i = 0; i + 1 < inputs.size(); ++i) {
      uint64_t s = inputs[i]->compensated_file_size;
      total += s;
      if (total >= limit) {
        covering_the_whole_range = false;
        inputs.files.resize(i + 1);
        break;
      }
    }
  }
  assert(output_path_id < static_cast<uint32_t>(ioptions_.cf_paths.size()));

  InternalKey key_storage;
  InternalKey* next_smallest = &key_storage;
  if (ExpandInputsToCleanCut(cf_name, vstorage, &inputs, &next_smallest) ==
      false) {
    // manual compaction is now multi-threaded, so it can
    // happen that ExpandWhileOverlapping fails
    // we handle it higher in RunManualCompaction
    *manual_conflict = true;
    return nullptr;
  }

  if (covering_the_whole_range || !next_smallest) {
    *compaction_end = nullptr;
  } else {
    **compaction_end = *next_smallest;
  }

  CompactionInputFiles output_level_inputs;
  if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
    assert(input_level == 0);
    output_level = vstorage->base_level();
    assert(output_level > 0);
  }
  output_level_inputs.level = output_level;
  if (input_level != output_level) {
    int parent_index = -1;
    if (!SetupOtherInputs(cf_name, mutable_cf_options, vstorage, &inputs,
                          &output_level_inputs, &parent_index, -1)) {
      // manual compaction is now multi-threaded, so it can
      // happen that SetupOtherInputs fails
      // we handle it higher in RunManualCompaction
      *manual_conflict = true;
      return nullptr;
    }
  }

  std::vector<CompactionInputFiles> compaction_inputs({inputs});
  if (!output_level_inputs.empty()) {
    compaction_inputs.push_back(output_level_inputs);
  }
  for (size_t i = 0; i < compaction_inputs.size(); i++) {
    if (AreFilesInCompaction(compaction_inputs[i].files)) {
      *manual_conflict = true;
      return nullptr;
    }
  }

  // 2 non-exclusive manual compactions could run at the same time producing
  // overlaping outputs in the same level.
  if (FilesRangeOverlapWithCompaction(compaction_inputs, output_level)) {
    // This compaction output could potentially conflict with the output
    // of a currently running compaction, we cannot run it.
    *manual_conflict = true;
    return nullptr;
  }

  std::vector<FileMetaData*> grandparents;
  GetGrandparents(vstorage, inputs, output_level_inputs, &grandparents);
  Compaction* compaction = new Compaction(
      vstorage, ioptions_, mutable_cf_options, std::move(compaction_inputs),
      output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          ioptions_.compaction_style, vstorage->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options.max_compaction_bytes, output_path_id,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, output_level,
                         vstorage->base_level()),
      GetCompressionOptions(ioptions_, vstorage, output_level),
      max_subcompactions, std::move(grandparents),
      /* is manual compaction */ true);

  TEST_SYNC_POINT_CALLBACK("CompactionPicker::CompactRange:Return", compaction);
  RegisterCompaction(compaction);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options);

  return compaction;
}

#ifndef ROCKSDB_LITE
namespace {
// Test whether two files have overlapping key-ranges.
bool HaveOverlappingKeyRanges(const Comparator* c, const SstFileMetaData& a,
                              const SstFileMetaData& b) {
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
}  // namespace

Status CompactionPicker::SanitizeCompactionInputFilesForAllLevels(
    std::unordered_set<uint64_t>* input_files,
    const ColumnFamilyMetaData& cf_meta, const int output_level) const {
  auto& levels = cf_meta.levels;
  auto comparator = icmp_->user_comparator();

  // TODO(yhchiang): add is_adjustable to CompactionOptions

  // the smallest and largest key of the current compaction input
  std::string smallestkey;
  std::string largestkey;
  // a flag for initializing smallest and largest key
  bool is_first = false;
  const int kNotFound = -1;

  // For each level, it does the following things:
  // 1. Find the first and the last compaction input files
  //    in the current level.
  // 2. Include all files between the first and the last
  //    compaction input files.
  // 3. Update the compaction key-range.
  // 4. For all remaining levels, include files that have
  //    overlapping key-range with the compaction key-range.
  for (int l = 0; l <= output_level; ++l) {
    auto& current_files = levels[l].files;
    int first_included = static_cast<int>(current_files.size());
    int last_included = kNotFound;

    // identify the first and the last compaction input files
    // in the current level.
    for (size_t f = 0; f < current_files.size(); ++f) {
      if (input_files->find(TableFileNameToNumber(current_files[f].name)) !=
          input_files->end()) {
        first_included = std::min(first_included, static_cast<int>(f));
        last_included = std::max(last_included, static_cast<int>(f));
        if (is_first == false) {
          smallestkey = current_files[f].smallestkey;
          largestkey = current_files[f].largestkey;
          is_first = true;
        }
      }
    }
    if (last_included == kNotFound) {
      continue;
    }

    if (l != 0) {
      // expend the compaction input of the current level if it
      // has overlapping key-range with other non-compaction input
      // files in the same level.
      while (first_included > 0) {
        if (comparator->Compare(current_files[first_included - 1].largestkey,
                                current_files[first_included].smallestkey) <
            0) {
          break;
        }
        first_included--;
      }

      while (last_included < static_cast<int>(current_files.size()) - 1) {
        if (comparator->Compare(current_files[last_included + 1].smallestkey,
                                current_files[last_included].largestkey) > 0) {
          break;
        }
        last_included++;
      }
    } else if (output_level > 0) {
      last_included = static_cast<int>(current_files.size() - 1);
    }

    // include all files between the first and the last compaction input files.
    for (int f = first_included; f <= last_included; ++f) {
      if (current_files[f].being_compacted) {
        return Status::Aborted("Necessary compaction input file " +
                               current_files[f].name +
                               " is currently being compacted.");
      }
      input_files->insert(TableFileNameToNumber(current_files[f].name));
    }

    // update smallest and largest key
    if (l == 0) {
      for (int f = first_included; f <= last_included; ++f) {
        if (comparator->Compare(smallestkey, current_files[f].smallestkey) >
            0) {
          smallestkey = current_files[f].smallestkey;
        }
        if (comparator->Compare(largestkey, current_files[f].largestkey) < 0) {
          largestkey = current_files[f].largestkey;
        }
      }
    } else {
      if (comparator->Compare(smallestkey,
                              current_files[first_included].smallestkey) > 0) {
        smallestkey = current_files[first_included].smallestkey;
      }
      if (comparator->Compare(largestkey,
                              current_files[last_included].largestkey) < 0) {
        largestkey = current_files[last_included].largestkey;
      }
    }

    SstFileMetaData aggregated_file_meta;
    aggregated_file_meta.smallestkey = smallestkey;
    aggregated_file_meta.largestkey = largestkey;

    // For all lower levels, include all overlapping files.
    // We need to add overlapping files from the current level too because even
    // if there no input_files in level l, we would still need to add files
    // which overlap with the range containing the input_files in levels 0 to l
    // Level 0 doesn't need to be handled this way because files are sorted by
    // time and not by key
    for (int m = std::max(l, 1); m <= output_level; ++m) {
      for (auto& next_lv_file : levels[m].files) {
        if (HaveOverlappingKeyRanges(comparator, aggregated_file_meta,
                                     next_lv_file)) {
          if (next_lv_file.being_compacted) {
            return Status::Aborted(
                "File " + next_lv_file.name +
                " that has overlapping key range with one of the compaction "
                " input file is currently being compacted.");
          }
          input_files->insert(TableFileNameToNumber(next_lv_file.name));
        }
      }
    }
  }
  if (RangeOverlapWithCompaction(smallestkey, largestkey, output_level)) {
    return Status::Aborted(
        "A running compaction is writing to the same output level in an "
        "overlapping key range");
  }
  return Status::OK();
}

Status CompactionPicker::SanitizeCompactionInputFiles(
    std::unordered_set<uint64_t>* input_files,
    const ColumnFamilyMetaData& cf_meta, const int output_level) const {
  assert(static_cast<int>(cf_meta.levels.size()) - 1 ==
         cf_meta.levels[cf_meta.levels.size() - 1].level);
  if (output_level >= static_cast<int>(cf_meta.levels.size())) {
    return Status::InvalidArgument(
        "Output level for column family " + cf_meta.name +
        " must between [0, " +
        ToString(cf_meta.levels[cf_meta.levels.size() - 1].level) + "].");
  }

  if (output_level > MaxOutputLevel()) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm --- " +
        ToString(MaxOutputLevel()));
  }

  if (output_level < 0) {
    return Status::InvalidArgument("Output level cannot be negative.");
  }

  if (input_files->size() == 0) {
    return Status::InvalidArgument(
        "A compaction must contain at least one file.");
  }

  Status s = SanitizeCompactionInputFilesForAllLevels(input_files, cf_meta,
                                                      output_level);

  if (!s.ok()) {
    return s;
  }

  // for all input files, check whether the file number matches
  // any currently-existing files.
  for (auto file_num : *input_files) {
    bool found = false;
    for (const auto& level_meta : cf_meta.levels) {
      for (const auto& file_meta : level_meta.files) {
        if (file_num == TableFileNameToNumber(file_meta.name)) {
          if (file_meta.being_compacted) {
            return Status::Aborted("Specified compaction input file " +
                                   MakeTableFileName("", file_num) +
                                   " is already being compacted.");
          }
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (!found) {
      return Status::InvalidArgument(
          "Specified compaction input file " + MakeTableFileName("", file_num) +
          " does not exist in column family " + cf_meta.name + ".");
    }
  }

  return Status::OK();
}
#endif  // !ROCKSDB_LITE

void CompactionPicker::RegisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return;
  }
#ifdef INDIRECT_VALUE_SUPPORT
  if(c->compaction_reason() != CompactionReason::kActiveRecycling)  // if Active Recycling, we do not touch any keys
  // this IF-statement affects the compilation outside this conditional block!!!
#endif
  assert(ioptions_.compaction_style != kCompactionStyleLevel ||   // watch statement above!!!
         c->output_level() == 0 ||
         !FilesRangeOverlapWithCompaction(*c->inputs(), c->output_level()));
  if (c->start_level() == 0 ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.insert(c);
  }
  compactions_in_progress_.insert(c);
}

void CompactionPicker::UnregisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return;
  }
  if (c->start_level() == 0 ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.erase(c);
  }
  compactions_in_progress_.erase(c);
}

void CompactionPicker::PickFilesMarkedForCompaction(
    const std::string& cf_name, VersionStorageInfo* vstorage, int* start_level,
    int* output_level, CompactionInputFiles* start_level_inputs) {
  if (vstorage->FilesMarkedForCompaction().empty()) {
    return;
  }

  auto continuation = [&, cf_name](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    *start_level = level_file.first;
    *output_level =
        (*start_level == 0) ? vstorage->base_level() : *start_level + 1;

    if (*start_level == 0 && !level0_compactions_in_progress()->empty()) {
      return false;
    }

    start_level_inputs->files = {level_file.second};
    start_level_inputs->level = *start_level;
    return ExpandInputsToCleanCut(cf_name, vstorage, start_level_inputs);
  };

  // take a chance on a random file first
  Random64 rnd(/* seed */ reinterpret_cast<uint64_t>(vstorage));
  size_t random_file_index = static_cast<size_t>(rnd.Uniform(
      static_cast<uint64_t>(vstorage->FilesMarkedForCompaction().size())));

  if (continuation(vstorage->FilesMarkedForCompaction()[random_file_index])) {
    // found the compaction!
    return;
  }

  for (auto& level_file : vstorage->FilesMarkedForCompaction()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }
  start_level_inputs->files.clear();
}

#ifdef INDIRECT_VALUE_SUPPORT
// We have collected a group of L0 files that can be processed in parallel into the output level with no overlap there.  This is almost surely sequential-key load.
// Next we push the output level to the bottom if possible.  This is for 2 reasons: to avoid compacting each file one by one through all the levels (remember, they don't overlap); and
// to end the load with a one-level database.  The one-level database is desirable because otherwise the levels above the last will be full of nonoverlapping files which will
// compact inefficiently and, most important, will not stir the database by compactions into the last level.  In such a configuration AR will be called on to do almost all the
// VLog defragmentation, with an increase in CPU time until the database equilibrates.
//
// We could check the keys above the last level, but there's no reason to, since in the case of interest every compaction will go to the bottom.  Instead, we just check to make
// sure the upper levels are empty.  For the bottom level, it is not logically necessary to check anything: the files we have don't overlap anything being compacted out of L0,
// and thus the normal checks for files-being-compacted will prevent us from conflicting with an ongoing compaction.  But we have to make sure that the first compaction AFTER the load
// completes, which might encompass the entire key space, does not attempt to compact the entire bottom level.  The simple solution is that we go into the bottom level only if
// the smallest new key is larger than all the keys in the bottom level.  Since we are checking BEFORE writing anything, and the L0 files stay in order, this will allow all compactions
// of a sequential-key load to go to the bottom level even if the compactions complete out of order.
// return true if this compaction can be redirected to the bottom level
bool CompactionPicker::IsCompactIntoBottomLevel(VersionStorageInfo* vstorage,int output_level,
  InternalKey& smallkey, bool isdynamic){  // the smallest key being added
  // for compatibility with existing testcases, write the last level only if the user has requested that we build from the bottom up
  if(!isdynamic)return false;  // if !level_compaction_dynamic_level_bytes
  // verify that the levels before the last are unpopulated
  for(int i=output_level;i<vstorage->num_levels()-1;++i)if(vstorage->LevelFiles(i).size()!=0)return false;  // if there are files before the last level, don't change output level
  // verify that the largest key in the last level is lower than the smallest key being added
  if(!vstorage->LevelFiles(vstorage->num_levels()-1).empty() && icmp_->Compare(vstorage->LevelFiles(vstorage->num_levels()-1).back()->largest,smallkey)>=0)return false;  // if there is a key >= one we are adding. don't change
  return true;
}

#endif

// When we compact an L0 file it is vital that any earlier overlapping L0 file be included too, otherwise L0 might be processed out of order
int64_t CompactionPicker::GetOverlappingL0Files(
    VersionStorageInfo* vstorage, CompactionInputFiles* start_level_inputs,
#ifdef INDIRECT_VALUE_SUPPORT
    int output_level, int* parent_index, const ImmutableCFOptions *ioptions) {
#else
    int output_level, int* parent_index, const ImmutableCFOptions* /* ioptions */) {
#endif
#ifdef INDIRECT_VALUE_SUPPORT
  if(!level0_compactions_in_progress()->empty()){
    // If another L0 compaction is going on, it must be that we determined that we can throw in all the remaining L0 files without overlapping any keys being compacted already.
    // Do that, replacing the currently-selected file so that we keep all the files in order
    FileMetaData *smallkeyfile=nullptr;  // the file with the smallest key
    start_level_inputs->files.clear();  // remove all files
    const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(0);   // files at level 0
    for(size_t i = 0; i<level_files.size();++i)if(!level_files[i]->being_compacted){
      start_level_inputs->files.push_back(level_files[i]);  // add file to the list
      if((smallkeyfile==nullptr)|| icmp_->Compare(level_files[i]->smallest,smallkeyfile->smallest)<0)smallkeyfile=level_files[i];  // remember the smallest key
    }
    return IsCompactIntoBottomLevel(vstorage,output_level,smallkeyfile->smallest,ioptions&&ioptions->level_compaction_dynamic_level_bytes)?1:0;  // return, requesting change of output_level if called for
  }
#else
  // Two level 0 compaction won't run at the same time, so don't need to worry
  // about files on level 0 being compacted.
  assert(level0_compactions_in_progress()->empty());
#endif
  InternalKey smallest, largest;
  GetRange(*start_level_inputs, &smallest, &largest);
  // Note that the next call will discard the file we placed in
  // c->inputs_[0] earlier and replace it with an overlapping set
  // which will include the picked file.
  start_level_inputs->files.clear();
  vstorage->GetOverlappingInputs(0, &smallest, &largest,
                                 &(start_level_inputs->files));

  // If we include more L0 files in the same compaction run it can
  // cause the 'smallest' and 'largest' key to get extended to a
  // larger range. So, re-invoke GetRange to get the new key range
  GetRange(*start_level_inputs, &smallest, &largest);
  if (IsRangeInCompaction(vstorage, &smallest, &largest, output_level,
                          parent_index)) {
    return -1;  // error return
  }

#ifdef INDIRECT_VALUE_SUPPORT  // this would be a good idea for all systems
  // If, after all this, there is only 1 file to be compacted, we are most likely doing sequential writes that produce nonoverlapping L0 files.
  // If all the keys in the selected file are higher than all the keys in output_level (as we expect they will be), we will throw in all the L0 files that have keys
  // above the selected file.
  if(start_level_inputs->files.size()==1){   // only 1 L0 file being compacted
    FileMetaData *lastl1file = nullptr;  // will be pointer to output_level file with largest key
    FileMetaData *pickedfile=start_level_inputs->files[0];  // the sole selected file, which also has the smallest key
    if(vstorage->num_levels()>output_level && vstorage->LevelFiles(output_level).size())lastl1file=vstorage->LevelFiles(output_level).back();  // If there is an L1, point to file with largest key
    if(lastl1file==nullptr || icmp_->Compare(pickedfile->smallest,lastl1file->largest)>0) {
      // Here the sole file is later than all keys in output_level.  Remove it from the list and then reinsert all such files with later keys.  We have to remove
      // it first to preserve the order of the files (not strictly required, because we know no other files overlap the single file, but we do it for peace of mind).
      // We have to worry about this case:  (low key) file0begin   L1end   file1bgn  file0end  file1end file2begin  file2end  (high key)
      // where the selected file is file 2.  We have to make sure we don't bring in file1, because that would require file0, which overlaps L1.  For simplicity
      // we will choose only files whose keys are file2begin or higher.  Since our normal picking order uses the oldest L0 file first, and all later files have
      // higher keys during sequential load, this will give the desired result.
      start_level_inputs->files.clear();  // remove the picked file
      const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(0);   // files at level 0
      for(size_t i = 0; i<level_files.size();++i){
        if(icmp_->Compare(level_files[i]->smallest,pickedfile->smallest)>=0)start_level_inputs->files.push_back(level_files[i]);  // insert those with keys >- picked file's keys
      }
      if(IsCompactIntoBottomLevel(vstorage,output_level,pickedfile->smallest,ioptions&&ioptions->level_compaction_dynamic_level_bytes))return 1;  // request change of output_level if called for
    }
  }
#endif
  assert(!start_level_inputs->files.empty());

  return 0;
}

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableCFOptions& ioptions)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.  Result -1=error, 0=OK, 1=OK, and change output_level_ to the last level
  int64_t SetupOtherL0FilesIfNeeded();

  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  void PickExpiredTtlFiles();

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;
#ifdef INDIRECT_VALUE_SUPPORT
  size_t ringno_;  // will hold the ring number if Active Recycling called for
  VLogRingRefFileno lastfileno_;  // will hold the file number of the last file in the recycled area
#endif

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableCFOptions& ioptions_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::PickExpiredTtlFiles() {
  if (vstorage_->ExpiredTtlFiles().empty()) {
    return;
  }

  auto continuation = [&](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    output_level_ =
        (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;

    if ((start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      return false;
    }

    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    return compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &start_level_inputs_);
  };

  for (auto& level_file : vstorage_->ExpiredTtlFiles()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }

  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    }
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  if (start_level_inputs_.empty()) {
    parent_index_ = base_index_ = -1;

    // PickFilesMarkedForCompaction();
    compaction_picker_->PickFilesMarkedForCompaction(
        cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_);
    if (!start_level_inputs_.empty()) {
      is_manual_ = true;
      compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
      return;
    }

    size_t i;
    for (i = 0; i < vstorage_->BottommostFilesMarkedForCompaction().size();
         ++i) {
      auto& level_and_file = vstorage_->BottommostFilesMarkedForCompaction()[i];
      assert(!level_and_file.second->being_compacted);
      start_level_inputs_.level = output_level_ = start_level_ =
          level_and_file.first;
      start_level_inputs_.files = {level_and_file.second};
      if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                     &start_level_inputs_)) {
        break;
      }
    }
    if (i == vstorage_->BottommostFilesMarkedForCompaction().size()) {
      start_level_inputs_.clear();
    } else {
      assert(!start_level_inputs_.empty());
      compaction_reason_ = CompactionReason::kBottommostFiles;
      return;
    }

    assert(start_level_inputs_.empty());
    PickExpiredTtlFiles();
    if (!start_level_inputs_.empty()) {
      compaction_reason_ = CompactionReason::kTtl;
    }
  }
}

int64_t LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_,  &ioptions_);
  }
  return 0;  // good return
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    if (!compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(compaction_inputs_,
                                                            output_level_)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* LevelCompactionBuilder::PickCompaction() {
#ifdef INDIRECT_VALUE_SUPPORT
  // See if we need to perform an Active Recycling pass becase the fragmentation is getting too high
  compaction_inputs_.clear();  // start with an empty set of files
  ColumnFamilyData *cfd =  vstorage_->GetCfd();   // get pointer to CF.  Can be null only during tests that don't open a CF
#if !defined(NDEBUG)
    // notify the test code of the compaction-picking info
    size_t pickerinfo[8]={0,(size_t)~0,(size_t)~0,0,0,0,0}; // inlevel, picked min ref0, min ref0 in lower levels, vlog file min, vlog file max, output level
    pickerinfo[6]=(size_t)cfd;  // 6 cf
#endif
  if(cfd!=nullptr)cfd->CheckForActiveRecycle(compaction_inputs_, ringno_, lastfileno_, mutable_cf_options_);  // see if we select a set of files to recycle
  if(!compaction_inputs_.empty()) {
    // Active Recycling needed.  Indicate that as the reason for compaction.  This reason code controls processing during the compaction
    compaction_reason_ = CompactionReason::kActiveRecycling;
    // Active Recycling processes SSTs in-place without changing any keys - it just remaps old VLog blocks.  We can skip most of the
    // checking and stat-gathering for a full compaction.  Right here we will calculate the stats we need: start_level_ and output_level_.
    // start_level_ is needed because it is used later to decide whether to suppress other level-0 compaction.  output_level_ is used only
    // for things like figuring the path to use for the output files; we just set it to the largest level we find
    // scaf problem: this sets compression for all levels to be the same as for the max level
    start_level_ = output_level_ = compaction_inputs_[0].level;
    for(size_t i = 1;i<compaction_inputs_.size();++i){
      if(start_level_>compaction_inputs_[i].level)start_level_ = compaction_inputs_[i].level;  // find minimum level touched
      if(output_level_<compaction_inputs_[i].level)output_level_ = compaction_inputs_[i].level;  // find maximum level touched
    }
#if !defined(NDEBUG)
    // notify the test code of the compaction-picking info
    pickerinfo[7]=1;  // 7 set to 1 if AR
    pickerinfo[2] = compaction_inputs_.size();  // 2 # AR inputs
#endif
  } else {
    // Not Active Recycling.  Do a normal compaction,  pick files, etc.
#endif
    // Pick up the first file to start compaction. It may have been extended
    // to a clean cut.
    SetupInitialFiles();
    if (start_level_inputs_.empty()) {
      return nullptr;
    }
    assert(start_level_ >= 0 && output_level_ >= 0);
#if defined(INDIRECT_VALUE_SUPPORT) && !defined(NDEBUG)
    for(auto startfile : start_level_inputs_.files){  // find the smallest ref0 in the input files (our expected ref0)
      ParsedFnameRing avgparent(startfile->avgparentfileno);
      if(avgparent.fileno()!=0)pickerinfo[1]=std::min(pickerinfo[1],avgparent.fileno());  // scaf wired to ring 0
    }
#endif

    // If it is a L0 -> base level compaction, we need to set up other L0
    // files if needed.
    int64_t l0status=SetupOtherL0FilesIfNeeded();
    if (l0status<0) {  // error
      return nullptr;
    }else if (l0status>0)output_level_ = vstorage_->num_levels()-1;  // if sequential-key load, load into last level

    // Pick files in the output level and expand more files in the start level
    // if needed.
    if (!SetupOtherInputsIfNeeded()) {
      return nullptr;
    }
#if defined(INDIRECT_VALUE_SUPPORT) && !defined(NDEBUG)
    // find the smallest ref0 among the descendants of the start level
    for(auto cfiles : compaction_inputs_) {
      if(cfiles.level>start_level_) {
        for(auto cfile : cfiles.files){  // find the smallest ref0 in the input files (our expected ref0)
          if(cfile->indirect_ref_0.size() && cfile->indirect_ref_0[0]!=0)pickerinfo[2]=std::min(pickerinfo[2],cfile->indirect_ref_0[0]);  // scaf wired to ring 0
        }
      }
    }
// obsolete     TEST_SYNC_POINT_CALLBACK("LevelCompactionBuilder::PickCompaction",
// obsolete                            &pickerinfo);
#endif

#ifdef INDIRECT_VALUE_SUPPORT
  }
  // Files have been picked for compaction
#endif
#if defined(INDIRECT_VALUE_SUPPORT) && !defined(NDEBUG)
    // 0 start level
    pickerinfo[0]=start_level_;
    // 5 output level
    pickerinfo[5]=output_level_;
    // 3 4 get the current file limits from the VLog
    if(cfd!=nullptr && cfd->vlog()!=nullptr && cfd->vlog()->rings().size()!=0){pickerinfo[3]= cfd->vlog()->rings()[0]->ringtail(); pickerinfo[4]=cfd->vlog()->rings()[0]->ringhead();}
    TEST_SYNC_POINT_CALLBACK("LevelCompactionBuilder::PickCompaction",
                           &pickerinfo);
#endif
#if 0 // scaf debug
printf("Compaction picked:");
size_t nfiles = 0;
  for(auto cfiles : compaction_inputs_){printf(" L%d: %zd",cfiles.level,cfiles.files.size()); nfiles += cfiles.files.size();}
printf("\n");
if(nfiles > 20)
  printf("**************************************************** huge compaction\n");
#endif
  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* LevelCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, std::move(compaction_inputs_),
      output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                         output_level_, vstorage_->base_level()),
      GetCompressionOptions(ioptions_, vstorage_, output_level_),
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      start_level_score_, false /* deletion_compaction */, compaction_reason_
#ifdef INDIRECT_VALUE_SUPPORT
      ,ringno_ , lastfileno_  , start_level_  // parms for Active Recycling, used if compaction_reason_ indicates
#endif
      );

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/master/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::PickFileToCompact() {

  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);   // files at the current level

#ifdef INDIRECT_VALUE_SUPPORT  // actually this is OK whether you have indirect values or not.  It allows multiple L0 compactions.  If you keep this, make sure UpdateFilesByCompactionPri() uses kReverseOrder for L0

  // If a compaction from L0 is running, usually there is no reason to bother trying to start another, since the keys usually overlap.
  // The exception is sequential load, when each new L0 file has keys beyond the range of all previous L0 files.
  // We want to detect this case as quickly as possible.  The point is that if a set of L0 files does not overlap any L0 file currently in compaction,
  // it is safe to compact the lot.  It could be that the L0 files overlap some L1 file that overlaps (and thus is part of) a current
  // L0 compaction, but we will detect that in the usual way, when we look for output-level files that are in compaction.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    // If we are getting behind on a sequential load, the first compaction will have originally been a single file that was expanded by
    // GetOverlappingL0Files, throwing in all the L0 files.  That means that for us to need to start another compaction, there must be
    // at least twice as many files in L0 as needed to trigger a compaction.  This test will turn away all normal cases except when we are
    // unable to keep up with Puts.
    if(level_files.size()<(size_t)(2*mutable_cf_options_.level0_file_num_compaction_trigger)){TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0"); return false;}

    // Go through the files for level 0, to find the index of the first file that is being compacted and the index of the first file NOT being compacted.
    // We scan through the files from back to front since any files not in the current compaction will normally have been added at the end
    // If there is no file not being compacted, return failure.
    int64_t compactx, noncompactx;  // index to a file being compacted, and one that is not
    for(noncompactx = level_files.size()-1;noncompactx>=0;--noncompactx)if(!level_files[noncompactx]->being_compacted)break;
    if(noncompactx+1<mutable_cf_options_.level0_file_num_compaction_trigger){TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0"); return false;}  // if there can't be enough non-compacting to start a compaction, stop looking
    for(compactx = 0;compactx<(int64_t)level_files.size();++compactx)if(level_files[compactx]->being_compacted)break;

    if(compactx<(int64_t)level_files.size()){
      // There is a file being compacted.  We have to make sure the non-compacting files do not overlap the keys of the compacting files.
      // We will check only for the ascending-key case, i. e. highest compacting key less than smallest non-compacting key
      // It is possible that the L1 files being created by the running compaction will overlap with the files we select here: that will be detected later in the usual way
      const InternalKeyComparator *icmp = compaction_picker_->GetComparator();  // the user's comparator

      // To give an early exit, compare the keys for the first 2 files and avoid further searching if there is overlap
      if(icmp->Compare(level_files[compactx]->largest,level_files[noncompactx]->smallest)>=0){TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0"); return false;}  // if overlap. we can't process it
      // No overlap in the first compare.  There's a very good chance that this is a sequential write.  Compare the rest of the files
      // Find the smallest key among the noncompacting files, and the largest key among the compacting
      int64_t minx = noncompactx, noncompactn = 1;  // loop sets minx to index of noncompacting file with smallest smallest key; n iis number of noncompacted files
      for(--noncompactx;noncompactx>=0;--noncompactx){
        if(!level_files[noncompactx]->being_compacted){
          ++noncompactn;
          if(icmp->Compare(level_files[noncompactx]->smallest,level_files[minx]->smallest)<0)minx=noncompactx;
        }
      }
      // If there aren't enough noncompacted files to start a compaction, abort
      if(noncompactn<mutable_cf_options_.level0_file_num_compaction_trigger){TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0"); return false;}

      int64_t maxx = compactx;  // loop sets maxx to index of compacting file with largest largest key
      for(++compactx;compactx<(int64_t)level_files.size();++compactx){
        if(level_files[compactx]->being_compacted&&icmp->Compare(level_files[compactx]->largest,level_files[maxx]->largest)>0)maxx=compactx;
      }
      // abort if there is overlap
      if(icmp->Compare(level_files[maxx]->largest,level_files[minx]->smallest)>=0){TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0"); return false;}  // if overlap. we can't process it
    }

    // no overlap, OK to continue with the compaction picking.  We will pick the earliest uncompacted file (regardless of level0_file_num_compaction_trigger) and then expand
    // the selection with anything that overlaps it.  Eventually we will throw in all the other files L0 too, known to be safe since they don't
    // overlap any existing compaction.
  }
#else
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }
#endif

  start_level_inputs_.clear();

  assert(start_level_ >= 0);

  // Pick the largest file in this level that is not already
  // being compacted
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);

  // We want to try level-0 compactions oldest first, to increase the chance that all uncompacted files come after the running compaction.
  // Thus, we have sorted L0 in descending order of age.  
  
  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_size.size(); cmp_idx++) {
    int index = file_size[cmp_idx];
    auto* f = level_files[index];
    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }
    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    // Now that input level is fully expanded, we check whether any output files
    // are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }
    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.  Or, if file 0, where we would start an intraL0, is being compacted, don't bother
    // To be able to compact with so few files would require that L0->L1 is blocked by a compaction out of L1, which is certainly possible.
    // The thing to worry about is the other side, during sequential-key load, where L0 has a lot of files that are being compacted in parallel.
    // In that case, this test will always pass, but we never want an L0->L0 compaction.  Fortunately, we get here only when the compaction
    // score is >= 1.0, which means there must be at least compaction_trigger files that are not compacting.  If they have ascending keys, we will
    // have had a chance to take them, so if we are here we know we are not bulk-loading.
    return false;
  }
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               port::kMaxUint64, &start_level_inputs_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, LogBuffer* log_buffer) {
  LevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_);
  return builder.PickCompaction();
}

}  // namespace rocksdb
