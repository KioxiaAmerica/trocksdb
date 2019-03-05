// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "options/db_options.h"
#include "rocksdb/options.h"
#include "util/compression.h"

namespace rocksdb {

// ImmutableCFOptions is a data struct used by RocksDB internal. It contains a
// subset of Options that should not be changed during the entire lifetime
// of DB. Raw pointers defined in this struct do not have ownership to the data
// they point to. Options contains shared_ptr to these data.
struct ImmutableCFOptions {
  ImmutableCFOptions();
  explicit ImmutableCFOptions(const Options& options);

  ImmutableCFOptions(const ImmutableDBOptions& db_options,
                     const ColumnFamilyOptions& cf_options);

  CompactionStyle compaction_style;

  CompactionPri compaction_pri;

  const Comparator* user_comparator;
  InternalKeyComparator internal_comparator;

  MergeOperator* merge_operator;

  const CompactionFilter* compaction_filter;

  CompactionFilterFactory* compaction_filter_factory;

  int min_write_buffer_number_to_merge;

  int max_write_buffer_number_to_maintain;

  bool inplace_update_support;

  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);

  Logger* info_log;

  Statistics* statistics;

  RateLimiter* rate_limiter;

  InfoLogLevel info_log_level;

  Env* env;

  // Allow the OS to mmap file for reading sst tables. Default: false
  bool allow_mmap_reads;

  // Allow the OS to mmap file for writing. Default: false
  bool allow_mmap_writes;

  std::vector<DbPath> db_paths;

  MemTableRepFactory* memtable_factory;

  TableFactory* table_factory;

  Options::TablePropertiesCollectorFactories
      table_properties_collector_factories;

  bool advise_random_on_open;

  // This options is required by PlainTableReader. May need to move it
  // to PlainTableOptions just like bloom_bits_per_key
  uint32_t bloom_locality;

  bool purge_redundant_kvs_while_flush;

  bool use_fsync;

  std::vector<CompressionType> compression_per_level;

  CompressionType bottommost_compression;

  CompressionOptions compression_opts;

  bool level_compaction_dynamic_level_bytes;

  Options::AccessHint access_hint_on_compaction_start;

  bool new_table_reader_for_compaction_inputs;

  int num_levels;

  bool optimize_filters_for_hits;

  bool force_consistency_checks;

  bool allow_ingest_behind;

  bool preserve_deletes;

  // A vector of EventListeners which callback functions will be called
  // when specific RocksDB event happens.
  std::vector<std::shared_ptr<EventListener>> listeners;

  std::shared_ptr<Cache> row_cache;

  uint32_t max_subcompactions;

  const SliceTransform* memtable_insert_with_hint_prefix_extractor;

  uint64_t ttl;

  std::vector<DbPath> cf_paths;
#ifdef INDIRECT_VALUE_SUPPORT
  uint64_t path_ids_per_level;
  int const MinPathIdForLevel(int32_t level) const  { return
    (path_ids_per_level>>(level*2))&3;  // must match kFileNumberMask
#else
  int const MinPathIdForLevel(int32_t level) const  { return
    0;  // in the vanilla system all levels are always available
#endif
  }
};

struct MutableCFOptions {
  explicit MutableCFOptions(const ColumnFamilyOptions& options)
      : write_buffer_size(options.write_buffer_size),
        max_write_buffer_number(options.max_write_buffer_number),
        arena_block_size(options.arena_block_size),
        memtable_prefix_bloom_size_ratio(
            options.memtable_prefix_bloom_size_ratio),
        memtable_huge_page_size(options.memtable_huge_page_size),
        max_successive_merges(options.max_successive_merges),
        inplace_update_num_locks(options.inplace_update_num_locks),
        prefix_extractor(options.prefix_extractor),
        disable_auto_compactions(options.disable_auto_compactions),
        soft_pending_compaction_bytes_limit(
            options.soft_pending_compaction_bytes_limit),
        hard_pending_compaction_bytes_limit(
            options.hard_pending_compaction_bytes_limit),
        level0_file_num_compaction_trigger(
            options.level0_file_num_compaction_trigger),
        level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
        level0_stop_writes_trigger(options.level0_stop_writes_trigger),
        max_compaction_bytes(options.max_compaction_bytes),
        target_file_size_base(options.target_file_size_base),
        target_file_size_multiplier(options.target_file_size_multiplier),
        max_bytes_for_level_base(options.max_bytes_for_level_base),
        max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
        max_bytes_for_level_multiplier_additional(
            options.max_bytes_for_level_multiplier_additional),
        compaction_options_fifo(options.compaction_options_fifo),
        compaction_options_universal(options.compaction_options_universal),
#ifdef INDIRECT_VALUE_SUPPORT   // declare mutable options
// Tuning parameters are mutable
        allow_trivial_move(options.allow_trivial_move),
        vlog_direct_IO(options.vlog_direct_IO),
        compaction_score_limit_L0(options.compaction_score_limit_L0),
// options related to ring structure and compression are immutable.  Ring structure may change over a restart, as long as a ring is not deleted while it is holding values
        vlogring_activation_level(options.vlogring_activation_level),
        min_indirect_val_size(options.min_indirect_val_size),
        fraction_remapped_during_compaction(options.fraction_remapped_during_compaction),
        fraction_remapped_during_active_recycling(options.fraction_remapped_during_active_recycling),
        fragmentation_active_recycling_trigger(options.fragmentation_active_recycling_trigger),
        fragmentation_active_recycling_klaxon(options.fragmentation_active_recycling_klaxon),
        active_recycling_sst_minct(options.active_recycling_sst_minct),
        active_recycling_sst_maxct(options.active_recycling_sst_maxct),
        active_recycling_vlogfile_freed_min(options.active_recycling_vlogfile_freed_min),
        active_recycling_size_trigger(options.active_recycling_size_trigger),
        vlogfile_max_size(options.vlogfile_max_size),
        compaction_picker_age_importance(options.compaction_picker_age_importance),
        ring_compression_style(options.ring_compression_style),
#endif
        max_sequential_skip_in_iterations(
            options.max_sequential_skip_in_iterations),
        paranoid_file_checks(options.paranoid_file_checks),
        report_bg_io_stats(options.report_bg_io_stats),
        compression(options.compression)
  {
    RefreshDerivedOptions(options.num_levels, options.compaction_style);
  }

  MutableCFOptions()
      : write_buffer_size(0),
        max_write_buffer_number(0),
        arena_block_size(0),
        memtable_prefix_bloom_size_ratio(0),
        memtable_huge_page_size(0),
        max_successive_merges(0),
        inplace_update_num_locks(0),
        prefix_extractor(nullptr),
        disable_auto_compactions(false),
        soft_pending_compaction_bytes_limit(0),
        hard_pending_compaction_bytes_limit(0),
        level0_file_num_compaction_trigger(0),
        level0_slowdown_writes_trigger(0),
        level0_stop_writes_trigger(0),
        max_compaction_bytes(0),
        target_file_size_base(0),
        target_file_size_multiplier(0),
        max_bytes_for_level_base(0),
        max_bytes_for_level_multiplier(0),
        compaction_options_fifo(),
#ifdef INDIRECT_VALUE_SUPPORT   // initialize mutable options
// Tuning parameters are mutable
        allow_trivial_move(false),
        vlog_direct_IO(false),
        compaction_score_limit_L0(0),
        vlogring_activation_level(std::vector<int32_t>({})),
        min_indirect_val_size(std::vector<uint64_t>({0})),
        fraction_remapped_during_compaction(std::vector<int32_t>({0})),
        fraction_remapped_during_active_recycling(std::vector<int32_t>({0})),
        fragmentation_active_recycling_trigger(std::vector<int32_t>({0})),
        fragmentation_active_recycling_klaxon(std::vector<int32_t>({0})),
        active_recycling_sst_minct(std::vector<int32_t>({0})),
        active_recycling_sst_maxct(std::vector<int32_t>({0})),
        active_recycling_vlogfile_freed_min(std::vector<int32_t>({0})),
        active_recycling_size_trigger(std::vector<int64_t>({0})),
        vlogfile_max_size(std::vector<uint64_t>({0})),
        compaction_picker_age_importance(std::vector<int32_t>({0})),
    //Ring Compression Style: indicates what kind of compression will be applied to the data
        ring_compression_style(std::vector<CompressionType>({kNoCompression})),
#endif
        max_sequential_skip_in_iterations(0),
        paranoid_file_checks(false),
        report_bg_io_stats(false),
        compression(Snappy_Supported() ? kSnappyCompression : kNoCompression)
  {}

  explicit MutableCFOptions(const Options& options);

  // Must be called after any change to MutableCFOptions
  void RefreshDerivedOptions(int num_levels, CompactionStyle compaction_style);

  void RefreshDerivedOptions(const ImmutableCFOptions& ioptions) {
    RefreshDerivedOptions(ioptions.num_levels, ioptions.compaction_style);
  }

  int MaxBytesMultiplerAdditional(int level) const {
    if (level >=
        static_cast<int>(max_bytes_for_level_multiplier_additional.size())) {
      return 1;
    }
    return max_bytes_for_level_multiplier_additional[level];
  }

  void Dump(Logger* log) const;

  // Memtable related options
  size_t write_buffer_size;
  int max_write_buffer_number;
  size_t arena_block_size;
  double memtable_prefix_bloom_size_ratio;
  size_t memtable_huge_page_size;
  size_t max_successive_merges;
  size_t inplace_update_num_locks;
  std::shared_ptr<const SliceTransform> prefix_extractor;

  // Compaction related options
  bool disable_auto_compactions;
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;
  int level0_file_num_compaction_trigger;
  int level0_slowdown_writes_trigger;
  int level0_stop_writes_trigger;
  uint64_t max_compaction_bytes;
  uint64_t target_file_size_base;
  int target_file_size_multiplier;
  uint64_t max_bytes_for_level_base;
  double max_bytes_for_level_multiplier;
  std::vector<int> max_bytes_for_level_multiplier_additional;
  CompactionOptionsFIFO compaction_options_fifo;
  CompactionOptionsUniversal compaction_options_universal;
#ifdef INDIRECT_VALUE_SUPPORT
  bool allow_trivial_move;  // allow trivial move, bypassing compaction.  Used to allow some tests to run
  bool vlog_direct_IO;  // use direct I/O for vlog operations
  double compaction_score_limit_L0;  // maximum compaction score assigned to L0
  // VLog Options (mutable)
// options related to ring structure and compression are immutable.  Ring structure may change over a restart, as long as a ring is not deleted while it is holding values
  std::vector<int32_t> vlogring_activation_level;
  std::vector<uint64_t> min_indirect_val_size;
  std::vector<int32_t> fraction_remapped_during_compaction;
  std::vector<int32_t> fraction_remapped_during_active_recycling;
  std::vector<int32_t> fragmentation_active_recycling_trigger;
  std::vector<int32_t> fragmentation_active_recycling_klaxon;
  std::vector<int32_t> active_recycling_sst_minct;
  std::vector<int32_t> active_recycling_sst_maxct;
  std::vector<int32_t> active_recycling_vlogfile_freed_min;
  std::vector<int64_t> active_recycling_size_trigger;
  std::vector<uint64_t> vlogfile_max_size;
  std::vector<int32_t> compaction_picker_age_importance;
  std::vector<CompressionType> ring_compression_style;
#endif

  // Misc options
  uint64_t max_sequential_skip_in_iterations;
  bool paranoid_file_checks;
  bool report_bg_io_stats;
  CompressionType compression;

  // Derived options
  // Per-level target file size.
  std::vector<uint64_t> max_file_size;
};

uint64_t MultiplyCheckOverflow(uint64_t op1, double op2);

// Get the max file size in a given level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options,
    int level, CompactionStyle compaction_style, int base_level = 1,
    bool level_compaction_dynamic_level_bytes = false);
}  // namespace rocksdb
