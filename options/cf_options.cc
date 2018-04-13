//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/cf_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <cassert>
#include <limits>
#include <string>
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace rocksdb {

ImmutableCFOptions::ImmutableCFOptions(const Options& options)
    : ImmutableCFOptions(ImmutableDBOptions(options), options) {}

ImmutableCFOptions::ImmutableCFOptions(const ImmutableDBOptions& db_options,
                                       const ColumnFamilyOptions& cf_options)
    : compaction_style(cf_options.compaction_style),
      compaction_pri(cf_options.compaction_pri),
      compaction_options_universal(cf_options.compaction_options_universal),
      compaction_options_fifo(cf_options.compaction_options_fifo),
      prefix_extractor(cf_options.prefix_extractor.get()),
      user_comparator(cf_options.comparator),
      internal_comparator(InternalKeyComparator(cf_options.comparator)),
      merge_operator(cf_options.merge_operator.get()),
      compaction_filter(cf_options.compaction_filter),
      compaction_filter_factory(cf_options.compaction_filter_factory.get()),
      min_write_buffer_number_to_merge(
          cf_options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          cf_options.max_write_buffer_number_to_maintain),
      inplace_update_support(cf_options.inplace_update_support),
      inplace_callback(cf_options.inplace_callback),
      info_log(db_options.info_log.get()),
      statistics(db_options.statistics.get()),
      rate_limiter(db_options.rate_limiter.get()),
      env(db_options.env),
      allow_mmap_reads(db_options.allow_mmap_reads),
      allow_mmap_writes(db_options.allow_mmap_writes),
      db_paths(db_options.db_paths),
      memtable_factory(cf_options.memtable_factory.get()),
      table_factory(cf_options.table_factory.get()),
      table_properties_collector_factories(
          cf_options.table_properties_collector_factories),
      advise_random_on_open(db_options.advise_random_on_open),
      bloom_locality(cf_options.bloom_locality),
      purge_redundant_kvs_while_flush(
          cf_options.purge_redundant_kvs_while_flush),
      use_fsync(db_options.use_fsync),
      compression_per_level(cf_options.compression_per_level),
      bottommost_compression(cf_options.bottommost_compression),
      compression_opts(cf_options.compression_opts),
      level_compaction_dynamic_level_bytes(
          cf_options.level_compaction_dynamic_level_bytes),
      access_hint_on_compaction_start(
          db_options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          db_options.new_table_reader_for_compaction_inputs),
      compaction_readahead_size(db_options.compaction_readahead_size),
      num_levels(cf_options.num_levels),
      optimize_filters_for_hits(cf_options.optimize_filters_for_hits),
      force_consistency_checks(cf_options.force_consistency_checks),
      allow_ingest_behind(db_options.allow_ingest_behind),
      listeners(db_options.listeners),
      row_cache(db_options.row_cache),
      max_subcompactions(db_options.max_subcompactions),
      memtable_insert_with_hint_prefix_extractor(
          cf_options.memtable_insert_with_hint_prefix_extractor.get())
#ifdef INDIRECT_VALUE_SUPPORT  // initialize immutable options
// Get immutable options for VLog
#endif
      {}

// Multiple two operands. If they overflow, return op1.
uint64_t MultiplyCheckOverflow(uint64_t op1, double op2) {
  if (op1 == 0 || op2 <= 0) {
    return 0;
  }
  if (port::kMaxUint64 / op1 < op2) {
    return op1;
  }
  return static_cast<uint64_t>(op1 * op2);
}

void MutableCFOptions::RefreshDerivedOptions(int num_levels,
                                             CompactionStyle compaction_style) {
  max_file_size.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i == 0 && compaction_style == kCompactionStyleUniversal) {
      max_file_size[i] = ULLONG_MAX;
    } else if (i > 1) {
      max_file_size[i] = MultiplyCheckOverflow(max_file_size[i - 1],
                                               target_file_size_multiplier);
    } else {
      max_file_size[i] = target_file_size_base;
    }
  }
}

uint64_t MutableCFOptions::MaxFileSizeForLevel(int level) const {
  assert(level >= 0);
  assert(level < (int)max_file_size.size());
  return max_file_size[level];
}

void MutableCFOptions::Dump(Logger* log) const {
  // Memtable related options
  ROCKS_LOG_INFO(log,
                 "                        write_buffer_size: %" ROCKSDB_PRIszt,
                 write_buffer_size);
  ROCKS_LOG_INFO(log, "                  max_write_buffer_number: %d",
                 max_write_buffer_number);
  ROCKS_LOG_INFO(log,
                 "                         arena_block_size: %" ROCKSDB_PRIszt,
                 arena_block_size);
  ROCKS_LOG_INFO(log, "              memtable_prefix_bloom_ratio: %f",
                 memtable_prefix_bloom_size_ratio);
  ROCKS_LOG_INFO(log,
                 "                  memtable_huge_page_size: %" ROCKSDB_PRIszt,
                 memtable_huge_page_size);
  ROCKS_LOG_INFO(log,
                 "                    max_successive_merges: %" ROCKSDB_PRIszt,
                 max_successive_merges);
  ROCKS_LOG_INFO(log,
                 "                 inplace_update_num_locks: %" ROCKSDB_PRIszt,
                 inplace_update_num_locks);
  ROCKS_LOG_INFO(log, "                 disable_auto_compactions: %d",
                 disable_auto_compactions);
  ROCKS_LOG_INFO(log, "      soft_pending_compaction_bytes_limit: %" PRIu64,
                 soft_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "      hard_pending_compaction_bytes_limit: %" PRIu64,
                 hard_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "       level0_file_num_compaction_trigger: %d",
                 level0_file_num_compaction_trigger);
  ROCKS_LOG_INFO(log, "           level0_slowdown_writes_trigger: %d",
                 level0_slowdown_writes_trigger);
  ROCKS_LOG_INFO(log, "               level0_stop_writes_trigger: %d",
                 level0_stop_writes_trigger);
  ROCKS_LOG_INFO(log, "                     max_compaction_bytes: %" PRIu64,
                 max_compaction_bytes);
#ifdef INDIRECT_VALUE_SUPPORT
  ROCKS_LOG_INFO(log, "                       allow_trivial_move: %d",
                 allow_trivial_move);
  ROCKS_LOG_INFO(log, "                compaction_score_limit_L0: %f",
                 compaction_score_limit_L0);
    for (size_t i = 0; i < vlogring_activation_level.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "             vlogring_activation_level[%" ROCKSDB_PRIszt
               "]: %d",
          i, vlogring_activation_level[i]);
    }
    for (size_t i = 0; i < min_indirect_val_size.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "                 min_indirect_val_size[%" ROCKSDB_PRIszt
               "]: %d",
          i, min_indirect_val_size[i]);
    }
    for (size_t i = 0; i < fraction_remapped_during_compaction.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "   fraction_remapped_during_compaction[%" ROCKSDB_PRIszt
               "]: %f",
          i, fraction_remapped_during_compaction[i]);
    }
    for (size_t i = 0; i < fraction_remapped_during_active_recycling.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "fraction_remapped_during_active_recycling[%" ROCKSDB_PRIszt
               "]: %f",
          i, fraction_remapped_during_active_recycling[i]);
    }
    for (size_t i = 0; i < fragmentation_active_recycling_trigger.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "fragmentation_active_recycling_trigger[%" ROCKSDB_PRIszt
               "]: %f",
          i, fragmentation_active_recycling_trigger[i]);
    }
    for (size_t i = 0; i < fragmentation_active_recycling_klaxon.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, " fragmentation_active_recycling_klaxon[%" ROCKSDB_PRIszt
               "]: %f",
          i, fragmentation_active_recycling_klaxon[i]);
    }
    for (size_t i = 0; i < active_recycling_sst_minct.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "            active_recycling_sst_minct[%" ROCKSDB_PRIszt
               "]: %d",
          i, active_recycling_sst_minct[i]);
    }
    for (size_t i = 0; i < active_recycling_sst_maxct.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "            active_recycling_sst_maxct[%" ROCKSDB_PRIszt
               "]: %d",
          i, active_recycling_sst_maxct[i]);
    }
    for (size_t i = 0; i < active_recycling_vlogfile_freed_min.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "   active_recycling_vlogfile_freed_min[%" ROCKSDB_PRIszt
               "]: %d",
          i, active_recycling_vlogfile_freed_min[i]);
    }
    for (size_t i = 0; i < vlogfile_max_size.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "                     vlogfile_max_size[%" ROCKSDB_PRIszt
               "]: %d",
          i, vlogfile_max_size[i]);
    }
    for (size_t i = 0; i < compaction_picker_age_importance.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "      compaction_picker_age_importance[%" ROCKSDB_PRIszt
               "]: %f",
          i, compaction_picker_age_importance[i]);
    }
    for (size_t i = 0; i < ring_compression_style.size();
         i++) {
      ROCKS_LOG_HEADER(
          log, "                ring_compression_style[%" ROCKSDB_PRIszt
               "]: %s",
          i, CompressionTypeToString(ring_compression_style[i]).c_str());
    }
#endif
  ROCKS_LOG_INFO(log, "                    target_file_size_base: %" PRIu64,
                 target_file_size_base);
  ROCKS_LOG_INFO(log, "              target_file_size_multiplier: %d",
                 target_file_size_multiplier);
  ROCKS_LOG_INFO(log, "                 max_bytes_for_level_base: %" PRIu64,
                 max_bytes_for_level_base);
  ROCKS_LOG_INFO(log, "           max_bytes_for_level_multiplier: %f",
                 max_bytes_for_level_multiplier);
  std::string result;
  char buf[10];
  for (const auto m : max_bytes_for_level_multiplier_additional) {
    snprintf(buf, sizeof(buf), "%d, ", m);
    result += buf;
  }
  if (result.size() >= 2) {
    result.resize(result.size() - 2);
  } else {
    result = "";
  }

  ROCKS_LOG_INFO(log, "max_bytes_for_level_multiplier_additional: %s",
                 result.c_str());
  ROCKS_LOG_INFO(log, "        max_sequential_skip_in_iterations: %" PRIu64,
                 max_sequential_skip_in_iterations);
  ROCKS_LOG_INFO(log, "                     paranoid_file_checks: %d",
                 paranoid_file_checks);
  ROCKS_LOG_INFO(log, "                       report_bg_io_stats: %d",
                 report_bg_io_stats);
  ROCKS_LOG_INFO(log, "                              compression: %d",
                 static_cast<int>(compression));
#ifdef INDIRECT_VALUE_SUPPORT   // string translation for mutable options
// give string form for mutable parameters
#endif
}

}  // namespace rocksdb
