//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2024-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"
#include <cstddef>
#include <optional>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/column_value_expression.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  // auto order_bys = order_bys_;
  int index = 0;
  for (auto &order_by : order_bys_) {
    auto &[type, expr] = order_by;
    if (type == OrderByType::DESC) {
      if (entry_a.first[index].CompareLessThan(entry_b.first[index]) == CmpBool::CmpTrue) {
        return false;
      }
      if (entry_b.first[index].CompareLessThan(entry_a.first[index]) == CmpBool::CmpTrue) {
        return true;
      }
    } else {
      if (entry_a.first[index].CompareLessThan(entry_b.first[index]) == CmpBool::CmpTrue) {
        return true;
      }
      if (entry_b.first[index].CompareLessThan(entry_a.first[index]) == CmpBool::CmpTrue) {
        return false;
      }
    }
    ++index;
  }

  return false;
}

auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey sort_key;
  for (auto &order_by : order_bys) {
    auto &[type, expr] = order_by;
    sort_key.emplace_back(expr->Evaluate(&tuple, schema));
  }
  return sort_key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base tuple. All logs in the undo_logs are
 * applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction, the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the tuple is deleted as the result, returns
 * std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // UNIMPLEMENTED("not implemented");
  bool is_deleted = base_meta.is_deleted_;
  // generate values of base tuple
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    values.emplace_back(base_tuple.GetValue(schema, i));
  }
  // Undo
  for (const auto &undo_log : undo_logs) {
    if (undo_log.is_deleted_) {
      is_deleted = true;
      for (size_t i = 0; i < schema->GetColumnCount(); i++) {
        values[i] = ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType());
      }
    } else {
      is_deleted = false;
      // generate undo schema
      std::vector<uint32_t> cols;
      for (size_t i = 0; i < schema->GetColumnCount(); i++) {
        if (undo_log.modified_fields_[i]) {
          cols.emplace_back(i);
        }
      }
      auto undo_schema = Schema::CopySchema(schema, cols);  // get value used
      // execute undo
      for (size_t i = 0, j = 0; i < undo_log.modified_fields_.size(); ++i) {
        if (undo_log.modified_fields_[i]) {
          values[i] = undo_log.tuple_.GetValue(&undo_schema, j);
          ++j;
        }
      }
    }
  }

  if (is_deleted) {
    return std::nullopt;
  }
  // generate new tuple
  Tuple res = Tuple(values, schema);
  res.SetRid(base_tuple.GetRid());
  return res;
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the txn.
 *
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple(). std::nullopt if the tuple did not exist at the
 * time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  // UNIMPLEMENTED("not implemented");
  std::vector<UndoLog> undo_logs;
  auto tuple_ts = base_meta.ts_;
  auto txn_ts = txn->GetReadTs();
  // 表堆中的 Tuples 已被另一个未提交的事务修改，或者它比事务读取时间戳更新，我们才会需要回滚
  if (tuple_ts > txn_ts && tuple_ts != txn->GetTransactionId()) {
    auto new_undo_link = undo_link.value_or(UndoLink{});
    auto optional_undo_log = txn_mgr->GetUndoLogOptional(new_undo_link);
    while (optional_undo_log.has_value() && tuple_ts > txn_ts) {
      undo_logs.emplace_back(*optional_undo_log);
      tuple_ts = optional_undo_log->ts_;
      new_undo_link = optional_undo_log->prev_version_;
      optional_undo_log = txn_mgr->GetUndoLogOptional(new_undo_link);
    }
    if (tuple_ts > txn_ts) {
      return std::nullopt;
    }
    return {undo_logs};
  }
  return {undo_logs};
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  // UNIMPLEMENTED("not implemented");
  bool is_delete = target_tuple == nullptr;
  bool is_insert = base_tuple == nullptr;
  if (is_insert) {
    return UndoLog{true, std::vector<bool>(schema->GetColumnCount(), true), Tuple{}, ts, prev_version};
  }
  if (is_delete) {
    // store all tuple in undolog
    std::vector<bool> modified_fields(schema->GetColumnCount(), true);
    return UndoLog{false, modified_fields, *base_tuple, ts, prev_version};
  }
  if (IsTupleContentEqual(*base_tuple, *target_tuple)) {
    // no need to generate undolog
    return UndoLog{is_delete, {}, *base_tuple, ts, prev_version};
  }
  // update
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    if (!base_tuple->GetValue(schema, i).CompareExactlyEquals(target_tuple->GetValue(schema, i))) {
      modified_fields[i] = true;
    }
  }
  // reconsturct tuple
  std::vector<Value> values;
  std::vector<uint32_t> cols;
  for (size_t i = 0; i < schema->GetColumnCount(); i++) {
    auto value = base_tuple->GetValue(schema, i);
    if (modified_fields[i]) {
      cols.emplace_back(i);
      values.emplace_back(value);
    }
  }
  auto undo_schema = Schema::CopySchema(schema, cols);

  Tuple undo_tuple = Tuple{values, &undo_schema};
  undo_tuple.SetRid(base_tuple->GetRid());
  // generate undo log
  UndoLog undo_log = UndoLog{false, modified_fields, undo_tuple, ts, prev_version};
  return undo_log;
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the tuple is already modified by this txn once.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from the table heap. nullptr if the tuple is
 * deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // UNIMPLEMENTED("not implemented");
  bool is_delete = target_tuple == nullptr;
  bool is_insert = base_tuple == nullptr;
  if (is_insert) {
    return log;
  }
  RID rid = base_tuple->GetRid();
  if (is_delete) {
    // need combine two undologs to one undolog
    // std::vector<bool> modified_fields(schema->GetColumnCount(), true);
    // return UndoLog{false, modified_fields, *base_tuple, log.ts_, log.prev_version_};
    return log;
  }
  if (IsTupleContentEqual(*base_tuple, *target_tuple)) {
    // no need to generate undolog

    return log;

    // return UndoLog{is_delete, {}, *base_tuple, log.ts_, log.prev_version_};
  }
  // update
  // 生成当前修改记录
  if (log.is_deleted_) {
    return log;
  }
  std::vector<Value> new_values;
  std::vector<bool> modified_fields;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    auto old_value = base_tuple->GetValue(schema, i);
    if (old_value.CompareExactlyEquals(target_tuple->GetValue(schema, i))) {
      modified_fields.emplace_back(false);
    } else {
      modified_fields.emplace_back(true);
    }
  }
  // 获取之前执行的modified记录
  std::vector<bool> old_modified_fields = log.modified_fields_;
  auto old_modified_tuple = log.tuple_;
  // 生成新的modified记录
  std::vector<bool> new_modified_fields;
  std::vector<Value> new_modified_values;
  std::vector<uint32_t> old_cols;
  std::vector<uint32_t> cols;
  // 生成old_modified_tuple的schema
  for (uint32_t i = 0; i < old_modified_fields.size(); ++i) {
    if (old_modified_fields[i]) {
      old_cols.emplace_back(i);
    }
  }
  auto old_schema = Schema::CopySchema(schema, old_cols);
  // 生成new_modified_tuple的schema和modified记录
  for (uint32_t i = 0, j = 0, k = 0; i < old_modified_fields.size(); ++i) {
    if (old_modified_fields[i]) {
      cols.emplace_back(i);
      new_modified_values.emplace_back(old_modified_tuple.GetValue(&old_schema, j));
      new_modified_fields.emplace_back(true);
    } else if (modified_fields[i]) {
      cols.emplace_back(i);
      new_modified_values.emplace_back(base_tuple->GetValue(schema, k));
      new_modified_fields.emplace_back(true);
    } else {
      new_modified_fields.emplace_back(false);
    }
    if (old_modified_fields[i]) {
      ++j;
    }
    if (modified_fields[i]) {
      ++k;
    }
  }
  auto undo_schema = Schema::CopySchema(schema, cols);

  Tuple undo_tuple = Tuple{new_modified_values, &undo_schema};
  undo_tuple.SetRid(base_tuple->GetRid());
  // generate undo log
  UndoLog undo_log = UndoLog{false, new_modified_fields, undo_tuple, log.ts_, log.prev_version_};
  return undo_log;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  for (auto &txn : txn_mgr->txn_map_) {
    fmt::println(stderr, "txn_id: {}, state: {}, read_ts: {}, commit_ts: {}",
                 txn.second->GetTransactionIdHumanReadable(), txn.second->GetTransactionState(),
                 txn.second->GetReadTs(), txn.second->GetCommitTs());
  }
  fmt::println(stderr, "table_name: {}, table_schema: {}", table_info->name_, table_info->schema_.ToString());
  for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto rid = iter.GetRID();
    auto tuple = iter.GetTuple().second;
    auto tuple_meta = iter.GetTuple().first;
    auto pre_link = txn_mgr->GetUndoLink(rid);
    bool is_deleted = tuple_meta.is_deleted_;
    if (!is_deleted) {
      if (tuple_meta.ts_ > TXN_START_ID) {
        fmt::println(stderr, "RID={}/{}, txn={}, tuple={}", rid.GetPageId(), rid.GetSlotNum(),
                     tuple_meta.ts_ ^ (TXN_START_ID), tuple.ToString(&table_info->schema_));
      } else {
        fmt::println(stderr, "RID={}/{}, ts={}, tuple={}", rid.GetPageId(), rid.GetSlotNum(), tuple_meta.ts_,
                     tuple.ToString(&table_info->schema_));
      }

    } else {
      if (tuple_meta.ts_ > TXN_START_ID) {
        fmt::println(stderr, "RID={}/{}, txn={}, <del marker> tuple={}", rid.GetPageId(), rid.GetSlotNum(),
                     tuple_meta.ts_ ^ (TXN_START_ID), tuple.ToString(&table_info->schema_));
      } else {
        fmt::println(stderr, "RID={}/{}, ts={}, <del marker> tuple={}", rid.GetPageId(), rid.GetSlotNum(),
                     tuple_meta.ts_, tuple.ToString(&table_info->schema_));
      }
    }
    if (!pre_link.has_value()) {
      continue;
    }
    UndoLink undo_link = pre_link.value();
    while (undo_link.IsValid()) {
      auto undo_log = txn_mgr->GetUndoLogOptional(undo_link);
      if (!undo_log.has_value()) {
        break;
      }
      auto old_tuple = ReconstructTuple(&table_info->schema_, tuple, tuple_meta, {*undo_log});
      // auto txn_id = undo_log->ts_;
      if (old_tuple.has_value()) {
        tuple = old_tuple.value();
        tuple_meta = TupleMeta{undo_log->ts_, undo_log->is_deleted_};
        bool is_deleted = undo_log->is_deleted_;
        if (!is_deleted) {
          fmt::println(stderr, "  txn{}, tuple={}, ts={}", undo_link.prev_txn_ ^ (TXN_START_ID),
                       tuple.ToString(&table_info->schema_), tuple_meta.ts_);
        } else {
          fmt::println(stderr, "  txn{}, <del>, ts={}", undo_link.prev_txn_ ^ (TXN_START_ID), undo_log->ts_);
        }

        undo_link = undo_log->prev_version_;
      } else {
        fmt::println(stderr, "  txn{}, <del>, ts={} ", undo_link.prev_txn_ ^ (TXN_START_ID), undo_log->ts_);
        tuple_meta = TupleMeta{undo_log->ts_, true};
        undo_link = undo_log->prev_version_;
      }
    }
  }

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
