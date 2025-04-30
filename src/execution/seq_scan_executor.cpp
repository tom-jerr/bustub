//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // Initialize the table iterator
  iter_ = std::make_unique<TableIterator>(table_info->table_->MakeIterator());
  auto txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto schema = GetOutputSchema();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  //  auto txn = exec_ctx_->GetTransaction();

  // Iterate through the table and apply the filter predicate
  while (!iter_->IsEnd()) {
    *rid = iter_->GetRID();

    auto [meta, tuple_data, pre_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), *rid);
    *tuple = tuple_data;

    ++(*iter_);
    auto txn = exec_ctx_->GetTransaction();
    auto tuple_ts = meta.ts_;
    auto txn_ts = txn->GetReadTs();

    if (tuple_ts > txn_ts && tuple_ts != txn->GetTransactionId()) {
      std::vector<UndoLog> undo_logs;
      std::optional<UndoLog> undo_log;
      // 循环获取undo_log，直到undo_log的timestamp小于等于txn的timestamp
      if (pre_link.has_value()) {
        auto min_ts = tuple_ts;
        UndoLink undo_link = pre_link.value();
        while (undo_link.IsValid()) {
          undo_log = txn_mgr->GetUndoLogOptional(undo_link);
          if (!undo_log.has_value()) {
            break;
          }
          min_ts = undo_log->ts_;
          undo_logs.emplace_back(*undo_log);
          if (min_ts <= txn_ts) {
            break;
          }
          undo_link = undo_log->prev_version_;
        }
        if (min_ts > txn_ts) {
          // goto next tuple
          continue;
        }

        // ReconstructTuple tuple
        auto new_tuple = ReconstructTuple(&schema, *tuple, meta, undo_logs);
        if (!new_tuple.has_value()) {
          // goto next tuple
          continue;
        }
        meta.is_deleted_ = false;
        *tuple = *new_tuple;
      } else {
        // 不应该看见tuple
        continue;
      }
    }
    if (!meta.is_deleted_ &&
        (plan_->filter_predicate_ == nullptr || plan_->filter_predicate_->Evaluate(tuple, schema).GetAs<bool>())) {
      return true;
    }
  }
  // No more tuples to scan
  LOG_DEBUG("SeqScanExecutor: no more tuples to scan.");
  return false;
}

}  // namespace bustub
