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
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto schema = GetOutputSchema();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto txn = exec_ctx_->GetTransaction();

  // Iterate through the table and apply the filter predicate
  while (!iter_->IsEnd()) {
    *rid = iter_->GetRID();

    auto page_guard = table_info->table_->AcquireTablePageReadLock(*rid);
    auto page = page_guard.As<TablePage>();
    auto [meta, tuple_data] = table_info->table_->GetTupleWithLockAcquired(*rid, page);
    *tuple = tuple_data;

    ++(*iter_);
    // get undo logs if tuple_ts > txn_ts
    auto undolink = txn_mgr->GetUndoLink(*rid);
    auto undologs = CollectUndoLogs(*rid, meta, *tuple, undolink, txn, txn_mgr);
    if (undologs.has_value()) {
      // reconstruct tuple
      auto new_tuple = ReconstructTuple(&schema, *tuple, meta, undologs.value());
      if (!new_tuple.has_value()) {
        continue;
      }
      meta.is_deleted_ = false;
      *tuple = *new_tuple;
    } else {
      continue;
    }

    if (!meta.is_deleted_ && (plan_->filter_predicate_ == nullptr ||
                              plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema()).GetAs<bool>())) {
      return true;
    }
  }
  // No more tuples to scan
  LOG_DEBUG("SeqScanExecutor: no more tuples to scan.");
  return false;
}

}  // namespace bustub
