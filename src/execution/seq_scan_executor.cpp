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
#include "common/logger.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  // Initialize the plan and the table heap
  plan_ = plan;
  if (plan_ == nullptr) {
    throw ExecutionException("SeqScanExecutor: plan is null.");
  }
  if (exec_ctx == nullptr) {
    throw ExecutionException("SeqScanExecutor: exec_ctx is null.");
  }
  if (exec_ctx->GetCatalog() == nullptr) {
    throw ExecutionException("SeqScanExecutor: catalog is null.");
  }
}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->table_oid_;
  // auto table_name = plan_->table_name_;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid);
  if (table_info == nullptr) {
    throw ExecutionException("SeqScanExecutor: table not found.");
  }
  auto table_heap = table_info->table_.get();
  if (table_heap == nullptr) {
    throw ExecutionException("SeqScanExecutor: table heap is null.");
  }
  auto schema = table_info->schema_;
  // Initialize the table iterator
  iter_ = std::make_unique<TableIterator>(table_heap->MakeIterator());
}
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_->IsEnd()) {
    // throw ExecutionException("SeqScanExecutor: table iterator is end.");
    LOG_DEBUG("SeqScanExecutor: table iterator is end.");
    return false;
  }
  // Iterate through the table and apply the filter predicate
  while (!iter_->IsEnd()) {
    auto [meta, current_tuple] = iter_->GetTuple();
    if (!meta.is_deleted_ && (plan_->filter_predicate_ == nullptr ||
                              plan_->filter_predicate_->Evaluate(&current_tuple, GetOutputSchema()).GetAs<bool>())) {
      *tuple = current_tuple;
      *rid = current_tuple.GetRid();
      ++(*iter_);
      return true;
    }
    ++(*iter_);
  }
  // No more tuples to scan
  LOG_DEBUG("SeqScanExecutor: no more tuples to scan.");
  return false;
}

}  // namespace bustub
