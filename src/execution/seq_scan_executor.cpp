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
  if (iter_->IsEnd()) {
    // throw ExecutionException("SeqScanExecutor: table iterator is end.");
    // LOG_DEBUG("SeqScanExecutor: table iterator is end.");
    return false;
  }
  // Iterate through the table and apply the filter predicate
  while (!iter_->IsEnd()) {
    auto [meta, current_tuple] = iter_->GetTuple();
    ++(*iter_);
    if (!meta.is_deleted_ && (plan_->filter_predicate_ == nullptr ||
                              plan_->filter_predicate_->Evaluate(&current_tuple, GetOutputSchema()).GetAs<bool>())) {
      *rid = current_tuple.GetRid();
      *tuple = current_tuple;
      return true;
    }
  }
  // No more tuples to scan
  // LOG_DEBUG("SeqScanExecutor: no more tuples to scan.");
  return false;
}

}  // namespace bustub
