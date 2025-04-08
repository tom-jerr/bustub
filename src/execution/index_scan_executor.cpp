//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/logger.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/index/b_plus_tree_index.h"
#include "type/value_factory.h"
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  b_tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(
      exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
  index_iter_ = b_tree_->GetBeginIterator();
}

auto IndexScanExecutor::FullScan(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  if (index_iter_ == b_tree_->GetEndIterator()) {
    return false;
  }
  auto [_, tree_rid] = *index_iter_;
  auto copy_rid = tree_rid;
  auto [meta_, tuple_] = table_info->table_->GetTuple(copy_rid);
  ++index_iter_;
  if (meta_.is_deleted_) {
    return false;
  }
  if (plan_->filter_predicate_) {
    auto value = plan_->filter_predicate_->Evaluate(&tuple_, GetOutputSchema());
    if (value.IsNull() || !value.GetAs<bool>()) {
      return false;
    }
  }
  *tuple = tuple_;
  *rid = tuple_.GetRid();
  return true;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  std::vector<Value> values;
  if (plan_->pred_keys_.empty()) {
    return FullScan(tuple, rid);
  }
  while (index_ < plan_->pred_keys_.size()) {
    values.emplace_back(plan_->pred_keys_[index_]->Evaluate(nullptr, plan_->OutputSchema()));
    ++index_;
    auto key_schema = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->key_schema_;
    auto new_tuple = Tuple{values, &key_schema};
    b_tree_->ScanKey(new_tuple, &result, exec_ctx_->GetTransaction());
    if (result.empty()) {
      return false;
    }
    auto temp_rid = result[0];
    auto [meta_, tuple_] = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(temp_rid);
    if (meta_.is_deleted_) {
      return false;
    }
    if (plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(&tuple_, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        return false;
      }
    }
    *tuple = tuple_;
    *rid = tuple_.GetRid();
    return true;
  }
  LOG_DEBUG("IndexScanExecutor: No more tuples to scan");
  return false;
}

}  // namespace bustub
