//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      ht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      ht_iterator_(ht_.Begin()) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  ht_.Clear();
  Tuple child_tuple{};
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    AggregateKey key = MakeAggregateKey(&child_tuple);
    AggregateValue value = MakeAggregateValue(&child_tuple);
    ht_.InsertCombine(key, value);
  }
  if (ht_.Size() == static_cast<size_t>(0) && GetOutputSchema().GetColumnCount() == 1) {
    ht_.InsertEmptyCombine();
  }
  ht_iterator_ = ht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ht_iterator_ == ht_.End()) {
    return false;
  }
  std::vector<Value> values{};
  values.insert(values.end(), ht_iterator_.Key().group_bys_.begin(), ht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), ht_iterator_.Val().aggregates_.begin(), ht_iterator_.Val().aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = RID{1, 0};
  ++ht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
