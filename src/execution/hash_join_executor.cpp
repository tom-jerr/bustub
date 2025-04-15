//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <memory>
#include "binder/table_ref/bound_join_ref.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)),
      jht_(std::make_unique<SimpleHashJoinHashTable>()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  // 1. init child executors
  left_child_executor_->Init();
  right_child_executor_->Init();

  // 2. build hash table for right table
  Tuple right_tuple{};
  RID right_rid;
  while (right_child_executor_->Next(&right_tuple, &right_rid)) {
    HashJoinKey key = GetRightJoinKey(&right_tuple);
    jht_->Insert(key, right_tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!results_.empty()) {
    *tuple = results_.front();
    *rid = RID{1, 0};
    results_.pop_front();
    return true;
  }
  auto left_schema = left_child_executor_->GetOutputSchema();
  auto right_schema = right_child_executor_->GetOutputSchema();
  while (left_child_executor_->Next(tuple, rid)) {
    auto left_hash_key = GetLeftJoinKey(tuple);
    if (jht_->GetValue(left_hash_key) != nullptr) {
      for (const auto &right_tuple : *jht_->GetValue(left_hash_key)) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.emplace_back(tuple->GetValue(&left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_schema, i));
        }
        results_.emplace_back(Tuple{values, &GetOutputSchema()});
      }
    } else {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.emplace_back(tuple->GetValue(&left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
          values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
        }
        results_.emplace_back(Tuple{values, &GetOutputSchema()});
      }
    }
    // 直到这次的left_tuple与right_tuples匹配的数组都被遍历完才会继续下一次的left_tuple
    if (!results_.empty()) {
      *tuple = results_.front();
      *rid = RID{1, 0};
      results_.pop_front();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
