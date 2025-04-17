//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

auto NestedLoopJoinExecutor::LeftAntiJoinTuple(Tuple *left_tuple) -> Tuple {
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
    values.emplace_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), i));
  }
  // 对于左连接中不存在的列添加 NULL
  for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
    // 获取右表元组中的值，但由于左连接中不存在右表的元组，因此添加 NULL 值
    values.emplace_back(ValueFactory::GetNullValueByType(plan_->GetRightPlan()->OutputSchema().GetColumn(i).GetType()));
  }
  return Tuple{values, &GetOutputSchema()};
}

auto NestedLoopJoinExecutor::InnerJoinTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {  //内连接
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());

  for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(left_tuple->GetValue(&left_executor_->GetOutputSchema(), idx));
  }
  for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
    values.push_back(right_tuple->GetValue(&right_executor_->GetOutputSchema(), idx));
  }

  return Tuple{values, &GetOutputSchema()};
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID left_rid;
  left_done_ = !left_executor_->Next(&left_tuple_, &left_rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple right_tuple{};
  RID left_rid{1, 0};
  RID right_rid{1, 0};
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  // right_done_ 保证不会重复输出 left_tuple
  while (!left_done_) {
    // scan all right tuples
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        right_done_ = true;
        *tuple = InnerJoinTuple(&left_tuple_, &right_tuple);
        *rid = left_rid;
        return true;
      }
    }
    // left join
    if (plan_->GetJoinType() == JoinType::LEFT && !right_done_) {
      *tuple = LeftAntiJoinTuple(&left_tuple_);
      *rid = left_rid;
      left_done_ = !left_executor_->Next(&left_tuple_, &left_rid);
      right_executor_->Init();
      right_done_ = false;
      return true;
    }
    // right is done
    left_done_ = !left_executor_->Next(&left_tuple_, &left_rid);
    right_executor_->Init();
    right_done_ = false;
  }
  return false;
}

}  // namespace bustub
