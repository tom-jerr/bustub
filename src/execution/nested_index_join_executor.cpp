//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid;

  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  // 对于从子表中提出的外部元组，向catalog索引中找到内部表元组。
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    Tuple key{};
    // construct key to scan table by index
    std::vector<Value> key_values;
    for (size_t i = 0; i < index_info->key_schema_.GetColumnCount(); i++) {
      key_values.emplace_back(plan_->KeyPredicate()->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    std::vector<RID> index_result;
    index_info->index_->ScanKey(Tuple(key_values, &index_info->key_schema_), &index_result,
                                exec_ctx_->GetTransaction());
    // inner table value
    std::vector<Value> values;
    for (size_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
      values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
    }
    // outer table value
    // 对每一个存在的索引元组，获取内部表元组，与inner tuple组成一个新的结果元组
    for (auto &rid : index_result) {
      std::vector<Value> outer_values;
      outer_values.assign(values.begin(), values.end());
      auto [meta, outer_tuple] = table_info->table_->GetTuple(rid);
      if (!meta.is_deleted_) {
        for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
          outer_values.emplace_back(outer_tuple.GetValue(&plan_->InnerTableSchema(), i));
        }
        outer_tuples_.emplace_back(Tuple(outer_values, &GetOutputSchema()));
      }
    }
    // if not index
    if (index_result.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      for (size_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
        values.emplace_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
      outer_tuples_.emplace_back(Tuple(values, &GetOutputSchema()));
    }
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ >= outer_tuples_.size()) {
    return false;
  }
  *tuple = outer_tuples_[index_++];
  *rid = tuple->GetRid();
  ++index_;
  return true;
}

}  // namespace bustub
