//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <optional>

#include "common/logger.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;                                 // The plan to execute
  child_executor_ = std::move(child_executor);  // The child executor that feeds the update
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid()).get();
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  // updataplan->filterplan->seqscanplan
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_return_) {
    return false;
  }
  auto index_vector = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  auto transaction = exec_ctx_->GetTransaction();
  Tuple old_tuple{};
  RID old_rid;
  std::optional<RID> new_rid;
  int num_update = 0;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // delete the old tuple
    auto meta = TupleMeta{transaction->GetTransactionTempTs(), true};  // Mark the old tuple as deleted
    table_info_->table_->UpdateTupleMeta(meta, old_rid);               // Mark the old tuple as deleted

    // insert new tuple
    std::vector<Value> tuple_values{};
    // values.reserve(GetOutputSchema().GetColumnCount());
    // only filter plan and behind its schema is the table schema
    for (const auto &expr : plan_->target_expressions_) {
      tuple_values.emplace_back(expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple{tuple_values, &child_executor_->GetOutputSchema()};
    auto new_meta = TupleMeta{transaction->GetTransactionTempTs(), false};
    new_rid = table_info_->table_->InsertTuple(new_meta, new_tuple);
    if (!new_rid.has_value()) {
      LOG_DEBUG("Failed to insert new tuple after update, rid: %s", old_rid.ToString().c_str());
      return false;
    }
    // update the index
    for (auto &index_info : index_vector) {
      index_info->index_->InsertEntry(
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          new_rid.value(), transaction);

      index_info->index_->DeleteEntry(
          old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          old_rid, transaction);
    }
    // old_rid = new_rid.value();  // Update the old rid to the new rid
    num_update++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(ValueFactory::GetIntegerValue(num_update));
  *tuple = Tuple{values, &GetOutputSchema()};  // Return the number of rows updated
  //*rid = new_rid.value();                      // not really uesd
  is_return_ = true;  // Mark that we have returned the result for this update
  return true;
}

}  // namespace bustub
