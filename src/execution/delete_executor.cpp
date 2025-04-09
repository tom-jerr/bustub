//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_return_) {
    return false;
  }
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_vector = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto transaction = exec_ctx_->GetTransaction();

  Tuple delete_tuple{};
  RID delete_rid;
  int num_deleted = 0;
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    auto meta = TupleMeta{transaction->GetTransactionTempTs(), true};
    table_info->table_->UpdateTupleMeta(meta, delete_rid);

    for (auto &index_info : index_vector) {
      // Delete the entry from the index
      index_info->index_->DeleteEntry(
          delete_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs()),
          delete_rid, transaction);
    }
    num_deleted++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(ValueFactory::GetIntegerValue(num_deleted));  // The number of rows deleted
  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = delete_rid.GetPageId() != INVALID_PAGE_ID ? delete_rid : RID{1, 0};
  is_return_ = true;
  return true;
}

}  // namespace bustub
