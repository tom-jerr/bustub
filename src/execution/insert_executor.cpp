//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>
#include <vector>
#include "common/logger.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/values_executor.h"
#include "execution/plans/values_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_return_) {
    return false;
  }

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto transaction = exec_ctx_->GetTransaction();
  auto index_vector = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int num_inserted = 0;

  // Insert tuples from the child executor into the table heap
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto ret = table_info->table_->InsertTuple(TupleMeta{transaction->GetTransactionTempTs(), false}, child_tuple,
                                               exec_ctx_->GetLockManager(), transaction);
    if (!ret.has_value()) {
      LOG_DEBUG("InsertExecutor: failed to insert tuple into table heap.");
      continue;
    }
    child_rid = ret.value();

    exec_ctx_->GetTransactionManager()->UpdateUndoLink(child_rid, std::nullopt, nullptr);
    exec_ctx_->GetTransaction()->AppendWriteSet(table_info->oid_, child_rid);

    for (auto &index : index_vector) {
      index->index_->InsertEntry(
          child_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs()), ret.value(),
          transaction);
    }

    num_inserted++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(ValueFactory::GetIntegerValue(num_inserted));
  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = child_rid;
  is_return_ = true;
  return true;
}

}  // namespace bustub
