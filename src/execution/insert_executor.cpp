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
#include <vector>
#include "common/logger.h"
#include "execution/executors/values_executor.h"
#include "execution/plans/values_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // Initialize the plan and the child executor
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
  child_executor_ = std::move(child_executor);
  if (child_executor_ == nullptr) {
    throw ExecutionException("InsertExecutor: child executor is null.");
  }
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_return_) {
    return false;
  }
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid);
  if (table_info == nullptr) {
    throw ExecutionException("InsertExecutor: table not found.");
  }
  auto table_heap = table_info->table_.get();
  if (table_heap == nullptr) {
    throw ExecutionException("InsertExecutor: table heap is null.");
  }
  // Insert tuples from the child executor into the table heap
  int num_inserted = 0;
  auto output_schema = GetOutputSchemaPtr();
  std::vector<Value> values(1);
  while (true) {
    Tuple child_tuple;
    RID child_rid;
    auto next = child_executor_->Next(&child_tuple, &child_rid);
    if (!next) {
      // No more tuples to insert
      LOG_DEBUG("InsertExecutor: no more tuples to insert.");

      break;
    }
    auto tuple_meta = TupleMeta{INVALID_TS, false};
    auto ret = table_heap->InsertTuple(tuple_meta, child_tuple, exec_ctx_->GetLockManager(),
                                       exec_ctx_->GetTransaction(), table_oid);
    if (ret == std::nullopt) {
      LOG_DEBUG("InsertExecutor: fail to insert this one.");
      values.clear();
      values.emplace_back(ValueFactory::GetIntegerValue(num_inserted));
      *tuple = Tuple{values, output_schema.get()};
      is_return_ = true;
      return true;
    }
    // update index
    auto table_name = catalog->GetTable(table_oid)->name_;
    auto table_schema = catalog->GetTable(table_oid)->schema_;
    auto indexs = catalog->GetTableIndexes(table_name);
    if (!indexs.empty()) {
      for (auto &index_info : indexs) {
        // auto index_oid = index_info->index_oid_;
        auto index_heap = index_info->index_.get();
        auto key_attrs = index_heap->GetKeyAttrs();
        auto key_schema = index_info->key_schema_;
        auto key_tuple = child_tuple.KeyFromTuple(table_schema, key_schema, key_attrs);
        auto status = index_heap->InsertEntry(key_tuple, *ret, exec_ctx_->GetTransaction());
        if (!status) {
          LOG_DEBUG("InsertExecutor: fail to insert into index.");
          return false;
        }
      }
    }
    num_inserted++;
  }
  values.clear();
  values.emplace_back(ValueFactory::GetIntegerValue(num_inserted));
  *tuple = Tuple{values, output_schema.get()};
  is_return_ = true;
  return true;
}

}  // namespace bustub
