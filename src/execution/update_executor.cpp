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

#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "storage/table/table_heap.h"
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
  // auto transaction = exec_ctx_->GetTransaction();
  TableHeap *table_heap = table_info_->table_.get();
  auto schema = table_info_->schema_;
  Tuple old_tuple{};
  RID old_rid;
  // std::optional<RID> new_rid;
  int num_update = 0;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    auto [tuple_meta, old_tuple, pre_link] = GetTupleAndUndoLink(txn_mgr, table_heap, old_rid);
    // whether has write-write confilict
    CheckWriteWriteConflict(txn, tuple_meta);
    auto checker = [txn](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> pre_link) -> bool {
      return !((meta.ts_ > TXN_START_ID || meta.ts_ > txn->GetReadTs()) && meta.ts_ != txn->GetTransactionId());
    };
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (auto &expr : plan_->target_expressions_) {
      new_values.emplace_back(expr->Evaluate(&old_tuple, schema));
    }
    Tuple new_tuple{new_values, &schema};

    // need to generate undo log
    if (tuple_meta.ts_ <= txn->GetReadTs()) {
      // generate new tuple
      // this is first generate
      BUSTUB_ASSERT(pre_link.has_value(), "UpdateExecutor: undolink is null");
      auto undo_log = GenerateNewUndoLog(&schema, &old_tuple, &new_tuple, tuple_meta.ts_, pre_link.value());

      tuple_meta.is_deleted_ = false;
      tuple_meta.ts_ = txn->GetTransactionTempTs();

      UndoLink new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};

      bool success =
          UpdateTupleAndUndoLink(txn_mgr, old_rid, new_undo_link, table_heap, txn, tuple_meta, new_tuple, checker);
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("update conflict");
      }
      txn->AppendUndoLog(undo_log);
      txn->AppendWriteSet(plan_->GetTableOid(), old_rid);

    } else {
      // 事务自己对自己修改过的tuple再次修改
      BUSTUB_ASSERT(tuple_meta.ts_ == txn->GetTransactionId(), "UpdateExecutor: 后面的事务修改了前面的事务");

      // combine undolog
      BUSTUB_ASSERT(pre_link.has_value(), "Update的undolink无值");
      auto old_undo_log = txn_mgr->GetUndoLogOptional(pre_link.value());
      UndoLog new_undo_log;
      if (old_undo_log.has_value()) {
        // 否则更新成一个undolog
        new_undo_log = GenerateUpdatedUndoLog(&schema, &old_tuple, &new_tuple, old_undo_log.value());
        txn->ModifyUndoLog(pre_link->prev_log_idx_, new_undo_log);
      }

      tuple_meta.is_deleted_ = false;
      tuple_meta.ts_ = txn->GetTransactionTempTs();
      bool success =
          UpdateTupleAndUndoLink(txn_mgr, old_rid, pre_link, table_heap, txn, tuple_meta, new_tuple, checker);
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("update conflict");
      }
    }
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
