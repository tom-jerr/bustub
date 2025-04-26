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
    auto tuple_meta = table_info_->table_->GetTupleMeta(old_rid);
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();

    // whether has write-write confilict
    if (tuple_meta.ts_ > txn->GetTransactionId() && tuple_meta.ts_ != txn->GetTransactionId()) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    }

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
      auto pre_link = txn_mgr->GetUndoLink(old_rid);
      // 如果是第一次insert的话，也存在没有undolink的可能
      if (!pre_link.has_value()) {
        // 说明是insert后第一次修改，因为Insert没有建立undolog，所以我们需要构建新的版本链并挂载生成的undolog
        UndoLink undo_link{INVALID_TXN_ID, 0};
        txn_mgr->UpdateUndoLink(old_rid, undo_link);
        pre_link = txn_mgr->GetUndoLink(old_rid);
        BUSTUB_ASSERT(pre_link.has_value(), "生成undolink失败");
      }
      auto undo_log = GenerateNewUndoLog(&schema, &old_tuple, &new_tuple, tuple_meta.ts_, pre_link.value());
      tuple_meta.is_deleted_ = undo_log.is_deleted_;
      tuple_meta.ts_ = txn->GetTransactionTempTs();

      UndoLink new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};
      txn->AppendUndoLog(undo_log);
      txn->AppendWriteSet(plan_->GetTableOid(), old_rid);
      UpdateTupleAndUndoLink(txn_mgr, old_rid, new_undo_link, table_heap, txn, tuple_meta, new_tuple);
    } else {
      // 事务自己对自己修改过的tuple再次修改
      BUSTUB_ASSERT(tuple_meta.ts_ == txn->GetTransactionId(), "后面的事务修改了前面的事务");

      // combine undolog
      auto old_link = txn_mgr->GetUndoLink(old_rid);
      if (!old_link.has_value()) {
        // 直接修改tuple
        auto page_write_guard = table_heap->AcquireTablePageWriteLock(old_rid);
        auto page = page_write_guard.AsMut<TablePage>();
        table_heap->UpdateTupleInPlaceWithLockAcquired(tuple_meta, new_tuple, old_rid, page);
        num_update++;

      } else {
        auto old_undo_log = txn_mgr->GetUndoLogOptional(old_link.value());
        BUSTUB_ASSERT(old_undo_log.has_value(), "Update的undo_log无值");
        if (old_undo_log.has_value()) {
          auto new_undo_log = GenerateUpdatedUndoLog(&schema, &old_tuple, &new_tuple, old_undo_log.value());

          auto page_write_guard = table_heap->AcquireTablePageWriteLock(old_rid);
          auto page = page_write_guard.AsMut<TablePage>();
          table_heap->UpdateTupleInPlaceWithLockAcquired(tuple_meta, new_tuple, old_rid, page);

          txn->ModifyUndoLog(old_link->prev_log_idx_, new_undo_log);
        }
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
