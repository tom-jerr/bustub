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

#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  // auto transaction = exec_ctx_->GetTransaction();
  auto schema = table_info->schema_;
  auto table_heap = table_info->table_.get();
  auto txn_mgr = exec_ctx_->GetTransactionManager();

  Tuple delete_tuple{};
  RID delete_rid;
  int num_deleted = 0;
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    auto tuple_meta = table_info->table_->GetTupleMeta(delete_rid);
    auto txn = exec_ctx_->GetTransaction();

    // whether has write-write confilict
    // 1. txnA confilict txnB
    // 2. txnA commit, txnB conflict tuple
    if ((tuple_meta.ts_ > TXN_START_ID && tuple_meta.ts_ != txn->GetTransactionId()) ||
        (tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionId())) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    }

    // need to generate undo log
    if (tuple_meta.ts_ <= txn->GetReadTs()) {
      // generate new tuple
      // this is first generate
      auto pre_link = txn_mgr->GetUndoLink(delete_rid);
      // 如果是第一次insert的话，也存在没有undolink的可能
      if (!pre_link.has_value()) {
        // 说明是insert后第一次delete，因为Insert没有建立undolog，所以我们需要构建新的版本链并挂载生成的undolog
        UndoLink undo_link{INVALID_TXN_ID, 0};
        txn_mgr->UpdateUndoLink(delete_rid, undo_link);
        pre_link = txn_mgr->GetUndoLink(delete_rid);
        BUSTUB_ASSERT(pre_link.has_value(), "生成undolink失败");
      }
      auto undo_log = GenerateNewUndoLog(&schema, &delete_tuple, nullptr, tuple_meta.ts_, pre_link.value());
      tuple_meta.is_deleted_ = true;
      tuple_meta.ts_ = txn->GetTransactionTempTs();

      UndoLink new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};
      txn->AppendUndoLog(undo_log);
      txn->AppendWriteSet(plan_->GetTableOid(), delete_rid);
      UpdateTupleAndUndoLink(txn_mgr, delete_rid, new_undo_link, table_heap, txn, tuple_meta, delete_tuple);
    } else {
      // 事务自己对自己修改过的tuple再次修改
      BUSTUB_ASSERT(tuple_meta.ts_ == txn->GetTransactionId(), "DeleteExectutor: 后面的事务修改了前面的事务");

      // combine undolog
      auto old_link = txn_mgr->GetUndoLink(delete_rid);
      if (!old_link.has_value()) {
        tuple_meta.is_deleted_ = true;
        tuple_meta.ts_ = txn->GetTransactionTempTs();
        // 直接修改tuple
        auto page_write_guard = table_heap->AcquireTablePageWriteLock(delete_rid);
        auto page = page_write_guard.AsMut<TablePage>();
        table_heap->UpdateTupleInPlaceWithLockAcquired(tuple_meta, delete_tuple, delete_rid, page);

      } else {
        auto old_undo_log = txn_mgr->GetUndoLogOptional(old_link.value());
        BUSTUB_ASSERT(old_undo_log.has_value(), "delete的undo_log无值");
        if (old_undo_log.has_value()) {
          auto new_undo_log = GenerateUpdatedUndoLog(&schema, &delete_tuple, nullptr, old_undo_log.value());
          tuple_meta.is_deleted_ = true;
          tuple_meta.ts_ = txn->GetTransactionTempTs();

          auto page_write_guard = table_heap->AcquireTablePageWriteLock(delete_rid);
          auto page = page_write_guard.AsMut<TablePage>();
          table_heap->UpdateTupleInPlaceWithLockAcquired(tuple_meta, delete_tuple, delete_rid, page);

          txn->ModifyUndoLog(old_link->prev_log_idx_, new_undo_log);
        }
      }
    }
    num_deleted++;
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(ValueFactory::GetIntegerValue(num_deleted));  // The number of rows deleted
  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = tuple->GetRid();
  is_return_ = true;
  return true;
}

}  // namespace bustub
