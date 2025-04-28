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
#include "common/macros.h"
#include "concurrency/transaction.h"
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
    // auto tuple_meta = table_info->table_->GetTupleMeta(delete_rid);
    auto txn = exec_ctx_->GetTransaction();

    auto [tuple_meta, delete_tuple, pre_link] = GetTupleAndUndoLink(txn_mgr, table_heap, delete_rid);

    CheckWriteWriteConflict(txn, tuple_meta);
    auto checker = [txn](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> pre_link) -> bool {
      return !((meta.ts_ > TXN_START_ID || meta.ts_ > txn->GetReadTs()) && meta.ts_ != txn->GetTransactionId());
    };
    // need to generate undo log
    if (tuple_meta.ts_ <= txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionId()) {
      // generate new tuple
      // 之前一定有insert or update
      BUSTUB_ASSERT(pre_link.has_value(), "DeleteExecutor: undolink is null");

      auto undo_log = GenerateNewUndoLog(&schema, &delete_tuple, nullptr, tuple_meta.ts_, pre_link.value());
      tuple_meta.is_deleted_ = true;
      tuple_meta.ts_ = txn->GetTransactionTempTs();

      UndoLink new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};
      bool success = UpdateTupleAndUndoLink(txn_mgr, delete_rid, new_undo_link, table_heap, txn, tuple_meta,
                                            delete_tuple, checker);
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("delete conflict");
      }
      txn->AppendUndoLog(undo_log);
      txn->AppendWriteSet(plan_->GetTableOid(), delete_rid);
    } else if (tuple_meta.ts_ == txn->GetTransactionId()) {
      // 事务自己对自己修改过的tuple再次修改
      BUSTUB_ASSERT(tuple_meta.ts_ == txn->GetTransactionId(), "DeleteExectutor: 后面的事务修改了前面的事务");
      // combine undolog
      // auto old_link = txn_mgr->GetUndoLink(delete_rid);
      BUSTUB_ASSERT(pre_link.has_value(), "delete的undolink无值");
      // 但insert可能没有undolog
      auto old_undo_log = txn_mgr->GetUndoLogOptional(pre_link.value());
      UndoLog new_undo_log;
      UndoLink new_undo_link = pre_link.value();
      if (old_undo_log.has_value()) {
        new_undo_log = GenerateUpdatedUndoLog(&schema, &delete_tuple, nullptr, old_undo_log.value());
        txn->ModifyUndoLog(pre_link->prev_log_idx_, new_undo_log);
        // new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum() - 1)};
      }

      tuple_meta.is_deleted_ = true;
      tuple_meta.ts_ = txn->GetTransactionTempTs();
      bool success = UpdateTupleAndUndoLink(txn_mgr, delete_rid, new_undo_link, table_heap, txn, tuple_meta,
                                            delete_tuple, checker);
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("delete conflict");
      }
    } else {
      txn->SetTainted();
      throw ExecutionException("delete conflict");
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
