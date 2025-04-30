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
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto schema = table_info->schema_;

  int num_inserted = 0;
  std::vector<RID> rids;

  // Insert tuples from the child executor into the table heap
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto txn = exec_ctx_->GetTransaction();
    // 判断是否存在索引冲突
    for (auto &index_info : index_vector) {
      auto key =
          child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->ScanKey(key, &rids, transaction);
      if (!rids.empty()) {
        auto [tuple_meta, tuple, _] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), rids[0]);
        // tuple没有被删除或者另一个事务在操作产生写写冲突
        if (!tuple_meta.is_deleted_) {
          transaction->SetTainted();
          throw ExecutionException("insert conflict");
        }
        CheckWriteWriteConflict(transaction, tuple_meta);
      }
    }

    if (rids.empty()) {
      auto ret = table_info->table_->InsertTuple(TupleMeta{transaction->GetTransactionTempTs(), false}, child_tuple,
                                                 exec_ctx_->GetLockManager(), transaction);
      if (!ret.has_value()) {
        LOG_DEBUG("InsertExecutor: failed to insert tuple into table heap.");
        continue;
      }
      child_rid = ret.value();
      UndoLink undo_link{INVALID_TXN_ID, 0};  // first undo_link
      auto checker = [](std::optional<UndoLink> prelink) -> bool {
        if (prelink.has_value()) {
          return prelink.value().prev_txn_ == TXN_START_ID;
        }
        return true;
      };
      bool success = txn_mgr->UpdateUndoLink(child_rid, undo_link, checker);
      if (!success) {
        // 说明其他事务已经插入了这个tuple，该事务需要终止
        transaction->SetTainted();
        throw ExecutionException("insert conflict");
      }
      txn->AppendWriteSet(table_info->oid_, child_rid);

      for (auto &index : index_vector) {
        auto key = child_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        auto res = index->index_->InsertEntry(key, ret.value(), transaction);
        if (!res) {
          transaction->SetTainted();
          throw ExecutionException("insert conflict");
        }
      }
    } else {
      // tuple 被删除，但是索引还存在，更新tuple元数据以及Undolog
      RID rid = rids[0];
      auto table_heap = table_info->table_.get();
      auto [meta, _, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);
      // 判断是否有写写冲突
      CheckWriteWriteConflict(transaction, meta);
      BUSTUB_ASSERT(undo_link.has_value(), "InsertExecutor: undolog is null");
      UndoLog undo_log;
      UndoLink new_undo_link = undo_link.value();
      if (meta.ts_ != transaction->GetTransactionId()) {
        // 如果不是当前事务删除的话，需要生成新的undolog插入到undo_link中
        undo_log = GenerateNewUndoLog(&schema, nullptr, &child_tuple, meta.ts_, new_undo_link);
        new_undo_link = {transaction->GetTransactionId(), static_cast<int>(transaction->GetUndoLogNum())};
        transaction->AppendUndoLog(undo_log);
      }

      meta.is_deleted_ = false;
      meta.ts_ = transaction->GetTransactionTempTs();

      auto checker = [transaction](const TupleMeta &meta, const Tuple &tuple, RID rid,
                                   std::optional<UndoLink> pre_link) -> bool {
        return !((meta.ts_ > TXN_START_ID || meta.ts_ > transaction->GetReadTs()) &&
                 meta.ts_ != transaction->GetTransactionId());
      };
      bool success =
          UpdateTupleAndUndoLink(txn_mgr, rid, new_undo_link, table_heap, transaction, meta, child_tuple, checker);
      if (!success) {
        transaction->SetTainted();
        throw ExecutionException("insert conflict");
      }
      transaction->AppendWriteSet(plan_->GetTableOid(), rid);
    }
    num_inserted++;
    rids.clear();
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(ValueFactory::GetIntegerValue(num_inserted));
  *tuple = Tuple{values, &GetOutputSchema()};
  // *rid = child_rid;
  is_return_ = true;
  return true;
}

}  // namespace bustub
