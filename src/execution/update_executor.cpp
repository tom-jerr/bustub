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
#include "execution/expressions/column_value_expression.h"
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
  std::vector<Tuple> new_tuples;
  std::vector<Tuple> old_tuples;
  std::vector<RID> rids;
  // std::optional<RID> new_rid;
  int num_update = 0;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 生成更新后数据
    std::vector<Value> new_values;
    new_values.reserve(schema.GetColumnCount());
    // 生成更新后的数据
    for (auto &expr : plan_->target_expressions_) {
      new_values.emplace_back(expr->Evaluate(&old_tuple, schema));
    }
    // 更新TableHeap中数据
    auto new_tuple = Tuple{new_values, &schema};
    old_tuples.emplace_back(old_tuple);
    new_tuples.emplace_back(new_tuple);
    rids.emplace_back(old_rid);
    ++num_update;
  }

  // 返回更新的tuple数目
  std::vector<Value> result{{TypeId::INTEGER, num_update}};
  *tuple = Tuple{result, &GetOutputSchema()};

  // 检查有无索引更新
  auto has_index_update = false;
  std::shared_ptr<IndexInfo> index_info;
  for (uint32_t col_idx = 0; col_idx < schema.GetColumnCount(); ++col_idx) {
    auto expr = plan_->target_expressions_[col_idx];
    auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
    if (column_value_expr == nullptr) {
      for (auto &index : index_vector) {
        auto key_attrs = index->index_->GetKeyAttrs();
        if (std::find(key_attrs.begin(), key_attrs.end(), col_idx) != key_attrs.end()) {
          has_index_update = true;
          index_info = index;
          break;
        }
      }
    }
  }

  if (has_index_update) {
    auto txn = exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    for (int i = 0; i < num_update; ++i) {
      // 先删除
      auto [tuple_meta, delete_tuple, pre_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rids[i]);

      CheckWriteWriteConflict(txn, tuple_meta);
      auto checker = [txn](const TupleMeta &meta, const Tuple &tuple, RID rid,
                           std::optional<UndoLink> pre_link) -> bool {
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
        txn->AppendUndoLog(undo_log);
        txn->AppendWriteSet(plan_->GetTableOid(), rids[i]);
        bool success =
            UpdateTupleAndUndoLink(txn_mgr, rids[i], new_undo_link, table_heap, txn, tuple_meta, delete_tuple, checker);
        if (!success) {
          txn->SetTainted();
          throw ExecutionException("delete conflict");
        }
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
          new_undo_log = GenerateUpdatedUndoLog(&schema, &old_tuples[i], nullptr, old_undo_log.value());
          txn->ModifyUndoLog(pre_link->prev_log_idx_, new_undo_log);
          // new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum() - 1)};
        }

        tuple_meta.is_deleted_ = true;
        tuple_meta.ts_ = txn->GetTransactionTempTs();
        bool success = UpdateTupleAndUndoLink(txn_mgr, rids[i], new_undo_link, table_heap, txn, tuple_meta,
                                              old_tuples[i], checker);
        if (!success) {
          txn->SetTainted();
          throw ExecutionException("delete conflict");
        }
      } else {
        txn->SetTainted();
        throw ExecutionException("delete conflict");
      }
    }
    // 再插入
    std::vector<RID> index_rids(1);
    for (int i = 0; i < num_update; ++i) {
      // 判断是否存在索引冲突
      index_rids.clear();
      for (auto &index_info : index_vector) {
        auto key = new_tuples[i].KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                              index_info->index_->GetKeyAttrs());
        index_info->index_->ScanKey(key, &index_rids, txn);
        if (!index_rids.empty()) {
          auto [tuple_meta, _, pre_link] = GetTupleAndUndoLink(txn_mgr, table_heap, index_rids[0]);
          // tuple没有被删除或者另一个事务在操作产生写写冲突
          if (!tuple_meta.is_deleted_) {
            txn->SetTainted();
            throw ExecutionException("insert conflict");
          }
          CheckWriteWriteConflict(txn, tuple_meta);
        }
      }

      if (index_rids.empty()) {
        auto ret = table_info_->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuples[i],
                                                    exec_ctx_->GetLockManager(), txn);
        if (!ret.has_value()) {
          LOG_DEBUG("InsertExecutor: failed to insert tuple into table heap.");
          continue;
        }
        RID new_rid = ret.value();
        UndoLink undo_link{INVALID_TXN_ID, 0};  // first undo_link
        txn->AppendWriteSet(table_info_->oid_, new_rid);
        auto checker = [](std::optional<UndoLink> prelink) -> bool {
          if (prelink.has_value()) {
            return prelink.value().prev_txn_ == TXN_START_ID;
          }
          return true;
        };
        bool success = txn_mgr->UpdateUndoLink(new_rid, undo_link, checker);
        if (!success) {
          // 说明其他事务已经插入了这个tuple，该事务需要终止
          txn->SetTainted();
          throw ExecutionException("insert conflict");
        }

        for (auto &index : index_vector) {
          auto key = new_tuples[i].KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
          auto res = index->index_->InsertEntry(key, new_rid, txn);
          if (!res) {
            txn->SetTainted();
            throw ExecutionException("insert conflict");
          }
        }
      } else {
        // tuple 被删除，但是索引还存在，更新tuple元数据以及Undolog
        RID rid = index_rids[0];
        auto table_heap = table_info_->table_.get();
        auto [meta, _, undo_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rid);
        // 判断是否有写写冲突
        CheckWriteWriteConflict(txn, meta);
        BUSTUB_ASSERT(undo_link.has_value(), "InsertExecutor: undolog is null");
        UndoLog undo_log;
        UndoLink new_undo_link = undo_link.value();
        if (meta.ts_ != txn->GetTransactionId()) {
          // 如果不是当前事务删除的话，需要生成新的undolog插入到undo_link中
          undo_log = GenerateNewUndoLog(&schema, nullptr, &new_tuples[i], meta.ts_, undo_link.value());
          new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};
          txn->AppendUndoLog(undo_log);
          txn->AppendWriteSet(plan_->GetTableOid(), rid);
        }

        meta.is_deleted_ = false;
        meta.ts_ = txn->GetTransactionTempTs();
        auto checker = [txn](const TupleMeta &meta, const Tuple &tuple, RID rid,
                             std::optional<UndoLink> pre_link) -> bool {
          return !((meta.ts_ > TXN_START_ID || meta.ts_ > txn->GetReadTs()) && meta.ts_ != txn->GetTransactionId());
        };
        bool success =
            UpdateTupleAndUndoLink(txn_mgr, rid, new_undo_link, table_heap, txn, meta, new_tuples[i], checker);
        if (!success) {
          txn->SetTainted();
          throw ExecutionException("insert conflict");
        }
      }
    }
    is_return_ = true;
    return true;
  }

  // 无索引更新
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  for (int i = 0; i < num_update; ++i) {
    auto [tuple_meta, _, pre_link] = GetTupleAndUndoLink(txn_mgr, table_heap, rids[i]);
    // whether has write-write confilict
    CheckWriteWriteConflict(txn, tuple_meta);
    auto checker = [txn](const TupleMeta &meta, const Tuple &tuple, RID rid, std::optional<UndoLink> pre_link) -> bool {
      return !((meta.ts_ > TXN_START_ID || meta.ts_ > txn->GetReadTs()) && meta.ts_ != txn->GetTransactionId());
    };

    // need to generate undo log
    if (tuple_meta.ts_ <= txn->GetReadTs()) {
      // generate new tuple
      // this is first generate
      BUSTUB_ASSERT(pre_link.has_value(), "UpdateExecutor: undolink is null");
      auto undo_log = GenerateNewUndoLog(&schema, &old_tuples[i], &new_tuples[i], tuple_meta.ts_, pre_link.value());

      tuple_meta.is_deleted_ = false;
      tuple_meta.ts_ = txn->GetTransactionTempTs();

      UndoLink new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum())};
      txn->AppendUndoLog(undo_log);
      txn->AppendWriteSet(plan_->GetTableOid(), rids[i]);

      bool success =
          UpdateTupleAndUndoLink(txn_mgr, rids[i], new_undo_link, table_heap, txn, tuple_meta, new_tuples[i], checker);
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("update conflict");
      }

    } else {
      // 事务自己对自己修改过的tuple再次修改
      BUSTUB_ASSERT(tuple_meta.ts_ == txn->GetTransactionId(), "UpdateExecutor: 后面的事务修改了前面的事务");

      // combine undolog
      BUSTUB_ASSERT(pre_link.has_value(), "Update的undolink无值");
      auto old_undo_log = txn_mgr->GetUndoLogOptional(pre_link.value());
      UndoLog new_undo_log;
      UndoLink new_undo_link = pre_link.value();
      if (old_undo_log.has_value()) {
        // 否则更新成一个undolog
        new_undo_log = GenerateUpdatedUndoLog(&schema, &old_tuples[i], &new_tuples[i], old_undo_log.value());
        txn->ModifyUndoLog(pre_link->prev_log_idx_, new_undo_log);
        // new_undo_link = {txn->GetTransactionId(), static_cast<int>(txn->GetUndoLogNum() - 1)};
      }

      tuple_meta.is_deleted_ = false;
      tuple_meta.ts_ = txn->GetTransactionTempTs();

      bool success =
          UpdateTupleAndUndoLink(txn_mgr, rids[i], new_undo_link, table_heap, txn, tuple_meta, new_tuples[i], checker);
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("update conflict");
      }
    }
  }

  is_return_ = true;  // Mark that we have returned the result for this update
  return true;
}

}  // namespace bustub
