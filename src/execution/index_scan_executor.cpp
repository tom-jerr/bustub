//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/index/b_plus_tree_index.h"
#include "type/value_factory.h"
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  b_tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(
      exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
  index_ = 0;
  if (plan_->pred_keys_.empty()) {
    // 只有在进行全表扫描的时候才进行初始化，否则会造成 leaf_guard 的读锁未释放，update 或者 insert 又要获取写锁
    index_iter_ = b_tree_->GetBeginIterator();
  }
  auto txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    txn->AppendScanPredicate(plan_->table_oid_, plan_->filter_predicate_);
  }
}

auto IndexScanExecutor::FullScan(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  if (index_iter_ == b_tree_->GetEndIterator()) {
    LOG_DEBUG("IndexScanExecutor: No more tuples to scan");
    return false;
  }
  auto [_, tree_rid] = *index_iter_;
  auto copy_rid = tree_rid;
  auto [meta_, tuple_] = table_info->table_->GetTuple(copy_rid);
  ++index_iter_;
  if (meta_.is_deleted_) {
    return false;
  }
  if (plan_->filter_predicate_) {
    auto value = plan_->filter_predicate_->Evaluate(&tuple_, GetOutputSchema());
    if (value.IsNull() || !value.GetAs<bool>()) {
      return false;
    }
  }
  *tuple = tuple_;
  *rid = tuple_.GetRid();
  return true;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<RID> result;
  std::vector<Value> values;
  if (plan_->pred_keys_.empty()) {
    return FullScan(tuple, rid);
  }
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto key_schema = index_info->key_schema_;

  while (index_ < plan_->pred_keys_.size()) {
    values.emplace_back(plan_->pred_keys_[index_]->Evaluate(nullptr, plan_->OutputSchema()));
    ++index_;
    auto new_tuple = Tuple{values, &key_schema};
    b_tree_->ScanKey(new_tuple, &result, exec_ctx_->GetTransaction());
    if (result.empty()) {
      LOG_DEBUG("IndexScanExecutor: This key does not exist");
      values.clear();
      continue;
    }
    *rid = result[0];
    auto page_guard = table_info->table_->AcquireTablePageReadLock(*rid);
    auto page = page_guard.As<TablePage>();
    auto tuple_meta = table_info->table_->GetTupleMetaWithLockAcquired(*rid, page);

    auto txn = exec_ctx_->GetTransaction();
    auto txn_ts = txn->GetReadTs();
    auto tuple_ts = tuple_meta.ts_;
    if (tuple_meta.is_deleted_ && ((txn_ts >= tuple_ts && tuple_ts < TXN_START_ID) ||
                                   (tuple_ts > TXN_START_ID && tuple_ts == txn->GetTransactionId()))) {
      // 说明tuple被删除了
      values.clear();
      continue;
    }

    auto [meta_again, original_tuple, pre_link] = GetTupleAndUndoLink(txn_mgr, table_info->table_.get(), *rid);
    tuple_ts = meta_again.ts_;
    if (tuple_ts > txn_ts && tuple_ts != txn->GetTransactionId()) {
      // 回退
      std::vector<UndoLog> undo_logs;
      BUSTUB_ASSERT(pre_link.has_value(), "IndexScanExecutor: undolink is null");
      UndoLink undo_link = pre_link.value();
      auto undo_log = txn_mgr->GetUndoLogOptional(undo_link);
      while (undo_log.has_value() && tuple_ts > txn_ts) {
        undo_logs.emplace_back(*undo_log);
        tuple_ts = undo_log->ts_;
        undo_link = undo_log->prev_version_;
        undo_log = txn_mgr->GetUndoLogOptional(undo_link);
      }

      if (tuple_ts > txn_ts) {
        // LOG_DEBUG("IndexScanExecutor: This key does not exist");
        return false;
      }
      auto recon_tuple = ReconstructTuple(&table_info->schema_, original_tuple, meta_again, undo_logs);
      if (!recon_tuple.has_value()) {
        return false;
      }

      *tuple = recon_tuple.value();
    } else {
      *tuple = original_tuple;
    }

    if (plan_->filter_predicate_) {
      auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        // *rid = RID();
        return true;
      }
    }
    return true;
  }
  // LOG_DEBUG("IndexScanExecutor: No more tuples to scan");
  return false;
}

}  // namespace bustub
