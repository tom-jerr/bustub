//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // read_ts 是最近提交事务的时间戳
  txn_ref->read_ts_.store(last_commit_ts_.load());

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->GetWriteSets().empty()) {
    return true;
  }
  // get predicate of txn
  auto predicate_map = txn->GetScanPredicates();
  std::shared_lock lck(txn_map_mutex_);
  for (auto &iter : txn_map_) {
    auto other_txn = iter.second;
    // if other txn is commited and commit ts is larger than txn's read ts, detect conflict
    if (other_txn->state_ == TransactionState::COMMITTED && other_txn->GetCommitTs() > txn->GetReadTs()) {
      auto write_sets = other_txn->GetWriteSets();
      for (auto &[table_oid, rids] : write_sets) {
        // if table does not have predicate
        if (predicate_map.find(table_oid) == predicate_map.end()) {
          continue;
        }
        auto predicates = predicate_map[table_oid];
        auto table_info = catalog_->GetTable(table_oid);
        auto schema = table_info->schema_;
        for (auto &rid : rids) {
          auto [tuple_meta, tuple, pre_link] = GetTupleAndUndoLink(this, table_info->table_.get(), rid);
          UndoLink undo_link = pre_link.value();
          auto undo_log = GetUndoLogOptional(undo_link);
          // 遍历版本链，确保txn不会访问任何一个比read_ts大的版本
          while (undo_log.has_value() && tuple_meta.ts_ > txn->GetReadTs()) {
            // 判断是否有冲突
            if (!tuple_meta.is_deleted_ && tuple_meta.ts_ < TXN_START_ID) {
              for (auto &predicate : predicates) {
                if (predicate->Evaluate(&tuple, schema).GetAs<bool>()) {
                  return false;
                }
              }
            }
            auto new_tuple = ReconstructTuple(&schema, tuple, tuple_meta, {*undo_log});
            if (new_tuple.has_value()) {
              tuple = new_tuple.value();
            }
            tuple_meta = TupleMeta{undo_log->ts_, undo_log->is_deleted_};
            // 获取下一个版本记录
            undo_link = undo_log->prev_version_;
            undo_log = GetUndoLogOptional(undo_link);
          }
          // detect conflict
          // 判断是否有冲突
          if (!tuple_meta.is_deleted_ && tuple_meta.ts_ < TXN_START_ID) {
            for (auto &predicate : predicates) {
              if (predicate->Evaluate(&tuple, schema).GetAs<bool>()) {
                return false;
              }
            }
          }
        }
      }
    }
  }
  return true;
}

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  // allocate a new commit ts for the transaction
  // commit_ts 是当前时间戳 + 1
  ++last_commit_ts_;
  timestamp_t commit_ts = last_commit_ts_;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  auto write_set = txn->GetWriteSets();
  for (auto &table : write_set) {
    auto table_info = catalog_->GetTable(table.first);
    auto rids = table.second;
    auto table_heap = table_info->table_.get();
    for (auto &rid : rids) {
      auto page_guard = table_info->table_->AcquireTablePageWriteLock(rid);
      auto page = page_guard.AsMut<TablePage>();
      TupleMeta tuple_meta = table_heap->GetTupleMetaWithLockAcquired(rid, page);
      page->UpdateTupleMeta({commit_ts, tuple_meta.is_deleted_}, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = last_commit_ts_.load();
  txn->state_ = TransactionState::COMMITTED;
  // txn_map_[txn->GetTransactionId()] = nullptr;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!
  auto write_set = txn->GetWriteSets();
  for (auto &table : write_set) {
    auto table_info = catalog_->GetTable(table.first);
    auto rids = table.second;
    auto table_heap = table_info->table_.get();
    for (auto &rid : rids) {
      // 更新时间戳和tuple内容
      auto [tuple_meta, tuple, pre_link] = GetTupleAndUndoLink(this, table_heap, rid);
      TupleMeta new_meta;
      Tuple new_tuple = tuple;
      BUSTUB_ASSERT(pre_link.has_value(), "Abort: undolink is null");
      UndoLink undo_link = pre_link.value();
      auto undo_log = GetUndoLogOptional(undo_link);
      if (!undo_log.has_value()) {
        // 全新tuples，只需要将ts设置为0
        new_meta.ts_ = 0;
        new_meta.is_deleted_ = true;
        undo_link = {INVALID_TXN_ID, 0};
      } else {
        auto old_tuple = ReconstructTuple(&table_info->schema_, tuple, tuple_meta, {undo_log.value()});
        if (!old_tuple.has_value()) {
          new_meta.ts_ = undo_log->ts_;
          new_meta.is_deleted_ = true;
        } else {
          new_meta.ts_ = undo_log->ts_;
          new_meta.is_deleted_ = undo_log->is_deleted_;
          new_tuple = old_tuple.value();
        }
        undo_link = {undo_log->prev_version_.prev_txn_, undo_log->prev_version_.prev_log_idx_};
      }
      auto checker = [txn](const TupleMeta &meta, const Tuple &tuple, RID rid,
                           std::optional<UndoLink> pre_link) -> bool {
        return !((meta.ts_ > TXN_START_ID || meta.ts_ > txn->GetReadTs()) && meta.ts_ != txn->GetTransactionId());
      };
      bool success = UpdateTupleAndUndoLink(this, rid, undo_link, table_heap, txn, new_meta, new_tuple, checker);
      if (!success) {
        // LOG_DEBUG("abort failed");
        continue;
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  // UNIMPLEMENTED("not implemented");

  std::unique_lock lock(txn_map_mutex_);
  // get watermark
  timestamp_t watermark_ts = GetWatermark();
  // delete all commited txns with commit_ts < watermark
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    if (it->second->commit_ts_ != INVALID_TXN_ID &&
        (it->second->undo_logs_.empty() || it->second->commit_ts_ < watermark_ts)) {
      it = txn_map_.erase(it);
    } else {
      ++it;
    }
  }
}
void CheckWriteWriteConflict(Transaction *txn, const TupleMeta &meta) {
  if ((meta.ts_ > TXN_START_ID || meta.ts_ > txn->GetReadTs()) && meta.ts_ != txn->GetTransactionId()) {
    txn->SetTainted();
    throw ExecutionException("write-write conflict");
  }
}
}  // namespace bustub
