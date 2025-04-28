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

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

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
