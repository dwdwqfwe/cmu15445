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

#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
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
  timestamp_t time_last = last_commit_ts_;
  txn_ref->read_ts_ = time_last;
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  LOG_DEBUG("commiting begin");
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  auto latest_time = last_commit_ts_.load() + 1;
  // TODO(fall2023): acquire commit ts!
  txn->commit_ts_ = latest_time;
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
  for (auto &write_iter : write_set) {
    for (auto &rid_iter : write_iter.second) {
      auto meta = catalog_->GetTable(write_iter.first)->table_->GetTupleMeta(rid_iter);
      // auto &tuple = tuple_info.second;
      if (meta.ts_ != 0) {
        meta.ts_ = latest_time;
        catalog_->GetTable(write_iter.first)->table_->UpdateTupleMeta(meta, rid_iter);
        if (auto iter = version_info_.find(rid_iter.GetPageId()); iter != version_info_.end()) {
          auto &prev_version = iter->second->prev_version_;
          if (auto iter = prev_version.find(rid_iter.GetSlotNum()); iter != prev_version.end()) {
            iter->second.base_ts_ = latest_time;
          }
        }
      }
      // std::cout<<meta.ts_<<"kkkkkkkkkkkkk\n";
      // std::cout<<catalog_->GetTable(write_iter.first)->table_->GetTuple(rid_iter).first.ts_<<"\n";
      // LOG_DEBUG("CHANGE TUPLE last time");
    }
  }
  undolog_counts_[txn->GetTransactionId()] = txn->undo_logs_.size();
  last_commit_ts_++;
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

  txn->state_ = TransactionState::COMMITTED;
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
  timestamp_t watermark = GetWatermark();

  std::unordered_map<txn_id_t, uint32_t> invalid_undolog_cnts;

  for (const auto &[page_id, page_version_info] : version_info_) {
    std::unique_lock<std::shared_mutex> page_version_lck(page_version_info->mutex_);

    for (const auto &[offset, version_link] : page_version_info->prev_version_) {
      UndoLink undo_link = version_link.prev_;
      // Traverse version link list.
      timestamp_t base_ts = version_link.base_ts_;
      bool first_smaller = base_ts <= watermark;  // If its the first smaller or equal than watermark.
      while (txn_map_.find(undo_link.prev_txn_) != txn_map_.end()) {
        auto undo_log = GetUndoLog(undo_link);
        if (undo_log.ts_ <= watermark) {
          if (!first_smaller) {
            // The first undolog smaller or equal than read_ts.
            first_smaller = true;
          } else {
            // The left undologs are all invalid.
            while (txn_map_.find(undo_link.prev_txn_) != txn_map_.end()) {
              undo_log = GetUndoLog(undo_link);
              invalid_undolog_cnts[undo_link.prev_txn_] += 1;
              undo_link = undo_log.prev_version_;
            }
            break;
          }
        }
        undo_link = undo_log.prev_version_;
      }
    }
  }

  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    auto txn = it->second;
    auto txn_id = txn->GetTransactionId();
    if (txn->state_ == TransactionState::COMMITTED &&
        (undolog_counts_[txn_id] == 0 || undolog_counts_[txn_id] == invalid_undolog_cnts[txn_id])) {
      it = txn_map_.erase(it);
      undolog_counts_.erase(txn_id);
    } else {
      ++it;
    }
  }
}

void TransactionManager::SetTxnTained(Transaction *txn) {
  std::unique_lock<std::shared_mutex> lc(txn_map_mutex_);
  txn->SetTainted();
}

}  // namespace bustub
