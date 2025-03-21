//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <vector>
#include "catalog/column.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/rid.h"
#include "execution/execution_common.h"
#include "include/concurrency/transaction_manager.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_iter_(nullptr) {
  txn_ = exec_ctx->GetTransaction();
  txn_manager_ = exec_ctx->GetTransactionManager();
}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  auto table_iter = table_heap_->MakeEagerIterator();
  rids_.clear();
  while (!table_iter.IsEnd()) {
    rids_.push_back(table_iter.GetRID());
    ++table_iter;
  }
  LOG_DEBUG("%zu", rids_.size());
  rids_iter_ = rids_.begin();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_DEBUG("scan begin");
  TupleMeta meta{};
  while (true) {
    // LOG_DEBUG(" scan looping begin");
    if (rids_iter_ == rids_.end()) {
      // LOG_DEBUG("end");
      return false;
    }
    meta = table_heap_->GetTupleMeta(*rids_iter_);
    *tuple = table_heap_->GetTuple(*rids_iter_).second;
    std::cout << tuple->GetValue(&GetOutputSchema(), 0).ToString();
    bool is_delete = meta.is_deleted_;
    bool is_get_log = true;
    if (meta.ts_ <= txn_->GetReadTs() || txn_->GetTransactionId() == meta.ts_) {
      // LOG_DEBUG("aaaaaaaaaaaaaaaaaaaaaa");
      if (!is_delete) {
        *tuple = table_heap_->GetTuple(*rids_iter_).second;
        *rid = *rids_iter_;
      }
    } else {
      // LOG_DEBUG("bbbbbbbbbbbbbbbbbbb");
      *tuple = table_heap_->GetTuple(*rids_iter_).second;
      *rid = *rids_iter_;
      auto undo_link_opt = txn_manager_->GetUndoLink(*rids_iter_);
      if (undo_link_opt == std::nullopt) {
        is_get_log = false;
      } else {
        auto undo_link = undo_link_opt.value();
        while (true) {
          // LOG_DEBUG("ccccccccccccccc");
          if (undo_link.prev_txn_ == INVALID_TXN_ID) {
            is_get_log = false;
            break;
          }
          auto undo_log = txn_manager_->GetUndoLog(undo_link);
          // LOG_DEBUG("deug 1");
          auto tuple_opt = ReconstructTuple(&GetOutputSchema(), *tuple, meta, {undo_log});
          if (tuple_opt != std::nullopt) {
            *tuple = tuple_opt.value();
          }
          // LOG_DEBUG("debug 2");
          if (undo_log.ts_ <= txn_->GetReadTs() && tuple_opt.has_value()) {
            is_delete = false;
            break;
          }
          undo_link = undo_log.prev_version_;
        }
      }
    }

    rids_iter_++;
    Value value;
    if (is_get_log && !is_delete && plan_->filter_predicate_ != nullptr) {
      value = plan_->filter_predicate_->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->schema_);
    }
    // LOG_DEBUG("debug3333");
    if (is_get_log && !is_delete && (value.IsNull() || (!value.IsNull() && value.GetAs<bool>()))) {
      break;
    }
    // LOG_DEBUG("scan loop end");
  };

  return true;
}

}  // namespace bustub
