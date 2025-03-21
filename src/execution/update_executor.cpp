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
#include <algorithm>
#include <memory>
#include <optional>
#include <vector>
#include "common/config.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    tuple_store_.emplace_back(tuple, rid);
  }
  iter_ = 0;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_insert_) {
    return false;
  }
  int count = 0;
  is_insert_ = true;
  auto index_names = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple child_tuple;
  RID child_rid;
  while (iter_ < tuple_store_.size()) {
    child_tuple = tuple_store_[iter_].first;
    child_rid = tuple_store_[iter_].second;
    iter_++;
    auto old_meta = table_info_->table_->GetTupleMeta(child_rid);
    auto old_tuple = table_info_->table_->GetTuple(child_rid).second;
    if (!CheckConflict(old_meta, child_rid, txn_)) {
      txn_manager_->SetTxnTained(txn_);
      throw ExecutionException("Write-write conflict detected in UpdateExecutor.");
    }
    table_info_->table_->UpdateTupleMeta(TupleMeta{txn_->GetTransactionId(), false}, child_rid);
    auto new_meta = table_info_->table_->GetTupleMeta(child_rid);
    std::vector<Value> target_value;
    target_value.reserve(plan_->target_expressions_.size());
    for (auto &express : plan_->target_expressions_) {
      target_value.push_back(express->Evaluate(&child_tuple, table_info_->schema_));
    }
    Tuple update_tuple(target_value, &table_info_->schema_);
    table_info_->table_->UpdateTupleInPlace(new_meta, update_tuple, child_rid);
    for (auto &index : index_names) {
      auto key = update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key, child_rid, exec_ctx_->GetTransaction());
      auto old_tuple = table_info_->table_->GetTuple(child_rid).second;
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key, child_rid, exec_ctx_->GetTransaction());
    }
    count++;
    txn_->AppendWriteSet(table_info_->oid_, child_rid);
    if (old_meta.ts_ >= TXN_START_ID) {
      auto version_undo_link = txn_manager_->GetVersionLink(child_rid);
      if (version_undo_link.has_value()) {
        auto before_log = txn_manager_->GetUndoLog(version_undo_link->prev_);
        auto undo_log = CreateUndoLog(update_tuple, old_tuple, table_info_->schema_, before_log);
        undo_log.ts_ = txn_manager_->GetUndoLog(version_undo_link->prev_).ts_;
        undo_log.prev_version_ = txn_manager_->GetUndoLog(version_undo_link->prev_).prev_version_;
        txn_->ModifyUndoLog(version_undo_link->prev_.prev_log_idx_, undo_log);
      }
    } else {
      auto undo_log = CreateUndoLog(update_tuple, old_tuple, table_info_->schema_);
      undo_log.is_deleted_ = false;
      undo_log.ts_ = old_meta.ts_;
      auto opt_undo_link = txn_manager_->GetUndoLink(child_rid);
      if (opt_undo_link.has_value()) {
        undo_log.prev_version_ = opt_undo_link.value();
      }
      auto undo_link = txn_->AppendUndoLog(undo_log);
      txn_manager_->UpdateUndoLink(child_rid, undo_link);
    }
  }
  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &GetOutputSchema());
  return true;
}
}  // namespace bustub
