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

#include "execution/executors/delete_executor.h"
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  txn_ = exec_ctx->GetTransaction();
  txn_manager_ = exec_ctx->GetTransactionManager();
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple{};
  RID child_rid{};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    store_.emplace_back(child_tuple, child_rid);
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_insert_) {
    return false;
  }
  is_insert_ = true;

  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto index_names = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  int count = 0;
  for (auto &tuple_info : store_) {
    LOG_DEBUG("delet loop begin");
    auto old_meta = table_info->table_->GetTupleMeta(tuple_info.second);
    bool check_res = CheckConflict(old_meta, tuple_info.second, txn_);
    if (!check_res) {
      txn_manager_->SetTxnTained(txn_);
      throw ExecutionException("delete false----------------");
    }
    auto rid = tuple_info.second;
    auto tuple = table_info->table_->GetTuple(tuple_info.second).second;
    if (old_meta.ts_ >= TXN_START_ID) {
      auto old_undo_link = txn_manager_->GetUndoLink(rid);
      if (old_undo_link.has_value()) {
        UndoLog undo_log;
        undo_log.tuple_ = tuple;
        undo_log.is_deleted_ = false;
        for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
          undo_log.modified_fields_.push_back(true);
        }
        undo_log.prev_version_ = txn_manager_->GetUndoLog(old_undo_link.value()).prev_version_;
        undo_log.ts_ = txn_manager_->GetUndoLog(old_undo_link.value()).ts_;
        table_info->table_->UpdateTupleMeta(TupleMeta{txn_->GetTransactionId(), true}, rid);
      }  //在这个事务内创建的元组
      else {
        table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, rid);
      }
    } else {
      LOG_DEBUG("init delet-------------------------");
      UndoLog undo_log;
      undo_log.tuple_ = tuple;
      undo_log.is_deleted_ = false;
      for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
        undo_log.modified_fields_.push_back(true);
      }
      auto opt_pre_link = txn_manager_->GetUndoLink(rid);
      if (opt_pre_link.has_value()) {
        undo_log.prev_version_ = opt_pre_link.value();
      }
      undo_log.ts_ = old_meta.ts_;
      auto undo_link = txn_->AppendUndoLog(undo_log);

      txn_manager_->UpdateUndoLink(rid, undo_link);
      table_info->table_->UpdateTupleMeta(TupleMeta{txn_->GetTransactionId(), true}, rid);
    }
    for (auto index_info : index_names) {
      auto key = tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, tuple_info.second, exec_ctx_->GetTransaction());
    }
    txn_->AppendWriteSet(table_info->oid_, rid);
    count++;
    LOG_DEBUG("delet loop end");
  }
  std::vector<Value> value{{TypeId::INTEGER, count}};
  *tuple = Tuple(value, &GetOutputSchema());
  return true;
}

}  // namespace bustub
