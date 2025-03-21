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

#include "execution/executors/insert_executor.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  txn_ = exec_ctx_->GetTransaction();
  txn_manager_ = exec_ctx_->GetTransactionManager();
  child_exec_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_insert_) {
    return false;
  }
  is_insert_ = true;
  int count = 0;
  auto *table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // std::unique_ptr<TableHeap> table_heap=std::move(table_info->table_);
  auto schema = table_info->schema_;

  auto index_names = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  while (child_exec_->Next(tuple, rid)) {
    for (auto &index_info : index_names) {
      if (index_info->is_primary_key_) {
        auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        std::vector<RID> res;
        index_info->index_->ScanKey(key, &res, txn_);
        if (!res.empty()) {
          txn_manager_->SetTxnTained(txn_);
          throw ExecutionException("this is g same primary key-----------");
        }
      }
    }
    TupleMeta meta{txn_->GetTransactionId(), false};
    std::optional<RID> rid_opt = table_info->table_->InsertTuple(meta, *tuple);
    RID new_rid;
    if (rid_opt != std::nullopt) {
      new_rid = rid_opt.value();
    }
    txn_->AppendWriteSet(plan_->GetTableOid(), new_rid);
    for (auto &index_info : index_names) {
      auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      bool exsit = index_info->index_->InsertEntry(key, new_rid, this->exec_ctx_->GetTransaction());
      if (!exsit) {
        txn_manager_->SetTxnTained(txn_);
        throw ExecutionException("insert false because exsit same primary");
      }
    }
    count++;
    LOG_DEBUG("insert");
  }
  std::vector<Value> res{{TypeId::INTEGER, count}};
  *tuple = Tuple(res, &GetOutputSchema());
  return true;
}

}  // namespace bustub
