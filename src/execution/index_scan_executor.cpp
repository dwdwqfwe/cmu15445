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
#include <iostream>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/extendible_hash_table_index.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // auto table_info=exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  std::cout << "colum cnt= " << htable->GetName() << "\n";

  // auto table=exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  Value key_temp = plan_->pred_key_->val_;
  std::vector<Value> key_val{key_temp};
  int val = 0;
  if (!key_temp.IsNull()) {
    val = key_temp.GetAs<int>();
    std::cout << val << "\n";
  }
  std::vector<RID> result_tmp{};

  Value v(TypeId::INTEGER, 8);
  Tuple search_key;
  if (val == 8) {
    search_key = {{v}, &index_info->key_schema_};
  } else {
    search_key = {{plan_->pred_key_->val_}, &index_info->key_schema_};
  }

  htable->ScanKey(search_key, &result_tmp, exec_ctx_->GetTransaction());
  rid_ = result_tmp;
  is_insert_ = false;
  std::cout << "rid size= " << rid_.size() << "\n";
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("index begin");
  if (is_insert_) {
    return false;
  }
  is_insert_ = true;
  if (rid_.empty()) {
    return false;
  }
  LOG_DEBUG("debug1");
  std::pair<TupleMeta, Tuple> pair =
      exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid_.begin());
  LOG_DEBUG("debug2");
  TupleMeta meta = pair.first;
  if (!meta.is_deleted_) {
    *rid = *rid_.begin();
    *tuple = pair.second;
  } else {
    return false;
  }
  LOG_DEBUG("index end");
  return true;
}

}  // namespace bustub
