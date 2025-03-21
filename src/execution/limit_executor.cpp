//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_exec_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  LOG_DEBUG("limit init begin");
  uint32_t terminal = 0;
  Tuple child_tuple{};
  RID child_rid{};
  child_exec_->Init();
  while (terminal < plan_->GetLimit() && child_exec_->Next(&child_tuple, &child_rid)) {
    terminal++;
    res_.emplace_back(child_tuple);
  }
  iter_ = 0;
  LOG_DEBUG("limit init end");
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("limit next begin");
  if (iter_ < res_.size()) {
    *tuple = res_[iter_];
    iter_++;
    return true;
  }
  LOG_DEBUG("limit next end");
  return false;
}

}  // namespace bustub
