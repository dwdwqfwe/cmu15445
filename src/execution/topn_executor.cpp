#include "execution/executors/topn_executor.h"
#include <cstdint>
#include <iostream>
#include <utility>
#include "common/logger.h"
#include "common/rid.h"
#include "execution/executors/sort_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      cmp_(plan->output_schema_.get(), plan_->order_bys_),
      pr_n_container_(cmp_) {}

void TopNExecutor::Init() {
  LOG_DEBUG("topn init begin");
  Tuple child_tuple{};
  RID child_rid{};
  auto temp_heap = pr_n_container_;
  child_executor_->Init();
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    LOG_DEBUG("11111111");
    temp_heap.emplace(child_tuple);
  }
  for (uint32_t i = 0; i < plan_->GetN() && !temp_heap.empty(); i++) {
    pr_n_container_.emplace(temp_heap.top());
    temp_heap.pop();
  }
  while (!temp_heap.empty()) {
    temp_heap.pop();
  }
  LOG_DEBUG("topn init end");
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("topn next begin");
  if (pr_n_container_.empty()) {
    return false;
  }
  *tuple = pr_n_container_.top();
  pr_n_container_.pop();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return pr_n_container_.size(); };

}  // namespace bustub
