#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <utility>
#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->output_schema_.get(), plan->GetOrderBy()) {
  child_exec_ = std::move(child_executor);
  order_by_ = plan->GetOrderBy();
}

void SortExecutor::Init() {
  LOG_DEBUG("sort init be");
  Tuple child_tuple{};
  RID child_rid{};
  child_exec_->Init();
  res_.clear();
  while (child_exec_->Next(&child_tuple, &child_rid)) {
    LOG_DEBUG("get one");
    res_.emplace_back(child_tuple);
  }
  LOG_DEBUG("sort init mid");
  std::sort(res_.begin(), res_.end(), cmp_);
  // for(auto &iter:cmp_store_){
  //   res_.emplace_back(temp_[iter.first]);
  // }
  // for(uint32_t i=0 ; i<cmp_store_.size()-1;i++){
  //   if(cmp_store_[i].second.comp)
  // }
  iter_ = 0;
  LOG_DEBUG("sort init end");
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("sort next begin");
  if (iter_ < res_.size()) {
    LOG_DEBUG("into begin");
    *tuple = res_[iter_];
    iter_++;
    LOG_DEBUG("sort next return ");
    return true;
  }
  return false;
}

}  // namespace bustub
