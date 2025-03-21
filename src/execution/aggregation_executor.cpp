//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include "common/logger.h"
#include "common/rid.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      ht_(std::make_unique<SimpleAggregationHashTable>(plan->GetAggregates(), plan->agg_types_)) {}

void AggregationExecutor::Init() {
  LOG_DEBUG("aggre init begin");
  Tuple child_tuple{};
  RID child_rid{};
  child_executor_->Init();
  ht_->Clear();
  uint32_t count = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    count++;
    auto key = MakeAggregateKey(&child_tuple);
    auto val = MakeAggregateValue(&child_tuple);
    ht_->InsertCombine(key, val);
  }
  LOG_DEBUG("init debug1");
  is_inert_ = false;
  iter_ = std::make_unique<SimpleAggregationHashTable::Iterator>(ht_->Begin());
  std::cout << count << "\n";
  LOG_DEBUG("aggre init end");
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // LOG_DEBUG("aggre next begin");
  if (ht_->Begin() != ht_->End()) {
    if (*iter_ == ht_->End()) {
      return false;
    }
    auto agg_key = iter_->Key();
    auto agg_value = iter_->Val();
    std::vector<Value> res_val;
    res_val.reserve(agg_value.aggregates_.size() + agg_key.group_bys_.size());
    for (auto &key : agg_key.group_bys_) {
      res_val.emplace_back(key);
    }
    for (auto &val : agg_value.aggregates_) {
      res_val.emplace_back(val);
    }
    *tuple = Tuple(res_val, &GetOutputSchema());
    ++(*iter_);
    // LOG_DEBUG("end1");
    return true;
  }
  if (is_inert_) {
    LOG_DEBUG("end2");
    return false;
  }
  is_inert_ = true;
  if (plan_->group_bys_.empty()) {
    auto agg_val = ht_->GenerateInitialAggregateValue();
    std::vector<Value> res_val;
    res_val.reserve(plan_->aggregates_.size());
    for (auto &val : agg_val.aggregates_) {
      res_val.emplace_back(val);
    }
    *tuple = Tuple{res_val, &GetOutputSchema()};
    LOG_DEBUG("end3");
    return true;
  }
  LOG_DEBUG("end4");
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
