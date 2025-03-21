//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

class CmpSort {
 public:
  CmpSort(const Schema *schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by)
      : order_by_(std::move(order_by)), schema_(schema) {}
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by_;
  const Schema *schema_;

  auto operator()(Tuple &tuple1, Tuple &tuple2) -> bool {
    for (auto &iter : order_by_) {
      auto cmp1 = iter.second->Evaluate(&tuple1, *schema_);
      auto cmp2 = iter.second->Evaluate(&tuple2, *schema_);
      if (cmp1.CompareEquals(cmp2) == CmpBool::CmpTrue) {
        continue;
      }
      if (iter.first == OrderByType::ASC || iter.first == OrderByType::DEFAULT) {
        if (cmp1.CompareLessThan(cmp2) == CmpBool::CmpTrue) {
          return true;
        }
      } else {
        if (cmp1.CompareGreaterThan(cmp2) == CmpBool::CmpTrue) {
          return true;
        }
      }
      return false;
    }
    return false;
  }
};
/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::vector<Tuple> res_;
  // std::vector<Tuple> temp_;
  std::unique_ptr<AbstractExecutor> child_exec_;
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by_;
  uint32_t iter_;
  CmpSort cmp_;
};
}  // namespace bustub
