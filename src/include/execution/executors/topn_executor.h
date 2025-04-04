//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class TopNSort {
 public:
  TopNSort(const Schema *schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by)
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
        if (cmp1.CompareGreaterThan(cmp2) == CmpBool::CmpTrue) {
          return true;
        }
      } else {
        if (cmp1.CompareLessThan(cmp2) == CmpBool::CmpTrue) {
          return true;
        }
      }
      return false;
    }
    return false;
  }
};
/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  TopNSort cmp_;
  std::priority_queue<Tuple, std::vector<Tuple>, TopNSort> pr_n_container_;
};
}  // namespace bustub
