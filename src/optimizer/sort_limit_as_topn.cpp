#include <memory>
#include <vector>
#include "common/logger.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  // LOG_DEBUG("opt limit begin");
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto opt_plan = plan->CloneWithChildren(children);
  if (opt_plan->GetType() == PlanType::Limit) {
    auto limit_plan = dynamic_cast<LimitPlanNode *>(opt_plan.get());
    if (limit_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      auto sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan->GetChildAt(0).get());
      // LOG_DEBUG("opt true end");
      return std::make_shared<TopNPlanNode>(opt_plan->output_schema_, limit_plan->GetChildAt(0), sort_plan->order_bys_,
                                            limit_plan->GetLimit());
    }
  }
  // LOG_DEBUG("opt false end");
  return opt_plan;
}

}  // namespace bustub
