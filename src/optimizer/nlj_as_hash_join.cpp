#include <algorithm>
#include <cstddef>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ParseExpress(const AbstractExpressionRef &pre, std::vector<AbstractExpressionRef> &left_key_express,
                  std::vector<AbstractExpressionRef> &right_key_express) -> bool {
  auto logic_express = dynamic_cast<LogicExpression *>(pre.get());
  if (logic_express != nullptr && logic_express->logic_type_ == LogicType::Or) {
    return false;
  }
  bool ret = true;
  if (logic_express != nullptr) {
    ret |= ParseExpress(logic_express->GetChildAt(0), left_key_express, right_key_express);
    ret |= ParseExpress(logic_express->GetChildAt(1), left_key_express, right_key_express);
  }
  auto cmp_express = dynamic_cast<ComparisonExpression *>(pre.get());
  if (cmp_express != nullptr) {
    auto col_express = dynamic_cast<ColumnValueExpression *>(cmp_express->GetChildAt(0).get());
    if (col_express->GetTupleIdx() == 0) {
      left_key_express.emplace_back(cmp_express->GetChildAt(0));
      right_key_express.emplace_back(cmp_express->GetChildAt(1));
    } else {
      left_key_express.emplace_back(cmp_express->GetChildAt(1));
      right_key_express.emplace_back(cmp_express->GetChildAt(0));
    }
  }
  return ret;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto opt_plan = plan->CloneWithChildren(children);

  if (opt_plan->GetType() == PlanType::NestedLoopJoin) {
    auto nlj_plan = dynamic_cast<NestedLoopJoinPlanNode *>(opt_plan.get());
    auto predicate = nlj_plan->Predicate();
    std::vector<AbstractExpressionRef> left_key_express;
    std::vector<AbstractExpressionRef> right_key_express;
    if (!ParseExpress(predicate, left_key_express, right_key_express)) {
      return opt_plan;
    }
    return std::make_shared<HashJoinPlanNode>(opt_plan->output_schema_, nlj_plan->GetLeftPlan(),
                                              nlj_plan->GetRightPlan(), left_key_express, right_key_express,
                                              nlj_plan->GetJoinType());
  }

  return opt_plan;
}

}  // namespace bustub
