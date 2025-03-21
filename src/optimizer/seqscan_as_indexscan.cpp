#include <cstdint>
#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> optize_children;
  for (auto &child : plan->GetChildren()) {
    optize_children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optize_plan = plan->CloneWithChildren(optize_children);

  if (optize_plan->GetType() == PlanType::SeqScan) {
    auto seq_scan = dynamic_cast<SeqScanPlanNode *>(optize_plan.get());
    auto predicate = seq_scan->filter_predicate_;
    if (predicate != nullptr) {
      auto comp_filt = std::dynamic_pointer_cast<ComparisonExpression>(predicate);
      if (comp_filt != nullptr && comp_filt->comp_type_ == ComparisonType::Equal) {
        auto colum = dynamic_cast<ColumnValueExpression *>(comp_filt->GetChildAt(0).get());
        auto table_info = catalog_.GetTable(seq_scan->table_oid_);
        auto col_idx = colum->GetColIdx();
        auto indexs_info = catalog_.GetTableIndexes(table_info->name_);
        for (auto &index_info : indexs_info) {
          auto colums_key = index_info->index_->GetKeyAttrs();
          std::vector<uint32_t> colums;
          colums.push_back(col_idx);
          if (colums == colums_key) {
            auto pre_key = dynamic_cast<ConstantValueExpression *>(comp_filt->GetChildAt(1).get());
            return std::make_shared<IndexScanPlanNode>(seq_scan->output_schema_, seq_scan->table_oid_,
                                                       index_info->index_oid_, predicate, pre_key);
          }
        }
      }
    }
  }

  return optize_plan;
}

}  // namespace bustub
