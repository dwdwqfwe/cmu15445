//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstdint>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_exec_ = std::move(left_executor);
  right_child_exec_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_child_exec_->Init();
  right_child_exec_->Init();
  left_has_tuple_ = left_child_exec_->Next(&left_tuple_, &left_rid_);
  has_done_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (plan_->GetJoinType() == JoinType::LEFT) {
    LOG_DEBUG("left join");
    while (left_has_tuple_) {
      LOG_DEBUG("outer");
      std::vector<Value> temp_val;
      while (right_child_exec_->Next(&right_tuple_, &right_rid_)) {
        auto statu = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_child_exec_->GetOutputSchema(), &right_tuple_,
                                                      right_child_exec_->GetOutputSchema());
        bool deter = statu.GetAs<bool>();
        if (deter) {
          for (uint32_t i = 0; i < left_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
            temp_val.emplace_back(left_tuple_.GetValue(&left_child_exec_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < right_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
            temp_val.emplace_back(right_tuple_.GetValue(&right_child_exec_->GetOutputSchema(), i));
          }
          *tuple = Tuple(temp_val, &GetOutputSchema());
          has_done_ = true;
          return true;
        }
      }
      if (!has_done_) {
        for (uint32_t i = 0; i < left_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
          temp_val.emplace_back(left_tuple_.GetValue(&left_child_exec_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
          temp_val.push_back(
              ValueFactory::GetNullValueByType(right_child_exec_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(temp_val, &GetOutputSchema());
        LOG_DEBUG("nullnullnull");
        has_done_ = true;
        return true;
      }
      left_has_tuple_ = left_child_exec_->Next(&left_tuple_, &left_rid_);
      right_child_exec_->Init();
      has_done_ = false;
    }
    return false;
  }
  if (plan_->join_type_ == JoinType::INNER) {
    LOG_DEBUG("inner join");
    while (left_has_tuple_) {
      while (right_child_exec_->Next(&right_tuple_, &right_rid_)) {
        LOG_DEBUG("inner loop1");
        auto statu = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_child_exec_->GetOutputSchema(), &right_tuple_,
                                                      right_child_exec_->GetOutputSchema());
        bool deter = statu.GetAs<bool>();
        std::vector<Value> temp_val;
        if (deter) {
          LOG_DEBUG("inner join get one");
          for (uint32_t i = 0; i < left_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
            temp_val.emplace_back(left_tuple_.GetValue(&left_child_exec_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < right_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
            temp_val.emplace_back(right_tuple_.GetValue(&right_child_exec_->GetOutputSchema(), i));
          }
          *tuple = Tuple(temp_val, &GetOutputSchema());
          return true;
        }
      }
      left_has_tuple_ = left_child_exec_->Next(&left_tuple_, &left_rid_);
      right_child_exec_->Init();
    }
    return false;
  }
  return false;
}

}  // namespace bustub
