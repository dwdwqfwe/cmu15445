//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_exec_ = std::move(left_child);
  right_child_exec_ = std::move(right_child);
  ht_ = SimpleHashJoinTable();
}

void HashJoinExecutor::Init() {
  LOG_DEBUG("hash join init begin");
  ht_.Clear();
  left_child_exec_->Init();
  right_child_exec_->Init();
  left_key_ = std::make_shared<HashJoinKey>();
  while (right_child_exec_->Next(&right_tuple_, &right_rid_)) {
    auto key = GetRightKey(&right_tuple_);
    ht_.InsertKey(key, &right_tuple_);
  }
  LOG_DEBUG("hash join init mid");
  GetNextLeft();
  LOG_DEBUG("hash join init end");
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  LOG_DEBUG("hash join next begin");
  while (left_bool_) {
    auto iter = ht_.FindKey(*left_key_);
    if (iter != ht_.End() && right_tuple_iter_ == iter->second.end()) {
      GetNextLeft();
      continue;
      ;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> res_val;
      if (iter == ht_.End() && !has_done_) {
        for (uint32_t i = 0; i < left_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
          res_val.emplace_back(left_tuple_.GetValue(&left_child_exec_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
          res_val.emplace_back(
              ValueFactory::GetNullValueByType(right_child_exec_->GetOutputSchema().GetColumn(i).GetType()));
        }

        *tuple = Tuple(res_val, &GetOutputSchema());
        has_done_ = true;
        return true;
      }
      if (iter == ht_.End()) {
        GetNextLeft();
        continue;
      }
      if (iter != ht_.End()) {
        for (uint32_t i = 0; i < left_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
          res_val.emplace_back(left_tuple_.GetValue(&left_child_exec_->GetOutputSchema(), i));
        }

        for (uint32_t i = 0; i < right_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
          res_val.emplace_back(right_tuple_iter_->GetValue(&right_child_exec_->GetOutputSchema(), i));
        }
        *tuple = Tuple(res_val, &GetOutputSchema());
        right_tuple_iter_++;
        has_done_ = true;
        return true;
      }
    }

    if (plan_->GetJoinType() == JoinType::INNER) {
      std::vector<Value> res_val;
      if (iter == ht_.End()) {
        GetNextLeft();
        continue;
      }
      for (uint32_t i = 0; i < left_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
        res_val.emplace_back(left_tuple_.GetValue(&left_child_exec_->GetOutputSchema(), i));
      }

      for (uint32_t i = 0; i < right_child_exec_->GetOutputSchema().GetColumnCount(); i++) {
        res_val.emplace_back(right_tuple_iter_->GetValue(&right_child_exec_->GetOutputSchema(), i));
      }
      *tuple = Tuple(res_val, &GetOutputSchema());
      right_tuple_iter_++;
      return true;
    }
  }
  return false;
}

auto HashJoinExecutor::GetNextLeft() -> void {
  left_bool_ = left_child_exec_->Next(&left_tuple_, &left_rid_);
  LOG_DEBUG("hash join init mid111");
  *left_key_ = GetLeftKey(&left_tuple_);
  LOG_DEBUG("hash join init mid2222");
  auto iter = ht_.FindKey(*left_key_);
  LOG_DEBUG("hash join getnext mid");
  if (iter != ht_.End()) {
    right_tuple_iter_ = iter->second.begin();
  }
  has_done_ = false;
}

}  // namespace bustub
