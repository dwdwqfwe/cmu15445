//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"

namespace bustub {
class HashJoinKey {
 public:
  std::vector<Value> key_;
  auto operator==(const HashJoinKey &other) const -> bool {
    if (key_.size() != other.key_.size()) {
      return false;
    }
    for (uint32_t i = 0; i < other.key_.size(); i++) {
      if (other.key_[i].CompareEquals(key_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub
namespace std {
template <>
class hash<bustub::HashJoinKey> {
 public:
  auto operator()(const bustub::HashJoinKey &hash_key) const {
    size_t cur = 0;
    for (auto &key : hash_key.key_) {
      if (!key.IsNull()) {
        cur = bustub::HashUtil::CombineHashes(cur, bustub::HashUtil::HashValue(&key));
      }
    }
    return cur;
  }
};
}  // namespace std
namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class SimpleHashJoinTable {
 public:
  auto InsertKey(HashJoinKey &key, Tuple *val) {
    if (ht_.count(key) == 0) {
      ht_.insert(std::make_pair(key, std::vector<Tuple>()));
    }
    ht_[key].emplace_back(*val);
  }

  auto FindKey(HashJoinKey &key) -> std::unordered_map<HashJoinKey, std::vector<Tuple>>::iterator {
    if (ht_.count(key) == 0) {
      return ht_.end();
    }
    return ht_.find(key);
  }

  void Clear() { ht_.clear(); }

  auto End() { return ht_.end(); }

 private:
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_;
};
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  auto GetLeftKey(Tuple *tuple) -> HashJoinKey {
    std::vector<Value> res;
    for (auto &expr : plan_->LeftJoinKeyExpressions()) {
      res.emplace_back(expr->Evaluate(tuple, plan_->GetLeftPlan()->OutputSchema()));
    }

    return {res};
  }

  auto GetRightKey(Tuple *tuple) -> HashJoinKey {
    std::vector<Value> res;
    for (auto &expr : plan_->RightJoinKeyExpressions()) {
      res.emplace_back(expr->Evaluate(tuple, plan_->GetRightPlan()->OutputSchema()));
    }
    return {res};
  }
  auto GetNextLeft() -> void;

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  SimpleHashJoinTable ht_;
  std::unique_ptr<AbstractExecutor> left_child_exec_;
  std::unique_ptr<AbstractExecutor> right_child_exec_;
  Tuple left_tuple_{};
  RID left_rid_{};
  std::shared_ptr<HashJoinKey> left_key_;
  Tuple right_tuple_{};
  RID right_rid_{};
  std::vector<Tuple>::iterator right_tuple_iter_;
  bool left_bool_;
  bool has_done_;
};

}  // namespace bustub
