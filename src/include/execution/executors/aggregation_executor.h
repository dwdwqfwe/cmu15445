//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.h
//
// Identification: src/include/execution/executors/aggregation_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for window functions
 */
class SimpleWindowHashTable {
 public:
  /**
   * Construct a new SimpleWindowHashTable instance.
   * @param window_agg_exprs the window aggregation expressions
   * @param window_agg_types the types of window aggregations
   */
  explicit SimpleWindowHashTable(const WindowFunctionType &window_function_type)
      : window_function_type_(window_function_type) {}

  /** @return The initial window aggregate value for this window executor*/
  auto GenerateInitialWindowAggregateValue() -> Value {
    Value value;
    switch (window_function_type_) {
      case WindowFunctionType::CountStarAggregate:
        return ValueFactory::GetIntegerValue(0);
      case WindowFunctionType::Rank:
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
    }
    return {};
  }

  /**
   * Combines the input into the aggregation result.
   * @param[out] result The output rows of aggregate value corresponding to one key
   * @param input The input value
   */
  auto CombineAggregateValues(Value *result, const Value &input) -> Value {
    Value &old_val = *result;
    const Value &new_val = input;
    switch (window_function_type_) {
      case WindowFunctionType::CountStarAggregate:
        old_val = old_val.Add(Value(TypeId::INTEGER, 1));
        break;
      case WindowFunctionType::CountAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = ValueFactory::GetIntegerValue(0);
          }
          old_val = old_val.Add(Value(TypeId::INTEGER, 1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = new_val;
          } else {
            old_val = old_val.Add(new_val);
          }
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = new_val;
          } else {
            old_val = new_val.CompareLessThan(old_val) == CmpBool::CmpTrue ? new_val.Copy() : old_val;
          }
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = new_val;
          } else {
            old_val = new_val.CompareGreaterThan(old_val) == CmpBool::CmpTrue ? new_val.Copy() : old_val;
          }
        }
        break;
      case WindowFunctionType::Rank:
        ++rank_count_;
        if (old_val.CompareEquals(new_val) != CmpBool::CmpTrue) {
          old_val = new_val;
          last_rank_count_ = rank_count_;
        }
        return ValueFactory::GetIntegerValue(last_rank_count_);
    }
    return old_val;
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation
   * @param win_key the key to be inserted
   * @param win_val the value to be inserted
   */
  auto InsertCombine(const AggregateKey &win_key, const Value &win_value) -> Value {
    if (ht_.count(win_key) == 0) {
      ht_.insert({win_key, GenerateInitialWindowAggregateValue()});
    }
    return CombineAggregateValues(&ht_[win_key], win_value);
  }

  /**
   * Find a value with give key
   * @param win_key the key to be used to find its corresponding value
   */
  auto Find(const AggregateKey &win_key) -> Value { return ht_.find(win_key)->second; }
  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  const WindowFunctionType window_function_type_;
  std::unordered_map<AggregateKey, Value> ht_;
  uint32_t rank_count_ = 0;
  uint32_t last_rank_count_ = 0;
};

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> &agg_exprs,
                             const std::vector<AggregationType> &agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue() -> AggregateValue {
    std::vector<Value> values{};
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          // Count start starts at zero.
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          // Others starts at null.
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      LOG_DEBUG("combin begin");
      auto &old_value = result->aggregates_[i];
      auto &new_value = input.aggregates_[i];
      LOG_DEBUG("combin debug");
      switch (agg_types_[i]) {
        case AggregationType::CountStarAggregate:
          old_value = old_value.Add(Value{TypeId::INTEGER, 1});
          break;
        case AggregationType::CountAggregate:
          if (!new_value.IsNull()) {
            if (old_value.IsNull()) {
              old_value = Value{TypeId::INTEGER, 1};
            } else {
              old_value = old_value.Add(Value{TypeId::INTEGER, 1});
            }
          }
          break;
        case AggregationType::SumAggregate:
          if (!new_value.IsNull()) {
            if (old_value.IsNull()) {
              old_value = Value({TypeId::INTEGER, 0});
              old_value = old_value.Add(new_value);
            } else {
              old_value = old_value.Add(new_value);
            }
          }
          break;
        case AggregationType::MinAggregate:
          if (!new_value.IsNull()) {
            if (old_value.IsNull()) {
              old_value = new_value;
            } else {
              old_value = old_value.CompareLessThan(new_value) == CmpBool::CmpTrue ? old_value : new_value;
            }
          }
          break;
        case AggregationType::MaxAggregate:
          if (!new_value.IsNull()) {
            if (old_value.IsNull()) {
              old_value = new_value;
            } else {
              old_value = old_value.CompareLessThan(new_value) == CmpBool::CmpFalse ? old_value : new_value;
            }
          }
          break;
      }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const AggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, AggregateValue> ht_{};
  /** The aggregate expressions that we have */
  const std::vector<AbstractExpressionRef> &agg_exprs_;
  /** The types of aggregations that we have */
  const std::vector<AggregationType> &agg_types_;
};

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX)
 * over the tuples produced by a child executor.
 */
class AggregationExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new AggregationExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the aggregation
   * @param[out] rid The next tuple RID produced by the aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the aggregation */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** Do not use or remove this function, otherwise you will get zero points. */
  auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : plan_->GetGroupBys()) {
      keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> vals;
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    return {vals};
  }

 private:
  /** The aggregation plan node */
  const AggregationPlanNode *plan_;

  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::unique_ptr<SimpleAggregationHashTable> ht_;
  std::unique_ptr<SimpleAggregationHashTable::Iterator> iter_;
  bool is_inert_{false};
  /** Simple aggregation hash table */
  // TODO(Student): Uncomment SimpleAggregationHashTable aht_;

  /** Simple aggregation hash table iterator */
  // TODO(Student): Uncomment SimpleAggregationHashTable::Iterator aht_iterator_;
};
}  // namespace bustub
