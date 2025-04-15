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

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/** HashJoinKeyrepresents a key in an join operation */
struct HashJoinKey {
  std::vector<Value> hash_keys_;
  /**
   * Compares two hash joi keys for equality
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join key have equivalent values
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    // 比较两个对象的hash_keys_成员中的每个Value对象是否相等
    for (uint32_t i = 0; i < other.hash_keys_.size(); ++i) {
      if (hash_keys_[i].CompareEquals(other.hash_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {
/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.hash_keys_) {
      if (!key.IsNull()) {
        // 对每一个非空的value对象，计算出它的哈希值
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
namespace bustub {
/**
 * A simplified hash table that has all the necessary functionality for join.
 */
class SimpleHashJoinHashTable {
 public:
  SimpleHashJoinHashTable() = default;
  /** 插入join key和tuple构建hash表 */
  void Insert(const HashJoinKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      std::vector<Tuple> tuple_vector;
      tuple_vector.push_back(tuple);
      ht_.insert({join_key, tuple_vector});
    } else {
      ht_.at(join_key).push_back(tuple);
    }
  }
  /** 获取该join key对应的tuple */
  auto GetValue(const HashJoinKey &join_key) -> std::vector<Tuple> * {
    if (ht_.find(join_key) == ht_.end()) {
      return nullptr;
    }
    return &(ht_.find(join_key)->second);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<HashJoinKey, std::vector<Tuple>>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const HashJoinKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const std::vector<Tuple> & { return iter_->second; }

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
    std::unordered_map<HashJoinKey, std::vector<Tuple>>::const_iterator iter_;
  };
  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
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
  auto GetLeftJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, left_child_executor_->GetOutputSchema()));
    }
    return {values};
  }

  auto GetRightJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, right_child_executor_->GetOutputSchema()));
    }
    return {values};
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_executor_;
  std::unique_ptr<AbstractExecutor> right_child_executor_;
  std::unique_ptr<SimpleHashJoinHashTable> jht_;
  std::list<Tuple> results_;
};

}  // namespace bustub
