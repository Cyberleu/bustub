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
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  /** Multiple attributes of the join key. */
  std::vector<Value> join_key_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < join_key_.size(); i++) {
      if (join_key_[i].CompareEquals(other.join_key_[i]) == CmpBool::CmpFalse) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  /** Tuples with the same join key */
  std::vector<Tuple> val_;
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &agg_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : agg_key.join_key_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {
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

  auto GetLeftJoinKey(const Tuple &tuple, const Schema &schema) const -> HashJoinKey {
    HashJoinKey join_key;
    for (const auto &exp : plan_->LeftJoinKeyExpressions()) {
      join_key.join_key_.push_back(exp->Evaluate(&tuple, schema));
    }
    return join_key;
  }

  auto GetRightJoinKey(const Tuple &tuple, const Schema &schema) const -> HashJoinKey {
    HashJoinKey join_key;
    for (const auto &exp : plan_->RightJoinKeyExpressions()) {
      join_key.join_key_.push_back(exp->Evaluate(&tuple, schema));
    }
    return join_key;
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_exec_;
  std::unique_ptr<AbstractExecutor> right_exec_;
  std::unordered_map<HashJoinKey, HashJoinValue> ht_{};
  /** The index of the pos hash value */
  std::unordered_map<HashJoinKey, int> count_{};
  int size_{-1};
  int index_{0};
  Tuple left_tuple_;
};

}  // namespace bustub
