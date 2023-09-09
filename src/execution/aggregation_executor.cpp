//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      ht_(std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      iter_(ht_->Begin()) {}

void AggregationExecutor::Init() {
  ht_->Clear();
  child_executor_->Init();
  auto *tuple = new Tuple();
  auto *rid = new RID();
  while (child_executor_->Next(tuple, rid)) {
    AggregateKey agg_key = MakeAggregateKey(tuple);
    AggregateValue agg_val = MakeAggregateValue(tuple);
    ht_->InsertCombine(agg_key, agg_val);
  }
  iter_ = ht_->Begin();
  delete tuple;
  delete rid;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (count_ == 0 && iter_ == ht_->End()) {
    // No group by, no output
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }
    *tuple = Tuple(ht_->GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema());
    count_++;
    return true;
  }
  if (iter_ != ht_->End()) {
    std::vector<Value> vals;
    for (const auto &key : iter_.Key().group_bys_) {
      vals.push_back(key);
    }
    for (const auto &val : iter_.Val().aggregates_) {
      vals.push_back(val);
    }
    *tuple = Tuple(vals, &plan_->OutputSchema());
    count_++;
    ++iter_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
