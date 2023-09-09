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
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_exec_(std::move(left_child)), right_exec_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  Tuple tuple = Tuple();
  RID rid = RID();
  left_exec_->Init();
  right_exec_->Init();
  // Construct the oringinal hash table with the right key.
  while (right_exec_->Next(&tuple, &rid)) {
    HashJoinKey join_key = GetRightJoinKey(tuple, right_exec_->GetOutputSchema());
    ht_[join_key].val_.push_back(tuple);
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  HashJoinKey left_key;
  while (true) {
    if (index_ < size_) {
      std::vector<Value> vals;
      for (uint32_t col_idx = 0; col_idx < left_exec_->GetOutputSchema().GetColumnCount(); col_idx++) {
        vals.push_back(left_tuple_.GetValue(&left_exec_->GetOutputSchema(), col_idx));
      }
      left_key = GetLeftJoinKey(left_tuple_, left_exec_->GetOutputSchema());
      HashJoinValue right_val = ht_[left_key];
      for (uint32_t col_idx = 0; col_idx < right_exec_->GetOutputSchema().GetColumnCount(); col_idx++) {
        vals.push_back(right_val.val_[index_].GetValue(&right_exec_->GetOutputSchema(), col_idx));
      }
      *tuple = Tuple(vals, &plan_->OutputSchema());
      index_++;
      return true;
    }
    if (!left_exec_->Next(tuple, rid)) {
      return false;
    }
    left_tuple_ = *tuple;
    left_key = GetLeftJoinKey(left_tuple_, left_exec_->GetOutputSchema());
    if (ht_.find(left_key) != ht_.end()) {
      size_ = ht_[left_key].val_.size();
      index_ = 0;
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> vals;
      for (uint32_t col_idx = 0; col_idx < left_exec_->GetOutputSchema().GetColumnCount(); col_idx++) {
        vals.push_back(left_tuple_.GetValue(&left_exec_->GetOutputSchema(), col_idx));
      }
      for (const auto &coloumn : right_exec_->GetOutputSchema().GetColumns()) {
        Value null_val = ValueFactory::GetNullValueByType(coloumn.GetType());
        vals.push_back(null_val);
      }
      *tuple = Tuple(vals, &plan_->OutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
