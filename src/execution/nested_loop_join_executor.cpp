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
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid = RID();
  left_executor_->Next(&left_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (no_next_) {
    return false;
  }
  Tuple right_tuple = Tuple();
  while (true) {
    while (right_executor_->Next(&right_tuple, rid)) {
      std::vector<Value> vals;
      Value val = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                   right_executor_->GetOutputSchema());
      auto joinable = val.GetAs<bool>();
      if (joinable) {
        std::cout << "i'm here1" << '\n';
        no_right_join_ = false;
        for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
          vals.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col_idx));
        }
        for (uint32_t col_idx = 0; col_idx < right_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
          vals.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), col_idx));
          std::cout << vals.back().GetAs<int>() << '\n';
        }
        *tuple = Tuple(vals, &plan_->OutputSchema());
        return true;
      }
    }
    RID rid = RID();
    if (plan_->GetJoinType() == JoinType::LEFT && no_right_join_) {
      std::cout << "i'm here2" << '\n';
      std::vector<Value> no_right_join_tuple;
      for (uint32_t col_idx = 0; col_idx < left_executor_->GetOutputSchema().GetColumnCount(); col_idx++) {
        no_right_join_tuple.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), col_idx));
      }
      for (const auto &coloumn : right_executor_->GetOutputSchema().GetColumns()) {
        Value null_val = ValueFactory::GetNullValueByType(coloumn.GetType());
        no_right_join_tuple.push_back(null_val);
      }
      *tuple = Tuple(no_right_join_tuple, &plan_->OutputSchema());
      std::cout << "i'm here4" << '\n';
      if (!left_executor_->Next(&left_tuple_, &rid)) {
        no_next_ = true;
      }
      right_executor_->Init();
      return true;
    }
    no_right_join_ = true;
    if (!left_executor_->Next(&left_tuple_, &rid)) {
      return false;
    }
    right_executor_->Init();
  }
  return false;
}
}  // namespace bustub
