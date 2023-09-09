#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  std::cout << "sort init!" << '\n';
  child_executor_->Init();
  tuples_.clear();
  index_ = 0;
  Tuple tuple = Tuple();
  RID rid = RID();
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &t1, const Tuple &t2) {
    for (const auto &order_by : plan_->GetOrderBy()) {
      Value v1 = order_by.second->Evaluate(&t1, plan_->OutputSchema());
      Value v2 = order_by.second->Evaluate(&t2, plan_->OutputSchema());
      if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
        continue;
      }
      if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
        return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
      }
      return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
    }
    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[index_];
  index_++;
  return true;
}

}  // namespace bustub
