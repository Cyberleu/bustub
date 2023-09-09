#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  auto cmp = [&](const Tuple &t1, const Tuple &t2) -> bool {
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
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);
  child_executor_->Init();
  Tuple tuple = Tuple();
  RID rid = RID();
  while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    if (heap.size() > plan_->n_) {
      heap.pop();
    }
  }
  while (!heap.empty()) {
    tuples_.push(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  *tuple = tuples_.top();
  tuples_.pop();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_.size(); };

}  // namespace bustub
