#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  std::cout << "optimize topn!" << '\n';
  for (const auto &child_ref : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child_ref));
  }
  auto optimize_plan = plan->CloneWithChildren(std::move(children));
  if (plan->GetChildren().empty()) {
    return optimize_plan;
  }
  if (optimize_plan->GetType() == PlanType::Limit && optimize_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
    const auto &sort_plan_node = dynamic_cast<const SortPlanNode &>(*optimize_plan->GetChildAt(0));
    const auto &limit_plan_node = dynamic_cast<const LimitPlanNode &>(*optimize_plan);
    return std::make_shared<TopNPlanNode>(optimize_plan->output_schema_, sort_plan_node.GetChildAt(0),
                                          sort_plan_node.GetOrderBy(), limit_plan_node.limit_);
  }
  return optimize_plan;
}

}  // namespace bustub
