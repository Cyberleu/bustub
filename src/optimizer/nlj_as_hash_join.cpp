#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/** A recursive function constructing both sides of hash join*/
auto TransformHelper(const AbstractExpressionRef &exp_ref, std::vector<AbstractExpressionRef> &left_key_expression,
                     std::vector<AbstractExpressionRef> &right_key_expressions) -> bool {
  if (exp_ref->GetChildren().size() < 2) {
    return false;
  }
  if (exp_ref->GetChildAt(0)->GetReturnType() != TypeId::BOOLEAN) {
    const auto &cmp_exp = dynamic_cast<const ComparisonExpression &>(*exp_ref);
    if (cmp_exp.comp_type_ != ComparisonType::Equal) {
      return false;
    }
    const auto &exp1 = dynamic_cast<const ColumnValueExpression &>(*exp_ref->GetChildAt(0));
    const auto &exp2 = dynamic_cast<const ColumnValueExpression &>(*exp_ref->GetChildAt(1));
    if (exp1.GetTupleIdx() == 0) {
      left_key_expression.push_back(std::make_shared<ColumnValueExpression>(exp1));
      right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(exp2));
    } else {
      left_key_expression.push_back(std::make_shared<ColumnValueExpression>(exp2));
      right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(exp1));
    }
    return true;
  }
  bool left = TransformHelper(exp_ref->GetChildAt(0), left_key_expression, right_key_expressions);
  bool right = TransformHelper(exp_ref->GetChildAt(1), left_key_expression, right_key_expressions);
  return left && right;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child_ref : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child_ref));
  }
  auto optimize_plan = plan->CloneWithChildren(std::move(children));
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimize_plan);
    std::vector<AbstractExpressionRef> left_key_expression;
    std::vector<AbstractExpressionRef> right_key_expressions;
    std::cout << nlj_plan.Predicate()->GetChildren().size() << '\n';
    if (TransformHelper(nlj_plan.Predicate(), left_key_expression, right_key_expressions)) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), left_key_expression, right_key_expressions,
                                                nlj_plan.GetJoinType());
    }
  }
  return plan;
}

}  // namespace bustub
