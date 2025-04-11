#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
// recursive parse and expression
void ParseAndExpression(const AbstractExpressionRef &expr, std::vector<AbstractExpressionRef> &left_exprs,
                        std::vector<AbstractExpressionRef> &right_exprs) {
  if (expr->GetType() == ExecExpressionType::Logic) {
    auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get());
    if (logic_expr != nullptr) {
      ParseAndExpression(logic_expr->GetChildAt(0), left_exprs, right_exprs);
      ParseAndExpression(expr->GetChildAt(1), left_exprs, right_exprs);
    }
  }
  if (expr->GetType() == ExecExpressionType::Comparison) {
    auto *comp_expr = dynamic_cast<ComparisonExpression *>(expr.get());
    // 区分每个数据元素是从左侧表还是右侧表提取的，例如 A.id = B.id时，系统需要知道 A.id 和 B.id 分别属于哪个数据源
    if (comp_expr != nullptr) {
      auto column_value_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
      if (column_value_expr->GetTupleIdx() == 0) {
        left_exprs.emplace_back(comp_expr->GetChildAt(0));
        right_exprs.emplace_back(comp_expr->GetChildAt(1));
      } else {
        left_exprs.emplace_back(comp_expr->GetChildAt(1));
        right_exprs.emplace_back(comp_expr->GetChildAt(0));
      }
    }
  }
}
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> optimized_children;
  for (const auto &child : plan->GetChildren()) {
    optimized_children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(optimized_children));
  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }
  const auto &join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

  auto predicate = join_plan.Predicate();
  std::vector<AbstractExpressionRef> left_predicate_exprs;
  std::vector<AbstractExpressionRef> right_predicate_exprs;
  ParseAndExpression(predicate, left_predicate_exprs, right_predicate_exprs);
  return std::make_shared<HashJoinPlanNode>(join_plan.OutputSchema(), join_plan.GetLeftPlan(), join_plan.GetRightPlan(),
                                            left_predicate_exprs, right_predicate_exprs, join_plan.GetJoinType());
}

}  // namespace bustub
