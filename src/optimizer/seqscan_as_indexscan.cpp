#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  std::vector<AbstractExpressionRef> pred_keys;
  bool is_logic_finished;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      const ColumnValueExpression *col_expr{nullptr};

      // const ConstantValueExpression *const_expr{nullptr};
      // case1 : where v1 = 1 and where 1 = v1;
      if (seq_scan_plan.filter_predicate_->GetType() == ExecExpressionType::Comparison) {
        const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
        if (comp_expr->comp_type_ == ComparisonType::Equal) {
          if (comp_expr->GetChildAt(0)->GetType() == ExecExpressionType::ColumnValue &&
              comp_expr->GetChildAt(1)->GetType() == ExecExpressionType::Value) {
            col_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
            // const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->GetChildAt(1).get());
            pred_keys.emplace_back(comp_expr->GetChildAt(1));
          } else if (comp_expr->GetChildAt(0)->GetType() == ExecExpressionType::Value &&
                     comp_expr->GetChildAt(1)->GetType() == ExecExpressionType::ColumnValue) {
            col_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr->GetChildAt(1).get());
            // const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr->GetChildAt(0).get());
            pred_keys.emplace_back(comp_expr->GetChildAt(0));
          }
        }
      } else if (seq_scan_plan.filter_predicate_->GetType() == ExecExpressionType::Logic) {
        // case 2: where v1 = 1 or v1 = 2 or ...;
        const ColumnValueExpression *logic_col_expr{nullptr};
        auto *new_logic_expr = dynamic_cast<LogicExpression *>(seq_scan_plan.filter_predicate_.get());

        // 循环处理logic expression
        while (new_logic_expr->GetType() == ExecExpressionType::Logic && !is_logic_finished) {
          if (dynamic_cast<LogicExpression *>(seq_scan_plan.filter_predicate_.get())->logic_type_ == LogicType::Or) {
            const auto *logic_expr = dynamic_cast<const LogicExpression *>(seq_scan_plan.filter_predicate_.get());
            // 先处理左边的 v1 = x
            if (logic_expr->GetChildAt(0)->GetType() == ExecExpressionType::Comparison) {
              // comp_expr_0: v1 = 3; comp_expr_1: v1 = 4 or v1 = 5
              const auto *comp_expr_0 = dynamic_cast<const ComparisonExpression *>(logic_expr->GetChildAt(0).get());
              // const auto *comp_expr_1 = dynamic_cast<const ComparisonExpression *>(logic_expr->GetChildAt(1).get());
              if (comp_expr_0->comp_type_ == ComparisonType::Equal) {
                if (comp_expr_0->GetChildAt(0)->GetType() == ExecExpressionType::ColumnValue &&
                    comp_expr_0->GetChildAt(1)->GetType() == ExecExpressionType::Value) {
                  col_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr_0->GetChildAt(0).get());
                  // const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr_0->GetChildAt(1).get());

                  pred_keys.emplace_back(comp_expr_0->GetChildAt(1));
                } else if (comp_expr_0->GetChildAt(0)->GetType() == ExecExpressionType::Value &&
                           comp_expr_0->GetChildAt(1)->GetType() == ExecExpressionType::ColumnValue) {
                  col_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr_0->GetChildAt(1).get());
                  // const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr_0->GetChildAt(0).get());
                  pred_keys.emplace_back(comp_expr_0->GetChildAt(0));
                }
              }
            }
            // 如果此时 column 不同直接放弃优化
            if (logic_col_expr != nullptr && logic_col_expr->GetColIdx() != col_expr->GetColIdx()) {
              return optimized_plan;
            }
            logic_col_expr = col_expr;

            // 继续处理右边的表达式，可能是 or v1 = x ... 或者只是单独的 v1 = x
            if (logic_expr->GetChildAt(1)->GetType() == ExecExpressionType::Logic) {
              new_logic_expr = dynamic_cast<LogicExpression *>(logic_expr->GetChildAt(1).get());

            } else if (logic_expr->GetChildAt(1)->GetType() == ExecExpressionType::Comparison) {
              const auto *comp_expr_1 = dynamic_cast<const ComparisonExpression *>(logic_expr->GetChildAt(1).get());

              if (comp_expr_1->comp_type_ == ComparisonType::Equal) {
                if (comp_expr_1->GetChildAt(0)->GetType() == ExecExpressionType::ColumnValue &&
                    comp_expr_1->GetChildAt(1)->GetType() == ExecExpressionType::Value) {
                  col_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr_1->GetChildAt(0).get());
                  // const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr_1->GetChildAt(1).get());
                  pred_keys.emplace_back(comp_expr_1->GetChildAt(1));
                } else if (comp_expr_1->GetChildAt(0)->GetType() == ExecExpressionType::Value &&
                           comp_expr_1->GetChildAt(1)->GetType() == ExecExpressionType::ColumnValue) {
                  col_expr = dynamic_cast<const ColumnValueExpression *>(comp_expr_1->GetChildAt(1).get());
                  // const_expr = dynamic_cast<const ConstantValueExpression *>(comp_expr_1->GetChildAt(0).get());
                  pred_keys.emplace_back(comp_expr_1->GetChildAt(0));
                }

                if (logic_col_expr != nullptr && logic_col_expr->GetColIdx() != col_expr->GetColIdx()) {
                  return optimized_plan;
                }
                is_logic_finished = true;

              } else {
                return optimized_plan;
              }

            } else {
              return optimized_plan;
            }

          } else {
            return optimized_plan;
          }
        }
      }

      const auto table_info = catalog_.GetTable(seq_scan_plan.table_oid_);
      const auto index_infos = catalog_.GetTableIndexes(table_info->name_);
      // 如果过滤列与索引列是同一列，则可以使用索引扫描
      for (const auto &index_info : index_infos) {
        const auto &columns = index_info->index_->GetKeyAttrs();
        std::vector<uint32_t> filter_column{col_expr->GetColIdx()};
        if (filter_column == columns) {
          // create index scan plan
          return std::make_shared<IndexScanPlanNode>(optimized_plan->output_schema_, table_info->oid_,
                                                     index_info->index_oid_, seq_scan_plan.filter_predicate_,
                                                     std::move(pred_keys));
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
