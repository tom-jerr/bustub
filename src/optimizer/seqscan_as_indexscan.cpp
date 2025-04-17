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
  bool is_logic_finished{false};
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    const auto table_info = catalog_.GetTable(seq_scan_plan.table_oid_);
    const auto index_infos = catalog_.GetTableIndexes(table_info->name_);
    auto comp_expr = std::dynamic_pointer_cast<ComparisonExpression>(seq_scan_plan.filter_predicate_);
    auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(seq_scan_plan.filter_predicate_);

    if (comp_expr != nullptr || logic_expr != nullptr) {
      uint32_t col_idx{UINT32_MAX};
      // case1 : where v1 = 1 and where 1 = v1;
      if (comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
        auto col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(comp_expr->GetChildAt(0));
        auto col_expr_rev = std::dynamic_pointer_cast<ColumnValueExpression>(comp_expr->GetChildAt(1));
        auto val_expr = std::dynamic_pointer_cast<ConstantValueExpression>(comp_expr->GetChildAt(1));
        auto val_expr_rev = std::dynamic_pointer_cast<ConstantValueExpression>(comp_expr->GetChildAt(0));
        if (col_expr != nullptr && val_expr != nullptr) {
          col_idx = col_expr->GetColIdx();
          pred_keys.emplace_back(val_expr);
        } else if (col_expr_rev != nullptr && val_expr_rev != nullptr) {
          col_idx = col_expr_rev->GetColIdx();
          pred_keys.emplace_back(val_expr_rev);
        }
      } else {
        // case 2: where v1 = 1 or v1 = 2 or ...;
        auto new_logic_expr = std::dynamic_pointer_cast<LogicExpression>(seq_scan_plan.filter_predicate_);
        // 循环处理logic expression
        while (new_logic_expr != nullptr && new_logic_expr->logic_type_ == LogicType::Or && !is_logic_finished) {
          // 先处理右边的 v1 = x
          auto right_comp_expr = std::dynamic_pointer_cast<ComparisonExpression>(new_logic_expr->GetChildAt(1));
          if (right_comp_expr != nullptr && right_comp_expr->comp_type_ == ComparisonType::Equal) {
            // comp_expr_0: v1 = 3; comp_expr_1: v1 = 4 or v1 = 5
            auto col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(right_comp_expr->GetChildAt(0));
            auto col_expr_rev = std::dynamic_pointer_cast<ColumnValueExpression>(right_comp_expr->GetChildAt(1));
            auto val_expr = std::dynamic_pointer_cast<ConstantValueExpression>(right_comp_expr->GetChildAt(1));
            auto val_expr_rev = std::dynamic_pointer_cast<ConstantValueExpression>(right_comp_expr->GetChildAt(0));
            size_t now_col_idx{UINT32_MAX};
            if (col_expr != nullptr && val_expr != nullptr) {
              col_idx = col_idx == UINT32_MAX ? col_expr->GetColIdx() : col_idx;
              now_col_idx = col_expr->GetColIdx();
              pred_keys.emplace_back(val_expr);
            } else if (col_expr_rev != nullptr && val_expr_rev != nullptr) {
              col_idx = col_idx == UINT32_MAX ? col_expr_rev->GetColIdx() : col_idx;
              now_col_idx = col_expr_rev->GetColIdx();
              pred_keys.emplace_back(val_expr_rev);
            }
            // 如果此时 column 不同直接放弃优化
            if (col_idx != now_col_idx) {
              return optimized_plan;
            }

            // 继续处理左边的表达式，可能是 or v1 = x ... 或者只是单独的 v1 = x
            auto last_comp_expr = std::dynamic_pointer_cast<ComparisonExpression>(new_logic_expr->GetChildAt(0));
            if (last_comp_expr != nullptr) {
              if (last_comp_expr->comp_type_ == ComparisonType::Equal) {
                auto col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(last_comp_expr->GetChildAt(0));
                auto col_expr_rev = std::dynamic_pointer_cast<ColumnValueExpression>(last_comp_expr->GetChildAt(1));
                auto val_expr = std::dynamic_pointer_cast<ConstantValueExpression>(last_comp_expr->GetChildAt(1));
                auto val_expr_rev = std::dynamic_pointer_cast<ConstantValueExpression>(last_comp_expr->GetChildAt(0));
                uint32_t now_col_idx{UINT32_MAX};
                if (col_expr != nullptr && val_expr != nullptr) {
                  now_col_idx = col_expr->GetColIdx();
                  pred_keys.emplace_back(val_expr);
                } else if (col_expr_rev != nullptr && val_expr_rev != nullptr) {
                  now_col_idx = col_expr_rev->GetColIdx();
                  pred_keys.emplace_back(val_expr_rev);
                }
                // 如果此时 column 不同直接放弃优化
                if (col_idx != now_col_idx) {
                  return optimized_plan;
                }
                is_logic_finished = true;
              } else {
                return optimized_plan;
              }
            }
            if (is_logic_finished) {
              break;
            }
            new_logic_expr = std::dynamic_pointer_cast<LogicExpression>(new_logic_expr->GetChildAt(0));

          } else {
            return optimized_plan;
          }
        }
      }

      if (col_idx == UINT32_MAX) {
        return optimized_plan;
      }

      // 如果过滤列与索引列是同一列，则可以使用索引扫描
      for (const auto &index_info : index_infos) {
        const auto &columns = index_info->index_->GetKeyAttrs();
        std::vector<uint32_t> filter_column{col_idx};
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
