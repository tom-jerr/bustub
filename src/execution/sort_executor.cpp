#include "execution/executors/sort_executor.h"
#include "common/rid.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    // auto tuple_rid = TupleRID{tuple, rid};
    sorted_tuples_.emplace_back(std::make_pair(tuple, rid));
  }
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), CompareTuple(&plan_->GetOrderBy(), &GetOutputSchema()));
  sorted_iter_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (sorted_iter_ != sorted_tuples_.end()) {
    *tuple = sorted_iter_->first;
    *rid = sorted_iter_->second;
    ++sorted_iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
