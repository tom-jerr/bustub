//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include <cstddef>
#include <functional>
#include <iostream>
#include <optional>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/table/tuple.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::FlushBufferToRun(std::vector<SortEntry> &buffer, BufferPoolManager *bpm,
                                                    size_t max_tuples_per_page, size_t tuple_length,
                                                    std::vector<page_id_t> &pages) {
  std::vector<Tuple> tuples;
  tuples.reserve(buffer.size());
  for (const auto &entry : buffer) {
    tuples.emplace_back(entry.second);
  }
  // std::vector<page_id_t> pages;
  // size_t tuple_length = plan_->OutputSchema().GetInlinedStorageSize();
  // size_t max_tuples_per_page = (BUSTUB_PAGE_SIZE - SORT_PAGE_HEADER_SIZE) / tuple_length;

  for (size_t i = 0; i < tuples.size(); i += max_tuples_per_page) {
    auto page_id = bpm->NewPage();
    auto new_page = bpm->CheckedWritePage(page_id);
    auto *sort_page = new_page.value().AsMut<SortPage>();
    sort_page->Init(max_tuples_per_page, tuple_length);
    sort_page->WriteTuples({tuples.begin() + i, tuples.begin() + std::min(i + max_tuples_per_page, tuples.size())});
    pages.emplace_back(new_page->GetPageId());
  }
  buffer.clear();
}

template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  if (is_inited_) {
    BUSTUB_ASSERT(!runs_.empty(), "MergeSortRun should not be empty");
    current_iterator_ = runs_[0].Begin();
    end_iterator_ = runs_[0].End();
    return;
  }
  child_executor_->Init();
  // 1. 生成初始的mergesortrun
  auto *bpm = exec_ctx_->GetBufferPoolManager();
  size_t tuple_length = GetOutputSchema().GetInlinedStorageSize();
  size_t max_tuples_per_page = (BUSTUB_PAGE_SIZE - 2 * SORT_PAGE_HEADER_SIZE) / tuple_length;
  std::vector<SortEntry> buffer;
  buffer.reserve(max_tuples_per_page * tuple_length);

  std::vector<page_id_t> initial_pages;
  Tuple child_tuple{};
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto sort_key = GenerateSortKey(child_tuple, plan_->GetOrderBy(), GetOutputSchema());
    buffer.emplace_back(SortEntry{sort_key, child_tuple});
    if (buffer.size() == max_tuples_per_page) {
      std::sort(buffer.begin(), buffer.end(), cmp_);
      FlushBufferToRun(buffer, bpm, max_tuples_per_page, tuple_length, initial_pages);
    }
  }
  // 未满一页也需要flush
  if (!buffer.empty()) {
    std::sort(buffer.begin(), buffer.end(), cmp_);
    FlushBufferToRun(buffer, bpm, max_tuples_per_page, tuple_length, initial_pages);
  }
  // 初始时有page num个mergerun
  for (const auto &pid : initial_pages) {
    runs_.emplace_back(MergeSortRun({pid}, bpm));
  }
  // 2. merge all runs
  while (runs_.size() > 1) {
    std::vector<MergeSortRun> new_runs;
    for (size_t i = 0; i < runs_.size(); i += K) {
      std::vector<MergeSortRun> runs_to_merge(runs_.begin() + i, runs_.begin() + std::min(i + K, runs_.size()));
      auto merge_run = MergeKRuns(runs_to_merge, bpm, max_tuples_per_page, tuple_length);
      new_runs.emplace_back(merge_run);
    }
    // delete all old runs
    for (const auto &run : runs_) {
      for (const auto &pid : run.GetPages()) {
        bpm->DeletePage(pid);
      }
    }
    runs_ = std::move(new_runs);
  }

  // 3. read the last run
  if (!runs_.empty()) {
    current_iterator_ = runs_[0].Begin();
    end_iterator_ = runs_[0].End();
    is_inited_ = true;
  }
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeKRuns(std::vector<MergeSortRun> &runs, BufferPoolManager *bpm,
                                              size_t max_tuples_per_page, size_t tuple_length) -> MergeSortRun {
  // we need 小顶堆
  auto heap_cmp = [&](const SortEntry &a, const SortEntry &b) {
    auto order_bys = plan_->GetOrderBy();
    int index = 0;
    for (auto &order_by : order_bys) {
      auto &[type, expr] = order_by;
      if (type == OrderByType::DESC) {
        if (a.first[index].CompareLessThan(b.first[index]) == CmpBool::CmpTrue) {
          return true;
        }
        if (b.first[index].CompareLessThan(a.first[index]) == CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (a.first[index].CompareLessThan(b.first[index]) == CmpBool::CmpTrue) {
          return false;
        }
        if (b.first[index].CompareLessThan(a.first[index]) == CmpBool::CmpTrue) {
          return true;
        }
      }
      ++index;
    }
    return false;
  };
  std::priority_queue<SortEntry, std::vector<SortEntry>, decltype(heap_cmp)> heap(heap_cmp);
  auto cmp = [this](const SortKey &a, const SortKey &b) { return cmp_(SortEntry{a, {}}, SortEntry{b, {}}); };
  std::map<SortKey, std::pair<MergeSortRun::Iterator, MergeSortRun::Iterator>, decltype(cmp)> map_key_iters(cmp);

  // std::vector<MergeSortRun> new_runs;
  std::vector<SortEntry> buffer;
  std::vector<page_id_t> merge_pages;
  // 1. 读取K个runs的第一个tuple
  for (size_t i = 0; i < K && i < runs.size(); i++) {
    auto iter = runs[i].Begin();
    auto sort_key = GenerateSortKey(*iter, plan_->GetOrderBy(), GetOutputSchema());
    SortEntry entry{sort_key, *iter};
    heap.push(entry);
    map_key_iters.insert({sort_key, {runs[i].Begin(), runs[i].End()}});
  }
  // 2. 逐个读取
  while (!heap.empty()) {
    auto top = heap.top();
    heap.pop();

    auto [sort_key, _] = top;

    buffer.emplace_back(top);

    if (buffer.size() == max_tuples_per_page) {
      FlushBufferToRun(buffer, bpm, max_tuples_per_page, tuple_length, merge_pages);
    }
    BUSTUB_ASSERT(map_key_iters.find(sort_key) != map_key_iters.end(), "map_key_iters should contain sort_key");
    auto [iter, end_iter] = map_key_iters[sort_key];
    // advance iterator

    ++iter;
    if (iter != end_iter) {
      sort_key = GenerateSortKey(*iter, plan_->GetOrderBy(), GetOutputSchema());
      heap.emplace(sort_key, *iter);
      map_key_iters[sort_key] = {iter, end_iter};
    }
  }

  if (!buffer.empty()) {
    FlushBufferToRun(buffer, bpm, max_tuples_per_page, tuple_length, merge_pages);
  }
  return {merge_pages, bpm};
}
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(Tuple *tuple, RID *rid) -> bool {
  if (current_iterator_ != end_iterator_) {
    *tuple = *current_iterator_;
    *rid = {1, 0};
    ++current_iterator_;
    return true;
  }
  return false;
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
