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
#include <memory>
#include <optional>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/table/tuple.h"

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      cmp_(plan->GetOrderBy()),
      child_executor_(std::move(child_executor)),
      thread_pool_(std::make_unique<ThreadPool>(16)) {}

template <size_t K>
void ExternalMergeSortExecutor<K>::FlushBufferToRun(std::vector<SortEntry> buffer, BufferPoolManager *bpm,
                                                    size_t max_tuples_per_page, size_t tuple_length,
                                                    std::vector<MergeSortRun> &runs) {
  std::sort(buffer.begin(), buffer.end(), cmp_);
  // for (size_t i = 0; i < buffer.size(); i += max_tuples_per_page) {
  auto page_id = bpm->NewPage();
  auto new_page = bpm->CheckedWritePage(page_id);
  auto *sort_page = new_page.value().AsMut<SortPage>();
  sort_page->Init(max_tuples_per_page, tuple_length);
  sort_page->WriteTuples({buffer.begin(), buffer.begin() + std::min(max_tuples_per_page, buffer.size())});
  {
    std::scoped_lock lock(thread_mutex_);
    runs.emplace_back(MergeSortRun({new_page->GetPageId()}, bpm));
  }
  // task_nums_.fetch_add(1);
  // }
  buffer.clear();
}

template <size_t K>
auto ExternalMergeSortExecutor<K>::FlushBufferToRun(std::vector<SortEntry> buffer, BufferPoolManager *bpm,
                                                    size_t max_tuples_per_page, size_t tuple_length,
                                                    std::vector<page_id_t> &pages) -> MergeSortRun {
  std::sort(buffer.begin(), buffer.end(), cmp_);
  // for (size_t i = 0; i < buffer.size(); i += max_tuples_per_page) {
  auto page_id = bpm->NewPage();
  auto new_page = bpm->CheckedWritePage(page_id);
  auto *sort_page = new_page.value().AsMut<SortPage>();
  sort_page->Init(max_tuples_per_page, tuple_length);
  sort_page->WriteTuples({buffer.begin(), buffer.begin() + std::min(max_tuples_per_page, buffer.size())});

  pages.emplace_back(new_page->GetPageId());

  // task_nums_.fetch_add(1);
  // }
  buffer.clear();
  return MergeSortRun({pages, bpm});
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
  // auto bpm_pool_size = bpm->Size();
  size_t tuple_length = GetOutputSchema().GetInlinedStorageSize();
  size_t max_tuples_per_page = (BUSTUB_PAGE_SIZE - 2 * SORT_PAGE_HEADER_SIZE) / tuple_length;
  std::vector<SortEntry> buffer;
  buffer.reserve(max_tuples_per_page * tuple_length);

  std::vector<page_id_t> initial_pages;
  Tuple child_tuple{};
  RID child_rid;
  int task_nums = 0;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    auto sort_key = GenerateSortKey(child_tuple, plan_->GetOrderBy(), GetOutputSchema());
    buffer.emplace_back(SortEntry{sort_key, child_tuple});
    if (buffer.size() == max_tuples_per_page) {
      task_nums++;
      thread_pool_->Enqueue([=]() {
        FlushBufferToRun(buffer, bpm, max_tuples_per_page, tuple_length, runs_);
        task_nums_.fetch_add(1);
      });
      buffer.clear();
      // FlushBufferToRun(buffer, bpm, max_tuples_per_page, tuple_length, initial_pages);
    }
  }
  // 未满一页也需要flush
  if (!buffer.empty()) {
    task_nums++;
    thread_pool_->Enqueue([=]() {
      FlushBufferToRun(buffer, bpm, max_tuples_per_page, tuple_length, runs_);
      task_nums_.fetch_add(1);
    });
    buffer.clear();
  }
  // LOG_DEBUG("Initial sort task nums: %d", task_nums);
  // 同步IO线程，保证所有的page都是排好序的
  while (task_nums != task_nums_.load()) {
    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  task_nums_.store(0);
  // LOG_DEBUG("Finish initial sort task nums: %d", task_nums);
  // 2. merge all runs
  // int merge_task_nums = 0;
  while (runs_.size() > 1) {
    std::vector<MergeSortRun> new_runs;
    // 2-way external merge sort
    for (size_t i = 0; i < runs_.size(); i += 2) {
      if (i + 1 < runs_.size()) {
        // auto run1 = runs_[i];
        // auto run2 = runs_[i + 1];
        Merge2Runs(runs_[i], runs_[i + 1], bpm, max_tuples_per_page, tuple_length, new_runs);
      } else {
        new_runs.emplace_back(runs_[i]);
      }
    }

    runs_ = std::move(new_runs);
  }

  // 3. read the last run
  if (!runs_.empty()) {
    current_iterator_ = runs_[0].Begin();
    end_iterator_ = runs_[0].End();
    is_inited_ = true;
  } else {
    // empty table
    current_iterator_ = {};
    end_iterator_ = {};
  }
}

template <size_t K>
void ExternalMergeSortExecutor<K>::Merge2Runs(MergeSortRun run1, MergeSortRun run2, BufferPoolManager *bpm,
                                              size_t max_tuples_per_page, size_t tuple_length,
                                              std::vector<MergeSortRun> &new_runs) {
  auto iter1 = run1.Begin();
  auto iter2 = run2.Begin();
  auto end1 = run1.End();
  auto end2 = run2.End();
  auto tuple_cmp = [this](const Tuple &a, const Tuple &b) {
    return cmp_({GenerateSortKey(a, plan_->GetOrderBy(), GetOutputSchema()), a},
                {GenerateSortKey(b, plan_->GetOrderBy(), GetOutputSchema()), b});
  };

  // std::vector<Tuple> buffer;
  std::vector<page_id_t> new_pages;
  auto new_page_id = bpm->NewPage();
  auto new_page = bpm->CheckedWritePage(new_page_id)->AsMut<SortPage>();
  new_page->Init(max_tuples_per_page, tuple_length);
  while (iter1 != end1 && iter2 != end2) {
    if (tuple_cmp(*iter1, *iter2) > 0) {
      new_page->WriteTuple(*iter1);
      ++iter1;
    } else {
      new_page->WriteTuple(*iter2);
      ++iter2;
    }
    if (new_page->GetSize() == max_tuples_per_page) {
      new_pages.emplace_back(new_page_id);
      new_page_id = bpm->NewPage();
      new_page = bpm->CheckedWritePage(new_page_id)->AsMut<SortPage>();
      new_page->Init(max_tuples_per_page, tuple_length);
    }
  }
  while (iter1 != end1) {
    new_page->WriteTuple(*iter1);
    ++iter1;
    if (new_page->GetSize() == max_tuples_per_page) {
      new_pages.emplace_back(new_page_id);
      new_page_id = bpm->NewPage();
      new_page = bpm->CheckedWritePage(new_page_id)->AsMut<SortPage>();
      new_page->Init(max_tuples_per_page, tuple_length);
    }
  }

  while (iter2 != end2) {
    new_page->WriteTuple(*iter2);
    ++iter2;
    if (new_page->GetSize() == max_tuples_per_page) {
      new_pages.emplace_back(new_page_id);
      new_page_id = bpm->NewPage();
      new_page = bpm->CheckedWritePage(new_page_id)->AsMut<SortPage>();
      new_page->Init(max_tuples_per_page, tuple_length);
    }
  }
  // delete old run pages
  thread_pool_->Enqueue([=]() {
    for (auto page_id : run1.GetPages()) {
      bpm->DeletePage(page_id);
    }
    for (auto page_id : run2.GetPages()) {
      bpm->DeletePage(page_id);
    }
  });
  // for (auto page_id : run1.GetPages()) {
  //   bpm->DeletePage(page_id);
  // }
  // for (auto page_id : run2.GetPages()) {
  //   bpm->DeletePage(page_id);
  // }
  if (new_page->GetSize() > 0) {
    new_pages.emplace_back(new_page_id);
  }
  new_runs.emplace_back(MergeSortRun(new_pages, bpm));

  // task_nums_.fetch_add(1);
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
