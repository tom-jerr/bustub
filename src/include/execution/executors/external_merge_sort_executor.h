//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification: src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

#define SORT_PAGE_HEADER_SIZE 12
// #define SORT_PAGE_SLOT_CNT ((BUSTUB_PAGE_SIZE - SORT_PAGE_HEADER_SIZE) / sizeof(Tuple))
namespace bustub {
class ThreadPool {
 public:
  explicit ThreadPool(size_t threadnum) : thread_num_(threadnum) {
    for (size_t i = 0; i < thread_num_; i++) {
      threads_.emplace_back([this]() {
        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(mutex_);
            cond_.wait(lock, [this]() { return !tasks_.empty() || stop_; });
            if (stop_ && tasks_.empty()) {
              return;
            }
            task = std::move(tasks_.front());
            tasks_.pop();
          }
          task();
        }
      });
    }
  }
  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      stop_ = true;
    }
    cond_.notify_all();
    for (std::thread &thread : threads_) {
      thread.join();
    }
  }
  template <class F>
  void Enqueue(F &&f) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      tasks_.emplace(std::forward<F>(f));
    }
    cond_.notify_one();
  }

 private:
  std::vector<std::thread> threads_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cond_;
  bool stop_{false};
  size_t thread_num_;
};
/**
 * Page to hold the intermediate data for external merge sort.
 *
 * Only fixed-length data will be supported in Fall 2024.
 */
class SortPage {
 public:
  /**
   * TODO: Define and implement the methods for reading data from and writing data to the sort
   * page. Feel free to add other helper methods.
   */
  SortPage() = default;
  void Init(uint32_t max_size, uint32_t tuple_length) {
    size_ = 0;
    max_size_ = max_size;
    tuple_length_ = tuple_length;
  }
  void WriteTuple(const Tuple &tuple) {
    size_ += 1;
    memcpy(page_start_ + SORT_PAGE_HEADER_SIZE + (size_ - 1) * tuple_length_, tuple.GetData(), tuple_length_);
  }
  void WriteTuples(const std::vector<SortEntry> &tuples) {
    size_ = tuples.size();
    // tuple_length_ = tuples[0].second.GetLength();
    // memcpy(page_start_ + SORT_PAGE_HEADER_SIZE, tuples[0].GetData(), tuple_length_);
    for (uint32_t i = 0; i < size_; i++) {
      memcpy(page_start_ + SORT_PAGE_HEADER_SIZE + i * tuple_length_, tuples[i].second.GetData(), tuple_length_);
    }
  }

  auto ReadTuples(std::vector<Tuple> &tuples) -> void {
    // std::vector<Tuple> tuples;
    tuples.clear();
    for (uint32_t i = 0; i < size_; i++) {
      tuples.emplace_back(Tuple{RID(), page_start_ + SORT_PAGE_HEADER_SIZE + i * tuple_length_, tuple_length_});
    }
    // return tuples;
  }
  auto GetSize() -> uint32_t { return size_; }
  auto GetMaxSize() -> uint32_t { return max_size_; }

 private:
  /**
   * TODO: Define the private members. You may want to have some necessary metadata for
   * the sort page before the start of the actual data.
   */
  uint32_t size_;
  uint32_t max_size_;
  uint32_t tuple_length_;
  char page_start_[0];
};

/**
 * A data structure that holds the sorted tuples as a run during external merge sort.
 * Tuples might be stored in multiple pages, and tuples are ordered both within one page
 * and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  auto GetPageCount() const -> size_t { return pages_.size(); }
  auto GetPages() const -> const std::vector<page_id_t> & { return pages_; }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;
    Iterator(const Iterator &other) {
      run_ = other.run_;
      bpm_ = other.bpm_;
      current_page_idx_ = other.current_page_idx_;
      current_tuple_idx_ = other.current_tuple_idx_;
      tuples_ = other.tuples_;
    }
    auto operator=(const Iterator &other) -> Iterator & = default;
    Iterator(const MergeSortRun *run, BufferPoolManager *bpm) : run_(run), bpm_(bpm) {
      if (bpm_ != nullptr && !run_->pages_.empty()) {
        auto page = bpm_->CheckedWritePage(run_->pages_[0]);
        BUSTUB_ASSERT(page.has_value(), "MergeSortRun: Failed to get page");
        auto sort_page = page.value().AsMut<SortPage>();
        sort_page->ReadTuples(tuples_);
        page->Drop();
      }
      if (bpm_ == nullptr) {
        tuples_ = {};
        current_tuple_idx_ = -1;
        current_page_idx_ = -1;
      }
    }

    /**
     * Advance the iterator to the next tuple. If the current sort page is exhausted, move to the
     * next sort page.
     *
     * TODO: Implement this method.
     */
    auto operator++() -> Iterator & {
      ++current_tuple_idx_;
      if (current_tuple_idx_ >= tuples_.size()) {
        // load next sortpage
        current_page_idx_++;
        if (current_page_idx_ < run_->GetPageCount()) {
          auto page = bpm_->CheckedWritePage(run_->pages_[current_page_idx_]);
          BUSTUB_ASSERT(page.has_value(), "MergeSortRun: Failed to get page");
          auto sort_page = page.value().AsMut<SortPage>();
          sort_page->ReadTuples(tuples_);
          current_tuple_idx_ = 0;
          page->Drop();

        } else {
          current_page_idx_ = -1;  // 已经遍历完所有页
          current_tuple_idx_ = -1;
          tuples_ = {};
          bpm_ = nullptr;
        }
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that the iterator is
     * pointing to.
     *
     * TODO: Implement this method.
     */
    auto operator*() -> Tuple { return tuples_[current_tuple_idx_]; }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same sorted run.
     *
     * TODO: Implement this method.
     */
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && current_page_idx_ == other.current_page_idx_ &&
             current_tuple_idx_ == other.current_tuple_idx_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted run or iterating
     * on different sorted runs.
     *
     * TODO: Implement this method.
     */
    auto operator!=(const Iterator &other) const -> bool {
      return run_ != other.run_ || current_page_idx_ != other.current_page_idx_ ||
             current_tuple_idx_ != other.current_tuple_idx_;
    }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {}

    /** The sorted run that the iterator is iterating on. */
    [[maybe_unused]] const MergeSortRun *run_;

    /**
     * TODO: Add your own private members here. You may want something to record your current
     * position in the sorted run. Also feel free to add additional constructors to initialize
     * your private members.
     */
    BufferPoolManager *bpm_;
    size_t current_page_idx_{0};
    size_t current_tuple_idx_{0};
    std::vector<Tuple> tuples_;
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first tuple.
   *
   * TODO: Implement this method.
   */
  auto Begin() -> Iterator { return {this, bpm_}; }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position after the last tuple.
   *
   * TODO: Implement this method.
   */
  auto End() -> Iterator { return {this, nullptr}; }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is responsible for
   * deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Fall 2024, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the external merge sort */
  void Init() override;

  /**
   * Yield the next tuple from the external merge sort.
   * @param[out] tuple The next tuple produced by the external merge sort.
   * @param[out] rid The next tuple RID produced by the external merge sort.
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  void FlushBufferToRun(std::vector<SortEntry> buffer, BufferPoolManager *bpm, size_t max_tuples_per_page,
                        size_t tuple_length, std::vector<MergeSortRun> &runs);

  void Merge2Runs(MergeSortRun run1, MergeSortRun run2, BufferPoolManager *bpm, size_t max_tuples_per_page,
                  size_t tuple_length, std::vector<MergeSortRun> &new_runs);
  auto FlushBufferToRun(std::vector<SortEntry> buffer, BufferPoolManager *bpm, size_t max_tuples_per_page,
                        size_t tuple_length, std::vector<page_id_t> &pages) -> MergeSortRun;

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO: You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<MergeSortRun> runs_;
  MergeSortRun::Iterator current_iterator_;
  MergeSortRun::Iterator end_iterator_;
  bool is_inited_{false};
  std::unique_ptr<ThreadPool> thread_pool_;
  std::mutex thread_mutex_;
  std::atomic<int> task_nums_{0};
};

}  // namespace bustub
