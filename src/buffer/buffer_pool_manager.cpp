//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <mutex>
#include <optional>
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"
// TODO(LZY): 结合DiskSchdular 实现真正的并发IO
namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  // std::fill(data_.begin(), data_.end(), 0);
  std::memset(data_.data(), 0, data_.size());
  pin_count_.store(0);
  is_dirty_ = false;
  current_page_id_ = INVALID_PAGE_ID;
}

auto FrameHeader::GetPinCount() -> size_t { return pin_count_.load(); }

auto FrameHeader::GetCurrPageId() -> page_id_t { return current_page_id_; }

auto FrameHeader::IsDirty() -> bool { return is_dirty_; }

void FrameHeader::FetchAdd() { pin_count_.fetch_add(1); }

void FrameHeader::FetchSub() { pin_count_.fetch_sub(1); }
/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param k_dist The backward k-distance for the LRU-K replacer.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, size_t k_dist,
                                     LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<LRUKReplacer>(num_frames, k_dist)),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * Also, make sure to read the documentation for `DeletePage`! You can assume that you will never run out of disk
 * space (via `DiskScheduler::IncreaseDiskSpace`), so this function _cannot_ fail.
 *
 * Once you have allocated the new page via the counter, make sure to call `DiskScheduler::IncreaseDiskSpace` so you
 * have enough space on disk!
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
auto BufferPoolManager::NewPage() -> page_id_t {
  // std::scoped_lock latch(*bpm_latch_);
  page_id_t page_id = next_page_id_.load();
  next_page_id_.fetch_add(1);
  disk_scheduler_->IncreaseDiskSpace(next_page_id_.load());
  return page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places a page or a page's metadata could be, and use that to guide you on implementing this
 * function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * Ideally, we would want to ensure that all space on disk is used efficiently. That would mean the space that deleted
 * pages on disk used to occupy should somehow be made available to new pages allocated by `NewPage`.
 *
 * If you would like to attempt this, you are free to do so. However, for this implementation, you are allowed to
 * assume you will not run out of disk space and simply keep allocating disk space upwards in `NewPage`.
 *
 * For (nonexistent) style points, you can still call `DeallocatePage` in case you want to implement something slightly
 * more space-efficient in the future.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock latch(*bpm_latch_);
  if (page_id < 0 || page_id >= next_page_id_.load()) {
    return false;
  }
  if (auto frame_id = page_table_.find(page_id); frame_id != page_table_.end()) {
    auto frame_idx = frame_id->second;
    // LOG_INFO("DeletePage: frame_id: %d, page_id: %d, PINCOUNT: %ld", frame_idx, page_id,
    //          frames_[frame_idx]->GetPinCount());
    if (frames_[frame_idx]->GetPinCount() > 0) {
      return false;
    }

    // if (frames_[frame_idx]->IsDirty()) {
    //   LoadPage(page_id, frame_idx, true);
    // }
    // 直接remove，该frame是不可驱逐的，我们需要先将其设置为可驱逐
    replacer_->SetEvictable(frame_idx, true);
    frames_[frame_idx]->Reset();
    free_frames_.push_back(frame_idx);
    page_table_.erase(frame_id);
    replacer_->Remove(frame_idx);
    // LOG_DEBUG("BPM deallocatePage: page_id: %d", page_id);
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are 3 main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */

auto BufferPoolManager::GetFreeFrame(page_id_t page_id) -> std::optional<frame_id_t> {
  if (page_id < 0 || page_id >= next_page_id_.load()) {
    return std::nullopt;
  }
  if (!free_frames_.empty()) {
    auto frame_id = free_frames_.front();
    free_frames_.pop_front();
    return frame_id;
  }
  // replacer 驱逐
  auto frame_id = replacer_->Evict();
  if (!frame_id.has_value()) {
    return std::nullopt;
  }
  auto evict_page_id = frames_[frame_id.value()]->GetCurrPageId();
  if (evict_page_id != INVALID_PAGE_ID) {
    if (frames_[frame_id.value()]->IsDirty()) {
      LoadPage(evict_page_id, frame_id.value(), true);
    }
    page_table_.erase(evict_page_id);
    frames_[frame_id.value()]->Reset();
    disk_scheduler_->DeallocatePage(evict_page_id);  // for sqllogictest 16
  }
  return frame_id;
}

auto BufferPoolManager::UpdatePageTable(page_id_t page_id, frame_id_t frame_id) -> void {
  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  frames_[frame_id]->SetCurrPageId(page_id);
  frames_[frame_id]->FetchAdd();
}

auto BufferPoolManager::LoadPage(page_id_t page_id, frame_id_t frame_id, bool is_write) -> void {
  auto data = is_write ? frames_[frame_id]->GetDataMut() : const_cast<char *>(frames_[frame_id]->GetData());
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  DiskRequest r{is_write, data, page_id, std::move(promise)};
  disk_scheduler_->Schedule(std::move(r));
  future.get();
}

auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  bpm_latch_->lock();
  if (auto frame_id = page_table_.find(page_id); frame_id != page_table_.end()) {
    UpdatePageTable(page_id, frame_id->second);
    bpm_latch_->unlock();
    // no addtional IO
    return WritePageGuard(page_id, frames_[frame_id->second], replacer_, bpm_latch_);
  }
  auto frame = GetFreeFrame(page_id);
  if (!frame.has_value()) {
    bpm_latch_->unlock();
    return std::nullopt;
  }
  UpdatePageTable(page_id, frame.value());
  LoadPage(page_id, frame.value(), false);
  bpm_latch_->unlock();  // 获取rw_latch前需要释放bpm_latch
  return WritePageGuard(page_id, frames_[frame.value()], replacer_, bpm_latch_);
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`, otherwise returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  bpm_latch_->lock();
  if (auto frame_id = page_table_.find(page_id); frame_id != page_table_.end()) {
    UpdatePageTable(page_id, frame_id->second);
    bpm_latch_->unlock();
    // no addtional IO
    return ReadPageGuard(page_id, frames_[frame_id->second], replacer_, bpm_latch_);
  }
  auto frame = GetFreeFrame(page_id);
  if (!frame.has_value()) {
    bpm_latch_->unlock();
    return std::nullopt;
  }
  UpdatePageTable(page_id, frame.value());
  LoadPage(page_id, frame.value(), false);
  bpm_latch_->unlock();  // 获取rw_latch前需要释放bpm_latch
  return ReadPageGuard(page_id, frames_[frame.value()], replacer_, bpm_latch_);
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table, otherwise `true`.
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::unique_lock latch(*bpm_latch_);
  if (page_id < 0 || page_id >= next_page_id_.load()) {
    return false;
  }
  auto flush_frame = page_table_.find(page_id);
  if (flush_frame == page_table_.end()) {
    return false;
  }
  if (frames_[flush_frame->second]->IsDirty()) {
    LoadPage(page_id, flush_frame->second, true);
  }
  // frames_[flush_frame->second]->Reset();
  frames_[flush_frame->second]->is_dirty_ = false;
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::unique_lock latch(*bpm_latch_);
  for (auto &[page_id, frame_id] : page_table_) {
    auto frame = frames_[frame_id];
    if (frame->IsDirty()) {
      LoadPage(page_id, frame_id, true);
    }
    frame->is_dirty_ = false;
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple
 * threads access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely
 * cause problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds
 * the page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will
 * still need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists, otherwise `std::nullopt`.
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  std::scoped_lock latch(*bpm_latch_);
  if (auto frame_id = page_table_.find(page_id); frame_id != page_table_.end()) {
    LOG_INFO("GetPinCount: frame_id: %d, page_id: %d, PINCOUNT: %ld, DATA: %s", frame_id->second, page_id,
             frames_[frame_id->second]->GetPinCount(), frames_[frame_id->second]->GetData());
    return frames_[frame_id->second]->GetPinCount();
  }
  return std::nullopt;
}

}  // namespace bustub
