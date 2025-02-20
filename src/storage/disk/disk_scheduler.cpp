//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <mutex>
#include <optional>
#include <shared_mutex>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
  // for (size_t i = 0; i < THREAD_NUM; ++i) {
  //   thread_pool_[i] = std::thread([&] { StartWorkerThread(i); });
  // }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
  // for (size_t i = 0; i < THREAD_NUM; ++i) {
  //   if (thread_pool_[i].has_value()) {
  //     thread_pool_[i]->join();
  //   }
  // }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

void DiskScheduler::StartWorkerThread() {
  // 尝试多次try_dequeue

  // int retry = 0;
  std::optional<DiskRequest> request;
  while (true) {
    request = request_queue_.Get();
    // TODO(LZY) ：执行 DiskManager 逻辑
    if (request != std::nullopt) {
      if (request->is_write_) {
        disk_manager_->WritePage(request->page_id_, request->data_);
        request->callback_.set_value(true);
      } else if (!request->is_write_) {
        disk_manager_->ReadPage(request->page_id_, request->data_);
        request->callback_.set_value(true);
      }
    } else {
      // 结束循环
      break;
    }
    // TODO(LZY) : 未解决读后写问题：两个线程同时执行写和读(写请求先schedule)，但是读却先执行造成结果错误
  }

  // request_queue_.wait_dequeue(request);
}

}  // namespace bustub
