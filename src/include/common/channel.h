//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// channel.h
//
// Identification: src/include/common/channel.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <utility>

namespace bustub {

/**
 * Channels allow for safe sharing of data between threads. This is a multi-producer multi-consumer channel.
 */
template <class T>
class Channel {
 public:
  Channel() = default;
  ~Channel() = default;

  Channel(const Channel &other) = delete;
  auto operator=(const Channel &other) -> Channel & = delete;

  Channel(Channel &&other) noexcept {
    std::lock_guard<std::mutex> lk(other.m_);
    q_ = std::move(other.q_);
  }

  auto operator=(Channel &&other) noexcept -> Channel & {
    if (this != &other) {
      std::lock(m_, other.m_);
      std::lock_guard<std::mutex> lk_this(m_, std::adopt_lock);
      std::lock_guard<std::mutex> lk_other(other.m_, std::adopt_lock);
      q_ = std::move(other.q_);
    }
    return *this;
  }
  /**
   * @brief Inserts an element into a shared queue.
   *
   * @param element The element to be inserted.
   */
  void Put(T element) {
    std::unique_lock<std::mutex> lk(m_);
    q_.push(std::move(element));
    lk.unlock();
    cv_.notify_all();
  }

  /**
   * @brief Gets an element from the shared queue. If the queue is empty, blocks until an element is available.
   */
  auto Get() -> T {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&]() { return !q_.empty(); });
    T element = std::move(q_.front());
    q_.pop();
    return element;
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::queue<T> q_;
};
}  // namespace bustub
