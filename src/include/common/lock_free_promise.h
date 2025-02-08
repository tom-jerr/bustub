#pragma once

#include <readerwriterqueue/readerwriterqueue.h>
#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include "common/channel.h"

#define RETRY_TIMES 100

template <typename T>
class LockFreePromise {
 public:
  LockFreePromise() : ready_(false) {}
  /**
   * @brief Copy Construct need to be deleted
   *
   */
  LockFreePromise(const LockFreePromise &other) = delete;
  //{
  //   ready_.store(other.ready_.load(std::memory_order_acquire), std::memory_order_release);
  //   value_ = other.value_;
  //   queue_ = other.queue_;
  // }
  LockFreePromise &operator=(const LockFreePromise &) = delete;
  /**
   * @brief Move Construct
   *
   * @param other
   */
  LockFreePromise(LockFreePromise &&other) {
    ready_.store(other.ready_.load(std::memory_order_acquire), std::memory_order_release);
    value_ = std::move(value_);
    queue_ = std::move(other.queue_);
  }
  /**
   * @brief Move assignment
   *
   * @param other
   * @return LockFreePromise&
   */
  LockFreePromise &operator=(LockFreePromise &&other) {
    ready_.store(other.ready_.load(std::memory_order_acquire), std::memory_order_release);
    value_ = std::move(other.value_);
    queue_ = std::move(other.queue_);
    return *this;
  }

  // 设置结果并通知等待的线程
  void set_value(T &&value) {
    value_ = std::move(value);
    ready_.store(true, std::memory_order_release);
    queue_.Put(value_);
  }

  void set_value(const T &value) {
    value_ = value;
    ready_.store(true, std::memory_order_release);
    queue_.Put(value_);
  }

  // 获取结果
  T get_value() {
    int retry = 0;
    while (++retry <= RETRY_TIMES && !ready_.load(std::memory_order_acquire)) {
      // 尝试100次，如果没有结果，就等待
    }
    [[unlikely]] if (retry > RETRY_TIMES) {
      // 必须先上unique_lock，才能使用conditional_variable
      std::unique_lock<std::mutex> get_value_lock(lock_);
      cv_.wait(get_value_lock, [this] { return ready_.load(std::memory_order_acquire); });
    }

    return queue_.Get();
  }

  // 获取与 `std::promise` 兼容的 `future`
  std::future<T> get_future() {
    auto promise = std::make_shared<std::promise<T>>();
    // std::thread([this, promise]() mutable {
    T result = this->get_value();
    promise->set_value(result);
    // }).detach();
    return promise->get_future();
  }

 private:
  std::atomic<bool> ready_;
  std::condition_variable cv_;
  std::mutex lock_;
  T value_;
  // moodycamel::ReaderWriterQueue<T> queue_;
  bustub::Channel<T> queue_;
};

template <typename T>
using LockFreeFuture = std::future<T>;
