//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <mutex>
#include <vector>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {
// ========================== LRUKNODE ==========================
LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

LRUKNode::LRUKNode(size_t k, frame_id_t fid, size_t current_timestamp) : k_(k), fid_(fid) {
  history_.push_front(current_timestamp);
}

auto LRUKNode::AccessNode(size_t current_timestamp) -> bool {
  if (history_.size() == k_) {
    history_.pop_back();
  }
  history_.push_front(current_timestamp);
  return true;
}

auto LRUKNode::BackwardKDistance(size_t current_timestamp) -> size_t {
  if (history_.size() < k_) {
    return std::numeric_limits<size_t>::max();
  }
  return current_timestamp - history_.front();
}

// ============================ LRU_K_REPLACER ============================
/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock<std::mutex> lru_latch(latch_);
  std::vector<frame_id_t> candidate_evicted;           // 不为inf的frame
  std::vector<frame_id_t> candidate_evicted_with_inf;  // backward distance 为inf的frame
  frame_id_t ret;                                      // 返回的frame_id
  for (auto &[frame, node] : node_store_) {
    if (node.IsEvictable()) {
      if (node.BackwardKDistance(current_timestamp_) == std::numeric_limits<size_t>::max()) {
        candidate_evicted_with_inf.emplace_back(frame);
      } else {
        candidate_evicted.emplace_back(frame);
      }
    }
  }
  if (candidate_evicted.empty() && candidate_evicted_with_inf.empty()) {
    return std::nullopt;
  }

  if (candidate_evicted_with_inf.empty()) {
    // 升序排列
    // 注意：使用[]需要默认构造函数
    std::sort(candidate_evicted.begin(), candidate_evicted.end(), [&](frame_id_t a, frame_id_t b) {
      return node_store_[a].BackwardKDistance(current_timestamp_) >
             node_store_[b].BackwardKDistance(current_timestamp_);
    });
    ret = candidate_evicted.front();
  } else {
    // 有多个少于k次的frame
    std::sort(candidate_evicted_with_inf.begin(), candidate_evicted_with_inf.end(), [&](frame_id_t a, frame_id_t b) {
      return node_store_[a].GetLastAccessTime() < node_store_[b].GetLastAccessTime();
    });
    ret = candidate_evicted_with_inf.front();
  }

  // 驱逐后对node_store进行处理
  node_store_.erase(ret);  // evict this frame
  curr_size_--;
  return ret;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock<std::mutex> lru_latch(latch_);
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("frame id is invalid");
  }
  // SetEvictable(frame_id, false);

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode(k_, frame_id, current_timestamp_));

  } else {
    iter->second.AccessNode(current_timestamp_);
  }
  current_timestamp_++;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lru_latch(latch_);
  if (frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("frame id is invalid");
  }

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }

  if (iter->second.IsEvictable() == set_evictable) {
    return;
  }

  iter->second.SetEvictable(set_evictable);
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lru_latch(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }

  if (!iter->second.IsEvictable()) {
    throw Exception("frame is non-evictable");
  }

  node_store_.erase(iter);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lru_latch(latch_);
  return curr_size_;
}

}  // namespace bustub
