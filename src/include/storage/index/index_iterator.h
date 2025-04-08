//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <optional>
#include <utility>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, std::optional<ReadPageGuard> &&leaf_page_guard, int index);

  IndexIterator();
  auto operator=(IndexIterator &&that) noexcept -> IndexIterator & {
    bpm_ = that.bpm_;
    leaf_page_guard_ = std::move(that.leaf_page_guard_);
    index_ = that.index_;
    that.index_ = -1;
    that.leaf_page_guard_ = std::nullopt;
    that.bpm_ = nullptr;
    return *this;
  }
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType &, const ValueType &>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (!this->leaf_page_guard_.has_value()) {
      return static_cast<bool>(!itr.leaf_page_guard_.has_value());
    }
    if (!itr.leaf_page_guard_.has_value()) {
      return static_cast<bool>(!this->leaf_page_guard_.has_value());
    }
    return leaf_page_guard_.value().GetPageId() == itr.leaf_page_guard_.value().GetPageId() && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  BufferPoolManager *bpm_;
  std::optional<ReadPageGuard> leaf_page_guard_;
  int index_;
};

}  // namespace bustub
