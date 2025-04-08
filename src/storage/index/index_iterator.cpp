/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <cassert>
#include <chrono>
#include <future>
#include <optional>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, std::optional<ReadPageGuard> &&leaf_page_guard, int index)
    : bpm_(bpm), leaf_page_guard_(std::move(leaf_page_guard)), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return !leaf_page_guard_.has_value(); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto leaf_page = leaf_page_guard_.value().As<LeafPage>();
  return std::make_pair(leaf_page->KeyAt(index_), leaf_page->ValueAt(index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto current_leaf_page = leaf_page_guard_.value().As<LeafPage>();
  ++index_;
  if (index_ >= current_leaf_page->GetSize()) {
    if (current_leaf_page->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_page_guard = bpm_->ReadPage(current_leaf_page->GetNextPageId());
      leaf_page_guard_ = std::optional<ReadPageGuard>(std::move(next_page_guard));
    } else {
      leaf_page_guard_ = std::nullopt;
    }
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
