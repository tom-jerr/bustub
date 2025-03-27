/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"
#include <cassert>
#include <chrono>
#include <future>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/b_plus_tree_page.h"

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
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, ReadPageGuard &&leaf_page_guard, int index)
    : bpm_(bpm), leaf_page_guard_(std::move(leaf_page_guard)), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto leaf_page = leaf_page_guard_.As<LeafPage>();
  return leaf_page->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  auto leaf_page = leaf_page_guard_.As<LeafPage>();
  return std::make_pair(leaf_page->KeyAt(index_), leaf_page->ValueAt(index_));
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto current_leaf_page = leaf_page_guard_.As<LeafPage>();
  auto next_page_id = current_leaf_page->GetNextPageId();
  if (index_ == current_leaf_page->GetSize() - 1 && next_page_id != INVALID_PAGE_ID) {
    auto next_page_guard = bpm_->ReadPage(next_page_id);
    leaf_page_guard_ = std::move(next_page_guard);
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
