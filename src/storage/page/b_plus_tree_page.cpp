//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"
#include <cmath>
#include "common/macros.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) {
  BUSTUB_ASSERT(size_ >= 0, "Size should be non-negative");
  size_ = size;
}
void BPlusTreePage::ChangeSizeBy(int amount) {
  max_size_ += amount;
  BUSTUB_ASSERT(max_size_ >= 0, "Size should be non-negative");
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) {
  BUSTUB_ASSERT(size >= 0, "Size should be non-negative");
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * But whether you will take ceil() or floor() depends on your implementation
 */
// 如果是ceil的话，需要先分裂再插入；使用floor的话，先插入再分裂，我们的实现采用后者
auto BPlusTreePage::GetMinSize() const -> int {
  return IsLeafPage() ? std::floor(GetMaxSize() / 2.0) : std::ceil(GetMaxSize() / 2.0);
}

}  // namespace bustub
