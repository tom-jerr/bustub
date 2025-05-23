//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::IncreaseSize(int amount) { SetSize(GetSize() + amount); }

/*
 * Helper method to get/set the key associated with input "index" (a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return key_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { key_array_[index] = key; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { page_id_array_[index] = value; }
/*
 * Helper method to get the value associated with input "index" (a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return page_id_array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  auto it = std::find_if(page_id_array_, page_id_array_ + GetSize(), [&](const ValueType &v) { return v == value; });
  if (it == page_id_array_ + GetSize()) {
    return -1;
  }
  return std::distance(page_id_array_, it);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator, int leftmost,
                                            int rightmost) const -> ValueType {
  BUSTUB_ASSERT(GetSize() > 0, "size should be greater than 0");
  if (leftmost != 0) {
    return page_id_array_[0];
  }
  if (rightmost != 0) {
    return page_id_array_[GetSize() - 1];
  }
  int l = 1;
  int r = GetSize() - 1;
  while (l < r) {
    int mid = (l + r) / 2;
    if (comparator(key_array_[mid], key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  if (comparator(key_array_[l], key) > 0) {
    // LOG_DEBUG("key: %ld, pid: %d", key.ToString(), page_id_array_[l - 1]);
    return page_id_array_[l - 1];  // 当前节点
  }
  // LOG_DEBUG("key: %ld, pid: %d", key.ToString(), page_id_array_[l]);
  return page_id_array_[l];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_node_id, const KeyType &new_key,
                                                     const ValueType &new_node_id) {
  SetKeyAt(1, new_key);
  SetValueAt(0, old_node_id);
  SetValueAt(1, new_node_id);
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfValueTo(BPlusTreeInternalPage *recipient) {
  // Move half of the keys and values to the recipient page
  int half_size = GetSize() / 2;
  int index = GetSize() % 2 == 0 ? half_size + 1 : half_size;
  // Move keys
  std::copy(key_array_ + index, key_array_ + GetSize(), recipient->key_array_ + 1);
  std::copy(page_id_array_ + half_size, page_id_array_ + GetSize(), recipient->page_id_array_);
  recipient->SetSize(half_size);
  SetSize(half_size);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, int old_index,
                                            const KeyComparator &comparator) -> bool {
  std::move(key_array_ + old_index + 1, key_array_ + GetSize(), key_array_ + old_index + 2);
  std::move(page_id_array_ + old_index + 1, page_id_array_ + GetSize(), page_id_array_ + old_index + 2);
  key_array_[old_index + 1] = key;
  page_id_array_[old_index + 1] = value;
  IncreaseSize(1);
  return GetSize() <= GetMaxSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveKVForward() {
  // 全部向前移动一位
  std::move(key_array_ + 1, key_array_ + GetSize(), key_array_);
  std::move(page_id_array_ + 1, page_id_array_ + GetSize(), page_id_array_);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseLastKV() {
  // 删除最后一个key和value
  key_array_[GetSize() - 1] = KeyType();
  page_id_array_[GetSize() - 1] = ValueType();
  IncreaseSize(-1);
}
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllValueForward() {
//   // 全部向前移动一位
//   std::move(page_id_array_ + 1, page_id_array_ + GetSize(), page_id_array_);
//   // IncreaseSize(1);
// }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllKeyForward() {
//   // 全部向前移动一位
//   std::move(key_array_ + 1, key_array_ + GetSize(), key_array_);
//   // IncreaseSize(1);
// }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyAfter(const KeyType &key, const KeyComparator &comparator) {
  key_array_[GetSize()] = key;
  // IncreaseSize(1);
}
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyBefore(const KeyType &key, const KeyComparator &comparator) {
//   if (GetSize() == 0) {
//     key_array_[0] = key;
//     IncreaseSize(1);
//     return;
//   }
//   std::move_backward(key_array_ + 1, key_array_ + GetSize(), key_array_ + GetSize() + 1);
//   key_array_[1] = key;
//   // IncreaseSize(1);
// }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const KeyType &key, const ValueType &new_node_id) {
  // if (GetSize() == 0) {
  //   page_id_array_[0] = new_node_id;
  //   key_array_[1] = key;
  //   IncreaseSize(1);
  //   return;
  // }
  // 因为internal page的大小最小是1，所以这里不需要判断
  key_array_[GetSize()] = key;
  page_id_array_[GetSize()] = new_node_id;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeBefore(const KeyType &key, const ValueType &new_node_id) {
  if (GetSize() == 0) {
    key_array_[1] = key;
    page_id_array_[0] = new_node_id;
    IncreaseSize(1);
    return;
  }
  std::move_backward(key_array_ + 1, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::move_backward(page_id_array_, page_id_array_ + GetSize(), page_id_array_ + GetSize() + 1);
  key_array_[1] = key;
  page_id_array_[0] = new_node_id;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAllNodeAfterFrom(BPlusTreeInternalPage *node,
                                                            const KeyComparator &comparator) {
  auto size = GetSize();
  auto node_size = node->GetSize();
  if (node_size == 1) {
    if (comparator(node->KeyAt(1), node->KeyAt(INTERNAL_PAGE_SLOT_CNT - 1)) != 0) {  // don't need to insert the key
      key_array_[size] = node->KeyAt(1);
    }
    page_id_array_[size] = node->ValueAt(0);
    IncreaseSize(1);
    return;
  }
  std::copy(node->key_array_ + 1, node->key_array_ + node_size, key_array_ + size + 1);
  std::copy(node->page_id_array_, node->page_id_array_ + node_size, page_id_array_ + size);
  IncreaseSize(node_size);
}

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAllNodeBefore(BPlusTreeInternalPage *node, const KeyComparator
// &comparator) {
//   auto node_size = node->GetSize();
//   if (node_size == 1) {
//     if (comparator(node->KeyAt(1), node->KeyAt(INTERNAL_PAGE_SLOT_CNT - 1)) != 0) {
//       std::move_backward(key_array_ + 1, key_array_ + GetSize(), key_array_ + GetSize() + node_size);

//       key_array_[1] = node->KeyAt(node_size);
//     }
//     std::move_backward(page_id_array_, page_id_array_ + GetSize(), page_id_array_ + GetSize() + node_size);
//     std::copy(node->page_id_array_, node->page_id_array_ + node_size, page_id_array_);
//     IncreaseSize(node_size);
//     return;
//   }
//   std::move_backward(key_array_ + 1, key_array_ + GetSize(), key_array_ + GetSize() + node_size);
//   std::move_backward(page_id_array_, page_id_array_ + GetSize(), page_id_array_ + GetSize() + node_size);
//   std::copy(node->key_array_ + 1, node->key_array_ + node_size, key_array_ + 1);
//   std::copy(node->page_id_array_, node->page_id_array_ + node_size, page_id_array_);
//   IncreaseSize(node_size);
// }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient) {
  auto key = key_array_[1];
  auto value = page_id_array_[0];
  std::move(key_array_ + 1, key_array_ + GetSize(), key_array_);
  std::move(page_id_array_, page_id_array_ + GetSize(), page_id_array_);
  IncreaseSize(-1);
  recipient->InsertNodeAfter(key, value);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient) {
  auto key = key_array_[GetSize() - 1];
  auto value = page_id_array_[GetSize() - 1];
  IncreaseSize(-1);
  recipient->InsertNodeBefore(key, value);
}

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const KeyType &key, const ValueType &value) {
//   key_array_[1] = key;
//   page_id_array_[0] = value;
//   IncreaseSize(1);
// }
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const KeyType &key, const ValueType &value) {
//   key_array_[GetSize()] = key;
//   page_id_array_[GetSize()] = value;
//   IncreaseSize(1);
// }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> page_id_t {
  BUSTUB_ASSERT(GetSize() == 1, "not only child");
  auto ret = page_id_array_[0];
  page_id_array_[0] = INVALID_PAGE_ID;
  SetSize(0);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, int index, const KeyComparator &comparator) -> bool {
  BUSTUB_ASSERT(GetSize() > 0, "size is 0");
  auto key_index = index != 0 ? index : index + 1;
  if (comparator(key_array_[key_index], key) == 0) {
    if (GetSize() == key_index + 1) {
      key_array_[key_index] = key_array_[INTERNAL_PAGE_SLOT_CNT - 1];
    } else {
      std::move(key_array_ + key_index + 1, key_array_ + GetSize(), key_array_ + key_index);
    }
  }

  std::move(page_id_array_ + index + 1, page_id_array_ + GetSize(), page_id_array_ + index);
  IncreaseSize(-1);
  return GetSize() >= GetMinSize();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
