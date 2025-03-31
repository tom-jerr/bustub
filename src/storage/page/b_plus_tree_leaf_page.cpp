//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
}

// Insert
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int size = GetSize();
  /*叶子节点插入kv，返回插入后kv数量*/
  auto insert_position = KeyIndex(key, comparator);
  if (insert_position == GetSize()) {
    /*说明在末尾追加, 这里代码逻辑可以稍微优化一下*/
    *(key_array_ + size) = key;
    *(rid_array_ + size) = value;
    IncreaseSize(1);
    return true;
  }
  /*key重复*/
  if (comparator(key_array_[insert_position], key) == 0) {
    return false;
  }
  /*一般位置*/
  // for (int i = size; i > insert_position; i--) {
  //   key_array_[i] = key_array_[i - 1];
  //   rid_array_[i] = rid_array_[i - 1];
  // }
  std::move_backward(key_array_ + insert_position, key_array_ + GetSize(), key_array_ + GetSize() + 1);

  std::move_backward(rid_array_ + insert_position, rid_array_ + GetSize(), rid_array_ + GetSize() + 1);

  key_array_[insert_position] = key;
  rid_array_[insert_position] = value;
  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::IncreaseSize(int amount) { SetSize(GetSize() + amount); }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertNodeAfter(const KeyType &key, const ValueType &value) {
  auto size = GetSize();
  key_array_[size] = key;
  rid_array_[size] = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertNodeBefore(const KeyType &key, const ValueType &value) {
  std::move_backward(key_array_, key_array_ + GetSize(), key_array_ + GetSize() + 1);
  std::move_backward(rid_array_, rid_array_ + GetSize(), rid_array_ + GetSize() + 1);
  *key_array_ = key;
  *rid_array_ = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  auto half_size = GetSize() / 2;
  auto size = GetSize();
  std::copy(key_array_ + half_size, key_array_ + size, recipient->key_array_);
  std::copy(rid_array_ + half_size, rid_array_ + size, recipient->rid_array_);
  SetSize(half_size);
  recipient->IncreaseSize(size - half_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAllNodeAfterFrom(BPlusTreeLeafPage *node) {
  auto size = GetSize();
  auto node_size = node->GetSize();
  if (node_size == 0) {
    return;
  }
  std::copy(node->key_array_, node->key_array_ + node->GetSize(), key_array_ + size);
  std::copy(node->rid_array_, node->rid_array_ + node->GetSize(), rid_array_ + size);
  IncreaseSize(node_size);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAllNodeBefore(BPlusTreeLeafPage *node) {
  if (node->GetSize() == 0) {
    return;
  }
  std::move_backward(key_array_, key_array_ + GetSize(), key_array_ + node->GetSize() + GetSize());
  std::move_backward(rid_array_, rid_array_ + GetSize(), rid_array_ + node->GetSize() + GetSize());
  std::copy(node->key_array_, node->key_array_ + node->GetSize(), key_array_);
  std::copy(node->rid_array_, node->rid_array_ + node->GetSize(), rid_array_);
  IncreaseSize(node->GetSize());
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < 0 || index >= GetSize()) {
    return {};
  }
  return key_array_[index];
}

/*
 * Helper method to find and return the value associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  if (index < 0 || index >= GetSize()) {
    return {};
  }
  return rid_array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) const -> int {
  auto target =
      std::lower_bound(key_array_, key_array_ + GetSize(), key,
                       [&keyComparator](const KeyType &a, const KeyType &b) { return keyComparator(a, b) < 0; });
  auto distance = std::distance(key_array_, target);
  return distance;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *split_node) {
  auto key = key_array_[0];
  auto value = rid_array_[0];
  std::move(key_array_ + 1, key_array_ + GetSize(), key_array_);
  std::move(rid_array_ + 1, rid_array_ + GetSize(), rid_array_);
  IncreaseSize(-1);
  split_node->InsertNodeAfter(key, value);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *split_node) {
  auto key = key_array_[GetSize() - 1];
  auto value = rid_array_[GetSize() - 1];
  IncreaseSize(-1);
  split_node->InsertNodeBefore(key, value);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, std::vector<ValueType> *result,
                                        KeyComparator comparator) const -> bool {
  // binary search
  int left = 0;
  int right = GetSize() - 1;

  // 二分查找
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) > 0) {
      right = mid - 1;
    } else if (comparator(KeyAt(mid), key) < 0) {
      left = mid + 1;
    } else {
      result->emplace_back(rid_array_[mid]);
      return true;
    }
  }

  // 如果未找到匹配的键值，则返回false
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> bool {
  int size = GetSize();
  auto delete_position = KeyIndex(key, comparator);
  if (delete_position == size) {
    return false;
  }
  if (comparator(key_array_[delete_position], key) != 0) {
    return false;
  }
  std::move(key_array_ + delete_position + 1, key_array_ + size, key_array_ + delete_position);
  std::move(rid_array_ + delete_position + 1, rid_array_ + size, rid_array_ + delete_position);
  IncreaseSize(-1);
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
