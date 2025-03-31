/**
 * b_plus_tree.h
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Definition of the Context class.
 *
 * Hint: This class is designed to help you keep track of the pages
 * that you're modifying or accessing.
 */
class Context {
 public:
  // std::mutex mutex_;
  // When you insert into / remove from the B+ tree, store the write guard of header page here.
  // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // Save the root page id here so that it's easier to know if the current page is the root page.
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // Store the write guards of the pages that you're modifying here.
  std::deque<WritePageGuard> write_set_;

  // You may want to use this when getting value, but not necessary.
  std::deque<ReadPageGuard> read_set_;

  // ~Context() {
  //   if (header_page_.has_value()) {
  //     header_page_ = std::nullopt;
  //   }
  //   for (auto &w : write_set_) {
  //     w.Drop();
  //   }
  //   for (auto &r : read_set_) {
  //     r.Drop();
  //   }
  // }

  auto IsRootPage(page_id_t page_id) -> bool {
    // std::lock_guard lock(mutex_);
    return page_id == root_page_id_;
  }

  void ReleaseWriteLatchExceptLast() {
    if (header_page_.has_value()) {
      header_page_.reset();
    }
    if (write_set_.empty() || write_set_.size() == 1) {
      return;
    }
    for (size_t i = 0; i < write_set_.size() - 1; i++) {
      write_set_.pop_front();
    }
  }
  void Print() {
    std::cout << "write set: ";
    for (auto &w : write_set_) {
      std::cout << w.GetPageId() << " ";
    }
    std::cout << std::endl;
  }
};

// find leaf 中对不同的操作，find leaf加锁的方式不同
enum class Operation : int8_t {
  SEARCH = 0,
  INSERT,
  REMOVE,
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// Main class providing the API for the Interactive B+ Tree.
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  // TODO(LZY): 源文件中需要as成叶子节点或者内部节点使用这两个别名！！！
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  struct SplitRet {
    page_id_t new_page_id_;
    KeyType new_key_;
  };

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  auto UpdateRoot(Context *ctx, page_id_t root_page_id) -> bool;
  // leftmost 和 rightmost是为了实现Begin()和end()
  auto FindLeafPage(Context *ctx, Operation op, const KeyType &key, const KeyComparator &comparator, int leftmost,
                    int rightmost) -> page_id_t;
  auto FindSearchLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator, int leftmost,
                          int rightmost) -> page_id_t;
  auto FindInsertLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator, int leftmost,
                          int rightmost) -> page_id_t;
  auto FindRemoveLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator, int leftmost,
                          int rightmost) -> page_id_t;
  // auto CreateNewNode() -> page_id_t;
  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;
  auto IsEmpty(Context *ctx) const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value) -> bool;
  auto StartNewTree(Context *ctx, const KeyType &key, const ValueType &value) -> bool;
  void InsertIntoParent(Context *ctx, page_id_t old_node_id, const KeyType &key, page_id_t new_node_id,
                        int recursive_level);

  auto SplitLeaf(Context *ctx) -> SplitRet;
  auto SplitInternal(Context *ctx, int recursive_level) -> SplitRet;
  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  // TODO(LZY): delete需要全部重写
  auto DeleteEntry(Context *ctx, const KeyType &key, int delete_index, page_id_t current_page_id, int recursive_level)
      -> bool;

  auto Coalesce(Context *ctx, int sibling_index, bool sibling_is_predecessor, int recursive_level,
                const KeyType &parent_key) -> bool;

  auto Redistribute(Context *ctx, int old_index, int sibling_index, int recursive_level, const KeyType &parent_key)
      -> bool;

  void AdjustRoot(Context *ctx);

  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() const -> page_id_t;

  // Index iterator
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  // Print the B+ tree
  void Print(BufferPoolManager *bpm);

  // Draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  /**
   * @brief draw a B+ tree, below is a printed
   * B+ tree(3 max leaf, 4 max internal) after inserting key:
   *  {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 18, 19, 20}
   *
   *                               (25)
   *                 (9,17,19)                          (33)
   *  (1,5)    (9,13)    (17,18)    (19,20,21)    (25,29)    (33,37)
   *
   * @return std::string
   */
  auto DrawBPlusTree() -> std::string;

  // read data from file and insert one by one
  void InsertFromFile(const std::filesystem::path &file_name);

  // read data from file and remove one by one
  void RemoveFromFile(const std::filesystem::path &file_name);

  /**
   * @brief Read batch operations from input file, below is a sample file format
   * insert some keys and delete 8, 9 from the tree with one step.
   * { i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i30 d8 d9 } //  batch.txt
   * B+ Tree(4 max leaf, 4 max internal) after processing:
   *                            (5)
   *                 (3)                (7)
   *            (1,2)    (3,4)    (5,6)    (7,10,30) //  The output tree example
   */
  void BatchOpsFromFile(const std::filesystem::path &file_name);

 private:
  /* Debug Routines for FREE!! */
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  /**
   * @brief Convert A B+ tree into a Printable B+ tree
   *
   * @param root_id
   * @return PrintableNode
   */
  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // member variable
  std::string index_name_;
  BufferPoolManager *bpm_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
  std::mutex context_insert_mutex_;
  // std::mutex log_mutex_;
};

/**
 * @brief for test only. PrintableBPlusTree is a printable B+ tree.
 * We first convert B+ tree into a printable B+ tree and the print it.
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief BFS traverse a printable B+ tree and print it into
   * into out_buf
   *
   * @param out_buf
   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
