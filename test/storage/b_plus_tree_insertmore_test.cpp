//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2024, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DISABLED_BiggerInternalInsertTest) {
  std::string filename = "../../vision/b_plus_tree_visualization.txt";
  std::ofstream out_file(filename);
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);
  GenericKey<8> index_key;
  RID rid;
  std::vector<int64_t> insert_keys;
  std::vector<int64_t> remove_keys;
  for (int i = 1; i <= 1000; i++) {
    if (i % 2 == 0) {
      remove_keys.push_back(i);
    } else {
      insert_keys.push_back(i);
    }
  }

  for (auto key : insert_keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    out_file << "Insert key " << key << std::endl;
    std::cout << "Insert key " << key << std::endl;
    tree.Insert(index_key, rid);
    auto tree_string = tree.DrawBPlusTree();
    out_file << tree_string << std::endl;
  }
  auto tree_string = tree.DrawBPlusTree();
  std::cout << tree_string << std::endl;
  std::vector<RID> rids;
  for (auto key : insert_keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  delete bpm;
}
TEST(BPlusTreeTests, ReInsertTest) {
  std::string filename = "../../vision/b_plus_tree_visualization.txt";
  std::ofstream out_file(filename);
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);
  GenericKey<8> index_key;
  RID rid;
  std::vector<int64_t> insert_keys;
  std::vector<int64_t> remove_keys;
  for (int i = 1; i <= 20; i++) {
    if (i % 2 == 0) {
      remove_keys.push_back(i);
    } else {
      insert_keys.push_back(i);
    }
  }

  for (auto key : remove_keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    out_file << "Insert key " << key << std::endl;
    std::cout << "Insert key " << key << std::endl;
    tree.Insert(index_key, rid);
  }
  auto tree_string = tree.DrawBPlusTree();
  out_file << tree_string << std::endl;
  // 交替删除和插入键值对
  for (size_t i = 0; i < insert_keys.size(); i++) {
    if (i % 2 == 0) {
      // 先删除再插入
      int64_t key = remove_keys[i];
      index_key.SetFromInteger(key);
      std::cout << "Delete key " << key << std::endl;
      out_file << "Delete key " << key << std::endl;
      tree.Remove(index_key);
      auto tree_string = tree.DrawBPlusTree();
      out_file << tree_string << std::endl;
      out_file.flush();

      // 插入
      int64_t insert_key = insert_keys[i];
      int64_t value = insert_key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(insert_key >> 32), value);
      index_key.SetFromInteger(insert_key);
      out_file << "Insert key " << insert_key << std::endl;
      std::cout << "Insert key " << insert_key << std::endl;
      tree.Insert(index_key, rid);
      tree_string = tree.DrawBPlusTree();
      out_file << tree_string << std::endl;
      out_file.flush();

    } else {
      // 先插入再删除
      int64_t insert_key = insert_keys[i];
      int64_t value = insert_key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(insert_key >> 32), value);
      index_key.SetFromInteger(insert_key);
      out_file << "Insert key " << insert_key << std::endl;
      std::cout << "Insert key " << insert_key << std::endl;
      tree.Insert(index_key, rid);
      auto tree_string = tree.DrawBPlusTree();
      out_file << tree_string << std::endl;
      out_file.flush();
      // 删除
      int64_t key = remove_keys[i];
      index_key.SetFromInteger(key);
      std::cout << "Delete key " << key << std::endl;
      out_file << "Delete key " << key << std::endl;
      tree.Remove(index_key);
      // auto tree_string = tree.DrawBPlusTree();
      // out_file << tree_string << std::endl;
      tree_string = tree.DrawBPlusTree();
      out_file << tree_string << std::endl;
      out_file.flush();
    }
  }

  // auto tree_string = tree.DrawBPlusTree();
  // std::cout << tree_string << std::endl;
  std::vector<RID> rids;
  for (auto key : insert_keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  // 验证删除的key是否不存在
  for (auto key : remove_keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    std::cout << "Check delete key " << key << std::endl;
    bool is_present = tree.GetValue(index_key, &rids);
    ASSERT_EQ(is_present, false);
  }

  delete bpm;
}
}  // namespace bustub
