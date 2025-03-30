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
#include <cstdio>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, BiggerInternalInsertTest) {
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

  std::vector<int64_t> keys = {1,  3,  5,  7,  9,  11, 13, 15, 17, 19, 21, 23, 25,
                               27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49};
  for (auto key : keys) {
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
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  delete bpm;
}
}  // namespace bustub
