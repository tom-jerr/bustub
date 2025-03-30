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
#include <cstddef>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/generic_key.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DISABLED_MixTest) {
  // 创建一个存放b+tree可视化的文件
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

  // 插入40个key，然后再删除20个key，再插入20个key
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    RID rid;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }
  auto tree_string = tree.DrawBPlusTree();
  std::cout << tree_string << std::endl;
  std::vector<int64_t> new_keys = {201, 202, 203, 204, 205, 206, 207, 208, 209, 210,
                                   211, 212, 213, 214, 215, 216, 217, 218, 219, 220};
  // std::vector<int64_t> newinsert_keys = {41, 42, 43, 44, 45, 46, 47, 48, 49, 50};
  // 交替删除和插入新的key
  // int round = 10;
  std::vector<int64_t> remove_keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
  for (size_t i = 0; i < remove_keys.size(); i++) {
    // 删除一个key
    int64_t key = remove_keys[i];

    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Delete key " << key << std::endl;
    out_file << "Delete key " << key << std::endl;
    tree.Remove(index_key);
    // 打印树的结构
    tree_string = tree.DrawBPlusTree();
    // std::cout << tree_string << std::endl;
    out_file << tree_string << std::endl;
    out_file.flush();
    // 插入一个新key
    int64_t value = new_keys[i] & 0xFFFFFFFF;
    RID rid;
    rid.Set(static_cast<int32_t>(new_keys.back() >> 32), value);
    index_key.SetFromInteger(value);
    std::cout << "Insert key " << value << std::endl;
    out_file << "Insert key " << value << std::endl;
    tree.Insert(index_key, rid);

    // 打印树的结构
    tree_string = tree.DrawBPlusTree();
    std::cout << tree_string << std::endl;
    out_file << tree_string << std::endl;
    out_file.flush();
  }

  // 验证插入的key是否存在
  std::vector<RID> rids;
  for (auto key : new_keys) {
    rids.clear();
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Check new insert key " << key << std::endl;
    bool is_present = tree.GetValue(index_key, &rids);
    ASSERT_EQ(is_present, true);
    ASSERT_EQ(rids.size(), 1);
    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetPageId(), 0);
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }
  // 验证删除的key是否不存在
  for (auto key : remove_keys) {
    rids.clear();
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Check delete key " << key << std::endl;
    bool is_present = tree.GetValue(index_key, &rids);
    ASSERT_EQ(is_present, false);
  }
  // 验证插入的key是否存在
  for (auto key : keys) {
    if (key > 50) {
      rids.clear();
      GenericKey<8> index_key;
      index_key.SetFromInteger(key);
      std::cout << "Check old insert key " << key << std::endl;
      bool is_present = tree.GetValue(index_key, &rids);
      ASSERT_EQ(is_present, true);
      ASSERT_EQ(rids.size(), 1);
      int64_t value = key & 0xFFFFFFFF;
      ASSERT_EQ(rids[0].GetPageId(), 0);
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    }
  }

  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_CrossMixTest) {
  // 创建一个存放b+tree可视化的文件
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

  // 插入40个key，然后再删除20个key，再插入20个key
  std::vector<int64_t> keys = {
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
      26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
      51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75,
      76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
  };

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    RID rid;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }
  auto tree_string = tree.DrawBPlusTree();
  std::cout << tree_string << std::endl;
  std::vector<int64_t> new_keys = {
      201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220,
      221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240,
      241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260,
      261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280,
      281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300,
  };
  std::vector<int64_t> newinsert_keys = {
      201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220,
      221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240,
      241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260,
      261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280,
      281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 297, 298, 299, 300,
  };
  // std::vector<int64_t> newinsert_keys = {41, 42, 43, 44, 45, 46, 47, 48, 49, 50};
  // 交替删除和插入新的key
  // int round = 10;
  std::vector<int64_t> remove_keys = {1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                      21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
                                      41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
                                      61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
                                      81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100};
  // 如果是奇数轮，就先删除1个，再插入1个，如果是偶数轮就先插入两个，再删除2个
  int round = 0;
  while (!new_keys.empty() && !remove_keys.empty()) {
    if (round % 2 == 0) {
      if (new_keys.size() < 1 || remove_keys.size() < 1) {
        break;
      }
      // 删除一个key
      int64_t key = remove_keys.back();
      remove_keys.pop_back();
      GenericKey<8> index_key;
      index_key.SetFromInteger(key);
      std::cout << "Delete key " << key << std::endl;
      out_file << "Delete key " << key << std::endl;
      tree.Remove(index_key);
      // 打印树的结构
      tree_string = tree.DrawBPlusTree();
      // std::cout << tree_string << std::endl;
      out_file << tree_string << std::endl;
      out_file.flush();

      // 插入一个新key
      int64_t new_key = new_keys.back();
      int64_t value = new_keys.back() & 0xFFFFFFFF;
      new_keys.pop_back();
      RID rid;
      rid.Set(static_cast<int32_t>(new_key >> 32), value);
      // GenericKey<8> index_key;
      index_key.SetFromInteger(value);
      std::cout << "Insert key " << value << std::endl;
      out_file << "Insert key " << value << std::endl;
      tree.Insert(index_key, rid);

      // 打印树的结构
      tree_string = tree.DrawBPlusTree();
      std::cout << tree_string << std::endl;
      out_file << tree_string << std::endl;
      out_file.flush();
    } else {
      if (new_keys.size() < 2 || remove_keys.size() < 2) {
        round++;
        continue;
      }
      // 插入两个新key
      int64_t value1 = new_keys.back() & 0xFFFFFFFF;
      new_keys.pop_back();
      int64_t value2 = new_keys.back() & 0xFFFFFFFF;
      new_keys.pop_back();
      RID rid1, rid2;
      rid1.Set(static_cast<int32_t>(value1 >> 32), value1);
      rid2.Set(static_cast<int32_t>(value2 >> 32), value2);
      GenericKey<8> index_key1;
      GenericKey<8> index_key2;
      index_key1.SetFromInteger(value1);
      index_key2.SetFromInteger(value2);
      std::cout << "Insert key " << value1 << " and " << value2 << std::endl;
      out_file << "Insert key " << value1 << " and " << value2 << std::endl;
      tree.Insert(index_key1, rid1);
      tree.Insert(index_key2, rid2);

      // 删除两个key
      int64_t key1 = remove_keys.back();
      remove_keys.pop_back();
      int64_t key2 = remove_keys.back();
      remove_keys.pop_back();
      // GenericKey<8> index_key1;
      index_key1.SetFromInteger(key1);
      // GenericKey<8> index_key2;
      index_key2.SetFromInteger(key2);
      std::cout << "Delete key " << key1 << " and " << key2 << std::endl;
      out_file << "Delete key " << key1 << " and " << key2 << std::endl;
      tree.Remove(index_key1);
      tree.Remove(index_key2);
      // 打印树的结构
      tree_string = tree.DrawBPlusTree();
      // std::cout << tree_string << std::endl;
      out_file << tree_string << std::endl;
      out_file.flush();
    }
    round++;
  }
  while (!new_keys.empty()) {
    // 插入一个新key
    int64_t value = new_keys.back() & 0xFFFFFFFF;
    new_keys.pop_back();
    RID rid;
    rid.Set(static_cast<int32_t>(new_keys.back() >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(value);
    std::cout << "Insert key " << value << std::endl;
    out_file << "Insert key " << value << std::endl;
    tree.Insert(index_key, rid);

    // 打印树的结构
    tree_string = tree.DrawBPlusTree();
    std::cout << tree_string << std::endl;
    out_file << tree_string << std::endl;
    out_file.flush();
  }
  while (!remove_keys.empty()) {
    // 删除一个key
    int64_t key = remove_keys.back();
    remove_keys.pop_back();
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Delete key " << key << std::endl;
    out_file << "Delete key " << key << std::endl;
    tree.Remove(index_key);
    // 打印树的结构
    tree_string = tree.DrawBPlusTree();
    // std::cout << tree_string << std::endl;
    out_file << tree_string << std::endl;
    out_file.flush();
  }
  // 验证插入的key是否存在
  std::vector<RID> rids;
  for (auto key : newinsert_keys) {
    rids.clear();
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Check new insert key " << key << std::endl;
    bool is_present = tree.GetValue(index_key, &rids);
    ASSERT_EQ(is_present, true);
    ASSERT_EQ(rids.size(), 1);
    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetPageId(), 0);
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }
  // 验证删除的key是否不存在
  // for (auto key : remove_keys) {
  //   rids.clear();
  //   GenericKey<8> index_key;
  //   index_key.SetFromInteger(key);
  //   std::cout << "Check delete key " << key << std::endl;
  //   bool is_present = tree.GetValue(index_key, &rids);
  //   ASSERT_EQ(is_present, false);
  // }
  // 验证插入的key是否存在
  for (auto key : keys) {
    rids.clear();
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Check old insert key " << key << std::endl;
    bool is_present = tree.GetValue(index_key, &rids);
    ASSERT_EQ(is_present, false);
  }
  delete bpm;
}
TEST(BPlusTreeTests, MultiThreadMixTest) {
  // 创建一个存放b+tree可视化的文件
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

  // 插入40个key，然后再删除20个key，再插入20个key
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    RID rid;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }
  auto tree_string = tree.DrawBPlusTree();
  std::cout << tree_string << std::endl;

  // 一个线程删除20个key，另一个线程插入20个key
  std::vector<int64_t> remove_keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
  std::vector<int64_t> insert_keys = {61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80};
  // 插入和删除分别用5个线程来执行
  std::vector<std::thread> delete_threads;
  std::vector<std::thread> insert_threads;
  for (size_t i = 0; i < 2; i++) {
    delete_threads.emplace_back([&]() {
      // 计算需要删除的范围
      size_t start = i * (remove_keys.size() / 2);
      size_t end = (i + 1) * (remove_keys.size() / 2);
      // if (i == 1) {
      //   end = remove_keys.size();
      // }
      std::vector<int64_t> delete_keys(remove_keys.begin() + start, remove_keys.begin() + end);
      for (auto key : delete_keys) {
        GenericKey<8> index_key;
        index_key.SetFromInteger(key);
        std::cout << "Delete key " << key << std::endl;
        out_file << "Delete key " << key << std::endl;
        tree.Remove(index_key);
        auto tree_string = tree.DrawBPlusTree();
        out_file << tree_string << std::endl;
        out_file.flush();
      }
    });
    insert_threads.emplace_back([&]() {
      size_t start = i * (insert_keys.size() / 2);
      size_t end = (i + 1) * (insert_keys.size() / 2);
      // if (i == 1) {
      //   end = insert_keys.size();
      // }
      std::vector<int64_t> newinsert_keys(insert_keys.begin() + start, insert_keys.begin() + end);
      for (auto key : newinsert_keys) {
        int64_t value = key & 0xFFFFFFFF;
        RID rid;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        GenericKey<8> index_key;
        index_key.SetFromInteger(key);
        out_file << "Insert key " << key << std::endl;
        std::cout << "Insert key " << key << std::endl;
        tree.Insert(index_key, rid);
        auto tree_string = tree.DrawBPlusTree();
        out_file << tree_string << std::endl;
        out_file.flush();
      }
    });
  }

  // 等待所有线程完成
  for (auto &thread : delete_threads) {
    thread.join();
  }
  for (auto &thread : insert_threads) {
    thread.join();
  }

  // 打印树的结构
  tree_string = tree.DrawBPlusTree();
  std::cout << tree_string << std::endl;

  // 验证插入的key是否存在
  std::vector<RID> rids;
  for (auto key : insert_keys) {
    rids.clear();
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Check new insert key " << key << std::endl;
    bool is_present = tree.GetValue(index_key, &rids);
    ASSERT_EQ(is_present, true);
    ASSERT_EQ(rids.size(), 1);
    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetPageId(), 0);
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }
  // 验证删除的key是否不存在
  for (auto key : remove_keys) {
    rids.clear();
    GenericKey<8> index_key;
    index_key.SetFromInteger(key);
    std::cout << "Check delete key " << key << std::endl;
    bool is_present = tree.GetValue(index_key, &rids);
    ASSERT_EQ(is_present, false);
  }
  // 验证插入的key是否存在
  // for (auto key : keys) {
  //   if (key > 20) {
  //     rids.clear();
  //     GenericKey<8> index_key;
  //     index_key.SetFromInteger(key);
  //     std::cout << "Check old insert key " << key << std::endl;
  //     bool is_present = tree.GetValue(index_key, &rids);
  //     ASSERT_EQ(is_present, true);
  //     ASSERT_EQ(rids.size(), 1);
  //     int64_t value = key & 0xFFFFFFFF;
  //     ASSERT_EQ(rids[0].GetPageId(), 0);
  //     ASSERT_EQ(rids[0].GetSlotNum(), value);
  //   }
  // }
  delete bpm;
}
}  // namespace bustub