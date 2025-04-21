#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <memory>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "gtest/gtest.h"
#include "storage/page/page_guard.h"
static std::filesystem::path db_fname("test.bustub");
// The number of frames we give to the buffer pool.
const size_t FRAMES = 50;
// Note that this test assumes you are using the an LRU-K replacement policy.
const size_t K_DIST = 5;
namespace bustub {

class BufferPoolManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    disk_manager_ = std::make_shared<DiskManager>(db_fname);
    // 初始化缓冲池管理器，缓冲池大小为 50 个帧
    bpm_ = std::make_shared<BufferPoolManager>(FRAMES, disk_manager_.get(), K_DIST);
  }

  void TearDown() override {
    // 删除缓冲池管理器
  }

  std::shared_ptr<BufferPoolManager> bpm_;
  std::shared_ptr<DiskManager> disk_manager_;
};

// 测试获取write page后马上flush page
TEST_F(BufferPoolManagerTest, WritePageAndFlushTest) {
  page_id_t page_id = bpm_->NewPage();
  ASSERT_NE(page_id, INVALID_PAGE_ID);

  {
    // 获取页面
    auto page = bpm_->CheckedWritePage(page_id);
    ASSERT_NE(page.has_value(), false);
    // 写入数据到页面
    std::string data = "Test Data";
    memcpy(page->GetDataMut(), data.c_str(), data.size());
    // 刷新脏页
    bpm_->FlushPage(page_id);
    // 验证数据是否正确
    EXPECT_EQ(memcmp(page->GetData(), data.c_str(), data.size()), 0);
  }
}

// // 测试获取针数
// TEST_F(BufferPoolManagerTest, GetPinCountTest) {
//   page_id_t page_ids[50];
//   for (int &page_id : page_ids) {
//     // 创建页面
//     page_id = bpm_->NewPage();
//     ASSERT_NE(page_id, INVALID_PAGE_ID);
//     // 获取页面
//     auto page = bpm_->CheckedWritePage(page_id);
//     ASSERT_NE(page.has_value(), false);
//     // 验证针数为 1（页面被获取时针数加 1）
//     EXPECT_EQ(bpm_->GetPinCount(page_id), 1);
//     // 释放页面
//     bpm_->DeletePage(page_id);
//     // 验证已删除页面的针数为 0
//     EXPECT_EQ(bpm_->GetPinCount(page_id), 0);
//   }
// }

// // 测试删除页面后获取针数
// TEST_F(BufferPoolManagerTest, DeletePageAndGetPinCountTest) {
//   page_id_t page_id = bpm_->NewPage();
//   ASSERT_NE(page_id, INVALID_PAGE_ID);
//   // 获取页面
//   auto page = bpm_->CheckedWritePage(page_id);
//   ASSERT_NE(page.has_value(), false);
//   // 验证针数为 1
//   EXPECT_EQ(bpm_->GetPinCount(page_id), 1);
//   // 删除页面
//   bpm_->DeletePage(page_id);
//   // 验证删除后针数为 0
//   EXPECT_EQ(bpm_->GetPinCount(page_id), 0);
// }

// // 测试创建和删除页面的顺序
// TEST_F(BufferPoolManagerTest, CreateAndDeletePageTest) {
//   page_id_t page_ids[50];
//   for (int &page_id : page_ids) {
//     // 创建页面
//     page_id = bpm_->NewPage();
//     ASSERT_NE(page_id, INVALID_PAGE_ID);
//   }
//   // 删除页面
//   for (int page_id : page_ids) {
//     bpm_->DeletePage(page_id);
//   }
//   // 验证所有页面的针数为 0
//   for (int page_id : page_ids) {
//     EXPECT_EQ(bpm_->GetPinCount(page_id), 0);
//   }
// }

// // 测试页面数据内容
// TEST_F(BufferPoolManagerTest, PageDataTest) {
//   page_id_t page_id = bpm_->NewPage();
//   ASSERT_NE(page_id, INVALID_PAGE_ID);
//   // 获取页面
//   auto page = bpm_->CheckedWritePage(page_id);
//   ASSERT_NE(page.has_value(), false);
//   // 写入数据到页面
//   std::string data = "Test Data";
//   memcpy(page->GetDataMut(), data.c_str(), data.size());
//   // 验证数据是否正确
//   EXPECT_EQ(memcmp(page->GetData(), data.c_str(), data.size()), 0);
//   // 释放页面
//   page->Drop();
//   // 删除页面
//   bpm_->DeletePage(page_id);
// }

// // 测试重复获取和释放页面
// TEST_F(BufferPoolManagerTest, RepeatedFetchAndUnpinTest) {
//   page_id_t page_id = bpm_->NewPage();
//   ASSERT_NE(page_id, INVALID_PAGE_ID);
//   // 多次获取和释放页面
//   for (int i = 0; i < 10; ++i) {
//     auto page_id = bpm_->NewPage();
//     ASSERT_NE(page_id, INVALID_PAGE_ID);
//     auto page = bpm_->CheckedWritePage(page_id);
//     // 验证针数为 1
//     EXPECT_EQ(bpm_->GetPinCount(page_id), 1);
//     // 释放页面
//     page->Drop();
//     // 验证针数为 0
//     EXPECT_EQ(bpm_->GetPinCount(page_id), 0);
//   }
//   // 删除页面
//   bpm_->DeletePage(page_id);
// }

// // 测试脏页标记和刷新
// TEST_F(BufferPoolManagerTest, DirtyPageTest) {
//   page_id_t page_id = bpm_->NewPage();
//   ASSERT_NE(page_id, INVALID_PAGE_ID);
//   // 获取页面
//   auto page = bpm_->CheckedWritePage(page_id);
//   ASSERT_NE(page.has_value(), false);
//   // 标记为脏页
//   memcpy(page->GetDataMut(), "Hard", 5);
//   // 验证是否为脏页
//   EXPECT_TRUE(page->IsDirty());
//   // 刷新脏页
//   bpm_->FlushPage(page_id);
//   // 验证是否不再为脏页
//   EXPECT_FALSE(page->IsDirty());
//   // 释放页面
//   page->Drop();
//   // 删除页面
//   bpm_->DeletePage(page_id);
// }

// // 验证日志中的断言失败情况
// TEST_F(BufferPoolManagerTest, AssertionFailureTest) {
//   page_id_t page_id1 = bpm_->NewPage();
//   page_id_t page_id2 = bpm_->NewPage();
//   ASSERT_NE(page_id1, INVALID_PAGE_ID);
//   ASSERT_NE(page_id2, INVALID_PAGE_ID);
//   // 获取页面
//   auto page1 = bpm_->CheckedWritePage(page_id1);
//   auto page2 = bpm_->CheckedWritePage(page_id2);
//   ASSERT_NE(page1.has_value(), false);
//   ASSERT_NE(page2.has_value(), false);
//   // 删除页面
//   bpm_->DeletePage(page_id1);
//   bpm_->DeletePage(page_id2);
//   // 验证删除后针数为 0
//   EXPECT_EQ(bpm_->GetPinCount(page_id1), 0);
//   EXPECT_EQ(bpm_->GetPinCount(page_id2), 0);
// }

int Main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
}  // namespace bustub