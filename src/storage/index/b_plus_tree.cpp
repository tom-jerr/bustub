#include "storage/index/b_plus_tree.h"
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"
namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto root_page_id = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return true;
  }
  auto page_guard = bpm_->ReadPage(root_page_id);
  return page_guard.As<BPlusTreePage>()->GetSize() == 0;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptiFindLeafPage(Context *ctx, Operation op, const KeyType &key, const KeyComparator &comparator,
                                      int leftmost, int rightmost) -> page_id_t {
  BUSTUB_ASSERT(ctx != nullptr, "context is nullptr");
  page_id_t leaf_pid = INVALID_PAGE_ID;
  if (op == Operation::SEARCH) {
    leaf_pid = FindSearchLeafPage(ctx, key, comparator, leftmost, rightmost);
  } else if (op == Operation::INSERT || op == Operation::REMOVE) {
    leaf_pid = OptiFindUpdateLeafPage(ctx, key, comparator, leftmost, rightmost);
  } else {
    throw Exception("Unknown find leaf page operation");
  }
  // BUSTUB_ASSERT(leaf_pid > 0, "leaf page id is invalid");
  return leaf_pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptiFindUpdateLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator,
                                            int leftmost, int rightmost) -> page_id_t {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto root_page_id = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  ctx->read_set_.emplace_back(std::move(header_page_guard));
  // get the root page
  auto root_page_guard = bpm_->ReadPage(root_page_id);
  ctx->read_set_.emplace_back(std::move(root_page_guard));
  // header_page_guard.Drop();
  auto *root_page = ctx->read_set_.back().As<BPlusTreePage>();
  // Find the leaf page that contains the input key.
  // 1. root page is leaf page
  if (root_page->IsLeafPage()) {
    ctx->read_set_.pop_back();
    ctx->write_set_.emplace_back(bpm_->WritePage(root_page_id));
    return root_page_id;
  }
  // 2. root page is internal page
  // 2.1 find the leaf page that contains the input key
  auto *internal_page = const_cast<InternalPage *>(ctx->read_set_.back().As<InternalPage>());
  auto child_pid = internal_page->Lookup(key, comparator, leftmost, rightmost);

  while (true) {
    auto child_page_guard = bpm_->ReadPage(child_pid);
    auto child_page = child_page_guard.template As<BPlusTreePage>();
    if (child_page->IsLeafPage()) {
      child_page_guard.Drop();
      ctx->write_set_.emplace_back(bpm_->WritePage(child_pid));
      return child_pid;
    }
    // LOG_DEBUG("child page id %d", child_pid);
    ctx->read_set_.emplace_back(std::move(child_page_guard));
    ctx->read_set_.pop_front();
    // ctx->read_set_.erase(ctx->read_set_.end() - 2);
    child_page = ctx->read_set_.back().template As<BPlusTreePage>();

    auto *child_internal_page = const_cast<InternalPage *>(ctx->read_set_.back().template As<InternalPage>());
    child_pid = child_internal_page->Lookup(key, comparator, leftmost, rightmost);
  }
  return child_pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(Context *ctx, Operation op, const KeyType &key, const KeyComparator &comparator,
                                  int leftmost, int rightmost) -> page_id_t {
  BUSTUB_ASSERT(ctx != nullptr, "context is nullptr");
  page_id_t leaf_pid = INVALID_PAGE_ID;
  if (op == Operation::SEARCH) {
    leaf_pid = FindSearchLeafPage(ctx, key, comparator, leftmost, rightmost);
  } else if (op == Operation::INSERT) {
    leaf_pid = FindInsertLeafPage(ctx, key, comparator, leftmost, rightmost);
  } else if (op == Operation::REMOVE) {
    leaf_pid = FindRemoveLeafPage(ctx, key, comparator, leftmost, rightmost);
  } else {
    throw Exception("Unknown find leaf page operation");
  }
  // BUSTUB_ASSERT(leaf_pid > 0, "leaf page id is invalid");
  return leaf_pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindSearchLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator, int leftmost,
                                        int rightmost) -> page_id_t {
  // LOG_DEBUG("get value: %ld", key.ToString());

  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto root_page_id = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  // get the root page
  auto root_page_guard = bpm_->ReadPage(root_page_id);
  ctx->read_set_.emplace_back(std::move(root_page_guard));
  header_page_guard.Drop();
  auto *root_page = ctx->read_set_.back().As<BPlusTreePage>();
  // Find the leaf page that contains the input key.
  // 1. root page is leaf page
  if (root_page->IsLeafPage()) {
    return root_page_id;
  }
  // 2. root page is internal page
  // 2.1 find the leaf page that contains the input key
  auto *internal_page = const_cast<InternalPage *>(ctx->read_set_.back().As<InternalPage>());
  auto child_pid = internal_page->Lookup(key, comparator, leftmost, rightmost);

  while (true) {
    auto child_page_guard = bpm_->ReadPage(child_pid);
    // LOG_DEBUG("child page id %d", child_pid);
    ctx->read_set_.emplace_back(std::move(child_page_guard));
    ctx->read_set_.pop_front();
    auto child_page = ctx->read_set_.back().template As<BPlusTreePage>();
    if (child_page->IsLeafPage()) {
      return child_pid;
    }
    auto *child_internal_page = const_cast<InternalPage *>(ctx->read_set_.back().template As<InternalPage>());
    child_pid = child_internal_page->Lookup(key, comparator, leftmost, rightmost);
  }
  return child_pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInsertLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator, int leftmost,
                                        int rightmost) -> page_id_t {
  // LOG_DEBUG("FindInsertLeafPage: key %ld", key.ToString());
  BUSTUB_ASSERT(ctx->header_page_.has_value(), "header page is nullptr");

  auto root_page_id = ctx->header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  // BUSTUB_ASSERT(root_page_id != INVALID_PAGE_ID, "root page id is invalid");
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  auto root_page_guard = bpm_->WritePage(root_page_id);
  ctx->root_page_id_ = root_page_id;
  ctx->write_set_.emplace_back(std::move(root_page_guard));

  auto *root_page = ctx->write_set_.back().AsMut<BPlusTreePage>();
  if (root_page->IsLeafPage() && root_page->GetSize() < root_page->GetMaxSize() - 1) {
    ctx->ReleaseWriteLatchExceptLast();
    return root_page_id;
  }
  if (!root_page->IsLeafPage() && root_page->GetSize() < root_page->GetMaxSize()) {
    ctx->ReleaseWriteLatchExceptLast();
  }
  // LOG_DEBUG("root page id %d", root_page_id);
  while (!ctx->write_set_.back().As<BPlusTreePage>()->IsLeafPage()) {
    auto *internal_page = ctx->write_set_.back().AsMut<InternalPage>();
    page_id_t child_pid = internal_page->Lookup(key, comparator, leftmost, rightmost);
    // LOG_DEBUG("child page id %d", child_pid);
    BUSTUB_ASSERT(child_pid > 0, "child page id is invalid");
    auto child_page_guard = bpm_->WritePage(child_pid);  // child hold write latch
    auto child_page = child_page_guard.As<InternalPage>();
    ctx->write_set_.emplace_back(std::move(child_page_guard));
    child_page = ctx->write_set_.back().AsMut<InternalPage>();

    // 可以提前释放上面的锁
    if ((child_page->IsLeafPage() && child_page->GetSize() < child_page->GetMaxSize() - 1) ||
        (!child_page->IsLeafPage() && child_page->GetSize() < child_page->GetMaxSize())) {
      ctx->ReleaseWriteLatchExceptLast();
    }
  }

  return ctx->write_set_.back().GetPageId();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindRemoveLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator, int leftmost,
                                        int rightmost) -> page_id_t {
  // LOG_DEBUG("FindRemoveLeafPage: key %ld", key.ToString());
  BUSTUB_ASSERT(ctx->header_page_.has_value(), "header page is nullptr");
  auto root_page_id = ctx->header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  auto root_page_guard = bpm_->WritePage(root_page_id);
  ctx->root_page_id_ = root_page_id;
  ctx->write_set_.emplace_back(std::move(root_page_guard));  // hold root page

  auto *root_page = ctx->write_set_.back().AsMut<BPlusTreePage>();
  if (root_page->IsLeafPage() && root_page->GetSize() > root_page->GetMinSize()) {
    ctx->ReleaseWriteLatchExceptLast();
    return root_page_id;
  }

  while (!ctx->write_set_.back().As<BPlusTreePage>()->IsLeafPage()) {
    auto *internal_page = ctx->write_set_.back().AsMut<InternalPage>();
    page_id_t child_pid = internal_page->Lookup(key, comparator, leftmost, rightmost);
    // LOG_DEBUG("child page id %d", child_pid);
    BUSTUB_ASSERT(child_pid > 0, "child page id is invalid");
    auto child_page_guard = bpm_->WritePage(child_pid);  // child hold write latch
    auto child_page = child_page_guard.As<InternalPage>();

    ctx->write_set_.emplace_back(std::move(child_page_guard));
    child_page = ctx->write_set_.back().AsMut<InternalPage>();
    // 可以提前释放上面的锁
    if (child_page->GetSize() > child_page->GetMinSize()) {
      ctx->ReleaseWriteLatchExceptLast();
    }
  }

  return ctx->write_set_.back().GetPageId();
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  // Declaration of context instance.
  Context ctx;
  auto leaf_pid = FindLeafPage(&ctx, Operation::SEARCH, key, comparator_, false, false);
  if (leaf_pid == INVALID_PAGE_ID) {
    LOG_DEBUG("get value: %ld, message: leaf page id is invalid", key.ToString());
    return false;
  }
  BUSTUB_ASSERT(leaf_pid > 0, "leaf page id is invalid");
  // find the key in the leaf page
  auto leaf_page = ctx.read_set_.back().template As<LeafPage>();  // need to use template
  return leaf_page->Lookup(key, result, comparator_);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpdateRoot(Context *ctx, page_id_t root_page_id) -> bool {
  BUSTUB_ASSERT(header_page_id_ != INVALID_PAGE_ID, "header page id is invalid");
  auto header_page = ctx->header_page_.value().AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = root_page_id;
  ctx->root_page_id_ = root_page_id;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty(Context *ctx) const -> bool {
  BUSTUB_ASSERT(ctx->header_page_.has_value(), "header page is nullptr");
  auto root_page_id = ctx->header_page_.value().As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return true;
  }
  auto page_guard = bpm_->WritePage(root_page_id);
  // ctx->write_set_.emplace_back(std::move(page_guard));
  auto root_page = page_guard.As<BPlusTreePage>();
  ctx->root_page_id_ = root_page_id;
  return root_page->GetSize() == 0;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // Declaration of context instance.
  Context ctx;
  return OptiInsert(&ctx, key, value);
  // ctx.header_page_ = bpm_->WritePage(header_page_id_);
  // /**
  //  * @brief notice: if IsEmpty and StartNewTree not hold the header_page_ lock all the time, there will be data race
  //  * between two thread.
  //  *
  //  */
  // if (IsEmpty(&ctx)) {
  //   return StartNewTree(&ctx, key, value);
  // }

  // auto leaf_page_id = FindLeafPage(&ctx, Operation::INSERT, key, comparator_, false, false);
  // if (leaf_page_id == INVALID_PAGE_ID) {
  //   LOG_DEBUG("insert: %ld, message: leaf page id is invalid", key.ToString());
  //   return false;
  // }
  // // 如果找到叶子节点，一定确保ctx.write_set_不为空
  // BUSTUB_ASSERT(!ctx.write_set_.empty(), "write set is empty");
  // auto leaf_page = ctx.write_set_.back().template AsMut<LeafPage>();
  // auto before_insert_size = leaf_page->GetSize();
  // leaf_page->Insert(key, value, comparator_);
  // auto new_size = leaf_page->GetSize();
  // // check leaf node whether it is full or not
  // // 1. duplicate key
  // if (before_insert_size == new_size) {
  //   return false;
  // }
  // // 2. leaf node is  not full
  // if (new_size < leaf_page->GetMaxSize()) {
  //   return true;
  // }
  // // 3. leaf node is full
  // auto ret = SplitLeaf(&ctx);
  // auto new_key = ret.new_key_;
  // auto new_page_id = ret.new_page_id_;

  // InsertIntoParent(&ctx, leaf_page_id, new_key, new_page_id, 0);

  // return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptiInsert(Context *ctx, const KeyType &key, const ValueType &value) -> bool {
  ctx->header_page_ = bpm_->WritePage(header_page_id_);
  if (IsEmpty(ctx)) {
    return StartNewTree(ctx, key, value);
  }
  ctx->ReleaseAllLatch();
  auto leaf_page_id = OptiFindLeafPage(ctx, Operation::INSERT, key, comparator_, false, false);
  if (leaf_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("insert: %ld, message: leaf page id is invalid", key.ToString());
    return false;
  }
  // 此时ctx.write_set_中保存这leaf_page的写锁
  BUSTUB_ASSERT(ctx->write_set_.size() == 1, "optifindleafpage write set size is not 1");
  auto leaf_page = ctx->write_set_.back().AsMut<LeafPage>();
  if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
    auto before_insert_size = leaf_page->GetSize();
    leaf_page->Insert(key, value, comparator_);
    auto new_size = leaf_page->GetSize();
    // check leaf node whether it is full or not
    // 1. duplicate key
    return before_insert_size != new_size;
  }
  ctx->ReleaseAllLatch();
  ctx->header_page_ = bpm_->WritePage(header_page_id_);

  auto new_leaf_page_id = FindLeafPage(ctx, Operation::INSERT, key, comparator_, false, false);
  if (new_leaf_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("insert: %ld, message: leaf page id is invalid", key.ToString());
    return false;
  }
  // 如果找到叶子节点，一定确保ctx.write_set_不为空
  BUSTUB_ASSERT(!ctx->write_set_.empty(), "write set is empty");
  auto new_leaf_page = ctx->write_set_.back().template AsMut<LeafPage>();

  auto before_insert_size = new_leaf_page->GetSize();
  new_leaf_page->Insert(key, value, comparator_);
  auto new_size = new_leaf_page->GetSize();
  // check leaf node whether it is full or not
  // 1. duplicate key
  if (before_insert_size == new_size) {
    return false;
  }
  // 2. leaf node is  not full
  if (new_size < new_leaf_page->GetMaxSize()) {
    return true;
  }
  // 3. leaf node is full
  auto ret = SplitLeaf(ctx);
  auto new_key = ret.new_key_;
  auto new_page_id = ret.new_page_id_;

  InsertIntoParent(ctx, new_leaf_page_id, new_key, new_page_id, 0);

  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(Context *ctx, const KeyType &key, const ValueType &value) -> bool {
  // BUSTUB_ASSERT(ctx->header_page_.has_value(), "header page is nullptr");

  if (ctx->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t leaf_page_id = bpm_->NewPage();
    auto leaf_page_guard = bpm_->WritePage(leaf_page_id);
    auto *leaf_page = leaf_page_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->Insert(key, value, comparator_);
    UpdateRoot(ctx, leaf_page_id);
  } else {
    auto root_page_guard = bpm_->WritePage(ctx->root_page_id_);
    auto *root_page = root_page_guard.AsMut<LeafPage>();
    root_page->Insert(key, value, comparator_);
    UpdateRoot(ctx, ctx->root_page_id_);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(Context *ctx) -> SplitRet {
  BUSTUB_ASSERT(!ctx->write_set_.empty(), "write set is empty");
  page_id_t split_page_id = bpm_->NewPage();
  if (split_page_id == INVALID_PAGE_ID) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
  }
  auto split_page_guard = bpm_->WritePage(split_page_id);
  auto *split_page = split_page_guard.AsMut<LeafPage>();
  split_page->Init(leaf_max_size_);
  // ctx->write_set_.emplace_back(std::move(split_page_guard));
  // 申请new page后需要init
  auto old_page = ctx->write_set_.back().AsMut<LeafPage>();
  auto new_page = split_page_guard.AsMut<LeafPage>();

  int split_point = old_page->GetSize() / 2;
  auto new_key = old_page->KeyAt(split_point);

  old_page->MoveHalfTo(new_page);

  new_page->SetNextPageId(old_page->GetNextPageId());
  old_page->SetNextPageId(split_page_id);
  return {split_page_id, new_key};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(Context *ctx, int recursive_level) -> SplitRet {
  BUSTUB_ASSERT(!ctx->write_set_.empty(), "write set is empty");
  page_id_t split_page_id = bpm_->NewPage();
  if (split_page_id == INVALID_PAGE_ID) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
  }
  auto split_page_guard = bpm_->WritePage(split_page_id);
  auto *split_page = split_page_guard.AsMut<InternalPage>();
  split_page->Init(internal_max_size_);

  // 申请new page后需要init
  auto *old_page = ctx->write_set_.rbegin()[recursive_level + 1].AsMut<InternalPage>();
  auto *new_page = split_page_guard.AsMut<InternalPage>();
  auto new_page_id = split_page_guard.GetPageId();
  int split_point = old_page->GetSize() / 2;
  auto new_key = old_page->KeyAt(split_point);
  old_page->MoveHalfValueTo(new_page);
  return {new_page_id, new_key};
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(Context *ctx, page_id_t old_node_id, const KeyType &key, page_id_t new_node_id,
                                      int recursive_level) {
  // 这里与IsEmpty并发会造成死锁
  // if (ctx->root_page_id_ == INVALID_PAGE_ID) {
  //   // root page id is invalid
  //   auto header_page_guard = bpm_->WritePage(header_page_id_);
  //   auto *header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  //   ctx->root_page_id_ = header_page->root_page_id_;
  // }
  if (ctx->IsRootPage(old_node_id)) {
    // root page
    auto new_root_page_id = bpm_->NewPage();
    auto new_root_page_guard = bpm_->WritePage(new_root_page_id);
    auto *new_root_page = new_root_page_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);
    // ctx->write_set_.emplace_back(std::move(new_root_page_guard));
    // ctx->write_set_.push_front(std::move(new_root_page_guard));
    // ctx->print();
    auto *root_page = new_root_page_guard.AsMut<InternalPage>();
    root_page->PopulateNewRoot(old_node_id, key, new_node_id);
    UpdateRoot(ctx, new_root_page_id);
    return;
  }

  // get parent page
  // ctx->print();
  auto parent_page_id = ctx->write_set_.rbegin()[recursive_level + 1].GetPageId();
  auto *parent_page = ctx->write_set_.rbegin()[recursive_level + 1].AsMut<InternalPage>();
  auto old_index = parent_page->ValueIndex(old_node_id);
  if (parent_page->Insert(key, new_node_id, old_index, comparator_)) {
    return;
  }

  // parent page is full, first split parent page
  auto ret = SplitInternal(ctx, recursive_level);
  auto new_page_id = ret.new_page_id_;
  auto new_key = ret.new_key_;
  // auto new_page_id = ctx->write_set_.rbegin()[2 * recursive_level + 2].GetPageId();
  // auto new_page = ctx->write_set_.rbegin()[2 * recursive_level + 2].AsMut<InternalPage>();
  // auto new_key = new_page->KeyAt(0);
  // std::cout << "new key: " << new_key << std::endl;
  InsertIntoParent(ctx, parent_page_id, new_key, new_page_id, recursive_level + 1);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  Context ctx;
  OptiRemove(&ctx, key);
  // ctx.header_page_ = bpm_->WritePage(header_page_id_);

  // // (void)ctx;
  // if (IsEmpty(&ctx)) {
  //   return;
  // }
  // auto leaf_page_id = FindLeafPage(&ctx, Operation::REMOVE, key, comparator_, false, false);
  // if (leaf_page_id == INVALID_PAGE_ID) {
  //   return;
  // }
  // // coalesce or redistribute
  // DeleteEntry(&ctx, key, -1, leaf_page_id, 0);
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::OptiRemove(Context *ctx, const KeyType &key) {
  ctx->header_page_ = bpm_->WritePage(header_page_id_);
  if (IsEmpty(ctx)) {
    return;
  }
  ctx->ReleaseAllLatch();
  auto leaf_page_id = OptiFindLeafPage(ctx, Operation::REMOVE, key, comparator_, false, false);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return;
  }

  BUSTUB_ASSERT(ctx->write_set_.size() == 1, "optifindleafpage write set size is not 1");
  auto leaf_page = ctx->write_set_.back().AsMut<LeafPage>();
  if (leaf_page->GetSize() > leaf_page->GetMinSize()) {
    leaf_page->RemoveAndDeleteRecord(key, comparator_);
    return;
  }
  ctx->ReleaseAllLatch();
  ctx->header_page_ = bpm_->WritePage(header_page_id_);
  auto new_leaf_page_id = FindLeafPage(ctx, Operation::REMOVE, key, comparator_, false, false);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return;
  }

  DeleteEntry(ctx, key, -1, new_leaf_page_id, 0);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntry(Context *ctx, const KeyType &key, int delete_index, page_id_t current_page_id,
                                 int recursive_level) -> bool {
  if (recursive_level == 0) {
    auto *leaf_page = ctx->write_set_.back().template AsMut<LeafPage>();
    // remove key from leaf page
    if (!leaf_page->RemoveAndDeleteRecord(key, comparator_)) {
      return true;
    }
    // check leaf node whether it is underflow or not
    if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
      return true;
    }
  } else {
    auto *internal_page = ctx->write_set_.rbegin()[recursive_level].AsMut<InternalPage>();
    // remove key from internal page
    internal_page->Remove(key, delete_index, comparator_);
    // check internal node whether it is underflow or not
    if (internal_page->GetSize() >= internal_page->GetMinSize()) {
      return true;
    }
  }

  if (ctx->IsRootPage(current_page_id)) {
    AdjustRoot(ctx);
    return true;
  }
  // get parent page
  // auto parent_page_id = ctx->write_set_.rbegin()[1].GetPageId();
  auto *parent_page = ctx->write_set_.rbegin()[recursive_level + 1].AsMut<InternalPage>();
  // auto parent_page = parent_page_guard.AsMut<InternalPage>();
  if (parent_page->GetSize() == 1) {
    return true;
  }
  // find the index of old page in parent page
  auto old_page_index = parent_page->ValueIndex(current_page_id);
  BUSTUB_ASSERT(old_page_index >= 0 && old_page_index < parent_page->GetSize(), "old page index is invalid");
  auto sibling_index = old_page_index == 0 ? 1 : old_page_index - 1;
  auto key_index = old_page_index == 0 ? 1 : old_page_index;
  auto parent_key = parent_page->KeyAt(key_index);
  bool redistribute = Redistribute(ctx, old_page_index, sibling_index, recursive_level, parent_key);

  [[maybe_unused]] bool coalesec = false;
  if (!redistribute) {
    if (old_page_index != 0) {
      coalesec = Coalesce(ctx, sibling_index, true, recursive_level, parent_key);
    } else {
      coalesec = Coalesce(ctx, sibling_index, false, recursive_level, parent_key);
    }
  }
  return true;
}
// borrow from sibling
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Redistribute(Context *ctx, int old_index, int sibling_index, int recursive_level,
                                  const KeyType &parent_key) -> bool {
  auto *parent_page = ctx->write_set_.rbegin()[recursive_level + 1].AsMut<InternalPage>();

  auto *old_page = ctx->write_set_.rbegin()[recursive_level].AsMut<BPlusTreePage>();

  auto neighbor_page_id = parent_page->ValueAt(sibling_index);
  BUSTUB_ASSERT(neighbor_page_id > 0, "message: neighbor page id is invalid");
  auto neighbor_page_guard = bpm_->WritePage(neighbor_page_id);
  auto *neighbor_page = neighbor_page_guard.template AsMut<BPlusTreePage>();

  if (neighbor_page->GetSize() <= neighbor_page->GetMinSize()) {
    return false;
  }
  if (old_page->IsLeafPage()) {
    auto *leaf_page = ctx->write_set_.rbegin()[recursive_level].AsMut<LeafPage>();
    auto *neighbor_leaf_page = neighbor_page_guard.template AsMut<LeafPage>();
    BUSTUB_ASSERT(old_index != sibling_index, "old index is equal to sibling index");

    if (old_index > sibling_index) {
      neighbor_leaf_page->MoveLastToFrontOf(leaf_page);
      parent_page->SetKeyAt(old_index, leaf_page->KeyAt(0));
    } else {
      neighbor_leaf_page->MoveFirstToEndOf(leaf_page);
      parent_page->SetKeyAt(old_index + 1, neighbor_leaf_page->KeyAt(0));
    }
  } else {
    auto *internal_page = ctx->write_set_.rbegin()[recursive_level].AsMut<InternalPage>();
    auto *neighbor_internal_page = neighbor_page_guard.template AsMut<InternalPage>();
    BUSTUB_ASSERT(old_index != sibling_index, "old index is equal to sibling index");
    if (old_index < sibling_index) {
      // 插入node节点的父节点的key，父节点得到next的第一个key
      // ！！！key_array_的第一个key是Invalid
      parent_page->SetKeyAt(old_index + 1, neighbor_internal_page->KeyAt(1));
      internal_page->InsertNodeAfter(parent_key, neighbor_internal_page->ValueAt(0));
      neighbor_internal_page->MoveKVForward();

    } else {
      // 插入node节点的父节点的key，父节点得到prev的最后一个key
      parent_page->SetKeyAt(old_index, neighbor_internal_page->KeyAt(neighbor_internal_page->GetSize() - 1));
      internal_page->InsertNodeBefore(parent_key,
                                      neighbor_internal_page->ValueAt(neighbor_internal_page->GetSize() - 1));
      neighbor_internal_page->EraseLastKV();
    }
  }
  return true;
}

// coalesce is to merge old page and sibling page
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Coalesce(Context *ctx, int sibling_index, bool sibling_is_predecessor, int recursive_level,
                              const KeyType &parent_key) -> bool {
  page_id_t neighbor_page_id = INVALID_PAGE_ID;

  auto parent_page_id = ctx->write_set_.rbegin()[recursive_level + 1].GetPageId();

  auto *parent_page = ctx->write_set_.rbegin()[recursive_level + 1].AsMut<InternalPage>();

  auto *old_page = ctx->write_set_.rbegin()[recursive_level].AsMut<BPlusTreePage>();
  neighbor_page_id = parent_page->ValueAt(sibling_index);
  auto neighbor_page_guard = bpm_->WritePage(neighbor_page_id);
  auto *neighbor_page = neighbor_page_guard.AsMut<BPlusTreePage>();
  if ((neighbor_page->GetSize() + old_page->GetSize()) > old_page->GetMaxSize()) {
    return false;
  }
  if (old_page->IsLeafPage()) {
    auto *leaf_page = ctx->write_set_.rbegin()[recursive_level].AsMut<LeafPage>();
    auto *neighbor_leaf_page = neighbor_page_guard.AsMut<LeafPage>();

    // merge old page and neighbor page
    if (!sibling_is_predecessor) {
      leaf_page->InsertAllNodeAfterFrom(neighbor_leaf_page);
      leaf_page->SetNextPageId(neighbor_leaf_page->GetNextPageId());
    } else {
      neighbor_leaf_page->InsertAllNodeAfterFrom(leaf_page);
      neighbor_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    }

    // neighbor_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  } else {
    auto *internal_page = ctx->write_set_.rbegin()[recursive_level].AsMut<InternalPage>();
    auto *neighbor_internal_page = neighbor_page_guard.AsMut<InternalPage>();
    // merge old page and neighbor page
    if (!sibling_is_predecessor) {
      internal_page->InsertKeyAfter(parent_key, comparator_);
      internal_page->InsertAllNodeAfterFrom(neighbor_internal_page, comparator_);
    } else {
      neighbor_internal_page->InsertKeyAfter(parent_key, comparator_);
      neighbor_internal_page->InsertAllNodeAfterFrom(internal_page, comparator_);
    }
  }

  auto delete_index = sibling_is_predecessor
                          ? parent_page->ValueIndex(ctx->write_set_.rbegin()[recursive_level].GetPageId())
                          : parent_page->ValueIndex(neighbor_page_id);
  DeleteEntry(ctx, parent_key, delete_index, parent_page_id, recursive_level + 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AdjustRoot(Context *ctx) {
  BUSTUB_ASSERT(ctx->header_page_.has_value(), "header page is nullptr");
  [[maybe_unused]] auto old_root_page_id = ctx->header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_;

  auto *old_root_page = ctx->write_set_.front().AsMut<BPlusTreePage>();
  BUSTUB_ASSERT(old_root_page_id == ctx->write_set_.front().GetPageId(), "old root page id is invalid");

  if (old_root_page->IsLeafPage()) {
    if (old_root_page->GetSize() == 0) {
      UpdateRoot(ctx, INVALID_PAGE_ID);
      // bpm_->DeletePage(old_root_page_id);
    }
    return;
  }
  // 如果根节点只有一个child，那么将child作为新的根节点
  if (old_root_page->GetSize() == 1) {
    auto new_root_page_id = ctx->write_set_.front().AsMut<InternalPage>()->RemoveAndReturnOnlyChild();
    UpdateRoot(ctx, new_root_page_id);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(bpm_, std::nullopt, 0);
  }
  Context ctx;
  FindLeafPage(&ctx, Operation::SEARCH, KeyType(), comparator_, true, false);
  return INDEXITERATOR_TYPE(bpm_, std::move(ctx.read_set_.back()), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  FindLeafPage(&ctx, Operation::SEARCH, key, comparator_, false, false);
  auto *leaf_page = ctx.read_set_.back().As<LeafPage>();
  auto index = leaf_page->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(bpm_, std::optional<ReadPageGuard>(std::move(ctx.read_set_.back())), index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  // Context ctx;
  // FindLeafPage(&ctx, Operation::SEARCH, KeyType(), comparator_, false, true);
  // auto *leaf_page = ctx.read_set_.back().As<LeafPage>();
  return INDEXITERATOR_TYPE(bpm_, std::nullopt, 0);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto root_page_guard = bpm_->ReadPage(header_page_id_);

  return root_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
