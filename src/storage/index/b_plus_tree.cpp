#include "storage/index/b_plus_tree.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_debug.h"
#include "storage/page/b_plus_tree_header_page.h"
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
  page_id_t root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto page_guard = bpm_->ReadPage(root_page_id);
  return page_guard.As<BPlusTreePage>()->GetSize() == 0;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(Context *ctx, Operation op, const KeyType &key, const KeyComparator &comparator)
    -> page_id_t {
  BUSTUB_ASSERT(ctx != nullptr, "context is nullptr");
  page_id_t leaf_pid = INVALID_PAGE_ID;
  if (op == Operation::SEARCH) {
    leaf_pid = FindLeafPage(ctx, op, key, comparator);
  } else if (op == Operation::INSERT) {
    leaf_pid = FindInsertLeafPage(ctx, key, comparator);
  }
  // BUSTUB_ASSERT(leaf_pid > 0, "leaf page id is invalid");
  return leaf_pid;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindSearchLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator)
    -> page_id_t {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto root_page_id = header_page_guard.As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  // get the root page
  auto root_page_guard = bpm_->ReadPage(root_page_id);
  header_page_guard.Drop();
  ctx->read_set_.emplace_back(root_page_guard);
  auto *root_page = root_page_guard.As<BPlusTreePage>();
  // Find the leaf page that contains the input key.
  // 1. root page is leaf page
  if (root_page->IsLeafPage()) {
    return root_page_id;
  }
  // 2. root page is internal page
  // 2.1 find the leaf page that contains the input key

  while (!ctx->read_set_.back().As<BPlusTreePage>()->IsLeafPage()) {
    auto *internal_page = const_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *>(
        ctx->read_set_.back().As<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>>());
    page_id_t child_pid = internal_page->Lookup(key, comparator);
    BUSTUB_ASSERT(child_pid > 0, "child page id is invalid");
    auto child_page_guard = bpm_->ReadPage(child_pid);

    ctx->read_set_.pop_back();
    ctx->read_set_.emplace_back(child_page_guard);
  }
  return ctx->read_set_.back().GetPageId();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindInsertLeafPage(Context *ctx, const KeyType &key, const KeyComparator &comparator)
    -> page_id_t {
  if (!ctx->header_page_.has_value()) {
    ctx->header_page_ = bpm_->WritePage(header_page_id_);
  }

  auto root_page_id = ctx->header_page_.value().AsMut<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  ctx->root_page_id_ = root_page_id;
  auto root_page_guard = bpm_->ReadPage(root_page_id);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ctx->write_set_.emplace_back(root_page_guard);  // hold root page
  if (root_page->IsLeafPage() && root_page->GetSize() < root_page->GetMaxSize() - 1) {
    ctx->ReleaseWriteLatch();
    return root_page_id;
  }
  if (!root_page->IsLeafPage() && root_page->GetSize() < root_page->GetMaxSize()) {
    ctx->ReleaseWriteLatch();
  }
  while (!ctx->write_set_.back().As<BPlusTreePage>()->IsLeafPage()) {
    auto *internal_page = ctx->write_set_.back().AsMut<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>>();
    page_id_t child_pid = internal_page->Lookup(key, comparator);
    BUSTUB_ASSERT(child_pid > 0, "child page id is invalid");
    auto child_page_guard = bpm_->WritePage(child_pid);  // child hold write latch
    auto child_page = child_page_guard.As<BPlusTreePage>();

    // 可以提前释放上面的锁
    if ((child_page->IsLeafPage() && child_page->GetSize() < child_page->GetMaxSize() - 1) ||
        (!child_page->IsLeafPage() && child_page->GetSize() < child_page->GetMaxSize())) {
      ctx->ReleaseWriteLatch();
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
  // (void)ctx;
  // ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto leaf_pid = FindLeafPage(&ctx, Operation::SEARCH, key, comparator_);
  if (leaf_pid == INVALID_PAGE_ID) {
    return false;
  }
  BUSTUB_ASSERT(leaf_pid > 0, "leaf page id is invalid");
  // find the key in the leaf page
  auto leaf_page_guard = bpm_->ReadPage(leaf_pid);
  auto leaf_page = const_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      leaf_page_guard.template As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>());  // need to use template
  return leaf_page->LookupKey(key, result, comparator_);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpdateRoot(page_id_t root_page_id) -> bool {
  BUSTUB_ASSERT(header_page_id_ != INVALID_PAGE_ID, "header page id is invalid");
  auto root_page_guard = bpm_->WritePage(header_page_id_);
  auto root_page = root_page_guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = root_page_id;
  return true;
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

  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  // ctx.root_page_id_ = GetRootPageId();
  ctx.header_page_ = bpm_->WritePage(header_page_id_);
  auto root_page_id = ctx.header_page_.value().As<BPlusTreeHeaderPage>()->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ctx.write_set_.emplace_back(bpm_->WritePage(root_page_id));
  ctx.root_page_id_ = root_page_id;
  return InsertIntoLeaf(&ctx, key, value);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto header_page_guard = bpm_->WritePage(header_page_id_);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();
  page_id_t leaf_page_id = bpm_->NewPage();
  auto leaf_page_guard = bpm_->WritePage(leaf_page_id);
  auto *leaf_page = leaf_page_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  leaf_page->Init();
  leaf_page->Insert(key, value, comparator_);
  header_page->root_page_id_ = leaf_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(Context *ctx) -> void {
  page_id_t split_page_id = bpm_->NewPage();
  auto split_page_guard = bpm_->WritePage(split_page_id);
  ctx->write_set_.emplace_back(split_page_guard);
  if (split_page_guard.AsMut<BPlusTreePage>()->IsLeafPage()) {
    auto *split_node = split_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    split_node->Init();
    std::next(ctx->write_set_.rbegin(), 1)
        ->AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>()
        ->MoveHalfTo(split_node);  // 倒数第二个元素
  } else {
  }
  // leaf_page->Init();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(Context *ctx, const KeyType &key, const ValueType &value) -> bool {
  auto leaf_page_id = FindLeafPage(ctx, Operation::INSERT, key, comparator_);
  auto leaf_page_guard = bpm_->WritePage(leaf_page_id);
  ctx->write_set_.emplace_back(leaf_page_guard);
  auto leaf_page = leaf_page_guard.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  auto before_insert_size = leaf_page->GetSize();
  auto new_size = leaf_page->Insert(key, value, comparator_);
  // check leaf node whether it is full or not
  // 1. duplicate key
  if (before_insert_size == new_size) {
    return false;
  }
  // 2. leaf node is full
  if (new_size < leaf_page->GetMaxSize()) {
    return true;
  }
  // 3. leaf node is full
  Split(ctx);
  auto *split_page = ctx->write_set_.back().AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  split_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(ctx->write_set_.back().GetPageId());

  auto new_key = split_page->KeyAt(0);  // 新分裂的节点的第一个key
  InsertIntoParent();
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
  (void)ctx;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
