#include "b_plus_tree.h"

#include <sstream>
#include <string>

namespace mybplus {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return root_page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPage(page_id_t page_id) -> BPlusTreePage * {
  auto it = pages_.find(page_id);
  if (it == pages_.end()) {
    return nullptr;
  }
  return it->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewPage(page_id_t *page_id) -> BPlusTreePage * {
  *page_id = next_page_id_++;

  BPlusTreePage *page = new BPlusTreePage();
  page->SetPageId(*page_id);

  pages_[*page_id] = page;
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewLeafPage(page_id_t *new_page_id) -> LeafPage * {
  *new_page_id = next_page_id_++;
  LeafPage *leaf_page = new LeafPage();
  leaf_page->SetPageId(*new_page_id);
  leaf_page->Init(leaf_max_size_);
  leaf_page->SetMaxSize(leaf_max_size_);
  pages_[*new_page_id] = leaf_page;
  return leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::NewInternalPage(page_id_t *new_page_id) -> InternalPage * {
  *new_page_id = next_page_id_++;
  InternalPage *internal_page = new InternalPage();
  internal_page->SetPageId(*new_page_id);
  internal_page->Init(internal_max_size_);
  internal_page->SetMaxSize(internal_max_size_);
  pages_[*new_page_id] = internal_page;
  return internal_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePage(page_id_t page_id) {
  auto it = pages_.find(page_id);
  if (it != pages_.end()) {
    delete it->second;
    pages_.erase(it);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> *result) -> bool {
  std::shared_lock<std::shared_mutex> lock(mutex_);

  if (root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  BPlusTreePage *page = GetPage(root_page_id_);
  if (!page) {
    return false;
  }

  // 遍历到叶子节点
  while (!page->IsLeafPage()) {
    InternalPage *internal_page = static_cast<InternalPage *>(page);
    page_id_t next_page_id =
        internal_page->FindValue(key, comparator_, nullptr);
    page = GetPage(next_page_id);
    if (!page) {
      return false;
    }
  }

  LeafPage *leaf_page = static_cast<LeafPage *>(page);
  ValueType value;
  if (leaf_page->FindValue(key, comparator_, value, nullptr)) {
    result->emplace_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value)
    -> bool {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  if (root_page_id_ == INVALID_PAGE_ID) {
    // 创建新的叶子页面
    page_id_t new_page_id;
    LeafPage *new_leaf_page = NewLeafPage(&new_page_id);
    if (!new_leaf_page) {
      return false;  // 分配新页面失败
    }
    new_leaf_page->Insert(key, value, comparator_);
    root_page_id_ = new_page_id;
    return true;
  }

  // 查找要插入的叶子页面
  std::vector<BPlusTreePage *> path;
  BPlusTreePage *page = GetPage(root_page_id_);
  path.push_back(page);

  while (!page->IsLeafPage()) {
    InternalPage *internal_page = static_cast<InternalPage *>(page);
    page_id_t next_page_id =
        internal_page->FindValue(key, comparator_, nullptr);
    page = GetPage(next_page_id);
    path.push_back(page);
  }

  LeafPage *leaf_page = static_cast<LeafPage *>(page);

  // 如果页面有足够空间，直接插入
  if (page->IsSafe(OperationType::INSERT)) {
    return leaf_page->Insert(key, value, comparator_);
  }

  // 页面需要分裂
  page_id_t new_page_id;
  LeafPage *new_leaf_page = NewLeafPage(&new_page_id);
  if (!new_leaf_page) {
    return false;  // 分配新页面失败
  }
  KeyType new_key =
      SplitLeafPage(leaf_page, new_leaf_page, key, value, new_page_id);
  if (comparator_(KeyType(), new_key) == 0) {
    DeletePage(new_page_id);
    return false;
  }

  // 插入到父节点
  path.pop_back();  // 移除叶子节点
  InsertIntoParent(leaf_page, new_key, new_leaf_page, path);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      std::vector<BPlusTreePage *> &path)
    -> void {
  // 如果旧节点是根节点
  if (old_node->GetPageId() == root_page_id_) {
    // 创建新的内部页面
    page_id_t new_page_id;
    InternalPage *new_internal_page = NewInternalPage(&new_page_id);
    if (!new_internal_page) {
      return;  // 分配新页面失败
    }

    new_internal_page->SetPageId(new_page_id);
    new_internal_page->Init(internal_max_size_);
    new_internal_page->SetMaxSize(internal_max_size_);
    new_internal_page->PopulateNewRoot(old_node->GetPageId(), key,
                                       new_node->GetPageId());

    root_page_id_ = new_page_id;
    return;
  }

  // 查找父节点
  BPlusTreePage *parent_page = path.back();
  path.pop_back();
  if (!parent_page || parent_page->IsLeafPage()) {
    return;
  }

  InternalPage *parent_internal = static_cast<InternalPage *>(parent_page);

  // 如果父页面有足够空间，直接插入
  if (parent_page->IsSafe(OperationType::INSERT)) {
    parent_internal->Insert(key, new_node->GetPageId(), comparator_);
    return;
  }

  // 父页面需要分裂
  page_id_t new_page_id;
  InternalPage *new_internal_page = NewInternalPage(&new_page_id);
  if (!new_internal_page) {
    return;  // 分配新页面失败
  }

  new_internal_page->Init(internal_max_size_);
  new_internal_page->SetMaxSize(internal_max_size_);
  new_internal_page->SetPageId(new_page_id);

  KeyType middle_key = SplitInternalPage(parent_internal, new_internal_page,
                                         key, new_node->GetPageId());
  InsertIntoParent(parent_internal, middle_key, new_internal_page, path);
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
  std::unique_lock<std::shared_mutex> lock(mutex_);

  if (root_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  // 查找要删除的叶子页面
  std::vector<BPlusTreePage *> path;
  BPlusTreePage *page = GetPage(root_page_id_);
  path.push_back(page);

  while (!page->IsLeafPage()) {
    InternalPage *internal_page = static_cast<InternalPage *>(page);
    page_id_t next_page_id =
        internal_page->FindValue(key, comparator_, nullptr);
    page = GetPage(next_page_id);
    path.push_back(page);
  }

  LeafPage *leaf_page = static_cast<LeafPage *>(page);
  int delete_index = -1;
  ValueType value;

  // 如果页面安全或者是根页面，直接删除
  if (leaf_page->IsSafe(OperationType::DELETE) ||
      leaf_page->GetPageId() == root_page_id_) {
    if (leaf_page->FindValue(key, comparator_, value, &delete_index)) {
      leaf_page->Delete(delete_index);
    }

    // 如果是根页面且为空，删除根页面
    if (leaf_page->GetPageId() == root_page_id_ && leaf_page->GetSize() == 0) {
      DeletePage(leaf_page->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
    }
    return;
  }

  // 页面不安全，需要借用或合并
  RemoveLeafEntry(leaf_page, key, path);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveLeafEntry(LeafPage *leaf_page, const KeyType &key,
                                     std::vector<BPlusTreePage *> &path)
    -> void {
  int delete_index = -1;
  ValueType value;
  if (!leaf_page->FindValue(key, comparator_, value, &delete_index)) {
    return;
  }

  leaf_page->Delete(delete_index);

  // 查找父节点和兄弟节点
  BPlusTreePage *last_page = path.back();
  path.pop_back();
  if (!last_page || last_page->IsLeafPage()) {
    return;  // 没有父节点或不是内部页面
  }
  InternalPage *parent_page = static_cast<InternalPage *>(last_page);

  // 查找兄弟节点
  LeafPage *left_bro = nullptr;
  LeafPage *right_bro = nullptr;
  int index = parent_page->ValueIndex(leaf_page->GetPageId());

  if (index > 0) {
    page_id_t left_page_id = parent_page->ValueAt(index - 1);
    BPlusTreePage *left_page = GetPage(left_page_id);
    if (left_page && left_page->IsLeafPage()) {
      left_bro = static_cast<LeafPage *>(left_page);
    }
  }
  if (index < parent_page->GetSize() - 1) {
    page_id_t right_page_id = parent_page->ValueAt(index + 1);
    BPlusTreePage *right_page = GetPage(right_page_id);
    if (right_page && right_page->IsLeafPage()) {
      right_bro = static_cast<LeafPage *>(right_page);
    }
  }

  // 尝试借用
  LeafPage *borrow_page = nullptr;
  bool isLeftBorrow = false;
  if (left_bro && left_bro->IsSafe(OperationType::DELETE)) {
    borrow_page = left_bro;
    isLeftBorrow = true;
  } else if (right_bro && right_bro->IsSafe(OperationType::DELETE)) {
    borrow_page = right_bro;
  }

  if (borrow_page) {
    if (isLeftBorrow) {
      // 从左兄弟借用
      int last_idx = borrow_page->GetSize() - 1;
      KeyType borrow_key = borrow_page->KeyAt(last_idx);
      ValueType borrow_value = borrow_page->ValueAt(last_idx);
      borrow_page->Delete(last_idx);
      leaf_page->Insert(borrow_key, borrow_value, comparator_);

      int parent_index = parent_page->ValueIndex(leaf_page->GetPageId());
      parent_page->SetKeyAt(parent_index, borrow_key);
    } else {
      // 从右兄弟借用
      KeyType borrow_key = borrow_page->KeyAt(0);
      ValueType borrow_value = borrow_page->ValueAt(0);
      borrow_page->Delete(0);
      leaf_page->Insert(borrow_key, borrow_value, comparator_);

      int right_parent_index =
          parent_page->ValueIndex(borrow_page->GetPageId());
      parent_page->SetKeyAt(right_parent_index, borrow_page->KeyAt(0));
    }
    return;
  }

  // 尝试合并
  auto [isLeft, canMerge] = LeafCanMerge(leaf_page, left_bro, right_bro);
  if (canMerge) {
    LeafPage *kept_page = nullptr;
    LeafPage *removed_page = nullptr;
    if (isLeft) {
      kept_page = left_bro;
      removed_page = leaf_page;
    } else {
      kept_page = leaf_page;
      removed_page = right_bro;
    }

    int merge_index = parent_page->ValueIndex(removed_page->GetPageId());
    KeyType parent_key = parent_page->KeyAt(merge_index);
    kept_page->MergeFrom(removed_page->GetData(), removed_page->GetSize());

    if (parent_page->IsSafe(OperationType::DELETE)) {
      parent_page->Delete(merge_index);
      kept_page->SetNextPageId(removed_page->GetNextPageId());
    } else {
      // RemoveInternalEntry(parent_page, parent_key, removed_page->GetPageId(),
      //                     path);
      RemoveInternalEntry(parent_page, parent_key, path);
    }

    DeletePage(removed_page->GetPageId());
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveInternalEntry(InternalPage *internal_page,
                                         const KeyType &key,
                                         std::vector<BPlusTreePage *> &path)
    -> void {
  int delete_index = -1;
  if (!internal_page->FindValue(key, comparator_, &delete_index)) {
    return;
  }

  internal_page->Delete(delete_index);

  // 如果是根页面且只有一个子节点
  if (internal_page->GetPageId() == root_page_id_ &&
      internal_page->GetSize() == 1) {
    page_id_t new_root_id = internal_page->ValueAt(0);
    root_page_id_ = new_root_id;
    DeletePage(internal_page->GetPageId());
    return;
  }

  // 查找父节点和兄弟节点（类似叶子节点处理）
  BPlusTreePage *last_page = path.back();
  path.pop_back();
  if (!last_page || last_page->IsLeafPage()) {
    return;  // 没有父节点或不是内部页面
  }
  InternalPage *parent_page = static_cast<InternalPage *>(last_page);
  if (parent_page->GetPageId() == internal_page->GetPageId()) {
    return;
  }

  // 查找兄弟节点
  InternalPage *left_bro = nullptr;
  InternalPage *right_bro = nullptr;
  int index = parent_page->ValueIndex(internal_page->GetPageId());

  if (index > 0) {
    page_id_t left_page_id = parent_page->ValueAt(index - 1);
    BPlusTreePage *left_page = GetPage(left_page_id);
    if (left_page && !left_page->IsLeafPage()) {
      left_bro = static_cast<InternalPage *>(left_page);
    }
  }
  if (index < parent_page->GetSize() - 1) {
    page_id_t right_page_id = parent_page->ValueAt(index + 1);
    BPlusTreePage *right_page = GetPage(right_page_id);
    if (right_page && !right_page->IsLeafPage()) {
      right_bro = static_cast<InternalPage *>(right_page);
    }
  }

  // 尝试借用
  InternalPage *borrow_page = nullptr;
  bool isLeftBorrow = false;
  if (left_bro && left_bro->IsSafe(OperationType::DELETE)) {
    borrow_page = left_bro;
    isLeftBorrow = true;
  } else if (right_bro && right_bro->IsSafe(OperationType::DELETE)) {
    borrow_page = right_bro;
  }

  if (borrow_page) {
    if (isLeftBorrow) {
      // 从左兄弟借用
      int last_idx = borrow_page->GetSize() - 1;
      KeyType borrow_key = borrow_page->KeyAt(last_idx);
      page_id_t borrow_page_id = borrow_page->ValueAt(last_idx);
      borrow_page->Delete(last_idx);
      internal_page->Insert(borrow_key, borrow_page_id, comparator_);

      int parent_index = parent_page->ValueIndex(internal_page->GetPageId());
      parent_page->SetKeyAt(parent_index, borrow_key);
    } else {
      // 从右兄弟借用（旋转操作）
      int parent_sep_index = parent_page->ValueIndex(borrow_page->GetPageId());
      KeyType separator_key = parent_page->KeyAt(parent_sep_index);
      page_id_t borrow_page_id = borrow_page->ValueAt(0);

      internal_page->Insert(separator_key, borrow_page_id, comparator_);
      KeyType new_separator_key = borrow_page->KeyAt(1);
      parent_page->SetKeyAt(parent_sep_index, new_separator_key);
      borrow_page->Delete(0);
    }
    return;
  }

  // 尝试合并
  auto [isLeft, canMerge] =
      InternalCanMerge(internal_page, left_bro, right_bro);
  if (canMerge) {
    InternalPage *kept_page = nullptr;
    InternalPage *removed_page = nullptr;
    if (isLeft) {
      kept_page = left_bro;
      removed_page = internal_page;
    } else {
      kept_page = internal_page;
      removed_page = right_bro;
    }

    int merge_index = parent_page->ValueIndex(removed_page->GetPageId());
    KeyType parent_key = parent_page->KeyAt(merge_index);
    kept_page->MergeFrom(removed_page->GetData(), removed_page->GetSize());

    if (parent_page->IsSafe(OperationType::DELETE)) {
      parent_page->Delete(merge_index);
    } else {
      RemoveInternalEntry(parent_page, parent_key, path);
    }

    DeletePage(removed_page->GetPageId());
  }
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeafPage(LeafPage *leaf_page, LeafPage *new_page,
                                   const KeyType &key, const ValueType &value,
                                   page_id_t new_page_id) -> KeyType {
  // 插入新键到旧节点
  leaf_page->Insert(key, value, comparator_);

  int cur_size = leaf_page->GetSize();
  int split_index = cur_size / 2;

  // 分裂节点
  new_page->CopyHalfFrom(leaf_page->GetData(), split_index, cur_size);
  new_page->SetSize(cur_size - split_index);
  leaf_page->SetSize(split_index);

  // 提升右节点的第一个键
  KeyType middle_key = new_page->KeyAt(0);
  new_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  return middle_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternalPage(InternalPage *internal_page,
                                       InternalPage *new_page,
                                       const KeyType &key,
                                       page_id_t new_page_id) -> KeyType {
  // 插入新键到旧节点
  internal_page->Insert(key, new_page_id, comparator_);

  int cur_size = internal_page->GetSize();
  int split_index = cur_size / 2;

  // 分裂节点
  new_page->CopyHalfFrom(internal_page->GetData(), split_index, cur_size);
  new_page->SetSize(cur_size - split_index);
  internal_page->SetSize(split_index);

  // 提升右节点的第一个键
  KeyType middle_key = new_page->KeyAt(0);
  return middle_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafCanMerge(LeafPage *merge_page, LeafPage *left_leaf,
                                  LeafPage *right_leaf)
    -> std::pair<bool, bool> {
  if (right_leaf != nullptr && merge_page->GetSize() + right_leaf->GetSize() <
                                   merge_page->GetMaxSize()) {
    return {false, true};
  }
  if (left_leaf != nullptr &&
      merge_page->GetSize() + left_leaf->GetSize() < merge_page->GetMaxSize()) {
    return {true, true};
  }
  return {false, false};
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalCanMerge(InternalPage *merge_page,
                                      InternalPage *left_internal,
                                      InternalPage *right_internal)
    -> std::pair<bool, bool> {
  if (right_internal != nullptr &&
      merge_page->GetSize() + right_internal->GetSize() <=
          merge_page->GetMaxSize()) {
    return {false, true};
  }
  if (left_internal != nullptr &&
      merge_page->GetSize() + left_internal->GetSize() <=
          merge_page->GetMaxSize()) {
    return {true, true};
  }
  return {false, false};
}

template class BPlusTree<int64_t, std::array<char, 16UL>, Comparator>;

}  // namespace mybplus