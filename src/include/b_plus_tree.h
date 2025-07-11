#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "b_plus_tree_internal.h"
#include "b_plus_tree_leaf.h"
#include "config.h" 

namespace mybplus {

class Context {
 public:
  Context(std::shared_mutex &root_mutex) : root_mutex_(root_mutex) {
    root_page_id_ = INVALID_PAGE_ID;
  }
  ~Context() {
    Clear();
    if (is_root_wlocked_) {
      WUnlockRoot();
    }
    if (is_root_rlocked_) {
      RUnlockRoot();
    }
  }

  auto WLockRoot() -> void {
#ifdef USING_CRABBING_PROTOCOL
    if (!is_root_wlocked_) {
      root_mutex_.lock();
      is_root_wlocked_ = true;
    }
#endif
  }
  auto RLockRoot() -> void {
#ifdef USING_CRABBING_PROTOCOL
    if (!is_root_rlocked_) {
      root_mutex_.lock_shared();
      is_root_rlocked_ = true;
    }
#endif
  }
  auto WUnlockRoot() -> void {
#ifdef USING_CRABBING_PROTOCOL
    if (is_root_wlocked_) {
      root_mutex_.unlock();
      is_root_wlocked_ = false;
    }
#endif
  }
  auto RUnlockRoot() -> void {
#ifdef USING_CRABBING_PROTOCOL
    if (is_root_rlocked_) {
      root_mutex_.unlock_shared();
      is_root_rlocked_ = false;
    }
#endif
  }

  auto CheckAndReleaseAncestors(BPlusTreePage *current_page, OperationType op)
      -> void {
#ifdef USING_CRABBING_PROTOCOL
    if (current_page->IsSafe(op)) {
      // 释放除当前页面外的所有祖先锁
      while (WritePath.size() > 1) {
        WPopFront();
      }
    }
#endif
  }
  auto WPush(BPlusTreePage *page) -> void {
#ifdef USING_CRABBING_PROTOCOL
    page->WLock();
#endif
    WritePath.push_back(page);
  }
  auto RPush(BPlusTreePage *page) -> void {
#ifdef USING_CRABBING_PROTOCOL
    page->RLock();
#endif
    ReadPath.push_back(page);
  }
  auto WPopBack() -> void {
    if (!WritePath.empty()) {
#ifdef USING_CRABBING_PROTOCOL
      WritePath.back()->Unlock();
#endif
      WritePath.pop_back();
    }
  }
  auto RPopBack() -> void {
    if (!ReadPath.empty()) {
#ifdef USING_CRABBING_PROTOCOL
      ReadPath.back()->RUnlock();
#endif
      ReadPath.pop_back();
    }
  }
  auto WPopFront() -> void {
    if (!WritePath.empty()) {
#ifdef USING_CRABBING_PROTOCOL
      WritePath.front()->Unlock();
#endif
      WritePath.pop_front();
    }
  }
  auto RPopFront() -> void {
    if (!ReadPath.empty()) {
#ifdef USING_CRABBING_PROTOCOL
      ReadPath.front()->RUnlock();
#endif
      ReadPath.pop_front();
    }
  }
  auto WBack() -> BPlusTreePage * {
    if (!WritePath.empty()) {
      return WritePath.back();
    }
    return nullptr;
  }
  auto RBack() -> BPlusTreePage * {
    if (!ReadPath.empty()) {
      return ReadPath.back();
    }
    return nullptr;
  }
  auto Clear() -> void {
#ifdef USING_CRABBING_PROTOCOL
    for (auto &page : WritePath) {
      page->Unlock();
    }
    for (auto &page : ReadPath) {
      page->RUnlock();
    }
#endif
    if (is_root_wlocked_) {
      WUnlockRoot();
    }
    if (is_root_rlocked_) {
      RUnlockRoot();
    }
    WritePath.clear();
    ReadPath.clear();
  }
  auto IsEmpty() const -> bool { return WritePath.empty() && ReadPath.empty(); }
  auto WSize() const -> size_t { return WritePath.size(); }
  auto RSize() const -> size_t { return ReadPath.size(); }
  std::deque<BPlusTreePage *> WritePath;
  std::deque<BPlusTreePage *> ReadPath;
  page_id_t root_page_id_ = INVALID_PAGE_ID;
  std::shared_mutex &root_mutex_;
  bool is_root_wlocked_ = false;
  bool is_root_rlocked_ = false;
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeSerializer;


INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  friend class BPlusTreeSerializer<KeyType, ValueType, KeyComparator>;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE,
                     int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  auto InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
                        BPlusTreePage *new_node, Context *ctx) -> void;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  auto RemoveLeafEntry(LeafPage *leaf_page, const KeyType &key, Context *ctx)
      -> void;

  auto RemoveInternalEntry(InternalPage *internal_page, const KeyType &key,
                           Context *ctx) -> void;

  auto LeafCanMerge(LeafPage *merge_page, LeafPage *left_leaf,
                    LeafPage *right_leaf) -> std::pair<bool, bool>;

  auto InternalCanMerge(InternalPage *merge_page, InternalPage *left_internal,
                        InternalPage *right_internal) -> std::pair<bool, bool>;
  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  auto CreateAndRegisterPage(page_id_t page_id, bool is_leaf) -> void;

  auto Clear() -> void;

  // Return the page id of the root node
  auto GetRootPageId() -> int32_t;

  auto GetLeafMaxSize() const -> int { return leaf_max_size_; }

  auto GetInternalMaxSize() const -> int { return internal_max_size_; }

  auto GetPageCount() const -> size_t {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return pages_.size();
  }
  auto SetLeafMaxSize(int size) -> void {
    leaf_max_size_ = size;
  }
  auto SetInternalMaxSize(int size) -> void {
    internal_max_size_ = size;
  }
  auto SetRootPageId(int32_t page_id) -> void {
    std::lock_guard<std::mutex> lock(root_mutex_);
    root_page_id_ = page_id;
  }

 private:
  auto GetPage(page_id_t page_id) -> BPlusTreePage *;
  auto NewPage(int32_t *new_page_id) -> BPlusTreePage *;
  auto NewLeafPage(int32_t *new_page_id) -> LeafPage *;
  auto NewInternalPage(int32_t *new_page_id) -> InternalPage *;
  auto DeletePage(page_id_t page_id) -> void;

  auto SplitLeafPage(LeafPage *leaf_page, LeafPage *new_page,
                     const KeyType &key, const ValueType &value,
                     int32_t new_page_id) -> KeyType;

  /**
   * @return The key that should be inserted into the parent page.
   */
  auto SplitInternalPage(InternalPage *internal_page, InternalPage *new_page,
                         const KeyType &key, int32_t new_page_id) -> KeyType;

  // member variable
  std::string index_name_;
  KeyComparator comparator_;
  std::vector<std::string> log;
  int leaf_max_size_;
  int internal_max_size_;
  // page_id_t header_page_id_;

  mutable std::shared_mutex mutex_;
  //  std::vector<page_id_t> page_ids_;
  std::mutex pages_mutex_;
  std::unordered_map<page_id_t, BPlusTreePage *> pages_;
  page_id_t next_page_id_ = 1;

  std::mutex root_mutex_;
  int32_t root_page_id_ = INVALID_PAGE_ID;
};

}  // namespace mybplus
