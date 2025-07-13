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
struct PrintableBPlusTree;
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

  auto CheckAndReleaseAncestors(BPlusTreePage *current_page, OperationType op) -> void {
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

  auto InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Context *ctx) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  auto RemoveLeafEntry(LeafPage *leaf_page, InternalPage *parent_page, const KeyType &key,
                       Context *ctx) -> void;

  auto RemoveInternalEntry(InternalPage *internal_page, InternalPage *parent_page,
                           const KeyType &key, Context *ctx) -> void;
  // auto RemoveLeafEntry(LeafPage *leaf_page, InternalPage *parent_page, const KeyType &key,
  //                      int delete_index, Context *ctx) -> void;
  // auto RemoveInternalEntry(InternalPage *internal_page, InternalPage *parent_page,
  //                          const KeyType &key, int delete_index, Context *ctx) -> void;
  auto LeafCanMerge(LeafPage *merge_page, LeafPage *left_leaf, LeafPage *right_leaf)
      -> std::pair<bool, bool>;

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
  auto SetLeafMaxSize(int size) -> void { leaf_max_size_ = size; }
  auto SetInternalMaxSize(int size) -> void { internal_max_size_ = size; }
  auto SetRootPageId(int32_t page_id) -> void {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    root_page_id_ = page_id;
  }
  // Print the B+ tree
  void Print();

  // Draw the B+ tree
  void Draw(const std::string &outf);

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

  // 下面的方法需要Transaction类型，暂时注释掉
  // read data from file and insert one by one
  // void InsertFromFile(const std::string &file_name, Transaction *txn = nullptr);

  // read data from file and remove one by one
  // void RemoveFromFile(const std::string &file_name, Transaction *txn = nullptr);

  /**
   * @brief Read batch operations from input file, below is a sample file format
   * insert some keys and delete 8, 9 from the tree with one step.
   * { i1 i2 i3 i4 i5 i6 i7 i8 i9 i10 i30 d8 d9 } //  batch.txt
   * B+ Tree(4 max leaf, 4 max internal) after processing:
   *                            (5)
   *                 (3)                (7)
   *            (1,2)    (3,4)    (5,6)    (7,10,30) //  The output tree example
   */
  // void BatchOpsFromFile(const std::string &file_name, Transaction *txn = nullptr);

 private:
  auto GetPage(page_id_t page_id) -> BPlusTreePage *;
  auto NewPage(int32_t *new_page_id) -> BPlusTreePage *;
  auto NewLeafPage(int32_t *new_page_id) -> LeafPage *;
  auto NewInternalPage(int32_t *new_page_id) -> InternalPage *;
  auto DeletePage(page_id_t page_id) -> void;

  auto SplitLeafPage(LeafPage *leaf_page, LeafPage *new_page, const KeyType &key,
                     const ValueType &value, int32_t new_page_id) -> KeyType;

  /**
   * @return The key that should be inserted into the parent page.
   */
  auto SplitInternalPage(InternalPage *internal_page, InternalPage *new_page, const KeyType &key,
                         int32_t new_page_id) -> KeyType;

  auto TryBorrowFromSibling(LeafPage *leaf_page, LeafPage *left_bro, LeafPage *right_bro,
                            InternalPage *parent_page, int index) -> bool;
  auto TryMergeWithSibling(LeafPage *leaf_page, LeafPage *left_bro, LeafPage *right_bro,
                           InternalPage *parent_page, int index, Context *ctx) -> bool;

  auto FindParentInPath(InternalPage *target_page, Context *ctx) -> InternalPage *;

  auto TryBorrowFromInternalSibling(InternalPage *internal_page, InternalPage *left_bro,
                                    InternalPage *right_bro, InternalPage *parent_page, int index)
      -> bool;

  auto TryMergeWithInternalSibling(InternalPage *internal_page, InternalPage *left_bro,
                                   InternalPage *right_bro, InternalPage *parent_page, int index,
                                   Context *ctx) -> bool;

  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;
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

  int32_t root_page_id_ = INVALID_PAGE_ID;
};

struct PrintableBPlusTree {
  int size_;
  page_id_t page_id_;
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
        out_buf << t->page_id_;
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

}  // namespace mybplus
