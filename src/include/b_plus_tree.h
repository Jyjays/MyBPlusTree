#pragma once

#include <algorithm>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>
#include <deque>
#include <unordered_map>

#include "b_plus_tree_internal.h"
#include "b_plus_tree_leaf.h"
#include "config.h"

namespace mybplus {

class Context {
  public:
    Context() = default;
    auto WPush(BPlusTreePage *page) -> void {
      page->WLock();  
      WritePath.push_back(page);
    }
    auto RPush(BPlusTreePage *page) -> void {
      page->RLock();
      ReadPath.push_back(page);
    }
    auto WPopBack() -> void {
      if (!WritePath.empty()) {
        WritePath.back()->Unlock();
        WritePath.pop_back();
      }
    }
    auto RPopBack() -> void {
      if (!ReadPath.empty()) {
        ReadPath.back()->RUnlock();
        ReadPath.pop_back();
      }
    }
    auto WPopFront() -> void {
      if (!WritePath.empty()) {
        WritePath.front()->Unlock();
        WritePath.pop_front();
      }
    }
    auto RPopFront() -> void {
      if (!ReadPath.empty()) {
        ReadPath.front()->RUnlock();
        ReadPath.pop_front();
      }
    }
    auto WBack() -> BPlusTreePage* {
      if (!WritePath.empty()) {
        return WritePath.back();
      }
      return nullptr;
    }
    auto RBack() -> BPlusTreePage* {
      if (!ReadPath.empty()) {
        return ReadPath.back();
      }
      return nullptr;
    }
    auto Clear() -> void {
      for (auto &page : WritePath) {
        page->Unlock();
      }
      for (auto &page : ReadPath) {
        page->RUnlock();
      }
      WritePath.clear();
      ReadPath.clear();
    }
    auto IsEmpty() const -> bool {
      return WritePath.empty() && ReadPath.empty();
    }
    auto WSize() const -> size_t {
      return WritePath.size();
    }
    auto RSize() const -> size_t {
      return ReadPath.size();
    }
    std::deque<BPlusTreePage*> WritePath;
    std::deque<BPlusTreePage*> ReadPath;
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
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
                        BPlusTreePage *new_node,
                        Context *ctx) -> void;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key);

  auto RemoveLeafEntry(LeafPage *leaf_page, const KeyType &key,
                       Context *ctx) -> void;

  auto RemoveInternalEntry(InternalPage *internal_page, const KeyType &key,
                           Context *ctx) -> void;

  auto LeafCanMerge(LeafPage *merge_page, LeafPage *left_leaf,
                    LeafPage *right_leaf) -> std::pair<bool, bool>;

  auto InternalCanMerge(InternalPage *merge_page, InternalPage *left_internal,
                        InternalPage *right_internal) -> std::pair<bool, bool>;
  // Return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // Return the page id of the root node
  auto GetRootPageId() -> int32_t;

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name);
  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name);

  void BatchOpsFromFile(const std::string &file_name);

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
  std::unordered_map<page_id_t, BPlusTreePage *> pages_;
  page_id_t next_page_id_ = 1;
  int32_t root_page_id_ = INVALID_PAGE_ID;
};

}  // namespace mybplus
