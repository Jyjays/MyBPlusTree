#include "b_plus_tree_wrapper.h"

#include <cstring>

#include "b_plus_tree.h"

using BPlusTree = mybplus::BPlusTree<mybplus::KeyType, mybplus::ValueType, mybplus::KeyComparator>;
using CppBasePage = mybplus::BPlusTreePage;
using CppLeafPage =
    mybplus::BPlusTreeLeafPage<mybplus::KeyType, mybplus::ValueType, mybplus::KeyComparator>;
using CppInternalPage =
    mybplus::BPlusTreeInternalPage<mybplus::KeyType, page_id_t, mybplus::KeyComparator>;

extern "C" {

// 类型转换辅助函数
static mybplus::ValueType c_to_cpp_value(const ValueType& c_value) {
  mybplus::ValueType cpp_value;
  std::memcpy(cpp_value.data(), c_value.data, sizeof(c_value.data));
  return cpp_value;
}

static ValueType cpp_to_c_value(const mybplus::ValueType& cpp_value) {
  ValueType c_value;
  std::memcpy(c_value.data, cpp_value.data(), sizeof(c_value.data));
  return c_value;
}

CBPlusTree* bpt_create(int leaf_max_size, int internal_max_size) {
  auto* comparator = new mybplus::KeyComparator();
  auto* tree = new BPlusTree("c_api_tree", *comparator, leaf_max_size, internal_max_size);
  return reinterpret_cast<CBPlusTree*>(tree);
}
void bpt_set_meta(CBPlusTree* tree, page_id_t root_page_id, int leaf_max_size,
                  int internal_max_size) {
  BPlusTree* cpp_tree = reinterpret_cast<BPlusTree*>(tree);
  cpp_tree->SetRootPageId(root_page_id);
  cpp_tree->SetLeafMaxSize(leaf_max_size);
  cpp_tree->SetInternalMaxSize(internal_max_size);
}
void bpt_clear(CBPlusTree* tree) {
  reinterpret_cast<BPlusTree*>(tree)->Clear();
}
void bpt_destroy(CBPlusTree* tree) {
  delete reinterpret_cast<BPlusTree*>(tree);
}

bool bpt_insert(CBPlusTree* tree, KeyType key, ValueType value) {
  return reinterpret_cast<BPlusTree*>(tree)->Insert(key, c_to_cpp_value(value));
}

bool bpt_get_value(CBPlusTree* tree, KeyType key, ValueType* out_value) {
  std::vector<mybplus::ValueType> results;
  bool found = reinterpret_cast<BPlusTree*>(tree)->GetValue(key, &results);
  if (found && !results.empty()) {
    *out_value = cpp_to_c_value(results[0]);
  }
  return found;
}

void bpt_create_page_with_id(CBPlusTree* tree, page_id_t page_id, bool is_leaf) {
  BPlusTree* cpp_tree = reinterpret_cast<BPlusTree*>(tree);
  cpp_tree->CreateAndRegisterPage(page_id, is_leaf);
}
page_id_t get_root_page_id(CBPlusTree* tree) {
  return reinterpret_cast<BPlusTree*>(tree)->GetRootPageId();
}

uint32_t get_page_count(const CBPlusTree* tree) {
  return reinterpret_cast<const BPlusTree*>(tree)->GetPageCount();
}

int get_leaf_max_size(const CBPlusTree* tree) {
  return reinterpret_cast<const BPlusTree*>(tree)->GetLeafMaxSize();
}

int get_internal_max_size(const CBPlusTree* tree) {
  return reinterpret_cast<const BPlusTree*>(tree)->GetInternalMaxSize();
}

CBPlusTreePage* get_page(CBPlusTree* tree, page_id_t page_id) {
  return reinterpret_cast<CBPlusTreePage*>(reinterpret_cast<BPlusTree*>(tree)->GetPage(page_id));
}

bool page_is_leaf(const CBPlusTreePage* page) {
  return reinterpret_cast<const CppBasePage*>(page)->IsLeafPage();
}

int page_get_size(const CBPlusTreePage* page) {
  return reinterpret_cast<const CppBasePage*>(page)->GetSize();
}
void bpt_page_set_size(CBPlusTreePage* page, int size) {
  reinterpret_cast<CppBasePage*>(page)->SetSize(size);
}

page_id_t page_get_id(const CBPlusTreePage* page) {
  return reinterpret_cast<const CppBasePage*>(page)->GetPageId();
}

KeyType leaf_page_get_key_at(const CBPlusTreePage* page, int index) {
  return reinterpret_cast<const CppLeafPage*>(page)->KeyAt(index);
}

ValueType leaf_page_get_value_at(const CBPlusTreePage* page, int index) {
  return cpp_to_c_value(reinterpret_cast<const CppLeafPage*>(page)->ValueAt(index));
}

page_id_t leaf_page_get_next_id(const CBPlusTreePage* page) {
  return reinterpret_cast<const CppLeafPage*>(page)->GetNextPageId();
}

void leaf_page_set_kv_at(CBPlusTreePage* page, int index, KeyType key, ValueType value) {
  reinterpret_cast<CppLeafPage*>(page)->SetAt(index, key, c_to_cpp_value(value));
}
void leaf_page_set_next_id(CBPlusTreePage* page, page_id_t next_page_id) {
  reinterpret_cast<CppLeafPage*>(page)->SetNextPageId(next_page_id);
}

KeyType internal_page_get_key_at(const CBPlusTreePage* page, int index) {
  return reinterpret_cast<const CppInternalPage*>(page)->KeyAt(index);
}

page_id_t internal_page_get_value_at(const CBPlusTreePage* page, int index) {
  return reinterpret_cast<const CppInternalPage*>(page)->ValueAt(index);
}

void internal_page_set_value_at(CBPlusTreePage* page, int index, page_id_t value) {
  reinterpret_cast<CppInternalPage*>(page)->SetValueAt(index, value);
}
void internal_page_set_key_at(CBPlusTreePage* page, int index, KeyType key) {
  reinterpret_cast<CppInternalPage*>(page)->SetKeyAt(index, key);
}

}  // extern "C"

namespace mybplus {
template class BPlusTree<KeyType, ValueType, KeyComparator>;
template class BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
template class BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
}  // namespace mybplus
