#include "b_plus_tree_internal.h"

#include <iostream>
#include <sstream>

namespace mybplus {

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) -> void {
  SetMaxSize(max_size);
  // allcocate memory for array_, but can's use new to allocate memory
  // array_ = new MappingType[max_size];

  SetPageType(IndexPageType::INTERNAL_PAGE);
  KeyType vice_key;
  vice_key = KeyType();
  array_.resize(max_size);
  SetKeyAt(0, vice_key);
  SetSize(1);
  SetValueAt(0, INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(page_id_t page_id_one, const KeyType &key,
                                                     page_id_t page_id_two) -> void {
  array_[0] = MappingType{KeyType(), page_id_one};
  array_[1] = MappingType{key, page_id_two};
  SetSize(2);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) -> void {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(const KeyType &key, const KeyComparator &comparator,
                                               int *child_page_index) const -> ValueType {
  auto compare_first = [comparator](const MappingType &lhs, KeyType rhs) -> bool {
    return comparator(lhs.first, rhs) <= 0;
  };
  // NOTE - the first key is a dummy key, so we start from the second key
  auto res = std::lower_bound(array_.begin() + 1, array_.begin() + GetSize(), key, compare_first);

  // Then we need to move back one step to get the the first element that is less than key
  res = std::prev(res);

  if (child_page_index != nullptr) {
    *child_page_index = std::distance(array_.begin(), res);
  }

  return res->second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                            const KeyComparator &comparator) -> bool {
  int size = GetSize();
  auto compare_first = [&comparator](const MappingType &lhs, const KeyType &rhs_key) -> bool {
    return comparator(lhs.first, rhs_key) < 0;
  };
  auto it = std::lower_bound(array_.begin() + 1, array_.begin() + GetSize(), key, compare_first);
  int index = std::distance(array_.begin(), it);

  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    return false;
  }
  // std::move_backward(array_.begin() + index, array_.begin() + size, array_.begin() + size + 1);
  // array_[index] = MappingType{key, value};
  // IncreaseSize(1);
  // 2. 【修正】使用 vector::insert，安全且自动管理内存和大小
  array_.insert(array_.begin() + index, MappingType{key, value});

  IncreaseSize(1);
  if (GetSize() > GetMaxSize()) {
    array_.resize(GetMaxSize());
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value)
    -> bool {
  if (GetSize() >= GetMaxSize()) {
    return false;
  }
  array_.insert(array_.begin() + 1, MappingType{key, array_[0].second});

  // 然后更新索引0处的指针为从兄弟节点借来的新指针
  array_[0].second = value;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(int child_page_index) -> bool {
  int size = GetSize();
  if (child_page_index < 0 || child_page_index >= size) {
    return false;
  }
  array_.erase(array_.begin() + child_page_index);
  IncreaseSize(-1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<int64_t, page_id_t, Comparator>;

}  // namespace mybplus
