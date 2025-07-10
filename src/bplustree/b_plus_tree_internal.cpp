#include "b_plus_tree_internal.h"

#include <iostream>
#include <sstream>

namespace mybplus {

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) -> void{
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(page_id_t page_id_one,
                                                     const KeyType &key,
                                                     page_id_t page_id_two)
    -> void {
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key)
    -> void {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index,
                                                const ValueType &value)
    -> void {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(const KeyType &key,
                                               const KeyComparator &comparator,
                                               int *child_page_index) const
    -> ValueType {
  int size = GetSize();
  auto compare_first = [&comparator](const KeyType &lhs_key,
                                     const MappingType &rhs) -> bool {
    return comparator(lhs_key, rhs.first) < 0;
  };

  auto it = std::upper_bound(array_.begin() + 1, array_.begin() + size, key,
                             compare_first);
  auto res = std::prev(it);

  if (child_page_index != nullptr) {
    *child_page_index = std::distance(array_.begin(), res);
  }

  return res->second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key,
                                            const ValueType &value,
                                            const KeyComparator &comparator)
    -> bool {
  int size = GetSize();
  auto compare_first = [&comparator](const MappingType &lhs,
                                     const KeyType &rhs_key) -> bool {
    return comparator(lhs.first, rhs_key) < 0;
  };
  auto it = std::lower_bound(array_.begin(), array_.begin() + GetSize(), key,
                             compare_first);
  int index = std::distance(array_.begin(), it);

  if (index < GetSize() && comparator(array_[index].first, key) == 0) {
    return false;  // 键重复
  }

  array_.insert(array_.begin() + index, MappingType{key, value});
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(int child_page_index) -> bool {
  int size = GetSize();
  if (child_page_index < 0 || child_page_index >= size) {
    return false;
  }
  // std::move(array_ + child_page_index + 1, array_ + size, array_ +
  // child_page_index);
  std::move(array_.begin() + child_page_index + 1, array_.begin() + GetSize(),
            array_.begin() + child_page_index);
  IncreaseSize(-1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const
    -> int {
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
