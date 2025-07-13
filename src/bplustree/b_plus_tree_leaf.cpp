#include "b_plus_tree_leaf.h"

#include <cassert>
#include <sstream>

namespace mybplus {

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  array_.resize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetAt(int index, const KeyType &key, const ValueType &value) {
  array_[index] = MappingType{key, value};
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                        const KeyComparator &comparator) -> bool {
  auto compare_first = [&comparator](const MappingType &lhs, const KeyType &key) -> bool {
    return comparator(lhs.first, key) < 0;
  };
  // auto it = std::lower_bound(array_, array_ + GetSize(), key, compare_first);
  int size = GetSize();
  auto it = std::lower_bound(array_.begin(), array_.begin() + size, key, compare_first);

  int index = std::distance(array_.begin(), it);
  if (index < size && comparator(array_[index].first, key) == 0) {
    return false;  // 键重复
  }

  // BUSTUB_ASSERT(size < GetMaxSize(), "The Leaf page is full.");

  // std::move_backward(array_.begin() + index, array_.begin() + size, array_.begin() + size + 1);
  // array_[index] = MappingType{key, value};

  array_.insert(array_.begin() + index, MappingType{key, value});

  IncreaseSize(1);
  if (GetSize() > GetMaxSize()) {
    array_.resize(GetMaxSize());
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValue(const KeyType &key, const KeyComparator &comparator,
                                           ValueType &value, int *key_index) const -> bool {
  int size = GetSize();
  auto compare_first = [&comparator](const MappingType &lhs, const KeyType &rhs) -> bool {
    return comparator(lhs.first, rhs) < 0;
  };
  auto it = std::lower_bound(array_.begin(), array_.begin() + size, key, compare_first);

  if (it == array_.begin() + size) {
    return false;  // 没有找到，或者key比所有元素都大
  }

  // 检查找到的键是否和目标键相等
  if (comparator(it->first, key) == 0) {
    value = it->second;  // 找到了，返回值
    if (key_index != nullptr) {
      *key_index = std::distance(array_.begin(), it);
    }
    return true;
  }

  return false;  // 没找到完全匹配的键
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> bool {
  if (GetSize() >= GetMaxSize()) {
    return false;
  }
  array_.insert(array_.begin(), MappingType{key, value});
  IncreaseSize(1);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(int child_page_index) -> bool {
  if (child_page_index < 0 || child_page_index >= GetSize()) {
    return false;
  }
  std::move(array_.begin() + child_page_index + 1, array_.begin() + GetSize(),
            array_.begin() + child_page_index);

  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *array, int min_size, int size) {
  std::copy(array + min_size, array + size, array_.begin());
  SetSize(size - min_size);
}

template class BPlusTreeLeafPage<int64_t, std::array<char, 16>, Comparator>;

}  // namespace mybplus
