#pragma once

#include <queue>
#include <string>

#include "b_plus_tree_page.h"

namespace mybplus {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  auto Init(int max_size = INTERNAL_PAGE_SIZE) -> void;

  auto KeyAt(int index) const -> KeyType;

  auto SetKeyAt(int index, const KeyType &key) -> void;

  auto SetValueAt(int index, const ValueType &value) -> void;

  auto ValueIndex(const ValueType &value) const -> int;

  auto ValueAt(int index) const -> ValueType;

  auto FindValue(const KeyType &key, const KeyComparator &comparator, int *child_page_index) const -> ValueType;

  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool;

  auto Delete(int child_page_index) -> bool;

  auto PopulateNewRoot(page_id_t page_id_one, const KeyType &key, page_id_t page_id_two) -> void;

  auto GetData() -> MappingType * { return array_.data(); }

  auto GetMinPageId() const -> page_id_t { return array_[0].second; }

  auto GetMaxPageId() const -> page_id_t { return array_[GetSize() - 1].second; }

  void CopyHalfFrom(MappingType *array, int min_size, int size) {
    // TODO: Copy half of the array to this page
    std::copy(array + min_size, array + size, array_.begin());
    SetSize(size - min_size);
  }

  void MergeFrom(MappingType *array, int size) {
    std::copy(array, array + size, array_.begin() + GetSize());
    SetSize(GetSize() + size);
  }

 private:
  std::vector<MappingType> array_;
};

}  // namespace mybplus
