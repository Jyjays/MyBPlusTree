#pragma once

#include <string>
#include <utility>
#include <vector>

#include "b_plus_tree_page.h"
#include "config.h"

namespace mybplus {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>

#define LEAF_PAGE_HEADER_SIZE PAGE_HEADER_SIZE + sizeof(page_id_t) + sizeof(int32_t)

#define LEAF_PAGE_SIZE ((PAGE_SIZE - sizeof(int32_t) - sizeof(int)) / (sizeof(MappingType)))
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  void Init(int max_size = LEAF_PAGE_SIZE);

  // Helper methods
  auto GetNextPageId() const -> page_id_t;

  void SetNextPageId(page_id_t next_page_id);

  auto KeyAt(int index) const -> KeyType;

  auto ValueAt(int index) const -> ValueType;

  void SetAt(int index, const KeyType &key, const ValueType &value);

  auto FindValue(const KeyType &key, const KeyComparator &comparator, ValueType &value,
                 int *child_page_index) const -> bool;

  auto GetData() -> MappingType * { return array_.data(); }

  void CopyHalfFrom(MappingType *array, int min_size, int size);

  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool;

  auto Delete(int child_page_index) -> bool;

  void MergeFrom(MappingType *array, int size) {
    std::copy(array, array + size, array_.begin() + GetSize());
    SetSize(GetSize() + size);
  }

 private:
  page_id_t next_page_id_;
  std::vector<MappingType> array_;
};

}  // namespace mybplus
