#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "config.h"
namespace mybplus {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS \
  template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

// define operation type enum
enum class OperationType { FIND = 0, INSERT, DELETE };

class BPlusTreePage {
 public:
  virtual ~BPlusTreePage() = default;

  auto IsLeafPage() const -> bool;
  void SetPageType(IndexPageType page_type);

  auto GetSize() const -> int;
  void SetSize(int size);
  void IncreaseSize(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;
  auto IsSafe(OperationType op_type) const -> bool;

  // auto GetParentPageId() const -> page_id_t;
  // void SetParentPageId(page_id_t parent_page_id);

  auto GetPageId() const -> int32_t { return page_id_; }
  void SetPageId(int32_t page_id) { page_id_ = page_id; }

 private:
  int size_ __attribute__((__unused__));
  int max_size_ __attribute__((__unused__));
  IndexPageType page_type_ __attribute__((__unused__));
  int32_t page_id_ __attribute__((__unused__));
};

}  // namespace mybplus
