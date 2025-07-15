#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "config.h"
namespace mybplus {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS \
  template <typename KeyType, typename ValueType, typename KeyComparator>
#ifdef USING_CRABBING_PROTOCOL
#define PAGE_HEADER_SIZE                                                     \
  (sizeof(int32_t) + sizeof(int) + sizeof(IndexPageType) + sizeof(int32_t) + \
   sizeof(std::shared_mutex))
#else
#define PAGE_HEADER_SIZE (sizeof(int32_t) + sizeof(int) + sizeof(IndexPageType) + sizeof(int32_t))
#endif
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
  virtual void SetSize(int size);

  virtual void IncreaseSize(int amount);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);
  auto GetMinSize() const -> int;
  auto IsSafe(OperationType op_type) const -> bool;

  // auto GetParentPageId() const -> page_id_t;
  // void SetParentPageId(page_id_t parent_page_id);

  auto GetPageId() const -> int32_t { return page_id_; }
  void SetPageId(int32_t page_id) { page_id_ = page_id; }

#ifdef USING_CRABBING_PROTOCOL
  auto WLock() const -> void { mutex_.lock(); }
  auto RLock() const -> void { mutex_.lock_shared(); }
  auto Unlock() const -> void { mutex_.unlock(); }
  auto RUnlock() const -> void { mutex_.unlock_shared(); }
#endif

 private:
  int size_;
  int max_size_;
  IndexPageType page_type_;
  int32_t page_id_;

#ifdef USING_CRABBING_PROTOCOL
  mutable std::shared_mutex mutex_;
#endif
};

}  // namespace mybplus
