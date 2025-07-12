
#include "b_plus_tree_page.h"

namespace mybplus {

auto BPlusTreePage::IsLeafPage() const -> bool {
  return page_type_ == IndexPageType::LEAF_PAGE ? true : false;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) {
  page_type_ = page_type;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
auto BPlusTreePage::GetSize() const -> int {
  return size_;
}
void BPlusTreePage::SetSize(int size) {
  size_ = size;
}
void BPlusTreePage::IncreaseSize(int amount) {
  int temp = size_ + amount;
  // BUSTUB_ENSURE(temp <= max_size_, "The size of page is bigger than max_size.");
  size_ = temp;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
auto BPlusTreePage::GetMaxSize() const -> int {
  return max_size_;
}
void BPlusTreePage::SetMaxSize(int size) {
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
auto BPlusTreePage::GetMinSize() const -> int {
  return max_size_ / 2;
}

// insert: true; delete: false;
auto BPlusTreePage::IsSafe(OperationType op_type) const -> bool {
  if (op_type == OperationType::INSERT) {
    return page_type_ == IndexPageType::LEAF_PAGE ? GetSize() < GetMaxSize() - 1
                                                  : GetSize() < GetMaxSize();
    // return GetSize() < GetMaxSize();
  }
  return page_type_ == IndexPageType::INTERNAL_PAGE ? GetSize() > GetMinSize() + 1
                                                    : GetSize() > GetMinSize();
}

// auto BPlusTreePage::GetParentPageId() const -> page_id_t { return parent_page_id_; }
// void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id;
// }

}  // namespace mybplus
