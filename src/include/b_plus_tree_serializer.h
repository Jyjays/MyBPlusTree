#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "b_plus_tree.h"
#include "config.h"

namespace mybplus {

// 定义文件头的结构，用于存储元数据
struct FileHeader {
  char magic_number_[8] = {'M', 'Y', 'B', 'P', 'T', 'R', 'E', 'E'};
  uint32_t version_ = 1;
  page_id_t root_page_id_ = INVALID_PAGE_ID;
  int leaf_max_size_ = 0;
  int internal_max_size_ = 0;
  uint32_t page_count_ = 0;
};

// 定义页面数据块的头部
struct PageHeader {
  page_id_t page_id_ = INVALID_PAGE_ID;
  uint8_t page_type_ = 0;  // 1 for Leaf, 2 for Internal
  int size_ = 0;
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeSerializer {
  using BPlusTreeType = BPlusTree<KeyType, ValueType, KeyComparator>;
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  BPlusTreeSerializer(BPlusTreeType &tree, const std::string &storage_path)
      : tree_(tree), storage_path_(storage_path) {}

  auto Serialize() -> bool {
    // 以二进制写模式打开文件
    std::ofstream out_file(storage_path_, std::ios::binary);
    if (!out_file.is_open()) {
      std::cerr << "Error: Cannot open file for writing: " << storage_path_ << std::endl;
      return false;
    }

    // 1. 准备并写入文件头
    FileHeader header;
    header.root_page_id_ = tree_.GetRootPageId();
    header.leaf_max_size_ = tree_.GetLeafMaxSize();
    header.internal_max_size_ = tree_.GetInternalMaxSize();
    header.page_count_ = tree_.GetPageCount();

    out_file.write(reinterpret_cast<const char *>(&header), sizeof(FileHeader));

    // 如果树为空，直接结束
    if (header.root_page_id_ == INVALID_PAGE_ID) {
      out_file.close();
      return true;
    }

    // 2. 使用广度优先搜索(BFS)遍历并写入所有页面
    std::queue<page_id_t> q;
    std::set<page_id_t> visited;

    q.push(header.root_page_id_);
    visited.insert(header.root_page_id_);

    while (!q.empty()) {
      page_id_t current_page_id = q.front();
      q.pop();

      BPlusTreePage *page = tree_.GetPage(current_page_id);
      if (page == nullptr) {
        continue;  // 不应该发生，但作为健壮性检查
      }

      // 写入页面数据块的头部
      PageHeader p_header;
      p_header.page_id_ = page->GetPageId();
      p_header.size_ = page->GetSize();

      if (page->IsLeafPage()) {
        p_header.page_type_ = 1;  // 1 = Leaf
        out_file.write(reinterpret_cast<const char *>(&p_header), sizeof(PageHeader));

        LeafPage *leaf_page = static_cast<LeafPage *>(page);
        // 写入键和值
        for (int i = 0; i < leaf_page->GetSize(); ++i) {
          KeyType key = leaf_page->KeyAt(i);
          ValueType value = leaf_page->ValueAt(i);
          out_file.write(reinterpret_cast<const char *>(&key), sizeof(KeyType));
          out_file.write(reinterpret_cast<const char *>(&value), sizeof(ValueType));
        }
        // 写入兄弟节点ID
        page_id_t next_page_id = leaf_page->GetNextPageId();
        out_file.write(reinterpret_cast<const char *>(&next_page_id), sizeof(page_id_t));

      } else {                    // Internal Page
        p_header.page_type_ = 2;  // 2 = Internal
        out_file.write(reinterpret_cast<const char *>(&p_header), sizeof(PageHeader));

        InternalPage *internal_page = static_cast<InternalPage *>(page);
        // 写入键和子节点ID
        for (int i = 0; i < internal_page->GetSize(); ++i) {
          // 注意：内部节点的ValueType是page_id_t
          KeyType key = internal_page->KeyAt(i);
          page_id_t value = internal_page->ValueAt(i);
          out_file.write(reinterpret_cast<const char *>(&key), sizeof(KeyType));
          out_file.write(reinterpret_cast<const char *>(&value), sizeof(page_id_t));

          // 将未访问过的子节点加入队列
          page_id_t child_page_id = internal_page->ValueAt(i);
          if (visited.find(child_page_id) == visited.end()) {
            q.push(child_page_id);
            visited.insert(child_page_id);
          }
        }
      }
    }

    out_file.close();
    return true;
  }

  auto Deserialize() -> bool {
    // 以二进制读模式打开文件
    std::ifstream in_file(storage_path_, std::ios::binary);
    if (!in_file.is_open()) {
      std::cerr << "错误: 无法打开文件进行读取: " << storage_path_ << std::endl;
      return false;
    }

    // 读取并验证文件头
    FileHeader header;
    in_file.read(reinterpret_cast<char *>(&header), sizeof(FileHeader));
    if (std::string(header.magic_number_, 8) != "MYBPTREE") {
      std::cerr << "错误: 无效的文件格式。" << std::endl;
      return false;
    }

    // 清空树并设置基本属性
    tree_.Clear();
    tree_.SetRootPageId(header.root_page_id_);
    tree_.SetLeafMaxSize(header.leaf_max_size_);
    tree_.SetInternalMaxSize(header.internal_max_size_);

    // 如果是空树，直接返回
    if (header.root_page_id_ == INVALID_PAGE_ID) {
      in_file.close();
      return true;
    }

    for (uint32_t i = 0; i < header.page_count_; ++i) {
      // 读取页面头部
      PageHeader p_header;
      in_file.read(reinterpret_cast<char *>(&p_header), sizeof(PageHeader));
      // 创建页面
      tree_.CreateAndRegisterPage(p_header.page_id_, p_header.page_type_ == 1);

      // 获取刚创建的页面
      BPlusTreePage *page = tree_.GetPage(p_header.page_id_);
      if (page == nullptr) {
        std::cerr << "反序列化错误: 无法创建页面 " << p_header.page_id_ << std::endl;
        in_file.close();
        return false;
      }

      page->SetSize(p_header.size_);

      if (p_header.page_type_ == 1) {  // Leaf Page
        auto *leaf_page = static_cast<LeafPage *>(page);
        for (int j = 0; j < p_header.size_; ++j) {
          KeyType key;
          ValueType value;
          in_file.read(reinterpret_cast<char *>(&key), sizeof(KeyType));
          in_file.read(reinterpret_cast<char *>(&value), sizeof(ValueType));
          leaf_page->SetAt(j, key, value);
        }

        page_id_t next_page_id;
        in_file.read(reinterpret_cast<char *>(&next_page_id), sizeof(page_id_t));
        leaf_page->SetNextPageId(next_page_id);

      } else {  // Internal Page
        auto *internal_page = static_cast<InternalPage *>(page);
        for (int j = 0; j < p_header.size_; ++j) {
          KeyType key;
          page_id_t child_page_id;
          in_file.read(reinterpret_cast<char *>(&key), sizeof(KeyType));
          in_file.read(reinterpret_cast<char *>(&child_page_id), sizeof(page_id_t));
          internal_page->SetKeyAt(j, key);
          internal_page->SetValueAt(j, child_page_id);
        }
      }
    }

    in_file.close();
    return true;
  }

 private:
  BPlusTreeType &tree_;
  std::string storage_path_;
};

}  // namespace mybplus
