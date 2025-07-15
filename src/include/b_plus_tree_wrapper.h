#ifndef BPLUS_TREE_WRAPPER
#define BPLUS_TREE_WRAPPER

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int64_t KeyType;
typedef struct {
  char data[16];
} ValueType;
typedef int32_t page_id_t;
#define INVALID_PAGE_ID -1

typedef struct CppBPlusTree CppBPlusTree;
typedef struct CppBPlusTreePage CppBPlusTreePage;

CppBPlusTree* bpt_create(int leaf_max_size, int internal_max_size);
void bpt_set_meta(CppBPlusTree* tree, page_id_t root_page_id, int leaf_max_size,
                  int internal_max_size);
void bpt_clear(CppBPlusTree* tree);
void bpt_destroy(CppBPlusTree* tree);

// 插入和查找的C接口
bool bpt_insert(CppBPlusTree* tree, KeyType key, ValueType value);
bool bpt_get_value(CppBPlusTree* tree, KeyType key, ValueType* out_value);

page_id_t get_root_page_id(CppBPlusTree* tree);
uint32_t get_page_count(const CppBPlusTree* tree);
int get_leaf_max_size(const CppBPlusTree* tree);
int get_internal_max_size(const CppBPlusTree* tree);
CppBPlusTreePage* get_page(CppBPlusTree* tree, page_id_t page_id);
void bpt_create_page_with_id(CppBPlusTree* tree, page_id_t page_id, bool is_leaf);
bool page_is_leaf(const CppBPlusTreePage* page);
int page_get_size(const CppBPlusTreePage* page);
page_id_t page_get_id(const CppBPlusTreePage* page);
void bpt_page_set_size(CppBPlusTreePage* page, int size);
// 叶子页面
KeyType leaf_page_get_key_at(const CppBPlusTreePage* page, int index);
ValueType leaf_page_get_value_at(const CppBPlusTreePage* page, int index);
page_id_t leaf_page_get_next_id(const CppBPlusTreePage* page);
void leaf_page_set_kv_at(CppBPlusTreePage* page, int index, KeyType key, ValueType value);
void leaf_page_set_next_id(CppBPlusTreePage* page, page_id_t next_page_id);

// 内部页面
KeyType internal_page_get_key_at(const CppBPlusTreePage* page, int index);
page_id_t internal_page_get_value_at(const CppBPlusTreePage* page, int index);
void internal_page_set_value_at(CppBPlusTreePage* page, int index, page_id_t value);
void internal_page_set_key_at(CppBPlusTreePage* page, int index, KeyType key);
#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // BPLUS_TREE_WRAPPER
