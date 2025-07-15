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

typedef struct CBPlusTree CBPlusTree;
typedef struct CBPlusTreePage CBPlusTreePage;

CBPlusTree* bpt_create(int leaf_max_size, int internal_max_size);
void bpt_set_meta(CBPlusTree* tree, page_id_t root_page_id, int leaf_max_size,
                  int internal_max_size);
void bpt_clear(CBPlusTree* tree);
void bpt_destroy(CBPlusTree* tree);

// 插入和查找的C接口
bool bpt_insert(CBPlusTree* tree, KeyType key, ValueType value);
bool bpt_get_value(CBPlusTree* tree, KeyType key, ValueType* out_value);

page_id_t get_root_page_id(CBPlusTree* tree);
uint32_t get_page_count(const CBPlusTree* tree);
int get_leaf_max_size(const CBPlusTree* tree);
int get_internal_max_size(const CBPlusTree* tree);
CBPlusTreePage* get_page(CBPlusTree* tree, page_id_t page_id);
void bpt_create_page_with_id(CBPlusTree* tree, page_id_t page_id, bool is_leaf);
bool page_is_leaf(const CBPlusTreePage* page);
int page_get_size(const CBPlusTreePage* page);
page_id_t page_get_id(const CBPlusTreePage* page);
void bpt_page_set_size(CBPlusTreePage* page, int size);
// 叶子页面
KeyType leaf_page_get_key_at(const CBPlusTreePage* page, int index);
ValueType leaf_page_get_value_at(const CBPlusTreePage* page, int index);
page_id_t leaf_page_get_next_id(const CBPlusTreePage* page);
void leaf_page_set_kv_at(CBPlusTreePage* page, int index, KeyType key, ValueType value);
void leaf_page_set_next_id(CBPlusTreePage* page, page_id_t next_page_id);

// 内部页面
KeyType internal_page_get_key_at(const CBPlusTreePage* page, int index);
page_id_t internal_page_get_value_at(const CBPlusTreePage* page, int index);
void internal_page_set_value_at(CBPlusTreePage* page, int index, page_id_t value);
void internal_page_set_key_at(CBPlusTreePage* page, int index, KeyType key);
#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // BPLUS_TREE_WRAPPER
