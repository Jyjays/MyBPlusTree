#ifndef B_PLUS_TREE_SERIALIZER_H
#define B_PLUS_TREE_SERIALIZER_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "b_plus_tree_wrapper.h"

#define MAGIC_NUMBER "MYBPTREE"
#define VERSION 1

// 文件头结构
typedef struct {
  char magic_number[8];
  uint32_t version;
  page_id_t root_page_id;
  int leaf_max_size;
  int internal_max_size;
  uint32_t page_count;
} FileHeader;

// 页面头结构
typedef struct {
  page_id_t page_id;
  uint8_t page_type;  // 1 for Leaf, 2 for Internal
  int size;
} PageHeader;

// 用于BFS
typedef struct QueueNode {
  page_id_t page_id;
  struct QueueNode *next;
} QueueNode;

typedef struct {
  QueueNode *front;
  QueueNode *rear;
} Queue;

// 序列化器结构
typedef struct {
  CBPlusTree *tree;
  char *storage_path;
} BPlusTreeSerializer;

Queue *queue_create();
void queue_push(Queue *q, page_id_t page_id);
page_id_t queue_pop(Queue *q);
bool queue_empty(Queue *q);
void queue_destroy(Queue *q);

BPlusTreeSerializer *serializer_create(CBPlusTree *tree, const char *storage_path);
void serializer_destroy(BPlusTreeSerializer *serializer);
bool serializer_serialize(BPlusTreeSerializer *serializer);
bool serializer_deserialize(BPlusTreeSerializer *serializer);
#endif  // B_PLUS_TREE_SERIALIZER_H
