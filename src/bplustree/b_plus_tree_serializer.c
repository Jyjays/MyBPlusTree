#include "b_plus_tree_serializer.h"

Queue *queue_create() {
  Queue *q = (Queue *)malloc(sizeof(Queue));
  q->front = q->rear = NULL;
  return q;
}

void queue_push(Queue *q, page_id_t page_id) {
  QueueNode *node = (QueueNode *)malloc(sizeof(QueueNode));
  node->page_id = page_id;
  node->next = NULL;
  if (q->rear == NULL) {
    q->front = q->rear = node;
  } else {
    q->rear->next = node;
    q->rear = node;
  }
}

page_id_t queue_pop(Queue *q) {
  if (q->front == NULL) {
    return INVALID_PAGE_ID;
  }

  QueueNode *node = q->front;
  page_id_t page_id = node->page_id;
  q->front = q->front->next;

  if (q->front == NULL) {
    q->rear = NULL;
  }

  free(node);
  return page_id;
}

bool queue_empty(Queue *q) {
  return q->front == NULL;
}

void queue_destroy(Queue *q) {
  while (!queue_empty(q)) {
    queue_pop(q);
  }
  free(q);
}

BPlusTreeSerializer *serializer_create(CBPlusTree *tree, const char *storage_path) {
  BPlusTreeSerializer *serializer = (BPlusTreeSerializer *)malloc(sizeof(BPlusTreeSerializer));
  serializer->tree = tree;
  serializer->storage_path = strdup(storage_path);
  return serializer;
}

// 销毁序列化器
void serializer_destroy(BPlusTreeSerializer *serializer) {
  if (serializer) {
    free(serializer->storage_path);
    free(serializer);
  }
}

bool serializer_serialize(BPlusTreeSerializer *serializer) {
  FILE *file = fopen(serializer->storage_path, "wb");
  if (!file) {
    perror("Failed to open file for serialization");
    return false;
  }

  // 准备文件头
  FileHeader header;
  strncpy(header.magic_number, MAGIC_NUMBER, sizeof(MAGIC_NUMBER));
  header.version = VERSION;
  header.root_page_id = get_root_page_id(serializer->tree);
  header.leaf_max_size = get_leaf_max_size(serializer->tree);
  header.internal_max_size = get_internal_max_size(serializer->tree);
  header.page_count = get_page_count(serializer->tree);

  // 写入文件头
  if (fwrite(&header, sizeof(FileHeader), 1, file) != 1) {
    perror("Failed to write file header");
    fclose(file);
    return false;
  }

  if (header.root_page_id == INVALID_PAGE_ID) {
    fclose(file);
    return true;
  }

  Queue *queue = queue_create();
  // Set *visited = set_create();

  queue_push(queue, header.root_page_id);

  while (!queue_empty(queue)) {
    page_id_t current_page_id = queue_pop(queue);

    CBPlusTreePage *page = get_page(serializer->tree, current_page_id);
    if (!page) {
      continue;
    }

    // 写入页面头
    PageHeader page_header;
    page_header.page_id = page_get_id(page);
    page_header.page_type = page_is_leaf(page) ? 1 : 2;
    page_header.size = page_get_size(page);

    if (fwrite(&page_header, sizeof(PageHeader), 1, file) != 1) {
      perror("Failed to write page header");
      queue_destroy(queue);
      // set_destroy(visited);
      fclose(file);
      return false;
    }

    // 写入页面数据
    if (page_is_leaf(page)) {
      // 叶子页面：写入键值对
      for (int i = 0; i < page_header.size; i++) {
        KeyType key = leaf_page_get_key_at(page, i);
        ValueType value = leaf_page_get_value_at(page, i);

        if (fwrite(&key, sizeof(KeyType), 1, file) != 1 ||
            fwrite(&value, sizeof(ValueType), 1, file) != 1) {
          perror("Failed to write leaf page data");
          queue_destroy(queue);
          // set_destroy(visited);
          fclose(file);
          return false;
        }
      }

      // 写入下一页指针
      page_id_t next_page_id = leaf_page_get_next_id(page);
      if (fwrite(&next_page_id, sizeof(page_id_t), 1, file) != 1) {
        perror("Failed to write next page id");
        queue_destroy(queue);
        // set_destroy(visited);
        fclose(file);
        return false;
      }
    } else {
      // 内部页面：写入键和子页面指针
      for (int i = 0; i < page_header.size; i++) {
        page_id_t child_page_id = internal_page_get_value_at(page, i);

        if (fwrite(&child_page_id, sizeof(page_id_t), 1, file) != 1) {
          perror("Failed to write child page id");
          queue_destroy(queue);
          // set_destroy(visited);
          fclose(file);
          return false;
        }

        // 第一个位置没有键
        if (i > 0) {
          KeyType key = internal_page_get_key_at(page, i);
          if (fwrite(&key, sizeof(KeyType), 1, file) != 1) {
            perror("Failed to write internal page key");
            queue_destroy(queue);
            // set_destroy(visited);
            fclose(file);
            return false;
          }
        }

        // 将子页面加入队列
        queue_push(queue, child_page_id);
      }
    }
  }

  queue_destroy(queue);
  // set_destroy(visited);
  fclose(file);
  return true;
}

bool serializer_deserialize(BPlusTreeSerializer *serializer) {
  FILE *file = fopen(serializer->storage_path, "rb");
  if (!file) {
    perror("Deserialization failed: Unable to open file");
    return false;
  }
  FileHeader header;
  if (fread(&header, sizeof(FileHeader), 1, file) != 1 ||
      strncmp(header.magic_number, MAGIC_NUMBER, 8) != 0) {
    fprintf(stderr, "Deserialization failed: Invalid file format\n");
    fclose(file);
    return false;
  }

  CBPlusTree *tree = serializer->tree;
  bpt_clear(tree);
  bpt_set_meta(tree, header.root_page_id, header.leaf_max_size, header.internal_max_size);

  if (header.root_page_id == INVALID_PAGE_ID) {
    fclose(file);
    return true;
  }

  for (uint32_t i = 0; i < header.page_count; ++i) {
    PageHeader p_header;
    fread(&p_header, sizeof(PageHeader), 1, file);

    bpt_create_page_with_id(tree, p_header.page_id, p_header.page_type == 1);

    CBPlusTreePage *page = get_page(tree, p_header.page_id);
    bpt_page_set_size(page, p_header.size);

    if (p_header.page_type == 1) {  // Leaf
      for (int j = 0; j < p_header.size; ++j) {
        KeyType key;
        ValueType value;
        fread(&key, sizeof(KeyType), 1, file);
        fread(&value, sizeof(ValueType), 1, file);
        leaf_page_set_kv_at(page, j, key, value);
      }
      page_id_t next_page_id;
      fread(&next_page_id, sizeof(page_id_t), 1, file);
      leaf_page_set_next_id(page, next_page_id);
    } else {  // Internal
      for (int j = 0; j < p_header.size; ++j) {
        page_id_t child_id;
        fread(&child_id, sizeof(page_id_t), 1, file);
        internal_page_set_value_at(page, j, child_id);

        if (j > 0) {
          KeyType key;
          fread(&key, sizeof(KeyType), 1, file);
          internal_page_set_key_at(page, j, key);
        }
      }
    }
  }

  fclose(file);
  return true;
}
