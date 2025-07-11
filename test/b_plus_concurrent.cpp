#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <numeric>
#include <iomanip>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "b_plus_tree.h"
#include "config.h"

namespace mybplus {
template <typename... Args>
void LaunchThreads(int num_threads, Args &&...args) {
  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(args..., i);
  }
  for (auto &thread : threads) {
    thread.join();
  }
}

// 生成一个从 start 开始的连续整数序列
std::vector<KeyType> GenerateSequentialKeys(size_t count, KeyType start = 1) {
  std::vector<KeyType> keys(count);
  std::iota(keys.begin(), keys.end(), start);
  return keys;
}

// 生成一个指定数量的随机整数序列
std::vector<KeyType> GenerateRandomKeys(size_t count) {
  std::vector<KeyType> keys = GenerateSequentialKeys(count);
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);
  return keys;
}

// 将一个整数键转换为一个可验证的字符串值
void KeyToValue(KeyType key, ValueType &value) {
  std::string str = "val_" + std::to_string(key);
  strncpy(value.data(), str.c_str(), value.size() - 1);
  std::strncpy(value.data(), str.c_str(), value.size() - 1);
}

// ==================================================================================
// 考核要求 2 & 5: 大量数据并发稳定测试
// ==================================================================================

class BPlusTreeConcurrentTest : public ::testing::Test {
 protected:
  const size_t scale_factor_ = 2000;  // 20万，并发测试下规模可以适当减小
  const int num_threads_ = 4;           // 定义测试使用的线程数
  KeyComparator comparator_;
  std::unique_ptr<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>> tree_;

  void SetUp() override {
    tree_ =
        std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
            "ConcurrentTestTree", comparator_, 128, 128);
  }
};

// 测试1：大规模并发顺序插入与验证
TEST_F(BPlusTreeConcurrentTest, ConcurrentSequentialInsertAndVerify) {
  std::vector<KeyType> keys = GenerateSequentialKeys(scale_factor_);

  // 定义插入任务，每个线程只插入自己负责的键
  auto insert_task = [&](int thread_id) {
    for (size_t i = thread_id; i < keys.size(); i += num_threads_) {
      KeyType key = keys[i];
      ValueType value;
      KeyToValue(key, value);
      ASSERT_TRUE(tree_->Insert(key, value));
    }
  };

  std::cout << "\n[CONCURRENT TEST] Inserting " << scale_factor_
            << " sequential keys using " << num_threads_ << " threads..."
            << std::endl;
  LaunchThreads(num_threads_, insert_task);
  std::cout << "[CONCURRENT TEST] Insertion complete." << std::endl;

  std::cout << "[CONCURRENT TEST] Verifying all keys exist..." << std::endl;
  for (const auto &key : keys) {
    std::vector<ValueType> result_values;
    ValueType expected_value;
    KeyToValue(key, expected_value);
    ASSERT_TRUE(tree_->GetValue(key, &result_values));
    ASSERT_EQ(result_values.size(), 1);
    ASSERT_STREQ(result_values[0].data(), expected_value.data());
  }
  std::cout << "[CONCURRENT TEST] Verification complete." << std::endl;
}

// 测试2：大规模并发随机插入与删除
TEST_F(BPlusTreeConcurrentTest, DISABLED_MixedConcurrentReadWrite) {
  // 1. 先串行插入一批基础数据
  size_t initial_keys_count = scale_factor_ / 2;
  std::vector<KeyType> initial_keys = GenerateRandomKeys(initial_keys_count);
  for (const auto &key : initial_keys) {
    ValueType value;
    KeyToValue(key, value);
    tree_->Insert(key, value);
  }

  // 2. 准备并发操作的数据
  size_t dynamic_keys_count = scale_factor_ / 2;
  std::vector<KeyType> insert_keys =
      GenerateSequentialKeys(dynamic_keys_count, initial_keys_count + 1);
  std::vector<KeyType> delete_keys = initial_keys;  // 删除之前插入的键

  std::atomic<bool> success = true;

  // 定义插入、删除和查找任务
  auto insert_task = [&](int thread_id) {
    for (size_t i = thread_id; i < insert_keys.size(); i += num_threads_ / 2) {
      KeyType key = insert_keys[i];
      ValueType value;
      KeyToValue(key, value);
      if (!tree_->Insert(key, value)) {
        success = false;
      }
      // std::cout<< "[Thread " << thread_id
      //           << "] Inserted key: " << key << std::endl;
    }
  };

  auto delete_task = [&](int thread_id) {
    for (size_t i = thread_id; i < delete_keys.size(); i += num_threads_ / 2) {
      tree_->Remove(delete_keys[i]);
    }
    // std::cout << "[Thread " << thread_id
    //           << "] Deleted keys up to: " << delete_keys.size() << std::endl;
  };

  // 3. 并发执行插入和删除
  std::cout << "\n[CONCURRENT TEST] Starting mixed insert/delete operations..."
            << std::endl;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads_ / 2; ++i) {
    threads.emplace_back(insert_task, i);
    threads.emplace_back(delete_task, i);
  }
  for (auto &thread : threads) {
    thread.join();
  }
  std::cout << "[CONCURRENT TEST] Mixed operations complete." << std::endl;
  ASSERT_TRUE(success);

  // 4. 验证最终状态
  std::cout << "[CONCURRENT TEST] Verifying final state..." << std::endl;
  // 验证被删除的键确实不存在
  for (const auto &key : delete_keys) {
    std::vector<ValueType> result_values;
    ASSERT_FALSE(tree_->GetValue(key, &result_values));
  }
  // 验证新插入的键确实存在
  for (const auto &key : insert_keys) {
    std::vector<ValueType> result_values;
    ASSERT_TRUE(tree_->GetValue(key, &result_values));
  }
  std::cout << "[CONCURRENT TEST] Final state verification complete."
            << std::endl;
}

// ==================================================================================
// 考核要求 3 & 5: B+树阶数并发性能测试
// ==================================================================================

class BPlusTreeConcurrentOrderTest : public ::testing::Test {
 protected:
  const size_t scale_factor_ = 1000000;  // 1 million
  const int num_threads_ = 8;
  KeyComparator comparator_;
  std::vector<KeyType> keys_;

  void SetUp() override { keys_ = GenerateRandomKeys(scale_factor_); }
};

TEST_F(BPlusTreeConcurrentOrderTest, ConcurrentPerformanceComparison) {
  std::vector<int> orders_to_test = {32, 64, 128, 256, 512};

  std::cout << "\n\n--- B+Tree Concurrent Performance Comparison ---"
            << std::endl;
  std::cout << "Dataset size: " << scale_factor_
            << " random keys, Threads: " << num_threads_ << std::endl;
  std::cout
      << "--------------------------------------------------------------------"
      << std::endl;
  std::cout << "| Order (Max Size) | Concurrent Insert (ms) | Concurrent "
               "Lookup (ms) |"
            << std::endl;
  std::cout
      << "--------------------------------------------------------------------"
      << std::endl;

  for (int order : orders_to_test) {
    auto tree =
        std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
            "PerfTestTree", comparator_, order, order);

    // 1. 测试并发插入性能
    auto start_time = std::chrono::high_resolution_clock::now();
    LaunchThreads(num_threads_, [&](int thread_id) {
      for (size_t i = thread_id; i < keys_.size(); i += num_threads_) {
        ValueType value;
        KeyToValue(keys_[i], value);
        tree->Insert(keys_[i], value);
      }
    });
    auto end_time = std::chrono::high_resolution_clock::now();
    auto insert_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count();

    // 2. 测试并发查找性能
    size_t lookup_count_per_thread = 10000;
    start_time = std::chrono::high_resolution_clock::now();
    LaunchThreads(num_threads_, [&](int thread_id) {
      for (size_t i = 0; i < lookup_count_per_thread; ++i) {
        std::vector<ValueType> result_values;
        // 每个线程查找不同的键，避免缓存效应过于理想化
        size_t key_index =
            (thread_id * lookup_count_per_thread + i) % keys_.size();
        tree->GetValue(keys_[key_index], &result_values);
      }
    });
    end_time = std::chrono::high_resolution_clock::now();
    auto lookup_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count();

    // 3. 打印结果
    std::cout << "| " << std::setw(16) << std::left << order << "| "
              << std::setw(22) << std::left << insert_duration << "| "
              << std::setw(22) << std::left << lookup_duration << "|"
              << std::endl;
  }
  std::cout
      << "--------------------------------------------------------------------"
      << std::endl;
}
}  // namespace mybplus