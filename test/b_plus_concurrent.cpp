#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
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

class BPlusTreeConcurrentOrderTest : public ::testing::Test {
 protected:
  const size_t scale_factor_ = 1000000;  // 1 million
  const int num_threads_ = 8;
  KeyComparator comparator_;
  std::vector<KeyType> keys_;

  // 在所有测试开始前准备好一份固定的随机数据
  void SetUp() override { keys_ = GenerateRandomKeys(scale_factor_); }
};

TEST_F(BPlusTreeConcurrentOrderTest, ConcurrentPerformanceComparison) {
  std::vector<int> orders_to_test = {32, 64, 128, 256, 512};

  std::cout << "\n\n--- B+Tree Concurrent Performance Comparison ---" << std::endl;
  std::cout << "Dataset size: " << scale_factor_ << " random keys, Threads: " << num_threads_
            << std::endl;
  std::cout << "--------------------------------------------------------------------" << std::endl;
  std::cout << "| Order (Max Size) | Concurrent Insert (ms) | Concurrent Lookup (ms) |"
            << std::endl;
  std::cout << "--------------------------------------------------------------------" << std::endl;

  for (int order : orders_to_test) {
    auto tree = std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
        "PerfTestTree", comparator_, order, order);

    // 1. 测试并发插入性能 (总共 100万次 Insert)
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
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // 每个线程分摊一部分工作
    size_t lookups_per_thread = keys_.size() / num_threads_;

    start_time = std::chrono::high_resolution_clock::now();
    LaunchThreads(num_threads_, [&](int thread_id) {
      // 计算每个线程负责查找的键的范围
      size_t start_index = thread_id * lookups_per_thread;
      size_t end_index =
          (thread_id == num_threads_ - 1) ? keys_.size() : start_index + lookups_per_thread;

      for (size_t i = start_index; i < end_index; ++i) {
        std::vector<ValueType> result_values;
        tree->GetValue(keys_[i], &result_values);
      }
    });
    end_time = std::chrono::high_resolution_clock::now();
    auto lookup_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // 3. 打印结果
    std::cout << "| " << std::setw(16) << std::left << order << "| " << std::setw(22) << std::left
              << insert_duration << "| " << std::setw(22) << std::left << lookup_duration << "|"
              << std::endl;
  }
  std::cout << "--------------------------------------------------------------------" << std::endl;
}
}  // namespace mybplus
