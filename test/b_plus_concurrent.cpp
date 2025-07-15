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
void LaunchThreads(int num_threads, Args&&... args) {
  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(args..., i);
  }
  for (auto& thread : threads) {
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
void KeyToValue(KeyType key, ValueType& value) {
  std::string str = "val_" + std::to_string(key);
  strncpy(value.data(), str.c_str(), value.size() - 1);
  std::strncpy(value.data(), str.c_str(), value.size() - 1);
}

class BPlusTreeConcurrentOrderTest : public ::testing::Test {
 protected:
  const size_t scale_factor_ = 100000;
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
  std::cout << "-----------------------------------------------------------------------------------"
               "------------"
            << std::endl;
  std::cout << "| Order (Max Size) | Concurrent Insert (ms) | Concurrent Lookup (ms) | Concurrent "
               "Delete (ms) |"
            << std::endl;
  std::cout << "-----------------------------------------------------------------------------------"
               "------------"
            << std::endl;

  for (int order : orders_to_test) {
    auto tree = std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
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

    // 2. 测试并发删除性能
    start_time = std::chrono::high_resolution_clock::now();
    LaunchThreads(num_threads_, [&](int thread_id) {
      for (size_t i = thread_id; i < keys_.size(); i += num_threads_) {
        tree->Remove(keys_[i]);
      }
    });
    end_time = std::chrono::high_resolution_clock::now();
    auto delete_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

    // 3. 打印结果
    std::cout << "| " << std::setw(16) << std::left << order << "| " << std::setw(22) << std::left
              << insert_duration << "| " << std::setw(22) << std::left << lookup_duration << "|"
              << std::setw(22) << std::left << delete_duration << "|" << std::endl;
  }
  std::cout << "-----------------------------------------------------------------------------------"
               "------------"
            << std::endl;
}
TEST_F(BPlusTreeConcurrentOrderTest, DISABLED_ConcurrentRandomOperationsTest) {
  const size_t test_scale = 100000;  // 测试规模
  const int test_threads = 8;        // 测试线程数
  const int tree_order = 64;         // B+树阶数

  // 创建B+树实例
  auto tree = std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
      "RandomOpsTestTree", comparator_, tree_order, tree_order);

  // 准备测试数据
  std::vector<KeyType> test_keys = GenerateSequentialKeys(test_scale);

  // 用于跟踪插入状态的线程安全容器
  std::atomic<size_t> total_insertions{0};
  std::atomic<size_t> total_deletions{0};
  std::atomic<size_t> total_lookups{0};
  std::atomic<size_t> successful_lookups{0};

  // 记录操作结果的容器（用于验证）
  std::vector<std::atomic<bool>> key_exists(test_scale + 1);
  for (size_t i = 0; i <= test_scale; ++i) {
    key_exists[i].store(false);
  }

  std::cout << "\n--- B+Tree Concurrent Random Operations Test ---" << std::endl;
  std::cout << "Test scale: " << test_scale << " keys, Threads: " << test_threads << std::endl;
  std::cout << "Each thread performs random INSERT/DELETE/LOOKUP operations" << std::endl;

  auto start_time = std::chrono::high_resolution_clock::now();

  // 启动并发测试线程
  LaunchThreads(test_threads, [&](int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd() + thread_id);             // 每个线程使用不同的随机种子
    std::uniform_int_distribution<> op_dist(0, 2);  // 0=INSERT, 1=DELETE, 2=LOOKUP
    std::uniform_int_distribution<> key_dist(1, test_scale);

    const int operations_per_thread = 10000;  // 每个线程执行的操作数

    for (int op = 0; op < operations_per_thread; ++op) {
      KeyType key = key_dist(gen);
      int operation = op_dist(gen);

      switch (operation) {
        case 0: {  // INSERT 操作
          ValueType value;
          KeyToValue(key, value);

          bool insert_success = tree->Insert(key, value);
          if (insert_success) {
            total_insertions.fetch_add(1);
            key_exists[key].store(true);
          }
          break;
        }

        case 1: {  // DELETE 操作
          tree->Remove(key);
          total_deletions.fetch_add(1);
          key_exists[key].store(false);
          break;
        }

        case 2: {  // LOOKUP 操作
          std::vector<ValueType> result_values;
          bool lookup_success = tree->GetValue(key, &result_values);

          total_lookups.fetch_add(1);
          if (lookup_success && !result_values.empty()) {
            successful_lookups.fetch_add(1);

            // 验证查找到的值是否正确
            ValueType expected_value;
            KeyToValue(key, expected_value);

            bool value_correct = false;
            for (const auto& found_value : result_values) {
              if (std::string(found_value.data()) == std::string(expected_value.data())) {
                value_correct = true;
                break;
              }
            }

            if (!value_correct) {
              std::cerr << "Thread " << thread_id << ": Value mismatch for key " << key
                        << std::endl;
            }
          }
          break;
        }
      }

      // 每执行一定数量的操作后，进行一次一致性验证
      if (op % 1000 == 0) {
        // 随机选择一些键进行验证
        for (int verify_count = 0; verify_count < 10; ++verify_count) {
          KeyType verify_key = key_dist(gen);
          std::vector<ValueType> verify_results;
          bool found = tree->GetValue(verify_key, &verify_results);

          // 检查查找结果与预期状态的一致性
          // 注意：由于并发操作，这里的检查可能不完全准确，但可以捕捉明显的错误
          if (found && verify_results.empty()) {
            std::cerr << "Thread " << thread_id
                      << ": Inconsistent state - found but empty results for key " << verify_key
                      << std::endl;
          }
        }
      }
    }
  });

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  // 打印统计信息
  std::cout << "\n--- Test Results ---" << std::endl;
  std::cout << "Total execution time: " << duration << " ms" << std::endl;
  std::cout << "Total insertions: " << total_insertions.load() << std::endl;
  std::cout << "Total deletions: " << total_deletions.load() << std::endl;
  std::cout << "Total lookups: " << total_lookups.load() << std::endl;
  std::cout << "Successful lookups: " << successful_lookups.load() << std::endl;
  std::cout << "Lookup success rate: " << std::fixed << std::setprecision(2)
            << (total_lookups.load() > 0
                    ? (double)successful_lookups.load() / total_lookups.load() * 100
                    : 0)
            << "%" << std::endl;

  // 最终一致性验证
  std::cout << "\n--- Final Consistency Check ---" << std::endl;
  size_t final_verification_errors = 0;
  size_t keys_to_verify = std::min(test_scale, size_t(1000));  // 验证前1000个键

  for (size_t i = 1; i <= keys_to_verify; ++i) {
    std::vector<ValueType> result_values;
    bool found = tree->GetValue(i, &result_values);

    if (found && result_values.empty()) {
      final_verification_errors++;
      std::cout << "Verification error: Key " << i << " found but no values returned" << std::endl;
    }

    if (found && !result_values.empty()) {
      // 验证值的正确性
      ValueType expected_value;
      KeyToValue(i, expected_value);

      bool value_correct = false;
      for (const auto& found_value : result_values) {
        if (std::string(found_value.data()) == std::string(expected_value.data())) {
          value_correct = true;
          break;
        }
      }

      if (!value_correct) {
        final_verification_errors++;
        std::cout << "Verification error: Key " << i << " has incorrect value" << std::endl;
      }
    }
  }

  std::cout << "Keys verified: " << keys_to_verify << std::endl;
  std::cout << "Verification errors: " << final_verification_errors << std::endl;

  // 测试断言
  EXPECT_EQ(final_verification_errors, 0)
      << "Found " << final_verification_errors << " consistency errors in final verification";

  EXPECT_GT(total_insertions.load(), 0) << "No insertions were performed";
  EXPECT_GT(total_lookups.load(), 0) << "No lookups were performed";

  std::cout << "--- Test Completed ---" << std::endl;
}

TEST_F(BPlusTreeConcurrentOrderTest, DISABLED_ConcurrentStressTest) {
  const size_t stress_scale = 50000;
  const int stress_threads = 16;
  const int tree_order = 128;

  auto tree = std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
      "StressTestTree", comparator_, tree_order, tree_order);

  std::atomic<size_t> total_operations{0};
  std::atomic<size_t> operation_errors{0};

  std::cout << "\n--- B+Tree Concurrent Stress Test ---" << std::endl;
  std::cout << "Stress scale: " << stress_scale << " keys, Threads: " << stress_threads
            << std::endl;

  auto start_time = std::chrono::high_resolution_clock::now();

  LaunchThreads(stress_threads, [&](int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd() + thread_id * 1000);
    std::uniform_int_distribution<> op_dist(0, 2);
    std::uniform_int_distribution<> key_dist(1, stress_scale);

    const int operations_per_thread = 20000;

    for (int op = 0; op < operations_per_thread; ++op) {
      KeyType key = key_dist(gen);
      int operation = op_dist(gen);

      try {
        switch (operation) {
          case 0: {  // INSERT
            ValueType value;
            KeyToValue(key, value);
            tree->Insert(key, value);
            break;
          }

          case 1: {  // DELETE
            tree->Remove(key);
            break;
          }

          case 2: {  // LOOKUP
            std::vector<ValueType> result_values;
            tree->GetValue(key, &result_values);
            break;
          }
        }
        total_operations.fetch_add(1);
      } catch (const std::exception& e) {
        operation_errors.fetch_add(1);
        std::cerr << "Thread " << thread_id << " operation error: " << e.what() << std::endl;
      }
    }
  });

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  std::cout << "Stress test completed in " << duration << " ms" << std::endl;
  std::cout << "Total operations: " << total_operations.load() << std::endl;
  std::cout << "Operation errors: " << operation_errors.load() << std::endl;
  std::cout << "Operations per second: " << (total_operations.load() * 1000 / duration)
            << std::endl;

  EXPECT_EQ(operation_errors.load(), 0)
      << "Found " << operation_errors.load() << " operation errors during stress test";
}
}  // namespace mybplus
