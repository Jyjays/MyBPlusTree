#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "b_plus_tree.h"
#include "config.h"

namespace mybplus {
namespace test {

void KeyToValue(KeyType key, ValueType &value) {
  std::string str = "val_" + std::to_string(key);
  // 清空数组以防止旧数据残留
  std::fill(value.begin(), value.end(), 0);
  std::strncpy(value.data(), str.c_str(), value.size() - 1);
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

std::vector<KeyType> GenerateUniqueKeys(size_t count) {
  std::vector<KeyType> keys;
  std::set<KeyType> unique_keys;

  std::random_device rd;
  std::mt19937 gen(rd());

  while (unique_keys.size() < count) {
    KeyType key = std::uniform_int_distribution<KeyType>(1, count * 10)(gen);
    if (unique_keys.find(key) == unique_keys.end()) {
      unique_keys.insert(key);
      keys.push_back(key);
    }
  }

  return keys;
}

void LaunchThreads(int num_threads, std::function<void(int)> task) {
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(task, i);
  }

  for (auto &thread : threads) {
    thread.join();
  }
}

class BPlusTreeDeleteTest : public ::testing::Test {
 protected:
  std::unique_ptr<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>> tree;
  KeyComparator comparator;

  void SetUp() override {
    tree = std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
        "DeleteTestTree", comparator, 5, 5);
  }
};

// 测试：简单删除，不触发结构调整
TEST_F(BPlusTreeDeleteTest, SimpleDelete) {
  // 插入 {1, 2, 3}，填满一个叶子节点
  for (KeyType i = 1; i <= 3; ++i) {
    ValueType v;
    KeyToValue(i, v);
    tree->Insert(i, v);
  }

  // 删除中间的键 2
  tree->Remove(2);

  std::vector<ValueType> result;
  // 验证 2 已被删除
  EXPECT_FALSE(tree->GetValue(2, &result));
  // 验证 1 和 3 仍然存在
  EXPECT_TRUE(tree->GetValue(1, &result));
  EXPECT_TRUE(tree->GetValue(3, &result));
  // 树中还剩两个元素
  EXPECT_FALSE(tree->IsEmpty());
}

TEST_F(BPlusTreeDeleteTest, DeleteCauseRedistribution) {
  auto keys = GenerateRandomKeys(1000);

  std::ofstream outfile("/home/jyjays/LAB/MyBPlusTree/test/delete_output.txt");
  if (!outfile.is_open()) {
    std::cerr << "无法打开文件 output.txt" << std::endl;
    return;
  }

  std::cout << "[SETUP] Inserting " << keys.size() << "unique keys... " << std::endl;
  for (const auto &key : keys) {
    ValueType v;
    KeyToValue(key, v);
    tree->Insert(key, v);
  }
  outfile << "[SETUP] Inserting " << keys.size() << " unique keys..." << std::endl;
  try {
    outfile << tree.get()->DrawBPlusTree() << std::endl;
  } catch (const std::exception &e) {
    outfile << "[ERROR] DrawBPlusTree failed: " << e.what() << std::endl;
  }

  // 验证初始插入
  std::cout << "[SETUP] Verifying initial insertion..." << std::endl;
  for (const auto &key : keys) {
    std::vector<ValueType> result;
    ASSERT_TRUE(tree->GetValue(key, &result)) << "Key " << key << " should exist after insertion.";
  }

  // 删除索引为偶数的键
  std::vector<KeyType> keys_to_delete;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i % 2 == 0) {
      keys_to_delete.push_back(keys[i]);
      tree->Remove(keys[i]);
      // std::cout << "[DELETE] Deleted key: " << keys[i] << std::endl;
      outfile << "[DELETE] Deleted key: " << keys[i] << std::endl;
      try {
        outfile << tree.get()->DrawBPlusTree() << std::endl;
      } catch (const std::exception &e) {
        outfile << "[ERROR] DrawBPlusTree failed: " << e.what() << std::endl;
      }
    }
  }
  // 验证删除
  std::cout << "[VERIFICATION] Verifying deletion..." << std::endl;
  for (const auto &key : keys_to_delete) {
    std::vector<ValueType> result;
    ASSERT_FALSE(tree->GetValue(key, &result)) << "Key " << key << " should be deleted.";
  }
  // 验证剩余键
  std::cout << "[VERIFICATION] Verifying remaining keys..." << std::endl;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (i % 2 != 0) {  // 保留奇数键
      std::vector<ValueType> result;
      ASSERT_TRUE(tree->GetValue(keys[i], &result)) << "Key " << keys[i] << " should still exist.";
    }
  }
}

class BPlusTreeConcurrentDeleteTest : public ::testing::Test {
 protected:
  const size_t scale_factor_ = 1000000;
  const int num_threads_ = 8;
  KeyComparator comparator_;
  std::unique_ptr<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>> tree_;
};

TEST_F(BPlusTreeConcurrentDeleteTest, ConcurrentDeleteAndVerify) {
  tree_ = std::make_unique<mybplus::BPlusTree<KeyType, ValueType, KeyComparator>>(
      "ConcurrentDeleteVerifyTree", comparator_, 5, 5);
  // 1. 生成唯一键并插入
  std::vector<KeyType> all_keys = GenerateUniqueKeys(scale_factor_);  // 确保唯一性
  std::cout << "[SETUP] Inserting " << all_keys.size() << " unique keys..." << std::endl;

  for (const auto &key : all_keys) {
    ValueType v;
    KeyToValue(key, v);
    tree_->Insert(key, v);
  }
  // 验证初始插入
  std::cout << "[SETUP] Verifying initial insertion..." << std::endl;
  for (const auto &key : all_keys) {
    std::vector<ValueType> result;
    ASSERT_TRUE(tree_->GetValue(key, &result)) << "Key " << key << " should exist after insertion.";
  }

  std::vector<KeyType> keys_to_delete;
  std::set<KeyType> keys_to_keep;

  for (size_t i = 0; i < all_keys.size(); ++i) {
    if (i % 2 == 0) {
      keys_to_delete.push_back(all_keys[i]);
    } else {
      keys_to_keep.insert(all_keys[i]);
    }
  }

  std::cout << "[SETUP] Keys to delete: " << keys_to_delete.size()
            << ", Keys to keep: " << keys_to_keep.size() << std::endl;

  // 3. 并发删除
  std::atomic<int> deletion_count{0};
  std::atomic<int> error_count{0};
  std::mutex error_mutex;
  std::vector<std::string> error_messages;

  auto delete_task = [&](int thread_id) {
    int local_deletions = 0;
    int local_errors = 0;

    try {
      for (size_t i = thread_id; i < keys_to_delete.size(); i += num_threads_) {
        tree_->Remove(keys_to_delete[i]);
        local_deletions++;
      }
    } catch (const std::exception &e) {
      local_errors++;
      std::lock_guard<std::mutex> lock(error_mutex);
      error_messages.push_back(std::string("Thread ") + std::to_string(thread_id) + ": " +
                               e.what());
    }

    deletion_count += local_deletions;
    error_count += local_errors;
  };

  std::cout << "[CONCURRENT TEST] Concurrently deleting " << keys_to_delete.size() << " keys using "
            << num_threads_ << " threads..." << std::endl;

  auto start_time = std::chrono::high_resolution_clock::now();
  LaunchThreads(num_threads_, delete_task);
  auto end_time = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  std::cout << "[CONCURRENT TEST] Concurrent deletion complete in " << duration.count() << "ms"
            << std::endl;
  std::cout << "[CONCURRENT TEST] Deletions performed: " << deletion_count << std::endl;
  std::cout << "[CONCURRENT TEST] Errors encountered: " << error_count << std::endl;

  if (!error_messages.empty()) {
    std::cout << "[ERROR] Errors during deletion:" << std::endl;
    for (const auto &msg : error_messages) {
      std::cout << "  " << msg << std::endl;
    }
  }
  std::cout << "[VERIFICATION] Verifying final state..." << std::endl;

  int deleted_verification_failures = 0;
  for (const auto &key : keys_to_delete) {
    std::vector<ValueType> result;
    if (tree_->GetValue(key, &result)) {
      deleted_verification_failures++;
      if (deleted_verification_failures <= 10) {  // 只打印前10个失败
        std::cout << "[ERROR] Key " << key << " should have been deleted but still exists."
                  << std::endl;
      }
    }
  }

  if (deleted_verification_failures > 0) {
    std::cout << "[ERROR] Total keys that should be deleted but still exist: "
              << deleted_verification_failures << std::endl;
  }

  int kept_verification_failures = 0;
  for (const auto &key : keys_to_keep) {
    std::vector<ValueType> result;
    ValueType expected_value;
    KeyToValue(key, expected_value);

    if (!tree_->GetValue(key, &result)) {
      kept_verification_failures++;
      if (kept_verification_failures <= 10) {
        std::cout << "[ERROR] Key " << key << " should still exist but was not found." << std::endl;
      }
    } else if (result.size() != 1) {
      kept_verification_failures++;
      if (kept_verification_failures <= 10) {
        std::cout << "[ERROR] Key " << key << " has unexpected result size: " << result.size()
                  << std::endl;
      }
    } else if (strcmp(result[0].data(), expected_value.data()) != 0) {
      kept_verification_failures++;
      if (kept_verification_failures <= 10) {
        std::cout << "[ERROR] Key " << key << " has incorrect value." << std::endl;
      }
    }
  }

  if (kept_verification_failures > 0) {
    std::cout << "[ERROR] Total keys that should be kept but failed verification: "
              << kept_verification_failures << std::endl;
  }

  // 最终断言
  ASSERT_EQ(error_count, 0) << "Errors occurred during concurrent deletion";
  ASSERT_EQ(deleted_verification_failures, 0) << "Some keys that should be deleted still exist";
  ASSERT_EQ(kept_verification_failures, 0) << "Some keys that should be kept failed verification";

  std::cout << "[VERIFICATION] Final state verification complete - ALL TESTS PASSED!" << std::endl;
}

}  // namespace test
}  // namespace mybplus
