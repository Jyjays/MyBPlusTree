#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include <chrono>
#include <random>
#include <algorithm>
#include "b_plus_tree.h"
#include "config.h"

namespace mybplus {
namespace test {

class BPlusTreePerformanceTest : public ::testing::Test {
protected:
    void SetUp() override {
        tree = std::make_unique<BPlusTree<KeyType, ValueType, KeyComparator>>("test_tree", comparator);
    }

    void TearDown() override {
        tree.reset();
    }

    std::unique_ptr<BPlusTree<KeyType, ValueType, KeyComparator>> tree;
    KeyComparator comparator;
};

TEST_F(BPlusTreePerformanceTest, InsertPerformance) {
    const int NUM_ITEMS = 10000;
    std::vector<KeyType> keys;
    
    // 生成随机键
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, NUM_ITEMS * 2);
    
    std::set<KeyType> unique_keys;
    while (unique_keys.size() < NUM_ITEMS) {
        unique_keys.insert(dis(gen));
    }
    
    keys.assign(unique_keys.begin(), unique_keys.end());
    std::shuffle(keys.begin(), keys.end(), gen);
    
    // 测量插入性能
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (const auto& key : keys) {
        ValueType value;
        std::string str = "v" + std::to_string(key);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(key, value));
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "插入 " << NUM_ITEMS << " 个元素耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "平均每个插入: " << (double)duration.count() / NUM_ITEMS << " ms" << std::endl;
    
    // 性能断言（应该在合理时间内完成）
    EXPECT_LT(duration.count(), 10000); // 应该在10秒内完成
}

TEST_F(BPlusTreePerformanceTest, SearchPerformance) {
    const int NUM_ITEMS = 5000;
    const int NUM_SEARCHES = 10000;
    std::vector<KeyType> keys;
    
    // 先插入数据
    for (int i = 0; i < NUM_ITEMS; ++i) {
        KeyType key = i * 2;
        ValueType value;
        std::string str = "v" + std::to_string(key);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(key, value));
        keys.push_back(key);
    }
    
    // 生成搜索序列
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, keys.size() - 1);
    
    std::vector<KeyType> search_keys;
    for (int i = 0; i < NUM_SEARCHES; ++i) {
        search_keys.push_back(keys[dis(gen)]);
    }
    
    // 测量搜索性能
    auto start_time = std::chrono::high_resolution_clock::now();
    
    int found_count = 0;
    for (const auto& key : search_keys) {
        std::vector<ValueType> results;
        if (tree->GetValue(key, &results)) {
            found_count++;
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "搜索 " << NUM_SEARCHES << " 次耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "平均每次搜索: " << (double)duration.count() / NUM_SEARCHES << " ms" << std::endl;
    std::cout << "找到的结果数: " << found_count << std::endl;
    
    EXPECT_EQ(found_count, NUM_SEARCHES); // 应该全部找到
    EXPECT_LT(duration.count(), 5000); // 应该在5秒内完成
}

TEST_F(BPlusTreePerformanceTest, MixedOperationPerformance) {
    const int NUM_OPERATIONS = 5000;
    std::vector<KeyType> inserted_keys;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_dis(1, NUM_OPERATIONS * 2);
    std::uniform_int_distribution<> op_dis(0, 1); // 0: insert, 1: search
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    int insert_count = 0;
    int search_count = 0;
    int found_count = 0;
    
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        KeyType key = key_dis(gen);
        int operation = op_dis(gen);
        
        if (operation == 0 || inserted_keys.empty()) {
            // 插入操作
            ValueType value;
            std::string str = "v" + std::to_string(key);
            std::strcpy(value.data(), str.c_str());
            
            if (std::find(inserted_keys.begin(), inserted_keys.end(), key) == inserted_keys.end()) {
                if (tree->Insert(key, value)) {
                    inserted_keys.push_back(key);
                    insert_count++;
                }
            }
        } else {
            // 搜索操作
            std::vector<ValueType> results;
            if (tree->GetValue(key, &results)) {
                found_count++;
            }
            search_count++;
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "混合操作 " << NUM_OPERATIONS << " 次耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "插入操作数: " << insert_count << std::endl;
    std::cout << "搜索操作数: " << search_count << std::endl;
    std::cout << "找到的结果数: " << found_count << std::endl;
    
    EXPECT_LT(duration.count(), 5000); // 应该在5秒内完成
}

TEST_F(BPlusTreePerformanceTest, SequentialInsertPerformance) {
    const int NUM_ITEMS = 10000;
    
    // 测量顺序插入性能
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < NUM_ITEMS; ++i) {
        ValueType value;
        std::string str = "v" + std::to_string(i);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(i, value));
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "顺序插入 " << NUM_ITEMS << " 个元素耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "平均每个插入: " << (double)duration.count() / NUM_ITEMS << " ms" << std::endl;
    
    EXPECT_LT(duration.count(), 5000); // 应该在5秒内完成
}

TEST_F(BPlusTreePerformanceTest, ReverseSequentialInsertPerformance) {
    const int NUM_ITEMS = 10000;
    
    // 测量逆序插入性能
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (int i = NUM_ITEMS - 1; i >= 0; --i) {
        ValueType value;
        std::string str = "v" + std::to_string(i);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(i, value));
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "逆序插入 " << NUM_ITEMS << " 个元素耗时: " << duration.count() << " ms" << std::endl;
    std::cout << "平均每个插入: " << (double)duration.count() / NUM_ITEMS << " ms" << std::endl;
    
    EXPECT_LT(duration.count(), 5000); // 应该在5秒内完成
}

TEST_F(BPlusTreePerformanceTest, MemoryUsageTest) {
    const int NUM_ITEMS = 1000;
    
    // 插入数据并观察内存使用
    for (int i = 0; i < NUM_ITEMS; ++i) {
        ValueType value;
        std::string str = "v" + std::to_string(i);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(i, value));
    }
    
    // 验证所有数据都能正确访问
    for (int i = 0; i < NUM_ITEMS; ++i) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(i, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "v" + std::to_string(i);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
    
    std::cout << "内存使用测试完成: " << NUM_ITEMS << " 个元素" << std::endl;
}

} // namespace test
} // namespace mybplus
