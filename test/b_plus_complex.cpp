#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include <algorithm>
#include <random>
#include <set>
#include <unistd.h>  // for getpid()
#include <fstream>   // for ifstream
#include <cstdio>    // for std::remove
#include "b_plus_tree.h"
#include "config.h"
#include "b_plus_tree_serializer.h"
namespace mybplus {
namespace test {

class BPlusTreeComplexTest : public ::testing::Test {
protected:
    void SetUp() override {
        tree = std::make_unique<BPlusTree<KeyType, ValueType, KeyComparator>>("test_tree", comparator, 3, 3);
    }

    void TearDown() override {
        tree.reset();
    }

    std::unique_ptr<BPlusTree<KeyType, ValueType, KeyComparator>> tree;
    KeyComparator comparator;
};

TEST_F(BPlusTreeComplexTest, LargeDataSet) {
    const int NUM_ITEMS = 1000;
    std::set<KeyType> inserted_keys;
    
    // 插入大量数据
    for (int i = 0; i < NUM_ITEMS; ++i) {
        KeyType key = i * 2; // 使用偶数键
        ValueType value;
        std::string str = "value_" + std::to_string(key);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(key, value));
        inserted_keys.insert(key);
    }
    
    // 验证所有插入的键都能找到
    for (const auto& key : inserted_keys) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(key, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "value_" + std::to_string(key);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
    
    // 验证不存在的键找不到
    for (int i = 1; i < NUM_ITEMS * 2; i += 2) { // 奇数键
        std::vector<ValueType> results;
        EXPECT_FALSE(tree->GetValue(i, &results));
    }
}

TEST_F(BPlusTreeComplexTest, RandomInsertOrder) {
    const int NUM_ITEMS = 500;
    std::vector<KeyType> keys;
    
    // 生成键序列
    for (int i = 0; i < NUM_ITEMS; ++i) {
        keys.push_back(i);
    }
    
    // 随机打乱
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);
    
    // 随机顺序插入
    for (const auto& key : keys) {
        ValueType value;
        std::string str = "value_" + std::to_string(key);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(key, value));
    }
    
    // 验证所有键都能正确找到
    for (int i = 0; i < NUM_ITEMS; ++i) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(i, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "value_" + std::to_string(i);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
    
    // 使用绝对路径进行序列化测试
    std::string serialize_path = "/home/jyjays/LAB/MyBPlusTree/test/" + std::to_string(getpid()) + ".bin";
    BPlusTreeSerializer<KeyType, ValueType, KeyComparator> serializer(*tree, serialize_path);
    EXPECT_TRUE(serializer.Serialize());
    
    // 验证序列化文件已创建
    std::ifstream check_file(serialize_path);
    EXPECT_TRUE(check_file.good());
    check_file.close();
    std::cout << "Serialized tree to: " << serialize_path << std::endl;
    // // 反序列化并验证数据
    BPlusTree<KeyType, ValueType, KeyComparator> new_tree("test_tree_deserialized", comparator, 3, 3);
    BPlusTreeSerializer<KeyType, ValueType, KeyComparator> deserializer(new_tree, serialize_path);
    EXPECT_TRUE(deserializer.Deserialize());
    for (const auto& key : keys) {
        std::vector<ValueType> results;
        EXPECT_TRUE(new_tree.GetValue(key, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "value_" + std::to_string(key);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }

    // 清理临时文件
    std::remove(serialize_path.c_str());
}

TEST_F(BPlusTreeComplexTest, BoundaryConditions) {
    // 测试边界值
    std::vector<KeyType> boundary_keys = {
        0, 1, 2, 999, 1000, 10000, 100000
    };
    
    for (const auto& key : boundary_keys) {
        ValueType value;
        std::string str = "boundary_" + std::to_string(key);
        std::strcpy(value.data(), str.c_str());
        
        EXPECT_TRUE(tree->Insert(key, value));
    }
    
    // 验证边界值
    for (const auto& key : boundary_keys) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(key, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "boundary_" + std::to_string(key);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
}

TEST_F(BPlusTreeComplexTest, DISABLED_StressTest) {
    const int NUM_OPERATIONS = 20000;
    std::set<KeyType> inserted_keys;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_dist(1, 10000);
    
    // 执行大量随机操作
    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        KeyType key = key_dist(gen);
        ValueType value;
        std::string str = "v_" + std::to_string(key);
        std::strcpy(value.data(), str.c_str());
        
        if (inserted_keys.find(key) == inserted_keys.end()) {
            // 如果键不存在，插入应该成功
            EXPECT_TRUE(tree->Insert(key, value));
            inserted_keys.insert(key);
        } else {
            // 如果键已存在，插入应该失败
            EXPECT_FALSE(tree->Insert(key, value));
        }
    }
    
    // 验证所有插入的键都能找到
    for (const auto& key : inserted_keys) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(key, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "v_" + std::to_string(key);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
}

TEST_F(BPlusTreeComplexTest, SequentialPattern) {
    const int NUM_SEQUENCES = 5;
    const int SEQUENCE_LENGTH = 100;
    
    // 插入多个连续序列
    for (int seq = 0; seq < NUM_SEQUENCES; ++seq) {
        int start = seq * SEQUENCE_LENGTH * 2; // 避免重叠
        
        for (int i = 0; i < SEQUENCE_LENGTH; ++i) {
            KeyType key = start + i;
            ValueType value;
            std::string str = "seq_" + std::to_string(seq) + "_" + std::to_string(i);
            std::strcpy(value.data(), str.c_str());
            
            EXPECT_TRUE(tree->Insert(key, value));
        }
    }
    
    // 验证所有序列
    for (int seq = 0; seq < NUM_SEQUENCES; ++seq) {
        int start = seq * SEQUENCE_LENGTH * 2;
        
        for (int i = 0; i < SEQUENCE_LENGTH; ++i) {
            KeyType key = start + i;
            std::vector<ValueType> results;
            EXPECT_TRUE(tree->GetValue(key, &results));
            EXPECT_EQ(results.size(), 1);
            
            std::string expected = "seq_" + std::to_string(seq) + "_" + std::to_string(i);
            EXPECT_STREQ(results[0].data(), expected.c_str());
        }
    }
}

TEST_F(BPlusTreeComplexTest, LongValueTest) {
    // 测试长值（接近最大长度）
    const int NUM_ITEMS = 100;
    
    for (int i = 0; i < NUM_ITEMS; ++i) {
        ValueType value;
        std::string long_str = "long_value_" + std::to_string(i);
        // 填充到接近最大长度
        while (long_str.length() < sizeof(ValueType) - 10) {
            long_str += "_padding";
        }
        std::strcpy(value.data(), long_str.c_str());
        
        EXPECT_TRUE(tree->Insert(i, value));
    }
    
    // 验证长值
    for (int i = 0; i < NUM_ITEMS; ++i) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(i, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "long_value_" + std::to_string(i);
        while (expected.length() < sizeof(ValueType) - 10) {
            expected += "_padding";
        }
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
}

} // namespace test
} // namespace mybplus
