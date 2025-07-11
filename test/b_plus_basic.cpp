#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include "b_plus_tree.h"
#include "config.h"
#include "b_plus_tree_serializer.h"
namespace mybplus {
namespace test {

class BPlusTreeBasicTest : public ::testing::Test {
protected:
    void SetUp() override {
        tree = std::make_unique<BPlusTree<KeyType, ValueType, KeyComparator>>("test_tree", comparator,3,3);
    }

    void TearDown() override {
        tree.reset();
    }

    std::unique_ptr<BPlusTree<KeyType, ValueType, KeyComparator>> tree;
    KeyComparator comparator;
};

TEST_F(BPlusTreeBasicTest, DISABLED_EmptyTree) {
    EXPECT_TRUE(tree->IsEmpty());
    
    std::vector<ValueType> results;
    EXPECT_FALSE(tree->GetValue(1, &results));
    EXPECT_TRUE(results.empty());
}

TEST_F(BPlusTreeBasicTest, DISABLED_SingleInsertAndSearch) {
    ValueType value;
    std::strcpy(value.data(), "test_value");
    
    EXPECT_TRUE(tree->Insert(1, value));
    EXPECT_FALSE(tree->IsEmpty());
    
    std::vector<ValueType> results;
    EXPECT_TRUE(tree->GetValue(1, &results));
    EXPECT_EQ(results.size(), 1);
    EXPECT_STREQ(results[0].data(), "test_value");
}

TEST_F(BPlusTreeBasicTest, DISABLED_MultipleInserts) {
    ValueType value1, value2, value3;
    std::strcpy(value1.data(), "value1");
    std::strcpy(value2.data(), "value2");
    std::strcpy(value3.data(), "value3");
    
    EXPECT_TRUE(tree->Insert(1, value1));
    EXPECT_TRUE(tree->Insert(2, value2));
    EXPECT_TRUE(tree->Insert(3, value3));
    
    std::vector<ValueType> results;
    
    EXPECT_TRUE(tree->GetValue(1, &results));
    EXPECT_EQ(results.size(), 1);
    EXPECT_STREQ(results[0].data(), "value1");
    
    results.clear();
    EXPECT_TRUE(tree->GetValue(2, &results));
    EXPECT_EQ(results.size(), 1);
    EXPECT_STREQ(results[0].data(), "value2");
    
    results.clear();
    EXPECT_TRUE(tree->GetValue(3, &results));
    EXPECT_EQ(results.size(), 1);
    EXPECT_STREQ(results[0].data(), "value3");
}

TEST_F(BPlusTreeBasicTest, DISABLED_DuplicateKeys) {
    ValueType value1, value2;
    std::strcpy(value1.data(), "value1");
    std::strcpy(value2.data(), "value2");
    
    EXPECT_TRUE(tree->Insert(1, value1));
    EXPECT_FALSE(tree->Insert(1, value2)); // 重复键应该失败
    
    std::vector<ValueType> results;
    EXPECT_TRUE(tree->GetValue(1, &results));
    EXPECT_EQ(results.size(), 1);
    EXPECT_STREQ(results[0].data(), "value1"); // 应该保持原值
}

TEST_F(BPlusTreeBasicTest, DISABLED_SearchNonExistentKey) {
    ValueType value;
    std::strcpy(value.data(), "test_value");
    
    EXPECT_TRUE(tree->Insert(1, value));
    
    std::vector<ValueType> results;
    EXPECT_FALSE(tree->GetValue(2, &results)); // 搜索不存在的键
    EXPECT_TRUE(results.empty());
}

TEST_F(BPlusTreeBasicTest, DISABLED_OrderedInsert) {
    // 按顺序插入
    for (int i = 1; i <= 10; ++i) {
        ValueType value;
        std::string str = "value" + std::to_string(i);
        std::strcpy(value.data(), str.c_str());
        EXPECT_TRUE(tree->Insert(i, value));
    }
    
    // 验证所有键都能找到
    for (int i = 1; i <= 10; ++i) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(i, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "value" + std::to_string(i);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
}

TEST_F(BPlusTreeBasicTest, DISABLED_ReverseOrderedInsert) {
    // 按逆序插入
    for (int i = 10; i >= 1; --i) {
        ValueType value;
        std::string str = "value" + std::to_string(i);
        std::strcpy(value.data(), str.c_str());
        EXPECT_TRUE(tree->Insert(i, value));
    }
    
    // 验证所有键都能找到
    for (int i = 1; i <= 10; ++i) {
        std::vector<ValueType> results;
        EXPECT_TRUE(tree->GetValue(i, &results));
        EXPECT_EQ(results.size(), 1);
        
        std::string expected = "value" + std::to_string(i);
        EXPECT_STREQ(results[0].data(), expected.c_str());
    }
}

} // namespace test
} // namespace mybplus
