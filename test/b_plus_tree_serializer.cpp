#include "b_plus_tree_serializer.h"

#include <gtest/gtest.h>
#include <unistd.h>  // for getpid()

#include <algorithm>
#include <cstdio>  // for std::remove
#include <cstring>
#include <fstream>  // for ifstream
#include <random>
#include <set>
#include <vector>

#include "b_plus_tree.h"
#include "config.h"
namespace mybplus {
namespace test {

class BPlusTreeComplexTest : public ::testing::Test {
 protected:
  void SetUp() override {
    tree = std::make_unique<BPlusTree<KeyType, ValueType, KeyComparator>>("test_tree", comparator,
                                                                          3, 3);
  }

  void TearDown() override { tree.reset(); }

  std::unique_ptr<BPlusTree<KeyType, ValueType, KeyComparator>> tree;
  KeyComparator comparator;
};

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
  std::string serialize_path =
      "/home/jyjays/LAB/MyBPlusTree/test/" + std::to_string(getpid()) + ".bin";
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
    std::cout << "value for key " << key << ": " << results[0].data() << std::endl;
    EXPECT_STREQ(results[0].data(), expected.c_str());
  }

  // 清理临时文件
  std::remove(serialize_path.c_str());
}

}  // namespace test
}  // namespace mybplus
