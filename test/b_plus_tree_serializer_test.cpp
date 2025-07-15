#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "b_plus_tree.h"
#include "config.h"

extern "C" {
#include "b_plus_tree_serializer.h"
#include "b_plus_tree_wrapper.h"
}

namespace mybplus {
namespace test {

void KeyToValue(KeyType key, ValueType& value) {
  std::string str = "value_" + std::to_string(key);
  std::fill(value.begin(), value.end(), 0);
  std::strncpy(value.data(), str.c_str(), value.size() - 1);
}

void GenerateUniqueKeys(size_t count, std::vector<KeyType>& keys) {
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
}

class BPlusTreeCSerializationTest : public ::testing::Test {
 protected:
  std::unique_ptr<BPlusTree<KeyType, ValueType, KeyComparator>> tree;
  KeyComparator comparator;
};

TEST_F(BPlusTreeCSerializationTest, SerializationAndDeserializationCorrectness) {
  tree = std::make_unique<BPlusTree<KeyType, ValueType, KeyComparator>>("test_tree", comparator,
                                                                        128, 128);

  const int NUM_ITEMS = 10000000;
  std::vector<KeyType> keys;
  GenerateUniqueKeys(NUM_ITEMS, keys);

  for (const auto& key : keys) {
    ValueType value;
    KeyToValue(key, value);
    ASSERT_TRUE(tree->Insert(key, value));
  }

  std::string serialize_path = std::to_string(getpid()) + ".bin";

  CBPlusTree* tree_handle = reinterpret_cast<CBPlusTree*>(tree.get());

  BPlusTreeSerializer* serializer = serializer_create(tree_handle, serialize_path.c_str());
  ASSERT_NE(serializer, nullptr);
  // 计时
  auto start_time = std::chrono::high_resolution_clock::now();
  EXPECT_TRUE(serializer_serialize(serializer));
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  std::cout << "Serialization took: " << duration << " ms" << std::endl;

  serializer_destroy(serializer);

  std::ifstream check_file(serialize_path);
  EXPECT_TRUE(check_file.good());
  check_file.close();

  BPlusTree<KeyType, ValueType, KeyComparator> new_tree("deserialized_tree", comparator, 3, 3);
  CBPlusTree* new_tree_handle = reinterpret_cast<CBPlusTree*>(&new_tree);

  BPlusTreeSerializer* deserializer = serializer_create(new_tree_handle, serialize_path.c_str());
  ASSERT_NE(deserializer, nullptr);
  auto deserialize_start_time = std::chrono::high_resolution_clock::now();
  EXPECT_TRUE(serializer_deserialize(deserializer));
  auto deserialize_end_time = std::chrono::high_resolution_clock::now();
  auto deserialize_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  deserialize_end_time - deserialize_start_time)
                                  .count();
  std::cout << "Deserialization took: " << deserialize_duration << " ms" << std::endl;
  EXPECT_EQ(new_tree.GetPageCount(), tree->GetPageCount());
  EXPECT_EQ(new_tree.GetRootPageId(), tree->GetRootPageId());
  EXPECT_EQ(new_tree.GetLeafMaxSize(), tree->GetLeafMaxSize());
  EXPECT_EQ(new_tree.GetInternalMaxSize(), tree->GetInternalMaxSize());
  EXPECT_EQ(new_tree.GetPage(new_tree.GetRootPageId())->GetSize(),
            tree->GetPage(tree->GetRootPageId())->GetSize());
  serializer_destroy(deserializer);

  for (const auto& key : keys) {
    std::vector<ValueType> results;
    EXPECT_TRUE(new_tree.GetValue(key, &results));
    EXPECT_EQ(results.size(), 1);

    std::string expected = "value_" + std::to_string(key);
    EXPECT_STREQ(results[0].data(), expected.c_str());
  }

  std::remove(serialize_path.c_str());
}

}  // namespace test
}  // namespace mybplus
