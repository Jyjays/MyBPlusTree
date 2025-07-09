#include <iostream>
#include <vector>
#include <functional>
#include <cstring>
#include "b_plus_tree.h"
#include "config.h"

using namespace mybplus;

int main() {
    std::cout << "=== 简化版B+树测试 ===" << std::endl;
    
    // 创建比较器
    KeyComparator comparator;
    
    // 创建B+树
    BPlusTree<KeyType, ValueType, KeyComparator> bplus_tree("test_tree", comparator);
    
    std::cout << "创建B+树完成" << std::endl;
    
    // 测试插入
    ValueType value1, value2, value3;
    std::strcpy(value1.data(), "value1");
    std::strcpy(value2.data(), "value2");
    std::strcpy(value3.data(), "value3");
    
    std::cout << "开始插入测试..." << std::endl;
    
    bool result1 = bplus_tree.Insert(1, value1);
    bool result2 = bplus_tree.Insert(2, value2);
    bool result3 = bplus_tree.Insert(3, value3);
    
    std::cout << "插入结果: " << result1 << ", " << result2 << ", " << result3 << std::endl;
    
    // 测试查找
    std::cout << "开始查找测试..." << std::endl;
    
    std::vector<ValueType> results;
    bool found = bplus_tree.GetValue(1, &results);
    
    if (found && !results.empty()) {
        std::cout << "找到键1的值: " << results[0].data() << std::endl;
    } else {
        std::cout << "未找到键1" << std::endl;
    }
    
    // 测试空树
    std::cout << "树是否为空: " << (bplus_tree.IsEmpty() ? "是" : "否") << std::endl;
    
    std::cout << "=== 测试完成 ===" << std::endl;
    return 0;
}
