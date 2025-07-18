#pragma once
#include <array>
#include <cstdint>
#include <functional>
#include <iostream>
#include <string>

namespace mybplus {

#define PAGE_SIZE 4096
#define INVALID_PAGE_ID -1
#define DEBUG
#define USING_CRABBING_PROTOCOL

struct Comparator {
  inline auto operator()(const int64_t &lhs, const int64_t &rhs) const -> int {
    return (lhs < rhs) ? -1 : (lhs > rhs) ? 1 : 0;
  }
};

using page_id_t = int32_t;  // page_id_t占4字节
using KeyType = int64_t;
using ValueType = std::array<char, 16>;  // ValueType占16字节
using KeyComparator = Comparator;

}  // namespace mybplus
