# ====================================================================
# 1. 项目基本设置
# ====================================================================
cmake_minimum_required(VERSION 3.16)
project(MyBPlusTree VERSION 1.0 LANGUAGES CXX)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


# ====================================================================
# 2. 编译器与构建选项
# ====================================================================

# 添加一个选项来控制是否启用地址消毒器 (ASan)
# 可以在配置时通过 -DENABLE_ASAN=ON 来开启
option(ENABLE_ASAN "Enable AddressSanitizer for memory debugging" OFF)

if(ENABLE_ASAN)
    message(STATUS "AddressSanitizer (ASan) is enabled.")
    # 适用于 GCC 和 Clang 编译器
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options("-g" "-fsanitize=address")
        add_link_options("-fsanitize=address")
    else()
        message(WARNING "ASan is enabled, but the current compiler (${CMAKE_CXX_COMPILER_ID}) might not support it.")
    endif()
else()
    message(STATUS "AddressSanitizer (ASan) is disabled.")
endif()

# 设置所有可执行文件的输出目录到 build/bin/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
# 设置所有库文件的输出目录到 build/lib/
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)


# ====================================================================
# 3. 项目源代码与库定义
# ====================================================================

# 添加头文件搜索路径
include_directories(src/include)

# 收集您的B+树源文件
file(GLOB_RECURSE SOURCES
    "src/bplustree/*.cpp"
)

# 将您的核心代码编译成一个静态库，方便多个测试程序复用
add_library(mybplustree STATIC ${SOURCES})


# ====================================================================
# 4. 依赖管理与测试
# ====================================================================

# 检查GTest子模块是否存在，并将其作为子项目添加
# 前提：您已经执行了 git submodule add ... third_party/googletest
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/googletest/CMakeLists.txt")
    message(STATUS "Found googletest, adding as subdirectory.")
    # 将GTest的构建输出（如警告）关闭，保持主项目输出整洁
    set(gtest_build_tests OFF CACHE BOOL "" FORCE)
    add_subdirectory(third_party/googletest)
else()
    message(FATAL_ERROR "googletest not found in third_party/googletest. Please run 'git submodule update --init --recursive'")
endif()

# 启用CMake的测试功能，这是ctest的基础
enable_testing()

# --- 创建GTest测试可执行文件 ---
add_executable(gtest_runner
    test/gtest_basic.cpp
    # 如果有更多测试文件，像这样继续添加
    # test/gtest_complex.cpp
)

# 为测试程序添加必要的头文件搜索路径
target_include_directories(gtest_runner PRIVATE
    "src/include"                                             # 您自己的头文件
    "${CMAKE_CURRENT_BINARY_DIR}/third_party/googletest/include" # GTest生成的头文件
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/googletest/include"
)

# 链接GTest测试程序
target_link_libraries(gtest_runner PRIVATE
    mybplustree # 链接您要测试的库
    gtest       # 链接GTest核心库
    gtest_main  # 链接GTest的main函数库
)

# 引入GoogleTest模块以使用gtest_discover_tests
include(GoogleTest)

# 自动发现并注册gtest_runner中的所有TEST宏为CTest测试用例
# 这比手动的add_test()更现代化且方便
gtest_discover_tests(gtest_runner)

# --- 保留您原有的基础测试程序（如果需要）---
add_executable(test_bplustree test/test_main.cpp)
target_link_libraries(test_bplustree PRIVATE mybplustree)