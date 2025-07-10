cmake_minimum_required(VERSION 3.16)
project(MyBPlusTree VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(ENABLE_ASAN "Enable AddressSanitizer for memory debugging" OFF)

if(ENABLE_ASAN)
    message(STATUS "AddressSanitizer (ASan) is enabled.")
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options("-g" "-fsanitize=address")
        add_link_options("-fsanitize=address")
    else()
        message(WARNING "ASan is enabled, but the current compiler (${CMAKE_CXX_COMPILER_ID}) might not support it.")
    endif()
else()
    message(STATUS "AddressSanitizer (ASan) is disabled.")
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

include_directories(src/include)

file(GLOB_RECURSE SOURCES
    "src/bplustree/*.cpp"
)

add_library(mybplustree STATIC ${SOURCES})

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/googletest/CMakeLists.txt")
    message(STATUS "Found googletest, adding as subdirectory.")
    set(gtest_build_tests OFF CACHE BOOL "" FORCE)
    add_subdirectory(third_party/googletest)
else()
    message(FATAL_ERROR "googletest not found in third_party/googletest. Please run 'git submodule update --init --recursive'")
endif()

enable_testing()

add_executable(gtest_runner
    test/gtest_basic.cpp
    test/gtest_complex.cpp
    test/gtest_performance.cpp
)

target_include_directories(gtest_runner PRIVATE
    "src/include"                                             
    "${CMAKE_CURRENT_BINARY_DIR}/third_party/googletest/include" 
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/googletest/include"
)

target_link_libraries(gtest_runner PRIVATE
    mybplustree 
    gtest       
    gtest_main  
)

include(GoogleTest)

gtest_discover_tests(gtest_runner)

add_executable(test_bplustree test/test_main.cpp)
target_link_libraries(test_bplustree PRIVATE mybplustree)