cmake_minimum_required(VERSION 2.9.10)
set(CMAKE_CXX_STANDARD 11)
project(sql-processor C CXX)

include(FindThreads)

include_directories(${CMAKE_CURRENT_BINARY_DIR})


# sql-parser
set(CMAKE_PREFIX_PATH ../../third-party/sql-parser/lib)
find_library(SQL_PARSER_LIB NAMES libsqlparser.so)

include_directories(../../third-party/sql-parser/include)

add_executable(test src/test.cpp)

target_link_libraries(test ${SQL_PARSER_LIB})
