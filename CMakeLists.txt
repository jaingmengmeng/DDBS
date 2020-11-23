cmake_minimum_required(VERSION 2.9.10)
set(CMAKE_CXX_STANDARD 11)
project(main C CXX)

include(FindThreads)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

# sql-parser
set(CMAKE_PREFIX_PATH ../third-party/sql-parser/lib)
find_library(SQL_PARSER_LIB NAMES libsqlparser.so)

message(STATUS ${SQL_PARSER_LIB})

include_directories(../third-party/sql-parser/include)

add_executable(main main/main.cpp)

target_link_libraries(main ${SQL_PARSER_LIB})
