cmake_minimum_required(VERSION 2.9.10)
set(CMAKE_CXX_STANDARD 11)
project(main C CXX)

# 添加 sql-processor 子目录
add_subdirectory(sql-processor)

add_executable(main main.cpp)

target_link_libraries(main SQLProcessor)
