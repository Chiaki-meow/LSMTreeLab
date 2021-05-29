cmake_minimum_required(VERSION 3.17)
project(lsm_lab)

set(CMAKE_CXX_STANDARD 14)

add_executable(lsm_lab main.cpp kvstore.cc kvstore.h kvstore_api.h MurmurHash3.h test.h utils.h)