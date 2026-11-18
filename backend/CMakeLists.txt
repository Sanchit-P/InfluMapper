cmake_minimum_required(VERSION 3.20)
project(SocialMediaInfluenceMapper VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Optimization flags
if(MSVC)
    add_compile_options(/W4 /O2 /permissive-)
else()
    add_compile_options(-Wall -Wextra -O3 -march=native)
endif()

# Find OpenMP for parallel PageRank (FR-3.1)
find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    message(STATUS "OpenMP CXX found. Multi-threading enabled.")
endif()

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Engine executable
add_executable(influence_mapper
    src/main.cpp
)

if(OpenMP_CXX_FOUND)
    target_link_libraries(influence_mapper PRIVATE OpenMP::OpenMP_CXX)
endif()

if(WIN32)
    target_link_libraries(influence_mapper PRIVATE ws2_32 winhttp)
endif()

# Unit & integration tests executable (QA Matrix TC-01 to TC-07)
add_executable(engine_tests
    tests/test_suite.cpp
)

if(OpenMP_CXX_FOUND)
    target_link_libraries(engine_tests PRIVATE OpenMP::OpenMP_CXX)
endif()

if(WIN32)
    target_link_libraries(engine_tests PRIVATE ws2_32)
endif()
