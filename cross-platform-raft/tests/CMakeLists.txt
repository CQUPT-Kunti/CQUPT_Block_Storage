find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
    find_package(GTest QUIET)
endif()

set(CPR_GTEST_LINK_TARGET "")
if(GTest_FOUND)
    set(CPR_GTEST_LINK_TARGET GTest::gtest_main)
else()
    set(CPR_GTEST_SOURCE_ROOT
        "/home/yangjilei/anaconda3/lib/python3.13/site-packages/mypyc/external/googletest")
    if(NOT EXISTS "${CPR_GTEST_SOURCE_ROOT}/src/gtest-all.cc" OR
       NOT EXISTS "${CPR_GTEST_SOURCE_ROOT}/src/gtest_main.cc" OR
       NOT EXISTS "${CPR_GTEST_SOURCE_ROOT}/include/gtest/gtest.h")
        message(FATAL_ERROR "T007 tests require GoogleTest")
    endif()

    find_package(Threads REQUIRED)
    add_library(cpr_gtest STATIC
        "${CPR_GTEST_SOURCE_ROOT}/src/gtest-all.cc"
        "${CPR_GTEST_SOURCE_ROOT}/src/gtest_main.cc"
    )
    target_include_directories(cpr_gtest
        PUBLIC
            "${CPR_GTEST_SOURCE_ROOT}/include"
        PRIVATE
            "${CPR_GTEST_SOURCE_ROOT}"
    )
    target_link_libraries(cpr_gtest PUBLIC Threads::Threads)
    set(CPR_GTEST_LINK_TARGET cpr_gtest)
endif()

include(GoogleTest)

add_executable(cpr_phase1_tests
    status_test.cpp
    checksum_test.cpp
    bounded_queue_test.cpp
    config_test.cpp
    file_ops_test.cpp
)

target_link_libraries(cpr_phase1_tests
    PRIVATE
        ${CPR_GTEST_LINK_TARGET}
        cpr_common
        cpr_config
        cpr_platform
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_phase1_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

gtest_discover_tests(cpr_phase1_tests)
