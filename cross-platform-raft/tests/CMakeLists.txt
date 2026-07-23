find_package(GTest CONFIG QUIET)
if(NOT GTest_FOUND)
    find_package(GTest QUIET)
endif()
if(GTest_FOUND AND TARGET GTest::gtest)
    get_target_property(_cpr_gtest_includes
        GTest::gtest INTERFACE_INCLUDE_DIRECTORIES)
    if(_cpr_gtest_includes MATCHES "/anaconda3/include")
        message(WARNING
            "Ignoring Anaconda GTest package because it exposes the whole "
            "Anaconda include directory and can shadow vcpkg protobuf headers")
        set(GTest_FOUND FALSE)
    endif()
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

add_library(cpr_test_support STATIC
    fixture_loader.cpp
    simulated_cluster.cpp
)

target_link_libraries(cpr_test_support
    PUBLIC
        cpr_common
        cpr_raft
        cpr_transport
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_test_support
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src
)

add_executable(cpr_fixture_loader_tests
    fixture_loader_test.cpp
    simulated_cluster_test.cpp
)

target_link_libraries(cpr_fixture_loader_tests
    PRIVATE
        ${CPR_GTEST_LINK_TARGET}
        cpr_test_support
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_fixture_loader_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

gtest_discover_tests(cpr_fixture_loader_tests
    TEST_PREFIX fixture_loader.
)

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

add_executable(cpr_raft_log_tests
    raft_log_test.cpp
)

target_link_libraries(cpr_raft_log_tests
    PRIVATE
        ${CPR_GTEST_LINK_TARGET}
        cpr_raft
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_raft_log_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

gtest_discover_tests(cpr_raft_log_tests
    TEST_PREFIX raft_log.
)

add_executable(cpr_raft_core_tests
    raft_core_test.cpp
)

target_link_libraries(cpr_raft_core_tests
    PRIVATE
        ${CPR_GTEST_LINK_TARGET}
        cpr_raft
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_raft_core_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

gtest_discover_tests(cpr_raft_core_tests
    TEST_PREFIX raft_core.
)

add_executable(cpr_raft_storage_tests
    raft_storage_test.cpp
)

target_link_libraries(cpr_raft_storage_tests
    PRIVATE
        ${CPR_GTEST_LINK_TARGET}
        cpr_raft
        cpr_platform
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_raft_storage_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

gtest_discover_tests(cpr_raft_storage_tests
    TEST_PREFIX raft_storage.
)

add_executable(cpr_raft_runtime_tests
    raft_runtime_test.cpp
)

target_link_libraries(cpr_raft_runtime_tests
    PRIVATE
        ${CPR_GTEST_LINK_TARGET}
        cpr_raft
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_raft_runtime_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

gtest_discover_tests(cpr_raft_runtime_tests
    TEST_PREFIX raft_runtime.
)

add_executable(cpr_raft_membership_tests
    raft_membership_state_test.cpp
    raft_membership_add_learner_test.cpp
    raft_membership_change_test.cpp
    raft_membership_transition_test.cpp
    raft_membership_test.cpp
)

target_link_libraries(cpr_raft_membership_tests
    PRIVATE
        ${CPR_GTEST_LINK_TARGET}
        cpr_raft
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

target_include_directories(cpr_raft_membership_tests
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

gtest_discover_tests(cpr_raft_membership_tests
    TEST_PREFIX raft_membership.
)

if(TARGET cpr_metadata)
    add_executable(cpr_metadata_command_tests
        metadata_command_test.cpp
    )

    target_link_libraries(cpr_metadata_command_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_metadata
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_metadata_command_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_metadata_command_tests
        TEST_PREFIX metadata_command.
    )

    add_executable(cpr_state_machine_contract_tests
        state_machine_contract_test.cpp
    )

    target_link_libraries(cpr_state_machine_contract_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_metadata
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_state_machine_contract_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_state_machine_contract_tests
        TEST_PREFIX state_machine_contract.
    )

    add_executable(cpr_metadata_state_machine_tests
        metadata_state_machine_test.cpp
    )

    target_link_libraries(cpr_metadata_state_machine_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_metadata
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_metadata_state_machine_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_metadata_state_machine_tests
        TEST_PREFIX metadata_state_machine.
    )

    add_executable(cpr_metadata_service_tests
        metadata_service_test.cpp
    )

    target_link_libraries(cpr_metadata_service_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_metadata
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_metadata_service_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_metadata_service_tests
        TEST_PREFIX metadata_service.
    )

    add_executable(cpr_state_machine_replacement_tests
        state_machine_replacement_test.cpp
    )

    target_link_libraries(cpr_state_machine_replacement_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_metadata
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_state_machine_replacement_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_state_machine_replacement_tests
        TEST_PREFIX state_machine_replacement.
    )
endif()

if(TARGET cpr_store)
    add_executable(cpr_store_registry_tests
        store_registry_test.cpp
    )

    target_link_libraries(cpr_store_registry_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_store
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_store_registry_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_store_registry_tests
        TEST_PREFIX store_registry.
    )

    add_executable(cpr_placement_policy_tests
        placement_policy_test.cpp
    )

    target_link_libraries(cpr_placement_policy_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_store
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_placement_policy_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_placement_policy_tests
        TEST_PREFIX placement_policy.
    )

    add_executable(cpr_task_manager_tests
        task_manager_test.cpp
    )

    target_link_libraries(cpr_task_manager_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_store
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_task_manager_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_task_manager_tests
        TEST_PREFIX task_manager.
    )

    add_executable(cpr_task_dispatcher_tests
        task_dispatcher_test.cpp
    )

    target_link_libraries(cpr_task_dispatcher_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_store
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_task_dispatcher_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_task_dispatcher_tests
        TEST_PREFIX task_dispatcher.
    )
endif()

if(TARGET cpr_rpc)
    add_executable(cpr_raft_rpc_tests
        raft_rpc_service_test.cpp
        metadata_rpc_service_test.cpp
        store_rpc_service_test.cpp
    )

    target_link_libraries(cpr_raft_rpc_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_rpc
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_raft_rpc_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_raft_rpc_tests
        TEST_PREFIX raft_rpc.
    )

    add_executable(cpr_store_metadata_tests
        store_metadata_test.cpp
    )

    target_link_libraries(cpr_store_metadata_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_rpc
            cpr_store
            cpr_metadata
            cpr_raft
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_store_metadata_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_store_metadata_tests
        TEST_PREFIX store_metadata.
    )
endif()

if(TARGET cpr_rpc AND TARGET cpr_transport)
    add_executable(cpr_raft_rpc_integration_tests
        raft_rpc_integration_test.cpp
    )

    target_link_libraries(cpr_raft_rpc_integration_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_transport
            cpr_rpc
            cpr_raft
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_raft_rpc_integration_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_raft_rpc_integration_tests
        TEST_PREFIX raft_rpc_integration.
    )
endif()

if(TARGET cpr_server)
    add_executable(cpr_server_tests
        server_lifecycle_test.cpp
        server_composition_test.cpp
    )

    target_link_libraries(cpr_server_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_server
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_server_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
    )

    gtest_discover_tests(cpr_server_tests
        TEST_PREFIX server.
    )
endif()

if(TARGET cpr_client_tool_lib AND TARGET cpr_raftctl_lib)
    add_executable(cpr_tools_rpc_tests
        client_tool_test.cpp
        raftctl_test.cpp
    )

    target_link_libraries(cpr_tools_rpc_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_client_tool_lib
            cpr_raftctl_lib
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_tools_rpc_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
            ${PROJECT_SOURCE_DIR}/tools
    )

    gtest_discover_tests(cpr_tools_rpc_tests
        TEST_PREFIX tools.
    )
endif()

if(TARGET cpr_wal_dump_lib)
    add_executable(cpr_wal_dump_tests
        wal_dump_test.cpp
    )

    target_link_libraries(cpr_wal_dump_tests
        PRIVATE
            ${CPR_GTEST_LINK_TARGET}
            cpr_wal_dump_lib
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    target_include_directories(cpr_wal_dump_tests
        PRIVATE
            ${PROJECT_SOURCE_DIR}/src
            ${PROJECT_SOURCE_DIR}/tools
    )

    gtest_discover_tests(cpr_wal_dump_tests
        TEST_PREFIX wal_dump.
    )
endif()
