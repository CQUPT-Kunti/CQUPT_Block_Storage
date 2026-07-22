add_library(cpr_tool_common STATIC
    tool_common.cpp
)

target_include_directories(cpr_tool_common
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_tool_common
    PUBLIC
        cpr_common
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

add_library(cpr_wal_dump_lib STATIC
    wal_dump/wal_dump_command.cpp
)

target_include_directories(cpr_wal_dump_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_wal_dump_lib
    PUBLIC
        cpr_tool_common
        cpr_raft
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

add_executable(wal_dump
    wal_dump/wal_dump_main.cpp
)

target_link_libraries(wal_dump
    PRIVATE
        cpr_wal_dump_lib
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

if(TARGET cpr_proto_generated)
    add_library(cpr_client_tool_lib STATIC
        client/client_command.cpp
    )

    target_include_directories(cpr_client_tool_lib
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${PROJECT_SOURCE_DIR}/src
    )

    target_link_libraries(cpr_client_tool_lib
        PUBLIC
            cpr_tool_common
            cpr_proto_generated
        PRIVATE
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    add_executable(cpr_client
        client/client_main.cpp
    )

    target_link_libraries(cpr_client
        PRIVATE
            cpr_client_tool_lib
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    add_library(cpr_raftctl_lib STATIC
        raftctl/raftctl_command.cpp
    )

    target_include_directories(cpr_raftctl_lib
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${PROJECT_SOURCE_DIR}/src
    )

    target_link_libraries(cpr_raftctl_lib
        PUBLIC
            cpr_tool_common
            cpr_proto_generated
        PRIVATE
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    add_executable(raftctl
        raftctl/raftctl_main.cpp
    )

    target_link_libraries(raftctl
        PRIVATE
            cpr_raftctl_lib
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )
else()
    message(WARNING "Skipping cpr_client and raftctl: generated protobuf/grpc target is unavailable")
endif()
