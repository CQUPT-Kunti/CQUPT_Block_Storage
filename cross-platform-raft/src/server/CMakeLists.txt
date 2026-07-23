add_library(cpr_server
    logging.cpp
    server.cpp
    server_lifecycle.cpp
)

if(TARGET protobuf::libprotobuf)
    get_target_property(CPR_SERVER_PROTOBUF_INCLUDES
        protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
    if(CPR_SERVER_PROTOBUF_INCLUDES)
        target_include_directories(cpr_server BEFORE
            PUBLIC
                ${CPR_SERVER_PROTOBUF_INCLUDES}
        )
    endif()
endif()

if(TARGET spdlog::spdlog)
    target_compile_definitions(cpr_server PRIVATE CPR_SERVER_WITH_SPDLOG=1)
    target_link_libraries(cpr_server PRIVATE spdlog::spdlog)
endif()

target_include_directories(cpr_server
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_server
    PUBLIC
        cpr_common
        cpr_config
        cpr_metadata
        cpr_platform
        cpr_raft
        cpr_rpc
        cpr_transport
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)

add_executable(cpr_server_main
    server_main.cpp
)

target_link_libraries(cpr_server_main
    PRIVATE
        cpr_server
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
