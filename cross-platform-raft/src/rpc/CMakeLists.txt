if(NOT TARGET cpr_proto_generated)
    message(WARNING "Skipping cpr_rpc: generated protobuf/grpc target is unavailable")
    return()
endif()

add_library(cpr_rpc
    raft_rpc_service.cpp
    metadata_rpc_service.cpp
)

target_include_directories(cpr_rpc
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_rpc
    PUBLIC
        cpr_raft
        cpr_proto_generated
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
