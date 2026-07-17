if(NOT TARGET cpr_proto_generated)
    message(WARNING "cpr_transport disabled because cpr_proto_generated is unavailable")
    return()
endif()

add_library(cpr_transport
    raft_transport.cpp
    grpc_raft_transport.cpp
)

target_include_directories(cpr_transport
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_transport
    PUBLIC
        cpr_common
        cpr_proto_generated
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
