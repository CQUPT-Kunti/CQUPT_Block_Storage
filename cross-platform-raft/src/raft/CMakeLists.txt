add_library(cpr_raft
    file_raft_storage.cpp
    memory_raft_storage.cpp
    raft_core.cpp
    raft_log.cpp
    raft_membership.cpp
    raft_runtime.cpp
    raft_snapshot.cpp
)

target_include_directories(cpr_raft
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_raft
    PUBLIC
        cpr_common
    PRIVATE
        cpr_platform
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
