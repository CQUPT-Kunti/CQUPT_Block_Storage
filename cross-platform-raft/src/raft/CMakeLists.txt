add_library(cpr_raft INTERFACE)

target_include_directories(cpr_raft
    INTERFACE
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_raft
    INTERFACE
        cpr_common
)
