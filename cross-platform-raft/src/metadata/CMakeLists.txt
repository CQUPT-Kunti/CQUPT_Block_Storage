add_library(cpr_metadata
    metadata_command.cpp
    metadata_service.cpp
    metadata_state_machine.cpp
)

target_include_directories(cpr_metadata
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_metadata
    PUBLIC
        cpr_common
        cpr_raft
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
