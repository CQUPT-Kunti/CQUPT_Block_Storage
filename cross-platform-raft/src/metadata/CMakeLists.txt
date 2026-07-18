add_library(cpr_metadata
    metadata_command.cpp
)

target_include_directories(cpr_metadata
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_metadata
    PUBLIC
        cpr_common
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
