add_library(cpr_platform
    file_ops.cpp
    process_ops.cpp
    time_ops.cpp
)

target_include_directories(cpr_platform
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_platform
    PUBLIC
        cpr_common
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
