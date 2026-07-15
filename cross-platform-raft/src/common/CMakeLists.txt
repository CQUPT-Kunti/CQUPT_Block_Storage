add_library(cpr_common
    bounded_queue.cpp
    checksum.cpp
    status.cpp
)

target_include_directories(cpr_common
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_common
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
