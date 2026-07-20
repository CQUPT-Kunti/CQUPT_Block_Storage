add_library(cpr_store
    store_registry.cpp
)

target_include_directories(cpr_store
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_store
    PUBLIC
        cpr_common
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
