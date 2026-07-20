add_library(cpr_store
    placement_policy.cpp
    simple_task_dispatcher.cpp
    task_manager.cpp
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
