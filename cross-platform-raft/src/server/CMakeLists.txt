add_library(cpr_server
    logging.cpp
    server_lifecycle.cpp
)

if(TARGET spdlog::spdlog)
    target_compile_definitions(cpr_server PRIVATE CPR_SERVER_WITH_SPDLOG=1)
    target_link_libraries(cpr_server PRIVATE spdlog::spdlog)
endif()

target_include_directories(cpr_server
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_server
    PUBLIC
        cpr_common
        cpr_config
        cpr_platform
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
)
