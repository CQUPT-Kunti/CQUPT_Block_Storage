add_library(cpr_config
    config.cpp
    config_loader.cpp
)

if(TARGET nlohmann_json::nlohmann_json)
    target_link_libraries(cpr_config PRIVATE nlohmann_json::nlohmann_json)
else()
    find_path(CPR_NLOHMANN_JSON_INCLUDE_DIR
        NAMES nlohmann/json.hpp
        HINTS
            "$ENV{CONDA_PREFIX}/include"
            "$ENV{HOME}/anaconda3/include"
    )
    if(NOT CPR_NLOHMANN_JSON_INCLUDE_DIR)
        message(FATAL_ERROR "cpr_config requires nlohmann/json.hpp")
    endif()
    target_include_directories(cpr_config PRIVATE
        ${CPR_NLOHMANN_JSON_INCLUDE_DIR}
    )
endif()

target_include_directories(cpr_config
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

target_link_libraries(cpr_config
    PRIVATE
        cpr_project_options
        cpr_project_warnings
        cpr_platform_options
        cpr_dependency_options
    PUBLIC
        cpr_common
)
