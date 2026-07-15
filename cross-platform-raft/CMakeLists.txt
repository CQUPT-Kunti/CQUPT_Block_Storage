cmake_minimum_required(VERSION 3.16)

project(CrossPlatformRaftMetadataFoundation
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CPR_REQUIRE_ALL_DEPENDENCIES
    "Fail configuration when required dependencies are unavailable"
    OFF
)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(CompilerOptions)
include(PlatformOptions)
include(Dependencies)

add_library(cpr_project_options INTERFACE)
add_library(cpr_project_warnings INTERFACE)
add_library(cpr_platform_options INTERFACE)
add_library(cpr_dependency_options INTERFACE)

cpr_validate_compiler()
cpr_configure_compiler_options(cpr_project_options cpr_project_warnings)
cpr_configure_platform_options(cpr_platform_options)
cpr_detect_dependencies(cpr_dependency_options)

add_subdirectory(src/common)
add_subdirectory(src/config)
add_subdirectory(src/platform)
add_subdirectory(src/server)

message(STATUS "CrossPlatformRaftMetadataFoundation base CMake configuration is ready")
