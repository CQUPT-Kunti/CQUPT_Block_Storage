cmake_minimum_required(VERSION 3.16)

if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    if(WIN32)
        set(CMAKE_TOOLCHAIN_FILE "D:/Tools/vcpkg/scripts/buildsystems/vcpkg.cmake"
            CACHE STRING "Vcpkg toolchain file")
    elseif(DEFINED ENV{VCPKG_ROOT} AND
           EXISTS "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake")
        set(CMAKE_TOOLCHAIN_FILE
            "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
            CACHE STRING "Vcpkg toolchain file")
    endif()
endif()

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
include(CTest)

add_library(cpr_project_options INTERFACE)
add_library(cpr_project_warnings INTERFACE)
add_library(cpr_platform_options INTERFACE)
add_library(cpr_dependency_options INTERFACE)

cpr_validate_compiler()
cpr_configure_compiler_options(cpr_project_options cpr_project_warnings)
cpr_configure_platform_options(cpr_platform_options)
cpr_detect_dependencies(cpr_dependency_options)

add_subdirectory(proto)
add_subdirectory(src/common)
add_subdirectory(src/config)
add_subdirectory(src/metadata)
add_subdirectory(src/platform)
add_subdirectory(src/raft)
add_subdirectory(src/store)
add_subdirectory(src/transport)
add_subdirectory(src/rpc)
add_subdirectory(src/server)
add_subdirectory(tests)

message(STATUS "CrossPlatformRaftMetadataFoundation base CMake configuration is ready")
