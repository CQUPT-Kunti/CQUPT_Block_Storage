include_guard(GLOBAL)

function(cpr_validate_compiler)
    if(CMAKE_CXX_COMPILER_ID MATCHES "MSVC|GNU|Clang")
        message(STATUS "Configuring compiler options for ${CMAKE_CXX_COMPILER_ID}")
    else()
        message(WARNING
            "Unsupported C++ compiler '${CMAKE_CXX_COMPILER_ID}'. "
            "Only MSVC, GCC, and Clang are explicitly configured."
        )
    endif()
endfunction()

function(cpr_configure_compiler_options options_target warnings_target)
    target_compile_features(${options_target} INTERFACE cxx_std_17)

    if(MSVC)
        target_compile_options(${warnings_target} INTERFACE
            /W4
            /permissive-
            /Zc:__cplusplus
            /EHsc
        )
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(${warnings_target} INTERFACE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wsign-conversion
        )
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        target_compile_options(${warnings_target} INTERFACE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wsign-conversion
        )
    endif()

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${options_target} INTERFACE
            -fno-omit-frame-pointer
        )
    endif()
endfunction()
