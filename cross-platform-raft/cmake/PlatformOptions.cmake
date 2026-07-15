include_guard(GLOBAL)

function(cpr_configure_platform_options target)
    if(WIN32)
        target_compile_definitions(${target} INTERFACE
            NOMINMAX
            WIN32_LEAN_AND_MEAN
            _CRT_SECURE_NO_WARNINGS
        )
    elseif(UNIX)
        find_package(Threads REQUIRED)
        target_link_libraries(${target} INTERFACE Threads::Threads)
        target_compile_definitions(${target} INTERFACE
            CPR_PLATFORM_POSIX=1
        )
    else()
        message(WARNING "Unsupported platform '${CMAKE_SYSTEM_NAME}'.")
    endif()
endfunction()
