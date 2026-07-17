add_custom_target(cpr_proto_contracts
    SOURCES
        common.proto
        raft.proto
        metadata.proto
        store.proto
)

set(CPR_PROTO_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

set(_cpr_proto_files
    "${CMAKE_CURRENT_SOURCE_DIR}/common.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/raft.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/metadata.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/store.proto"
)

set(_cpr_grpc_proto_files
    "${CMAKE_CURRENT_SOURCE_DIR}/raft.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/metadata.proto"
    "${CMAKE_CURRENT_SOURCE_DIR}/store.proto"
)

set(_cpr_proto_generated_srcs
    "${CPR_PROTO_GENERATED_DIR}/common.pb.cc"
    "${CPR_PROTO_GENERATED_DIR}/raft.pb.cc"
    "${CPR_PROTO_GENERATED_DIR}/metadata.pb.cc"
    "${CPR_PROTO_GENERATED_DIR}/store.pb.cc"
)

set(_cpr_proto_generated_hdrs
    "${CPR_PROTO_GENERATED_DIR}/common.pb.h"
    "${CPR_PROTO_GENERATED_DIR}/raft.pb.h"
    "${CPR_PROTO_GENERATED_DIR}/metadata.pb.h"
    "${CPR_PROTO_GENERATED_DIR}/store.pb.h"
)

set(_cpr_grpc_generated_srcs
    "${CPR_PROTO_GENERATED_DIR}/raft.grpc.pb.cc"
    "${CPR_PROTO_GENERATED_DIR}/metadata.grpc.pb.cc"
    "${CPR_PROTO_GENERATED_DIR}/store.grpc.pb.cc"
)

set(_cpr_grpc_generated_hdrs
    "${CPR_PROTO_GENERATED_DIR}/raft.grpc.pb.h"
    "${CPR_PROTO_GENERATED_DIR}/metadata.grpc.pb.h"
    "${CPR_PROTO_GENERATED_DIR}/store.grpc.pb.h"
)

if(CPR_PROTO_CODEGEN_AVAILABLE)
    if(TARGET protobuf::protoc)
        set(_cpr_protoc_command protobuf::protoc)
    else()
        set(_cpr_protoc_command "${CPR_PROTOC_EXECUTABLE}")
    endif()

    if(TARGET gRPC::grpc_cpp_plugin)
        set(_cpr_grpc_cpp_plugin "$<TARGET_FILE:gRPC::grpc_cpp_plugin>")
    else()
        set(_cpr_grpc_cpp_plugin "${CPR_GRPC_CPP_PLUGIN_EXECUTABLE}")
    endif()

    add_custom_command(
        OUTPUT
            ${_cpr_proto_generated_srcs}
            ${_cpr_proto_generated_hdrs}
        COMMAND ${CMAKE_COMMAND} -E make_directory "${CPR_PROTO_GENERATED_DIR}"
        COMMAND ${_cpr_protoc_command}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
            --cpp_out=${CPR_PROTO_GENERATED_DIR}
            ${_cpr_proto_files}
        DEPENDS ${_cpr_proto_files}
        COMMENT "Generating protobuf C++ sources"
        VERBATIM
    )

    add_custom_command(
        OUTPUT
            ${_cpr_grpc_generated_srcs}
            ${_cpr_grpc_generated_hdrs}
        COMMAND ${CMAKE_COMMAND} -E make_directory "${CPR_PROTO_GENERATED_DIR}"
        COMMAND ${_cpr_protoc_command}
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
            --grpc_out=${CPR_PROTO_GENERATED_DIR}
            --plugin=protoc-gen-grpc=${_cpr_grpc_cpp_plugin}
            ${_cpr_grpc_proto_files}
        DEPENDS ${_cpr_proto_files}
        COMMENT "Generating gRPC C++ sources"
        VERBATIM
    )

    add_library(cpr_proto_generated STATIC
        ${_cpr_proto_generated_srcs}
        ${_cpr_grpc_generated_srcs}
    )

    target_include_directories(cpr_proto_generated
        PUBLIC
            "${CPR_PROTO_GENERATED_DIR}"
    )

    target_link_libraries(cpr_proto_generated
        PUBLIC
            protobuf::libprotobuf
            gRPC::grpc++
        PRIVATE
            cpr_project_options
            cpr_project_warnings
            cpr_platform_options
    )

    add_dependencies(cpr_proto_generated cpr_proto_contracts)
    message(STATUS
        "Proto code generation enabled: output directory=${CPR_PROTO_GENERATED_DIR}")
else()
    message(WARNING
        "Proto code generation disabled; missing components: "
        "${CPR_PROTO_CODEGEN_MISSING_COMPONENTS}")
endif()
