#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "raft/raft_types.h"
#include "store/store_registry.h"
#include "store/task_manager.h"

namespace cpr::metadata
{

    enum class MetadataCommandType : std::uint8_t
    {
        OPAQUE_OPERATION = 1,
        STORE_OPERATION = 2,
    };

    enum class StoreBusinessCommandKind : std::uint8_t
    {
        REGISTER_STORE = 1,
        UPDATE_STORE_STATE = 2,
        REMOVE_STORE = 3,
        CREATE_TASK = 4,
        POLL_TASKS = 5,
        REPORT_TASK_RESULT = 6,
    };

    struct StoreBusinessCommand
    {
        StoreBusinessCommandKind kind = StoreBusinessCommandKind::REGISTER_STORE;
        store::StoreInfo store;
        store::StoreUpdate store_update;
        store::TaskCreateRequest task_create;
        store::TaskResultReport task_result;
        store::StoreId store_id = 0;
        std::uint32_t max_tasks = 0;
    };

    struct StoreBusinessResult
    {
        store::StoreInfo store;
        std::vector<store::TaskRecord> tasks;
        bool duplicate_result = false;
    };

    struct MetadataCommand
    {
        MetadataCommandType type = MetadataCommandType::OPAQUE_OPERATION;
        std::string command_id;
        std::string target_id;
        std::optional<std::uint64_t> expected_generation;
        raft::OpaquePayload payload;
    };

    constexpr std::uint8_t kMetadataCommandFormatVersion = 1;
    constexpr std::size_t kMaxMetadataCommandIdBytes = 4 * 1024;
    constexpr std::size_t kMaxMetadataCommandTargetBytes = 4 * 1024;
    constexpr std::size_t kMaxMetadataCommandPayloadBytes = 4 * 1024 * 1024;

    common::Status ValidateMetadataCommand(const MetadataCommand &command);
    common::Status EncodeMetadataCommand(const MetadataCommand &command,
                                         raft::OpaquePayload *payload);
    common::Status DecodeMetadataCommand(const raft::OpaquePayload &payload,
                                         MetadataCommand *command);

    common::Status EncodeStoreBusinessCommand(
        const StoreBusinessCommand &command,
        raft::OpaquePayload *payload);
    common::Status DecodeStoreBusinessCommand(
        const raft::OpaquePayload &payload,
        StoreBusinessCommand *command);
    common::Status EncodeStoreBusinessResult(
        const StoreBusinessResult &result,
        raft::OpaquePayload *payload);
    common::Status DecodeStoreBusinessResult(
        const raft::OpaquePayload &payload,
        StoreBusinessResult *result);

} // namespace cpr::metadata
