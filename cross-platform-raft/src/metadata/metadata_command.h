#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "common/status.h"
#include "raft/raft_types.h"

namespace cpr::metadata
{

    enum class MetadataCommandType : std::uint8_t
    {
        OPAQUE_OPERATION = 1,
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

} // namespace cpr::metadata
