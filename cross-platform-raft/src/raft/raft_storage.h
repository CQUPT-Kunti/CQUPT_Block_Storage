#pragma once

#include <filesystem>
#include <optional>
#include <vector>

#include "common/status.h"
#include "raft/raft_types.h"

namespace cpr::raft
{

    struct SnapshotData
    {
        SnapshotMetadata metadata;
        OpaquePayload payload;
    };

    struct RaftStorageLoadResult
    {
        HardState hard_state;
        std::vector<LogEntry> entries;
        std::optional<SnapshotData> snapshot;
        bool empty = true;
    };

    struct RaftStorageOperationResult
    {
        common::Status status;
        bool has_hard_state = false;
        HardState hard_state;
        common::LogIndex last_log_index = common::kInvalidLogIndex;
        bool has_snapshot = false;
        SnapshotMetadata snapshot;
    };

    // Each successful write returns Status today; Runtime can summarize it with
    // RaftStorageOperationResult before confirming RaftCore persistence gates.
    class IRaftStorage
    {
    public:
        virtual ~IRaftStorage() = default;

        virtual common::Status Open(const std::filesystem::path &storage_path) = 0;
        virtual common::Status Load(RaftStorageLoadResult *result) const = 0;
        virtual common::Status SaveHardState(const HardState &hard_state) = 0;
        virtual common::Status AppendEntries(const std::vector<LogEntry> &entries) = 0;
        virtual common::Status TruncateSuffix(common::LogIndex first_index) = 0;
        virtual common::Status SaveSnapshot(const SnapshotData &snapshot) = 0;
    };

} // namespace cpr::raft
