#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "raft/raft_storage.h"

namespace cpr::raft
{

    class MemoryRaftStorage final : public IRaftStorage
    {
    public:
        common::Status Open(const std::filesystem::path &storage_path) override;
        common::Status Load(RaftStorageLoadResult *result) const override;
        common::Status SaveHardState(const HardState &hard_state) override;
        common::Status AppendEntries(const std::vector<LogEntry> &entries) override;
        common::Status TruncateSuffix(common::LogIndex first_index) override;
        common::Status SaveSnapshot(const SnapshotData &snapshot) override;

    private:
        common::Status RequireOpen() const;
        common::LogIndex SnapshotIndex() const noexcept;
        common::Term SnapshotTerm() const noexcept;
        common::LogIndex LastLogIndex() const noexcept;
        common::LogIndex FirstLogIndex() const noexcept;
        bool HasEntry(common::LogIndex index) const noexcept;
        std::size_t Offset(common::LogIndex index) const noexcept;
        common::Status GetTerm(common::LogIndex index, common::Term *term) const;
        common::Status ValidateEntryBatch(const std::vector<LogEntry> &entries) const;

        bool open_ = false;
        HardState hard_state_;
        std::vector<LogEntry> entries_;
        std::optional<SnapshotData> snapshot_;
    };

} // namespace cpr::raft
