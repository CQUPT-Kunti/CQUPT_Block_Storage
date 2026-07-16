#pragma once

#include <filesystem>
#include <vector>

#include "raft/raft_storage.h"

namespace cpr::raft
{

    class FileRaftStorage final : public IRaftStorage
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
        common::Status LoadFromDisk(HardState *hard_state,
                                    std::vector<LogEntry> *entries) const;
        common::Status ValidateHardState(const HardState &hard_state) const;
        common::Status ValidateEntryBatch(const std::vector<LogEntry> &entries) const;
        common::LogIndex LastLogIndex() const noexcept;
        bool HasEntry(common::LogIndex index) const noexcept;
        std::size_t Offset(common::LogIndex index) const noexcept;

        bool open_ = false;
        std::filesystem::path hard_state_path_;
        std::filesystem::path log_path_;
        HardState hard_state_;
        std::vector<LogEntry> entries_;
    };

} // namespace cpr::raft
