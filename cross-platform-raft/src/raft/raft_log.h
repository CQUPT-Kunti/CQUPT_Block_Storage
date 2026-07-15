#pragma once

#include <cstddef>
#include <vector>

#include "common/status.h"
#include "raft/raft_types.h"

namespace cpr::raft
{

    class RaftLog
    {
    public:
        enum class ConflictType
        {
            NONE,
            CONFLICT,
            APPEND,
        };

        struct ConflictResult
        {
            ConflictType type = ConflictType::NONE;
            common::LogIndex index = common::kInvalidLogIndex;
        };

        RaftLog() = default;

        common::Status Append(const LogEntry &entry);
        common::Status Append(const std::vector<LogEntry> &entries);

        common::Status GetEntry(common::LogIndex index, LogEntry *entry) const;
        common::Status GetTerm(common::LogIndex index, common::Term *term) const;
        common::Status GetEntries(common::LogIndex begin,
                                  common::LogIndex end,
                                  std::vector<LogEntry> *entries) const;

        common::Status FindConflict(const std::vector<LogEntry> &incoming,
                                    ConflictResult *result) const;
        common::Status TruncateSuffix(common::LogIndex index);

        common::Status AdvanceStableTo(common::LogIndex index);
        common::Status AdvanceCommitTo(common::LogIndex index);
        common::Status AdvanceAppliedTo(common::LogIndex index);
        common::Status UpdateSnapshotBoundary(common::LogIndex index, common::Term term);

        bool Matches(common::LogIndex index, common::Term term) const;
        bool empty() const noexcept;
        std::size_t size() const noexcept;

        common::LogIndex first_index() const noexcept;
        common::LogIndex last_index() const noexcept;
        common::LogIndex snapshot_index() const noexcept;
        common::Term snapshot_term() const noexcept;
        common::LogIndex stable_index() const noexcept;
        common::LogIndex commit_index() const noexcept;
        common::LogIndex applied_index() const noexcept;

    private:
        common::Status ValidateEntries(const std::vector<LogEntry> &entries,
                                       common::LogIndex expected_start) const;
        common::Status ValidateInvariants() const;
        common::Status RequireEntryPointer(const LogEntry *entry) const;
        common::Status RequireTermPointer(const common::Term *term) const;
        common::Status RequireEntriesPointer(const std::vector<LogEntry> *entries) const;
        common::Status RequireConflictPointer(const ConflictResult *result) const;
        bool HasLogEntry(common::LogIndex index) const noexcept;
        std::size_t Offset(common::LogIndex index) const noexcept;

        std::vector<LogEntry> entries_;
        common::LogIndex snapshot_index_ = common::kInvalidLogIndex;
        common::Term snapshot_term_ = common::kInitialTerm;
        common::LogIndex stable_index_ = common::kInvalidLogIndex;
        common::LogIndex commit_index_ = common::kInvalidLogIndex;
        common::LogIndex applied_index_ = common::kInvalidLogIndex;
    };

} // namespace cpr::raft
