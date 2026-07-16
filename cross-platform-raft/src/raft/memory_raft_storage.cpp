#include "raft/memory_raft_storage.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>

namespace cpr::raft
{
    namespace
    {

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        bool SameEntry(const LogEntry &lhs, const LogEntry &rhs)
        {
            return lhs.index == rhs.index &&
                   lhs.term == rhs.term &&
                   lhs.type == rhs.type &&
                   lhs.payload == rhs.payload;
        }

    } // namespace

    common::Status MemoryRaftStorage::Open(const std::filesystem::path &)
    {
        open_ = true;
        return common::Status::OK();
    }

    common::Status MemoryRaftStorage::Load(RaftStorageLoadResult *result) const
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }
        if (result == nullptr)
        {
            return Invalid("storage load result must not be null");
        }

        result->hard_state = hard_state_;
        result->entries = entries_;
        result->snapshot = snapshot_;
        result->empty = entries_.empty() &&
                        !snapshot_.has_value() &&
                        hard_state_.current_term == common::kInitialTerm &&
                        hard_state_.voted_for == common::kInvalidNodeId &&
                        hard_state_.commit_index == common::kInvalidLogIndex &&
                        hard_state_.membership_configuration_id == 0;
        return common::Status::OK();
    }

    common::Status MemoryRaftStorage::SaveHardState(const HardState &hard_state)
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }
        if (hard_state.current_term < hard_state_.current_term)
        {
            return Invalid("hard state term cannot move backward");
        }
        if (hard_state.commit_index < hard_state_.commit_index)
        {
            return Invalid("hard state commit index cannot move backward");
        }
        if (hard_state.commit_index > LastLogIndex())
        {
            return Invalid("hard state commit index exceeds available log");
        }

        hard_state_ = hard_state;
        return common::Status::OK();
    }

    common::Status MemoryRaftStorage::AppendEntries(const std::vector<LogEntry> &entries)
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }
        status = ValidateEntryBatch(entries);
        if (!status.ok() || entries.empty())
        {
            return status;
        }

        const common::LogIndex last_log_index = LastLogIndex();
        auto append_from = entries.begin();
        if (entries.front().index <= last_log_index)
        {
            for (auto it = entries.begin();
                 it != entries.end() && it->index <= last_log_index;
                 ++it)
            {
                if (!HasEntry(it->index) || !SameEntry(entries_[Offset(it->index)], *it))
                {
                    return Invalid("append entries cannot overwrite existing log entries");
                }
                append_from = it + 1;
            }
        }

        entries_.insert(entries_.end(), append_from, entries.end());
        return common::Status::OK();
    }

    common::Status MemoryRaftStorage::TruncateSuffix(common::LogIndex first_index)
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }
        if (first_index <= SnapshotIndex())
        {
            return Invalid("truncate index must be after snapshot boundary");
        }
        if (first_index <= hard_state_.commit_index)
        {
            return Invalid("truncate index cannot remove committed entries");
        }
        if (first_index > LastLogIndex() + 1)
        {
            return Invalid("truncate index exceeds append position");
        }
        if (first_index <= LastLogIndex())
        {
            entries_.erase(entries_.begin() + static_cast<std::ptrdiff_t>(Offset(first_index)),
                           entries_.end());
        }
        return common::Status::OK();
    }

    common::Status MemoryRaftStorage::SaveSnapshot(const SnapshotData &snapshot)
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }

        const common::LogIndex index = snapshot.metadata.last_included_index;
        const common::Term term = snapshot.metadata.last_included_term;
        if (index == common::kInvalidLogIndex)
        {
            return Invalid("snapshot index must be positive");
        }
        if (term == common::kInitialTerm)
        {
            return Invalid("snapshot term must be positive");
        }
        if (index < SnapshotIndex())
        {
            return Invalid("snapshot boundary cannot move backward");
        }
        if (index > hard_state_.commit_index)
        {
            return Invalid("snapshot index cannot exceed committed index");
        }

        common::Term stored_term = common::kInitialTerm;
        status = GetTerm(index, &stored_term);
        if (!status.ok())
        {
            return status;
        }
        if (stored_term != term)
        {
            return Invalid("snapshot term does not match stored log term");
        }

        snapshot_ = snapshot;
        entries_.erase(std::remove_if(entries_.begin(),
                                      entries_.end(),
                                      [index](const LogEntry &entry)
                                      {
                                          return entry.index <= index;
                                      }),
                       entries_.end());
        return common::Status::OK();
    }

    common::Status MemoryRaftStorage::RequireOpen() const
    {
        if (!open_)
        {
            return Invalid("raft storage is not open");
        }
        return common::Status::OK();
    }

    common::LogIndex MemoryRaftStorage::SnapshotIndex() const noexcept
    {
        return snapshot_ ? snapshot_->metadata.last_included_index
                         : common::kInvalidLogIndex;
    }

    common::Term MemoryRaftStorage::SnapshotTerm() const noexcept
    {
        return snapshot_ ? snapshot_->metadata.last_included_term
                         : common::kInitialTerm;
    }

    common::LogIndex MemoryRaftStorage::LastLogIndex() const noexcept
    {
        return entries_.empty() ? SnapshotIndex() : entries_.back().index;
    }

    common::LogIndex MemoryRaftStorage::FirstLogIndex() const noexcept
    {
        return SnapshotIndex() + 1;
    }

    bool MemoryRaftStorage::HasEntry(common::LogIndex index) const noexcept
    {
        return index >= FirstLogIndex() && index <= LastLogIndex() && !entries_.empty();
    }

    std::size_t MemoryRaftStorage::Offset(common::LogIndex index) const noexcept
    {
        return static_cast<std::size_t>(index - FirstLogIndex());
    }

    common::Status MemoryRaftStorage::GetTerm(common::LogIndex index,
                                              common::Term *term) const
    {
        if (term == nullptr)
        {
            return Invalid("term output pointer must not be null");
        }
        if (index == common::kInvalidLogIndex)
        {
            *term = common::kInitialTerm;
            return common::Status::OK();
        }
        if (snapshot_ && index == SnapshotIndex())
        {
            *term = SnapshotTerm();
            return common::Status::OK();
        }
        if (!HasEntry(index))
        {
            return common::Status::NotFound("log term not found");
        }
        *term = entries_[Offset(index)].term;
        return common::Status::OK();
    }

    common::Status MemoryRaftStorage::ValidateEntryBatch(
        const std::vector<LogEntry> &entries) const
    {
        if (entries.empty())
        {
            return common::Status::OK();
        }

        const common::LogIndex snapshot_index = SnapshotIndex();
        for (std::size_t i = 0; i < entries.size(); ++i)
        {
            const LogEntry &entry = entries[i];
            if (entry.index <= snapshot_index)
            {
                return Invalid("entry index must be after snapshot boundary");
            }
            if (entry.term == common::kInitialTerm)
            {
                return Invalid("entry term must be positive");
            }
            if (i > 0 && entry.index != entries[i - 1].index + 1)
            {
                return Invalid("entry indexes must be contiguous");
            }
        }

        if (entries.front().index > LastLogIndex() + 1)
        {
            return Invalid("append entries cannot skip log indexes");
        }
        return common::Status::OK();
    }

} // namespace cpr::raft
