#include "raft/raft_log.h"

#include <algorithm>
#include <string>

namespace cpr::raft
{
    namespace
    {

        using cpr::common::LogIndex;
        using cpr::common::Status;
        using cpr::common::Term;

        Status MakeInvalid(std::string message)
        {
            return Status::InvalidArgument(std::move(message));
        }

    } // namespace

    common::Status RaftLog::Append(const LogEntry &entry)
    {
        return Append(std::vector<LogEntry>{entry});
    }

    common::Status RaftLog::Append(const std::vector<LogEntry> &entries)
    {
        if (entries.empty())
        {
            return Status::OK();
        }

        const LogIndex expected_start = last_index() + 1;
        Status status = ValidateEntries(entries, expected_start);
        if (!status.ok())
        {
            return status;
        }

        entries_.insert(entries_.end(), entries.begin(), entries.end());
        return ValidateInvariants();
    }

    common::Status RaftLog::GetEntry(LogIndex index, LogEntry *entry) const
    {
        Status status = RequireEntryPointer(entry);
        if (!status.ok())
        {
            return status;
        }
        if (index <= snapshot_index_)
        {
            return Status::NotFound("log index " + std::to_string(index) +
                                    " is not available as a normal log entry");
        }
        if (!HasLogEntry(index))
        {
            return Status::NotFound("log index " + std::to_string(index) + " not found");
        }
        *entry = entries_[Offset(index)];
        return Status::OK();
    }

    common::Status RaftLog::GetTerm(LogIndex index, Term *term) const
    {
        Status status = RequireTermPointer(term);
        if (!status.ok())
        {
            return status;
        }
        if (index == common::kInvalidLogIndex)
        {
            *term = common::kInitialTerm;
            return Status::OK();
        }
        if (index == snapshot_index_)
        {
            *term = snapshot_term_;
            return Status::OK();
        }
        if (index < snapshot_index_)
        {
            return Status::NotFound("log index " + std::to_string(index) +
                                    " is before the snapshot boundary");
        }
        if (!HasLogEntry(index))
        {
            return Status::NotFound("log index " + std::to_string(index) + " not found");
        }
        *term = entries_[Offset(index)].term;
        return Status::OK();
    }

    common::Status RaftLog::GetEntries(LogIndex begin,
                                       LogIndex end,
                                       std::vector<LogEntry> *entries) const
    {
        Status status = RequireEntriesPointer(entries);
        if (!status.ok())
        {
            return status;
        }
        if (begin > end)
        {
            return MakeInvalid("invalid log range: begin is greater than end");
        }
        if (begin <= snapshot_index_)
        {
            return Status::NotFound("log range begins at or before the snapshot boundary");
        }
        if (end > last_index() + 1)
        {
            return Status::NotFound("log range exceeds the current last index");
        }

        entries->clear();
        if (begin == end)
        {
            return Status::OK();
        }

        entries->insert(entries->end(),
                        entries_.begin() + static_cast<std::ptrdiff_t>(Offset(begin)),
                        entries_.begin() + static_cast<std::ptrdiff_t>(Offset(end)));
        return Status::OK();
    }

    common::Status RaftLog::FindConflict(const std::vector<LogEntry> &incoming,
                                         ConflictResult *result) const
    {
        Status status = RequireConflictPointer(result);
        if (!status.ok())
        {
            return status;
        }
        result->type = ConflictType::NONE;
        result->index = common::kInvalidLogIndex;
        if (incoming.empty())
        {
            return Status::OK();
        }

        status = ValidateEntries(incoming, incoming.front().index);
        if (!status.ok())
        {
            return status;
        }

        for (const LogEntry &entry : incoming)
        {
            if (entry.index <= snapshot_index_)
            {
                return MakeInvalid("incoming log index " + std::to_string(entry.index) +
                                   " is at or before the snapshot boundary");
            }
            if (entry.index > last_index())
            {
                result->type = ConflictType::APPEND;
                result->index = entry.index;
                return Status::OK();
            }

            Term local_term = common::kInitialTerm;
            status = GetTerm(entry.index, &local_term);
            if (!status.ok())
            {
                return status;
            }
            if (local_term != entry.term)
            {
                result->type = ConflictType::CONFLICT;
                result->index = entry.index;
                return Status::OK();
            }
        }

        return Status::OK();
    }

    common::Status RaftLog::TruncateSuffix(LogIndex index)
    {
        if (index <= snapshot_index_)
        {
            return MakeInvalid("cannot truncate at or before the snapshot boundary");
        }
        if (index <= commit_index_)
        {
            return MakeInvalid("cannot truncate committed log entries");
        }
        if (index > last_index() + 1)
        {
            return MakeInvalid("truncate index is beyond the append position");
        }
        if (index == last_index() + 1)
        {
            return Status::OK();
        }

        entries_.resize(Offset(index));
        stable_index_ = std::min(stable_index_, last_index());
        return ValidateInvariants();
    }

    common::Status RaftLog::AdvanceStableTo(LogIndex index)
    {
        if (index < stable_index_)
        {
            return MakeInvalid("stable index cannot move backward");
        }
        if (index > last_index())
        {
            return MakeInvalid("stable index cannot exceed the last index");
        }
        stable_index_ = index;
        return ValidateInvariants();
    }

    common::Status RaftLog::AdvanceCommitTo(LogIndex index)
    {
        if (index < commit_index_)
        {
            return MakeInvalid("commit index cannot move backward");
        }
        if (index > last_index())
        {
            return MakeInvalid("commit index cannot exceed the last index");
        }
        commit_index_ = index;
        return ValidateInvariants();
    }

    common::Status RaftLog::AdvanceAppliedTo(LogIndex index)
    {
        if (index < applied_index_)
        {
            return MakeInvalid("applied index cannot move backward");
        }
        if (index > commit_index_)
        {
            return MakeInvalid("applied index cannot exceed the commit index");
        }
        applied_index_ = index;
        return ValidateInvariants();
    }

    common::Status RaftLog::UpdateSnapshotBoundary(LogIndex index, Term term)
    {
        if (index < snapshot_index_)
        {
            return MakeInvalid("snapshot boundary cannot move backward");
        }
        if (index > applied_index_)
        {
            return MakeInvalid("snapshot boundary cannot exceed the applied index");
        }
        if (index > last_index())
        {
            return MakeInvalid("snapshot boundary cannot exceed the last index");
        }
        if (index > common::kInvalidLogIndex && term == common::kInitialTerm)
        {
            return MakeInvalid("snapshot boundary term must be positive");
        }
        if (index == snapshot_index_)
        {
            if (term != snapshot_term_)
            {
                return MakeInvalid("snapshot boundary term does not match the current boundary");
            }
            return Status::OK();
        }

        Term existing_term = common::kInitialTerm;
        common::Status status = GetTerm(index, &existing_term);
        if (!status.ok())
        {
            return status;
        }
        if (existing_term != term)
        {
            return MakeInvalid("snapshot boundary term does not match the current log");
        }

        if (index > snapshot_index_)
        {
            entries_.erase(entries_.begin(),
                           entries_.begin() + static_cast<std::ptrdiff_t>(Offset(index + 1)));
        }

        snapshot_index_ = index;
        snapshot_term_ = term;
        stable_index_ = std::max(stable_index_, snapshot_index_);
        commit_index_ = std::max(commit_index_, snapshot_index_);
        applied_index_ = std::max(applied_index_, snapshot_index_);
        return ValidateInvariants();
    }

    bool RaftLog::Matches(LogIndex index, Term term) const
    {
        Term local_term = common::kInitialTerm;
        if (!GetTerm(index, &local_term).ok())
        {
            return false;
        }
        return local_term == term;
    }

    bool RaftLog::empty() const noexcept
    {
        return entries_.empty();
    }

    std::size_t RaftLog::size() const noexcept
    {
        return entries_.size();
    }

    LogIndex RaftLog::first_index() const noexcept
    {
        return snapshot_index_ + 1;
    }

    LogIndex RaftLog::last_index() const noexcept
    {
        if (entries_.empty())
        {
            return snapshot_index_;
        }
        return entries_.back().index;
    }

    LogIndex RaftLog::snapshot_index() const noexcept
    {
        return snapshot_index_;
    }

    Term RaftLog::snapshot_term() const noexcept
    {
        return snapshot_term_;
    }

    LogIndex RaftLog::stable_index() const noexcept
    {
        return stable_index_;
    }

    LogIndex RaftLog::commit_index() const noexcept
    {
        return commit_index_;
    }

    LogIndex RaftLog::applied_index() const noexcept
    {
        return applied_index_;
    }

    common::Status RaftLog::ValidateEntries(const std::vector<LogEntry> &entries,
                                            LogIndex expected_start) const
    {
        if (entries.empty())
        {
            return Status::OK();
        }
        if (entries.front().index != expected_start)
        {
            return MakeInvalid("log append start index does not match the expected position");
        }
        for (std::size_t i = 0; i < entries.size(); ++i)
        {
            const LogEntry &entry = entries[i];
            const LogIndex expected_index = expected_start + static_cast<LogIndex>(i);
            if (entry.index != expected_index)
            {
                return MakeInvalid("log indexes must be contiguous and ordered");
            }
            if (entry.index <= snapshot_index_)
            {
                return MakeInvalid("log index " + std::to_string(entry.index) +
                                   " is at or before the snapshot boundary");
            }
            if (entry.term == common::kInitialTerm)
            {
                return MakeInvalid("log term must be positive for appended entries");
            }
        }
        return Status::OK();
    }

    common::Status RaftLog::ValidateInvariants() const
    {
        if (applied_index_ > commit_index_)
        {
            return Status::InternalError("applied index exceeds commit index");
        }
        if (commit_index_ > last_index())
        {
            return Status::InternalError("commit index exceeds last index");
        }
        if (stable_index_ > last_index())
        {
            return Status::InternalError("stable index exceeds last index");
        }
        if (snapshot_index_ > last_index())
        {
            return Status::InternalError("snapshot index exceeds last index");
        }
        if (commit_index_ < snapshot_index_)
        {
            return Status::InternalError("commit index is behind the snapshot boundary");
        }
        if (applied_index_ < snapshot_index_)
        {
            return Status::InternalError("applied index is behind the snapshot boundary");
        }
        if (stable_index_ < snapshot_index_)
        {
            return Status::InternalError("stable index is behind the snapshot boundary");
        }
        LogIndex expected = snapshot_index_ + 1;
        for (const LogEntry &entry : entries_)
        {
            if (entry.index != expected)
            {
                return Status::InternalError("log indexes are not contiguous");
            }
            if (entry.term == common::kInitialTerm)
            {
                return Status::InternalError("log term must be positive");
            }
            ++expected;
        }
        return Status::OK();
    }

    common::Status RaftLog::RequireEntryPointer(const LogEntry *entry) const
    {
        if (entry == nullptr)
        {
            return MakeInvalid("entry output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftLog::RequireTermPointer(const Term *term) const
    {
        if (term == nullptr)
        {
            return MakeInvalid("term output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftLog::RequireEntriesPointer(const std::vector<LogEntry> *entries) const
    {
        if (entries == nullptr)
        {
            return MakeInvalid("entries output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftLog::RequireConflictPointer(const ConflictResult *result) const
    {
        if (result == nullptr)
        {
            return MakeInvalid("conflict result pointer must not be null");
        }
        return Status::OK();
    }

    bool RaftLog::HasLogEntry(LogIndex index) const noexcept
    {
        return index > snapshot_index_ && index <= last_index();
    }

    std::size_t RaftLog::Offset(LogIndex index) const noexcept
    {
        return static_cast<std::size_t>(index - snapshot_index_ - 1);
    }

} // namespace cpr::raft
