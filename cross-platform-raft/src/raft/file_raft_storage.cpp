#include "raft/file_raft_storage.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

#include "common/checksum.h"
#include "platform/file_ops.h"
#include "raft/raft_snapshot.h"

namespace cpr::raft
{
    namespace
    {

        using platform::FileData;

        constexpr std::uint32_t kFormatVersion = 1;
        constexpr std::uint32_t kHardStateRecord = 1;
        constexpr std::uint32_t kLogEntryRecord = 2;
        constexpr std::uint64_t kMaxPayloadLength = 64ULL * 1024ULL * 1024ULL;
        constexpr std::size_t kRecordHeaderSize = 40;
        constexpr const char *kHardStateFileName = "hard_state.bin";
        constexpr const char *kLogFileName = "log.bin";
        constexpr const char *kSnapshotFileName = "snapshot.bin";

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        common::Status Corrupt(std::string message)
        {
            return common::Status::Corruption(std::move(message));
        }

        void PutU32(FileData *data, std::uint32_t value)
        {
            for (int shift = 0; shift < 32; shift += 8)
            {
                data->push_back(static_cast<common::Byte>((value >> shift) & 0xffU));
            }
        }

        void PutU64(FileData *data, std::uint64_t value)
        {
            for (int shift = 0; shift < 64; shift += 8)
            {
                data->push_back(static_cast<common::Byte>((value >> shift) & 0xffULL));
            }
        }

        common::Status ReadU32(const FileData &data,
                               std::size_t *offset,
                               std::uint32_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return Invalid("read pointer must not be null");
            }
            if (data.size() - *offset < 4)
            {
                return Corrupt("record is truncated while reading uint32");
            }
            std::uint32_t result = 0;
            for (int shift = 0; shift < 32; shift += 8)
            {
                result |= static_cast<std::uint32_t>(data[(*offset)++]) << shift;
            }
            *value = result;
            return common::Status::OK();
        }

        common::Status ReadU64(const FileData &data,
                               std::size_t *offset,
                               std::uint64_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return Invalid("read pointer must not be null");
            }
            if (data.size() - *offset < 8)
            {
                return Corrupt("record is truncated while reading uint64");
            }
            std::uint64_t result = 0;
            for (int shift = 0; shift < 64; shift += 8)
            {
                result |= static_cast<std::uint64_t>(data[(*offset)++]) << shift;
            }
            *value = result;
            return common::Status::OK();
        }

        FileData ChecksumInput(std::uint32_t record_type,
                               common::LogIndex index,
                               common::Term term,
                               const FileData &payload)
        {
            FileData data;
            PutU32(&data, kFormatVersion);
            PutU32(&data, record_type);
            PutU64(&data, index);
            PutU64(&data, term);
            PutU64(&data, static_cast<std::uint64_t>(payload.size()));
            data.insert(data.end(), payload.begin(), payload.end());
            return data;
        }

        void AppendRecord(FileData *data,
                          std::uint32_t record_type,
                          common::LogIndex index,
                          common::Term term,
                          const FileData &payload)
        {
            const FileData checksum_input = ChecksumInput(record_type, index, term, payload);
            const common::ChecksumValue checksum = common::Checksum::Compute(checksum_input);
            PutU32(data, kFormatVersion);
            PutU32(data, record_type);
            PutU64(data, index);
            PutU64(data, term);
            PutU64(data, static_cast<std::uint64_t>(payload.size()));
            PutU64(data, checksum);
            data->insert(data->end(), payload.begin(), payload.end());
        }

        FileData EncodeHardStatePayload(const HardState &hard_state)
        {
            FileData payload;
            PutU64(&payload, hard_state.voted_for);
            PutU64(&payload, hard_state.commit_index);
            PutU64(&payload, hard_state.membership_configuration_id);
            return payload;
        }

        common::Status DecodeHardStatePayload(const FileData &payload,
                                              common::Term term,
                                              HardState *hard_state)
        {
            if (hard_state == nullptr)
            {
                return Invalid("hard state output pointer must not be null");
            }
            if (payload.size() != 24)
            {
                return Corrupt("hard state payload has invalid length");
            }
            std::size_t offset = 0;
            HardState decoded;
            decoded.current_term = term;
            common::Status status = ReadU64(payload, &offset, &decoded.voted_for);
            if (!status.ok())
            {
                return status;
            }
            status = ReadU64(payload, &offset, &decoded.commit_index);
            if (!status.ok())
            {
                return status;
            }
            status = ReadU64(payload, &offset, &decoded.membership_configuration_id);
            if (!status.ok())
            {
                return status;
            }
            *hard_state = decoded;
            return common::Status::OK();
        }

        FileData EncodeLogEntryPayload(const LogEntry &entry)
        {
            FileData payload;
            PutU32(&payload, static_cast<std::uint32_t>(entry.type));
            payload.insert(payload.end(), entry.payload.begin(), entry.payload.end());
            return payload;
        }

        common::Status DecodeLogEntryPayload(const FileData &payload,
                                             common::LogIndex index,
                                             common::Term term,
                                             LogEntry *entry)
        {
            if (entry == nullptr)
            {
                return Invalid("log entry output pointer must not be null");
            }
            if (payload.size() < 4)
            {
                return Corrupt("log entry payload is too short");
            }
            std::size_t offset = 0;
            std::uint32_t type = 0;
            common::Status status = ReadU32(payload, &offset, &type);
            if (!status.ok())
            {
                return status;
            }
            if (type > static_cast<std::uint32_t>(LogEntryType::MEMBERSHIP_CHANGE))
            {
                return Corrupt("log entry type is invalid");
            }
            entry->index = index;
            entry->term = term;
            entry->type = static_cast<LogEntryType>(type);
            entry->payload.assign(payload.begin() + static_cast<std::ptrdiff_t>(offset),
                                  payload.end());
            return common::Status::OK();
        }

        struct DecodedRecord
        {
            std::uint32_t type = 0;
            common::LogIndex index = common::kInvalidLogIndex;
            common::Term term = common::kInitialTerm;
            FileData payload;
        };

        common::Status DecodeRecords(const FileData &data,
                                     std::vector<DecodedRecord> *records,
                                     bool allow_incomplete_tail = false,
                                     bool *had_incomplete_tail = nullptr)
        {
            if (records == nullptr)
            {
                return Invalid("record output pointer must not be null");
            }
            records->clear();
            if (had_incomplete_tail != nullptr)
            {
                *had_incomplete_tail = false;
            }
            std::size_t offset = 0;
            while (offset < data.size())
            {
                if (data.size() - offset < kRecordHeaderSize)
                {
                    if (allow_incomplete_tail)
                    {
                        if (had_incomplete_tail != nullptr)
                        {
                            *had_incomplete_tail = true;
                        }
                        return common::Status::OK();
                    }
                    return Corrupt("incomplete record tail");
                }

                const std::size_t record_begin = offset;
                std::uint32_t version = 0;
                DecodedRecord record;
                std::uint64_t payload_length = 0;
                std::uint64_t checksum = 0;
                common::Status status = ReadU32(data, &offset, &version);
                if (!status.ok())
                {
                    return status;
                }
                status = ReadU32(data, &offset, &record.type);
                if (!status.ok())
                {
                    return status;
                }
                status = ReadU64(data, &offset, &record.index);
                if (!status.ok())
                {
                    return status;
                }
                status = ReadU64(data, &offset, &record.term);
                if (!status.ok())
                {
                    return status;
                }
                status = ReadU64(data, &offset, &payload_length);
                if (!status.ok())
                {
                    return status;
                }
                status = ReadU64(data, &offset, &checksum);
                if (!status.ok())
                {
                    return status;
                }
                if (version != kFormatVersion)
                {
                    return Corrupt("unsupported storage record version");
                }
                if (record.type != kHardStateRecord && record.type != kLogEntryRecord)
                {
                    return Corrupt("unknown storage record type");
                }
                if (payload_length > kMaxPayloadLength)
                {
                    return Corrupt("storage record payload is too large");
                }
                if (payload_length > data.size() - offset)
                {
                    if (allow_incomplete_tail)
                    {
                        if (had_incomplete_tail != nullptr)
                        {
                            *had_incomplete_tail = true;
                        }
                        return common::Status::OK();
                    }
                    return Corrupt("incomplete record payload");
                }

                record.payload.assign(data.begin() + static_cast<std::ptrdiff_t>(offset),
                                      data.begin() + static_cast<std::ptrdiff_t>(
                                                         offset + payload_length));
                offset += static_cast<std::size_t>(payload_length);

                const FileData expected_input =
                    ChecksumInput(record.type, record.index, record.term, record.payload);
                if (common::Checksum::Compute(expected_input) != checksum)
                {
                    return Corrupt("storage record checksum mismatch at offset " +
                                   std::to_string(record_begin));
                }
                records->push_back(std::move(record));
            }
            return common::Status::OK();
        }

        common::Status ReadFileIfPresent(const std::filesystem::path &path,
                                         FileData *data,
                                         bool *exists_out = nullptr)
        {
            bool exists = false;
            common::Status status = platform::Exists(path, &exists);
            if (!status.ok())
            {
                return status;
            }
            if (exists_out != nullptr)
            {
                *exists_out = exists;
            }
            data->clear();
            if (!exists)
            {
                return common::Status::OK();
            }
            platform::FileHandle file;
            status = platform::FileHandle::OpenExisting(path, &file);
            if (!status.ok())
            {
                return status;
            }
            return file.ReadAll(data);
        }

        bool SameEntry(const LogEntry &lhs, const LogEntry &rhs)
        {
            return lhs.index == rhs.index &&
                   lhs.term == rhs.term &&
                   lhs.type == rhs.type &&
                   lhs.payload == rhs.payload;
        }

        FileData EncodeHardStateFile(const HardState &hard_state)
        {
            FileData data;
            AppendRecord(&data,
                         kHardStateRecord,
                         hard_state.commit_index,
                         hard_state.current_term,
                         EncodeHardStatePayload(hard_state));
            return data;
        }

        FileData EncodeLogFile(const std::vector<LogEntry> &entries)
        {
            FileData data;
            for (const LogEntry &entry : entries)
            {
                AppendRecord(&data,
                             kLogEntryRecord,
                             entry.index,
                             entry.term,
                             EncodeLogEntryPayload(entry));
            }
            return data;
        }

        common::LogIndex LastIndex(const std::optional<SnapshotData> &snapshot,
                                   const std::vector<LogEntry> &entries)
        {
            if (!entries.empty())
            {
                return entries.back().index;
            }
            return snapshot ? snapshot->metadata.last_included_index
                            : common::kInvalidLogIndex;
        }

    } // namespace

    common::Status FileRaftStorage::Open(const std::filesystem::path &storage_path)
    {
        if (storage_path.empty())
        {
            return Invalid("storage path must not be empty");
        }
        common::Status status = platform::CreateDirectories(storage_path);
        if (!status.ok())
        {
            return status;
        }

        hard_state_path_ = storage_path / kHardStateFileName;
        log_path_ = storage_path / kLogFileName;
        snapshot_path_ = storage_path / kSnapshotFileName;

        platform::FileHandle file;
        status = platform::FileHandle::OpenOrCreate(hard_state_path_, &file);
        if (!status.ok())
        {
            return status;
        }
        status = file.Close();
        if (!status.ok())
        {
            return status;
        }
        status = platform::FileHandle::OpenOrCreate(log_path_, &file);
        if (!status.ok())
        {
            return status;
        }
        status = file.Close();
        if (!status.ok())
        {
            return status;
        }

        HardState loaded_hard_state;
        std::vector<LogEntry> loaded_entries;
        std::optional<SnapshotData> loaded_snapshot;
        status = LoadFromDisk(&loaded_hard_state, &loaded_entries, &loaded_snapshot);
        if (!status.ok())
        {
            open_ = false;
            return status;
        }

        hard_state_ = loaded_hard_state;
        entries_ = std::move(loaded_entries);
        snapshot_ = std::move(loaded_snapshot);
        open_ = true;
        return common::Status::OK();
    }

    common::Status FileRaftStorage::Load(RaftStorageLoadResult *result) const
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

        HardState hard_state;
        std::vector<LogEntry> entries;
        std::optional<SnapshotData> snapshot;
        status = LoadFromDisk(&hard_state, &entries, &snapshot);
        if (!status.ok())
        {
            return status;
        }
        result->hard_state = hard_state;
        result->entries = std::move(entries);
        result->snapshot = std::move(snapshot);
        result->empty = result->entries.empty() &&
                        !result->snapshot.has_value() &&
                        result->hard_state.current_term == common::kInitialTerm &&
                        result->hard_state.voted_for == common::kInvalidNodeId &&
                        result->hard_state.commit_index == common::kInvalidLogIndex &&
                        result->hard_state.membership_configuration_id == 0;
        return common::Status::OK();
    }

    common::Status FileRaftStorage::SaveHardState(const HardState &hard_state)
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }
        status = ValidateHardState(hard_state);
        if (!status.ok())
        {
            return status;
        }
        status = platform::AtomicReplace(hard_state_path_, EncodeHardStateFile(hard_state));
        if (!status.ok())
        {
            return status;
        }
        hard_state_ = hard_state;
        return common::Status::OK();
    }

    common::Status FileRaftStorage::AppendEntries(const std::vector<LogEntry> &entries)
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
        if (append_from == entries.end())
        {
            return common::Status::OK();
        }

        std::vector<LogEntry> next_entries = entries_;
        next_entries.insert(next_entries.end(), append_from, entries.end());
        status = platform::AtomicReplace(log_path_, EncodeLogFile(next_entries));
        if (!status.ok())
        {
            return status;
        }
        entries_ = std::move(next_entries);
        return common::Status::OK();
    }

    common::Status FileRaftStorage::TruncateSuffix(common::LogIndex first_index)
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }
        if (first_index <= hard_state_.commit_index)
        {
            return Invalid("truncate index cannot remove committed entries");
        }
        if (first_index > LastLogIndex() + 1)
        {
            return Invalid("truncate index exceeds append position");
        }
        if (first_index > LastLogIndex())
        {
            return common::Status::OK();
        }

        std::vector<LogEntry> next_entries;
        next_entries.reserve(entries_.size());
        for (const LogEntry &entry : entries_)
        {
            if (entry.index < first_index)
            {
                next_entries.push_back(entry);
            }
        }

        status = platform::AtomicReplace(log_path_, EncodeLogFile(next_entries));
        if (!status.ok())
        {
            return status;
        }
        entries_ = std::move(next_entries);
        return common::Status::OK();
    }

    common::Status FileRaftStorage::SaveSnapshot(const SnapshotData &snapshot)
    {
        common::Status status = RequireOpen();
        if (!status.ok())
        {
            return status;
        }
        status = ValidateSnapshot(snapshot);
        if (!status.ok())
        {
            return status;
        }

        SnapshotBytes snapshot_bytes;
        status = EncodeSnapshotData(snapshot, &snapshot_bytes);
        if (!status.ok())
        {
            return status;
        }

        std::vector<LogEntry> next_entries;
        next_entries.reserve(entries_.size());
        for (const LogEntry &entry : entries_)
        {
            if (entry.index > snapshot.metadata.last_included_index)
            {
                next_entries.push_back(entry);
            }
        }

        status = platform::AtomicReplace(snapshot_path_, snapshot_bytes);
        if (!status.ok())
        {
            return status;
        }
        status = platform::AtomicReplace(log_path_, EncodeLogFile(next_entries));
        if (!status.ok())
        {
            return status;
        }

        snapshot_ = snapshot;
        entries_ = std::move(next_entries);
        return common::Status::OK();
    }

    common::Status FileRaftStorage::RequireOpen() const
    {
        if (!open_)
        {
            return Invalid("raft file storage is not open");
        }
        return common::Status::OK();
    }

    common::Status FileRaftStorage::LoadFromDisk(
        HardState *hard_state,
        std::vector<LogEntry> *entries,
        std::optional<SnapshotData> *snapshot) const
    {
        if (hard_state == nullptr || entries == nullptr || snapshot == nullptr)
        {
            return Invalid("load output pointer must not be null");
        }
        snapshot->reset();

        FileData snapshot_data;
        bool snapshot_exists = false;
        common::Status status = ReadFileIfPresent(snapshot_path_,
                                                  &snapshot_data,
                                                  &snapshot_exists);
        if (!status.ok())
        {
            return status;
        }
        if (snapshot_exists)
        {
            SnapshotData decoded_snapshot;
            status = DecodeSnapshotData(snapshot_data, &decoded_snapshot);
            if (!status.ok())
            {
                return status;
            }
            *snapshot = std::move(decoded_snapshot);
        }

        FileData hard_state_data;
        status = ReadFileIfPresent(hard_state_path_, &hard_state_data);
        if (!status.ok())
        {
            return status;
        }
        *hard_state = HardState();
        if (!hard_state_data.empty())
        {
            std::vector<DecodedRecord> records;
            status = DecodeRecords(hard_state_data, &records);
            if (!status.ok())
            {
                return status;
            }
            if (records.size() != 1 || records.front().type != kHardStateRecord)
            {
                return Corrupt("hard state file must contain one hard state record");
            }
            status = DecodeHardStatePayload(records.front().payload,
                                            records.front().term,
                                            hard_state);
            if (!status.ok())
            {
                return status;
            }
            if (records.front().index != hard_state->commit_index)
            {
                return Corrupt("hard state record index does not match commit index");
            }
        }

        FileData log_data;
        status = ReadFileIfPresent(log_path_, &log_data);
        if (!status.ok())
        {
            return status;
        }
        entries->clear();
        if (!log_data.empty())
        {
            std::vector<DecodedRecord> records;
            bool had_incomplete_tail = false;
            status = DecodeRecords(log_data, &records, true, &had_incomplete_tail);
            if (!status.ok())
            {
                return status;
            }
            entries->reserve(records.size());
            const common::LogIndex snapshot_index =
                *snapshot ? (*snapshot)->metadata.last_included_index
                          : common::kInvalidLogIndex;
            for (const DecodedRecord &record : records)
            {
                if (record.type != kLogEntryRecord)
                {
                    return Corrupt("log file contains non-log record");
                }
                if (record.index == common::kInvalidLogIndex ||
                    record.term == common::kInitialTerm)
                {
                    return Corrupt("log record has invalid index or term");
                }
                LogEntry entry;
                status = DecodeLogEntryPayload(record.payload,
                                               record.index,
                                               record.term,
                                               &entry);
                if (!status.ok())
                {
                    return status;
                }
                if (entry.index <= snapshot_index)
                {
                    continue;
                }
                if (!entries->empty() && entry.index != entries->back().index + 1)
                {
                    return Corrupt("log records are not contiguous");
                }
                if (entries->empty() && entry.index != snapshot_index + 1)
                {
                    return Corrupt("log records do not start after snapshot boundary");
                }
                entries->push_back(std::move(entry));
            }
            if (had_incomplete_tail)
            {
                status = platform::AtomicReplace(log_path_, EncodeLogFile(*entries));
                if (!status.ok())
                {
                    return status;
                }
            }
        }
        const common::LogIndex last_index = LastIndex(*snapshot, *entries);
        if (hard_state->commit_index > last_index)
        {
            return Corrupt("hard state commit index exceeds loaded log");
        }
        if (*snapshot && hard_state->commit_index < (*snapshot)->metadata.last_included_index)
        {
            return Corrupt("hard state commit index is behind snapshot boundary");
        }
        return common::Status::OK();
    }

    common::Status FileRaftStorage::ValidateHardState(const HardState &hard_state) const
    {
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
        return common::Status::OK();
    }

    common::Status FileRaftStorage::ValidateEntryBatch(
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
            if (entry.payload.size() + 4U > kMaxPayloadLength)
            {
                return Invalid("entry payload is too large");
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

    common::Status FileRaftStorage::ValidateSnapshot(
        const SnapshotData &snapshot) const
    {
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
        common::Status status = GetTerm(index, &stored_term);
        if (!status.ok())
        {
            return status;
        }
        if (stored_term != term)
        {
            return Invalid("snapshot term does not match stored log term");
        }
        return common::Status::OK();
    }

    common::Status FileRaftStorage::GetTerm(common::LogIndex index,
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

    common::LogIndex FileRaftStorage::SnapshotIndex() const noexcept
    {
        return snapshot_ ? snapshot_->metadata.last_included_index
                         : common::kInvalidLogIndex;
    }

    common::Term FileRaftStorage::SnapshotTerm() const noexcept
    {
        return snapshot_ ? snapshot_->metadata.last_included_term
                         : common::kInitialTerm;
    }

    common::LogIndex FileRaftStorage::FirstLogIndex() const noexcept
    {
        return SnapshotIndex() + 1;
    }

    common::LogIndex FileRaftStorage::LastLogIndex() const noexcept
    {
        return entries_.empty() ? SnapshotIndex() : entries_.back().index;
    }

    bool FileRaftStorage::HasEntry(common::LogIndex index) const noexcept
    {
        return !entries_.empty() &&
               index >= FirstLogIndex() &&
               index <= entries_.back().index;
    }

    std::size_t FileRaftStorage::Offset(common::LogIndex index) const noexcept
    {
        return static_cast<std::size_t>(index - FirstLogIndex());
    }

} // namespace cpr::raft
