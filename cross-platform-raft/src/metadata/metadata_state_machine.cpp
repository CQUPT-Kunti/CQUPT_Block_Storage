#include "metadata/metadata_state_machine.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

#include "metadata/metadata_command.h"

namespace cpr::metadata
{
    namespace
    {

        constexpr std::uint8_t kMetadataSnapshotFormatVersion = 1;
        constexpr std::size_t kMaxMetadataSnapshotRecords = 4096;
        constexpr std::size_t kSnapshotHeaderBytes = 1 + 4 + 8 + 8;

        common::Status RequireRecord(MetadataStateRecord *record)
        {
            if (record == nullptr)
            {
                return common::Status::InvalidArgument("record must not be null");
            }
            return common::Status::OK();
        }

        common::Status RequireSnapshotPayload(raft::OpaquePayload *payload)
        {
            if (payload == nullptr)
            {
                return common::Status::InvalidArgument("snapshot payload must not be null");
            }
            return common::Status::OK();
        }

        common::Status RequireAppliedPointers(common::LogIndex *index,
                                              common::Term *term)
        {
            if (index == nullptr || term == nullptr)
            {
                return common::Status::InvalidArgument("applied pointers must not be null");
            }
            return common::Status::OK();
        }

        common::Status ValidateSnapshotLength(std::size_t size,
                                              std::size_t max_size,
                                              const char *name)
        {
            if (size > max_size)
            {
                return common::Status::Corruption(
                    std::string(name) + " exceeds limit");
            }
            return common::Status::OK();
        }

        void AppendU8(std::uint8_t value, raft::OpaquePayload *payload)
        {
            payload->push_back(value);
        }

        void AppendU32(std::uint32_t value, raft::OpaquePayload *payload)
        {
            payload->push_back(static_cast<common::Byte>((value >> 24) & 0xFF));
            payload->push_back(static_cast<common::Byte>((value >> 16) & 0xFF));
            payload->push_back(static_cast<common::Byte>((value >> 8) & 0xFF));
            payload->push_back(static_cast<common::Byte>(value & 0xFF));
        }

        void AppendU64(std::uint64_t value, raft::OpaquePayload *payload)
        {
            for (int shift = 56; shift >= 0; shift -= 8)
            {
                payload->push_back(
                    static_cast<common::Byte>((value >> shift) & 0xFF));
            }
        }

        void AppendBytes(const std::string &value, raft::OpaquePayload *payload)
        {
            if (!value.empty())
            {
                payload->insert(payload->end(), value.begin(), value.end());
            }
        }

        void AppendBytes(const raft::OpaquePayload &value, raft::OpaquePayload *payload)
        {
            if (!value.empty())
            {
                payload->insert(payload->end(), value.begin(), value.end());
            }
        }

        class Decoder
        {
        public:
            explicit Decoder(const raft::OpaquePayload &payload)
                : payload_(payload)
            {
            }

            common::Status ReadU8(std::uint8_t *value)
            {
                if (value == nullptr)
                {
                    return common::Status::InvalidArgument("value must not be null");
                }
                if (remaining() < 1)
                {
                    return common::Status::Corruption("truncated metadata snapshot");
                }
                *value = payload_[offset_++];
                return common::Status::OK();
            }

            common::Status ReadU32(std::uint32_t *value)
            {
                if (value == nullptr)
                {
                    return common::Status::InvalidArgument("value must not be null");
                }
                if (remaining() < 4)
                {
                    return common::Status::Corruption("truncated metadata snapshot");
                }
                *value =
                    (static_cast<std::uint32_t>(payload_[offset_]) << 24) |
                    (static_cast<std::uint32_t>(payload_[offset_ + 1]) << 16) |
                    (static_cast<std::uint32_t>(payload_[offset_ + 2]) << 8) |
                    static_cast<std::uint32_t>(payload_[offset_ + 3]);
                offset_ += 4;
                return common::Status::OK();
            }

            common::Status ReadU64(std::uint64_t *value)
            {
                if (value == nullptr)
                {
                    return common::Status::InvalidArgument("value must not be null");
                }
                if (remaining() < 8)
                {
                    return common::Status::Corruption("truncated metadata snapshot");
                }
                std::uint64_t result = 0;
                for (int i = 0; i < 8; ++i)
                {
                    result = (result << 8) | payload_[offset_ + i];
                }
                offset_ += 8;
                *value = result;
                return common::Status::OK();
            }

            common::Status ReadString(std::size_t length,
                                      std::size_t max_length,
                                      std::string *value)
            {
                if (value == nullptr)
                {
                    return common::Status::InvalidArgument("value must not be null");
                }
                if (length > max_length)
                {
                    return common::Status::Corruption("metadata snapshot string length exceeds limit");
                }
                if (length > remaining())
                {
                    return common::Status::Corruption("truncated metadata snapshot");
                }
                *value = std::string(
                    reinterpret_cast<const char *>(&payload_[offset_]),
                    length);
                offset_ += length;
                return common::Status::OK();
            }

            common::Status ReadPayload(std::size_t length,
                                       std::size_t max_length,
                                       raft::OpaquePayload *value)
            {
                if (value == nullptr)
                {
                    return common::Status::InvalidArgument("value must not be null");
                }
                if (length > max_length)
                {
                    return common::Status::Corruption("metadata snapshot payload length exceeds limit");
                }
                if (length > remaining())
                {
                    return common::Status::Corruption("truncated metadata snapshot");
                }
                value->assign(payload_.begin() + static_cast<std::ptrdiff_t>(offset_),
                              payload_.begin() + static_cast<std::ptrdiff_t>(offset_ + length));
                offset_ += length;
                return common::Status::OK();
            }

            std::size_t remaining() const
            {
                return payload_.size() - offset_;
            }

        private:
            const raft::OpaquePayload &payload_;
            std::size_t offset_ = 0;
        };

        common::Status ValidateSnapshotRecord(const MetadataStateRecord &record,
                                              common::LogIndex last_applied_index,
                                              common::Term last_applied_term)
        {
            if (record.target_id.empty())
            {
                return common::Status::Corruption("metadata snapshot target id must not be empty");
            }
            if (record.generation == 0)
            {
                return common::Status::Corruption("metadata snapshot generation must be non-zero");
            }
            if (record.last_command_id.empty())
            {
                return common::Status::Corruption("metadata snapshot command id must not be empty");
            }
            if (record.last_update_index == common::kInvalidLogIndex ||
                record.last_update_term == common::kInitialTerm)
            {
                return common::Status::Corruption("metadata snapshot update progress is invalid");
            }
            if (record.last_update_index > last_applied_index)
            {
                return common::Status::Corruption("metadata snapshot record index exceeds applied index");
            }
            if (record.last_update_term > last_applied_term)
            {
                return common::Status::Corruption("metadata snapshot record term exceeds applied term");
            }
            common::Status status = ValidateSnapshotLength(
                record.target_id.size(),
                kMaxMetadataCommandTargetBytes,
                "metadata snapshot target id");
            if (!status.ok())
            {
                return status;
            }
            status = ValidateSnapshotLength(
                record.last_command_id.size(),
                kMaxMetadataCommandIdBytes,
                "metadata snapshot command id");
            if (!status.ok())
            {
                return status;
            }
            status = ValidateSnapshotLength(
                record.payload.size(),
                kMaxMetadataCommandPayloadBytes,
                "metadata snapshot payload");
            if (!status.ok())
            {
                return status;
            }
            return common::Status::OK();
        }

        common::Status ValidateProgress(common::LogIndex index, common::Term term)
        {
            if (index == common::kInvalidLogIndex)
            {
                if (term != common::kInitialTerm)
                {
                    return common::Status::Corruption(
                        "metadata snapshot term must be zero when applied index is zero");
                }
                return common::Status::OK();
            }
            if (term == common::kInitialTerm)
            {
                return common::Status::Corruption(
                    "metadata snapshot applied term must be non-zero");
            }
            return common::Status::OK();
        }

    } // namespace

    common::Status MetadataStateMachine::Apply(
        common::LogIndex index,
        common::Term term,
        const raft::OpaquePayload &command_payload)
    {
        if (index == common::kInvalidLogIndex)
        {
            return common::Status::InvalidArgument("metadata apply index must be non-zero");
        }
        if (term == common::kInitialTerm)
        {
            return common::Status::InvalidArgument("metadata apply term must be non-zero");
        }

        MetadataCommand command;
        common::Status status = DecodeMetadataCommand(command_payload, &command);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        const common::LogIndex expected_index = last_applied_index_ + 1;
        if (index == last_applied_index_)
        {
            return common::Status::Busy("metadata log index already applied");
        }
        if (index < expected_index)
        {
            return common::Status::Busy("metadata log index moved backward");
        }
        if (index > expected_index)
        {
            return common::Status::Busy("metadata log index gap is not allowed");
        }

        MetadataStateRecord candidate;
        auto it = records_.find(command.target_id);
        const bool exists = it != records_.end();
        if (exists)
        {
            candidate = it->second;
        }
        else
        {
            candidate.target_id = command.target_id;
        }

        if (command.expected_generation.has_value())
        {
            if (!exists)
            {
                return common::Status::NotFound("metadata target not found for expected generation");
            }
            if (candidate.generation != command.expected_generation.value())
            {
                return common::Status::Busy("metadata expected generation mismatch");
            }
        }

        candidate.payload = command.payload;
        candidate.generation = exists ? candidate.generation + 1 : 1;
        candidate.last_command_id = command.command_id;
        candidate.last_update_index = index;
        candidate.last_update_term = term;

        RecordMap next_records = records_;
        next_records[candidate.target_id] = std::move(candidate);

        records_ = std::move(next_records);
        last_applied_index_ = index;
        last_applied_term_ = term;
        return common::Status::OK();
    }

    common::Status MetadataStateMachine::CreateSnapshot(
        common::LogIndex last_applied_index,
        common::Term last_applied_term,
        raft::OpaquePayload *snapshot_payload)
    {
        common::Status status = RequireSnapshotPayload(snapshot_payload);
        if (!status.ok())
        {
            return status;
        }

        RecordMap records_copy;
        common::LogIndex current_index = common::kInvalidLogIndex;
        common::Term current_term = common::kInitialTerm;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            current_index = last_applied_index_;
            current_term = last_applied_term_;
            records_copy = records_;
        }

        if (last_applied_index != current_index || last_applied_term != current_term)
        {
            return common::Status::Busy("metadata snapshot progress does not match state");
        }

        raft::OpaquePayload encoded;
        encoded.reserve(kSnapshotHeaderBytes);
        AppendU8(kMetadataSnapshotFormatVersion, &encoded);
        AppendU32(static_cast<std::uint32_t>(records_copy.size()), &encoded);
        AppendU64(current_index, &encoded);
        AppendU64(current_term, &encoded);

        for (const auto &[target_id, record] : records_copy)
        {
            (void)target_id;
            status = ValidateSnapshotRecord(record, current_index, current_term);
            if (!status.ok())
            {
                return status;
            }
            AppendU32(static_cast<std::uint32_t>(record.target_id.size()), &encoded);
            AppendU32(static_cast<std::uint32_t>(record.payload.size()), &encoded);
            AppendU32(static_cast<std::uint32_t>(record.last_command_id.size()), &encoded);
            AppendU64(record.generation, &encoded);
            AppendU64(record.last_update_index, &encoded);
            AppendU64(record.last_update_term, &encoded);
            AppendBytes(record.target_id, &encoded);
            AppendBytes(record.payload, &encoded);
            AppendBytes(record.last_command_id, &encoded);
        }

        *snapshot_payload = std::move(encoded);
        return common::Status::OK();
    }

    common::Status MetadataStateMachine::RestoreSnapshot(
        const raft::OpaquePayload &snapshot_payload)
    {
        Decoder decoder(snapshot_payload);

        std::uint8_t version = 0;
        std::uint32_t record_count = 0;
        std::uint64_t last_applied_index = common::kInvalidLogIndex;
        std::uint64_t last_applied_term = common::kInitialTerm;

        common::Status status = decoder.ReadU8(&version);
        if (!status.ok())
        {
            return status;
        }
        if (version != kMetadataSnapshotFormatVersion)
        {
            return common::Status::Corruption("metadata snapshot version is not supported");
        }

        status = decoder.ReadU32(&record_count);
        if (!status.ok())
        {
            return status;
        }
        if (record_count > kMaxMetadataSnapshotRecords)
        {
            return common::Status::Corruption("metadata snapshot record count exceeds limit");
        }

        status = decoder.ReadU64(&last_applied_index);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadU64(&last_applied_term);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateProgress(last_applied_index, last_applied_term);
        if (!status.ok())
        {
            return status;
        }

        RecordMap restored_records;
        for (std::uint32_t i = 0; i < record_count; ++i)
        {
            std::uint32_t target_length = 0;
            std::uint32_t payload_length = 0;
            std::uint32_t command_id_length = 0;
            std::uint64_t generation = 0;
            std::uint64_t last_update_index = 0;
            std::uint64_t last_update_term = 0;

            status = decoder.ReadU32(&target_length);
            if (!status.ok())
            {
                return status;
            }
            status = decoder.ReadU32(&payload_length);
            if (!status.ok())
            {
                return status;
            }
            status = decoder.ReadU32(&command_id_length);
            if (!status.ok())
            {
                return status;
            }
            status = decoder.ReadU64(&generation);
            if (!status.ok())
            {
                return status;
            }
            status = decoder.ReadU64(&last_update_index);
            if (!status.ok())
            {
                return status;
            }
            status = decoder.ReadU64(&last_update_term);
            if (!status.ok())
            {
                return status;
            }

            MetadataStateRecord record;
            status = decoder.ReadString(target_length,
                                        kMaxMetadataCommandTargetBytes,
                                        &record.target_id);
            if (!status.ok())
            {
                return status;
            }
            status = decoder.ReadPayload(payload_length,
                                         kMaxMetadataCommandPayloadBytes,
                                         &record.payload);
            if (!status.ok())
            {
                return status;
            }
            status = decoder.ReadString(command_id_length,
                                        kMaxMetadataCommandIdBytes,
                                        &record.last_command_id);
            if (!status.ok())
            {
                return status;
            }

            record.generation = generation;
            record.last_update_index = last_update_index;
            record.last_update_term = last_update_term;

            status = ValidateSnapshotRecord(
                record,
                static_cast<common::LogIndex>(last_applied_index),
                static_cast<common::Term>(last_applied_term));
            if (!status.ok())
            {
                return status;
            }

            auto [it, inserted] =
                restored_records.emplace(record.target_id, std::move(record));
            if (!inserted)
            {
                return common::Status::Corruption("metadata snapshot contains duplicate target id");
            }
        }

        if (decoder.remaining() != 0)
        {
            return common::Status::Corruption("metadata snapshot has trailing bytes");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        records_ = std::move(restored_records);
        last_applied_index_ = static_cast<common::LogIndex>(last_applied_index);
        last_applied_term_ = static_cast<common::Term>(last_applied_term);
        return common::Status::OK();
    }

    common::Status MetadataStateMachine::GetRecord(const std::string &target_id,
                                                   MetadataStateRecord *record) const
    {
        common::Status status = RequireRecord(record);
        if (!status.ok())
        {
            return status;
        }
        if (target_id.empty())
        {
            return common::Status::InvalidArgument("metadata target id must not be empty");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = records_.find(target_id);
        if (it == records_.end())
        {
            return common::Status::NotFound("metadata target not found");
        }
        *record = it->second;
        return common::Status::OK();
    }

    common::Status MetadataStateMachine::GetLastApplied(common::LogIndex *index,
                                                        common::Term *term) const
    {
        common::Status status = RequireAppliedPointers(index, term);
        if (!status.ok())
        {
            return status;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        *index = last_applied_index_;
        *term = last_applied_term_;
        return common::Status::OK();
    }

} // namespace cpr::metadata
