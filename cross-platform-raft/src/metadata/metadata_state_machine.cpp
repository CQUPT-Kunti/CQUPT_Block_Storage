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

        constexpr std::uint8_t kMetadataSnapshotFormatVersion = 2;
        constexpr std::uint8_t kLegacyMetadataSnapshotFormatVersion = 1;
        constexpr std::size_t kMaxMetadataSnapshotRecords = 4096;
        constexpr std::size_t kMaxMetadataSnapshotStores = 4096;
        constexpr std::size_t kMaxMetadataSnapshotTasks = 4096;
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

        void AppendStoreState(store::StoreState state, raft::OpaquePayload *payload)
        {
            AppendU8(static_cast<std::uint8_t>(state), payload);
        }

        void AppendTaskType(store::TaskType type, raft::OpaquePayload *payload)
        {
            AppendU8(static_cast<std::uint8_t>(type), payload);
        }

        void AppendTaskState(store::TaskState state, raft::OpaquePayload *payload)
        {
            AppendU8(static_cast<std::uint8_t>(state), payload);
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

            common::Status ReadU16(std::uint16_t *value)
            {
                if (value == nullptr)
                {
                    return common::Status::InvalidArgument("value must not be null");
                }
                if (remaining() < 2)
                {
                    return common::Status::Corruption("truncated metadata snapshot");
                }
                *value = (static_cast<std::uint16_t>(payload_[offset_]) << 8) |
                         static_cast<std::uint16_t>(payload_[offset_ + 1]);
                offset_ += 2;
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
                for (std::size_t i = 0; i < 8; ++i)
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

        void AppendU16(std::uint16_t value, raft::OpaquePayload *payload)
        {
            payload->push_back(static_cast<common::Byte>((value >> 8) & 0xFF));
            payload->push_back(static_cast<common::Byte>(value & 0xFF));
        }

        common::Status AppendSnapshotString(const std::string &value,
                                            raft::OpaquePayload *payload)
        {
            common::Status status = ValidateSnapshotLength(
                value.size(),
                kMaxMetadataCommandTargetBytes,
                "metadata snapshot string");
            if (!status.ok())
            {
                return status;
            }
            AppendU32(static_cast<std::uint32_t>(value.size()), payload);
            AppendBytes(value, payload);
            return common::Status::OK();
        }

        common::Status AppendSnapshotPayload(const raft::OpaquePayload &value,
                                             raft::OpaquePayload *payload)
        {
            common::Status status = ValidateSnapshotLength(
                value.size(),
                kMaxMetadataCommandPayloadBytes,
                "metadata snapshot payload");
            if (!status.ok())
            {
                return status;
            }
            AppendU32(static_cast<std::uint32_t>(value.size()), payload);
            AppendBytes(value, payload);
            return common::Status::OK();
        }

        common::Status AppendPersistentStore(
            const store::PersistentStoreInfo &store,
            raft::OpaquePayload *payload)
        {
            AppendU64(store.id, payload);
            AppendU64(store.capacity_bytes, payload);
            AppendU64(store.used_bytes, payload);
            AppendStoreState(store.state, payload);
            AppendU64(store.generation, payload);
            AppendU16(store.address.port, payload);
            return AppendSnapshotString(store.address.host, payload);
        }

        common::Status ReadPersistentStore(
            Decoder *decoder,
            store::PersistentStoreInfo *store)
        {
            if (decoder == nullptr || store == nullptr)
            {
                return common::Status::InvalidArgument("store snapshot output must not be null");
            }
            std::uint8_t state = 0;
            common::Status status = decoder->ReadU64(&store->id);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU64(&store->capacity_bytes);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU64(&store->used_bytes);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU8(&state);
            if (!status.ok())
            {
                return status;
            }
            store->state = static_cast<store::StoreState>(state);
            status = decoder->ReadU64(&store->generation);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU16(&store->address.port);
            if (!status.ok())
            {
                return status;
            }
            std::uint32_t host_length = 0;
            status = decoder->ReadU32(&host_length);
            if (!status.ok())
            {
                return status;
            }
            return decoder->ReadString(host_length,
                                       kMaxMetadataCommandTargetBytes,
                                       &store->address.host);
        }

        common::Status AppendTaskRecord(const store::TaskRecord &task,
                                        raft::OpaquePayload *payload)
        {
            AppendTaskType(task.type, payload);
            AppendTaskState(task.state, payload);
            AppendU64(task.assigned_store, payload);
            AppendU64(task.generation, payload);
            AppendU64(task.sequence, payload);
            common::Status status = AppendSnapshotString(task.task_id, payload);
            if (!status.ok())
            {
                return status;
            }
            status = AppendSnapshotPayload(task.target_payload, payload);
            if (!status.ok())
            {
                return status;
            }
            return AppendSnapshotPayload(task.result_payload, payload);
        }

        common::Status ReadTaskRecord(Decoder *decoder,
                                      store::TaskRecord *task)
        {
            if (decoder == nullptr || task == nullptr)
            {
                return common::Status::InvalidArgument("task snapshot output must not be null");
            }
            std::uint8_t type = 0;
            std::uint8_t state = 0;
            common::Status status = decoder->ReadU8(&type);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU8(&state);
            if (!status.ok())
            {
                return status;
            }
            task->type = static_cast<store::TaskType>(type);
            task->state = static_cast<store::TaskState>(state);
            status = decoder->ReadU64(&task->assigned_store);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU64(&task->generation);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU64(&task->sequence);
            if (!status.ok())
            {
                return status;
            }
            std::uint32_t length = 0;
            status = decoder->ReadU32(&length);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadString(length,
                                         kMaxMetadataCommandIdBytes,
                                         &task->task_id);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU32(&length);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadPayload(length,
                                          kMaxMetadataCommandPayloadBytes,
                                          &task->target_payload);
            if (!status.ok())
            {
                return status;
            }
            status = decoder->ReadU32(&length);
            if (!status.ok())
            {
                return status;
            }
            return decoder->ReadPayload(length,
                                        kMaxMetadataCommandPayloadBytes,
                                        &task->result_payload);
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

        if (command.type == MetadataCommandType::STORE_OPERATION)
        {
            status = ApplyStoreCommand(index, term, command);
        }
        else
        {
            status = ApplyOpaqueCommand(index, term, command);
        }
        if (!status.ok())
        {
            return status;
        }
        last_applied_index_ = index;
        last_applied_term_ = term;
        return common::Status::OK();
    }

    common::Status MetadataStateMachine::ApplyOpaqueCommand(
        common::LogIndex index,
        common::Term term,
        const MetadataCommand &command)
    {
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
        return common::Status::OK();
    }

    common::Status MetadataStateMachine::ApplyStoreCommand(
        common::LogIndex index,
        common::Term term,
        const MetadataCommand &command)
    {
        StoreBusinessCommand store_command;
        common::Status status =
            DecodeStoreBusinessCommand(command.payload, &store_command);
        if (!status.ok())
        {
            return status;
        }

        StoreBusinessResult result;
        switch (store_command.kind)
        {
        case StoreBusinessCommandKind::REGISTER_STORE:
            status = stores_.RegisterStore(store_command.store, &result.store);
            break;
        case StoreBusinessCommandKind::UPDATE_STORE_STATE:
            status = stores_.UpdateStore(store_command.store_update, &result.store);
            break;
        case StoreBusinessCommandKind::REMOVE_STORE:
        {
            status = stores_.GetStore(store_command.store_id, &result.store);
            if (!status.ok())
            {
                break;
            }
            if (store_command.store_update.expected_generation &&
                result.store.generation != *store_command.store_update.expected_generation)
            {
                status = common::Status::Busy("store generation mismatch");
                break;
            }
            status = stores_.RemoveStore(store_command.store_id);
            break;
        }
        case StoreBusinessCommandKind::CREATE_TASK:
        {
            store::TaskRecord task;
            status = tasks_.CreateTask(store_command.task_create, &task);
            if (status.ok())
            {
                result.tasks.push_back(std::move(task));
            }
            break;
        }
        case StoreBusinessCommandKind::POLL_TASKS:
            status = tasks_.AssignNextWaitingTasks(store_command.store_id,
                                                   store_command.max_tasks,
                                                   &result.tasks);
            break;
        case StoreBusinessCommandKind::REPORT_TASK_RESULT:
        {
            store::TaskRecord before;
            const bool had_before =
                tasks_.GetTask(store_command.task_result.task_id, &before).ok();
            store::TaskRecord after;
            status = tasks_.CompleteTask(store_command.task_result, &after);
            if (status.ok())
            {
                result.duplicate_result =
                    had_before && before.state == after.state &&
                    before.result_payload == after.result_payload &&
                    before.generation == after.generation;
                result.tasks.push_back(std::move(after));
            }
            break;
        }
        }
        if (!status.ok())
        {
            return status;
        }

        raft::OpaquePayload result_payload;
        status = EncodeStoreBusinessResult(result, &result_payload);
        if (!status.ok())
        {
            return status;
        }

        MetadataStateRecord record;
        auto it = records_.find(command.target_id);
        const bool exists = it != records_.end();
        if (exists)
        {
            record = it->second;
        }
        else
        {
            record.target_id = command.target_id;
        }
        record.payload = std::move(result_payload);
        record.generation = exists ? record.generation + 1 : 1;
        record.last_command_id = command.command_id;
        record.last_update_index = index;
        record.last_update_term = term;
        records_[record.target_id] = std::move(record);
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
        std::vector<store::PersistentStoreInfo> stores_copy;
        std::vector<store::TaskRecord> tasks_copy;
        common::LogIndex current_index = common::kInvalidLogIndex;
        common::Term current_term = common::kInitialTerm;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            current_index = last_applied_index_;
            current_term = last_applied_term_;
            records_copy = records_;
            status = stores_.ExportPersistent(&stores_copy);
            if (!status.ok())
            {
                return status;
            }
            status = tasks_.ExportPersistent(&tasks_copy);
            if (!status.ok())
            {
                return status;
            }
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

        if (stores_copy.size() > kMaxMetadataSnapshotStores ||
            tasks_copy.size() > kMaxMetadataSnapshotTasks)
        {
            return common::Status::Busy("metadata snapshot business state exceeds limit");
        }
        AppendU32(static_cast<std::uint32_t>(stores_copy.size()), &encoded);
        for (const store::PersistentStoreInfo &store : stores_copy)
        {
            status = AppendPersistentStore(store, &encoded);
            if (!status.ok())
            {
                return status;
            }
        }
        AppendU32(static_cast<std::uint32_t>(tasks_copy.size()), &encoded);
        for (const store::TaskRecord &task : tasks_copy)
        {
            status = AppendTaskRecord(task, &encoded);
            if (!status.ok())
            {
                return status;
            }
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
        if (version != kMetadataSnapshotFormatVersion &&
            version != kLegacyMetadataSnapshotFormatVersion)
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

        std::vector<store::PersistentStoreInfo> restored_stores;
        std::vector<store::TaskRecord> restored_tasks;
        if (version == kMetadataSnapshotFormatVersion)
        {
            std::uint32_t store_count = 0;
            status = decoder.ReadU32(&store_count);
            if (!status.ok())
            {
                return status;
            }
            if (store_count > kMaxMetadataSnapshotStores)
            {
                return common::Status::Corruption("metadata snapshot store count exceeds limit");
            }
            restored_stores.reserve(store_count);
            for (std::uint32_t i = 0; i < store_count; ++i)
            {
                store::PersistentStoreInfo store;
                status = ReadPersistentStore(&decoder, &store);
                if (!status.ok())
                {
                    return status;
                }
                restored_stores.push_back(std::move(store));
            }

            std::uint32_t task_count = 0;
            status = decoder.ReadU32(&task_count);
            if (!status.ok())
            {
                return status;
            }
            if (task_count > kMaxMetadataSnapshotTasks)
            {
                return common::Status::Corruption("metadata snapshot task count exceeds limit");
            }
            restored_tasks.reserve(task_count);
            for (std::uint32_t i = 0; i < task_count; ++i)
            {
                store::TaskRecord task;
                status = ReadTaskRecord(&decoder, &task);
                if (!status.ok())
                {
                    return status;
                }
                restored_tasks.push_back(std::move(task));
            }
        }

        if (decoder.remaining() != 0)
        {
            return common::Status::Corruption("metadata snapshot has trailing bytes");
        }

        store::StoreRegistry validation_registry;
        status = validation_registry.RestorePersistent(restored_stores);
        if (!status.ok())
        {
            return status;
        }
        store::TaskManager validation_task_manager;
        status = validation_task_manager.RestorePersistent(restored_tasks);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        status = stores_.RestorePersistent(restored_stores);
        if (!status.ok())
        {
            return status;
        }
        status = tasks_.RestorePersistent(restored_tasks);
        if (!status.ok())
        {
            return status;
        }
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

    common::Status MetadataStateMachine::GetStore(store::StoreId store_id,
                                                  store::StoreInfo *store) const
    {
        return stores_.GetStore(store_id, store);
    }

    common::Status MetadataStateMachine::ListStores(
        std::vector<store::StoreInfo> *stores) const
    {
        return stores_.ListStores(stores);
    }

    common::Status MetadataStateMachine::UpdateStoreHeartbeat(
        store::StoreId store_id,
        std::int64_t heartbeat_ms,
        store::StoreInfo *store)
    {
        return stores_.UpdateHeartbeat(store_id, heartbeat_ms, store);
    }

    common::Status MetadataStateMachine::GetTask(
        const store::TaskId &task_id,
        store::TaskRecord *task) const
    {
        return tasks_.GetTask(task_id, task);
    }

    common::Status MetadataStateMachine::ListTasks(
        std::vector<store::TaskRecord> *tasks) const
    {
        return tasks_.ListTasks(tasks);
    }

} // namespace cpr::metadata
