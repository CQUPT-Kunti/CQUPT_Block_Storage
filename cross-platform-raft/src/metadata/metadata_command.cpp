#include "metadata/metadata_command.h"

#include <limits>
#include <utility>

namespace cpr::metadata
{
    namespace
    {

        constexpr std::uint16_t kFlagHasExpectedGeneration = 0x0001;
        constexpr std::uint8_t kStoreBusinessCommandVersion = 1;
        constexpr std::uint8_t kStoreBusinessResultVersion = 1;
        constexpr std::size_t kMaxStoreBusinessItems = 4096;

        bool IsKnownType(MetadataCommandType type)
        {
            switch (type)
            {
            case MetadataCommandType::OPAQUE_OPERATION:
            case MetadataCommandType::STORE_OPERATION:
                return true;
            }
            return false;
        }

        bool IsKnownStoreCommandKind(StoreBusinessCommandKind kind)
        {
            switch (kind)
            {
            case StoreBusinessCommandKind::REGISTER_STORE:
            case StoreBusinessCommandKind::UPDATE_STORE_STATE:
            case StoreBusinessCommandKind::REMOVE_STORE:
            case StoreBusinessCommandKind::CREATE_TASK:
            case StoreBusinessCommandKind::POLL_TASKS:
            case StoreBusinessCommandKind::REPORT_TASK_RESULT:
                return true;
            }
            return false;
        }

        std::uint8_t EncodeStoreState(store::StoreState state)
        {
            return static_cast<std::uint8_t>(state);
        }

        std::uint8_t EncodeTaskType(store::TaskType type)
        {
            return static_cast<std::uint8_t>(type);
        }

        std::uint8_t EncodeTaskState(store::TaskState state)
        {
            return static_cast<std::uint8_t>(state);
        }

        store::StoreState DecodeStoreState(std::uint8_t state)
        {
            return static_cast<store::StoreState>(state);
        }

        store::TaskType DecodeTaskType(std::uint8_t type)
        {
            return static_cast<store::TaskType>(type);
        }

        store::TaskState DecodeTaskState(std::uint8_t state)
        {
            return static_cast<store::TaskState>(state);
        }

        common::Status RequirePayload(raft::OpaquePayload *payload)
        {
            if (payload == nullptr)
            {
                return common::Status::InvalidArgument("payload must not be null");
            }
            return common::Status::OK();
        }

        common::Status RequireCommand(MetadataCommand *command)
        {
            if (command == nullptr)
            {
                return common::Status::InvalidArgument("command must not be null");
            }
            return common::Status::OK();
        }

        common::Status ValidateLength(std::size_t size,
                                      std::size_t max_size,
                                      const char *name)
        {
            if (size > max_size)
            {
                return common::Status::InvalidArgument(
                    std::string(name) + " exceeds limit");
            }
            if (size > static_cast<std::size_t>(
                           std::numeric_limits<std::uint32_t>::max()))
            {
                return common::Status::InvalidArgument(
                    std::string(name) + " is too large to encode");
            }
            return common::Status::OK();
        }

        void AppendU8(std::uint8_t value, raft::OpaquePayload *payload)
        {
            payload->push_back(value);
        }

        void AppendU16(std::uint16_t value, raft::OpaquePayload *payload)
        {
            payload->push_back(static_cast<common::Byte>((value >> 8) & 0xFF));
            payload->push_back(static_cast<common::Byte>(value & 0xFF));
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

        common::Status AppendBytes(const std::string &value,
                                   raft::OpaquePayload *payload)
        {
            if (!value.empty())
            {
                payload->insert(payload->end(), value.begin(), value.end());
            }
            return common::Status::OK();
        }

        common::Status AppendBytes(const raft::OpaquePayload &value,
                                   raft::OpaquePayload *payload)
        {
            if (!value.empty())
            {
                payload->insert(payload->end(), value.begin(), value.end());
            }
            return common::Status::OK();
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
                    return common::Status::Corruption("truncated metadata command");
                }
                *value = payload_[offset_++];
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
                    return common::Status::Corruption("truncated metadata command");
                }
                *value = (static_cast<std::uint16_t>(payload_[offset_]) << 8) |
                         static_cast<std::uint16_t>(payload_[offset_ + 1]);
                offset_ += 2;
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
                    return common::Status::Corruption("truncated metadata command");
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
                    return common::Status::Corruption("truncated metadata command");
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
                    return common::Status::Corruption("metadata command string length exceeds limit");
                }
                if (length > remaining())
                {
                    return common::Status::Corruption("truncated metadata command");
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
                    return common::Status::Corruption("metadata command payload length exceeds limit");
                }
                if (length > remaining())
                {
                    return common::Status::Corruption("truncated metadata command");
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

        common::Status ValidateStoreBusinessCommand(
            const StoreBusinessCommand &command)
        {
            if (!IsKnownStoreCommandKind(command.kind))
            {
                return common::Status::InvalidArgument("store command kind is invalid");
            }
            switch (command.kind)
            {
            case StoreBusinessCommandKind::REGISTER_STORE:
                if (command.store.id == 0 || command.store.address.host.empty() ||
                    command.store.address.port == 0 ||
                    command.store.capacity_bytes == 0 ||
                    command.store.used_bytes > command.store.capacity_bytes)
                {
                    return common::Status::InvalidArgument("store registration command is invalid");
                }
                return ValidateLength(command.store.address.host.size(),
                                      kMaxMetadataCommandTargetBytes,
                                      "store address host");
            case StoreBusinessCommandKind::UPDATE_STORE_STATE:
                if (command.store_update.id == 0 ||
                    !command.store_update.expected_generation ||
                    !command.store_update.state)
                {
                    return common::Status::InvalidArgument("store state update command is invalid");
                }
                return common::Status::OK();
            case StoreBusinessCommandKind::REMOVE_STORE:
                if (command.store_id == 0 ||
                    !command.store_update.expected_generation)
                {
                    return common::Status::InvalidArgument("store remove command is invalid");
                }
                return common::Status::OK();
            case StoreBusinessCommandKind::CREATE_TASK:
                if (command.task_create.task_id.empty())
                {
                    return common::Status::InvalidArgument("task create command id is empty");
                }
                if (command.task_create.target_payload.size() >
                    kMaxMetadataCommandPayloadBytes)
                {
                    return common::Status::InvalidArgument("task create payload exceeds limit");
                }
                return ValidateLength(command.task_create.task_id.size(),
                                      kMaxMetadataCommandIdBytes,
                                      "task id");
            case StoreBusinessCommandKind::POLL_TASKS:
                if (command.store_id == 0 || command.max_tasks == 0)
                {
                    return common::Status::InvalidArgument("task poll command is invalid");
                }
                return common::Status::OK();
            case StoreBusinessCommandKind::REPORT_TASK_RESULT:
                if (command.task_result.store_id == 0 ||
                    command.task_result.task_id.empty() ||
                    (command.task_result.final_state != store::TaskState::SUCCESS &&
                     command.task_result.final_state != store::TaskState::FAILED))
                {
                    return common::Status::InvalidArgument("task result command is invalid");
                }
                if (command.task_result.result_payload.size() >
                    kMaxMetadataCommandPayloadBytes)
                {
                    return common::Status::InvalidArgument("task result payload exceeds limit");
                }
                return ValidateLength(command.task_result.task_id.size(),
                                      kMaxMetadataCommandIdBytes,
                                      "task id");
            }
            return common::Status::InvalidArgument("store command kind is invalid");
        }

        common::Status AppendStringField(const std::string &value,
                                         raft::OpaquePayload *payload)
        {
            common::Status status =
                ValidateLength(value.size(), kMaxMetadataCommandTargetBytes, "string field");
            if (!status.ok())
            {
                return status;
            }
            AppendU32(static_cast<std::uint32_t>(value.size()), payload);
            return AppendBytes(value, payload);
        }

        common::Status AppendPayloadField(const raft::OpaquePayload &value,
                                          raft::OpaquePayload *payload)
        {
            common::Status status =
                ValidateLength(value.size(), kMaxMetadataCommandPayloadBytes, "payload field");
            if (!status.ok())
            {
                return status;
            }
            AppendU32(static_cast<std::uint32_t>(value.size()), payload);
            return AppendBytes(value, payload);
        }

        common::Status ReadStringField(Decoder *decoder,
                                       std::size_t max_length,
                                       std::string *value)
        {
            std::uint32_t length = 0;
            common::Status status = decoder->ReadU32(&length);
            if (!status.ok())
            {
                return status;
            }
            return decoder->ReadString(length, max_length, value);
        }

        common::Status ReadPayloadField(Decoder *decoder,
                                        raft::OpaquePayload *value)
        {
            std::uint32_t length = 0;
            common::Status status = decoder->ReadU32(&length);
            if (!status.ok())
            {
                return status;
            }
            return decoder->ReadPayload(length,
                                        kMaxMetadataCommandPayloadBytes,
                                        value);
        }

        void AppendStoreInfo(const store::StoreInfo &store,
                             raft::OpaquePayload *payload)
        {
            AppendU64(store.id, payload);
            AppendU64(store.capacity_bytes, payload);
            AppendU64(store.used_bytes, payload);
            AppendU8(EncodeStoreState(store.state), payload);
            AppendU64(store.generation, payload);
            AppendU16(store.address.port, payload);
        }

        common::Status AppendStoreInfoWithHost(const store::StoreInfo &store,
                                               raft::OpaquePayload *payload)
        {
            AppendStoreInfo(store, payload);
            return AppendStringField(store.address.host, payload);
        }

        common::Status ReadStoreInfo(Decoder *decoder,
                                     store::StoreInfo *store)
        {
            if (store == nullptr)
            {
                return common::Status::InvalidArgument("store output must not be null");
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
            store->state = DecodeStoreState(state);
            status = decoder->ReadU64(&store->generation);
            if (!status.ok())
            {
                return status;
            }
            std::uint16_t port = 0;
            status = decoder->ReadU16(&port);
            if (!status.ok())
            {
                return status;
            }
            store->address.port = port;
            status = ReadStringField(decoder,
                                     kMaxMetadataCommandTargetBytes,
                                     &store->address.host);
            if (!status.ok())
            {
                return status;
            }
            store->last_heartbeat_ms = 0;
            return common::Status::OK();
        }

        common::Status AppendTaskRecord(const store::TaskRecord &task,
                                        raft::OpaquePayload *payload)
        {
            AppendU8(EncodeTaskType(task.type), payload);
            AppendU8(EncodeTaskState(task.state), payload);
            AppendU64(task.assigned_store, payload);
            AppendU64(task.generation, payload);
            AppendU64(task.sequence, payload);
            common::Status status = AppendStringField(task.task_id, payload);
            if (!status.ok())
            {
                return status;
            }
            status = AppendPayloadField(task.target_payload, payload);
            if (!status.ok())
            {
                return status;
            }
            return AppendPayloadField(task.result_payload, payload);
        }

        common::Status ReadTaskRecord(Decoder *decoder,
                                      store::TaskRecord *task)
        {
            if (task == nullptr)
            {
                return common::Status::InvalidArgument("task output must not be null");
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
            task->type = DecodeTaskType(type);
            task->state = DecodeTaskState(state);
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
            status = ReadStringField(decoder,
                                     kMaxMetadataCommandIdBytes,
                                     &task->task_id);
            if (!status.ok())
            {
                return status;
            }
            status = ReadPayloadField(decoder, &task->target_payload);
            if (!status.ok())
            {
                return status;
            }
            return ReadPayloadField(decoder, &task->result_payload);
        }

    } // namespace

    common::Status ValidateMetadataCommand(const MetadataCommand &command)
    {
        if (!IsKnownType(command.type))
        {
            return common::Status::InvalidArgument("metadata command type is not supported");
        }
        if (command.command_id.empty())
        {
            return common::Status::InvalidArgument("metadata command id must not be empty");
        }
        if (command.target_id.empty())
        {
            return common::Status::InvalidArgument("metadata command target id must not be empty");
        }
        common::Status status = ValidateLength(
            command.command_id.size(), kMaxMetadataCommandIdBytes, "metadata command id");
        if (!status.ok())
        {
            return status;
        }
        status = ValidateLength(
            command.target_id.size(), kMaxMetadataCommandTargetBytes, "metadata command target id");
        if (!status.ok())
        {
            return status;
        }
        status = ValidateLength(
            command.payload.size(), kMaxMetadataCommandPayloadBytes, "metadata command payload");
        if (!status.ok())
        {
            return status;
        }
        if (command.expected_generation.has_value() &&
            command.expected_generation.value() == 0)
        {
            return common::Status::InvalidArgument(
                "metadata command expected generation must be non-zero");
        }
        return common::Status::OK();
    }

    common::Status EncodeMetadataCommand(const MetadataCommand &command,
                                         raft::OpaquePayload *payload)
    {
        common::Status status = RequirePayload(payload);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateMetadataCommand(command);
        if (!status.ok())
        {
            return status;
        }

        raft::OpaquePayload encoded;
        std::uint16_t flags = 0;
        if (command.expected_generation.has_value())
        {
            flags |= kFlagHasExpectedGeneration;
        }

        encoded.reserve(1 + 1 + 2 + 4 + 4 + 4 + 8 +
                        command.command_id.size() +
                        command.target_id.size() +
                        command.payload.size());

        AppendU8(kMetadataCommandFormatVersion, &encoded);
        AppendU8(static_cast<std::uint8_t>(command.type), &encoded);
        AppendU16(flags, &encoded);
        AppendU32(static_cast<std::uint32_t>(command.command_id.size()), &encoded);
        AppendU32(static_cast<std::uint32_t>(command.target_id.size()), &encoded);
        AppendU32(static_cast<std::uint32_t>(command.payload.size()), &encoded);
        if (command.expected_generation.has_value())
        {
            AppendU64(command.expected_generation.value(), &encoded);
        }
        AppendBytes(command.command_id, &encoded);
        AppendBytes(command.target_id, &encoded);
        AppendBytes(command.payload, &encoded);

        *payload = std::move(encoded);
        return common::Status::OK();
    }

    common::Status DecodeMetadataCommand(const raft::OpaquePayload &payload,
                                         MetadataCommand *command)
    {
        common::Status status = RequireCommand(command);
        if (!status.ok())
        {
            return status;
        }

        Decoder decoder(payload);
        std::uint8_t version = 0;
        std::uint8_t type_value = 0;
        std::uint16_t flags = 0;
        std::uint32_t command_id_length = 0;
        std::uint32_t target_id_length = 0;
        std::uint32_t payload_length = 0;

        status = decoder.ReadU8(&version);
        if (!status.ok())
        {
            return status;
        }
        if (version != kMetadataCommandFormatVersion)
        {
            return common::Status::Corruption("metadata command version is not supported");
        }

        status = decoder.ReadU8(&type_value);
        if (!status.ok())
        {
            return status;
        }

        status = decoder.ReadU16(&flags);
        if (!status.ok())
        {
            return status;
        }
        if ((flags & ~kFlagHasExpectedGeneration) != 0)
        {
            return common::Status::Corruption("metadata command flags are not supported");
        }

        status = decoder.ReadU32(&command_id_length);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadU32(&target_id_length);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadU32(&payload_length);
        if (!status.ok())
        {
            return status;
        }

        MetadataCommand decoded;
        decoded.type = static_cast<MetadataCommandType>(type_value);
        if (!IsKnownType(decoded.type))
        {
            return common::Status::Corruption("metadata command type is not supported");
        }

        if ((flags & kFlagHasExpectedGeneration) != 0)
        {
            std::uint64_t generation = 0;
            status = decoder.ReadU64(&generation);
            if (!status.ok())
            {
                return status;
            }
            decoded.expected_generation = generation;
        }

        status = decoder.ReadString(command_id_length,
                                    kMaxMetadataCommandIdBytes,
                                    &decoded.command_id);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadString(target_id_length,
                                    kMaxMetadataCommandTargetBytes,
                                    &decoded.target_id);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadPayload(payload_length,
                                     kMaxMetadataCommandPayloadBytes,
                                     &decoded.payload);
        if (!status.ok())
        {
            return status;
        }
        if (decoder.remaining() != 0)
        {
            return common::Status::Corruption("metadata command has trailing bytes");
        }

        status = ValidateMetadataCommand(decoded);
        if (!status.ok())
        {
            return status;
        }

        *command = std::move(decoded);
        return common::Status::OK();
    }

    common::Status EncodeStoreBusinessCommand(
        const StoreBusinessCommand &command,
        raft::OpaquePayload *payload)
    {
        common::Status status = RequirePayload(payload);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateStoreBusinessCommand(command);
        if (!status.ok())
        {
            return status;
        }

        raft::OpaquePayload encoded;
        AppendU8(kStoreBusinessCommandVersion, &encoded);
        AppendU8(static_cast<std::uint8_t>(command.kind), &encoded);
        AppendU64(command.store_id, &encoded);
        AppendU32(command.max_tasks, &encoded);

        status = AppendStoreInfoWithHost(command.store, &encoded);
        if (!status.ok())
        {
            return status;
        }

        AppendU64(command.store_update.id, &encoded);
        AppendU64(command.store_update.expected_generation.value_or(0), &encoded);
        AppendU64(command.store_update.capacity_bytes.value_or(0), &encoded);
        AppendU64(command.store_update.used_bytes.value_or(0), &encoded);
        AppendU8(command.store_update.state
                     ? EncodeStoreState(*command.store_update.state)
                     : 0xFF,
                 &encoded);

        AppendU8(EncodeTaskType(command.task_create.type), &encoded);
        status = AppendStringField(command.task_create.task_id, &encoded);
        if (!status.ok())
        {
            return status;
        }
        status = AppendPayloadField(command.task_create.target_payload, &encoded);
        if (!status.ok())
        {
            return status;
        }

        AppendU64(command.task_result.store_id, &encoded);
        AppendU8(EncodeTaskState(command.task_result.final_state), &encoded);
        AppendU64(command.task_result.expected_generation.value_or(0), &encoded);
        status = AppendStringField(command.task_result.task_id, &encoded);
        if (!status.ok())
        {
            return status;
        }
        status = AppendPayloadField(command.task_result.result_payload, &encoded);
        if (!status.ok())
        {
            return status;
        }

        *payload = std::move(encoded);
        return common::Status::OK();
    }

    common::Status DecodeStoreBusinessCommand(
        const raft::OpaquePayload &payload,
        StoreBusinessCommand *command)
    {
        if (command == nullptr)
        {
            return common::Status::InvalidArgument("store command output must not be null");
        }

        Decoder decoder(payload);
        std::uint8_t version = 0;
        std::uint8_t kind = 0;
        StoreBusinessCommand decoded;

        common::Status status = decoder.ReadU8(&version);
        if (!status.ok())
        {
            return status;
        }
        if (version != kStoreBusinessCommandVersion)
        {
            return common::Status::Corruption("store command version is not supported");
        }
        status = decoder.ReadU8(&kind);
        if (!status.ok())
        {
            return status;
        }
        decoded.kind = static_cast<StoreBusinessCommandKind>(kind);

        status = decoder.ReadU64(&decoded.store_id);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadU32(&decoded.max_tasks);
        if (!status.ok())
        {
            return status;
        }
        status = ReadStoreInfo(&decoder, &decoded.store);
        if (!status.ok())
        {
            return status;
        }

        status = decoder.ReadU64(&decoded.store_update.id);
        if (!status.ok())
        {
            return status;
        }
        std::uint64_t expected_generation = 0;
        std::uint64_t capacity = 0;
        std::uint64_t used = 0;
        std::uint8_t state = 0;
        status = decoder.ReadU64(&expected_generation);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadU64(&capacity);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadU64(&used);
        if (!status.ok())
        {
            return status;
        }
        status = decoder.ReadU8(&state);
        if (!status.ok())
        {
            return status;
        }
        if (expected_generation != 0)
        {
            decoded.store_update.expected_generation = expected_generation;
        }
        if (capacity != 0)
        {
            decoded.store_update.capacity_bytes = capacity;
        }
        if (used != 0)
        {
            decoded.store_update.used_bytes = used;
        }
        if (state != 0xFF)
        {
            decoded.store_update.state = DecodeStoreState(state);
        }

        std::uint8_t task_type = 0;
        status = decoder.ReadU8(&task_type);
        if (!status.ok())
        {
            return status;
        }
        decoded.task_create.type = DecodeTaskType(task_type);
        status = ReadStringField(&decoder,
                                 kMaxMetadataCommandIdBytes,
                                 &decoded.task_create.task_id);
        if (!status.ok())
        {
            return status;
        }
        status = ReadPayloadField(&decoder, &decoded.task_create.target_payload);
        if (!status.ok())
        {
            return status;
        }

        status = decoder.ReadU64(&decoded.task_result.store_id);
        if (!status.ok())
        {
            return status;
        }
        std::uint8_t final_state = 0;
        status = decoder.ReadU8(&final_state);
        if (!status.ok())
        {
            return status;
        }
        decoded.task_result.final_state = DecodeTaskState(final_state);
        std::uint64_t task_generation = 0;
        status = decoder.ReadU64(&task_generation);
        if (!status.ok())
        {
            return status;
        }
        if (task_generation != 0)
        {
            decoded.task_result.expected_generation = task_generation;
        }
        status = ReadStringField(&decoder,
                                 kMaxMetadataCommandIdBytes,
                                 &decoded.task_result.task_id);
        if (!status.ok())
        {
            return status;
        }
        status = ReadPayloadField(&decoder, &decoded.task_result.result_payload);
        if (!status.ok())
        {
            return status;
        }
        if (decoder.remaining() != 0)
        {
            return common::Status::Corruption("store command has trailing bytes");
        }

        status = ValidateStoreBusinessCommand(decoded);
        if (!status.ok())
        {
            return status.code() == common::StatusCode::kInvalidArgument
                       ? common::Status::Corruption(status.message())
                       : status;
        }
        *command = std::move(decoded);
        return common::Status::OK();
    }

    common::Status EncodeStoreBusinessResult(
        const StoreBusinessResult &result,
        raft::OpaquePayload *payload)
    {
        common::Status status = RequirePayload(payload);
        if (!status.ok())
        {
            return status;
        }
        if (result.tasks.size() > kMaxStoreBusinessItems)
        {
            return common::Status::InvalidArgument("store result has too many tasks");
        }

        raft::OpaquePayload encoded;
        AppendU8(kStoreBusinessResultVersion, &encoded);
        AppendU8(result.duplicate_result ? 1 : 0, &encoded);
        status = AppendStoreInfoWithHost(result.store, &encoded);
        if (!status.ok())
        {
            return status;
        }
        AppendU32(static_cast<std::uint32_t>(result.tasks.size()), &encoded);
        for (const store::TaskRecord &task : result.tasks)
        {
            status = AppendTaskRecord(task, &encoded);
            if (!status.ok())
            {
                return status;
            }
        }
        *payload = std::move(encoded);
        return common::Status::OK();
    }

    common::Status DecodeStoreBusinessResult(
        const raft::OpaquePayload &payload,
        StoreBusinessResult *result)
    {
        if (result == nullptr)
        {
            return common::Status::InvalidArgument("store result output must not be null");
        }

        Decoder decoder(payload);
        std::uint8_t version = 0;
        std::uint8_t duplicate = 0;
        StoreBusinessResult decoded;

        common::Status status = decoder.ReadU8(&version);
        if (!status.ok())
        {
            return status;
        }
        if (version != kStoreBusinessResultVersion)
        {
            return common::Status::Corruption("store result version is not supported");
        }
        status = decoder.ReadU8(&duplicate);
        if (!status.ok())
        {
            return status;
        }
        if (duplicate > 1)
        {
            return common::Status::Corruption("store result duplicate flag is invalid");
        }
        decoded.duplicate_result = duplicate == 1;
        status = ReadStoreInfo(&decoder, &decoded.store);
        if (!status.ok())
        {
            return status;
        }
        std::uint32_t task_count = 0;
        status = decoder.ReadU32(&task_count);
        if (!status.ok())
        {
            return status;
        }
        if (task_count > kMaxStoreBusinessItems)
        {
            return common::Status::Corruption("store result task count exceeds limit");
        }
        decoded.tasks.reserve(task_count);
        for (std::uint32_t i = 0; i < task_count; ++i)
        {
            store::TaskRecord task;
            status = ReadTaskRecord(&decoder, &task);
            if (!status.ok())
            {
                return status;
            }
            decoded.tasks.push_back(std::move(task));
        }
        if (decoder.remaining() != 0)
        {
            return common::Status::Corruption("store result has trailing bytes");
        }

        *result = std::move(decoded);
        return common::Status::OK();
    }

} // namespace cpr::metadata
