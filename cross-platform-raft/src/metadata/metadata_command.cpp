#include "metadata/metadata_command.h"

#include <limits>
#include <utility>

namespace cpr::metadata
{
    namespace
    {

        constexpr std::uint16_t kFlagHasExpectedGeneration = 0x0001;

        bool IsKnownType(MetadataCommandType type)
        {
            switch (type)
            {
            case MetadataCommandType::OPAQUE_OPERATION:
                return true;
            }
            return false;
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

} // namespace cpr::metadata
