#include "raft/raft_snapshot.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "common/checksum.h"

namespace cpr::raft
{
    namespace
    {

        constexpr std::uint32_t kSnapshotFormatVersion = 1;
        constexpr std::uint64_t kMaxSnapshotPayloadLength = 64ULL * 1024ULL * 1024ULL;
        constexpr std::uint64_t kMaxMembers = 10000;
        constexpr std::uint64_t kMaxHostLength = 4096;

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        common::Status Corrupt(std::string message)
        {
            return common::Status::Corruption(std::move(message));
        }

        void PutU32(SnapshotBytes *bytes, std::uint32_t value)
        {
            for (int shift = 0; shift < 32; shift += 8)
            {
                bytes->push_back(static_cast<common::Byte>((value >> shift) & 0xffU));
            }
        }

        void PutU64(SnapshotBytes *bytes, std::uint64_t value)
        {
            for (int shift = 0; shift < 64; shift += 8)
            {
                bytes->push_back(static_cast<common::Byte>((value >> shift) & 0xffULL));
            }
        }

        common::Status ReadU32(const SnapshotBytes &bytes,
                               std::size_t *offset,
                               std::uint32_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return Invalid("snapshot read pointer must not be null");
            }
            if (*offset > bytes.size() || bytes.size() - *offset < 4)
            {
                return Corrupt("snapshot is truncated while reading uint32");
            }
            std::uint32_t result = 0;
            for (int shift = 0; shift < 32; shift += 8)
            {
                result |= static_cast<std::uint32_t>(bytes[(*offset)++]) << shift;
            }
            *value = result;
            return common::Status::OK();
        }

        common::Status ReadU64(const SnapshotBytes &bytes,
                               std::size_t *offset,
                               std::uint64_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return Invalid("snapshot read pointer must not be null");
            }
            if (*offset > bytes.size() || bytes.size() - *offset < 8)
            {
                return Corrupt("snapshot is truncated while reading uint64");
            }
            std::uint64_t result = 0;
            for (int shift = 0; shift < 64; shift += 8)
            {
                result |= static_cast<std::uint64_t>(bytes[(*offset)++]) << shift;
            }
            *value = result;
            return common::Status::OK();
        }

        common::Status EncodeMember(const RaftMember &member, SnapshotBytes *bytes)
        {
            if (member.node_id == common::kInvalidNodeId)
            {
                return Invalid("snapshot member id must be positive");
            }
            if (member.address.host.size() > kMaxHostLength)
            {
                return Invalid("snapshot member host is too long");
            }
            PutU64(bytes, member.node_id);
            PutU32(bytes, member.address.port);
            PutU64(bytes, static_cast<std::uint64_t>(member.address.host.size()));
            bytes->insert(bytes->end(), member.address.host.begin(), member.address.host.end());
            return common::Status::OK();
        }

        common::Status DecodeMember(const SnapshotBytes &bytes,
                                    std::size_t *offset,
                                    RaftMember *member)
        {
            if (member == nullptr)
            {
                return Invalid("snapshot member output pointer must not be null");
            }
            std::uint64_t node_id = 0;
            std::uint32_t port = 0;
            std::uint64_t host_length = 0;
            common::Status status = ReadU64(bytes, offset, &node_id);
            if (!status.ok())
            {
                return status;
            }
            status = ReadU32(bytes, offset, &port);
            if (!status.ok())
            {
                return status;
            }
            status = ReadU64(bytes, offset, &host_length);
            if (!status.ok())
            {
                return status;
            }
            if (node_id == common::kInvalidNodeId)
            {
                return Corrupt("snapshot member id is invalid");
            }
            if (port > 65535U)
            {
                return Corrupt("snapshot member port is invalid");
            }
            if (host_length > kMaxHostLength)
            {
                return Corrupt("snapshot member host is too long");
            }
            if (*offset > bytes.size() || host_length > bytes.size() - *offset)
            {
                return Corrupt("snapshot member host is truncated");
            }
            member->node_id = node_id;
            member->address.port = static_cast<std::uint16_t>(port);
            member->address.host.assign(bytes.begin() + static_cast<std::ptrdiff_t>(*offset),
                                        bytes.begin() + static_cast<std::ptrdiff_t>(*offset + host_length));
            *offset += static_cast<std::size_t>(host_length);
            return common::Status::OK();
        }

        common::Status EncodeMembers(const std::vector<RaftMember> &members,
                                     SnapshotBytes *bytes)
        {
            PutU64(bytes, static_cast<std::uint64_t>(members.size()));
            for (const RaftMember &member : members)
            {
                common::Status status = EncodeMember(member, bytes);
                if (!status.ok())
                {
                    return status;
                }
            }
            return common::Status::OK();
        }

        common::Status DecodeMembers(const SnapshotBytes &bytes,
                                     std::size_t *offset,
                                     std::vector<RaftMember> *members)
        {
            std::uint64_t count = 0;
            common::Status status = ReadU64(bytes, offset, &count);
            if (!status.ok())
            {
                return status;
            }
            if (count > kMaxMembers)
            {
                return Corrupt("snapshot member count is too large");
            }
            members->clear();
            members->reserve(static_cast<std::size_t>(count));
            for (std::uint64_t i = 0; i < count; ++i)
            {
                RaftMember member;
                status = DecodeMember(bytes, offset, &member);
                if (!status.ok())
                {
                    return status;
                }
                members->push_back(std::move(member));
            }
            return common::Status::OK();
        }

    } // namespace

    common::Status EncodeSnapshotData(const SnapshotData &snapshot,
                                      SnapshotBytes *bytes)
    {
        if (bytes == nullptr)
        {
            return Invalid("snapshot output pointer must not be null");
        }
        if (snapshot.metadata.last_included_index == common::kInvalidLogIndex)
        {
            return Invalid("snapshot index must be positive");
        }
        if (snapshot.metadata.last_included_term == common::kInitialTerm)
        {
            return Invalid("snapshot term must be positive");
        }
        if (snapshot.payload.size() > kMaxSnapshotPayloadLength)
        {
            return Invalid("snapshot payload is too large");
        }

        SnapshotBytes body;
        PutU32(&body, kSnapshotFormatVersion);
        PutU64(&body, snapshot.metadata.last_included_index);
        PutU64(&body, snapshot.metadata.last_included_term);
        PutU64(&body, snapshot.metadata.membership.configuration_id);
        PutU32(&body, snapshot.metadata.membership.has_active_transition ? 1U : 0U);
        common::Status status = EncodeMembers(snapshot.metadata.membership.voters, &body);
        if (!status.ok())
        {
            return status;
        }
        status = EncodeMembers(snapshot.metadata.membership.learners, &body);
        if (!status.ok())
        {
            return status;
        }
        PutU64(&body, static_cast<std::uint64_t>(snapshot.payload.size()));
        body.insert(body.end(), snapshot.payload.begin(), snapshot.payload.end());
        const common::ChecksumValue checksum = common::Checksum::Compute(body);
        PutU64(&body, checksum);

        *bytes = std::move(body);
        return common::Status::OK();
    }

    common::Status DecodeSnapshotData(const SnapshotBytes &bytes,
                                      SnapshotData *snapshot)
    {
        if (snapshot == nullptr)
        {
            return Invalid("snapshot output pointer must not be null");
        }
        if (bytes.empty())
        {
            return Corrupt("snapshot file is empty");
        }

        std::size_t offset = 0;
        std::uint32_t version = 0;
        SnapshotData decoded;
        common::Status status = ReadU32(bytes, &offset, &version);
        if (!status.ok())
        {
            return status;
        }
        if (version != kSnapshotFormatVersion)
        {
            return Corrupt("unsupported snapshot format version");
        }
        status = ReadU64(bytes, &offset, &decoded.metadata.last_included_index);
        if (!status.ok())
        {
            return status;
        }
        status = ReadU64(bytes, &offset, &decoded.metadata.last_included_term);
        if (!status.ok())
        {
            return status;
        }
        status = ReadU64(bytes, &offset, &decoded.metadata.membership.configuration_id);
        if (!status.ok())
        {
            return status;
        }
        std::uint32_t active_transition = 0;
        status = ReadU32(bytes, &offset, &active_transition);
        if (!status.ok())
        {
            return status;
        }
        if (active_transition > 1)
        {
            return Corrupt("snapshot active transition flag is invalid");
        }
        decoded.metadata.membership.has_active_transition = active_transition == 1;
        status = DecodeMembers(bytes, &offset, &decoded.metadata.membership.voters);
        if (!status.ok())
        {
            return status;
        }
        status = DecodeMembers(bytes, &offset, &decoded.metadata.membership.learners);
        if (!status.ok())
        {
            return status;
        }

        std::uint64_t payload_length = 0;
        status = ReadU64(bytes, &offset, &payload_length);
        if (!status.ok())
        {
            return status;
        }
        if (payload_length > kMaxSnapshotPayloadLength)
        {
            return Corrupt("snapshot payload is too large");
        }
        if (offset > bytes.size() || payload_length > bytes.size() - offset)
        {
            return Corrupt("snapshot payload is truncated");
        }
        decoded.payload.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                               bytes.begin() + static_cast<std::ptrdiff_t>(offset + payload_length));
        offset += static_cast<std::size_t>(payload_length);
        if (bytes.size() - offset != 8)
        {
            return Corrupt("snapshot checksum is missing or has trailing bytes");
        }
        std::uint64_t checksum = 0;
        status = ReadU64(bytes, &offset, &checksum);
        if (!status.ok())
        {
            return status;
        }
        SnapshotBytes checksum_input(bytes.begin(), bytes.end() - 8);
        if (common::Checksum::Compute(checksum_input) != checksum)
        {
            return Corrupt("snapshot checksum mismatch");
        }
        if (decoded.metadata.last_included_index == common::kInvalidLogIndex)
        {
            return Corrupt("snapshot index is invalid");
        }
        if (decoded.metadata.last_included_term == common::kInitialTerm)
        {
            return Corrupt("snapshot term is invalid");
        }

        *snapshot = std::move(decoded);
        return common::Status::OK();
    }

} // namespace cpr::raft
