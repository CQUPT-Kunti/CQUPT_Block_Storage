#include "raft/raft_membership.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace cpr::raft
{
    namespace
    {

        constexpr std::uint8_t kMembershipLogFormatVersion = 1;
        constexpr std::uint8_t kActiveTransitionFlag = 0x01;

        bool MemberLess(const RaftMember &lhs, const RaftMember &rhs)
        {
            if (lhs.node_id != rhs.node_id)
            {
                return lhs.node_id < rhs.node_id;
            }
            if (lhs.address.host != rhs.address.host)
            {
                return lhs.address.host < rhs.address.host;
            }
            return lhs.address.port < rhs.address.port;
        }

        bool MemberEqual(const RaftMember &lhs, const RaftMember &rhs)
        {
            return lhs.node_id == rhs.node_id &&
                   lhs.address.host == rhs.address.host &&
                   lhs.address.port == rhs.address.port;
        }

        common::Status ValidateMembers(const std::vector<RaftMember> &members,
                                       const char *label)
        {
            std::vector<RaftMember> normalized = members;
            std::sort(normalized.begin(), normalized.end(), MemberLess);

            for (std::size_t i = 0; i < normalized.size(); ++i)
            {
                const RaftMember &member = normalized[i];
                if (member.node_id == common::kInvalidNodeId)
                {
                    return common::Status::InvalidArgument(
                        std::string(label) + " contains invalid node id");
                }
                if (i > 0 && normalized[i - 1].node_id == member.node_id)
                {
                    return common::Status::InvalidArgument(
                        std::string(label) + " contains duplicate node id");
                }
            }

            return common::Status::OK();
        }

        void NormalizeMembers(std::vector<RaftMember> *members)
        {
            std::sort(members->begin(), members->end(), MemberLess);
        }

        common::Status NormalizeView(const MembershipView &view,
                                     MembershipView *normalized)
        {
            if (normalized == nullptr)
            {
                return common::Status::InvalidArgument(
                    "normalized membership view output is null");
            }

            common::Status status = ValidateMembershipView(view);
            if (!status.ok())
            {
                return status;
            }

            *normalized = view;
            NormalizeMembers(&normalized->voters);
            NormalizeMembers(&normalized->learners);
            return common::Status::OK();
        }

        common::Status NormalizeLogEntry(const MembershipLogEntry &entry,
                                         MembershipLogEntry *normalized)
        {
            if (normalized == nullptr)
            {
                return common::Status::InvalidArgument(
                    "normalized membership entry output is null");
            }

            common::Status status = ValidateMembershipLogEntry(entry);
            if (!status.ok())
            {
                return status;
            }

            *normalized = entry;
            NormalizeMembers(&normalized->voters);
            NormalizeMembers(&normalized->learners);
            return common::Status::OK();
        }

        bool ViewsEqual(const MembershipView &lhs, const MembershipView &rhs)
        {
            return lhs.configuration_id == rhs.configuration_id &&
                   lhs.has_active_transition == rhs.has_active_transition &&
                   lhs.voters.size() == rhs.voters.size() &&
                   lhs.learners.size() == rhs.learners.size() &&
                   std::equal(lhs.voters.begin(),
                              lhs.voters.end(),
                              rhs.voters.begin(),
                              MemberEqual) &&
                   std::equal(lhs.learners.begin(),
                              lhs.learners.end(),
                              rhs.learners.begin(),
                              MemberEqual);
        }

        void PutU8(OpaquePayload *payload, std::uint8_t value)
        {
            payload->push_back(value);
        }

        void PutU16(OpaquePayload *payload, std::uint16_t value)
        {
            payload->push_back(static_cast<common::Byte>(value & 0xFFu));
            payload->push_back(static_cast<common::Byte>((value >> 8) & 0xFFu));
        }

        void PutU32(OpaquePayload *payload, std::uint32_t value)
        {
            for (std::uint32_t shift = 0; shift < 32; shift += 8)
            {
                payload->push_back(
                    static_cast<common::Byte>((value >> shift) & 0xFFu));
            }
        }

        void PutU64(OpaquePayload *payload, std::uint64_t value)
        {
            for (std::uint32_t shift = 0; shift < 64; shift += 8)
            {
                payload->push_back(
                    static_cast<common::Byte>((value >> shift) & 0xFFu));
            }
        }

        common::Status ReadU8(const OpaquePayload &payload,
                              std::size_t *offset,
                              std::uint8_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return common::Status::InvalidArgument(
                    "membership payload reader received null output");
            }
            if (*offset >= payload.size())
            {
                return common::Status::Corruption(
                    "membership payload truncated while reading u8");
            }
            *value = payload[*offset];
            ++(*offset);
            return common::Status::OK();
        }

        common::Status ReadU16(const OpaquePayload &payload,
                               std::size_t *offset,
                               std::uint16_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return common::Status::InvalidArgument(
                    "membership payload reader received null output");
            }
            if (payload.size() - *offset < sizeof(std::uint16_t))
            {
                return common::Status::Corruption(
                    "membership payload truncated while reading u16");
            }

            const std::uint16_t low =
                static_cast<std::uint16_t>(payload[*offset]);
            const std::uint16_t high = static_cast<std::uint16_t>(
                static_cast<std::uint16_t>(payload[*offset + 1]) << 8);
            const std::uint16_t decoded =
                static_cast<std::uint16_t>(low | high);
            *offset += sizeof(std::uint16_t);
            *value = decoded;
            return common::Status::OK();
        }

        common::Status ReadU32(const OpaquePayload &payload,
                               std::size_t *offset,
                               std::uint32_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return common::Status::InvalidArgument(
                    "membership payload reader received null output");
            }
            if (payload.size() - *offset < sizeof(std::uint32_t))
            {
                return common::Status::Corruption(
                    "membership payload truncated while reading u32");
            }

            std::uint32_t decoded = 0;
            for (std::uint32_t shift = 0; shift < 32; shift += 8)
            {
                decoded |= static_cast<std::uint32_t>(payload[*offset]) << shift;
                ++(*offset);
            }
            *value = decoded;
            return common::Status::OK();
        }

        common::Status ReadU64(const OpaquePayload &payload,
                               std::size_t *offset,
                               std::uint64_t *value)
        {
            if (offset == nullptr || value == nullptr)
            {
                return common::Status::InvalidArgument(
                    "membership payload reader received null output");
            }
            if (payload.size() - *offset < sizeof(std::uint64_t))
            {
                return common::Status::Corruption(
                    "membership payload truncated while reading u64");
            }

            std::uint64_t decoded = 0;
            for (std::uint32_t shift = 0; shift < 64; shift += 8)
            {
                decoded |= static_cast<std::uint64_t>(payload[*offset]) << shift;
                ++(*offset);
            }
            *value = decoded;
            return common::Status::OK();
        }

        common::Status EncodeMember(const RaftMember &member,
                                    OpaquePayload *payload)
        {
            if (payload == nullptr)
            {
                return common::Status::InvalidArgument(
                    "membership payload output is null");
            }

            PutU64(payload, member.node_id);
            if (member.address.host.size() > UINT32_MAX)
            {
                return common::Status::InvalidArgument(
                    "member host is too large to encode");
            }
            PutU32(payload,
                   static_cast<std::uint32_t>(member.address.host.size()));
            payload->insert(payload->end(),
                            member.address.host.begin(),
                            member.address.host.end());
            PutU16(payload, member.address.port);
            return common::Status::OK();
        }

        common::Status DecodeMember(const OpaquePayload &payload,
                                    std::size_t *offset,
                                    RaftMember *member)
        {
            if (member == nullptr)
            {
                return common::Status::InvalidArgument(
                    "decoded member output is null");
            }

            std::uint64_t node_id = 0;
            common::Status status = ReadU64(payload, offset, &node_id);
            if (!status.ok())
            {
                return status;
            }

            std::uint32_t host_size = 0;
            status = ReadU32(payload, offset, &host_size);
            if (!status.ok())
            {
                return status;
            }
            if (payload.size() - *offset < host_size)
            {
                return common::Status::Corruption(
                    "membership payload truncated while reading host");
            }

            RaftMember decoded;
            decoded.node_id = node_id;
            decoded.address.host.assign(
                reinterpret_cast<const char *>(payload.data() + *offset),
                host_size);
            *offset += host_size;

            std::uint16_t port = 0;
            status = ReadU16(payload, offset, &port);
            if (!status.ok())
            {
                return status;
            }
            decoded.address.port = port;
            *member = std::move(decoded);
            return common::Status::OK();
        }

        common::Status EncodeMembers(const std::vector<RaftMember> &members,
                                     OpaquePayload *payload)
        {
            if (members.size() > UINT32_MAX)
            {
                return common::Status::InvalidArgument(
                    "member set is too large to encode");
            }

            PutU32(payload, static_cast<std::uint32_t>(members.size()));
            for (const RaftMember &member : members)
            {
                common::Status status = EncodeMember(member, payload);
                if (!status.ok())
                {
                    return status;
                }
            }
            return common::Status::OK();
        }

        common::Status DecodeMembers(const OpaquePayload &payload,
                                     std::size_t *offset,
                                     std::vector<RaftMember> *members)
        {
            if (members == nullptr)
            {
                return common::Status::InvalidArgument(
                    "decoded member set output is null");
            }

            std::uint32_t count = 0;
            common::Status status = ReadU32(payload, offset, &count);
            if (!status.ok())
            {
                return status;
            }

            std::vector<RaftMember> decoded;
            decoded.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i)
            {
                RaftMember member;
                status = DecodeMember(payload, offset, &member);
                if (!status.ok())
                {
                    return status;
                }
                decoded.push_back(std::move(member));
            }
            *members = std::move(decoded);
            return common::Status::OK();
        }

    } // namespace

    common::Status ValidateMembershipView(const MembershipView &view)
    {
        if (view.configuration_id == 0)
        {
            return common::Status::InvalidArgument(
                "membership configuration id must be non-zero");
        }
        if (view.voters.empty())
        {
            return common::Status::InvalidArgument(
                "membership must contain at least one voter");
        }

        common::Status status = ValidateMembers(view.voters, "voters");
        if (!status.ok())
        {
            return status;
        }
        status = ValidateMembers(view.learners, "learners");
        if (!status.ok())
        {
            return status;
        }

        std::vector<RaftMember> voters = view.voters;
        std::vector<RaftMember> learners = view.learners;
        NormalizeMembers(&voters);
        NormalizeMembers(&learners);

        std::size_t voter_index = 0;
        std::size_t learner_index = 0;
        while (voter_index < voters.size() && learner_index < learners.size())
        {
            if (voters[voter_index].node_id == learners[learner_index].node_id)
            {
                return common::Status::InvalidArgument(
                    "membership node cannot be both voter and learner");
            }
            if (voters[voter_index].node_id < learners[learner_index].node_id)
            {
                ++voter_index;
            }
            else
            {
                ++learner_index;
            }
        }

        return common::Status::OK();
    }

    common::Status ValidateMembershipLogEntry(const MembershipLogEntry &entry)
    {
        MembershipView view;
        view.voters = entry.voters;
        view.learners = entry.learners;
        view.has_active_transition = entry.has_active_transition;
        view.configuration_id = entry.configuration_id;
        return ValidateMembershipView(view);
    }

    common::Status MembershipState::FromView(const MembershipView &view,
                                             MembershipState *state)
    {
        if (state == nullptr)
        {
            return common::Status::InvalidArgument(
                "membership state output is null");
        }

        MembershipView normalized;
        common::Status status = NormalizeView(view, &normalized);
        if (!status.ok())
        {
            return status;
        }

        MembershipState candidate;
        candidate.voters_ = std::move(normalized.voters);
        candidate.learners_ = std::move(normalized.learners);
        candidate.has_active_transition_ = normalized.has_active_transition;
        candidate.configuration_id_ = normalized.configuration_id;
        *state = std::move(candidate);
        return common::Status::OK();
    }

    common::Status MembershipState::Bootstrap(
        common::NodeId local_node_id,
        const MembershipView &bootstrap_view,
        const HardState &hard_state,
        const std::optional<SnapshotMetadata> &snapshot,
        MembershipState *state)
    {
        if (local_node_id == common::kInvalidNodeId)
        {
            return common::Status::InvalidArgument(
                "bootstrap local node id is invalid");
        }
        if (snapshot.has_value())
        {
            return common::Status::Busy(
                "bootstrap rejected because snapshot membership already exists");
        }
        if (hard_state.membership_configuration_id != 0)
        {
            return common::Status::Busy(
                "bootstrap rejected because hard state membership already exists");
        }

        MembershipState candidate;
        common::Status status = FromView(bootstrap_view, &candidate);
        if (!status.ok())
        {
            return status;
        }
        if (!candidate.Contains(local_node_id))
        {
            return common::Status::InvalidArgument(
                "bootstrap membership does not include local node");
        }

        *state = std::move(candidate);
        return common::Status::OK();
    }

    common::Status MembershipState::ApplyCommitted(
        const MembershipLogEntry &entry)
    {
        MembershipLogEntry normalized;
        common::Status status = NormalizeLogEntry(entry, &normalized);
        if (!status.ok())
        {
            return status;
        }

        const MembershipView current = ToView();
        MembershipView incoming;
        incoming.voters = normalized.voters;
        incoming.learners = normalized.learners;
        incoming.has_active_transition = normalized.has_active_transition;
        incoming.configuration_id = normalized.configuration_id;

        if (empty())
        {
            return FromView(incoming, this);
        }
        if (normalized.configuration_id < configuration_id_)
        {
            return common::Status::InvalidArgument(
                "membership configuration id cannot move backwards");
        }
        if (normalized.configuration_id == configuration_id_)
        {
            MembershipView normalized_current;
            status = NormalizeView(current, &normalized_current);
            if (!status.ok())
            {
                return status;
            }
            if (!ViewsEqual(normalized_current, incoming))
            {
                return common::Status::InvalidArgument(
                    "membership configuration id already exists with different content");
            }
            return common::Status::OK();
        }

        MembershipState candidate;
        status = FromView(incoming, &candidate);
        if (!status.ok())
        {
            return status;
        }

        *this = std::move(candidate);
        return common::Status::OK();
    }

    bool MembershipState::empty() const noexcept
    {
        return configuration_id_ == 0 && voters_.empty() && learners_.empty() &&
               !has_active_transition_;
    }

    bool MembershipState::IsVoter(common::NodeId node_id) const noexcept
    {
        return std::any_of(voters_.begin(),
                           voters_.end(),
                           [node_id](const RaftMember &member)
                           { return member.node_id == node_id; });
    }

    bool MembershipState::IsLearner(common::NodeId node_id) const noexcept
    {
        return std::any_of(learners_.begin(),
                           learners_.end(),
                           [node_id](const RaftMember &member)
                           { return member.node_id == node_id; });
    }

    bool MembershipState::Contains(common::NodeId node_id) const noexcept
    {
        return IsVoter(node_id) || IsLearner(node_id);
    }

    bool MembershipState::CanVote(common::NodeId node_id) const noexcept
    {
        return IsVoter(node_id);
    }

    bool MembershipState::CountsTowardQuorum(common::NodeId node_id) const noexcept
    {
        return IsVoter(node_id);
    }

    bool MembershipState::CanBecomeLeader(common::NodeId node_id) const noexcept
    {
        return IsVoter(node_id);
    }

    const std::vector<RaftMember> &MembershipState::voters() const noexcept
    {
        return voters_;
    }

    const std::vector<RaftMember> &MembershipState::learners() const noexcept
    {
        return learners_;
    }

    bool MembershipState::has_active_transition() const noexcept
    {
        return has_active_transition_;
    }

    std::uint64_t MembershipState::configuration_id() const noexcept
    {
        return configuration_id_;
    }

    MembershipView MembershipState::ToView() const
    {
        MembershipView view;
        view.voters = voters_;
        view.learners = learners_;
        view.has_active_transition = has_active_transition_;
        view.configuration_id = configuration_id_;
        return view;
    }

    MembershipLogEntry MembershipState::ToLogEntry() const
    {
        MembershipLogEntry entry;
        entry.configuration_id = configuration_id_;
        entry.voters = voters_;
        entry.learners = learners_;
        entry.has_active_transition = has_active_transition_;
        return entry;
    }

    common::Status EncodeMembershipLogEntry(const MembershipLogEntry &entry,
                                            OpaquePayload *payload)
    {
        if (payload == nullptr)
        {
            return common::Status::InvalidArgument(
                "membership payload output is null");
        }

        MembershipLogEntry normalized;
        common::Status status = NormalizeLogEntry(entry, &normalized);
        if (!status.ok())
        {
            return status;
        }

        OpaquePayload encoded;
        encoded.reserve(32);
        PutU8(&encoded, kMembershipLogFormatVersion);
        PutU8(&encoded,
              normalized.has_active_transition ? kActiveTransitionFlag : 0);
        PutU64(&encoded, normalized.configuration_id);
        status = EncodeMembers(normalized.voters, &encoded);
        if (!status.ok())
        {
            return status;
        }
        status = EncodeMembers(normalized.learners, &encoded);
        if (!status.ok())
        {
            return status;
        }

        *payload = std::move(encoded);
        return common::Status::OK();
    }

    common::Status DecodeMembershipLogEntry(const OpaquePayload &payload,
                                            MembershipLogEntry *entry)
    {
        if (entry == nullptr)
        {
            return common::Status::InvalidArgument(
                "decoded membership entry output is null");
        }

        std::size_t offset = 0;
        std::uint8_t version = 0;
        common::Status status = ReadU8(payload, &offset, &version);
        if (!status.ok())
        {
            return status;
        }
        if (version != kMembershipLogFormatVersion)
        {
            return common::Status::Corruption(
                "membership payload has unknown format version");
        }

        std::uint8_t flags = 0;
        status = ReadU8(payload, &offset, &flags);
        if (!status.ok())
        {
            return status;
        }
        if ((flags & ~kActiveTransitionFlag) != 0)
        {
            return common::Status::Corruption(
                "membership payload contains unknown flags");
        }

        MembershipLogEntry decoded;
        status = ReadU64(payload, &offset, &decoded.configuration_id);
        if (!status.ok())
        {
            return status;
        }
        decoded.has_active_transition = (flags & kActiveTransitionFlag) != 0;

        status = DecodeMembers(payload, &offset, &decoded.voters);
        if (!status.ok())
        {
            return status;
        }
        status = DecodeMembers(payload, &offset, &decoded.learners);
        if (!status.ok())
        {
            return status;
        }
        if (offset != payload.size())
        {
            return common::Status::Corruption(
                "membership payload has trailing bytes");
        }

        MembershipLogEntry normalized;
        status = NormalizeLogEntry(decoded, &normalized);
        if (!status.ok())
        {
            return status.code() == common::StatusCode::kInvalidArgument
                       ? common::Status::Corruption(status.message())
                       : status;
        }

        *entry = std::move(normalized);
        return common::Status::OK();
    }

} // namespace cpr::raft
