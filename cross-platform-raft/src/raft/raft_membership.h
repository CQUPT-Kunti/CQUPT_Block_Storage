#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "common/status.h"
#include "common/types.h"
#include "raft/raft_types.h"

namespace cpr::raft
{

    struct MembershipLogEntry
    {
        std::uint64_t configuration_id = 0;
        std::vector<RaftMember> voters;
        std::vector<RaftMember> learners;
        bool has_active_transition = false;
    };

    class MembershipState
    {
    public:
        MembershipState() = default;

        static common::Status FromView(const MembershipView &view,
                                       MembershipState *state);
        static common::Status Bootstrap(common::NodeId local_node_id,
                                        const MembershipView &bootstrap_view,
                                        const HardState &hard_state,
                                        const std::optional<SnapshotMetadata> &snapshot,
                                        MembershipState *state);

        common::Status ApplyCommitted(const MembershipLogEntry &entry);

        bool empty() const noexcept;
        bool IsVoter(common::NodeId node_id) const noexcept;
        bool IsLearner(common::NodeId node_id) const noexcept;
        bool Contains(common::NodeId node_id) const noexcept;
        bool CanVote(common::NodeId node_id) const noexcept;
        bool CountsTowardQuorum(common::NodeId node_id) const noexcept;
        bool CanBecomeLeader(common::NodeId node_id) const noexcept;

        const std::vector<RaftMember> &voters() const noexcept;
        const std::vector<RaftMember> &learners() const noexcept;
        bool has_active_transition() const noexcept;
        std::uint64_t configuration_id() const noexcept;

        MembershipView ToView() const;
        MembershipLogEntry ToLogEntry() const;

    private:
        std::vector<RaftMember> voters_;
        std::vector<RaftMember> learners_;
        bool has_active_transition_ = false;
        std::uint64_t configuration_id_ = 0;
    };

    common::Status ValidateMembershipView(const MembershipView &view);
    common::Status ValidateMembershipLogEntry(const MembershipLogEntry &entry);
    common::Status BuildAddLearnerLogEntry(const MembershipState &state,
                                           const RaftMember &learner,
                                           MembershipLogEntry *entry);
    common::Status BuildPromoteLearnerLogEntry(const MembershipState &state,
                                               common::NodeId learner_node_id,
                                               MembershipLogEntry *entry);
    common::Status BuildRemoveMemberLogEntry(const MembershipState &state,
                                             common::NodeId member_node_id,
                                             MembershipLogEntry *entry);
    common::Status EncodeMembershipLogEntry(const MembershipLogEntry &entry,
                                            OpaquePayload *payload);
    common::Status DecodeMembershipLogEntry(const OpaquePayload &payload,
                                            MembershipLogEntry *entry);

} // namespace cpr::raft
