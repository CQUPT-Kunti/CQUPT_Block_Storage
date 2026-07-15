#pragma once

#include <cstdint>

#include "common/status.h"
#include "raft/raft_log.h"
#include "raft/raft_types.h"

namespace cpr::raft {

class RaftCore {
public:
    struct Options {
        common::NodeId node_id = common::kInvalidNodeId;
        RaftRole initial_role = RaftRole::FOLLOWER;
        std::uint64_t election_timeout_ticks = 0;
        HardState hard_state;
    };

    enum class TickAction {
        NONE,
        START_ELECTION,
    };

    RaftCore() = default;

    common::Status Initialize(const Options& options);
    common::Status Tick(TickAction* action);
    common::Status UpdateElectionTimeout(std::uint64_t ticks);
    common::Status BecomeFollower(common::Term term, common::NodeId leader_id);
    common::Status StartElection();
    common::Status BecomeLeader();

    common::NodeId node_id() const noexcept;
    RaftRole role() const noexcept;
    common::Term current_term() const noexcept;
    common::NodeId voted_for() const noexcept;
    common::NodeId leader_id() const noexcept;
    std::uint64_t logical_ticks() const noexcept;
    std::uint64_t election_elapsed_ticks() const noexcept;
    std::uint64_t election_timeout_ticks() const noexcept;
    const HardState& hard_state() const noexcept;
    const RaftLog& log() const noexcept;

private:
    common::Status ValidateOptions(const Options& options) const;
    common::Status RequireTickAction(TickAction* action) const;
    common::Status ValidateTransitionTerm(common::Term term) const;
    void ResetElectionTicks() noexcept;

    common::NodeId node_id_ = common::kInvalidNodeId;
    RaftRole role_ = RaftRole::FOLLOWER;
    common::NodeId leader_id_ = common::kInvalidNodeId;
    std::uint64_t logical_ticks_ = 0;
    std::uint64_t election_elapsed_ticks_ = 0;
    std::uint64_t election_timeout_ticks_ = 0;
    HardState hard_state_;
    RaftLog log_;
    bool initialized_ = false;
};

}  // namespace cpr::raft
