#include "raft/raft_core.h"

#include <string>

namespace cpr::raft {
namespace {

using cpr::common::Status;

Status MakeInvalid(std::string message) {
    return Status::InvalidArgument(std::move(message));
}

}  // namespace

common::Status RaftCore::Initialize(const Options& options) {
    Status status = ValidateOptions(options);
    if (!status.ok()) {
        return status;
    }

    node_id_ = options.node_id;
    role_ = options.initial_role;
    leader_id_ = common::kInvalidNodeId;
    logical_ticks_ = 0;
    election_elapsed_ticks_ = 0;
    election_timeout_ticks_ = options.election_timeout_ticks;
    hard_state_ = options.hard_state;
    log_ = RaftLog();
    initialized_ = true;
    return Status::OK();
}

common::Status RaftCore::Tick(TickAction* action) {
    Status status = RequireTickAction(action);
    if (!status.ok()) {
        return status;
    }
    *action = TickAction::NONE;
    ++logical_ticks_;

    if (role_ == RaftRole::LEADER || role_ == RaftRole::LEARNER) {
        return Status::OK();
    }

    ++election_elapsed_ticks_;
    if (election_elapsed_ticks_ < election_timeout_ticks_) {
        return Status::OK();
    }

    status = StartElection();
    if (!status.ok()) {
        return status;
    }
    *action = TickAction::START_ELECTION;
    return Status::OK();
}

common::Status RaftCore::UpdateElectionTimeout(std::uint64_t ticks) {
    if (ticks == 0) {
        return MakeInvalid("election timeout ticks must be greater than zero");
    }
    election_timeout_ticks_ = ticks;
    return Status::OK();
}

common::Status RaftCore::BecomeFollower(common::Term term, common::NodeId leader_id) {
    Status status = ValidateTransitionTerm(term);
    if (!status.ok()) {
        return status;
    }
    if (role_ == RaftRole::LEARNER) {
        return MakeInvalid("learner cannot transition to follower");
    }

    if (term > hard_state_.current_term) {
        hard_state_.current_term = term;
        hard_state_.voted_for = common::kInvalidNodeId;
    }
    role_ = RaftRole::FOLLOWER;
    leader_id_ = leader_id;
    ResetElectionTicks();
    return Status::OK();
}

common::Status RaftCore::StartElection() {
    if (!initialized_) {
        return MakeInvalid("raft core is not initialized");
    }
    if (role_ == RaftRole::LEADER) {
        return MakeInvalid("leader cannot start a new election");
    }
    if (role_ == RaftRole::LEARNER) {
        return MakeInvalid("learner cannot start an election");
    }

    ++hard_state_.current_term;
    hard_state_.voted_for = node_id_;
    role_ = RaftRole::CANDIDATE;
    leader_id_ = common::kInvalidNodeId;
    ResetElectionTicks();
    return Status::OK();
}

common::Status RaftCore::BecomeLeader() {
    if (!initialized_) {
        return MakeInvalid("raft core is not initialized");
    }
    if (role_ == RaftRole::LEARNER) {
        return MakeInvalid("learner cannot become leader");
    }
    if (role_ != RaftRole::CANDIDATE) {
        return MakeInvalid("only a candidate can become leader");
    }

    role_ = RaftRole::LEADER;
    leader_id_ = node_id_;
    ResetElectionTicks();
    return Status::OK();
}

common::NodeId RaftCore::node_id() const noexcept {
    return node_id_;
}

RaftRole RaftCore::role() const noexcept {
    return role_;
}

common::Term RaftCore::current_term() const noexcept {
    return hard_state_.current_term;
}

common::NodeId RaftCore::voted_for() const noexcept {
    return hard_state_.voted_for;
}

common::NodeId RaftCore::leader_id() const noexcept {
    return leader_id_;
}

std::uint64_t RaftCore::logical_ticks() const noexcept {
    return logical_ticks_;
}

std::uint64_t RaftCore::election_elapsed_ticks() const noexcept {
    return election_elapsed_ticks_;
}

std::uint64_t RaftCore::election_timeout_ticks() const noexcept {
    return election_timeout_ticks_;
}

const HardState& RaftCore::hard_state() const noexcept {
    return hard_state_;
}

const RaftLog& RaftCore::log() const noexcept {
    return log_;
}

common::Status RaftCore::ValidateOptions(const Options& options) const {
    if (options.node_id == common::kInvalidNodeId) {
        return MakeInvalid("node id must be valid");
    }
    if (options.election_timeout_ticks == 0) {
        return MakeInvalid("election timeout ticks must be greater than zero");
    }
    if (options.initial_role == RaftRole::LEARNER &&
        options.hard_state.voted_for == options.node_id) {
        return MakeInvalid("learner cannot initialize with a self vote");
    }
    return Status::OK();
}

common::Status RaftCore::RequireTickAction(TickAction* action) const {
    if (!initialized_) {
        return MakeInvalid("raft core is not initialized");
    }
    if (action == nullptr) {
        return MakeInvalid("tick action output pointer must not be null");
    }
    return Status::OK();
}

common::Status RaftCore::ValidateTransitionTerm(common::Term term) const {
    if (!initialized_) {
        return MakeInvalid("raft core is not initialized");
    }
    if (term < hard_state_.current_term) {
        return MakeInvalid("term cannot move backward");
    }
    return Status::OK();
}

void RaftCore::ResetElectionTicks() noexcept {
    election_elapsed_ticks_ = 0;
}

}  // namespace cpr::raft
