#include "raft/raft_core.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>

namespace cpr::raft
{
    namespace
    {

        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::Status;
        using cpr::common::Term;

        Status MakeInvalid(std::string message)
        {
            return Status::InvalidArgument(std::move(message));
        }

        void ResetVoteResponse(RequestVoteResponse *response, Term term)
        {
            response->term = term;
            response->vote_granted = false;
            response->reject_reason = VoteRejectReason::NONE;
        }

        void ResetAppendResponse(AppendEntriesResponse *response, Term term)
        {
            response->term = term;
            response->success = false;
            response->match_index = common::kInvalidLogIndex;
            response->conflict_index = common::kInvalidLogIndex;
            response->conflict_term = common::kInitialTerm;
        }

    } // namespace

    common::Status RaftCore::Initialize(const Options &options)
    {
        Status status = ValidateOptions(options);
        if (!status.ok())
        {
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

    common::Status RaftCore::Tick(TickAction *action)
    {
        Status status = RequireTickAction(action);
        if (!status.ok())
        {
            return status;
        }
        *action = TickAction::NONE;
        ++logical_ticks_;

        if (role_ == RaftRole::LEADER || role_ == RaftRole::LEARNER)
        {
            return Status::OK();
        }

        ++election_elapsed_ticks_;
        if (election_elapsed_ticks_ < election_timeout_ticks_)
        {
            return Status::OK();
        }

        status = StartElection();
        if (!status.ok())
        {
            return status;
        }
        *action = TickAction::START_ELECTION;
        return Status::OK();
    }

    common::Status RaftCore::UpdateElectionTimeout(std::uint64_t ticks)
    {
        if (ticks == 0)
        {
            return MakeInvalid("election timeout ticks must be greater than zero");
        }
        election_timeout_ticks_ = ticks;
        return Status::OK();
    }

    common::Status RaftCore::BecomeFollower(Term term, NodeId leader_id)
    {
        Status status = ValidateTransitionTerm(term);
        if (!status.ok())
        {
            return status;
        }
        if (role_ == RaftRole::LEARNER)
        {
            return MakeInvalid("learner cannot transition to follower");
        }
        return BecomeFollowerForRemote(term, leader_id);
    }

    common::Status RaftCore::StartElection()
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (role_ == RaftRole::LEADER)
        {
            return MakeInvalid("leader cannot start a new election");
        }
        if (role_ == RaftRole::LEARNER)
        {
            return MakeInvalid("learner cannot start an election");
        }

        ++hard_state_.current_term;
        hard_state_.voted_for = node_id_;
        role_ = RaftRole::CANDIDATE;
        leader_id_ = common::kInvalidNodeId;
        ResetElectionTicks();
        return Status::OK();
    }

    common::Status RaftCore::BecomeLeader()
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (role_ == RaftRole::LEARNER)
        {
            return MakeInvalid("learner cannot become leader");
        }
        if (role_ != RaftRole::CANDIDATE)
        {
            return MakeInvalid("only a candidate can become leader");
        }

        role_ = RaftRole::LEADER;
        leader_id_ = node_id_;
        ResetElectionTicks();
        return Status::OK();
    }

    common::Status RaftCore::HandleRequestVote(const RequestVoteRequest &request,
                                               RequestVoteResponse *response)
    {
        Status status = RequireRequestVoteResponse(response);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateRequestVoteRequest(request);
        if (!status.ok())
        {
            return status;
        }

        ResetVoteResponse(response, current_term());
        if (role_ == RaftRole::LEARNER)
        {
            response->reject_reason = VoteRejectReason::LEARNER;
            return Status::OK();
        }
        if (request.term < current_term())
        {
            response->reject_reason = VoteRejectReason::STALE_TERM;
            return Status::OK();
        }
        if (request.term > current_term())
        {
            status = BecomeFollowerForRemote(request.term, common::kInvalidNodeId);
            if (!status.ok())
            {
                return status;
            }
            response->term = current_term();
        }
        if (!IsCandidateLogUpToDate(request.last_log_index, request.last_log_term))
        {
            response->reject_reason = VoteRejectReason::LOG_OUTDATED;
            return Status::OK();
        }
        if (hard_state_.voted_for != common::kInvalidNodeId &&
            hard_state_.voted_for != request.candidate_id)
        {
            response->reject_reason = VoteRejectReason::ALREADY_VOTED;
            return Status::OK();
        }

        hard_state_.voted_for = request.candidate_id;
        ResetElectionTicks();
        response->vote_granted = true;
        response->reject_reason = VoteRejectReason::NONE;
        return Status::OK();
    }

    common::Status RaftCore::HandleRequestVoteResponse(NodeId source_node_id,
                                                       const RequestVoteResponse &response,
                                                       VoteResponseAction *action)
    {
        Status status = RequireVoteResponseAction(action);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateRequestVoteResponse(source_node_id, response);
        if (!status.ok())
        {
            return status;
        }

        *action = VoteResponseAction::NONE;
        if (response.term < current_term())
        {
            return Status::OK();
        }
        if (response.term > current_term())
        {
            status = BecomeFollowerForRemote(response.term, common::kInvalidNodeId);
            if (!status.ok())
            {
                return status;
            }
            *action = VoteResponseAction::STEPPED_DOWN;
            return Status::OK();
        }
        if (role_ != RaftRole::CANDIDATE)
        {
            return Status::OK();
        }

        *action = response.vote_granted ? VoteResponseAction::RECORDED_GRANT
                                        : VoteResponseAction::RECORDED_REJECTION;
        return Status::OK();
    }

    common::Status RaftCore::BuildHeartbeat(AppendEntriesRequest *request) const
    {
        Status status = RequireAppendEntriesRequest(request);
        if (!status.ok())
        {
            return status;
        }
        if (role_ != RaftRole::LEADER)
        {
            return MakeInvalid("only a leader can build heartbeat requests");
        }

        Term prev_term = common::kInitialTerm;
        status = log_.GetTerm(log_.last_index(), &prev_term);
        if (!status.ok())
        {
            return status;
        }

        request->term = current_term();
        request->leader_id = node_id_;
        request->prev_log_index = log_.last_index();
        request->prev_log_term = prev_term;
        request->entries.clear();
        request->leader_commit = log_.commit_index();
        return Status::OK();
    }

    common::Status RaftCore::HandleAppendEntries(const AppendEntriesRequest &request,
                                                 AppendEntriesResponse *response)
    {
        Status status = RequireAppendEntriesResponse(response);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateAppendEntriesRequest(request);
        if (!status.ok())
        {
            return status;
        }

        ResetAppendResponse(response, current_term());
        if (request.term < current_term())
        {
            return Status::OK();
        }
        if (request.term > current_term())
        {
            status = UpdateCurrentTerm(request.term);
            if (!status.ok())
            {
                return status;
            }
        }
        if (role_ == RaftRole::CANDIDATE)
        {
            role_ = RaftRole::FOLLOWER;
        }
        else if (role_ == RaftRole::LEADER && request.leader_id != node_id_)
        {
            return Status::OK();
        }

        leader_id_ = request.leader_id;
        ResetElectionTicks();
        response->term = current_term();

        if (!log_.Matches(request.prev_log_index, request.prev_log_term))
        {
            response->conflict_index = std::min(request.prev_log_index, log_.last_index() + 1);
            if (request.prev_log_index > log_.last_index())
            {
                response->conflict_index = log_.last_index() + 1;
                return Status::OK();
            }

            Term conflict_term = common::kInitialTerm;
            status = log_.GetTerm(request.prev_log_index, &conflict_term);
            if (!status.ok())
            {
                if (request.prev_log_index < log_.first_index())
                {
                    response->conflict_index = log_.first_index();
                    response->conflict_term = log_.snapshot_term();
                    return Status::OK();
                }
                return status;
            }

            response->conflict_term = conflict_term;
            response->conflict_index = FindFirstIndexOfTerm(conflict_term, request.prev_log_index);
            return Status::OK();
        }

        if (!request.entries.empty())
        {
            RaftLog::ConflictResult conflict;
            status = log_.FindConflict(request.entries, &conflict);
            if (!status.ok())
            {
                return status;
            }

            if (conflict.type == RaftLog::ConflictType::CONFLICT)
            {
                status = log_.TruncateSuffix(conflict.index);
                if (!status.ok())
                {
                    return status;
                }
            }
            if (conflict.type != RaftLog::ConflictType::NONE)
            {
                auto begin = request.entries.begin() +
                             static_cast<std::ptrdiff_t>(conflict.index - request.entries.front().index);
                status = log_.Append(std::vector<LogEntry>(begin, request.entries.end()));
                if (!status.ok())
                {
                    return status;
                }
            }
        }

        const LogIndex target_commit = std::min(request.leader_commit, log_.last_index());
        if (target_commit > log_.commit_index())
        {
            status = log_.AdvanceCommitTo(target_commit);
            if (!status.ok())
            {
                return status;
            }
        }

        response->success = true;
        response->match_index = log_.last_index();
        return Status::OK();
    }

    common::Status RaftCore::HandleAppendEntriesResponse(NodeId source_node_id,
                                                         const AppendEntriesResponse &response,
                                                         AppendResponseAction *action)
    {
        Status status = RequireAppendResponseAction(action);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateAppendEntriesResponse(source_node_id, response);
        if (!status.ok())
        {
            return status;
        }

        *action = AppendResponseAction::NONE;
        if (response.term < current_term())
        {
            return Status::OK();
        }
        if (response.term > current_term())
        {
            if (role_ == RaftRole::LEARNER)
            {
                status = UpdateCurrentTerm(response.term);
                if (!status.ok())
                {
                    return status;
                }
            }
            else
            {
                status = BecomeFollowerForRemote(response.term, common::kInvalidNodeId);
                if (!status.ok())
                {
                    return status;
                }
            }
            *action = AppendResponseAction::STEPPED_DOWN;
            return Status::OK();
        }
        if (role_ != RaftRole::LEADER)
        {
            return Status::OK();
        }

        *action = response.success ? AppendResponseAction::ACCEPTED
                                   : AppendResponseAction::REJECTED;
        return Status::OK();
    }

    common::NodeId RaftCore::node_id() const noexcept
    {
        return node_id_;
    }

    RaftRole RaftCore::role() const noexcept
    {
        return role_;
    }

    common::Term RaftCore::current_term() const noexcept
    {
        return hard_state_.current_term;
    }

    common::NodeId RaftCore::voted_for() const noexcept
    {
        return hard_state_.voted_for;
    }

    common::NodeId RaftCore::leader_id() const noexcept
    {
        return leader_id_;
    }

    std::uint64_t RaftCore::logical_ticks() const noexcept
    {
        return logical_ticks_;
    }

    std::uint64_t RaftCore::election_elapsed_ticks() const noexcept
    {
        return election_elapsed_ticks_;
    }

    std::uint64_t RaftCore::election_timeout_ticks() const noexcept
    {
        return election_timeout_ticks_;
    }

    const HardState &RaftCore::hard_state() const noexcept
    {
        return hard_state_;
    }

    const RaftLog &RaftCore::log() const noexcept
    {
        return log_;
    }

    common::Status RaftCore::ValidateOptions(const Options &options) const
    {
        if (options.node_id == common::kInvalidNodeId)
        {
            return MakeInvalid("node id must be valid");
        }
        if (options.election_timeout_ticks == 0)
        {
            return MakeInvalid("election timeout ticks must be greater than zero");
        }
        if (options.initial_role == RaftRole::LEARNER &&
            options.hard_state.voted_for == options.node_id)
        {
            return MakeInvalid("learner cannot initialize with a self vote");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireTickAction(TickAction *action) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (action == nullptr)
        {
            return MakeInvalid("tick action output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::ValidateTransitionTerm(Term term) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (term < hard_state_.current_term)
        {
            return MakeInvalid("term cannot move backward");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireRequestVoteResponse(RequestVoteResponse *response) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (response == nullptr)
        {
            return MakeInvalid("request vote response output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireVoteResponseAction(VoteResponseAction *action) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (action == nullptr)
        {
            return MakeInvalid("request vote action output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireAppendEntriesRequest(AppendEntriesRequest *request) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (request == nullptr)
        {
            return MakeInvalid("append entries request output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireAppendEntriesResponse(AppendEntriesResponse *response) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (response == nullptr)
        {
            return MakeInvalid("append entries response output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireAppendResponseAction(AppendResponseAction *action) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (action == nullptr)
        {
            return MakeInvalid("append entries action output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::ValidateRequestVoteRequest(const RequestVoteRequest &request) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (request.candidate_id == common::kInvalidNodeId)
        {
            return MakeInvalid("candidate id must be valid");
        }
        return Status::OK();
    }

    common::Status RaftCore::ValidateRequestVoteResponse(NodeId source_node_id,
                                                         const RequestVoteResponse &response) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (source_node_id == common::kInvalidNodeId)
        {
            return MakeInvalid("source node id must be valid");
        }
        if (response.vote_granted && response.reject_reason != VoteRejectReason::NONE)
        {
            return MakeInvalid("granted vote response cannot include a reject reason");
        }
        if (!response.vote_granted && response.reject_reason == VoteRejectReason::NONE)
        {
            return MakeInvalid("rejected vote response must include a reject reason");
        }
        return Status::OK();
    }

    common::Status RaftCore::ValidateAppendEntriesRequest(const AppendEntriesRequest &request) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (request.leader_id == common::kInvalidNodeId)
        {
            return MakeInvalid("leader id must be valid");
        }
        if (request.prev_log_index > common::kInvalidLogIndex &&
            request.prev_log_term == common::kInitialTerm)
        {
            return MakeInvalid("prev log term must be positive when prev log index is set");
        }
        return Status::OK();
    }

    common::Status RaftCore::ValidateAppendEntriesResponse(NodeId source_node_id,
                                                           const AppendEntriesResponse &response) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (source_node_id == common::kInvalidNodeId)
        {
            return MakeInvalid("source node id must be valid");
        }
        if (response.success && response.match_index == common::kInvalidLogIndex &&
            log_.last_index() != common::kInvalidLogIndex)
        {
            return MakeInvalid("successful append response must include a match index");
        }
        return Status::OK();
    }

    common::Status RaftCore::BecomeFollowerForRemote(Term term, NodeId leader_id)
    {
        Status status = UpdateCurrentTerm(term);
        if (!status.ok())
        {
            return status;
        }
        if (role_ != RaftRole::LEARNER)
        {
            role_ = RaftRole::FOLLOWER;
        }
        leader_id_ = leader_id;
        ResetElectionTicks();
        return Status::OK();
    }

    common::Status RaftCore::UpdateCurrentTerm(Term term)
    {
        Status status = ValidateTransitionTerm(term);
        if (!status.ok())
        {
            return status;
        }
        if (term > hard_state_.current_term)
        {
            hard_state_.current_term = term;
            hard_state_.voted_for = common::kInvalidNodeId;
        }
        return Status::OK();
    }

    common::Status RaftCore::GetLastLogTerm(Term *term) const
    {
        if (term == nullptr)
        {
            return MakeInvalid("last log term output pointer must not be null");
        }
        return log_.GetTerm(log_.last_index(), term);
    }

    bool RaftCore::IsCandidateLogUpToDate(LogIndex last_log_index, Term last_log_term) const
    {
        Term local_last_term = common::kInitialTerm;
        if (!GetLastLogTerm(&local_last_term).ok())
        {
            return false;
        }
        if (last_log_term != local_last_term)
        {
            return last_log_term > local_last_term;
        }
        return last_log_index >= log_.last_index();
    }

    LogIndex RaftCore::FindFirstIndexOfTerm(Term term, LogIndex hint_index) const
    {
        if (hint_index == log_.snapshot_index() && term == log_.snapshot_term())
        {
            return hint_index;
        }
        LogIndex index = hint_index;
        while (index > log_.snapshot_index())
        {
            Term current = common::kInitialTerm;
            if (!log_.GetTerm(index, &current).ok() || current != term)
            {
                break;
            }
            --index;
        }
        return index + 1;
    }

    void RaftCore::ResetElectionTicks() noexcept
    {
        election_elapsed_ticks_ = 0;
    }

} // namespace cpr::raft
