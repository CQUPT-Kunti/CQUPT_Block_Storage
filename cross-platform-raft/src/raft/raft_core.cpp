#include "raft/raft_core.h"

#include <algorithm>
#include <cstddef>
#include <set>
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

        bool ContainsNode(const std::vector<NodeId> &nodes, NodeId node_id)
        {
            return std::find(nodes.begin(), nodes.end(), node_id) != nodes.end();
        }

        bool SameHardState(const HardState &lhs, const HardState &rhs)
        {
            return lhs.current_term == rhs.current_term &&
                   lhs.voted_for == rhs.voted_for &&
                   lhs.commit_index == rhs.commit_index &&
                   lhs.membership_configuration_id == rhs.membership_configuration_id;
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
        leader_id_ = role_ == RaftRole::LEADER ? node_id_ : common::kInvalidNodeId;
        logical_ticks_ = 0;
        election_elapsed_ticks_ = 0;
        election_timeout_ticks_ = options.election_timeout_ticks;
        hard_state_ = options.hard_state;
        log_ = RaftLog();
        voter_ids_ = options.voter_ids;
        learner_ids_ = options.learner_ids;
        immediate_messages_.clear();
        persisted_messages_.clear();
        initialized_ = true;
        hard_state_.commit_index = log_.commit_index();
        persisted_hard_state_ = hard_state_;
        status = ResetPeerProgress();
        if (!status.ok())
        {
            initialized_ = false;
            return status;
        }
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
        return ResetPeerProgress();
    }

    common::Status RaftCore::Propose(const OpaquePayload &payload)
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (role_ != RaftRole::LEADER)
        {
            return MakeInvalid("only the leader can accept proposals");
        }

        LogEntry entry;
        entry.index = log_.last_index() + 1;
        entry.term = current_term();
        entry.type = LogEntryType::COMMAND;
        entry.payload = payload;

        Status status = log_.Append(entry);
        if (!status.ok())
        {
            return status;
        }

        // Update local peer progress to include the new entry.
        auto it = peer_progress_.find(node_id_);
        if (it != peer_progress_.end())
        {
            it->second.match_index = entry.index;
            it->second.next_index = entry.index + 1;
        }

        // Proposal does not advance commit index or notify client —
        // that requires quorum replication (T013) and apply (T023).
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

        const HardState before_hard_state = hard_state_;
        ResetVoteResponse(response, current_term());
        if (role_ == RaftRole::LEARNER)
        {
            response->reject_reason = VoteRejectReason::LEARNER;
        }
        else if (request.term < current_term())
        {
            response->reject_reason = VoteRejectReason::STALE_TERM;
        }
        else
        {
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
            }
            else if (hard_state_.voted_for != common::kInvalidNodeId &&
                     hard_state_.voted_for != request.candidate_id)
            {
                response->reject_reason = VoteRejectReason::ALREADY_VOTED;
            }
            else
            {
                hard_state_.voted_for = request.candidate_id;
                ResetElectionTicks();
                response->vote_granted = true;
                response->reject_reason = VoteRejectReason::NONE;
            }
        }

        RaftMessage message;
        message.type = RaftMessageType::REQUEST_VOTE_RESPONSE;
        message.source_node_id = node_id_;
        message.target_node_id = request.candidate_id;
        message.payload = *response;
        return StageMessage(message,
                            !SameHardState(before_hard_state, hard_state_),
                            common::kInvalidLogIndex);
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

    common::Status RaftCore::BuildAppendEntriesForPeer(common::NodeId target_node_id,
                                                       AppendEntriesRequest *request)
    {
        Status status = RequireAppendEntriesRequest(request);
        if (!status.ok())
        {
            return status;
        }
        if (role_ != RaftRole::LEADER)
        {
            return MakeInvalid("only a leader can build append entries");
        }
        if (target_node_id == node_id_)
        {
            return MakeInvalid("append entries target must not be the local node");
        }

        const PeerProgress *progress = nullptr;
        status = FindPeerProgress(target_node_id, &progress);
        if (!status.ok())
        {
            return status;
        }
        if (progress->next_index <= log_.snapshot_index())
        {
            return Status::RetryLater("peer requires snapshot before append entries can be built");
        }

        Term prev_term = common::kInitialTerm;
        status = log_.GetTerm(progress->next_index - 1, &prev_term);
        if (!status.ok())
        {
            return status;
        }

        request->term = current_term();
        request->leader_id = node_id_;
        request->prev_log_index = progress->next_index - 1;
        request->prev_log_term = prev_term;
        request->leader_commit = log_.commit_index();
        request->entries.clear();
        if (progress->next_index <= log_.last_index())
        {
            status = log_.GetEntries(progress->next_index,
                                     log_.last_index() + 1,
                                     &request->entries);
            if (!status.ok())
            {
                return status;
            }
        }
        RaftMessage message;
        message.type = RaftMessageType::APPEND_ENTRIES_REQUEST;
        message.source_node_id = node_id_;
        message.target_node_id = target_node_id;
        message.payload = *request;
        const LogIndex required_stable_index = request->entries.empty()
                                                   ? common::kInvalidLogIndex
                                                   : request->entries.back().index;
        return StageMessage(message, HasDirtyHardState(), required_stable_index);
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

        const HardState before_hard_state = hard_state_;
        const LogIndex before_last_index = log_.last_index();
        ResetAppendResponse(response, current_term());
        if (request.term < current_term())
        {
            RaftMessage message;
            message.type = RaftMessageType::APPEND_ENTRIES_RESPONSE;
            message.source_node_id = node_id_;
            message.target_node_id = request.leader_id;
            message.payload = *response;
            return StageMessage(message, false, common::kInvalidLogIndex);
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
            RaftMessage message;
            message.type = RaftMessageType::APPEND_ENTRIES_RESPONSE;
            message.source_node_id = node_id_;
            message.target_node_id = request.leader_id;
            message.payload = *response;
            return StageMessage(message,
                                !SameHardState(before_hard_state, hard_state_),
                                common::kInvalidLogIndex);
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
            }
            else
            {
                Term conflict_term = common::kInitialTerm;
                status = log_.GetTerm(request.prev_log_index, &conflict_term);
                if (!status.ok())
                {
                    if (request.prev_log_index < log_.first_index())
                    {
                        response->conflict_index = log_.first_index();
                        response->conflict_term = log_.snapshot_term();
                    }
                    else
                    {
                        return status;
                    }
                }
                else
                {
                    response->conflict_term = conflict_term;
                    response->conflict_index = FindFirstIndexOfTerm(conflict_term, request.prev_log_index);
                }
            }

            RaftMessage message;
            message.type = RaftMessageType::APPEND_ENTRIES_RESPONSE;
            message.source_node_id = node_id_;
            message.target_node_id = request.leader_id;
            message.payload = *response;
            return StageMessage(message,
                                !SameHardState(before_hard_state, hard_state_),
                                common::kInvalidLogIndex);
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
            hard_state_.commit_index = log_.commit_index();
        }

        status = SyncLocalPeerProgress();
        if (!status.ok())
        {
            return status;
        }

        response->success = true;
        response->match_index = log_.last_index();

        RaftMessage message;
        message.type = RaftMessageType::APPEND_ENTRIES_RESPONSE;
        message.source_node_id = node_id_;
        message.target_node_id = request.leader_id;
        message.payload = *response;
        return StageMessage(message,
                            !SameHardState(before_hard_state, hard_state_),
                            log_.last_index() > before_last_index ? log_.last_index()
                                                                  : common::kInvalidLogIndex);
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

        PeerProgress *progress = nullptr;
        status = FindPeerProgress(source_node_id, &progress);
        if (!status.ok())
        {
            return status;
        }

        progress->is_active = true;
        if (response.success)
        {
            if (response.match_index > log_.last_index())
            {
                return MakeInvalid("append response match index exceeds local last index");
            }
            if (response.match_index > progress->match_index)
            {
                progress->match_index = response.match_index;
            }
            progress->next_index = std::max(progress->next_index, progress->match_index + 1);
            status = TryAdvanceCommitIndex();
            if (!status.ok())
            {
                return status;
            }
            *action = AppendResponseAction::ACCEPTED;
            return Status::OK();
        }

        LogIndex next_index = response.conflict_index;
        if (response.conflict_term != common::kInitialTerm)
        {
            const LogIndex local_index = FindLastIndexOfTerm(response.conflict_term);
            if (local_index != common::kInvalidLogIndex)
            {
                next_index = local_index + 1;
            }
        }
        next_index = ClampPeerNextIndex(next_index);
        progress->next_index = std::min(progress->next_index, next_index);
        *action = AppendResponseAction::REJECTED;
        return Status::OK();
    }

    common::Status RaftCore::GetPeerProgress(common::NodeId target_node_id,
                                             PeerProgress *progress) const
    {
        Status status = RequirePeerProgress(progress);
        if (!status.ok())
        {
            return status;
        }

        const PeerProgress *current = nullptr;
        status = FindPeerProgress(target_node_id, &current);
        if (!status.ok())
        {
            return status;
        }
        *progress = *current;
        return Status::OK();
    }

    common::Status RaftCore::GetApplyReadyRange(ApplyRange *range) const
    {
        Status status = RequireApplyRange(range);
        if (!status.ok())
        {
            return status;
        }
        range->begin = log_.applied_index() + 1;
        range->end = log_.commit_index();
        return Status::OK();
    }

    common::Status RaftCore::GetOutput(RaftOutput *output) const
    {
        Status status = RequireOutput(output);
        if (!status.ok())
        {
            return status;
        }

        output->has_hard_state = HardStateChanged();
        output->hard_state = hard_state_;
        output->unstable_entries.clear();
        output->persistence = PersistenceRequirement{};
        output->persistence.has_hard_state = output->has_hard_state;
        output->persistence.hard_state = hard_state_;
        if (log_.stable_index() < log_.last_index())
        {
            status = log_.GetEntries(log_.stable_index() + 1,
                                     log_.last_index() + 1,
                                     &output->unstable_entries);
            if (!status.ok())
            {
                return status;
            }
            output->persistence.first_log_index = output->unstable_entries.front().index;
            output->persistence.last_log_index = output->unstable_entries.back().index;
        }
        output->immediate_messages = immediate_messages_;
        output->persisted_messages.clear();
        output->persisted_messages.reserve(persisted_messages_.size());
        for (const PendingMessage &pending : persisted_messages_)
        {
            output->persisted_messages.push_back(pending.message);
        }
        status = GetApplyReadyRange(&output->committed_range);
        if (!status.ok())
        {
            return status;
        }
        output->role = role_;
        output->term = current_term();
        output->leader_id = leader_id_;
        return Status::OK();
    }

    common::Status RaftCore::ConfirmPersistence(const PersistenceConfirmation &confirmation)
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (!confirmation.status.ok())
        {
            return confirmation.status;
        }
        const LogIndex stable_index = confirmation.stable_index;
        if (stable_index < log_.stable_index())
        {
            return MakeInvalid("stable index cannot move backward");
        }
        if (stable_index > log_.last_index())
        {
            return MakeInvalid("stable index cannot exceed last index");
        }
        if (confirmation.max_stable_index != common::kInvalidLogIndex &&
            stable_index > confirmation.max_stable_index)
        {
            return MakeInvalid("stable index exceeds confirmed persistence batch");
        }

        if (confirmation.hard_state_persisted)
        {
            persisted_hard_state_ = hard_state_;
        }
        if (stable_index > log_.stable_index())
        {
            Status status = log_.AdvanceStableTo(stable_index);
            if (!status.ok())
            {
                return status;
            }
        }
        return ReclassifyBlockedMessages();
    }

    common::Status RaftCore::ConfirmPersistence(bool hard_state_persisted,
                                                LogIndex stable_index)
    {
        PersistenceConfirmation confirmation;
        confirmation.hard_state_persisted = hard_state_persisted;
        confirmation.stable_index = stable_index;
        confirmation.max_stable_index = stable_index;
        return ConfirmPersistence(confirmation);
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
        if (options.voter_ids.empty())
        {
            return MakeInvalid("at least one voter is required");
        }

        std::set<NodeId> seen;
        for (NodeId node_id : options.voter_ids)
        {
            if (node_id == common::kInvalidNodeId)
            {
                return MakeInvalid("voter id must be valid");
            }
            if (!seen.insert(node_id).second)
            {
                return MakeInvalid("duplicate node id exists in member sets");
            }
        }
        for (NodeId node_id : options.learner_ids)
        {
            if (node_id == common::kInvalidNodeId)
            {
                return MakeInvalid("learner id must be valid");
            }
            if (!seen.insert(node_id).second)
            {
                return MakeInvalid("duplicate node id exists in member sets");
            }
        }

        const bool is_voter = ContainsNode(options.voter_ids, options.node_id);
        const bool is_learner = ContainsNode(options.learner_ids, options.node_id);
        if (!is_voter && !is_learner)
        {
            return MakeInvalid("current node must exist in the member set");
        }
        if (is_learner && options.initial_role != RaftRole::LEARNER)
        {
            return MakeInvalid("learner node must initialize as learner");
        }
        if (is_voter && options.initial_role == RaftRole::LEARNER)
        {
            return MakeInvalid("voter node cannot initialize as learner");
        }
        if (options.initial_role == RaftRole::LEADER && !is_voter)
        {
            return MakeInvalid("learner cannot initialize as leader");
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

    common::Status RaftCore::RequirePeerProgress(PeerProgress *progress) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (progress == nullptr)
        {
            return MakeInvalid("peer progress output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireApplyRange(ApplyRange *range) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (range == nullptr)
        {
            return MakeInvalid("apply range output pointer must not be null");
        }
        return Status::OK();
    }

    common::Status RaftCore::RequireOutput(RaftOutput *output) const
    {
        if (!initialized_)
        {
            return MakeInvalid("raft core is not initialized");
        }
        if (output == nullptr)
        {
            return MakeInvalid("raft output pointer must not be null");
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
        if (!ContainsNode(voter_ids_, source_node_id) &&
            !ContainsNode(learner_ids_, source_node_id))
        {
            return MakeInvalid("source node id is not in the member set");
        }
        if (response.success &&
            response.match_index == common::kInvalidLogIndex &&
            log_.last_index() != common::kInvalidLogIndex)
        {
            return MakeInvalid("successful append response must include a match index");
        }
        if (!response.success &&
            response.term == current_term() &&
            response.conflict_index == common::kInvalidLogIndex)
        {
            return MakeInvalid("failed append response must include a conflict index");
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

    common::Status RaftCore::TryAdvanceCommitIndex()
    {
        std::vector<LogIndex> matches;
        matches.reserve(voter_ids_.size());
        for (NodeId node_id : voter_ids_)
        {
            const PeerProgress *progress = nullptr;
            Status status = FindPeerProgress(node_id, &progress);
            if (!status.ok())
            {
                return status;
            }
            matches.push_back(progress->match_index);
        }

        std::sort(matches.begin(), matches.end());
        const LogIndex candidate = matches[matches.size() / 2];
        if (candidate <= log_.commit_index())
        {
            return Status::OK();
        }

        Term candidate_term = common::kInitialTerm;
        Status status = log_.GetTerm(candidate, &candidate_term);
        if (!status.ok())
        {
            return status;
        }
        if (candidate_term != current_term())
        {
            return Status::OK();
        }

        status = log_.AdvanceCommitTo(candidate);
        if (!status.ok())
        {
            return status;
        }
        hard_state_.commit_index = log_.commit_index();
        return Status::OK();
    }

    common::Status RaftCore::StageMessage(const RaftMessage &message,
                                          bool require_hard_state,
                                          LogIndex required_stable_index)
    {
        const bool has_log_dependency = required_stable_index > log_.stable_index();
        const bool has_hard_state_dependency = require_hard_state && HasDirtyHardState();

        auto same_message = [&message](const auto &entry)
        {
            return entry.message.type == message.type &&
                   entry.message.target_node_id == message.target_node_id;
        };

        immediate_messages_.erase(
            std::remove_if(immediate_messages_.begin(),
                           immediate_messages_.end(),
                           [&message](const RaftMessage &entry)
                           {
                               return entry.type == message.type &&
                                      entry.target_node_id == message.target_node_id;
                           }),
            immediate_messages_.end());
        persisted_messages_.erase(
            std::remove_if(persisted_messages_.begin(),
                           persisted_messages_.end(),
                           same_message),
            persisted_messages_.end());

        if (has_hard_state_dependency || has_log_dependency)
        {
            PendingMessage pending;
            pending.message = message;
            pending.require_hard_state = has_hard_state_dependency;
            pending.required_stable_index = has_log_dependency ? required_stable_index
                                                               : common::kInvalidLogIndex;
            persisted_messages_.push_back(std::move(pending));
        }
        else
        {
            immediate_messages_.push_back(message);
        }
        return Status::OK();
    }

    common::Status RaftCore::ReclassifyBlockedMessages()
    {
        std::vector<PendingMessage> remaining;
        remaining.reserve(persisted_messages_.size());
        for (const PendingMessage &pending : persisted_messages_)
        {
            const bool hard_state_ready = !pending.require_hard_state || !HasDirtyHardState();
            const bool logs_ready = pending.required_stable_index == common::kInvalidLogIndex ||
                                    pending.required_stable_index <= log_.stable_index();
            if (hard_state_ready && logs_ready)
            {
                immediate_messages_.push_back(pending.message);
            }
            else
            {
                remaining.push_back(pending);
            }
        }
        persisted_messages_ = std::move(remaining);
        return Status::OK();
    }

    common::Status RaftCore::ResetPeerProgress()
    {
        peer_progress_.clear();
        const LogIndex last_index = log_.last_index();
        const LogIndex next_index = last_index + 1;

        for (NodeId node_id : voter_ids_)
        {
            PeerProgress progress;
            progress.node_id = node_id;
            progress.is_learner = false;
            progress.is_active = node_id == node_id_;
            progress.match_index = node_id == node_id_ ? last_index : common::kInvalidLogIndex;
            progress.next_index = next_index;
            peer_progress_.emplace(node_id, progress);
        }
        for (NodeId node_id : learner_ids_)
        {
            PeerProgress progress;
            progress.node_id = node_id;
            progress.is_learner = true;
            progress.is_active = node_id == node_id_;
            progress.match_index = node_id == node_id_ ? last_index : common::kInvalidLogIndex;
            progress.next_index = next_index;
            peer_progress_.emplace(node_id, progress);
        }
        return SyncLocalPeerProgress();
    }

    common::Status RaftCore::SyncLocalPeerProgress()
    {
        PeerProgress *progress = nullptr;
        Status status = FindPeerProgress(node_id_, &progress);
        if (!status.ok())
        {
            return status;
        }
        progress->is_active = true;
        progress->match_index = log_.last_index();
        progress->next_index = log_.last_index() + 1;
        return Status::OK();
    }

    common::Status RaftCore::FindPeerProgress(common::NodeId target_node_id,
                                              PeerProgress **progress)
    {
        if (progress == nullptr)
        {
            return MakeInvalid("peer progress pointer must not be null");
        }
        auto it = peer_progress_.find(target_node_id);
        if (it == peer_progress_.end())
        {
            return Status::NotFound("peer progress not found for node " + std::to_string(target_node_id));
        }
        *progress = &it->second;
        return Status::OK();
    }

    common::Status RaftCore::FindPeerProgress(common::NodeId target_node_id,
                                              const PeerProgress **progress) const
    {
        if (progress == nullptr)
        {
            return MakeInvalid("peer progress pointer must not be null");
        }
        auto it = peer_progress_.find(target_node_id);
        if (it == peer_progress_.end())
        {
            return Status::NotFound("peer progress not found for node " + std::to_string(target_node_id));
        }
        *progress = &it->second;
        return Status::OK();
    }

    bool RaftCore::HasDirtyHardState() const noexcept
    {
        return !SameHardState(hard_state_, persisted_hard_state_);
    }

    bool RaftCore::HardStateChanged() const noexcept
    {
        return HasDirtyHardState();
    }

    bool RaftCore::IsCandidateLogUpToDate(LogIndex last_log_index,
                                          Term last_log_term) const
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

    LogIndex RaftCore::FindFirstIndexOfTerm(Term term,
                                            LogIndex hint_index) const
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

    LogIndex RaftCore::FindLastIndexOfTerm(Term term) const
    {
        for (LogIndex index = log_.last_index(); index > log_.snapshot_index(); --index)
        {
            Term current = common::kInitialTerm;
            if (!log_.GetTerm(index, &current).ok())
            {
                break;
            }
            if (current == term)
            {
                return index;
            }
            if (current < term)
            {
                break;
            }
        }
        if (term == log_.snapshot_term())
        {
            return log_.snapshot_index();
        }
        return common::kInvalidLogIndex;
    }

    LogIndex RaftCore::ClampPeerNextIndex(LogIndex index) const noexcept
    {
        const LogIndex minimum = log_.snapshot_index() > common::kInvalidLogIndex
                                     ? log_.snapshot_index()
                                     : log_.first_index();
        const LogIndex maximum = log_.last_index() + 1;
        if (index < minimum)
        {
            return minimum;
        }
        if (index > maximum)
        {
            return maximum;
        }
        return index;
    }

    void RaftCore::ResetElectionTicks() noexcept
    {
        election_elapsed_ticks_ = 0;
    }

} // namespace cpr::raft
