#pragma once

#include <cstdint>
#include <map>
#include <vector>

#include "common/status.h"
#include "raft/raft_log.h"
#include "raft/raft_message.h"
#include "raft/raft_types.h"

namespace cpr::raft
{

    class RaftCore
    {
    public:
        struct Options
        {
            common::NodeId node_id = common::kInvalidNodeId;
            RaftRole initial_role = RaftRole::FOLLOWER;
            std::uint64_t election_timeout_ticks = 0;
            HardState hard_state;
            std::vector<common::NodeId> voter_ids;
            std::vector<common::NodeId> learner_ids;
        };

        struct ApplyRange
        {
            common::LogIndex begin = 1;
            common::LogIndex end = 0;

            bool empty() const noexcept
            {
                return begin > end;
            }
        };

        struct PersistenceRequirement
        {
            bool has_hard_state = false;
            HardState hard_state;
            common::LogIndex first_log_index = common::kInvalidLogIndex;
            common::LogIndex last_log_index = common::kInvalidLogIndex;

            bool has_log_entries() const noexcept
            {
                return first_log_index != common::kInvalidLogIndex &&
                       last_log_index >= first_log_index;
            }
        };

        // Runtime must call ConfirmPersistence only after Storage reports success.
        struct PersistenceConfirmation
        {
            common::Status status;
            bool hard_state_persisted = false;
            common::LogIndex stable_index = common::kInvalidLogIndex;
            common::LogIndex max_stable_index = common::kInvalidLogIndex;
        };

        struct RaftOutput
        {
            bool has_hard_state = false;
            HardState hard_state;
            std::vector<LogEntry> unstable_entries;
            PersistenceRequirement persistence;
            std::vector<RaftMessage> immediate_messages;
            std::vector<RaftMessage> persisted_messages;
            // Proposal success is outside RaftCore: Runtime may report it only after commit and apply.
            ApplyRange committed_range;
            RaftRole role = RaftRole::FOLLOWER;
            common::Term term = common::kInitialTerm;
            common::NodeId leader_id = common::kInvalidNodeId;
        };

        enum class TickAction
        {
            NONE,
            START_ELECTION,
        };

        enum class VoteResponseAction
        {
            NONE,
            RECORDED_GRANT,
            RECORDED_REJECTION,
            STEPPED_DOWN,
        };

        enum class AppendResponseAction
        {
            NONE,
            ACCEPTED,
            REJECTED,
            STEPPED_DOWN,
        };

        RaftCore() = default;

        common::Status Initialize(const Options &options);
        common::Status Tick(TickAction *action);
        common::Status UpdateElectionTimeout(std::uint64_t ticks);
        common::Status BecomeFollower(common::Term term, common::NodeId leader_id);
        common::Status StartElection();
        common::Status BecomeLeader();
        common::Status HandleRequestVote(const RequestVoteRequest &request,
                                         RequestVoteResponse *response);
        common::Status HandleRequestVoteResponse(common::NodeId source_node_id,
                                                 const RequestVoteResponse &response,
                                                 VoteResponseAction *action);
        common::Status BuildHeartbeat(AppendEntriesRequest *request) const;
        common::Status BuildAppendEntriesForPeer(common::NodeId target_node_id,
                                                 AppendEntriesRequest *request);
        common::Status HandleAppendEntries(const AppendEntriesRequest &request,
                                           AppendEntriesResponse *response);
        common::Status HandleAppendEntriesResponse(common::NodeId source_node_id,
                                                   const AppendEntriesResponse &response,
                                                   AppendResponseAction *action);
        common::Status Propose(const OpaquePayload &payload);
        common::Status GetPeerProgress(common::NodeId target_node_id,
                                       PeerProgress *progress) const;
        common::Status GetApplyReadyRange(ApplyRange *range) const;
        common::Status GetOutput(RaftOutput *output) const;
        common::Status ConfirmPersistence(const PersistenceConfirmation &confirmation);
        common::Status ConfirmPersistence(bool hard_state_persisted,
                                          common::LogIndex stable_index);

        common::NodeId node_id() const noexcept;
        RaftRole role() const noexcept;
        common::Term current_term() const noexcept;
        common::NodeId voted_for() const noexcept;
        common::NodeId leader_id() const noexcept;
        std::uint64_t logical_ticks() const noexcept;
        std::uint64_t election_elapsed_ticks() const noexcept;
        std::uint64_t election_timeout_ticks() const noexcept;
        const HardState &hard_state() const noexcept;
        const RaftLog &log() const noexcept;

    private:
        common::Status ValidateOptions(const Options &options) const;
        common::Status RequireTickAction(TickAction *action) const;
        common::Status ValidateTransitionTerm(common::Term term) const;
        common::Status RequireRequestVoteResponse(RequestVoteResponse *response) const;
        common::Status RequireVoteResponseAction(VoteResponseAction *action) const;
        common::Status RequireAppendEntriesRequest(AppendEntriesRequest *request) const;
        common::Status RequireAppendEntriesResponse(AppendEntriesResponse *response) const;
        common::Status RequireAppendResponseAction(AppendResponseAction *action) const;
        common::Status RequirePeerProgress(PeerProgress *progress) const;
        common::Status RequireApplyRange(ApplyRange *range) const;
        common::Status RequireOutput(RaftOutput *output) const;
        common::Status ValidateRequestVoteRequest(const RequestVoteRequest &request) const;
        common::Status ValidateRequestVoteResponse(common::NodeId source_node_id,
                                                   const RequestVoteResponse &response) const;
        common::Status ValidateAppendEntriesRequest(const AppendEntriesRequest &request) const;
        common::Status ValidateAppendEntriesResponse(common::NodeId source_node_id,
                                                     const AppendEntriesResponse &response) const;
        common::Status BecomeFollowerForRemote(common::Term term, common::NodeId leader_id);
        common::Status UpdateCurrentTerm(common::Term term);
        common::Status GetLastLogTerm(common::Term *term) const;
        common::Status TryAdvanceCommitIndex();
        common::Status StageMessage(const RaftMessage &message,
                                    bool require_hard_state,
                                    common::LogIndex required_stable_index);
        common::Status ReclassifyBlockedMessages();
        common::Status ResetPeerProgress();
        common::Status SyncLocalPeerProgress();
        common::Status FindPeerProgress(common::NodeId target_node_id,
                                        PeerProgress **progress);
        common::Status FindPeerProgress(common::NodeId target_node_id,
                                        const PeerProgress **progress) const;
        bool HasDirtyHardState() const noexcept;
        bool HardStateChanged() const noexcept;
        bool IsCandidateLogUpToDate(common::LogIndex last_log_index,
                                    common::Term last_log_term) const;
        common::LogIndex FindFirstIndexOfTerm(common::Term term,
                                              common::LogIndex hint_index) const;
        common::LogIndex FindLastIndexOfTerm(common::Term term) const;
        common::LogIndex ClampPeerNextIndex(common::LogIndex index) const noexcept;
        void ResetElectionTicks() noexcept;

        common::NodeId node_id_ = common::kInvalidNodeId;
        RaftRole role_ = RaftRole::FOLLOWER;
        common::NodeId leader_id_ = common::kInvalidNodeId;
        std::uint64_t logical_ticks_ = 0;
        std::uint64_t election_elapsed_ticks_ = 0;
        std::uint64_t election_timeout_ticks_ = 0;
        HardState hard_state_;
        RaftLog log_;
        std::vector<common::NodeId> voter_ids_;
        std::vector<common::NodeId> learner_ids_;
        std::map<common::NodeId, PeerProgress> peer_progress_;
        HardState persisted_hard_state_;
        struct PendingMessage
        {
            RaftMessage message;
            bool require_hard_state = false;
            common::LogIndex required_stable_index = common::kInvalidLogIndex;
        };
        std::vector<RaftMessage> immediate_messages_;
        std::vector<PendingMessage> persisted_messages_;
        bool initialized_ = false;
    };

} // namespace cpr::raft
