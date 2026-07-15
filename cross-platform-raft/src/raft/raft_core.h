#pragma once

#include <cstdint>

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
        common::Status HandleAppendEntries(const AppendEntriesRequest &request,
                                           AppendEntriesResponse *response);
        common::Status HandleAppendEntriesResponse(common::NodeId source_node_id,
                                                   const AppendEntriesResponse &response,
                                                   AppendResponseAction *action);

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
        common::Status ValidateRequestVoteRequest(const RequestVoteRequest &request) const;
        common::Status ValidateRequestVoteResponse(common::NodeId source_node_id,
                                                   const RequestVoteResponse &response) const;
        common::Status ValidateAppendEntriesRequest(const AppendEntriesRequest &request) const;
        common::Status ValidateAppendEntriesResponse(common::NodeId source_node_id,
                                                     const AppendEntriesResponse &response) const;
        common::Status BecomeFollowerForRemote(common::Term term, common::NodeId leader_id);
        common::Status UpdateCurrentTerm(common::Term term);
        common::Status GetLastLogTerm(common::Term *term) const;
        bool IsCandidateLogUpToDate(common::LogIndex last_log_index,
                                    common::Term last_log_term) const;
        common::LogIndex FindFirstIndexOfTerm(common::Term term,
                                              common::LogIndex hint_index) const;
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

} // namespace cpr::raft
