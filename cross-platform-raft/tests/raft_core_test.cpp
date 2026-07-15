#include <algorithm>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "raft/raft_core.h"

namespace cpr::raft
{
    namespace
    {

        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::Term;

        LogEntry MakeEntry(LogIndex index, Term term, std::string payload = "")
        {
            LogEntry entry;
            entry.index = index;
            entry.term = term;
            entry.type = LogEntryType::COMMAND;
            entry.payload.assign(payload.begin(), payload.end());
            return entry;
        }

        std::vector<LogEntry> MakeEntries(LogIndex start,
                                          std::initializer_list<Term> terms)
        {
            std::vector<LogEntry> entries;
            LogIndex index = start;
            for (Term term : terms)
            {
                entries.push_back(MakeEntry(index, term, "e" + std::to_string(index)));
                ++index;
            }
            return entries;
        }

        RaftCore::Options MakeOptions(
            NodeId node_id = 1,
            RaftRole role = RaftRole::FOLLOWER,
            Term term = 0,
            std::vector<NodeId> voters = {1, 2, 3},
            std::vector<NodeId> learners = {})
        {
            RaftCore::Options options;
            options.node_id = node_id;
            options.initial_role = role;
            options.election_timeout_ticks = 3;
            options.hard_state.current_term = term;
            options.hard_state.voted_for = role == RaftRole::CANDIDATE ? node_id
                                                                        : common::kInvalidNodeId;
            options.voter_ids = std::move(voters);
            options.learner_ids = std::move(learners);
            return options;
        }

        RaftCore MakeCore(const RaftCore::Options &options = MakeOptions())
        {
            RaftCore core;
            EXPECT_TRUE(core.Initialize(options).ok());
            return core;
        }

        RaftCore MakeLeader(Term term = 1,
                            std::vector<NodeId> voters = {1, 2, 3},
                            std::vector<NodeId> learners = {})
        {
            RaftCore core = MakeCore(MakeOptions(1,
                                                 RaftRole::CANDIDATE,
                                                 term,
                                                 std::move(voters),
                                                 std::move(learners)));
            EXPECT_TRUE(core.BecomeLeader().ok());
            return core;
        }

        void AppendAsLeader(RaftCore *core,
                            const std::vector<LogEntry> &entries,
                            LogIndex leader_commit = common::kInvalidLogIndex)
        {
            ASSERT_NE(core, nullptr);
            const LogIndex prev_index = entries.empty() ? core->log().last_index()
                                                        : entries.front().index - 1;
            Term prev_term = common::kInitialTerm;
            ASSERT_TRUE(core->log().GetTerm(prev_index, &prev_term).ok());

            AppendEntriesRequest request;
            request.term = core->current_term();
            request.leader_id = core->node_id();
            request.prev_log_index = prev_index;
            request.prev_log_term = prev_term;
            request.entries = entries;
            request.leader_commit = leader_commit;

            AppendEntriesResponse response;
            ASSERT_TRUE(core->HandleAppendEntries(request, &response).ok());
            ASSERT_TRUE(response.success);
        }

        AppendEntriesResponse SuccessAppendResponse(Term term, LogIndex match_index)
        {
            AppendEntriesResponse response;
            response.term = term;
            response.success = true;
            response.match_index = match_index;
            return response;
        }

        AppendEntriesResponse ConflictAppendResponse(Term term,
                                                     LogIndex conflict_index,
                                                     Term conflict_term = common::kInitialTerm)
        {
            AppendEntriesResponse response;
            response.term = term;
            response.success = false;
            response.conflict_index = conflict_index;
            response.conflict_term = conflict_term;
            return response;
        }

        bool HasMessage(const std::vector<RaftMessage> &messages,
                        RaftMessageType type,
                        NodeId target)
        {
            return std::any_of(messages.begin(),
                               messages.end(),
                               [type, target](const RaftMessage &message)
                               {
                                   return message.type == type &&
                                          message.target_node_id == target;
                               });
        }

        TEST(RaftCoreTest, InitializationAndLogicalTicksDriveDeterministicElections)
        {
            RaftCore core = MakeCore();
            RaftCore::TickAction action = RaftCore::TickAction::START_ELECTION;

            EXPECT_EQ(core.role(), RaftRole::FOLLOWER);
            EXPECT_EQ(core.current_term(), 0U);
            EXPECT_EQ(core.voted_for(), common::kInvalidNodeId);
            EXPECT_EQ(core.leader_id(), common::kInvalidNodeId);

            ASSERT_TRUE(core.Tick(&action).ok());
            EXPECT_EQ(action, RaftCore::TickAction::NONE);
            ASSERT_TRUE(core.Tick(&action).ok());
            EXPECT_EQ(action, RaftCore::TickAction::NONE);
            ASSERT_TRUE(core.Tick(&action).ok());
            EXPECT_EQ(action, RaftCore::TickAction::START_ELECTION);
            EXPECT_EQ(core.role(), RaftRole::CANDIDATE);
            EXPECT_EQ(core.current_term(), 1U);
            EXPECT_EQ(core.voted_for(), 1U);

            ASSERT_TRUE(core.Tick(&action).ok());
            ASSERT_TRUE(core.Tick(&action).ok());
            ASSERT_TRUE(core.Tick(&action).ok());
            EXPECT_EQ(action, RaftCore::TickAction::START_ELECTION);
            EXPECT_EQ(core.current_term(), 2U);

            ASSERT_TRUE(core.BecomeLeader().ok());
            ASSERT_TRUE(core.Tick(&action).ok());
            EXPECT_EQ(action, RaftCore::TickAction::NONE);
            EXPECT_EQ(core.role(), RaftRole::LEADER);

            RaftCore learner = MakeCore(MakeOptions(4, RaftRole::LEARNER, 0, {1, 2, 3}, {4}));
            for (int i = 0; i < 5; ++i)
            {
                ASSERT_TRUE(learner.Tick(&action).ok());
                EXPECT_EQ(action, RaftCore::TickAction::NONE);
            }
            EXPECT_EQ(learner.role(), RaftRole::LEARNER);
        }

        TEST(RaftCoreTest, RoleAndTermTransitionsRejectInvalidMovesAtomically)
        {
            RaftCore core = MakeCore();

            ASSERT_TRUE(core.StartElection().ok());
            ASSERT_TRUE(core.BecomeLeader().ok());
            EXPECT_EQ(core.role(), RaftRole::LEADER);

            ASSERT_TRUE(core.BecomeFollower(3, 2).ok());
            EXPECT_EQ(core.role(), RaftRole::FOLLOWER);
            EXPECT_EQ(core.current_term(), 3U);
            EXPECT_EQ(core.voted_for(), common::kInvalidNodeId);
            EXPECT_EQ(core.leader_id(), 2U);

            const Term term = core.current_term();
            const RaftRole role = core.role();
            EXPECT_EQ(core.BecomeFollower(2, 2).code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(core.current_term(), term);
            EXPECT_EQ(core.role(), role);

            RaftCore learner = MakeCore(MakeOptions(4, RaftRole::LEARNER, 0, {1, 2, 3}, {4}));
            EXPECT_EQ(learner.StartElection().code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(learner.BecomeLeader().code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(learner.role(), RaftRole::LEARNER);
        }

        TEST(RaftCoreTest, RequestVoteHonorsTermLogVoteAndLearnerRules)
        {
            RaftCore core = MakeCore(MakeOptions(1, RaftRole::FOLLOWER, 2));
            RequestVoteResponse response;

            RequestVoteRequest stale;
            stale.term = 1;
            stale.candidate_id = 2;
            ASSERT_TRUE(core.HandleRequestVote(stale, &response).ok());
            EXPECT_FALSE(response.vote_granted);
            EXPECT_EQ(response.reject_reason, VoteRejectReason::STALE_TERM);

            RequestVoteRequest higher;
            higher.term = 3;
            higher.candidate_id = 2;
            higher.last_log_index = 0;
            higher.last_log_term = 0;
            ASSERT_TRUE(core.HandleRequestVote(higher, &response).ok());
            EXPECT_TRUE(response.vote_granted);
            EXPECT_EQ(core.role(), RaftRole::FOLLOWER);
            EXPECT_EQ(core.current_term(), 3U);
            EXPECT_EQ(core.voted_for(), 2U);

            ASSERT_TRUE(core.HandleRequestVote(higher, &response).ok());
            EXPECT_TRUE(response.vote_granted);

            higher.candidate_id = 3;
            ASSERT_TRUE(core.HandleRequestVote(higher, &response).ok());
            EXPECT_FALSE(response.vote_granted);
            EXPECT_EQ(response.reject_reason, VoteRejectReason::ALREADY_VOTED);

            RaftCore log_core = MakeCore();
            AppendEntriesRequest append;
            append.term = 1;
            append.leader_id = 2;
            append.entries = MakeEntries(1, {2});
            AppendEntriesResponse append_response;
            ASSERT_TRUE(log_core.HandleAppendEntries(append, &append_response).ok());

            RequestVoteRequest outdated;
            outdated.term = 3;
            outdated.candidate_id = 3;
            outdated.last_log_index = 0;
            outdated.last_log_term = 0;
            ASSERT_TRUE(log_core.HandleRequestVote(outdated, &response).ok());
            EXPECT_FALSE(response.vote_granted);
            EXPECT_EQ(response.reject_reason, VoteRejectReason::LOG_OUTDATED);

            outdated.last_log_index = 1;
            outdated.last_log_term = 2;
            ASSERT_TRUE(log_core.HandleRequestVote(outdated, &response).ok());
            EXPECT_TRUE(response.vote_granted);

            RaftCore learner = MakeCore(MakeOptions(4, RaftRole::LEARNER, 0, {1, 2, 3}, {4}));
            outdated.term = 1;
            outdated.candidate_id = 1;
            ASSERT_TRUE(learner.HandleRequestVote(outdated, &response).ok());
            EXPECT_FALSE(response.vote_granted);
            EXPECT_EQ(response.reject_reason, VoteRejectReason::LEARNER);
        }

        TEST(RaftCoreTest, HigherTermVoteResponseStepsCandidateDown)
        {
            RaftCore core = MakeCore();
            RaftCore::VoteResponseAction action = RaftCore::VoteResponseAction::NONE;
            RequestVoteResponse response;

            ASSERT_TRUE(core.StartElection().ok());
            response.term = core.current_term() + 1;
            response.vote_granted = false;
            response.reject_reason = VoteRejectReason::STALE_TERM;

            ASSERT_TRUE(core.HandleRequestVoteResponse(2, response, &action).ok());
            EXPECT_EQ(action, RaftCore::VoteResponseAction::STEPPED_DOWN);
            EXPECT_EQ(core.role(), RaftRole::FOLLOWER);
            EXPECT_EQ(core.current_term(), 2U);
        }

        TEST(RaftCoreTest, HeartbeatBuildAndHandlingUseAppendEntriesSemantics)
        {
            RaftCore leader = MakeLeader();
            AppendEntriesRequest heartbeat;
            ASSERT_TRUE(leader.BuildHeartbeat(&heartbeat).ok());
            EXPECT_TRUE(heartbeat.entries.empty());
            EXPECT_EQ(heartbeat.term, leader.current_term());
            EXPECT_EQ(heartbeat.leader_id, leader.node_id());

            RaftCore follower = MakeCore();
            RaftCore::TickAction tick_action = RaftCore::TickAction::NONE;
            ASSERT_TRUE(follower.Tick(&tick_action).ok());
            ASSERT_TRUE(follower.Tick(&tick_action).ok());
            AppendEntriesResponse response;
            ASSERT_TRUE(follower.HandleAppendEntries(heartbeat, &response).ok());
            EXPECT_TRUE(response.success);
            EXPECT_EQ(follower.leader_id(), leader.node_id());
            EXPECT_EQ(follower.election_elapsed_ticks(), 0U);

            heartbeat.term = 0;
            ASSERT_TRUE(follower.HandleAppendEntries(heartbeat, &response).ok());
            EXPECT_FALSE(response.success);

            RaftCore candidate = MakeCore();
            ASSERT_TRUE(candidate.StartElection().ok());
            heartbeat.term = 2;
            heartbeat.prev_log_index = 0;
            heartbeat.prev_log_term = 0;
            ASSERT_TRUE(candidate.HandleAppendEntries(heartbeat, &response).ok());
            EXPECT_TRUE(response.success);
            EXPECT_EQ(candidate.role(), RaftRole::FOLLOWER);
            EXPECT_EQ(candidate.current_term(), 2U);

            heartbeat.prev_log_index = 5;
            heartbeat.prev_log_term = 1;
            ASSERT_TRUE(candidate.HandleAppendEntries(heartbeat, &response).ok());
            EXPECT_FALSE(response.success);
            EXPECT_EQ(response.conflict_index, candidate.log().last_index() + 1);
        }

        TEST(RaftCoreTest, AppendEntriesHandlesDuplicatesConflictsCommitsAndTerms)
        {
            RaftCore core = MakeCore();
            AppendEntriesRequest request;
            AppendEntriesResponse response;

            request.term = 1;
            request.leader_id = 2;
            request.entries = MakeEntries(1, {1, 1});
            request.leader_commit = 1;
            ASSERT_TRUE(core.HandleAppendEntries(request, &response).ok());
            EXPECT_TRUE(response.success);
            EXPECT_EQ(core.log().last_index(), 2U);
            EXPECT_EQ(core.log().commit_index(), 1U);

            ASSERT_TRUE(core.HandleAppendEntries(request, &response).ok());
            EXPECT_TRUE(response.success);
            EXPECT_EQ(core.log().size(), 2U);

            request.prev_log_index = 1;
            request.prev_log_term = 1;
            request.entries = MakeEntries(2, {2});
            request.leader_commit = 1;
            ASSERT_TRUE(core.HandleAppendEntries(request, &response).ok());
            EXPECT_TRUE(response.success);
            Term term = 0;
            ASSERT_TRUE(core.log().GetTerm(2, &term).ok());
            EXPECT_EQ(term, 2U);

            request.entries.clear();
            request.leader_commit = 99;
            ASSERT_TRUE(core.HandleAppendEntries(request, &response).ok());
            EXPECT_EQ(core.log().commit_index(), 2U);

            request.entries = MakeEntries(2, {3});
            EXPECT_EQ(core.HandleAppendEntries(request, &response).code(),
                      common::StatusCode::kInvalidArgument);
            ASSERT_TRUE(core.log().GetTerm(2, &term).ok());
            EXPECT_EQ(term, 2U);

            RaftCore stale_core = MakeCore(MakeOptions(1, RaftRole::FOLLOWER, 3));
            request.term = 2;
            request.leader_id = 2;
            request.prev_log_index = 0;
            request.prev_log_term = 0;
            request.entries.clear();
            ASSERT_TRUE(stale_core.HandleAppendEntries(request, &response).ok());
            EXPECT_FALSE(response.success);

            request.term = 4;
            ASSERT_TRUE(stale_core.HandleAppendEntries(request, &response).ok());
            EXPECT_TRUE(response.success);
            EXPECT_EQ(stale_core.current_term(), 4U);
            EXPECT_EQ(stale_core.role(), RaftRole::FOLLOWER);
        }

        TEST(RaftCoreTest, LeaderPeerProgressBackoffAndCatchupAreDeterministic)
        {
            RaftCore leader = MakeLeader(2, {1, 2, 3}, {4});
            AppendAsLeader(&leader, MakeEntries(1, {1, 2, 2}));

            PeerProgress progress;
            ASSERT_TRUE(leader.GetPeerProgress(1, &progress).ok());
            EXPECT_EQ(progress.match_index, 3U);
            EXPECT_EQ(progress.next_index, 4U);
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_EQ(progress.match_index, 0U);
            EXPECT_EQ(progress.next_index, 1U);
            ASSERT_TRUE(leader.GetPeerProgress(4, &progress).ok());
            EXPECT_TRUE(progress.is_learner);

            AppendEntriesRequest request;
            ASSERT_TRUE(leader.BuildAppendEntriesForPeer(2, &request).ok());
            EXPECT_EQ(request.prev_log_index, 0U);
            ASSERT_EQ(request.entries.size(), 3U);

            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2,
                                                           SuccessAppendResponse(2, 1),
                                                           &action)
                            .ok());
            EXPECT_EQ(action, RaftCore::AppendResponseAction::ACCEPTED);
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_EQ(progress.match_index, 1U);
            EXPECT_EQ(progress.next_index, 2U);

            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2,
                                                           SuccessAppendResponse(2, 1),
                                                           &action)
                            .ok());
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_EQ(progress.match_index, 1U);
            EXPECT_EQ(progress.next_index, 2U);

            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2,
                                                           SuccessAppendResponse(2, 3),
                                                           &action)
                            .ok());
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_EQ(progress.next_index, 4U);

            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2,
                                                           ConflictAppendResponse(2, 1, 1),
                                                           &action)
                            .ok());
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_EQ(action, RaftCore::AppendResponseAction::REJECTED);
            EXPECT_EQ(progress.next_index, 2U);

            ASSERT_TRUE(leader.BuildAppendEntriesForPeer(2, &request).ok());
            EXPECT_EQ(request.prev_log_index, 1U);
            ASSERT_EQ(request.entries.size(), 2U);
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2,
                                                           SuccessAppendResponse(2, 3),
                                                           &action)
                            .ok());
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_EQ(progress.match_index, 3U);
            EXPECT_EQ(progress.next_index, 4U);

            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2,
                                                           SuccessAppendResponse(3, 3),
                                                           &action)
                            .ok());
            EXPECT_EQ(action, RaftCore::AppendResponseAction::STEPPED_DOWN);
            EXPECT_EQ(leader.role(), RaftRole::FOLLOWER);
        }

        TEST(RaftCoreTest, MajorityCommitCountsOnlyVotersAndCurrentTermEntries)
        {
            RaftCore leader = MakeLeader(2, {1, 2, 3}, {4});
            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;

            AppendAsLeader(&leader, MakeEntries(1, {2}));
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(4,
                                                           SuccessAppendResponse(2, 1),
                                                           &action)
                            .ok());
            EXPECT_EQ(leader.log().commit_index(), 0U);
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2,
                                                           SuccessAppendResponse(2, 1),
                                                           &action)
                            .ok());
            EXPECT_EQ(leader.log().commit_index(), 1U);

            RaftCore old_term_leader = MakeLeader(2);
            AppendAsLeader(&old_term_leader, MakeEntries(1, {1}));
            ASSERT_TRUE(old_term_leader.HandleAppendEntriesResponse(2,
                                                                    SuccessAppendResponse(2, 1),
                                                                    &action)
                            .ok());
            EXPECT_EQ(old_term_leader.log().commit_index(), 0U);

            RaftCore five_voters = MakeLeader(2, {1, 2, 3, 4, 5});
            AppendAsLeader(&five_voters, MakeEntries(1, {2}));
            ASSERT_TRUE(five_voters.HandleAppendEntriesResponse(2,
                                                                SuccessAppendResponse(2, 1),
                                                                &action)
                            .ok());
            EXPECT_EQ(five_voters.log().commit_index(), 0U);
            ASSERT_TRUE(five_voters.HandleAppendEntriesResponse(3,
                                                                SuccessAppendResponse(2, 1),
                                                                &action)
                            .ok());
            EXPECT_EQ(five_voters.log().commit_index(), 1U);
        }

        TEST(RaftCoreTest, ApplyReadyAndOutputBatchesReflectPersistenceGates)
        {
            RaftCore empty = MakeCore();
            RaftCore::ApplyRange range;
            ASSERT_TRUE(empty.GetApplyReadyRange(&range).ok());
            EXPECT_TRUE(range.empty());

            RaftCore voter = MakeCore();
            RequestVoteRequest vote;
            vote.term = 1;
            vote.candidate_id = 2;
            RequestVoteResponse vote_response;
            ASSERT_TRUE(voter.HandleRequestVote(vote, &vote_response).ok());

            RaftCore::RaftOutput output;
            ASSERT_TRUE(voter.GetOutput(&output).ok());
            EXPECT_TRUE(output.has_hard_state);
            EXPECT_TRUE(output.immediate_messages.empty());
            EXPECT_TRUE(HasMessage(output.persisted_messages,
                                   RaftMessageType::REQUEST_VOTE_RESPONSE,
                                   2));
            ASSERT_TRUE(voter.ConfirmPersistence(true, 0).ok());
            ASSERT_TRUE(voter.GetOutput(&output).ok());
            EXPECT_FALSE(output.has_hard_state);
            EXPECT_TRUE(HasMessage(output.immediate_messages,
                                   RaftMessageType::REQUEST_VOTE_RESPONSE,
                                   2));

            RaftCore rejecter = MakeCore(MakeOptions(1, RaftRole::FOLLOWER, 2));
            vote.term = 1;
            ASSERT_TRUE(rejecter.HandleRequestVote(vote, &vote_response).ok());
            ASSERT_TRUE(rejecter.GetOutput(&output).ok());
            EXPECT_TRUE(HasMessage(output.immediate_messages,
                                   RaftMessageType::REQUEST_VOTE_RESPONSE,
                                   2));

            RaftCore follower = MakeCore();
            AppendEntriesRequest append;
            append.term = 1;
            append.leader_id = 2;
            append.entries = MakeEntries(1, {1});
            append.leader_commit = 1;
            AppendEntriesResponse append_response;
            ASSERT_TRUE(follower.HandleAppendEntries(append, &append_response).ok());
            ASSERT_TRUE(follower.GetOutput(&output).ok());
            EXPECT_TRUE(output.has_hard_state);
            ASSERT_EQ(output.unstable_entries.size(), 1U);
            EXPECT_EQ(output.unstable_entries[0].index, 1U);
            EXPECT_TRUE(HasMessage(output.persisted_messages,
                                   RaftMessageType::APPEND_ENTRIES_RESPONSE,
                                   2));
            EXPECT_FALSE(HasMessage(output.immediate_messages,
                                    RaftMessageType::APPEND_ENTRIES_RESPONSE,
                                    2));

            ASSERT_TRUE(follower.ConfirmPersistence(false, 1).ok());
            ASSERT_TRUE(follower.GetOutput(&output).ok());
            EXPECT_TRUE(output.unstable_entries.empty());
            EXPECT_TRUE(HasMessage(output.persisted_messages,
                                   RaftMessageType::APPEND_ENTRIES_RESPONSE,
                                   2));

            ASSERT_TRUE(follower.ConfirmPersistence(true, 1).ok());
            ASSERT_TRUE(follower.GetOutput(&output).ok());
            EXPECT_FALSE(output.has_hard_state);
            EXPECT_TRUE(HasMessage(output.immediate_messages,
                                   RaftMessageType::APPEND_ENTRIES_RESPONSE,
                                   2));
            EXPECT_FALSE(output.committed_range.empty());
            EXPECT_EQ(output.committed_range.begin, 1U);
            EXPECT_EQ(output.committed_range.end, 1U);

            const Term term = follower.current_term();
            const RaftRole role = follower.role();
            const LogIndex last_index = follower.log().last_index();
            ASSERT_TRUE(follower.GetOutput(&output).ok());
            EXPECT_EQ(follower.current_term(), term);
            EXPECT_EQ(follower.role(), role);
            EXPECT_EQ(follower.log().last_index(), last_index);
        }

    } // namespace
} // namespace cpr::raft
