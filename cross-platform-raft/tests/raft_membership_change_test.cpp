#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "common/types.h"
#include "raft/raft_core.h"

namespace cpr::raft
{
    namespace
    {

        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::StatusCode;

        RaftMember MakeMember(NodeId node_id, const std::string &host)
        {
            return RaftMember{node_id, {host, static_cast<std::uint16_t>(9000 + node_id)}};
        }

        MembershipView MakeMembership(std::uint64_t configuration_id,
                                      std::vector<RaftMember> voters,
                                      std::vector<RaftMember> learners = {},
                                      bool active_transition = false)
        {
            MembershipView view;
            view.configuration_id = configuration_id;
            view.voters = std::move(voters);
            view.learners = std::move(learners);
            view.has_active_transition = active_transition;
            return view;
        }

        RaftCore::Options MakeOptions(NodeId node_id,
                                      RaftRole role,
                                      std::vector<NodeId> voters,
                                      std::vector<NodeId> learners,
                                      const MembershipView &membership)
        {
            RaftCore::Options options;
            options.node_id = node_id;
            options.initial_role = role == RaftRole::LEADER ? RaftRole::CANDIDATE : role;
            options.election_timeout_ticks = 3;
            options.hard_state.current_term =
                role == RaftRole::LEADER ? 1 : common::kInitialTerm;
            options.voter_ids = std::move(voters);
            options.learner_ids = std::move(learners);
            options.membership = membership;
            return options;
        }

        RaftCore MakeCore(const RaftCore::Options &options)
        {
            RaftCore core;
            EXPECT_TRUE(core.Initialize(options).ok());
            if (options.initial_role == RaftRole::CANDIDATE)
            {
                EXPECT_TRUE(core.BecomeLeader().ok());
            }
            return core;
        }

        LogEntry GetEntry(const RaftCore &core, LogIndex index)
        {
            LogEntry entry;
            EXPECT_TRUE(core.log().GetEntry(index, &entry).ok());
            return entry;
        }

        void MarkPeerActive(RaftCore *core, NodeId peer_id)
        {
            ASSERT_NE(core, nullptr);
            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            AppendEntriesResponse response;
            response.term = core->current_term();
            response.success = true;
            response.match_index = core->log().last_index();
            ASSERT_TRUE(core->HandleAppendEntriesResponse(peer_id, response, &action).ok());
        }

        void CommitLearnerAdd(RaftCore *core,
                              NodeId learner_id,
                              const std::string &host = "10.0.0.2")
        {
            ASSERT_NE(core, nullptr);
            ASSERT_TRUE(core->AddLearner(MakeMember(learner_id, host)).ok());
            const LogIndex membership_index = core->log().last_index();
            for (NodeId voter_id : core->voter_ids())
            {
                if (voter_id != core->node_id())
                {
                    MarkPeerActive(core, voter_id);
                }
            }
            ASSERT_TRUE(core->ApplyCommittedMembershipEntry(
                GetEntry(*core, membership_index)).ok());
            ASSERT_TRUE(core->ConfirmApplied(membership_index).ok());
        }

        void CatchUpLearner(RaftCore *core, NodeId learner_id)
        {
            ASSERT_NE(core, nullptr);
            ASSERT_TRUE(core->Propose(OpaquePayload{'x'}).ok());
            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            AppendEntriesResponse response;
            response.term = core->current_term();
            response.success = true;
            response.match_index = core->log().last_index();
            ASSERT_TRUE(core->HandleAppendEntriesResponse(learner_id, response, &action).ok());
        }

        TEST(RaftMembershipChangeTest, LeaderPromotesHealthyCaughtUpLearnerViaMembershipLog)
        {
            RaftCore leader = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1},
                                                   {},
                                                   MakeMembership(1, {MakeMember(1, "10.0.0.1")})));
            CommitLearnerAdd(&leader, 2);
            CatchUpLearner(&leader, 2);
            ASSERT_TRUE(leader.ConfirmApplied(2).ok());

            LogIndex proposal_index = 0;
            ASSERT_TRUE(leader.PromoteLearner(2, &proposal_index).ok());
            EXPECT_EQ(GetEntry(leader, proposal_index).type, LogEntryType::MEMBERSHIP_CHANGE);
            MarkPeerActive(&leader, 2);

            MembershipView before_apply;
            ASSERT_TRUE(leader.GetMembershipView(&before_apply).ok());
            EXPECT_FALSE(std::any_of(before_apply.voters.begin(),
                                     before_apply.voters.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 2; }));
            EXPECT_TRUE(std::any_of(before_apply.learners.begin(),
                                    before_apply.learners.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 2; }));

            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, proposal_index)).ok());
            ASSERT_TRUE(leader.ConfirmApplied(proposal_index).ok());

            MembershipView after_apply;
            ASSERT_TRUE(leader.GetMembershipView(&after_apply).ok());
            EXPECT_TRUE(after_apply.has_active_transition);
            EXPECT_TRUE(std::any_of(after_apply.learners.begin(),
                                     after_apply.learners.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 2; }));
            EXPECT_TRUE(std::any_of(after_apply.next_voters.begin(),
                                    after_apply.next_voters.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 2; }));

            LogIndex final_index = 0;
            ASSERT_TRUE(leader.FinalizeMembershipChange(&final_index).ok());
            MarkPeerActive(&leader, 2);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, final_index)).ok());
            ASSERT_TRUE(leader.ConfirmApplied(final_index).ok());

            MembershipView final_view;
            ASSERT_TRUE(leader.GetMembershipView(&final_view).ok());
            EXPECT_TRUE(std::any_of(final_view.voters.begin(),
                                    final_view.voters.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 2; }));
            EXPECT_FALSE(std::any_of(final_view.learners.begin(),
                                     final_view.learners.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 2; }));

            PeerProgress progress;
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_FALSE(progress.is_learner);
        }

        TEST(RaftMembershipChangeTest, PromoteLearnerRejectsNonLeaderAndInvalidTargets)
        {
            const MembershipView membership = MakeMembership(
                2,
                {MakeMember(1, "10.0.0.1")},
                {MakeMember(2, "10.0.0.2")});

            RaftCore follower = MakeCore(MakeOptions(1, RaftRole::FOLLOWER, {1}, {2}, membership));
            RaftCore learner = MakeCore(MakeOptions(2, RaftRole::LEARNER, {1}, {2}, membership));
            RaftCore candidate;
            ASSERT_TRUE(candidate.Initialize(MakeOptions(1, RaftRole::CANDIDATE, {1}, {2}, membership)).ok());

            EXPECT_EQ(follower.PromoteLearner(2).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(learner.PromoteLearner(2).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(candidate.PromoteLearner(2).code(), StatusCode::kInvalidArgument);

            RaftCore leader = MakeCore(MakeOptions(1, RaftRole::LEADER, {1}, {2}, membership));
            EXPECT_EQ(leader.PromoteLearner(common::kInvalidNodeId).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(leader.PromoteLearner(9).code(), StatusCode::kNotFound);
            EXPECT_EQ(leader.PromoteLearner(1).code(), StatusCode::kInvalidArgument);
        }

        TEST(RaftMembershipChangeTest, PromoteLearnerRejectsLaggingUnhealthyAndSnapshotRequiredLearners)
        {
            RaftCore leader = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1},
                                                   {2},
                                                   MakeMembership(2,
                                                                  {MakeMember(1, "10.0.0.1")},
                                                                  {MakeMember(2, "10.0.0.2")})));

            EXPECT_EQ(leader.PromoteLearner(2).code(), StatusCode::kBusy);

            CatchUpLearner(&leader, 2);
            SnapshotMetadata metadata;
            metadata.last_included_index = 1;
            metadata.last_included_term = 1;
            metadata.membership = MakeMembership(2,
                                                 {MakeMember(1, "10.0.0.1")},
                                                 {MakeMember(2, "10.0.0.2")});
            ASSERT_TRUE(leader.ConfirmApplied(1).ok());
            ASSERT_TRUE(leader.UpdateSnapshotBoundary(metadata).ok());

            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            AppendEntriesResponse reject;
            reject.term = leader.current_term();
            reject.success = false;
            reject.conflict_index = 1;
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(2, reject, &action).ok());

            EXPECT_EQ(leader.PromoteLearner(2).code(), StatusCode::kBusy);
        }

        TEST(RaftMembershipChangeTest, LeaderRemovesLearnerAndVoterThroughMembershipLog)
        {
            RaftCore leader = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1, 3},
                                                   {},
                                                   MakeMembership(1,
                                                                  {MakeMember(1, "10.0.0.1"),
                                                                   MakeMember(3, "10.0.0.3")})));
            MarkPeerActive(&leader, 3);
            CommitLearnerAdd(&leader, 2);

            LogIndex remove_learner_index = 0;
            ASSERT_TRUE(leader.RemoveMember(2, &remove_learner_index).ok());
            MarkPeerActive(&leader, 3);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, remove_learner_index)).ok());
            ASSERT_TRUE(leader.ConfirmApplied(remove_learner_index).ok());
            PeerProgress progress;
            EXPECT_EQ(leader.GetPeerProgress(2, &progress).code(), StatusCode::kNotFound);

            LogIndex remove_voter_index = 0;
            ASSERT_TRUE(leader.RemoveMember(3, &remove_voter_index).ok());
            MarkPeerActive(&leader, 3);

            MembershipView before_apply;
            ASSERT_TRUE(leader.GetMembershipView(&before_apply).ok());
            EXPECT_TRUE(std::any_of(before_apply.voters.begin(),
                                    before_apply.voters.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 3; }));

            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, remove_voter_index)).ok());
            ASSERT_TRUE(leader.ConfirmApplied(remove_voter_index).ok());
            MembershipView after_apply;
            ASSERT_TRUE(leader.GetMembershipView(&after_apply).ok());
            EXPECT_TRUE(after_apply.has_active_transition);
            EXPECT_FALSE(std::any_of(after_apply.next_voters.begin(),
                                     after_apply.next_voters.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 3; }));

            LogIndex final_index = 0;
            ASSERT_TRUE(leader.FinalizeMembershipChange(&final_index).ok());
            MarkPeerActive(&leader, 3);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, final_index)).ok());
            ASSERT_TRUE(leader.ConfirmApplied(final_index).ok());
            MembershipView final_view;
            ASSERT_TRUE(leader.GetMembershipView(&final_view).ok());
            EXPECT_FALSE(std::any_of(final_view.voters.begin(),
                                     final_view.voters.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 3; }));
            EXPECT_EQ(leader.GetPeerProgress(3, &progress).code(), StatusCode::kNotFound);
        }

        TEST(RaftMembershipChangeTest, RemoveMemberRejectsInvalidTargetsLastVoterAndNoQuorum)
        {
            RaftCore single = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1},
                                                   {},
                                                   MakeMembership(1, {MakeMember(1, "10.0.0.1")})));
            EXPECT_EQ(single.RemoveMember(common::kInvalidNodeId).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(single.RemoveMember(9).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(single.RemoveMember(1).code(), StatusCode::kInvalidArgument);

            RaftCore no_quorum = MakeCore(MakeOptions(1,
                                                      RaftRole::LEADER,
                                                      {1, 3, 4},
                                                      {2},
                                                      MakeMembership(2,
                                                                     {MakeMember(1, "10.0.0.1"),
                                                                      MakeMember(3, "10.0.0.3"),
                                                                      MakeMember(4, "10.0.0.4")},
                                                                     {MakeMember(2, "10.0.0.2")})));
            CatchUpLearner(&no_quorum, 2);
            EXPECT_EQ(no_quorum.RemoveMember(3).code(), StatusCode::kBusy);
            EXPECT_EQ(no_quorum.PromoteLearner(2).code(), StatusCode::kBusy);
        }

        TEST(RaftMembershipChangeTest, OnlyOneMembershipChangeCanBePendingAndFailedRequestsDoNotOccupySlot)
        {
            RaftCore leader = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1},
                                                   {},
                                                   MakeMembership(1, {MakeMember(1, "10.0.0.1")})));

            EXPECT_EQ(leader.AddLearner(RaftMember{}).code(), StatusCode::kInvalidArgument);
            ASSERT_TRUE(leader.AddLearner(MakeMember(2, "10.0.0.2")).ok());
            EXPECT_EQ(leader.RemoveMember(1).code(), StatusCode::kBusy);
            EXPECT_EQ(leader.PromoteLearner(2).code(), StatusCode::kBusy);
        }

        TEST(RaftMembershipChangeTest, PromoteRejectsWhenOldConfigQuorumCountsOnlyCommittedVoters)
        {
            RaftCore leader = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1, 3, 4},
                                                   {2},
                                                   MakeMembership(2,
                                                                  {MakeMember(1, "10.0.0.1"),
                                                                   MakeMember(3, "10.0.0.3"),
                                                                   MakeMember(4, "10.0.0.4")},
                                                                  {MakeMember(2, "10.0.0.2")})));
            CatchUpLearner(&leader, 2);
            EXPECT_EQ(leader.PromoteLearner(2).code(), StatusCode::kBusy);

            MarkPeerActive(&leader, 3);
            ASSERT_TRUE(leader.PromoteLearner(2).ok());
        }

        TEST(RaftMembershipChangeTest, RepeatedCommittedApplyIsIdempotentAndConflictingContentRejected)
        {
            RaftCore leader = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1},
                                                   {2},
                                                   MakeMembership(2,
                                                                  {MakeMember(1, "10.0.0.1")},
                                                                  {MakeMember(2, "10.0.0.2")})));
            CatchUpLearner(&leader, 2);
            LogIndex index = 0;
            ASSERT_TRUE(leader.PromoteLearner(2, &index).ok());
            const LogEntry promote_entry = GetEntry(leader, index);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(promote_entry).ok());
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(promote_entry).ok());

            MembershipLogEntry conflicting;
            ASSERT_TRUE(DecodeMembershipLogEntry(promote_entry.payload, &conflicting).ok());
            conflicting.learners.push_back(MakeMember(9, "10.0.0.9"));
            OpaquePayload payload;
            ASSERT_TRUE(EncodeMembershipLogEntry(conflicting, &payload).ok());

            LogEntry conflict_entry = promote_entry;
            conflict_entry.payload = std::move(payload);
            EXPECT_EQ(leader.ApplyCommittedMembershipEntry(conflict_entry).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST(RaftMembershipChangeTest, RemovingCurrentLeaderDemotesLocalNodeSafelyAfterCommit)
        {
            RaftCore leader = MakeCore(MakeOptions(1,
                                                   RaftRole::LEADER,
                                                   {1, 2},
                                                   {},
                                                   MakeMembership(1,
                                                                  {MakeMember(1, "10.0.0.1"),
                                                                   MakeMember(2, "10.0.0.2")})));
            MarkPeerActive(&leader, 2);

            LogIndex index = 0;
            ASSERT_TRUE(leader.RemoveMember(1, &index).ok());
            MarkPeerActive(&leader, 2);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, index)).ok());
            ASSERT_TRUE(leader.ConfirmApplied(index).ok());

            EXPECT_NE(leader.role(), RaftRole::LEARNER);

            LogIndex final_index = 0;
            ASSERT_TRUE(leader.FinalizeMembershipChange(&final_index).ok());
            MarkPeerActive(&leader, 2);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, final_index)).ok());
            ASSERT_TRUE(leader.ConfirmApplied(final_index).ok());

            EXPECT_EQ(leader.role(), RaftRole::LEARNER);
            EXPECT_EQ(leader.leader_id(), common::kInvalidNodeId);
            EXPECT_TRUE(leader.voter_ids() == std::vector<NodeId>({2}));

            RequestVoteRequest vote_request;
            vote_request.term = leader.current_term() + 1;
            vote_request.candidate_id = 2;
            RequestVoteResponse vote_response;
            ASSERT_TRUE(leader.HandleRequestVote(vote_request, &vote_response).ok());
            EXPECT_FALSE(vote_response.vote_granted);
            EXPECT_EQ(vote_response.reject_reason, VoteRejectReason::LEARNER);
        }

    } // namespace
} // namespace cpr::raft
