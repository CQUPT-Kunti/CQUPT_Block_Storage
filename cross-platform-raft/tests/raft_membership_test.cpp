#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "common/types.h"
#include "raft/raft_core.h"
#include "raft/raft_membership.h"

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
                                      bool active_transition = false,
                                      std::vector<RaftMember> next_voters = {},
                                      std::vector<RaftMember> next_learners = {})
        {
            MembershipView view;
            view.configuration_id = configuration_id;
            view.voters = std::move(voters);
            view.learners = std::move(learners);
            view.has_active_transition = active_transition;
            view.next_voters = std::move(next_voters);
            view.next_learners = std::move(next_learners);
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
            options.election_timeout_ticks = 5;
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

        void AcceptReplication(RaftCore *core, NodeId peer_id, LogIndex match_index)
        {
            ASSERT_NE(core, nullptr);
            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            AppendEntriesResponse response;
            response.term = core->current_term();
            response.success = true;
            response.match_index = match_index;
            ASSERT_TRUE(core->HandleAppendEntriesResponse(peer_id, response, &action).ok());
        }

        void RejectReplication(RaftCore *core, NodeId peer_id, LogIndex conflict_index)
        {
            ASSERT_NE(core, nullptr);
            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            AppendEntriesResponse response;
            response.term = core->current_term();
            response.success = false;
            response.conflict_index = conflict_index;
            ASSERT_TRUE(core->HandleAppendEntriesResponse(peer_id, response, &action).ok());
        }

        void ApplyCommittedMembershipAt(RaftCore *core, LogIndex index)
        {
            ASSERT_NE(core, nullptr);
            ASSERT_TRUE(core->ApplyCommittedMembershipEntry(GetEntry(*core, index)).ok());
            ASSERT_TRUE(core->ConfirmApplied(index).ok());
        }

        void CommitLearnerAdd(RaftCore *core, NodeId learner_id, const std::string &host)
        {
            ASSERT_NE(core, nullptr);
            LogIndex index = 0;
            ASSERT_TRUE(core->AddLearner(MakeMember(learner_id, host), &index).ok());
            for (NodeId voter_id : core->voter_ids())
            {
                if (voter_id != core->node_id())
                {
                    AcceptReplication(core, voter_id, index);
                }
            }
            ApplyCommittedMembershipAt(core, index);
        }

        void CommitCommandAndCatchUpLearner(RaftCore *core, NodeId learner_id)
        {
            ASSERT_NE(core, nullptr);
            ASSERT_TRUE(core->Propose(OpaquePayload{'x'}).ok());
            const LogIndex index = core->log().last_index();
            for (NodeId voter_id : core->voter_ids())
            {
                if (voter_id != core->node_id())
                {
                    AcceptReplication(core, voter_id, index);
                }
            }
            AcceptReplication(core, learner_id, core->log().last_index());
            ASSERT_TRUE(core->ConfirmApplied(index).ok());
        }

        TEST(RaftMembershipTest, LearnerLifecycleUsesLogThenTwoPhasePromotion)
        {
            RaftCore leader = MakeCore(MakeOptions(
                1,
                RaftRole::LEADER,
                {1, 2, 3},
                {},
                MakeMembership(1,
                               {MakeMember(1, "10.0.0.1"),
                                MakeMember(2, "10.0.0.2"),
                                MakeMember(3, "10.0.0.3")})));

            AcceptReplication(&leader, 2, 0);
            AcceptReplication(&leader, 3, 0);
            CommitLearnerAdd(&leader, 4, "10.0.0.4");

            MembershipView learner_view;
            ASSERT_TRUE(leader.GetMembershipView(&learner_view).ok());
            MembershipState learner_state;
            ASSERT_TRUE(MembershipState::FromView(learner_view, &learner_state).ok());
            EXPECT_FALSE(learner_state.CanVote(4));
            EXPECT_FALSE(learner_state.CountsTowardQuorum(4));
            EXPECT_FALSE(learner_state.CanBecomeLeader(4));

            CommitCommandAndCatchUpLearner(&leader, 4);

            LogIndex phase1 = 0;
            ASSERT_TRUE(leader.PromoteLearner(4, &phase1).ok());
            AcceptReplication(&leader, 2, phase1);
            AcceptReplication(&leader, 3, phase1);
            AcceptReplication(&leader, 4, phase1);
            ApplyCommittedMembershipAt(&leader, phase1);

            MembershipView transition_view;
            ASSERT_TRUE(leader.GetMembershipView(&transition_view).ok());
            EXPECT_TRUE(transition_view.has_active_transition);
            MembershipState transition_state;
            ASSERT_TRUE(MembershipState::FromView(transition_view, &transition_state).ok());
            EXPECT_TRUE(transition_state.CountsTowardQuorum(4));
            EXPECT_TRUE(transition_state.CanVote(4));
            EXPECT_TRUE(transition_state.CanBecomeLeader(4));
            EXPECT_TRUE(std::any_of(transition_view.learners.begin(),
                                    transition_view.learners.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 4; }));

            LogIndex phase2 = 0;
            ASSERT_TRUE(leader.FinalizeMembershipChange(&phase2).ok());
            AcceptReplication(&leader, 2, phase2);
            AcceptReplication(&leader, 3, phase2);
            AcceptReplication(&leader, 4, phase2);
            ApplyCommittedMembershipAt(&leader, phase2);

            MembershipView stable_view;
            ASSERT_TRUE(leader.GetMembershipView(&stable_view).ok());
            EXPECT_FALSE(stable_view.has_active_transition);
            EXPECT_TRUE(std::any_of(stable_view.voters.begin(),
                                    stable_view.voters.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 4; }));
            EXPECT_FALSE(std::any_of(stable_view.learners.begin(),
                                     stable_view.learners.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 4; }));
            EXPECT_EQ(stable_view.configuration_id, 4U);
        }

        TEST(RaftMembershipTest, SnapshotCatchUpBlocksPromotionUntilLearnerIsHealthyAndCaughtUp)
        {
            RaftCore leader = MakeCore(MakeOptions(
                1,
                RaftRole::LEADER,
                {1},
                {},
                MakeMembership(1, {MakeMember(1, "10.0.0.1")})));

            CommitLearnerAdd(&leader, 4, "10.0.0.4");
            ASSERT_TRUE(leader.Propose(OpaquePayload{'a'}).ok());
            ASSERT_TRUE(leader.Propose(OpaquePayload{'b'}).ok());
            ASSERT_TRUE(leader.ConfirmApplied(2).ok());

            SnapshotMetadata metadata;
            metadata.last_included_index = 2;
            metadata.last_included_term = 1;
            ASSERT_TRUE(leader.GetMembershipView(&metadata.membership).ok());
            ASSERT_TRUE(leader.UpdateSnapshotBoundary(metadata).ok());

            RejectReplication(&leader, 4, 1);
            RaftCore::ReplicationMode mode = RaftCore::ReplicationMode::APPEND_ENTRIES;
            ASSERT_TRUE(leader.GetReplicationModeForPeer(4, &mode).ok());
            EXPECT_EQ(mode, RaftCore::ReplicationMode::INSTALL_SNAPSHOT);
            EXPECT_EQ(leader.PromoteLearner(4).code(), StatusCode::kBusy);

            InstallSnapshotRequest request;
            ASSERT_TRUE(leader.BuildInstallSnapshotForPeer(4, &request).ok());
            EXPECT_EQ(request.metadata.last_included_index, 2U);
            EXPECT_EQ(request.metadata.membership.configuration_id, metadata.membership.configuration_id);
            ASSERT_TRUE(leader.HandleInstallSnapshotResponse(
                4, InstallSnapshotResponse{leader.current_term(), true, 2}).ok());

            ASSERT_TRUE(leader.GetReplicationModeForPeer(4, &mode).ok());
            EXPECT_EQ(mode, RaftCore::ReplicationMode::APPEND_ENTRIES);

            AppendEntriesRequest append_request;
            ASSERT_TRUE(leader.BuildAppendEntriesForPeer(4, &append_request).ok());
            EXPECT_EQ(append_request.prev_log_index, 2U);
            EXPECT_EQ(leader.PromoteLearner(4).code(), StatusCode::kBusy);

            AcceptReplication(&leader, 4, leader.log().last_index());
            RaftCore::LearnerState learner_state;
            ASSERT_TRUE(leader.GetLearnerState(4, &learner_state).ok());
            EXPECT_TRUE(learner_state.healthy);
            EXPECT_TRUE(learner_state.caught_up);
        }

        TEST(RaftMembershipTest, SingleMembershipChangeSlotBlocksConcurrentRequestsAndRecoveryKeepsBusyState)
        {
            RaftCore leader = MakeCore(MakeOptions(
                1,
                RaftRole::LEADER,
                {1, 2, 3},
                {},
                MakeMembership(1,
                               {MakeMember(1, "10.0.0.1"),
                                MakeMember(2, "10.0.0.2"),
                                MakeMember(3, "10.0.0.3")})));

            AcceptReplication(&leader, 2, 0);
            AcceptReplication(&leader, 3, 0);

            LogIndex add_index = 0;
            ASSERT_TRUE(leader.AddLearner(MakeMember(4, "10.0.0.4"), &add_index).ok());
            EXPECT_EQ(leader.RemoveMember(2).code(), StatusCode::kBusy);
            EXPECT_EQ(leader.PromoteLearner(4).code(), StatusCode::kBusy);

            AcceptReplication(&leader, 2, add_index);
            AcceptReplication(&leader, 3, add_index);
            ApplyCommittedMembershipAt(&leader, add_index);
            CommitCommandAndCatchUpLearner(&leader, 4);

            LogIndex phase1 = 0;
            ASSERT_TRUE(leader.PromoteLearner(4, &phase1).ok());
            AcceptReplication(&leader, 2, phase1);
            AcceptReplication(&leader, 3, phase1);
            AcceptReplication(&leader, 4, phase1);
            ApplyCommittedMembershipAt(&leader, phase1);

            EXPECT_EQ(leader.AddLearner(MakeMember(5, "10.0.0.5")).code(), StatusCode::kBusy);

            MembershipView persisted_view;
            ASSERT_TRUE(leader.GetMembershipView(&persisted_view).ok());
            RaftCore restored = MakeCore(MakeOptions(
                1,
                RaftRole::LEADER,
                {1, 2, 3},
                {4},
                persisted_view));

            EXPECT_EQ(restored.RemoveMember(2).code(), StatusCode::kBusy);

            LogIndex phase2 = 0;
            ASSERT_TRUE(restored.FinalizeMembershipChange(&phase2).ok());
            AcceptReplication(&restored, 2, phase2);
            AcceptReplication(&restored, 3, phase2);
            AcceptReplication(&restored, 4, phase2);
            ApplyCommittedMembershipAt(&restored, phase2);

            EXPECT_TRUE(restored.AddLearner(MakeMember(5, "10.0.0.5")).ok());
        }

        TEST(RaftMembershipTest, QuorumLossRejectsUnsafePromoteAndRemoveWhileAllowingManualReplacementWithQuorum)
        {
            const MembershipView membership = MakeMembership(
                5,
                {MakeMember(1, "10.0.0.1"),
                 MakeMember(2, "10.0.0.2"),
                 MakeMember(3, "10.0.0.3")},
                {MakeMember(4, "10.0.0.4")});

            RaftCore blocked = MakeCore(MakeOptions(1, RaftRole::LEADER, {1, 2, 3}, {4}, membership));
            EXPECT_EQ(blocked.PromoteLearner(4).code(), StatusCode::kBusy);
            EXPECT_EQ(blocked.RemoveMember(2).code(), StatusCode::kBusy);

            RaftCore leader = MakeCore(MakeOptions(1, RaftRole::LEADER, {1, 2, 3}, {}, MakeMembership(
                1,
                {MakeMember(1, "10.0.0.1"),
                 MakeMember(2, "10.0.0.2"),
                 MakeMember(3, "10.0.0.3")})));
            AcceptReplication(&leader, 2, 0);
            CommitLearnerAdd(&leader, 4, "10.0.0.4");
            CommitCommandAndCatchUpLearner(&leader, 4);

            LogIndex promote_phase1 = 0;
            ASSERT_TRUE(leader.PromoteLearner(4, &promote_phase1).ok());
            AcceptReplication(&leader, 2, promote_phase1);
            AcceptReplication(&leader, 4, promote_phase1);
            ApplyCommittedMembershipAt(&leader, promote_phase1);

            LogIndex promote_phase2 = 0;
            ASSERT_TRUE(leader.FinalizeMembershipChange(&promote_phase2).ok());
            AcceptReplication(&leader, 2, promote_phase2);
            AcceptReplication(&leader, 4, promote_phase2);
            ApplyCommittedMembershipAt(&leader, promote_phase2);

            LogIndex remove_phase1 = 0;
            ASSERT_TRUE(leader.RemoveMember(3, &remove_phase1).ok());
            AcceptReplication(&leader, 2, remove_phase1);
            AcceptReplication(&leader, 4, remove_phase1);
            ApplyCommittedMembershipAt(&leader, remove_phase1);

            LogIndex remove_phase2 = 0;
            ASSERT_TRUE(leader.FinalizeMembershipChange(&remove_phase2).ok());
            AcceptReplication(&leader, 2, remove_phase2);
            AcceptReplication(&leader, 4, remove_phase2);
            ApplyCommittedMembershipAt(&leader, remove_phase2);

            MembershipView final_view;
            ASSERT_TRUE(leader.GetMembershipView(&final_view).ok());
            EXPECT_TRUE(std::any_of(final_view.voters.begin(),
                                    final_view.voters.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 4; }));
            EXPECT_FALSE(std::any_of(final_view.voters.begin(),
                                     final_view.voters.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 3; }));

            PeerProgress progress;
            EXPECT_EQ(leader.GetPeerProgress(3, &progress).code(), StatusCode::kNotFound);
        }

        TEST(RaftMembershipTest, RemoveCurrentLeaderDemotesLocallyOnlyAfterFinalStableConfig)
        {
            RaftCore leader = MakeCore(MakeOptions(
                1,
                RaftRole::LEADER,
                {1, 2, 3},
                {},
                MakeMembership(10,
                               {MakeMember(1, "10.0.0.1"),
                                MakeMember(2, "10.0.0.2"),
                                MakeMember(3, "10.0.0.3")})));

            AcceptReplication(&leader, 2, 0);
            AcceptReplication(&leader, 3, 0);

            LogIndex phase1 = 0;
            ASSERT_TRUE(leader.RemoveMember(1, &phase1).ok());
            AcceptReplication(&leader, 2, phase1);
            AcceptReplication(&leader, 3, phase1);
            ApplyCommittedMembershipAt(&leader, phase1);

            EXPECT_EQ(leader.role(), RaftRole::LEADER);

            LogIndex phase2 = 0;
            ASSERT_TRUE(leader.FinalizeMembershipChange(&phase2).ok());
            AcceptReplication(&leader, 2, phase2);
            AcceptReplication(&leader, 3, phase2);
            ApplyCommittedMembershipAt(&leader, phase2);

            EXPECT_EQ(leader.role(), RaftRole::LEARNER);
            MembershipView final_view;
            ASSERT_TRUE(leader.GetMembershipView(&final_view).ok());
            EXPECT_FALSE(std::any_of(final_view.voters.begin(),
                                     final_view.voters.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 1; }));
        }

    } // namespace
} // namespace cpr::raft
