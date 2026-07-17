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
        using cpr::common::Term;

        RaftMember MakeMember(NodeId node_id, const std::string &host)
        {
            return RaftMember{node_id, {host, static_cast<std::uint16_t>(9000 + node_id)}};
        }

        MembershipView MakeMembership(std::uint64_t configuration_id = 1,
                                      bool active_transition = false)
        {
            MembershipView view;
            view.configuration_id = configuration_id;
            view.has_active_transition = active_transition;
            view.voters = {
                MakeMember(1, "10.0.0.1"),
            };
            return view;
        }

        RaftCore::Options MakeOptions(NodeId node_id = 1,
                                      RaftRole role = RaftRole::LEADER,
                                      std::vector<NodeId> voters = {1},
                                      std::vector<NodeId> learners = {},
                                      std::optional<MembershipView> membership = MakeMembership())
        {
            RaftCore::Options options;
            options.node_id = node_id;
            options.initial_role = role == RaftRole::LEADER ? RaftRole::CANDIDATE : role;
            options.election_timeout_ticks = 3;
            options.voter_ids = std::move(voters);
            options.learner_ids = std::move(learners);
            options.membership = std::move(membership);
            options.hard_state.current_term =
                role == RaftRole::LEADER ? 1 : common::kInitialTerm;
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

        TEST(RaftMembershipAddLearnerTest, LeaderAcceptsValidAddLearnerAndGeneratesMembershipLog)
        {
            RaftCore leader = MakeCore(MakeOptions());
            const LogIndex before_last_index = leader.log().last_index();

            LogIndex log_index = 0;
            ASSERT_TRUE(leader.AddLearner(MakeMember(2, "10.0.0.2"), &log_index).ok());
            EXPECT_EQ(log_index, before_last_index + 1);
            EXPECT_EQ(leader.log().last_index(), before_last_index + 1);

            const LogEntry entry = GetEntry(leader, log_index);
            EXPECT_EQ(entry.type, LogEntryType::MEMBERSHIP_CHANGE);

            MembershipLogEntry membership_entry;
            ASSERT_TRUE(DecodeMembershipLogEntry(entry.payload, &membership_entry).ok());
            EXPECT_EQ(membership_entry.configuration_id, 2U);
            ASSERT_EQ(membership_entry.voters.size(), 1U);
            ASSERT_EQ(membership_entry.learners.size(), 1U);
            EXPECT_EQ(membership_entry.learners[0].node_id, 2U);

            MembershipView committed;
            ASSERT_TRUE(leader.GetMembershipView(&committed).ok());
            EXPECT_TRUE(committed.learners.empty());
        }

        TEST(RaftMembershipAddLearnerTest, NonLeaderRejectsAddLearner)
        {
            RaftCore follower = MakeCore(MakeOptions(1, RaftRole::FOLLOWER));
            RaftCore candidate;
            ASSERT_TRUE(candidate.Initialize(MakeOptions(1, RaftRole::CANDIDATE)).ok());
            RaftCore learner = MakeCore(MakeOptions(2,
                                                    RaftRole::LEARNER,
                                                    {1},
                                                    {2},
                                                    MembershipView{
                                                        {MakeMember(1, "10.0.0.1")},
                                                        {MakeMember(2, "10.0.0.2")},
                                                        false,
                                                        1}));

            EXPECT_EQ(follower.AddLearner(MakeMember(2, "10.0.0.2")).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(candidate.AddLearner(MakeMember(2, "10.0.0.2")).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(learner.AddLearner(MakeMember(3, "10.0.0.3")).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST(RaftMembershipAddLearnerTest, AddLearnerRejectsInvalidExistingAndConflictingRequestsWithoutMutation)
        {
            RaftCore leader = MakeCore(MakeOptions());
            MembershipView before;
            ASSERT_TRUE(leader.GetMembershipView(&before).ok());

            EXPECT_EQ(leader.AddLearner(RaftMember{}).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(leader.AddLearner(MakeMember(1, "10.0.0.1")).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(leader.AddLearner(RaftMember{2, {"10.0.0.1", 9001}}).code(),
                      StatusCode::kInvalidArgument);

            ASSERT_TRUE(leader.AddLearner(MakeMember(2, "10.0.0.2")).ok());
            const LogEntry membership_entry = GetEntry(leader, 1);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(membership_entry).ok());

            EXPECT_EQ(leader.AddLearner(MakeMember(2, "10.0.0.2")).code(),
                      StatusCode::kInvalidArgument);

            RaftCore blocked = MakeCore(MakeOptions(1,
                                                    RaftRole::LEADER,
                                                    {1},
                                                    {},
                                                    MakeMembership(1, true)));
            EXPECT_EQ(blocked.AddLearner(MakeMember(3, "10.0.0.3")).code(),
                      StatusCode::kBusy);

            MembershipView after;
            ASSERT_TRUE(leader.GetMembershipView(&after).ok());
            EXPECT_EQ(leader.log().last_index(), 1U);
            EXPECT_EQ(after.configuration_id, 2U);
            EXPECT_EQ(before.configuration_id, 1U);
        }

        TEST(RaftMembershipAddLearnerTest, CommittedMembershipEntryAddsLearnerAndInitializesPeerProgress)
        {
            RaftCore leader = MakeCore(MakeOptions());
            ASSERT_TRUE(leader.AddLearner(MakeMember(2, "10.0.0.2")).ok());

            const LogEntry entry = GetEntry(leader, 1);
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(entry).ok());

            MembershipView membership;
            ASSERT_TRUE(leader.GetMembershipView(&membership).ok());
            ASSERT_EQ(membership.learners.size(), 1U);
            EXPECT_EQ(membership.learners[0].node_id, 2U);
            EXPECT_TRUE(leader.learner_ids() == std::vector<NodeId>({2}));

            PeerProgress progress;
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_TRUE(progress.is_learner);
            EXPECT_EQ(progress.match_index, common::kInvalidLogIndex);
            EXPECT_EQ(progress.next_index, leader.log().last_index() + 1);

            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(entry).ok());
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_TRUE(progress.is_learner);
        }

        TEST(RaftMembershipAddLearnerTest, LearnerReplicationProgressAndCaughtUpStateFollowExistingAppendLogic)
        {
            RaftCore leader = MakeCore(MakeOptions());
            ASSERT_TRUE(leader.AddLearner(MakeMember(2, "10.0.0.2")).ok());
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, 1)).ok());
            ASSERT_TRUE(leader.Propose(OpaquePayload{'c', 'm', 'd'}).ok());

            RaftCore::LearnerState learner_state;
            ASSERT_TRUE(leader.GetLearnerState(2, &learner_state).ok());
            EXPECT_FALSE(learner_state.healthy);
            EXPECT_FALSE(learner_state.caught_up);
            EXPECT_FALSE(learner_state.requires_snapshot);

            AppendEntriesRequest request;
            ASSERT_TRUE(leader.BuildAppendEntriesForPeer(2, &request).ok());
            EXPECT_EQ(request.entries.size(), 1U);

            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(
                2, AppendEntriesResponse{leader.current_term(), true, 2, 0, 0}, &action).ok());
            EXPECT_EQ(action, RaftCore::AppendResponseAction::ACCEPTED);

            PeerProgress progress;
            ASSERT_TRUE(leader.GetPeerProgress(2, &progress).ok());
            EXPECT_EQ(progress.match_index, 2U);
            EXPECT_EQ(progress.next_index, 3U);

            ASSERT_TRUE(leader.GetLearnerState(2, &learner_state).ok());
            EXPECT_TRUE(learner_state.healthy);
            EXPECT_TRUE(learner_state.caught_up);
        }

        TEST(RaftMembershipAddLearnerTest, LearnerDoesNotAffectQuorumAndCommit)
        {
            RaftCore leader = MakeCore(MakeOptions());
            ASSERT_TRUE(leader.AddLearner(MakeMember(2, "10.0.0.2")).ok());
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, 1)).ok());

            ASSERT_TRUE(leader.Propose(OpaquePayload{'x'}).ok());
            EXPECT_EQ(leader.log().commit_index(), 2U);

            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(
                2, AppendEntriesResponse{leader.current_term(), true, 2, 0, 0}, &action).ok());
            EXPECT_EQ(leader.log().commit_index(), 2U);
        }

        TEST(RaftMembershipAddLearnerTest, ReplicationModeChoosesSnapshotWhenLogIsCompactedAndAppendAfterSnapshotResponse)
        {
            RaftCore leader = MakeCore(MakeOptions());
            ASSERT_TRUE(leader.AddLearner(MakeMember(2, "10.0.0.2")).ok());
            ASSERT_TRUE(leader.ApplyCommittedMembershipEntry(GetEntry(leader, 1)).ok());
            ASSERT_TRUE(leader.Propose(OpaquePayload{'a'}).ok());
            ASSERT_TRUE(leader.Propose(OpaquePayload{'b'}).ok());
            ASSERT_TRUE(leader.ConfirmApplied(1).ok());
            ASSERT_TRUE(leader.ConfirmApplied(2).ok());

            SnapshotMetadata metadata;
            metadata.last_included_index = 2;
            metadata.last_included_term = 1;
            ASSERT_TRUE(leader.GetMembershipView(&metadata.membership).ok());
            ASSERT_TRUE(leader.UpdateSnapshotBoundary(metadata).ok());

            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            ASSERT_TRUE(leader.HandleAppendEntriesResponse(
                2, AppendEntriesResponse{leader.current_term(), false, 0, 1, 0}, &action).ok());
            EXPECT_EQ(action, RaftCore::AppendResponseAction::REJECTED);

            RaftCore::ReplicationMode mode = RaftCore::ReplicationMode::APPEND_ENTRIES;
            ASSERT_TRUE(leader.GetReplicationModeForPeer(2, &mode).ok());
            EXPECT_EQ(mode, RaftCore::ReplicationMode::INSTALL_SNAPSHOT);

            InstallSnapshotRequest snapshot_request;
            ASSERT_TRUE(leader.BuildInstallSnapshotForPeer(2, &snapshot_request).ok());
            EXPECT_EQ(snapshot_request.metadata.last_included_index, 2U);
            EXPECT_EQ(snapshot_request.metadata.membership.configuration_id, 2U);

            ASSERT_TRUE(leader.HandleInstallSnapshotResponse(
                2, InstallSnapshotResponse{leader.current_term(), true, 2}).ok());
            ASSERT_TRUE(leader.GetReplicationModeForPeer(2, &mode).ok());
            EXPECT_EQ(mode, RaftCore::ReplicationMode::APPEND_ENTRIES);

            ASSERT_TRUE(leader.Propose(OpaquePayload{'c'}).ok());
            AppendEntriesRequest append_request;
            ASSERT_TRUE(leader.BuildAppendEntriesForPeer(2, &append_request).ok());
            EXPECT_EQ(append_request.prev_log_index, 2U);
            ASSERT_EQ(append_request.entries.size(), 2U);

            RaftCore::LearnerState learner_state;
            ASSERT_TRUE(leader.GetLearnerState(2, &learner_state).ok());
            EXPECT_FALSE(learner_state.requires_snapshot);
        }

    } // namespace
} // namespace cpr::raft
