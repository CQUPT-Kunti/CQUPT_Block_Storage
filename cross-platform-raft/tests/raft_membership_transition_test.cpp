#include <algorithm>
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

        RaftMember MakeMember(NodeId node_id, const std::string &host)
        {
            return RaftMember{node_id, {host, static_cast<std::uint16_t>(9000 + node_id)}};
        }

        MembershipView MakeMembership(std::uint64_t configuration_id,
                                      std::vector<RaftMember> voters,
                                      std::vector<RaftMember> learners = {})
        {
            MembershipView view;
            view.configuration_id = configuration_id;
            view.voters = std::move(voters);
            view.learners = std::move(learners);
            return view;
        }

        RaftCore::Options MakeOptions(NodeId node_id,
                                      std::vector<NodeId> voters,
                                      std::vector<NodeId> learners,
                                      const MembershipView &membership)
        {
            RaftCore::Options options;
            options.node_id = node_id;
            options.initial_role = RaftRole::CANDIDATE;
            options.election_timeout_ticks = 3;
            options.hard_state.current_term = 1;
            options.voter_ids = std::move(voters);
            options.learner_ids = std::move(learners);
            options.membership = membership;
            return options;
        }

        RaftCore MakeLeader(const RaftCore::Options &options)
        {
            RaftCore core;
            EXPECT_TRUE(core.Initialize(options).ok());
            EXPECT_TRUE(core.BecomeLeader().ok());
            return core;
        }

        LogEntry GetEntry(const RaftCore &core, LogIndex index)
        {
            LogEntry entry;
            EXPECT_TRUE(core.log().GetEntry(index, &entry).ok());
            return entry;
        }

        void ConfirmFollowerMatch(RaftCore *core, NodeId peer_id, LogIndex match_index)
        {
            ASSERT_NE(core, nullptr);
            RaftCore::AppendResponseAction action = RaftCore::AppendResponseAction::NONE;
            AppendEntriesResponse response;
            response.term = core->current_term();
            response.success = true;
            response.match_index = match_index;
            ASSERT_TRUE(core->HandleAppendEntriesResponse(peer_id, response, &action).ok());
        }

        TEST(RaftMembershipTransitionTest, PromoteLearnerUsesTransitionThenStableConfig)
        {
            RaftCore core = MakeLeader(MakeOptions(
                1,
                {1},
                {},
                MakeMembership(1, {MakeMember(1, "10.0.0.1")})));

            ASSERT_TRUE(core.AddLearner(MakeMember(2, "10.0.0.2")).ok());
            ASSERT_TRUE(core.ApplyCommittedMembershipEntry(GetEntry(core, 1)).ok());
            ASSERT_TRUE(core.ConfirmApplied(1).ok());

            ASSERT_TRUE(core.Propose(OpaquePayload{'x'}).ok());
            ConfirmFollowerMatch(&core, 2, core.log().last_index());
            ASSERT_TRUE(core.ConfirmApplied(2).ok());

            LogIndex phase1 = 0;
            ASSERT_TRUE(core.PromoteLearner(2, &phase1).ok());
            MembershipLogEntry phase1_entry;
            ASSERT_TRUE(DecodeMembershipLogEntry(GetEntry(core, phase1).payload, &phase1_entry).ok());
            EXPECT_TRUE(phase1_entry.has_active_transition);
            EXPECT_EQ(phase1_entry.next_voters.size(), 2U);

            ConfirmFollowerMatch(&core, 2, phase1);
            ASSERT_TRUE(core.ApplyCommittedMembershipEntry(GetEntry(core, phase1)).ok());
            ASSERT_TRUE(core.ConfirmApplied(phase1).ok());

            MembershipView transitioning;
            ASSERT_TRUE(core.GetMembershipView(&transitioning).ok());
            EXPECT_TRUE(transitioning.has_active_transition);
            EXPECT_EQ(transitioning.next_voters.size(), 2U);

            ASSERT_TRUE(core.Propose(OpaquePayload{'y'}).ok());
            EXPECT_LT(core.log().commit_index(), core.log().last_index());

            ConfirmFollowerMatch(&core, 2, core.log().last_index());
            EXPECT_EQ(core.log().commit_index(), core.log().last_index());
            ASSERT_TRUE(core.ConfirmApplied(phase1 + 1).ok());

            LogIndex phase2 = 0;
            ASSERT_TRUE(core.FinalizeMembershipChange(&phase2).ok());
            ConfirmFollowerMatch(&core, 2, phase2);
            ASSERT_TRUE(core.ApplyCommittedMembershipEntry(GetEntry(core, phase2)).ok());
            ASSERT_TRUE(core.ConfirmApplied(phase2).ok());

            MembershipView stable;
            ASSERT_TRUE(core.GetMembershipView(&stable).ok());
            EXPECT_FALSE(stable.has_active_transition);
            EXPECT_TRUE(std::any_of(stable.voters.begin(), stable.voters.end(),
                                    [](const RaftMember &member)
                                    { return member.node_id == 2; }));
            EXPECT_TRUE(stable.next_voters.empty());
        }

        TEST(RaftMembershipTransitionTest, RemoveVoterUsesTransitionThenStableConfig)
        {
            RaftCore core = MakeLeader(MakeOptions(
                1,
                {1, 2, 3},
                {},
                MakeMembership(1,
                               {MakeMember(1, "10.0.0.1"),
                                MakeMember(2, "10.0.0.2"),
                                MakeMember(3, "10.0.0.3")})));

            ConfirmFollowerMatch(&core, 2, 0);
            ConfirmFollowerMatch(&core, 3, 0);

            LogIndex phase1 = 0;
            ASSERT_TRUE(core.RemoveMember(2, &phase1).ok());
            MembershipLogEntry phase1_entry;
            ASSERT_TRUE(DecodeMembershipLogEntry(GetEntry(core, phase1).payload, &phase1_entry).ok());
            EXPECT_TRUE(phase1_entry.has_active_transition);
            EXPECT_EQ(phase1_entry.voters.size(), 3U);
            EXPECT_EQ(phase1_entry.next_voters.size(), 2U);

            ConfirmFollowerMatch(&core, 2, phase1);
            ConfirmFollowerMatch(&core, 3, phase1);
            ASSERT_TRUE(core.ApplyCommittedMembershipEntry(GetEntry(core, phase1)).ok());
            ASSERT_TRUE(core.ConfirmApplied(phase1).ok());

            MembershipView transitioning;
            ASSERT_TRUE(core.GetMembershipView(&transitioning).ok());
            EXPECT_TRUE(transitioning.has_active_transition);
            EXPECT_EQ(transitioning.next_voters.size(), 2U);

            LogIndex phase2 = 0;
            ASSERT_TRUE(core.FinalizeMembershipChange(&phase2).ok());
            ConfirmFollowerMatch(&core, 3, phase2);
            ASSERT_TRUE(core.ApplyCommittedMembershipEntry(GetEntry(core, phase2)).ok());
            ASSERT_TRUE(core.ConfirmApplied(phase2).ok());

            MembershipView stable;
            ASSERT_TRUE(core.GetMembershipView(&stable).ok());
            EXPECT_FALSE(stable.has_active_transition);
            EXPECT_FALSE(std::any_of(stable.voters.begin(), stable.voters.end(),
                                     [](const RaftMember &member)
                                     { return member.node_id == 2; }));
        }

        TEST(RaftMembershipTransitionTest, ActiveTransitionRoundTripsThroughView)
        {
            RaftCore core = MakeLeader(MakeOptions(
                1,
                {1},
                {},
                MakeMembership(1, {MakeMember(1, "10.0.0.1")})));

            ASSERT_TRUE(core.AddLearner(MakeMember(2, "10.0.0.2")).ok());
            ASSERT_TRUE(core.ApplyCommittedMembershipEntry(GetEntry(core, 1)).ok());
            ASSERT_TRUE(core.ConfirmApplied(1).ok());
            ASSERT_TRUE(core.Propose(OpaquePayload{'x'}).ok());
            ConfirmFollowerMatch(&core, 2, core.log().last_index());
            ASSERT_TRUE(core.ConfirmApplied(2).ok());
            ASSERT_TRUE(core.PromoteLearner(2).ok());
            const LogIndex phase1 = core.log().last_index();
            ConfirmFollowerMatch(&core, 2, phase1);
            ASSERT_TRUE(core.ApplyCommittedMembershipEntry(GetEntry(core, phase1)).ok());
            ASSERT_TRUE(core.ConfirmApplied(phase1).ok());

            MembershipView view;
            ASSERT_TRUE(core.GetMembershipView(&view).ok());
            EXPECT_TRUE(view.has_active_transition);

            MembershipState restored;
            ASSERT_TRUE(MembershipState::FromView(view, &restored).ok());
            EXPECT_TRUE(restored.has_active_transition());
            EXPECT_EQ(restored.next_voters().size(), 2U);
        }

    } // namespace
} // namespace cpr::raft
