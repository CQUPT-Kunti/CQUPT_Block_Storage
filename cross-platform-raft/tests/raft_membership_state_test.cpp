#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "common/types.h"
#include "raft/raft_membership.h"

namespace cpr::raft
{
    namespace
    {

        using cpr::common::NodeId;
        using cpr::common::StatusCode;

        RaftMember MakeMember(NodeId node_id, const std::string &host)
        {
            return RaftMember{node_id, {host, static_cast<std::uint16_t>(9000 + node_id)}};
        }

        MembershipView MakeStableView(std::uint64_t configuration_id = 1)
        {
            MembershipView view;
            view.configuration_id = configuration_id;
            view.voters = {
                MakeMember(2, "10.0.0.2"),
                MakeMember(1, "10.0.0.1"),
            };
            view.learners = {
                MakeMember(4, "10.0.0.4"),
            };
            return view;
        }

        MembershipLogEntry MakeEntry(std::uint64_t configuration_id,
                                     bool has_active_transition = false)
        {
            MembershipLogEntry entry;
            entry.configuration_id = configuration_id;
            entry.has_active_transition = has_active_transition;
            entry.voters = {
                MakeMember(3, "10.0.0.3"),
                MakeMember(1, "10.0.0.1"),
            };
            entry.learners = {
                MakeMember(5, "10.0.0.5"),
            };
            if (has_active_transition)
            {
                entry.next_voters = {
                    MakeMember(1, "10.0.0.1"),
                    MakeMember(3, "10.0.0.3"),
                    MakeMember(5, "10.0.0.5"),
                };
            }
            return entry;
        }

        TEST(MembershipStateTest, BootstrapBuildsStableDeterministicState)
        {
            MembershipState state;
            HardState hard_state;
            MembershipView view = MakeStableView(7);

            ASSERT_TRUE(MembershipState::Bootstrap(1, view, hard_state, std::nullopt, &state).ok());
            EXPECT_EQ(state.configuration_id(), 7U);
            EXPECT_FALSE(state.has_active_transition());
            ASSERT_EQ(state.voters().size(), 2U);
            EXPECT_EQ(state.voters()[0].node_id, 1U);
            EXPECT_EQ(state.voters()[1].node_id, 2U);
            ASSERT_EQ(state.learners().size(), 1U);
            EXPECT_EQ(state.learners()[0].node_id, 4U);
        }

        TEST(MembershipStateTest, BootstrapRejectsExistingPersistentMembership)
        {
            MembershipState state;
            HardState hard_state;
            hard_state.membership_configuration_id = 3;
            SnapshotMetadata snapshot;
            snapshot.membership = MakeStableView(5);

            EXPECT_EQ(MembershipState::Bootstrap(1, MakeStableView(7), hard_state, std::nullopt, &state).code(),
                      StatusCode::kBusy);
            EXPECT_EQ(MembershipState::Bootstrap(1, MakeStableView(7), HardState(), snapshot, &state).code(),
                      StatusCode::kBusy);
        }

        TEST(MembershipStateTest, BootstrapRejectsMissingLocalNode)
        {
            MembershipState state;
            EXPECT_EQ(MembershipState::Bootstrap(9, MakeStableView(1), HardState(), std::nullopt, &state).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST(MembershipStateTest, FromViewRejectsOverlapAndEmptyVoters)
        {
            MembershipState state;
            MembershipView overlap = MakeStableView(2);
            overlap.learners.push_back(MakeMember(1, "10.0.0.1"));

            EXPECT_EQ(MembershipState::FromView(overlap, &state).code(),
                      StatusCode::kInvalidArgument);

            MembershipView empty_voters;
            empty_voters.configuration_id = 3;
            empty_voters.learners = {MakeMember(4, "10.0.0.4")};
            EXPECT_EQ(MembershipState::FromView(empty_voters, &state).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST(MembershipStateTest, LearnerQueriesStayOutOfVotingAndLeadership)
        {
            MembershipState state;
            ASSERT_TRUE(MembershipState::FromView(MakeStableView(11), &state).ok());

            EXPECT_TRUE(state.IsVoter(1));
            EXPECT_TRUE(state.IsLearner(4));
            EXPECT_FALSE(state.IsVoter(4));
            EXPECT_TRUE(state.CanVote(1));
            EXPECT_FALSE(state.CanVote(4));
            EXPECT_TRUE(state.CountsTowardQuorum(2));
            EXPECT_FALSE(state.CountsTowardQuorum(4));
            EXPECT_TRUE(state.CanBecomeLeader(2));
            EXPECT_FALSE(state.CanBecomeLeader(4));
            EXPECT_FALSE(state.Contains(8));
        }

        TEST(MembershipStateTest, TransitionFlagRoundTripsThroughStateAndView)
        {
            MembershipView view = MakeStableView(12);
            view.has_active_transition = true;
            view.next_voters = view.voters;
            view.next_learners = view.learners;

            MembershipState state;
            ASSERT_TRUE(MembershipState::FromView(view, &state).ok());
            EXPECT_TRUE(state.has_active_transition());

            MembershipView round_trip = state.ToView();
            EXPECT_TRUE(round_trip.has_active_transition);
            EXPECT_EQ(round_trip.configuration_id, 12U);
        }

        TEST(MembershipStateTest, ApplyCommittedUpdatesStateAndIsIdempotent)
        {
            MembershipState state;
            ASSERT_TRUE(MembershipState::FromView(MakeStableView(5), &state).ok());

            MembershipLogEntry entry = MakeEntry(6, true);
            ASSERT_TRUE(state.ApplyCommitted(entry).ok());
            EXPECT_EQ(state.configuration_id(), 6U);
            EXPECT_TRUE(state.has_active_transition());
            ASSERT_EQ(state.voters().size(), 2U);
            EXPECT_EQ(state.voters()[0].node_id, 1U);
            EXPECT_EQ(state.voters()[1].node_id, 3U);

            EXPECT_TRUE(state.ApplyCommitted(entry).ok());
            EXPECT_EQ(state.configuration_id(), 6U);
        }

        TEST(MembershipStateTest, ApplyCommittedRejectsRollbackAndPreservesStateOnFailure)
        {
            MembershipState state;
            ASSERT_TRUE(MembershipState::FromView(MakeStableView(10), &state).ok());
            MembershipView before = state.ToView();

            MembershipLogEntry old_entry = MakeEntry(9);
            EXPECT_EQ(state.ApplyCommitted(old_entry).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(state.ToView().configuration_id, before.configuration_id);

            MembershipLogEntry invalid_entry = MakeEntry(11);
            invalid_entry.learners.push_back(MakeMember(3, "10.0.0.3"));
            EXPECT_EQ(state.ApplyCommitted(invalid_entry).code(),
                      StatusCode::kInvalidArgument);

            MembershipView after = state.ToView();
            EXPECT_EQ(after.configuration_id, before.configuration_id);
            EXPECT_EQ(after.voters.size(), before.voters.size());
            EXPECT_EQ(after.learners.size(), before.learners.size());
        }

        TEST(MembershipStateTest, ApplyCommittedRejectsConflictingSameConfigurationId)
        {
            MembershipState state;
            ASSERT_TRUE(MembershipState::FromView(MakeStableView(20), &state).ok());

            MembershipLogEntry conflicting = state.ToLogEntry();
            conflicting.learners.push_back(MakeMember(8, "10.0.0.8"));

            EXPECT_EQ(state.ApplyCommitted(conflicting).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST(MembershipStateTest, MembershipLogEntryEncodeDecodeRoundTripsDeterministically)
        {
            MembershipLogEntry entry = MakeEntry(21, true);
            OpaquePayload payload;
            ASSERT_TRUE(EncodeMembershipLogEntry(entry, &payload).ok());

            MembershipLogEntry decoded;
            ASSERT_TRUE(DecodeMembershipLogEntry(payload, &decoded).ok());
            EXPECT_EQ(decoded.configuration_id, 21U);
            EXPECT_TRUE(decoded.has_active_transition);
            ASSERT_EQ(decoded.voters.size(), 2U);
            EXPECT_EQ(decoded.voters[0].node_id, 1U);
            EXPECT_EQ(decoded.voters[1].node_id, 3U);
            ASSERT_EQ(decoded.learners.size(), 1U);
            EXPECT_EQ(decoded.learners[0].node_id, 5U);
        }

        TEST(MembershipStateTest, MembershipLogEntryDecodeRejectsUnknownVersionTruncationAndInvalidMembers)
        {
            MembershipLogEntry entry = MakeEntry(30);
            OpaquePayload payload;
            ASSERT_TRUE(EncodeMembershipLogEntry(entry, &payload).ok());

            OpaquePayload unknown_version = payload;
            unknown_version[0] = 9;
            MembershipLogEntry decoded;
            EXPECT_EQ(DecodeMembershipLogEntry(unknown_version, &decoded).code(),
                      StatusCode::kCorruption);

            OpaquePayload truncated(payload.begin(), payload.end() - 1);
            EXPECT_EQ(DecodeMembershipLogEntry(truncated, &decoded).code(),
                      StatusCode::kCorruption);

            MembershipLogEntry invalid = entry;
            invalid.learners.push_back(MakeMember(3, "10.0.0.3"));
            ASSERT_TRUE(EncodeMembershipLogEntry(entry, &payload).ok());
            EXPECT_EQ(EncodeMembershipLogEntry(invalid, &payload).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST(MembershipStateTest, PersistentViewRoundTripRestoresExactState)
        {
            MembershipView view = MakeStableView(41);
            view.has_active_transition = true;
            view.learners.push_back(MakeMember(6, "10.0.0.6"));
            view.next_voters = view.voters;
            view.next_learners = view.learners;

            MembershipState state;
            ASSERT_TRUE(MembershipState::FromView(view, &state).ok());

            MembershipState restored;
            ASSERT_TRUE(MembershipState::FromView(state.ToView(), &restored).ok());

            MembershipView restored_view = restored.ToView();
            EXPECT_EQ(restored_view.configuration_id, 41U);
            EXPECT_TRUE(restored_view.has_active_transition);
            ASSERT_EQ(restored_view.voters.size(), 2U);
            ASSERT_EQ(restored_view.learners.size(), 2U);
            EXPECT_EQ(restored_view.learners[0].node_id, 4U);
            EXPECT_EQ(restored_view.learners[1].node_id, 6U);
        }

    } // namespace
} // namespace cpr::raft
