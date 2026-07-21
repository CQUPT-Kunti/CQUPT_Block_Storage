#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "fixture_loader.h"
#include "simulated_cluster.h"

namespace cpr::tests
{
    namespace
    {

        std::string WithChecksum(const std::string &body)
        {
            return body + "checksum: " +
                   std::to_string(FixtureLoader::ComputeChecksumForText(body)) +
                   "\n";
        }

        Fixture MakeFixture()
        {
            const std::string body =
                "version: 1\n"
                "scenario: simulated-cluster\n"
                "node: 1,node-1,leader,127.0.0.1,9001\n"
                "node: 2,node-2,follower,127.0.0.1,9002\n"
                "node: 3,node-3,learner,127.0.0.1,9003\n"
                "step: 1,deliver,1,2,request-vote\n"
                "expect: delivered,1\n";
            Fixture fixture;
            EXPECT_TRUE(FixtureLoader::LoadFromString(WithChecksum(body), &fixture).ok());
            return fixture;
        }

        raft::RaftMessage MakeVote(common::NodeId source,
                                   common::NodeId target)
        {
            raft::RequestVoteRequest request;
            request.term = 7;
            request.candidate_id = source;
            request.last_log_index = 3;
            request.last_log_term = 6;

            raft::RaftMessage message;
            message.type = raft::RaftMessageType::REQUEST_VOTE_REQUEST;
            message.source_node_id = source;
            message.target_node_id = target;
            message.payload = request;
            return message;
        }

        TEST(SimulatedClusterTest, BuildsNodesFromFixtureInDeterministicOrder)
        {
            SimulatedCluster cluster;
            ASSERT_TRUE(cluster.Initialize(MakeFixture()).ok());

            const std::vector<SimulatedNodeSnapshot> nodes = cluster.Nodes();
            ASSERT_EQ(nodes.size(), 3U);
            EXPECT_EQ(nodes[0].node_id, 1U);
            EXPECT_EQ(nodes[0].role, FixtureNodeRole::LEADER);
            EXPECT_EQ(nodes[1].node_id, 2U);
            EXPECT_EQ(nodes[1].role, FixtureNodeRole::FOLLOWER);
            EXPECT_EQ(nodes[2].node_id, 3U);
            EXPECT_EQ(nodes[2].role, FixtureNodeRole::LEARNER);
            EXPECT_EQ(nodes[0].state, raft::NodeState::RUNNING);
        }

        TEST(SimulatedClusterTest, TransportDeliversMessageAndReturnsDeterministicResponse)
        {
            SimulatedCluster cluster;
            ASSERT_TRUE(cluster.Initialize(MakeFixture()).ok());
            ASSERT_TRUE(cluster.Start().ok());

            std::unique_ptr<transport::IRaftTransport> transport;
            ASSERT_TRUE(cluster.CreateTransport(1, &transport).ok());
            ASSERT_TRUE(transport->Start().ok());

            raft::RaftMessage response;
            ASSERT_TRUE(transport->Send(MakeVote(1, 2), &response).ok());
            EXPECT_EQ(cluster.InboxSize(2), 1U);
            EXPECT_EQ(response.type, raft::RaftMessageType::REQUEST_VOTE_RESPONSE);
            EXPECT_EQ(response.source_node_id, 2U);
            EXPECT_EQ(response.target_node_id, 1U);
            const auto &body = std::get<raft::RequestVoteResponse>(response.payload);
            EXPECT_EQ(body.term, 7U);
            EXPECT_TRUE(body.vote_granted);

            SimulatedEnvelope envelope;
            ASSERT_TRUE(cluster.DequeueMessage(2, &envelope).ok());
            EXPECT_EQ(envelope.sequence, 1U);
            EXPECT_EQ(envelope.message.source_node_id, 1U);
            EXPECT_EQ(envelope.message.target_node_id, 2U);
            EXPECT_EQ(cluster.InboxSize(2), 0U);
        }

        TEST(SimulatedClusterTest, PartitionAndStoppedNodeRejectDelivery)
        {
            SimulatedCluster cluster;
            ASSERT_TRUE(cluster.Initialize(MakeFixture()).ok());
            ASSERT_TRUE(cluster.Start().ok());
            ASSERT_TRUE(cluster.Partition(1, 2).ok());
            EXPECT_TRUE(cluster.IsPartitioned(2, 1));

            std::unique_ptr<transport::IRaftTransport> transport;
            ASSERT_TRUE(cluster.CreateTransport(1, &transport).ok());
            ASSERT_TRUE(transport->Start().ok());

            raft::RaftMessage response;
            EXPECT_EQ(transport->Send(MakeVote(1, 2), &response).code(),
                      common::StatusCode::kRetryLater);
            EXPECT_EQ(cluster.InboxSize(2), 0U);

            ASSERT_TRUE(cluster.Heal(1, 2).ok());
            ASSERT_TRUE(cluster.SetNodeState(2, raft::NodeState::STOPPED).ok());
            EXPECT_EQ(transport->Send(MakeVote(1, 2), &response).code(),
                      common::StatusCode::kBusy);
            EXPECT_EQ(cluster.InboxSize(2), 0U);
        }

        TEST(SimulatedClusterTest, LifecycleAndBadTargetsReturnExplicitStatus)
        {
            SimulatedCluster cluster;
            EXPECT_EQ(cluster.Start().code(), common::StatusCode::kBusy);
            ASSERT_TRUE(cluster.Initialize(MakeFixture()).ok());
            ASSERT_TRUE(cluster.Start().ok());
            EXPECT_TRUE(cluster.Start().ok());
            EXPECT_TRUE(cluster.Stop().ok());
            EXPECT_TRUE(cluster.Stop().ok());

            std::unique_ptr<transport::IRaftTransport> missing;
            EXPECT_EQ(cluster.CreateTransport(99, &missing).code(),
                      common::StatusCode::kNotFound);

            std::unique_ptr<transport::IRaftTransport> transport;
            ASSERT_TRUE(cluster.CreateTransport(1, &transport).ok());
            EXPECT_EQ(transport->Send(MakeVote(1, 2), nullptr).code(),
                      common::StatusCode::kBusy);
        }

    } // namespace
} // namespace cpr::tests
