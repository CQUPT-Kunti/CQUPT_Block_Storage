#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "fixture_loader.h"
#include "raft/raft_message.h"
#include "raft/raft_types.h"
#include "transport/raft_transport.h"

namespace cpr::tests
{

    struct SimulatedNodeSnapshot
    {
        common::NodeId node_id = common::kInvalidNodeId;
        std::string name;
        FixtureNodeRole role = FixtureNodeRole::FOLLOWER;
        raft::NodeAddress address;
        raft::NodeState state = raft::NodeState::RUNNING;
        std::size_t inbox_size = 0;
    };

    struct SimulatedEnvelope
    {
        std::uint64_t sequence = 0;
        raft::RaftMessage message;
    };

    class SimulatedCluster;

    class SimulatedTransport final : public transport::IRaftTransport
    {
    public:
        explicit SimulatedTransport(SimulatedCluster *cluster);

        common::Status Initialize(
            common::NodeId local_node_id,
            const std::vector<raft::RaftMember> &peers) override;
        common::Status Start() override;
        common::Status Stop() override;
        common::Status Send(const raft::RaftMessage &message,
                            raft::RaftMessage *response) override;

    private:
        SimulatedCluster *cluster_ = nullptr;
        common::NodeId local_node_id_ = common::kInvalidNodeId;
        std::set<common::NodeId> peer_ids_;
        bool initialized_ = false;
        bool started_ = false;
    };

    class SimulatedCluster
    {
    public:
        common::Status Initialize(const Fixture &fixture);
        common::Status Start();
        common::Status Stop();

        common::Status CreateTransport(
            common::NodeId local_node_id,
            std::unique_ptr<transport::IRaftTransport> *transport);

        common::Status SetNodeState(common::NodeId node_id,
                                    raft::NodeState state);
        common::Status Partition(common::NodeId first, common::NodeId second);
        common::Status Heal(common::NodeId first, common::NodeId second);
        bool IsPartitioned(common::NodeId first, common::NodeId second) const;

        common::Status DequeueMessage(common::NodeId node_id,
                                      SimulatedEnvelope *envelope);
        std::size_t InboxSize(common::NodeId node_id) const;
        std::vector<SimulatedNodeSnapshot> Nodes() const;

    private:
        friend class SimulatedTransport;

        common::Status Deliver(const raft::RaftMessage &message,
                               raft::RaftMessage *response);
        bool HasNode(common::NodeId node_id) const;
        static common::Status ValidateNodeState(raft::NodeState state);
        static std::pair<common::NodeId, common::NodeId> LinkKey(
            common::NodeId first,
            common::NodeId second);

        struct Node
        {
            SimulatedNodeSnapshot snapshot;
            std::deque<SimulatedEnvelope> inbox;
        };

        std::map<common::NodeId, Node> nodes_;
        std::set<std::pair<common::NodeId, common::NodeId>> partitions_;
        std::uint64_t next_sequence_ = 1;
        bool initialized_ = false;
        bool started_ = false;
    };

} // namespace cpr::tests
