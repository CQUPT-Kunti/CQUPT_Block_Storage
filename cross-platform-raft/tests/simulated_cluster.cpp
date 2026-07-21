#include "simulated_cluster.h"

#include <algorithm>
#include <utility>

namespace cpr::tests
{
    namespace
    {

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        void FillDeterministicResponse(const raft::RaftMessage &message,
                                       raft::RaftMessage *response)
        {
            if (response == nullptr)
            {
                return;
            }

            response->source_node_id = message.target_node_id;
            response->target_node_id = message.source_node_id;
            switch (message.type)
            {
            case raft::RaftMessageType::REQUEST_VOTE_REQUEST:
            {
                const auto &request =
                    std::get<raft::RequestVoteRequest>(message.payload);
                raft::RequestVoteResponse body;
                body.term = request.term;
                body.vote_granted = true;
                response->type = raft::RaftMessageType::REQUEST_VOTE_RESPONSE;
                response->payload = body;
                return;
            }
            case raft::RaftMessageType::APPEND_ENTRIES_REQUEST:
            {
                const auto &request =
                    std::get<raft::AppendEntriesRequest>(message.payload);
                raft::AppendEntriesResponse body;
                body.term = request.term;
                body.success = true;
                body.match_index = request.entries.empty()
                                       ? request.prev_log_index
                                       : request.entries.back().index;
                response->type = raft::RaftMessageType::APPEND_ENTRIES_RESPONSE;
                response->payload = body;
                return;
            }
            case raft::RaftMessageType::INSTALL_SNAPSHOT_REQUEST:
            {
                const auto &request =
                    std::get<raft::InstallSnapshotRequest>(message.payload);
                raft::InstallSnapshotResponse body;
                body.term = request.term;
                body.success = true;
                body.last_included_index =
                    request.metadata.last_included_index;
                response->type = raft::RaftMessageType::INSTALL_SNAPSHOT_RESPONSE;
                response->payload = body;
                return;
            }
            case raft::RaftMessageType::REQUEST_VOTE_RESPONSE:
            case raft::RaftMessageType::APPEND_ENTRIES_RESPONSE:
            case raft::RaftMessageType::INSTALL_SNAPSHOT_RESPONSE:
                *response = message;
                return;
            }
        }

    } // namespace

    SimulatedTransport::SimulatedTransport(SimulatedCluster *cluster)
        : cluster_(cluster)
    {
    }

    common::Status SimulatedTransport::Initialize(
        common::NodeId local_node_id,
        const std::vector<raft::RaftMember> &peers)
    {
        if (cluster_ == nullptr)
        {
            return Invalid("simulated transport cluster must not be null");
        }
        if (local_node_id == common::kInvalidNodeId)
        {
            return Invalid("local node id must be positive");
        }
        if (!cluster_->HasNode(local_node_id))
        {
            return common::Status::NotFound("local node is not in simulated cluster");
        }

        std::set<common::NodeId> candidate_peers;
        for (const raft::RaftMember &peer : peers)
        {
            if (peer.node_id == common::kInvalidNodeId)
            {
                return Invalid("peer node id must be positive");
            }
            if (peer.node_id == local_node_id)
            {
                return Invalid("peer list must not contain local node");
            }
            if (!cluster_->HasNode(peer.node_id))
            {
                return common::Status::NotFound("peer is not in simulated cluster");
            }
            if (!candidate_peers.insert(peer.node_id).second)
            {
                return Invalid("duplicate simulated peer");
            }
        }

        local_node_id_ = local_node_id;
        peer_ids_ = std::move(candidate_peers);
        initialized_ = true;
        started_ = false;
        return common::Status::OK();
    }

    common::Status SimulatedTransport::Start()
    {
        if (!initialized_)
        {
            return common::Status::Busy("simulated transport is not initialized");
        }
        started_ = true;
        return common::Status::OK();
    }

    common::Status SimulatedTransport::Stop()
    {
        started_ = false;
        return common::Status::OK();
    }

    common::Status SimulatedTransport::Send(const raft::RaftMessage &message,
                                            raft::RaftMessage *response)
    {
        if (!started_)
        {
            return common::Status::Busy("simulated transport is not started");
        }
        if (message.source_node_id != local_node_id_)
        {
            return Invalid("message source does not match simulated transport local node");
        }
        if (peer_ids_.find(message.target_node_id) == peer_ids_.end())
        {
            return common::Status::NotFound("target is not a configured simulated peer");
        }
        return cluster_->Deliver(message, response);
    }

    common::Status SimulatedCluster::Initialize(const Fixture &fixture)
    {
        if (fixture.nodes.empty())
        {
            return Invalid("fixture must contain nodes for simulated cluster");
        }

        std::map<common::NodeId, Node> candidate;
        for (const FixtureNode &fixture_node : fixture.nodes)
        {
            if (fixture_node.node_id == common::kInvalidNodeId)
            {
                return Invalid("simulated node id must be positive");
            }
            if (candidate.find(fixture_node.node_id) != candidate.end())
            {
                return Invalid("duplicate simulated node id");
            }

            Node node;
            node.snapshot.node_id = fixture_node.node_id;
            node.snapshot.name = fixture_node.name;
            node.snapshot.role = fixture_node.role;
            node.snapshot.address = fixture_node.address;
            node.snapshot.state = raft::NodeState::RUNNING;
            candidate.emplace(fixture_node.node_id, std::move(node));
        }

        nodes_ = std::move(candidate);
        partitions_.clear();
        next_sequence_ = 1;
        initialized_ = true;
        started_ = false;
        return common::Status::OK();
    }

    common::Status SimulatedCluster::Start()
    {
        if (!initialized_)
        {
            return common::Status::Busy("simulated cluster is not initialized");
        }
        started_ = true;
        return common::Status::OK();
    }

    common::Status SimulatedCluster::Stop()
    {
        started_ = false;
        return common::Status::OK();
    }

    common::Status SimulatedCluster::CreateTransport(
        common::NodeId local_node_id,
        std::unique_ptr<transport::IRaftTransport> *transport)
    {
        if (transport == nullptr)
        {
            return Invalid("transport output must not be null");
        }
        if (!HasNode(local_node_id))
        {
            return common::Status::NotFound("local node is not in simulated cluster");
        }

        std::vector<raft::RaftMember> peers;
        for (const auto &item : nodes_)
        {
            if (item.first != local_node_id)
            {
                peers.push_back(raft::RaftMember{
                    item.second.snapshot.node_id,
                    item.second.snapshot.address});
            }
        }

        std::unique_ptr<SimulatedTransport> candidate(
            new SimulatedTransport(this));
        common::Status status = candidate->Initialize(local_node_id, peers);
        if (!status.ok())
        {
            return status;
        }
        *transport = std::move(candidate);
        return common::Status::OK();
    }

    common::Status SimulatedCluster::SetNodeState(common::NodeId node_id,
                                                  raft::NodeState state)
    {
        common::Status status = ValidateNodeState(state);
        if (!status.ok())
        {
            return status;
        }
        auto node = nodes_.find(node_id);
        if (node == nodes_.end())
        {
            return common::Status::NotFound("simulated node not found");
        }
        node->second.snapshot.state = state;
        return common::Status::OK();
    }

    common::Status SimulatedCluster::Partition(common::NodeId first,
                                               common::NodeId second)
    {
        if (!HasNode(first) || !HasNode(second))
        {
            return common::Status::NotFound("partition references unknown node");
        }
        if (first == second)
        {
            return Invalid("cannot partition node from itself");
        }
        partitions_.insert(LinkKey(first, second));
        return common::Status::OK();
    }

    common::Status SimulatedCluster::Heal(common::NodeId first,
                                          common::NodeId second)
    {
        if (!HasNode(first) || !HasNode(second))
        {
            return common::Status::NotFound("heal references unknown node");
        }
        partitions_.erase(LinkKey(first, second));
        return common::Status::OK();
    }

    bool SimulatedCluster::IsPartitioned(common::NodeId first,
                                         common::NodeId second) const
    {
        return partitions_.find(LinkKey(first, second)) != partitions_.end();
    }

    common::Status SimulatedCluster::DequeueMessage(
        common::NodeId node_id,
        SimulatedEnvelope *envelope)
    {
        if (envelope == nullptr)
        {
            return Invalid("simulated envelope output must not be null");
        }
        auto node = nodes_.find(node_id);
        if (node == nodes_.end())
        {
            return common::Status::NotFound("simulated node not found");
        }
        if (node->second.inbox.empty())
        {
            return common::Status::NotFound("simulated node inbox is empty");
        }
        *envelope = std::move(node->second.inbox.front());
        node->second.inbox.pop_front();
        node->second.snapshot.inbox_size = node->second.inbox.size();
        return common::Status::OK();
    }

    std::size_t SimulatedCluster::InboxSize(common::NodeId node_id) const
    {
        auto node = nodes_.find(node_id);
        return node == nodes_.end() ? 0 : node->second.inbox.size();
    }

    std::vector<SimulatedNodeSnapshot> SimulatedCluster::Nodes() const
    {
        std::vector<SimulatedNodeSnapshot> result;
        result.reserve(nodes_.size());
        for (const auto &item : nodes_)
        {
            result.push_back(item.second.snapshot);
        }
        return result;
    }

    common::Status SimulatedCluster::Deliver(const raft::RaftMessage &message,
                                             raft::RaftMessage *response)
    {
        if (!started_)
        {
            return common::Status::Busy("simulated cluster is not started");
        }
        auto source = nodes_.find(message.source_node_id);
        auto target = nodes_.find(message.target_node_id);
        if (source == nodes_.end() || target == nodes_.end())
        {
            return common::Status::NotFound("simulated message references unknown node");
        }
        if (source->second.snapshot.state != raft::NodeState::RUNNING ||
            target->second.snapshot.state != raft::NodeState::RUNNING)
        {
            return common::Status::Busy("simulated message endpoint is not running");
        }
        if (IsPartitioned(message.source_node_id, message.target_node_id))
        {
            return common::Status::RetryLater("simulated link is partitioned");
        }

        SimulatedEnvelope envelope;
        envelope.sequence = next_sequence_++;
        envelope.message = message;
        target->second.inbox.push_back(envelope);
        target->second.snapshot.inbox_size = target->second.inbox.size();
        FillDeterministicResponse(message, response);
        return common::Status::OK();
    }

    bool SimulatedCluster::HasNode(common::NodeId node_id) const
    {
        return nodes_.find(node_id) != nodes_.end();
    }

    common::Status SimulatedCluster::ValidateNodeState(raft::NodeState state)
    {
        if (state == raft::NodeState::RUNNING ||
            state == raft::NodeState::STOPPED ||
            state == raft::NodeState::FAILED)
        {
            return common::Status::OK();
        }
        return Invalid("invalid simulated node state");
    }

    std::pair<common::NodeId, common::NodeId> SimulatedCluster::LinkKey(
        common::NodeId first,
        common::NodeId second)
    {
        if (first > second)
        {
            std::swap(first, second);
        }
        return {first, second};
    }

} // namespace cpr::tests
