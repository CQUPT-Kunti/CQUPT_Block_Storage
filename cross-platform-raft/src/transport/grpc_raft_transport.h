#pragma once

#include <memory>

#include "transport/raft_transport.h"

namespace cpr::transport
{

    class GrpcRaftTransport final : public IRaftTransport
    {
    public:
        explicit GrpcRaftTransport(
            RaftTransportOptions options = RaftTransportOptions{});
        ~GrpcRaftTransport() override;

        GrpcRaftTransport(const GrpcRaftTransport &) = delete;
        GrpcRaftTransport &operator=(const GrpcRaftTransport &) = delete;

        common::Status Initialize(
            common::NodeId local_node_id,
            const std::vector<raft::RaftMember> &peers) override;
        common::Status Start() override;
        common::Status Stop() override;
        common::Status Send(const raft::RaftMessage &message,
                            raft::RaftMessage *response) override;

    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

} // namespace cpr::transport
