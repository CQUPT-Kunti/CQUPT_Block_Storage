#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "common/types.h"
#include "raft/raft_message.h"
#include "raft/raft_types.h"

namespace cpr::transport
{

    struct RaftTransportOptions
    {
        std::uint64_t rpc_timeout_ms = 1000;
    };

    class IRaftTransport
    {
    public:
        virtual ~IRaftTransport();

        virtual common::Status Initialize(
            common::NodeId local_node_id,
            const std::vector<raft::RaftMember> &peers) = 0;
        virtual common::Status Start() = 0;
        virtual common::Status Stop() = 0;

        // Sends one outbound Raft request and returns the unary response.
        virtual common::Status Send(const raft::RaftMessage &message,
                                    raft::RaftMessage *response) = 0;
    };

} // namespace cpr::transport
