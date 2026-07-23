#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "config/config.h"
#include "metadata/metadata_service.h"
#include "metadata/metadata_state_machine.h"
#include "raft/file_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"
#include "rpc/metadata_rpc_service.h"
#include "rpc/raft_rpc_service.h"
#include "rpc/store_rpc_service.h"
#include "server/server_lifecycle.h"
#include "transport/grpc_raft_transport.h"

namespace grpc
{
    class Server;
}

namespace cpr::server
{

    class ServerApplication
    {
    public:
        ServerApplication();
        ~ServerApplication();

        ServerApplication(const ServerApplication &) = delete;
        ServerApplication &operator=(const ServerApplication &) = delete;

        common::Status Initialize(const config::Config &config);
        common::Status Start();
        common::Status Stop();

        bool IsRunning() const noexcept;
        bool HasShutdownRequest() const noexcept;
        void RequestStop() noexcept;

        std::string raft_endpoint() const;
        std::string metadata_endpoint() const;
        std::string store_endpoint() const;

    private:
        struct ParsedMember
        {
            common::NodeId node_id = common::kInvalidNodeId;
            config::InitialMember member;
        };

        common::Status BuildBootstrapMembers(std::vector<ParsedMember> *members,
                                             ParsedMember *local_member) const;
        common::Status BuildRaftCoreOptions(const ParsedMember &local_member,
                                            const std::vector<ParsedMember> &members,
                                            raft::RaftCore::Options *options) const;
        common::Status StartGrpcServer();
        void TickLoop();
        void PeerPumpLoop();
        void JoinThreads();
        std::string EndpointForPort(std::uint16_t port) const;

        config::Config config_{};
        ServerLifecycle lifecycle_;
        bool initialized_ = false;
        std::atomic<bool> running_{false};
        std::atomic<bool> stop_requested_{false};

        std::unique_ptr<raft::FileRaftStorage> storage_;
        std::unique_ptr<raft::RaftCore> core_;
        std::unique_ptr<raft::RaftRuntime> runtime_;
        std::unique_ptr<transport::GrpcRaftTransport> transport_;
        std::unique_ptr<metadata::MetadataStateMachine> state_machine_;
        std::unique_ptr<metadata::MetadataService> metadata_service_;
        std::unique_ptr<rpc::RaftRpcServiceAdapter> raft_rpc_;
        std::unique_ptr<rpc::MetadataRpcServiceAdapter> metadata_rpc_;
        std::unique_ptr<rpc::StoreRpcServiceAdapter> store_rpc_;
        std::unique_ptr<grpc::Server> grpc_server_;

        std::vector<common::NodeId> peer_ids_;
        std::thread tick_thread_;
        std::thread peer_pump_thread_;
    };

} // namespace cpr::server
