#pragma once

#include <chrono>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "raft.grpc.pb.h"

namespace cpr::raft
{
    class RaftRuntime;
}

namespace cpr::rpc
{

    class RaftRpcServiceAdapter final : public cpr::raft::v1::RaftService::Service
    {
    public:
        struct Options
        {
            std::chrono::milliseconds response_timeout{1000};
            std::chrono::milliseconds poll_interval{1};
        };

        explicit RaftRpcServiceAdapter(cpr::raft::RaftRuntime *runtime);
        RaftRpcServiceAdapter(cpr::raft::RaftRuntime *runtime,
                              Options options);

        ::grpc::Status RequestVote(
            ::grpc::ServerContext *context,
            const ::cpr::raft::v1::RequestVoteRequest *request,
            ::cpr::raft::v1::RequestVoteResponse *response) override;

        ::grpc::Status AppendEntries(
            ::grpc::ServerContext *context,
            const ::cpr::raft::v1::AppendEntriesRequest *request,
            ::cpr::raft::v1::AppendEntriesResponse *response) override;

        ::grpc::Status InstallSnapshot(
            ::grpc::ServerContext *context,
            const ::cpr::raft::v1::InstallSnapshotRequest *request,
            ::cpr::raft::v1::InstallSnapshotResponse *response) override;

    private:
        cpr::raft::RaftRuntime *runtime_ = nullptr;
        Options options_{};
    };

} // namespace cpr::rpc
