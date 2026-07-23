#pragma once

#include <chrono>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "metadata.grpc.pb.h"

namespace cpr::metadata
{
    class MetadataService;
}

namespace cpr::raft
{
    class RaftRuntime;
}

namespace cpr::rpc
{

    class MetadataRpcServiceAdapter final
        : public cpr::metadata::v1::MetadataService::Service
    {
    public:
        struct Options
        {
            std::chrono::milliseconds response_timeout{1000};
            std::chrono::milliseconds poll_interval{1};
        };

        explicit MetadataRpcServiceAdapter(cpr::raft::RaftRuntime *runtime,
                                           cpr::metadata::MetadataService *service = nullptr);
        MetadataRpcServiceAdapter(cpr::raft::RaftRuntime *runtime,
                                  cpr::metadata::MetadataService *service,
                                  Options options);

        ::grpc::Status Propose(
            ::grpc::ServerContext *context,
            const ::cpr::metadata::v1::ProposeRequest *request,
            ::cpr::metadata::v1::ProposeResponse *response) override;

        ::grpc::Status Query(
            ::grpc::ServerContext *context,
            const ::cpr::metadata::v1::QueryRequest *request,
            ::cpr::metadata::v1::QueryResponse *response) override;

        ::grpc::Status GetLeader(
            ::grpc::ServerContext *context,
            const ::cpr::metadata::v1::GetLeaderRequest *request,
            ::cpr::metadata::v1::GetLeaderResponse *response) override;

        ::grpc::Status GetStatus(
            ::grpc::ServerContext *context,
            const ::cpr::metadata::v1::GetStatusRequest *request,
            ::cpr::metadata::v1::GetStatusResponse *response) override;

        ::grpc::Status AddLearner(
            ::grpc::ServerContext *context,
            const ::cpr::metadata::v1::AddLearnerRequest *request,
            ::cpr::metadata::v1::MembershipChangeResponse *response) override;

        ::grpc::Status PromoteLearner(
            ::grpc::ServerContext *context,
            const ::cpr::metadata::v1::PromoteLearnerRequest *request,
            ::cpr::metadata::v1::MembershipChangeResponse *response) override;

        ::grpc::Status RemoveMember(
            ::grpc::ServerContext *context,
            const ::cpr::metadata::v1::RemoveMemberRequest *request,
            ::cpr::metadata::v1::MembershipChangeResponse *response) override;

    private:
        cpr::raft::RaftRuntime *runtime_ = nullptr;
        cpr::metadata::MetadataService *service_ = nullptr;
        Options options_{};
    };

} // namespace cpr::rpc
