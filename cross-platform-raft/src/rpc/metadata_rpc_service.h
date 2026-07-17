#pragma once

#include <chrono>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "metadata.grpc.pb.h"

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

        explicit MetadataRpcServiceAdapter(cpr::raft::RaftRuntime *runtime);
        MetadataRpcServiceAdapter(cpr::raft::RaftRuntime *runtime, Options options);

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
        Options options_{};
    };

} // namespace cpr::rpc
