#pragma once

#include <chrono>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "store.grpc.pb.h"

namespace cpr::metadata
{
    class MetadataService;
}

namespace cpr::rpc
{

    class StoreRpcServiceAdapter final
        : public ::cpr::store::v1::StoreControlService::Service
    {
    public:
        struct Options
        {
            std::chrono::milliseconds response_timeout{1000};
        };

        explicit StoreRpcServiceAdapter(cpr::metadata::MetadataService *service);
        StoreRpcServiceAdapter(cpr::metadata::MetadataService *service,
                               Options options);

        ::grpc::Status Register(
            ::grpc::ServerContext *context,
            const ::cpr::store::v1::RegisterStoreRequest *request,
            ::cpr::store::v1::StoreCommandResponse *response) override;

        ::grpc::Status Heartbeat(
            ::grpc::ServerContext *context,
            const ::cpr::store::v1::StoreHeartbeatRequest *request,
            ::cpr::store::v1::StoreHeartbeatResponse *response) override;

        ::grpc::Status Stop(
            ::grpc::ServerContext *context,
            const ::cpr::store::v1::StoreStopRequest *request,
            ::cpr::store::v1::StoreCommandResponse *response) override;

        ::grpc::Status Remove(
            ::grpc::ServerContext *context,
            const ::cpr::store::v1::StoreRemoveRequest *request,
            ::cpr::store::v1::StoreCommandResponse *response) override;

        ::grpc::Status PollTasks(
            ::grpc::ServerContext *context,
            const ::cpr::store::v1::PollTasksRequest *request,
            ::cpr::store::v1::PollTasksResponse *response) override;

        ::grpc::Status ReportTaskResult(
            ::grpc::ServerContext *context,
            const ::cpr::store::v1::ReportTaskResultRequest *request,
            ::cpr::store::v1::ReportTaskResultResponse *response) override;

    private:
        cpr::metadata::MetadataService *service_ = nullptr;
        Options options_{};
    };

} // namespace cpr::rpc
