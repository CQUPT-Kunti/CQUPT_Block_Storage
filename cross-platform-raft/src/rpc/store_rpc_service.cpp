#include "rpc/store_rpc_service.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>

#include "metadata/metadata_service.h"
#include "store/store_types.h"
#include "store/task_types.h"

namespace cpr::rpc
{
    namespace
    {

        using ProtoStatusCode = ::cpr::common::v1::RpcStatusCode;

        ProtoStatusCode ToProtoStatusCode(const common::Status &status)
        {
            if (status.ok())
            {
                return ProtoStatusCode::RPC_STATUS_CODE_OK;
            }
            if (status.message().find("not leader") != std::string::npos)
            {
                return ProtoStatusCode::RPC_STATUS_CODE_NOT_LEADER;
            }
            switch (status.code())
            {
            case common::StatusCode::kInvalidArgument:
                return ProtoStatusCode::RPC_STATUS_CODE_INVALID_ARGUMENT;
            case common::StatusCode::kNotFound:
                return ProtoStatusCode::RPC_STATUS_CODE_NOT_FOUND;
            case common::StatusCode::kBusy:
                return ProtoStatusCode::RPC_STATUS_CODE_BUSY;
            case common::StatusCode::kResourceExhausted:
                return ProtoStatusCode::RPC_STATUS_CODE_RESOURCE_EXHAUSTED;
            case common::StatusCode::kRetryLater:
                return ProtoStatusCode::RPC_STATUS_CODE_RETRY_LATER;
            case common::StatusCode::kIoError:
                return ProtoStatusCode::RPC_STATUS_CODE_IO_ERROR;
            case common::StatusCode::kCorruption:
                return ProtoStatusCode::RPC_STATUS_CODE_CORRUPTION;
            case common::StatusCode::kInternalError:
                return ProtoStatusCode::RPC_STATUS_CODE_INTERNAL_ERROR;
            case common::StatusCode::kOk:
                return ProtoStatusCode::RPC_STATUS_CODE_OK;
            }
            return ProtoStatusCode::RPC_STATUS_CODE_INTERNAL_ERROR;
        }

        void FillStatus(const common::Status &status,
                        ::cpr::common::v1::RpcStatus *out)
        {
            if (out == nullptr)
            {
                return;
            }
            out->set_code(ToProtoStatusCode(status));
            out->set_message(status.message());
        }

        void FillStoreStatus(const common::Status &status,
                             const metadata::StoreControlResult &result,
                             ::cpr::common::v1::RpcStatus *out)
        {
            FillStatus(status, out);
            if (out != nullptr && !status.ok() &&
                result.leader_id != common::kInvalidNodeId &&
                result.applied_index == common::kInvalidLogIndex &&
                (status.code() == common::StatusCode::kInvalidArgument ||
                 status.code() == common::StatusCode::kNotFound))
            {
                out->set_code(ProtoStatusCode::RPC_STATUS_CODE_NOT_LEADER);
            }
        }

        void FillLeaderHint(const metadata::StoreControlResult &result,
                            ::cpr::common::v1::LeaderHint *hint)
        {
            if (hint == nullptr || result.leader_id == common::kInvalidNodeId)
            {
                return;
            }
            hint->set_leader_id(result.leader_id);
            if (!result.leader_address.host.empty())
            {
                hint->mutable_address()->set_host(result.leader_address.host);
                hint->mutable_address()->set_port(result.leader_address.port);
            }
        }

        store::StoreState FromProto(::cpr::store::v1::StoreState state)
        {
            switch (state)
            {
            case ::cpr::store::v1::STORE_STATE_STOPPED:
                return store::StoreState::STOPPED;
            case ::cpr::store::v1::STORE_STATE_FAILED:
                return store::StoreState::FAILED;
            case ::cpr::store::v1::STORE_STATE_RUNNING:
            case ::cpr::store::v1::STORE_STATE_UNSPECIFIED:
            default:
                return store::StoreState::RUNNING;
            }
        }

        ::cpr::store::v1::StoreState ToProto(store::StoreState state)
        {
            switch (state)
            {
            case store::StoreState::RUNNING:
                return ::cpr::store::v1::STORE_STATE_RUNNING;
            case store::StoreState::STOPPED:
                return ::cpr::store::v1::STORE_STATE_STOPPED;
            case store::StoreState::FAILED:
                return ::cpr::store::v1::STORE_STATE_FAILED;
            }
            return ::cpr::store::v1::STORE_STATE_UNSPECIFIED;
        }

        store::TaskState FromProto(::cpr::store::v1::TaskState state)
        {
            switch (state)
            {
            case ::cpr::store::v1::TASK_STATE_SUCCESS:
                return store::TaskState::SUCCESS;
            case ::cpr::store::v1::TASK_STATE_FAILED:
                return store::TaskState::FAILED;
            case ::cpr::store::v1::TASK_STATE_RUNNING:
                return store::TaskState::RUNNING;
            case ::cpr::store::v1::TASK_STATE_WAITING:
            case ::cpr::store::v1::TASK_STATE_UNSPECIFIED:
            default:
                return store::TaskState::WAITING;
            }
        }

        ::cpr::store::v1::TaskState ToProto(store::TaskState state)
        {
            switch (state)
            {
            case store::TaskState::WAITING:
                return ::cpr::store::v1::TASK_STATE_WAITING;
            case store::TaskState::RUNNING:
                return ::cpr::store::v1::TASK_STATE_RUNNING;
            case store::TaskState::SUCCESS:
                return ::cpr::store::v1::TASK_STATE_SUCCESS;
            case store::TaskState::FAILED:
                return ::cpr::store::v1::TASK_STATE_FAILED;
            }
            return ::cpr::store::v1::TASK_STATE_UNSPECIFIED;
        }

        ::cpr::store::v1::TaskType ToProto(store::TaskType type)
        {
            switch (type)
            {
            case store::TaskType::CREATE:
                return ::cpr::store::v1::TASK_TYPE_CREATE;
            case store::TaskType::DELETE:
                return ::cpr::store::v1::TASK_TYPE_DELETE;
            case store::TaskType::COPY:
                return ::cpr::store::v1::TASK_TYPE_COPY;
            case store::TaskType::CUSTOM:
                return ::cpr::store::v1::TASK_TYPE_CUSTOM;
            }
            return ::cpr::store::v1::TASK_TYPE_UNSPECIFIED;
        }

        store::StoreInfo FromProto(const ::cpr::store::v1::StoreDescriptor &source)
        {
            store::StoreInfo info;
            info.id = source.store_id();
            info.address.host = source.address().host();
            info.address.port = static_cast<std::uint16_t>(source.address().port());
            info.capacity_bytes = source.total_capacity_bytes();
            info.used_bytes = source.used_capacity_bytes();
            info.state = FromProto(source.state());
            info.generation = source.generation();
            return info;
        }

        void FillStore(const store::StoreInfo &source,
                       ::cpr::store::v1::StoreDescriptor *target)
        {
            if (target == nullptr || source.id == 0)
            {
                return;
            }
            target->set_store_id(source.id);
            target->mutable_address()->set_host(source.address.host);
            target->mutable_address()->set_port(source.address.port);
            target->set_total_capacity_bytes(source.capacity_bytes);
            target->set_used_capacity_bytes(source.used_bytes);
            target->set_state(ToProto(source.state));
            target->set_generation(source.generation);
        }

        void FillTask(const store::TaskRecord &source,
                      ::cpr::store::v1::TaskAssignment *target)
        {
            if (target == nullptr)
            {
                return;
            }
            target->set_task_id(source.task_id);
            target->set_type(ToProto(source.type));
            target->set_state(ToProto(source.state));
            target->set_target_payload(source.target_payload.data(),
                                       source.target_payload.size());
            target->set_sequence(source.sequence);
        }

        std::chrono::milliseconds TimeoutOrDefault(
            std::uint64_t timeout_ms,
            std::chrono::milliseconds fallback)
        {
            if (timeout_ms == 0)
            {
                return fallback;
            }
            const std::uint64_t capped =
                std::min<std::uint64_t>(timeout_ms,
                                        static_cast<std::uint64_t>(std::chrono::milliseconds::max().count()));
            return std::chrono::milliseconds(capped);
        }

        grpc::Status RequireInitialized(const metadata::MetadataService *service,
                                        const void *context,
                                        const void *request,
                                        const void *response)
        {
            if (service == nullptr || context == nullptr ||
                request == nullptr || response == nullptr)
            {
                return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                    "store rpc is not initialized");
            }
            return grpc::Status::OK;
        }

    } // namespace

    StoreRpcServiceAdapter::StoreRpcServiceAdapter(metadata::MetadataService *service)
        : service_(service)
    {
    }

    StoreRpcServiceAdapter::StoreRpcServiceAdapter(metadata::MetadataService *service,
                                                   Options options)
        : service_(service),
          options_(std::move(options))
    {
    }

    grpc::Status StoreRpcServiceAdapter::Register(
        grpc::ServerContext *context,
        const ::cpr::store::v1::RegisterStoreRequest *request,
        ::cpr::store::v1::StoreCommandResponse *response)
    {
        grpc::Status init = RequireInitialized(service_, context, request, response);
        if (!init.ok())
        {
            return init;
        }
        if (request->request_id().empty())
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                "request id must not be empty");
        }

        metadata::StoreControlResult result;
        const common::Status status = service_->RegisterStore(
            request->request_id(),
            FromProto(request->store()),
            TimeoutOrDefault(request->timeout_ms(), options_.response_timeout),
            &result);
        FillStoreStatus(status, result, response->mutable_status());
        response->set_log_index(result.applied_index);
        FillLeaderHint(result, response->mutable_leader());
        FillStore(result.store, response->mutable_store());
        return grpc::Status::OK;
    }

    grpc::Status StoreRpcServiceAdapter::Heartbeat(
        grpc::ServerContext *context,
        const ::cpr::store::v1::StoreHeartbeatRequest *request,
        ::cpr::store::v1::StoreHeartbeatResponse *response)
    {
        grpc::Status init = RequireInitialized(service_, context, request, response);
        if (!init.ok())
        {
            return init;
        }
        store::StoreInfo store;
        const common::Status status = service_->UpdateStoreHeartbeat(
            request->store_id(),
            static_cast<std::int64_t>(request->generation()),
            &store);
        FillStatus(status, response->mutable_status());
        return grpc::Status::OK;
    }

    grpc::Status StoreRpcServiceAdapter::Stop(
        grpc::ServerContext *context,
        const ::cpr::store::v1::StoreStopRequest *request,
        ::cpr::store::v1::StoreCommandResponse *response)
    {
        grpc::Status init = RequireInitialized(service_, context, request, response);
        if (!init.ok())
        {
            return init;
        }
        if (request->request_id().empty())
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                "request id must not be empty");
        }

        metadata::StoreControlResult result;
        const common::Status status = service_->StopStore(
            request->request_id(),
            request->store_id(),
            request->generation(),
            TimeoutOrDefault(request->timeout_ms(), options_.response_timeout),
            &result);
        FillStoreStatus(status, result, response->mutable_status());
        response->set_log_index(result.applied_index);
        FillLeaderHint(result, response->mutable_leader());
        FillStore(result.store, response->mutable_store());
        return grpc::Status::OK;
    }

    grpc::Status StoreRpcServiceAdapter::Remove(
        grpc::ServerContext *context,
        const ::cpr::store::v1::StoreRemoveRequest *request,
        ::cpr::store::v1::StoreCommandResponse *response)
    {
        grpc::Status init = RequireInitialized(service_, context, request, response);
        if (!init.ok())
        {
            return init;
        }
        if (request->request_id().empty())
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                "request id must not be empty");
        }

        metadata::StoreControlResult result;
        const common::Status status = service_->RemoveStore(
            request->request_id(),
            request->store_id(),
            request->generation(),
            TimeoutOrDefault(request->timeout_ms(), options_.response_timeout),
            &result);
        FillStoreStatus(status, result, response->mutable_status());
        response->set_log_index(result.applied_index);
        FillLeaderHint(result, response->mutable_leader());
        FillStore(result.store, response->mutable_store());
        return grpc::Status::OK;
    }

    grpc::Status StoreRpcServiceAdapter::PollTasks(
        grpc::ServerContext *context,
        const ::cpr::store::v1::PollTasksRequest *request,
        ::cpr::store::v1::PollTasksResponse *response)
    {
        grpc::Status init = RequireInitialized(service_, context, request, response);
        if (!init.ok())
        {
            return init;
        }

        metadata::StoreControlResult result;
        const common::Status status = service_->PollTasks(
            request->store_id(),
            request->max_tasks(),
            options_.response_timeout,
            &result);
        FillStoreStatus(status, result, response->mutable_status());
        if (status.ok())
        {
            for (const store::TaskRecord &task : result.tasks)
            {
                FillTask(task, response->add_tasks());
            }
        }
        return grpc::Status::OK;
    }

    grpc::Status StoreRpcServiceAdapter::ReportTaskResult(
        grpc::ServerContext *context,
        const ::cpr::store::v1::ReportTaskResultRequest *request,
        ::cpr::store::v1::ReportTaskResultResponse *response)
    {
        grpc::Status init = RequireInitialized(service_, context, request, response);
        if (!init.ok())
        {
            return init;
        }

        store::TaskResultReport report;
        report.store_id = request->store_id();
        report.task_id = request->task_id();
        report.final_state = FromProto(request->final_state());
        report.result_payload.assign(request->result_payload().begin(),
                                     request->result_payload().end());

        metadata::StoreControlResult result;
        const common::Status status =
            service_->ReportTaskResult(report, options_.response_timeout, &result);
        FillStatus(status, response->mutable_status());
        response->set_duplicate_result(result.duplicate_result);
        if (status.ok() && !result.tasks.empty())
        {
            FillTask(result.tasks.front(), response->mutable_task());
        }
        return grpc::Status::OK;
    }

} // namespace cpr::rpc
