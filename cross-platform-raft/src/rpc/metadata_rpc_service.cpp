#include "rpc/metadata_rpc_service.h"

#include <chrono>
#include <thread>
#include <utility>

#include "raft/raft_runtime.h"

namespace cpr::rpc
{
    namespace
    {

        using ProtoMember = cpr::common::v1::ClusterMember;
        using ProtoResponse = cpr::metadata::v1::MembershipChangeResponse;
        using ProtoStatusCode = cpr::common::v1::RpcStatusCode;

        raft::RaftMember FromProto(const ProtoMember &source)
        {
            raft::RaftMember member;
            member.node_id = source.node_id();
            member.address.host = source.address().host();
            member.address.port = static_cast<std::uint16_t>(source.address().port());
            return member;
        }

        grpc::Status ToGrpcStatus(const common::Status &status)
        {
            switch (status.code())
            {
            case common::StatusCode::kOk:
                return grpc::Status::OK;
            case common::StatusCode::kInvalidArgument:
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, status.message());
            case common::StatusCode::kBusy:
                return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, status.message());
            case common::StatusCode::kResourceExhausted:
                return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, status.message());
            case common::StatusCode::kRetryLater:
                return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, status.message());
            case common::StatusCode::kNotFound:
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, status.message());
            case common::StatusCode::kIoError:
            case common::StatusCode::kCorruption:
                return grpc::Status(grpc::StatusCode::DATA_LOSS, status.message());
            case common::StatusCode::kInternalError:
                return grpc::Status(grpc::StatusCode::INTERNAL, status.message());
            }
            return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
        }

        ProtoStatusCode ToBusinessCode(const common::Status &status)
        {
            if (status.code() == common::StatusCode::kOk)
            {
                return ProtoStatusCode::RPC_STATUS_CODE_OK;
            }
            if (status.code() == common::StatusCode::kInvalidArgument)
            {
                return status.message() == "not leader"
                           ? ProtoStatusCode::RPC_STATUS_CODE_NOT_LEADER
                           : ProtoStatusCode::RPC_STATUS_CODE_INVALID_ARGUMENT;
            }
            if (status.code() == common::StatusCode::kNotFound)
            {
                return status.message() == "not leader"
                           ? ProtoStatusCode::RPC_STATUS_CODE_NOT_LEADER
                           : ProtoStatusCode::RPC_STATUS_CODE_NOT_FOUND;
            }
            if (status.code() == common::StatusCode::kBusy)
            {
                return ProtoStatusCode::RPC_STATUS_CODE_BUSY;
            }
            if (status.code() == common::StatusCode::kRetryLater)
            {
                return ProtoStatusCode::RPC_STATUS_CODE_RETRY_LATER;
            }
            if (status.code() == common::StatusCode::kResourceExhausted)
            {
                return ProtoStatusCode::RPC_STATUS_CODE_RESOURCE_EXHAUSTED;
            }
            return ProtoStatusCode::RPC_STATUS_CODE_FAILED_PRECONDITION;
        }

        void FillLeaderHint(common::NodeId leader_id,
                            const raft::NodeAddress &address,
                            cpr::common::v1::LeaderHint *hint)
        {
            if (hint == nullptr || leader_id == common::kInvalidNodeId)
            {
                return;
            }
            hint->set_leader_id(leader_id);
            if (!address.host.empty())
            {
                hint->mutable_address()->set_host(address.host);
                hint->mutable_address()->set_port(address.port);
            }
        }

        std::chrono::steady_clock::time_point ResolveDeadline(
            const grpc::ServerContext *context,
            std::chrono::milliseconds fallback_timeout)
        {
            const auto now_system = std::chrono::system_clock::now();
            const auto deadline_system = context->deadline();
            if (deadline_system != decltype(deadline_system)::max())
            {
                const auto remaining =
                    std::chrono::duration_cast<std::chrono::milliseconds>(deadline_system - now_system);
                if (remaining.count() > 0)
                {
                    return std::chrono::steady_clock::now() + remaining;
                }
            }
            return std::chrono::steady_clock::now() + fallback_timeout;
        }

        grpc::Status WaitForMembershipResult(
            grpc::ServerContext *context,
            raft::RaftRuntime *runtime,
            const std::string &request_id,
            std::chrono::steady_clock::time_point deadline,
            std::chrono::milliseconds poll_interval,
            ProtoResponse *response)
        {
            while (true)
            {
                if (context->IsCancelled())
                {
                    return grpc::Status(grpc::StatusCode::CANCELLED, "request cancelled");
                }

                raft::MembershipChangeResult result;
                if (runtime->TryTakeMembershipChangeResult(request_id, &result))
                {
                    response->set_log_index(result.log_index);
                    response->set_term(result.term);
                    response->mutable_status()->set_code(ToBusinessCode(result.status));
                    response->mutable_status()->set_message(result.status.message());
                    FillLeaderHint(result.leader_id, result.leader_address, response->mutable_leader());
                    return grpc::Status::OK;
                }

                if (runtime->state() != raft::RuntimeState::RUNNING)
                {
                    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "runtime is not running");
                }
                if (std::chrono::steady_clock::now() >= deadline)
                {
                    return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED,
                                        "timed out waiting for membership result");
                }

                if (poll_interval.count() <= 0)
                {
                    std::this_thread::yield();
                }
                else
                {
                    std::this_thread::sleep_for(poll_interval);
                }
            }
        }

    } // namespace

    MetadataRpcServiceAdapter::MetadataRpcServiceAdapter(raft::RaftRuntime *runtime)
        : runtime_(runtime)
    {
    }

    MetadataRpcServiceAdapter::MetadataRpcServiceAdapter(raft::RaftRuntime *runtime,
                                                         Options options)
        : runtime_(runtime),
          options_(std::move(options))
    {
    }

    grpc::Status MetadataRpcServiceAdapter::AddLearner(
        grpc::ServerContext *context,
        const cpr::metadata::v1::AddLearnerRequest *request,
        ProtoResponse *response)
    {
        if (runtime_ == nullptr || context == nullptr || request == nullptr || response == nullptr)
        {
            return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "metadata rpc is not initialized");
        }
        if (request->request_id().empty())
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "request id must not be empty");
        }

        raft::MembershipChangeEvent event;
        event.kind = raft::MembershipChangeEvent::Kind::ADD_LEARNER;
        event.request_id = request->request_id();
        event.member = FromProto(request->learner());
        const common::Status enqueue_status = runtime_->EnqueueMembershipChange(std::move(event));
        if (!enqueue_status.ok())
        {
            return ToGrpcStatus(enqueue_status);
        }

        return WaitForMembershipResult(context,
                                       runtime_,
                                       request->request_id(),
                                       ResolveDeadline(context, std::chrono::milliseconds(request->timeout_ms() > 0 ? request->timeout_ms() : options_.response_timeout.count())),
                                       options_.poll_interval,
                                       response);
    }

    grpc::Status MetadataRpcServiceAdapter::PromoteLearner(
        grpc::ServerContext *context,
        const cpr::metadata::v1::PromoteLearnerRequest *request,
        ProtoResponse *response)
    {
        if (runtime_ == nullptr || context == nullptr || request == nullptr || response == nullptr)
        {
            return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "metadata rpc is not initialized");
        }
        if (request->request_id().empty() || request->node_id() == common::kInvalidNodeId)
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "request id and node id must be valid");
        }

        raft::MembershipChangeEvent event;
        event.kind = raft::MembershipChangeEvent::Kind::PROMOTE_LEARNER;
        event.request_id = request->request_id();
        event.target_node_id = request->node_id();
        const common::Status enqueue_status = runtime_->EnqueueMembershipChange(std::move(event));
        if (!enqueue_status.ok())
        {
            return ToGrpcStatus(enqueue_status);
        }

        return WaitForMembershipResult(context,
                                       runtime_,
                                       request->request_id(),
                                       ResolveDeadline(context, std::chrono::milliseconds(request->timeout_ms() > 0 ? request->timeout_ms() : options_.response_timeout.count())),
                                       options_.poll_interval,
                                       response);
    }

    grpc::Status MetadataRpcServiceAdapter::RemoveMember(
        grpc::ServerContext *context,
        const cpr::metadata::v1::RemoveMemberRequest *request,
        ProtoResponse *response)
    {
        if (runtime_ == nullptr || context == nullptr || request == nullptr || response == nullptr)
        {
            return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "metadata rpc is not initialized");
        }
        if (request->request_id().empty() || request->node_id() == common::kInvalidNodeId)
        {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "request id and node id must be valid");
        }

        raft::MembershipChangeEvent event;
        event.kind = raft::MembershipChangeEvent::Kind::REMOVE_MEMBER;
        event.request_id = request->request_id();
        event.target_node_id = request->node_id();
        const common::Status enqueue_status = runtime_->EnqueueMembershipChange(std::move(event));
        if (!enqueue_status.ok())
        {
            return ToGrpcStatus(enqueue_status);
        }

        return WaitForMembershipResult(context,
                                       runtime_,
                                       request->request_id(),
                                       ResolveDeadline(context, std::chrono::milliseconds(request->timeout_ms() > 0 ? request->timeout_ms() : options_.response_timeout.count())),
                                       options_.poll_interval,
                                       response);
    }

} // namespace cpr::rpc
