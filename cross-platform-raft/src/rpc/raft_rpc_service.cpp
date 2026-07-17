#include "rpc/raft_rpc_service.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/types.h"
#include "raft/raft_message.h"
#include "raft/raft_runtime.h"
#include "raft/raft_storage.h"
#include "raft/raft_types.h"

namespace cpr::rpc
{
    namespace
    {

        using ProtoAppendEntriesRequest = cpr::raft::v1::AppendEntriesRequest;
        using ProtoAppendEntriesResponse = cpr::raft::v1::AppendEntriesResponse;
        using ProtoInstallSnapshotRequest = cpr::raft::v1::InstallSnapshotRequest;
        using ProtoInstallSnapshotResponse = cpr::raft::v1::InstallSnapshotResponse;
        using ProtoLogEntry = cpr::common::v1::LogEntry;
        using ProtoMembershipView = cpr::common::v1::MembershipView;
        using ProtoRequestVoteRequest = cpr::raft::v1::RequestVoteRequest;
        using ProtoRequestVoteResponse = cpr::raft::v1::RequestVoteResponse;
        using ProtoSnapshotMetadata = cpr::common::v1::SnapshotMetadata;
        using ProtoVoteRejectReason = cpr::raft::v1::VoteRejectReason;

        raft::OpaquePayload BytesToPayload(const std::string &payload)
        {
            return raft::OpaquePayload(payload.begin(), payload.end());
        }

        grpc::Status ToGrpcStatus(const common::Status &status)
        {
            switch (status.code())
            {
            case common::StatusCode::kOk:
                return grpc::Status::OK;
            case common::StatusCode::kInvalidArgument:
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                    status.message());
            case common::StatusCode::kNotFound:
                return grpc::Status(grpc::StatusCode::NOT_FOUND,
                                    status.message());
            case common::StatusCode::kBusy:
                return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                    status.message());
            case common::StatusCode::kResourceExhausted:
                return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                    status.message());
            case common::StatusCode::kRetryLater:
                return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                                    status.message());
            case common::StatusCode::kIoError:
                return grpc::Status(grpc::StatusCode::DATA_LOSS,
                                    status.message());
            case common::StatusCode::kCorruption:
                return grpc::Status(grpc::StatusCode::DATA_LOSS,
                                    status.message());
            case common::StatusCode::kInternalError:
                return grpc::Status(grpc::StatusCode::INTERNAL,
                                    status.message());
            }
            return grpc::Status(grpc::StatusCode::INTERNAL, status.ToString());
        }

        common::Status WaitForPeerResponse(raft::RaftRuntime *runtime,
                                           common::NodeId peer_id,
                                           raft::RaftMessageType expected_type,
                                           std::chrono::steady_clock::time_point deadline,
                                           std::chrono::milliseconds poll_interval,
                                           raft::RaftMessage *response)
        {
            if (response == nullptr)
            {
                return common::Status::InvalidArgument(
                    "response output must not be null");
            }

            for (;;)
            {
                if (runtime->state() != raft::RuntimeState::RUNNING)
                {
                    return common::Status::Busy("runtime is not running");
                }

                common::Status status = runtime->TryDequeuePeerMessage(peer_id, response);
                if (status.ok())
                {
                    if (response->type != expected_type)
                    {
                        return common::Status::InternalError(
                            "unexpected raft response type");
                    }
                    return common::Status::OK();
                }
                if (status.code() != common::StatusCode::kNotFound)
                {
                    return status;
                }

                if (std::chrono::steady_clock::now() >= deadline)
                {
                    return common::Status::RetryLater(
                        "timed out waiting for raft response");
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

        std::chrono::steady_clock::time_point ResolveDeadline(
            const grpc::ServerContext *context,
            std::chrono::milliseconds fallback_timeout)
        {
            const auto now_system = std::chrono::system_clock::now();
            const auto deadline_system = context->deadline();
            if (deadline_system != decltype(deadline_system)::max())
            {
                const auto remaining =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        deadline_system - now_system);
                if (remaining.count() > 0)
                {
                    return std::chrono::steady_clock::now() + remaining;
                }
            }
            return std::chrono::steady_clock::now() + fallback_timeout;
        }

        ProtoVoteRejectReason ToProtoVoteRejectReason(
            raft::VoteRejectReason reason) noexcept
        {
            switch (reason)
            {
            case raft::VoteRejectReason::NONE:
                return ProtoVoteRejectReason::VOTE_REJECT_REASON_NONE;
            case raft::VoteRejectReason::STALE_TERM:
                return ProtoVoteRejectReason::VOTE_REJECT_REASON_STALE_TERM;
            case raft::VoteRejectReason::LOG_OUTDATED:
                return ProtoVoteRejectReason::VOTE_REJECT_REASON_LOG_OUTDATED;
            case raft::VoteRejectReason::ALREADY_VOTED:
                return ProtoVoteRejectReason::VOTE_REJECT_REASON_ALREADY_VOTED;
            case raft::VoteRejectReason::LEARNER:
                return ProtoVoteRejectReason::VOTE_REJECT_REASON_LEARNER;
            }
            return ProtoVoteRejectReason::VOTE_REJECT_REASON_UNSPECIFIED;
        }

        raft::LogEntryType FromProtoLogEntryType(
            cpr::common::v1::LogEntryKind type) noexcept
        {
            switch (type)
            {
            case cpr::common::v1::LOG_ENTRY_KIND_NO_OP:
                return raft::LogEntryType::NO_OP;
            case cpr::common::v1::LOG_ENTRY_KIND_COMMAND:
                return raft::LogEntryType::COMMAND;
            case cpr::common::v1::LOG_ENTRY_KIND_MEMBERSHIP_CHANGE:
                return raft::LogEntryType::MEMBERSHIP_CHANGE;
            case cpr::common::v1::LOG_ENTRY_KIND_UNSPECIFIED:
            case cpr::common::v1::LogEntryKind_INT_MIN_SENTINEL_DO_NOT_USE_:
            case cpr::common::v1::LogEntryKind_INT_MAX_SENTINEL_DO_NOT_USE_:
                break;
            }
            return raft::LogEntryType::NO_OP;
        }

        raft::LogEntry FromProto(const ProtoLogEntry &source)
        {
            raft::LogEntry entry;
            entry.index = source.index();
            entry.term = source.term();
            entry.type = FromProtoLogEntryType(source.type());
            entry.payload = BytesToPayload(source.payload());
            return entry;
        }

        raft::RaftMember FromProto(const cpr::common::v1::ClusterMember &source)
        {
            raft::RaftMember member;
            member.node_id = source.node_id();
            member.address.host = source.address().host();
            member.address.port = static_cast<std::uint16_t>(source.address().port());
            return member;
        }

        raft::MembershipView FromProto(const ProtoMembershipView &source)
        {
            raft::MembershipView view;
            view.has_active_transition = source.has_active_transition();
            view.configuration_id = source.configuration_id();
            for (const auto &member : source.voters())
            {
                view.voters.push_back(FromProto(member));
            }
            for (const auto &member : source.learners())
            {
                view.learners.push_back(FromProto(member));
            }
            return view;
        }

        raft::SnapshotMetadata FromProto(const ProtoSnapshotMetadata &source)
        {
            raft::SnapshotMetadata metadata;
            metadata.last_included_index = source.last_included_index();
            metadata.last_included_term = source.last_included_term();
            metadata.membership = FromProto(source.membership());
            return metadata;
        }

        raft::RaftMessage ToInternalMessage(const ProtoRequestVoteRequest &request)
        {
            raft::RequestVoteRequest inner;
            inner.term = request.term();
            inner.candidate_id = request.candidate_id();
            inner.last_log_index = request.last_log_index();
            inner.last_log_term = request.last_log_term();

            raft::RaftMessage message;
            message.type = raft::RaftMessageType::REQUEST_VOTE_REQUEST;
            message.source_node_id = inner.candidate_id;
            message.target_node_id = common::kInvalidNodeId;
            message.payload = std::move(inner);
            return message;
        }

        raft::RaftMessage ToInternalMessage(const ProtoAppendEntriesRequest &request)
        {
            raft::AppendEntriesRequest inner;
            inner.term = request.term();
            inner.leader_id = request.leader_id();
            inner.prev_log_index = request.prev_log_index();
            inner.prev_log_term = request.prev_log_term();
            inner.leader_commit = request.leader_commit();
            inner.entries.reserve(
                static_cast<std::size_t>(request.entries_size()));
            for (const auto &entry : request.entries())
            {
                inner.entries.push_back(FromProto(entry));
            }

            raft::RaftMessage message;
            message.type = raft::RaftMessageType::APPEND_ENTRIES_REQUEST;
            message.source_node_id = inner.leader_id;
            message.target_node_id = common::kInvalidNodeId;
            message.payload = std::move(inner);
            return message;
        }

        raft::RaftMessage ToInternalMessage(const ProtoInstallSnapshotRequest &request)
        {
            raft::InstallSnapshotRequest inner;
            inner.term = request.term();
            inner.leader_id = request.leader_id();
            inner.metadata = FromProto(request.metadata());
            inner.payload = BytesToPayload(request.payload());

            raft::RaftMessage message;
            message.type = raft::RaftMessageType::INSTALL_SNAPSHOT_REQUEST;
            message.source_node_id = inner.leader_id;
            message.target_node_id = common::kInvalidNodeId;
            message.payload = std::move(inner);
            return message;
        }

        common::Status ValidateRequest(const ProtoRequestVoteRequest &request)
        {
            if (request.candidate_id() == common::kInvalidNodeId)
            {
                return common::Status::InvalidArgument(
                    "candidate id must be valid");
            }
            return common::Status::OK();
        }

        common::Status ValidateRequest(const ProtoAppendEntriesRequest &request)
        {
            if (request.leader_id() == common::kInvalidNodeId)
            {
                return common::Status::InvalidArgument("leader id must be valid");
            }
            if (request.prev_log_index() > common::kInvalidLogIndex &&
                request.prev_log_term() == common::kInitialTerm)
            {
                return common::Status::InvalidArgument(
                    "prev log term must be positive when prev log index is set");
            }
            return common::Status::OK();
        }

        common::Status ValidateRequest(const ProtoInstallSnapshotRequest &request)
        {
            if (request.leader_id() == common::kInvalidNodeId)
            {
                return common::Status::InvalidArgument("leader id must be valid");
            }
            if (request.metadata().last_included_index() ==
                common::kInvalidLogIndex)
            {
                return common::Status::InvalidArgument(
                    "snapshot index must be positive");
            }
            if (request.metadata().last_included_term() == common::kInitialTerm)
            {
                return common::Status::InvalidArgument(
                    "snapshot term must be positive");
            }
            return common::Status::OK();
        }

        common::Status FillResponse(const raft::RaftMessage &message,
                                    ProtoRequestVoteResponse *response)
        {
            const auto *inner =
                std::get_if<raft::RequestVoteResponse>(&message.payload);
            if (inner == nullptr)
            {
                return common::Status::InternalError(
                    "raft response payload mismatch");
            }
            response->set_term(inner->term);
            response->set_vote_granted(inner->vote_granted);
            response->set_reject_reason(ToProtoVoteRejectReason(
                inner->reject_reason));
            return common::Status::OK();
        }

        common::Status FillResponse(const raft::RaftMessage &message,
                                    ProtoAppendEntriesResponse *response)
        {
            const auto *inner =
                std::get_if<raft::AppendEntriesResponse>(&message.payload);
            if (inner == nullptr)
            {
                return common::Status::InternalError(
                    "raft response payload mismatch");
            }
            response->set_term(inner->term);
            response->set_success(inner->success);
            response->set_match_index(inner->match_index);
            response->set_conflict_index(inner->conflict_index);
            response->set_conflict_term(inner->conflict_term);
            return common::Status::OK();
        }

        common::Status FillResponse(const raft::RaftMessage &message,
                                    ProtoInstallSnapshotResponse *response)
        {
            const auto *inner =
                std::get_if<raft::InstallSnapshotResponse>(&message.payload);
            if (inner == nullptr)
            {
                return common::Status::InternalError(
                    "raft response payload mismatch");
            }
            response->set_term(inner->term);
            response->set_success(inner->success);
            response->set_last_included_index(inner->last_included_index);
            return common::Status::OK();
        }

        template <typename ProtoRequest, typename ProtoResponse>
        grpc::Status DispatchUnaryRequest(
            grpc::ServerContext *context,
            raft::RaftRuntime *runtime,
            const ProtoRequest *request,
            ProtoResponse *response,
            const RaftRpcServiceAdapter::Options &options,
            raft::RaftMessageType expected_response_type)
        {
            if (context == nullptr || request == nullptr || response == nullptr)
            {
                return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                    "grpc request context and message must not be null");
            }
            if (runtime == nullptr)
            {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                                    "raft runtime is not configured");
            }
            if (runtime->state() != raft::RuntimeState::RUNNING)
            {
                return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                    "raft runtime is not running");
            }

            common::Status validation = ValidateRequest(*request);
            if (!validation.ok())
            {
                return ToGrpcStatus(validation);
            }

            raft::RaftMessage internal = ToInternalMessage(*request);
            const common::NodeId peer_id = internal.source_node_id;
            common::Status enqueue_status =
                runtime->EnqueueMessage(peer_id, std::move(internal));
            if (!enqueue_status.ok())
            {
                return ToGrpcStatus(enqueue_status);
            }

            raft::RaftMessage internal_response;
            common::Status wait_status = WaitForPeerResponse(
                runtime,
                peer_id,
                expected_response_type,
                ResolveDeadline(context, options.response_timeout),
                options.poll_interval,
                &internal_response);
            if (!wait_status.ok())
            {
                return ToGrpcStatus(wait_status);
            }

            common::Status fill_status = FillResponse(internal_response, response);
            if (!fill_status.ok())
            {
                return ToGrpcStatus(fill_status);
            }

            return grpc::Status::OK;
        }

    } // namespace

    RaftRpcServiceAdapter::RaftRpcServiceAdapter(
        raft::RaftRuntime *runtime)
        : runtime_(runtime)
    {
    }

    RaftRpcServiceAdapter::RaftRpcServiceAdapter(
        raft::RaftRuntime *runtime,
        Options options)
        : runtime_(runtime), options_(std::move(options))
    {
    }

    grpc::Status RaftRpcServiceAdapter::RequestVote(
        grpc::ServerContext *context,
        const ProtoRequestVoteRequest *request,
        ProtoRequestVoteResponse *response)
    {
        return DispatchUnaryRequest(
            context, runtime_, request, response, options_,
            raft::RaftMessageType::REQUEST_VOTE_RESPONSE);
    }

    grpc::Status RaftRpcServiceAdapter::AppendEntries(
        grpc::ServerContext *context,
        const ProtoAppendEntriesRequest *request,
        ProtoAppendEntriesResponse *response)
    {
        return DispatchUnaryRequest(
            context, runtime_, request, response, options_,
            raft::RaftMessageType::APPEND_ENTRIES_RESPONSE);
    }

    grpc::Status RaftRpcServiceAdapter::InstallSnapshot(
        grpc::ServerContext *context,
        const ProtoInstallSnapshotRequest *request,
        ProtoInstallSnapshotResponse *response)
    {
        return DispatchUnaryRequest(
            context, runtime_, request, response, options_,
            raft::RaftMessageType::INSTALL_SNAPSHOT_RESPONSE);
    }

} // namespace cpr::rpc
