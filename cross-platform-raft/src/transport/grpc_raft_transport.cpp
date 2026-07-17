#include "transport/grpc_raft_transport.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/grpcpp.h>

#include "raft.grpc.pb.h"

namespace cpr::transport
{
namespace detail
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

common::Status MakeInvalid(const std::string &message)
{
    return common::Status::InvalidArgument(message);
}

std::string AddressToTarget(const raft::NodeAddress &address)
{
    return address.host + ":" + std::to_string(address.port);
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

raft::VoteRejectReason FromProtoVoteRejectReason(
    ProtoVoteRejectReason reason) noexcept
{
    switch (reason)
    {
    case ProtoVoteRejectReason::VOTE_REJECT_REASON_NONE:
        return raft::VoteRejectReason::NONE;
    case ProtoVoteRejectReason::VOTE_REJECT_REASON_STALE_TERM:
        return raft::VoteRejectReason::STALE_TERM;
    case ProtoVoteRejectReason::VOTE_REJECT_REASON_LOG_OUTDATED:
        return raft::VoteRejectReason::LOG_OUTDATED;
    case ProtoVoteRejectReason::VOTE_REJECT_REASON_ALREADY_VOTED:
        return raft::VoteRejectReason::ALREADY_VOTED;
    case ProtoVoteRejectReason::VOTE_REJECT_REASON_LEARNER:
        return raft::VoteRejectReason::LEARNER;
    case ProtoVoteRejectReason::VOTE_REJECT_REASON_UNSPECIFIED:
    case ProtoVoteRejectReason::VoteRejectReason_INT_MIN_SENTINEL_DO_NOT_USE_:
    case ProtoVoteRejectReason::VoteRejectReason_INT_MAX_SENTINEL_DO_NOT_USE_:
        break;
    }
    return raft::VoteRejectReason::NONE;
}

void ToProto(const raft::NodeAddress &source,
             cpr::common::v1::NodeAddress *target)
{
    target->set_host(source.host);
    target->set_port(source.port);
}

void ToProto(const raft::RaftMember &source,
             cpr::common::v1::ClusterMember *target)
{
    target->set_node_id(source.node_id);
    ToProto(source.address, target->mutable_address());
}

void ToProto(const raft::MembershipView &source, ProtoMembershipView *target)
{
    target->clear_voters();
    target->clear_learners();
    for (const raft::RaftMember &member : source.voters)
    {
        ToProto(member, target->add_voters());
    }
    for (const raft::RaftMember &member : source.learners)
    {
        ToProto(member, target->add_learners());
    }
    target->set_has_active_transition(source.has_active_transition);
    target->set_configuration_id(source.configuration_id);
}

void ToProto(const raft::LogEntry &source, ProtoLogEntry *target)
{
    target->set_index(source.index);
    target->set_term(source.term);
    switch (source.type)
    {
    case raft::LogEntryType::NO_OP:
        target->set_type(cpr::common::v1::LOG_ENTRY_KIND_NO_OP);
        break;
    case raft::LogEntryType::COMMAND:
        target->set_type(cpr::common::v1::LOG_ENTRY_KIND_COMMAND);
        break;
    case raft::LogEntryType::MEMBERSHIP_CHANGE:
        target->set_type(cpr::common::v1::LOG_ENTRY_KIND_MEMBERSHIP_CHANGE);
        break;
    }
    target->set_payload(
        reinterpret_cast<const char *>(source.payload.data()),
        static_cast<int>(source.payload.size()));
}

void ToProto(const raft::SnapshotMetadata &source,
             ProtoSnapshotMetadata *target)
{
    target->set_last_included_index(source.last_included_index);
    target->set_last_included_term(source.last_included_term);
    ToProto(source.membership, target->mutable_membership());
}

void ToProto(const raft::RequestVoteRequest &source,
             ProtoRequestVoteRequest *target)
{
    target->set_term(source.term);
    target->set_candidate_id(source.candidate_id);
    target->set_last_log_index(source.last_log_index);
    target->set_last_log_term(source.last_log_term);
}

void ToProto(const raft::AppendEntriesRequest &source,
             ProtoAppendEntriesRequest *target)
{
    target->set_term(source.term);
    target->set_leader_id(source.leader_id);
    target->set_prev_log_index(source.prev_log_index);
    target->set_prev_log_term(source.prev_log_term);
    target->set_leader_commit(source.leader_commit);
    target->clear_entries();
    for (const raft::LogEntry &entry : source.entries)
    {
        ToProto(entry, target->add_entries());
    }
}

void ToProto(const raft::InstallSnapshotRequest &source,
             ProtoInstallSnapshotRequest *target)
{
    target->set_term(source.term);
    target->set_leader_id(source.leader_id);
    ToProto(source.metadata, target->mutable_metadata());
    target->set_payload(
        reinterpret_cast<const char *>(source.payload.data()),
        static_cast<int>(source.payload.size()));
}

raft::OpaquePayload BytesToPayload(const std::string &payload)
{
    return raft::OpaquePayload(payload.begin(), payload.end());
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

raft::RaftMessage BuildResponseMessage(
    common::NodeId local_node_id, common::NodeId remote_node_id,
    const ProtoRequestVoteResponse &source)
{
    raft::RequestVoteResponse response;
    response.term = source.term();
    response.vote_granted = source.vote_granted();
    response.reject_reason = FromProtoVoteRejectReason(source.reject_reason());

    raft::RaftMessage message;
    message.type = raft::RaftMessageType::REQUEST_VOTE_RESPONSE;
    message.source_node_id = remote_node_id;
    message.target_node_id = local_node_id;
    message.payload = std::move(response);
    return message;
}

raft::RaftMessage BuildResponseMessage(
    common::NodeId local_node_id, common::NodeId remote_node_id,
    const ProtoAppendEntriesResponse &source)
{
    raft::AppendEntriesResponse response;
    response.term = source.term();
    response.success = source.success();
    response.match_index = source.match_index();
    response.conflict_index = source.conflict_index();
    response.conflict_term = source.conflict_term();

    raft::RaftMessage message;
    message.type = raft::RaftMessageType::APPEND_ENTRIES_RESPONSE;
    message.source_node_id = remote_node_id;
    message.target_node_id = local_node_id;
    message.payload = std::move(response);
    return message;
}

raft::RaftMessage BuildResponseMessage(
    common::NodeId local_node_id, common::NodeId remote_node_id,
    const ProtoInstallSnapshotResponse &source)
{
    raft::InstallSnapshotResponse response;
    response.term = source.term();
    response.success = source.success();
    response.last_included_index = source.last_included_index();

    raft::RaftMessage message;
    message.type = raft::RaftMessageType::INSTALL_SNAPSHOT_RESPONSE;
    message.source_node_id = remote_node_id;
    message.target_node_id = local_node_id;
    message.payload = std::move(response);
    return message;
}

common::Status ToCommonStatus(const ::grpc::Status &status)
{
    if (status.ok())
    {
        return common::Status::OK();
    }

    using grpc::StatusCode;
    switch (status.error_code())
    {
    case StatusCode::INVALID_ARGUMENT:
        return common::Status::InvalidArgument(status.error_message());
    case StatusCode::NOT_FOUND:
        return common::Status::NotFound(status.error_message());
    case StatusCode::RESOURCE_EXHAUSTED:
        return common::Status::ResourceExhausted(status.error_message());
    case StatusCode::DEADLINE_EXCEEDED:
    case StatusCode::UNAVAILABLE:
        return common::Status::RetryLater(status.error_message());
    default:
        return common::Status::InternalError(status.error_message());
    }
}

common::Status ValidateRequestMessage(const raft::RaftMessage &message)
{
    if (message.source_node_id == common::kInvalidNodeId)
    {
        return MakeInvalid("source node id must not be invalid");
    }
    if (message.target_node_id == common::kInvalidNodeId)
    {
        return MakeInvalid("target node id must not be invalid");
    }

    switch (message.type)
    {
    case raft::RaftMessageType::REQUEST_VOTE_REQUEST:
        if (!std::holds_alternative<raft::RequestVoteRequest>(message.payload))
        {
            return MakeInvalid(
                "request vote message payload type does not match");
        }
        return common::Status::OK();
    case raft::RaftMessageType::APPEND_ENTRIES_REQUEST:
        if (!std::holds_alternative<raft::AppendEntriesRequest>(message.payload))
        {
            return MakeInvalid(
                "append entries message payload type does not match");
        }
        return common::Status::OK();
    case raft::RaftMessageType::INSTALL_SNAPSHOT_REQUEST:
        if (!std::holds_alternative<raft::InstallSnapshotRequest>(message.payload))
        {
            return MakeInvalid(
                "install snapshot message payload type does not match");
        }
        return common::Status::OK();
    case raft::RaftMessageType::REQUEST_VOTE_RESPONSE:
    case raft::RaftMessageType::APPEND_ENTRIES_RESPONSE:
    case raft::RaftMessageType::INSTALL_SNAPSHOT_RESPONSE:
        return MakeInvalid(
            "transport only sends outbound request messages; response "
            "messages are returned by the remote unary RPC");
    }

    return MakeInvalid("unknown raft message type");
}

} // namespace detail

struct GrpcRaftTransport::Impl
{
    enum class State : std::uint8_t
    {
        CREATED,
        INITIALIZED,
        STARTED,
        STOPPED,
    };

    explicit Impl(RaftTransportOptions transport_options)
        : options(transport_options) {}

    RaftTransportOptions options;
    common::NodeId local_node_id = common::kInvalidNodeId;
    State state = State::CREATED;
    std::unordered_map<common::NodeId, raft::RaftMember> peers;
    std::unordered_map<common::NodeId,
                       std::unique_ptr<cpr::raft::v1::RaftService::Stub>>
        stubs;
    std::mutex mutex;
};

GrpcRaftTransport::GrpcRaftTransport(RaftTransportOptions options)
    : impl_(std::make_unique<Impl>(options))
{
}

GrpcRaftTransport::~GrpcRaftTransport()
{
    const common::Status stop_status = Stop();
    static_cast<void>(stop_status);
}

common::Status GrpcRaftTransport::Initialize(
    common::NodeId local_node_id, const std::vector<raft::RaftMember> &peers)
{
    if (local_node_id == common::kInvalidNodeId)
    {
        return common::Status::InvalidArgument(
            "local node id must not be invalid");
    }

    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->state == Impl::State::STARTED)
    {
        return common::Status::Busy(
            "transport must be stopped before reinitialization");
    }

    impl_->local_node_id = local_node_id;
    impl_->peers.clear();
    impl_->stubs.clear();

    for (const raft::RaftMember &peer : peers)
    {
        if (peer.node_id == common::kInvalidNodeId)
        {
            return common::Status::InvalidArgument(
                "peer node id must not be invalid");
        }
        if (peer.node_id == local_node_id)
        {
            continue;
        }
        if (peer.address.host.empty())
        {
            return common::Status::InvalidArgument(
                "peer host must not be empty");
        }
        if (peer.address.port == 0)
        {
            return common::Status::InvalidArgument(
                "peer port must not be zero");
        }
        if (impl_->peers.find(peer.node_id) != impl_->peers.end())
        {
            return common::Status::InvalidArgument(
                "duplicate peer node id in transport initialization");
        }

        impl_->peers.emplace(peer.node_id, peer);
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
            detail::AddressToTarget(peer.address),
            grpc::InsecureChannelCredentials());
        impl_->stubs.emplace(peer.node_id,
                             cpr::raft::v1::RaftService::NewStub(channel));
    }

    impl_->state = Impl::State::INITIALIZED;
    return common::Status::OK();
}

common::Status GrpcRaftTransport::Start()
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->state == Impl::State::STARTED)
    {
        return common::Status::OK();
    }
    if (impl_->state == Impl::State::CREATED)
    {
        return common::Status::InvalidArgument(
            "transport must be initialized before start");
    }
    impl_->state = Impl::State::STARTED;
    return common::Status::OK();
}

common::Status GrpcRaftTransport::Stop()
{
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->state == Impl::State::CREATED ||
        impl_->state == Impl::State::STOPPED)
    {
        return common::Status::OK();
    }
    impl_->state = Impl::State::STOPPED;
    return common::Status::OK();
}

common::Status GrpcRaftTransport::Send(const raft::RaftMessage &message,
                                       raft::RaftMessage *response)
{
    if (response == nullptr)
    {
        return common::Status::InvalidArgument(
            "response output pointer must not be null");
    }

    common::Status status = detail::ValidateRequestMessage(message);
    if (!status.ok())
    {
        return status;
    }

    std::unique_ptr<cpr::raft::v1::RaftService::Stub> *stub = nullptr;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (impl_->state != Impl::State::STARTED)
        {
            return common::Status::Busy("transport is not started");
        }
        if (message.source_node_id != impl_->local_node_id)
        {
            return common::Status::InvalidArgument(
                "outbound message source node id does not match local node id");
        }
        auto iter = impl_->stubs.find(message.target_node_id);
        if (iter == impl_->stubs.end())
        {
            return common::Status::NotFound(
                "transport target node id is not configured");
        }
        stub = &iter->second;
    }

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(impl_->options.rpc_timeout_ms));

    switch (message.type)
    {
    case raft::RaftMessageType::REQUEST_VOTE_REQUEST:
    {
        cpr::raft::v1::RequestVoteRequest request_proto;
        detail::ToProto(std::get<raft::RequestVoteRequest>(message.payload),
                        &request_proto);

        cpr::raft::v1::RequestVoteResponse response_proto;
        grpc::Status grpc_status =
            (*stub)->RequestVote(&context, request_proto, &response_proto);
        status = detail::ToCommonStatus(grpc_status);
        if (!status.ok())
        {
            return status;
        }
        *response = detail::BuildResponseMessage(
            impl_->local_node_id, message.target_node_id, response_proto);
        return common::Status::OK();
    }
    case raft::RaftMessageType::APPEND_ENTRIES_REQUEST:
    {
        detail::ProtoAppendEntriesRequest request_proto;
        detail::ToProto(std::get<raft::AppendEntriesRequest>(message.payload),
                        &request_proto);

        detail::ProtoAppendEntriesResponse response_proto;
        grpc::Status grpc_status =
            (*stub)->AppendEntries(&context, request_proto, &response_proto);
        status = detail::ToCommonStatus(grpc_status);
        if (!status.ok())
        {
            return status;
        }
        *response = detail::BuildResponseMessage(
            impl_->local_node_id, message.target_node_id, response_proto);
        return common::Status::OK();
    }
    case raft::RaftMessageType::INSTALL_SNAPSHOT_REQUEST:
    {
        detail::ProtoInstallSnapshotRequest request_proto;
        detail::ToProto(std::get<raft::InstallSnapshotRequest>(message.payload),
                        &request_proto);

        detail::ProtoInstallSnapshotResponse response_proto;
        grpc::Status grpc_status =
            (*stub)->InstallSnapshot(&context, request_proto, &response_proto);
        status = detail::ToCommonStatus(grpc_status);
        if (!status.ok())
        {
            return status;
        }
        *response = detail::BuildResponseMessage(
            impl_->local_node_id, message.target_node_id, response_proto);
        return common::Status::OK();
    }
    case raft::RaftMessageType::REQUEST_VOTE_RESPONSE:
    case raft::RaftMessageType::APPEND_ENTRIES_RESPONSE:
    case raft::RaftMessageType::INSTALL_SNAPSHOT_RESPONSE:
        break;
    }

    return common::Status::InvalidArgument("unsupported outbound raft message type");
}

} // namespace cpr::transport
