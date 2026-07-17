#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>

#include "common/status.h"
#include "common/types.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_message.h"
#include "raft/raft_runtime.h"
#include "raft/raft_storage.h"
#include "rpc/raft_rpc_service.h"
#include "transport/grpc_raft_transport.h"

namespace cpr::tests
{
namespace
{

using cpr::common::Byte;
using cpr::common::LogIndex;
using cpr::common::NodeId;
using cpr::common::StatusCode;
using cpr::common::Term;

constexpr NodeId kClientNodeId = 1;
constexpr NodeId kServerNodeId = 2;

raft::RaftCore::Options FollowerOptions(
    Term current_term = 1,
    LogIndex commit_index = 0,
    std::vector<NodeId> voters = {kClientNodeId, kServerNodeId, 3})
{
    raft::RaftCore::Options options;
    options.node_id = kServerNodeId;
    options.initial_role = raft::RaftRole::FOLLOWER;
    options.election_timeout_ticks = 10;
    options.hard_state.current_term = current_term;
    options.hard_state.commit_index = commit_index;
    options.voter_ids = std::move(voters);
    return options;
}

raft::LogEntry MakeEntry(LogIndex index, Term term,
                         std::string payload)
{
    raft::LogEntry entry;
    entry.index = index;
    entry.term = term;
    entry.type = raft::LogEntryType::COMMAND;
    entry.payload = raft::OpaquePayload(
        payload.begin(), payload.end());
    return entry;
}

class LoopbackRaftNode
{
public:
    void Create(Term current_term = 1,
                LogIndex commit_index = 0,
                bool start_runtime = true,
                std::vector<NodeId> voters = {
                    kClientNodeId, kServerNodeId, 3},
                rpc::RaftRpcServiceAdapter::Options adapter_options = {})
    {
        core_ = std::make_unique<raft::RaftCore>();
        ASSERT_TRUE(core_->Initialize(
            FollowerOptions(current_term, commit_index,
                            std::move(voters))).ok());

        storage_ = std::make_unique<raft::MemoryRaftStorage>();
        ASSERT_TRUE(storage_->Open("").ok());
        if (commit_index > 0)
        {
            std::vector<raft::LogEntry> entries;
            entries.reserve(static_cast<std::size_t>(commit_index));
            for (LogIndex index = 1; index <= commit_index; ++index)
            {
                entries.push_back(MakeEntry(index, current_term,
                                            std::string(1, static_cast<char>('a' + index - 1))));
            }
            ASSERT_TRUE(storage_->AppendEntries(entries).ok());

            raft::HardState hard_state;
            hard_state.current_term = current_term;
            hard_state.commit_index = commit_index;
            ASSERT_TRUE(storage_->SaveHardState(hard_state).ok());
        }

        raft::RaftRuntime::Options runtime_options;
        runtime_options.event_queue_capacity = 64;
        runtime_options.persistence_queue_capacity = 8;
        runtime_options.apply_queue_capacity = 8;
        runtime_options.proposal_result_queue_capacity = 8;
        runtime_options.peer_queue_capacity = 8;

        runtime_ = std::make_unique<raft::RaftRuntime>(runtime_options);
        auto apply = [](const raft::LogEntry &)
        { return cpr::common::Status::OK(); };
        ASSERT_TRUE(runtime_->Initialize(core_.get(), storage_.get(),
                                         apply).ok());
        if (start_runtime)
        {
            ASSERT_TRUE(runtime_->Start().ok());
        }

        service_ = std::make_unique<rpc::RaftRpcServiceAdapter>(
            runtime_.get(), adapter_options);
    }

    void StartServer()
    {
        ASSERT_NE(service_, nullptr);
        grpc::ServerBuilder builder;
        int selected_port = 0;
        builder.AddListeningPort(
            "127.0.0.1:0",
            grpc::InsecureServerCredentials(),
            &selected_port);
        builder.RegisterService(service_.get());
        server_ = builder.BuildAndStart();
        ASSERT_NE(server_, nullptr);
        ASSERT_GT(selected_port, 0);

        address_.host = "127.0.0.1";
        address_.port = static_cast<std::uint16_t>(selected_port);
    }

    void StopServer()
    {
        if (!server_)
        {
            return;
        }
        server_->Shutdown();
        server_->Wait();
        server_.reset();
    }

    void StopRuntime()
    {
        if (!runtime_)
        {
            return;
        }
        if (runtime_->state() == raft::RuntimeState::RUNNING)
        {
            const cpr::common::Status status = runtime_->RequestShutdown();
            EXPECT_TRUE(status.ok() ||
                        status.code() == StatusCode::kBusy);
        }
        runtime_->WaitForShutdown();
    }

    const raft::NodeAddress &address() const
    {
        return address_;
    }

    raft::RaftCore *core() const
    {
        return core_.get();
    }

    raft::MemoryRaftStorage *storage() const
    {
        return storage_.get();
    }

private:
    raft::NodeAddress address_;
    std::unique_ptr<raft::RaftCore> core_;
    std::unique_ptr<raft::MemoryRaftStorage> storage_;
    std::unique_ptr<raft::RaftRuntime> runtime_;
    std::unique_ptr<rpc::RaftRpcServiceAdapter> service_;
    std::unique_ptr<grpc::Server> server_;
};

class GrpcRaftTransportIntegrationTest : public ::testing::Test
{
protected:
    void TearDown() override
    {
        StopTransport();
        node_.StopServer();
        node_.StopRuntime();
    }

    void CreateTransport(std::uint64_t timeout_ms = 200,
                         bool start_transport = true,
                         bool include_peer = true)
    {
        transport::RaftTransportOptions options;
        options.rpc_timeout_ms = timeout_ms;
        transport_ =
            std::make_unique<transport::GrpcRaftTransport>(options);

        std::vector<raft::RaftMember> peers;
        if (include_peer)
        {
            raft::RaftMember peer;
            peer.node_id = kServerNodeId;
            peer.address = node_.address();
            peers.push_back(peer);
        }

        ASSERT_TRUE(transport_->Initialize(kClientNodeId, peers).ok());
        if (start_transport)
        {
            ASSERT_TRUE(transport_->Start().ok());
        }
    }

    void StopTransport()
    {
        if (!transport_)
        {
            return;
        }
        EXPECT_TRUE(transport_->Stop().ok());
    }

    raft::RaftMessage MakeRequestVote(Term term,
                                      LogIndex last_log_index = 0,
                                      Term last_log_term = 0)
    {
        raft::RequestVoteRequest payload;
        payload.term = term;
        payload.candidate_id = kClientNodeId;
        payload.last_log_index = last_log_index;
        payload.last_log_term = last_log_term;

        raft::RaftMessage message;
        message.type = raft::RaftMessageType::REQUEST_VOTE_REQUEST;
        message.source_node_id = kClientNodeId;
        message.target_node_id = kServerNodeId;
        message.payload = payload;
        return message;
    }

    raft::RaftMessage MakeAppendEntries(
        Term term,
        LogIndex prev_log_index,
        Term prev_log_term,
        std::string payload,
        LogIndex leader_commit)
    {
        raft::AppendEntriesRequest request;
        request.term = term;
        request.leader_id = kClientNodeId;
        request.prev_log_index = prev_log_index;
        request.prev_log_term = prev_log_term;
        request.leader_commit = leader_commit;
        request.entries.push_back(MakeEntry(prev_log_index + 1, term,
                                            std::move(payload)));

        raft::RaftMessage message;
        message.type = raft::RaftMessageType::APPEND_ENTRIES_REQUEST;
        message.source_node_id = kClientNodeId;
        message.target_node_id = kServerNodeId;
        message.payload = std::move(request);
        return message;
    }

    raft::RaftMessage MakeInstallSnapshot(
        Term term,
        LogIndex last_included_index,
        Term last_included_term,
        std::string payload)
    {
        raft::InstallSnapshotRequest request;
        request.term = term;
        request.leader_id = kClientNodeId;
        request.metadata.last_included_index = last_included_index;
        request.metadata.last_included_term = last_included_term;
        request.metadata.membership.configuration_id = 9;

        raft::RaftMember voter1;
        voter1.node_id = kClientNodeId;
        voter1.address.host = "127.0.0.1";
        voter1.address.port = 1;
        request.metadata.membership.voters.push_back(voter1);

        raft::RaftMember voter2;
        voter2.node_id = kServerNodeId;
        voter2.address = node_.address();
        request.metadata.membership.voters.push_back(voter2);

        request.payload = raft::OpaquePayload(
            payload.begin(), payload.end());

        raft::RaftMessage message;
        message.type = raft::RaftMessageType::INSTALL_SNAPSHOT_REQUEST;
        message.source_node_id = kClientNodeId;
        message.target_node_id = kServerNodeId;
        message.payload = std::move(request);
        return message;
    }

    LoopbackRaftNode node_;
    std::unique_ptr<transport::GrpcRaftTransport> transport_;
};

TEST_F(GrpcRaftTransportIntegrationTest, RequestVoteEndToEndSuccess)
{
    node_.Create(1);
    node_.StartServer();
    CreateTransport();

    raft::RaftMessage response;
    const cpr::common::Status status =
        transport_->Send(MakeRequestVote(2), &response);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.type,
              raft::RaftMessageType::REQUEST_VOTE_RESPONSE);
    const auto &vote =
        std::get<raft::RequestVoteResponse>(response.payload);
    EXPECT_EQ(response.source_node_id, kServerNodeId);
    EXPECT_EQ(response.target_node_id, kClientNodeId);
    EXPECT_EQ(vote.term, 2U);
    EXPECT_TRUE(vote.vote_granted);
    EXPECT_EQ(vote.reject_reason, raft::VoteRejectReason::NONE);
    EXPECT_EQ(node_.core()->current_term(), 2U);
    EXPECT_EQ(node_.core()->voted_for(), kClientNodeId);
}

TEST_F(GrpcRaftTransportIntegrationTest, AppendEntriesEndToEndSuccess)
{
    node_.Create(1);
    node_.StartServer();
    CreateTransport();

    raft::RaftMessage response;
    const cpr::common::Status status = transport_->Send(
        MakeAppendEntries(2, 0, 0, "abc", 1), &response);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.type,
              raft::RaftMessageType::APPEND_ENTRIES_RESPONSE);
    const auto &append =
        std::get<raft::AppendEntriesResponse>(response.payload);
    EXPECT_EQ(append.term, 2U);
    EXPECT_TRUE(append.success);
    EXPECT_EQ(append.match_index, 1U);

    raft::LogEntry stored_entry;
    ASSERT_TRUE(node_.core()->log().GetEntry(1, &stored_entry).ok());
    EXPECT_EQ(stored_entry.index, 1U);
    EXPECT_EQ(stored_entry.term, 2U);
    EXPECT_EQ(std::string(stored_entry.payload.begin(),
                          stored_entry.payload.end()),
              "abc");
    EXPECT_EQ(node_.core()->hard_state().commit_index, 1U);
}

TEST_F(GrpcRaftTransportIntegrationTest, InstallSnapshotEndToEndSuccess)
{
    node_.Create(3, 2);
    node_.StartServer();
    CreateTransport();

    raft::RaftMessage response;
    const cpr::common::Status status = transport_->Send(
        MakeInstallSnapshot(3, 2, 3, "snap"), &response);

    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.type,
              raft::RaftMessageType::INSTALL_SNAPSHOT_RESPONSE);
    const auto &snapshot =
        std::get<raft::InstallSnapshotResponse>(response.payload);
    EXPECT_EQ(snapshot.term, 3U);
    EXPECT_TRUE(snapshot.success);
    EXPECT_EQ(snapshot.last_included_index, 2U);

    raft::RaftStorageLoadResult load;
    ASSERT_TRUE(node_.storage()->Load(&load).ok());
    ASSERT_TRUE(load.snapshot.has_value());
    EXPECT_EQ(load.snapshot->metadata.last_included_index, 2U);
    EXPECT_EQ(load.snapshot->metadata.last_included_term, 3U);
    EXPECT_EQ(load.snapshot->metadata.membership.configuration_id, 9U);
    EXPECT_EQ(std::string(load.snapshot->payload.begin(),
                          load.snapshot->payload.end()),
              "snap");
}

TEST_F(GrpcRaftTransportIntegrationTest, BusinessRejectStillReturnsOkStatus)
{
    node_.Create(3);
    node_.StartServer();
    CreateTransport();

    raft::RaftMessage response;
    const cpr::common::Status status =
        transport_->Send(MakeRequestVote(2), &response);

    ASSERT_TRUE(status.ok());
    const auto &vote =
        std::get<raft::RequestVoteResponse>(response.payload);
    EXPECT_FALSE(vote.vote_granted);
    EXPECT_EQ(vote.reject_reason,
              raft::VoteRejectReason::STALE_TERM);
    EXPECT_EQ(vote.term, 3U);
}

TEST_F(GrpcRaftTransportIntegrationTest, UnknownTargetRejectedBeforeRpc)
{
    node_.Create(1);
    node_.StartServer();
    CreateTransport(200, true, false);

    raft::RaftMessage response;
    const cpr::common::Status status =
        transport_->Send(MakeRequestVote(2), &response);

    EXPECT_EQ(status.code(), StatusCode::kNotFound);
}

TEST_F(GrpcRaftTransportIntegrationTest, UnstartedAndStoppedTransportRejected)
{
    node_.Create(1);
    node_.StartServer();
    CreateTransport(200, false, true);

    raft::RaftMessage response;
    cpr::common::Status status =
        transport_->Send(MakeRequestVote(2), &response);
    EXPECT_EQ(status.code(), StatusCode::kBusy);

    ASSERT_TRUE(transport_->Start().ok());
    ASSERT_TRUE(transport_->Stop().ok());
    ASSERT_TRUE(transport_->Stop().ok());

    status = transport_->Send(MakeRequestVote(2), &response);
    EXPECT_EQ(status.code(), StatusCode::kBusy);
}

TEST_F(GrpcRaftTransportIntegrationTest, RuntimeNotRunningReturnsClearFailure)
{
    node_.Create(1, 0, false);
    node_.StartServer();
    CreateTransport();

    raft::RaftMessage response;
    const cpr::common::Status status =
        transport_->Send(MakeRequestVote(2), &response);

    EXPECT_EQ(status.code(), StatusCode::kInternalError);
    EXPECT_NE(status.message().find("runtime is not running"),
              std::string::npos);
}

TEST_F(GrpcRaftTransportIntegrationTest, ServerStopCausesTransportFailure)
{
    node_.Create(1);
    node_.StartServer();
    CreateTransport();
    node_.StopServer();
    node_.StopServer();

    raft::RaftMessage response;
    const cpr::common::Status status =
        transport_->Send(MakeRequestVote(2), &response);

    EXPECT_EQ(status.code(), StatusCode::kRetryLater);
}

TEST_F(GrpcRaftTransportIntegrationTest, MissingPeerQueueOnServerCausesTimeout)
{
    rpc::RaftRpcServiceAdapter::Options adapter_options;
    adapter_options.response_timeout = std::chrono::milliseconds(30);
    adapter_options.poll_interval = std::chrono::milliseconds(1);

    node_.Create(1, 0, true, {kServerNodeId}, adapter_options);
    node_.StartServer();
    CreateTransport(200, true, true);

    raft::RaftMessage response;
    const cpr::common::Status status =
        transport_->Send(MakeRequestVote(2), &response);

    EXPECT_EQ(status.code(), StatusCode::kRetryLater);
    EXPECT_NE(status.message().find("timed out"),
              std::string::npos);
}

} // namespace
} // namespace cpr::tests
