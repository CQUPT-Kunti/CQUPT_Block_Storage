#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/server_context.h>
#include <gtest/gtest.h>

#include "common/status.h"
#include "common/types.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"
#include "raft/raft_storage.h"
#include "rpc/raft_rpc_service.h"

namespace cpr::rpc
{
namespace
{

using cpr::common::Byte;
using cpr::common::LogIndex;
using cpr::common::NodeId;
using cpr::common::StatusCode;
using cpr::common::Term;

raft::RaftCore::Options FollowerOptions(Term current_term = 1,
                                        LogIndex commit_index = 0)
{
    raft::RaftCore::Options opts;
    opts.node_id = 2;
    opts.initial_role = raft::RaftRole::FOLLOWER;
    opts.election_timeout_ticks = 10;
    opts.hard_state.current_term = current_term;
    opts.hard_state.commit_index = commit_index;
    opts.voter_ids = {1, 2, 3};
    return opts;
}

raft::LogEntry MakeEntry(LogIndex index, Term term,
                         std::vector<Byte> payload = {})
{
    raft::LogEntry entry;
    entry.index = index;
    entry.term = term;
    entry.type = raft::LogEntryType::COMMAND;
    entry.payload = std::move(payload);
    return entry;
}

class RaftRpcServiceTest : public ::testing::Test
{
protected:
    void TearDown() override
    {
        if (runtime_ &&
            runtime_->state() == raft::RuntimeState::RUNNING)
        {
            runtime_->RequestShutdown();
            runtime_->WaitForShutdown();
        }
    }

    void CreateFollowerRuntime(Term current_term = 1,
                               LogIndex commit_index = 0,
                               bool start = true)
    {
        core_ = std::make_unique<raft::RaftCore>();
        ASSERT_TRUE(core_->Initialize(
            FollowerOptions(current_term, commit_index)).ok());

        storage_ = std::make_unique<raft::MemoryRaftStorage>();
        ASSERT_TRUE(storage_->Open("").ok());
        if (commit_index > 0)
        {
            std::vector<raft::LogEntry> entries;
            entries.reserve(static_cast<std::size_t>(commit_index));
            for (LogIndex index = 1; index <= commit_index; ++index)
            {
                entries.push_back(MakeEntry(index, current_term,
                                            {static_cast<Byte>(index)}));
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
        if (start)
        {
            ASSERT_TRUE(runtime_->Start().ok());
        }

        RaftRpcServiceAdapter::Options adapter_options;
        adapter_options.response_timeout =
            std::chrono::milliseconds(1000);
        adapter_options.poll_interval = std::chrono::milliseconds(1);
        service_ = std::make_unique<RaftRpcServiceAdapter>(
            runtime_.get(), adapter_options);
    }

    raft::RaftStorageLoadResult WaitForStorageLoad(
        LogIndex expected_last_index,
        bool expect_snapshot = false)
    {
        raft::RaftStorageLoadResult result;
        for (int i = 0; i < 1000; ++i)
        {
            const cpr::common::Status status = storage_->Load(&result);
            if (!status.ok())
            {
                ADD_FAILURE() << status.ToString();
                return result;
            }
            const LogIndex last_index = result.entries.empty()
                                            ? cpr::common::kInvalidLogIndex
                                            : result.entries.back().index;
            if ((!expect_snapshot && last_index >= expected_last_index) ||
                (expect_snapshot && result.snapshot.has_value()))
            {
                return result;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return result;
    }

    std::unique_ptr<raft::RaftCore> core_;
    std::unique_ptr<raft::MemoryRaftStorage> storage_;
    std::unique_ptr<raft::RaftRuntime> runtime_;
    std::unique_ptr<RaftRpcServiceAdapter> service_;
};

TEST_F(RaftRpcServiceTest, RequestVoteDeliveredAndResponseConverted)
{
    CreateFollowerRuntime(1);

    grpc::ServerContext context;
    cpr::raft::v1::RequestVoteRequest request;
    request.set_term(2);
    request.set_candidate_id(1);
    request.set_last_log_index(0);
    request.set_last_log_term(0);

    cpr::raft::v1::RequestVoteResponse response;
    const grpc::Status status =
        service_->RequestVote(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(response.term(), 2U);
    EXPECT_TRUE(response.vote_granted());
    EXPECT_EQ(response.reject_reason(),
              cpr::raft::v1::VOTE_REJECT_REASON_NONE);
}

TEST_F(RaftRpcServiceTest, AppendEntriesDeliveredAndPayloadPersisted)
{
    CreateFollowerRuntime(1);

    grpc::ServerContext context;
    cpr::raft::v1::AppendEntriesRequest request;
    request.set_term(2);
    request.set_leader_id(1);
    request.set_prev_log_index(0);
    request.set_prev_log_term(0);
    request.set_leader_commit(1);

    auto *entry = request.add_entries();
    entry->set_index(1);
    entry->set_term(2);
    entry->set_type(cpr::common::v1::LOG_ENTRY_KIND_COMMAND);
    entry->set_payload("abc");

    cpr::raft::v1::AppendEntriesResponse response;
    const grpc::Status status =
        service_->AppendEntries(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(response.term(), 2U);
    EXPECT_TRUE(response.success());
    EXPECT_EQ(response.match_index(), 1U);

    raft::LogEntry persisted_entry;
    ASSERT_TRUE(core_->log().GetEntry(1, &persisted_entry).ok());
    EXPECT_EQ(persisted_entry.index, 1U);
    EXPECT_EQ(persisted_entry.term, 2U);
    EXPECT_EQ(std::string(persisted_entry.payload.begin(),
                          persisted_entry.payload.end()),
              "abc");
}

TEST_F(RaftRpcServiceTest, InstallSnapshotDeliveredAndStored)
{
    CreateFollowerRuntime(3, 2);

    grpc::ServerContext context;
    cpr::raft::v1::InstallSnapshotRequest request;
    request.set_term(3);
    request.set_leader_id(1);
    request.mutable_metadata()->set_last_included_index(2);
    request.mutable_metadata()->set_last_included_term(3);
    request.mutable_metadata()->mutable_membership()->set_configuration_id(9);
    request.mutable_metadata()->mutable_membership()->add_voters()->set_node_id(1);
    request.mutable_metadata()->mutable_membership()->add_voters()->set_node_id(2);
    request.mutable_metadata()->mutable_membership()->add_voters()->set_node_id(3);
    request.set_payload("snap");

    cpr::raft::v1::InstallSnapshotResponse response;
    const grpc::Status status =
        service_->InstallSnapshot(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_EQ(response.term(), 3U);
    EXPECT_TRUE(response.success());
    EXPECT_EQ(response.last_included_index(), 2U);

    const raft::RaftStorageLoadResult load =
        WaitForStorageLoad(0, true);
    ASSERT_TRUE(load.snapshot.has_value());
    EXPECT_EQ(load.snapshot->metadata.last_included_index, 2U);
    EXPECT_EQ(load.snapshot->metadata.last_included_term, 3U);
    EXPECT_EQ(load.snapshot->metadata.membership.configuration_id, 9U);
    EXPECT_EQ(load.snapshot->metadata.membership.voters.size(), 3U);
    EXPECT_EQ(std::string(load.snapshot->payload.begin(),
                          load.snapshot->payload.end()),
              "snap");
}

TEST_F(RaftRpcServiceTest, RuntimeNotRunningReturnsTransportError)
{
    CreateFollowerRuntime(1, 0, false);

    grpc::ServerContext context;
    cpr::raft::v1::RequestVoteRequest request;
    request.set_term(2);
    request.set_candidate_id(1);

    cpr::raft::v1::RequestVoteResponse response;
    const grpc::Status status =
        service_->RequestVote(&context, &request, &response);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(),
              grpc::StatusCode::FAILED_PRECONDITION);
}

TEST_F(RaftRpcServiceTest, BusinessRejectStaysInOkGrpcStatus)
{
    CreateFollowerRuntime(3);

    grpc::ServerContext context;
    cpr::raft::v1::RequestVoteRequest request;
    request.set_term(2);
    request.set_candidate_id(1);
    request.set_last_log_index(0);
    request.set_last_log_term(0);

    cpr::raft::v1::RequestVoteResponse response;
    const grpc::Status status =
        service_->RequestVote(&context, &request, &response);

    ASSERT_TRUE(status.ok());
    EXPECT_FALSE(response.vote_granted());
    EXPECT_EQ(response.term(), 3U);
    EXPECT_EQ(response.reject_reason(),
              cpr::raft::v1::VOTE_REJECT_REASON_STALE_TERM);
}

} // namespace
} // namespace cpr::rpc
