#include <chrono>
#include <memory>
#include <thread>

#include <grpcpp/server_context.h>
#include <gtest/gtest.h>

#include "common/types.h"
#include "metadata/metadata_command.h"
#include "metadata/metadata_service.h"
#include "metadata/metadata_state_machine.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"
#include "rpc/metadata_rpc_service.h"

namespace cpr::rpc
{
    namespace
    {

        raft::RaftMember MakeMember(cpr::common::NodeId node_id, const char *host)
        {
            return raft::RaftMember{
                node_id,
                {host, static_cast<std::uint16_t>(9000 + node_id)}};
        }

        raft::MembershipView LeaderMembership()
        {
            raft::MembershipView view;
            view.configuration_id = 1;
            view.voters = {MakeMember(1, "10.0.0.1")};
            return view;
        }

        raft::MembershipView FollowerMembership()
        {
            raft::MembershipView view;
            view.configuration_id = 1;
            view.voters = {
                MakeMember(1, "10.0.0.1"),
                MakeMember(2, "10.0.0.2"),
                MakeMember(3, "10.0.0.3")};
            return view;
        }

        raft::RaftCore::Options LeaderOptions()
        {
            raft::RaftCore::Options options;
            options.node_id = 1;
            options.initial_role = raft::RaftRole::CANDIDATE;
            options.election_timeout_ticks = 10;
            options.hard_state.current_term = 1;
            options.voter_ids = {1};
            options.membership = LeaderMembership();
            return options;
        }

        raft::RaftCore::Options FollowerOptions()
        {
            raft::RaftCore::Options options;
            options.node_id = 2;
            options.initial_role = raft::RaftRole::FOLLOWER;
            options.election_timeout_ticks = 10;
            options.hard_state.current_term = 1;
            options.voter_ids = {1, 2, 3};
            options.membership = FollowerMembership();
            return options;
        }

        metadata::MetadataCommand MakeCommand(const std::string &command_id,
                                              const std::string &target_id,
                                              raft::OpaquePayload payload,
                                              std::optional<std::uint64_t> expected_generation = std::nullopt)
        {
            metadata::MetadataCommand command;
            command.type = metadata::MetadataCommandType::OPAQUE_OPERATION;
            command.command_id = command_id;
            command.target_id = target_id;
            command.expected_generation = expected_generation;
            command.payload = std::move(payload);
            return command;
        }

        class MetadataRpcServiceTest : public ::testing::Test
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

            void CreateRuntime(const raft::RaftCore::Options &options, bool become_leader)
            {
                core_ = std::make_unique<raft::RaftCore>();
                ASSERT_TRUE(core_->Initialize(options).ok());
                if (become_leader)
                {
                    ASSERT_TRUE(core_->BecomeLeader().ok());
                }
                else
                {
                    ASSERT_TRUE(core_->BecomeFollower(1, 1).ok());
                }

                storage_ = std::make_unique<raft::MemoryRaftStorage>();
                ASSERT_TRUE(storage_->Open("").ok());

                raft::RaftRuntime::Options runtime_options;
                runtime_options.event_queue_capacity = 64;
                runtime_options.persistence_queue_capacity = 8;
                runtime_options.apply_queue_capacity = 8;
                runtime_options.proposal_result_queue_capacity = 8;
                runtime_options.peer_queue_capacity = 8;

                runtime_ = std::make_unique<raft::RaftRuntime>(runtime_options);
                auto apply = [this](const raft::LogEntry &entry)
                {
                    if (entry.type != raft::LogEntryType::COMMAND)
                    {
                        return cpr::common::Status::OK();
                    }
                    return state_machine_.Apply(entry.index, entry.term, entry.payload);
                };
                ASSERT_TRUE(runtime_->Initialize(core_.get(), storage_.get(), apply).ok());
                ASSERT_TRUE(runtime_->Start().ok());

                metadata_service_ = std::make_unique<metadata::MetadataService>(
                    runtime_.get(), &state_machine_);

                MetadataRpcServiceAdapter::Options service_options;
                service_options.response_timeout = std::chrono::milliseconds(200);
                service_options.poll_interval = std::chrono::milliseconds(1);
                service_ = std::make_unique<MetadataRpcServiceAdapter>(
                    runtime_.get(), metadata_service_.get(), service_options);
            }

            metadata::MetadataStateMachine state_machine_;
            std::unique_ptr<raft::RaftCore> core_;
            std::unique_ptr<raft::MemoryRaftStorage> storage_;
            std::unique_ptr<raft::RaftRuntime> runtime_;
            std::unique_ptr<metadata::MetadataService> metadata_service_;
            std::unique_ptr<MetadataRpcServiceAdapter> service_;
        };

        TEST_F(MetadataRpcServiceTest, AddLearnerReturnsSuccessAfterRuntimeApply)
        {
            CreateRuntime(LeaderOptions(), true);

            grpc::ServerContext context;
            cpr::metadata::v1::AddLearnerRequest request;
            request.set_request_id("add-1");
            request.mutable_learner()->set_node_id(2);
            request.mutable_learner()->mutable_address()->set_host("10.0.0.2");
            request.mutable_learner()->mutable_address()->set_port(9002);
            request.set_timeout_ms(500);

            cpr::metadata::v1::MembershipChangeResponse response;
            const grpc::Status status = service_->AddLearner(&context, &request, &response);

            ASSERT_TRUE(status.ok());
            EXPECT_EQ(response.status().code(), cpr::common::v1::RPC_STATUS_CODE_OK);
            raft::MembershipView membership;
            ASSERT_TRUE(core_->GetMembershipView(&membership).ok());
            ASSERT_EQ(membership.learners.size(), 1U);
            EXPECT_EQ(membership.learners[0].node_id, 2U);
        }

        TEST_F(MetadataRpcServiceTest, NonLeaderReturnsBusinessNotLeaderAndLeaderHint)
        {
            CreateRuntime(FollowerOptions(), false);

            grpc::ServerContext context;
            cpr::metadata::v1::RemoveMemberRequest request;
            request.set_request_id("rm-1");
            request.set_node_id(3);
            request.set_timeout_ms(200);

            cpr::metadata::v1::MembershipChangeResponse response;
            const grpc::Status status = service_->RemoveMember(&context, &request, &response);

            ASSERT_TRUE(status.ok());
            EXPECT_EQ(response.status().code(), cpr::common::v1::RPC_STATUS_CODE_NOT_LEADER);
            EXPECT_EQ(response.leader().leader_id(), 1U);
            EXPECT_EQ(response.leader().address().host(), "10.0.0.1");
        }

        TEST_F(MetadataRpcServiceTest, StoppedRuntimeReturnsTransportFailure)
        {
            CreateRuntime(LeaderOptions(), true);
            ASSERT_TRUE(runtime_->RequestShutdown().ok());
            runtime_->WaitForShutdown();

            grpc::ServerContext context;
            cpr::metadata::v1::AddLearnerRequest request;
            request.set_request_id("add-2");
            request.mutable_learner()->set_node_id(2);
            request.mutable_learner()->mutable_address()->set_host("10.0.0.2");
            request.mutable_learner()->mutable_address()->set_port(9002);

            cpr::metadata::v1::MembershipChangeResponse response;
            const grpc::Status status = service_->AddLearner(&context, &request, &response);

            EXPECT_FALSE(status.ok());
            EXPECT_EQ(status.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
        }

        TEST_F(MetadataRpcServiceTest, ProposeAndQueryUseMetadataService)
        {
            CreateRuntime(LeaderOptions(), true);

            const metadata::MetadataCommand command =
                MakeCommand("cmd-1", "alpha", {0x10, 0x11});
            raft::OpaquePayload encoded;
            ASSERT_TRUE(metadata::EncodeMetadataCommand(command, &encoded).ok());

            grpc::ServerContext propose_context;
            cpr::metadata::v1::ProposeRequest propose_request;
            propose_request.set_request_id("ignored-by-server");
            propose_request.set_command_payload(encoded.data(), encoded.size());
            cpr::metadata::v1::ProposeResponse propose_response;

            grpc::Status status =
                service_->Propose(&propose_context, &propose_request, &propose_response);
            ASSERT_TRUE(status.ok());
            EXPECT_EQ(propose_response.status().code(),
                      cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_GT(propose_response.log_index(), 0U);
            EXPECT_EQ(1U, propose_response.term());

            grpc::ServerContext query_context;
            cpr::metadata::v1::QueryRequest query_request;
            query_request.set_request_id("q-1");
            query_request.set_query_payload("alpha");
            cpr::metadata::v1::QueryResponse query_response;
            status = service_->Query(&query_context, &query_request, &query_response);
            ASSERT_TRUE(status.ok());
            EXPECT_EQ(query_response.status().code(),
                      cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_EQ(std::string("\x10\x11", 2), query_response.result_payload());
            EXPECT_EQ(1U, query_response.term());
        }

        TEST_F(MetadataRpcServiceTest, GetLeaderAndStatusExposeRuntimeSnapshot)
        {
            CreateRuntime(FollowerOptions(), false);

            grpc::ServerContext leader_context;
            cpr::metadata::v1::GetLeaderRequest leader_request;
            cpr::metadata::v1::GetLeaderResponse leader_response;
            grpc::Status status =
                service_->GetLeader(&leader_context, &leader_request, &leader_response);
            ASSERT_TRUE(status.ok());
            EXPECT_EQ(leader_response.status().code(),
                      cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_EQ(1U, leader_response.leader().leader_id());

            grpc::ServerContext status_context;
            cpr::metadata::v1::GetStatusRequest status_request;
            status_request.set_include_membership(true);
            status_request.set_include_peer_progress(true);
            cpr::metadata::v1::GetStatusResponse status_response;
            status = service_->GetStatus(&status_context, &status_request, &status_response);
            ASSERT_TRUE(status.ok());
            EXPECT_EQ(status_response.status().code(),
                      cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_EQ(2U, status_response.node().node_id());
            EXPECT_EQ(cpr::common::v1::CLUSTER_ROLE_FOLLOWER,
                      status_response.node().role());
            EXPECT_EQ(cpr::common::v1::NODE_LIFECYCLE_STATE_RUNNING,
                      status_response.node().state());
            EXPECT_EQ(1U, status_response.node().leader().leader_id());
            EXPECT_EQ(3, status_response.node().membership().voters_size());
            EXPECT_EQ(3, status_response.node().peers_size());
        }

    } // namespace
} // namespace cpr::rpc
