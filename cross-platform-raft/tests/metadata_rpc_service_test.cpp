#include <chrono>
#include <memory>
#include <thread>

#include <grpcpp/server_context.h>
#include <gtest/gtest.h>

#include "common/types.h"
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
                auto apply = [](const raft::LogEntry &)
                { return cpr::common::Status::OK(); };
                ASSERT_TRUE(runtime_->Initialize(core_.get(), storage_.get(), apply).ok());
                ASSERT_TRUE(runtime_->Start().ok());

                MetadataRpcServiceAdapter::Options service_options;
                service_options.response_timeout = std::chrono::milliseconds(200);
                service_options.poll_interval = std::chrono::milliseconds(1);
                service_ = std::make_unique<MetadataRpcServiceAdapter>(runtime_.get(), service_options);
            }

            std::unique_ptr<raft::RaftCore> core_;
            std::unique_ptr<raft::MemoryRaftStorage> storage_;
            std::unique_ptr<raft::RaftRuntime> runtime_;
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

    } // namespace
} // namespace cpr::rpc
