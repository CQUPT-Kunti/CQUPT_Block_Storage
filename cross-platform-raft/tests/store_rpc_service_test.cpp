#include <chrono>
#include <memory>
#include <string>

#include <grpcpp/server_context.h>
#include <gtest/gtest.h>

#include "metadata/metadata_service.h"
#include "metadata/metadata_state_machine.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"
#include "rpc/store_rpc_service.h"

namespace cpr::rpc
{
    namespace
    {

        raft::RaftMember MakeMember(common::NodeId node_id, const char *host)
        {
            return raft::RaftMember{
                node_id,
                {host, static_cast<std::uint16_t>(9000 + node_id)}};
        }

        raft::MembershipView SingleLeaderMembership()
        {
            raft::MembershipView membership;
            membership.configuration_id = 1;
            membership.voters = {MakeMember(1, "127.0.0.1")};
            return membership;
        }

        raft::MembershipView FollowerMembership()
        {
            raft::MembershipView membership;
            membership.configuration_id = 1;
            membership.voters = {
                MakeMember(1, "10.0.0.1"),
                MakeMember(2, "10.0.0.2"),
                MakeMember(3, "10.0.0.3")};
            return membership;
        }

        raft::RaftCore::Options LeaderOptions()
        {
            raft::RaftCore::Options options;
            options.node_id = 1;
            options.initial_role = raft::RaftRole::CANDIDATE;
            options.election_timeout_ticks = 10;
            options.hard_state.current_term = 1;
            options.voter_ids = {1};
            options.membership = SingleLeaderMembership();
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

        class StoreRpcServiceTest : public ::testing::Test
        {
        protected:
            void TearDown() override
            {
                Shutdown();
            }

            void CreateRuntime(const raft::RaftCore::Options &options,
                               bool become_leader)
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
                runtime_options.proposal_result_queue_capacity = 64;
                runtime_options.peer_queue_capacity = 8;

                runtime_ = std::make_unique<raft::RaftRuntime>(runtime_options);
                auto apply = [this](const raft::LogEntry &entry)
                {
                    if (entry.type != raft::LogEntryType::COMMAND)
                    {
                        return common::Status::OK();
                    }
                    return machine_.Apply(entry.index, entry.term, entry.payload);
                };
                ASSERT_TRUE(runtime_->Initialize(core_.get(), storage_.get(), apply).ok());
                ASSERT_TRUE(runtime_->Start().ok());

                metadata::MetadataService::Options service_options;
                service_options.proposal_timeout = std::chrono::milliseconds(500);
                service_options.poll_interval = std::chrono::milliseconds(1);
                metadata_service_ =
                    std::make_unique<metadata::MetadataService>(runtime_.get(),
                                                                 &machine_,
                                                                 service_options);

                StoreRpcServiceAdapter::Options rpc_options;
                rpc_options.response_timeout = std::chrono::milliseconds(500);
                store_rpc_ =
                    std::make_unique<StoreRpcServiceAdapter>(metadata_service_.get(),
                                                             rpc_options);
            }

            void Shutdown()
            {
                if (runtime_ && runtime_->state() == raft::RuntimeState::RUNNING)
                {
                    runtime_->RequestShutdown();
                    runtime_->WaitForShutdown();
                }
            }

            void RegisterStore(std::uint64_t id = 7)
            {
                grpc::ServerContext context;
                ::cpr::store::v1::RegisterStoreRequest request;
                request.set_request_id("register-" + std::to_string(id));
                request.mutable_store()->set_store_id(id);
                request.mutable_store()->mutable_address()->set_host("127.0.0.1");
                request.mutable_store()->mutable_address()->set_port(10000 + id);
                request.mutable_store()->set_total_capacity_bytes(1024);
                request.mutable_store()->set_used_capacity_bytes(10);
                request.mutable_store()->set_state(::cpr::store::v1::STORE_STATE_RUNNING);
                request.mutable_store()->set_generation(1);
                request.set_timeout_ms(500);

                ::cpr::store::v1::StoreCommandResponse response;
                ASSERT_TRUE(store_rpc_->Register(&context, &request, &response).ok());
                ASSERT_EQ(response.status().code(),
                          ::cpr::common::v1::RPC_STATUS_CODE_OK);
            }

            void CreateTask(const std::string &task_id)
            {
                store::TaskCreateRequest task;
                task.task_id = task_id;
                task.type = store::TaskType::CUSTOM;
                task.target_payload = {0x10};
                metadata::StoreControlResult result;
                ASSERT_TRUE(metadata_service_
                                ->CreateTaskForTest("create-" + task_id,
                                                    task,
                                                    std::chrono::milliseconds(500),
                                                    &result)
                                .ok());
            }

            metadata::MetadataStateMachine machine_;
            std::unique_ptr<raft::RaftCore> core_;
            std::unique_ptr<raft::MemoryRaftStorage> storage_;
            std::unique_ptr<raft::RaftRuntime> runtime_;
            std::unique_ptr<metadata::MetadataService> metadata_service_;
            std::unique_ptr<StoreRpcServiceAdapter> store_rpc_;
        };

        TEST_F(StoreRpcServiceTest, RegisterAppliesThroughMetadataConsensusBeforeSuccess)
        {
            CreateRuntime(LeaderOptions(), true);

            RegisterStore();

            store::StoreInfo store;
            ASSERT_TRUE(metadata_service_->GetStore(7, &store).ok());
            EXPECT_EQ(store.id, 7U);
            EXPECT_EQ(store.generation, 1U);
            EXPECT_EQ(store.address.host, "127.0.0.1");
        }

        TEST_F(StoreRpcServiceTest, RegisterRejectsInvalidCapacityAndDuplicateAddress)
        {
            CreateRuntime(LeaderOptions(), true);
            RegisterStore(7);

            grpc::ServerContext context;
            ::cpr::store::v1::RegisterStoreRequest bad;
            bad.set_request_id("bad");
            bad.mutable_store()->set_store_id(8);
            bad.mutable_store()->mutable_address()->set_host("127.0.0.1");
            bad.mutable_store()->mutable_address()->set_port(10007);
            bad.mutable_store()->set_total_capacity_bytes(0);
            bad.mutable_store()->set_used_capacity_bytes(1);

            ::cpr::store::v1::StoreCommandResponse response;
            ASSERT_TRUE(store_rpc_->Register(&context, &bad, &response).ok());
            EXPECT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_INVALID_ARGUMENT);

            bad.set_request_id("dup-address");
            bad.mutable_store()->set_total_capacity_bytes(1024);
            bad.mutable_store()->set_used_capacity_bytes(1);
            ASSERT_TRUE(store_rpc_->Register(&context, &bad, &response).ok());
            EXPECT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_BUSY);
        }

        TEST_F(StoreRpcServiceTest, NonLeaderRegisterReturnsBusinessNotLeaderWithHint)
        {
            CreateRuntime(FollowerOptions(), false);

            grpc::ServerContext context;
            ::cpr::store::v1::RegisterStoreRequest request;
            request.set_request_id("register");
            request.mutable_store()->set_store_id(7);
            request.mutable_store()->mutable_address()->set_host("127.0.0.1");
            request.mutable_store()->mutable_address()->set_port(10007);
            request.mutable_store()->set_total_capacity_bytes(1024);

            ::cpr::store::v1::StoreCommandResponse response;
            ASSERT_TRUE(store_rpc_->Register(&context, &request, &response).ok());
            EXPECT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_NOT_LEADER);
            EXPECT_EQ(response.leader().leader_id(), 1U);
            EXPECT_EQ(response.leader().address().host(), "10.0.0.1");
        }

        TEST_F(StoreRpcServiceTest, HeartbeatUpdatesOnlyLeaderLocalTemporaryState)
        {
            CreateRuntime(LeaderOptions(), true);
            RegisterStore();

            grpc::ServerContext context;
            ::cpr::store::v1::StoreHeartbeatRequest request;
            request.set_store_id(7);
            request.set_generation(123);
            request.set_used_capacity_bytes(999);
            request.set_available_capacity_bytes(1);

            ::cpr::store::v1::StoreHeartbeatResponse response;
            ASSERT_TRUE(store_rpc_->Heartbeat(&context, &request, &response).ok());
            EXPECT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);

            store::StoreInfo store;
            ASSERT_TRUE(metadata_service_->GetStore(7, &store).ok());
            EXPECT_EQ(store.generation, 1U);
            EXPECT_EQ(store.used_bytes, 10U);
            EXPECT_EQ(store.last_heartbeat_ms, 123);
        }

        TEST_F(StoreRpcServiceTest, StopAndRemoveApplyAsPersistentStoreChanges)
        {
            CreateRuntime(LeaderOptions(), true);
            RegisterStore();

            grpc::ServerContext context;
            ::cpr::store::v1::StoreStopRequest stop;
            stop.set_request_id("stop");
            stop.set_store_id(7);
            stop.set_generation(1);

            ::cpr::store::v1::StoreCommandResponse response;
            ASSERT_TRUE(store_rpc_->Stop(&context, &stop, &response).ok());
            EXPECT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_EQ(response.store().state(),
                      ::cpr::store::v1::STORE_STATE_STOPPED);
            EXPECT_EQ(response.store().generation(), 2U);

            ::cpr::store::v1::StoreRemoveRequest remove;
            remove.set_request_id("remove");
            remove.set_store_id(7);
            remove.set_generation(2);
            ASSERT_TRUE(store_rpc_->Remove(&context, &remove, &response).ok());
            EXPECT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);

            store::StoreInfo missing;
            EXPECT_EQ(metadata_service_->GetStore(7, &missing).code(),
                      common::StatusCode::kNotFound);
        }

        TEST_F(StoreRpcServiceTest, PollTasksAndReportResultUseConsensusAppliedState)
        {
            CreateRuntime(LeaderOptions(), true);
            RegisterStore();
            CreateTask("task-1");
            CreateTask("task-2");

            grpc::ServerContext context;
            ::cpr::store::v1::PollTasksRequest poll;
            poll.set_store_id(7);
            poll.set_generation(1);
            poll.set_max_tasks(1);

            ::cpr::store::v1::PollTasksResponse poll_response;
            ASSERT_TRUE(store_rpc_->PollTasks(&context, &poll, &poll_response).ok());
            ASSERT_EQ(poll_response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            ASSERT_EQ(poll_response.tasks_size(), 1);
            EXPECT_EQ(poll_response.tasks(0).task_id(), "task-1");

            store::TaskRecord task;
            ASSERT_TRUE(metadata_service_->GetTask("task-1", &task).ok());
            EXPECT_EQ(task.state, store::TaskState::RUNNING);
            EXPECT_EQ(task.assigned_store, 7U);

            ::cpr::store::v1::ReportTaskResultRequest report;
            report.set_store_id(7);
            report.set_generation(1);
            report.set_task_id("task-1");
            report.set_final_state(::cpr::store::v1::TASK_STATE_SUCCESS);
            report.set_result_payload("ok");

            ::cpr::store::v1::ReportTaskResultResponse report_response;
            ASSERT_TRUE(store_rpc_->ReportTaskResult(&context, &report, &report_response).ok());
            EXPECT_EQ(report_response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_FALSE(report_response.duplicate_result());

            ASSERT_TRUE(store_rpc_->ReportTaskResult(&context, &report, &report_response).ok());
            EXPECT_EQ(report_response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_TRUE(report_response.duplicate_result());
        }

    } // namespace
} // namespace cpr::rpc
