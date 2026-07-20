#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/server_context.h>
#include <gtest/gtest.h>

#include "common/status.h"
#include "metadata/metadata_command.h"
#include "metadata/metadata_service.h"
#include "metadata/metadata_state_machine.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"
#include "raft/raft_storage.h"
#include "rpc/store_rpc_service.h"
#include "store/placement_policy.h"
#include "store/simple_task_dispatcher.h"

namespace cpr::rpc
{
    namespace
    {

        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::StatusCode;

        raft::RaftMember MakeMember(NodeId node_id, const char *host)
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

        store::StoreInfo MakeStore(store::StoreId id,
                                   std::uint64_t capacity,
                                   std::uint64_t used = 0)
        {
            store::StoreInfo info;
            info.id = id;
            info.address.host = "127.0.0." + std::to_string(id);
            info.address.port = static_cast<std::uint16_t>(10000 + id);
            info.capacity_bytes = capacity;
            info.used_bytes = used;
            info.state = store::StoreState::RUNNING;
            info.generation = 1;
            return info;
        }

        class FailingAppendStorage final : public raft::IRaftStorage
        {
        public:
            common::Status Open(const std::filesystem::path &) override
            {
                return common::Status::OK();
            }

            common::Status Load(raft::RaftStorageLoadResult *result) const override
            {
                if (result == nullptr)
                {
                    return common::Status::InvalidArgument("load result must not be null");
                }
                *result = raft::RaftStorageLoadResult{};
                return common::Status::OK();
            }

            common::Status SaveHardState(const raft::HardState &) override
            {
                return common::Status::OK();
            }

            common::Status AppendEntries(const std::vector<raft::LogEntry> &) override
            {
                return common::Status::IoError("append failed for store metadata test");
            }

            common::Status TruncateSuffix(LogIndex) override
            {
                return common::Status::OK();
            }

            common::Status SaveSnapshot(const raft::SnapshotData &) override
            {
                return common::Status::OK();
            }
        };

        class BlockingApply
        {
        public:
            common::Status Apply(const raft::LogEntry &entry,
                                 metadata::MetadataStateMachine *machine)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    entered_ = true;
                    cv_.notify_all();
                }

                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]
                         { return release_; });
                lock.unlock();

                if (entry.type != raft::LogEntryType::COMMAND)
                {
                    return common::Status::OK();
                }
                return machine->Apply(entry.index, entry.term, entry.payload);
            }

            bool WaitUntilEntered(std::chrono::milliseconds timeout)
            {
                std::unique_lock<std::mutex> lock(mutex_);
                return cv_.wait_for(lock, timeout, [this]
                                    { return entered_; });
            }

            void Release()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                release_ = true;
                cv_.notify_all();
            }

        private:
            std::mutex mutex_;
            std::condition_variable cv_;
            bool entered_ = false;
            bool release_ = false;
        };

        common::Status ApplyStoreCommand(metadata::MetadataStateMachine *machine,
                                         LogIndex index,
                                         const std::string &command_id,
                                         const std::string &target_id,
                                         const metadata::StoreBusinessCommand &store_command)
        {
            raft::OpaquePayload business_payload;
            common::Status status =
                metadata::EncodeStoreBusinessCommand(store_command,
                                                     &business_payload);
            if (!status.ok())
            {
                return status;
            }

            metadata::MetadataCommand command;
            command.type = metadata::MetadataCommandType::STORE_OPERATION;
            command.command_id = command_id;
            command.target_id = target_id;
            command.payload = std::move(business_payload);

            raft::OpaquePayload payload;
            status = metadata::EncodeMetadataCommand(command, &payload);
            if (!status.ok())
            {
                return status;
            }
            return machine->Apply(index, 1, payload);
        }

        class StoreMetadataTest : public ::testing::Test
        {
        protected:
            void TearDown() override
            {
                Shutdown();
            }

            void CreateRuntime(const raft::RaftCore::Options &options,
                               bool become_leader,
                               std::unique_ptr<raft::IRaftStorage> storage = nullptr,
                               raft::RaftRuntime::ApplyFn apply_fn = nullptr,
                               std::size_t event_capacity = 64)
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

                if (!storage)
                {
                    storage = std::make_unique<raft::MemoryRaftStorage>();
                }
                storage_ = std::move(storage);
                ASSERT_TRUE(storage_->Open("").ok());

                raft::RaftRuntime::Options runtime_options;
                runtime_options.event_queue_capacity = event_capacity;
                runtime_options.persistence_queue_capacity = 8;
                runtime_options.apply_queue_capacity = 8;
                runtime_options.proposal_result_queue_capacity = 64;
                runtime_options.peer_queue_capacity = 8;

                runtime_ = std::make_unique<raft::RaftRuntime>(runtime_options);
                if (!apply_fn)
                {
                    apply_fn = [this](const raft::LogEntry &entry)
                    {
                        if (entry.type != raft::LogEntryType::COMMAND)
                        {
                            return common::Status::OK();
                        }
                        return machine_.Apply(entry.index, entry.term, entry.payload);
                    };
                }
                ASSERT_TRUE(runtime_->Initialize(core_.get(), storage_.get(), apply_fn).ok());
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

            ::cpr::store::v1::StoreCommandResponse RegisterStore(
                store::StoreId id,
                std::uint64_t capacity = 1024,
                std::uint64_t used = 0,
                const std::string &request_id = "")
            {
                grpc::ServerContext context;
                ::cpr::store::v1::RegisterStoreRequest request;
                request.set_request_id(request_id.empty()
                                           ? "register-" + std::to_string(id)
                                           : request_id);
                request.mutable_store()->set_store_id(id);
                request.mutable_store()->mutable_address()->set_host("127.0.0." + std::to_string(id));
                request.mutable_store()->mutable_address()->set_port(
                    static_cast<std::uint32_t>(10000 + id));
                request.mutable_store()->set_total_capacity_bytes(capacity);
                request.mutable_store()->set_used_capacity_bytes(used);
                request.mutable_store()->set_state(::cpr::store::v1::STORE_STATE_RUNNING);
                request.mutable_store()->set_generation(1);
                request.set_timeout_ms(500);

                ::cpr::store::v1::StoreCommandResponse response;
                EXPECT_TRUE(store_rpc_->Register(&context, &request, &response).ok());
                return response;
            }

            ::cpr::store::v1::StoreCommandResponse StopStore(
                store::StoreId id,
                std::uint64_t generation,
                const std::string &request_id)
            {
                grpc::ServerContext context;
                ::cpr::store::v1::StoreStopRequest request;
                request.set_request_id(request_id);
                request.set_store_id(id);
                request.set_generation(generation);

                ::cpr::store::v1::StoreCommandResponse response;
                EXPECT_TRUE(store_rpc_->Stop(&context, &request, &response).ok());
                return response;
            }

            ::cpr::store::v1::StoreCommandResponse RemoveStore(
                store::StoreId id,
                std::uint64_t generation,
                const std::string &request_id)
            {
                grpc::ServerContext context;
                ::cpr::store::v1::StoreRemoveRequest request;
                request.set_request_id(request_id);
                request.set_store_id(id);
                request.set_generation(generation);

                ::cpr::store::v1::StoreCommandResponse response;
                EXPECT_TRUE(store_rpc_->Remove(&context, &request, &response).ok());
                return response;
            }

            void CreateTask(const std::string &task_id,
                            const raft::OpaquePayload &payload = {0x10})
            {
                store::TaskCreateRequest task;
                task.task_id = task_id;
                task.type = store::TaskType::CUSTOM;
                task.target_payload = payload;
                metadata::StoreControlResult result;
                ASSERT_TRUE(metadata_service_
                                ->CreateTaskForTest("create-" + task_id,
                                                    task,
                                                    std::chrono::milliseconds(500),
                                                    &result)
                                .ok());
            }

            ::cpr::store::v1::PollTasksResponse PollTasks(store::StoreId id,
                                                          std::uint32_t max_tasks)
            {
                grpc::ServerContext context;
                ::cpr::store::v1::PollTasksRequest request;
                request.set_store_id(id);
                request.set_generation(1);
                request.set_max_tasks(max_tasks);

                ::cpr::store::v1::PollTasksResponse response;
                EXPECT_TRUE(store_rpc_->PollTasks(&context, &request, &response).ok());
                return response;
            }

            ::cpr::store::v1::ReportTaskResultResponse ReportTask(
                store::StoreId id,
                const std::string &task_id,
                const std::string &payload = "done")
            {
                grpc::ServerContext context;
                ::cpr::store::v1::ReportTaskResultRequest request;
                request.set_store_id(id);
                request.set_generation(1);
                request.set_task_id(task_id);
                request.set_final_state(::cpr::store::v1::TASK_STATE_SUCCESS);
                request.set_result_payload(payload);

                ::cpr::store::v1::ReportTaskResultResponse response;
                EXPECT_TRUE(store_rpc_->ReportTaskResult(&context, &request, &response).ok());
                return response;
            }

            metadata::MetadataStateMachine machine_;
            std::unique_ptr<raft::RaftCore> core_;
            std::unique_ptr<raft::IRaftStorage> storage_;
            std::unique_ptr<raft::RaftRuntime> runtime_;
            std::unique_ptr<metadata::MetadataService> metadata_service_;
            std::unique_ptr<StoreRpcServiceAdapter> store_rpc_;
        };

        TEST_F(StoreMetadataTest, RegisterRejectsDuplicatesAndUpdatesOnlyAfterConsensusApply)
        {
            BlockingApply blocker;
            CreateRuntime(LeaderOptions(),
                          true,
                          nullptr,
                          [&blocker, this](const raft::LogEntry &entry)
                          {
                              return blocker.Apply(entry, &machine_);
                          });

            auto pending = std::async(std::launch::async,
                                      [this]
                                      {
                                          return RegisterStore(7);
                                      });
            ASSERT_TRUE(blocker.WaitUntilEntered(std::chrono::milliseconds(500)));
            EXPECT_EQ(pending.wait_for(std::chrono::milliseconds(20)),
                      std::future_status::timeout);

            store::StoreInfo missing;
            EXPECT_EQ(metadata_service_->GetStore(7, &missing).code(),
                      StatusCode::kNotFound);

            blocker.Release();
            const auto response = pending.get();
            ASSERT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);

            const auto duplicate = RegisterStore(7, 1024, 0, "register-dup");
            EXPECT_EQ(duplicate.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_BUSY);

            grpc::ServerContext context;
            ::cpr::store::v1::RegisterStoreRequest bad;
            bad.set_request_id("bad-used");
            bad.mutable_store()->set_store_id(8);
            bad.mutable_store()->mutable_address()->set_host("127.0.0.8");
            bad.mutable_store()->mutable_address()->set_port(10008);
            bad.mutable_store()->set_total_capacity_bytes(1);
            bad.mutable_store()->set_used_capacity_bytes(2);
            ::cpr::store::v1::StoreCommandResponse bad_response;
            ASSERT_TRUE(store_rpc_->Register(&context, &bad, &bad_response).ok());
            EXPECT_EQ(bad_response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_INVALID_ARGUMENT);

            EXPECT_EQ(metadata_service_->GetStore(8, &missing).code(),
                      StatusCode::kNotFound);
        }

        TEST_F(StoreMetadataTest, HeartbeatIsLeaderLocalAndNotPersistedInSnapshot)
        {
            CreateRuntime(LeaderOptions(), true);
            ASSERT_EQ(RegisterStore(7).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);

            common::LogIndex before_index = 0;
            common::Term before_term = 0;
            ASSERT_TRUE(machine_.GetLastApplied(&before_index, &before_term).ok());

            grpc::ServerContext context;
            ::cpr::store::v1::StoreHeartbeatRequest heartbeat;
            heartbeat.set_store_id(7);
            heartbeat.set_generation(12345);
            ::cpr::store::v1::StoreHeartbeatResponse heartbeat_response;
            ASSERT_TRUE(store_rpc_->Heartbeat(&context, &heartbeat, &heartbeat_response).ok());
            EXPECT_EQ(heartbeat_response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);

            store::StoreInfo store;
            ASSERT_TRUE(metadata_service_->GetStore(7, &store).ok());
            EXPECT_EQ(store.last_heartbeat_ms, 12345);
            EXPECT_EQ(store.generation, 1U);
            EXPECT_EQ(store.used_bytes, 0U);

            common::LogIndex after_index = 0;
            common::Term after_term = 0;
            ASSERT_TRUE(machine_.GetLastApplied(&after_index, &after_term).ok());
            EXPECT_EQ(after_index, before_index);
            EXPECT_EQ(after_term, before_term);

            raft::OpaquePayload snapshot;
            ASSERT_TRUE(machine_.CreateSnapshot(after_index, after_term, &snapshot).ok());

            metadata::MetadataStateMachine restored;
            ASSERT_TRUE(restored.RestoreSnapshot(snapshot).ok());
            ASSERT_TRUE(restored.GetStore(7, &store).ok());
            EXPECT_EQ(store.last_heartbeat_ms, 0);
        }

        TEST_F(StoreMetadataTest, StoreStateChangesAndPlacementUseAppliedStoreSnapshot)
        {
            CreateRuntime(LeaderOptions(), true);
            ASSERT_EQ(RegisterStore(7, 1000, 100).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            ASSERT_EQ(RegisterStore(8, 2000, 100).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            ASSERT_EQ(RegisterStore(9, 3000, 2900).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);

            ASSERT_EQ(StopStore(7, 1, "stop-7").status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);

            std::vector<store::StoreInfo> stores;
            ASSERT_TRUE(machine_.ListStores(&stores).ok());

            store::SimplePlacementPolicy policy;
            store::PlacementRequest request;
            request.replica_count = 1;
            request.required_capacity_bytes = 500;
            store::PlacementResult result;
            ASSERT_TRUE(policy.SelectStores(request, stores, &result).ok());
            ASSERT_EQ(result.selected_store_ids.size(), 1U);
            EXPECT_EQ(result.selected_store_ids[0], 8U);

            const auto removed = RemoveStore(7, 2, "remove-7");
            EXPECT_EQ(removed.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_EQ(metadata_service_->GetStore(7, nullptr).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST_F(StoreMetadataTest, FailedStoreCanBeAppliedAndIsExcludedFromRunningPlacement)
        {
            metadata::MetadataStateMachine machine;
            metadata::StoreBusinessCommand reg;
            reg.kind = metadata::StoreBusinessCommandKind::REGISTER_STORE;
            reg.store = MakeStore(7, 1024, 0);
            ASSERT_TRUE(ApplyStoreCommand(&machine, 1, "reg-7", "reg-7", reg).ok());

            metadata::StoreBusinessCommand fail;
            fail.kind = metadata::StoreBusinessCommandKind::UPDATE_STORE_STATE;
            fail.store_update.id = 7;
            fail.store_update.expected_generation = 1;
            fail.store_update.state = store::StoreState::FAILED;
            ASSERT_TRUE(ApplyStoreCommand(&machine, 2, "fail-7", "fail-7", fail).ok());

            store::StoreInfo store;
            ASSERT_TRUE(machine.GetStore(7, &store).ok());
            EXPECT_EQ(store.state, store::StoreState::FAILED);
            EXPECT_EQ(store.generation, 2U);

            std::vector<store::StoreInfo> stores;
            ASSERT_TRUE(machine.ListStores(&stores).ok());
            store::SimplePlacementPolicy policy;
            store::PlacementRequest request{1, 1};
            store::PlacementResult result{{99}};
            EXPECT_EQ(policy.SelectStores(request, stores, &result).code(),
                      StatusCode::kResourceExhausted);
            EXPECT_EQ(result.selected_store_ids, (std::vector<store::StoreId>{99}));
        }

        TEST_F(StoreMetadataTest, TaskPollingIsConsensusBackedAndDuplicateResultsAreIdempotent)
        {
            CreateRuntime(LeaderOptions(), true);
            ASSERT_EQ(RegisterStore(7).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            CreateTask("task-1");
            CreateTask("task-2");

            const auto poll = PollTasks(7, 1);
            ASSERT_EQ(poll.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            ASSERT_EQ(poll.tasks_size(), 1);
            EXPECT_EQ(poll.tasks(0).task_id(), "task-1");

            store::TaskRecord task;
            ASSERT_TRUE(metadata_service_->GetTask("task-1", &task).ok());
            EXPECT_EQ(task.state, store::TaskState::RUNNING);
            EXPECT_EQ(task.assigned_store, 7U);

            const auto first = ReportTask(7, "task-1");
            ASSERT_EQ(first.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_FALSE(first.duplicate_result());

            const auto second = ReportTask(7, "task-1");
            ASSERT_EQ(second.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            EXPECT_TRUE(second.duplicate_result());
        }

        TEST_F(StoreMetadataTest, ConcurrentPollingAssignsEachTaskOnce)
        {
            CreateRuntime(LeaderOptions(), true);
            ASSERT_EQ(RegisterStore(7).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            ASSERT_EQ(RegisterStore(8).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            CreateTask("task-1");
            CreateTask("task-2");
            CreateTask("task-3");

            auto poll7 = std::async(std::launch::async,
                                    [this]
                                    {
                                        return PollTasks(7, 2);
                                    });
            auto poll8 = std::async(std::launch::async,
                                    [this]
                                    {
                                        return PollTasks(8, 2);
                                    });

            std::vector<std::string> ids;
            auto first = poll7.get();
            auto second = poll8.get();
            ASSERT_EQ(first.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            ASSERT_EQ(second.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            for (const auto &task : first.tasks())
            {
                ids.push_back(task.task_id());
            }
            for (const auto &task : second.tasks())
            {
                ids.push_back(task.task_id());
            }
            std::sort(ids.begin(), ids.end());
            const auto unique_end = std::unique(ids.begin(), ids.end());
            EXPECT_EQ(unique_end, ids.end());
        }

        TEST_F(StoreMetadataTest, ErrorPathsReturnBusinessStatusAndDoNotMutateMetadata)
        {
            CreateRuntime(FollowerOptions(), false);
            const auto not_leader = RegisterStore(7);
            EXPECT_EQ(not_leader.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_NOT_LEADER);
            EXPECT_EQ(not_leader.leader().leader_id(), 1U);
            Shutdown();

            CreateRuntime(LeaderOptions(), true);
            ASSERT_EQ(RegisterStore(7).status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_OK);
            Shutdown();

            const auto stopped = RegisterStore(8);
            EXPECT_EQ(stopped.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_BUSY);

            metadata::MetadataStateMachine failed_apply_machine;
            CreateRuntime(LeaderOptions(),
                          true,
                          nullptr,
                          [](const raft::LogEntry &entry)
                          {
                              if (entry.type == raft::LogEntryType::COMMAND)
                              {
                                  return common::Status::InvalidArgument("apply rejected for test");
                              }
                              return common::Status::OK();
                          });
            const auto apply_failed = RegisterStore(9);
            EXPECT_EQ(apply_failed.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_INVALID_ARGUMENT);
            store::StoreInfo missing;
            EXPECT_EQ(metadata_service_->GetStore(9, &missing).code(),
                      StatusCode::kNotFound);
        }

        TEST_F(StoreMetadataTest, PersistenceFailurePropagatesAndDoesNotApplyStore)
        {
            CreateRuntime(LeaderOptions(),
                          true,
                          std::make_unique<FailingAppendStorage>());

            const auto response = RegisterStore(7);
            EXPECT_EQ(response.status().code(),
                      ::cpr::common::v1::RPC_STATUS_CODE_IO_ERROR);

            store::StoreInfo missing;
            EXPECT_EQ(metadata_service_->GetStore(7, &missing).code(),
                      StatusCode::kNotFound);
        }

        TEST_F(StoreMetadataTest, SnapshotRestoreKeepsBusinessStateAndSupportsIncrementalReplay)
        {
            metadata::MetadataStateMachine prefix;
            metadata::StoreBusinessCommand reg7;
            reg7.kind = metadata::StoreBusinessCommandKind::REGISTER_STORE;
            reg7.store = MakeStore(7, 1024, 100);
            ASSERT_TRUE(ApplyStoreCommand(&prefix, 1, "reg-7", "reg-7", reg7).ok());

            metadata::StoreBusinessCommand task1;
            task1.kind = metadata::StoreBusinessCommandKind::CREATE_TASK;
            task1.task_create.task_id = "task-1";
            task1.task_create.type = store::TaskType::CUSTOM;
            task1.task_create.target_payload = {0x01};
            ASSERT_TRUE(ApplyStoreCommand(&prefix, 2, "task-1", "task-1", task1).ok());

            common::LogIndex index = 0;
            common::Term term = 0;
            ASSERT_TRUE(prefix.GetLastApplied(&index, &term).ok());
            raft::OpaquePayload snapshot;
            ASSERT_TRUE(prefix.CreateSnapshot(index, term, &snapshot).ok());

            metadata::MetadataStateMachine restored;
            ASSERT_TRUE(restored.RestoreSnapshot(snapshot).ok());

            metadata::MetadataStateMachine full;
            ASSERT_TRUE(ApplyStoreCommand(&full, 1, "reg-7", "reg-7", reg7).ok());
            ASSERT_TRUE(ApplyStoreCommand(&full, 2, "task-1", "task-1", task1).ok());

            metadata::StoreBusinessCommand reg8;
            reg8.kind = metadata::StoreBusinessCommandKind::REGISTER_STORE;
            reg8.store = MakeStore(8, 2048, 0);
            ASSERT_TRUE(ApplyStoreCommand(&restored, 3, "reg-8", "reg-8", reg8).ok());
            ASSERT_TRUE(ApplyStoreCommand(&full, 3, "reg-8", "reg-8", reg8).ok());

            std::vector<store::StoreInfo> restored_stores;
            std::vector<store::StoreInfo> full_stores;
            ASSERT_TRUE(restored.ListStores(&restored_stores).ok());
            ASSERT_TRUE(full.ListStores(&full_stores).ok());
            ASSERT_EQ(restored_stores.size(), full_stores.size());
            for (std::size_t i = 0; i < full_stores.size(); ++i)
            {
                EXPECT_EQ(restored_stores[i].id, full_stores[i].id);
                EXPECT_EQ(restored_stores[i].generation, full_stores[i].generation);
                EXPECT_EQ(restored_stores[i].last_heartbeat_ms, 0);
            }

            std::vector<store::TaskRecord> restored_tasks;
            std::vector<store::TaskRecord> full_tasks;
            ASSERT_TRUE(restored.ListTasks(&restored_tasks).ok());
            ASSERT_TRUE(full.ListTasks(&full_tasks).ok());
            ASSERT_EQ(restored_tasks.size(), full_tasks.size());
            ASSERT_EQ(restored_tasks.front().task_id, full_tasks.front().task_id);
        }

        TEST_F(StoreMetadataTest, SnapshotRejectsCorruptPayloadAndPreservesExistingState)
        {
            metadata::MetadataStateMachine machine;
            metadata::StoreBusinessCommand reg;
            reg.kind = metadata::StoreBusinessCommandKind::REGISTER_STORE;
            reg.store = MakeStore(7, 1024, 0);
            ASSERT_TRUE(ApplyStoreCommand(&machine, 1, "reg-7", "reg-7", reg).ok());

            common::LogIndex index = 0;
            common::Term term = 0;
            raft::OpaquePayload snapshot;
            ASSERT_TRUE(machine.GetLastApplied(&index, &term).ok());
            ASSERT_TRUE(machine.CreateSnapshot(index, term, &snapshot).ok());

            raft::OpaquePayload corrupt = snapshot;
            corrupt.push_back(0xFF);
            EXPECT_EQ(machine.RestoreSnapshot(corrupt).code(),
                      StatusCode::kCorruption);

            store::StoreInfo store;
            ASSERT_TRUE(machine.GetStore(7, &store).ok());
            EXPECT_EQ(store.id, 7U);
        }

        TEST_F(StoreMetadataTest, LegacyEmptySnapshotIsStillAccepted)
        {
            raft::OpaquePayload legacy = {
                1,
                0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0};

            metadata::MetadataStateMachine machine;
            EXPECT_TRUE(machine.RestoreSnapshot(legacy).ok());
            common::LogIndex index = 1;
            common::Term term = 1;
            ASSERT_TRUE(machine.GetLastApplied(&index, &term).ok());
            EXPECT_EQ(index, common::kInvalidLogIndex);
            EXPECT_EQ(term, common::kInitialTerm);
        }

    } // namespace
} // namespace cpr::rpc
