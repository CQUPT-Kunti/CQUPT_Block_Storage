#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "metadata/metadata_command.h"
#include "metadata/metadata_service.h"
#include "metadata/metadata_state_machine.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"
#include "raft/raft_storage.h"

namespace cpr::metadata
{
    namespace
    {

        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::StatusCode;
        using cpr::common::Term;

        MetadataCommand MakeCommand(const std::string &command_id,
                                    const std::string &target_id,
                                    raft::OpaquePayload payload,
                                    std::optional<std::uint64_t> expected_generation = std::nullopt)
        {
            MetadataCommand command;
            command.type = MetadataCommandType::OPAQUE_OPERATION;
            command.command_id = command_id;
            command.target_id = target_id;
            command.expected_generation = expected_generation;
            command.payload = std::move(payload);
            return command;
        }

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
                return common::Status::IoError("append failed for test");
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
            common::Status Apply(raft::LogEntry entry,
                                 MetadataStateMachine *machine)
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

        class MetadataServiceTest : public ::testing::Test
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

                MetadataService::Options service_options;
                service_options.proposal_timeout = std::chrono::milliseconds(500);
                service_options.poll_interval = std::chrono::milliseconds(1);
                service_ = std::make_unique<MetadataService>(runtime_.get(),
                                                             &machine_,
                                                             service_options);
            }

            void Shutdown()
            {
                if (runtime_ && runtime_->state() == raft::RuntimeState::RUNNING)
                {
                    runtime_->RequestShutdown();
                    runtime_->WaitForShutdown();
                }
            }

            MetadataStateMachine machine_;
            std::unique_ptr<raft::RaftCore> core_;
            std::unique_ptr<raft::IRaftStorage> storage_;
            std::unique_ptr<raft::RaftRuntime> runtime_;
            std::unique_ptr<MetadataService> service_;
        };

        TEST_F(MetadataServiceTest, ProposeReturnsAfterCommittedApplyAndQueryReadsLocalState)
        {
            CreateRuntime(LeaderOptions(), true);

            MetadataProposalResult proposal;
            const common::Status status =
                service_->Propose(MakeCommand("cmd-1", "alpha", {0x10, 0x11}),
                                  &proposal);

            ASSERT_TRUE(status.ok());
            EXPECT_TRUE(proposal.final_result);
            EXPECT_EQ(proposal.command_id, "cmd-1");
            EXPECT_EQ(proposal.applied_index, 1U);

            MetadataQueryResult query;
            ASSERT_TRUE(service_->Query("alpha", &query).ok());
            EXPECT_EQ(query.record.target_id, "alpha");
            EXPECT_EQ(query.record.generation, 1U);
            EXPECT_EQ(query.record.payload, (raft::OpaquePayload{0x10, 0x11}));
            EXPECT_EQ(query.last_applied_index, 1U);
            EXPECT_EQ(query.last_applied_term, 1U);
        }

        TEST_F(MetadataServiceTest, InvalidCommandIsRejectedBeforeRuntimeQueue)
        {
            CreateRuntime(LeaderOptions(), true);

            MetadataCommand invalid = MakeCommand("", "alpha", {0x10});
            MetadataProposalResult proposal;
            const common::Status status = service_->Propose(invalid, &proposal);

            EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(runtime_->event_queue_size(), 0U);
            MetadataQueryResult query;
            EXPECT_EQ(service_->Query("alpha", &query).code(), StatusCode::kNotFound);
        }

        TEST_F(MetadataServiceTest, RuntimeNotRunningAndStoppedRuntimeRejectProposal)
        {
            raft::RaftCore core;
            ASSERT_TRUE(core.Initialize(LeaderOptions()).ok());
            ASSERT_TRUE(core.BecomeLeader().ok());
            raft::MemoryRaftStorage storage;
            ASSERT_TRUE(storage.Open("").ok());
            raft::RaftRuntime::Options runtime_options{64, 8, 8, 8, 8};
            raft::RaftRuntime runtime(runtime_options);
            ASSERT_TRUE(runtime.Initialize(&core, &storage).ok());
            MetadataStateMachine machine;
            MetadataService service(&runtime, &machine);

            MetadataProposalResult proposal;
            EXPECT_EQ(service.Propose(MakeCommand("cmd-1", "alpha", {0x10}),
                                      std::chrono::milliseconds(10),
                                      &proposal)
                          .code(),
                      StatusCode::kBusy);

            ASSERT_TRUE(runtime.Start().ok());
            ASSERT_TRUE(runtime.RequestShutdown().ok());
            runtime.WaitForShutdown();
            EXPECT_EQ(service.Propose(MakeCommand("cmd-2", "alpha", {0x20}),
                                      std::chrono::milliseconds(10),
                                      &proposal)
                          .code(),
                      StatusCode::kBusy);
        }

        TEST_F(MetadataServiceTest, NonLeaderResultPreservesKnownLeaderHint)
        {
            CreateRuntime(FollowerOptions(), false);

            MetadataProposalResult proposal;
            const common::Status status =
                service_->Propose(MakeCommand("cmd-1", "alpha", {0x10}),
                                  &proposal);

            EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(status.message(), "only the leader can accept proposals");
            EXPECT_EQ(proposal.command_id, "cmd-1");
            EXPECT_EQ(proposal.leader_id, 1U);
            EXPECT_EQ(proposal.leader_address.host, "10.0.0.1");
            EXPECT_EQ(proposal.leader_address.port, 9001U);
        }

        TEST_F(MetadataServiceTest, ApplyFailureIsReturnedAsFinalProposalResult)
        {
            CreateRuntime(LeaderOptions(), true);

            MetadataProposalResult proposal;
            const common::Status status =
                service_->Propose(MakeCommand("cmd-1", "missing", {0x10}, 1),
                                  &proposal);

            EXPECT_EQ(status.code(), StatusCode::kNotFound);
            EXPECT_TRUE(proposal.final_result);
            EXPECT_EQ(proposal.command_id, "cmd-1");
            MetadataQueryResult query;
            EXPECT_EQ(service_->Query("missing", &query).code(), StatusCode::kNotFound);
        }

        TEST_F(MetadataServiceTest, PersistenceFailureIsReturnedWithoutWaitingForTimeout)
        {
            CreateRuntime(LeaderOptions(),
                          true,
                          std::make_unique<FailingAppendStorage>());

            MetadataProposalResult proposal;
            const common::Status status =
                service_->Propose(MakeCommand("cmd-1", "alpha", {0x10}),
                                  std::chrono::milliseconds(500),
                                  &proposal);

            EXPECT_EQ(status.code(), StatusCode::kIoError);
            EXPECT_TRUE(proposal.final_result);
            EXPECT_EQ(proposal.command_id, "cmd-1");
            EXPECT_EQ(proposal.applied_index, 1U);
            MetadataQueryResult query;
            EXPECT_EQ(service_->Query("alpha", &query).code(), StatusCode::kNotFound);
        }

        TEST_F(MetadataServiceTest, ConcurrentProposalsKeepResultsAssociatedWithCommands)
        {
            CreateRuntime(LeaderOptions(), true);

            MetadataProposalResult first;
            MetadataProposalResult second;
            common::Status first_status;
            common::Status second_status;

            std::thread first_thread([&]
                                     {
                first_status = service_->Propose(
                    MakeCommand("cmd-1", "alpha", {0x01}), &first); });
            std::thread second_thread([&]
                                      {
                second_status = service_->Propose(
                    MakeCommand("cmd-2", "beta", {0x02}), &second); });

            first_thread.join();
            second_thread.join();

            ASSERT_TRUE(first_status.ok()) << first_status.ToString();
            ASSERT_TRUE(second_status.ok()) << second_status.ToString();
            EXPECT_EQ(first.command_id, "cmd-1");
            EXPECT_EQ(second.command_id, "cmd-2");
            EXPECT_NE(first.proposal_id, second.proposal_id);

            MetadataQueryResult alpha;
            MetadataQueryResult beta;
            ASSERT_TRUE(service_->Query("alpha", &alpha).ok());
            ASSERT_TRUE(service_->Query("beta", &beta).ok());
            EXPECT_EQ(alpha.record.payload, (raft::OpaquePayload{0x01}));
            EXPECT_EQ(beta.record.payload, (raft::OpaquePayload{0x02}));
        }

        TEST_F(MetadataServiceTest, TimeoutAbandonsOnlyThatProposalAndLateResultDoesNotPolluteNextRequest)
        {
            auto blocker = std::make_shared<BlockingApply>();
            CreateRuntime(
                LeaderOptions(),
                true,
                nullptr,
                [this, blocker](const raft::LogEntry &entry)
                { return blocker->Apply(entry, &machine_); });

            MetadataProposalResult timed_out;
            const common::Status timeout_status =
                service_->Propose(MakeCommand("cmd-1", "alpha", {0x01}),
                                  std::chrono::milliseconds(20),
                                  &timed_out);
            EXPECT_TRUE(blocker->WaitUntilEntered(std::chrono::milliseconds(500)));
            EXPECT_EQ(timeout_status.code(), StatusCode::kRetryLater);

            blocker->Release();

            MetadataProposalResult second;
            const common::Status second_status =
                service_->Propose(MakeCommand("cmd-2", "beta", {0x02}),
                                  std::chrono::milliseconds(500),
                                  &second);

            ASSERT_TRUE(second_status.ok());
            EXPECT_EQ(second.command_id, "cmd-2");
            EXPECT_NE(second.command_id, "cmd-1");
            MetadataQueryResult beta;
            ASSERT_TRUE(service_->Query("beta", &beta).ok());
            EXPECT_EQ(beta.record.payload, (raft::OpaquePayload{0x02}));
        }

        TEST_F(MetadataServiceTest, QueryRejectsInvalidTargetAndReturnsCopiesWithoutAdvancingProgress)
        {
            CreateRuntime(LeaderOptions(), true);
            MetadataProposalResult proposal;
            ASSERT_TRUE(service_->Propose(MakeCommand("cmd-1", "alpha", {0x10}),
                                          &proposal)
                            .ok());

            MetadataQueryResult query;
            EXPECT_EQ(service_->Query("", &query).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(service_->Query("missing", &query).code(), StatusCode::kNotFound);

            ASSERT_TRUE(service_->Query("alpha", &query).ok());
            query.record.payload[0] = 0xFF;

            MetadataQueryResult fresh;
            ASSERT_TRUE(service_->Query("alpha", &fresh).ok());
            EXPECT_EQ(fresh.record.payload, (raft::OpaquePayload{0x10}));
            EXPECT_EQ(fresh.last_applied_index, 1U);
        }

    } // namespace
} // namespace cpr::metadata
