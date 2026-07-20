#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "metadata/metadata_command.h"
#include "metadata/metadata_service.h"
#include "metadata/metadata_state_machine.h"
#include "metadata/state_machine.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"
#include "raft/raft_types.h"

namespace cpr::metadata
{
    namespace
    {

        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::StatusCode;
        using cpr::common::Term;

        constexpr std::chrono::milliseconds kShortWait{20};
        constexpr std::chrono::milliseconds kLongWait{1000};

        struct AppliedEntry
        {
            LogIndex index = common::kInvalidLogIndex;
            Term term = common::kInitialTerm;
            raft::OpaquePayload payload;
        };

        void AppendU32(std::uint32_t value, raft::OpaquePayload *out)
        {
            out->push_back(static_cast<common::Byte>((value >> 24) & 0xFF));
            out->push_back(static_cast<common::Byte>((value >> 16) & 0xFF));
            out->push_back(static_cast<common::Byte>((value >> 8) & 0xFF));
            out->push_back(static_cast<common::Byte>(value & 0xFF));
        }

        void AppendU64(std::uint64_t value, raft::OpaquePayload *out)
        {
            for (int shift = 56; shift >= 0; shift -= 8)
            {
                out->push_back(static_cast<common::Byte>((value >> shift) & 0xFF));
            }
        }

        bool ReadU32(const raft::OpaquePayload &input,
                     std::size_t *offset,
                     std::uint32_t *value)
        {
            if (offset == nullptr || value == nullptr || *offset + 4 > input.size())
            {
                return false;
            }
            *value = (static_cast<std::uint32_t>(input[*offset]) << 24) |
                     (static_cast<std::uint32_t>(input[*offset + 1]) << 16) |
                     (static_cast<std::uint32_t>(input[*offset + 2]) << 8) |
                     static_cast<std::uint32_t>(input[*offset + 3]);
            *offset += 4;
            return true;
        }

        bool ReadU64(const raft::OpaquePayload &input,
                     std::size_t *offset,
                     std::uint64_t *value)
        {
            if (offset == nullptr || value == nullptr || *offset + 8 > input.size())
            {
                return false;
            }
            std::uint64_t result = 0;
            for (int i = 0; i < 8; ++i)
            {
                result = (result << 8) | input[*offset + static_cast<std::size_t>(i)];
            }
            *offset += 8;
            *value = result;
            return true;
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

        class AlternateStateMachine final : public IRaftStateMachine
        {
        public:
            common::Status Apply(LogIndex index,
                                 Term term,
                                 const raft::OpaquePayload &command_payload) override
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (fail_next_apply_)
                {
                    fail_next_apply_ = false;
                    return common::Status::IoError("alternate apply failed");
                }
                if (index == common::kInvalidLogIndex)
                {
                    return common::Status::InvalidArgument("apply index must be positive");
                }
                if (index != last_index_ + 1)
                {
                    return common::Status::Busy("alternate apply index is not sequential");
                }
                entries_.push_back({index, term, command_payload});
                last_index_ = index;
                last_term_ = term;
                return common::Status::OK();
            }

            common::Status CreateSnapshot(LogIndex last_applied_index,
                                          Term last_applied_term,
                                          raft::OpaquePayload *snapshot_payload) override
            {
                if (snapshot_payload == nullptr)
                {
                    return common::Status::InvalidArgument("snapshot payload must not be null");
                }

                std::lock_guard<std::mutex> lock(mutex_);
                if (last_applied_index != last_index_ ||
                    last_applied_term != last_term_)
                {
                    return common::Status::Busy("alternate snapshot progress mismatch");
                }

                raft::OpaquePayload out;
                out.push_back(1);
                AppendU64(last_index_, &out);
                AppendU64(last_term_, &out);
                AppendU32(static_cast<std::uint32_t>(entries_.size()), &out);
                for (const AppliedEntry &entry : entries_)
                {
                    AppendU64(entry.index, &out);
                    AppendU64(entry.term, &out);
                    AppendU32(static_cast<std::uint32_t>(entry.payload.size()), &out);
                    out.insert(out.end(), entry.payload.begin(), entry.payload.end());
                }

                *snapshot_payload = std::move(out);
                return common::Status::OK();
            }

            common::Status RestoreSnapshot(const raft::OpaquePayload &snapshot_payload) override
            {
                if (snapshot_payload.empty() || snapshot_payload[0] != 1)
                {
                    return common::Status::Corruption("unknown alternate snapshot version");
                }

                std::size_t offset = 1;
                std::uint64_t last_index = 0;
                std::uint64_t last_term = 0;
                std::uint32_t count = 0;
                if (!ReadU64(snapshot_payload, &offset, &last_index) ||
                    !ReadU64(snapshot_payload, &offset, &last_term) ||
                    !ReadU32(snapshot_payload, &offset, &count))
                {
                    return common::Status::Corruption("truncated alternate snapshot header");
                }

                std::vector<AppliedEntry> candidate;
                candidate.reserve(count);
                LogIndex expected = 1;
                for (std::uint32_t i = 0; i < count; ++i)
                {
                    std::uint64_t index = 0;
                    std::uint64_t term = 0;
                    std::uint32_t payload_size = 0;
                    if (!ReadU64(snapshot_payload, &offset, &index) ||
                        !ReadU64(snapshot_payload, &offset, &term) ||
                        !ReadU32(snapshot_payload, &offset, &payload_size) ||
                        offset + payload_size > snapshot_payload.size())
                    {
                        return common::Status::Corruption("truncated alternate snapshot entry");
                    }
                    if (index != expected)
                    {
                        return common::Status::Corruption("alternate snapshot indexes are not sequential");
                    }

                    raft::OpaquePayload payload(snapshot_payload.begin() + static_cast<std::ptrdiff_t>(offset),
                                                snapshot_payload.begin() + static_cast<std::ptrdiff_t>(offset + payload_size));
                    offset += payload_size;
                    candidate.push_back({index, term, std::move(payload)});
                    expected = index + 1;
                }
                if (offset != snapshot_payload.size())
                {
                    return common::Status::Corruption("alternate snapshot has trailing bytes");
                }
                if ((candidate.empty() && last_index != 0) ||
                    (!candidate.empty() && candidate.back().index != last_index))
                {
                    return common::Status::Corruption("alternate snapshot progress is inconsistent");
                }

                std::lock_guard<std::mutex> lock(mutex_);
                entries_ = std::move(candidate);
                last_index_ = last_index;
                last_term_ = last_term;
                return common::Status::OK();
            }

            void FailNextApply()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                fail_next_apply_ = true;
            }

            std::vector<AppliedEntry> entries() const
            {
                std::lock_guard<std::mutex> lock(mutex_);
                return entries_;
            }

            LogIndex last_index() const
            {
                std::lock_guard<std::mutex> lock(mutex_);
                return last_index_;
            }

            Term last_term() const
            {
                std::lock_guard<std::mutex> lock(mutex_);
                return last_term_;
            }

        private:
            mutable std::mutex mutex_;
            std::vector<AppliedEntry> entries_;
            LogIndex last_index_ = common::kInvalidLogIndex;
            Term last_term_ = common::kInitialTerm;
            bool fail_next_apply_ = false;
        };

        class BlockingApply
        {
        public:
            common::Status Apply(const raft::LogEntry &entry,
                                 IRaftStateMachine *state_machine)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    entered_ = true;
                    cv_.notify_all();
                }

                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]
                         { return released_; });
                lock.unlock();

                if (entry.type != raft::LogEntryType::COMMAND)
                {
                    return common::Status::OK();
                }
                return state_machine->Apply(entry.index, entry.term, entry.payload);
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
                released_ = true;
                cv_.notify_all();
            }

        private:
            std::mutex mutex_;
            std::condition_variable cv_;
            bool entered_ = false;
            bool released_ = false;
        };

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

        raft::OpaquePayload EncodeCommand(const MetadataCommand &command)
        {
            raft::OpaquePayload payload;
            EXPECT_TRUE(EncodeMetadataCommand(command, &payload).ok());
            return payload;
        }

        class RuntimeHarness
        {
        public:
            RuntimeHarness() = default;

            ~RuntimeHarness()
            {
                Shutdown();
            }

            RuntimeHarness(const RuntimeHarness &) = delete;
            RuntimeHarness &operator=(const RuntimeHarness &) = delete;

            void Start(IRaftStateMachine *state_machine,
                       std::unique_ptr<raft::MemoryRaftStorage> storage = nullptr,
                       raft::RaftRuntime::ApplyFn apply_fn = nullptr,
                       raft::RaftCore::Options options = LeaderOptions())
            {
                ASSERT_NE(state_machine, nullptr);
                state_machine_ = state_machine;

                core_ = std::make_unique<raft::RaftCore>();
                ASSERT_TRUE(core_->Initialize(options).ok());
                ASSERT_TRUE(core_->BecomeLeader().ok());

                if (!storage)
                {
                    storage = std::make_unique<raft::MemoryRaftStorage>();
                    ASSERT_TRUE(storage->Open("").ok());
                }
                storage_ = std::move(storage);

                raft::RaftRuntime::Options runtime_options;
                runtime_options.event_queue_capacity = 64;
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
                        return state_machine_->Apply(entry.index, entry.term, entry.payload);
                    };
                }
                ASSERT_TRUE(runtime_->Initialize(core_.get(), storage_.get(), apply_fn).ok());
                ASSERT_TRUE(runtime_->Start().ok());
            }

            void Shutdown()
            {
                if (runtime_ && runtime_->state() == raft::RuntimeState::RUNNING)
                {
                    runtime_->RequestShutdown();
                    runtime_->WaitForShutdown();
                }
            }

            bool WaitForResult(std::uint64_t proposal_id,
                               std::chrono::milliseconds timeout,
                               raft::ProposalResult *result)
            {
                const auto deadline = std::chrono::steady_clock::now() + timeout;
                while (std::chrono::steady_clock::now() < deadline)
                {
                    auto cached = cached_results_.find(proposal_id);
                    if (cached != cached_results_.end())
                    {
                        if (result != nullptr)
                        {
                            *result = std::move(cached->second);
                        }
                        cached_results_.erase(cached);
                        return true;
                    }

                    for (raft::ProposalResult item : runtime_->CollectProposalResults(64))
                    {
                        if (item.proposal_id == proposal_id)
                        {
                            if (result != nullptr)
                            {
                                *result = std::move(item);
                            }
                            return true;
                        }
                        cached_results_.emplace(item.proposal_id, std::move(item));
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                return false;
            }

            std::vector<raft::ProposalResult> CollectResults()
            {
                return runtime_->CollectProposalResults(64);
            }

            common::Status Propose(std::uint64_t proposal_id,
                                   raft::OpaquePayload payload)
            {
                raft::ProposalEvent event;
                event.proposal_id = proposal_id;
                event.payload = std::move(payload);
                return runtime_->EnqueueProposal(std::move(event));
            }

            raft::RaftCore &core()
            {
                return *core_;
            }

            raft::MemoryRaftStorage &storage()
            {
                return *storage_;
            }

            raft::RaftRuntime &runtime()
            {
                return *runtime_;
            }

        private:
            IRaftStateMachine *state_machine_ = nullptr;
            std::unique_ptr<raft::RaftCore> core_;
            std::unique_ptr<raft::MemoryRaftStorage> storage_;
            std::unique_ptr<raft::RaftRuntime> runtime_;
            std::map<std::uint64_t, raft::ProposalResult> cached_results_;
        };

        TEST(StateMachineReplacementTest, AlternateStateMachineReceivesSequentialOpaqueProposalsInLogOrder)
        {
            AlternateStateMachine alternate;
            RuntimeHarness harness;
            harness.Start(&alternate);

            ASSERT_TRUE(harness.Propose(101, {0x01}).ok());
            raft::ProposalResult first;
            ASSERT_TRUE(harness.WaitForResult(101, kLongWait, &first));

            ASSERT_TRUE(harness.Propose(102, {0x02, 0x03}).ok());
            raft::ProposalResult second;
            ASSERT_TRUE(harness.WaitForResult(102, kLongWait, &second));

            ASSERT_TRUE(harness.Propose(103, {0x04}).ok());
            raft::ProposalResult third;
            ASSERT_TRUE(harness.WaitForResult(103, kLongWait, &third));

            EXPECT_TRUE(first.status.ok()) << first.status.ToString();
            EXPECT_TRUE(second.status.ok()) << second.status.ToString();
            EXPECT_TRUE(third.status.ok()) << third.status.ToString();
            EXPECT_EQ(first.log_index, 1U);
            EXPECT_EQ(second.log_index, 2U);
            EXPECT_EQ(third.log_index, 3U);

            const std::vector<AppliedEntry> applied = alternate.entries();
            ASSERT_EQ(applied.size(), 3U);
            EXPECT_EQ(applied[0].index, 1U);
            EXPECT_EQ(applied[0].payload, (raft::OpaquePayload{0x01}));
            EXPECT_EQ(applied[1].index, 2U);
            EXPECT_EQ(applied[1].payload, (raft::OpaquePayload{0x02, 0x03}));
            EXPECT_EQ(applied[2].index, 3U);
            EXPECT_EQ(applied[2].payload, (raft::OpaquePayload{0x04}));
        }

        TEST(StateMachineReplacementTest, ProposalResultIsNotFinalBeforeApplyCompletes)
        {
            AlternateStateMachine alternate;
            auto blocker = std::make_shared<BlockingApply>();
            RuntimeHarness harness;
            harness.Start(
                &alternate,
                nullptr,
                [blocker, &alternate](const raft::LogEntry &entry)
                { return blocker->Apply(entry, &alternate); });

            ASSERT_TRUE(harness.Propose(201, {0xAA}).ok());
            ASSERT_TRUE(blocker->WaitUntilEntered(kLongWait));
            EXPECT_FALSE(harness.WaitForResult(201, kShortWait, nullptr));
            EXPECT_TRUE(alternate.entries().empty());

            blocker->Release();

            raft::ProposalResult result;
            ASSERT_TRUE(harness.WaitForResult(201, kLongWait, &result));
            EXPECT_TRUE(result.status.ok()) << result.status.ToString();
            EXPECT_TRUE(result.final_result);
            EXPECT_EQ(result.proposal_id, 201U);
            EXPECT_EQ(result.log_index, 1U);
            ASSERT_EQ(alternate.entries().size(), 1U);
        }

        TEST(StateMachineReplacementTest, FailedApplyPropagatesAndDoesNotAdvanceAlternateState)
        {
            AlternateStateMachine alternate;
            alternate.FailNextApply();
            RuntimeHarness harness;
            harness.Start(&alternate);

            ASSERT_TRUE(harness.Propose(301, {0x55}).ok());

            raft::ProposalResult result;
            ASSERT_TRUE(harness.WaitForResult(301, kLongWait, &result));
            EXPECT_EQ(result.status.code(), StatusCode::kIoError);
            EXPECT_TRUE(result.final_result);
            EXPECT_TRUE(alternate.entries().empty());
        }

        TEST(StateMachineReplacementTest, DuplicateApplyCompletionDoesNotReapplyOrCompleteWrongRequest)
        {
            AlternateStateMachine alternate;
            RuntimeHarness harness;
            harness.Start(&alternate);

            ASSERT_TRUE(harness.Propose(401, {0x10}).ok());
            raft::ProposalResult result;
            ASSERT_TRUE(harness.WaitForResult(401, kLongWait, &result));
            ASSERT_TRUE(result.status.ok());
            ASSERT_EQ(alternate.entries().size(), 1U);

            raft::ApplyCompletionEvent duplicate;
            duplicate.applied_index = 1;
            duplicate.status = common::Status::OK();
            duplicate.proposal_id = 999;
            ASSERT_TRUE(harness.runtime().EnqueueApplyCompletion(std::move(duplicate)).ok());

            EXPECT_FALSE(harness.WaitForResult(999, kShortWait, nullptr));
            EXPECT_EQ(alternate.entries().size(), 1U);
        }

        TEST(StateMachineReplacementTest, MetadataServiceRunsCommandApplyQueryAndGenerationConflict)
        {
            MetadataStateMachine machine;
            RuntimeHarness harness;
            harness.Start(&machine);
            MetadataService::Options options;
            options.proposal_timeout = kLongWait;
            options.poll_interval = std::chrono::milliseconds(1);
            MetadataService service(&harness.runtime(), &machine, options);

            MetadataProposalResult create;
            ASSERT_TRUE(service.Propose(MakeCommand("cmd-1", "alpha", {0x01}),
                                        &create)
                            .ok());
            EXPECT_EQ(create.command_id, "cmd-1");
            EXPECT_EQ(create.applied_index, 1U);

            MetadataQueryResult query;
            ASSERT_TRUE(service.Query("alpha", &query).ok());
            EXPECT_EQ(query.record.payload, (raft::OpaquePayload{0x01}));
            EXPECT_EQ(query.record.generation, 1U);

            MetadataProposalResult update;
            ASSERT_TRUE(service.Propose(MakeCommand("cmd-2", "alpha", {0x02}, 1),
                                        &update)
                            .ok());

            MetadataProposalResult conflict;
            EXPECT_EQ(service.Propose(MakeCommand("cmd-3", "alpha", {0x03}, 1),
                                      &conflict)
                          .code(),
                      StatusCode::kBusy);
            EXPECT_TRUE(conflict.final_result);

            ASSERT_TRUE(service.Query("alpha", &query).ok());
            EXPECT_EQ(query.record.payload, (raft::OpaquePayload{0x02}));
            EXPECT_EQ(query.record.generation, 2U);
            EXPECT_EQ(query.record.last_command_id, "cmd-2");
        }

        TEST(StateMachineReplacementTest, ConcurrentMetadataServiceProposalsKeepCommandResultsIsolated)
        {
            MetadataStateMachine machine;
            RuntimeHarness harness;
            harness.Start(&machine);
            MetadataService service(&harness.runtime(), &machine);

            MetadataProposalResult first;
            MetadataProposalResult second;
            common::Status first_status;
            common::Status second_status;

            std::thread first_thread([&]
                                     { first_status = service.Propose(
                                           MakeCommand("cmd-1", "alpha", {0x01}), &first); });
            std::thread second_thread([&]
                                      { second_status = service.Propose(
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
            ASSERT_TRUE(service.Query("alpha", &alpha).ok());
            ASSERT_TRUE(service.Query("beta", &beta).ok());
            EXPECT_EQ(alpha.record.payload, (raft::OpaquePayload{0x01}));
            EXPECT_EQ(beta.record.payload, (raft::OpaquePayload{0x02}));
        }

        TEST(StateMachineReplacementTest, MetadataSnapshotPlusIncrementalReplayMatchesFullRuntimeReplay)
        {
            const MetadataCommand c1 = MakeCommand("cmd-1", "alpha", {0x01});
            const MetadataCommand c2 = MakeCommand("cmd-2", "beta", {0x10});
            const MetadataCommand c3 = MakeCommand("cmd-3", "alpha", {0x02}, 1);

            MetadataStateMachine full;
            RuntimeHarness full_harness;
            full_harness.Start(&full);
            MetadataService full_service(&full_harness.runtime(), &full);
            MetadataProposalResult full_result;
            ASSERT_TRUE(full_service.Propose(c1, &full_result).ok());
            ASSERT_TRUE(full_service.Propose(c2, &full_result).ok());
            ASSERT_TRUE(full_service.Propose(c3, &full_result).ok());

            MetadataStateMachine prefix;
            RuntimeHarness prefix_harness;
            prefix_harness.Start(&prefix);
            MetadataService prefix_service(&prefix_harness.runtime(), &prefix);
            MetadataProposalResult prefix_result;
            ASSERT_TRUE(prefix_service.Propose(c1, &prefix_result).ok());
            ASSERT_TRUE(prefix_service.Propose(c2, &prefix_result).ok());

            raft::OpaquePayload business_snapshot;
            ASSERT_TRUE(prefix.CreateSnapshot(2, 1, &business_snapshot).ok());

            MetadataStateMachine restored;
            ASSERT_TRUE(restored.RestoreSnapshot(business_snapshot).ok());
            ASSERT_TRUE(restored.Apply(3, 1, EncodeCommand(c3)).ok());

            MetadataQueryResult full_alpha;
            MetadataQueryResult full_beta;
            ASSERT_TRUE(full_service.Query("alpha", &full_alpha).ok());
            ASSERT_TRUE(full_service.Query("beta", &full_beta).ok());

            MetadataStateRecord restored_alpha;
            MetadataStateRecord restored_beta;
            ASSERT_TRUE(restored.GetRecord("alpha", &restored_alpha).ok());
            ASSERT_TRUE(restored.GetRecord("beta", &restored_beta).ok());
            LogIndex restored_index = 0;
            Term restored_term = 0;
            ASSERT_TRUE(restored.GetLastApplied(&restored_index, &restored_term).ok());

            EXPECT_EQ(restored_alpha.payload, full_alpha.record.payload);
            EXPECT_EQ(restored_alpha.generation, full_alpha.record.generation);
            EXPECT_EQ(restored_alpha.last_command_id, full_alpha.record.last_command_id);
            EXPECT_EQ(restored_beta.payload, full_beta.record.payload);
            EXPECT_EQ(restored_beta.generation, full_beta.record.generation);
            EXPECT_EQ(restored_beta.last_command_id, full_beta.record.last_command_id);
            EXPECT_EQ(restored_index, full_alpha.last_applied_index);
            EXPECT_EQ(restored_term, full_alpha.last_applied_term);
        }

        TEST(StateMachineReplacementTest, AlternateSnapshotIsOpaqueToRaftInterfaces)
        {
            AlternateStateMachine alternate;
            IRaftStateMachine *state_machine = &alternate;
            ASSERT_TRUE(state_machine->Apply(1, 1, {0x01}).ok());
            ASSERT_TRUE(state_machine->Apply(2, 1, {0x02, 0x03}).ok());

            raft::OpaquePayload snapshot;
            ASSERT_TRUE(state_machine->CreateSnapshot(2, 1, &snapshot).ok());

            AlternateStateMachine restored;
            IRaftStateMachine *restored_interface = &restored;
            ASSERT_TRUE(restored_interface->RestoreSnapshot(snapshot).ok());
            const std::vector<AppliedEntry> entries = restored.entries();
            ASSERT_EQ(entries.size(), 2U);
            EXPECT_EQ(entries[0].payload, (raft::OpaquePayload{0x01}));
            EXPECT_EQ(entries[1].payload, (raft::OpaquePayload{0x02, 0x03}));
            EXPECT_EQ(restored.last_index(), 2U);
            EXPECT_EQ(restored.last_term(), 1U);
        }

    } // namespace
} // namespace cpr::metadata
