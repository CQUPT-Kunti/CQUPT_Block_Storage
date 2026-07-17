#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "common/types.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_core.h"
#include "raft/raft_runtime.h"

namespace cpr::raft
{
    namespace
    {

        using cpr::common::Byte;
        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::StatusCode;
        using cpr::common::Term;

        // =================================================================
        //  Test utilities
        // =================================================================

        // Records applied entries for verification.
        class ApplyRecorder
        {
        public:
            common::Status Apply(const LogEntry &entry)
            {
                const bool should_fail = fail_next_.exchange(false);
                common::Status result = should_fail ? fail_status_ : common::Status::OK();
                std::lock_guard<std::mutex> lock(mutex_);
                entries_.push_back(entry);
                applied_.push_back(entry.index);
                cv_.notify_all();
                return result;
            }

            void SetFailNext(common::Status status)
            {
                fail_next_ = true;
                fail_status_ = std::move(status);
            }

            std::vector<LogIndex> WaitForApplied(std::size_t count,
                                                 int timeout_ms = 3000)
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                             [this, count]
                             { return applied_.size() >= count; });
                return applied_;
            }

            std::vector<LogIndex> Applied() const
            {
                std::lock_guard<std::mutex> lock(mutex_);
                return applied_;
            }

            std::size_t AppliedCount() const
            {
                std::lock_guard<std::mutex> lock(mutex_);
                return applied_.size();
            }

            void Reset()
            {
                std::lock_guard<std::mutex> lock(mutex_);
                entries_.clear();
                applied_.clear();
                fail_next_ = false;
            }

        private:
            mutable std::mutex mutex_;
            std::condition_variable cv_;
            std::vector<LogEntry> entries_;
            std::vector<LogIndex> applied_;
            std::atomic<bool> fail_next_{false};
            common::Status fail_status_;
        };

        // Build a leader core with single-node cluster (self-vote = majority).
        RaftCore::Options LeaderOptions(NodeId id = 1, LogIndex term = 1)
        {
            RaftCore::Options opts;
            opts.node_id = id;
            opts.initial_role = RaftRole::CANDIDATE;
            opts.election_timeout_ticks = 10;
            opts.hard_state.current_term = term;
            opts.voter_ids = {id};
            return opts;
        }

        RaftCore::Options FollowerOptions(NodeId id = 2,
                                          std::vector<NodeId> voters = {1, 2, 3})
        {
            RaftCore::Options opts;
            opts.node_id = id;
            opts.initial_role = RaftRole::FOLLOWER;
            opts.election_timeout_ticks = 10;
            opts.hard_state.current_term = 1;
            opts.voter_ids = std::move(voters);
            return opts;
        }

        ProposalEvent MakeProposal(std::uint64_t pid, Byte data = 0x01)
        {
            ProposalEvent p;
            p.proposal_id = pid;
            p.payload = {data};
            return p;
        }

        // =================================================================
        //  Runtime Test Fixture
        // =================================================================

        class RuntimeTest : public ::testing::Test
        {
        protected:
            void SetUp() override
            {
                recorder_.Reset();
            }

            void TearDown() override
            {
                if (rt_ && rt_->state() == RuntimeState::RUNNING)
                {
                    rt_->RequestShutdown();
                    rt_->WaitForShutdown();
                }
            }

            // Creates a fully initialized + started runtime with a leader core.
            void CreateLeaderRuntime(std::size_t event_cap = 64,
                                     std::size_t persist_cap = 8,
                                     std::size_t apply_cap = 8,
                                     std::size_t result_cap = 8,
                                     std::size_t peer_cap = 8,
                                     RaftRuntime::ApplyFn fn = nullptr)
            {
                core_ = std::make_unique<RaftCore>();
                ASSERT_TRUE(core_->Initialize(LeaderOptions()).ok());
                ASSERT_TRUE(core_->BecomeLeader().ok());
                storage_ = std::make_unique<MemoryRaftStorage>();
                ASSERT_TRUE(storage_->Open("").ok());

                RaftRuntime::Options opts;
                opts.event_queue_capacity = event_cap;
                opts.persistence_queue_capacity = persist_cap;
                opts.apply_queue_capacity = apply_cap;
                opts.proposal_result_queue_capacity = result_cap;
                opts.peer_queue_capacity = peer_cap;

                rt_ = std::make_unique<RaftRuntime>(opts);
                if (!fn)
                    fn = [this](const LogEntry &e)
                    { return recorder_.Apply(e); };
                ASSERT_TRUE(rt_->Initialize(core_.get(), storage_.get(), fn).ok());
                ASSERT_TRUE(rt_->Start().ok());
            }

            // Creates with a follower core (for non-leader tests).
            void CreateFollowerRuntime(std::size_t event_cap = 64,
                                       std::size_t persist_cap = 8,
                                       std::size_t apply_cap = 8,
                                       std::size_t result_cap = 8,
                                       std::size_t peer_cap = 8)
            {
                core_ = std::make_unique<RaftCore>();
                ASSERT_TRUE(core_->Initialize(FollowerOptions()).ok());
                storage_ = std::make_unique<MemoryRaftStorage>();
                ASSERT_TRUE(storage_->Open("").ok());

                RaftRuntime::Options opts;
                opts.event_queue_capacity = event_cap;
                opts.persistence_queue_capacity = persist_cap;
                opts.apply_queue_capacity = apply_cap;
                opts.proposal_result_queue_capacity = result_cap;
                opts.peer_queue_capacity = peer_cap;

                rt_ = std::make_unique<RaftRuntime>(opts);
                auto fn = [this](const LogEntry &e)
                { return recorder_.Apply(e); };
                ASSERT_TRUE(rt_->Initialize(core_.get(), storage_.get(), fn).ok());
                ASSERT_TRUE(rt_->Start().ok());
            }

            void Shutdown()
            {
                if (rt_)
                {
                    rt_->RequestShutdown();
                    rt_->WaitForShutdown();
                }
            }

            std::unique_ptr<RaftRuntime> rt_;
            std::unique_ptr<RaftCore> core_;
            std::unique_ptr<MemoryRaftStorage> storage_;
            ApplyRecorder recorder_;
        };

        // =================================================================
        //  1. Runtime initialization and lifecycle
        // =================================================================

        TEST_F(RuntimeTest, ZeroQueueCapacityRejected)
        {
            RaftRuntime::Options opts{};
            RaftRuntime rt(opts);
            RaftCore core;
            ASSERT_TRUE(core.Initialize(LeaderOptions()).ok());
            MemoryRaftStorage storage;
            ASSERT_TRUE(storage.Open("").ok());
            EXPECT_EQ(rt.Initialize(&core, &storage).code(),
                      StatusCode::kInvalidArgument); // peer_queue_capacity = 0
        }

        TEST_F(RuntimeTest, NullCoreOrStorageRejected)
        {
            RaftRuntime::Options opts{64, 8, 8, 8, 8};
            RaftRuntime rt(opts);
            EXPECT_EQ(rt.Initialize(nullptr, nullptr).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST_F(RuntimeTest, LifecycleStateTransitions)
        {
            RaftRuntime::Options opts{64, 8, 8, 8, 8};
            RaftRuntime rt(opts);
            EXPECT_EQ(rt.state(), RuntimeState::CREATED);

            RaftCore core;
            ASSERT_TRUE(core.Initialize(LeaderOptions()).ok());
            MemoryRaftStorage storage;
            ASSERT_TRUE(storage.Open("").ok());
            ASSERT_TRUE(rt.Initialize(&core, &storage).ok());
            EXPECT_EQ(rt.state(), RuntimeState::INITIALIZED);

            ASSERT_TRUE(rt.Start().ok());
            EXPECT_EQ(rt.state(), RuntimeState::RUNNING);

            ASSERT_TRUE(rt.RequestShutdown().ok());
            EXPECT_EQ(rt.state(), RuntimeState::STOPPING);

            rt.WaitForShutdown();
            EXPECT_EQ(rt.state(), RuntimeState::STOPPED);
        }

        TEST_F(RuntimeTest, EnqueueBeforeStartRejected)
        {
            RaftRuntime::Options opts{64, 8, 8, 8, 8};
            RaftRuntime rt(opts);
            RaftCore core;
            ASSERT_TRUE(core.Initialize(LeaderOptions()).ok());
            MemoryRaftStorage storage;
            ASSERT_TRUE(storage.Open("").ok());
            ASSERT_TRUE(rt.Initialize(&core, &storage).ok());
            EXPECT_EQ(rt.EnqueueTick().code(), StatusCode::kBusy);
        }

        TEST_F(RuntimeTest, CannotStartTwice)
        {
            CreateLeaderRuntime();
            EXPECT_EQ(rt_->Start().code(), StatusCode::kBusy);
            Shutdown();
        }

        TEST_F(RuntimeTest, RepeatShutdownAndWaitSafe)
        {
            CreateLeaderRuntime();
            ASSERT_TRUE(rt_->RequestShutdown().ok());
            // Second shutdown returns busy (already stopping).
            EXPECT_EQ(rt_->RequestShutdown().code(), StatusCode::kBusy);
            rt_->WaitForShutdown();
            rt_->WaitForShutdown(); // safe to repeat
            EXPECT_EQ(rt_->state(), RuntimeState::STOPPED);
        }

        TEST_F(RuntimeTest, AfterShutdownNewEventsRejected)
        {
            CreateLeaderRuntime();
            ASSERT_TRUE(rt_->RequestShutdown().ok());
            rt_->WaitForShutdown();
            EXPECT_EQ(rt_->EnqueueTick().code(), StatusCode::kBusy);
        }

        TEST_F(RuntimeTest, DestructorCleansUpThreads)
        {
            // Just verify no crash/hang/leak.
            {
                RaftRuntime::Options opts{64, 8, 8, 8, 8};
                RaftRuntime rt(opts);
                RaftCore core;
                ASSERT_TRUE(core.Initialize(LeaderOptions()).ok());
                ASSERT_TRUE(core.BecomeLeader().ok());
                MemoryRaftStorage storage;
                ASSERT_TRUE(storage.Open("").ok());
                ASSERT_TRUE(rt.Initialize(&core, &storage).ok());
                ASSERT_TRUE(rt.Start().ok());
            }
            SUCCEED();
        }

        // =================================================================
        //  2. Event queue FIFO and backpressure
        // =================================================================

        TEST_F(RuntimeTest, QueueFullReturnsBackpressure)
        {
            // Use a tiny queue and fill it faster than the protocol thread drains.
            // Push events in a tight loop until we get backpressure.
            CreateLeaderRuntime(2, 8, 8, 8, 8);
            int accepted = 0;
            int rejected = 0;
            for (int i = 0; i < 10; ++i)
            {
                auto s = rt_->EnqueueTick();
                if (s.ok())
                    ++accepted;
                else
                {
                    ++rejected;
                    break;
                }
            }
            EXPECT_GT(rejected, 0);
            // Max 2 accepted for capacity 2.
            EXPECT_LE(accepted, 2);
            Shutdown();
        }

        TEST_F(RuntimeTest, MembershipChangeEventProducesResult)
        {
            CreateLeaderRuntime();
            MembershipChangeEvent mc;
            mc.kind = MembershipChangeEvent::Kind::ADD_LEARNER;
            mc.request_id = "membership-1";
            mc.member.node_id = 2;
            mc.member.address.host = "10.0.0.2";
            mc.member.address.port = 9002;
            ASSERT_TRUE(rt_->EnqueueMembershipChange(mc).ok());

            MembershipChangeResult result;
            for (int i = 0; i < 1000; ++i)
            {
                if (rt_->TryTakeMembershipChangeResult(mc.request_id, &result))
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            EXPECT_EQ(result.request_id, mc.request_id);
            Shutdown();
        }

        // =================================================================
        //  3. Protocol thread serial execution
        // =================================================================

        TEST_F(RuntimeTest, TickDrivesRaftCore)
        {
            CreateLeaderRuntime();
            // Propose first, then tick to process.
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(1)).ok());
            // Wait for apply.
            auto applied = recorder_.WaitForApplied(1, 2000);
            EXPECT_GE(applied.size(), 1U);
            Shutdown();
        }

        // =================================================================
        //  4. Persistence gating
        // =================================================================

        TEST_F(RuntimeTest, LeaderProposalGoesThroughPersistence)
        {
            CreateLeaderRuntime();
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(1)).ok());

            // Wait for apply.
            auto applied = recorder_.WaitForApplied(1, 2000);
            EXPECT_GE(applied.size(), 1U);

            // Poll for proposal result.
            std::vector<ProposalResult> results;
            for (int i = 0; i < 20 && results.empty(); ++i)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                results = rt_->CollectProposalResults(10);
            }
            ASSERT_GE(results.size(), 1U);
            EXPECT_TRUE(results[0].status.ok());
            EXPECT_TRUE(results[0].final_result);
            Shutdown();
        }

        TEST_F(RuntimeTest, NonLeaderProposalReturnsFailure)
        {
            CreateFollowerRuntime();
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(42)).ok());

            // Wait for result (non-leader failure is immediate).
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            auto results = rt_->CollectProposalResults(10);
            ASSERT_GE(results.size(), 1U);
            EXPECT_EQ(results[0].proposal_id, 42U);
            EXPECT_FALSE(results[0].status.ok());
            EXPECT_TRUE(results[0].final_result);
            Shutdown();
        }

        // =================================================================
        //  5. Apply ordering
        // =================================================================

        TEST_F(RuntimeTest, CommittedEntriesAppliedInOrder)
        {
            CreateLeaderRuntime();
            for (std::uint64_t i = 1; i <= 3; ++i)
            {
                ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(i)).ok());
            }
            auto applied = recorder_.WaitForApplied(3, 3000);
            ASSERT_GE(applied.size(), 3U);
            EXPECT_EQ(applied[0], 1U);
            EXPECT_EQ(applied[1], 2U);
            EXPECT_EQ(applied[2], 3U);
            Shutdown();
        }

        TEST_F(RuntimeTest, ApplyFailureBlocksSubsequentEntries)
        {
            CreateLeaderRuntime(64, 8, 8, 8, 8);
            recorder_.SetFailNext(common::Status::InternalError("fail"));

            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(1)).ok());
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(2)).ok());

            std::this_thread::sleep_for(std::chrono::milliseconds(300));

            // Collect proposal results — must include at least one failure.
            auto results = rt_->CollectProposalResults(10);
            bool found_failure = false;
            for (const auto &r : results)
            {
                if (!r.status.ok())
                    found_failure = true;
            }
            EXPECT_TRUE(found_failure);
            Shutdown();
        }

        // =================================================================
        //  6. Proposal result semantics
        // =================================================================

        TEST_F(RuntimeTest, NonLeaderProposalNoCommitOrApply)
        {
            CreateFollowerRuntime();
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(1)).ok());

            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            auto results = rt_->CollectProposalResults(10);
            ASSERT_GE(results.size(), 1U);
            EXPECT_FALSE(results[0].status.ok());
            Shutdown();
        }

        TEST_F(RuntimeTest, ProposalResultDeliveredOnlyOnce)
        {
            CreateLeaderRuntime();
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(1)).ok());

            // Wait for apply, then wait a bit more for the completion event.
            auto applied = recorder_.WaitForApplied(1, 2000);
            ASSERT_GE(applied.size(), 1U);

            // Poll for results with short timeout.
            std::vector<ProposalResult> r1;
            for (int i = 0; i < 20 && r1.empty(); ++i)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                r1 = rt_->CollectProposalResults(10);
            }
            ASSERT_GE(r1.size(), 1U);
            EXPECT_TRUE(r1[0].final_result);
            EXPECT_TRUE(r1[0].status.ok());

            auto r2 = rt_->CollectProposalResults(10);
            EXPECT_TRUE(r2.empty());
            Shutdown();
        }

        // =================================================================
        //  7. Peer outbound queues
        // =================================================================

        TEST_F(RuntimeTest, PeerQueuesCreatedForRemoteNodes)
        {
            // Leader with voters {1,2,3} should create queues for 2 and 3.
            RaftRuntime::Options opts{64, 8, 8, 8, 8};
            RaftRuntime rt(opts);
            RaftCore core;
            auto copts = LeaderOptions(1);
            copts.voter_ids = {1, 2, 3};
            ASSERT_TRUE(core.Initialize(copts).ok());
            ASSERT_TRUE(core.BecomeLeader().ok());
            MemoryRaftStorage storage;
            ASSERT_TRUE(storage.Open("").ok());
            ASSERT_TRUE(rt.Initialize(&core, &storage).ok());
            ASSERT_TRUE(rt.Start().ok());

            EXPECT_EQ(rt.peer_count(), 2U);
            EXPECT_TRUE(rt.IsPeerRegistered(2));
            EXPECT_TRUE(rt.IsPeerRegistered(3));
            EXPECT_FALSE(rt.IsPeerRegistered(1)); // self
            EXPECT_FALSE(rt.IsPeerRegistered(99));

            rt.RequestShutdown();
            rt.WaitForShutdown();
        }

        TEST_F(RuntimeTest, PeerQueueBackpressureIndependent)
        {
            RaftRuntime::Options opts{64, 8, 8, 8, 1}; // peer cap = 1
            RaftRuntime rt(opts);
            RaftCore core;
            auto copts = LeaderOptions(1);
            copts.voter_ids = {1, 2, 3};
            ASSERT_TRUE(core.Initialize(copts).ok());
            ASSERT_TRUE(core.BecomeLeader().ok());
            MemoryRaftStorage storage;
            ASSERT_TRUE(storage.Open("").ok());
            ASSERT_TRUE(rt.Initialize(&core, &storage).ok());
            ASSERT_TRUE(rt.Start().ok());

            // Generation of output messages happens when processing responses.
            // With peer cap=1, one peer being full shouldn't stop the other.
            EXPECT_EQ(rt.PeerQueueSize(2), 0U);
            EXPECT_EQ(rt.PeerQueueSize(3), 0U);

            rt.RequestShutdown();
            rt.WaitForShutdown();
        }

        // =================================================================
        //  8. Concurrent proposals
        // =================================================================

        TEST_F(RuntimeTest, ConcurrentProposalsDontCorruptOrder)
        {
            CreateLeaderRuntime(64, 16, 16, 16, 16);

            constexpr int kThreads = 4;
            constexpr int kPropsPerThread = 8;
            std::atomic<int> next_id{1};
            std::vector<std::thread> threads;

            for (int t = 0; t < kThreads; ++t)
            {
                threads.emplace_back([this, &next_id]()
                                     {
                    for (int i = 0; i < kPropsPerThread; ++i)
                    {
                        int id = next_id.fetch_add(1);
                        auto prop = MakeProposal(static_cast<std::uint64_t>(id));
                        rt_->EnqueueProposal(prop);
                    } });
            }

            for (auto &th : threads)
                th.join();

            auto applied = recorder_.WaitForApplied(
                kThreads * kPropsPerThread, 5000);

            // All proposals should have been applied.
            EXPECT_EQ(applied.size(), kThreads * kPropsPerThread);

            // Verify sequential order.
            for (size_t i = 0; i < applied.size(); ++i)
            {
                EXPECT_EQ(applied[i], i + 1);
            }
            Shutdown();
        }

        TEST_F(RuntimeTest, ConcurrentProposalsRespectQueueCapacity)
        {
            CreateLeaderRuntime(8, 16, 16, 16, 16);

            constexpr int kTotal = 100;
            std::atomic<int> accepted{0};
            std::atomic<int> rejected{0};
            std::vector<std::thread> threads;

            for (int t = 0; t < 4; ++t)
            {
                threads.emplace_back([this, &accepted, &rejected]()
                                     {
                    for (int i = 0; i < 25; ++i)
                    {
                        auto prop = MakeProposal(static_cast<std::uint64_t>(i + 1));
                        auto s = rt_->EnqueueProposal(prop);
                        if (s.ok()) accepted.fetch_add(1);
                        else rejected.fetch_add(1);
                    } });
            }

            for (auto &th : threads)
                th.join();

            // Some proposals should have been accepted, some rejected.
            EXPECT_GT(accepted.load(), 0);
            EXPECT_GT(rejected.load(), 0);
            EXPECT_EQ(accepted.load() + rejected.load(), kTotal);

            Shutdown();
        }

        // =================================================================
        //  9. Shutdown behavior
        // =================================================================

        TEST_F(RuntimeTest, ShutdownCompletesInProgressPersistence)
        {
            CreateLeaderRuntime(64, 8, 8, 8, 8);
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(1)).ok());
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            // Shutdown while operation is in progress.
            ASSERT_TRUE(rt_->RequestShutdown().ok());
            rt_->WaitForShutdown();

            // Threads should be joined.
            SUCCEED();
        }

        TEST_F(RuntimeTest, ShutdownNoJoinableThreadsRemaining)
        {
            CreateLeaderRuntime();
            ASSERT_TRUE(rt_->RequestShutdown().ok());
            rt_->WaitForShutdown();
            // No way to directly verify joinability, but WaitForShutdown
            // does not crash or hang.
            SUCCEED();
        }

        // =================================================================
        //  10. ProposalResult overflow detection
        // =================================================================

        TEST_F(RuntimeTest, ProposalResultOverflowDetected)
        {
            CreateFollowerRuntime(64, 8, 8, 2, 8);
            EXPECT_FALSE(rt_->HasProposalResultOverflow());

            // Generate more results than capacity (2).
            for (int i = 0; i < 5; ++i)
            {
                ASSERT_TRUE(rt_->EnqueueProposal(
                                   MakeProposal(static_cast<std::uint64_t>(i + 1)))
                                .ok());
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(300));

            EXPECT_TRUE(rt_->HasProposalResultOverflow());

            // Only 2 results should be available.
            auto results = rt_->CollectProposalResults(10);
            EXPECT_EQ(results.size(), 2U);

            // Overflow flag should persist until cleared.
            EXPECT_TRUE(rt_->HasProposalResultOverflow());

            rt_->ClearProposalResultOverflow();
            EXPECT_FALSE(rt_->HasProposalResultOverflow());

            Shutdown();
        }

        // =================================================================
        //  11. ProposalResult overflow does not lose accepted proposals
        // =================================================================

        TEST_F(RuntimeTest, ProposalResultOverflowDoesNotDiscardAlreadyAccepted)
        {
            CreateLeaderRuntime(64, 8, 8, 2, 8);
            EXPECT_FALSE(rt_->HasProposalResultOverflow());

            // Enqueue one proposal that will succeed.
            ASSERT_TRUE(rt_->EnqueueProposal(MakeProposal(1)).ok());

            // Wait for it to apply.
            recorder_.WaitForApplied(1, 2000);

            // Poll for the result.
            std::vector<ProposalResult> r1;
            for (int i = 0; i < 20 && r1.empty(); ++i)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                r1 = rt_->CollectProposalResults(10);
            }
            EXPECT_EQ(r1.size(), 1U);
            EXPECT_TRUE(r1[0].status.ok());
            EXPECT_FALSE(rt_->HasProposalResultOverflow());

            Shutdown();
        }

        // =================================================================
        //  12. No transport or RPC created
        // =================================================================

        TEST_F(RuntimeTest, NoTransportOrRPCCreated)
        {
            CreateLeaderRuntime();
            ASSERT_TRUE(rt_->EnqueueTick().ok());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            Shutdown();
            SUCCEED();
        }

    } // namespace
} // namespace cpr::raft
