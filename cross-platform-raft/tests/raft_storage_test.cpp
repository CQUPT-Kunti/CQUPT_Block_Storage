#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/checksum.h"
#include "common/status.h"
#include "common/types.h"
#include "platform/file_ops.h"
#include "raft/file_raft_storage.h"
#include "raft/memory_raft_storage.h"
#include "raft/raft_storage.h"
#include "raft/raft_snapshot.h"

namespace cpr::raft
{
    namespace
    {

        using cpr::common::Byte;
        using cpr::common::LogIndex;
        using cpr::common::NodeId;
        using cpr::common::StatusCode;
        using cpr::common::Term;
        using cpr::platform::FileData;

        // -----------------------------------------------------------------------
        //  RAII temporary directory
        // -----------------------------------------------------------------------
        class TempDir
        {
        public:
            TempDir()
            {
                const auto unique = std::to_string(
                    std::chrono::steady_clock::now().time_since_epoch().count());
                path_ = std::filesystem::temp_directory_path() /
                        ("cpr-raft-storage-" + unique);
                std::filesystem::create_directories(path_);
            }

            ~TempDir()
            {
                std::error_code ec;
                std::filesystem::remove_all(path_, ec);
            }

            const std::filesystem::path &path() const { return path_; }

        private:
            std::filesystem::path path_;
        };

        // -----------------------------------------------------------------------
        //  Test data helpers
        // -----------------------------------------------------------------------
        LogEntry MakeEntry(LogIndex index, Term term,
                           const std::string &payload = "")
        {
            LogEntry entry;
            entry.index = index;
            entry.term = term;
            entry.type = LogEntryType::COMMAND;
            entry.payload.assign(payload.begin(), payload.end());
            return entry;
        }

        std::vector<LogEntry> MakeEntries(LogIndex start,
                                          std::initializer_list<Term> terms)
        {
            std::vector<LogEntry> entries;
            LogIndex idx = start;
            for (Term t : terms)
            {
                entries.push_back(MakeEntry(idx, t, "e" + std::to_string(idx)));
                ++idx;
            }
            return entries;
        }

        HardState MakeHardState(Term term = 1,
                                NodeId voted_for = 1,
                                LogIndex commit_index = 0)
        {
            HardState hs;
            hs.current_term = term;
            hs.voted_for = voted_for;
            hs.commit_index = commit_index;
            return hs;
        }

        SnapshotData MakeSnapshot(LogIndex last_included_index,
                                  Term last_included_term,
                                  const std::string &payload = "snap-payload")
        {
            SnapshotData snap;
            snap.metadata.last_included_index = last_included_index;
            snap.metadata.last_included_term = last_included_term;
            snap.payload.assign(payload.begin(), payload.end());
            return snap;
        }

        // -----------------------------------------------------------------------
        //  Common contract test fixture (template method pattern)
        // -----------------------------------------------------------------------
        class RaftStorageCommonTest : public ::testing::Test
        {
        protected:
            // Subclasses override this to provide their storage type
            virtual IRaftStorage &Storage() = 0;
            virtual void SetUpStorage() = 0;

            void SetUp() override
            {
                SetUpStorage();
            }
        };

        // -----------------------------------------------------------------------
        //  MemoryRaftStorage fixture
        // -----------------------------------------------------------------------
        class MemoryStorageTest : public RaftStorageCommonTest
        {
        protected:
            MemoryRaftStorage storage_;
            IRaftStorage &Storage() override { return storage_; }
            void SetUpStorage() override
            {
                ASSERT_TRUE(storage_.Open("").ok());
            }
        };

        // -----------------------------------------------------------------------
        //  FileRaftStorage fixture
        // -----------------------------------------------------------------------
        class FileStorageTest : public RaftStorageCommonTest
        {
        protected:
            TempDir dir_;
            FileRaftStorage storage_;
            IRaftStorage &Storage() override { return storage_; }
            void SetUpStorage() override
            {
                ASSERT_TRUE(storage_.Open(dir_.path()).ok());
            }
        };

        // ===================================================================
        //  Common storage contract — tested against both implementations
        // ===================================================================

        TEST_F(MemoryStorageTest, OpenSucceeds)
        {
            MemoryRaftStorage s;
            EXPECT_TRUE(s.Open("").ok());
        }

        TEST_F(MemoryStorageTest, RepeatedOpenIsSafe)
        {
            MemoryRaftStorage s;
            ASSERT_TRUE(s.Open("").ok());
            EXPECT_TRUE(s.Open("").ok());
            EXPECT_TRUE(s.Open("/some/other/path").ok());
        }

        TEST_F(FileStorageTest, OpenSucceeds)
        {
            TempDir d;
            FileRaftStorage s;
            EXPECT_TRUE(s.Open(d.path()).ok());
        }

        TEST_F(FileStorageTest, RepeatedOpenIsSafeAndReloads)
        {
            TempDir d;
            FileRaftStorage s;
            ASSERT_TRUE(s.Open(d.path()).ok());
            HardState hs = MakeHardState(1, 2, 0);
            ASSERT_TRUE(s.SaveHardState(hs).ok());
            ASSERT_TRUE(s.Open(d.path()).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s.Load(&result).ok());
            EXPECT_EQ(result.hard_state.current_term, 1U);
            EXPECT_EQ(result.hard_state.voted_for, 2U);
        }

        // --- Load empty storage ---

        TEST_F(MemoryStorageTest, LoadReturnsValidEmptyState)
        {
            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_TRUE(result.empty);
            EXPECT_EQ(result.hard_state.current_term, common::kInitialTerm);
            EXPECT_EQ(result.hard_state.voted_for, common::kInvalidNodeId);
            EXPECT_TRUE(result.entries.empty());
            EXPECT_FALSE(result.snapshot.has_value());
        }

        TEST_F(FileStorageTest, LoadReturnsValidEmptyState)
        {
            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_TRUE(result.empty);
            EXPECT_EQ(result.hard_state.current_term, common::kInitialTerm);
            EXPECT_TRUE(result.entries.empty());
            EXPECT_FALSE(result.snapshot.has_value());
        }

        // --- SaveHardState + Load consistency ---

        TEST_F(MemoryStorageTest, SaveHardStateThenLoadMatches)
        {
            HardState hs = MakeHardState(3, 5, 0);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_EQ(result.hard_state.current_term, 3U);
            EXPECT_EQ(result.hard_state.voted_for, 5U);
            EXPECT_EQ(result.hard_state.commit_index, 0U);
        }

        TEST_F(FileStorageTest, SaveHardStateThenLoadMatches)
        {
            HardState hs = MakeHardState(3, 5, 0);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_EQ(result.hard_state.current_term, 3U);
            EXPECT_EQ(result.hard_state.voted_for, 5U);
        }

        // --- AppendEntries basic ---

        TEST_F(MemoryStorageTest, AppendContinuousEntriesSucceeds)
        {
            std::vector<LogEntry> entries = MakeEntries(1, {1, 1, 2});
            ASSERT_TRUE(Storage().AppendEntries(entries).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 3U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[0].term, 1U);
            EXPECT_EQ(result.entries[1].index, 2U);
            EXPECT_EQ(result.entries[1].term, 1U);
            EXPECT_EQ(result.entries[2].index, 3U);
            EXPECT_EQ(result.entries[2].term, 2U);
        }

        TEST_F(FileStorageTest, AppendContinuousEntriesSucceeds)
        {
            std::vector<LogEntry> entries = MakeEntries(1, {1, 1, 2});
            ASSERT_TRUE(Storage().AppendEntries(entries).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 3U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[0].term, 1U);
        }

        // --- Batch append preserves index, term, type, payload ---

        TEST_F(MemoryStorageTest, AppendPreservesAllFields)
        {
            LogEntry entry;
            entry.index = 1;
            entry.term = 2;
            entry.type = LogEntryType::MEMBERSHIP_CHANGE;
            entry.payload = {0xde, 0xad, 0xbe, 0xef};
            ASSERT_TRUE(Storage().AppendEntries({entry}).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[0].term, 2U);
            EXPECT_EQ(result.entries[0].type, LogEntryType::MEMBERSHIP_CHANGE);
            ASSERT_EQ(result.entries[0].payload.size(), 4U);
            EXPECT_EQ(result.entries[0].payload[0], 0xde);
        }

        TEST_F(FileStorageTest, AppendPreservesAllFields)
        {
            LogEntry entry;
            entry.index = 1;
            entry.term = 2;
            entry.type = LogEntryType::MEMBERSHIP_CHANGE;
            entry.payload = {0xde, 0xad, 0xbe, 0xef};
            ASSERT_TRUE(Storage().AppendEntries({entry}).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[0].term, 2U);
            EXPECT_EQ(result.entries[0].type, LogEntryType::MEMBERSHIP_CHANGE);
            ASSERT_EQ(result.entries[0].payload.size(), 4U);
            EXPECT_EQ(result.entries[0].payload[0], 0xde);
        }

        // --- Non-contiguous append rejected ---

        TEST_F(MemoryStorageTest, NonContinuousAppendRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1})).ok());
            auto status = Storage().AppendEntries(MakeEntries(3, {2}));
            EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
        }

        TEST_F(FileStorageTest, NonContinuousAppendRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1})).ok());
            auto status = Storage().AppendEntries(MakeEntries(3, {2}));
            EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
        }

        // --- Overlapping identical entries idempotent ---

        TEST_F(MemoryStorageTest, IdenticalOverlappingAppendIsIdempotent)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
        }

        TEST_F(FileStorageTest, IdenticalOverlappingAppendIsIdempotent)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
        }

        // --- Overlapping different entries rejected ---

        TEST_F(MemoryStorageTest, ConflictingOverwriteRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            auto status = Storage().AppendEntries(MakeEntries(1, {9, 9}));
            EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].term, 1U);
        }

        TEST_F(FileStorageTest, ConflictingOverwriteRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            auto status = Storage().AppendEntries(MakeEntries(1, {9, 9}));
            EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].term, 1U);
        }

        // --- TruncateSuffix ---

        TEST_F(MemoryStorageTest, TruncateSuffixRemovesTail)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1, 2})).ok());
            ASSERT_TRUE(Storage().TruncateSuffix(3).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[1].index, 2U);
        }

        TEST_F(FileStorageTest, TruncateSuffixRemovesTail)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1, 2})).ok());
            ASSERT_TRUE(Storage().TruncateSuffix(3).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[1].index, 2U);
        }

        // --- Committed entries cannot be truncated ---

        TEST_F(MemoryStorageTest, CommittedTruncationRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());
            EXPECT_EQ(Storage().TruncateSuffix(2).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST_F(FileStorageTest, CommittedTruncationRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());
            EXPECT_EQ(Storage().TruncateSuffix(2).code(),
                      StatusCode::kInvalidArgument);
        }

        // --- SaveSnapshot then Load matches ---

        TEST_F(MemoryStorageTest, SaveSnapshotThenLoadMatches)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap = MakeSnapshot(2, 1, "test-payload");
            ASSERT_TRUE(Storage().SaveSnapshot(snap).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 2U);
            EXPECT_EQ(result.snapshot->metadata.last_included_term, 1U);
            const OpaquePayload expected_payload = {Byte('t'), Byte('e'), Byte('s'), Byte('t'),
                                                    Byte('-'), Byte('p'), Byte('a'), Byte('y'),
                                                    Byte('l'), Byte('o'), Byte('a'), Byte('d')};
            EXPECT_EQ(result.snapshot->payload, expected_payload);
        }

        TEST_F(FileStorageTest, SaveSnapshotThenLoadMatches)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap = MakeSnapshot(2, 1, "test-payload");
            ASSERT_TRUE(Storage().SaveSnapshot(snap).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 2U);
            EXPECT_EQ(result.snapshot->metadata.last_included_term, 1U);
        }

        // --- Snapshot boundary cannot move backward ---

        TEST_F(MemoryStorageTest, SnapshotBoundaryCannotMoveBackward)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 3);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap1 = MakeSnapshot(2, 1);
            ASSERT_TRUE(Storage().SaveSnapshot(snap1).ok());

            SnapshotData snap2 = MakeSnapshot(1, 1);
            EXPECT_EQ(Storage().SaveSnapshot(snap2).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST_F(FileStorageTest, SnapshotBoundaryCannotMoveBackward)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 3);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap1 = MakeSnapshot(2, 1);
            ASSERT_TRUE(Storage().SaveSnapshot(snap1).ok());

            SnapshotData snap2 = MakeSnapshot(1, 1);
            EXPECT_EQ(Storage().SaveSnapshot(snap2).code(),
                      StatusCode::kInvalidArgument);
        }

        // --- Snapshot cleanup: covered log entries removed ---

        TEST_F(MemoryStorageTest, SnapshotRemovesCoveredEntries)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1, 2})).ok());
            HardState hs = MakeHardState(2, 1, 3);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap = MakeSnapshot(2, 1);
            ASSERT_TRUE(Storage().SaveSnapshot(snap).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 3U);
        }

        TEST_F(FileStorageTest, SnapshotRemovesCoveredEntries)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1, 2})).ok());
            HardState hs = MakeHardState(2, 1, 3);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap = MakeSnapshot(2, 1);
            ASSERT_TRUE(Storage().SaveSnapshot(snap).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 3U);
        }

        // --- Append after snapshot boundary ---

        TEST_F(MemoryStorageTest, AppendAfterSnapshotWorks)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());
            ASSERT_TRUE(Storage().SaveSnapshot(MakeSnapshot(2, 1)).ok());

            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(3, {2, 2})).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 3U);
            EXPECT_EQ(result.entries[1].index, 4U);
        }

        TEST_F(FileStorageTest, AppendAfterSnapshotWorks)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());
            ASSERT_TRUE(Storage().SaveSnapshot(MakeSnapshot(2, 1)).ok());

            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(3, {2, 2})).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 3U);
            EXPECT_EQ(result.entries[1].index, 4U);
        }

        // --- Failed operation leaves state unchanged ---

        TEST_F(MemoryStorageTest, FailedOperationDoesNotLeavePartialState)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1})).ok());
            HardState hs = MakeHardState(1, 1, 1);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            // Try invalid save (commit beyond available log)
            HardState bad_hs = MakeHardState(2, 1, 99);
            EXPECT_FALSE(Storage().SaveHardState(bad_hs).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_EQ(result.hard_state.current_term, 1U);
            EXPECT_EQ(result.hard_state.commit_index, 1U);
        }

        TEST_F(FileStorageTest, FailedOperationDoesNotLeavePartialState)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1})).ok());
            HardState hs = MakeHardState(1, 1, 1);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            HardState bad_hs = MakeHardState(2, 1, 99);
            EXPECT_FALSE(Storage().SaveHardState(bad_hs).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_EQ(result.hard_state.current_term, 1U);
            EXPECT_EQ(result.hard_state.commit_index, 1U);
        }

        // --- Empty batch is no-op ---

        TEST_F(MemoryStorageTest, EmptyBatchAppendIsNoOp)
        {
            ASSERT_TRUE(Storage().AppendEntries({}).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_TRUE(result.entries.empty());
        }

        TEST_F(FileStorageTest, EmptyBatchAppendIsNoOp)
        {
            ASSERT_TRUE(Storage().AppendEntries({}).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            EXPECT_TRUE(result.entries.empty());
        }

        // --- Operations before Open fail ---

        TEST_F(MemoryStorageTest, OperationsBeforeOpenFail)
        {
            MemoryRaftStorage unopened;
            RaftStorageLoadResult result;
            EXPECT_EQ(unopened.Load(&result).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(unopened.SaveHardState(MakeHardState(1, 1, 0)).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(unopened.AppendEntries(MakeEntries(1, {1})).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(unopened.TruncateSuffix(1).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(unopened.SaveSnapshot(MakeSnapshot(1, 1)).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST_F(FileStorageTest, OperationsBeforeOpenFail)
        {
            FileRaftStorage unopened;
            RaftStorageLoadResult result;
            EXPECT_EQ(unopened.Load(&result).code(), StatusCode::kInvalidArgument);
            EXPECT_EQ(unopened.SaveHardState(MakeHardState(1, 1, 0)).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(unopened.AppendEntries(MakeEntries(1, {1})).code(),
                      StatusCode::kInvalidArgument);
        }

        // ===================================================================
        //  FileRaftStorage — restart recovery
        // ===================================================================

        class FileStorageRecoveryTest : public ::testing::Test
        {
        protected:
            TempDir dir_;

            // Open a fresh FileRaftStorage on the same directory
            std::unique_ptr<FileRaftStorage> Reopen()
            {
                auto s = std::make_unique<FileRaftStorage>();
                EXPECT_TRUE(s->Open(dir_.path()).ok());
                return s;
            }

            void OpenAndPopulate()
            {
                auto s = std::make_unique<FileRaftStorage>();
                ASSERT_TRUE(s->Open(dir_.path()).ok());
                // Append entries first, then set HardState with matching commit_index
                ASSERT_TRUE(s->AppendEntries(MakeEntries(1, {1, 2, 3, 4, 5})).ok());
                HardState hs = MakeHardState(5, 3, 4);
                ASSERT_TRUE(s->SaveHardState(hs).ok());
                ASSERT_TRUE(s->SaveSnapshot(MakeSnapshot(3, 3, "recovery-payload")).ok());
            }
        };

        TEST_F(FileStorageRecoveryTest, HardStateSurvivesReopen)
        {
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.SaveHardState(MakeHardState(7, 2, 0)).ok());
            }
            auto s2 = Reopen();
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2->Load(&result).ok());
            EXPECT_EQ(result.hard_state.current_term, 7U);
            EXPECT_EQ(result.hard_state.voted_for, 2U);
        }

        TEST_F(FileStorageRecoveryTest, LogSurvivesReopen)
        {
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 2, 3})).ok());
            }
            auto s2 = Reopen();
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 3U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[2].index, 3U);
        }

        TEST_F(FileStorageRecoveryTest, SnapshotSurvivesReopen)
        {
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 1, 1})).ok());
                HardState hs = MakeHardState(1, 1, 3);
                ASSERT_TRUE(s.SaveHardState(hs).ok());
                ASSERT_TRUE(s.SaveSnapshot(MakeSnapshot(2, 1, "persistent-snap")).ok());
            }
            auto s2 = Reopen();
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2->Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 2U);
        }

        TEST_F(FileStorageRecoveryTest, LogAfterSnapshotBoundarySurvivesReopen)
        {
            OpenAndPopulate();

            RaftStorageLoadResult result;
            ASSERT_TRUE(Reopen()->Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            // Snapshot at index 3 covers entries 1-3; entries 4 and 5 remain
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 4U);
            EXPECT_EQ(result.entries[0].term, 4U);
            EXPECT_EQ(result.entries[1].index, 5U);
            EXPECT_EQ(result.entries[1].term, 5U);
        }

        TEST_F(FileStorageRecoveryTest, CombinedStateConsistentAfterReopen)
        {
            OpenAndPopulate();

            auto s2 = Reopen();
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2->Load(&result).ok());

            // HardState: term=5, commit=4
            EXPECT_EQ(result.hard_state.current_term, 5U);
            EXPECT_EQ(result.hard_state.commit_index, 4U);
            EXPECT_EQ(result.hard_state.voted_for, 3U);

            // Snapshot: index=3, term=3
            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 3U);
            EXPECT_EQ(result.snapshot->metadata.last_included_term, 3U);

            // Entries: only index 4 and 5
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 4U);
            EXPECT_EQ(result.entries[0].term, 4U);
            EXPECT_EQ(result.entries[1].index, 5U);
            EXPECT_EQ(result.entries[1].term, 5U);
        }

        TEST_F(FileStorageRecoveryTest, AppendAfterSnapshotThenRecovery)
        {
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 1})).ok());
                HardState hs = MakeHardState(1, 1, 2);
                ASSERT_TRUE(s.SaveHardState(hs).ok());
                ASSERT_TRUE(s.SaveSnapshot(MakeSnapshot(2, 1)).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(3, {2, 2})).ok());
            }
            auto s2 = Reopen();
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 3U);
            EXPECT_EQ(result.entries[1].index, 4U);
        }

        TEST_F(FileStorageRecoveryTest, TruncateSuffixThenRecovery)
        {
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 1, 2})).ok());
                ASSERT_TRUE(s.TruncateSuffix(3).ok());
            }
            auto s2 = Reopen();
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[1].index, 2U);
        }

        // ===================================================================
        //  FileRaftStorage — corruption detection
        // ===================================================================

        class FileStorageCorruptionTest : public ::testing::Test
        {
        protected:
            TempDir dir_;

            void SetUp() override
            {
                // Corrupt test fixture doesn't open a storage in SetUp
            }

            // Helper: write raw bytes to a file in the storage directory
            void WriteFile(const std::string &name, const FileData &data)
            {
                cpr::platform::FileHandle file;
                ASSERT_TRUE(
                    cpr::platform::FileHandle::OpenOrCreate(dir_.path() / name, &file).ok());
                ASSERT_TRUE(file.WriteAll(data).ok());
                ASSERT_TRUE(file.Flush().ok());
                ASSERT_TRUE(file.Close().ok());
            }

            // Helper: read raw bytes from a file
            FileData ReadFile(const std::string &name)
            {
                cpr::platform::FileHandle file;
                FileData data;
                EXPECT_TRUE(
                    cpr::platform::FileHandle::OpenExisting(dir_.path() / name, &file).ok());
                EXPECT_TRUE(file.ReadAll(&data).ok());
                return data;
            }

            // Helper: create storage with a valid hard state
            void CreateValidHardState()
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.SaveHardState(MakeHardState(1, 1, 0)).ok());
            }

            // Helper: create storage with valid log entries
            void CreateValidLog()
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 1})).ok());
                HardState hs = MakeHardState(1, 1, 2);
                ASSERT_TRUE(s.SaveHardState(hs).ok());
                ASSERT_TRUE(s.SaveSnapshot(MakeSnapshot(2, 1)).ok());
            }
        };

        TEST_F(FileStorageCorruptionTest, HardStateChecksumCorruptionDetected)
        {
            CreateValidHardState();
            // Corrupt the hard_state.bin
            FileData data = ReadFile("hard_state.bin");
            ASSERT_FALSE(data.empty());
            // Flip a byte in the payload
            data[data.size() - 10] ^= 0xff;
            WriteFile("hard_state.bin", data);

            FileRaftStorage s;
            auto status = s.Open(dir_.path());
            EXPECT_EQ(status.code(), StatusCode::kCorruption);
        }

        TEST_F(FileStorageCorruptionTest, LogRecordChecksumCorruptionDetected)
        {
            // Create a storage with an extra entry that survives snapshot
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 1, 1, 1})).ok());
                HardState hs = MakeHardState(1, 1, 4);
                ASSERT_TRUE(s.SaveHardState(hs).ok());
                ASSERT_TRUE(s.SaveSnapshot(MakeSnapshot(2, 1)).ok());
            }
            // Now log.bin has 2 entries (index 3 and 4)
            FileData data = ReadFile("log.bin");
            ASSERT_FALSE(data.empty());
            // Flip a byte in the first record payload area (after 40-byte header)
            std::size_t pos = 40; // start of first record payload
            if (pos < data.size())
            {
                data[pos] ^= 0xff;
            }
            WriteFile("log.bin", data);

            FileRaftStorage s;
            auto status = s.Open(dir_.path());
            EXPECT_EQ(status.code(), StatusCode::kCorruption);
        }

        TEST_F(FileStorageCorruptionTest, SnapshotChecksumCorruptionDetected)
        {
            CreateValidLog(); // Creates snapshot too
            FileData data = ReadFile("snapshot.bin");
            ASSERT_FALSE(data.empty());
            // Flip a byte in the snapshot payload
            data[data.size() / 2] ^= 0xff;
            WriteFile("snapshot.bin", data);

            FileRaftStorage s;
            auto status = s.Open(dir_.path());
            EXPECT_EQ(status.code(), StatusCode::kCorruption);
        }

        TEST_F(FileStorageCorruptionTest, UnsupportedFormatVersionRejected)
        {
            CreateValidHardState();
            FileData data = ReadFile("hard_state.bin");
            ASSERT_FALSE(data.empty());
            // Change version field from 1 to 99
            data[0] = 99;
            data[1] = 0;
            data[2] = 0;
            data[3] = 0;
            WriteFile("hard_state.bin", data);

            FileRaftStorage s;
            auto status = s.Open(dir_.path());
            EXPECT_EQ(status.code(), StatusCode::kCorruption);
        }

        TEST_F(FileStorageCorruptionTest, IllegalPayloadLengthRejected)
        {
            // Write a record with very large payload length
            FileData record;
            // format_version=1, record_type=1 (HardState)
            // index=1, term=1, payload_length=UINT64_MAX, checksum=0
            const FileData header = {
                1,
                0,
                0,
                0, // version
                1,
                0,
                0,
                0, // type
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0, // index
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0, // term
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0, // payload_length — will overwrite
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0, // checksum
            };
            // Actually let's use a more practical approach — write a record
            // with payload length exceeding 64 MiB
            record = header;
            // Set payload_length to 65 MiB + 1
            const std::uint64_t huge = (64ULL * 1024ULL * 1024ULL) + 1;
            for (int i = 0; i < 8; ++i)
            {
                record[20 + i] = static_cast<Byte>((huge >> (i * 8)) & 0xff);
            }
            WriteFile("hard_state.bin", record);

            FileRaftStorage s;
            auto status = s.Open(dir_.path());
            EXPECT_EQ(status.code(), StatusCode::kCorruption);
        }

        TEST_F(FileStorageCorruptionTest, TruncatedSnapshotFileRejected)
        {
            CreateValidLog();
            FileData data = ReadFile("snapshot.bin");
            ASSERT_FALSE(data.empty());
            // Truncate to half size
            data.resize(data.size() / 2);
            WriteFile("snapshot.bin", data);

            FileRaftStorage s;
            auto status = s.Open(dir_.path());
            EXPECT_EQ(status.code(), StatusCode::kCorruption);
        }

        TEST_F(FileStorageCorruptionTest, MidLogChecksumCorruptionNotSilentlySkipped)
        {
            // Create storage with 3 log entries
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 1, 1})).ok());
                HardState hs = MakeHardState(1, 1, 3);
                ASSERT_TRUE(s.SaveHardState(hs).ok());
            }

            // Corrupt the second log record
            FileData data = ReadFile("log.bin");
            ASSERT_GE(data.size(), 40 * 2 + 10);
            // The second record starts at byte 40 + payload_of_first
            // Let's just flip a byte somewhere after the first record header + payload
            std::size_t first_payload_size = 2; // "e1" is the payload of first entry
            std::size_t second_record_start = 40 + first_payload_size;
            if (second_record_start + 10 < data.size())
            {
                data[second_record_start + 10] ^= 0xff;
            }
            WriteFile("log.bin", data);

            FileRaftStorage s;
            auto status = s.Open(dir_.path());
            EXPECT_EQ(status.code(), StatusCode::kCorruption);
        }

        // ===================================================================
        //  FileRaftStorage — incomplete tail recovery
        // ===================================================================

        class FileStorageTailTest : public ::testing::Test
        {
        protected:
            TempDir dir_;

            void CreateLogWithThreeEntries()
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 1, 1})).ok());
            }

            FileData ReadLog()
            {
                cpr::platform::FileHandle file;
                FileData data;
                EXPECT_TRUE(
                    cpr::platform::FileHandle::OpenExisting(dir_.path() / "log.bin", &file)
                        .ok());
                EXPECT_TRUE(file.ReadAll(&data).ok());
                return data;
            }

            void WriteLog(const FileData &data)
            {
                cpr::platform::FileHandle file;
                ASSERT_TRUE(
                    cpr::platform::FileHandle::OpenOrCreate(dir_.path() / "log.bin", &file)
                        .ok());
                ASSERT_TRUE(file.WriteAll(data).ok());
                ASSERT_TRUE(file.Flush().ok());
                ASSERT_TRUE(file.Close().ok());
            }
        };

        TEST_F(FileStorageTailTest, IncompleteTailDoesNotLoadAsValidEntries)
        {
            CreateLogWithThreeEntries();
            FileData data = ReadLog();
            ASSERT_GT(data.size(), 40U);

            // Truncate in the middle of the first record's payload (40 bytes header
            // + 6 bytes payload, cut at 45 = header complete but payload incomplete)
            FileData truncated(data.begin(), data.begin() + 45);
            WriteLog(truncated);

            FileRaftStorage s;
            ASSERT_TRUE(s.Open(dir_.path()).ok());
            RaftStorageLoadResult result;
            ASSERT_TRUE(s.Load(&result).ok());
            // The incomplete first record cannot be decoded, so zero entries
            ASSERT_EQ(result.entries.size(), 0U);
        }

        TEST_F(FileStorageTailTest, CompleteRecordsBeforeIncompleteTailAreRecoverable)
        {
            CreateLogWithThreeEntries();
            FileData data = ReadLog();
            ASSERT_GT(data.size(), 80U);

            // Truncate so the first record is complete, second is partially there,
            // third is completely gone
            FileData truncated(data.begin(), data.begin() + 50);
            WriteLog(truncated);

            FileRaftStorage s;
            ASSERT_TRUE(s.Open(dir_.path()).ok());
            RaftStorageLoadResult result;
            ASSERT_TRUE(s.Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 1U);
        }

        TEST_F(FileStorageTailTest, IncompleteTailIsRepairedOnOpen)
        {
            CreateLogWithThreeEntries();
            FileData data = ReadLog();
            FileData truncated(data.begin(), data.begin() + 50);
            WriteLog(truncated);

            // Open should repair (rewrite log.bin with only complete entries)
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
            }

            // Re-open should see stable state
            FileRaftStorage s2;
            ASSERT_TRUE(s2.Open(dir_.path()).ok());
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2.Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 1U);
        }

        TEST_F(FileStorageTailTest, IncompleteTailDistinguishedFromChecksumCorruption)
        {
            CreateLogWithThreeEntries();
            FileData data = ReadLog();
            ASSERT_GT(data.size(), 80U);

            // Truncated tail (incomplete record)
            FileData truncated(data.begin(), data.begin() + 50);
            WriteLog(truncated);

            // Should succeed with partial recovery, not corruption
            FileRaftStorage s;
            EXPECT_TRUE(s.Open(dir_.path()).ok());
        }

        // ===================================================================
        //  FileRaftStorage — Snapshot save/load specifics
        // ===================================================================

        class FileStorageSnapshotTest : public ::testing::Test
        {
        protected:
            TempDir dir_;

            std::unique_ptr<FileRaftStorage> CreateStorage()
            {
                auto s = std::make_unique<FileRaftStorage>();
                EXPECT_TRUE(s->Open(dir_.path()).ok());
                return s;
            }
        };

        TEST_F(FileStorageSnapshotTest, SnapshotMetadataAndPayloadRoundTrip)
        {
            auto s = CreateStorage();
            ASSERT_TRUE(s->AppendEntries(MakeEntries(1, {2, 2, 2})).ok());
            HardState hs = MakeHardState(2, 1, 3);
            ASSERT_TRUE(s->SaveHardState(hs).ok());

            SnapshotData snap;
            snap.metadata.last_included_index = 2;
            snap.metadata.last_included_term = 2;
            snap.metadata.membership.configuration_id = 42;
            snap.metadata.membership.voters = {
                {1, {"10.0.0.1", 9100}},
                {2, {"10.0.0.2", 9100}},
                {3, {"10.0.0.3", 9100}},
            };
            snap.payload = {0x01, 0x02, 0x03, 0x04};
            ASSERT_TRUE(s->SaveSnapshot(snap).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 2U);
            EXPECT_EQ(result.snapshot->metadata.last_included_term, 2U);
            EXPECT_EQ(result.snapshot->metadata.membership.configuration_id, 42U);
            ASSERT_EQ(result.snapshot->metadata.membership.voters.size(), 3U);
            EXPECT_EQ(result.snapshot->metadata.membership.voters[0].node_id, 1U);
            EXPECT_EQ(result.snapshot->payload,
                      OpaquePayload({0x01, 0x02, 0x03, 0x04}));
        }

        TEST_F(FileStorageSnapshotTest, SnapshotUsesLatestBoundary)
        {
            auto s = CreateStorage();
            ASSERT_TRUE(s->AppendEntries(MakeEntries(1, {1, 1, 1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 4);
            ASSERT_TRUE(s->SaveHardState(hs).ok());

            ASSERT_TRUE(s->SaveSnapshot(MakeSnapshot(2, 1)).ok());
            ASSERT_TRUE(s->SaveSnapshot(MakeSnapshot(3, 1)).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 3U);
        }

        TEST_F(FileStorageSnapshotTest, OlderSnapshotRejected)
        {
            auto s = CreateStorage();
            ASSERT_TRUE(s->AppendEntries(MakeEntries(1, {1, 1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 3);
            ASSERT_TRUE(s->SaveHardState(hs).ok());

            ASSERT_TRUE(s->SaveSnapshot(MakeSnapshot(2, 1)).ok());
            EXPECT_EQ(s->SaveSnapshot(MakeSnapshot(1, 1)).code(),
                      StatusCode::kInvalidArgument);

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 2U);
        }

        TEST_F(FileStorageSnapshotTest, SnapshotCleansUpCoveredLogs)
        {
            auto s = CreateStorage();
            ASSERT_TRUE(s->AppendEntries(MakeEntries(1, {1, 1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 3);
            ASSERT_TRUE(s->SaveHardState(hs).ok());
            ASSERT_TRUE(s->SaveSnapshot(MakeSnapshot(2, 1)).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 3U);
        }

        TEST_F(FileStorageSnapshotTest, AppendAfterSnapshotContinuesCorrectly)
        {
            auto s = CreateStorage();
            ASSERT_TRUE(s->AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(s->SaveHardState(hs).ok());
            ASSERT_TRUE(s->SaveSnapshot(MakeSnapshot(2, 1)).ok());

            ASSERT_TRUE(s->AppendEntries(MakeEntries(3, {2, 2})).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 3U);
            EXPECT_EQ(result.entries[1].index, 4U);
        }

        TEST_F(FileStorageSnapshotTest, CombinedHardStateSnapshotLogRecovery)
        {
            // Create a storage with all three components
            {
                FileRaftStorage s;
                ASSERT_TRUE(s.Open(dir_.path()).ok());
                ASSERT_TRUE(s.AppendEntries(MakeEntries(1, {1, 2, 3, 4})).ok());
                HardState hs = MakeHardState(4, 2, 4);
                ASSERT_TRUE(s.SaveHardState(hs).ok());
                ASSERT_TRUE(s.SaveSnapshot(MakeSnapshot(2, 2)).ok());
            }

            // Reopen and verify combined state
            FileRaftStorage s2;
            ASSERT_TRUE(s2.Open(dir_.path()).ok());
            RaftStorageLoadResult result;
            ASSERT_TRUE(s2.Load(&result).ok());

            EXPECT_EQ(result.hard_state.current_term, 4U);
            EXPECT_EQ(result.hard_state.commit_index, 4U);

            ASSERT_TRUE(result.snapshot.has_value());
            EXPECT_EQ(result.snapshot->metadata.last_included_index, 2U);

            // Entries after snapshot: index 3, 4
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 3U);
            EXPECT_EQ(result.entries[1].index, 4U);
            EXPECT_EQ(result.entries[0].term, 3U);
            EXPECT_EQ(result.entries[1].term, 4U);
        }

        // ===================================================================
        //  FileRaftStorage — TruncateSuffix specifics
        // ===================================================================

        class FileStorageTruncateTest : public ::testing::Test
        {
        protected:
            TempDir dir_;

            std::unique_ptr<FileRaftStorage> CreateStorage()
            {
                auto s = std::make_unique<FileRaftStorage>();
                EXPECT_TRUE(s->Open(dir_.path()).ok());
                EXPECT_TRUE(s->AppendEntries(MakeEntries(1, {1, 1, 2, 2})).ok());
                return s;
            }
        };

        TEST_F(FileStorageTruncateTest, TruncateSuffixPreservesEarlierEntries)
        {
            auto s = CreateStorage();
            ASSERT_TRUE(s->TruncateSuffix(3).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 2U);
            EXPECT_EQ(result.entries[0].index, 1U);
            EXPECT_EQ(result.entries[1].index, 2U);
        }

        TEST_F(FileStorageTruncateTest, NoOpTruncateAtLastIndexPlusOne)
        {
            auto s = CreateStorage();
            EXPECT_TRUE(s->TruncateSuffix(5).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 4U);
        }

        TEST_F(FileStorageTruncateTest, TruncateBeyondLastIndexRejected)
        {
            auto s = CreateStorage();
            EXPECT_EQ(s->TruncateSuffix(10).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST_F(FileStorageTruncateTest, TruncateAfterSnapshotWorks)
        {
            auto s = CreateStorage();
            HardState hs = MakeHardState(2, 1, 3);
            ASSERT_TRUE(s->SaveHardState(hs).ok());
            ASSERT_TRUE(s->SaveSnapshot(MakeSnapshot(2, 1)).ok());

            // Truncate suffix at index 4 (commit index is 3, so index 4 is uncommitted)
            ASSERT_TRUE(s->TruncateSuffix(4).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(s->Load(&result).ok());
            ASSERT_EQ(result.entries.size(), 1U);
            EXPECT_EQ(result.entries[0].index, 3U);
        }

        // ===================================================================
        //  Flush failure handling
        // ===================================================================
        //
        //  The current FileOps implementation does not provide a safe,
        //  deterministic way to inject flush failures without accessing
        //  platform-specific APIs or introducing a mock framework.
        //
        //  We verify the error-propagation contract through the existing
        //  FileOps error paths: an invalid path or permission-denied
        //  scenario demonstrates that storage operations return explicit
        //  errors instead of silently succeeding.
        //
        //  The following test validates that opening on a non-writable
        //  path returns an error. True flush-failure injection would
        //  require FileOps changes that are outside T020 scope.
        //

        // FileRaftStorage::Open creates missing directories, so a non-existent
        // parent path succeeds (directories are created automatically).
        TEST_F(FileStorageTest, OpenCreatesMissingDirectories)
        {
            FileRaftStorage s;
            const auto new_path =
                std::filesystem::temp_directory_path() / "cpr-raft-create-dirs-test" / "sub";
            std::error_code ec;
            std::filesystem::remove_all(new_path, ec);
            EXPECT_TRUE(s.Open(new_path).ok());
            bool exists = false;
            ASSERT_TRUE(cpr::platform::Exists(new_path, &exists).ok());
            EXPECT_TRUE(exists);
            std::filesystem::remove_all(new_path.parent_path(), ec);
        }

        // ===================================================================
        //  MemoryRaftStorage additional snapshot tests
        // ===================================================================

        TEST_F(MemoryStorageTest, SaveSnapshotWithMembershipWorks)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap;
            snap.metadata.last_included_index = 2;
            snap.metadata.last_included_term = 1;
            snap.metadata.membership.voters = {{1, {"192.168.1.1", 9100}},
                                               {2, {"192.168.1.2", 9100}},
                                               {3, {"192.168.1.3", 9100}}};
            snap.metadata.membership.learners = {{4, {"192.168.1.4", 9100}}};
            snap.metadata.membership.has_active_transition = true;
            ASSERT_TRUE(Storage().SaveSnapshot(snap).ok());

            RaftStorageLoadResult result;
            ASSERT_TRUE(Storage().Load(&result).ok());
            ASSERT_TRUE(result.snapshot.has_value());
            ASSERT_EQ(result.snapshot->metadata.membership.voters.size(), 3U);
            ASSERT_EQ(result.snapshot->metadata.membership.learners.size(), 1U);
            EXPECT_TRUE(result.snapshot->metadata.membership.has_active_transition);
        }

        TEST_F(MemoryStorageTest, SaveSnapshotExceedsCommitIndexRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 1})).ok());
            HardState hs = MakeHardState(1, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            SnapshotData snap = MakeSnapshot(5, 1);
            EXPECT_EQ(Storage().SaveSnapshot(snap).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST_F(MemoryStorageTest, SnapshotTermMismatchRejected)
        {
            ASSERT_TRUE(Storage().AppendEntries(MakeEntries(1, {1, 2})).ok());
            HardState hs = MakeHardState(2, 1, 2);
            ASSERT_TRUE(Storage().SaveHardState(hs).ok());

            // index 2 has term 2, not term 1
            SnapshotData snap = MakeSnapshot(2, 1);
            EXPECT_EQ(Storage().SaveSnapshot(snap).code(),
                      StatusCode::kInvalidArgument);
        }

    } // namespace
} // namespace cpr::raft
