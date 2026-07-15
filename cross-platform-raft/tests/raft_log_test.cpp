#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "raft/raft_log.h"

namespace cpr::raft
{
    namespace
    {

        LogEntry MakeEntry(common::LogIndex index,
                           common::Term term,
                           std::string payload = "")
        {
            LogEntry entry;
            entry.index = index;
            entry.term = term;
            entry.type = LogEntryType::COMMAND;
            entry.payload.assign(payload.begin(), payload.end());
            return entry;
        }

        std::vector<LogEntry> MakeEntries(common::LogIndex start,
                                          std::initializer_list<common::Term> terms)
        {
            std::vector<LogEntry> entries;
            common::LogIndex index = start;
            for (common::Term term : terms)
            {
                entries.push_back(MakeEntry(index, term, "e" + std::to_string(index)));
                ++index;
            }
            return entries;
        }

        RaftLog MakeLog(std::initializer_list<common::Term> terms)
        {
            RaftLog log;
            EXPECT_TRUE(log.Append(MakeEntries(1, terms)).ok());
            return log;
        }

        void ExpectIndexInvariant(const RaftLog &log)
        {
            EXPECT_LE(log.applied_index(), log.commit_index());
            EXPECT_LE(log.commit_index(), log.last_index());
        }

        TEST(RaftLogTest, InitialStateHasExpectedIndexesAndRejectsMissingQueries)
        {
            RaftLog log;
            LogEntry entry;
            common::Term term = 99;
            std::vector<LogEntry> entries;

            EXPECT_TRUE(log.empty());
            EXPECT_EQ(log.size(), 0U);
            EXPECT_EQ(log.first_index(), 1U);
            EXPECT_EQ(log.last_index(), 0U);
            EXPECT_EQ(log.stable_index(), 0U);
            EXPECT_EQ(log.commit_index(), 0U);
            EXPECT_EQ(log.applied_index(), 0U);
            EXPECT_EQ(log.GetEntry(1, &entry).code(), common::StatusCode::kNotFound);
            EXPECT_EQ(log.GetTerm(1, &term).code(), common::StatusCode::kNotFound);
            EXPECT_TRUE(log.GetEntries(1, 1, &entries).ok());
            EXPECT_TRUE(entries.empty());
            ExpectIndexInvariant(log);
        }

        TEST(RaftLogTest, SingleAppendAndSequentialAppendPreserveContentAndIndexes)
        {
            RaftLog log;
            LogEntry entry;

            ASSERT_TRUE(log.Append(MakeEntry(1, 1, "a")).ok());
            ASSERT_TRUE(log.Append(MakeEntry(2, 2, "b")).ok());
            ASSERT_TRUE(log.Append(MakeEntry(3, 2, "c")).ok());

            EXPECT_EQ(log.last_index(), 3U);
            ASSERT_TRUE(log.GetEntry(2, &entry).ok());
            EXPECT_EQ(entry.index, 2U);
            EXPECT_EQ(entry.term, 2U);
            EXPECT_EQ(std::string(entry.payload.begin(), entry.payload.end()), "b");
            ExpectIndexInvariant(log);
        }

        TEST(RaftLogTest, RejectsDuplicateAndGapAppendWithoutMutatingExistingLog)
        {
            RaftLog log = MakeLog({1, 1});
            LogEntry entry;

            const auto last_index = log.last_index();
            const auto size = log.size();

            EXPECT_EQ(log.Append(MakeEntry(2, 1)).code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.Append(MakeEntry(4, 2)).code(),
                      common::StatusCode::kInvalidArgument);

            EXPECT_EQ(log.last_index(), last_index);
            EXPECT_EQ(log.size(), size);
            ASSERT_TRUE(log.GetEntry(2, &entry).ok());
            EXPECT_EQ(entry.term, 1U);
        }

        TEST(RaftLogTest, BatchAppendPreservesOrderAndRejectsInvalidBatchesAtomically)
        {
            RaftLog log = MakeLog({1});
            std::vector<LogEntry> out;

            ASSERT_TRUE(log.Append(MakeEntries(2, {1, 2, 2})).ok());
            ASSERT_TRUE(log.GetEntries(1, 5, &out).ok());
            ASSERT_EQ(out.size(), 4U);
            EXPECT_EQ(out[0].index, 1U);
            EXPECT_EQ(out[1].index, 2U);
            EXPECT_EQ(out[2].index, 3U);
            EXPECT_EQ(out[3].index, 4U);

            const auto size = log.size();
            const auto last_index = log.last_index();
            EXPECT_EQ(log.Append({MakeEntry(5, 3), MakeEntry(7, 3)}).code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.Append(MakeEntries(6, {3, 3})).code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_TRUE(log.Append(std::vector<LogEntry>{}).ok());
            EXPECT_EQ(log.size(), size);
            EXPECT_EQ(log.last_index(), last_index);
        }

        TEST(RaftLogTest, QueryInterfacesRespectBoundsAndReturnCopies)
        {
            RaftLog log = MakeLog({1, 2, 3});
            std::vector<LogEntry> entries;
            LogEntry entry;
            common::Term term = 0;

            ASSERT_TRUE(log.GetEntry(1, &entry).ok());
            entry.term = 99;
            ASSERT_TRUE(log.GetEntry(1, &entry).ok());
            EXPECT_EQ(entry.term, 1U);

            ASSERT_TRUE(log.GetTerm(2, &term).ok());
            EXPECT_EQ(term, 2U);
            ASSERT_TRUE(log.GetEntries(2, 4, &entries).ok());
            ASSERT_EQ(entries.size(), 2U);
            EXPECT_EQ(entries[0].index, 2U);
            EXPECT_EQ(entries[1].index, 3U);
            EXPECT_TRUE(log.GetEntries(2, 2, &entries).ok());
            EXPECT_TRUE(entries.empty());
            EXPECT_EQ(log.GetEntries(4, 2, &entries).code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.GetEntries(2, 5, &entries).code(),
                      common::StatusCode::kNotFound);
        }

        TEST(RaftLogTest, MatchHandlesExistingMissingAndSnapshotBoundaryIndexes)
        {
            RaftLog log = MakeLog({1, 2, 2});

            ASSERT_TRUE(log.AdvanceCommitTo(3).ok());
            ASSERT_TRUE(log.AdvanceAppliedTo(3).ok());
            ASSERT_TRUE(log.UpdateSnapshotBoundary(2, 2).ok());

            EXPECT_TRUE(log.Matches(0, 0));
            EXPECT_TRUE(log.Matches(3, 2));
            EXPECT_TRUE(log.Matches(2, 2));
            EXPECT_FALSE(log.Matches(3, 1));
            EXPECT_FALSE(log.Matches(1, 1));
            EXPECT_FALSE(log.Matches(4, 2));
        }

        TEST(RaftLogTest, FindConflictReportsNoneConflictAndAppendWithoutMutation)
        {
            RaftLog log = MakeLog({1, 1, 2, 3});
            RaftLog::ConflictResult result;

            ASSERT_TRUE(log.FindConflict(MakeEntries(1, {1, 1, 2, 3}), &result).ok());
            EXPECT_EQ(result.type, RaftLog::ConflictType::NONE);

            ASSERT_TRUE(log.FindConflict(MakeEntries(3, {9, 3}), &result).ok());
            EXPECT_EQ(result.type, RaftLog::ConflictType::CONFLICT);
            EXPECT_EQ(result.index, 3U);

            ASSERT_TRUE(log.FindConflict(MakeEntries(5, {4, 4}), &result).ok());
            EXPECT_EQ(result.type, RaftLog::ConflictType::APPEND);
            EXPECT_EQ(result.index, 5U);

            ASSERT_TRUE(log.FindConflict(MakeEntries(2, {1, 5, 5}), &result).ok());
            EXPECT_EQ(result.type, RaftLog::ConflictType::CONFLICT);
            EXPECT_EQ(result.index, 3U);
            EXPECT_EQ(log.last_index(), 4U);
        }

        TEST(RaftLogTest, TruncateSuffixRemovesOnlyTailAndAllowsReappend)
        {
            RaftLog log = MakeLog({1, 1, 2, 2});
            LogEntry entry;

            ASSERT_TRUE(log.TruncateSuffix(3).ok());
            EXPECT_EQ(log.last_index(), 2U);
            ASSERT_TRUE(log.GetEntry(2, &entry).ok());
            EXPECT_EQ(entry.term, 1U);
            EXPECT_EQ(log.GetEntry(3, &entry).code(), common::StatusCode::kNotFound);
            ASSERT_TRUE(log.Append(MakeEntries(3, {3, 3})).ok());
            EXPECT_EQ(log.last_index(), 4U);
        }

        TEST(RaftLogTest, TruncateRejectsCommittedOrSnapshotCoveredIndexesWithoutMutation)
        {
            RaftLog log = MakeLog({1, 1, 2, 2});

            ASSERT_TRUE(log.AdvanceCommitTo(2).ok());
            ASSERT_TRUE(log.AdvanceAppliedTo(2).ok());
            ASSERT_TRUE(log.UpdateSnapshotBoundary(1, 1).ok());

            const auto last_index = log.last_index();
            const auto stable_index = log.stable_index();
            const auto commit_index = log.commit_index();
            const auto applied_index = log.applied_index();

            EXPECT_EQ(log.TruncateSuffix(2).code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.TruncateSuffix(1).code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.last_index(), last_index);
            EXPECT_EQ(log.stable_index(), stable_index);
            EXPECT_EQ(log.commit_index(), commit_index);
            EXPECT_EQ(log.applied_index(), applied_index);
        }

        TEST(RaftLogTest, ProgressIndexesAdvanceMonotonicallyAndRejectInvalidMoves)
        {
            RaftLog log = MakeLog({1, 1, 2});

            ASSERT_TRUE(log.AdvanceStableTo(2).ok());
            ASSERT_TRUE(log.AdvanceCommitTo(2).ok());
            ASSERT_TRUE(log.AdvanceAppliedTo(1).ok());
            ExpectIndexInvariant(log);

            const auto stable_index = log.stable_index();
            const auto commit_index = log.commit_index();
            const auto applied_index = log.applied_index();

            EXPECT_EQ(log.AdvanceStableTo(1).code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.AdvanceCommitTo(4).code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.AdvanceAppliedTo(3).code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.stable_index(), stable_index);
            EXPECT_EQ(log.commit_index(), commit_index);
            EXPECT_EQ(log.applied_index(), applied_index);
            ExpectIndexInvariant(log);
        }

        TEST(RaftLogTest, IndexMarkersIndirectlyDistinguishUnstableCommittedAndAppliedRanges)
        {
            RaftLog log = MakeLog({1, 1, 2, 2});

            ASSERT_TRUE(log.AdvanceStableTo(2).ok());
            ASSERT_TRUE(log.AdvanceCommitTo(3).ok());
            ASSERT_TRUE(log.AdvanceAppliedTo(1).ok());

            EXPECT_EQ(log.stable_index(), 2U);
            EXPECT_EQ(log.commit_index(), 3U);
            EXPECT_EQ(log.applied_index(), 1U);
            EXPECT_LT(log.applied_index(), log.commit_index());
            EXPECT_LT(log.stable_index(), log.last_index());
            ExpectIndexInvariant(log);
        }

        TEST(RaftLogTest, SnapshotBoundaryUpdatesIndexesTermsAndFutureAppendBehavior)
        {
            RaftLog log = MakeLog({1, 1, 2, 3});
            std::vector<LogEntry> entries;
            common::Term term = 0;

            ASSERT_TRUE(log.AdvanceStableTo(4).ok());
            ASSERT_TRUE(log.AdvanceCommitTo(4).ok());
            ASSERT_TRUE(log.AdvanceAppliedTo(4).ok());
            ASSERT_TRUE(log.UpdateSnapshotBoundary(2, 1).ok());

            EXPECT_EQ(log.snapshot_index(), 2U);
            EXPECT_EQ(log.snapshot_term(), 1U);
            EXPECT_EQ(log.first_index(), 3U);
            EXPECT_EQ(log.last_index(), 4U);
            ASSERT_TRUE(log.GetTerm(2, &term).ok());
            EXPECT_EQ(term, 1U);
            EXPECT_EQ(log.GetEntry(2, nullptr).code(), common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.GetEntries(2, 3, &entries).code(), common::StatusCode::kNotFound);
            EXPECT_EQ(log.GetTerm(1, &term).code(), common::StatusCode::kNotFound);
            EXPECT_EQ(log.UpdateSnapshotBoundary(1, 1).code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_EQ(log.UpdateSnapshotBoundary(3, 1).code(),
                      common::StatusCode::kInvalidArgument);
            ASSERT_TRUE(log.TruncateSuffix(5).ok());
            ASSERT_TRUE(log.Append(MakeEntry(5, 3, "next")).ok());
            EXPECT_EQ(log.last_index(), 5U);
            ExpectIndexInvariant(log);
        }

    } // namespace
} // namespace cpr::raft
