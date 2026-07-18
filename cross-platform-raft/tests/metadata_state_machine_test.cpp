#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "metadata/metadata_command.h"
#include "metadata/metadata_state_machine.h"

namespace cpr::metadata
{
    namespace
    {

        void AppendU8(std::uint8_t value, raft::OpaquePayload *payload)
        {
            payload->push_back(value);
        }

        void AppendU32(std::uint32_t value, raft::OpaquePayload *payload)
        {
            payload->push_back(static_cast<common::Byte>((value >> 24) & 0xFF));
            payload->push_back(static_cast<common::Byte>((value >> 16) & 0xFF));
            payload->push_back(static_cast<common::Byte>((value >> 8) & 0xFF));
            payload->push_back(static_cast<common::Byte>(value & 0xFF));
        }

        void AppendU64(std::uint64_t value, raft::OpaquePayload *payload)
        {
            for (int shift = 56; shift >= 0; shift -= 8)
            {
                payload->push_back(
                    static_cast<common::Byte>((value >> shift) & 0xFF));
            }
        }

        void AppendBytes(const std::string &value, raft::OpaquePayload *payload)
        {
            payload->insert(payload->end(), value.begin(), value.end());
        }

        void AppendBytes(const raft::OpaquePayload &value, raft::OpaquePayload *payload)
        {
            payload->insert(payload->end(), value.begin(), value.end());
        }

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

        raft::OpaquePayload Encode(const MetadataCommand &command)
        {
            raft::OpaquePayload payload;
            EXPECT_TRUE(EncodeMetadataCommand(command, &payload).ok());
            return payload;
        }

        common::Status ApplyCommand(MetadataStateMachine *machine,
                                    common::LogIndex index,
                                    common::Term term,
                                    const MetadataCommand &command)
        {
            EXPECT_NE(machine, nullptr);
            return machine->Apply(index, term, Encode(command));
        }

        MetadataStateRecord GetRecord(MetadataStateMachine *machine,
                                      const std::string &target_id)
        {
            MetadataStateRecord record;
            EXPECT_TRUE(machine->GetRecord(target_id, &record).ok());
            return record;
        }

        raft::OpaquePayload BuildSnapshotBytes(
            common::LogIndex last_applied_index,
            common::Term last_applied_term,
            const std::vector<MetadataStateRecord> &records)
        {
            raft::OpaquePayload payload;
            AppendU8(1, &payload);
            AppendU32(static_cast<std::uint32_t>(records.size()), &payload);
            AppendU64(last_applied_index, &payload);
            AppendU64(last_applied_term, &payload);
            for (const auto &record : records)
            {
                AppendU32(static_cast<std::uint32_t>(record.target_id.size()), &payload);
                AppendU32(static_cast<std::uint32_t>(record.payload.size()), &payload);
                AppendU32(static_cast<std::uint32_t>(record.last_command_id.size()), &payload);
                AppendU64(record.generation, &payload);
                AppendU64(record.last_update_index, &payload);
                AppendU64(record.last_update_term, &payload);
                AppendBytes(record.target_id, &payload);
                AppendBytes(record.payload, &payload);
                AppendBytes(record.last_command_id, &payload);
            }
            return payload;
        }

        TEST(MetadataStateMachineTest, ApplyAndQueryTrackIndependentTargetsDeterministically)
        {
            MetadataStateMachine machine;

            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x10})).ok());
            ASSERT_TRUE(ApplyCommand(&machine, 2, 1, MakeCommand("cmd-2", "beta", {0x20, 0x21})).ok());

            const MetadataStateRecord alpha = GetRecord(&machine, "alpha");
            EXPECT_EQ(alpha.target_id, "alpha");
            EXPECT_EQ(alpha.generation, 1U);
            EXPECT_EQ(alpha.last_command_id, "cmd-1");
            ASSERT_EQ(alpha.payload.size(), 1U);
            EXPECT_EQ(alpha.payload[0], 0x10);

            const MetadataStateRecord beta = GetRecord(&machine, "beta");
            EXPECT_EQ(beta.target_id, "beta");
            EXPECT_EQ(beta.generation, 1U);
            EXPECT_EQ(beta.last_command_id, "cmd-2");
            ASSERT_EQ(beta.payload.size(), 2U);
            EXPECT_EQ(beta.payload[0], 0x20);
            EXPECT_EQ(beta.payload[1], 0x21);
        }

        TEST(MetadataStateMachineTest, ExpectedGenerationUpdatesAtomicallyAndMismatchLeavesStateUnchanged)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x01})).ok());

            ASSERT_TRUE(ApplyCommand(&machine, 2, 1, MakeCommand("cmd-2", "alpha", {0x02}, 1)).ok());
            MetadataStateRecord record = GetRecord(&machine, "alpha");
            EXPECT_EQ(record.generation, 2U);
            ASSERT_EQ(record.payload.size(), 1U);
            EXPECT_EQ(record.payload[0], 0x02);

            const common::Status mismatch =
                ApplyCommand(&machine, 3, 1, MakeCommand("cmd-3", "alpha", {0x03}, 1));
            EXPECT_EQ(mismatch.code(), common::StatusCode::kBusy);

            record = GetRecord(&machine, "alpha");
            EXPECT_EQ(record.generation, 2U);
            ASSERT_EQ(record.payload.size(), 1U);
            EXPECT_EQ(record.payload[0], 0x02);

            common::LogIndex last_index = 0;
            common::Term last_term = 0;
            ASSERT_TRUE(machine.GetLastApplied(&last_index, &last_term).ok());
            EXPECT_EQ(last_index, 2U);
            EXPECT_EQ(last_term, 1U);
        }

        TEST(MetadataStateMachineTest, ApplyRejectsDuplicateRollbackAndGapWithoutAdvancingProgress)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x01})).ok());

            EXPECT_EQ(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-dup", "alpha", {0x02})).code(),
                      common::StatusCode::kBusy);
            EXPECT_EQ(ApplyCommand(&machine, 0, 1, MakeCommand("cmd-bad", "alpha", {0x03})).code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_EQ(ApplyCommand(&machine, 3, 1, MakeCommand("cmd-gap", "alpha", {0x04})).code(),
                      common::StatusCode::kBusy);

            const MetadataStateRecord record = GetRecord(&machine, "alpha");
            EXPECT_EQ(record.generation, 1U);
            ASSERT_EQ(record.payload.size(), 1U);
            EXPECT_EQ(record.payload[0], 0x01);

            common::LogIndex last_index = 0;
            common::Term last_term = 0;
            ASSERT_TRUE(machine.GetLastApplied(&last_index, &last_term).ok());
            EXPECT_EQ(last_index, 1U);
            EXPECT_EQ(last_term, 1U);
        }

        TEST(MetadataStateMachineTest, DecodeFailureAndMissingTargetLeaveStateUnchanged)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x01})).ok());

            raft::OpaquePayload invalid = Encode(MakeCommand("cmd-2", "beta", {0x02}));
            invalid[0] = 9;
            EXPECT_EQ(machine.Apply(2, 1, invalid).code(),
                      common::StatusCode::kCorruption);

            EXPECT_EQ(ApplyCommand(&machine, 2, 1, MakeCommand("cmd-2", "beta", {0x02}, 1)).code(),
                      common::StatusCode::kNotFound);

            common::LogIndex last_index = 0;
            common::Term last_term = 0;
            ASSERT_TRUE(machine.GetLastApplied(&last_index, &last_term).ok());
            EXPECT_EQ(last_index, 1U);
            EXPECT_EQ(last_term, 1U);
            EXPECT_EQ(machine.GetRecord("beta", nullptr).code(),
                      common::StatusCode::kInvalidArgument);
            MetadataStateRecord missing;
            EXPECT_EQ(machine.GetRecord("beta", &missing).code(),
                      common::StatusCode::kNotFound);
        }

        TEST(MetadataStateMachineTest, QueryReturnsCopyAndMissingTargetIsNotFound)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x01, 0x02})).ok());

            MetadataStateRecord record = GetRecord(&machine, "alpha");
            record.payload[0] = 0xFF;
            record.last_command_id = "changed";

            const MetadataStateRecord fresh = GetRecord(&machine, "alpha");
            ASSERT_EQ(fresh.payload.size(), 2U);
            EXPECT_EQ(fresh.payload[0], 0x01);
            EXPECT_EQ(fresh.last_command_id, "cmd-1");

            MetadataStateRecord missing;
            EXPECT_EQ(machine.GetRecord("missing", &missing).code(),
                      common::StatusCode::kNotFound);
        }

        TEST(MetadataStateMachineTest, EmptyStateSnapshotIsDeterministic)
        {
            MetadataStateMachine machine;

            raft::OpaquePayload first;
            raft::OpaquePayload second;
            ASSERT_TRUE(machine.CreateSnapshot(0, 0, &first).ok());
            ASSERT_TRUE(machine.CreateSnapshot(0, 0, &second).ok());
            EXPECT_EQ(first, second);
        }

        TEST(MetadataStateMachineTest, SnapshotIsDeterministicAndRestoreRecoversStateAndProgress)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "beta", {0x20})).ok());
            ASSERT_TRUE(ApplyCommand(&machine, 2, 1, MakeCommand("cmd-2", "alpha", {0x10})).ok());
            ASSERT_TRUE(ApplyCommand(&machine, 3, 2, MakeCommand("cmd-3", "alpha", {0x11}, 1)).ok());

            raft::OpaquePayload first;
            raft::OpaquePayload second;
            ASSERT_TRUE(machine.CreateSnapshot(3, 2, &first).ok());
            ASSERT_TRUE(machine.CreateSnapshot(3, 2, &second).ok());
            EXPECT_EQ(first, second);

            MetadataStateMachine restored;
            ASSERT_TRUE(restored.RestoreSnapshot(first).ok());

            const MetadataStateRecord alpha = GetRecord(&restored, "alpha");
            EXPECT_EQ(alpha.generation, 2U);
            ASSERT_EQ(alpha.payload.size(), 1U);
            EXPECT_EQ(alpha.payload[0], 0x11);

            const MetadataStateRecord beta = GetRecord(&restored, "beta");
            EXPECT_EQ(beta.generation, 1U);
            ASSERT_EQ(beta.payload.size(), 1U);
            EXPECT_EQ(beta.payload[0], 0x20);

            common::LogIndex last_index = 0;
            common::Term last_term = 0;
            ASSERT_TRUE(restored.GetLastApplied(&last_index, &last_term).ok());
            EXPECT_EQ(last_index, 3U);
            EXPECT_EQ(last_term, 2U);
        }

        TEST(MetadataStateMachineTest, SnapshotProgressMismatchIsRejected)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x01})).ok());

            raft::OpaquePayload snapshot;
            EXPECT_EQ(machine.CreateSnapshot(2, 1, &snapshot).code(),
                      common::StatusCode::kBusy);
            EXPECT_EQ(machine.CreateSnapshot(1, 2, &snapshot).code(),
                      common::StatusCode::kBusy);
        }

        TEST(MetadataStateMachineTest, RestoreRejectsUnknownVersionTruncationAndTrailingGarbage)
        {
            MetadataStateMachine machine;
            raft::OpaquePayload snapshot = BuildSnapshotBytes(0, 0, {});

            raft::OpaquePayload unknown = snapshot;
            unknown[0] = 9;
            EXPECT_EQ(machine.RestoreSnapshot(unknown).code(),
                      common::StatusCode::kCorruption);

            raft::OpaquePayload truncated = snapshot;
            truncated.pop_back();
            EXPECT_EQ(machine.RestoreSnapshot(truncated).code(),
                      common::StatusCode::kCorruption);

            raft::OpaquePayload trailing = snapshot;
            trailing.push_back(0xAA);
            EXPECT_EQ(machine.RestoreSnapshot(trailing).code(),
                      common::StatusCode::kCorruption);
        }

        TEST(MetadataStateMachineTest, RestoreRejectsIllegalLengthsRecordCountsDuplicatesAndGeneration)
        {
            MetadataStateMachine machine;

            raft::OpaquePayload too_many = BuildSnapshotBytes(0, 0, {});
            too_many[1] = 0x00;
            too_many[2] = 0x00;
            too_many[3] = 0x10;
            too_many[4] = 0x01;
            EXPECT_EQ(machine.RestoreSnapshot(too_many).code(),
                      common::StatusCode::kCorruption);

            MetadataStateRecord record;
            record.target_id = "alpha";
            record.payload = {0x01};
            record.generation = 1;
            record.last_command_id = "cmd-1";
            record.last_update_index = 1;
            record.last_update_term = 1;

            raft::OpaquePayload duplicate = BuildSnapshotBytes(1, 1, {record, record});
            EXPECT_EQ(machine.RestoreSnapshot(duplicate).code(),
                      common::StatusCode::kCorruption);

            record.generation = 0;
            raft::OpaquePayload bad_generation = BuildSnapshotBytes(1, 1, {record});
            EXPECT_EQ(machine.RestoreSnapshot(bad_generation).code(),
                      common::StatusCode::kCorruption);

            MetadataStateRecord oversize = record;
            oversize.generation = 1;
            oversize.target_id.assign(kMaxMetadataCommandTargetBytes + 1, 'x');
            raft::OpaquePayload bad_length = BuildSnapshotBytes(1, 1, {oversize});
            EXPECT_EQ(machine.RestoreSnapshot(bad_length).code(),
                      common::StatusCode::kCorruption);
        }

        TEST(MetadataStateMachineTest, RestoreFailureDoesNotModifyExistingState)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x01})).ok());

            MetadataStateRecord bad;
            bad.target_id = "alpha";
            bad.payload = {0x02};
            bad.generation = 0;
            bad.last_command_id = "cmd-2";
            bad.last_update_index = 2;
            bad.last_update_term = 1;

            EXPECT_EQ(machine.RestoreSnapshot(BuildSnapshotBytes(2, 1, {bad})).code(),
                      common::StatusCode::kCorruption);

            const MetadataStateRecord record = GetRecord(&machine, "alpha");
            EXPECT_EQ(record.generation, 1U);
            ASSERT_EQ(record.payload.size(), 1U);
            EXPECT_EQ(record.payload[0], 0x01);

            common::LogIndex last_index = 0;
            common::Term last_term = 0;
            ASSERT_TRUE(machine.GetLastApplied(&last_index, &last_term).ok());
            EXPECT_EQ(last_index, 1U);
            EXPECT_EQ(last_term, 1U);
        }

        TEST(MetadataStateMachineTest, RestoreAllowsNextSequentialApply)
        {
            MetadataStateMachine machine;
            ASSERT_TRUE(ApplyCommand(&machine, 1, 1, MakeCommand("cmd-1", "alpha", {0x01})).ok());
            ASSERT_TRUE(ApplyCommand(&machine, 2, 1, MakeCommand("cmd-2", "beta", {0x02})).ok());

            raft::OpaquePayload snapshot;
            ASSERT_TRUE(machine.CreateSnapshot(2, 1, &snapshot).ok());

            MetadataStateMachine restored;
            ASSERT_TRUE(restored.RestoreSnapshot(snapshot).ok());
            ASSERT_TRUE(ApplyCommand(&restored, 3, 2, MakeCommand("cmd-3", "alpha", {0x03}, 1)).ok());

            const MetadataStateRecord record = GetRecord(&restored, "alpha");
            EXPECT_EQ(record.generation, 2U);
            ASSERT_EQ(record.payload.size(), 1U);
            EXPECT_EQ(record.payload[0], 0x03);
        }

        TEST(MetadataStateMachineTest, FullReplayMatchesSnapshotPlusIncrementalReplay)
        {
            const MetadataCommand c1 = MakeCommand("cmd-1", "alpha", {0x01});
            const MetadataCommand c2 = MakeCommand("cmd-2", "beta", {0x10});
            const MetadataCommand c3 = MakeCommand("cmd-3", "alpha", {0x02}, 1);
            const MetadataCommand c4 = MakeCommand("cmd-4", "beta", {0x11}, 1);

            MetadataStateMachine replay_all;
            ASSERT_TRUE(ApplyCommand(&replay_all, 1, 1, c1).ok());
            ASSERT_TRUE(ApplyCommand(&replay_all, 2, 1, c2).ok());
            ASSERT_TRUE(ApplyCommand(&replay_all, 3, 2, c3).ok());
            ASSERT_TRUE(ApplyCommand(&replay_all, 4, 2, c4).ok());

            MetadataStateMachine snapshot_source;
            ASSERT_TRUE(ApplyCommand(&snapshot_source, 1, 1, c1).ok());
            ASSERT_TRUE(ApplyCommand(&snapshot_source, 2, 1, c2).ok());

            raft::OpaquePayload snapshot;
            ASSERT_TRUE(snapshot_source.CreateSnapshot(2, 1, &snapshot).ok());

            MetadataStateMachine restore_then_replay;
            ASSERT_TRUE(restore_then_replay.RestoreSnapshot(snapshot).ok());
            ASSERT_TRUE(ApplyCommand(&restore_then_replay, 3, 2, c3).ok());
            ASSERT_TRUE(ApplyCommand(&restore_then_replay, 4, 2, c4).ok());

            const MetadataStateRecord all_alpha = GetRecord(&replay_all, "alpha");
            const MetadataStateRecord snap_alpha = GetRecord(&restore_then_replay, "alpha");
            EXPECT_EQ(all_alpha.payload, snap_alpha.payload);
            EXPECT_EQ(all_alpha.generation, snap_alpha.generation);
            EXPECT_EQ(all_alpha.last_command_id, snap_alpha.last_command_id);

            const MetadataStateRecord all_beta = GetRecord(&replay_all, "beta");
            const MetadataStateRecord snap_beta = GetRecord(&restore_then_replay, "beta");
            EXPECT_EQ(all_beta.payload, snap_beta.payload);
            EXPECT_EQ(all_beta.generation, snap_beta.generation);
            EXPECT_EQ(all_beta.last_command_id, snap_beta.last_command_id);

            common::LogIndex all_index = 0;
            common::Term all_term = 0;
            common::LogIndex snap_index = 0;
            common::Term snap_term = 0;
            ASSERT_TRUE(replay_all.GetLastApplied(&all_index, &all_term).ok());
            ASSERT_TRUE(restore_then_replay.GetLastApplied(&snap_index, &snap_term).ok());
            EXPECT_EQ(all_index, snap_index);
            EXPECT_EQ(all_term, snap_term);
        }

    } // namespace
} // namespace cpr::metadata
