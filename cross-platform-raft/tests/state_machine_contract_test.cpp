#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "common/status.h"
#include "metadata/state_machine.h"

namespace cpr::metadata
{
    namespace
    {

        class RecordingStateMachine final : public IRaftStateMachine
        {
        public:
            common::Status Apply(common::LogIndex index,
                                 common::Term term,
                                 const raft::OpaquePayload &command_payload) override
            {
                last_index_ = index;
                last_term_ = term;
                state_ = command_payload;
                return common::Status::OK();
            }

            common::Status CreateSnapshot(common::LogIndex last_applied_index,
                                          common::Term last_applied_term,
                                          raft::OpaquePayload *snapshot_payload) override
            {
                if (snapshot_payload == nullptr)
                {
                    return common::Status::InvalidArgument("snapshot payload must not be null");
                }
                *snapshot_payload = state_;
                snapshot_index_ = last_applied_index;
                snapshot_term_ = last_applied_term;
                return common::Status::OK();
            }

            common::Status RestoreSnapshot(
                const raft::OpaquePayload &snapshot_payload) override
            {
                raft::OpaquePayload restored = snapshot_payload;
                state_ = std::move(restored);
                return common::Status::OK();
            }

            common::LogIndex last_index() const
            {
                return last_index_;
            }

            common::Term last_term() const
            {
                return last_term_;
            }

            common::LogIndex snapshot_index() const
            {
                return snapshot_index_;
            }

            common::Term snapshot_term() const
            {
                return snapshot_term_;
            }

            const raft::OpaquePayload &state() const
            {
                return state_;
            }

        private:
            common::LogIndex last_index_ = common::kInvalidLogIndex;
            common::Term last_term_ = common::kInitialTerm;
            common::LogIndex snapshot_index_ = common::kInvalidLogIndex;
            common::Term snapshot_term_ = common::kInitialTerm;
            raft::OpaquePayload state_;
        };

        TEST(StateMachineContractTest, ApplyReceivesCommittedIndexTermAndOpaquePayload)
        {
            RecordingStateMachine impl;
            IRaftStateMachine &machine = impl;

            const raft::OpaquePayload payload{0x01, 0x02, 0x03};
            ASSERT_TRUE(machine.Apply(7, 3, payload).ok());
            EXPECT_EQ(impl.last_index(), 7U);
            EXPECT_EQ(impl.last_term(), 3U);
            EXPECT_EQ(impl.state(), payload);
        }

        TEST(StateMachineContractTest, CreateSnapshotReturnsOpaqueBusinessPayload)
        {
            RecordingStateMachine impl;
            ASSERT_TRUE(impl.Apply(4, 2, raft::OpaquePayload{0xA0, 0xB0}).ok());

            raft::OpaquePayload snapshot;
            IRaftStateMachine *machine = &impl;
            ASSERT_TRUE(machine->CreateSnapshot(4, 2, &snapshot).ok());
            EXPECT_EQ(snapshot, impl.state());
            EXPECT_EQ(impl.snapshot_index(), 4U);
            EXPECT_EQ(impl.snapshot_term(), 2U);
        }

        TEST(StateMachineContractTest, RestoreSnapshotReplacesBusinessState)
        {
            RecordingStateMachine impl;
            ASSERT_TRUE(impl.Apply(1, 1, raft::OpaquePayload{0x01}).ok());
            ASSERT_TRUE(impl.RestoreSnapshot(raft::OpaquePayload{0x09, 0x08}).ok());
            ASSERT_EQ(impl.state().size(), 2U);
            EXPECT_EQ(impl.state()[0], 0x09);
            EXPECT_EQ(impl.state()[1], 0x08);
        }

        TEST(StateMachineContractTest, InterfaceRemainsReplaceableThroughBasePointer)
        {
            RecordingStateMachine impl;
            IRaftStateMachine *machine = &impl;

            ASSERT_TRUE(machine->Apply(11, 5, raft::OpaquePayload{0x55}).ok());

            raft::OpaquePayload snapshot;
            ASSERT_TRUE(machine->CreateSnapshot(11, 5, &snapshot).ok());
            ASSERT_TRUE(machine->RestoreSnapshot(raft::OpaquePayload{0x33, 0x22}).ok());
            ASSERT_EQ(snapshot.size(), 1U);
            EXPECT_EQ(snapshot[0], 0x55);
            ASSERT_EQ(impl.state().size(), 2U);
            EXPECT_EQ(impl.state()[0], 0x33);
        }

    } // namespace
} // namespace cpr::metadata
