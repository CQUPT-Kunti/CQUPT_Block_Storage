#include <cstdint>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "common/status.h"
#include "metadata/metadata_command.h"

namespace cpr::metadata
{
    namespace
    {

        MetadataCommand MakeCommand()
        {
            MetadataCommand command;
            command.type = MetadataCommandType::OPAQUE_OPERATION;
            command.command_id = "cmd-1";
            command.target_id = "store-7";
            command.expected_generation = 42;
            command.payload = {0x10, 0x20, 0x30};
            return command;
        }

        TEST(MetadataCommandTest, RoundTripPreservesFieldsWithExpectedGeneration)
        {
            const MetadataCommand command = MakeCommand();

            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(command, &encoded).ok());

            MetadataCommand decoded;
            ASSERT_TRUE(DecodeMetadataCommand(encoded, &decoded).ok());
            EXPECT_EQ(decoded.type, command.type);
            EXPECT_EQ(decoded.command_id, command.command_id);
            EXPECT_EQ(decoded.target_id, command.target_id);
            ASSERT_TRUE(decoded.expected_generation.has_value());
            EXPECT_EQ(decoded.expected_generation.value(), 42U);
            EXPECT_EQ(decoded.payload, command.payload);
        }

        TEST(MetadataCommandTest, RoundTripPreservesFieldsWithoutExpectedGeneration)
        {
            MetadataCommand command = MakeCommand();
            command.expected_generation.reset();

            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(command, &encoded).ok());

            MetadataCommand decoded;
            ASSERT_TRUE(DecodeMetadataCommand(encoded, &decoded).ok());
            EXPECT_FALSE(decoded.expected_generation.has_value());
            EXPECT_EQ(decoded.command_id, command.command_id);
            EXPECT_EQ(decoded.target_id, command.target_id);
        }

        TEST(MetadataCommandTest, RepeatedEncodingIsDeterministic)
        {
            const MetadataCommand command = MakeCommand();

            raft::OpaquePayload first;
            raft::OpaquePayload second;
            ASSERT_TRUE(EncodeMetadataCommand(command, &first).ok());
            ASSERT_TRUE(EncodeMetadataCommand(command, &second).ok());
            EXPECT_EQ(first, second);
        }

        TEST(MetadataCommandTest, FieldChangesChangeEncodedBytes)
        {
            const MetadataCommand base = MakeCommand();

            raft::OpaquePayload encoded_base;
            ASSERT_TRUE(EncodeMetadataCommand(base, &encoded_base).ok());

            MetadataCommand changed_payload = base;
            changed_payload.payload.push_back(0x40);
            raft::OpaquePayload encoded_payload;
            ASSERT_TRUE(EncodeMetadataCommand(changed_payload, &encoded_payload).ok());
            EXPECT_NE(encoded_base, encoded_payload);

            MetadataCommand changed_target = base;
            changed_target.target_id = "store-8";
            raft::OpaquePayload encoded_target;
            ASSERT_TRUE(EncodeMetadataCommand(changed_target, &encoded_target).ok());
            EXPECT_NE(encoded_base, encoded_target);
        }

        TEST(MetadataCommandTest, EmptyPayloadIsAllowed)
        {
            MetadataCommand command = MakeCommand();
            command.payload.clear();

            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(command, &encoded).ok());

            MetadataCommand decoded;
            ASSERT_TRUE(DecodeMetadataCommand(encoded, &decoded).ok());
            EXPECT_TRUE(decoded.payload.empty());
        }

        TEST(MetadataCommandTest, UnknownVersionIsRejected)
        {
            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(MakeCommand(), &encoded).ok());
            encoded[0] = 9;

            MetadataCommand decoded;
            const common::Status status = DecodeMetadataCommand(encoded, &decoded);
            EXPECT_EQ(status.code(), common::StatusCode::kCorruption);
        }

        TEST(MetadataCommandTest, TruncatedInputIsRejected)
        {
            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(MakeCommand(), &encoded).ok());
            encoded.pop_back();

            MetadataCommand decoded;
            const common::Status status = DecodeMetadataCommand(encoded, &decoded);
            EXPECT_EQ(status.code(), common::StatusCode::kCorruption);
        }

        TEST(MetadataCommandTest, InvalidLengthAndOverflowRiskAreRejected)
        {
            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(MakeCommand(), &encoded).ok());

            encoded[4] = 0x00;
            encoded[5] = 0x00;
            encoded[6] = 0x20;
            encoded[7] = 0x01;

            MetadataCommand decoded;
            const common::Status status = DecodeMetadataCommand(encoded, &decoded);
            EXPECT_EQ(status.code(), common::StatusCode::kCorruption);
        }

        TEST(MetadataCommandTest, UnknownTypeAndIllegalFieldCombinationsAreRejected)
        {
            MetadataCommand unknown_type = MakeCommand();
            unknown_type.type = static_cast<MetadataCommandType>(99);
            raft::OpaquePayload encoded;
            EXPECT_EQ(EncodeMetadataCommand(unknown_type, &encoded).code(),
                      common::StatusCode::kInvalidArgument);

            MetadataCommand empty_id = MakeCommand();
            empty_id.command_id.clear();
            EXPECT_EQ(EncodeMetadataCommand(empty_id, &encoded).code(),
                      common::StatusCode::kInvalidArgument);

            MetadataCommand empty_target = MakeCommand();
            empty_target.target_id.clear();
            EXPECT_EQ(EncodeMetadataCommand(empty_target, &encoded).code(),
                      common::StatusCode::kInvalidArgument);

            MetadataCommand zero_generation = MakeCommand();
            zero_generation.expected_generation = 0;
            EXPECT_EQ(EncodeMetadataCommand(zero_generation, &encoded).code(),
                      common::StatusCode::kInvalidArgument);
        }

        TEST(MetadataCommandTest, TrailingGarbageIsRejected)
        {
            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(MakeCommand(), &encoded).ok());
            encoded.push_back(0xAA);

            MetadataCommand decoded;
            const common::Status status = DecodeMetadataCommand(encoded, &decoded);
            EXPECT_EQ(status.code(), common::StatusCode::kCorruption);
        }

        TEST(MetadataCommandTest, DecodeFailureDoesNotModifyOutputObject)
        {
            raft::OpaquePayload encoded;
            ASSERT_TRUE(EncodeMetadataCommand(MakeCommand(), &encoded).ok());
            encoded[0] = 7;

            MetadataCommand decoded = MakeCommand();
            decoded.command_id = "keep";
            decoded.target_id = "target";
            decoded.expected_generation = 5;
            decoded.payload = {0x77};

            const common::Status status = DecodeMetadataCommand(encoded, &decoded);
            EXPECT_EQ(status.code(), common::StatusCode::kCorruption);
            EXPECT_EQ(decoded.command_id, "keep");
            EXPECT_EQ(decoded.target_id, "target");
            ASSERT_TRUE(decoded.expected_generation.has_value());
            EXPECT_EQ(decoded.expected_generation.value(), 5U);
            ASSERT_EQ(decoded.payload.size(), 1U);
            EXPECT_EQ(decoded.payload[0], 0x77);
        }

    } // namespace
} // namespace cpr::metadata
