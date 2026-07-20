#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "common/checksum.h"
#include "common/status.h"
#include "raft/raft_types.h"

namespace cpr::tests
{

    enum class FixtureNodeRole : std::uint8_t
    {
        FOLLOWER,
        CANDIDATE,
        LEADER,
        LEARNER,
    };

    enum class FixtureStepType : std::uint8_t
    {
        TICK,
        PROPOSE,
        DELIVER,
        PARTITION,
        HEAL,
    };

    struct FixtureNode
    {
        common::NodeId node_id = common::kInvalidNodeId;
        std::string name;
        FixtureNodeRole role = FixtureNodeRole::FOLLOWER;
        raft::NodeAddress address;
    };

    struct FixtureStep
    {
        std::uint64_t step_id = 0;
        FixtureStepType type = FixtureStepType::TICK;
        common::NodeId node_id = common::kInvalidNodeId;
        common::NodeId target_node_id = common::kInvalidNodeId;
        std::string payload;
    };

    struct FixtureExpectation
    {
        std::string key;
        std::string value;
    };

    struct Fixture
    {
        std::uint32_t version = 0;
        std::string scenario;
        common::ChecksumValue checksum = 0;
        std::vector<FixtureNode> nodes;
        std::vector<FixtureStep> steps;
        std::vector<FixtureExpectation> expectations;
    };

    class FixtureLoader
    {
    public:
        static common::ChecksumValue ComputeChecksumForText(std::string_view text);
        static common::Status LoadFromString(std::string_view text,
                                             Fixture *fixture);
        static common::Status LoadFromFile(const std::filesystem::path &path,
                                           Fixture *fixture);
    };

} // namespace cpr::tests
