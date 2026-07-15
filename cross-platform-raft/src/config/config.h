#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common/status.h"

namespace cpr::config {

enum class MemberRole {
    kVoter,
    kLearner,
};

enum class LogLevel {
    kTrace,
    kDebug,
    kInfo,
    kWarn,
    kError,
    kCritical,
};

struct InitialMember {
    std::string node_id;
    std::string ip_address;
    std::uint16_t raft_port = 0;
    std::uint16_t metadata_port = 0;
    std::uint16_t store_control_port = 0;
    MemberRole role = MemberRole::kVoter;
};

struct Config {
    std::string node_id;
    std::string ip_address;
    std::uint16_t raft_port = 0;
    std::uint16_t metadata_port = 0;
    std::uint16_t store_control_port = 0;
    std::vector<InitialMember> initial_members;
    std::uint32_t heartbeat_interval_ms = 0;
    std::uint32_t election_timeout_min_ms = 0;
    std::uint32_t election_timeout_max_ms = 0;
    std::uint32_t rpc_timeout_ms = 0;
    std::size_t queue_capacity = 0;
    std::size_t worker_count = 0;
    std::size_t max_message_size = 0;
    std::size_t log_batch_size = 0;
    std::filesystem::path data_directory;
    std::filesystem::path snapshot_directory;
    std::uint32_t store_heartbeat_timeout_ms = 0;
    std::uint32_t failure_detection_interval_ms = 0;
    std::size_t task_poll_limit = 0;
    LogLevel log_level = LogLevel::kInfo;
};

common::Status ValidateConfig(const Config& config);

const char* ToString(MemberRole role) noexcept;
const char* ToString(LogLevel level) noexcept;

}  // namespace cpr::config
