#include "config/config.h"

#include <array>
#include <set>
#include <sstream>

namespace cpr::config {

namespace {

template <typename T>
common::Status ValidatePositive(T value, const char* field_name) {
    if (value == 0) {
        return common::Status::InvalidArgument(
            std::string(field_name) + " must be greater than zero");
    }
    return common::Status::OK();
}

std::string FormatMemberAddress(const InitialMember& member) {
    std::ostringstream stream;
    stream << member.ip_address << ':' << member.raft_port << ':'
           << member.metadata_port << ':' << member.store_control_port;
    return stream.str();
}

}  // namespace

common::Status ValidateConfig(const Config& config) {
    if (config.node_id.empty()) {
        return common::Status::InvalidArgument("node_id must not be empty");
    }
    if (config.ip_address.empty()) {
        return common::Status::InvalidArgument("ip_address must not be empty");
    }
    if (config.initial_members.empty()) {
        return common::Status::InvalidArgument(
            "initial_members must contain at least one member");
    }

    const std::array<std::pair<const char*, std::uint16_t>, 3> ports = {{
        {"raft_port", config.raft_port},
        {"metadata_port", config.metadata_port},
        {"store_control_port", config.store_control_port},
    }};
    for (const auto& port : ports) {
        if (port.second == 0) {
            return common::Status::InvalidArgument(
                std::string(port.first) + " must be in range 1-65535");
        }
    }
    if (config.raft_port == config.metadata_port ||
        config.raft_port == config.store_control_port ||
        config.metadata_port == config.store_control_port) {
        return common::Status::InvalidArgument(
            "raft_port, metadata_port, and store_control_port must be distinct");
    }

    if (config.election_timeout_min_ms > config.election_timeout_max_ms) {
        return common::Status::InvalidArgument(
            "election_timeout_min_ms must be less than or equal to "
            "election_timeout_max_ms");
    }
    if (config.heartbeat_interval_ms >= config.election_timeout_min_ms) {
        return common::Status::InvalidArgument(
            "heartbeat_interval_ms must be less than election_timeout_min_ms");
    }

    for (const auto& check : {
             ValidatePositive(config.heartbeat_interval_ms,
                              "heartbeat_interval_ms"),
             ValidatePositive(config.election_timeout_min_ms,
                              "election_timeout_min_ms"),
             ValidatePositive(config.election_timeout_max_ms,
                              "election_timeout_max_ms"),
             ValidatePositive(config.rpc_timeout_ms, "rpc_timeout_ms"),
             ValidatePositive(config.queue_capacity, "queue_capacity"),
             ValidatePositive(config.worker_count, "worker_count"),
             ValidatePositive(config.max_message_size, "max_message_size"),
             ValidatePositive(config.log_batch_size, "log_batch_size"),
             ValidatePositive(config.store_heartbeat_timeout_ms,
                              "store_heartbeat_timeout_ms"),
             ValidatePositive(config.failure_detection_interval_ms,
                              "failure_detection_interval_ms"),
             ValidatePositive(config.task_poll_limit, "task_poll_limit"),
         }) {
        if (!check.ok()) {
            return check;
        }
    }

    if (config.data_directory.empty()) {
        return common::Status::InvalidArgument(
            "data_directory must not be empty");
    }
    if (config.snapshot_directory.empty()) {
        return common::Status::InvalidArgument(
            "snapshot_directory must not be empty");
    }

    std::set<std::string> node_ids;
    std::set<std::string> addresses;
    bool current_node_found = false;
    for (const auto& member : config.initial_members) {
        if (member.node_id.empty()) {
            return common::Status::InvalidArgument(
                "initial_members contains a member with an empty node_id");
        }
        if (member.ip_address.empty()) {
            return common::Status::InvalidArgument(
                "initial_members contains a member with an empty ip_address");
        }
        if (member.raft_port == 0 || member.metadata_port == 0 ||
            member.store_control_port == 0) {
            return common::Status::InvalidArgument(
                "initial_members contains a member with a port outside range "
                "1-65535");
        }

        if (!node_ids.insert(member.node_id).second) {
            return common::Status::InvalidArgument(
                "initial_members contains duplicate node_id: " + member.node_id);
        }

        const std::string address = FormatMemberAddress(member);
        if (!addresses.insert(address).second) {
            return common::Status::InvalidArgument(
                "initial_members contains duplicate address: " + address);
        }

        if (member.node_id == config.node_id) {
            current_node_found = true;
        }
    }

    if (!current_node_found) {
        return common::Status::InvalidArgument(
            "node_id must exist in initial_members: " + config.node_id);
    }

    return common::Status::OK();
}

const char* ToString(MemberRole role) noexcept {
    switch (role) {
    case MemberRole::kVoter:
        return "VOTER";
    case MemberRole::kLearner:
        return "LEARNER";
    }
    return "VOTER";
}

const char* ToString(LogLevel level) noexcept {
    switch (level) {
    case LogLevel::kTrace:
        return "TRACE";
    case LogLevel::kDebug:
        return "DEBUG";
    case LogLevel::kInfo:
        return "INFO";
    case LogLevel::kWarn:
        return "WARN";
    case LogLevel::kError:
        return "ERROR";
    case LogLevel::kCritical:
        return "CRITICAL";
    }
    return "INFO";
}

}  // namespace cpr::config
