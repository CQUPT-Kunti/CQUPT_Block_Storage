#include "config/config_loader.h"

#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include <nlohmann/json.hpp>

namespace cpr::config {

namespace {

using Json = nlohmann::json;

common::Status MakeFieldError(const std::string& field,
                              const std::string& reason) {
    return common::Status::InvalidArgument(field + ": " + reason);
}

common::Status RequireObject(const Json& value, const std::string& field) {
    if (!value.is_object()) {
        return MakeFieldError(field, "must be a JSON object");
    }
    return common::Status::OK();
}

common::Status RejectUnknownFields(
    const Json& object,
    std::initializer_list<std::string_view> allowed_fields,
    const std::string& object_name) {
    for (auto it = object.begin(); it != object.end(); ++it) {
        bool allowed = false;
        for (std::string_view field : allowed_fields) {
            if (it.key() == field) {
                allowed = true;
                break;
            }
        }
        if (!allowed) {
            return MakeFieldError(object_name + "." + it.key(),
                                  "unknown field");
        }
    }
    return common::Status::OK();
}

common::Status GetRequiredField(const Json& object,
                                const std::string& object_name,
                                const char* field_name,
                                const Json** value) {
    const auto it = object.find(field_name);
    if (it == object.end()) {
        return MakeFieldError(object_name + "." + field_name,
                              "missing required field");
    }
    *value = &(*it);
    return common::Status::OK();
}

common::Status ReadString(const Json& object,
                          const std::string& object_name,
                          const char* field_name,
                          std::string* value) {
    const Json* field = nullptr;
    common::Status status =
        GetRequiredField(object, object_name, field_name, &field);
    if (!status.ok()) {
        return status;
    }
    if (!field->is_string()) {
        return MakeFieldError(object_name + "." + field_name,
                              "must be a string");
    }
    *value = field->get<std::string>();
    return common::Status::OK();
}

template <typename T>
common::Status ReadUnsignedInteger(const Json& object,
                                   const std::string& object_name,
                                   const char* field_name,
                                   T max_value,
                                   T* value) {
    const Json* field = nullptr;
    common::Status status =
        GetRequiredField(object, object_name, field_name, &field);
    if (!status.ok()) {
        return status;
    }
    if (!field->is_number_integer() && !field->is_number_unsigned()) {
        return MakeFieldError(object_name + "." + field_name,
                              "must be an integer");
    }

    std::uint64_t parsed = 0;
    if (field->is_number_unsigned()) {
        parsed = field->get<std::uint64_t>();
    } else {
        const std::int64_t signed_value = field->get<std::int64_t>();
        if (signed_value <= 0) {
            std::ostringstream stream;
            stream << "must be in range 1-" << max_value;
            return MakeFieldError(object_name + "." + field_name,
                                  stream.str());
        }
        parsed = static_cast<std::uint64_t>(signed_value);
    }
    if (parsed == 0 || parsed > max_value) {
        std::ostringstream stream;
        stream << "must be in range 1-" << max_value;
        return MakeFieldError(object_name + "." + field_name, stream.str());
    }

    *value = static_cast<T>(parsed);
    return common::Status::OK();
}

common::Status ReadMemberRole(const Json& object,
                              const std::string& object_name,
                              MemberRole* role) {
    std::string value;
    common::Status status = ReadString(object, object_name, "role", &value);
    if (!status.ok()) {
        return status;
    }

    if (value == "VOTER") {
        *role = MemberRole::kVoter;
        return common::Status::OK();
    }
    if (value == "LEARNER") {
        *role = MemberRole::kLearner;
        return common::Status::OK();
    }
    return MakeFieldError(object_name + ".role",
                          "must be one of: VOTER, LEARNER");
}

common::Status ReadLogLevel(const Json& object,
                            const std::string& object_name,
                            LogLevel* log_level) {
    std::string value;
    common::Status status =
        ReadString(object, object_name, "log_level", &value);
    if (!status.ok()) {
        return status;
    }

    if (value == "TRACE") {
        *log_level = LogLevel::kTrace;
    } else if (value == "DEBUG") {
        *log_level = LogLevel::kDebug;
    } else if (value == "INFO") {
        *log_level = LogLevel::kInfo;
    } else if (value == "WARN") {
        *log_level = LogLevel::kWarn;
    } else if (value == "ERROR") {
        *log_level = LogLevel::kError;
    } else if (value == "CRITICAL") {
        *log_level = LogLevel::kCritical;
    } else {
        return MakeFieldError(object_name + ".log_level",
                              "must be one of: TRACE, DEBUG, INFO, WARN, "
                              "ERROR, CRITICAL");
    }
    return common::Status::OK();
}

common::Status ParseMember(const Json& value,
                           std::size_t index,
                           InitialMember* member) {
    const std::string object_name =
        "initial_members[" + std::to_string(index) + "]";
    common::Status status = RequireObject(value, object_name);
    if (!status.ok()) {
        return status;
    }
    status = RejectUnknownFields(
        value,
        {"node_id", "ip_address", "raft_port", "metadata_port",
         "store_control_port", "role"},
        object_name);
    if (!status.ok()) {
        return status;
    }

    if (!(status =
              ReadString(value, object_name, "node_id", &member->node_id))
             .ok() ||
        !(status = ReadString(value, object_name, "ip_address",
                              &member->ip_address))
             .ok() ||
        !(status = ReadUnsignedInteger(
                value, object_name, "raft_port",
                static_cast<std::uint16_t>(
                    std::numeric_limits<std::uint16_t>::max()),
                &member->raft_port))
             .ok() ||
        !(status = ReadUnsignedInteger(
                value, object_name, "metadata_port",
                static_cast<std::uint16_t>(
                    std::numeric_limits<std::uint16_t>::max()),
                &member->metadata_port))
             .ok() ||
        !(status = ReadUnsignedInteger(
                value, object_name, "store_control_port",
                static_cast<std::uint16_t>(
                    std::numeric_limits<std::uint16_t>::max()),
                &member->store_control_port))
             .ok() ||
        !(status = ReadMemberRole(value, object_name, &member->role)).ok()) {
        return status;
    }

    return common::Status::OK();
}

common::Status ReadMembers(const Json& object,
                           const std::string& object_name,
                           std::vector<InitialMember>* members) {
    const Json* field = nullptr;
    common::Status status =
        GetRequiredField(object, object_name, "initial_members", &field);
    if (!status.ok()) {
        return status;
    }
    if (!field->is_array()) {
        return MakeFieldError(object_name + ".initial_members",
                              "must be an array");
    }

    members->clear();
    members->reserve(field->size());
    for (std::size_t index = 0; index < field->size(); ++index) {
        InitialMember member;
        status = ParseMember((*field)[index], index, &member);
        if (!status.ok()) {
            return status;
        }
        members->push_back(std::move(member));
    }
    return common::Status::OK();
}

common::Status ParseConfig(const Json& root, Config* config) {
    common::Status status = RequireObject(root, "root");
    if (!status.ok()) {
        return status;
    }

    status = RejectUnknownFields(
        root,
        {"node_id", "ip_address", "raft_port", "metadata_port",
         "store_control_port", "initial_members", "heartbeat_interval_ms",
         "election_timeout_min_ms", "election_timeout_max_ms",
         "rpc_timeout_ms", "queue_capacity", "worker_count",
         "max_message_size", "log_batch_size", "data_directory",
         "snapshot_directory", "store_heartbeat_timeout_ms",
         "failure_detection_interval_ms", "task_poll_limit", "log_level"},
        "root");
    if (!status.ok()) {
        return status;
    }

    if (!(status = ReadString(root, "root", "node_id", &config->node_id)).ok() ||
        !(status =
              ReadString(root, "root", "ip_address", &config->ip_address))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "raft_port",
                static_cast<std::uint16_t>(
                    std::numeric_limits<std::uint16_t>::max()),
                &config->raft_port))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "metadata_port",
                static_cast<std::uint16_t>(
                    std::numeric_limits<std::uint16_t>::max()),
                &config->metadata_port))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "store_control_port",
                static_cast<std::uint16_t>(
                    std::numeric_limits<std::uint16_t>::max()),
                &config->store_control_port))
             .ok() ||
        !(status = ReadMembers(root, "root", &config->initial_members)).ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "heartbeat_interval_ms",
                std::numeric_limits<std::uint32_t>::max(),
                &config->heartbeat_interval_ms))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "election_timeout_min_ms",
                std::numeric_limits<std::uint32_t>::max(),
                &config->election_timeout_min_ms))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "election_timeout_max_ms",
                std::numeric_limits<std::uint32_t>::max(),
                &config->election_timeout_max_ms))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "rpc_timeout_ms",
                std::numeric_limits<std::uint32_t>::max(),
                &config->rpc_timeout_ms))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "queue_capacity",
                std::numeric_limits<std::size_t>::max(),
                &config->queue_capacity))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "worker_count",
                std::numeric_limits<std::size_t>::max(),
                &config->worker_count))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "max_message_size",
                std::numeric_limits<std::size_t>::max(),
                &config->max_message_size))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "log_batch_size",
                std::numeric_limits<std::size_t>::max(),
                &config->log_batch_size))
             .ok()) {
        return status;
    }

    std::string data_directory;
    std::string snapshot_directory;
    if (!(status =
              ReadString(root, "root", "data_directory", &data_directory))
             .ok() ||
        !(status = ReadString(root, "root", "snapshot_directory",
                              &snapshot_directory))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "store_heartbeat_timeout_ms",
                std::numeric_limits<std::uint32_t>::max(),
                &config->store_heartbeat_timeout_ms))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "failure_detection_interval_ms",
                std::numeric_limits<std::uint32_t>::max(),
                &config->failure_detection_interval_ms))
             .ok() ||
        !(status = ReadUnsignedInteger(
                root, "root", "task_poll_limit",
                std::numeric_limits<std::size_t>::max(),
                &config->task_poll_limit))
             .ok() ||
        !(status = ReadLogLevel(root, "root", &config->log_level)).ok()) {
        return status;
    }

    config->data_directory = std::filesystem::path(data_directory);
    config->snapshot_directory = std::filesystem::path(snapshot_directory);
    return ValidateConfig(*config);
}

}  // namespace

common::Status ConfigLoader::LoadFromFile(const std::filesystem::path& path,
                                          Config* config) {
    if (config == nullptr) {
        return common::Status::InvalidArgument(
            "config output pointer must not be null");
    }

    std::ifstream input(path);
    if (!input.is_open()) {
        return common::Status::IoError(
            "failed to open config file: " + path.string());
    }

    try {
        const Json root = Json::parse(input, nullptr, true, true);
        Config parsed;
        common::Status status = ParseConfig(root, &parsed);
        if (!status.ok()) {
            return status;
        }
        *config = std::move(parsed);
        return common::Status::OK();
    } catch (const Json::parse_error& error) {
        return common::Status::InvalidArgument(
            "failed to parse JSON in " + path.string() + ": " + error.what());
    } catch (const Json::exception& error) {
        return common::Status::InvalidArgument(
            "invalid JSON in " + path.string() + ": " + error.what());
    }
}

}  // namespace cpr::config
