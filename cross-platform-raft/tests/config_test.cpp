#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "config/config_loader.h"

namespace cpr::config
{
  namespace
  {

    class TempDir
    {
    public:
      TempDir()
      {
        const auto unique =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        path_ = std::filesystem::temp_directory_path() /
                ("cpr-config-test-" + unique);
        std::filesystem::create_directories(path_);
      }

      ~TempDir()
      {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
      }

      const std::filesystem::path &path() const { return path_; }

    private:
      std::filesystem::path path_;
    };

    std::filesystem::path WriteConfigFile(const TempDir &dir,
                                          const std::string &file_name,
                                          const std::string &body)
    {
      const std::filesystem::path path = dir.path() / file_name;
      std::ofstream output(path);
      output << body;
      return path;
    }

    TEST(ConfigTest, LoadsValidConfiguration)
    {
      TempDir dir;
      const std::filesystem::path path = WriteConfigFile(
          dir,
          "valid.json",
          R"({
  "node_id": "node-1",
  "ip_address": "127.0.0.1",
  "raft_port": 7101,
  "metadata_port": 7201,
  "store_control_port": 7301,
  "initial_members": [
    {
      "node_id": "node-1",
      "ip_address": "127.0.0.1",
      "raft_port": 7101,
      "metadata_port": 7201,
      "store_control_port": 7301,
      "role": "VOTER"
    }
  ],
  "heartbeat_interval_ms": 150,
  "election_timeout_min_ms": 500,
  "election_timeout_max_ms": 900,
  "rpc_timeout_ms": 1000,
  "queue_capacity": 1024,
  "worker_count": 4,
  "max_message_size": 4096,
  "log_batch_size": 16,
  "data_directory": "data/node-1",
  "snapshot_directory": "data/node-1/snapshots",
  "store_heartbeat_timeout_ms": 3000,
  "failure_detection_interval_ms": 1000,
  "task_poll_limit": 8,
  "log_level": "INFO"
})");

      Config config;
      const common::Status status = ConfigLoader::LoadFromFile(path, &config);
      ASSERT_TRUE(status.ok()) << status.ToString();
      EXPECT_EQ(config.node_id, "node-1");
      EXPECT_EQ(config.initial_members.size(), 1U);
    }

    TEST(ConfigTest, RejectsInvalidNodeIdPortAndEmptyPath)
    {
      TempDir dir;
      const std::filesystem::path path = WriteConfigFile(
          dir,
          "invalid.json",
          R"({
  "node_id": "",
  "ip_address": "127.0.0.1",
  "raft_port": 0,
  "metadata_port": 7201,
  "store_control_port": 7301,
  "initial_members": [
    {
      "node_id": "node-1",
      "ip_address": "127.0.0.1",
      "raft_port": 7101,
      "metadata_port": 7201,
      "store_control_port": 7301,
      "role": "VOTER"
    }
  ],
  "heartbeat_interval_ms": 150,
  "election_timeout_min_ms": 500,
  "election_timeout_max_ms": 900,
  "rpc_timeout_ms": 1000,
  "queue_capacity": 1024,
  "worker_count": 4,
  "max_message_size": 4096,
  "log_batch_size": 16,
  "data_directory": "",
  "snapshot_directory": "snapshots",
  "store_heartbeat_timeout_ms": 3000,
  "failure_detection_interval_ms": 1000,
  "task_poll_limit": 8,
  "log_level": "INFO"
})");

      Config config;
      const common::Status status = ConfigLoader::LoadFromFile(path, &config);
      EXPECT_EQ(status.code(), common::StatusCode::kInvalidArgument);
    }

    TEST(ConfigTest, RejectsCurrentNodeMissingFromInitialMembers)
    {
      TempDir dir;
      const std::filesystem::path path = WriteConfigFile(
          dir,
          "missing-node.json",
          R"({
  "node_id": "node-1",
  "ip_address": "127.0.0.1",
  "raft_port": 7101,
  "metadata_port": 7201,
  "store_control_port": 7301,
  "initial_members": [
    {
      "node_id": "node-2",
      "ip_address": "127.0.0.1",
      "raft_port": 7102,
      "metadata_port": 7202,
      "store_control_port": 7302,
      "role": "VOTER"
    }
  ],
  "heartbeat_interval_ms": 150,
  "election_timeout_min_ms": 500,
  "election_timeout_max_ms": 900,
  "rpc_timeout_ms": 1000,
  "queue_capacity": 1024,
  "worker_count": 4,
  "max_message_size": 4096,
  "log_batch_size": 16,
  "data_directory": "data",
  "snapshot_directory": "snapshots",
  "store_heartbeat_timeout_ms": 3000,
  "failure_detection_interval_ms": 1000,
  "task_poll_limit": 8,
  "log_level": "INFO"
})");

      Config config;
      const common::Status status = ConfigLoader::LoadFromFile(path, &config);
      EXPECT_EQ(status.code(), common::StatusCode::kInvalidArgument);
      EXPECT_NE(status.message().find("node_id must exist in initial_members"),
                std::string::npos);
    }

    TEST(ConfigTest, RejectsHeartbeatNotLessThanElectionTimeout)
    {
      TempDir dir;
      const std::filesystem::path path = WriteConfigFile(
          dir,
          "heartbeat.json",
          R"({
  "node_id": "node-1",
  "ip_address": "127.0.0.1",
  "raft_port": 7101,
  "metadata_port": 7201,
  "store_control_port": 7301,
  "initial_members": [
    {
      "node_id": "node-1",
      "ip_address": "127.0.0.1",
      "raft_port": 7101,
      "metadata_port": 7201,
      "store_control_port": 7301,
      "role": "VOTER"
    }
  ],
  "heartbeat_interval_ms": 500,
  "election_timeout_min_ms": 500,
  "election_timeout_max_ms": 900,
  "rpc_timeout_ms": 1000,
  "queue_capacity": 1024,
  "worker_count": 4,
  "max_message_size": 4096,
  "log_batch_size": 16,
  "data_directory": "data",
  "snapshot_directory": "snapshots",
  "store_heartbeat_timeout_ms": 3000,
  "failure_detection_interval_ms": 1000,
  "task_poll_limit": 8,
  "log_level": "INFO"
})");

      Config config;
      const common::Status status = ConfigLoader::LoadFromFile(path, &config);
      EXPECT_EQ(status.code(), common::StatusCode::kInvalidArgument);
      EXPECT_NE(status.message().find("heartbeat_interval_ms"), std::string::npos);
    }

    TEST(ConfigTest, RejectsZeroQueueCapacityAndMessageSize)
    {
      TempDir dir;
      const std::filesystem::path path = WriteConfigFile(
          dir,
          "limits.json",
          R"({
  "node_id": "node-1",
  "ip_address": "127.0.0.1",
  "raft_port": 7101,
  "metadata_port": 7201,
  "store_control_port": 7301,
  "initial_members": [
    {
      "node_id": "node-1",
      "ip_address": "127.0.0.1",
      "raft_port": 7101,
      "metadata_port": 7201,
      "store_control_port": 7301,
      "role": "VOTER"
    }
  ],
  "heartbeat_interval_ms": 150,
  "election_timeout_min_ms": 500,
  "election_timeout_max_ms": 900,
  "rpc_timeout_ms": 1000,
  "queue_capacity": 0,
  "worker_count": 4,
  "max_message_size": 0,
  "log_batch_size": 16,
  "data_directory": "data",
  "snapshot_directory": "snapshots",
  "store_heartbeat_timeout_ms": 3000,
  "failure_detection_interval_ms": 1000,
  "task_poll_limit": 8,
  "log_level": "INFO"
})");

      Config config;
      const common::Status status = ConfigLoader::LoadFromFile(path, &config);
      EXPECT_EQ(status.code(), common::StatusCode::kInvalidArgument);
      EXPECT_NE(status.message().find("queue_capacity"), std::string::npos);
    }

    TEST(ConfigTest, RejectsMissingFieldWrongTypeAndInvalidJson)
    {
      TempDir dir;
      const std::filesystem::path missing_path = WriteConfigFile(
          dir,
          "missing.json",
          R"({"node_id":"node-1"})");
      const std::filesystem::path wrong_type_path = WriteConfigFile(
          dir,
          "wrong-type.json",
          R"({
  "node_id": "node-1",
  "ip_address": 123,
  "raft_port": 7101,
  "metadata_port": 7201,
  "store_control_port": 7301,
  "initial_members": [],
  "heartbeat_interval_ms": 150,
  "election_timeout_min_ms": 500,
  "election_timeout_max_ms": 900,
  "rpc_timeout_ms": 1000,
  "queue_capacity": 1,
  "worker_count": 1,
  "max_message_size": 1,
  "log_batch_size": 1,
  "data_directory": "data",
  "snapshot_directory": "snapshots",
  "store_heartbeat_timeout_ms": 1,
  "failure_detection_interval_ms": 1,
  "task_poll_limit": 1,
  "log_level": "INFO"
})");
      const std::filesystem::path invalid_json_path =
          WriteConfigFile(dir, "invalid-json.json", R"({"node_id":)");

      Config config;
      EXPECT_EQ(ConfigLoader::LoadFromFile(missing_path, &config).code(),
                common::StatusCode::kInvalidArgument);
      EXPECT_EQ(ConfigLoader::LoadFromFile(wrong_type_path, &config).code(),
                common::StatusCode::kInvalidArgument);
      EXPECT_EQ(ConfigLoader::LoadFromFile(invalid_json_path, &config).code(),
                common::StatusCode::kInvalidArgument);
    }

  } // namespace
} // namespace cpr::config
