#include <filesystem>
#include <memory>
#include <string>

#include <gtest/gtest.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "config/config.h"
#include "metadata.grpc.pb.h"
#include "raft/file_raft_storage.h"
#include "server/server.h"

namespace cpr::tests
{
    namespace
    {

        class TempDir
        {
        public:
            explicit TempDir(const char *prefix)
            {
                path_ = std::filesystem::temp_directory_path() /
                        std::filesystem::path(prefix + std::string("-") +
                                              std::to_string(counter_++));
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
            inline static int counter_ = 0;
        };

        std::uint16_t ReservePort()
        {
            class DummyService final : public cpr::metadata::v1::MetadataService::Service
            {
            };

            grpc::ServerBuilder builder;
            std::unique_ptr<grpc::Server> server;
            int port = 0;
            builder.AddListeningPort("127.0.0.1:0",
                                     grpc::InsecureServerCredentials(),
                                     &port);
            DummyService service;
            builder.RegisterService(&service);
            server = builder.BuildAndStart();
            EXPECT_NE(server, nullptr);
            server->Shutdown();
            server->Wait();
            return static_cast<std::uint16_t>(port);
        }

        config::Config MakeConfig(const std::filesystem::path &base)
        {
            config::Config config;
            config.node_id = "node-1";
            config.ip_address = "127.0.0.1";
            config.raft_port = ReservePort();
            config.metadata_port = ReservePort();
            config.store_control_port = ReservePort();
            config.heartbeat_interval_ms = 50;
            config.election_timeout_min_ms = 150;
            config.election_timeout_max_ms = 250;
            config.rpc_timeout_ms = 200;
            config.queue_capacity = 32;
            config.worker_count = 1;
            config.max_message_size = 1024 * 1024;
            config.log_batch_size = 64;
            config.data_directory = base / "data";
            config.snapshot_directory = base / "snapshots";
            config.store_heartbeat_timeout_ms = 500;
            config.failure_detection_interval_ms = 100;
            config.task_poll_limit = 8;
            config.initial_members.push_back(
                {"node-1",
                 "127.0.0.1",
                 config.raft_port,
                 config.metadata_port,
                 config.store_control_port,
                 config::MemberRole::kVoter});
            return config;
        }

        TEST(ServerLifecycleTest, StartAndStopAreSafeForFreshSingleNode)
        {
            TempDir dir("cpr-server-fresh");
            server::ServerApplication app;
            const config::Config config = MakeConfig(dir.path());

            ASSERT_TRUE(app.Initialize(config).ok());
            ASSERT_TRUE(app.Start().ok());
            EXPECT_TRUE(app.IsRunning());
            EXPECT_EQ(app.metadata_endpoint(),
                      "127.0.0.1:" + std::to_string(config.metadata_port));
            EXPECT_TRUE(app.Stop().ok());
            EXPECT_TRUE(app.Stop().ok());
        }

        TEST(ServerLifecycleTest, PersistedStateCurrentlyFailsFastAtInitialize)
        {
            TempDir dir("cpr-server-persisted");
            const config::Config config = MakeConfig(dir.path());

            raft::FileRaftStorage storage;
            ASSERT_TRUE(storage.Open(config.data_directory).ok());
            raft::HardState hard_state;
            hard_state.current_term = 1;
            ASSERT_TRUE(storage.SaveHardState(hard_state).ok());

            server::ServerApplication app;
            const common::Status status = app.Initialize(config);
            EXPECT_EQ(status.code(), common::StatusCode::kRetryLater);
        }

    } // namespace
} // namespace cpr::tests
