#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "config/config.h"
#include "metadata.grpc.pb.h"
#include "metadata/metadata_command.h"
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
            config.rpc_timeout_ms = 300;
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

        metadata::MetadataCommand MakeCommand()
        {
            metadata::MetadataCommand command;
            command.type = metadata::MetadataCommandType::OPAQUE_OPERATION;
            command.command_id = "cmd-1";
            command.target_id = "alpha";
            command.payload = {0x21, 0x22};
            return command;
        }

        bool WaitUntilLeader(cpr::metadata::v1::MetadataService::Stub *stub)
        {
            for (int attempt = 0; attempt < 30; ++attempt)
            {
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() +
                                     std::chrono::milliseconds(200));
                cpr::metadata::v1::GetStatusRequest request;
                cpr::metadata::v1::GetStatusResponse response;
                grpc::Status status = stub->GetStatus(&context, request, &response);
                if (status.ok() &&
                    response.status().code() ==
                        cpr::common::v1::RPC_STATUS_CODE_OK &&
                    response.node().role() ==
                        cpr::common::v1::CLUSTER_ROLE_LEADER)
                {
                    return true;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            return false;
        }

        TEST(ServerCompositionTest, SingleNodeServerServesMetadataRpc)
        {
            TempDir dir("cpr-server-compose");
            server::ServerApplication app;
            const config::Config config = MakeConfig(dir.path());

            ASSERT_TRUE(app.Initialize(config).ok());
            ASSERT_TRUE(app.Start().ok());

            auto channel = grpc::CreateChannel(app.metadata_endpoint(),
                                               grpc::InsecureChannelCredentials());
            auto stub = cpr::metadata::v1::MetadataService::NewStub(channel);
            ASSERT_TRUE(WaitUntilLeader(stub.get()));

            metadata::MetadataCommand command = MakeCommand();
            raft::OpaquePayload encoded;
            ASSERT_TRUE(metadata::EncodeMetadataCommand(command, &encoded).ok());

            grpc::ClientContext propose_context;
            propose_context.set_deadline(std::chrono::system_clock::now() +
                                         std::chrono::milliseconds(500));
            cpr::metadata::v1::ProposeRequest propose_request;
            propose_request.set_request_id("req-1");
            propose_request.set_command_payload(encoded.data(), encoded.size());
            propose_request.set_timeout_ms(500);
            cpr::metadata::v1::ProposeResponse propose_response;
            grpc::Status status =
                stub->Propose(&propose_context, propose_request, &propose_response);
            ASSERT_TRUE(status.ok());
            EXPECT_EQ(cpr::common::v1::RPC_STATUS_CODE_OK,
                      propose_response.status().code());

            grpc::ClientContext query_context;
            query_context.set_deadline(std::chrono::system_clock::now() +
                                       std::chrono::milliseconds(500));
            cpr::metadata::v1::QueryRequest query_request;
            query_request.set_request_id("q-1");
            query_request.set_query_payload("alpha");
            cpr::metadata::v1::QueryResponse query_response;
            status = stub->Query(&query_context, query_request, &query_response);
            ASSERT_TRUE(status.ok());
            EXPECT_EQ(cpr::common::v1::RPC_STATUS_CODE_OK,
                      query_response.status().code());
            EXPECT_EQ(std::string("\x21\x22", 2), query_response.result_payload());

            EXPECT_TRUE(app.Stop().ok());
        }

    } // namespace
} // namespace cpr::tests
