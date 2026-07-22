#include "client/client_command.h"

#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "metadata.grpc.pb.h"
#include "store.grpc.pb.h"
#include "tool_common.h"

namespace
{

    class FakeMetadata final : public cpr::metadata::v1::MetadataService::Service
    {
    public:
        grpc::Status Propose(grpc::ServerContext *context,
                             const cpr::metadata::v1::ProposeRequest *request,
                             cpr::metadata::v1::ProposeResponse *response) override
        {
            saw_deadline = context->deadline() != std::chrono::system_clock::time_point::max();
            last_request_id = request->request_id();
            last_payload = request->command_payload();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->set_log_index(7);
            response->set_term(3);
            return grpc::Status::OK;
        }

        grpc::Status Query(grpc::ServerContext *,
                           const cpr::metadata::v1::QueryRequest *request,
                           cpr::metadata::v1::QueryResponse *response) override
        {
            last_request_id = request->request_id();
            last_payload = request->query_payload();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->set_result_payload("result");
            response->set_term(4);
            return grpc::Status::OK;
        }

        grpc::Status GetLeader(grpc::ServerContext *,
                               const cpr::metadata::v1::GetLeaderRequest *,
                               cpr::metadata::v1::GetLeaderResponse *response) override
        {
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->mutable_leader()->set_leader_id(2);
            response->mutable_leader()->mutable_address()->set_host("127.0.0.1");
            response->mutable_leader()->mutable_address()->set_port(9102);
            return grpc::Status::OK;
        }

        grpc::Status GetStatus(grpc::ServerContext *,
                               const cpr::metadata::v1::GetStatusRequest *request,
                               cpr::metadata::v1::GetStatusResponse *response) override
        {
            include_membership = request->include_membership();
            include_peers = request->include_peer_progress();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->mutable_node()->set_node_id(1);
            response->mutable_node()->set_role(cpr::common::v1::CLUSTER_ROLE_LEADER);
            response->mutable_node()->set_current_term(5);
            response->mutable_node()->set_commit_index(6);
            return grpc::Status::OK;
        }

        std::string last_request_id;
        std::string last_payload;
        bool include_membership = false;
        bool include_peers = false;
        bool saw_deadline = false;
    };

    class FakeStore final : public cpr::store::v1::StoreControlService::Service
    {
    public:
        grpc::Status Register(grpc::ServerContext *,
                              const cpr::store::v1::RegisterStoreRequest *request,
                              cpr::store::v1::StoreCommandResponse *response) override
        {
            last_request_id = request->request_id();
            last_store_id = request->store().store_id();
            last_host = request->store().address().host();
            last_port = request->store().address().port();
            last_capacity = request->store().total_capacity_bytes();
            last_used = request->store().used_capacity_bytes();
            response->mutable_status()->set_code(register_status);
            response->set_log_index(9);
            *response->mutable_store() = request->store();
            return grpc::Status::OK;
        }

        grpc::Status Heartbeat(grpc::ServerContext *,
                               const cpr::store::v1::StoreHeartbeatRequest *request,
                               cpr::store::v1::StoreHeartbeatResponse *response) override
        {
            last_store_id = request->store_id();
            last_generation = request->generation();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            return grpc::Status::OK;
        }

        grpc::Status Stop(grpc::ServerContext *,
                          const cpr::store::v1::StoreStopRequest *request,
                          cpr::store::v1::StoreCommandResponse *response) override
        {
            last_request_id = request->request_id();
            last_store_id = request->store_id();
            last_generation = request->generation();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->set_log_index(10);
            return grpc::Status::OK;
        }

        grpc::Status Remove(grpc::ServerContext *,
                            const cpr::store::v1::StoreRemoveRequest *request,
                            cpr::store::v1::StoreCommandResponse *response) override
        {
            last_request_id = request->request_id();
            last_store_id = request->store_id();
            last_generation = request->generation();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->set_log_index(11);
            return grpc::Status::OK;
        }

        grpc::Status PollTasks(grpc::ServerContext *,
                               const cpr::store::v1::PollTasksRequest *request,
                               cpr::store::v1::PollTasksResponse *response) override
        {
            last_store_id = request->store_id();
            last_generation = request->generation();
            last_max_tasks = request->max_tasks();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            auto *task = response->add_tasks();
            task->set_task_id("task-a");
            task->set_type(cpr::store::v1::TASK_TYPE_CREATE);
            task->set_state(cpr::store::v1::TASK_STATE_RUNNING);
            task->set_sequence(1);
            return grpc::Status::OK;
        }

        grpc::Status ReportTaskResult(grpc::ServerContext *,
                                      const cpr::store::v1::ReportTaskResultRequest *request,
                                      cpr::store::v1::ReportTaskResultResponse *response) override
        {
            last_store_id = request->store_id();
            last_generation = request->generation();
            last_task_id = request->task_id();
            last_task_state = request->final_state();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->mutable_task()->set_task_id(request->task_id());
            return grpc::Status::OK;
        }

        cpr::common::v1::RpcStatusCode register_status = cpr::common::v1::RPC_STATUS_CODE_OK;
        std::string last_request_id;
        std::uint64_t last_store_id = 0;
        std::string last_host;
        std::uint32_t last_port = 0;
        std::uint64_t last_capacity = 0;
        std::uint64_t last_used = 0;
        std::uint64_t last_generation = 0;
        std::uint32_t last_max_tasks = 0;
        std::string last_task_id;
        cpr::store::v1::TaskState last_task_state = cpr::store::v1::TASK_STATE_UNSPECIFIED;
    };

    struct LoopbackServer
    {
        explicit LoopbackServer(grpc::Service *service)
        {
            grpc::ServerBuilder builder;
            builder.AddListeningPort("127.0.0.1:0",
                                     grpc::InsecureServerCredentials(),
                                     &port);
            builder.RegisterService(service);
            server = builder.BuildAndStart();
            endpoint = "127.0.0.1:" + std::to_string(port);
        }

        ~LoopbackServer()
        {
            if (server)
            {
                server->Shutdown();
                server->Wait();
            }
        }

        int port = 0;
        std::string endpoint;
        std::unique_ptr<grpc::Server> server;
    };

    int RunClient(std::initializer_list<const char *> args,
                  std::string *out,
                  std::string *err)
    {
        std::vector<const char *> argv(args);
        std::ostringstream stdout_stream;
        std::ostringstream stderr_stream;
        const int rc = cpr::tools::client::RunClientTool(
            static_cast<int>(argv.size()),
            argv.data(),
            stdout_stream,
            stderr_stream);
        *out = stdout_stream.str();
        *err = stderr_stream.str();
        return rc;
    }

} // namespace

TEST(ClientToolTest, HelpDoesNotAccessNetwork)
{
    std::string out;
    std::string err;
    EXPECT_EQ(0, RunClient({"cpr_client", "--help"}, &out, &err));
    EXPECT_NE(std::string::npos, out.find("metadata-propose"));
}

TEST(ClientToolTest, MetadataCommandsUseGeneratedStubAndDeadline)
{
    FakeMetadata service;
    LoopbackServer server(&service);
    std::string out;
    std::string err;

    EXPECT_EQ(0, RunClient({"cpr_client", "metadata-propose", "--endpoint", server.endpoint.c_str(),
                            "--request-id", "req-1", "--payload", "abc", "--timeout-ms", "100"},
                           &out, &err));
    EXPECT_EQ("req-1", service.last_request_id);
    EXPECT_EQ("abc", service.last_payload);
    EXPECT_TRUE(service.saw_deadline);
    EXPECT_NE(std::string::npos, out.find("log_index=7"));

    EXPECT_EQ(0, RunClient({"cpr_client", "metadata-query", "--endpoint", server.endpoint.c_str(),
                            "--request-id", "req-2", "--payload", "q"},
                           &out, &err));
    EXPECT_EQ("req-2", service.last_request_id);
    EXPECT_EQ("q", service.last_payload);

    EXPECT_EQ(0, RunClient({"cpr_client", "metadata-get-leader", "--endpoint", server.endpoint.c_str()},
                           &out, &err));
    EXPECT_NE(std::string::npos, out.find("leader_id=2"));

    EXPECT_EQ(0, RunClient({"cpr_client", "metadata-get-status", "--endpoint", server.endpoint.c_str(),
                            "--peer-progress", "--membership"},
                           &out, &err));
    EXPECT_TRUE(service.include_membership);
    EXPECT_TRUE(service.include_peers);
}

TEST(ClientToolTest, StoreCommandsMapRequests)
{
    FakeStore service;
    LoopbackServer server(&service);
    std::string out;
    std::string err;

    EXPECT_EQ(0, RunClient({"cpr_client", "store-register", "--endpoint", server.endpoint.c_str(),
                            "--request-id", "s1", "--store-id", "10", "--host", "127.0.0.1",
                            "--port", "9201", "--capacity", "1000", "--used", "7", "--generation", "1"},
                           &out, &err));
    EXPECT_EQ("s1", service.last_request_id);
    EXPECT_EQ(10U, service.last_store_id);
    EXPECT_EQ("127.0.0.1", service.last_host);
    EXPECT_EQ(9201U, service.last_port);
    EXPECT_EQ(1000U, service.last_capacity);
    EXPECT_EQ(7U, service.last_used);

    EXPECT_EQ(0, RunClient({"cpr_client", "store-heartbeat", "--endpoint", server.endpoint.c_str(),
                            "--store-id", "10", "--generation", "3", "--used", "11", "--available", "989"},
                           &out, &err));
    EXPECT_EQ(3U, service.last_generation);

    EXPECT_EQ(0, RunClient({"cpr_client", "store-stop", "--endpoint", server.endpoint.c_str(),
                            "--request-id", "stop", "--store-id", "10", "--generation", "4"},
                           &out, &err));
    EXPECT_EQ("stop", service.last_request_id);

    EXPECT_EQ(0, RunClient({"cpr_client", "store-remove", "--endpoint", server.endpoint.c_str(),
                            "--request-id", "remove", "--store-id", "10", "--generation", "5"},
                           &out, &err));
    EXPECT_EQ("remove", service.last_request_id);

    EXPECT_EQ(0, RunClient({"cpr_client", "store-poll-tasks", "--endpoint", server.endpoint.c_str(),
                            "--store-id", "10", "--generation", "6", "--max-tasks", "2"},
                           &out, &err));
    EXPECT_EQ(2U, service.last_max_tasks);
    EXPECT_NE(std::string::npos, out.find("task_id=task-a"));

    EXPECT_EQ(0, RunClient({"cpr_client", "store-report-task-result", "--endpoint", server.endpoint.c_str(),
                            "--store-id", "10", "--generation", "7", "--task-id", "task-a",
                            "--state", "success", "--result", "ok"},
                           &out, &err));
    EXPECT_EQ("task-a", service.last_task_id);
    EXPECT_EQ(cpr::store::v1::TASK_STATE_SUCCESS, service.last_task_state);
}

TEST(ClientToolTest, InvalidArgumentsDoNotNeedServer)
{
    std::string out;
    std::string err;
    EXPECT_EQ(cpr::tools::kExitUsage,
              RunClient({"cpr_client", "store-register", "--endpoint", "127.0.0.1:1",
                         "--request-id", "bad", "--store-id", "-1"},
                        &out, &err));
    EXPECT_NE(std::string::npos, err.find("store-id"));
}

TEST(ClientToolTest, BusinessFailureDiffersFromGrpcFailure)
{
    FakeStore service;
    service.register_status = cpr::common::v1::RPC_STATUS_CODE_NOT_LEADER;
    LoopbackServer server(&service);
    std::string out;
    std::string err;

    EXPECT_EQ(cpr::tools::kExitBusiness,
              RunClient({"cpr_client", "store-register", "--endpoint", server.endpoint.c_str(),
                         "--request-id", "s1", "--store-id", "10", "--host", "127.0.0.1",
                         "--port", "9201", "--capacity", "1000", "--used", "7"},
                        &out, &err));

    EXPECT_EQ(cpr::tools::kExitRpc,
              RunClient({"cpr_client", "metadata-get-leader", "--endpoint", "127.0.0.1:1",
                         "--timeout-ms", "1"},
                        &out, &err));
}
