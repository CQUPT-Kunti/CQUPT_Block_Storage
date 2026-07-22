#include "raftctl/raftctl_command.h"

#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "metadata.grpc.pb.h"
#include "tool_common.h"

namespace
{

    class FakeMetadata final : public cpr::metadata::v1::MetadataService::Service
    {
    public:
        grpc::Status GetLeader(grpc::ServerContext *,
                               const cpr::metadata::v1::GetLeaderRequest *,
                               cpr::metadata::v1::GetLeaderResponse *response) override
        {
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->mutable_leader()->set_leader_id(1);
            response->mutable_leader()->mutable_address()->set_host("127.0.0.1");
            response->mutable_leader()->mutable_address()->set_port(9001);
            return grpc::Status::OK;
        }

        grpc::Status GetStatus(grpc::ServerContext *,
                               const cpr::metadata::v1::GetStatusRequest *request,
                               cpr::metadata::v1::GetStatusResponse *response) override
        {
            include_membership = request->include_membership();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->mutable_node()->set_node_id(1);
            response->mutable_node()->set_role(cpr::common::v1::CLUSTER_ROLE_LEADER);
            response->mutable_node()->set_state(cpr::common::v1::NODE_LIFECYCLE_STATE_RUNNING);
            response->mutable_node()->set_current_term(8);
            return grpc::Status::OK;
        }

        grpc::Status AddLearner(grpc::ServerContext *,
                                const cpr::metadata::v1::AddLearnerRequest *request,
                                cpr::metadata::v1::MembershipChangeResponse *response) override
        {
            last_kind = "add";
            last_node_id = request->learner().node_id();
            last_host = request->learner().address().host();
            last_port = request->learner().address().port();
            response->mutable_status()->set_code(next_status);
            response->mutable_leader()->set_leader_id(2);
            response->mutable_leader()->mutable_address()->set_host("127.0.0.2");
            response->mutable_leader()->mutable_address()->set_port(9002);
            return grpc::Status::OK;
        }

        grpc::Status PromoteLearner(grpc::ServerContext *,
                                    const cpr::metadata::v1::PromoteLearnerRequest *request,
                                    cpr::metadata::v1::MembershipChangeResponse *response) override
        {
            last_kind = "promote";
            last_node_id = request->node_id();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->set_log_index(12);
            return grpc::Status::OK;
        }

        grpc::Status RemoveMember(grpc::ServerContext *,
                                  const cpr::metadata::v1::RemoveMemberRequest *request,
                                  cpr::metadata::v1::MembershipChangeResponse *response) override
        {
            last_kind = "remove";
            last_node_id = request->node_id();
            response->mutable_status()->set_code(cpr::common::v1::RPC_STATUS_CODE_OK);
            response->set_log_index(13);
            return grpc::Status::OK;
        }

        bool include_membership = false;
        std::string last_kind;
        std::uint64_t last_node_id = 0;
        std::string last_host;
        std::uint32_t last_port = 0;
        cpr::common::v1::RpcStatusCode next_status = cpr::common::v1::RPC_STATUS_CODE_OK;
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

    int RunRaftCtl(std::initializer_list<const char *> args,
                   std::string *out,
                   std::string *err)
    {
        std::vector<const char *> argv(args);
        std::ostringstream stdout_stream;
        std::ostringstream stderr_stream;
        const int rc = cpr::tools::raftctl::RunRaftCtl(
            static_cast<int>(argv.size()),
            argv.data(),
            stdout_stream,
            stderr_stream);
        *out = stdout_stream.str();
        *err = stderr_stream.str();
        return rc;
    }

} // namespace

TEST(RaftCtlTest, HelpDoesNotAccessNetwork)
{
    std::string out;
    std::string err;
    EXPECT_EQ(0, RunRaftCtl({"raftctl", "--help"}, &out, &err));
    EXPECT_NE(std::string::npos, out.find("add-learner"));
}

TEST(RaftCtlTest, LeaderAndStatusUseMetadataRpc)
{
    FakeMetadata service;
    LoopbackServer server(&service);
    std::string out;
    std::string err;

    EXPECT_EQ(0, RunRaftCtl({"raftctl", "leader", "--endpoint", server.endpoint.c_str()},
                            &out, &err));
    EXPECT_NE(std::string::npos, out.find("leader_id=1"));

    EXPECT_EQ(0, RunRaftCtl({"raftctl", "status", "--endpoint", server.endpoint.c_str(),
                             "--membership"},
                            &out, &err));
    EXPECT_TRUE(service.include_membership);
    EXPECT_NE(std::string::npos, out.find("role=CLUSTER_ROLE_LEADER"));
}

TEST(RaftCtlTest, MembershipCommandsMapRequestsWithoutLocalQuorumLogic)
{
    FakeMetadata service;
    LoopbackServer server(&service);
    std::string out;
    std::string err;

    EXPECT_EQ(0, RunRaftCtl({"raftctl", "add-learner", "--endpoint", server.endpoint.c_str(),
                             "--request-id", "add", "--node-id", "4", "--host", "127.0.0.4",
                             "--port", "9004"},
                            &out, &err));
    EXPECT_EQ("add", service.last_kind);
    EXPECT_EQ(4U, service.last_node_id);
    EXPECT_EQ("127.0.0.4", service.last_host);
    EXPECT_EQ(9004U, service.last_port);

    EXPECT_EQ(0, RunRaftCtl({"raftctl", "promote-learner", "--endpoint", server.endpoint.c_str(),
                             "--request-id", "promote", "--node-id", "4"},
                            &out, &err));
    EXPECT_EQ("promote", service.last_kind);

    EXPECT_EQ(0, RunRaftCtl({"raftctl", "remove-member", "--endpoint", server.endpoint.c_str(),
                             "--request-id", "remove", "--node-id", "4", "--yes"},
                            &out, &err));
    EXPECT_EQ("remove", service.last_kind);
}

TEST(RaftCtlTest, RejectsInvalidNodeAndRequiresExplicitRemoveConfirmation)
{
    std::string out;
    std::string err;
    EXPECT_EQ(cpr::tools::kExitUsage,
              RunRaftCtl({"raftctl", "add-learner", "--endpoint", "127.0.0.1:1",
                          "--request-id", "bad", "--node-id", "0", "--host", "h", "--port", "1"},
                         &out, &err));
    EXPECT_NE(std::string::npos, err.find("node-id"));

    EXPECT_EQ(cpr::tools::kExitUsage,
              RunRaftCtl({"raftctl", "remove-member", "--endpoint", "127.0.0.1:1",
                          "--request-id", "remove", "--node-id", "1"},
                         &out, &err));
    EXPECT_NE(std::string::npos, err.find("--yes"));
}

TEST(RaftCtlTest, NotLeaderBusinessResponseKeepsLeaderHint)
{
    FakeMetadata service;
    service.next_status = cpr::common::v1::RPC_STATUS_CODE_NOT_LEADER;
    LoopbackServer server(&service);
    std::string out;
    std::string err;

    EXPECT_EQ(cpr::tools::kExitBusiness,
              RunRaftCtl({"raftctl", "add-learner", "--endpoint", server.endpoint.c_str(),
                          "--request-id", "add", "--node-id", "4", "--host", "127.0.0.4",
                          "--port", "9004"},
                         &out, &err));
    EXPECT_NE(std::string::npos, out.find("RPC_STATUS_CODE_NOT_LEADER"));
    EXPECT_NE(std::string::npos, out.find("leader_id=2"));
}
