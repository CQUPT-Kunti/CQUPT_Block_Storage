#include "raftctl/raftctl_command.h"

#include <chrono>
#include <map>
#include <ostream>
#include <string>
#include <string_view>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "metadata.grpc.pb.h"
#include "tool_common.h"

namespace cpr::tools::raftctl
{
    namespace
    {

        constexpr std::uint64_t kDefaultTimeoutMs = 1000;

        struct Options
        {
            std::map<std::string, std::string> values;
        };

        void PrintHelp(std::ostream &out)
        {
            out << "Usage: raftctl <command> --endpoint HOST:PORT [options]\n"
                << "Commands:\n"
                << "  leader\n"
                << "  status [--peer-progress] [--membership]\n"
                << "  add-learner --request-id ID --node-id N --host HOST --port P [--timeout-ms N]\n"
                << "  promote-learner --request-id ID --node-id N [--timeout-ms N]\n"
                << "  remove-member --request-id ID --node-id N [--timeout-ms N] --yes\n";
        }

        common::Status ParseOptions(int argc, const char *const argv[], Options *options)
        {
            if (options == nullptr)
            {
                return common::Status::InvalidArgument("options output pointer is null");
            }
            for (int i = 2; i < argc; ++i)
            {
                std::string key = argv[i];
                if (key.rfind("--", 0) != 0)
                {
                    return common::Status::InvalidArgument("unexpected positional argument: " + key);
                }
                if (key == "--peer-progress" || key == "--membership" || key == "--yes")
                {
                    if (!options->values.emplace(key, "true").second)
                    {
                        return common::Status::InvalidArgument("duplicate option: " + key);
                    }
                    continue;
                }
                if (i + 1 >= argc)
                {
                    return common::Status::InvalidArgument("missing value for option: " + key);
                }
                if (!options->values.emplace(std::move(key), argv[++i]).second)
                {
                    return common::Status::InvalidArgument("duplicate option");
                }
            }
            return common::Status::OK();
        }

        common::Status Required(const Options &options,
                                const std::string &key,
                                std::string *value)
        {
            const auto it = options.values.find(key);
            if (it == options.values.end() || it->second.empty())
            {
                return common::Status::InvalidArgument("missing required option " + key);
            }
            *value = it->second;
            return common::Status::OK();
        }

        common::Status ParseNodeId(const Options &options, common::NodeId *node_id)
        {
            std::string text;
            common::Status status = Required(options, "--node-id", &text);
            if (!status.ok())
            {
                return status;
            }
            std::uint64_t value = 0;
            status = tools::ParseU64(text, "node-id", &value);
            if (!status.ok())
            {
                return status;
            }
            if (value == common::kInvalidNodeId)
            {
                return common::Status::InvalidArgument("node-id must be positive");
            }
            *node_id = value;
            return common::Status::OK();
        }

        std::uint64_t OptionalTimeout(const Options &options)
        {
            const auto it = options.values.find("--timeout-ms");
            if (it == options.values.end())
            {
                return kDefaultTimeoutMs;
            }
            std::uint64_t value = 0;
            if (!tools::ParseU64(it->second, "timeout-ms", &value).ok())
            {
                return 0;
            }
            return value;
        }

        void SetDeadline(grpc::ClientContext *context, std::uint64_t timeout_ms)
        {
            context->set_deadline(std::chrono::system_clock::now() +
                                  tools::TimeoutOrDefault(timeout_ms, std::chrono::milliseconds(kDefaultTimeoutMs)));
        }

        bool BusinessOk(const cpr::common::v1::RpcStatus &status)
        {
            return status.code() == cpr::common::v1::RPC_STATUS_CODE_OK;
        }

        void PrintRpcStatus(std::ostream &out, const cpr::common::v1::RpcStatus &status)
        {
            out << "status=" << cpr::common::v1::RpcStatusCode_Name(status.code());
            if (!status.message().empty())
            {
                out << " message=\"" << status.message() << "\"";
            }
            out << '\n';
        }

        void PrintLeader(std::ostream &out, const cpr::common::v1::LeaderHint &leader)
        {
            out << "leader_id=" << leader.leader_id();
            if (!leader.address().host().empty())
            {
                out << " leader_address=" << leader.address().host()
                    << ':' << leader.address().port();
            }
            out << '\n';
        }

        int RpcExit(const grpc::Status &status, std::ostream &err)
        {
            if (status.ok())
            {
                return tools::kExitOk;
            }
            err << "grpc_status=" << status.error_code()
                << " message=\"" << status.error_message() << "\"\n";
            return tools::kExitRpc;
        }

        int BusinessExit(const cpr::common::v1::RpcStatus &status)
        {
            return BusinessOk(status) ? tools::kExitOk : tools::kExitBusiness;
        }

    } // namespace

    int RunRaftCtl(int argc,
                   const char *const argv[],
                   std::ostream &out,
                   std::ostream &err)
    {
        if (argc < 2 || HasHelp(argc, argv))
        {
            PrintHelp(out);
            return argc < 2 ? tools::kExitUsage : tools::kExitOk;
        }

        Options options;
        common::Status status = ParseOptions(argc, argv, &options);
        if (!status.ok())
        {
            tools::PrintStatus(err, status);
            return tools::kExitUsage;
        }
        std::string endpoint;
        status = Required(options, "--endpoint", &endpoint);
        if (!status.ok())
        {
            tools::PrintStatus(err, status);
            return tools::kExitUsage;
        }

        auto stub = cpr::metadata::v1::MetadataService::NewStub(
            grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));
        const std::string command = argv[1];

        if (command == "leader")
        {
            cpr::metadata::v1::GetLeaderRequest request;
            cpr::metadata::v1::GetLeaderResponse response;
            grpc::ClientContext context;
            SetDeadline(&context, OptionalTimeout(options));
            grpc::Status rpc = stub->GetLeader(&context, request, &response);
            int rc = RpcExit(rpc, err);
            if (rc != tools::kExitOk)
            {
                return rc;
            }
            PrintRpcStatus(out, response.status());
            PrintLeader(out, response.leader());
            return BusinessExit(response.status());
        }

        if (command == "status")
        {
            cpr::metadata::v1::GetStatusRequest request;
            request.set_include_peer_progress(options.values.count("--peer-progress") != 0);
            request.set_include_membership(options.values.count("--membership") != 0);
            cpr::metadata::v1::GetStatusResponse response;
            grpc::ClientContext context;
            SetDeadline(&context, OptionalTimeout(options));
            grpc::Status rpc = stub->GetStatus(&context, request, &response);
            int rc = RpcExit(rpc, err);
            if (rc != tools::kExitOk)
            {
                return rc;
            }
            PrintRpcStatus(out, response.status());
            out << "node_id=" << response.node().node_id()
                << " role=" << cpr::common::v1::ClusterRole_Name(response.node().role())
                << " state=" << cpr::common::v1::NodeLifecycleState_Name(response.node().state())
                << " term=" << response.node().current_term()
                << " commit_index=" << response.node().commit_index() << '\n';
            PrintLeader(out, response.node().leader());
            return BusinessExit(response.status());
        }

        if (command == "add-learner")
        {
            common::NodeId node_id = 0;
            std::string request_id, host, port;
            if (!(status = Required(options, "--request-id", &request_id)).ok() ||
                !(status = ParseNodeId(options, &node_id)).ok() ||
                !(status = Required(options, "--host", &host)).ok() ||
                !(status = Required(options, "--port", &port)).ok())
            {
                tools::PrintStatus(err, status);
                return tools::kExitUsage;
            }
            ParsedAddress address;
            status = tools::ParseAddress(host, port, &address);
            if (!status.ok())
            {
                tools::PrintStatus(err, status);
                return tools::kExitUsage;
            }
            cpr::metadata::v1::AddLearnerRequest request;
            request.set_request_id(request_id);
            request.set_timeout_ms(OptionalTimeout(options));
            request.mutable_learner()->set_node_id(node_id);
            request.mutable_learner()->set_is_learner(true);
            request.mutable_learner()->mutable_address()->set_host(address.host);
            request.mutable_learner()->mutable_address()->set_port(address.port);
            cpr::metadata::v1::MembershipChangeResponse response;
            grpc::ClientContext context;
            SetDeadline(&context, request.timeout_ms());
            grpc::Status rpc = stub->AddLearner(&context, request, &response);
            int rc = RpcExit(rpc, err);
            if (rc != tools::kExitOk) return rc;
            PrintRpcStatus(out, response.status());
            out << "log_index=" << response.log_index()
                << " term=" << response.term() << '\n';
            PrintLeader(out, response.leader());
            return BusinessExit(response.status());
        }

        if (command == "promote-learner" || command == "remove-member")
        {
            if (command == "remove-member" && options.values.count("--yes") == 0)
            {
                tools::PrintStatus(err, common::Status::InvalidArgument("remove-member requires --yes"));
                return tools::kExitUsage;
            }
            common::NodeId node_id = 0;
            std::string request_id;
            if (!(status = Required(options, "--request-id", &request_id)).ok() ||
                !(status = ParseNodeId(options, &node_id)).ok())
            {
                tools::PrintStatus(err, status);
                return tools::kExitUsage;
            }
            cpr::metadata::v1::MembershipChangeResponse response;
            grpc::ClientContext context;
            SetDeadline(&context, OptionalTimeout(options));
            grpc::Status rpc;
            if (command == "promote-learner")
            {
                cpr::metadata::v1::PromoteLearnerRequest request;
                request.set_request_id(request_id);
                request.set_node_id(node_id);
                request.set_timeout_ms(OptionalTimeout(options));
                rpc = stub->PromoteLearner(&context, request, &response);
            }
            else
            {
                cpr::metadata::v1::RemoveMemberRequest request;
                request.set_request_id(request_id);
                request.set_node_id(node_id);
                request.set_timeout_ms(OptionalTimeout(options));
                rpc = stub->RemoveMember(&context, request, &response);
            }
            int rc = RpcExit(rpc, err);
            if (rc != tools::kExitOk) return rc;
            PrintRpcStatus(out, response.status());
            out << "log_index=" << response.log_index()
                << " term=" << response.term() << '\n';
            PrintLeader(out, response.leader());
            return BusinessExit(response.status());
        }

        tools::PrintStatus(err, common::Status::InvalidArgument("unknown command: " + command));
        return tools::kExitUsage;
    }

} // namespace cpr::tools::raftctl
