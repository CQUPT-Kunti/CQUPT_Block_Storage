#include "client/client_command.h"

#include <chrono>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "metadata.grpc.pb.h"
#include "store.grpc.pb.h"
#include "tool_common.h"

namespace cpr::tools::client
{
    namespace
    {

        constexpr std::size_t kMaxPayloadBytes = 4 * 1024 * 1024;
        constexpr std::uint64_t kDefaultTimeoutMs = 1000;

        struct Options
        {
            std::map<std::string, std::string> values;
        };

        void PrintHelp(std::ostream &out)
        {
            out << "Usage: cpr_client <command> --endpoint HOST:PORT [options]\n"
                << "Commands:\n"
                << "  metadata-propose --request-id ID (--payload TEXT|--payload-file PATH) [--timeout-ms N]\n"
                << "  metadata-query --request-id ID (--payload TEXT|--payload-file PATH)\n"
                << "  metadata-get-leader\n"
                << "  metadata-get-status [--peer-progress] [--membership]\n"
                << "  store-register --request-id ID --store-id N --host HOST --port P --capacity N --used N [--generation N]\n"
                << "  store-heartbeat --store-id N --generation N --used N --available N\n"
                << "  store-stop --request-id ID --store-id N --generation N [--timeout-ms N]\n"
                << "  store-remove --request-id ID --store-id N --generation N [--timeout-ms N]\n"
                << "  store-poll-tasks --store-id N --generation N --max-tasks N\n"
                << "  store-report-task-result --store-id N --generation N --task-id ID --state success|failed [--result TEXT]\n";
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
                if (key == "--peer-progress" || key == "--membership")
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

        common::Status PayloadFromOptions(const Options &options, std::string *payload)
        {
            const bool has_payload = options.values.count("--payload") != 0;
            const bool has_file = options.values.count("--payload-file") != 0;
            if (has_payload == has_file)
            {
                return common::Status::InvalidArgument("specify exactly one of --payload or --payload-file");
            }
            if (has_payload)
            {
                *payload = options.values.at("--payload");
                if (payload->size() > kMaxPayloadBytes)
                {
                    return common::Status::ResourceExhausted("payload is too large");
                }
                return common::Status::OK();
            }
            return tools::ReadPayloadFile(options.values.at("--payload-file"),
                                          kMaxPayloadBytes,
                                          payload);
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

        cpr::store::v1::StoreState ParseStoreState(std::string_view value)
        {
            if (value == "stopped")
            {
                return cpr::store::v1::STORE_STATE_STOPPED;
            }
            if (value == "failed")
            {
                return cpr::store::v1::STORE_STATE_FAILED;
            }
            return cpr::store::v1::STORE_STATE_RUNNING;
        }

        common::Status ParseTaskState(std::string_view value,
                                      cpr::store::v1::TaskState *state)
        {
            if (state == nullptr)
            {
                return common::Status::InvalidArgument("task state output pointer is null");
            }
            if (value == "success")
            {
                *state = cpr::store::v1::TASK_STATE_SUCCESS;
                return common::Status::OK();
            }
            if (value == "failed")
            {
                *state = cpr::store::v1::TASK_STATE_FAILED;
                return common::Status::OK();
            }
            return common::Status::InvalidArgument("task final state must be success or failed");
        }

        int MetadataCommand(const std::string &command,
                            const Options &options,
                            std::ostream &out,
                            std::ostream &err)
        {
            std::string endpoint;
            common::Status status = Required(options, "--endpoint", &endpoint);
            if (!status.ok())
            {
                tools::PrintStatus(err, status);
                return tools::kExitUsage;
            }
            auto stub = cpr::metadata::v1::MetadataService::NewStub(
                grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));

            if (command == "metadata-get-leader")
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

            if (command == "metadata-get-status")
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
                    << " term=" << response.node().current_term()
                    << " commit_index=" << response.node().commit_index() << '\n';
                return BusinessExit(response.status());
            }

            std::string request_id;
            status = Required(options, "--request-id", &request_id);
            if (!status.ok())
            {
                tools::PrintStatus(err, status);
                return tools::kExitUsage;
            }
            std::string payload;
            status = PayloadFromOptions(options, &payload);
            if (!status.ok())
            {
                tools::PrintStatus(err, status);
                return tools::kExitUsage;
            }

            if (command == "metadata-propose")
            {
                cpr::metadata::v1::ProposeRequest request;
                request.set_request_id(request_id);
                request.set_command_payload(payload);
                request.set_timeout_ms(OptionalTimeout(options));
                cpr::metadata::v1::ProposeResponse response;
                grpc::ClientContext context;
                SetDeadline(&context, request.timeout_ms());
                grpc::Status rpc = stub->Propose(&context, request, &response);
                int rc = RpcExit(rpc, err);
                if (rc != tools::kExitOk)
                {
                    return rc;
                }
                PrintRpcStatus(out, response.status());
                out << "log_index=" << response.log_index()
                    << " term=" << response.term() << '\n';
                PrintLeader(out, response.leader());
                return BusinessExit(response.status());
            }

            if (command == "metadata-query")
            {
                cpr::metadata::v1::QueryRequest request;
                request.set_request_id(request_id);
                request.set_query_payload(payload);
                cpr::metadata::v1::QueryResponse response;
                grpc::ClientContext context;
                SetDeadline(&context, OptionalTimeout(options));
                grpc::Status rpc = stub->Query(&context, request, &response);
                int rc = RpcExit(rpc, err);
                if (rc != tools::kExitOk)
                {
                    return rc;
                }
                PrintRpcStatus(out, response.status());
                out << "result_payload_length=" << response.result_payload().size()
                    << " term=" << response.term() << '\n';
                PrintLeader(out, response.leader());
                return BusinessExit(response.status());
            }

            tools::PrintStatus(err, common::Status::InvalidArgument("unknown metadata command"));
            return tools::kExitUsage;
        }

        int StoreCommand(const std::string &command,
                         const Options &options,
                         std::ostream &out,
                         std::ostream &err)
        {
            std::string endpoint;
            common::Status status = Required(options, "--endpoint", &endpoint);
            if (!status.ok())
            {
                tools::PrintStatus(err, status);
                return tools::kExitUsage;
            }
            auto stub = cpr::store::v1::StoreControlService::NewStub(
                grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));

            std::uint64_t store_id = 0;
            std::string text;
            status = Required(options, "--store-id", &text);
            if (!status.ok() || !(status = tools::ParseU64(text, "store-id", &store_id)).ok() ||
                store_id == 0)
            {
                tools::PrintStatus(err, status.ok() ? common::Status::InvalidArgument("store-id must be positive") : status);
                return tools::kExitUsage;
            }

            if (command == "store-register")
            {
                std::string request_id;
                std::string host;
                std::string port;
                std::string capacity;
                std::string used;
                if (!(status = Required(options, "--request-id", &request_id)).ok() ||
                    !(status = Required(options, "--host", &host)).ok() ||
                    !(status = Required(options, "--port", &port)).ok() ||
                    !(status = Required(options, "--capacity", &capacity)).ok() ||
                    !(status = Required(options, "--used", &used)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                ParsedAddress address;
                std::uint64_t capacity_value = 0;
                std::uint64_t used_value = 0;
                std::uint64_t generation = 1;
                if (!(status = tools::ParseAddress(host, port, &address)).ok() ||
                    !(status = tools::ParseU64(capacity, "capacity", &capacity_value)).ok() ||
                    !(status = tools::ParseU64(used, "used", &used_value)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                const auto generation_it = options.values.find("--generation");
                if (generation_it != options.values.end() &&
                    !(status = tools::ParseU64(generation_it->second, "generation", &generation)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                cpr::store::v1::RegisterStoreRequest request;
                request.set_request_id(request_id);
                request.set_timeout_ms(OptionalTimeout(options));
                request.mutable_store()->set_store_id(store_id);
                request.mutable_store()->mutable_address()->set_host(address.host);
                request.mutable_store()->mutable_address()->set_port(address.port);
                request.mutable_store()->set_total_capacity_bytes(capacity_value);
                request.mutable_store()->set_used_capacity_bytes(used_value);
                request.mutable_store()->set_generation(generation);
                request.mutable_store()->set_state(ParseStoreState(options.values.count("--state") ? options.values.at("--state") : "running"));
                cpr::store::v1::StoreCommandResponse response;
                grpc::ClientContext context;
                SetDeadline(&context, request.timeout_ms());
                grpc::Status rpc = stub->Register(&context, request, &response);
                int rc = RpcExit(rpc, err);
                if (rc != tools::kExitOk) return rc;
                PrintRpcStatus(out, response.status());
                out << "log_index=" << response.log_index()
                    << " store_id=" << response.store().store_id()
                    << " generation=" << response.store().generation() << '\n';
                PrintLeader(out, response.leader());
                return BusinessExit(response.status());
            }

            if (command == "store-heartbeat")
            {
                std::string generation_text, used_text, available_text;
                if (!(status = Required(options, "--generation", &generation_text)).ok() ||
                    !(status = Required(options, "--used", &used_text)).ok() ||
                    !(status = Required(options, "--available", &available_text)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                std::uint64_t generation = 0, used = 0, available = 0;
                if (!(status = tools::ParseU64(generation_text, "generation", &generation)).ok() ||
                    !(status = tools::ParseU64(used_text, "used", &used)).ok() ||
                    !(status = tools::ParseU64(available_text, "available", &available)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                cpr::store::v1::StoreHeartbeatRequest request;
                request.set_store_id(store_id);
                request.set_generation(generation);
                request.set_used_capacity_bytes(used);
                request.set_available_capacity_bytes(available);
                cpr::store::v1::StoreHeartbeatResponse response;
                grpc::ClientContext context;
                SetDeadline(&context, OptionalTimeout(options));
                grpc::Status rpc = stub->Heartbeat(&context, request, &response);
                int rc = RpcExit(rpc, err);
                if (rc != tools::kExitOk) return rc;
                PrintRpcStatus(out, response.status());
                PrintLeader(out, response.leader());
                return BusinessExit(response.status());
            }

            if (command == "store-stop" || command == "store-remove")
            {
                std::string request_id, generation_text;
                if (!(status = Required(options, "--request-id", &request_id)).ok() ||
                    !(status = Required(options, "--generation", &generation_text)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                std::uint64_t generation = 0;
                if (!(status = tools::ParseU64(generation_text, "generation", &generation)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                cpr::store::v1::StoreCommandResponse response;
                grpc::ClientContext context;
                SetDeadline(&context, OptionalTimeout(options));
                grpc::Status rpc;
                if (command == "store-stop")
                {
                    cpr::store::v1::StoreStopRequest request;
                    request.set_request_id(request_id);
                    request.set_store_id(store_id);
                    request.set_generation(generation);
                    request.set_timeout_ms(OptionalTimeout(options));
                    rpc = stub->Stop(&context, request, &response);
                }
                else
                {
                    cpr::store::v1::StoreRemoveRequest request;
                    request.set_request_id(request_id);
                    request.set_store_id(store_id);
                    request.set_generation(generation);
                    request.set_timeout_ms(OptionalTimeout(options));
                    rpc = stub->Remove(&context, request, &response);
                }
                int rc = RpcExit(rpc, err);
                if (rc != tools::kExitOk) return rc;
                PrintRpcStatus(out, response.status());
                out << "log_index=" << response.log_index() << '\n';
                PrintLeader(out, response.leader());
                return BusinessExit(response.status());
            }

            if (command == "store-poll-tasks")
            {
                std::string generation_text, max_text;
                if (!(status = Required(options, "--generation", &generation_text)).ok() ||
                    !(status = Required(options, "--max-tasks", &max_text)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                std::uint64_t generation = 0;
                std::uint32_t max_tasks = 0;
                if (!(status = tools::ParseU64(generation_text, "generation", &generation)).ok() ||
                    !(status = tools::ParseU32(max_text, "max-tasks", &max_tasks)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                cpr::store::v1::PollTasksRequest request;
                request.set_store_id(store_id);
                request.set_generation(generation);
                request.set_max_tasks(max_tasks);
                cpr::store::v1::PollTasksResponse response;
                grpc::ClientContext context;
                SetDeadline(&context, OptionalTimeout(options));
                grpc::Status rpc = stub->PollTasks(&context, request, &response);
                int rc = RpcExit(rpc, err);
                if (rc != tools::kExitOk) return rc;
                PrintRpcStatus(out, response.status());
                out << "tasks=" << response.tasks_size() << '\n';
                for (const auto &task : response.tasks())
                {
                    out << "task_id=" << task.task_id()
                        << " type=" << cpr::store::v1::TaskType_Name(task.type())
                        << " state=" << cpr::store::v1::TaskState_Name(task.state())
                        << " sequence=" << task.sequence() << '\n';
                }
                return BusinessExit(response.status());
            }

            if (command == "store-report-task-result")
            {
                std::string generation_text, task_id, state_text;
                if (!(status = Required(options, "--generation", &generation_text)).ok() ||
                    !(status = Required(options, "--task-id", &task_id)).ok() ||
                    !(status = Required(options, "--state", &state_text)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                std::uint64_t generation = 0;
                cpr::store::v1::TaskState final_state;
                if (!(status = tools::ParseU64(generation_text, "generation", &generation)).ok() ||
                    !(status = ParseTaskState(state_text, &final_state)).ok())
                {
                    tools::PrintStatus(err, status);
                    return tools::kExitUsage;
                }
                cpr::store::v1::ReportTaskResultRequest request;
                request.set_store_id(store_id);
                request.set_generation(generation);
                request.set_task_id(task_id);
                request.set_final_state(final_state);
                if (options.values.count("--result"))
                {
                    request.set_result_payload(options.values.at("--result"));
                }
                cpr::store::v1::ReportTaskResultResponse response;
                grpc::ClientContext context;
                SetDeadline(&context, OptionalTimeout(options));
                grpc::Status rpc = stub->ReportTaskResult(&context, request, &response);
                int rc = RpcExit(rpc, err);
                if (rc != tools::kExitOk) return rc;
                PrintRpcStatus(out, response.status());
                out << "duplicate_result=" << (response.duplicate_result() ? "true" : "false")
                    << " task_id=" << response.task().task_id() << '\n';
                return BusinessExit(response.status());
            }

            tools::PrintStatus(err, common::Status::InvalidArgument("unknown store command"));
            return tools::kExitUsage;
        }

    } // namespace

    int RunClientTool(int argc,
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

        const std::string command = argv[1];
        if (command.rfind("metadata-", 0) == 0)
        {
            return MetadataCommand(command, options, out, err);
        }
        if (command.rfind("store-", 0) == 0)
        {
            return StoreCommand(command, options, out, err);
        }

        tools::PrintStatus(err, common::Status::InvalidArgument("unknown command: " + command));
        return tools::kExitUsage;
    }

} // namespace cpr::tools::client
