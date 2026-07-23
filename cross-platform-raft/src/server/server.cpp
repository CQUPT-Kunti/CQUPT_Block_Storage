#include "server/server.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <string>
#include <utility>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include "metadata/metadata_command.h"
#include "server/logging.h"

namespace cpr::server
{
    namespace
    {

        common::Status ParseNodeIdText(const std::string &text,
                                       common::NodeId *node_id)
        {
            if (node_id == nullptr)
            {
                return common::Status::InvalidArgument("node id output must not be null");
            }
            if (text.empty())
            {
                return common::Status::InvalidArgument("node id must not be empty");
            }

            const auto parse_positive = [](const std::string &value,
                                           common::NodeId *parsed)
            {
                char *end = nullptr;
                const unsigned long long raw = std::strtoull(value.c_str(), &end, 10);
                if (end == value.c_str() || end == nullptr || *end != '\0' ||
                    raw == 0ULL)
                {
                    return false;
                }
                *parsed = static_cast<common::NodeId>(raw);
                return true;
            };

            common::NodeId parsed = common::kInvalidNodeId;
            if (parse_positive(text, &parsed))
            {
                *node_id = parsed;
                return common::Status::OK();
            }

            constexpr char kPrefix[] = "node-";
            if (text.rfind(kPrefix, 0) != 0)
            {
                return common::Status::InvalidArgument("node id must be numeric or node-N");
            }

            const std::string suffix = text.substr(sizeof(kPrefix) - 1);
            if (!parse_positive(suffix, &parsed))
            {
                return common::Status::InvalidArgument("node id suffix must be a positive integer");
            }
            *node_id = parsed;
            return common::Status::OK();
        }

        std::uint64_t CeilDiv(std::uint64_t dividend, std::uint64_t divisor)
        {
            return (dividend + divisor - 1ULL) / divisor;
        }

        raft::RaftRole ToInitialRole(config::MemberRole role)
        {
            return role == config::MemberRole::kLearner
                       ? raft::RaftRole::LEARNER
                       : raft::RaftRole::FOLLOWER;
        }

        raft::RaftMember ToRaftMember(common::NodeId node_id,
                                      const config::InitialMember &member)
        {
            raft::RaftMember result;
            result.node_id = node_id;
            result.address.host = member.ip_address;
            result.address.port = member.raft_port;
            return result;
        }

    } // namespace

    ServerApplication::ServerApplication() = default;

    ServerApplication::~ServerApplication()
    {
        (void)Stop();
    }

    common::Status ServerApplication::Initialize(const config::Config &config)
    {
        if (initialized_)
        {
            return common::Status::Busy("server application is already initialized");
        }

        common::Status status = config::ValidateConfig(config);
        if (!status.ok())
        {
            return status;
        }
        status = lifecycle_.Initialize(config);
        if (!status.ok())
        {
            return status;
        }

        config_ = config;

        std::vector<ParsedMember> members;
        ParsedMember local_member;
        status = BuildBootstrapMembers(&members, &local_member);
        if (!status.ok())
        {
            return status;
        }

        storage_ = std::make_unique<raft::FileRaftStorage>();
        status = storage_->Open(config_.data_directory);
        if (!status.ok())
        {
            return status;
        }

        raft::RaftStorageLoadResult load_result;
        status = storage_->Load(&load_result);
        if (!status.ok())
        {
            return status;
        }
        if (!load_result.empty)
        {
            return common::Status::RetryLater(
                "server startup from persisted raft state is not wired yet");
        }

        raft::RaftCore::Options core_options;
        status = BuildRaftCoreOptions(local_member, members, &core_options);
        if (!status.ok())
        {
            return status;
        }

        core_ = std::make_unique<raft::RaftCore>();
        status = core_->Initialize(core_options);
        if (!status.ok())
        {
            return status;
        }
        if (core_options.initial_role == raft::RaftRole::CANDIDATE)
        {
            status = core_->BecomeLeader();
            if (!status.ok())
            {
                return status;
            }
        }

        raft::RaftRuntime::Options runtime_options;
        runtime_options.event_queue_capacity = config_.queue_capacity;
        runtime_options.persistence_queue_capacity = config_.queue_capacity;
        runtime_options.apply_queue_capacity = config_.queue_capacity;
        runtime_options.proposal_result_queue_capacity = config_.queue_capacity;
        runtime_options.peer_queue_capacity = config_.queue_capacity;

        state_machine_ = std::make_unique<metadata::MetadataStateMachine>();
        runtime_ = std::make_unique<raft::RaftRuntime>(runtime_options);
        status = runtime_->Initialize(
            core_.get(),
            storage_.get(),
            [this](const raft::LogEntry &entry)
            {
                if (entry.type != raft::LogEntryType::COMMAND)
                {
                    return common::Status::OK();
                }
                return state_machine_->Apply(entry.index, entry.term, entry.payload);
            });
        if (!status.ok())
        {
            return status;
        }

        transport::RaftTransportOptions transport_options;
        transport_options.rpc_timeout_ms = config_.rpc_timeout_ms;
        transport_ = std::make_unique<transport::GrpcRaftTransport>(transport_options);
        std::vector<raft::RaftMember> peers;
        peer_ids_.clear();
        for (const ParsedMember &parsed : members)
        {
            if (parsed.node_id == local_member.node_id)
            {
                continue;
            }
            peers.push_back(ToRaftMember(parsed.node_id, parsed.member));
            peer_ids_.push_back(parsed.node_id);
        }
        status = transport_->Initialize(local_member.node_id, peers);
        if (!status.ok())
        {
            return status;
        }

        metadata_service_ = std::make_unique<metadata::MetadataService>(
            runtime_.get(), state_machine_.get());
        raft_rpc_ = std::make_unique<rpc::RaftRpcServiceAdapter>(runtime_.get());
        metadata_rpc_ = std::make_unique<rpc::MetadataRpcServiceAdapter>(
            runtime_.get(), metadata_service_.get());
        store_rpc_ = std::make_unique<rpc::StoreRpcServiceAdapter>(
            metadata_service_.get());

        initialized_ = true;
        return common::Status::OK();
    }

    common::Status ServerApplication::Start()
    {
        if (!initialized_)
        {
            return common::Status::Busy("server application is not initialized");
        }
        bool expected = false;
        if (!running_.compare_exchange_strong(expected, true))
        {
            return common::Status::OK();
        }

        stop_requested_.store(false);
        common::Status status = lifecycle_.Start();
        if (!status.ok())
        {
            running_.store(false);
            return status;
        }

        status = transport_->Start();
        if (!status.ok())
        {
            (void)lifecycle_.Stop();
            running_.store(false);
            return status;
        }

        status = runtime_->Start();
        if (!status.ok())
        {
            (void)transport_->Stop();
            (void)lifecycle_.Stop();
            running_.store(false);
            return status;
        }

        status = StartGrpcServer();
        if (!status.ok())
        {
            RequestStop();
            if (runtime_->state() == raft::RuntimeState::RUNNING)
            {
                (void)runtime_->RequestShutdown();
            }
            runtime_->WaitForShutdown();
            (void)transport_->Stop();
            (void)lifecycle_.Stop();
            running_.store(false);
            return status;
        }

        tick_thread_ = std::thread(&ServerApplication::TickLoop, this);
        peer_pump_thread_ = std::thread(&ServerApplication::PeerPumpLoop, this);

        LogInfo("server started for node " + config_.node_id +
                " raft=" + raft_endpoint() +
                " metadata=" + metadata_endpoint() +
                " store=" + store_endpoint());
        return common::Status::OK();
    }

    common::Status ServerApplication::Stop()
    {
        if (!initialized_)
        {
            return common::Status::OK();
        }

        RequestStop();

        if (grpc_server_ != nullptr)
        {
            grpc_server_->Shutdown();
            grpc_server_->Wait();
            grpc_server_.reset();
        }

        JoinThreads();

        if (runtime_ && runtime_->state() == raft::RuntimeState::RUNNING)
        {
            (void)runtime_->RequestShutdown();
        }
        if (runtime_)
        {
            runtime_->WaitForShutdown();
        }
        if (transport_)
        {
            (void)transport_->Stop();
        }

        running_.store(false);
        initialized_ = false;
        return lifecycle_.Stop();
    }

    bool ServerApplication::IsRunning() const noexcept
    {
        return running_.load();
    }

    bool ServerApplication::HasShutdownRequest() const noexcept
    {
        return stop_requested_.load() || lifecycle_.HasShutdownRequest();
    }

    void ServerApplication::RequestStop() noexcept
    {
        stop_requested_.store(true);
        lifecycle_.RequestStop();
    }

    std::string ServerApplication::raft_endpoint() const
    {
        return EndpointForPort(config_.raft_port);
    }

    std::string ServerApplication::metadata_endpoint() const
    {
        return EndpointForPort(config_.metadata_port);
    }

    std::string ServerApplication::store_endpoint() const
    {
        return EndpointForPort(config_.store_control_port);
    }

    common::Status ServerApplication::BuildBootstrapMembers(
        std::vector<ParsedMember> *members,
        ParsedMember *local_member) const
    {
        if (members == nullptr || local_member == nullptr)
        {
            return common::Status::InvalidArgument("server member outputs must not be null");
        }
        members->clear();
        local_member->node_id = common::kInvalidNodeId;

        for (const config::InitialMember &member : config_.initial_members)
        {
            ParsedMember parsed;
            parsed.member = member;
            common::Status status = ParseNodeIdText(member.node_id, &parsed.node_id);
            if (!status.ok())
            {
                return status;
            }
            members->push_back(parsed);
        }

        common::NodeId local_node_id = common::kInvalidNodeId;
        common::Status status = ParseNodeIdText(config_.node_id, &local_node_id);
        if (!status.ok())
        {
            return status;
        }
        auto it = std::find_if(
            members->begin(), members->end(),
            [local_node_id](const ParsedMember &member)
            { return member.node_id == local_node_id; });
        if (it == members->end())
        {
            return common::Status::InvalidArgument("configured node id was not found in initial members");
        }
        *local_member = *it;
        return common::Status::OK();
    }

    common::Status ServerApplication::BuildRaftCoreOptions(
        const ParsedMember &local_member,
        const std::vector<ParsedMember> &members,
        raft::RaftCore::Options *options) const
    {
        if (options == nullptr)
        {
            return common::Status::InvalidArgument("raft core options must not be null");
        }

        options->node_id = local_member.node_id;
        options->initial_role = ToInitialRole(local_member.member.role);
        options->election_timeout_ticks =
            std::max<std::uint64_t>(1,
                                    CeilDiv(config_.election_timeout_min_ms,
                                            config_.heartbeat_interval_ms));

        raft::MembershipView membership;
        membership.configuration_id = 1;
        for (const ParsedMember &member : members)
        {
            raft::RaftMember raft_member = ToRaftMember(member.node_id, member.member);
            if (member.member.role == config::MemberRole::kLearner)
            {
                membership.learners.push_back(raft_member);
                options->learner_ids.push_back(member.node_id);
            }
            else
            {
                membership.voters.push_back(raft_member);
                options->voter_ids.push_back(member.node_id);
            }
        }
        options->membership = std::move(membership);
        if (options->voter_ids.size() == 1 &&
            options->voter_ids.front() == local_member.node_id &&
            options->learner_ids.empty())
        {
            options->initial_role = raft::RaftRole::CANDIDATE;
            options->hard_state.current_term = 1;
        }
        return common::Status::OK();
    }

    common::Status ServerApplication::StartGrpcServer()
    {
        grpc::ServerBuilder builder;
        int raft_port = 0;
        int metadata_port = 0;
        int store_port = 0;
        builder.AddListeningPort(raft_endpoint(), grpc::InsecureServerCredentials(),
                                 &raft_port);
        if (config_.metadata_port != config_.raft_port)
        {
            builder.AddListeningPort(metadata_endpoint(),
                                     grpc::InsecureServerCredentials(),
                                     &metadata_port);
        }
        else
        {
            metadata_port = raft_port;
        }
        if (config_.store_control_port != config_.raft_port &&
            config_.store_control_port != config_.metadata_port)
        {
            builder.AddListeningPort(store_endpoint(),
                                     grpc::InsecureServerCredentials(),
                                     &store_port);
        }
        else if (config_.store_control_port == config_.metadata_port)
        {
            store_port = metadata_port;
        }
        else
        {
            store_port = raft_port;
        }
        builder.RegisterService(raft_rpc_.get());
        builder.RegisterService(metadata_rpc_.get());
        builder.RegisterService(store_rpc_.get());
        grpc_server_ = builder.BuildAndStart();
        if (grpc_server_ == nullptr || raft_port <= 0 || metadata_port <= 0 ||
            store_port <= 0)
        {
            grpc_server_.reset();
            return common::Status::IoError("failed to bind grpc server ports");
        }
        return common::Status::OK();
    }

    void ServerApplication::TickLoop()
    {
        const auto interval =
            std::chrono::milliseconds(config_.heartbeat_interval_ms);
        while (!HasShutdownRequest())
        {
            std::this_thread::sleep_for(interval);
            if (runtime_->state() == raft::RuntimeState::RUNNING)
            {
                (void)runtime_->EnqueueTick();
            }
        }
    }

    void ServerApplication::PeerPumpLoop()
    {
        while (!HasShutdownRequest())
        {
            bool did_work = false;
            for (common::NodeId peer_id : peer_ids_)
            {
                raft::RaftMessage request;
                common::Status status =
                    runtime_->TryDequeuePeerMessage(peer_id, &request);
                if (!status.ok())
                {
                    continue;
                }
                did_work = true;

                raft::RaftMessage response;
                status = transport_->Send(request, &response);
                if (status.ok())
                {
                    (void)runtime_->EnqueueMessage(response.source_node_id,
                                                   std::move(response));
                }
            }
            if (!did_work)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    void ServerApplication::JoinThreads()
    {
        if (tick_thread_.joinable())
        {
            tick_thread_.join();
        }
        if (peer_pump_thread_.joinable())
        {
            peer_pump_thread_.join();
        }
    }

    std::string ServerApplication::EndpointForPort(std::uint16_t port) const
    {
        return config_.ip_address + ":" + std::to_string(port);
    }

} // namespace cpr::server
