#include "metadata/metadata_service.h"

#include <string>
#include <thread>
#include <utility>

namespace cpr::metadata
{
    namespace
    {

        constexpr std::size_t kMaxCachedProposalResults = 4096;
        constexpr std::size_t kCollectBatchSize = 64;

        MetadataProposalResult ToMetadataResult(const MetadataCommand &command,
                                                const raft::ProposalResult &source)
        {
            MetadataProposalResult result;
            result.status = source.status;
            result.proposal_id = source.proposal_id;
            result.command_id = command.command_id;
            result.applied_index = source.log_index;
            result.leader_id = source.leader_id;
            result.leader_address = source.leader_address;
            result.final_result = source.final_result;
            return result;
        }

        StoreControlResult ToStoreResult(const MetadataProposalResult &proposal)
        {
            StoreControlResult result;
            result.status = proposal.status;
            result.proposal_id = proposal.proposal_id;
            result.command_id = proposal.command_id;
            result.applied_index = proposal.applied_index;
            result.leader_id = proposal.leader_id;
            result.leader_address = proposal.leader_address;
            result.final_result = proposal.final_result;
            return result;
        }

        std::chrono::milliseconds EffectiveTimeout(
            std::chrono::milliseconds timeout,
            std::chrono::milliseconds fallback)
        {
            return timeout.count() > 0 ? timeout : fallback;
        }

    } // namespace

    MetadataService::MetadataService(raft::RaftRuntime *runtime,
                                     MetadataStateMachine *state_machine)
        : runtime_(runtime),
          state_machine_(state_machine)
    {
    }

    MetadataService::MetadataService(raft::RaftRuntime *runtime,
                                     MetadataStateMachine *state_machine,
                                     Options options)
        : runtime_(runtime),
          state_machine_(state_machine),
          options_(std::move(options))
    {
    }

    common::Status MetadataService::Propose(const MetadataCommand &command,
                                            MetadataProposalResult *result)
    {
        return Propose(command, options_.proposal_timeout, result);
    }

    common::Status MetadataService::Propose(const MetadataCommand &command,
                                            std::chrono::milliseconds timeout,
                                            MetadataProposalResult *result)
    {
        if (result == nullptr)
        {
            return common::Status::InvalidArgument("metadata proposal result must not be null");
        }

        common::Status status = ValidateDependencies();
        if (!status.ok())
        {
            return status;
        }

        status = ValidateMetadataCommand(command);
        if (!status.ok())
        {
            return status;
        }

        raft::OpaquePayload payload;
        status = EncodeMetadataCommand(command, &payload);
        if (!status.ok())
        {
            return status;
        }

        raft::ProposalEvent event;
        event.proposal_id = NextProposalId();
        event.payload = std::move(payload);

        status = runtime_->EnqueueProposal(std::move(event));
        if (!status.ok())
        {
            return status;
        }

        const auto deadline = std::chrono::steady_clock::now() + timeout;
        raft::ProposalResult runtime_result;
        while (true)
        {
            if (TakeProposalResult(event.proposal_id, &runtime_result))
            {
                *result = ToMetadataResult(command, runtime_result);
                return runtime_result.status;
            }

            if (runtime_->state() != raft::RuntimeState::RUNNING)
            {
                AbandonProposal(event.proposal_id);
                return common::Status::Busy("runtime stopped while waiting for metadata proposal result");
            }
            if (std::chrono::steady_clock::now() >= deadline)
            {
                AbandonProposal(event.proposal_id);
                return common::Status::RetryLater("timed out waiting for metadata proposal result");
            }

            if (options_.poll_interval.count() <= 0)
            {
                std::this_thread::yield();
            }
            else
            {
                std::this_thread::sleep_for(options_.poll_interval);
            }
        }
    }

    common::Status MetadataService::Query(const std::string &target_id,
                                          MetadataQueryResult *result) const
    {
        if (result == nullptr)
        {
            return common::Status::InvalidArgument("metadata query result must not be null");
        }
        common::Status status = ValidateDependencies();
        if (!status.ok())
        {
            return status;
        }
        if (target_id.empty())
        {
            return common::Status::InvalidArgument("metadata query target id must not be empty");
        }

        MetadataQueryResult candidate;
        status = state_machine_->GetRecord(target_id, &candidate.record);
        if (!status.ok())
        {
            return status;
        }
        status = state_machine_->GetLastApplied(&candidate.last_applied_index,
                                                &candidate.last_applied_term);
        if (!status.ok())
        {
            return status;
        }

        *result = std::move(candidate);
        return common::Status::OK();
    }

    common::Status MetadataService::RegisterStore(
        const std::string &command_id,
        const store::StoreInfo &store,
        std::chrono::milliseconds timeout,
        StoreControlResult *result)
    {
        StoreBusinessCommand command;
        command.kind = StoreBusinessCommandKind::REGISTER_STORE;
        command.store = store;
        return ProposeStoreCommand(command_id,
                                   "store-register:" + command_id,
                                   command,
                                   timeout,
                                   result);
    }

    common::Status MetadataService::StopStore(
        const std::string &command_id,
        store::StoreId store_id,
        std::uint64_t expected_generation,
        std::chrono::milliseconds timeout,
        StoreControlResult *result)
    {
        StoreBusinessCommand command;
        command.kind = StoreBusinessCommandKind::UPDATE_STORE_STATE;
        command.store_update.id = store_id;
        command.store_update.expected_generation = expected_generation;
        command.store_update.state = store::StoreState::STOPPED;
        return ProposeStoreCommand(command_id,
                                   "store-stop:" + command_id,
                                   command,
                                   timeout,
                                   result);
    }

    common::Status MetadataService::RemoveStore(
        const std::string &command_id,
        store::StoreId store_id,
        std::uint64_t expected_generation,
        std::chrono::milliseconds timeout,
        StoreControlResult *result)
    {
        StoreBusinessCommand command;
        command.kind = StoreBusinessCommandKind::REMOVE_STORE;
        command.store_id = store_id;
        command.store_update.expected_generation = expected_generation;
        return ProposeStoreCommand(command_id,
                                   "store-remove:" + command_id,
                                   command,
                                   timeout,
                                   result);
    }

    common::Status MetadataService::CreateTaskForTest(
        const std::string &command_id,
        const store::TaskCreateRequest &task,
        std::chrono::milliseconds timeout,
        StoreControlResult *result)
    {
        StoreBusinessCommand command;
        command.kind = StoreBusinessCommandKind::CREATE_TASK;
        command.task_create = task;
        return ProposeStoreCommand(command_id,
                                   "task-create:" + command_id,
                                   command,
                                   timeout,
                                   result);
    }

    common::Status MetadataService::PollTasks(
        store::StoreId store_id,
        std::uint32_t max_tasks,
        std::chrono::milliseconds timeout,
        StoreControlResult *result)
    {
        const std::string command_id = NextGeneratedCommandId("poll");
        StoreBusinessCommand command;
        command.kind = StoreBusinessCommandKind::POLL_TASKS;
        command.store_id = store_id;
        command.max_tasks = max_tasks;
        return ProposeStoreCommand(command_id,
                                   "task-poll:" + command_id,
                                   command,
                                   timeout,
                                   result);
    }

    common::Status MetadataService::ReportTaskResult(
        const store::TaskResultReport &report,
        std::chrono::milliseconds timeout,
        StoreControlResult *result)
    {
        const std::string command_id = NextGeneratedCommandId("result");
        StoreBusinessCommand command;
        command.kind = StoreBusinessCommandKind::REPORT_TASK_RESULT;
        command.task_result = report;
        return ProposeStoreCommand(command_id,
                                   "task-result:" + command_id,
                                   command,
                                   timeout,
                                   result);
    }

    common::Status MetadataService::UpdateStoreHeartbeat(
        store::StoreId store_id,
        std::int64_t heartbeat_ms,
        store::StoreInfo *store)
    {
        common::Status status = ValidateDependencies();
        if (!status.ok())
        {
            return status;
        }
        return state_machine_->UpdateStoreHeartbeat(store_id, heartbeat_ms, store);
    }

    common::Status MetadataService::GetStore(store::StoreId store_id,
                                             store::StoreInfo *store) const
    {
        common::Status status = ValidateDependencies();
        if (!status.ok())
        {
            return status;
        }
        return state_machine_->GetStore(store_id, store);
    }

    common::Status MetadataService::GetTask(const store::TaskId &task_id,
                                            store::TaskRecord *task) const
    {
        common::Status status = ValidateDependencies();
        if (!status.ok())
        {
            return status;
        }
        return state_machine_->GetTask(task_id, task);
    }

    common::Status MetadataService::ValidateDependencies() const
    {
        if (runtime_ == nullptr || state_machine_ == nullptr)
        {
            return common::Status::InvalidArgument("metadata service dependencies must not be null");
        }
        return common::Status::OK();
    }

    std::uint64_t MetadataService::NextProposalId()
    {
        std::uint64_t id = next_proposal_id_.fetch_add(1);
        if (id == 0)
        {
            id = next_proposal_id_.fetch_add(1);
        }
        return id;
    }

    std::string MetadataService::NextGeneratedCommandId(const char *prefix)
    {
        std::uint64_t id = next_store_command_id_.fetch_add(1);
        if (id == 0)
        {
            id = next_store_command_id_.fetch_add(1);
        }
        return std::string(prefix) + "-" + std::to_string(id);
    }

    common::Status MetadataService::ProposeStoreCommand(
        const std::string &command_id,
        const std::string &target_id,
        const StoreBusinessCommand &command,
        std::chrono::milliseconds timeout,
        StoreControlResult *result)
    {
        if (result == nullptr)
        {
            return common::Status::InvalidArgument("store control result must not be null");
        }
        if (command_id.empty())
        {
            return common::Status::InvalidArgument("store command id must not be empty");
        }

        raft::OpaquePayload command_payload;
        common::Status status =
            EncodeStoreBusinessCommand(command, &command_payload);
        if (!status.ok())
        {
            return status;
        }

        MetadataCommand metadata_command;
        metadata_command.type = MetadataCommandType::STORE_OPERATION;
        metadata_command.command_id = command_id;
        metadata_command.target_id = target_id;
        metadata_command.payload = std::move(command_payload);

        MetadataProposalResult proposal;
        status = Propose(metadata_command,
                         EffectiveTimeout(timeout, options_.proposal_timeout),
                         &proposal);
        StoreControlResult candidate = ToStoreResult(proposal);
        if (!status.ok())
        {
            *result = std::move(candidate);
            return status;
        }

        MetadataQueryResult query;
        status = Query(target_id, &query);
        if (!status.ok())
        {
            *result = std::move(candidate);
            return status;
        }
        StoreBusinessResult business_result;
        status = DecodeStoreBusinessResult(query.record.payload, &business_result);
        if (!status.ok())
        {
            *result = std::move(candidate);
            return status;
        }

        candidate.store = std::move(business_result.store);
        candidate.tasks = std::move(business_result.tasks);
        candidate.duplicate_result = business_result.duplicate_result;
        *result = std::move(candidate);
        return common::Status::OK();
    }

    bool MetadataService::TakeProposalResult(std::uint64_t proposal_id,
                                             raft::ProposalResult *result)
    {
        if (result == nullptr)
        {
            return false;
        }

        std::lock_guard<std::mutex> lock(results_mutex_);
        auto cached = cached_results_.find(proposal_id);
        if (cached != cached_results_.end())
        {
            *result = std::move(cached->second);
            cached_results_.erase(cached);
            return true;
        }

        bool found = false;
        raft::ProposalResult matched;
        for (raft::ProposalResult &item :
             runtime_->CollectProposalResults(kCollectBatchSize))
        {
            if (item.proposal_id == proposal_id)
            {
                matched = std::move(item);
                found = true;
                continue;
            }
            if (abandoned_proposals_.erase(item.proposal_id) == 0)
            {
                cached_results_.emplace(item.proposal_id, std::move(item));
            }
        }

        TrimResultCache();
        if (!found)
        {
            return false;
        }
        *result = std::move(matched);
        return true;
    }

    void MetadataService::AbandonProposal(std::uint64_t proposal_id)
    {
        std::lock_guard<std::mutex> lock(results_mutex_);
        cached_results_.erase(proposal_id);
        abandoned_proposals_.insert(proposal_id);
        TrimResultCache();
    }

    void MetadataService::TrimResultCache()
    {
        while (cached_results_.size() > kMaxCachedProposalResults)
        {
            cached_results_.erase(cached_results_.begin());
        }
        while (abandoned_proposals_.size() > kMaxCachedProposalResults)
        {
            abandoned_proposals_.erase(abandoned_proposals_.begin());
        }
    }

} // namespace cpr::metadata
