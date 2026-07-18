#include "metadata/metadata_service.h"

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
