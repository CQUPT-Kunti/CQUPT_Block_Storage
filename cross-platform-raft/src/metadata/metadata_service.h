#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>

#include "common/status.h"
#include "metadata/metadata_command.h"
#include "metadata/metadata_state_machine.h"
#include "raft/raft_runtime.h"
#include "raft/raft_types.h"

namespace cpr::metadata
{

    struct MetadataProposalResult
    {
        common::Status status;
        std::uint64_t proposal_id = 0;
        std::string command_id;
        common::LogIndex applied_index = common::kInvalidLogIndex;
        common::NodeId leader_id = common::kInvalidNodeId;
        raft::NodeAddress leader_address;
        bool final_result = false;
    };

    struct MetadataQueryResult
    {
        MetadataStateRecord record;
        common::LogIndex last_applied_index = common::kInvalidLogIndex;
        common::Term last_applied_term = common::kInitialTerm;
    };

    class MetadataService
    {
    public:
        struct Options
        {
            std::chrono::milliseconds proposal_timeout{1000};
            std::chrono::milliseconds poll_interval{1};
        };

        MetadataService(raft::RaftRuntime *runtime,
                        MetadataStateMachine *state_machine);
        MetadataService(raft::RaftRuntime *runtime,
                        MetadataStateMachine *state_machine,
                        Options options);

        common::Status Propose(const MetadataCommand &command,
                               MetadataProposalResult *result);
        common::Status Propose(const MetadataCommand &command,
                               std::chrono::milliseconds timeout,
                               MetadataProposalResult *result);
        common::Status Query(const std::string &target_id,
                             MetadataQueryResult *result) const;

    private:
        common::Status ValidateDependencies() const;
        std::uint64_t NextProposalId();
        bool TakeProposalResult(std::uint64_t proposal_id,
                                raft::ProposalResult *result);
        void AbandonProposal(std::uint64_t proposal_id);
        void TrimResultCache();

        raft::RaftRuntime *runtime_ = nullptr;
        MetadataStateMachine *state_machine_ = nullptr;
        Options options_;
        std::atomic<std::uint64_t> next_proposal_id_{1};

        mutable std::mutex results_mutex_;
        std::map<std::uint64_t, raft::ProposalResult> cached_results_;
        std::set<std::uint64_t> abandoned_proposals_;
    };

} // namespace cpr::metadata
