#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "metadata/state_machine.h"

namespace cpr::metadata
{

    struct MetadataStateRecord
    {
        std::string target_id;
        raft::OpaquePayload payload;
        std::uint64_t generation = 0;
        std::string last_command_id;
        common::LogIndex last_update_index = common::kInvalidLogIndex;
        common::Term last_update_term = common::kInitialTerm;
    };

    class MetadataStateMachine final : public IRaftStateMachine
    {
    public:
        MetadataStateMachine() = default;

        common::Status Apply(common::LogIndex index,
                             common::Term term,
                             const raft::OpaquePayload &command_payload) override;

        common::Status CreateSnapshot(common::LogIndex last_applied_index,
                                      common::Term last_applied_term,
                                      raft::OpaquePayload *snapshot_payload) override;

        common::Status RestoreSnapshot(
            const raft::OpaquePayload &snapshot_payload) override;

        common::Status GetRecord(const std::string &target_id,
                                 MetadataStateRecord *record) const;
        common::Status GetLastApplied(common::LogIndex *index,
                                      common::Term *term) const;

    private:
        using RecordMap = std::map<std::string, MetadataStateRecord>;

        mutable std::mutex mutex_;
        RecordMap records_;
        common::LogIndex last_applied_index_ = common::kInvalidLogIndex;
        common::Term last_applied_term_ = common::kInitialTerm;
    };

} // namespace cpr::metadata
