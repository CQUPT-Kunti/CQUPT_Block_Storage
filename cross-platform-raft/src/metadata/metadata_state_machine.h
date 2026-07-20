#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "metadata/metadata_command.h"
#include "metadata/state_machine.h"
#include "store/store_registry.h"
#include "store/task_manager.h"

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
        common::Status GetStore(store::StoreId store_id,
                                store::StoreInfo *store) const;
        common::Status ListStores(std::vector<store::StoreInfo> *stores) const;
        common::Status UpdateStoreHeartbeat(store::StoreId store_id,
                                            std::int64_t heartbeat_ms,
                                            store::StoreInfo *store = nullptr);
        common::Status GetTask(const store::TaskId &task_id,
                               store::TaskRecord *task) const;
        common::Status ListTasks(std::vector<store::TaskRecord> *tasks) const;

    private:
        using RecordMap = std::map<std::string, MetadataStateRecord>;

        common::Status ApplyOpaqueCommand(common::LogIndex index,
                                          common::Term term,
                                          const MetadataCommand &command);
        common::Status ApplyStoreCommand(common::LogIndex index,
                                         common::Term term,
                                         const MetadataCommand &command);

        mutable std::mutex mutex_;
        RecordMap records_;
        common::LogIndex last_applied_index_ = common::kInvalidLogIndex;
        common::Term last_applied_term_ = common::kInitialTerm;
        store::StoreRegistry stores_;
        store::TaskManager tasks_;
    };

} // namespace cpr::metadata
