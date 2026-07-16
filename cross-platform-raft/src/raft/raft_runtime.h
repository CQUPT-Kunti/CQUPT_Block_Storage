#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <variant>
#include <vector>

#include "common/bounded_queue.h"
#include "common/status.h"
#include "common/types.h"
#include "raft/raft_core.h"
#include "raft/raft_message.h"
#include "raft/raft_storage.h"
#include "raft/raft_types.h"

namespace cpr::raft
{

    // ===================================================================
    //  Runtime event types
    // ===================================================================

    struct TickEvent
    {
    };
    struct ShutdownEvent
    {
    };

    struct MessageEvent
    {
        common::NodeId source_node_id = common::kInvalidNodeId;
        RaftMessage message;
    };

    struct ProposalEvent
    {
        std::uint64_t proposal_id = 0;
        OpaquePayload payload;
    };

    struct MembershipChangeEvent
    {
        std::vector<RaftMember> added_voters;
        std::vector<RaftMember> added_learners;
        std::vector<common::NodeId> removed_nodes;
    };

    struct PersistenceCompletionEvent
    {
        common::Status status;
        bool has_hard_state = false;
        HardState hard_state;
        common::LogIndex last_log_index = common::kInvalidLogIndex;
        bool has_snapshot = false;
        SnapshotMetadata snapshot;
    };

    struct ApplyCompletionEvent
    {
        common::LogIndex applied_index = common::kInvalidLogIndex;
        common::Status status;
        std::uint64_t proposal_id = 0;
    };

    using RuntimeEvent = std::variant<
        TickEvent, MessageEvent, ProposalEvent, MembershipChangeEvent,
        PersistenceCompletionEvent, ApplyCompletionEvent, ShutdownEvent>;

    // ===================================================================
    //  Persistence work
    // ===================================================================

    struct PersistenceWork
    {
        bool has_hard_state = false;
        HardState hard_state;
        std::vector<LogEntry> entries;
        common::LogIndex max_stable_index = common::kInvalidLogIndex;
    };

    // ===================================================================
    //  Apply work
    // ===================================================================

    struct ApplyWork
    {
        common::LogIndex index = common::kInvalidLogIndex;
        common::Term term = common::kInitialTerm;
        LogEntryType type = LogEntryType::NO_OP;
        OpaquePayload payload;
        std::uint64_t proposal_id = 0;
    };

    // ===================================================================
    //  Proposal result
    // ===================================================================

    struct ProposalResult
    {
        std::uint64_t proposal_id = 0;
        common::LogIndex log_index = common::kInvalidLogIndex;
        common::Status status;
        bool final_result = false;
    };

    // ===================================================================
    //  RaftRuntime
    // ===================================================================

    class RaftRuntime
    {
    public:
        // Apply callback: called by the apply worker thread for each
        // committed entry. Must be safe to call from a dedicated thread.
        // Returns OK on success, or an error status.
        using ApplyFn = std::function<common::Status(const LogEntry &entry)>;

        struct Options
        {
            std::size_t event_queue_capacity = 0;
            std::size_t persistence_queue_capacity = 0;
            std::size_t apply_queue_capacity = 0;
            std::size_t proposal_result_queue_capacity = 0;
        };

        explicit RaftRuntime(const Options &options);
        ~RaftRuntime();

        RaftRuntime(const RaftRuntime &) = delete;
        RaftRuntime &operator=(const RaftRuntime &) = delete;

        // --- Lifecycle ---

        common::Status Initialize(RaftCore *core, IRaftStorage *storage,
                                  ApplyFn apply_fn = nullptr);
        common::Status Start();
        common::Status RequestShutdown();
        void WaitForShutdown();

        // --- Enqueue (thread-safe) ---

        common::Status EnqueueTick();
        common::Status EnqueueMessage(common::NodeId source_node_id,
                                      const RaftMessage &message);
        common::Status EnqueueMessage(common::NodeId source_node_id,
                                      RaftMessage &&message);
        common::Status EnqueueProposal(const ProposalEvent &event);
        common::Status EnqueueProposal(ProposalEvent &&event);
        common::Status EnqueueMembershipChange(const MembershipChangeEvent &event);
        common::Status EnqueueMembershipChange(MembershipChangeEvent &&event);
        common::Status EnqueuePersistenceCompletion(
            const PersistenceCompletionEvent &event);
        common::Status EnqueuePersistenceCompletion(
            PersistenceCompletionEvent &&event);
        common::Status EnqueueApplyCompletion(const ApplyCompletionEvent &event);
        common::Status EnqueueApplyCompletion(ApplyCompletionEvent &&event);

        // --- Output ---

        std::vector<RaftMessage> CollectOutputMessages();

        // Collect finished proposal results. Thread-safe. Clears buffer.
        std::vector<ProposalResult> CollectProposalResults();

        // --- Query ---

        bool shutdown_requested() const noexcept;
        bool accepting_events() const noexcept;
        std::size_t event_queue_size() const;
        std::size_t event_queue_capacity() const noexcept;
        std::size_t persistence_queue_size() const;
        std::size_t apply_queue_size() const;

    private:
        common::Status Enqueue(RuntimeEvent &&event);
        common::Status ScheduleApplyWork();
        void EmitProposalResult(std::uint64_t proposal_id,
                                common::LogIndex log_index,
                                common::Status status, bool final_result);
        bool EmitPersistenceCompletion(PersistenceCompletionEvent &&event);
        bool EmitApplyCompletion(ApplyCompletionEvent &&event);

        void ProtocolLoop();
        void PersistenceLoop();
        void ApplyLoop();

        void ProcessTick();
        void ProcessMessage(const MessageEvent &event);
        void ProcessProposal(const ProposalEvent &event);
        void ProcessPersistenceCompletion(const PersistenceCompletionEvent &event);
        void ProcessMembershipChange();
        void ProcessApplyCompletion(const ApplyCompletionEvent &event);

        common::Status DrainCoreOutput();

        RaftCore *core_ = nullptr;
        IRaftStorage *storage_ = nullptr;
        ApplyFn apply_fn_;

        bool initialized_ = false;
        bool threads_running_ = false;

        common::BoundedQueue<RuntimeEvent> event_queue_;
        common::BoundedQueue<PersistenceWork> persistence_queue_;
        common::BoundedQueue<ApplyWork> apply_queue_;

        std::thread protocol_thread_;
        std::thread persistence_thread_;
        std::thread apply_thread_;
        bool shutdown_requested_ = false;
        bool protocol_stopped_ = false;
        bool persistence_stopped_ = false;
        bool apply_stopped_ = false;

        // Output message buffer
        std::mutex output_mutex_;
        std::vector<RaftMessage> released_messages_;

        // Proposal result buffer
        std::mutex result_mutex_;
        std::vector<ProposalResult> proposal_results_;

        // Proposal ID -> log index tracking (protocol thread only)
        std::map<std::uint64_t, common::LogIndex> pending_proposals_;

        // Next apply index to schedule (protocol thread only)
        common::LogIndex next_apply_schedule_ = common::kInvalidLogIndex;
    };

} // namespace cpr::raft
