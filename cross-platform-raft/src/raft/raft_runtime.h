#pragma once

#include <cstddef>
#include <cstdint>
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

    // -----------------------------------------------------------------------
    //  Runtime event types
    // -----------------------------------------------------------------------

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
    };

    using RuntimeEvent = std::variant<
        TickEvent,
        MessageEvent,
        ProposalEvent,
        MembershipChangeEvent,
        PersistenceCompletionEvent,
        ApplyCompletionEvent,
        ShutdownEvent>;

    // -----------------------------------------------------------------------
    //  Persistence work — one ordered batch for the persistence worker
    // -----------------------------------------------------------------------

    struct PersistenceWork
    {
        bool has_hard_state = false;
        HardState hard_state;
        std::vector<LogEntry> entries;
        common::LogIndex max_stable_index = common::kInvalidLogIndex;
    };

    // -----------------------------------------------------------------------
    //  RaftRuntime — protocol loop, persistence worker, and message gating
    // -----------------------------------------------------------------------

    class RaftRuntime
    {
    public:
        struct Options
        {
            // Capacity of the shared event queue (must be > 0).
            std::size_t event_queue_capacity = 0;
            // Capacity of the persistence work queue (must be > 0).
            std::size_t persistence_queue_capacity = 0;
        };

        explicit RaftRuntime(const Options &options);
        ~RaftRuntime();

        RaftRuntime(const RaftRuntime &) = delete;
        RaftRuntime &operator=(const RaftRuntime &) = delete;

        // --- Lifecycle ---

        // Set the RaftCore (protocol thread access) and IRaftStorage
        // (persistence worker access). Must be called before Start().
        // Both pointers must remain valid until after WaitForShutdown().
        common::Status Initialize(RaftCore *core, IRaftStorage *storage);

        // Start protocol and persistence threads. Returns an error if
        // Initialize() has not been called or threads are already running.
        common::Status Start();

        // Request graceful shutdown. Thread-safe.
        common::Status RequestShutdown();

        // Block until both protocol and persistence threads have stopped.
        void WaitForShutdown();

        // --- Enqueue methods (thread-safe) ---

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

        // Collect all released output messages (immediate + newly unblocked
        // after persistence). Thread-safe. Clears the internal buffer.
        std::vector<RaftMessage> CollectOutputMessages();

        // --- Query ---

        bool shutdown_requested() const noexcept;
        bool accepting_events() const noexcept;
        std::size_t event_queue_size() const;
        std::size_t event_queue_capacity() const noexcept;
        std::size_t persistence_queue_size() const;

    private:
        common::Status Enqueue(RuntimeEvent &&event);
        void ProtocolLoop();
        void PersistenceLoop();

        void ProcessTick();
        void ProcessMessage(const MessageEvent &event);
        void ProcessProposal(const ProposalEvent &event);
        void ProcessPersistenceCompletion(const PersistenceCompletionEvent &event);
        void ProcessMembershipChange();
        void ProcessApplyCompletion();

        common::Status DrainCoreOutput();
        bool EmitPersistenceCompletion(PersistenceCompletionEvent &&event);

        RaftCore *core_ = nullptr;
        IRaftStorage *storage_ = nullptr;

        bool initialized_ = false;
        bool threads_running_ = false;

        common::BoundedQueue<RuntimeEvent> event_queue_;
        common::BoundedQueue<PersistenceWork> persistence_queue_;

        std::thread protocol_thread_;
        std::thread persistence_thread_;
        bool shutdown_requested_ = false;
        bool protocol_stopped_ = false;
        bool persistence_stopped_ = false;

        std::mutex output_mutex_;
        std::vector<RaftMessage> released_messages_;

        common::LogIndex last_persisted_index_ = common::kInvalidLogIndex;
        bool last_persisted_hard_state_ = false;
    };

} // namespace cpr::raft
