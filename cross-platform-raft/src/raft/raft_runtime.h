#pragma once

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

#include "common/bounded_queue.h"
#include "common/status.h"
#include "common/types.h"
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

    // Carries the result of a completed storage operation so Runtime can
    // forward it to RaftCore::ConfirmPersistence().
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
        // Proposal correlation data will be added in T023.
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
    //  RaftRuntime — bounded event queue and lifecycle state
    // -----------------------------------------------------------------------

    class RaftRuntime
    {
    public:
        struct Options
        {
            // Capacity of the shared event queue.
            // Must be greater than zero.
            std::size_t event_queue_capacity = 0;
        };

        explicit RaftRuntime(const Options &options);

        RaftRuntime(const RaftRuntime &) = delete;
        RaftRuntime &operator=(const RaftRuntime &) = delete;

        // --- Enqueue methods ---

        // Enqueue a logical Tick event. Always accepted unless shutdown.
        common::Status EnqueueTick();

        // Enqueue an inbound Raft message from a peer.
        common::Status EnqueueMessage(common::NodeId source_node_id,
                                      const RaftMessage &message);
        common::Status EnqueueMessage(common::NodeId source_node_id,
                                      RaftMessage &&message);

        // Enqueue a client Proposal. Success does NOT mean the proposal has
        // been committed or applied — only that it entered the queue.
        common::Status EnqueueProposal(const ProposalEvent &event);
        common::Status EnqueueProposal(ProposalEvent &&event);

        // Enqueue a membership change request.
        common::Status EnqueueMembershipChange(const MembershipChangeEvent &event);
        common::Status EnqueueMembershipChange(MembershipChangeEvent &&event);

        // Enqueue a persistence completion notification carrying the storage
        // operation result.
        common::Status EnqueuePersistenceCompletion(
            const PersistenceCompletionEvent &event);
        common::Status EnqueuePersistenceCompletion(
            PersistenceCompletionEvent &&event);

        // Enqueue an apply completion notification.
        common::Status EnqueueApplyCompletion(const ApplyCompletionEvent &event);
        common::Status EnqueueApplyCompletion(ApplyCompletionEvent &&event);

        // Request graceful shutdown. After the first successful shutdown
        // request, subsequent ordinary events are rejected. Repeated calls
        // are safe and return an explicit ALREADY_SHUTDOWN status.
        common::Status RequestShutdown();

        // --- Query methods ---

        bool shutdown_requested() const noexcept;
        bool accepting_events() const noexcept;
        std::size_t event_queue_size() const;
        std::size_t event_queue_capacity() const noexcept;

    private:
        common::Status Enqueue(RuntimeEvent &&event);

        common::BoundedQueue<RuntimeEvent> event_queue_;
        bool shutdown_requested_ = false;
    };

} // namespace cpr::raft
