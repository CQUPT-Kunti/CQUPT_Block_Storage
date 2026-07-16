#include "raft/raft_runtime.h"

#include <string>
#include <utility>

namespace cpr::raft
{
    namespace
    {

        common::Status InvalidArgument(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        common::Status ResourceExhausted(std::string message)
        {
            return common::Status::ResourceExhausted(std::move(message));
        }

        common::Status Busy(std::string message)
        {
            return common::Status::Busy(std::move(message));
        }

    } // namespace

    // -----------------------------------------------------------------------
    //  Construction
    // -----------------------------------------------------------------------

    RaftRuntime::RaftRuntime(const Options &options)
        : event_queue_(options.event_queue_capacity)
    {
    }

    // -----------------------------------------------------------------------
    //  Enqueue (private)
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::Enqueue(RuntimeEvent &&event)
    {
        if (shutdown_requested_)
        {
            return Busy("runtime is shutting down");
        }
        return event_queue_.Push(std::move(event));
    }

    // -----------------------------------------------------------------------
    //  Tick
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::EnqueueTick()
    {
        return Enqueue(TickEvent{});
    }

    // -----------------------------------------------------------------------
    //  Message
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::EnqueueMessage(common::NodeId source_node_id,
                                               const RaftMessage &message)
    {
        MessageEvent event;
        event.source_node_id = source_node_id;
        event.message = message;
        return Enqueue(std::move(event));
    }

    common::Status RaftRuntime::EnqueueMessage(common::NodeId source_node_id,
                                               RaftMessage &&message)
    {
        MessageEvent event;
        event.source_node_id = source_node_id;
        event.message = std::move(message);
        return Enqueue(std::move(event));
    }

    // -----------------------------------------------------------------------
    //  Proposal
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::EnqueueProposal(const ProposalEvent &event)
    {
        return Enqueue(RuntimeEvent(event));
    }

    common::Status RaftRuntime::EnqueueProposal(ProposalEvent &&event)
    {
        return Enqueue(RuntimeEvent(std::move(event)));
    }

    // -----------------------------------------------------------------------
    //  Membership change
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::EnqueueMembershipChange(
        const MembershipChangeEvent &event)
    {
        return Enqueue(RuntimeEvent(event));
    }

    common::Status RaftRuntime::EnqueueMembershipChange(
        MembershipChangeEvent &&event)
    {
        return Enqueue(RuntimeEvent(std::move(event)));
    }

    // -----------------------------------------------------------------------
    //  Persistence completion
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::EnqueuePersistenceCompletion(
        const PersistenceCompletionEvent &event)
    {
        return Enqueue(RuntimeEvent(event));
    }

    common::Status RaftRuntime::EnqueuePersistenceCompletion(
        PersistenceCompletionEvent &&event)
    {
        return Enqueue(RuntimeEvent(std::move(event)));
    }

    // -----------------------------------------------------------------------
    //  Apply completion
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::EnqueueApplyCompletion(
        const ApplyCompletionEvent &event)
    {
        return Enqueue(RuntimeEvent(event));
    }

    common::Status RaftRuntime::EnqueueApplyCompletion(
        ApplyCompletionEvent &&event)
    {
        return Enqueue(RuntimeEvent(std::move(event)));
    }

    // -----------------------------------------------------------------------
    //  Shutdown
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::RequestShutdown()
    {
        if (shutdown_requested_)
        {
            return Busy("shutdown already requested");
        }
        shutdown_requested_ = true;
        const common::Status status = event_queue_.Push(ShutdownEvent{});
        if (!status.ok())
        {
            // Queue push failed; reset the flag so a retry is possible.
            shutdown_requested_ = false;
            return status;
        }
        return common::Status::OK();
    }

    // -----------------------------------------------------------------------
    //  Queries
    // -----------------------------------------------------------------------

    bool RaftRuntime::shutdown_requested() const noexcept
    {
        return shutdown_requested_;
    }

    bool RaftRuntime::accepting_events() const noexcept
    {
        return !shutdown_requested_;
    }

    std::size_t RaftRuntime::event_queue_size() const
    {
        return event_queue_.size();
    }

    std::size_t RaftRuntime::event_queue_capacity() const noexcept
    {
        return event_queue_.capacity();
    }

} // namespace cpr::raft
