#include "raft/raft_runtime.h"

#include <algorithm>
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

        common::Status Busy(std::string message)
        {
            return common::Status::Busy(std::move(message));
        }

    } // namespace

    // -----------------------------------------------------------------------
    //  Construction / Destruction
    // -----------------------------------------------------------------------

    RaftRuntime::RaftRuntime(const Options &options)
        : event_queue_(options.event_queue_capacity),
          persistence_queue_(options.persistence_queue_capacity)
    {
    }

    RaftRuntime::~RaftRuntime()
    {
        if (threads_running_)
        {
            RequestShutdown();
            WaitForShutdown();
        }
    }

    // -----------------------------------------------------------------------
    //  Lifecycle
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::Initialize(RaftCore *core, IRaftStorage *storage)
    {
        if (core == nullptr || storage == nullptr)
        {
            return InvalidArgument("core and storage must not be null");
        }
        core_ = core;
        storage_ = storage;
        initialized_ = true;
        return common::Status::OK();
    }

    common::Status RaftRuntime::Start()
    {
        if (!initialized_)
        {
            return InvalidArgument("runtime is not initialized");
        }
        if (threads_running_)
        {
            return Busy("threads are already running");
        }
        threads_running_ = true;
        protocol_thread_ = std::thread(&RaftRuntime::ProtocolLoop, this);
        persistence_thread_ = std::thread(&RaftRuntime::PersistenceLoop, this);
        return common::Status::OK();
    }

    common::Status RaftRuntime::RequestShutdown()
    {
        if (shutdown_requested_)
        {
            return Busy("shutdown already requested");
        }
        shutdown_requested_ = true;

        // Close queues so that blocking Pop() calls unblock.
        event_queue_.Close();
        persistence_queue_.Close();
        return common::Status::OK();
    }

    void RaftRuntime::WaitForShutdown()
    {
        if (protocol_thread_.joinable())
        {
            protocol_thread_.join();
        }
        if (persistence_thread_.joinable())
        {
            persistence_thread_.join();
        }
        threads_running_ = false;
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
    //  Enqueue implementations
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::EnqueueTick()
    {
        return Enqueue(TickEvent{});
    }

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

    common::Status RaftRuntime::EnqueueProposal(const ProposalEvent &event)
    {
        return Enqueue(RuntimeEvent(event));
    }

    common::Status RaftRuntime::EnqueueProposal(ProposalEvent &&event)
    {
        return Enqueue(RuntimeEvent(std::move(event)));
    }

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
    //  Output
    // -----------------------------------------------------------------------

    std::vector<RaftMessage> RaftRuntime::CollectOutputMessages()
    {
        std::lock_guard<std::mutex> lock(output_mutex_);
        std::vector<RaftMessage> result;
        result.swap(released_messages_);
        return result;
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

    std::size_t RaftRuntime::persistence_queue_size() const
    {
        return persistence_queue_.size();
    }

    // -----------------------------------------------------------------------
    //  Protocol loop
    // -----------------------------------------------------------------------

    void RaftRuntime::ProtocolLoop()
    {
        while (true)
        {
            RuntimeEvent event;
            const common::Status pop_status = event_queue_.Pop(&event);

            if (pop_status.code() == common::StatusCode::kRetryLater)
            {
                // Queue was closed and is empty — time to stop.
                break;
            }
            if (!pop_status.ok())
            {
                continue;
            }

            // Dispatch by event type
            if (std::holds_alternative<TickEvent>(event))
            {
                ProcessTick();
            }
            else if (std::holds_alternative<MessageEvent>(event))
            {
                ProcessMessage(std::get<MessageEvent>(event));
            }
            else if (std::holds_alternative<ProposalEvent>(event))
            {
                ProcessProposal(std::get<ProposalEvent>(event));
            }
            else if (std::holds_alternative<PersistenceCompletionEvent>(event))
            {
                ProcessPersistenceCompletion(
                    std::get<PersistenceCompletionEvent>(event));
            }
            else if (std::holds_alternative<MembershipChangeEvent>(event))
            {
                ProcessMembershipChange();
            }
            else if (std::holds_alternative<ApplyCompletionEvent>(event))
            {
                ProcessApplyCompletion();
            }
            else if (std::holds_alternative<ShutdownEvent>(event))
            {
                break;
            }
        }
        protocol_stopped_ = true;
    }

    // -----------------------------------------------------------------------
    //  Event handlers (called from protocol thread only)
    // -----------------------------------------------------------------------

    void RaftRuntime::ProcessTick()
    {
        RaftCore::TickAction action = RaftCore::TickAction::NONE;
        const common::Status status = core_->Tick(&action);
        if (!status.ok())
        {
            return;
        }
        DrainCoreOutput();
    }

    void RaftRuntime::ProcessMessage(const MessageEvent &event)
    {
        const RaftMessage &msg = event.message;
        common::Status status;

        switch (msg.type)
        {
        case RaftMessageType::REQUEST_VOTE_REQUEST:
        {
            const auto *req = std::get_if<RequestVoteRequest>(&msg.payload);
            if (req == nullptr)
                return;
            RequestVoteResponse response;
            status = core_->HandleRequestVote(*req, &response);
            break;
        }
        case RaftMessageType::REQUEST_VOTE_RESPONSE:
        {
            const auto *resp = std::get_if<RequestVoteResponse>(&msg.payload);
            if (resp == nullptr)
                return;
            RaftCore::VoteResponseAction action;
            status = core_->HandleRequestVoteResponse(event.source_node_id,
                                                      *resp, &action);
            break;
        }
        case RaftMessageType::APPEND_ENTRIES_REQUEST:
        {
            const auto *req = std::get_if<AppendEntriesRequest>(&msg.payload);
            if (req == nullptr)
                return;
            AppendEntriesResponse response;
            status = core_->HandleAppendEntries(*req, &response);
            break;
        }
        case RaftMessageType::APPEND_ENTRIES_RESPONSE:
        {
            const auto *resp = std::get_if<AppendEntriesResponse>(&msg.payload);
            if (resp == nullptr)
                return;
            RaftCore::AppendResponseAction action;
            status = core_->HandleAppendEntriesResponse(event.source_node_id,
                                                        *resp, &action);
            break;
        }
        default:
            // InstallSnapshot and unknown types are not handled in T022.
            return;
        }

        if (!status.ok())
        {
            return;
        }
        DrainCoreOutput();
    }

    void RaftRuntime::ProcessProposal(const ProposalEvent &event)
    {
        const common::Status status = core_->Propose(event.payload);
        if (!status.ok())
        {
            return;
        }
        DrainCoreOutput();
    }

    void RaftRuntime::ProcessPersistenceCompletion(
        const PersistenceCompletionEvent &event)
    {
        RaftCore::PersistenceConfirmation confirmation;
        confirmation.status = event.status;
        confirmation.hard_state_persisted =
            event.status.ok() && event.has_hard_state;
        confirmation.stable_index = event.last_log_index;
        confirmation.max_stable_index = event.last_log_index;

        const common::Status confirm_status =
            core_->ConfirmPersistence(confirmation);
        if (!confirm_status.ok())
        {
            return;
        }
        DrainCoreOutput();
    }

    void RaftRuntime::ProcessMembershipChange()
    {
        // Membership change is not implemented in T022.
    }

    void RaftRuntime::ProcessApplyCompletion()
    {
        // Apply completion handling is deferred to T023.
    }

    // -----------------------------------------------------------------------
    //  Drain RaftCore output -> persistence work + released messages
    // -----------------------------------------------------------------------

    common::Status RaftRuntime::DrainCoreOutput()
    {
        RaftCore::RaftOutput output;
        common::Status status = core_->GetOutput(&output);
        if (!status.ok())
        {
            return status;
        }

        // --- Persistence work ---
        if (output.persistence.has_hard_state || output.persistence.has_log_entries())
        {
            PersistenceWork work;
            work.has_hard_state = output.persistence.has_hard_state;
            work.hard_state = output.persistence.hard_state;
            work.max_stable_index = output.persistence.last_log_index;

            if (!output.unstable_entries.empty())
            {
                work.entries = output.unstable_entries;
            }

            const common::Status push_status = persistence_queue_.Push(
                std::move(work));
            if (!push_status.ok())
            {
                return push_status;
            }
        }

        // --- Immediate + newly released messages ---
        {
            std::lock_guard<std::mutex> lock(output_mutex_);
            for (auto &msg : output.immediate_messages)
            {
                released_messages_.push_back(std::move(msg));
            }
            for (auto &msg : output.persisted_messages)
            {
                released_messages_.push_back(std::move(msg));
            }
        }

        return common::Status::OK();
    }

    // -----------------------------------------------------------------------
    //  Emit PersistenceCompletionEvent
    // -----------------------------------------------------------------------

    bool RaftRuntime::EmitPersistenceCompletion(
        PersistenceCompletionEvent &&event)
    {
        const common::Status status = event_queue_.Push(std::move(event));
        return status.ok();
    }

    // -----------------------------------------------------------------------
    //  Persistence worker loop
    // -----------------------------------------------------------------------

    void RaftRuntime::PersistenceLoop()
    {
        while (true)
        {
            PersistenceWork work;
            const common::Status pop_status = persistence_queue_.Pop(&work);

            if (pop_status.code() == common::StatusCode::kRetryLater)
            {
                break;
            }
            if (!pop_status.ok())
            {
                continue;
            }

            PersistenceCompletionEvent completion;
            completion.last_log_index = work.max_stable_index;
            completion.has_hard_state = false;
            completion.has_snapshot = false;
            completion.status = common::Status::OK();

            bool failed = false;

            // 1. Save HardState (if present)
            if (work.has_hard_state)
            {
                common::Status s = storage_->SaveHardState(work.hard_state);
                if (!s.ok())
                {
                    completion.status = s;
                    completion.has_hard_state = true;
                    completion.hard_state = work.hard_state;
                    failed = true;
                }
                else
                {
                    completion.has_hard_state = true;
                    completion.hard_state = work.hard_state;
                }
            }

            // 2. Append log entries (only if not already failed)
            if (!failed && !work.entries.empty())
            {
                common::Status s = storage_->AppendEntries(work.entries);
                if (!s.ok())
                {
                    completion.status = s;
                    failed = true;
                }
            }

            if (failed)
            {
                // Preserve the first failure status.
                // completion.status is already set.
            }

            if (!EmitPersistenceCompletion(std::move(completion)))
            {
                // Event queue full — completion lost. Critical error.
                break;
            }
        }
        persistence_stopped_ = true;
    }

} // namespace cpr::raft
