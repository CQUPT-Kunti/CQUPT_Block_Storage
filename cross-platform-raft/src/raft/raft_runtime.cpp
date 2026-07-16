#include "raft/raft_runtime.h"

#include <algorithm>
#include <deque>
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

        common::Status NotFound(std::string message)
        {
            return common::Status::NotFound(std::move(message));
        }

        common::Status NotSupported(std::string message)
        {
            return common::Status::RetryLater(std::move(message));
        }

        common::Status ResourceExhausted(std::string message)
        {
            return common::Status::ResourceExhausted(std::move(message));
        }

    } // namespace

    // ===================================================================
    //  Construction / Destruction
    // ===================================================================

    RaftRuntime::RaftRuntime(const Options &options)
        : event_queue_(options.event_queue_capacity),
          persistence_queue_(options.persistence_queue_capacity),
          apply_queue_(options.apply_queue_capacity),
          peer_queue_capacity_(options.peer_queue_capacity),
          proposal_results_{.max_capacity = options.proposal_result_queue_capacity}
    {
    }

    RaftRuntime::~RaftRuntime()
    {
        if (state_ == RuntimeState::RUNNING ||
            state_ == RuntimeState::STOPPING)
        {
            RequestShutdown();
            WaitForShutdown();
        }
    }

    // ===================================================================
    //  Lifecycle
    // ===================================================================

    common::Status RaftRuntime::Initialize(RaftCore *core, IRaftStorage *storage,
                                           ApplyFn apply_fn)
    {
        if (state_ != RuntimeState::CREATED)
        {
            return Busy("runtime is already initialized");
        }
        if (core == nullptr || storage == nullptr)
        {
            return InvalidArgument("core and storage must not be null");
        }

        if (peer_queue_capacity_ == 0)
        {
            return InvalidArgument(
                "peer queue capacity must be greater than zero");
        }

        core_ = core;
        storage_ = storage;
        apply_fn_ = std::move(apply_fn);

        // Build peer queues from RaftCore's voter/learner lists.
        const auto &voters = core_->voter_ids();
        const auto &learners = core_->learner_ids();
        const common::NodeId local_id = core_->node_id();

        std::vector<common::NodeId> all_peers;
        all_peers.reserve(voters.size() + learners.size());
        for (auto id : voters)
            if (id != local_id)
                all_peers.push_back(id);
        for (auto id : learners)
            if (id != local_id)
                all_peers.push_back(id);

        std::sort(all_peers.begin(), all_peers.end());
        all_peers.erase(std::unique(all_peers.begin(), all_peers.end()),
                        all_peers.end());

        {
            std::lock_guard<std::mutex> lock(peer_queues_mutex_);
            for (auto peer_id : all_peers)
            {
                peer_queues_.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(peer_id),
                    std::forward_as_tuple(peer_queue_capacity_));
            }
        }

        state_ = RuntimeState::INITIALIZED;
        return common::Status::OK();
    }

    common::Status RaftRuntime::Start()
    {
        if (state_ != RuntimeState::INITIALIZED)
        {
            return Busy("runtime is not in initialized state");
        }
        state_ = RuntimeState::RUNNING;
        protocol_thread_ = std::thread(&RaftRuntime::ProtocolLoop, this);
        persistence_thread_ = std::thread(&RaftRuntime::PersistenceLoop, this);
        apply_thread_ = std::thread(&RaftRuntime::ApplyLoop, this);
        return common::Status::OK();
    }

    common::Status RaftRuntime::RequestShutdown()
    {
        if (state_ == RuntimeState::STOPPED ||
            state_ == RuntimeState::STOPPING)
        {
            return Busy("runtime is already stopping or stopped");
        }
        if (state_ != RuntimeState::RUNNING)
        {
            return Busy("runtime is not running");
        }

        state_ = RuntimeState::STOPPING;
        event_queue_.Close();
        persistence_queue_.Close();
        apply_queue_.Close();
        CloseAllPeerQueues();
        return common::Status::OK();
    }

    void RaftRuntime::WaitForShutdown()
    {
        if (protocol_thread_.joinable())
            protocol_thread_.join();
        if (persistence_thread_.joinable())
            persistence_thread_.join();
        if (apply_thread_.joinable())
            apply_thread_.join();
        state_ = RuntimeState::STOPPED;
    }

    RuntimeState RaftRuntime::state() const noexcept
    {
        return state_;
    }

    // ===================================================================
    //  Enqueue (private)
    // ===================================================================

    common::Status RaftRuntime::Enqueue(RuntimeEvent &&event)
    {
        if (state_ != RuntimeState::RUNNING)
        {
            return Busy("runtime is not running");
        }
        return event_queue_.Push(std::move(event));
    }

    // ===================================================================
    //  Enqueue implementations
    // ===================================================================

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
        const MembershipChangeEvent &)
    {
        return NotSupported("membership change is not implemented");
    }

    common::Status RaftRuntime::EnqueueMembershipChange(
        MembershipChangeEvent &&)
    {
        return NotSupported("membership change is not implemented");
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

    // ===================================================================
    //  Peer output
    // ===================================================================

    common::Status RaftRuntime::TryDequeuePeerMessage(
        common::NodeId peer_id, RaftMessage *message)
    {
        if (message == nullptr)
        {
            return InvalidArgument("message output must not be null");
        }
        std::lock_guard<std::mutex> lock(peer_queues_mutex_);
        auto it = peer_queues_.find(peer_id);
        if (it == peer_queues_.end())
        {
            return NotFound("unknown peer");
        }
        PeerMessageQueue &q = it->second;
        if (q.messages.empty())
        {
            return NotFound("peer queue is empty");
        }
        *message = std::move(q.messages.front());
        q.messages.pop_front();
        return common::Status::OK();
    }

    bool RaftRuntime::IsPeerRegistered(common::NodeId peer_id) const noexcept
    {
        std::lock_guard<std::mutex> lock(peer_queues_mutex_);
        return peer_queues_.find(peer_id) != peer_queues_.end();
    }

    std::size_t RaftRuntime::PeerQueueSize(common::NodeId peer_id) const
    {
        std::lock_guard<std::mutex> lock(peer_queues_mutex_);
        auto it = peer_queues_.find(peer_id);
        if (it == peer_queues_.end())
            return 0;
        return it->second.messages.size();
    }

    std::vector<ProposalResult> RaftRuntime::CollectProposalResults(
        std::size_t max_count)
    {
        std::vector<ProposalResult> results;
        results.reserve(max_count);
        std::lock_guard<std::mutex> lock(results_mutex_);
        for (std::size_t i = 0; i < max_count && !proposal_results_.items.empty(); ++i)
        {
            results.push_back(std::move(proposal_results_.items.front()));
            proposal_results_.items.pop_front();
        }
        return results;
    }

    // ===================================================================
    //  DistributeOutputMessages
    // ===================================================================

    common::Status RaftRuntime::DistributeOutputMessages(
        std::vector<RaftMessage> &&messages)
    {
        for (auto &msg : messages)
        {
            const common::NodeId target = msg.target_node_id;
            if (target == core_->node_id())
                continue;

            std::lock_guard<std::mutex> lock(peer_queues_mutex_);
            auto it = peer_queues_.find(target);
            if (it == peer_queues_.end())
            {
                return NotFound("output message targets unknown peer " +
                                std::to_string(target));
            }
            PeerMessageQueue &q = it->second;
            if (q.closed)
            {
                return Busy("peer queue is closed");
            }
            if (q.messages.size() >= q.max_capacity)
            {
                return ResourceExhausted("peer queue is full");
            }
            q.messages.push_back(std::move(msg));
        }
        return common::Status::OK();
    }

    void RaftRuntime::CloseAllPeerQueues()
    {
        std::lock_guard<std::mutex> lock(peer_queues_mutex_);
        for (auto &[id, q] : peer_queues_)
        {
            (void)id;
            q.closed = true;
        }
    }

    // ===================================================================
    //  Queries
    // ===================================================================

    bool RaftRuntime::shutdown_requested() const noexcept
    {
        return state_ == RuntimeState::STOPPING ||
               state_ == RuntimeState::STOPPED;
    }

    bool RaftRuntime::accepting_events() const noexcept
    {
        return state_ == RuntimeState::RUNNING;
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

    std::size_t RaftRuntime::apply_queue_size() const
    {
        return apply_queue_.size();
    }

    std::size_t RaftRuntime::peer_count() const noexcept
    {
        std::lock_guard<std::mutex> lock(peer_queues_mutex_);
        return peer_queues_.size();
    }

    // ===================================================================
    //  Protocol loop
    // ===================================================================

    void RaftRuntime::ProtocolLoop()
    {
        while (true)
        {
            RuntimeEvent event;
            const common::Status pop_status = event_queue_.Pop(&event);

            if (pop_status.code() == common::StatusCode::kRetryLater)
                break;
            if (!pop_status.ok())
                continue;

            if (std::holds_alternative<TickEvent>(event))
                ProcessTick();
            else if (std::holds_alternative<MessageEvent>(event))
                ProcessMessage(std::get<MessageEvent>(event));
            else if (std::holds_alternative<ProposalEvent>(event))
                ProcessProposal(std::get<ProposalEvent>(event));
            else if (std::holds_alternative<PersistenceCompletionEvent>(event))
                ProcessPersistenceCompletion(
                    std::get<PersistenceCompletionEvent>(event));
            else if (std::holds_alternative<ApplyCompletionEvent>(event))
                ProcessApplyCompletion(
                    std::get<ApplyCompletionEvent>(event));
            else if (std::holds_alternative<MembershipChangeEvent>(event))
                ProcessMembershipChange();
            else if (std::holds_alternative<ShutdownEvent>(event))
                break;
        }
        protocol_stopped_ = true;
    }

    // ===================================================================
    //  Event handlers (protocol thread only)
    // ===================================================================

    void RaftRuntime::ProcessTick()
    {
        RaftCore::TickAction action = RaftCore::TickAction::NONE;
        const common::Status status = core_->Tick(&action);
        if (!status.ok())
            return;
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
            status = core_->HandleRequestVoteResponse(
                event.source_node_id, *resp, &action);
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
            status = core_->HandleAppendEntriesResponse(
                event.source_node_id, *resp, &action);
            break;
        }
        default:
            return;
        }

        if (!status.ok())
            return;
        DrainCoreOutput();
    }

    void RaftRuntime::ProcessProposal(const ProposalEvent &event)
    {
        const common::Status status = core_->Propose(event.payload);
        if (!status.ok())
        {
            EmitProposalResult(event.proposal_id,
                               common::kInvalidLogIndex, status, true);
            return;
        }
        pending_proposals_[event.proposal_id] = core_->log().last_index();
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
            return;
        DrainCoreOutput();
    }

    void RaftRuntime::ProcessMembershipChange()
    {
        // Not implemented.
    }

    void RaftRuntime::ProcessApplyCompletion(
        const ApplyCompletionEvent &event)
    {
        if (!event.status.ok())
        {
            if (event.proposal_id != 0)
            {
                EmitProposalResult(event.proposal_id,
                                   event.applied_index, event.status, true);
                pending_proposals_.erase(event.proposal_id);
            }
            return;
        }

        const common::LogIndex expected = core_->log().applied_index() + 1;
        if (event.applied_index != expected)
            return;

        const common::Status confirm_status =
            core_->ConfirmApplied(event.applied_index);
        if (!confirm_status.ok())
            return;

        if (event.proposal_id != 0)
        {
            EmitProposalResult(event.proposal_id,
                               event.applied_index,
                               common::Status::OK(), true);
            pending_proposals_.erase(event.proposal_id);
        }
    }

    // ===================================================================
    //  Drain RaftCore output
    // ===================================================================

    common::Status RaftRuntime::DrainCoreOutput()
    {
        RaftCore::RaftOutput output;
        common::Status status = core_->GetOutput(&output);
        if (!status.ok())
            return status;

        // Persistence work
        if (output.persistence.has_hard_state ||
            output.persistence.has_log_entries())
        {
            PersistenceWork work;
            work.has_hard_state = output.persistence.has_hard_state;
            work.hard_state = output.persistence.hard_state;
            work.max_stable_index = output.persistence.last_log_index;
            if (!output.unstable_entries.empty())
                work.entries = output.unstable_entries;
            status = persistence_queue_.Push(std::move(work));
            if (!status.ok())
                return status;
        }

        // Distribute immediate messages
        if (!output.immediate_messages.empty())
        {
            status = DistributeOutputMessages(
                std::move(output.immediate_messages));
            if (!status.ok())
                return status;
        }

        // Distribute persisted messages
        if (!output.persisted_messages.empty())
        {
            status = DistributeOutputMessages(
                std::move(output.persisted_messages));
            if (!status.ok())
                return status;
        }

        // Schedule apply work
        status = ScheduleApplyWork();
        if (!status.ok())
            return status;

        return common::Status::OK();
    }

    // ===================================================================
    //  Schedule apply work
    // ===================================================================

    common::Status RaftRuntime::ScheduleApplyWork()
    {
        RaftCore::ApplyRange range;
        common::Status status = core_->GetApplyReadyRange(&range);
        if (!status.ok())
            return status;
        if (range.empty())
            return common::Status::OK();

        common::LogIndex schedule_start = range.begin;
        if (next_apply_schedule_ != common::kInvalidLogIndex &&
            next_apply_schedule_ >= schedule_start)
            schedule_start = next_apply_schedule_ + 1;
        if (schedule_start > range.end)
            return common::Status::OK();

        for (common::LogIndex idx = schedule_start; idx <= range.end; ++idx)
        {
            LogEntry entry;
            status = core_->log().GetEntry(idx, &entry);
            if (!status.ok())
                return status;

            ApplyWork work;
            work.index = entry.index;
            work.term = entry.term;
            work.type = entry.type;
            work.payload = entry.payload;

            for (const auto &[pid, log_idx] : pending_proposals_)
                if (log_idx == idx)
                {
                    work.proposal_id = pid;
                    break;
                }

            status = apply_queue_.Push(std::move(work));
            if (!status.ok())
                return status;
            next_apply_schedule_ = idx;
        }
        return common::Status::OK();
    }

    // ===================================================================
    //  Emit helpers
    // ===================================================================

    bool RaftRuntime::EmitPersistenceCompletion(
        PersistenceCompletionEvent &&event)
    {
        return event_queue_.Push(std::move(event)).ok();
    }

    bool RaftRuntime::EmitApplyCompletion(ApplyCompletionEvent &&event)
    {
        return event_queue_.Push(std::move(event)).ok();
    }

    void RaftRuntime::EmitProposalResult(std::uint64_t proposal_id,
                                         common::LogIndex log_index,
                                         common::Status status,
                                         bool final_result)
    {
        ProposalResult result;
        result.proposal_id = proposal_id;
        result.log_index = log_index;
        result.status = std::move(status);
        result.final_result = final_result;
        std::lock_guard<std::mutex> lock(results_mutex_);
        if (proposal_results_.items.size() < proposal_results_.max_capacity)
        {
            proposal_results_.items.push_back(std::move(result));
        }
    }

    // ===================================================================
    //  Persistence worker loop
    // ===================================================================

    void RaftRuntime::PersistenceLoop()
    {
        while (true)
        {
            PersistenceWork work;
            const common::Status pop_status = persistence_queue_.Pop(&work);
            if (pop_status.code() == common::StatusCode::kRetryLater)
                break;
            if (!pop_status.ok())
                continue;

            PersistenceCompletionEvent completion;
            completion.last_log_index = work.max_stable_index;
            completion.has_hard_state = false;
            completion.has_snapshot = false;
            completion.status = common::Status::OK();
            bool failed = false;

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
            if (!failed && !work.entries.empty())
            {
                common::Status s = storage_->AppendEntries(work.entries);
                if (!s.ok())
                {
                    completion.status = s;
                    failed = true;
                }
            }

            if (!EmitPersistenceCompletion(std::move(completion)))
                break;
        }
        persistence_stopped_ = true;
    }

    // ===================================================================
    //  Apply worker loop
    // ===================================================================

    void RaftRuntime::ApplyLoop()
    {
        while (true)
        {
            ApplyWork work;
            const common::Status pop_status = apply_queue_.Pop(&work);
            if (pop_status.code() == common::StatusCode::kRetryLater)
                break;
            if (!pop_status.ok())
                continue;

            ApplyCompletionEvent completion;
            completion.applied_index = work.index;
            completion.proposal_id = work.proposal_id;

            if (!apply_fn_)
            {
                completion.status = common::Status::OK();
            }
            else
            {
                LogEntry entry;
                entry.index = work.index;
                entry.term = work.term;
                entry.type = work.type;
                entry.payload = std::move(work.payload);
                completion.status = apply_fn_(entry);
            }

            if (!EmitApplyCompletion(std::move(completion)))
                break;
        }
        apply_stopped_ = true;
    }

} // namespace cpr::raft
