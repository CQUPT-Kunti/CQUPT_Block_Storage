#include "store/task_manager.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace cpr::store
{
    namespace
    {

        constexpr std::size_t kMaxTaskIdBytes = 4 * 1024;
        constexpr std::size_t kMaxTaskPayloadBytes = 4 * 1024 * 1024;
        constexpr std::uint64_t kInitialTaskGeneration = 1;

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        common::Status Busy(std::string message)
        {
            return common::Status::Busy(std::move(message));
        }

        bool LessByFifo(const TaskRecord &lhs, const TaskRecord &rhs)
        {
            if (lhs.sequence != rhs.sequence)
            {
                return lhs.sequence < rhs.sequence;
            }
            return lhs.task_id < rhs.task_id;
        }

    } // namespace

    common::Status TaskManager::CreateTask(const TaskCreateRequest &request,
                                           TaskRecord *created)
    {
        common::Status status = ValidateTaskId(request.task_id);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateTaskType(request.type);
        if (!status.ok())
        {
            return status;
        }
        if (request.target_payload.size() > kMaxTaskPayloadBytes)
        {
            return Invalid("task target payload is too large");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto existing = tasks_.find(request.task_id);
        if (existing != tasks_.end())
        {
            if (!SameCreateRequest(existing->second, request))
            {
                return Busy("task id already exists with different content");
            }
            if (created != nullptr)
            {
                *created = existing->second;
            }
            return common::Status::OK();
        }
        if (next_sequence_ == std::numeric_limits<std::uint64_t>::max())
        {
            return Busy("task sequence cannot advance");
        }

        TaskRecord record;
        record.task_id = request.task_id;
        record.type = request.type;
        record.state = TaskState::WAITING;
        record.target_payload = request.target_payload;
        record.generation = kInitialTaskGeneration;
        record.sequence = next_sequence_++;

        tasks_.emplace(record.task_id, record);
        if (created != nullptr)
        {
            *created = record;
        }
        return common::Status::OK();
    }

    common::Status TaskManager::GetTask(const TaskId &task_id,
                                        TaskRecord *task) const
    {
        if (task == nullptr)
        {
            return Invalid("task output must not be null");
        }
        common::Status status = ValidateTaskId(task_id);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tasks_.find(task_id);
        if (it == tasks_.end())
        {
            return common::Status::NotFound("task not found");
        }
        *task = it->second;
        return common::Status::OK();
    }

    common::Status TaskManager::ListTasks(std::vector<TaskRecord> *tasks) const
    {
        if (tasks == nullptr)
        {
            return Invalid("task list output must not be null");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        *tasks = SortedTasks(tasks_);
        return common::Status::OK();
    }

    common::Status TaskManager::AssignTask(
        const TaskId &task_id,
        StoreId store_id,
        std::optional<std::uint64_t> expected_generation,
        TaskRecord *updated)
    {
        common::Status status = ValidateTaskId(task_id);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateStoreId(store_id);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tasks_.find(task_id);
        if (it == tasks_.end())
        {
            return common::Status::NotFound("task not found");
        }

        TaskRecord candidate = it->second;
        if (expected_generation &&
            *expected_generation != candidate.generation)
        {
            return Busy("task generation mismatch");
        }
        if (candidate.state == TaskState::RUNNING &&
            candidate.assigned_store == store_id)
        {
            if (updated != nullptr)
            {
                *updated = candidate;
            }
            return common::Status::OK();
        }
        if (candidate.state != TaskState::WAITING)
        {
            return Busy("task is not waiting");
        }
        if (candidate.generation == std::numeric_limits<std::uint64_t>::max())
        {
            return Busy("task generation cannot advance");
        }

        candidate.state = TaskState::RUNNING;
        candidate.assigned_store = store_id;
        ++candidate.generation;

        it->second = candidate;
        if (updated != nullptr)
        {
            *updated = candidate;
        }
        return common::Status::OK();
    }

    common::Status TaskManager::AssignNextWaitingTasks(
        StoreId store_id,
        std::uint32_t max_tasks,
        std::vector<TaskRecord> *assigned)
    {
        if (assigned == nullptr)
        {
            return Invalid("assigned task output must not be null");
        }
        common::Status status = ValidateStoreId(store_id);
        if (!status.ok())
        {
            return status;
        }
        if (max_tasks == 0)
        {
            return Invalid("task poll limit must be positive");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<TaskRecord> waiting;
        for (const auto &item : tasks_)
        {
            if (item.second.state == TaskState::WAITING)
            {
                waiting.push_back(item.second);
            }
        }
        std::sort(waiting.begin(), waiting.end(), LessByFifo);

        std::vector<TaskRecord> result;
        const std::size_t count =
            std::min<std::size_t>(waiting.size(), max_tasks);
        result.reserve(count);
        for (std::size_t i = 0; i < count; ++i)
        {
            TaskRecord candidate = waiting[i];
            if (candidate.generation == std::numeric_limits<std::uint64_t>::max())
            {
                return Busy("task generation cannot advance");
            }
            candidate.state = TaskState::RUNNING;
            candidate.assigned_store = store_id;
            ++candidate.generation;
            result.push_back(candidate);
        }

        for (const TaskRecord &task : result)
        {
            tasks_[task.task_id] = task;
        }
        *assigned = std::move(result);
        return common::Status::OK();
    }

    common::Status TaskManager::CompleteTask(const TaskResultReport &report,
                                             TaskRecord *updated)
    {
        common::Status status = ValidateStoreId(report.store_id);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateTaskId(report.task_id);
        if (!status.ok())
        {
            return status;
        }
        if (report.final_state != TaskState::SUCCESS &&
            report.final_state != TaskState::FAILED)
        {
            return Invalid("task result final state must be success or failed");
        }
        if (report.result_payload.size() > kMaxTaskPayloadBytes)
        {
            return Invalid("task result payload is too large");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = tasks_.find(report.task_id);
        if (it == tasks_.end())
        {
            return common::Status::NotFound("task not found");
        }

        TaskRecord candidate = it->second;
        if (report.expected_generation &&
            *report.expected_generation != candidate.generation)
        {
            return Busy("task generation mismatch");
        }
        if (IsTerminal(candidate.state))
        {
            if (SameResult(candidate, report))
            {
                if (updated != nullptr)
                {
                    *updated = candidate;
                }
                return common::Status::OK();
            }
            return Busy("task already has a different final result");
        }
        if (candidate.state != TaskState::RUNNING)
        {
            return Busy("task is not running");
        }
        if (candidate.assigned_store != report.store_id)
        {
            return Busy("task is assigned to a different store");
        }
        if (candidate.generation == std::numeric_limits<std::uint64_t>::max())
        {
            return Busy("task generation cannot advance");
        }

        candidate.state = report.final_state;
        candidate.result_payload = report.result_payload;
        ++candidate.generation;

        it->second = candidate;
        if (updated != nullptr)
        {
            *updated = candidate;
        }
        return common::Status::OK();
    }

    common::Status TaskManager::ExportPersistent(
        std::vector<TaskRecord> *tasks) const
    {
        return ListTasks(tasks);
    }

    common::Status TaskManager::RestorePersistent(
        const std::vector<TaskRecord> &tasks)
    {
        TaskMap candidate;
        std::uint64_t next_sequence = 1;
        for (const TaskRecord &task : tasks)
        {
            common::Status status = ValidateTaskRecord(task);
            if (!status.ok())
            {
                return status;
            }
            if (candidate.find(task.task_id) != candidate.end())
            {
                return Busy("duplicate task id in persistent view");
            }
            candidate.emplace(task.task_id, task);
            if (task.sequence >= next_sequence)
            {
                if (task.sequence == std::numeric_limits<std::uint64_t>::max())
                {
                    return Busy("task sequence cannot advance");
                }
                next_sequence = task.sequence + 1;
            }
        }

        std::lock_guard<std::mutex> lock(mutex_);
        tasks_ = std::move(candidate);
        next_sequence_ = next_sequence;
        return common::Status::OK();
    }

    common::Status TaskManager::ValidateTaskId(const TaskId &task_id)
    {
        if (task_id.empty())
        {
            return Invalid("task id must not be empty");
        }
        if (task_id.size() > kMaxTaskIdBytes)
        {
            return Invalid("task id is too large");
        }
        return common::Status::OK();
    }

    common::Status TaskManager::ValidateStoreId(StoreId store_id)
    {
        if (store_id == 0)
        {
            return Invalid("store id must be positive");
        }
        return common::Status::OK();
    }

    common::Status TaskManager::ValidateTaskType(TaskType type)
    {
        if (type != TaskType::CREATE &&
            type != TaskType::DELETE &&
            type != TaskType::COPY &&
            type != TaskType::CUSTOM)
        {
            return Invalid("task type is invalid");
        }
        return common::Status::OK();
    }

    common::Status TaskManager::ValidateTaskState(TaskState state)
    {
        if (state != TaskState::WAITING &&
            state != TaskState::RUNNING &&
            state != TaskState::SUCCESS &&
            state != TaskState::FAILED)
        {
            return Invalid("task state is invalid");
        }
        return common::Status::OK();
    }

    common::Status TaskManager::ValidateTaskRecord(const TaskRecord &task)
    {
        common::Status status = ValidateTaskId(task.task_id);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateTaskType(task.type);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateTaskState(task.state);
        if (!status.ok())
        {
            return status;
        }
        if (task.target_payload.size() > kMaxTaskPayloadBytes ||
            task.result_payload.size() > kMaxTaskPayloadBytes)
        {
            return Invalid("task payload is too large");
        }
        if (task.generation == 0)
        {
            return Invalid("task generation must be positive");
        }
        if (task.sequence == 0)
        {
            return Invalid("task sequence must be positive");
        }
        if (task.state == TaskState::WAITING && task.assigned_store != 0)
        {
            return Invalid("waiting task must not have an assigned store");
        }
        if (task.state == TaskState::RUNNING)
        {
            status = ValidateStoreId(task.assigned_store);
            if (!status.ok())
            {
                return status;
            }
            if (!task.result_payload.empty())
            {
                return Invalid("running task must not have a result");
            }
        }
        if (IsTerminal(task.state))
        {
            status = ValidateStoreId(task.assigned_store);
            if (!status.ok())
            {
                return status;
            }
        }
        return common::Status::OK();
    }

    bool TaskManager::IsTerminal(TaskState state) noexcept
    {
        return state == TaskState::SUCCESS || state == TaskState::FAILED;
    }

    bool TaskManager::SameCreateRequest(const TaskRecord &existing,
                                        const TaskCreateRequest &request)
    {
        return existing.type == request.type &&
               existing.target_payload == request.target_payload;
    }

    bool TaskManager::SameResult(const TaskRecord &existing,
                                 const TaskResultReport &report)
    {
        return existing.assigned_store == report.store_id &&
               existing.state == report.final_state &&
               existing.result_payload == report.result_payload;
    }

    std::vector<TaskRecord> TaskManager::SortedTasks(const TaskMap &tasks)
    {
        std::vector<TaskRecord> out;
        out.reserve(tasks.size());
        for (const auto &item : tasks)
        {
            out.push_back(item.second);
        }
        std::sort(out.begin(), out.end(), LessByFifo);
        return out;
    }

} // namespace cpr::store
