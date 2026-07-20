#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "store/task_types.h"

namespace cpr::store
{

    class TaskManager
    {
    public:
        TaskManager() = default;

        common::Status CreateTask(const TaskCreateRequest &request,
                                  TaskRecord *created = nullptr);
        common::Status GetTask(const TaskId &task_id, TaskRecord *task) const;
        common::Status ListTasks(std::vector<TaskRecord> *tasks) const;

        common::Status AssignTask(const TaskId &task_id,
                                  StoreId store_id,
                                  std::optional<std::uint64_t> expected_generation,
                                  TaskRecord *updated = nullptr);
        common::Status AssignNextWaitingTasks(StoreId store_id,
                                              std::uint32_t max_tasks,
                                              std::vector<TaskRecord> *assigned);
        common::Status CompleteTask(const TaskResultReport &report,
                                    TaskRecord *updated = nullptr);

        common::Status ExportPersistent(std::vector<TaskRecord> *tasks) const;
        common::Status RestorePersistent(const std::vector<TaskRecord> &tasks);

    private:
        using TaskMap = std::map<TaskId, TaskRecord>;

        static common::Status ValidateTaskId(const TaskId &task_id);
        static common::Status ValidateStoreId(StoreId store_id);
        static common::Status ValidateTaskType(TaskType type);
        static common::Status ValidateTaskState(TaskState state);
        static common::Status ValidateTaskRecord(const TaskRecord &task);
        static bool IsTerminal(TaskState state) noexcept;
        static bool SameCreateRequest(const TaskRecord &existing,
                                      const TaskCreateRequest &request);
        static bool SameResult(const TaskRecord &existing,
                               const TaskResultReport &report);
        static std::vector<TaskRecord> SortedTasks(const TaskMap &tasks);

        mutable std::mutex mutex_;
        TaskMap tasks_;
        std::uint64_t next_sequence_ = 1;
    };

} // namespace cpr::store
