#include "store/simple_task_dispatcher.h"

#include <algorithm>

namespace cpr::store
{

    SimpleTaskDispatcher::SimpleTaskDispatcher(TaskManager *tasks,
                                               std::uint32_t max_poll_tasks)
        : tasks_(tasks),
          max_poll_tasks_(max_poll_tasks)
    {
    }

    common::Status SimpleTaskDispatcher::PollTasks(
        const PollTasksRequest &request,
        PollTasksResult *result)
    {
        if (result == nullptr)
        {
            return common::Status::InvalidArgument("poll task result must not be null");
        }
        if (tasks_ == nullptr)
        {
            return common::Status::InvalidArgument("task manager must not be null");
        }
        if (request.store_id == 0)
        {
            return common::Status::InvalidArgument("store id must be positive");
        }
        if (request.max_tasks == 0)
        {
            return common::Status::InvalidArgument("task poll limit must be positive");
        }
        if (max_poll_tasks_ == 0)
        {
            return common::Status::InvalidArgument("dispatcher max poll limit must be positive");
        }

        const std::uint32_t limit =
            std::min(request.max_tasks, max_poll_tasks_);
        PollTasksResult candidate;
        common::Status status =
            tasks_->AssignNextWaitingTasks(request.store_id,
                                           limit,
                                           &candidate.tasks);
        if (!status.ok())
        {
            return status;
        }

        *result = std::move(candidate);
        return common::Status::OK();
    }

    common::Status SimpleTaskDispatcher::ReportTaskResult(
        const TaskResultReport &report,
        TaskRecord *updated)
    {
        if (tasks_ == nullptr)
        {
            return common::Status::InvalidArgument("task manager must not be null");
        }
        return tasks_->CompleteTask(report, updated);
    }

} // namespace cpr::store
