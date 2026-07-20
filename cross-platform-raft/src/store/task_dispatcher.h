#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "store/task_types.h"

namespace cpr::store
{

    struct PollTasksRequest
    {
        StoreId store_id = 0;
        std::uint32_t max_tasks = 0;
    };

    struct PollTasksResult
    {
        std::vector<TaskRecord> tasks;
    };

    class ITaskDispatcher
    {
    public:
        virtual ~ITaskDispatcher() = default;

        virtual common::Status PollTasks(const PollTasksRequest &request,
                                         PollTasksResult *result) = 0;
        virtual common::Status ReportTaskResult(
            const TaskResultReport &report,
            TaskRecord *updated = nullptr) = 0;
    };

} // namespace cpr::store
