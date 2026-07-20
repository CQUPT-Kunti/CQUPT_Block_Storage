#pragma once

#include <cstdint>

#include "store/task_dispatcher.h"
#include "store/task_manager.h"

namespace cpr::store
{

    class SimpleTaskDispatcher final : public ITaskDispatcher
    {
    public:
        explicit SimpleTaskDispatcher(TaskManager *tasks,
                                      std::uint32_t max_poll_tasks = 1024);

        common::Status PollTasks(const PollTasksRequest &request,
                                 PollTasksResult *result) override;
        common::Status ReportTaskResult(const TaskResultReport &report,
                                        TaskRecord *updated = nullptr) override;

    private:
        TaskManager *tasks_ = nullptr;
        std::uint32_t max_poll_tasks_ = 0;
    };

} // namespace cpr::store
