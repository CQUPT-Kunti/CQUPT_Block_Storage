#include <algorithm>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "store/simple_task_dispatcher.h"
#include "store/task_dispatcher.h"
#include "store/task_manager.h"

namespace cpr::store
{
    namespace
    {

        using cpr::common::StatusCode;

        TaskCreateRequest MakeCreate(const char *id)
        {
            TaskCreateRequest request;
            request.task_id = id;
            request.type = TaskType::CUSTOM;
            request.target_payload = {0x01};
            return request;
        }

        PollTasksRequest Poll(StoreId store_id, std::uint32_t max_tasks)
        {
            PollTasksRequest request;
            request.store_id = store_id;
            request.max_tasks = max_tasks;
            return request;
        }

        class EmptyDispatcher final : public ITaskDispatcher
        {
        public:
            common::Status PollTasks(const PollTasksRequest &,
                                     PollTasksResult *result) override
            {
                if (result == nullptr)
                {
                    return common::Status::InvalidArgument("result must not be null");
                }
                result->tasks.clear();
                return common::Status::OK();
            }

            common::Status ReportTaskResult(const TaskResultReport &,
                                            TaskRecord *) override
            {
                return common::Status::OK();
            }
        };

        TEST(TaskDispatcherTest, PollAssignsWaitingTasksInFifoOrderWithinLimit)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-2")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-3")).ok());

            SimpleTaskDispatcher dispatcher(&manager);
            PollTasksResult result;
            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 2), &result).ok());

            ASSERT_EQ(result.tasks.size(), 2U);
            EXPECT_EQ(result.tasks[0].task_id, "task-1");
            EXPECT_EQ(result.tasks[1].task_id, "task-2");
            EXPECT_EQ(result.tasks[0].assigned_store, 7U);
            EXPECT_EQ(result.tasks[0].state, TaskState::RUNNING);

            TaskRecord task;
            ASSERT_TRUE(manager.GetTask("task-1", &task).ok());
            EXPECT_EQ(task.state, TaskState::RUNNING);
            EXPECT_EQ(task.assigned_store, 7U);
            EXPECT_EQ(task.generation, 2U);
        }

        TEST(TaskDispatcherTest, PollRejectsInvalidStoreAndZeroLimitWithoutMutation)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            SimpleTaskDispatcher dispatcher(&manager);
            PollTasksResult result;
            result.tasks.push_back(TaskRecord{});

            EXPECT_EQ(dispatcher.PollTasks(Poll(0, 1), &result).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.tasks.size(), 1U);

            EXPECT_EQ(dispatcher.PollTasks(Poll(1, 0), &result).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.tasks.size(), 1U);

            TaskRecord task;
            ASSERT_TRUE(manager.GetTask("task-1", &task).ok());
            EXPECT_EQ(task.state, TaskState::WAITING);
            EXPECT_EQ(task.generation, 1U);
        }

        TEST(TaskDispatcherTest, PollUsesDispatcherMaxAsUpperBoundAndReturnsEmptyWhenNoWaitingTasks)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-2")).ok());
            SimpleTaskDispatcher dispatcher(&manager, 1);

            PollTasksResult result;
            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 10), &result).ok());
            ASSERT_EQ(result.tasks.size(), 1U);

            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 10), &result).ok());
            ASSERT_EQ(result.tasks.size(), 1U);

            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 10), &result).ok());
            EXPECT_TRUE(result.tasks.empty());
        }

        TEST(TaskDispatcherTest, RepeatedPollDoesNotReturnRunningOrTerminalTasks)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            SimpleTaskDispatcher dispatcher(&manager);

            PollTasksResult result;
            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 1), &result).ok());
            ASSERT_EQ(result.tasks.size(), 1U);

            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 1), &result).ok());
            EXPECT_TRUE(result.tasks.empty());

            TaskResultReport report;
            report.store_id = 7;
            report.task_id = "task-1";
            report.final_state = TaskState::SUCCESS;
            ASSERT_TRUE(dispatcher.ReportTaskResult(report).ok());

            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 1), &result).ok());
            EXPECT_TRUE(result.tasks.empty());
        }

        TEST(TaskDispatcherTest, DifferentStoresCannotPollTheSameTask)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            SimpleTaskDispatcher dispatcher(&manager);

            PollTasksResult first;
            PollTasksResult second;
            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 1), &first).ok());
            ASSERT_TRUE(dispatcher.PollTasks(Poll(8, 1), &second).ok());

            ASSERT_EQ(first.tasks.size(), 1U);
            EXPECT_TRUE(second.tasks.empty());
            EXPECT_EQ(first.tasks[0].assigned_store, 7U);
        }

        TEST(TaskDispatcherTest, ConcurrentPollDoesNotDuplicateAssignments)
        {
            TaskManager manager;
            for (int i = 0; i < 20; ++i)
            {
                ASSERT_TRUE(manager.CreateTask(MakeCreate(("task-" + std::to_string(i)).c_str())).ok());
            }
            SimpleTaskDispatcher dispatcher(&manager);

            std::mutex mutex;
            std::vector<TaskId> seen;
            auto poll = [&](StoreId store)
            {
                PollTasksResult result;
                const common::Status status = dispatcher.PollTasks(Poll(store, 20), &result);
                ASSERT_TRUE(status.ok()) << status.ToString();
                std::lock_guard<std::mutex> lock(mutex);
                for (const TaskRecord &task : result.tasks)
                {
                    seen.push_back(task.task_id);
                }
            };

            std::thread first(poll, 7);
            std::thread second(poll, 8);
            first.join();
            second.join();

            std::sort(seen.begin(), seen.end());
            auto duplicate = std::adjacent_find(seen.begin(), seen.end());
            EXPECT_EQ(duplicate, seen.end());
            EXPECT_EQ(seen.size(), 20U);
        }

        TEST(TaskDispatcherTest, CorrectStoreCanReportSuccessAndFailedResult)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-success")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-failed")).ok());
            SimpleTaskDispatcher dispatcher(&manager);

            PollTasksResult polled;
            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 2), &polled).ok());
            ASSERT_EQ(polled.tasks.size(), 2U);

            TaskResultReport success;
            success.store_id = 7;
            success.task_id = "task-success";
            success.final_state = TaskState::SUCCESS;
            success.result_payload = {0x10};
            TaskRecord updated;
            ASSERT_TRUE(dispatcher.ReportTaskResult(success, &updated).ok());
            EXPECT_EQ(updated.state, TaskState::SUCCESS);
            EXPECT_EQ(updated.result_payload, (TaskPayload{0x10}));

            TaskResultReport failed;
            failed.store_id = 7;
            failed.task_id = "task-failed";
            failed.final_state = TaskState::FAILED;
            failed.result_payload = {0x20};
            ASSERT_TRUE(dispatcher.ReportTaskResult(failed, &updated).ok());
            EXPECT_EQ(updated.state, TaskState::FAILED);
            EXPECT_EQ(updated.result_payload, (TaskPayload{0x20}));
        }

        TEST(TaskDispatcherTest, ReportRejectsWrongStoreWaitingTaskAndConflictingResults)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-2")).ok());
            SimpleTaskDispatcher dispatcher(&manager);

            TaskResultReport waiting;
            waiting.store_id = 7;
            waiting.task_id = "task-1";
            waiting.final_state = TaskState::SUCCESS;
            EXPECT_EQ(dispatcher.ReportTaskResult(waiting).code(), StatusCode::kBusy);

            PollTasksResult polled;
            ASSERT_TRUE(dispatcher.PollTasks(Poll(7, 1), &polled).ok());

            TaskResultReport wrong_store = waiting;
            wrong_store.task_id = "task-1";
            wrong_store.store_id = 8;
            EXPECT_EQ(dispatcher.ReportTaskResult(wrong_store).code(), StatusCode::kBusy);

            TaskResultReport success = waiting;
            success.task_id = "task-1";
            success.result_payload = {0x10};
            ASSERT_TRUE(dispatcher.ReportTaskResult(success).ok());
            ASSERT_TRUE(dispatcher.ReportTaskResult(success).ok());

            TaskResultReport conflict = success;
            conflict.result_payload = {0x11};
            EXPECT_EQ(dispatcher.ReportTaskResult(conflict).code(), StatusCode::kBusy);
        }

        TEST(TaskDispatcherTest, AlternateDispatcherCanReplaceSimpleDispatcherThroughInterface)
        {
            EmptyDispatcher empty;
            ITaskDispatcher *dispatcher = &empty;

            PollTasksResult result;
            ASSERT_TRUE(dispatcher->PollTasks(Poll(7, 1), &result).ok());
            EXPECT_TRUE(result.tasks.empty());

            TaskResultReport report;
            report.store_id = 7;
            report.task_id = "task-1";
            report.final_state = TaskState::SUCCESS;
            EXPECT_TRUE(dispatcher->ReportTaskResult(report).ok());
        }

    } // namespace
} // namespace cpr::store
