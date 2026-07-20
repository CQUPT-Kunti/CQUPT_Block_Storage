#include <cstdint>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "store/task_manager.h"
#include "store/task_types.h"

namespace cpr::store
{
    namespace
    {

        using cpr::common::StatusCode;

        static_assert(static_cast<std::uint8_t>(TaskType::CREATE) == 0,
                      "TaskType CREATE value changed");
        static_assert(static_cast<std::uint8_t>(TaskType::DELETE) == 1,
                      "TaskType DELETE value changed");
        static_assert(static_cast<std::uint8_t>(TaskType::COPY) == 2,
                      "TaskType COPY value changed");
        static_assert(static_cast<std::uint8_t>(TaskType::CUSTOM) == 3,
                      "TaskType CUSTOM value changed");
        static_assert(static_cast<std::uint8_t>(TaskState::WAITING) == 0,
                      "TaskState WAITING value changed");
        static_assert(static_cast<std::uint8_t>(TaskState::RUNNING) == 1,
                      "TaskState RUNNING value changed");
        static_assert(static_cast<std::uint8_t>(TaskState::SUCCESS) == 2,
                      "TaskState SUCCESS value changed");
        static_assert(static_cast<std::uint8_t>(TaskState::FAILED) == 3,
                      "TaskState FAILED value changed");

        TaskCreateRequest MakeCreate(const char *id,
                                     TaskType type = TaskType::CREATE,
                                     TaskPayload payload = {0x01})
        {
            TaskCreateRequest request;
            request.task_id = id;
            request.type = type;
            request.target_payload = std::move(payload);
            return request;
        }

        TaskRecord MustGet(TaskManager *manager, const char *id)
        {
            TaskRecord task;
            EXPECT_TRUE(manager->GetTask(id, &task).ok());
            return task;
        }

        TEST(TaskManagerTest, CreateValidTaskInitializesWaitingRecord)
        {
            TaskManager manager;
            TaskRecord created;

            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1"), &created).ok());

            EXPECT_EQ(created.task_id, "task-1");
            EXPECT_EQ(created.type, TaskType::CREATE);
            EXPECT_EQ(created.state, TaskState::WAITING);
            EXPECT_EQ(created.assigned_store, 0U);
            EXPECT_TRUE(created.result_payload.empty());
            EXPECT_EQ(created.generation, 1U);
            EXPECT_EQ(created.sequence, 1U);
        }

        TEST(TaskManagerTest, CreateRejectsInvalidIdAndTypeWithoutMutation)
        {
            TaskManager manager;
            TaskRecord out;

            EXPECT_EQ(manager.CreateTask(MakeCreate(""), &out).code(),
                      StatusCode::kInvalidArgument);

            TaskCreateRequest bad = MakeCreate("task-1");
            bad.type = static_cast<TaskType>(99);
            EXPECT_EQ(manager.CreateTask(bad, &out).code(),
                      StatusCode::kInvalidArgument);

            std::vector<TaskRecord> tasks;
            ASSERT_TRUE(manager.ListTasks(&tasks).ok());
            EXPECT_TRUE(tasks.empty());
        }

        TEST(TaskManagerTest, DuplicateCreateIsIdempotentAndConflictIsRejected)
        {
            TaskManager manager;
            TaskRecord first;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1"), &first).ok());

            TaskRecord duplicate;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1"), &duplicate).ok());
            EXPECT_EQ(duplicate.generation, first.generation);
            EXPECT_EQ(duplicate.sequence, first.sequence);

            EXPECT_EQ(manager.CreateTask(MakeCreate("task-1",
                                                    TaskType::DELETE,
                                                    {0x01}))
                          .code(),
                      StatusCode::kBusy);
            EXPECT_EQ(manager.CreateTask(MakeCreate("task-1",
                                                    TaskType::CREATE,
                                                    {0x02}))
                          .code(),
                      StatusCode::kBusy);

            std::vector<TaskRecord> tasks;
            ASSERT_TRUE(manager.ListTasks(&tasks).ok());
            ASSERT_EQ(tasks.size(), 1U);
            EXPECT_EQ(tasks[0].task_id, "task-1");
        }

        TEST(TaskManagerTest, QueryReturnsCopyAndListUsesFifoOrder)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-b")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-a")).ok());

            TaskRecord copy = MustGet(&manager, "task-b");
            copy.target_payload = {0xFF};
            EXPECT_EQ(MustGet(&manager, "task-b").target_payload,
                      (TaskPayload{0x01}));

            std::vector<TaskRecord> tasks;
            ASSERT_TRUE(manager.ListTasks(&tasks).ok());
            ASSERT_EQ(tasks.size(), 2U);
            EXPECT_EQ(tasks[0].task_id, "task-b");
            EXPECT_EQ(tasks[1].task_id, "task-a");

            TaskRecord missing;
            EXPECT_EQ(manager.GetTask("missing", &missing).code(),
                      StatusCode::kNotFound);
        }

        TEST(TaskManagerTest, AssignWaitingTaskAdvancesGenerationAndIsIdempotentForSameStore)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());

            TaskRecord assigned;
            ASSERT_TRUE(manager.AssignTask("task-1", 10, 1, &assigned).ok());
            EXPECT_EQ(assigned.state, TaskState::RUNNING);
            EXPECT_EQ(assigned.assigned_store, 10U);
            EXPECT_EQ(assigned.generation, 2U);

            TaskRecord duplicate;
            ASSERT_TRUE(manager.AssignTask("task-1", 10, 2, &duplicate).ok());
            EXPECT_EQ(duplicate.generation, 2U);

            EXPECT_EQ(manager.AssignTask("task-1", 11, 2).code(),
                      StatusCode::kBusy);
            EXPECT_EQ(MustGet(&manager, "task-1").assigned_store, 10U);
        }

        TEST(TaskManagerTest, AssignRejectsInvalidStoreMissingTaskAndGenerationConflict)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());

            EXPECT_EQ(manager.AssignTask("task-1", 0, 1).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(manager.AssignTask("missing", 1, 1).code(),
                      StatusCode::kNotFound);
            EXPECT_EQ(manager.AssignTask("task-1", 1, 99).code(),
                      StatusCode::kBusy);

            const TaskRecord task = MustGet(&manager, "task-1");
            EXPECT_EQ(task.state, TaskState::WAITING);
            EXPECT_EQ(task.generation, 1U);
            EXPECT_EQ(task.assigned_store, 0U);
        }

        TEST(TaskManagerTest, CompleteStoresSuccessAndFailedResults)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-success")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-failed")).ok());
            ASSERT_TRUE(manager.AssignTask("task-success", 7, 1).ok());
            ASSERT_TRUE(manager.AssignTask("task-failed", 7, 1).ok());

            TaskResultReport success;
            success.store_id = 7;
            success.task_id = "task-success";
            success.final_state = TaskState::SUCCESS;
            success.result_payload = {0x10};
            TaskRecord updated;
            ASSERT_TRUE(manager.CompleteTask(success, &updated).ok());
            EXPECT_EQ(updated.state, TaskState::SUCCESS);
            EXPECT_EQ(updated.result_payload, (TaskPayload{0x10}));
            EXPECT_EQ(updated.generation, 3U);

            TaskResultReport failed = success;
            failed.task_id = "task-failed";
            failed.final_state = TaskState::FAILED;
            failed.result_payload = {0x20};
            ASSERT_TRUE(manager.CompleteTask(failed, &updated).ok());
            EXPECT_EQ(updated.state, TaskState::FAILED);
            EXPECT_EQ(updated.result_payload, (TaskPayload{0x20}));
        }

        TEST(TaskManagerTest, CompleteRejectsWaitingWrongStoreAndInvalidFinalState)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());

            TaskResultReport report;
            report.store_id = 7;
            report.task_id = "task-1";
            report.final_state = TaskState::SUCCESS;
            report.result_payload = {0x10};
            EXPECT_EQ(manager.CompleteTask(report).code(), StatusCode::kBusy);

            ASSERT_TRUE(manager.AssignTask("task-1", 7, 1).ok());
            report.store_id = 8;
            EXPECT_EQ(manager.CompleteTask(report).code(), StatusCode::kBusy);

            report.store_id = 7;
            report.final_state = TaskState::RUNNING;
            EXPECT_EQ(manager.CompleteTask(report).code(),
                      StatusCode::kInvalidArgument);

            const TaskRecord task = MustGet(&manager, "task-1");
            EXPECT_EQ(task.state, TaskState::RUNNING);
            EXPECT_TRUE(task.result_payload.empty());
            EXPECT_EQ(task.generation, 2U);
        }

        TEST(TaskManagerTest, DuplicateFinalResultIsIdempotentAndConflictIsRejected)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            ASSERT_TRUE(manager.AssignTask("task-1", 7, 1).ok());

            TaskResultReport report;
            report.store_id = 7;
            report.task_id = "task-1";
            report.final_state = TaskState::SUCCESS;
            report.result_payload = {0x10};
            TaskRecord first;
            ASSERT_TRUE(manager.CompleteTask(report, &first).ok());

            TaskRecord duplicate;
            ASSERT_TRUE(manager.CompleteTask(report, &duplicate).ok());
            EXPECT_EQ(duplicate.generation, first.generation);

            TaskResultReport conflict = report;
            conflict.result_payload = {0x11};
            EXPECT_EQ(manager.CompleteTask(conflict).code(), StatusCode::kBusy);

            conflict = report;
            conflict.final_state = TaskState::FAILED;
            EXPECT_EQ(manager.CompleteTask(conflict).code(), StatusCode::kBusy);

            const TaskRecord task = MustGet(&manager, "task-1");
            EXPECT_EQ(task.state, TaskState::SUCCESS);
            EXPECT_EQ(task.result_payload, (TaskPayload{0x10}));
            EXPECT_EQ(task.generation, 3U);
        }

        TEST(TaskManagerTest, AssignNextWaitingTasksIsAtomicAndSkipsRunningOrTerminalTasks)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-2")).ok());
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-3")).ok());
            ASSERT_TRUE(manager.AssignTask("task-2", 9, 1).ok());

            TaskResultReport report;
            report.store_id = 9;
            report.task_id = "task-2";
            report.final_state = TaskState::SUCCESS;
            ASSERT_TRUE(manager.CompleteTask(report).ok());

            std::vector<TaskRecord> assigned;
            ASSERT_TRUE(manager.AssignNextWaitingTasks(5, 10, &assigned).ok());
            ASSERT_EQ(assigned.size(), 2U);
            EXPECT_EQ(assigned[0].task_id, "task-1");
            EXPECT_EQ(assigned[1].task_id, "task-3");
            EXPECT_EQ(MustGet(&manager, "task-1").assigned_store, 5U);
            EXPECT_EQ(MustGet(&manager, "task-3").assigned_store, 5U);
        }

        TEST(TaskManagerTest, PersistentViewRoundTripsAndRestoreFailureIsAtomic)
        {
            TaskManager manager;
            ASSERT_TRUE(manager.CreateTask(MakeCreate("task-1")).ok());
            ASSERT_TRUE(manager.AssignTask("task-1", 7, 1).ok());

            std::vector<TaskRecord> persistent;
            ASSERT_TRUE(manager.ExportPersistent(&persistent).ok());
            ASSERT_EQ(persistent.size(), 1U);

            TaskManager restored;
            ASSERT_TRUE(restored.RestorePersistent(persistent).ok());
            EXPECT_EQ(MustGet(&restored, "task-1").assigned_store, 7U);

            TaskRecord invalid = persistent[0];
            invalid.generation = 0;
            EXPECT_EQ(restored.RestorePersistent({invalid}).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(MustGet(&restored, "task-1").assigned_store, 7U);

            TaskRecord duplicate = persistent[0];
            duplicate.sequence = 2;
            EXPECT_EQ(restored.RestorePersistent({persistent[0], duplicate}).code(),
                      StatusCode::kBusy);
            EXPECT_EQ(MustGet(&restored, "task-1").assigned_store, 7U);
        }

    } // namespace
} // namespace cpr::store
