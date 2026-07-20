#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/types.h"
#include "store/store_types.h"

namespace cpr::store
{

    using TaskId = std::string;
    using TaskPayload = std::vector<common::Byte>;

    enum class TaskType : std::uint8_t
    {
        CREATE,
        DELETE,
        COPY,
        CUSTOM,
    };

    enum class TaskState : std::uint8_t
    {
        WAITING,
        RUNNING,
        SUCCESS,
        FAILED,
    };

    struct TaskRecord
    {
        TaskId task_id;
        TaskType type = TaskType::CREATE;
        TaskState state = TaskState::WAITING;
        TaskPayload target_payload;
        StoreId assigned_store = 0;
        TaskPayload result_payload;
        std::uint64_t generation = 0;
        std::uint64_t sequence = 0;
    };

    struct TaskCreateRequest
    {
        TaskId task_id;
        TaskType type = TaskType::CREATE;
        TaskPayload target_payload;
    };

    struct TaskResultReport
    {
        StoreId store_id = 0;
        TaskId task_id;
        TaskState final_state = TaskState::SUCCESS;
        TaskPayload result_payload;
        std::optional<std::uint64_t> expected_generation;
    };

} // namespace cpr::store
