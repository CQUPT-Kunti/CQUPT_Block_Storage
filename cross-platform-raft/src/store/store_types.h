#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace cpr::store
{

    using StoreId = std::uint64_t;

    enum class StoreState : std::uint8_t
    {
        RUNNING,
        STOPPED,
        FAILED,
    };

    struct StoreAddress
    {
        std::string host;
        std::uint16_t port = 0;
    };

    struct StoreInfo
    {
        StoreId id;
        StoreAddress address;
        std::uint64_t capacity_bytes = 0;
        std::uint64_t used_bytes = 0;
        StoreState state = StoreState::RUNNING;
        std::uint64_t generation = 0;
        std::int64_t last_heartbeat_ms = 0;
    };

    struct PersistentStoreInfo
    {
        StoreId id;
        StoreAddress address;
        std::uint64_t capacity_bytes = 0;
        std::uint64_t used_bytes = 0;
        StoreState state = StoreState::RUNNING;
        std::uint64_t generation = 0;
    };

    struct StoreUpdate
    {
        StoreId id;
        std::optional<std::uint64_t> expected_generation;
        std::optional<std::uint64_t> capacity_bytes;
        std::optional<std::uint64_t> used_bytes;
        std::optional<StoreState> state;
    };

    bool operator==(const StoreAddress &lhs, const StoreAddress &rhs);
    bool operator!=(const StoreAddress &lhs, const StoreAddress &rhs);

} // namespace cpr::store
