#pragma once

#include <cstddef>
#include <cstdint>

namespace cpr::common
{

    using NodeId = std::uint64_t;
    using Term = std::uint64_t;
    using LogIndex = std::uint64_t;
    using QueueSize = std::size_t;
    using Byte = std::uint8_t;

    constexpr NodeId kInvalidNodeId = 0;
    constexpr Term kInitialTerm = 0;
    constexpr LogIndex kInvalidLogIndex = 0;

} // namespace cpr::common
