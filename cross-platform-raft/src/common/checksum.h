#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace cpr::common
{

    using ChecksumValue = std::uint64_t;

    class Checksum
    {
    public:
        static ChecksumValue Compute(const void *data, std::size_t size) noexcept;
        static ChecksumValue Compute(const std::string &data) noexcept;
        static ChecksumValue Compute(const std::vector<std::uint8_t> &data) noexcept;
        static bool Verify(const void *data, std::size_t size, ChecksumValue expected) noexcept;
        static bool Verify(const std::string &data, ChecksumValue expected) noexcept;
        static bool Verify(const std::vector<std::uint8_t> &data, ChecksumValue expected) noexcept;
    };

} // namespace cpr::common
