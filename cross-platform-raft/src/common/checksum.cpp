#include "common/checksum.h"

namespace cpr::common {

namespace {

constexpr ChecksumValue kFnvOffsetBasis = 14695981039346656037ULL;
constexpr ChecksumValue kFnvPrime = 1099511628211ULL;

}  // namespace

ChecksumValue Checksum::Compute(const void* data, std::size_t size) noexcept {
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    ChecksumValue value = kFnvOffsetBasis;
    for (std::size_t i = 0; i < size; ++i) {
        value ^= static_cast<ChecksumValue>(bytes[i]);
        value *= kFnvPrime;
    }
    return value;
}

ChecksumValue Checksum::Compute(const std::string& data) noexcept {
    return Compute(data.data(), data.size());
}

ChecksumValue Checksum::Compute(const std::vector<std::uint8_t>& data) noexcept {
    return Compute(data.data(), data.size());
}

bool Checksum::Verify(const void* data, std::size_t size, ChecksumValue expected) noexcept {
    return Compute(data, size) == expected;
}

bool Checksum::Verify(const std::string& data, ChecksumValue expected) noexcept {
    return Verify(data.data(), data.size(), expected);
}

bool Checksum::Verify(const std::vector<std::uint8_t>& data, ChecksumValue expected) noexcept {
    return Verify(data.data(), data.size(), expected);
}

}  // namespace cpr::common
