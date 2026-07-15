#pragma once

#include <cstdint>

namespace cpr::platform {

std::uint64_t GetMonotonicTimeMs() noexcept;

class ManualTimeSource {
public:
    explicit ManualTimeSource(std::uint64_t initial_time_ms = 0) noexcept;

    std::uint64_t NowMs() const noexcept;
    void AdvanceMs(std::uint64_t delta_ms) noexcept;
    void SetNowMs(std::uint64_t now_ms) noexcept;

private:
    std::uint64_t now_ms_;
};

}  // namespace cpr::platform
