#include "platform/time_ops.h"

#include <chrono>

namespace cpr::platform {

std::uint64_t GetMonotonicTimeMs() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

ManualTimeSource::ManualTimeSource(std::uint64_t initial_time_ms) noexcept
    : now_ms_(initial_time_ms) {}

std::uint64_t ManualTimeSource::NowMs() const noexcept {
    return now_ms_;
}

void ManualTimeSource::AdvanceMs(std::uint64_t delta_ms) noexcept {
    now_ms_ += delta_ms;
}

void ManualTimeSource::SetNowMs(std::uint64_t now_ms) noexcept {
    now_ms_ = now_ms;
}

}  // namespace cpr::platform
