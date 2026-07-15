#pragma once

#include "common/status.h"

namespace cpr::platform {

using ShutdownHandler = void (*)();

common::Status RegisterShutdownHandler(ShutdownHandler handler);
bool IsShutdownRequested() noexcept;
void RequestShutdownForTests() noexcept;
void ResetShutdownForTests() noexcept;

}  // namespace cpr::platform
