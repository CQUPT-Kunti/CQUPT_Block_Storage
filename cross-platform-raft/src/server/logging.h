#pragma once

#include <string_view>

#include "common/status.h"
#include "config/config.h"

namespace cpr::server {

common::Status InitializeLogging(config::LogLevel level);
common::Status SetLogLevel(config::LogLevel level);
void LogDebug(std::string_view message);
void LogInfo(std::string_view message);
void LogWarning(std::string_view message);
void LogError(std::string_view message);
common::Status FlushLogging();
common::Status ShutdownLogging();

}  // namespace cpr::server
