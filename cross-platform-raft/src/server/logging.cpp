#include "server/logging.h"

#include <iostream>
#include <mutex>
#include <string>

#if defined(CPR_SERVER_WITH_SPDLOG)
#include <spdlog/spdlog.h>
#endif

namespace cpr::server {

namespace {

enum class LoggingBackend {
    kConsole,
    kSpdlog,
};

struct LoggingState {
    bool initialized = false;
    config::LogLevel level = config::LogLevel::kInfo;
    LoggingBackend backend = LoggingBackend::kConsole;
};

std::mutex g_logging_mutex;
LoggingState g_logging_state;

bool ShouldLog(config::LogLevel level) {
    return static_cast<int>(level) >= static_cast<int>(g_logging_state.level);
}

const char* LevelToText(config::LogLevel level) {
    switch (level) {
    case config::LogLevel::kTrace:
        return "TRACE";
    case config::LogLevel::kDebug:
        return "DEBUG";
    case config::LogLevel::kInfo:
        return "INFO";
    case config::LogLevel::kWarn:
        return "WARN";
    case config::LogLevel::kError:
        return "ERROR";
    case config::LogLevel::kCritical:
        return "CRITICAL";
    }
    return "INFO";
}

#if defined(CPR_SERVER_WITH_SPDLOG)
spdlog::level::level_enum ToSpdlogLevel(config::LogLevel level) {
    switch (level) {
    case config::LogLevel::kTrace:
        return spdlog::level::trace;
    case config::LogLevel::kDebug:
        return spdlog::level::debug;
    case config::LogLevel::kInfo:
        return spdlog::level::info;
    case config::LogLevel::kWarn:
        return spdlog::level::warn;
    case config::LogLevel::kError:
        return spdlog::level::err;
    case config::LogLevel::kCritical:
        return spdlog::level::critical;
    }
    return spdlog::level::info;
}
#endif

void LogMessage(config::LogLevel level, std::string_view message) {
    std::lock_guard<std::mutex> lock(g_logging_mutex);
    if (!g_logging_state.initialized || !ShouldLog(level)) {
        return;
    }

#if defined(CPR_SERVER_WITH_SPDLOG)
    if (g_logging_state.backend == LoggingBackend::kSpdlog) {
        spdlog::log(ToSpdlogLevel(level), "{}", message);
        return;
    }
#endif

    std::ostream& stream =
        (level >= config::LogLevel::kError) ? std::cerr : std::cout;
    stream << '[' << LevelToText(level) << "] " << message << '\n';
}

}  // namespace

common::Status InitializeLogging(config::LogLevel level) {
    std::lock_guard<std::mutex> lock(g_logging_mutex);
    g_logging_state.initialized = true;
    g_logging_state.level = level;

#if defined(CPR_SERVER_WITH_SPDLOG)
    g_logging_state.backend = LoggingBackend::kSpdlog;
    try {
        spdlog::set_pattern("[%l] %v");
        spdlog::set_level(ToSpdlogLevel(level));
        spdlog::flush_on(spdlog::level::err);
    } catch (const spdlog::spdlog_ex& error) {
        g_logging_state.initialized = false;
        return common::Status::InternalError(
            std::string("failed to initialize spdlog: ") + error.what());
    }
#else
    g_logging_state.backend = LoggingBackend::kConsole;
#endif

    return common::Status::OK();
}

common::Status SetLogLevel(config::LogLevel level) {
    std::lock_guard<std::mutex> lock(g_logging_mutex);
    if (!g_logging_state.initialized) {
        return common::Status::Busy("logging is not initialized");
    }
    g_logging_state.level = level;
#if defined(CPR_SERVER_WITH_SPDLOG)
    if (g_logging_state.backend == LoggingBackend::kSpdlog) {
        spdlog::set_level(ToSpdlogLevel(level));
    }
#endif
    return common::Status::OK();
}

void LogDebug(std::string_view message) {
    LogMessage(config::LogLevel::kDebug, message);
}

void LogInfo(std::string_view message) {
    LogMessage(config::LogLevel::kInfo, message);
}

void LogWarning(std::string_view message) {
    LogMessage(config::LogLevel::kWarn, message);
}

void LogError(std::string_view message) {
    LogMessage(config::LogLevel::kError, message);
}

common::Status FlushLogging() {
    std::lock_guard<std::mutex> lock(g_logging_mutex);
    if (!g_logging_state.initialized) {
        return common::Status::OK();
    }

#if defined(CPR_SERVER_WITH_SPDLOG)
    if (g_logging_state.backend == LoggingBackend::kSpdlog) {
        spdlog::default_logger()->flush();
        return common::Status::OK();
    }
#endif

    std::cout.flush();
    std::cerr.flush();
    return common::Status::OK();
}

common::Status ShutdownLogging() {
    std::lock_guard<std::mutex> lock(g_logging_mutex);
    if (!g_logging_state.initialized) {
        return common::Status::OK();
    }

#if defined(CPR_SERVER_WITH_SPDLOG)
    if (g_logging_state.backend == LoggingBackend::kSpdlog) {
        spdlog::shutdown();
    }
#endif

    std::cout.flush();
    std::cerr.flush();
    g_logging_state = LoggingState{};
    return common::Status::OK();
}

}  // namespace cpr::server
