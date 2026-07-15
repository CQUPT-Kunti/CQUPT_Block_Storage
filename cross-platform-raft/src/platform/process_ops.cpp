#include "platform/process_ops.h"

#include <atomic>

#if defined(_WIN32)
#include <windows.h>
#else
#include <csignal>
#endif

namespace cpr::platform {

namespace {

#if defined(_WIN32)
std::atomic<bool> g_shutdown_requested{false};
#else
volatile std::sig_atomic_t g_shutdown_requested = 0;
#endif
ShutdownHandler g_shutdown_handler = nullptr;

void MarkShutdownRequested() noexcept {
#if defined(_WIN32)
    g_shutdown_requested.store(true, std::memory_order_relaxed);
#else
    g_shutdown_requested = 1;
#endif
}

#if defined(_WIN32)
BOOL WINAPI HandleConsoleEvent(DWORD event_type) {
    switch (event_type) {
    case CTRL_C_EVENT:
    case CTRL_BREAK_EVENT:
    case CTRL_CLOSE_EVENT:
    case CTRL_LOGOFF_EVENT:
    case CTRL_SHUTDOWN_EVENT:
        MarkShutdownRequested();
        return TRUE;
    default:
        return FALSE;
    }
}
#else
void HandleSignal(int) {
    MarkShutdownRequested();
}
#endif

}  // namespace

common::Status RegisterShutdownHandler(ShutdownHandler handler) {
    g_shutdown_handler = handler;

#if defined(_WIN32)
    if (!SetConsoleCtrlHandler(HandleConsoleEvent, TRUE)) {
        return common::Status::InternalError(
            "failed to register Windows console shutdown handler");
    }
#else
    struct sigaction action {};
    action.sa_handler = HandleSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;

    if (sigaction(SIGINT, &action, nullptr) != 0 ||
        sigaction(SIGTERM, &action, nullptr) != 0) {
        return common::Status::InternalError(
            "failed to register POSIX shutdown signal handlers");
    }
#endif

    return common::Status::OK();
}

bool IsShutdownRequested() noexcept {
#if defined(_WIN32)
    return g_shutdown_requested.load(std::memory_order_relaxed);
#else
    return g_shutdown_requested != 0;
#endif
}

void RequestShutdownForTests() noexcept {
    MarkShutdownRequested();
    if (g_shutdown_handler != nullptr) {
        g_shutdown_handler();
    }
}

void ResetShutdownForTests() noexcept {
#if defined(_WIN32)
    g_shutdown_requested.store(false, std::memory_order_relaxed);
#else
    g_shutdown_requested = 0;
#endif
}

}  // namespace cpr::platform
