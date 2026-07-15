#pragma once

#include <atomic>

#include "common/status.h"
#include "config/config.h"

namespace cpr::server {

class ServerLifecycle {
public:
    ServerLifecycle();
    ~ServerLifecycle();

    ServerLifecycle(const ServerLifecycle&) = delete;
    ServerLifecycle& operator=(const ServerLifecycle&) = delete;

    common::Status Initialize(const config::Config& config);
    common::Status Start();
    bool IsRunning() const noexcept;
    bool HasShutdownRequest() const noexcept;
    void RequestStop() noexcept;
    common::Status Stop();

private:
    void MarkShutdownRequested() noexcept;
    static void HandleProcessShutdown() noexcept;

    config::Config config_;
    bool initialized_ = false;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
};

}  // namespace cpr::server
