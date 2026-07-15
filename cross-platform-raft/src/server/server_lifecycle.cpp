#include "server/server_lifecycle.h"

#include <mutex>
#include <string>

#include "platform/process_ops.h"
#include "server/logging.h"

namespace cpr::server {

namespace {

std::mutex g_lifecycle_mutex;
ServerLifecycle* g_active_lifecycle = nullptr;

}  // namespace

ServerLifecycle::ServerLifecycle() = default;

ServerLifecycle::~ServerLifecycle() {
    Stop();
}

common::Status ServerLifecycle::Initialize(const config::Config& config) {
    if (initialized_) {
        config_ = config;
        return SetLogLevel(config.log_level);
    }

    common::Status status = InitializeLogging(config.log_level);
    if (!status.ok()) {
        return status;
    }

    status = platform::RegisterShutdownHandler(&ServerLifecycle::HandleProcessShutdown);
    if (!status.ok()) {
        ShutdownLogging();
        return status;
    }

    config_ = config;
    stop_requested_.store(false, std::memory_order_relaxed);
    initialized_ = true;
    LogInfo("server lifecycle initialized");
    return common::Status::OK();
}

common::Status ServerLifecycle::Start() {
    if (!initialized_) {
        return common::Status::Busy("server lifecycle is not initialized");
    }

    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true,
                                          std::memory_order_relaxed)) {
        return common::Status::OK();
    }

    {
        std::lock_guard<std::mutex> lock(g_lifecycle_mutex);
        g_active_lifecycle = this;
    }
    stop_requested_.store(false, std::memory_order_relaxed);
    LogInfo("server lifecycle started for node " + config_.node_id);
    return common::Status::OK();
}

bool ServerLifecycle::IsRunning() const noexcept {
    return running_.load(std::memory_order_relaxed);
}

bool ServerLifecycle::HasShutdownRequest() const noexcept {
    return stop_requested_.load(std::memory_order_relaxed) ||
           platform::IsShutdownRequested();
}

void ServerLifecycle::RequestStop() noexcept {
    MarkShutdownRequested();
}

common::Status ServerLifecycle::Stop() {
    if (!initialized_) {
        return common::Status::OK();
    }

    RequestStop();
    running_.store(false, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(g_lifecycle_mutex);
        if (g_active_lifecycle == this) {
            g_active_lifecycle = nullptr;
        }
    }
    LogInfo("server lifecycle stopped for node " + config_.node_id);
    FlushLogging();
    ShutdownLogging();
    initialized_ = false;
    return common::Status::OK();
}

void ServerLifecycle::MarkShutdownRequested() noexcept {
    stop_requested_.store(true, std::memory_order_relaxed);
}

void ServerLifecycle::HandleProcessShutdown() noexcept {
    std::lock_guard<std::mutex> lock(g_lifecycle_mutex);
    if (g_active_lifecycle != nullptr) {
        g_active_lifecycle->MarkShutdownRequested();
    }
}

}  // namespace cpr::server
