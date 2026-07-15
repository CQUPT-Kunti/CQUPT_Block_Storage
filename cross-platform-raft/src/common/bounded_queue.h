#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <utility>

#include "common/status.h"

namespace cpr::common {

template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

    BoundedQueue(const BoundedQueue&) = delete;
    BoundedQueue& operator=(const BoundedQueue&) = delete;

    Status Push(const T& value) {
        return PushImpl(value);
    }

    Status Push(T&& value) {
        return PushImpl(std::move(value));
    }

    Status Pop(T* value) {
        if (value == nullptr) {
            return Status::InvalidArgument("value pointer must not be null");
        }

        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this]() { return closed_ || !queue_.empty(); });

        if (queue_.empty()) {
            return closed_
                ? Status::RetryLater("queue is closed")
                : Status::InternalError("queue wait ended without data");
        }

        *value = std::move(queue_.front());
        queue_.pop_front();
        return Status::OK();
    }

    void Close() {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        not_empty_.notify_all();
    }

    bool closed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    std::size_t capacity() const noexcept {
        return capacity_;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    template <typename U>
    Status PushImpl(U&& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            return Status::Busy("queue is closed");
        }
        if (capacity_ == 0) {
            return Status::ResourceExhausted("queue capacity is zero");
        }
        if (queue_.size() >= capacity_) {
            return Status::ResourceExhausted("queue is full");
        }

        queue_.emplace_back(std::forward<U>(value));
        not_empty_.notify_one();
        return Status::OK();
    }

    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::deque<T> queue_;
    bool closed_ = false;
};

}  // namespace cpr::common
