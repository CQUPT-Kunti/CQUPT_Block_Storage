#include "common/status.h"

#include <utility>

namespace cpr::common {

Status::Status() : code_(StatusCode::kOk) {}

Status::Status(StatusCode code, std::string message)
    : code_(code), message_(std::move(message)) {}

Status Status::OK() {
    return Status();
}

Status Status::InvalidArgument(std::string message) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
}

Status Status::NotFound(std::string message) {
    return Status(StatusCode::kNotFound, std::move(message));
}

Status Status::IoError(std::string message) {
    return Status(StatusCode::kIoError, std::move(message));
}

Status Status::Corruption(std::string message) {
    return Status(StatusCode::kCorruption, std::move(message));
}

Status Status::Busy(std::string message) {
    return Status(StatusCode::kBusy, std::move(message));
}

Status Status::ResourceExhausted(std::string message) {
    return Status(StatusCode::kResourceExhausted, std::move(message));
}

Status Status::RetryLater(std::string message) {
    return Status(StatusCode::kRetryLater, std::move(message));
}

Status Status::InternalError(std::string message) {
    return Status(StatusCode::kInternalError, std::move(message));
}

bool Status::ok() const noexcept {
    return code_ == StatusCode::kOk;
}

StatusCode Status::code() const noexcept {
    return code_;
}

const std::string& Status::message() const noexcept {
    return message_;
}

std::string Status::ToString() const {
    if (ok()) {
        return StatusCodeToString(code_);
    }
    return std::string(StatusCodeToString(code_)) + ": " + message_;
}

const char* StatusCodeToString(StatusCode code) noexcept {
    switch (code) {
    case StatusCode::kOk:
        return "OK";
    case StatusCode::kInvalidArgument:
        return "INVALID_ARGUMENT";
    case StatusCode::kNotFound:
        return "NOT_FOUND";
    case StatusCode::kIoError:
        return "IO_ERROR";
    case StatusCode::kCorruption:
        return "CORRUPTION";
    case StatusCode::kBusy:
        return "BUSY";
    case StatusCode::kResourceExhausted:
        return "RESOURCE_EXHAUSTED";
    case StatusCode::kRetryLater:
        return "RETRY_LATER";
    case StatusCode::kInternalError:
        return "INTERNAL_ERROR";
    }
    return "INTERNAL_ERROR";
}

}  // namespace cpr::common
