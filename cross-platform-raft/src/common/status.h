#pragma once

#include <string>
#include <utility>

namespace cpr::common
{

    enum class StatusCode
    {
        kOk = 0,
        kInvalidArgument,
        kNotFound,
        kIoError,
        kCorruption,
        kBusy,
        kResourceExhausted,
        kRetryLater,
        kInternalError,
    };

    class Status
    {
    public:
        Status();
        Status(StatusCode code, std::string message);

        static Status OK();
        static Status InvalidArgument(std::string message);
        static Status NotFound(std::string message);
        static Status IoError(std::string message);
        static Status Corruption(std::string message);
        static Status Busy(std::string message);
        static Status ResourceExhausted(std::string message);
        static Status RetryLater(std::string message);
        static Status InternalError(std::string message);

        bool ok() const noexcept;
        StatusCode code() const noexcept;
        const std::string &message() const noexcept;
        std::string ToString() const;

    private:
        StatusCode code_;
        std::string message_;
    };

    const char *StatusCodeToString(StatusCode code) noexcept;

    inline bool operator==(const Status &lhs, const Status &rhs)
    {
        return lhs.code() == rhs.code() && lhs.message() == rhs.message();
    }

    inline bool operator!=(const Status &lhs, const Status &rhs)
    {
        return !(lhs == rhs);
    }

} // namespace cpr::common
