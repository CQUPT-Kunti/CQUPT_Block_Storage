#include "tool_common.h"

#include <cerrno>
#include <charconv>
#include <fstream>
#include <iterator>
#include <limits>
#include <ostream>
#include <system_error>

namespace cpr::tools
{
    namespace
    {

        bool IsEmpty(std::string_view value)
        {
            return value.empty();
        }

    } // namespace

    bool HasHelp(int argc, const char *const argv[])
    {
        for (int i = 1; i < argc; ++i)
        {
            if (std::string_view(argv[i]) == "--help" ||
                std::string_view(argv[i]) == "-h")
            {
                return true;
            }
        }
        return false;
    }

    common::Status ParseU64(std::string_view text,
                            const char *name,
                            std::uint64_t *value)
    {
        if (value == nullptr)
        {
            return common::Status::InvalidArgument("numeric output pointer is null");
        }
        if (IsEmpty(text) || text.front() == '-')
        {
            return common::Status::InvalidArgument(std::string(name) + " must be a non-negative integer");
        }
        std::uint64_t parsed = 0;
        const char *begin = text.data();
        const char *end = text.data() + text.size();
        const auto result = std::from_chars(begin, end, parsed);
        if (result.ec != std::errc() || result.ptr != end)
        {
            return common::Status::InvalidArgument(std::string(name) + " is not a valid integer");
        }
        *value = parsed;
        return common::Status::OK();
    }

    common::Status ParseU32(std::string_view text,
                            const char *name,
                            std::uint32_t *value)
    {
        std::uint64_t parsed = 0;
        common::Status status = ParseU64(text, name, &parsed);
        if (!status.ok())
        {
            return status;
        }
        if (parsed > std::numeric_limits<std::uint32_t>::max())
        {
            return common::Status::InvalidArgument(std::string(name) + " exceeds uint32 range");
        }
        *value = static_cast<std::uint32_t>(parsed);
        return common::Status::OK();
    }

    common::Status ParsePort(std::string_view text,
                             std::uint16_t *value)
    {
        std::uint64_t parsed = 0;
        common::Status status = ParseU64(text, "port", &parsed);
        if (!status.ok())
        {
            return status;
        }
        if (parsed == 0 || parsed > 65535)
        {
            return common::Status::InvalidArgument("port must be in 1..65535");
        }
        *value = static_cast<std::uint16_t>(parsed);
        return common::Status::OK();
    }

    common::Status ParseAddress(std::string_view host,
                                std::string_view port,
                                ParsedAddress *address)
    {
        if (address == nullptr)
        {
            return common::Status::InvalidArgument("address output pointer is null");
        }
        if (host.empty())
        {
            return common::Status::InvalidArgument("host must not be empty");
        }
        std::uint16_t parsed_port = 0;
        common::Status status = ParsePort(port, &parsed_port);
        if (!status.ok())
        {
            return status;
        }
        ParsedAddress candidate;
        candidate.host.assign(host.begin(), host.end());
        candidate.port = parsed_port;
        *address = std::move(candidate);
        return common::Status::OK();
    }

    common::Status ReadPayloadFile(const std::filesystem::path &path,
                                   std::size_t max_bytes,
                                   std::string *payload)
    {
        if (payload == nullptr)
        {
            return common::Status::InvalidArgument("payload output pointer is null");
        }
        if (path.empty())
        {
            return common::Status::InvalidArgument("payload file path must not be empty");
        }
        std::error_code ec;
        if (!std::filesystem::exists(path, ec))
        {
            return common::Status::NotFound("payload file not found");
        }
        if (ec || !std::filesystem::is_regular_file(path, ec))
        {
            return common::Status::InvalidArgument("payload path must be a regular file");
        }
        const auto size = std::filesystem::file_size(path, ec);
        if (ec)
        {
            return common::Status::IoError("failed to inspect payload file");
        }
        if (size > max_bytes)
        {
            return common::Status::ResourceExhausted("payload file is too large");
        }

        std::ifstream file(path, std::ios::binary);
        if (!file)
        {
            return common::Status::IoError("failed to open payload file");
        }
        std::string candidate((std::istreambuf_iterator<char>(file)),
                              std::istreambuf_iterator<char>());
        if (!file.good() && !file.eof())
        {
            return common::Status::IoError("failed to read payload file");
        }
        *payload = std::move(candidate);
        return common::Status::OK();
    }

    std::string StatusCodeName(common::StatusCode code)
    {
        return common::StatusCodeToString(code);
    }

    void PrintStatus(std::ostream &out, const common::Status &status)
    {
        out << "status=" << StatusCodeName(status.code());
        if (!status.message().empty())
        {
            out << " message=\"" << status.message() << "\"";
        }
        out << '\n';
    }

    std::chrono::milliseconds TimeoutOrDefault(std::uint64_t timeout_ms,
                                               std::chrono::milliseconds fallback)
    {
        if (timeout_ms == 0)
        {
            return fallback;
        }
        const std::uint64_t max_ms =
            static_cast<std::uint64_t>(std::chrono::milliseconds::max().count());
        return std::chrono::milliseconds(timeout_ms > max_ms ? max_ms : timeout_ms);
    }

} // namespace cpr::tools
