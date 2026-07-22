#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iosfwd>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "common/types.h"

namespace cpr::tools
{

    enum ExitCode
    {
        kExitOk = 0,
        kExitUsage = 2,
        kExitRpc = 3,
        kExitBusiness = 4,
        kExitStorage = 5,
    };

    struct ParsedAddress
    {
        std::string host;
        std::uint16_t port = 0;
    };

    bool HasHelp(int argc, const char *const argv[]);
    common::Status ParseU64(std::string_view text,
                            const char *name,
                            std::uint64_t *value);
    common::Status ParseU32(std::string_view text,
                            const char *name,
                            std::uint32_t *value);
    common::Status ParsePort(std::string_view text,
                             std::uint16_t *value);
    common::Status ParseAddress(std::string_view host,
                                std::string_view port,
                                ParsedAddress *address);
    common::Status ReadPayloadFile(const std::filesystem::path &path,
                                   std::size_t max_bytes,
                                   std::string *payload);
    std::string StatusCodeName(common::StatusCode code);
    void PrintStatus(std::ostream &out, const common::Status &status);
    std::chrono::milliseconds TimeoutOrDefault(std::uint64_t timeout_ms,
                                               std::chrono::milliseconds fallback);

} // namespace cpr::tools
