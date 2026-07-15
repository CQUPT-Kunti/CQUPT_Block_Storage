#pragma once

#include <filesystem>

#include "common/status.h"
#include "config/config.h"

namespace cpr::config {

class ConfigLoader {
public:
    static common::Status LoadFromFile(const std::filesystem::path& path,
                                       Config* config);
};

}  // namespace cpr::config
