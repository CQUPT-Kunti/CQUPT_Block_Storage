#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <vector>

#include "common/status.h"
#include "common/types.h"

namespace cpr::platform {

using FileData = std::vector<common::Byte>;

class FileHandle {
public:
    FileHandle() = default;
    ~FileHandle();

    FileHandle(FileHandle&& other) noexcept;
    FileHandle& operator=(FileHandle&& other) noexcept;

    FileHandle(const FileHandle&) = delete;
    FileHandle& operator=(const FileHandle&) = delete;

    static common::Status OpenExisting(const std::filesystem::path& path,
                                       FileHandle* handle);
    static common::Status OpenOrCreate(const std::filesystem::path& path,
                                       FileHandle* handle);

    common::Status ReadAll(FileData* data) const;
    common::Status WriteAll(const FileData& data);
    common::Status Size(std::uint64_t* size) const;
    common::Status Truncate(std::uint64_t size);
    common::Status Flush();
    common::Status Close();

    bool is_open() const noexcept;
    const std::filesystem::path& path() const noexcept;

private:
    std::filesystem::path path_;
    std::intptr_t native_handle_ = -1;
};

common::Status CreateDirectories(const std::filesystem::path& path);
common::Status Exists(const std::filesystem::path& path, bool* exists);
common::Status Remove(const std::filesystem::path& path);
common::Status Rename(const std::filesystem::path& from,
                      const std::filesystem::path& to);
common::Status AtomicReplace(const std::filesystem::path& target,
                             const FileData& data);

}  // namespace cpr::platform
