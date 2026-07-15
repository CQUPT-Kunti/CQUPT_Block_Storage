#include "platform/file_ops.h"

#include <cerrno>
#include <cstring>
#include <limits>
#include <string>

#if defined(_WIN32)
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace cpr::platform {

namespace {

constexpr std::intptr_t kInvalidHandle = static_cast<std::intptr_t>(-1);

common::Status MakeIoError(const std::string& action,
                           const std::filesystem::path& path,
                           const std::string& detail) {
    return common::Status::IoError(action + " '" + path.string() + "' failed: " +
                                   detail);
}

#if defined(_WIN32)
HANDLE ToNativeHandle(std::intptr_t handle) {
    return reinterpret_cast<HANDLE>(handle);
}

std::intptr_t FromNativeHandle(HANDLE handle) {
    return reinterpret_cast<std::intptr_t>(handle);
}

common::Status MakeLastError(const std::string& action,
                             const std::filesystem::path& path) {
    const DWORD error = GetLastError();
    return MakeIoError(action, path, std::to_string(error));
}
#else
int ToNativeHandle(std::intptr_t handle) {
    return static_cast<int>(handle);
}

std::intptr_t FromNativeHandle(int handle) {
    return static_cast<std::intptr_t>(handle);
}

common::Status MakeErrnoError(const std::string& action,
                              const std::filesystem::path& path) {
    return MakeIoError(action, path, std::strerror(errno));
}
#endif

common::Status CloseNative(std::intptr_t* handle,
                           const std::filesystem::path& path) {
    if (handle == nullptr || *handle == kInvalidHandle) {
        return common::Status::OK();
    }

#if defined(_WIN32)
    if (!CloseHandle(ToNativeHandle(*handle))) {
        return MakeLastError("close", path);
    }
#else
    if (close(ToNativeHandle(*handle)) != 0) {
        return MakeErrnoError("close", path);
    }
#endif

    *handle = kInvalidHandle;
    return common::Status::OK();
}

common::Status WriteAllNative(std::intptr_t handle,
                              const std::filesystem::path& path,
                              const FileData& data) {
#if defined(_WIN32)
    const auto native = ToNativeHandle(handle);
    LARGE_INTEGER zero;
    zero.QuadPart = 0;
    if (!SetFilePointerEx(native, zero, nullptr, FILE_BEGIN)) {
        return MakeLastError("seek", path);
    }

    std::size_t written_total = 0;
    while (written_total < data.size()) {
        const std::size_t remaining = data.size() - written_total;
        const DWORD chunk = static_cast<DWORD>(
            remaining > static_cast<std::size_t>(std::numeric_limits<DWORD>::max())
                ? std::numeric_limits<DWORD>::max()
                : remaining);
        DWORD written = 0;
        if (!WriteFile(native,
                       data.data() + written_total,
                       chunk,
                       &written,
                       nullptr)) {
            return MakeLastError("write", path);
        }
        written_total += written;
    }
    LARGE_INTEGER end;
    end.QuadPart = static_cast<LONGLONG>(data.size());
    if (!SetFilePointerEx(native, end, nullptr, FILE_BEGIN) ||
        !SetEndOfFile(native)) {
        return MakeLastError("truncate", path);
    }
#else
    const int native = ToNativeHandle(handle);
    if (lseek(native, 0, SEEK_SET) < 0) {
        return MakeErrnoError("seek", path);
    }

    std::size_t written_total = 0;
    while (written_total < data.size()) {
        const ssize_t written =
            write(native, data.data() + written_total, data.size() - written_total);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return MakeErrnoError("write", path);
        }
        written_total += static_cast<std::size_t>(written);
    }

    if (ftruncate(native, static_cast<off_t>(data.size())) != 0) {
        return MakeErrnoError("truncate", path);
    }
#endif
    return common::Status::OK();
}

std::filesystem::path MakeAtomicReplaceTempPath(
    const std::filesystem::path& target) {
    return target.parent_path() /
           (target.filename().string() + ".tmp.replace");
}

}  // namespace

FileHandle::~FileHandle() {
    Close();
}

FileHandle::FileHandle(FileHandle&& other) noexcept
    : path_(std::move(other.path_)), native_handle_(other.native_handle_) {
    other.native_handle_ = kInvalidHandle;
}

FileHandle& FileHandle::operator=(FileHandle&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    Close();
    path_ = std::move(other.path_);
    native_handle_ = other.native_handle_;
    other.native_handle_ = kInvalidHandle;
    return *this;
}

common::Status FileHandle::OpenExisting(const std::filesystem::path& path,
                                        FileHandle* handle) {
    if (handle == nullptr) {
        return common::Status::InvalidArgument(
            "file handle output pointer must not be null");
    }

    FileHandle opened;
#if defined(_WIN32)
    HANDLE native = CreateFileW(path.c_str(),
                                GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ,
                                nullptr,
                                OPEN_EXISTING,
                                FILE_ATTRIBUTE_NORMAL,
                                nullptr);
    if (native == INVALID_HANDLE_VALUE) {
        return MakeLastError("open", path);
    }
    opened.native_handle_ = FromNativeHandle(native);
#else
    const int native = open(path.c_str(), O_RDWR, 0644);
    if (native < 0) {
        return MakeErrnoError("open", path);
    }
    opened.native_handle_ = FromNativeHandle(native);
#endif
    opened.path_ = path;
    *handle = std::move(opened);
    return common::Status::OK();
}

common::Status FileHandle::OpenOrCreate(const std::filesystem::path& path,
                                        FileHandle* handle) {
    if (handle == nullptr) {
        return common::Status::InvalidArgument(
            "file handle output pointer must not be null");
    }

    FileHandle opened;
#if defined(_WIN32)
    HANDLE native = CreateFileW(path.c_str(),
                                GENERIC_READ | GENERIC_WRITE,
                                FILE_SHARE_READ,
                                nullptr,
                                OPEN_ALWAYS,
                                FILE_ATTRIBUTE_NORMAL,
                                nullptr);
    if (native == INVALID_HANDLE_VALUE) {
        return MakeLastError("open or create", path);
    }
    opened.native_handle_ = FromNativeHandle(native);
#else
    const int native = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (native < 0) {
        return MakeErrnoError("open or create", path);
    }
    opened.native_handle_ = FromNativeHandle(native);
#endif
    opened.path_ = path;
    *handle = std::move(opened);
    return common::Status::OK();
}

common::Status FileHandle::ReadAll(FileData* data) const {
    if (data == nullptr) {
        return common::Status::InvalidArgument(
            "file data output pointer must not be null");
    }
    if (!is_open()) {
        return common::Status::Busy("file handle is not open");
    }

    std::uint64_t size = 0;
    common::Status status = Size(&size);
    if (!status.ok()) {
        return status;
    }

    if (size > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        return common::Status::ResourceExhausted(
            "file is too large to read into memory");
    }

    data->assign(static_cast<std::size_t>(size), 0);
    if (size == 0) {
        return common::Status::OK();
    }

#if defined(_WIN32)
    LARGE_INTEGER zero;
    zero.QuadPart = 0;
    if (!SetFilePointerEx(ToNativeHandle(native_handle_), zero, nullptr, FILE_BEGIN)) {
        return MakeLastError("seek", path_);
    }

    std::size_t read_total = 0;
    while (read_total < data->size()) {
        const std::size_t remaining = data->size() - read_total;
        const DWORD chunk = static_cast<DWORD>(
            remaining > static_cast<std::size_t>(std::numeric_limits<DWORD>::max())
                ? std::numeric_limits<DWORD>::max()
                : remaining);
        DWORD bytes_read = 0;
        if (!ReadFile(ToNativeHandle(native_handle_),
                      data->data() + read_total,
                      chunk,
                      &bytes_read,
                      nullptr)) {
            return MakeLastError("read", path_);
        }
        if (bytes_read == 0) {
            return common::Status::Corruption(
                "unexpected end of file while reading '" + path_.string() + "'");
        }
        read_total += bytes_read;
    }
#else
    if (lseek(ToNativeHandle(native_handle_), 0, SEEK_SET) < 0) {
        return MakeErrnoError("seek", path_);
    }

    std::size_t read_total = 0;
    while (read_total < data->size()) {
        const ssize_t bytes_read = read(ToNativeHandle(native_handle_),
                                        data->data() + read_total,
                                        data->size() - read_total);
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }
            return MakeErrnoError("read", path_);
        }
        if (bytes_read == 0) {
            return common::Status::Corruption(
                "unexpected end of file while reading '" + path_.string() + "'");
        }
        read_total += static_cast<std::size_t>(bytes_read);
    }
#endif

    return common::Status::OK();
}

common::Status FileHandle::WriteAll(const FileData& data) {
    if (!is_open()) {
        return common::Status::Busy("file handle is not open");
    }
    return WriteAllNative(native_handle_, path_, data);
}

common::Status FileHandle::Size(std::uint64_t* size) const {
    if (size == nullptr) {
        return common::Status::InvalidArgument(
            "file size output pointer must not be null");
    }
    if (!is_open()) {
        return common::Status::Busy("file handle is not open");
    }

#if defined(_WIN32)
    LARGE_INTEGER file_size;
    if (!GetFileSizeEx(ToNativeHandle(native_handle_), &file_size)) {
        return MakeLastError("get size", path_);
    }
    *size = static_cast<std::uint64_t>(file_size.QuadPart);
#else
    struct stat file_stat {};
    if (fstat(ToNativeHandle(native_handle_), &file_stat) != 0) {
        return MakeErrnoError("get size", path_);
    }
    *size = static_cast<std::uint64_t>(file_stat.st_size);
#endif
    return common::Status::OK();
}

common::Status FileHandle::Truncate(std::uint64_t size) {
    if (!is_open()) {
        return common::Status::Busy("file handle is not open");
    }

#if defined(_WIN32)
    LARGE_INTEGER target;
    target.QuadPart = static_cast<LONGLONG>(size);
    if (!SetFilePointerEx(ToNativeHandle(native_handle_), target, nullptr, FILE_BEGIN) ||
        !SetEndOfFile(ToNativeHandle(native_handle_))) {
        return MakeLastError("truncate", path_);
    }
#else
    if (ftruncate(ToNativeHandle(native_handle_), static_cast<off_t>(size)) != 0) {
        return MakeErrnoError("truncate", path_);
    }
#endif

    return common::Status::OK();
}

common::Status FileHandle::Flush() {
    if (!is_open()) {
        return common::Status::Busy("file handle is not open");
    }

#if defined(_WIN32)
    if (!FlushFileBuffers(ToNativeHandle(native_handle_))) {
        return MakeLastError("flush", path_);
    }
#else
    if (fsync(ToNativeHandle(native_handle_)) != 0) {
        return MakeErrnoError("flush", path_);
    }
#endif
    return common::Status::OK();
}

common::Status FileHandle::Close() {
    return CloseNative(&native_handle_, path_);
}

bool FileHandle::is_open() const noexcept {
    return native_handle_ != kInvalidHandle;
}

const std::filesystem::path& FileHandle::path() const noexcept {
    return path_;
}

common::Status CreateDirectories(const std::filesystem::path& path) {
    std::error_code error;
    std::filesystem::create_directories(path, error);
    if (error) {
        return MakeIoError("create directories", path, error.message());
    }
    return common::Status::OK();
}

common::Status Exists(const std::filesystem::path& path, bool* exists) {
    if (exists == nullptr) {
        return common::Status::InvalidArgument(
            "exists output pointer must not be null");
    }
    std::error_code error;
    *exists = std::filesystem::exists(path, error);
    if (error) {
        return MakeIoError("check existence", path, error.message());
    }
    return common::Status::OK();
}

common::Status Remove(const std::filesystem::path& path) {
    std::error_code error;
    const bool removed = std::filesystem::remove(path, error);
    if (error) {
        return MakeIoError("remove", path, error.message());
    }
    if (!removed) {
        return common::Status::NotFound("path not found: " + path.string());
    }
    return common::Status::OK();
}

common::Status Rename(const std::filesystem::path& from,
                      const std::filesystem::path& to) {
    std::error_code error;
    std::filesystem::rename(from, to, error);
    if (error) {
        return MakeIoError("rename", from, error.message());
    }
    return common::Status::OK();
}

common::Status AtomicReplace(const std::filesystem::path& target,
                             const FileData& data) {
    const std::filesystem::path temp_path = MakeAtomicReplaceTempPath(target);
    FileHandle temp_file;
    common::Status status = FileHandle::OpenOrCreate(temp_path, &temp_file);
    if (!status.ok()) {
        return status;
    }

    status = temp_file.WriteAll(data);
    if (!status.ok()) {
        temp_file.Close();
        std::error_code ignore_error;
        std::filesystem::remove(temp_path, ignore_error);
        return status;
    }

    status = temp_file.Flush();
    if (!status.ok()) {
        temp_file.Close();
        std::error_code ignore_error;
        std::filesystem::remove(temp_path, ignore_error);
        return status;
    }

    status = temp_file.Close();
    if (!status.ok()) {
        std::error_code ignore_error;
        std::filesystem::remove(temp_path, ignore_error);
        return status;
    }

#if defined(_WIN32)
    if (!MoveFileExW(temp_path.c_str(),
                     target.c_str(),
                     MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        std::error_code ignore_error;
        std::filesystem::remove(temp_path, ignore_error);
        return MakeLastError("atomic replace", target);
    }
#else
    if (rename(temp_path.c_str(), target.c_str()) != 0) {
        std::error_code ignore_error;
        std::filesystem::remove(temp_path, ignore_error);
        return MakeErrnoError("atomic replace", target);
    }
#endif

    return common::Status::OK();
}

}  // namespace cpr::platform
