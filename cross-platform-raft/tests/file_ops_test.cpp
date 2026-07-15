#include <chrono>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

#include "platform/file_ops.h"

namespace cpr::platform {
namespace {

class TempDir {
public:
    TempDir() {
        const auto unique =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        path_ = std::filesystem::temp_directory_path() /
                ("cpr-file-ops-test-" + unique);
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    const std::filesystem::path& path() const { return path_; }

private:
    std::filesystem::path path_;
};

FileData ToData(const std::string& text) {
    return FileData(text.begin(), text.end());
}

std::string ToString(const FileData& data) {
    return std::string(data.begin(), data.end());
}

TEST(FileOpsTest, CreateDirectoryAndCheckExistence) {
    TempDir dir;
    const auto nested = dir.path() / "nested";
    bool exists = false;

    ASSERT_TRUE(CreateDirectories(nested).ok());
    ASSERT_TRUE(Exists(nested, &exists).ok());
    EXPECT_TRUE(exists);
}

TEST(FileOpsTest, OpenWriteReadSizeAndTruncateFile) {
    TempDir dir;
    const auto file = dir.path() / "sample.bin";
    FileHandle handle;
    FileData data;
    std::uint64_t size = 0;

    ASSERT_TRUE(FileHandle::OpenOrCreate(file, &handle).ok());
    ASSERT_TRUE(handle.WriteAll(ToData("abcdef")).ok());
    ASSERT_TRUE(handle.ReadAll(&data).ok());
    EXPECT_EQ(ToString(data), "abcdef");
    ASSERT_TRUE(handle.Size(&size).ok());
    EXPECT_EQ(size, 6U);
    ASSERT_TRUE(handle.Truncate(3).ok());
    ASSERT_TRUE(handle.ReadAll(&data).ok());
    EXPECT_EQ(ToString(data), "abc");
}

TEST(FileOpsTest, FlushRenameDeleteAndAtomicReplaceWork) {
    TempDir dir;
    const auto original = dir.path() / "sample.bin";
    const auto renamed = dir.path() / "renamed.bin";
    FileHandle handle;
    FileData data;
    bool exists = false;

    ASSERT_TRUE(FileHandle::OpenOrCreate(original, &handle).ok());
    ASSERT_TRUE(handle.WriteAll(ToData("old")).ok());
    ASSERT_TRUE(handle.Flush().ok());
    ASSERT_TRUE(handle.Close().ok());

    ASSERT_TRUE(Rename(original, renamed).ok());
    ASSERT_TRUE(AtomicReplace(renamed, ToData("new-content")).ok());

    ASSERT_TRUE(FileHandle::OpenExisting(renamed, &handle).ok());
    ASSERT_TRUE(handle.ReadAll(&data).ok());
    EXPECT_EQ(ToString(data), "new-content");
    ASSERT_TRUE(handle.Close().ok());

    ASSERT_TRUE(Remove(renamed).ok());
    ASSERT_TRUE(Exists(renamed, &exists).ok());
    EXPECT_FALSE(exists);
}

TEST(FileOpsTest, MissingFileReturnsExplicitError) {
    TempDir dir;
    FileHandle handle;
    const auto missing = dir.path() / "missing.bin";

    EXPECT_EQ(FileHandle::OpenExisting(missing, &handle).code(),
              common::StatusCode::kIoError);
    EXPECT_EQ(Remove(missing).code(), common::StatusCode::kNotFound);
}

}  // namespace
}  // namespace cpr::platform
