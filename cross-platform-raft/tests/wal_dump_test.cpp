#include "wal_dump/wal_dump_command.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/types.h"
#include "raft/file_raft_storage.h"
#include "tool_common.h"

namespace
{

    class TempDir
    {
    public:
        explicit TempDir(const std::string &name)
        {
            path_ = std::filesystem::temp_directory_path() / name;
            std::error_code ec;
            std::filesystem::remove_all(path_, ec);
            std::filesystem::create_directories(path_);
        }

        ~TempDir()
        {
            std::error_code ec;
            std::filesystem::remove_all(path_, ec);
        }

        const std::filesystem::path &path() const { return path_; }

    private:
        std::filesystem::path path_;
    };

    cpr::raft::LogEntry Entry(cpr::common::LogIndex index,
                              cpr::common::Term term,
                              const std::string &payload)
    {
        cpr::raft::LogEntry entry;
        entry.index = index;
        entry.term = term;
        entry.type = cpr::raft::LogEntryType::COMMAND;
        entry.payload.assign(payload.begin(), payload.end());
        return entry;
    }

    cpr::raft::SnapshotData Snapshot()
    {
        cpr::raft::SnapshotData snapshot;
        snapshot.metadata.last_included_index = 1;
        snapshot.metadata.last_included_term = 1;
        snapshot.metadata.membership.configuration_id = 1;
        cpr::raft::RaftMember voter;
        voter.node_id = 1;
        voter.address.host = "127.0.0.1";
        voter.address.port = 9001;
        snapshot.metadata.membership.voters.push_back(voter);
        snapshot.payload = {1, 2, 3};
        return snapshot;
    }

    void WriteBytes(const std::filesystem::path &path,
                    const std::vector<unsigned char> &bytes)
    {
        std::ofstream file(path, std::ios::binary | std::ios::trunc);
        file.write(reinterpret_cast<const char *>(bytes.data()),
                   static_cast<std::streamsize>(bytes.size()));
    }

    std::vector<unsigned char> ReadBytes(const std::filesystem::path &path)
    {
        std::ifstream file(path, std::ios::binary);
        return std::vector<unsigned char>(std::istreambuf_iterator<char>(file),
                                          std::istreambuf_iterator<char>());
    }

    int RunWalDump(std::initializer_list<const char *> args,
                   std::string *out,
                   std::string *err)
    {
        std::vector<const char *> argv(args);
        std::ostringstream stdout_stream;
        std::ostringstream stderr_stream;
        const int rc = cpr::tools::wal_dump::RunWalDump(
            static_cast<int>(argv.size()),
            argv.data(),
            stdout_stream,
            stderr_stream);
        *out = stdout_stream.str();
        *err = stderr_stream.str();
        return rc;
    }

    void CreateStorageSample(const std::filesystem::path &path)
    {
        cpr::raft::FileRaftStorage storage;
        ASSERT_TRUE(storage.Open(path).ok());
        ASSERT_TRUE(storage.AppendEntries({Entry(1, 1, "a"),
                                           Entry(2, 1, "b")})
                        .ok());
        cpr::raft::HardState hard_state;
        hard_state.current_term = 1;
        hard_state.voted_for = 1;
        hard_state.commit_index = 2;
        hard_state.membership_configuration_id = 1;
        ASSERT_TRUE(storage.SaveHardState(hard_state).ok());
        ASSERT_TRUE(storage.SaveSnapshot(Snapshot()).ok());
    }

} // namespace

TEST(WalDumpTest, HelpDoesNotReadFiles)
{
    std::string out;
    std::string err;
    EXPECT_EQ(0, RunWalDump({"wal_dump", "--help"}, &out, &err));
    EXPECT_NE(std::string::npos, out.find("Usage"));
}

TEST(WalDumpTest, DumpsStorageDirectoryWithoutModifyingFiles)
{
    TempDir dir("cpr-wal-dump-storage");
    CreateStorageSample(dir.path());
    const std::vector<unsigned char> before_log = ReadBytes(dir.path() / "log.bin");
    const std::vector<unsigned char> before_hard = ReadBytes(dir.path() / "hard_state.bin");
    const std::vector<unsigned char> before_snapshot = ReadBytes(dir.path() / "snapshot.bin");

    std::string out;
    std::string err;
    EXPECT_EQ(0, RunWalDump({"wal_dump", dir.path().string().c_str()}, &out, &err));
    EXPECT_NE(std::string::npos, out.find("record_type=HardState"));
    EXPECT_NE(std::string::npos, out.find("current_term=1"));
    EXPECT_NE(std::string::npos, out.find("record_type=LogEntry"));
    EXPECT_NE(std::string::npos, out.find("checksum=OK"));
    EXPECT_NE(std::string::npos, out.find("last_included_index=1"));

    EXPECT_EQ(before_log, ReadBytes(dir.path() / "log.bin"));
    EXPECT_EQ(before_hard, ReadBytes(dir.path() / "hard_state.bin"));
    EXPECT_EQ(before_snapshot, ReadBytes(dir.path() / "snapshot.bin"));
}

TEST(WalDumpTest, OutputIsDeterministicForSameFile)
{
    TempDir dir("cpr-wal-dump-deterministic");
    CreateStorageSample(dir.path());
    std::string first;
    std::string second;
    std::string err;
    EXPECT_EQ(0, RunWalDump({"wal_dump", (dir.path() / "log.bin").string().c_str()}, &first, &err));
    EXPECT_EQ(0, RunWalDump({"wal_dump", (dir.path() / "log.bin").string().c_str()}, &second, &err));
    EXPECT_EQ(first, second);
}

TEST(WalDumpTest, ReportsMissingEmptyAndTruncatedFiles)
{
    TempDir dir("cpr-wal-dump-errors");
    std::string out;
    std::string err;
    EXPECT_EQ(cpr::tools::kExitUsage,
              RunWalDump({"wal_dump", (dir.path() / "missing.bin").string().c_str()}, &out, &err));

    const auto empty = dir.path() / "empty.bin";
    WriteBytes(empty, {});
    EXPECT_EQ(0, RunWalDump({"wal_dump", empty.string().c_str()}, &out, &err));
    EXPECT_NE(std::string::npos, out.find("records=0"));

    const auto truncated = dir.path() / "truncated.bin";
    WriteBytes(truncated, {1, 2, 3, 4});
    EXPECT_EQ(cpr::tools::kExitStorage,
              RunWalDump({"wal_dump", truncated.string().c_str()}, &out, &err));
    EXPECT_NE(std::string::npos, err.find("truncated record header"));
}

TEST(WalDumpTest, ReportsUnknownVersionRecordTypeChecksumAndHugePayload)
{
    TempDir dir("cpr-wal-dump-corruption");
    CreateStorageSample(dir.path());
    const std::vector<unsigned char> original = ReadBytes(dir.path() / "log.bin");
    std::string out;
    std::string err;

    std::vector<unsigned char> bytes = original;
    bytes[0] = 2;
    const auto bad_version = dir.path() / "bad-version.bin";
    WriteBytes(bad_version, bytes);
    EXPECT_EQ(cpr::tools::kExitStorage,
              RunWalDump({"wal_dump", bad_version.string().c_str()}, &out, &err));
    EXPECT_NE(std::string::npos, err.find("unsupported record version"));

    bytes = original;
    bytes[4] = 99;
    const auto bad_type = dir.path() / "bad-type.bin";
    WriteBytes(bad_type, bytes);
    EXPECT_EQ(cpr::tools::kExitStorage,
              RunWalDump({"wal_dump", bad_type.string().c_str()}, &out, &err));
    EXPECT_NE(std::string::npos, err.find("unknown record type"));

    bytes = original;
    bytes.back() ^= 0xffU;
    const auto bad_checksum = dir.path() / "bad-checksum.bin";
    WriteBytes(bad_checksum, bytes);
    EXPECT_EQ(cpr::tools::kExitStorage,
              RunWalDump({"wal_dump", bad_checksum.string().c_str()}, &out, &err));
    EXPECT_NE(std::string::npos, err.find("checksum mismatch"));

    bytes = original;
    bytes[24] = 0xffU;
    bytes[25] = 0xffU;
    bytes[26] = 0xffU;
    bytes[27] = 0xffU;
    bytes[28] = 0xffU;
    bytes[29] = 0xffU;
    bytes[30] = 0xffU;
    bytes[31] = 0x7fU;
    const auto huge_payload = dir.path() / "huge-payload.bin";
    WriteBytes(huge_payload, bytes);
    EXPECT_EQ(cpr::tools::kExitStorage,
              RunWalDump({"wal_dump", huge_payload.string().c_str()}, &out, &err));
    EXPECT_NE(std::string::npos, err.find("payload is too large"));
}

TEST(WalDumpTest, ReportsSnapshotChecksumCorruption)
{
    TempDir dir("cpr-wal-dump-snapshot");
    CreateStorageSample(dir.path());
    std::vector<unsigned char> bytes = ReadBytes(dir.path() / "snapshot.bin");
    bytes.back() ^= 0xffU;
    const auto bad_snapshot = dir.path() / "bad snapshot.bin";
    WriteBytes(bad_snapshot, bytes);

    std::string out;
    std::string err;
    EXPECT_EQ(cpr::tools::kExitStorage,
              RunWalDump({"wal_dump", bad_snapshot.string().c_str(), "--snapshot"}, &out, &err));
    EXPECT_NE(std::string::npos, err.find("snapshot checksum mismatch"));
}
