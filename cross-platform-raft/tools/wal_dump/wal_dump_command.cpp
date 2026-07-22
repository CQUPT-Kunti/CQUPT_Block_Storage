#include "wal_dump/wal_dump_command.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <ostream>
#include <string>
#include <vector>

#include "common/checksum.h"
#include "raft/raft_snapshot.h"
#include "tool_common.h"

namespace cpr::tools::wal_dump
{
    namespace
    {

        constexpr std::uint32_t kFormatVersion = 1;
        constexpr std::uint32_t kHardStateRecord = 1;
        constexpr std::uint32_t kLogEntryRecord = 2;
        constexpr std::uint64_t kMaxPayloadLength = 64ULL * 1024ULL * 1024ULL;
        constexpr std::uint64_t kMaxFileLength = 128ULL * 1024ULL * 1024ULL;
        constexpr std::size_t kRecordHeaderSize = 40;

        using Bytes = std::vector<common::Byte>;

        struct RecordHeader
        {
            std::uint32_t version = 0;
            std::uint32_t type = 0;
            std::uint64_t index = 0;
            std::uint64_t term = 0;
            std::uint64_t payload_length = 0;
            std::uint64_t checksum = 0;
        };

        void PrintHelp(std::ostream &out)
        {
            out << "Usage: wal_dump <storage-dir|record-file|snapshot-file> [--snapshot]\n"
                << "Reads FileRaftStorage files without modifying them.\n";
        }

        void PutU32(Bytes *bytes, std::uint32_t value)
        {
            for (int shift = 0; shift < 32; shift += 8)
            {
                bytes->push_back(static_cast<common::Byte>((value >> shift) & 0xffU));
            }
        }

        void PutU64(Bytes *bytes, std::uint64_t value)
        {
            for (int shift = 0; shift < 64; shift += 8)
            {
                bytes->push_back(static_cast<common::Byte>((value >> shift) & 0xffULL));
            }
        }

        common::Status ReadU32(const Bytes &bytes,
                               std::size_t *offset,
                               std::uint32_t *value)
        {
            if (*offset > bytes.size() || bytes.size() - *offset < 4)
            {
                return common::Status::Corruption("truncated uint32");
            }
            std::uint32_t result = 0;
            for (int shift = 0; shift < 32; shift += 8)
            {
                result |= static_cast<std::uint32_t>(bytes[(*offset)++]) << shift;
            }
            *value = result;
            return common::Status::OK();
        }

        common::Status ReadU64(const Bytes &bytes,
                               std::size_t *offset,
                               std::uint64_t *value)
        {
            if (*offset > bytes.size() || bytes.size() - *offset < 8)
            {
                return common::Status::Corruption("truncated uint64");
            }
            std::uint64_t result = 0;
            for (int shift = 0; shift < 64; shift += 8)
            {
                result |= static_cast<std::uint64_t>(bytes[(*offset)++]) << shift;
            }
            *value = result;
            return common::Status::OK();
        }

        common::Status ReadFile(const std::filesystem::path &path, Bytes *bytes)
        {
            if (bytes == nullptr)
            {
                return common::Status::InvalidArgument("file output pointer is null");
            }
            std::error_code ec;
            if (!std::filesystem::exists(path, ec))
            {
                return common::Status::NotFound("input file not found");
            }
            if (ec || !std::filesystem::is_regular_file(path, ec))
            {
                return common::Status::InvalidArgument("input path is not a regular file");
            }
            const auto size = std::filesystem::file_size(path, ec);
            if (ec)
            {
                return common::Status::IoError("failed to inspect input file");
            }
            if (size > kMaxFileLength)
            {
                return common::Status::ResourceExhausted("input file is too large");
            }
            std::ifstream file(path, std::ios::binary);
            if (!file)
            {
                return common::Status::IoError("failed to open input file");
            }
            bytes->assign(std::istreambuf_iterator<char>(file),
                          std::istreambuf_iterator<char>());
            if (!file.good() && !file.eof())
            {
                return common::Status::IoError("failed to read input file");
            }
            return common::Status::OK();
        }

        Bytes ChecksumInput(const RecordHeader &header, const Bytes &payload)
        {
            Bytes input;
            PutU32(&input, header.version);
            PutU32(&input, header.type);
            PutU64(&input, header.index);
            PutU64(&input, header.term);
            PutU64(&input, header.payload_length);
            input.insert(input.end(), payload.begin(), payload.end());
            return input;
        }

        const char *RecordTypeName(std::uint32_t type)
        {
            if (type == kHardStateRecord)
            {
                return "HardState";
            }
            if (type == kLogEntryRecord)
            {
                return "LogEntry";
            }
            return "Unknown";
        }

        const char *LogEntryTypeName(std::uint32_t type)
        {
            switch (type)
            {
            case 0:
                return "NO_OP";
            case 1:
                return "COMMAND";
            case 2:
                return "MEMBERSHIP_CHANGE";
            default:
                return "INVALID";
            }
        }

        common::Status DumpRecords(const std::filesystem::path &path,
                                   std::ostream &out)
        {
            Bytes data;
            common::Status status = ReadFile(path, &data);
            if (!status.ok())
            {
                return status;
            }
            out << "file=" << path.filename().string()
                << " bytes=" << data.size() << '\n';
            if (data.empty())
            {
                out << "records=0\n";
                return common::Status::OK();
            }

            std::size_t offset = 0;
            std::size_t ordinal = 0;
            while (offset < data.size())
            {
                if (data.size() - offset < kRecordHeaderSize)
                {
                    return common::Status::Corruption("truncated record header at offset " + std::to_string(offset));
                }
                const std::size_t record_offset = offset;
                RecordHeader header;
                if (!(status = ReadU32(data, &offset, &header.version)).ok() ||
                    !(status = ReadU32(data, &offset, &header.type)).ok() ||
                    !(status = ReadU64(data, &offset, &header.index)).ok() ||
                    !(status = ReadU64(data, &offset, &header.term)).ok() ||
                    !(status = ReadU64(data, &offset, &header.payload_length)).ok() ||
                    !(status = ReadU64(data, &offset, &header.checksum)).ok())
                {
                    return status;
                }
                if (header.version != kFormatVersion)
                {
                    return common::Status::Corruption("unsupported record version at offset " + std::to_string(record_offset));
                }
                if (header.type != kHardStateRecord && header.type != kLogEntryRecord)
                {
                    return common::Status::Corruption("unknown record type at offset " + std::to_string(record_offset));
                }
                if (header.payload_length > kMaxPayloadLength)
                {
                    return common::Status::Corruption("record payload is too large at offset " + std::to_string(record_offset));
                }
                if (header.payload_length > data.size() - offset)
                {
                    return common::Status::Corruption("truncated record payload at offset " + std::to_string(record_offset));
                }
                Bytes payload(data.begin() + static_cast<std::ptrdiff_t>(offset),
                              data.begin() + static_cast<std::ptrdiff_t>(offset + header.payload_length));
                offset += static_cast<std::size_t>(header.payload_length);
                const bool checksum_ok =
                    common::Checksum::Compute(ChecksumInput(header, payload)) == header.checksum;
                if (!checksum_ok)
                {
                    out << "record ordinal=" << ordinal
                        << " offset=" << record_offset
                        << " checksum=FAIL\n";
                    return common::Status::Corruption("record checksum mismatch at offset " + std::to_string(record_offset));
                }

                out << "record ordinal=" << ordinal
                    << " offset=" << record_offset
                    << " format_version=" << header.version
                    << " record_type=" << RecordTypeName(header.type)
                    << " index=" << header.index
                    << " term=" << header.term
                    << " payload_length=" << header.payload_length
                    << " checksum=OK";
                if (header.type == kHardStateRecord && payload.size() == 24)
                {
                    std::size_t payload_offset = 0;
                    std::uint64_t voted_for = 0;
                    std::uint64_t commit_index = 0;
                    std::uint64_t config_id = 0;
                    status = ReadU64(payload, &payload_offset, &voted_for);
                    status = status.ok() ? ReadU64(payload, &payload_offset, &commit_index) : status;
                    status = status.ok() ? ReadU64(payload, &payload_offset, &config_id) : status;
                    if (!status.ok())
                    {
                        return status;
                    }
                    out << " current_term=" << header.term
                        << " voted_for=" << voted_for
                        << " commit_index=" << commit_index
                        << " membership_configuration_id=" << config_id;
                }
                if (header.type == kLogEntryRecord && payload.size() >= 4)
                {
                    std::size_t payload_offset = 0;
                    std::uint32_t entry_type = 0;
                    status = ReadU32(payload, &payload_offset, &entry_type);
                    if (!status.ok())
                    {
                        return status;
                    }
                    out << " log_entry_type=" << LogEntryTypeName(entry_type)
                        << " log_payload_length=" << (payload.size() - payload_offset);
                }
                out << '\n';
                ++ordinal;
            }
            return common::Status::OK();
        }

        common::Status DumpSnapshot(const std::filesystem::path &path,
                                    std::ostream &out)
        {
            Bytes data;
            common::Status status = ReadFile(path, &data);
            if (!status.ok())
            {
                return status;
            }
            raft::SnapshotData snapshot;
            status = raft::DecodeSnapshotData(data, &snapshot);
            if (!status.ok())
            {
                return status;
            }
            out << "file=" << path.filename().string()
                << " bytes=" << data.size()
                << " snapshot_format_version=1"
                << " last_included_index=" << snapshot.metadata.last_included_index
                << " last_included_term=" << snapshot.metadata.last_included_term
                << " membership_configuration_id=" << snapshot.metadata.membership.configuration_id
                << " active_transition=" << (snapshot.metadata.membership.has_active_transition ? "true" : "false")
                << " voters=" << snapshot.metadata.membership.voters.size()
                << " learners=" << snapshot.metadata.membership.learners.size()
                << " payload_length=" << snapshot.payload.size()
                << " checksum=OK\n";
            return common::Status::OK();
        }

        common::Status DumpDirectory(const std::filesystem::path &path,
                                     std::ostream &out)
        {
            const std::filesystem::path hard_state = path / "hard_state.bin";
            const std::filesystem::path log = path / "log.bin";
            const std::filesystem::path snapshot = path / "snapshot.bin";
            std::error_code ec;
            if (!std::filesystem::exists(hard_state, ec) ||
                !std::filesystem::exists(log, ec))
            {
                return common::Status::NotFound("storage directory must contain hard_state.bin and log.bin");
            }
            common::Status status = DumpRecords(hard_state, out);
            if (!status.ok())
            {
                return status;
            }
            status = DumpRecords(log, out);
            if (!status.ok())
            {
                return status;
            }
            if (std::filesystem::exists(snapshot, ec))
            {
                status = DumpSnapshot(snapshot, out);
                if (!status.ok())
                {
                    return status;
                }
            }
            return common::Status::OK();
        }

    } // namespace

    int RunWalDump(int argc,
                   const char *const argv[],
                   std::ostream &out,
                   std::ostream &err)
    {
        if (argc < 2 || HasHelp(argc, argv))
        {
            PrintHelp(out);
            return argc < 2 ? tools::kExitUsage : tools::kExitOk;
        }
        if (argc > 3)
        {
            tools::PrintStatus(err, common::Status::InvalidArgument("too many arguments"));
            return tools::kExitUsage;
        }
        const std::filesystem::path path(argv[1]);
        const bool force_snapshot = argc == 3 && std::string_view(argv[2]) == "--snapshot";
        if (argc == 3 && !force_snapshot)
        {
            tools::PrintStatus(err, common::Status::InvalidArgument("unknown option"));
            return tools::kExitUsage;
        }

        std::error_code ec;
        common::Status status;
        if (std::filesystem::is_directory(path, ec))
        {
            status = force_snapshot
                         ? common::Status::InvalidArgument("--snapshot expects a file")
                         : DumpDirectory(path, out);
        }
        else if (force_snapshot || path.filename() == "snapshot.bin")
        {
            status = DumpSnapshot(path, out);
        }
        else
        {
            status = DumpRecords(path, out);
        }

        if (status.ok())
        {
            return tools::kExitOk;
        }
        tools::PrintStatus(err, status);
        return status.code() == common::StatusCode::kInvalidArgument ||
                       status.code() == common::StatusCode::kNotFound
                   ? tools::kExitUsage
                   : tools::kExitStorage;
    }

} // namespace cpr::tools::wal_dump
