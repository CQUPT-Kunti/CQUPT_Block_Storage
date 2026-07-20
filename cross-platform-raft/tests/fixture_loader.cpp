#include "fixture_loader.h"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>

namespace cpr::tests
{
    namespace
    {

        constexpr std::uint32_t kFixtureVersion = 1;
        constexpr std::uintmax_t kMaxFixtureFileBytes = 64 * 1024;
        constexpr std::size_t kMaxFixtureTextBytes = 64 * 1024;
        constexpr std::size_t kMaxNodes = 64;
        constexpr std::size_t kMaxSteps = 1024;
        constexpr std::size_t kMaxExpectations = 1024;
        constexpr std::size_t kMaxStringBytes = 256;

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        std::string Trim(std::string_view text)
        {
            std::size_t first = 0;
            while (first < text.size() &&
                   (text[first] == ' ' || text[first] == '\t'))
            {
                ++first;
            }
            std::size_t last = text.size();
            while (last > first &&
                   (text[last - 1] == ' ' || text[last - 1] == '\t' ||
                    text[last - 1] == '\r'))
            {
                --last;
            }
            return std::string(text.substr(first, last - first));
        }

        bool IsCommentOrEmpty(const std::string &line)
        {
            return line.empty() || line[0] == '#';
        }

        bool IsChecksumLine(const std::string &line)
        {
            const std::size_t colon = line.find(':');
            if (colon == std::string::npos)
            {
                return false;
            }
            return Trim(std::string_view(line).substr(0, colon)) == "checksum";
        }

        std::vector<std::string> SplitLines(std::string_view text)
        {
            std::vector<std::string> lines;
            std::size_t start = 0;
            while (start <= text.size())
            {
                const std::size_t end = text.find('\n', start);
                const std::size_t count =
                    end == std::string_view::npos ? text.size() - start : end - start;
                lines.push_back(Trim(text.substr(start, count)));
                if (end == std::string_view::npos)
                {
                    break;
                }
                start = end + 1;
            }
            return lines;
        }

        std::string CanonicalTextForChecksum(std::string_view text)
        {
            std::string canonical;
            for (const std::string &line : SplitLines(text))
            {
                if (IsCommentOrEmpty(line) || IsChecksumLine(line))
                {
                    continue;
                }
                canonical += line;
                canonical.push_back('\n');
            }
            return canonical;
        }

        common::Status RequireString(const std::string &value,
                                     const char *field)
        {
            if (value.empty())
            {
                return Invalid(std::string(field) + " must not be empty");
            }
            if (value.size() > kMaxStringBytes)
            {
                return common::Status::ResourceExhausted(
                    std::string(field) + " exceeds fixture string limit");
            }
            return common::Status::OK();
        }

        common::Status ParseU64(const std::string &text,
                                const char *field,
                                std::uint64_t *value)
        {
            if (value == nullptr)
            {
                return Invalid("numeric output must not be null");
            }
            if (text.empty() || text[0] == '-')
            {
                return Invalid(std::string(field) + " must be an unsigned integer");
            }
            std::uint64_t parsed = 0;
            const char *begin = text.data();
            const char *end = text.data() + text.size();
            const auto result = std::from_chars(begin, end, parsed);
            if (result.ec != std::errc() || result.ptr != end)
            {
                return Invalid(std::string(field) + " is not a valid integer");
            }
            *value = parsed;
            return common::Status::OK();
        }

        common::Status ParseU32(const std::string &text,
                                const char *field,
                                std::uint32_t *value)
        {
            std::uint64_t parsed = 0;
            common::Status status = ParseU64(text, field, &parsed);
            if (!status.ok())
            {
                return status;
            }
            if (parsed > UINT32_MAX)
            {
                return Invalid(std::string(field) + " exceeds uint32 range");
            }
            *value = static_cast<std::uint32_t>(parsed);
            return common::Status::OK();
        }

        std::vector<std::string> SplitCsv(const std::string &text)
        {
            std::vector<std::string> parts;
            std::size_t start = 0;
            while (start <= text.size())
            {
                const std::size_t end = text.find(',', start);
                const std::size_t count =
                    end == std::string::npos ? text.size() - start : end - start;
                parts.push_back(Trim(std::string_view(text).substr(start, count)));
                if (end == std::string::npos)
                {
                    break;
                }
                start = end + 1;
            }
            return parts;
        }

        common::Status ParseRole(const std::string &text,
                                 FixtureNodeRole *role)
        {
            if (role == nullptr)
            {
                return Invalid("role output must not be null");
            }
            if (text == "follower")
            {
                *role = FixtureNodeRole::FOLLOWER;
                return common::Status::OK();
            }
            if (text == "candidate")
            {
                *role = FixtureNodeRole::CANDIDATE;
                return common::Status::OK();
            }
            if (text == "leader")
            {
                *role = FixtureNodeRole::LEADER;
                return common::Status::OK();
            }
            if (text == "learner")
            {
                *role = FixtureNodeRole::LEARNER;
                return common::Status::OK();
            }
            return Invalid("unknown fixture node role: " + text);
        }

        common::Status ParseStepType(const std::string &text,
                                     FixtureStepType *type)
        {
            if (type == nullptr)
            {
                return Invalid("step type output must not be null");
            }
            if (text == "tick")
            {
                *type = FixtureStepType::TICK;
                return common::Status::OK();
            }
            if (text == "propose")
            {
                *type = FixtureStepType::PROPOSE;
                return common::Status::OK();
            }
            if (text == "deliver")
            {
                *type = FixtureStepType::DELIVER;
                return common::Status::OK();
            }
            if (text == "partition")
            {
                *type = FixtureStepType::PARTITION;
                return common::Status::OK();
            }
            if (text == "heal")
            {
                *type = FixtureStepType::HEAL;
                return common::Status::OK();
            }
            return Invalid("unknown fixture step type: " + text);
        }

        common::Status ParseNode(const std::string &value,
                                 FixtureNode *node)
        {
            if (node == nullptr)
            {
                return Invalid("node output must not be null");
            }
            const std::vector<std::string> parts = SplitCsv(value);
            if (parts.size() != 5)
            {
                return Invalid("node must have id,name,role,host,port");
            }

            common::Status status = ParseU64(parts[0], "node id", &node->node_id);
            if (!status.ok())
            {
                return status;
            }
            if (node->node_id == common::kInvalidNodeId)
            {
                return Invalid("node id must be positive");
            }
            status = RequireString(parts[1], "node name");
            if (!status.ok())
            {
                return status;
            }
            node->name = parts[1];
            status = ParseRole(parts[2], &node->role);
            if (!status.ok())
            {
                return status;
            }
            status = RequireString(parts[3], "node host");
            if (!status.ok())
            {
                return status;
            }
            node->address.host = parts[3];
            std::uint64_t port = 0;
            status = ParseU64(parts[4], "node port", &port);
            if (!status.ok())
            {
                return status;
            }
            if (port == 0 || port > UINT16_MAX)
            {
                return Invalid("node port must be in range 1..65535");
            }
            node->address.port = static_cast<std::uint16_t>(port);
            return common::Status::OK();
        }

        common::Status ParseStep(const std::string &value,
                                 FixtureStep *step)
        {
            if (step == nullptr)
            {
                return Invalid("step output must not be null");
            }
            const std::vector<std::string> parts = SplitCsv(value);
            if (parts.size() < 3 || parts.size() > 5)
            {
                return Invalid("step must have id,type,node[,target][,payload]");
            }

            common::Status status = ParseU64(parts[0], "step id", &step->step_id);
            if (!status.ok())
            {
                return status;
            }
            if (step->step_id == 0)
            {
                return Invalid("step id must be positive");
            }
            status = ParseStepType(parts[1], &step->type);
            if (!status.ok())
            {
                return status;
            }
            status = ParseU64(parts[2], "step node id", &step->node_id);
            if (!status.ok())
            {
                return status;
            }
            if (step->node_id == common::kInvalidNodeId)
            {
                return Invalid("step node id must be positive");
            }
            if (parts.size() >= 4 && !parts[3].empty())
            {
                status = ParseU64(parts[3], "step target node id", &step->target_node_id);
                if (!status.ok())
                {
                    return status;
                }
            }
            if (parts.size() == 5)
            {
                status = RequireString(parts[4], "step payload");
                if (!status.ok())
                {
                    return status;
                }
                step->payload = parts[4];
            }
            return common::Status::OK();
        }

        common::Status ParseExpectation(const std::string &value,
                                        FixtureExpectation *expectation)
        {
            if (expectation == nullptr)
            {
                return Invalid("expectation output must not be null");
            }
            const std::vector<std::string> parts = SplitCsv(value);
            if (parts.size() != 2)
            {
                return Invalid("expectation must have key,value");
            }
            common::Status status = RequireString(parts[0], "expectation key");
            if (!status.ok())
            {
                return status;
            }
            status = RequireString(parts[1], "expectation value");
            if (!status.ok())
            {
                return status;
            }
            expectation->key = parts[0];
            expectation->value = parts[1];
            return common::Status::OK();
        }

        std::string AddressKey(const raft::NodeAddress &address)
        {
            return address.host + ":" + std::to_string(address.port);
        }

        common::Status ValidateFixture(const Fixture &fixture)
        {
            if (fixture.version != kFixtureVersion)
            {
                return Invalid("unsupported fixture version");
            }
            common::Status status = RequireString(fixture.scenario, "scenario");
            if (!status.ok())
            {
                return status;
            }
            if (fixture.nodes.empty())
            {
                return Invalid("fixture must declare at least one node");
            }
            if (fixture.nodes.size() > kMaxNodes)
            {
                return common::Status::ResourceExhausted("fixture has too many nodes");
            }
            if (fixture.steps.size() > kMaxSteps)
            {
                return common::Status::ResourceExhausted("fixture has too many steps");
            }
            if (fixture.expectations.size() > kMaxExpectations)
            {
                return common::Status::ResourceExhausted("fixture has too many expectations");
            }

            std::set<common::NodeId> node_ids;
            std::set<std::string> node_names;
            std::set<std::string> addresses;
            std::size_t leaders = 0;
            for (const FixtureNode &node : fixture.nodes)
            {
                if (!node_ids.insert(node.node_id).second)
                {
                    return Invalid("duplicate fixture node id");
                }
                if (!node_names.insert(node.name).second)
                {
                    return Invalid("duplicate fixture node name");
                }
                if (!addresses.insert(AddressKey(node.address)).second)
                {
                    return Invalid("duplicate fixture node address");
                }
                if (node.role == FixtureNodeRole::LEADER)
                {
                    ++leaders;
                }
            }
            if (leaders > 1)
            {
                return Invalid("fixture may declare at most one initial leader");
            }

            std::set<std::uint64_t> step_ids;
            for (const FixtureStep &step : fixture.steps)
            {
                if (!step_ids.insert(step.step_id).second)
                {
                    return Invalid("duplicate fixture step id");
                }
                if (node_ids.find(step.node_id) == node_ids.end())
                {
                    return Invalid("fixture step references unknown node");
                }
                if (step.target_node_id != common::kInvalidNodeId &&
                    node_ids.find(step.target_node_id) == node_ids.end())
                {
                    return Invalid("fixture step references unknown target node");
                }
                if ((step.type == FixtureStepType::DELIVER ||
                     step.type == FixtureStepType::PARTITION ||
                     step.type == FixtureStepType::HEAL) &&
                    step.target_node_id == common::kInvalidNodeId)
                {
                    return Invalid("step type requires a target node");
                }
            }
            return common::Status::OK();
        }

    } // namespace

    common::ChecksumValue FixtureLoader::ComputeChecksumForText(std::string_view text)
    {
        const std::string canonical = CanonicalTextForChecksum(text);
        return common::Checksum::Compute(canonical);
    }

    common::Status FixtureLoader::LoadFromString(std::string_view text,
                                                 Fixture *fixture)
    {
        if (fixture == nullptr)
        {
            return Invalid("fixture output must not be null");
        }
        if (text.empty())
        {
            return Invalid("fixture text must not be empty");
        }
        if (text.size() > kMaxFixtureTextBytes)
        {
            return common::Status::ResourceExhausted("fixture text exceeds size limit");
        }

        Fixture candidate;
        bool saw_version = false;
        bool saw_scenario = false;
        bool saw_checksum = false;

        const common::ChecksumValue computed = ComputeChecksumForText(text);
        std::size_t line_number = 0;
        for (const std::string &line : SplitLines(text))
        {
            ++line_number;
            if (IsCommentOrEmpty(line))
            {
                continue;
            }
            const std::size_t colon = line.find(':');
            if (colon == std::string::npos)
            {
                return Invalid("fixture line " + std::to_string(line_number) +
                               " is missing ':'");
            }

            const std::string key = Trim(std::string_view(line).substr(0, colon));
            const std::string value = Trim(std::string_view(line).substr(colon + 1));
            if (key.empty())
            {
                return Invalid("fixture line " + std::to_string(line_number) +
                               " has empty key");
            }

            common::Status status;
            if (key == "version")
            {
                if (saw_version)
                {
                    return Invalid("duplicate fixture version field");
                }
                saw_version = true;
                status = ParseU32(value, "fixture version", &candidate.version);
            }
            else if (key == "scenario")
            {
                if (saw_scenario)
                {
                    return Invalid("duplicate fixture scenario field");
                }
                saw_scenario = true;
                status = RequireString(value, "scenario");
                if (status.ok())
                {
                    candidate.scenario = value;
                }
            }
            else if (key == "checksum")
            {
                if (saw_checksum)
                {
                    return Invalid("duplicate fixture checksum field");
                }
                saw_checksum = true;
                status = ParseU64(value, "fixture checksum", &candidate.checksum);
            }
            else if (key == "node")
            {
                if (candidate.nodes.size() >= kMaxNodes)
                {
                    return common::Status::ResourceExhausted("fixture has too many nodes");
                }
                FixtureNode node;
                status = ParseNode(value, &node);
                if (status.ok())
                {
                    candidate.nodes.push_back(std::move(node));
                }
            }
            else if (key == "step")
            {
                if (candidate.steps.size() >= kMaxSteps)
                {
                    return common::Status::ResourceExhausted("fixture has too many steps");
                }
                FixtureStep step;
                status = ParseStep(value, &step);
                if (status.ok())
                {
                    candidate.steps.push_back(std::move(step));
                }
            }
            else if (key == "expect")
            {
                if (candidate.expectations.size() >= kMaxExpectations)
                {
                    return common::Status::ResourceExhausted("fixture has too many expectations");
                }
                FixtureExpectation expectation;
                status = ParseExpectation(value, &expectation);
                if (status.ok())
                {
                    candidate.expectations.push_back(std::move(expectation));
                }
            }
            else
            {
                return Invalid("unknown fixture field: " + key);
            }

            if (!status.ok())
            {
                return status;
            }
        }

        if (!saw_version)
        {
            return Invalid("fixture version is required");
        }
        if (!saw_scenario)
        {
            return Invalid("fixture scenario is required");
        }
        if (!saw_checksum)
        {
            return Invalid("fixture checksum is required");
        }
        if (candidate.checksum != computed)
        {
            return common::Status::Corruption("fixture checksum mismatch");
        }

        common::Status status = ValidateFixture(candidate);
        if (!status.ok())
        {
            return status;
        }

        *fixture = std::move(candidate);
        return common::Status::OK();
    }

    common::Status FixtureLoader::LoadFromFile(const std::filesystem::path &path,
                                               Fixture *fixture)
    {
        if (fixture == nullptr)
        {
            return Invalid("fixture output must not be null");
        }

        std::error_code ec;
        if (!std::filesystem::exists(path, ec))
        {
            return common::Status::NotFound("fixture file does not exist: " +
                                            path.string());
        }
        if (ec)
        {
            return common::Status::IoError("failed to inspect fixture path: " +
                                           path.string());
        }
        if (!std::filesystem::is_regular_file(path, ec))
        {
            return Invalid("fixture path is not a regular file: " + path.string());
        }
        if (ec)
        {
            return common::Status::IoError("failed to inspect fixture file: " +
                                           path.string());
        }

        const std::uintmax_t size = std::filesystem::file_size(path, ec);
        if (ec)
        {
            return common::Status::IoError("failed to read fixture file size: " +
                                           path.string());
        }
        if (size > kMaxFixtureFileBytes)
        {
            return common::Status::ResourceExhausted("fixture file exceeds size limit: " +
                                                     path.string());
        }

        std::ifstream input(path, std::ios::binary);
        if (!input)
        {
            return common::Status::IoError("failed to open fixture file: " +
                                           path.string());
        }

        std::ostringstream buffer;
        buffer << input.rdbuf();
        if (!input.good() && !input.eof())
        {
            return common::Status::IoError("failed to read fixture file: " +
                                           path.string());
        }
        return LoadFromString(buffer.str(), fixture);
    }

} // namespace cpr::tests
