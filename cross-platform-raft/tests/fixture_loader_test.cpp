#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "fixture_loader.h"

namespace cpr::tests
{
    namespace
    {

        std::string WithChecksum(const std::string &body)
        {
            return body + "checksum: " +
                   std::to_string(FixtureLoader::ComputeChecksumForText(body)) +
                   "\n";
        }

        std::string MinimalBody()
        {
            return "# fixture loader test\n"
                   "version: 1\n"
                   "scenario: basic-election\n"
                   "node: 1,node-1,leader,127.0.0.1,9001\n"
                   "node: 2,node-2,follower,127.0.0.1,9002\n"
                   "step: 1,tick,1\n"
                   "step: 2,deliver,1,2,append\n"
                   "expect: leader,1\n";
        }

        class TempDir
        {
        public:
            TempDir()
            {
                const std::filesystem::path base =
                    std::filesystem::temp_directory_path();
                for (int i = 0; i < 100; ++i)
                {
                    path_ = base / ("cpr fixture loader " + std::to_string(i) +
                                    "-" + std::to_string(reinterpret_cast<std::uintptr_t>(this)));
                    std::error_code ec;
                    if (std::filesystem::create_directory(path_, ec))
                    {
                        return;
                    }
                }
                path_.clear();
            }

            ~TempDir()
            {
                if (!path_.empty())
                {
                    std::error_code ec;
                    std::filesystem::remove_all(path_, ec);
                }
            }

            const std::filesystem::path &path() const noexcept
            {
                return path_;
            }

        private:
            std::filesystem::path path_;
        };

        void WriteFile(const std::filesystem::path &path,
                       const std::string &content)
        {
            std::ofstream output(path, std::ios::binary);
            ASSERT_TRUE(output.good());
            output << content;
            ASSERT_TRUE(output.good());
        }

        void ExpectInvalid(const std::string &body)
        {
            Fixture fixture;
            const common::Status status =
                FixtureLoader::LoadFromString(WithChecksum(body), &fixture);
            EXPECT_FALSE(status.ok());
        }

        TEST(FixtureLoaderTest, LoadsMinimalLegalFixtureFromString)
        {
            Fixture fixture;
            const common::Status status =
                FixtureLoader::LoadFromString(WithChecksum(MinimalBody()), &fixture);

            ASSERT_TRUE(status.ok()) << status.ToString();
            EXPECT_EQ(fixture.version, 1U);
            EXPECT_EQ(fixture.scenario, "basic-election");
            ASSERT_EQ(fixture.nodes.size(), 2U);
            EXPECT_EQ(fixture.nodes[0].node_id, 1U);
            EXPECT_EQ(fixture.nodes[0].name, "node-1");
            EXPECT_EQ(fixture.nodes[0].role, FixtureNodeRole::LEADER);
            EXPECT_EQ(fixture.nodes[0].address.host, "127.0.0.1");
            EXPECT_EQ(fixture.nodes[0].address.port, 9001);
            ASSERT_EQ(fixture.steps.size(), 2U);
            EXPECT_EQ(fixture.steps[1].type, FixtureStepType::DELIVER);
            EXPECT_EQ(fixture.steps[1].target_node_id, 2U);
            EXPECT_EQ(fixture.steps[1].payload, "append");
            ASSERT_EQ(fixture.expectations.size(), 1U);
            EXPECT_EQ(fixture.expectations[0].key, "leader");
            EXPECT_EQ(fixture.expectations[0].value, "1");
        }

        TEST(FixtureLoaderTest, LoadFromFileSupportsSpacesAndReportsMissingOrDirectory)
        {
            TempDir dir;
            ASSERT_FALSE(dir.path().empty());
            const std::filesystem::path file = dir.path() / "valid fixture.cprfix";
            WriteFile(file, WithChecksum(MinimalBody()));

            Fixture fixture;
            EXPECT_TRUE(FixtureLoader::LoadFromFile(file, &fixture).ok());
            EXPECT_EQ(fixture.scenario, "basic-election");

            Fixture missing = fixture;
            EXPECT_EQ(FixtureLoader::LoadFromFile(dir.path() / "missing.cprfix",
                                                  &missing)
                          .code(),
                      common::StatusCode::kNotFound);
            EXPECT_EQ(missing.scenario, "basic-election");

            EXPECT_EQ(FixtureLoader::LoadFromFile(dir.path(), &missing).code(),
                      common::StatusCode::kInvalidArgument);
        }

        TEST(FixtureLoaderTest, RejectsSyntaxVersionChecksumAndUnknownFields)
        {
            Fixture fixture;
            EXPECT_EQ(FixtureLoader::LoadFromString("", &fixture).code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_EQ(FixtureLoader::LoadFromString("version 1\n", &fixture).code(),
                      common::StatusCode::kInvalidArgument);

            ExpectInvalid("scenario: missing-version\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n");
            ExpectInvalid("version: 2\n"
                          "scenario: bad-version\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n");
            ExpectInvalid("version: 1\n"
                          "scenario: unknown-field\n"
                          "foo: bar\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n");

            const std::string bad_checksum = MinimalBody() + "checksum: 123\n";
            EXPECT_EQ(FixtureLoader::LoadFromString(bad_checksum, &fixture).code(),
                      common::StatusCode::kCorruption);
        }

        TEST(FixtureLoaderTest, RejectsInvalidFieldsAndCrossReferences)
        {
            ExpectInvalid("version: 1\n"
                          "scenario: bad-node\n"
                          "node: 0,node-1,leader,127.0.0.1,9001\n");
            ExpectInvalid("version: 1\n"
                          "scenario: bad-port\n"
                          "node: 1,node-1,leader,127.0.0.1,70000\n");
            ExpectInvalid("version: 1\n"
                          "scenario: duplicate-node\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n"
                          "node: 1,node-2,follower,127.0.0.1,9002\n");
            ExpectInvalid("version: 1\n"
                          "scenario: duplicate-address\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n"
                          "node: 2,node-2,follower,127.0.0.1,9001\n");
            ExpectInvalid("version: 1\n"
                          "scenario: two-leaders\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n"
                          "node: 2,node-2,leader,127.0.0.1,9002\n");
            ExpectInvalid("version: 1\n"
                          "scenario: unknown-ref\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n"
                          "step: 1,deliver,1,2\n");
            ExpectInvalid("version: 1\n"
                          "scenario: missing-target\n"
                          "node: 1,node-1,leader,127.0.0.1,9001\n"
                          "step: 1,partition,1\n");
            ExpectInvalid("version: 1\n"
                          "scenario: bad-number\n"
                          "node: -1,node-1,leader,127.0.0.1,9001\n");
        }

        TEST(FixtureLoaderTest, LoadingFailureDoesNotModifyExistingOutput)
        {
            Fixture fixture;
            ASSERT_TRUE(FixtureLoader::LoadFromString(WithChecksum(MinimalBody()),
                                                      &fixture)
                            .ok());
            ASSERT_EQ(fixture.scenario, "basic-election");

            const common::Status status =
                FixtureLoader::LoadFromString(WithChecksum("version: 1\n"
                                                           "scenario: invalid\n"),
                                              &fixture);

            EXPECT_FALSE(status.ok());
            EXPECT_EQ(fixture.scenario, "basic-election");
            ASSERT_EQ(fixture.nodes.size(), 2U);
        }

        TEST(FixtureLoaderTest, SameContentLoadsDeterministicallyAndPreservesStepOrder)
        {
            Fixture first;
            Fixture second;
            const std::string text = WithChecksum(MinimalBody());

            ASSERT_TRUE(FixtureLoader::LoadFromString(text, &first).ok());
            ASSERT_TRUE(FixtureLoader::LoadFromString(text, &second).ok());

            EXPECT_EQ(first.checksum, second.checksum);
            ASSERT_EQ(first.steps.size(), second.steps.size());
            for (std::size_t i = 0; i < first.steps.size(); ++i)
            {
                EXPECT_EQ(first.steps[i].step_id, second.steps[i].step_id);
                EXPECT_EQ(first.steps[i].type, second.steps[i].type);
                EXPECT_EQ(first.steps[i].payload, second.steps[i].payload);
            }
            EXPECT_EQ(first.steps[0].step_id, 1U);
            EXPECT_EQ(first.steps[1].step_id, 2U);
        }

        TEST(FixtureLoaderTest, RejectsResourceLimits)
        {
            Fixture fixture;
            std::string too_large(70 * 1024, 'a');
            EXPECT_EQ(FixtureLoader::LoadFromString(too_large, &fixture).code(),
                      common::StatusCode::kResourceExhausted);

            std::string many_nodes = "version: 1\nscenario: many-nodes\n";
            for (int i = 1; i <= 65; ++i)
            {
                many_nodes += "node: " + std::to_string(i) +
                              ",node-" + std::to_string(i) +
                              ",follower,127.0.0." + std::to_string(i) +
                              "," + std::to_string(9000 + i) + "\n";
            }
            EXPECT_EQ(FixtureLoader::LoadFromString(WithChecksum(many_nodes),
                                                    &fixture)
                          .code(),
                      common::StatusCode::kResourceExhausted);

            const std::string long_name(300, 'x');
            ExpectInvalid("version: 1\n"
                          "scenario: long-name\n"
                          "node: 1," +
                          long_name + ",leader,127.0.0.1,9001\n");
        }

        TEST(FixtureLoaderTest, NullOutputIsRejected)
        {
            EXPECT_EQ(FixtureLoader::LoadFromString(WithChecksum(MinimalBody()),
                                                    nullptr)
                          .code(),
                      common::StatusCode::kInvalidArgument);
            EXPECT_EQ(FixtureLoader::LoadFromFile("unused.cprfix", nullptr).code(),
                      common::StatusCode::kInvalidArgument);
        }

    } // namespace
} // namespace cpr::tests
