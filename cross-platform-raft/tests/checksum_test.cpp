#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/checksum.h"

namespace cpr::common
{
    namespace
    {

        TEST(ChecksumTest, SameContentProducesSameValue)
        {
            const std::string text = "raft-metadata";
            EXPECT_EQ(Checksum::Compute(text), Checksum::Compute(text));
        }

        TEST(ChecksumTest, DifferentContentProducesDifferentValue)
        {
            EXPECT_NE(Checksum::Compute(std::string("abc")),
                      Checksum::Compute(std::string("abd")));
        }

        TEST(ChecksumTest, EmptyContentUsesStableOffsetBasis)
        {
            EXPECT_EQ(Checksum::Compute("", 0), 14695981039346656037ULL);
            EXPECT_TRUE(Checksum::Verify("", 0, 14695981039346656037ULL));
        }

        TEST(ChecksumTest, KnownInputHasStableValue)
        {
            EXPECT_EQ(Checksum::Compute(std::string("abc")), 16654208175385433931ULL);
            EXPECT_EQ(Checksum::Compute(std::vector<std::uint8_t>{'a', 'b', 'c'}),
                      16654208175385433931ULL);
        }

    } // namespace
} // namespace cpr::common
