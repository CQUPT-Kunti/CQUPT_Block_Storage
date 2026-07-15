#include <string>

#include <gtest/gtest.h>

#include "common/status.h"

namespace cpr::common {
namespace {

TEST(StatusTest, OkStatusHasExpectedDefaults) {
    const Status status = Status::OK();
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kOk);
    EXPECT_TRUE(status.message().empty());
    EXPECT_EQ(status.ToString(), "OK");
}

TEST(StatusTest, InvalidArgumentKeepsCodeAndMessage) {
    const Status status = Status::InvalidArgument("bad field");
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
    EXPECT_EQ(status.message(), "bad field");
    EXPECT_EQ(status.ToString(), "INVALID_ARGUMENT: bad field");
}

TEST(StatusTest, CommonBusyAndRetryStatusesAreAvailable) {
    const Status busy = Status::Busy("queue closed");
    const Status exhausted = Status::ResourceExhausted("full");
    const Status retry = Status::RetryLater("try again");

    EXPECT_EQ(busy.code(), StatusCode::kBusy);
    EXPECT_EQ(exhausted.code(), StatusCode::kResourceExhausted);
    EXPECT_EQ(retry.code(), StatusCode::kRetryLater);
    EXPECT_EQ(StatusCodeToString(busy.code()), std::string("BUSY"));
    EXPECT_EQ(StatusCodeToString(exhausted.code()),
              std::string("RESOURCE_EXHAUSTED"));
    EXPECT_EQ(StatusCodeToString(retry.code()), std::string("RETRY_LATER"));
}

TEST(StatusTest, OtherErrorFactoriesReturnExpectedCodes) {
    EXPECT_EQ(Status::NotFound("missing").code(), StatusCode::kNotFound);
    EXPECT_EQ(Status::IoError("io").code(), StatusCode::kIoError);
    EXPECT_EQ(Status::Corruption("corrupt").code(), StatusCode::kCorruption);
    EXPECT_EQ(Status::InternalError("internal").code(),
              StatusCode::kInternalError);
}

}  // namespace
}  // namespace cpr::common
