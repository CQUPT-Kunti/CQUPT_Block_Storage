#include <thread>

#include <gtest/gtest.h>

#include "common/bounded_queue.h"

namespace cpr::common
{
    namespace
    {

        TEST(BoundedQueueTest, PushAndPopRoundTrip)
        {
            BoundedQueue<int> queue(2);
            int value = 0;

            EXPECT_TRUE(queue.Push(7).ok());
            EXPECT_TRUE(queue.Pop(&value).ok());
            EXPECT_EQ(value, 7);
        }

        TEST(BoundedQueueTest, PreservesFifoOrder)
        {
            BoundedQueue<int> queue(3);
            int first = 0;
            int second = 0;

            EXPECT_TRUE(queue.Push(1).ok());
            EXPECT_TRUE(queue.Push(2).ok());
            EXPECT_TRUE(queue.Pop(&first).ok());
            EXPECT_TRUE(queue.Pop(&second).ok());
            EXPECT_EQ(first, 1);
            EXPECT_EQ(second, 2);
        }

        TEST(BoundedQueueTest, FullQueueReturnsResourceExhausted)
        {
            BoundedQueue<int> queue(1);
            EXPECT_TRUE(queue.Push(1).ok());
            const Status status = queue.Push(2);
            EXPECT_EQ(status.code(), StatusCode::kResourceExhausted);
        }

        TEST(BoundedQueueTest, ZeroCapacityReturnsResourceExhausted)
        {
            BoundedQueue<int> queue(0);
            const Status status = queue.Push(1);
            EXPECT_EQ(status.code(), StatusCode::kResourceExhausted);
        }

        TEST(BoundedQueueTest, CloseMakesPushBusyAndPopRetryLaterWhenEmpty)
        {
            BoundedQueue<int> queue(1);
            queue.Close();

            int value = 0;
            EXPECT_EQ(queue.Push(1).code(), StatusCode::kBusy);
            EXPECT_EQ(queue.Pop(&value).code(), StatusCode::kRetryLater);
        }

        TEST(BoundedQueueTest, EmptyPopUnblocksAfterClose)
        {
            BoundedQueue<int> queue(1);
            Status result = Status::InternalError("not set");
            int value = 0;

            std::thread worker([&]()
                               { result = queue.Pop(&value); });
            queue.Close();
            worker.join();

            EXPECT_EQ(result.code(), StatusCode::kRetryLater);
        }

        TEST(BoundedQueueTest, NullPopPointerReturnsInvalidArgument)
        {
            BoundedQueue<int> queue(1);
            const Status status = queue.Pop(nullptr);
            EXPECT_EQ(status.code(), StatusCode::kInvalidArgument);
        }

    } // namespace
} // namespace cpr::common
