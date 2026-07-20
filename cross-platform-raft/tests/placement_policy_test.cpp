#include <algorithm>
#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "store/placement_policy.h"
#include "store/store_registry.h"
#include "store/store_types.h"

namespace cpr::store
{
    namespace
    {

        using cpr::common::StatusCode;

        StoreInfo MakeStore(StoreId id,
                            std::uint64_t capacity,
                            std::uint64_t used,
                            StoreState state = StoreState::RUNNING,
                            const char *host = "127.0.0.1")
        {
            StoreInfo store;
            store.id = id;
            store.address = {host, static_cast<std::uint16_t>(9000 + id)};
            store.capacity_bytes = capacity;
            store.used_bytes = used;
            store.state = state;
            store.generation = 1;
            store.last_heartbeat_ms = 7;
            return store;
        }

        PlacementRequest Request(std::uint32_t replicas,
                                 std::uint64_t required_capacity)
        {
            PlacementRequest request;
            request.replica_count = replicas;
            request.required_capacity_bytes = required_capacity;
            return request;
        }

        class AscendingPlacementPolicy final : public IPlacementPolicy
        {
        public:
            common::Status SelectStores(
                const PlacementRequest &request,
                const std::vector<StoreInfo> &candidates,
                PlacementResult *result) const override
            {
                if (result == nullptr)
                {
                    return common::Status::InvalidArgument("result must not be null");
                }
                if (request.replica_count == 0)
                {
                    return common::Status::InvalidArgument("replicas must be positive");
                }

                std::vector<StoreId> ids;
                for (const StoreInfo &store : candidates)
                {
                    if (store.state == StoreState::RUNNING)
                    {
                        ids.push_back(store.id);
                    }
                }
                std::sort(ids.begin(), ids.end());
                if (ids.size() < request.replica_count)
                {
                    return common::Status::ResourceExhausted("not enough stores");
                }
                ids.resize(request.replica_count);
                result->selected_store_ids = std::move(ids);
                return common::Status::OK();
            }
        };

        TEST(PlacementPolicyTest, SelectsRequestedRunningStoresByCapacityThenId)
        {
            SimplePlacementPolicy policy;
            const std::vector<StoreInfo> stores = {
                MakeStore(4, 100, 20), // remaining 80
                MakeStore(1, 100, 30), // remaining 70
                MakeStore(2, 100, 20), // remaining 80, lower id than 4
                MakeStore(3, 100, 90),
            };

            PlacementResult result;
            ASSERT_TRUE(policy.SelectStores(Request(3, 10), stores, &result).ok());

            EXPECT_EQ(result.selected_store_ids,
                      (std::vector<StoreId>{2, 4, 1}));
        }

        TEST(PlacementPolicyTest, ExcludesStoppedFailedAndCapacityInsufficientStores)
        {
            SimplePlacementPolicy policy;
            const std::vector<StoreInfo> stores = {
                MakeStore(1, 100, 0, StoreState::STOPPED),
                MakeStore(2, 100, 0, StoreState::FAILED),
                MakeStore(3, 100, 95, StoreState::RUNNING),
                MakeStore(4, 100, 20, StoreState::RUNNING),
            };

            PlacementResult result;
            ASSERT_TRUE(policy.SelectStores(Request(1, 50), stores, &result).ok());
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{4}));
        }

        TEST(PlacementPolicyTest, InputOrderDoesNotAffectResult)
        {
            SimplePlacementPolicy policy;
            std::vector<StoreInfo> first = {
                MakeStore(3, 100, 40),
                MakeStore(1, 100, 20),
                MakeStore(2, 100, 20),
            };
            std::vector<StoreInfo> second = {
                first[2],
                first[0],
                first[1],
            };

            PlacementResult a;
            PlacementResult b;
            ASSERT_TRUE(policy.SelectStores(Request(2, 1), first, &a).ok());
            ASSERT_TRUE(policy.SelectStores(Request(2, 1), second, &b).ok());

            EXPECT_EQ(a.selected_store_ids, b.selected_store_ids);
            EXPECT_EQ(a.selected_store_ids, (std::vector<StoreId>{1, 2}));
        }

        TEST(PlacementPolicyTest, InvalidRequestAndCandidatesLeaveOutputUnchanged)
        {
            SimplePlacementPolicy policy;
            PlacementResult result;
            result.selected_store_ids = {99};

            EXPECT_EQ(policy.SelectStores(Request(0, 1),
                                          {MakeStore(1, 10, 0)},
                                          &result)
                          .code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));

            EXPECT_EQ(policy.SelectStores(Request(1, 0),
                                          {MakeStore(1, 10, 0)},
                                          &result)
                          .code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));

            EXPECT_EQ(policy.SelectStores(Request(1, 1),
                                          {MakeStore(0, 10, 0)},
                                          &result)
                          .code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));

            EXPECT_EQ(policy.SelectStores(Request(1, 1),
                                          {MakeStore(1, 10, 11)},
                                          &result)
                          .code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));

            StoreInfo bad_state = MakeStore(1, 10, 0);
            bad_state.state = static_cast<StoreState>(99);
            EXPECT_EQ(policy.SelectStores(Request(1, 1), {bad_state}, &result).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));
        }

        TEST(PlacementPolicyTest, DuplicateIdsAndAddressesAreRejected)
        {
            SimplePlacementPolicy policy;
            PlacementResult result;
            result.selected_store_ids = {99};

            EXPECT_EQ(policy.SelectStores(Request(1, 1),
                                          {MakeStore(1, 10, 0),
                                           MakeStore(1, 20, 0, StoreState::RUNNING, "127.0.0.2")},
                                          &result)
                          .code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));

            StoreInfo first = MakeStore(1, 10, 0);
            StoreInfo second = MakeStore(2, 20, 0);
            second.address = first.address;
            EXPECT_EQ(policy.SelectStores(Request(1, 1), {first, second}, &result).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));
        }

        TEST(PlacementPolicyTest, RunningStoreShortageAndCapacityShortageReturnResourceExhausted)
        {
            SimplePlacementPolicy policy;
            PlacementResult result;
            result.selected_store_ids = {99};

            EXPECT_EQ(policy.SelectStores(Request(2, 1),
                                          {MakeStore(1, 10, 0, StoreState::RUNNING),
                                           MakeStore(2, 10, 0, StoreState::STOPPED)},
                                          &result)
                          .code(),
                      StatusCode::kResourceExhausted);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));

            EXPECT_EQ(policy.SelectStores(Request(2, 50),
                                          {MakeStore(1, 100, 0),
                                           MakeStore(2, 100, 90),
                                           MakeStore(3, 100, 95)},
                                          &result)
                          .code(),
                      StatusCode::kResourceExhausted);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));
        }

        TEST(PlacementPolicyTest, EmptyCandidatesFailWithoutPartialResult)
        {
            SimplePlacementPolicy policy;
            PlacementResult result;
            result.selected_store_ids = {99};

            EXPECT_EQ(policy.SelectStores(Request(1, 1), {}, &result).code(),
                      StatusCode::kResourceExhausted);
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{99}));
        }

        TEST(PlacementPolicyTest, SelectionDoesNotModifyInputStores)
        {
            SimplePlacementPolicy policy;
            const std::vector<StoreInfo> original = {
                MakeStore(1, 100, 10),
                MakeStore(2, 100, 20),
            };
            std::vector<StoreInfo> stores = original;

            PlacementResult result;
            ASSERT_TRUE(policy.SelectStores(Request(1, 1), stores, &result).ok());

            ASSERT_EQ(stores.size(), original.size());
            EXPECT_EQ(stores[0].used_bytes, original[0].used_bytes);
            EXPECT_EQ(stores[0].generation, original[0].generation);
            EXPECT_EQ(stores[0].last_heartbeat_ms, original[0].last_heartbeat_ms);
            EXPECT_EQ(stores[1].used_bytes, original[1].used_bytes);
        }

        TEST(PlacementPolicyTest, PlacementSuccessDoesNotModifyStoreRegistry)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, 100, 10)).ok());
            ASSERT_TRUE(registry.UpdateHeartbeat(1, 123).ok());

            std::vector<StoreInfo> snapshot;
            ASSERT_TRUE(registry.ListStores(&snapshot).ok());

            SimplePlacementPolicy policy;
            PlacementResult result;
            ASSERT_TRUE(policy.SelectStores(Request(1, 1), snapshot, &result).ok());

            StoreInfo after;
            ASSERT_TRUE(registry.GetStore(1, &after).ok());
            EXPECT_EQ(after.used_bytes, 10U);
            EXPECT_EQ(after.generation, 1U);
            EXPECT_EQ(after.last_heartbeat_ms, 123);
        }

        TEST(PlacementPolicyTest, StoreRegistrySnapshotIsIndependentFromLaterRegistryChanges)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, 100, 0)).ok());
            ASSERT_TRUE(registry.RegisterStore(MakeStore(2, 100, 0)).ok());

            std::vector<StoreInfo> snapshot;
            ASSERT_TRUE(registry.ListStores(&snapshot).ok());

            StoreUpdate stop;
            stop.id = 1;
            stop.state = StoreState::STOPPED;
            ASSERT_TRUE(registry.UpdateStore(stop).ok());

            SimplePlacementPolicy policy;
            PlacementResult result;
            ASSERT_TRUE(policy.SelectStores(Request(1, 1), snapshot, &result).ok());
            EXPECT_EQ(result.selected_store_ids, (std::vector<StoreId>{1}));
        }

        TEST(PlacementPolicyTest, SameInputRepeatedExecutionIsDeterministic)
        {
            SimplePlacementPolicy policy;
            const std::vector<StoreInfo> stores = {
                MakeStore(3, 100, 0),
                MakeStore(2, 100, 0),
                MakeStore(1, 100, 0),
            };

            PlacementResult first;
            PlacementResult second;
            ASSERT_TRUE(policy.SelectStores(Request(2, 1), stores, &first).ok());
            ASSERT_TRUE(policy.SelectStores(Request(2, 1), stores, &second).ok());
            EXPECT_EQ(first.selected_store_ids, second.selected_store_ids);
        }

        TEST(PlacementPolicyTest, AlternatePolicyCanReplaceSimplePolicyThroughInterface)
        {
            AscendingPlacementPolicy ascending;
            IPlacementPolicy *policy = &ascending;

            PlacementResult result;
            ASSERT_TRUE(policy->SelectStores(Request(2, 1),
                                             {MakeStore(3, 1, 0),
                                              MakeStore(1, 1, 0),
                                              MakeStore(2, 1, 0)},
                                             &result)
                            .ok());

            EXPECT_EQ(result.selected_store_ids,
                      (std::vector<StoreId>{1, 2}));
        }

    } // namespace
} // namespace cpr::store
