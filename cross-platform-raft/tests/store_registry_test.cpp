#include <cstdint>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "common/status.h"
#include "store/store_registry.h"
#include "store/store_types.h"

namespace cpr::store
{
    namespace
    {

        using cpr::common::StatusCode;

        static_assert(static_cast<std::uint8_t>(StoreState::RUNNING) == 0,
                      "StoreState RUNNING value changed");
        static_assert(static_cast<std::uint8_t>(StoreState::STOPPED) == 1,
                      "StoreState STOPPED value changed");
        static_assert(static_cast<std::uint8_t>(StoreState::FAILED) == 2,
                      "StoreState FAILED value changed");

        StoreInfo MakeStore(StoreId id,
                            const char *host,
                            std::uint16_t port,
                            std::uint64_t capacity = 100,
                            std::uint64_t used = 10)
        {
            StoreInfo store;
            store.id = id;
            store.address = {host, port};
            store.capacity_bytes = capacity;
            store.used_bytes = used;
            return store;
        }

        StoreInfo MustGet(StoreRegistry *registry, StoreId id)
        {
            StoreInfo store;
            EXPECT_TRUE(registry->GetStore(id, &store).ok());
            return store;
        }

        TEST(StoreRegistryTest, RegisterValidStoreInitializesRunningGenerationAndQueriesByIdAddress)
        {
            StoreRegistry registry;
            StoreInfo registered;

            ASSERT_TRUE(registry.RegisterStore(
                                    MakeStore(1, "127.0.0.1", 9001),
                                    &registered)
                            .ok());

            EXPECT_EQ(registered.id, 1U);
            EXPECT_EQ(registered.state, StoreState::RUNNING);
            EXPECT_EQ(registered.generation, 1U);
            EXPECT_EQ(registered.last_heartbeat_ms, 0);

            StoreInfo by_id;
            ASSERT_TRUE(registry.GetStore(1, &by_id).ok());
            EXPECT_EQ(by_id.address.host, "127.0.0.1");
            EXPECT_EQ(by_id.address.port, 9001U);

            StoreInfo by_address;
            ASSERT_TRUE(registry.GetStoreByAddress({"127.0.0.1", 9001}, &by_address).ok());
            EXPECT_EQ(by_address.id, 1U);
        }

        TEST(StoreRegistryTest, RegisterRejectsInvalidFieldsAndLeavesRegistryUnchanged)
        {
            StoreRegistry registry;
            StoreInfo out;

            EXPECT_EQ(registry.RegisterStore(MakeStore(0, "h", 1), &out).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(registry.RegisterStore(MakeStore(1, "", 1), &out).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(registry.RegisterStore(MakeStore(1, "h", 0), &out).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(registry.RegisterStore(MakeStore(1, "h", 1, 0, 0), &out).code(),
                      StatusCode::kInvalidArgument);
            EXPECT_EQ(registry.RegisterStore(MakeStore(1, "h", 1, 10, 11), &out).code(),
                      StatusCode::kInvalidArgument);

            std::vector<StoreInfo> stores;
            ASSERT_TRUE(registry.ListStores(&stores).ok());
            EXPECT_TRUE(stores.empty());
        }

        TEST(StoreRegistryTest, DuplicateIdAndAddressAreRejected)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1)).ok());

            EXPECT_EQ(registry.RegisterStore(MakeStore(1, "h1", 1)).code(),
                      StatusCode::kBusy);
            EXPECT_EQ(registry.RegisterStore(MakeStore(1, "h2", 2)).code(),
                      StatusCode::kBusy);
            EXPECT_EQ(registry.RegisterStore(MakeStore(2, "h1", 1)).code(),
                      StatusCode::kBusy);

            std::vector<StoreInfo> stores;
            ASSERT_TRUE(registry.ListStores(&stores).ok());
            ASSERT_EQ(stores.size(), 1U);
            EXPECT_EQ(stores[0].id, 1U);
        }

        TEST(StoreRegistryTest, QueryReturnsCopyAndListsAreSortedByStoreId)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(2, "h2", 2)).ok());
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1)).ok());

            StoreInfo copy = MustGet(&registry, 1);
            copy.used_bytes = 99;
            EXPECT_EQ(MustGet(&registry, 1).used_bytes, 10U);

            std::vector<StoreInfo> stores;
            ASSERT_TRUE(registry.ListStores(&stores).ok());
            ASSERT_EQ(stores.size(), 2U);
            EXPECT_EQ(stores[0].id, 1U);
            EXPECT_EQ(stores[1].id, 2U);

            StoreInfo missing;
            EXPECT_EQ(registry.GetStore(999, &missing).code(),
                      StatusCode::kNotFound);
        }

        TEST(StoreRegistryTest, PersistentUpdatesAdvanceGenerationAndHonorExpectedGeneration)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1)).ok());

            StoreUpdate update;
            update.id = 1;
            update.expected_generation = 1;
            update.capacity_bytes = 200;
            update.used_bytes = 50;
            StoreInfo updated;
            ASSERT_TRUE(registry.UpdateStore(update, &updated).ok());
            EXPECT_EQ(updated.capacity_bytes, 200U);
            EXPECT_EQ(updated.used_bytes, 50U);
            EXPECT_EQ(updated.generation, 2U);

            StoreUpdate conflict;
            conflict.id = 1;
            conflict.expected_generation = 1;
            conflict.used_bytes = 60;
            EXPECT_EQ(registry.UpdateStore(conflict).code(), StatusCode::kBusy);

            const StoreInfo unchanged = MustGet(&registry, 1);
            EXPECT_EQ(unchanged.used_bytes, 50U);
            EXPECT_EQ(unchanged.generation, 2U);
        }

        TEST(StoreRegistryTest, InvalidUpdateDoesNotPartiallyModifyState)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1, 100, 10)).ok());

            StoreUpdate too_used;
            too_used.id = 1;
            too_used.capacity_bytes = 20;
            too_used.used_bytes = 30;
            EXPECT_EQ(registry.UpdateStore(too_used).code(),
                      StatusCode::kInvalidArgument);

            StoreUpdate bad_state;
            bad_state.id = 1;
            bad_state.state = static_cast<StoreState>(99);
            EXPECT_EQ(registry.UpdateStore(bad_state).code(),
                      StatusCode::kInvalidArgument);

            StoreUpdate no_change;
            no_change.id = 1;
            EXPECT_EQ(registry.UpdateStore(no_change).code(),
                      StatusCode::kInvalidArgument);

            const StoreInfo store = MustGet(&registry, 1);
            EXPECT_EQ(store.capacity_bytes, 100U);
            EXPECT_EQ(store.used_bytes, 10U);
            EXPECT_EQ(store.state, StoreState::RUNNING);
            EXPECT_EQ(store.generation, 1U);
        }

        TEST(StoreRegistryTest, StateTransitionsAffectRunningList)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1)).ok());
            ASSERT_TRUE(registry.RegisterStore(MakeStore(2, "h2", 2)).ok());
            ASSERT_TRUE(registry.RegisterStore(MakeStore(3, "h3", 3)).ok());

            StoreUpdate stop;
            stop.id = 2;
            stop.state = StoreState::STOPPED;
            ASSERT_TRUE(registry.UpdateStore(stop).ok());

            StoreUpdate fail;
            fail.id = 3;
            fail.state = StoreState::FAILED;
            ASSERT_TRUE(registry.UpdateStore(fail).ok());

            std::vector<StoreInfo> running;
            ASSERT_TRUE(registry.ListRunningStores(&running).ok());
            ASSERT_EQ(running.size(), 1U);
            EXPECT_EQ(running[0].id, 1U);
        }

        TEST(StoreRegistryTest, HeartbeatUpdatesOnlyLeaderLocalField)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1)).ok());

            StoreInfo updated;
            ASSERT_TRUE(registry.UpdateHeartbeat(1, 12345, &updated).ok());
            EXPECT_EQ(updated.last_heartbeat_ms, 12345);
            EXPECT_EQ(updated.generation, 1U);

            std::vector<PersistentStoreInfo> persistent;
            ASSERT_TRUE(registry.ExportPersistent(&persistent).ok());
            ASSERT_EQ(persistent.size(), 1U);
            EXPECT_EQ(persistent[0].generation, 1U);

            EXPECT_EQ(registry.UpdateHeartbeat(999, 1).code(),
                      StatusCode::kNotFound);
            EXPECT_EQ(registry.UpdateHeartbeat(1, -1).code(),
                      StatusCode::kInvalidArgument);
        }

        TEST(StoreRegistryTest, RemoveClearsIdAndAddressIndexes)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1)).ok());
            ASSERT_TRUE(registry.RemoveStore(1).ok());

            StoreInfo out;
            EXPECT_EQ(registry.GetStore(1, &out).code(),
                      StatusCode::kNotFound);
            EXPECT_EQ(registry.GetStoreByAddress({"h1", 1}, &out).code(),
                      StatusCode::kNotFound);
            ASSERT_TRUE(registry.RegisterStore(MakeStore(2, "h1", 1)).ok());

            EXPECT_EQ(registry.RemoveStore(999).code(), StatusCode::kNotFound);
        }

        TEST(StoreRegistryTest, PersistentViewRoundTripsWithoutHeartbeat)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(2, "h2", 2, 200, 20)).ok());
            ASSERT_TRUE(registry.RegisterStore(MakeStore(1, "h1", 1, 100, 10)).ok());
            ASSERT_TRUE(registry.UpdateHeartbeat(1, 999).ok());

            StoreUpdate stop;
            stop.id = 2;
            stop.state = StoreState::STOPPED;
            ASSERT_TRUE(registry.UpdateStore(stop).ok());

            std::vector<PersistentStoreInfo> persistent;
            ASSERT_TRUE(registry.ExportPersistent(&persistent).ok());
            ASSERT_EQ(persistent.size(), 2U);
            EXPECT_EQ(persistent[0].id, 1U);
            EXPECT_EQ(persistent[1].id, 2U);

            StoreRegistry restored;
            ASSERT_TRUE(restored.RestorePersistent(persistent).ok());
            StoreInfo restored_a = MustGet(&restored, 1);
            EXPECT_EQ(restored_a.last_heartbeat_ms, 0);
            EXPECT_EQ(restored_a.capacity_bytes, 100U);

            StoreInfo restored_b = MustGet(&restored, 2);
            EXPECT_EQ(restored_b.state, StoreState::STOPPED);
            EXPECT_EQ(restored_b.generation, 2U);
        }

        TEST(StoreRegistryTest, RestoreRejectsInvalidPersistentViewAtomically)
        {
            StoreRegistry registry;
            ASSERT_TRUE(registry.RegisterStore(MakeStore(10, "old", 1)).ok());

            PersistentStoreInfo first;
            first.id = 1;
            first.address = {"h", 1};
            first.capacity_bytes = 10;
            first.used_bytes = 1;
            first.state = StoreState::RUNNING;
            first.generation = 1;

            PersistentStoreInfo duplicate_id = first;
            duplicate_id.address = {"h2", 2};
            EXPECT_EQ(registry.RestorePersistent({first, duplicate_id}).code(),
                      StatusCode::kBusy);

            PersistentStoreInfo duplicate_address = first;
            duplicate_address.id = 2;
            EXPECT_EQ(registry.RestorePersistent({first, duplicate_address}).code(),
                      StatusCode::kBusy);

            PersistentStoreInfo invalid = first;
            invalid.used_bytes = 20;
            EXPECT_EQ(registry.RestorePersistent({invalid}).code(),
                      StatusCode::kInvalidArgument);

            StoreInfo existing;
            ASSERT_TRUE(registry.GetStore(10, &existing).ok());
            EXPECT_EQ(existing.address.host, "old");
        }

    } // namespace
} // namespace cpr::store
