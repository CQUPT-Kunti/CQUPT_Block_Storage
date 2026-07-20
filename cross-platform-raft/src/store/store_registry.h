#pragma once

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "store/store_types.h"

namespace cpr::store
{

    class StoreRegistry
    {
    public:
        StoreRegistry() = default;

        common::Status RegisterStore(const StoreInfo &store,
                                     StoreInfo *registered = nullptr);
        common::Status UpdateStore(const StoreUpdate &update,
                                   StoreInfo *updated = nullptr);
        common::Status UpdateHeartbeat(const StoreId &id,
                                       std::int64_t heartbeat_ms,
                                       StoreInfo *updated = nullptr);
        common::Status RemoveStore(const StoreId &id);

        common::Status GetStore(const StoreId &id, StoreInfo *store) const;
        common::Status GetStoreByAddress(const StoreAddress &address,
                                         StoreInfo *store) const;
        common::Status ListStores(std::vector<StoreInfo> *stores) const;
        common::Status ListRunningStores(std::vector<StoreInfo> *stores) const;

        common::Status ExportPersistent(
            std::vector<PersistentStoreInfo> *stores) const;
        common::Status RestorePersistent(
            const std::vector<PersistentStoreInfo> &stores);

    private:
        using StoreMap = std::map<StoreId, StoreInfo>;
        using AddressIndex = std::map<std::string, StoreId>;

        static common::Status ValidateStoreId(const StoreId &id);
        static common::Status ValidateAddress(const StoreAddress &address);
        static common::Status ValidateStoreFields(const StoreInfo &store);
        static common::Status ValidatePersistentFields(
            const PersistentStoreInfo &store);
        static std::string AddressKey(const StoreAddress &address);
        static PersistentStoreInfo ToPersistent(const StoreInfo &store);
        static StoreInfo FromPersistent(const PersistentStoreInfo &store);

        mutable std::mutex mutex_;
        StoreMap stores_;
        AddressIndex address_index_;
    };

} // namespace cpr::store
