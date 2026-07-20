#include "store/store_registry.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace cpr::store
{
    namespace
    {

        constexpr std::uint64_t kInitialStoreGeneration = 1;
        constexpr std::size_t kMaxStoreHostBytes = 4 * 1024;

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        common::Status Busy(std::string message)
        {
            return common::Status::Busy(std::move(message));
        }

        bool IsKnownState(StoreState state)
        {
            return state == StoreState::RUNNING ||
                   state == StoreState::STOPPED ||
                   state == StoreState::FAILED;
        }

    } // namespace

    bool operator==(const StoreAddress &lhs, const StoreAddress &rhs)
    {
        return lhs.host == rhs.host && lhs.port == rhs.port;
    }

    bool operator!=(const StoreAddress &lhs, const StoreAddress &rhs)
    {
        return !(lhs == rhs);
    }

    common::Status StoreRegistry::RegisterStore(const StoreInfo &store,
                                                StoreInfo *registered)
    {
        StoreInfo candidate = store;
        candidate.state = StoreState::RUNNING;
        candidate.generation = kInitialStoreGeneration;
        candidate.last_heartbeat_ms = store.last_heartbeat_ms;

        common::Status status = ValidateStoreFields(candidate);
        if (!status.ok())
        {
            return status;
        }
        if (candidate.last_heartbeat_ms < 0)
        {
            return Invalid("store heartbeat timestamp must not be negative");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (stores_.find(candidate.id) != stores_.end())
        {
            return Busy("store id already exists");
        }

        const std::string key = AddressKey(candidate.address);
        if (address_index_.find(key) != address_index_.end())
        {
            return Busy("store address already exists");
        }

        stores_.emplace(candidate.id, candidate);
        address_index_.emplace(key, candidate.id);
        if (registered != nullptr)
        {
            *registered = candidate;
        }
        return common::Status::OK();
    }

    common::Status StoreRegistry::UpdateStore(const StoreUpdate &update,
                                              StoreInfo *updated)
    {
        common::Status status = ValidateStoreId(update.id);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = stores_.find(update.id);
        if (it == stores_.end())
        {
            return common::Status::NotFound("store not found");
        }
        if (!update.capacity_bytes && !update.used_bytes && !update.state)
        {
            return Invalid("store update must change at least one persistent field");
        }

        const StoreInfo current = it->second;
        if (update.expected_generation &&
            *update.expected_generation != current.generation)
        {
            return Busy("store generation mismatch");
        }

        StoreInfo candidate = current;
        if (update.capacity_bytes)
        {
            candidate.capacity_bytes = *update.capacity_bytes;
        }
        if (update.used_bytes)
        {
            candidate.used_bytes = *update.used_bytes;
        }
        if (update.state)
        {
            candidate.state = *update.state;
        }
        if (candidate.generation == std::numeric_limits<std::uint64_t>::max())
        {
            return Busy("store generation cannot advance");
        }
        ++candidate.generation;

        status = ValidateStoreFields(candidate);
        if (!status.ok())
        {
            return status;
        }

        it->second = candidate;
        if (updated != nullptr)
        {
            *updated = candidate;
        }
        return common::Status::OK();
    }

    common::Status StoreRegistry::UpdateHeartbeat(const StoreId &id,
                                                  std::int64_t heartbeat_ms,
                                                  StoreInfo *updated)
    {
        common::Status status = ValidateStoreId(id);
        if (!status.ok())
        {
            return status;
        }
        if (heartbeat_ms < 0)
        {
            return Invalid("store heartbeat timestamp must not be negative");
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = stores_.find(id);
        if (it == stores_.end())
        {
            return common::Status::NotFound("store not found");
        }

        it->second.last_heartbeat_ms = heartbeat_ms;
        if (updated != nullptr)
        {
            *updated = it->second;
        }
        return common::Status::OK();
    }

    common::Status StoreRegistry::RemoveStore(const StoreId &id)
    {
        common::Status status = ValidateStoreId(id);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = stores_.find(id);
        if (it == stores_.end())
        {
            return common::Status::NotFound("store not found");
        }

        address_index_.erase(AddressKey(it->second.address));
        stores_.erase(it);
        return common::Status::OK();
    }

    common::Status StoreRegistry::GetStore(const StoreId &id,
                                           StoreInfo *store) const
    {
        if (store == nullptr)
        {
            return Invalid("store output must not be null");
        }
        common::Status status = ValidateStoreId(id);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = stores_.find(id);
        if (it == stores_.end())
        {
            return common::Status::NotFound("store not found");
        }
        *store = it->second;
        return common::Status::OK();
    }

    common::Status StoreRegistry::GetStoreByAddress(const StoreAddress &address,
                                                    StoreInfo *store) const
    {
        if (store == nullptr)
        {
            return Invalid("store output must not be null");
        }
        common::Status status = ValidateAddress(address);
        if (!status.ok())
        {
            return status;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        auto address_it = address_index_.find(AddressKey(address));
        if (address_it == address_index_.end())
        {
            return common::Status::NotFound("store address not found");
        }

        auto store_it = stores_.find(address_it->second);
        if (store_it == stores_.end())
        {
            return common::Status::InternalError("store address index is inconsistent");
        }
        *store = store_it->second;
        return common::Status::OK();
    }

    common::Status StoreRegistry::ListStores(std::vector<StoreInfo> *stores) const
    {
        if (stores == nullptr)
        {
            return Invalid("store list output must not be null");
        }

        std::vector<StoreInfo> out;
        std::lock_guard<std::mutex> lock(mutex_);
        out.reserve(stores_.size());
        for (const auto &item : stores_)
        {
            out.push_back(item.second);
        }
        *stores = std::move(out);
        return common::Status::OK();
    }

    common::Status StoreRegistry::ListRunningStores(
        std::vector<StoreInfo> *stores) const
    {
        if (stores == nullptr)
        {
            return Invalid("running store list output must not be null");
        }

        std::vector<StoreInfo> out;
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto &item : stores_)
        {
            if (item.second.state == StoreState::RUNNING)
            {
                out.push_back(item.second);
            }
        }
        *stores = std::move(out);
        return common::Status::OK();
    }

    common::Status StoreRegistry::ExportPersistent(
        std::vector<PersistentStoreInfo> *stores) const
    {
        if (stores == nullptr)
        {
            return Invalid("persistent store list output must not be null");
        }

        std::vector<PersistentStoreInfo> out;
        std::lock_guard<std::mutex> lock(mutex_);
        out.reserve(stores_.size());
        for (const auto &item : stores_)
        {
            out.push_back(ToPersistent(item.second));
        }
        *stores = std::move(out);
        return common::Status::OK();
    }

    common::Status StoreRegistry::RestorePersistent(
        const std::vector<PersistentStoreInfo> &stores)
    {
        StoreMap candidate;
        AddressIndex candidate_addresses;
        for (const PersistentStoreInfo &store : stores)
        {
            common::Status status = ValidatePersistentFields(store);
            if (!status.ok())
            {
                return status;
            }
            if (candidate.find(store.id) != candidate.end())
            {
                return Busy("duplicate store id in persistent view");
            }

            const std::string key = AddressKey(store.address);
            if (candidate_addresses.find(key) != candidate_addresses.end())
            {
                return Busy("duplicate store address in persistent view");
            }

            candidate.emplace(store.id, FromPersistent(store));
            candidate_addresses.emplace(key, store.id);
        }

        std::lock_guard<std::mutex> lock(mutex_);
        stores_ = std::move(candidate);
        address_index_ = std::move(candidate_addresses);
        return common::Status::OK();
    }

    common::Status StoreRegistry::ValidateStoreId(const StoreId &id)
    {
        if (id == 0)
        {
            return Invalid("store id must be positive");
        }
        return common::Status::OK();
    }

    common::Status StoreRegistry::ValidateAddress(const StoreAddress &address)
    {
        if (address.host.empty())
        {
            return Invalid("store address host must not be empty");
        }
        if (address.host.size() > kMaxStoreHostBytes)
        {
            return Invalid("store address host is too large");
        }
        if (address.port == 0)
        {
            return Invalid("store address port must be positive");
        }
        return common::Status::OK();
    }

    common::Status StoreRegistry::ValidateStoreFields(const StoreInfo &store)
    {
        common::Status status = ValidateStoreId(store.id);
        if (!status.ok())
        {
            return status;
        }
        status = ValidateAddress(store.address);
        if (!status.ok())
        {
            return status;
        }
        if (store.capacity_bytes == 0)
        {
            return Invalid("store capacity must be positive");
        }
        if (store.used_bytes > store.capacity_bytes)
        {
            return Invalid("store used capacity must not exceed capacity");
        }
        if (!IsKnownState(store.state))
        {
            return Invalid("store state is invalid");
        }
        if (store.generation == 0)
        {
            return Invalid("store generation must be positive");
        }
        return common::Status::OK();
    }

    common::Status StoreRegistry::ValidatePersistentFields(
        const PersistentStoreInfo &store)
    {
        return ValidateStoreFields(FromPersistent(store));
    }

    std::string StoreRegistry::AddressKey(const StoreAddress &address)
    {
        return address.host + ":" + std::to_string(address.port);
    }

    PersistentStoreInfo StoreRegistry::ToPersistent(const StoreInfo &store)
    {
        PersistentStoreInfo persistent;
        persistent.id = store.id;
        persistent.address = store.address;
        persistent.capacity_bytes = store.capacity_bytes;
        persistent.used_bytes = store.used_bytes;
        persistent.state = store.state;
        persistent.generation = store.generation;
        return persistent;
    }

    StoreInfo StoreRegistry::FromPersistent(const PersistentStoreInfo &store)
    {
        StoreInfo info;
        info.id = store.id;
        info.address = store.address;
        info.capacity_bytes = store.capacity_bytes;
        info.used_bytes = store.used_bytes;
        info.state = store.state;
        info.generation = store.generation;
        info.last_heartbeat_ms = 0;
        return info;
    }

} // namespace cpr::store
