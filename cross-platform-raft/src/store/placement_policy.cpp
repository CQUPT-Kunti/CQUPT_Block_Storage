#include "store/placement_policy.h"

#include <algorithm>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace cpr::store
{
    namespace
    {

        common::Status Invalid(std::string message)
        {
            return common::Status::InvalidArgument(std::move(message));
        }

        std::string AddressKey(const StoreAddress &address)
        {
            return address.host + ":" + std::to_string(address.port);
        }

        bool IsKnownState(StoreState state)
        {
            return state == StoreState::RUNNING ||
                   state == StoreState::STOPPED ||
                   state == StoreState::FAILED;
        }

        common::Status ValidateRequest(const PlacementRequest &request)
        {
            if (request.replica_count == 0)
            {
                return Invalid("placement replica count must be positive");
            }
            if (request.required_capacity_bytes == 0)
            {
                return Invalid("placement required capacity must be positive");
            }
            return common::Status::OK();
        }

        common::Status ValidateCandidate(const StoreInfo &store)
        {
            if (store.id == 0)
            {
                return Invalid("candidate store id must be positive");
            }
            if (store.address.host.empty())
            {
                return Invalid("candidate store host must not be empty");
            }
            if (store.address.port == 0)
            {
                return Invalid("candidate store port must be positive");
            }
            if (!IsKnownState(store.state))
            {
                return Invalid("candidate store state is invalid");
            }
            if (store.generation == 0)
            {
                return Invalid("candidate store generation must be positive");
            }
            if (store.capacity_bytes < store.used_bytes)
            {
                return Invalid("candidate store used capacity exceeds capacity");
            }
            return common::Status::OK();
        }

        struct EligibleStore
        {
            StoreId id = 0;
            std::uint64_t remaining_capacity = 0;
        };

    } // namespace

    common::Status SimplePlacementPolicy::SelectStores(
        const PlacementRequest &request,
        const std::vector<StoreInfo> &candidates,
        PlacementResult *result) const
    {
        if (result == nullptr)
        {
            return Invalid("placement result must not be null");
        }

        common::Status status = ValidateRequest(request);
        if (!status.ok())
        {
            return status;
        }

        std::map<StoreId, StoreInfo> by_id;
        std::map<std::string, StoreId> by_address;
        for (const StoreInfo &candidate : candidates)
        {
            status = ValidateCandidate(candidate);
            if (!status.ok())
            {
                return status;
            }

            if (by_id.find(candidate.id) != by_id.end())
            {
                return Invalid("duplicate candidate store id");
            }

            const std::string address = AddressKey(candidate.address);
            if (by_address.find(address) != by_address.end())
            {
                return Invalid("duplicate candidate store address");
            }

            by_id.emplace(candidate.id, candidate);
            by_address.emplace(address, candidate.id);
        }

        std::uint32_t running_count = 0;
        std::vector<EligibleStore> eligible;
        for (const auto &item : by_id)
        {
            const StoreInfo &store = item.second;
            if (store.state != StoreState::RUNNING)
            {
                continue;
            }
            ++running_count;

            const std::uint64_t remaining =
                store.capacity_bytes - store.used_bytes;
            if (remaining >= request.required_capacity_bytes)
            {
                eligible.push_back({store.id, remaining});
            }
        }

        if (running_count < request.replica_count)
        {
            return common::Status::ResourceExhausted("not enough running stores for placement");
        }
        if (eligible.size() < request.replica_count)
        {
            return common::Status::ResourceExhausted("not enough store capacity for placement");
        }

        std::sort(eligible.begin(),
                  eligible.end(),
                  [](const EligibleStore &lhs, const EligibleStore &rhs)
                  {
                      if (lhs.remaining_capacity != rhs.remaining_capacity)
                      {
                          return lhs.remaining_capacity > rhs.remaining_capacity;
                      }
                      return lhs.id < rhs.id;
                  });

        PlacementResult candidate_result;
        candidate_result.selected_store_ids.reserve(request.replica_count);
        for (std::uint32_t i = 0; i < request.replica_count; ++i)
        {
            candidate_result.selected_store_ids.push_back(eligible[i].id);
        }

        *result = std::move(candidate_result);
        return common::Status::OK();
    }

} // namespace cpr::store
