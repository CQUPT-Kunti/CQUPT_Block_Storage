#pragma once

#include <cstdint>
#include <vector>

#include "common/status.h"
#include "store/store_types.h"

namespace cpr::store
{

    struct PlacementRequest
    {
        std::uint32_t replica_count = 0;
        std::uint64_t required_capacity_bytes = 0;
    };

    struct PlacementResult
    {
        std::vector<StoreId> selected_store_ids;
    };

    class IPlacementPolicy
    {
    public:
        virtual ~IPlacementPolicy() = default;

        virtual common::Status SelectStores(
            const PlacementRequest &request,
            const std::vector<StoreInfo> &candidates,
            PlacementResult *result) const = 0;
    };

    class SimplePlacementPolicy final : public IPlacementPolicy
    {
    public:
        common::Status SelectStores(
            const PlacementRequest &request,
            const std::vector<StoreInfo> &candidates,
            PlacementResult *result) const override;
    };

} // namespace cpr::store
