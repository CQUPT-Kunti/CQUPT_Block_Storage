#pragma once

#include <vector>

#include "common/status.h"
#include "raft/raft_storage.h"

namespace cpr::raft
{

    using SnapshotBytes = std::vector<common::Byte>;

    common::Status EncodeSnapshotData(const SnapshotData &snapshot,
                                      SnapshotBytes *bytes);
    common::Status DecodeSnapshotData(const SnapshotBytes &bytes,
                                      SnapshotData *snapshot);

} // namespace cpr::raft
