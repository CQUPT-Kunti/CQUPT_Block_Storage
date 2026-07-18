#pragma once

#include "common/status.h"
#include "common/types.h"
#include "raft/raft_types.h"

namespace cpr::metadata
{

    // IRaftStateMachine is the only Raft-to-business apply/snapshot boundary.
    // The caller is responsible for invoking Apply() only for committed log
    // entries and in strictly increasing LogIndex order.
    class IRaftStateMachine
    {
    public:
        virtual ~IRaftStateMachine() = default;

        // Apply one committed command payload. Failure means the command was not
        // applied successfully and must not be treated as committed business success.
        virtual common::Status Apply(common::LogIndex index,
                                     common::Term term,
                                     const raft::OpaquePayload &command_payload) = 0;

        // Create only the business-state snapshot payload. Raft metadata such as
        // lastIncludedIndex/Term, membership, and checksum remain outside this
        // interface.
        virtual common::Status CreateSnapshot(common::LogIndex last_applied_index,
                                              common::Term last_applied_term,
                                              raft::OpaquePayload *snapshot_payload) = 0;

        // Replace the business state from an opaque snapshot payload. Implementations
        // must avoid exposing partially restored state when this fails.
        virtual common::Status RestoreSnapshot(
            const raft::OpaquePayload &snapshot_payload) = 0;
    };

} // namespace cpr::metadata
