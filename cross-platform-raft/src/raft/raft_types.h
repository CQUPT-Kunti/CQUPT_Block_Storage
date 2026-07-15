#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/types.h"

namespace cpr::raft {

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER,
    LEARNER,
};

enum class NodeState {
    RUNNING,
    STOPPED,
    FAILED,
};

enum class LogEntryType {
    NO_OP,
    COMMAND,
    MEMBERSHIP_CHANGE,
};

using OpaquePayload = std::vector<common::Byte>;

struct NodeAddress {
    std::string host;
    std::uint16_t port = 0;
};

struct RaftMember {
    common::NodeId node_id = common::kInvalidNodeId;
    NodeAddress address;
};

struct MembershipView {
    std::vector<RaftMember> voters;
    std::vector<RaftMember> learners;
    bool has_active_transition = false;
    std::uint64_t configuration_id = 0;
};

struct LogEntry {
    common::LogIndex index = common::kInvalidLogIndex;
    common::Term term = common::kInitialTerm;
    LogEntryType type = LogEntryType::NO_OP;
    OpaquePayload payload;
};

struct HardState {
    common::Term current_term = common::kInitialTerm;
    common::NodeId voted_for = common::kInvalidNodeId;
    common::LogIndex commit_index = common::kInvalidLogIndex;
    std::uint64_t membership_configuration_id = 0;
};

struct SnapshotMetadata {
    common::LogIndex last_included_index = common::kInvalidLogIndex;
    common::Term last_included_term = common::kInitialTerm;
    MembershipView membership;
};

struct PeerProgress {
    common::NodeId node_id = common::kInvalidNodeId;
    bool is_learner = false;
    bool is_active = false;
    common::LogIndex match_index = common::kInvalidLogIndex;
    common::LogIndex next_index = common::kInvalidLogIndex;
};

}  // namespace cpr::raft
