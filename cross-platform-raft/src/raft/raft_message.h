#pragma once

#include <variant>
#include <vector>

#include "raft/raft_types.h"

namespace cpr::raft {

struct RequestVoteRequest {
    common::Term term = common::kInitialTerm;
    common::NodeId candidate_id = common::kInvalidNodeId;
    common::LogIndex last_log_index = common::kInvalidLogIndex;
    common::Term last_log_term = common::kInitialTerm;
};

struct RequestVoteResponse {
    common::Term term = common::kInitialTerm;
    bool vote_granted = false;
};

struct AppendEntriesRequest {
    common::Term term = common::kInitialTerm;
    common::NodeId leader_id = common::kInvalidNodeId;
    common::LogIndex prev_log_index = common::kInvalidLogIndex;
    common::Term prev_log_term = common::kInitialTerm;
    std::vector<LogEntry> entries;
    common::LogIndex leader_commit = common::kInvalidLogIndex;
};

struct AppendEntriesResponse {
    common::Term term = common::kInitialTerm;
    bool success = false;
    common::LogIndex match_index = common::kInvalidLogIndex;
    common::LogIndex conflict_index = common::kInvalidLogIndex;
    common::Term conflict_term = common::kInitialTerm;
};

struct InstallSnapshotRequest {
    common::Term term = common::kInitialTerm;
    common::NodeId leader_id = common::kInvalidNodeId;
    SnapshotMetadata metadata;
    OpaquePayload payload;
};

struct InstallSnapshotResponse {
    common::Term term = common::kInitialTerm;
    bool success = false;
    common::LogIndex last_included_index = common::kInvalidLogIndex;
};

enum class RaftMessageType {
    REQUEST_VOTE_REQUEST,
    REQUEST_VOTE_RESPONSE,
    APPEND_ENTRIES_REQUEST,
    APPEND_ENTRIES_RESPONSE,
    INSTALL_SNAPSHOT_REQUEST,
    INSTALL_SNAPSHOT_RESPONSE,
};

using RaftMessagePayload = std::variant<
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse>;

struct RaftMessage {
    RaftMessageType type = RaftMessageType::REQUEST_VOTE_REQUEST;
    common::NodeId source_node_id = common::kInvalidNodeId;
    common::NodeId target_node_id = common::kInvalidNodeId;
    RaftMessagePayload payload = RequestVoteRequest{};
};

}  // namespace cpr::raft
