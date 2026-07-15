# Contracts: Interfaces and Services

## Internal Interface Contracts

### RaftCore

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| Tick | Logical tick event | Role/term changes, heartbeat/election actions | No system time reads |
| RequestVote | Candidate term, candidate id, last log index/term | Vote response, hard-state update if term/vote changes | No RPC calls |
| AppendEntries | Leader term/id, prev index/term, entries, leader commit | Append response, log changes, commit updates | No storage calls directly |
| Proposal | Opaque command payload | Entries to persist and later replicate | No business type dependency |
| Membership change | AddLearner, PromoteLearner, RemoveMember request | Membership log entry or rejection | One active change in v1 |

### RaftLog

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| Append | Entry or entry batch | Append status and new last index | Preserve contiguous indexes |
| Query | Index or index range | Entry, term, or not-found status | Respect snapshot boundary |
| Conflict handling | Incoming prev index/term and entries | Conflict index and truncation result | Never truncate before safe point |
| Progress markers | Stable, commit, applied indexes | Updated marker or invariant error | Enforce applied <= commit <= last |

### IRaftStorage

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| Open/Load | Storage location and options | HardState, entries, snapshot metadata | Uses FileOps for file behavior |
| SaveHardState | HardState | Durable result | Flush required state |
| AppendEntries | Entry batch | Durable append result | Validate record checksum on load |
| TruncateSuffix | First removed index | Durable truncation result | Preserve prior entries |
| SaveSnapshot | Snapshot metadata and payload | Durable snapshot result | Atomic replacement where required |

### IRaftTransport

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| Send RequestVote | Peer, request message | Delivery status and response | No consensus decisions |
| Send AppendEntries | Peer, request message | Delivery status and response | Backpressure via peer queue |
| Send InstallSnapshot | Peer, snapshot message | Delivery status and response | Supports simulated transport |

### RaftRuntime

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| Enqueue proposal | Client proposal | Accepted, busy, or retry status | Does not imply commit success |
| Enqueue network message | Peer message | Accepted, busy, or retry status | RPC callback returns promptly |
| Persistence completion | Storage result | Release messages or fail dependent work | Preserves durability order |
| Apply completion | Applied index/result | Client result completion | One apply per index |
| Shutdown | Shutdown event | Drained/rejected work and stopped threads | Deterministic lifecycle |

### IRaftStateMachine

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| Apply | Log index, term, opaque payload | Apply result and business state change | Called in index order |
| CreateSnapshot | Last applied index/term | Snapshot payload | No Raft internals required |
| RestoreSnapshot | Snapshot payload | Restore result | Replaces business state safely |

### IPlacementPolicy

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| Select | Placement request and store snapshot | Selected store ids or failure status | No RaftCore dependency |

### ITaskDispatcher

| Contract | Inputs | Outputs | Required Boundary |
|----------|--------|---------|-------------------|
| PollTasks | Store id and max count | Ordered task list | Idempotent repeated polling |
| ReportTaskResult | Store id, task id, result | Updated task state or duplicate result | No RaftCore dependency |

## gRPC Service Scope

### raft.proto

Services:

- RequestVote
- AppendEntries
- InstallSnapshot

Contract rules:

- Requests carry term, sender identity, log metadata, and payload needed by RaftCore.
- Responses carry term, success/rejection status, and conflict/progress metadata.
- Receiver persists required hard state/log changes before returning success when the response depends on that durability.

### metadata.proto

Services:

- Propose
- Query
- GetLeader
- GetStatus
- AddLearner
- PromoteLearner
- RemoveMember

Contract rules:

- Write operations accepted by non-leaders return NOT_LEADER and leader address when known.
- Propose success means the entry is committed and applied, not merely accepted or locally persisted.
- Query reads MetadataService state exposed by the current node according to the v1 consistency contract.

### store.proto

Services:

- Register
- Heartbeat
- Stop
- Remove
- PollTasks
- ReportTaskResult

Contract rules:

- Register/Stop/Remove and important state transitions go through Metadata consensus.
- Heartbeats update leader-local transient data and are not individually written to Raft.
- PollTasks and ReportTaskResult preserve task_id idempotency.

## Tool Contracts

- `raftctl`: operator-facing cluster status and membership commands using public Metadata service calls.
- `wal_dump`: offline inspection of FileRaftStorage records, checksums, hard state, log entries, and snapshots.
- `fixture_generator`: creates versioned fixtures outside normal test execution.
- `client`: simple Metadata and Store control client for manual validation.
