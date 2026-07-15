# Data Model: Cross-Platform Raft Metadata Foundation

## RaftNode

- Fields: node_id, address set, NodeState, RaftRole, current term, voted-for, log, storage handle, transport handle, runtime queues, membership view.
- Relationships: owns one RaftCore, one RaftLog, one RaftRuntime composition, one storage implementation, and transport connections to peers.
- Validation: node_id must be configured, unique, and present in bootstrap members for initial startup.

## HardState

- Fields: current term, voted-for node id, commit index, optional durable membership marker.
- Relationships: persisted by IRaftStorage; loaded before RaftCore starts.
- Validation: term is monotonic; vote is valid for the term; commit index does not exceed last durable log index.

## LogEntry

- Fields: index, term, entry type, payload, persistence state, commit/apply state.
- Relationships: belongs to RaftLog; payload is opaque to Raft except membership entries.
- Validation: indexes are contiguous after snapshot boundary; terms are nonzero for real entries; committed/applied ordering follows RaftLog invariants.

## Snapshot

- Fields: last included index, last included term, state-machine payload, membership view, checksum metadata.
- Relationships: created/restored through IRaftStateMachine and persisted by IRaftStorage.
- Validation: snapshot boundary cannot move backward; restored log state must be compatible with last included index/term.

## MembershipView

- Fields: voters, learners, active transition state, committed configuration id.
- Relationships: used by RaftCore quorum/election logic and persisted through log entries/snapshots.
- Validation: learners cannot vote, count toward quorum, or become leader; only one change can be active in v1.

## RuntimeEvent

- Fields: event type, source, sequence metadata, payload, enqueue result.
- Relationships: delivered to RaftRuntime queues and processed by protocol/persistence/apply threads.
- Validation: queue capacity is enforced; shutdown transitions reject or drain work deterministically.

## MetadataCommand

- Fields: command type, command id, target entity, payload, expected generation when needed.
- Relationships: committed as Raft log payload and applied by MetadataStateMachine.
- Validation: command ids support idempotency where required; business validation happens before proposal and during apply.

## StoreInfo

- Fields: id, address, capacity, used, StoreState, generation, last_heartbeat_ms.
- Relationships: durable fields are managed by StoreRegistry through MetadataStateMachine; last_heartbeat_ms is leader-local.
- Validation: id/address uniqueness; capacity >= used; state is RUNNING, STOPPED, or FAILED.

## PlacementRequest

- Fields: replica count, required capacity, candidate store snapshot.
- Relationships: consumed by IPlacementPolicy.
- Validation: replica count is positive; selected stores are unique, RUNNING, and have enough remaining capacity.

## TaskRecord

- Fields: task_id, TaskType, TaskState, target payload, assigned store, result payload, generation/update metadata.
- Relationships: managed by TaskManager and exposed through ITaskDispatcher.
- Validation: task_id is idempotent; state is WAITING, RUNNING, SUCCESS, or FAILED; type is CREATE, DELETE, COPY, or CUSTOM.

## Config

- Fields: node id, IP address, Raft port, Metadata port, Store-control port, initial members, member roles, heartbeat interval, election timeout range, RPC timeout, queue capacity, worker count, max message size, log batch size, data directory, snapshot directory, store heartbeat timeout, failure detection interval, task poll limit, log level.
- Relationships: loaded by ConfigLoader and used by server composition.
- Validation: ports valid and distinct as needed; paths valid; current node appears in bootstrap members; heartbeat timeout is less than election timeout; queue/message limits are positive.

## Fixture

- Fields: fixture_version, scenario name, input records/messages/config, expected results, checksum or verification metadata.
- Relationships: loaded by FixtureLoader and used by module/integration tests.
- Validation: fixture version recognized; checksums pass; scenario name maps to one stored fixture.
