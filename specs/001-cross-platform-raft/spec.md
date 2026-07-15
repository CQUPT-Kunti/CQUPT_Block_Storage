# Feature Specification: Cross-Platform Raft Metadata Foundation

**Feature Branch**: `main`  
**Created**: 2026-07-14  
**Status**: Draft  
**Input**: User description: "Cross-Platform Raft Metadata Foundation"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Operate a Durable Raft Metadata Cluster (Priority: P1)

As a storage-system developer, I need a reusable Raft foundation that can elect a leader, replicate metadata changes, persist consensus state, recover after restart, and keep the replicated state machine consistent across nodes.

**Why this priority**: Without correct consensus, persistence, and recovery, the foundation cannot safely support metadata control planes or future systems.

**Independent Test**: Start a configured multi-node cluster, submit metadata proposals through the leader, restart nodes, and verify that committed state, leadership behavior, and log contents remain consistent.

**Acceptance Scenarios**:

1. **Given** a bootstrapped cluster with a majority of voters running, **When** election ticks advance, **Then** exactly one eligible leader is chosen for the current term and followers reject stale-term requests.
2. **Given** a leader receives a metadata proposal, **When** the proposal is accepted, persisted, replicated to a quorum, committed, and applied, **Then** all available voters eventually expose the same committed metadata state.
3. **Given** a follower has missing, extra, or conflicting log entries, **When** the leader sends replication messages, **Then** the follower removes only the conflicting suffix, catches up in index order, and never applies an entry twice.
4. **Given** a node crashes after partial persistence, **When** it restarts, **Then** incomplete tails are ignored or repaired, checksums are validated, and the node resumes from the latest durable hard state, log, and snapshot.
5. **Given** a required durable write has not completed, **When** the system is about to send a success response or outbound consensus message that depends on that write, **Then** it waits for durable completion or returns an explicit error.

---

### User Story 2 - Replace Business Logic Without Rewriting Raft (Priority: P2)

As a developer building KV, block-storage, object-storage, or other replicated systems, I need stable replacement boundaries so that only the state machine, placement policy, task dispatcher, and business RPC layer must change.

**Why this priority**: The feature is intended to be a reusable foundation, not a one-off metadata service.

**Independent Test**: Replace the metadata state machine with a minimal alternate state machine in tests and verify that consensus, persistence, membership, transport, and runtime behavior do not require structural changes.

**Acceptance Scenarios**:

1. **Given** a new state machine implementation with apply, snapshot creation, and snapshot restore behavior, **When** the Raft runtime commits entries, **Then** the implementation receives entries in strict log-index order without depending on metadata-specific types.
2. **Given** a new placement policy, **When** the metadata service asks for store placement, **Then** selection behavior changes without modifying RaftCore, StoreRegistry, transport, or storage.
3. **Given** a new task dispatcher, **When** stores poll and report task results, **Then** scheduling behavior changes without modifying RaftCore, MetadataStateMachine, or the primary Store control contract.

---

### User Story 3 - Manage Stores and Tasks Through a Metadata Control Plane (Priority: P3)

As an operator or client of the metadata control plane, I need to register stores, track durable store state transitions, select stores for placement, create tasks, poll tasks, report task results, and discover leader/status information.

**Why this priority**: This proves the Raft foundation with a realistic but replaceable metadata example.

**Independent Test**: Use the client-facing metadata and store-control services against a cluster and verify durable metadata changes, in-memory heartbeat behavior, placement decisions, and idempotent task handling.

**Acceptance Scenarios**:

1. **Given** a store registers with a unique id and address, **When** registration is proposed through the leader, **Then** the durable registry records the id, address, generation, and RUNNING state.
2. **Given** a store sends repeated heartbeats, **When** only transient heartbeat fields change, **Then** the leader updates in-memory liveness without writing each heartbeat through consensus.
3. **Given** insufficient running stores or insufficient remaining capacity, **When** placement is requested, **Then** the request fails with a clear capacity or availability reason and no duplicate store is selected.
4. **Given** a store polls tasks multiple times or reports the same result repeatedly, **When** the task id is already known, **Then** task state and stored result remain idempotent.
5. **Given** a non-leader receives a write request, **When** the current leader is known, **Then** the response is NOT_LEADER and includes the leader address.

---

### User Story 4 - Safely Change Cluster Membership (Priority: P4)

As an operator, I need to add learners, promote caught-up learners, and remove members without violating quorum safety or allowing learners to vote or lead.

**Why this priority**: Membership changes are required for operational maintenance but must never compromise consensus safety.

**Independent Test**: Add a learner, replicate logs and snapshots to it, promote it only after it catches up, remove a voter through the log, and verify quorum calculations and leader eligibility at each step.

**Acceptance Scenarios**:

1. **Given** a new node joins, **When** it is added, **Then** it starts as a learner, receives logs and snapshots, does not vote, is not counted in quorum, and cannot become leader.
2. **Given** a learner is behind the leader commit index, communication is unhealthy, or another membership change is in progress, **When** promotion is requested, **Then** promotion is rejected.
3. **Given** the old voter configuration has lost quorum, **When** an automatic membership change would reduce safety, **Then** the system does not apply it automatically.
4. **Given** a membership change is accepted, **When** it is committed through the consensus log, **Then** all nodes apply the same new membership view in log order.

### Edge Cases

- Election split votes, leader failure, stale term messages, duplicate votes, and learner election attempts.
- Empty follower catch-up, lagging followers, followers with extra entries, equal index with different terms, and multi-term conflicts.
- Queue-full conditions for proposals, inbound messages, outbound peer queues, membership changes, persistence completions, and shutdown.
- Persistence failures, incomplete tails, checksum mismatch, snapshot boundary conflicts, durable flush failure, and restart after interrupted writes.
- Concurrent client proposals and concurrent requests during leader changes.
- Store duplicate registration, missing store, STOPPED/FAILED transitions, heartbeat timeout, and transient heartbeat data loss after leader failover.
- Insufficient placement nodes, insufficient capacity, deterministic tie-breaking, and duplicate selection prevention.
- Duplicate task creation, duplicate polling, duplicate result reporting, maximum poll-count limits, and failed task result storage.
- Windows and Linux path separators, file replacement behavior, flush semantics, temporary directory behavior, and process shutdown signals.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST provide a reusable Raft foundation whose consensus, log, storage, runtime, transport, membership, and test framework can be reused for future replicated systems without major changes.
- **FR-002**: The system MUST keep RaftCore independent from JSON parsing, file access, network RPC calls, thread creation, system-time reads, platform-specific APIs, and metadata business types.
- **FR-003**: RaftRole MUST contain only FOLLOWER, CANDIDATE, LEADER, and LEARNER.
- **FR-004**: NodeState MUST contain only RUNNING, STOPPED, and FAILED.
- **FR-005**: RaftCore MUST own term progression, voting, role transitions, elections, heartbeats, RequestVote handling, AppendEntries handling, log matching, conflict handling, next-index tracking, match-index tracking, commit-index advancement, and learner behavior.
- **FR-006**: RaftCore MUST be driven by logical ticks and MUST NOT directly read wall-clock or monotonic system time.
- **FR-007**: RaftCore MUST be driven serially by one protocol execution thread; concurrent external work MUST enter through bounded queues.
- **FR-008**: gRPC worker callbacks MUST only validate parameters, convert protocol messages, and enqueue work; they MUST NOT directly mutate RaftCore state.
- **FR-009**: The runtime MUST process tick events, network messages, proposals, membership changes, persistence-completion events, and shutdown events through bounded queues.
- **FR-010**: When any bounded queue is full, the caller MUST receive BUSY, RESOURCE_EXHAUSTED, RETRY_LATER, or an equivalent explicit throttling result.
- **FR-011**: Outbound consensus traffic MUST use one independent bounded queue per peer.
- **FR-012**: Responses and outbound messages that depend on a durable state transition MUST NOT be sent until the required persistence has completed successfully.
- **FR-013**: The state machine MUST apply committed entries strictly in increasing log-index order, and each log index MUST be applied no more than once.
- **FR-014**: RaftLog MUST support single append, batch append, entry lookup by index, term lookup by index, conflict detection, conflicting suffix truncation, range reads, stable-index tracking, commit-index tracking, applied-index tracking, and snapshot boundary tracking.
- **FR-015**: RaftLog MUST maintain the invariant appliedIndex <= commitIndex <= lastIndex at all times.
- **FR-016**: RaftLog MUST clearly distinguish unpersisted, persisted, committed, and applied entries.
- **FR-017**: RaftStorage MUST expose stable operations for opening storage, loading durable state, saving hard state, appending entries, truncating suffixes, and saving snapshots.
- **FR-018**: The system MUST provide both in-memory and file-backed Raft storage implementations.
- **FR-019**: The file-backed persistent format MUST include format_version, record_type, index, term, payload_length, and checksum for durable records.
- **FR-020**: File-backed storage MUST save and load hard state, logs, and snapshots; truncate conflicting entries; recover from incomplete tails; validate checksums; flush durable data; and recover after restart.
- **FR-021**: The storage boundary MUST allow replacement by a different storage implementation without changing RaftCore.
- **FR-022**: Platform differences MUST be isolated in the platform boundary; business and Raft modules MUST NOT directly depend on operating-system-specific file, process, time, memory-mapping, or async I/O APIs.
- **FR-023**: Path handling MUST consistently use platform-neutral path values.
- **FR-024**: File operations MUST cover directory creation, file opening, reading, writing, truncation, flushing, atomic replacement, file-size queries, deletion, and renaming.
- **FR-025**: Process operations MUST cover shutdown handling and graceful termination.
- **FR-026**: Time operations MUST cover monotonic time and test-time utilities outside RaftCore.
- **FR-027**: RaftTransport MUST expose a stable boundary that supports real network transport and in-memory simulated transport for tests.
- **FR-028**: The Raft-node service scope MUST include RequestVote, AppendEntries, and InstallSnapshot.
- **FR-029**: The client-facing Metadata service scope MUST include Propose, Query, GetLeader, GetStatus, AddLearner, PromoteLearner, and RemoveMember.
- **FR-030**: The Store control service scope MUST include Register, Heartbeat, Stop, Remove, PollTasks, and ReportTaskResult.
- **FR-031**: Non-leader write requests MUST return NOT_LEADER and include the known leader address when available.
- **FR-032**: Membership MUST support voters and learners.
- **FR-033**: A new node MUST join as a learner before it can become a voter.
- **FR-034**: A learner MUST receive logs and snapshots, MUST NOT vote, MUST NOT count toward quorum, and MUST NOT become leader.
- **FR-035**: A learner MUST be promotable only after catching up to the leader's current commit index, maintaining healthy communication, and when no other membership change is in progress.
- **FR-036**: Runtime membership changes MUST be committed through the Raft log.
- **FR-037**: Bootstrap member lists from configuration MUST be used only for initial bootstrap.
- **FR-038**: The first version MUST allow only one membership change at a time.
- **FR-039**: Membership MUST NOT change automatically if the old voter configuration has lost quorum.
- **FR-040**: Membership changes MUST use a safe two-phase transition or an equivalent safe process that preserves quorum safety.
- **FR-041**: The replaceable state-machine boundary MUST contain only apply, create-snapshot, and restore-snapshot responsibilities.
- **FR-042**: The Raft module MUST NOT contain StoreInfo, Placement, Task, Volume, Object, KeyValue, or other business-specific types.
- **FR-043**: Future KV, block-storage, or object-storage systems SHOULD require only a new state machine and new business commands, not RaftCore changes.
- **FR-044**: StoreRegistry MUST remain part of the metadata example and MUST NOT be part of RaftCore.
- **FR-045**: StoreState MUST contain only RUNNING, STOPPED, and FAILED.
- **FR-046**: StoreInfo MUST support id, address, capacity, used, state, generation, and last_heartbeat_ms.
- **FR-047**: StoreRegistry MUST support registration, duplicate-registration checking, updates, stopping, removal, lookup, listing all stores, and listing RUNNING stores.
- **FR-048**: Store id, address, generation, and important state transitions MUST be persisted through consensus.
- **FR-049**: Store last_heartbeat_ms, temporary capacity, temporary load, and RPC latency MUST remain only in the current leader's memory.
- **FR-050**: Store heartbeats MUST NOT be individually written to the Raft log.
- **FR-051**: Important store transitions such as RUNNING, STOPPED, and FAILED MUST be submitted through consensus.
- **FR-052**: Placement MUST expose a stable policy boundary.
- **FR-053**: The first placement policy MUST select only RUNNING stores, require sufficient remaining capacity, prevent duplicate selection within one request, sort by remaining capacity descending, use store id as the deterministic tie-breaker, and select the first requested number of stores.
- **FR-054**: Replacing the placement policy MUST NOT require changes to RaftCore, StoreRegistry, MetadataService, or primary RPC contracts.
- **FR-055**: TaskState MUST contain only WAITING, RUNNING, SUCCESS, and FAILED.
- **FR-056**: TaskType MUST contain only CREATE, DELETE, COPY, and CUSTOM.
- **FR-057**: TaskManager MUST support task creation, lookup, state updates, task_id idempotency, and result storage.
- **FR-058**: The task dispatcher boundary MUST support task polling and result reporting.
- **FR-059**: The first task dispatcher MUST support FIFO polling, maximum poll count, state updates, and idempotent duplicate handling.
- **FR-060**: Future priority scheduling, dependencies, workflows, and advanced dispatching MUST NOT require changes to RaftCore, MetadataStateMachine, or primary Store RPC contracts.
- **FR-061**: All manually configurable parameters MUST be loaded from JSON into strongly typed configuration objects before use.
- **FR-062**: Configuration MUST include node id, IP address, Raft port, Metadata port, Store-control port, initial members, member roles, heartbeat interval, election-timeout range, RPC timeout, queue capacity, gRPC worker count, maximum message size, log batch size, data directory, snapshot directory, store heartbeat timeout, failure-detection interval, task poll limit, and log level.
- **FR-063**: Configuration validation MUST cover node IDs, ports, paths, current-node membership in bootstrap members, heartbeat timeout being less than election timeout, queue capacities, and message-size limits.
- **FR-064**: Configuration loading MUST remain outside RaftCore.
- **FR-065**: The build and project layout MUST support matching header and source files in the same directory for manually written code.
- **FR-066**: The first version MUST include cross-platform builds, leader election, RequestVote, AppendEntries, heartbeats, log replication, log conflict handling, follower catch-up, majority commit, hard-state persistence, log persistence, restart recovery, snapshots, InstallSnapshot, voters, learners, AddLearner, PromoteLearner, RemoveMember, node communication, client communication, bounded queues, single-threaded Raft execution, multiple worker threads, replaceable state machine, store registration and heartbeats, store state transitions, placement policy, simple placement, task manager, task dispatcher, simple task dispatcher, JSON configuration, tests, fixtures, and Windows/Linux validation.
- **FR-067**: The first version MUST NOT include real store data persistence, Multi-Raft, cross-group transactions, advanced placement, automatic data migration, rack awareness, complex workflows, lease reads, follower reads, ReadIndex optimization, advanced platform-specific storage optimizations, memory-mapped WAL, lock-free queues, coroutines, or Byzantine fault tolerance.

### Non-Functional Requirements

- **NFR-001**: Correctness, durability, recovery behavior, and data safety MUST take priority over throughput, latency, and code size.
- **NFR-002**: The design MUST be clear and direct without avoidable abstraction, complex templates, coroutines, lock-free structures, unnecessary inheritance, or interfaces with no practical replacement value.
- **NFR-003**: Raft logic MUST NOT be centralized into one all-purpose node object; responsibilities MUST remain separated by consensus, log, storage, runtime, transport, membership, state machine, metadata, store, task, RPC, platform, and configuration boundaries.
- **NFR-004**: The system MUST support Windows and Linux behavior with comparable safety contracts.
- **NFR-005**: The system MUST support MSVC, GCC, and Clang compatible behavior for the supported first-version scope.
- **NFR-006**: Queue capacities MUST be bounded to prevent unlimited memory growth.
- **NFR-007**: Tests for RaftCore MUST use logical ticks and MUST NOT depend on real sleep calls.
- **NFR-008**: Integration tests MAY use bounded timeout waits but MUST NOT depend on long fixed sleeps.

### Module Boundaries and Dependency Direction

- **Common**: Shared status, identifiers, bounded queues, and checksum utilities. May be used by all modules and MUST NOT depend on Raft, metadata, store, RPC, server, or platform-specific internals.
- **Platform**: File, process, and time operations. May use operating-system APIs internally. Raft, metadata, store, RPC, and server modules MUST depend only on this boundary for platform-sensitive behavior.
- **Config**: JSON loading, validation, and strongly typed runtime settings. May depend on common and platform path handling. MUST NOT be used directly by RaftCore.
- **Raft**: Consensus types, messages, log, storage boundary, storage implementations, transport boundary, runtime, membership, and snapshots. MUST NOT depend on metadata, store, task, placement, business RPC services, or JSON parsing.
- **Metadata**: Replaceable state-machine example, metadata commands, metadata service behavior, and durable metadata application. May depend on Raft state-machine boundary and store/task/placement modules as business logic.
- **Store**: Store registry, store placement policy boundary, simple placement policy, task manager, task dispatcher boundary, and simple task dispatcher. MUST NOT depend on RaftCore internals.
- **RPC**: Raft-node, metadata, and store-control RPC service adapters. MUST validate and enqueue work through runtime/service boundaries instead of mutating RaftCore directly.
- **Server**: Composition root for config, storage, transport, runtime, metadata service, store service, and process lifecycle.
- **Tests**: Fixtures, simulated cluster, simulated transport, persistence checks, module tests, and integration tests. Test support MAY depend on public module boundaries but MUST NOT require production-only hidden hooks.
- **Tools**: Client, cluster control, WAL inspection, and fixture generation utilities. Tools MUST use public formats and service contracts.

### Core Data Structures

- **RaftRole**: FOLLOWER, CANDIDATE, LEADER, LEARNER.
- **NodeState**: RUNNING, STOPPED, FAILED.
- **HardState**: Durable current term, vote, commit index, and any required durable membership marker.
- **LogEntry**: Log index, term, command payload, and record classification needed for consensus and membership entries.
- **Snapshot**: Last included index, last included term, state-machine snapshot payload, membership view, and checksum-protected metadata.
- **MembershipView**: Voters, learners, active transition marker, and committed configuration identity.
- **StoreState**: RUNNING, STOPPED, FAILED.
- **StoreInfo**: id, address, capacity, used, state, generation, and leader-local last_heartbeat_ms.
- **PlacementRequest**: requested replica count, capacity need, and eligible running stores.
- **TaskState**: WAITING, RUNNING, SUCCESS, FAILED.
- **TaskType**: CREATE, DELETE, COPY, CUSTOM.
- **TaskRecord**: task id, type, target information, state, assignment, and stored result.
- **Config**: validated node, network, timeout, queue, storage, snapshot, store, task, and logging settings.

### Major Interfaces

- **IRaftStateMachine**: Applies committed entries, creates snapshots, and restores snapshots. It MUST be the only Raft-to-business execution boundary.
- **RaftStorage**: Opens, loads, saves hard state, appends entries, truncates suffixes, and saves snapshots with durable semantics.
- **RaftTransport**: Sends Raft messages between nodes and supports a real transport and simulated in-memory transport.
- **IPlacementPolicy**: Selects stores for a placement request from registry state without RaftCore involvement.
- **ITaskDispatcher**: Handles task polling and result reporting without RaftCore involvement.
- **FileOps**: Provides platform-neutral file and directory operations with explicit flush and atomic replacement behavior.
- **ProcessOps**: Provides graceful shutdown and termination handling.
- **TimeOps**: Provides monotonic time and controllable test-time behavior outside RaftCore.
- **ConfigLoader**: Reads JSON, validates it, and produces strongly typed configuration objects.

### Service Scope

- **Raft-node service**: RequestVote, AppendEntries, InstallSnapshot.
- **Metadata service**: Propose, Query, GetLeader, GetStatus, AddLearner, PromoteLearner, RemoveMember.
- **Store control service**: Register, Heartbeat, Stop, Remove, PollTasks, ReportTaskResult.
- **Write routing rule**: Any write received by a non-leader MUST return NOT_LEADER and the known leader address when available.
- **RPC callback rule**: RPC callbacks MUST perform validation, protocol conversion, and queue insertion only.

### JSON Configuration Scope

Configuration MUST cover:

- Node identity and addresses.
- Raft, metadata, and store-control ports.
- Initial members and member roles.
- Heartbeat interval and election-timeout range.
- RPC timeout and maximum message size.
- Queue capacity and worker-count settings.
- Log batch size.
- Data and snapshot directories.
- Store heartbeat timeout and failure-detection interval.
- Task poll limit.
- Log level.

### Concurrency Model

- One Raft protocol thread MUST serially drive RaftCore.
- One ordered apply thread MUST apply committed state-machine entries in log-index order.
- One persistence thread MUST handle ordered durable state changes.
- Multiple worker threads MAY accept RPC requests, but they MUST enqueue validated work instead of mutating consensus state.
- One independent outbound queue per peer MUST isolate peer backpressure.
- Shutdown MUST enter the runtime as an explicit event and drain or reject queued work according to clear state transitions.

### Persistence Requirements

- Persistent records MUST include format_version, record_type, index, term, payload_length, and checksum.
- Hard state, log entries, and snapshots MUST be durable and reloadable.
- Conflicting suffix truncation MUST preserve all entries before the conflict point.
- Incomplete tails MUST be detected and recovered without treating partial records as valid entries.
- Checksum errors MUST be reported as corruption and MUST NOT be silently ignored.
- Required flush operations MUST return explicit success or failure; a no-op flush MUST NOT be reported as durable success unless the platform contract proves it is safe.
- Snapshot save and install MUST preserve the snapshot boundary and membership view.
- Restart recovery MUST rebuild the latest safe hard state, log, snapshot, stable index, commit index, and applied index.

### Membership-Change Requirements

- Bootstrap configuration initializes the first membership view only.
- AddLearner, PromoteLearner, and RemoveMember MUST be represented as log-committed membership changes.
- Only one membership change MAY be in progress in the first version.
- Learner promotion requires catch-up to leader commit index, healthy communication, and no concurrent membership change.
- The system MUST reject unsafe automatic membership changes when the old voter set lacks quorum.
- The process MUST preserve quorum safety using a two-phase or equivalent safe transition.

### Cross-Platform Constraints

- Production code MUST run on Windows and Linux.
- Build definitions MUST support Visual Studio, Ninja, and Unix Makefiles.
- Manually written matching `.h` and `.cpp` files MUST be placed in the same directory.
- Tests MUST use temporary directories and MUST NOT depend on `/tmp`, `fork`, Bash, Linux signals, or fixed path separators.
- Platform compiler options, platform behavior, and dependency handling MUST remain separated by build-support boundaries.
- File flush, truncation, rename, atomic replacement, and path behavior MUST be specified and tested for both Windows and Linux semantics.

### Testing Requirements

- Unit tests MUST cover normal elections, split votes, leader failure, stale terms, learner constraints, log matching, conflict handling, commit advancement, and logical-tick behavior.
- Storage tests MUST cover normal restart, incomplete log tails, checksum errors, truncation, snapshot save/load, snapshot install, and durable flush error reporting.
- Runtime tests MUST cover queue-full throttling, serial RaftCore execution, ordered apply, duplicate apply prevention, persistence-completion gating, shutdown, and outbound per-peer queue backpressure.
- Membership tests MUST cover AddLearner, learner catch-up, learner promotion, voter removal, concurrent membership-change rejection, and quorum-loss safety.
- Metadata tests MUST cover store registration, duplicate registration, durable state transitions, heartbeat timeout behavior, leader-local transient heartbeat fields, and non-leader redirects.
- Placement tests MUST cover insufficient nodes, insufficient capacity, duplicate prevention, remaining-capacity ordering, deterministic tie-breaking, and replacement-policy boundary behavior.
- Task tests MUST cover task creation, lookup, FIFO polling, maximum poll count, duplicate polling, duplicate result reporting, state updates, and result storage.
- RPC and integration tests MUST cover node communication, client communication, leader changes during concurrent requests, and bounded timeout waits without long fixed sleeps.
- Cross-platform validation MUST cover Windows with MSVC, Linux with GCC, and Linux with Clang.

### Fixture Format

- Fixtures MUST be generated in advance and stored by scenario under `tests/fixtures`.
- Fixtures MUST be loaded by scenario name through FixtureLoader.
- Fixtures MUST be versioned and verifiable.
- Tests MUST NOT regenerate large log datasets on every run.
- Fixture scenarios MUST include elections, log conflicts, learner catch-up, restart recovery, corrupted storage, snapshots, store metadata, placement, task handling, and queue-full behavior.

### First-Version Scope

The first version includes reusable Raft consensus, persistent storage, restart recovery, snapshots, safe membership basics, real and simulated transport, bounded runtime queues, replaceable state machine, metadata control-plane example, store registry, simple placement, task management, simple task dispatching, JSON configuration, test fixtures, and cross-platform build/test coverage.

### Non-Goals

- Real Store data persistence.
- Multi-Raft.
- Cross-Raft-group transactions.
- Advanced placement, automatic data migration, rack awareness, or failure-domain awareness.
- Complex task workflows, dependency scheduling, or priority scheduling.
- Lease Read, Follower Read, or ReadIndex optimization.
- io_uring, O_DIRECT, IOCP storage optimization, mmap-based WAL, or complex WAL segmentation.
- Lock-free queues.
- Coroutines.
- Byzantine fault tolerance.

### Key Entities *(include if feature involves data)*

- **Raft Node**: A consensus participant with identity, state, role, term, vote, log, storage, transport, runtime queues, and membership view.
- **Raft Log**: Ordered entries with index, term, payload, persistence state, commit state, apply state, and snapshot boundary.
- **Hard State**: Durable consensus state required for safe restart.
- **Snapshot**: Compact durable state-machine image plus the log boundary and membership information needed for recovery.
- **Membership View**: Voter and learner sets plus transition state.
- **Metadata Command**: A durable command applied through the metadata state machine.
- **Store**: A registered storage participant in the metadata example.
- **Placement Policy**: Replaceable selection rule for choosing stores.
- **Task**: A metadata-managed unit of work assigned to stores.
- **Configuration**: Validated node, cluster, timeout, queue, storage, snapshot, store, task, and logging settings.
- **Fixture**: Versioned test input with expected verification metadata.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In a four-node test cluster with three voters and one learner, a single leader is elected within the configured election window in at least 99% of deterministic logical-tick test runs.
- **SC-002**: After 1,000 sequential metadata proposals under normal majority availability, every committed proposal is applied exactly once and in increasing index order on all available voters.
- **SC-003**: A restarted node recovers to the latest safe durable state for normal logs, incomplete tails, valid snapshots, and checksum-protected records in 100% of covered recovery scenarios.
- **SC-004**: Log conflict tests for empty followers, lagging followers, extra follower entries, same-index different-term conflicts, and multi-term conflicts all converge without applying a conflicting entry.
- **SC-005**: Queue-full scenarios return an explicit throttling result without unbounded memory growth in 100% of covered runtime and RPC-entry tests.
- **SC-006**: Learner promotion succeeds only after catch-up and healthy communication and is rejected in 100% of tested unsafe conditions.
- **SC-007**: Metadata store registration, durable store state transitions, placement selection, and task idempotency pass all module tests on Windows and Linux supported toolchains.
- **SC-008**: The metadata state machine can be replaced in a test-only alternate state machine without changing RaftCore, RaftLog, RaftStorage, RaftRuntime, RaftTransport, membership logic, or test framework contracts.
- **SC-009**: All required fixture scenarios are loaded by name from pre-generated, versioned fixtures without regenerating large datasets during normal test execution.
- **SC-010**: The first-version non-goals are absent from production behavior and do not create unused abstractions or placeholder extension layers.

## Assumptions

- The first feature specification is stored under `specs/001-cross-platform-raft` because no existing `specs/` feature directories were present.
- The mandatory git feature hook was not executed because repository rules require explicit authorization before branch switching and the working tree already had an unrelated `AGENTS.md` modification.
- The initial deployment model is a small metadata cluster with a configured bootstrap member list.
- Authentication, authorization, encryption, certificate management, and multi-tenant access control are outside this first specification unless added later.
- The first file-backed storage format is new and does not need to migrate existing production Raft data.
- The metadata example is intentionally small and exists to prove the reusable Raft boundaries.
