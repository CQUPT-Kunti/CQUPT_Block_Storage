# Tasks: Cross-Platform Raft Metadata Foundation

**Input**: Design documents from `specs/001-cross-platform-raft/`  
**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/`, `quickstart.md`  
**Organization**: Tasks follow the 10 implementation phases from the plan. Story labels map phase work to the specification stories: US1 durable Raft cluster, US2 replacement boundaries, US3 Metadata store/task control plane, US4 safe membership changes.  
**Format**: `- [ ] T### [P?] [US?] P#-T## Task name with file path`

## Phase 1: Project Foundation and Cross-Platform Framework

**Goal**: Establish build, module layout, shared utilities, config, platform wrappers, logging, and lifecycle foundations.

- [X] T001 P1-T01 Create the C++17 project skeleton and module directories in `cross-platform-raft/`
  - Scope: Create `cross-platform-raft/CMakeLists.txt`, `cmake/`, `config/`, `proto/`, `src/common/`, `src/platform/`, `src/config/`, `src/raft/`, `src/metadata/`, `src/store/`, `src/rpc/`, `src/server/`, `tests/`, and `tools/`.
  - Dependencies: None.
  - Done: Directory structure exists, manually maintained `.h`/`.cpp` pairs are planned in the same directories, and root CMake can include module subdirectories without adding feature code.
  - Parallel: No.

- [X] T002 P1-T02 Configure CMake dependencies and compiler/platform options in `cross-platform-raft/CMakeLists.txt` and `cross-platform-raft/cmake/`
  - Scope: Add C++17 standard, MSVC/GCC/Clang warnings, Visual Studio/Ninja/Unix Makefiles compatibility, gRPC, Protocol Buffers, GoogleTest, nlohmann/json, optional spdlog, and generated protobuf build integration.
  - Dependencies: T001.
  - Done: CMake configuration uses `CompilerOptions.cmake`, `Dependencies.cmake`, and `PlatformOptions.cmake`; no dependency version or compiler standard beyond the plan is introduced.
  - Parallel: No.

- [X] T003 [P] P1-T03 Implement common status, IDs, checksum, and bounded queue utilities in `cross-platform-raft/src/common/`
  - Scope: Implement `types.h`, `status.h/.cpp`, `checksum.h/.cpp`, and `bounded_queue.h/.cpp` with explicit BUSY, RESOURCE_EXHAUSTED, and RETRY_LATER style statuses.
  - Dependencies: T001.
  - Done: Common utilities have no dependency on Raft, platform-specific APIs, metadata, store, RPC, or server modules.
  - Parallel: Yes, after T001; edits only `src/common/`.

- [X] T004 [P] P1-T04 Implement JSON configuration loading and validation in `cross-platform-raft/src/config/` and `cross-platform-raft/config/`
  - Scope: Implement `config.h/.cpp`, `config_loader.h/.cpp`, and `node-1.json` through `node-4.json` covering all manually configurable parameters from the spec.
  - Dependencies: T003.
  - Done: Config validates node ids, ports, paths, bootstrap membership, heartbeat/election timeout relationship, queue capacities, message-size limits, and keeps JSON parsing outside RaftCore.
  - Parallel: Yes, after T003; edits only config files.

- [X] T005 [P] P1-T05 Implement FileOps, ProcessOps, and TimeOps in `cross-platform-raft/src/platform/`
  - Scope: Implement `file_ops.h/.cpp`, `process_ops.h/.cpp`, and `time_ops.h/.cpp` for Windows/Linux file operations, graceful shutdown hooks, monotonic time, and test-time utilities.
  - Dependencies: T003.
  - Done: All direct platform APIs are isolated here; business and Raft modules use platform abstractions only.
  - Parallel: Yes, after T003; edits only `src/platform/`.

- [X] T006 [P] P1-T06 Add basic logging and server lifecycle scaffolding in `cross-platform-raft/src/server/`
  - Scope: Add minimal logging integration with optional spdlog fallback and lifecycle shell for startup/shutdown composition.
  - Dependencies: T002, T003, T004, T005.
  - Done: Server lifecycle does not contain Raft algorithms or business logic; logging remains optional.
  - Parallel: Yes, after dependencies; edits only server scaffolding.

- [X] T007 P1-T07 Run consolidated phase 1 verification for build, config, and platform basics using `cross-platform-raft/tests/`
  - Scope: Add grouped GoogleTest coverage for status, checksum, bounded queue, config loading, and FileOps temporary directory/read/write/truncate/flush/rename/atomic replacement behavior.
  - Dependencies: T001-T006.
  - Done: Linux preset build and test commands are documented/run when available; Windows preset commands are documented; tests use `std::filesystem` temporary directories and avoid `/tmp`, Bash, fork, Linux-only signals, and fixed separators.
  - Parallel: No.

## Phase 2: Raft Data Structures and RaftLog

**Goal**: Establish protocol data types and log invariants before protocol logic.

- [X] T008 P2-T01 Implement Raft base types and protocol data structures in `cross-platform-raft/src/raft/raft_types.h` and `cross-platform-raft/src/raft/raft_message.h`
  - Scope: Define RaftRole, NodeState, LogEntry, HardState, RaftMessage, RaftMember, PeerProgress, Snapshot metadata, membership data, and replacement-safe opaque payload fields.
  - Dependencies: T003.
  - Done: RaftRole contains only FOLLOWER, CANDIDATE, LEADER, LEARNER; NodeState contains only RUNNING, STOPPED, FAILED; no business types are introduced.
  - Parallel: No.

- [X] T009 P2-T02 Implement RaftLog append, lookup, conflict, and progress behavior in `cross-platform-raft/src/raft/raft_log.h` and `cross-platform-raft/src/raft/raft_log.cpp`
  - Scope: Implement append, batch append, index lookup, term lookup, log matching, conflict detection, conflict truncation, range reads, stableIndex, commitIndex, appliedIndex, and snapshot boundary behavior.
  - Dependencies: T008.
  - Done: RaftLog distinguishes unpersisted, persisted, committed, and applied entries and enforces `appliedIndex <= commitIndex <= lastIndex`.
  - Parallel: No.

- [X] T010 P2-T03 Run consolidated RaftLog verification in `cross-platform-raft/tests/raft_log_test.cpp`
  - Scope: Test log append, batch append, range query, term match, conflict truncation, marker advancement, snapshot boundary behavior, and invariant rejection paths.
  - Dependencies: T008, T009.
  - Done: Primary RaftLog behaviors and invariants pass as one grouped test target.
  - Parallel: No.

## Phase 3: RaftCore Protocol Logic

**Goal**: Deliver deterministic in-memory Raft elections, replication, conflict catch-up, and learner restrictions for US1.

- [X] T011 [US1] P3-T01 Implement RaftCore state transitions and logical tick handling in `cross-platform-raft/src/raft/raft_core.h` and `cross-platform-raft/src/raft/raft_core.cpp`
  - Scope: Implement Follower, Candidate, Leader, Learner transitions; logical Tick handling; randomized election timeout input; election start; term/vote changes.
  - Dependencies: T009.
  - Done: RaftCore remains single-threaded and does not include gRPC, file, JSON, platform, thread, time, or business dependencies.
  - Parallel: No.

- [X] T012 [US1] P3-T02 Implement RequestVote, heartbeat, and AppendEntries handling in `cross-platform-raft/src/raft/raft_core.cpp`
  - Scope: Implement RequestVote request/response, leader heartbeat generation, AppendEntries request/response, stale-term handling, and log match checks.
  - Dependencies: T011.
  - Done: Vote and append behavior follows Raft log matching and term rules using RaftLog only.
  - Parallel: No.

- [X] T013 [US1] P3-T03 Implement leader replication progress and majority commit in `cross-platform-raft/src/raft/raft_core.cpp`
  - Scope: Implement nextIndex, matchIndex, follower catch-up, conflict backoff, majority commitIndex advancement, apply-ready output, and learner progress without quorum participation.
  - Dependencies: T012.
  - Done: Leader advances commitIndex only after majority replication and never counts learners in quorum.
  - Parallel: No.

- [X] T014 [US1] P3-T04 Implement RaftCore output batches for persistence, messages, and committed ranges in `cross-platform-raft/src/raft/raft_core.h` and `cross-platform-raft/src/raft/raft_core.cpp`
  - Scope: Expose stable outputs for HardState updates, log entries needing persistence, outbound messages gated by persistence, and committed ranges.
  - Dependencies: T013.
  - Done: RaftCore reports what must be persisted before dependent messages but does not perform storage or transport work itself.
  - Parallel: No.

- [X] T015 [US1] P3-T05 Run consolidated RaftCore simulation tests in `cross-platform-raft/tests/raft_core_test.cpp`
  - Scope: Use logical ticks and in-memory logs to test elections, split votes, leader failure, RequestVote, heartbeats, replication, conflict catch-up, majority commit, and learner restrictions.
  - Dependencies: T011-T014.
  - Done: Tests contain no real sleeps or wall-clock dependencies and verify deterministic protocol behavior.
  - Parallel: No.

## Phase 4: Raft Persistence, Snapshot, and Recovery

**Goal**: Provide durable storage and recovery while preserving platform isolation and persistence-before-message rules for US1.

- [X] T016 [US1] P4-T01 Define IRaftStorage and implement MemoryRaftStorage in `cross-platform-raft/src/raft/raft_storage.h`, `memory_raft_storage.h`, and `memory_raft_storage.cpp`
  - Scope: Define Open, Load, SaveHardState, AppendEntries, TruncateSuffix, SaveSnapshot, and in-memory behavior for tests.
  - Dependencies: T014.
  - Done: Storage interface is stable and replaceable without RaftCore changes.
  - Parallel: No.

- [X] T017 [US1] P4-T02 Implement FileRaftStorage record format and durable log operations in `cross-platform-raft/src/raft/raft_storage.cpp`, `file_raft_storage.h`, and `file_raft_storage.cpp`
  - Scope: Implement format_version, record_type, index, term, payload_length, checksum, HardState save/load, log append/load, conflict truncation, and durable flush via FileOps.
  - Dependencies: T016, T005.
  - Done: FileRaftStorage does not call Windows/Linux APIs directly and never treats missing flush as successful durability.
  - Parallel: No.

- [X] T018 [US1] P4-T03 Implement Snapshot save, load, validation, compaction boundary, and startup recovery in `cross-platform-raft/src/raft/raft_snapshot.h`, `raft_snapshot.cpp`, and storage files
  - Scope: Save/load snapshots, validate checksums, preserve last included index/term and membership view, recover Snapshot plus HardState plus logs on startup.
  - Dependencies: T017.
  - Done: Incomplete tails are recovered safely, checksum errors are explicit corruption, and snapshot boundaries remain compatible with RaftLog.
  - Parallel: No.

- [X] T019 [US1] P4-T04 Document and enforce persistence gating between RaftCore, Runtime, and Storage in `cross-platform-raft/src/raft/raft_storage.h` and `cross-platform-raft/src/raft/raft_core.h`
  - Scope: Capture that HardState/log data required by outgoing messages or successful responses must persist before those messages/responses are released.
  - Dependencies: T014, T018.
  - Done: Interfaces expose enough result metadata for Runtime to gate messages and client success without returning success for memory-only or leader-local-disk-only entries.
  - Parallel: No.

- [X] T020 [US1] P4-T05 Run consolidated persistence and recovery tests in `cross-platform-raft/tests/raft_storage_test.cpp`
  - Scope: Verify clean restart, incomplete tails, checksum failures, conflict truncation, flush failure handling, snapshot save/load, and snapshot recovery with temporary directories.
  - Dependencies: T016-T019.
  - Done: Memory and file storage contract behavior is covered without direct platform API use outside FileOps.
  - Parallel: No.

## Phase 5: RaftRuntime and Concurrency Execution Model

**Goal**: Drive RaftCore through bounded queues and ordered worker threads for US1.

- [X] T021 [US1] P5-T01 Implement RaftRuntime event model and bounded queues in `cross-platform-raft/src/raft/raft_runtime.h` and `raft_runtime.cpp`
  - Scope: Define Tick, network message, Proposal, membership change, persistence completion, apply completion, and shutdown events with bounded queue insertion results.
  - Dependencies: T019, T003.
  - Done: Full queues return BUSY, RESOURCE_EXHAUSTED, RETRY_LATER, or equivalent explicit statuses.
  - Parallel: No.

- [X] T022 [US1] P5-T02 Implement protocol loop, persistence worker, and persistence completion gating in `cross-platform-raft/src/raft/raft_runtime.cpp`
  - Scope: Drive RaftCore from one protocol thread, dispatch ordered persistence work, release dependent Raft messages only after storage success, and fail dependent work on storage errors.
  - Dependencies: T021.
  - Done: gRPC-style callers can only enqueue work; they cannot mutate RaftCore.
  - Parallel: No.

- [X] T023 [US1] P5-T03 Implement ordered apply worker and Proposal result notification in `cross-platform-raft/src/raft/raft_runtime.cpp`
  - Scope: Apply committed entries in LogIndex order, prevent duplicate application, correlate Proposal completion, and return client success only after commit and apply complete.
  - Dependencies: T022.
  - Done: Runtime never returns proposal success for entries that are only in memory or only on local leader disk.
  - Parallel: No.

- [X] T024 [US1] P5-T04 Implement peer outbound queues, startup, shutdown, and graceful thread lifecycle in `cross-platform-raft/src/raft/raft_runtime.cpp`
  - Scope: Add one bounded outbound queue per peer, lifecycle start/stop, queue draining/rejection rules, and thread joins.
  - Dependencies: T023.
  - Done: Shutdown is deterministic and does not leave required runtime threads running.
  - Parallel: No.

- [X] T025 [US1] P5-T05 Run consolidated runtime concurrency tests in `cross-platform-raft/tests/raft_runtime_test.cpp`
  - Scope: Verify concurrent Proposals, queue throttling, persistence completion gating, ordered apply, duplicate apply protection, per-peer backpressure, and thread shutdown.
  - Dependencies: T021-T024.
  - Done: Tests use bounded waits only and do not rely on long fixed sleeps.
  - Parallel: No.

## Phase 6: gRPC Peer and Client Communication

**Goal**: Connect nodes and clients through thin RPC adapters for US1.

- [X] T026 [P] [US1] P6-T01 Define Raft, Metadata, and Store protobuf contracts in `cross-platform-raft/proto/raft.proto`, `metadata.proto`, and `store.proto`
  - Scope: Define RequestVote, AppendEntries, InstallSnapshot, Metadata Propose/Query/GetLeader/GetStatus, and Store service message shells needed by current phases.
  - Dependencies: T008, T021.
  - Done: Contracts match `contracts/interfaces-and-services.md` and avoid embedding consensus algorithms or business-only behavior in protobuf design.
  - Parallel: Yes, after stable runtime/core interfaces.

- [X] T027 [US1] P6-T02 Integrate protobuf/gRPC generation and libraries in `cross-platform-raft/CMakeLists.txt` and `cross-platform-raft/cmake/Dependencies.cmake`
  - Scope: Add generated source targets, link RPC/server/tests to generated protobuf/gRPC outputs, and preserve MSVC/GCC/Clang build compatibility.
  - Dependencies: T026, T002.
  - Done: Generated files are build artifacts; manually maintained `.h`/`.cpp` files remain paired in the same source directories.
  - Parallel: No.

- [X] T028 [US1] P6-T03 Implement IRaftTransport and GrpcRaftTransport in `cross-platform-raft/src/raft/raft_transport.h`, `raft_transport.cpp`, `grpc_raft_transport.h`, and `grpc_raft_transport.cpp`
  - Scope: Define transport send contracts, peer connection management, timeouts, errors, and per-peer outbound queue integration.
  - Dependencies: T024, T027.
  - Done: Transport has real gRPC and later simulated implementations behind the same stable interface.
  - Parallel: No.

- [X] T029 [US1] P6-T04 Implement Raft and Metadata RPC service adapters in `cross-platform-raft/src/rpc/raft_rpc_service.h/.cpp` and `metadata_rpc_service.h/.cpp`
  - Scope: Implement RequestVote, AppendEntries, InstallSnapshot, Propose, Query, GetLeader, and GetStatus adapters with validation, conversion, queue insertion, timeout handling, error mapping, and NOT_LEADER leader-address responses.
  - Dependencies: T027, T028.
  - Done: RPC code does not contain election, quorum, conflict, or direct RaftCore mutation logic.
  - Parallel: No.

- [X] T030 [US1] P6-T05 Run consolidated three-node gRPC integration tests in `cross-platform-raft/tests/grpc_cluster_test.cpp`
  - Scope: Start three real gRPC nodes, verify leader election, Proposal, log replication, leader shutdown, and re-election.
  - Dependencies: T026-T029.
  - Done: Integration uses bounded timeouts and logs full output under `tmp/test-logs/` when needed.
  - Parallel: No.

## Phase 7: Raft Membership Management

**Goal**: Add safe voter/learner membership changes for US4 without weakening quorum safety.

- [ ] T031 [US4] P7-T01 Implement membership state and log entries in `cross-platform-raft/src/raft/raft_membership.h` and `raft_membership.cpp`
  - Scope: Implement voters, learners, active transition state, membership log entries, bootstrap-only initial config, and persistence metadata.
  - Dependencies: T018, T024.
  - Done: Learners do not vote, count in quorum, or become leader; bootstrap config is not runtime truth after startup.
  - Parallel: No.

- [ ] T032 [US4] P7-T02 Implement AddLearner and learner catch-up decisions in `cross-platform-raft/src/raft/raft_core.cpp` and `raft_membership.cpp`
  - Scope: Add learner membership proposals, learner replication tracking, log/snapshot catch-up decisions, and health/caught-up status.
  - Dependencies: T031, T028.
  - Done: New nodes join as learners and receive logs or snapshots without voting.
  - Parallel: No.

- [ ] T033 [US4] P7-T03 Implement PromoteLearner, RemoveMember, and one-change-at-a-time enforcement in `cross-platform-raft/src/raft/raft_membership.cpp` and `raft_core.cpp`
  - Scope: Enforce promotion preconditions, removal rules, single active change, and rejection when old configuration lost quorum.
  - Dependencies: T032.
  - Done: Membership changes are committed through the Raft log and unsafe automatic membership modification is impossible.
  - Parallel: No.

- [ ] T034 [US4] P7-T04 Implement two-phase or equivalent safe configuration transition and membership RPC adapters in `cross-platform-raft/src/rpc/metadata_rpc_service.cpp`
  - Scope: Add AddLearner, PromoteLearner, and RemoveMember request flow through Metadata/Raft runtime queues and membership log application.
  - Dependencies: T033, T029.
  - Done: Runtime exposes membership results through RPC without moving consensus rules into RPC.
  - Parallel: No.

- [ ] T035 [US4] P7-T05 Run consolidated membership tests in `cross-platform-raft/tests/raft_membership_test.cpp`
  - Scope: Verify learners do not vote/count/lead, learner catch-up, promotion, removal, one-change rejection, quorum-loss safety, and permanently failed voter replacement while quorum remains.
  - Dependencies: T031-T034.
  - Done: Membership tests include both simulation and phase-level integration paths.
  - Parallel: No.

## Phase 8: Replaceable State Machine and Metadata Example

**Goal**: Provide the replaceable state-machine boundary and Metadata example for US2.

- [ ] T036 [US2] P8-T01 Define IRaftStateMachine and MetadataCommand in `cross-platform-raft/src/metadata/state_machine.h` and `metadata_command.h/.cpp`
  - Scope: Define Apply, CreateSnapshot, RestoreSnapshot, opaque command payload handling, and MetadataCommand serialization boundaries.
  - Dependencies: T024, T008.
  - Done: Raft module does not include Store, Placement, Task, or metadata business types.
  - Parallel: No.

- [ ] T037 [US2] P8-T02 Implement MetadataStateMachine apply, query, snapshot, and restore in `cross-platform-raft/src/metadata/metadata_state_machine.h/.cpp`
  - Scope: Apply MetadataCommand in log-index order, expose query state, create snapshots, restore snapshots, and prevent duplicate application by index.
  - Dependencies: T036.
  - Done: Metadata state can recover from snapshots and command replay without Raft internals.
  - Parallel: No.

- [ ] T038 [US2] P8-T03 Implement MetadataService proposal/query/result correlation in `cross-platform-raft/src/metadata/metadata_service.h/.cpp`
  - Scope: Connect Metadata commands to RaftRuntime Proposal flow, query state machine data, and correlate client results after commit/apply.
  - Dependencies: T037, T023, T029.
  - Done: Propose success means committed and applied; alternate state machine tests can replace MetadataStateMachine without RaftCore changes.
  - Parallel: No.

- [ ] T039 [US2] P8-T04 Run consolidated state-machine tests in `cross-platform-raft/tests/metadata_state_machine_test.cpp`
  - Scope: Verify command submission, ordered apply, duplicate apply protection, query, snapshot create/restore, recovery, and test-only alternate state machine replacement.
  - Dependencies: T036-T038.
  - Done: Tests prove replacement boundaries without modifying RaftCore, RaftLog, RaftRuntime, RaftStorage, RaftTransport, or membership logic.
  - Parallel: No.

## Phase 9: StoreRegistry, Placement, and Task Examples

**Goal**: Implement the Metadata example store, placement, and task control plane for US3.

- [ ] T040 [US3] P9-T01 Implement StoreRegistry and StoreInfo in `cross-platform-raft/src/store/store_types.h` and `store_registry.h/.cpp`
  - Scope: Implement StoreInfo, RUNNING/STOPPED/FAILED, registration, duplicate registration, updates, stopping, failed state, removal, lookup, listing, generation, and durable transition commands.
  - Dependencies: T038.
  - Done: Store id/address/generation/important states go through Raft; last_heartbeat_ms and transient load/latency remain leader-local.
  - Parallel: No.

- [ ] T041 [P] [US3] P9-T02 Implement IPlacementPolicy and SimplePlacementPolicy in `cross-platform-raft/src/store/placement_policy.h` and `simple_placement_policy.h/.cpp`
  - Scope: Select RUNNING stores with sufficient capacity, prevent duplicates, sort by remaining capacity descending, break ties by Store ID, and return clear insufficient node/capacity errors.
  - Dependencies: T040.
  - Done: Placement can later be replaced without changing RaftCore, StoreRegistry durable data, MetadataService primary contract, or RPC contracts.
  - Parallel: Yes, after T040; edits placement files only.

- [ ] T042 [P] [US3] P9-T03 Implement TaskManager and SimpleTaskDispatcher in `cross-platform-raft/src/store/task_manager.h/.cpp`, `task_dispatcher.h`, and `simple_task_dispatcher.h/.cpp`
  - Scope: Implement TaskType, TaskState, task creation, lookup, state updates, task_id idempotency, result storage, FIFO polling, poll limit, and duplicate result handling.
  - Dependencies: T040.
  - Done: TaskDispatcher can later be replaced without changing RaftCore, MetadataStateMachine, or primary Store RPC contracts.
  - Parallel: Yes, after T040; edits task files only.

- [ ] T043 [US3] P9-T04 Implement Store control service integration in `cross-platform-raft/src/rpc/store_rpc_service.h/.cpp` and `cross-platform-raft/src/metadata/metadata_service.cpp`
  - Scope: Implement Register, Heartbeat, Stop, Remove, PollTasks, ReportTaskResult adapters; make heartbeats leader-local; route important transitions through Metadata consensus.
  - Dependencies: T041, T042, T029, T038.
  - Done: Ordinary heartbeats are not individually written to Raft, while important RUNNING/STOPPED/FAILED transitions are committed.
  - Parallel: No.

- [ ] T044 [US3] P9-T05 Run consolidated business example tests in `cross-platform-raft/tests/store_metadata_test.cpp`
  - Scope: Verify registration, duplicate registration, heartbeat timeout, failure detection, simple placement, insufficient capacity/nodes, task polling, duplicate polling, and duplicate result reporting.
  - Dependencies: T040-T043.
  - Done: Tests validate Metadata example behavior without adding Store/Placement/Task dependencies to RaftCore.
  - Parallel: No.

## Phase 10: Fixtures, Tools, Cross-Platform Integration, and Final Acceptance

**Goal**: Finalize fixture-driven testing, tooling, cross-platform validation, and v1 acceptance.

- [ ] T045 [P] P10-T01 Implement FixtureLoader and SimulatedCluster in `cross-platform-raft/tests/fixture_loader.h/.cpp` and `simulated_cluster.h/.cpp`
  - Scope: Load versioned fixtures by scenario name, validate checksums, provide deterministic simulated clusters and simulated transport for tests.
  - Dependencies: T035, T039.
  - Done: Normal tests load existing fixtures and do not regenerate large datasets.
  - Parallel: Yes, after membership/state-machine interfaces are stable.

- [ ] T046 [P] P10-T02 Create versioned fixture sets and fixture_generator in `cross-platform-raft/tests/fixtures/` and `cross-platform-raft/tools/fixture_generator/`
  - Scope: Organize election, log_replication, membership, recovery, snapshot, store, placement, and task fixture data with version and verification metadata.
  - Dependencies: T045.
  - Done: Fixture generator is deterministic and only used when intentionally regenerating fixtures.
  - Parallel: Yes, after T045; edits fixture/tool files.

- [ ] T047 [P] P10-T03 Implement client, raftctl, and wal_dump tools in `cross-platform-raft/tools/`
  - Scope: Implement simple Metadata/Store client, raftctl cluster/membership commands, and wal_dump inspection of records, checksums, hard state, log entries, and snapshots.
  - Dependencies: T030, T035, T043.
  - Done: Tools use public RPC contracts or storage formats and do not depend on private test hooks.
  - Parallel: Yes, after service/storage contracts are stable.

- [ ] T048 P10-T04 Add server composition and runbook updates in `cross-platform-raft/src/server/` and `cross-platform-raft/README.md`
  - Scope: Compose config, storage, transport, runtime, metadata, store service, process lifecycle, and document first-version limitations and run commands.
  - Dependencies: T030, T035, T043, T047.
  - Done: Server owns lifecycle only; docs list excluded v1 features and platform test constraints.
  - Parallel: No.

- [ ] T049 P10-T05 Run final consolidated acceptance verification across `cross-platform-raft/`
  - Scope: Run Linux build/test presets when available, document Windows MSVC build/test commands, execute GoogleTest suites, three-node integration, recovery, concurrency, membership, metadata/store/task, and fixture-loading acceptance.
  - Dependencies: T001-T048.
  - Done: Full logs are stored under `tmp/test-logs/`; tests use `std::filesystem` temporary directories and avoid `/tmp`, fork, Bash, Linux-only signals, fixed separators, long fixed sleeps, and first-version non-goal behavior.
  - Parallel: No.

## Dependency Graph

```text
P1 foundation -> P2 Raft types/log -> P3 RaftCore -> P4 storage/recovery -> P5 runtime
P5 runtime -> P6 gRPC transport/RPC -> P7 membership -> P8 state machine -> P9 metadata examples
P6/P7/P8/P9 -> P10 fixtures/tools/acceptance
```

Detailed blocking path:

- T001 -> T002/T003 -> T004/T005/T006 -> T007.
- T003 -> T008 -> T009 -> T010.
- T009 -> T011 -> T012 -> T013 -> T014 -> T015.
- T014 + T005 -> T016 -> T017 -> T018 -> T019 -> T020.
- T019 -> T021 -> T022 -> T023 -> T024 -> T025.
- T024 + T008 -> T026 -> T027 -> T028 -> T029 -> T030.
- T018 + T024 -> T031 -> T032 -> T033 -> T034 -> T035.
- T024 + T008 -> T036 -> T037 -> T038 -> T039.
- T038 -> T040 -> T041/T042 -> T043 -> T044.
- T035 + T039 -> T045 -> T046; T030 + T035 + T043 -> T047 -> T048 -> T049.

## User Story Coverage and Independent Test Criteria

- **US1 Operate a Durable Raft Metadata Cluster**: Covered by T008-T030. Independent test: deterministic logical-tick simulations, persistence recovery, runtime concurrency checks, and three-node gRPC integration show election, replication, commit/apply, restart, and leader failover.
- **US2 Replace Business Logic Without Rewriting Raft**: Covered by T036-T039. Independent test: replace MetadataStateMachine with a test-only alternate state machine without changing RaftCore, RaftLog, RaftRuntime, RaftStorage, RaftTransport, or membership logic.
- **US3 Manage Stores and Tasks Through Metadata Control Plane**: Covered by T040-T044. Independent test: Store registration, heartbeat timeout, placement, task polling, duplicate result handling, and non-Raft business behavior run through Metadata service boundaries.
- **US4 Safely Change Cluster Membership**: Covered by T031-T035. Independent test: learner add/catch-up/promotion/removal and failed voter replacement preserve quorum safety.

## Parallel Execution Examples

After T001 and T003:

```text
T004 config loading can proceed with T005 platform wrappers.
```

After T040:

```text
T041 placement policy and T042 task dispatcher can proceed in parallel because they edit different files and share only StoreRegistry contracts.
```

After T045:

```text
T046 fixture generation and T047 tools can proceed in parallel once service/storage contracts are stable.
```

## Implementation Strategy

1. Complete P1 and P2 first; they block all useful implementation.
2. MVP for the durable Raft cluster is P3 through P6 with T030 as the first end-to-end cluster check.
3. Add membership safety in P7 before presenting operational cluster management as complete.
4. Add replaceable state-machine proof in P8, then Metadata examples in P9.
5. Finish with fixtures, tools, and final cross-platform acceptance in P10.

## Scope Guard

- Do not implement real Store data persistence, Multi-Raft, cross-Raft-group transactions, advanced placement, automatic migration, rack awareness, complex workflows, Lease Read, Follower Read, ReadIndex optimization, io_uring, O_DIRECT, IOCP storage optimization, mmap WAL, lock-free queues, coroutines, or Byzantine fault tolerance.
- Do not move consensus logic into RPC, storage, metadata, store, tools, or server composition.
- Do not return client success before the corresponding log entry is committed and applied.
