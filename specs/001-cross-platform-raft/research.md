# Research: Cross-Platform Raft Metadata Foundation

## Decision: Keep RaftCore Deterministic and Single-Threaded

**Rationale**: The specification requires RaftCore to avoid direct threads, time reads, RPC, files, JSON, platform APIs, and business objects. A single protocol thread gives deterministic tests with logical ticks and keeps consensus reasoning local.

**Alternatives considered**: Multi-threaded RaftCore and lock-free message handling were rejected because they add concurrency risk and are explicit non-goals.

## Decision: Use Bounded Queues at All External Entry Points

**Rationale**: gRPC workers, client proposals, peer messages, persistence completions, apply completions, and outbound peer traffic need backpressure. Bounded queues satisfy the memory-growth requirement and provide explicit BUSY/RESOURCE_EXHAUSTED/RETRY_LATER behavior.

**Alternatives considered**: Unbounded queues were rejected because they can hide overload until memory exhaustion. Dropping messages silently was rejected because it breaks safety and debuggability.

## Decision: Persist Before Dependent Messages and Success Responses

**Rationale**: Raft safety depends on durable terms, votes, accepted entries, and snapshots. Any outbound message or client success that depends on such state must wait for storage completion.

**Alternatives considered**: Sending AppendEntries or success responses before local persistence was rejected because restart could later contradict the already-sent response.

## Decision: Isolate Windows/Linux File Semantics in FileOps

**Rationale**: The specification forbids business and Raft code from calling platform APIs directly. FileOps is the single place to handle create/open/read/write/truncate/flush/atomic replace/rename/delete and platform differences.

**Alternatives considered**: Direct calls from FileRaftStorage were rejected because they spread platform-specific durability rules into Raft persistence code.

## Decision: Use a Simple v1 WAL Record Format

**Rationale**: The required format fields are format_version, record_type, index, term, payload_length, and checksum. A simple append/truncate/load model is enough for v1 and keeps recovery testable.

**Alternatives considered**: mmap WAL, segmented WAL, O_DIRECT, io_uring, and IOCP storage optimization were rejected because they are explicit non-goals.

## Decision: Use Explicit Storage, Transport, State-Machine, Placement, and Task Boundaries

**Rationale**: These are the replacement points required by the specification. Other abstractions should not be added unless a phase needs them.

**Alternatives considered**: Interface-per-class designs were rejected because they would add abstraction without a replacement requirement.

## Decision: Implement Membership Changes as Log-Committed Two-Phase Transitions

**Rationale**: Membership must be replicated through the log, allow one change at a time, prevent learners from voting/leading, and avoid automatic unsafe changes after quorum loss.

**Alternatives considered**: Direct config-file membership edits and automatic replacement after quorum loss were rejected because they bypass consensus safety.

## Decision: Keep Fixtures Pre-Generated and Versioned

**Rationale**: Fixture-driven tests must be deterministic, load by scenario name, and avoid regenerating large datasets in normal test runs.

**Alternatives considered**: Runtime generation for every test was rejected because it slows tests and hides fixture compatibility issues.
