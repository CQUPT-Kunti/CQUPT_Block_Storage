# Quickstart: Cross-Platform Raft Metadata Foundation Plan

## Read Order

1. `specs/001-cross-platform-raft/spec.md`
2. `specs/001-cross-platform-raft/plan.md`
3. `specs/001-cross-platform-raft/research.md`
4. `specs/001-cross-platform-raft/data-model.md`
5. `specs/001-cross-platform-raft/contracts/interfaces-and-services.md`

## Implementation Entry Point

Use `/speckit-tasks` after this plan to generate an implementation task list. Keep task granularity phase-oriented: implement each phase first, then run the phase's consolidated verification group.

## Build Commands

Linux:

```bash
cmake --preset debug-ninja-low-parallel
cmake --build --preset debug-ninja-low-parallel
ctest --preset debug-tests
```

Windows:

```powershell
cmake --preset windows
cmake --build --preset windows-debug
ctest --preset windows-debug-tests
```

## Phase Verification Pattern

For each phase:

1. Implement the phase deliverables.
2. Run one consolidated build check for the relevant platform.
3. Run the phase's grouped unit tests.
4. Run the phase-level integration tests only after the phase components exist.
5. Record full logs under `tmp/test-logs/` when test output is needed.

## Cross-Platform Test Rules

- Use `std::filesystem` temporary directories.
- Do not depend on `/tmp`, fork, Bash, Linux-only signals, or fixed path separators.
- Keep platform API calls inside FileOps, ProcessOps, or TimeOps.
- Verify Windows flush/rename/truncation semantics through FileOps tests instead of RaftStorage direct platform tests.

## Scope Guard

Do not add v1 support for Multi-Raft, real Store data persistence, advanced placement, automatic migration, complex workflows, lease reads, follower reads, ReadIndex optimization, lock-free queues, coroutines, mmap WAL, O_DIRECT, io_uring, IOCP storage optimization, or Byzantine fault tolerance.
