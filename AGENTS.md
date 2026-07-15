<!-- SPECKIT START -->
Current Spec Kit plan: `specs/001-cross-platform-raft/plan.md`

# CQUPT_Block_Storage

## Purpose

This file defines repository-wide rules for project structure, module documentation, C++ code organization, build verification, and safe code modification.

CQUPT_Block_Storage is a cross-platform distributed block storage system implemented with C++14 or C++17 for Windows and Linux.

The project prioritizes data integrity, durability, concurrency safety, failure recovery, portability, and persistent format compatibility.

Correctness and data safety take priority over performance, code size, and development speed.

## Language

Use Chinese for conversation, analysis, progress reports, execution summaries, test results, and project introduction documents.

Use English for technical documents, architecture documents, module documents, design documents, code comments, filenames, identifiers, test names, and CMake targets.

Keep commands and code blocks unchanged.

## Instruction order

Read the current user request first, then the root `AGENTS.md`, the nearest module-level `AGENTS.md`, `CMakePresets.json`, relevant source files, related tests, and required documentation.

Module-level rules may add local constraints but must not weaken data integrity, durability, concurrency, recovery, or portability requirements.

Report instruction conflicts instead of guessing.

## Build

All configuration and builds must use presets already defined in `CMakePresets.json`.

Linux:

```bash
cmake --preset debug-ninja-low-parallel
cmake --build --preset debug-ninja-low-parallel
```

Windows:

```powershell
cmake --preset windows
cmake --build --preset windows
```

Read `CMakePresets.json` before building.

Do not invent preset names or bypass the preset workflow.

Follow the C++ standard selected by the preset.

Do not upgrade the C++ standard, compiler requirement, dependency version, or build tool unless explicitly required.

Use existing project test commands or test presets.

For platform-sensitive changes, verify both Windows and Linux behavior when possible.

## Project structure

The root `AGENTS.md` stores only repository-wide rules, top-level architecture principles, and common workflows.

Do not maintain a large fixed module index in this file.

Use CodeGraph, the root `CMakeLists.txt`, `CMakePresets.json`, source code, and tests to determine the real project structure.

Do not infer business behavior only from directory names or class names.

Do not invent modules, layers, services, interfaces, or responsibilities that are not present in the code.

Project analysis should identify actual responsibilities such as external interfaces, block read/write paths, metadata, persistence, distributed coordination, replication, scheduling, recovery, platform adaptation, shared infrastructure, and tests.

## Module AGENTS.md

Each major module should maintain its own `AGENTS.md`.

A module document should briefly describe the module responsibility, current important directories and files, main business flow, important types and entry points, dependencies, dependents, data flow, invariants, threading rules, lifetime rules, persistence constraints, CMake targets, and tests.

Module documentation must describe the current implementation.

Do not copy all root rules into module documents.

Do not place temporary task requirements in long-term module documentation.

Update the module document when its structure, responsibility, public interface, or major flow changes.

## C++ resource rules

Use RAII for files, memory, locks, threads, sockets, operating-system handles, and temporary resources.

Avoid owning raw pointers.

Use `std::unique_ptr` for exclusive ownership.

Use `std::shared_ptr` only when shared ownership is necessary.

Non-owning pointers and references must have clear lifetimes.

Do not rely on multiple manual cleanup branches.

Do not introduce global mutable state without a real architectural requirement.

## Implementation priority

Choose solutions in this order: reuse existing project code, use the C++ standard library, use existing dependencies, use platform capabilities, then add the smallest new implementation.

Do not reimplement existing project or standard-library functionality.

Do not add dependencies when existing code is sufficient.

## Abstraction rules

Do not add abstractions that are not required by the current task.

Avoid unnecessary wrappers, managers, factories, adapters, interfaces, helpers, conversion layers, extension points, and inheritance hierarchies.

Do not create an interface for one implementation without a real replacement requirement.

Do not design for hypothetical future needs.

Do not use design patterns only for appearance.

Before adding a class, structure, interface, or inheritance relationship, verify that existing types, functions, or direct composition cannot satisfy the requirement.

Prefer direct, local, and minimal implementations.

## Structure conversion rules

Do not duplicate existing structures without a real semantic requirement.

Do not create intermediate structures with nearly identical fields.

Do not add mechanical field-copy conversions.

Use generated gRPC, Protobuf, or RPC structures directly when they already meet the requirement.

A separate structure is allowed only for validation boundaries, persistence compatibility, protocol versions, ABI isolation, security boundaries, platform isolation, or genuinely different invariants.

Explain why direct reuse is insufficient before adding a conversion layer.

## Header and source rules

`.h` files contain structure definitions, enum definitions, class definitions, inheritance relationships, public types, public interfaces, and function declarations.

`.cpp` files contain normal function implementations, business logic, I/O, persistence logic, networking logic, platform-specific logic, and file-local helpers.

Do not define new public structures, classes, or inheritance hierarchies in `.cpp` files.

Prefer modifying `.cpp` files.

Modify `.h` files only when a type, interface, function signature, inheritance relationship, or contract changes.

When modifying a header, explain why and identify the impact range.

File-local helpers should normally be functions in an anonymous namespace.

Do not create an internal class when a simple function is sufficient.

Templates, `inline`, `constexpr`, defaulted functions, required instantiation-point implementations, header-only code, and generated code may remain in headers.

## Minimal code

Use the smallest correct implementation that fully satisfies the requirement.

Prefer early returns, standard algorithms, existing utilities, direct expressions, simple control flow, and small local changes.

Do not compress unrelated operations into one line.

Do not use obscure expressions, unnecessary macros, code golf, or undefined behavior.

Do not shorten code by removing validation, error handling, synchronization, durability, recovery behavior, portability, readability, or tests.

The goal is maximum practical value with minimum necessary code.

## Change scope

Modify only files required by the current task.

Do not perform unrelated refactoring, formatting, renaming, file movement, directory reorganization, dependency upgrades, interface changes, or cleanup.

Do not change unrelated business behavior only to make compilation succeed.

Report unrelated problems instead of fixing them without authorization.

## Storage safety

Do not change disk layout, block size, block identifiers, metadata format, serialization format, network protocol, checksums, file layout, flush behavior, write atomicity, crash consistency, replication behavior, recovery behavior, or public API behavior without explicit requirements, compatibility analysis, and tests.

Persistence changes must consider existing data, format versions, migration, rollback, missing data, truncated data, corrupted data, interrupted writes, restart recovery, Windows behavior, and Linux behavior.

Durability operations must not silently degrade.

A required durability operation must not return success after performing no work.

If a platform cannot provide equivalent behavior, return an explicit error or document the weaker contract.

Do not silently ignore corrupted data.

## Concurrency and errors

For concurrent code, identify shared state, accessing threads, synchronization, lock scope, lock order, deadlock risk, data race risk, callback behavior, and object lifetime.

Prefer standard synchronization primitives and RAII lock management.

Do not introduce complex lock-free code without a demonstrated need and sufficient verification.

Do not swallow errors or exceptions.

Do not log an error and continue with unsafe behavior.

Destructors must not throw.

Do not change exception guarantees or `noexcept` without justification.

## Tools

At the beginning of repository analysis, use CodeGraph to understand project structure, module relationships, symbols, call chains, dependencies, and change impact.

Check CodeGraph availability with:

```bash
codegraph status
```

Use CodeGraph to reduce the search scope, then read the actual source code before making conclusions or edits.

Do not rely only on graph results or symbol names.

Use Ponytail while designing and writing code to avoid unnecessary abstractions, conversions, dependencies, duplication, and excessive output.

After code changes, run:

```text
@ponytail-review
```

Apply valid simplifications, then rebuild and retest when the review causes meaningful changes.

Ponytail does not replace builds, tests, correctness review, concurrency analysis, or persistence analysis.

## Workflow

Before editing: run `git status`, read the root and nearest module `AGENTS.md`, check CodeGraph, identify relevant modules and call chains, read implementations and tests, determine the exact requirement or root cause, and select the smallest correct solution.

During editing: modify only necessary files, reuse existing types and functions, avoid unnecessary structures and conversions, preserve C++14/C++17 compatibility, preserve Windows/Linux compatibility, and preserve storage and protocol contracts.

After editing: review `git diff` and `git status`, build with the platform preset, run relevant tests, check warnings, review cross-platform impact, check CodeGraph impact when needed, run `@ponytail-review`, remove unnecessary code, then rebuild and retest.

## Test output

Do not paste full test logs into chat.

Store full logs under a local path such as `tmp/test-logs/`.

For passing tests, report the command, `PASS`, and duration.

For failing tests, report the failed test, key assertion, failure category, final 50 log lines, and full log path.

Do not rerun tests only to produce display output.

## Git safety

Without explicit authorization, do not delete uncommitted changes, run destructive Git commands, switch branches, rewrite history, commit, push, force-push, delete tests, skip failing tests, or modify third-party and generated code.

Obtain explicit authorization before any operation that may cause data loss.

## Completion report

Report what changed, why it changed, important modified files, header or public interface changes, protocol or persistence impact, build preset used, tests executed, test results, compiler warnings, CodeGraph findings, Ponytail findings, unverified items, remaining risks, and module documentation updates.

Never claim that a build, test, analysis, or verification step was performed when it was not.
<!-- SPECKIT END -->
