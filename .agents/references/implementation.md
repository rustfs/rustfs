# Implementation Rules

These implementation, style, and quality rules apply to all contributions, regardless of author or editor. Paths below are repository-relative. Read only the relevant sections during read-only review; select verification from the root `AGENTS.md`.

## Shared Style Contract

- Use the toolchain declared in `rust-toolchain.toml`, respect the minimum supported Rust version in `Cargo.toml`, and let `rustfmt.toml` and the configured Rust/Clippy lints define mechanical style. Do not override repository settings with personal formatter or lint preferences.
- Follow the owning module's organization, imports, and domain terminology where repository rules leave a choice. Existing violations do not justify new ones; avoid unrelated cleanup in a focused change.
- Rules using "must" or "do not" are requirements. Rules using "prefer" are defaults; depart when correctness, ownership, performance, or readability benefits justify it. Explain consequential tradeoffs, not every idiomatic choice. Where several approaches meet the contract equally well, the implementer chooses.
- Follow the declared toolchain locally and the affected CI lane's toolchain for reproduction. The repository currently tracks `stable`; for a formatter/lint mismatch, compare `rustc -Vv`, `cargo fmt --version`, and `cargo clippy --version` with the failing run before changing code. Toolchain upgrades must keep local and CI settings aligned and pass their affected gates; do not silently raise `rust-version` or change lint policy to make a check pass.

## Worktree and Disk Hygiene

- For a new task, start from the latest `origin/main` unless a different base is requested, and confirm the requested change is not already present. When continuing existing work or a PR, preserve its branch, base, and task-owned changes; do not restart it from `main`.
- An existing clean, isolated task worktree is sufficient. Create another worktree only when the current checkout is shared, dirty with unrelated work, or belongs to another task.
- Never commit from a shared checkout.
- Use a task-specific branch named `<type>/<topic>`, such as `fix/...`, `feat/...`, `test/...`, or `docs/...`, unless the user specifies a name.
- Do not include agent, tool, contributor, account, or organization names in branch names.
- Push to the user-requested remote or the repository's configured push remote. Do not hard-code or infer a remote from an account name.
- Check free space before artifact-heavy builds, tests, coverage, or downloads. Re-check before a broad gate when space is tight.
- Remove only task-owned temporary/build artifacts. Never delete another task's worktree or uncommitted data.
- At handoff, mention disk or cleanup details only when they affected execution or artifacts/worktrees remain intentionally.
- For Docker verification, ensure the tested image contains the intended binary. `buildx build` without `--load` leaves the image in the build cache; use the Makefile's loaded image and rebuild without cache only when stale artifacts are evidenced.

## Change Style

- Preserve unrelated control flow. Restructure touched code only when needed for the requested behavior or an explicitly requested refactor; style preferences alone do not justify changing execution order or cleanup behavior.
- Prefer a direct local edit over new files, wrappers, managers, or speculative abstractions.
- Add a helper only when it removes current duplication, names a real domain boundary, or isolates a non-trivial invariant. Keep helpers near the code they support when the module's organization permits it.
- Remove an in-scope path superseded by the change. If compatibility requires it, adapt at the boundary to one canonical core and use the repository's `RUSTFS_COMPAT_TODO` policy.
- Comments explain non-obvious invariants or reasons. Do not narrate code or record change history.
- Mention unrelated problems when useful; do not fix them in a narrow task.

## Rust Style

These repository-owned rules incorporate selected ideas from the [OpenCode style guide](https://github.com/anomalyco/opencode/blob/dev/AGENTS.md#style-guide), adapted to RustFS. That link is background, not an additional rule source. Apply these defaults to new or changed code; correctness and domain invariants take priority.

- Default to immutable `let` bindings; use `mut` for actual state changes. Prefer `if` or `match` expressions when branches compute a value instead of initializing a placeholder and reassigning it.
- Use type inference for obvious local values. Add annotations when inference needs help or the numeric, collection, or error type matters to understanding the code. Keep explicit types in function signatures and public contracts.
- Keep the main path readable with guard clauses, `let ... else`, and `?` when immediate propagation is correct. Preserve explicit handling for recovery, cleanup, aggregation, and quorum decisions; `if`/`else` expressions and exhaustive `match` remain idiomatic.
- Prefer readable iterator chains for straightforward mapping and filtering. Keep loops when they make state changes, early exits, or async operations clearer. Do not add intermediate collections or clones just to use iterators.
- Inline trivial single-use expressions only when a binding adds no meaning. Keep names that explain domain concepts, aid debugging, or make ownership and guard lifetimes clear. Do not change lock release or drop timing for style.
- Keep pure parsing, validation, and option-building helpers synchronous unless an async interface contract requires otherwise. This does not make blocking I/O or CPU-heavy work safe to run on async executor threads; offload it when needed to keep those threads responsive.

## Reuse and Boundary Rules

- Before adding helpers, constants, fixtures, or wrappers, search the touched code and domain owner; check `crates/utils`, `crates/common`, and direct dependencies when they could own the behavior. Stop once the relevant reuse boundary is clear.
- Reuse requires matching semantics: normalization, error types, deadlines, durability, and compatibility must fit the call site. A narrowly named local helper is better than forced reuse with different semantics.
- Validate untrusted input at the boundary that owns it, then pass validated types inward without repeating checks whose guarantees still hold. Deserializing into a Rust type alone does not establish domain validity. Values newly read from disk, RPC, persistence, or another version must be validated at that consuming boundary.
- For destructive actions such as delete or overwrite and for quorum decisions, check the relevant identity, generation, and authority under the operation's existing synchronization or precondition contract. A separate unguarded recheck does not prevent a check/use race.
- Every new control-flow branch needs a concrete triggering input/state. For decoded or peer data, corruption and mixed-version input are valid triggers.
- Required values must return a typed error when absent or corrupt; do not use a default that converts corruption into a plausible result.
- Attach error context once where it is actionable. Do not erase typed errors below aggregation or quorum layers.

## Rust Boundaries

- Production `unwrap`/`expect` must follow a type guarantee or checked invariant; explain only non-obvious guarantees. Stricter scoped prohibitions, including `crates/AGENTS.md`, still apply. Invalid client, disk, or peer data must return errors rather than trigger panics.
- Public APIs do not return `Result<_, String>`. Library APIs use domain errors unless intentional erasure is part of the boundary contract; expose a stored inner error through `Error::source()`.
- Numeric conversions must not silently truncate. Use fallible conversions for untrusted values; validate finiteness, sign, and range before float-to-integer conversion. Clamp/saturate only when required by the domain.
- Do not add crate-root `#![allow(dead_code)]`.

## Naming

Use Rust API naming: `SCREAMING_SNAKE_CASE` constants/statics, `snake_case` functions/variables/modules, and `PascalCase` types/traits. Name domain concepts and include units where a value would otherwise be ambiguous; do not optimize for short names. Do not rename public, serialized, or persisted fields merely to match a local style preference, or rename unrelated existing violations.

## API and Operational Contracts

- For new or changed public APIs, document the caller-visible behavior and relevant `# Errors`, `# Panics`, and `# Safety` conditions. Include a useful example or link to one when usage is non-obvious; avoid boilerplate sections that say nothing. Follow the [Rust API documentation guidelines](https://rust-lang.github.io/api-guidelines/documentation.html) within the scope of the change.
- For async or storage APIs, state cancellation, retry/idempotency, partial-completion, ownership, and durability guarantees when callers rely on them. Make failure cleanup and commit points explicit in code and tests; do not imply atomicity that the implementation does not provide.
- For protocol, persisted-format, configuration, dependency, or operational changes, check affected consumers and supported versions. Update relevant docs/examples and describe compatibility, upgrade/rollback limits, and new configuration defaults when they change. New dependencies must solve a current need and fit the workspace's license, toolchain, and feature policy.
- For hot-path or resource-management changes, check allocation/copying, blocking, unbounded work, and lock duration where affected. Support performance claims with an appropriate measurement or clearly label them as expectations; a benchmark is not required for unrelated changes.

## Behavioral Tests

- For behavior changes, add or update the narrowest test that calls the real implementation and asserts an observable result. Existing tests can provide coverage when they directly exercise the changed behavior; do not duplicate production logic as the test oracle.
- Cover the changed contract's relevant failure and boundary cases, not only success. A bug regression should fail without the fix; do not broaden assertions or rely only on a successful call when the result or side effect is what matters.
- Follow `docs/testing/README.md` for test layers, runners, isolation, synchronization, and flaky tests. Use controlled clocks or explicit synchronization for time/concurrency tests instead of fixed sleeps, and keep waits bounded.
- Report the checks actually run and their outcomes, including skipped or blocked checks and the behavior they leave unverified. Do not describe focused local checks as full CI coverage.
