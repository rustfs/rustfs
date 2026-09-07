# Implementation Rules

Applies when changing code or running artifact-heavy work. Paths below
are repository-relative. Read only the relevant sections during read-only review.

## Worktree and Disk Hygiene

- Start implementation from the latest `origin/main` and confirm the requested
  change is not already present.
- An existing clean, isolated task worktree is sufficient. Create another
  worktree only when the current checkout is shared, dirty with unrelated work,
  or belongs to another task.
- Never commit from a shared checkout.
- Use a task-specific branch named `<type>/<topic>`, such as `fix/...`,
  `feat/...`, `test/...`, or `docs/...`, unless the user specifies a name.
- Do not include agent, tool, contributor, account, or organization names in
  branch names.
- Push to the user-requested remote or the repository's configured push remote.
  Do not hard-code or infer a remote from an account name.
- Check free space before artifact-heavy builds, tests, coverage, or downloads.
  Re-check before a broad gate when space is tight.
- Remove only task-owned temporary/build artifacts. Never delete another task's
  worktree or uncommitted data.
- At handoff, mention disk or cleanup details only when they affected execution
  or artifacts/worktrees remain intentionally.

## Change Style

- Preserve existing control flow unless changing it is required for correctness.
- Prefer a direct local edit over new files, wrappers, managers, or speculative
  abstractions.
- Add a helper only when it removes current duplication, names a real domain
  boundary, or isolates a non-trivial invariant.
- Remove an in-scope path superseded by the change. If compatibility requires it,
  adapt at the boundary to one canonical core and use the repository's
  `RUSTFS_COMPAT_TODO` policy.
- Comments explain non-obvious invariants or reasons. Do not narrate code or
  record change history.
- Mention unrelated problems when useful; do not fix them in a narrow task.

## Reuse and Boundary Rules

- Before adding helpers, constants, fixtures, or wrappers, search the touched
  crate, the domain-owning crate, `crates/utils`, `crates/common`, and relevant
  direct dependencies.
- Reuse requires matching semantics: normalization, error types, deadlines,
  durability, and compatibility must fit the call site. A narrowly named local
  helper is better than forced reuse with different semantics.
- Validate untrusted input at its trust boundary, then trust the validated type.
  Values crossing disk, RPC, persistence, or version boundaries remain
  untrusted at every consumer.
- Re-check boundary values immediately before destructive actions such as
  delete, overwrite, or quorum decisions.
- Every new branch needs a concrete triggering input/state. For decoded or peer
  data, corruption and mixed-version input are valid triggers.
- Required values must return a typed error when absent or corrupt; do not use a
  default that converts corruption into a plausible result.
- Attach error context once where it is actionable. Do not erase typed errors
  below aggregation or quorum layers.

## Naming

Use Rust API naming: `SCREAMING_SNAKE_CASE` constants/statics, `snake_case`
functions/variables, and `PascalCase` types. Do not rename unrelated existing
violations.
