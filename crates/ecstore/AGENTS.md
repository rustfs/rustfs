# ECStore Crate Instructions

Applies to `crates/ecstore/`.

## Data Durability and Integrity

- Do not weaken quorum checks, bitrot checks, or metadata validation paths.
- Treat any change affecting read/write/repair correctness as high risk and test accordingly.
- Prefer explicit failure over silent data corruption or implicit success.

## Performance-Critical Paths

- Be careful with allocations and locking in hot paths.
- Keep network and disk operations async-friendly; avoid introducing unnecessary blocking.
- Benchmark-sensitive changes should include measurable rationale.

## Allocation Discipline in Hot Paths

- Do not implement `Clone` on structs with >5 heap-allocated fields without considering `Arc` for heavy fields.
- Before cloning a struct in a loop or per-request path, check if a reference or `Cow` would suffice.
- When a struct contains a large buffer (e.g., erasure coding block), wrap it in `Arc` and pass by reference rather than cloning.
- Use `&str` and `Cow<str>` instead of `String` for temporary computations (header parsing, signature building, path manipulation).
- Use `HashMap::with_capacity()` / `Vec::with_capacity()` when the size is known or estimable.

## Lock Ordering

- When a function acquires multiple locks, document the acquisition order in a comment.
- Never acquire the same set of locks in different orders across code paths — this is a deadlock.
- Prefer `compare_exchange` loops over load-then-store for concurrent counters.

## Recursion Safety

- Recursive tree traversals (cache trees, directory walks) must have a depth limit or use iterative traversal with an explicit `Vec` stack.
- `flatten()`, `delete_recursive()`, `copy_with_children()`, `total_children_rec()`, `mark()` — all must handle deep or corrupted trees safely.

## Dead Code

- Do not add `#![allow(dead_code)]` at the crate root. If code is unused, remove it or gate it behind a feature flag.
- Each `#[allow(dead_code)]` annotation must have a comment explaining why the code is kept.

## Cross-Module Coordination

- Validate behavior impacts on:
  - `rustfs/src/storage/`
  - `crates/filemeta/`
  - `crates/heal/`
  - `crates/checksums/`

## Suggested Validation

- `cargo test -p rustfs-ecstore`
- Full gate before commit: `make pre-commit`
