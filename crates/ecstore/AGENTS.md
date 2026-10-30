# ECStore Crate Instructions

Applies to `crates/ecstore/`.

## Data Durability and Integrity

- Do not weaken quorum checks, bitrot checks, or metadata validation paths.
- Treat any change affecting read/write/repair correctness as high risk and test accordingly.
- Prefer explicit failure over silent data corruption or implicit success.

## Performance-Critical Paths

- Be careful with allocations and locking in hot paths.
- Keep network and disk operations async-friendly; avoid introducing unnecessary blocking.
- Coalesce related `open`, `read`/`write`, `fsync`, and `close` filesystem steps into a single per-disk blocking closure rather than chaining multiple async or blocking dispatches, and route any closure that performs file or directory sync to the disk's dedicated fsync lane so stalled syncs cannot starve foreground metadata reads.
- Pin a parent directory once (`openat2(BENEATH | NO_SYMLINKS)` on Linux) and reuse the pinned handle across sibling reads or same-directory renames instead of re-walking ancestor components per file.
- For small metadata reads with a known size, pre-size buffers when it avoids repeated growth. Preserve the format's size limits, checked length conversions, and overflow handling; a filesystem size hint does not establish that an allocation is safe or that the file cannot change.
- Benchmark-sensitive changes should include measurable rationale.

## Allocation Discipline in Hot Paths

- Before cloning in a loop or per-request path, consider the data copied and whether borrowing or moving ownership fits. Use `Arc` when shared ownership avoids meaningful copies and its lifetime and synchronization costs fit the operation.
- Borrow temporary strings when lifetimes permit; choose `String` or `Cow<str>` when ownership or mixed borrowed/owned data requires it.
- Preallocate collections when the size estimate is useful and bounded; do not reserve directly from unchecked input or speculative worst-case sizes.

## Lock Ordering

- When a function acquires multiple locks, document the acquisition order in a comment.
- Never acquire the same set of locks in different orders across code paths — this is a deadlock.
- Use atomic `fetch_*` operations for unconditional counter updates and `compare_exchange` loops for conditional updates. A separate load followed by store must not lose concurrent updates.

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

- `cargo nextest run -p rustfs-ecstore --no-tests fail`, narrowed to affected tests and features using [the testing guide](../../docs/testing/README.md).
