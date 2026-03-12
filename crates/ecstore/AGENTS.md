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

## Cross-Module Coordination

- Validate behavior impacts on:
  - `rustfs/src/storage/`
  - `crates/filemeta/`
  - `crates/heal/`
  - `crates/checksums/`

## Suggested Validation

- `cargo test -p rustfs-ecstore`
- Full gate before commit: `make pre-commit`
