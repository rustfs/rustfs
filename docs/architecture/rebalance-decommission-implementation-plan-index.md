# Rebalance and Decommission Implementation Plan Index

> This index is based on `docs/architecture/rebalance-decommission-remediation-plan.md`. The remediation scope is intentionally split into smaller implementation plans because the fixes touch independent risk areas: object-version safety, distributed operation semantics, data movement internals, and operational hardening.

## Why Split the Work

The remediation backlog has fourteen fix blocks. Implementing them in one PR would make review risky and would mix unrelated failure modes. The safer path is:

1. Fix the data semantics that can lose or corrupt object-version meaning.
2. Fix distributed start/stop/recovery semantics so operators can trust cluster state.
3. Fix resource and metadata correctness in shared data movement.
4. Add observability and compatibility hardening.

Each plan below should be reviewed and executed independently unless the plan explicitly says two fixes must share the same implementation.

## Plan Set

| Plan | Fixes | Status | Purpose |
| --- | --- | --- | --- |
| `rebalance-decommission-phase1-safety-plan.md` | F01, F02, F03, F04, F05 | Drafted | Protect object-version semantics and cluster operation safety |
| `rebalance-decommission-phase2-data-movement-plan.md` | F06, F07, F08, F09, F10 | Drafted | Stream multipart migration, preserve metadata, and improve convergence |
| `rebalance-decommission-phase3-hardening-plan.md` | F11, F12, F13, F14 | Drafted | Improve cleanup reporting, auditability, metadata decoding, and threshold docs |

## Execution Recommendation

Start with `rebalance-decommission-phase1-safety-plan.md`. Phase 1 contains the highest-risk issues and defines semantics that later fixes depend on.

Within Phase 1:

1. F01 should be analyzed first because rebalance delete marker and remote tiered behavior determines whether RustFS should move or skip these versions.
2. F02 can be implemented independently once the expected decommission overwrite semantics are confirmed.
3. F03, F04, and F05 can be designed together but should remain separate PRs unless a shared peer-failure helper is introduced.

For a long-running implementation task, use the phase plans as checkpoints:

1. Implement one fix block at a time.
2. Run the focused tests listed in that block.
3. Review the diff before moving to the next fix block.
4. Run the phase-level test matrix before considering the phase complete.

## Upstream Change Impact Notes

### 2026-06-17: `ed55857b refactor: move bucket operations contract (#3507)`

This upstream change moved `BucketOperations` from `crates/ecstore/src/store_api/traits.rs` into `crates/storage-api/src/bucket.rs` and re-exported it from `rustfs_storage_api`.

Impact on this remediation plan:

- No F01-F14 risk item is fixed by this upstream change.
- No planned fix block needs priority changes because of this upstream change.
- Implementation code that imports or bounds `BucketOperations` must now use `rustfs_storage_api::BucketOperations`.
- Generic implementations that need the ECStore error type should use an explicit associated error bound such as `BucketOperations<Error = crate::error::Error>`.
- The only touched planning-relevant files are import/boundary changes in paths such as `crates/ecstore/src/pools.rs`, `crates/ecstore/src/store.rs`, `crates/ecstore/src/set_disk.rs`, and `rustfs/src/admin/handlers/rebalance.rs`; the rebalance/decommission safety logic remains unchanged.

## Review Gates Before Implementation

Before any code patch starts for a fix block:

- Confirm the selected behavior in the corresponding plan.
- Identify the minimal set of files for that fix.
- Write or update failing tests first.
- Keep unrelated refactors out of scope.
- Run focused tests for the touched crate before broader checks.

## Verification Baseline

For any code PR generated from these plans:

- Run focused `cargo test` commands for touched crates and modules.
- Run `cargo fmt --all`.
- Run `cargo fmt --all --check`.
- Run `make pre-commit` before opening a PR when the implementation is ready.
- Clean generated build artifacts after build-based verification.
