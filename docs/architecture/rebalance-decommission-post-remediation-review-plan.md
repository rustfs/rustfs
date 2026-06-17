# Rebalance and Decommission Post-Remediation Review

> **Status:** Updated for branch `cxymds/rebalance-decommission-remediation`.
>
> **Scope:** Rebalance, decommission, shared data-movement helpers, source cleanup,
> delete-marker handling, lifecycle expiry, restart/cancel recovery, and CI coverage.
>
> **Conclusion:** The main code-level P1/P2 remediation set has been implemented on
> this branch. The remaining release gate is verification, not another known
> correctness rewrite: full CI must pass, `make pre-commit` must pass before the
> final PR state, and the data-movement e2e proof added to CI must complete in the
> GitHub runner environment.

## Review Corrections

The previous report needed two important corrections.

First, the R28 decommission permit issue is real but was overstated. The worker
permit was released before entry migration work, so `Workers` did not account for
entry lifetime and `wk.wait()` could not prove entry work had drained. However,
the current listing path awaits entry callbacks inline and bucket processing is
bounded, so the old "unbounded object movement" wording was not supported by the
code. The fix remains correct because permit lifetime now matches entry lifetime
and prevents future spawn-based regressions.

Second, SSE-C migration is related to raw data movement but has a distinct
failure mode. Before the raw migration path, SSE-C objects did not silently
corrupt through an empty `HeaderMap`; migration reads failed while resolving
SSE-C material because the customer SSE-C headers were missing. The raw internal
read path avoids requiring request customer headers for data movement.

## Implemented Remediation

| Area | Current Branch State | Representative Guards |
| --- | --- | --- |
| Raw migration reads | Rebalance and decommission use raw data-movement read options instead of normal transformed GET semantics. | `test_rebalance_object_migration_read_opts_are_raw_data_movement`, `test_decommission_object_migration_read_opts_are_raw_data_movement` |
| Raw write invariants | Data-movement writes preserve source version, ETag, tags, expires, part metadata, and checksums consistently. Single-part raw ETag validation no longer rejects preserved source ETags. | `test_data_movement_put_object_opts_preserves_version_and_etag`, `test_data_movement_opts_preserve_tags_and_expires`, `test_data_movement_single_part_raw_reader_does_not_validate_source_etag` |
| Terminal decommission recovery | Cancel/failed/complete paths cancel in-memory workers before terminal persistence, and terminal-save failure no longer leaves ghost cancelers. | decommission canceler unit tests in `crates/ecstore/src/pools.rs` |
| Rebalance operation identity | Rebalance merge/save/stop paths are id-aware and do not let stale operation state clobber a newer operation. | rebalance id-gate and merge tests |
| Start coordination | Rebalance and decommission starts cross-check persisted peer operation state before starting conflicting work. | start cross-check tests |
| Decommission worker accounting | Entry permits are held through entry completion. R28 is no longer a pending worker-accounting bug. | decommission worker permit tests |
| Rebalance rollback | Failed start rollback finalizes metadata instead of leaving `Started` plus `stopping=true` without a worker. | rollback tests |
| Decommission progress save | Periodic progress-save errors are best-effort; terminal saves remain strict. | progress-save tests |
| Queue semantics | Multi-pool decommission is queued, local-leader prefix scheduling is supported, completed restart is rejected, failed/canceled retry preserves bucket progress, and promoted queued pools are canceled if cancellation arrives before work starts. | queue/promotion/retry tests in `pools.rs` |
| Cancel/clear operations | Non-leader cancel intent is accepted and failed/canceled terminal decommission can be explicitly cleared. | remote cancel and clear tests |
| Resume equivalence | Delete-marker replication state, tiered-object metadata, multipart part numbers, tags, expires, and version counts are covered more strictly. | data-movement equivalence tests |
| Lifecycle expiry | Data movement no longer treats failed lifecycle expiry application as a successful skip. | `resolve_data_movement_lifecycle_expiry_result_rejects_apply_failure` |
| CI coverage | Existing RustFS e2e delete-marker migration proof is wired into the `e2e-tests` CI job with `--test-threads=1`, reusing the downloaded debug binary. | `.github/workflows/ci.yml` |

## Compatibility Decisions

The current RustFS decommission contract is no longer single-pool only. Multi-pool
requests are supported as queued operations on multi-pool deployments. The
current behavior is documented in
`docs/architecture/decommission-compatibility.md`.

Delete-marker behavior is intentionally characterized rather than guessed:

- a lone delete marker without replication is cleanup-only metadata and can be
  skipped;
- delete markers with replication metadata remain eligible for movement;
- rebalance and decommission share the same predicate.

Lifecycle-expired versions are also explicit:

- decommission can count safely expired versions toward source cleanup only when
  lifecycle expiry application succeeds;
- rebalance remains stricter and requires actual data-movement completion for
  cleanup.

## Remaining Risks and Gates

There are no remaining known P1/P2 code changes in this remediation set, but the
branch is not final-release-ready until verification completes:

1. `make pre-commit` must pass before the PR is marked ready.
2. GitHub CI must pass, including the newly wired delete-marker migration e2e
   proof.
3. Local full e2e verification was attempted but the machine ran out of disk
   space while building `rustfs`; this is an environment blocker, not a test
   assertion failure. The `target/` directory was cleaned afterward.
4. If product requires stronger end-to-end proof for encrypted and compressed
   migration, add those scenarios to `crates/e2e_test` before release. Current
   branch coverage is strongest at the internal option/invariant layer, with CI
   e2e coverage focused on versioning/delete-marker semantics.

## Focused Verification Run During Implementation

Focused checks were run per task, including:

```bash
cargo test -p rustfs-ecstore data_movement_single_part_raw_reader --lib
cargo test -p rustfs-ecstore test_merge_rebalance_meta_preserves_stopping_stop_snapshot --lib
cargo test -p rustfs-ecstore test_merge_rebalance_pool_stats_clears_stopping_for_terminal_status --lib
cargo test -p rustfs-ecstore test_first_resumable_decommission_queue_indices_skips_terminal_states --lib
cargo test -p rustfs-ecstore test_return_resumable_pools_skips_failed_decommission --lib
cargo test -p rustfs-ecstore resolve_data_movement_lifecycle_expiry_result --lib
cargo test -p rustfs-ecstore lifecycle_action_removes_data_movement_version --lib
cargo test -p rustfs-ecstore test_pool_meta_promoted_queued_decommission_can_be_canceled --lib
cargo fmt --all --check
```

The attempted local e2e command was:

```bash
cargo test -p e2e_test delete_marker_migration_semantics -- --nocapture
```

It failed because the local filesystem reached `No space left on device` while
building the RustFS debug binary. CI now runs the proof in the e2e job after the
debug binary artifact has been downloaded, and uses `--test-threads=1` to avoid
parallel test-server startup for that proof.

## Final PR Gate

Before marking the draft PR ready:

```bash
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

If CI reports failures after the branch is pushed, inspect the failing job logs
instead of weakening the e2e gate. The delete-marker migration proof was added
because skipping it would leave the exact versioning regression class invisible
to CI.
