# Rebalance and Decommission Follow-up Review Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining gaps found during the post-implementation review of F01-F14.

**Architecture:** This plan treats the original F01-F14 work as the baseline and adds focused follow-up tasks with new `Rxx` identifiers. Each task is independently reviewable and should be committed separately. The plan favors fail-closed behavior for state propagation and strict equivalence for data movement.

**Tech Stack:** Rust, Tokio, ECStore, admin handlers, tonic peer RPC, MessagePack metadata, `tracing`, existing crate-local unit tests.

---

## Scope

This plan covers only the gaps found during the follow-up review:

- P1 gaps that can leave cluster state inconsistent or object metadata incomplete.
- P1 async-lock gaps that can block pool routing, status reads, and decommission progress while disk or peer operations are pending.
- P2 gaps where the implementation is safer than before but does not fully meet the written acceptance criteria.
- One P3 test-depth gap for startup recovery.
- One P3 MinIO compatibility gap where RustFS is intentionally more conservative today but the product contract is not yet explicit.

The following original items had no new material finding in this review and do not need a follow-up task here:

- F02: decommission unsafe overwrite cleanup safety.
- F06: multipart streaming memory behavior.
- F10: source cleanup preflight placement and not-found idempotence.
- F14: MinIO-like rebalance completion tolerance.

## Validated Additional Findings

The following externally reported issues were checked against the current code and are treated as real planning items:

- Decommission start still writes active local `pool_meta` before peer `reload_pool_meta()`. If reload fails, the admin handler returns an error before worker spawning, leaving an active decommission marker that can suspend the source pool without a running worker. Covered by upgraded R06.
- Rebalance start still performs `check -> init_rebalance_meta -> start_rebalance -> peer load_rebalance_meta(true)` without one atomic start guard. Peer failure is covered by R01; concurrent start identity races are covered by new R13.
- Multiple decommission paths still hold `pool_meta.write()` across `save(...).await`, including start, cancel, failed, complete, bucket-done, and progress-save paths. Covered by new R12.
- Last-delete-marker skip behavior exists in both decommission and rebalance and has helper-level tests, but lacks MinIO-compatible version-listing regression coverage after migration and cleanup. Covered by new R14.
- The admin handler accepts comma-separated decommission targets, but store validation rejects more than one target pool. This is conservative, but the MinIO compatibility contract is not documented or implemented. Covered by new R15.

## Execution Order

| Order | Task | Original Fix | Priority | Main Risk |
| --- | --- | --- | --- | --- |
| 1 | R01 | F05 | P1 | Admin rebalance start can fail while local worker keeps running |
| 2 | R06 | F03 | P1 | Failed decommission reload can leave active metadata without a worker |
| 3 | R12 | F03 | P1 | Pool metadata write lock is held across async saves |
| 4 | R02 | F07 | P1 | Multipart migration can drop per-part checksum metadata |
| 5 | R03 | F09 | P1 | Overwrite convergence can accept incomplete target metadata |
| 6 | R04 | F11 | P1 | Cleanup warning metadata can become self-inconsistent and fail decode |
| 7 | R13 | F05 | P2 | Concurrent rebalance starts can return stale operation IDs |
| 8 | R05 | F01 | P2 | Rebalance delete marker target write can stall when default placement selects source |
| 9 | R07 | F05 | P2 | Stop status can report stopped while workers are still winding down |
| 10 | R08 | F08/F10 | P2 | Cleanup preflight can reject safely expired versions already removed by lifecycle |
| 11 | R09 | F12 | P2 | Decommission rejected-request logs lack complete audit context |
| 12 | R10 | F13 | P2 | Legacy pool metadata fallback can bypass unknown-field hardening |
| 13 | R14 | F01 | P2 | Last delete marker migration semantics lack MinIO-compatible E2E proof |
| 14 | R11 | F04 | P3 | Store init recovery lacks full integration-level regression coverage |
| 15 | R15 | Compatibility | P3 | Multi-pool decommission support is accepted by API shape but rejected by store |

## Shared Rules

- Read this file before starting each task and restate the selected task in the commit summary or work log.
- Implement exactly one `Rxx` task per commit.
- Keep unrelated refactors out of scope.
- Preserve the existing code shape unless a small helper is needed for the task.
- For Rust changes, run the focused tests listed under the task plus `cargo fmt --all --check`.
- Leave unrelated untracked files, such as `CLAUDE-FABLE-5.md`, untouched.

---

## R01: Roll Back Local Rebalance Start on Propagation Failure

### Original Fix

F05: Rebalance distributed start/stop semantics.

### Finding

`rustfs/src/admin/handlers/rebalance.rs` starts the local rebalance before peer propagation. If `notification_sys.load_rebalance_meta(true)` fails, the admin API returns an error, but the local worker may continue running.

### Files

- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Modify if needed: `crates/ecstore/src/rebalance.rs`
- Test: `rustfs/src/admin/handlers/rebalance.rs`
- Test if needed: `crates/ecstore/src/rebalance.rs`

### Design

On peer propagation failure after local start:

1. Attempt local `store.stop_rebalance().await`.
2. Persist stopped metadata through the same path used by explicit stop.
3. Return an admin error that includes both propagation failure and rollback failure if rollback fails.
4. Log `result = "rollback_success"` or `result = "rollback_failed"` with `request_id`, masked `actor`, `remote_addr`, and `rebalance_id`.

Do not silently convert propagation failure into success.

### Implementation Steps

- [ ] Add a helper in `rustfs/src/admin/handlers/rebalance.rs` such as `rollback_local_rebalance_start(store, rebalance_id)` that calls `stop_rebalance()` and persists stopped metadata if required by the current code path.
- [ ] Add a unit test for formatting rollback failure context so an error includes both `failed to propagate rebalance start` and `failed to roll back local rebalance start`.
- [ ] Add an admin handler or helper-level test that simulates propagation failure after local start and verifies rollback is attempted.
- [ ] Update the `RebalanceStart` error path to call the rollback helper before returning.
- [ ] Run:

```bash
cargo test -p rustfs rebalance --lib
cargo test -p rustfs-ecstore stop_rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Admin start does not leave local rebalance running after peer propagation failure.
- Rollback failure is visible in the returned error and logs.
- Existing successful start behavior is unchanged.

### Commit

```bash
git add rustfs/src/admin/handlers/rebalance.rs crates/ecstore/src/rebalance.rs
git commit -m "fix(rebalance): roll back failed start propagation"
```

---

## R02: Preserve Multipart Per-part Checksums in Completed Metadata

### Original Fix

F07: Preserve and verify full data movement metadata.

### Finding

`data_movement_complete_part()` fills checksum fields on `CompletePart`, but `complete_multipart_upload` rebuilds `ObjectPartInfo` without copying `ext_part.checksums`, so migrated multipart objects can lose per-part checksum metadata.

### Files

- Modify: `crates/ecstore/src/set_disk.rs`
- Test: `crates/ecstore/src/data_movement.rs`
- Test if needed: `crates/ecstore/src/set_disk.rs`

### Design

Preserve per-part checksums when completing multipart upload:

- Copy `ext_part.checksums.clone()` into the new `ObjectPartInfo`.
- Add a test that exercises the actual complete path, not only `CompletePart` construction.
- Ensure the test verifies the resulting target part metadata contains the original checksum map.

### Implementation Steps

- [ ] Add a failing test that builds an uploaded multipart part with checksums and completes it through the same code that pushes `ObjectPartInfo`.
- [ ] Update the `ObjectPartInfo` construction in `crates/ecstore/src/set_disk.rs` to include:

```rust
checksums: ext_part.checksums.clone(),
```

- [ ] Add or update a data movement test that migrates a multipart object and asserts target parts preserve checksum entries.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore multipart --lib
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore checksum --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Migrated multipart target metadata contains source per-part checksums.
- Existing multipart checksum validation still rejects mismatched checksums.
- No source cleanup path can remove the only copy of per-part checksum metadata.

### Commit

```bash
git add crates/ecstore/src/set_disk.rs crates/ecstore/src/data_movement.rs
git commit -m "fix(data-movement): preserve part checksums"
```

---

## R03: Strengthen Overwrite Equivalence for Metadata-complete Targets

### Original Fix

F09: Rebalance overwrite race equivalence.

### Finding

`is_equivalent_data_movement_object()` compares core object fields but does not compare multipart part metadata/checksums or replication/version purge state. A target missing required metadata can be treated as equivalent and allow source cleanup.

### Files

- Modify: `crates/ecstore/src/data_movement.rs`
- Test: `crates/ecstore/src/data_movement.rs`
- Test: `crates/ecstore/src/rebalance.rs`

### Design

Extend overwrite equivalence to match the fields required by F07:

- version ID
- delete marker state
- size and actual size
- ETag
- checksum
- mod time
- storage class
- user-defined metadata
- replication status/internal state
- version purge status/internal state
- multipart part count and per-part number, ETag, size, actual size, mod time, index, and checksums

Use exact equality for these fields. A false negative is safer than a false positive.

### Implementation Steps

- [ ] Add a helper such as `is_equivalent_data_movement_part(source, target)` in `crates/ecstore/src/data_movement.rs`.
- [ ] Update `is_equivalent_data_movement_object()` to compare replication/version purge fields and call the part helper.
- [ ] Add a failing test where source and target differ only by missing per-part checksum and assert overwrite convergence is rejected.
- [ ] Add a failing test where source and target differ only by version purge status and assert overwrite convergence is rejected.
- [ ] Add a positive test where all fields match and overwrite convergence is accepted.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore DataMovementOverwriteErr --lib
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Equivalent overwrite accepts only metadata-complete targets.
- Missing multipart checksum or replication/version purge state blocks convergence.
- Decommission remains fail-closed for unsafe overwrite.

### Commit

```bash
git add crates/ecstore/src/data_movement.rs crates/ecstore/src/rebalance.rs
git commit -m "fix(data-movement): require full overwrite equivalence"
```

---

## R04: Keep Cleanup Warning Merge Metadata Self-consistent

### Original Fix

F11: Rebalance cleanup failure reporting.

### Finding

`merge_rebalance_cleanup_warnings()` uses `max(count)` but merges and de-duplicates entries. Two nodes with different warning entries and count `1` can produce `count = 1` and `entries.len() = 2`, which violates the F13 decode validation.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Test: `crates/ecstore/src/rebalance.rs`

### Design

After merging entries:

- Preserve total warning count semantics as much as possible.
- Guarantee `count >= entries.len()` before metadata can be saved.
- Keep bounded entries at `REBALANCE_CLEANUP_WARNING_ENTRY_LIMIT`.

Minimal safe rule:

```rust
remote.count = remote.count.max(local.count);
merge_rebalance_cleanup_warning_entries(&mut remote.entries, &local.entries);
remote.count = remote.count.max(remote.entries.len() as u64);
```

If avoiding `as` casts, use `u64::try_from(remote.entries.len()).unwrap_or(u64::MAX)` or an error-returning helper if the function becomes fallible.

### Implementation Steps

- [ ] Add a failing test where remote has one warning entry, local has a different warning entry, both have count `1`, and merge produces `count >= 2`.
- [ ] Add a decode test that serializes the merged metadata and decodes it through `RebalanceMeta::decode_rebalance_meta_payload`.
- [ ] Update `merge_rebalance_cleanup_warnings()` to normalize count after merging entries.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore cleanup_warning --lib
cargo test -p rustfs-ecstore rebalance_meta --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Merged cleanup warnings never violate metadata validation.
- Count remains at least the number of retained entries.
- Entry retention remains bounded.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs
git commit -m "fix(rebalance): normalize cleanup warning counts"
```

---

## R05: Force Rebalance Delete Marker Writes to a Non-source Target

### Original Fix

F01: Rebalance delete marker and remote tiered version safety.

### Finding

Rebalance delete marker movement routes through `ECStore::delete_object()`. If default placement resolves to the source pool, the data movement path may return `DataMovementOverwriteErr` rather than forcing the known non-source fallback target.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify if needed: `crates/ecstore/src/store/object.rs`
- Test: `crates/ecstore/src/rebalance.rs`

### Design

The rebalance-specific delete marker path should:

1. Exclude `src_pool_idx` from target selection.
2. Write the delete marker metadata to the selected non-source target.
3. Treat overwrite as complete only after strict target equivalence is proven.
4. Return failure if no non-source target is available.

### Implementation Steps

- [ ] Add a failing test where default placement selects the source pool but another target pool is available; rebalance delete marker migration must still write to the non-source target.
- [ ] Refactor `rebalance_delete_marker()` to choose or pass a concrete non-source target pool.
- [ ] Preserve version ID, delete marker flag, mod time, replication delete marker state, and `src_pool_idx`.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore rebalance_delete_marker --lib
cargo test -p rustfs-ecstore rebalance_entry --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Rebalance delete markers are written to a non-source target when one exists.
- Source cleanup remains blocked on target write failure.
- Remote tiered versions remain skipped without cleanup permission.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs crates/ecstore/src/store/object.rs
git commit -m "fix(rebalance): target delete marker movement"
```

---

## R06: Prevent Active-but-no-worker Decommission Starts

### Original Fix

F03: Decommission pool meta reload barrier.

### Finding

`start_decommission()` saves active `pool_meta` before peer reload. Reload failure returns an API error before the admin handler reaches `spawn_decommission_routines`, but the local pool already has active decommission metadata. That state can make `is_suspended()` exclude the pool and `is_decommission_running()` block rebalance while no local decommission worker is moving data until restart recovery happens.

### Files

- Modify: `crates/ecstore/src/pools.rs`
- Modify: `crates/ecstore/src/store/init.rs`
- Modify if status fields are added: `rustfs/src/admin/handlers/pools.rs`
- Test: `crates/ecstore/src/pools.rs`
- Test: `crates/ecstore/src/store/init.rs`

### Design Options

Choose one implementation before coding:

1. **Rollback on reload failure.**
   - Revert the just-written decommission state and save pool meta again.
   - Safer for user-visible API semantics, but needs careful persistence handling.
   - Do not use cancel semantics if cancel means "decommissioned pool remains out of ordinary placement" in MinIO-compatible behavior.

2. **Persist degraded state.**
   - Add a durable `reload_failed` or `start_degraded` marker to decommission info.
   - Admin status exposes the marker.
   - Store init refuses to auto-resume degraded decommission until reload succeeds or an admin action clears it.
   - The admin handler may still start local workers only if the degraded state is explicit and visible in status/logs.

Recommended first implementation: rollback if the current pool meta mutation can be reverted locally without losing unrelated state; otherwise persist degraded state and keep local workers running while surfacing the propagation failure. Do not return an error while leaving active metadata with no worker.

### Implementation Steps

- [ ] Add a failing test where `reload_pool_meta()` fails after `pool_meta.save()` and the resulting local state is not active without a worker.
- [ ] Implement the selected rollback or degraded-state behavior.
- [ ] If rollback is chosen, prove `is_suspended()` and `is_decommission_running()` return normal non-decommission behavior after the failed start.
- [ ] If degraded-start-with-worker is chosen, prove status/logs expose `reload_pool_meta` failure and local worker startup proceeds.
- [ ] Add restart coverage proving failed starts do not auto-resume as successful starts unless the durable degraded-state design explicitly allows it.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore start_decommission --lib
cargo test -p rustfs-ecstore reload_pool_meta --lib
cargo test -p rustfs-ecstore init --lib
cargo test -p rustfs pools --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Admin start failure cannot leave active decommission metadata without a local worker.
- Failed propagation either rolls back local active state or starts local workers with visible degraded status.
- Rebalance is not blocked by a hidden half-started decommission.
- Operators can see that reload failed, rollback happened, or degraded local execution is active.
- Existing successful decommission start behavior remains unchanged.

### Commit

```bash
git add crates/ecstore/src/pools.rs crates/ecstore/src/store/init.rs rustfs/src/admin/handlers/pools.rs
git commit -m "fix(decommission): prevent half-started state"
```

---

## R07: Distinguish Rebalance Stopping from Stopped

### Original Fix

F05: Rebalance distributed stop semantics.

### Finding

`stop_rebalance_state()` cancels the token and immediately marks started pools as `Stopped`. Admin status can report stopped while workers are still winding down.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Test: `crates/ecstore/src/rebalance.rs`
- Test: `rustfs/src/admin/handlers/rebalance.rs`

### Design

Add an explicit stop-requested state without breaking existing clients:

- Prefer an additive admin status field such as `stopping: bool` on pool status.
- Keep existing `status` values backward-compatible if adding a new enum variant would be too disruptive.
- Mark `stopping = true` after cancellation is requested and before worker terminal acknowledgement.
- Mark final stopped only after worker terminal handling records the stop event.

### Implementation Steps

- [ ] Add a test where stop is requested but a pool still has an active worker/cancel token and admin status reports `stopping = true`.
- [ ] Add a test where terminal stop event clears `stopping` and reports stopped.
- [ ] Implement the minimal metadata/status fields required to distinguish the states.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore stop_rebalance --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Operators can distinguish stop requested from fully stopped.
- Stop failure propagation from F05 remains intact.
- Existing JSON consumers still receive the original status fields.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs rustfs/src/admin/handlers/rebalance.rs
git commit -m "fix(rebalance): expose stopping status"
```

---

## R08: Allow Cleanup Preflight to Tolerate Confirmed Safe-expired Versions

### Original Fix

F08 and F10: lifecycle-expired cleanup semantics and cleanup preflight.

### Finding

Decommission can count safe lifecycle-expired versions toward cleanup, but cleanup preflight compares the full original `FileInfoVersions`. If lifecycle removes the safe-expired version before preflight, cleanup is incorrectly blocked.

### Files

- Modify: `crates/ecstore/src/data_movement.rs`
- Modify: `crates/ecstore/src/pools.rs`
- Test: `crates/ecstore/src/pools.rs`
- Test: `crates/ecstore/src/data_movement.rs`

### Design

Build an expected cleanup identity set that distinguishes:

- required versions that must still match;
- versions explicitly confirmed safe-expired that may be absent;
- any new or changed version, which must still fail preflight.

Do not allow arbitrary missing versions. Only allow absence for versions counted as safe-expired during the same entry processing.

### Implementation Steps

- [ ] Extend the cleanup preflight helper to accept optional allowed-missing identities.
- [ ] In decommission, collect identities for versions counted in `expired`.
- [ ] Pass the allowed-missing set to cleanup preflight.
- [ ] Add a test where a safe-expired version is absent during preflight and cleanup is allowed.
- [ ] Add a negative test where a non-expired migrated version is absent or changed and cleanup is blocked.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore decommission_entry --lib
cargo test -p rustfs-ecstore cleanup --lib
cargo test -p rustfs-ecstore lifecycle --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Confirmed safe-expired versions may disappear before cleanup without blocking cleanup.
- Changed, new, or unexpectedly missing protected versions still block cleanup.
- Rebalance preflight behavior remains strict unless it explicitly tracks safe-expired versions.

### Commit

```bash
git add crates/ecstore/src/data_movement.rs crates/ecstore/src/pools.rs
git commit -m "fix(decommission): tolerate expired cleanup preflight"
```

---

## R09: Complete Decommission Audit Context on Rejected Requests

### Original Fix

F12: Structured audit fields.

### Finding

Some decommission rejected paths still call older helpers that log operation/reason/pool without request ID, actor, or remote address after those values are already available.

### Files

- Modify: `rustfs/src/admin/handlers/pools.rs`
- Test: `rustfs/src/admin/handlers/pools.rs`

### Design

Normalize all decommission start/cancel reject logs after authentication to include:

- `event`
- `component`
- `subsystem`
- `operation`
- `action`
- `result = "rejected"`
- `reason`
- `request_id`
- `actor`
- `remote_addr`
- target `pool` or `pool_index` when available

### Implementation Steps

- [ ] Add helper overloads or a small `PoolAuditContext` struct containing `request_id`, `actor`, and `remote_addr`.
- [ ] Update invalid query, invalid pool, pool not found, and pool index out-of-range paths in decommission start/cancel to use contextual logging after authentication.
- [ ] Add log-capture tests if an existing tracing test helper is available; otherwise add helper-level tests for field construction.
- [ ] Run:

```bash
cargo test -p rustfs pools --lib
scripts/check_logging_guardrails.sh
cargo fmt --all --check
```

### Acceptance Criteria

- Rejected decommission admin operations can be searched by actor, request ID, remote address, and target.
- Logs still avoid raw credentials and authorization headers.
- Existing error responses remain unchanged.

### Commit

```bash
git add rustfs/src/admin/handlers/pools.rs
git commit -m "fix(admin): complete decommission audit context"
```

---

## R10: Harden Legacy Pool Metadata Fallback

### Original Fix

F13: Persisted metadata decode hardening.

### Finding

If strict `PersistedPoolMeta` decode fails, code falls back to lenient `PoolMeta` decode. Legacy-shaped payloads containing `version`, `pools`, `dont_save`, and unexpected fields can be silently accepted.

### Files

- Modify: `crates/ecstore/src/pools.rs`
- Test: `crates/ecstore/src/pools.rs`

### Design

Keep legacy compatibility but make it explicit:

- Introduce a private `LegacyPoolMeta` DTO with `#[serde(deny_unknown_fields)]`.
- Include exactly the legacy fields that are known to be supported: `version`, `pools`, and `dont_save`.
- Convert `LegacyPoolMeta` into `PoolMeta`.
- Reset `dont_save` to `false` after decode.
- Preserve terminal-state validation.

### Implementation Steps

- [ ] Add a failing test for a legacy-shaped payload with an extra `unexpected` field and assert decode fails.
- [ ] Add a passing test for a legacy-shaped payload with only supported legacy fields.
- [ ] Add `LegacyPoolMeta`, `LegacyPoolStatus` if needed, and `LegacyPoolDecommissionInfo` if needed.
- [ ] Replace lenient `rmp_serde::from_slice::<PoolMeta>` fallback with strict legacy DTO decode.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore pool_meta --lib
cargo test -p rustfs-ecstore metadata --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Supported legacy metadata still decodes.
- Unknown legacy-shaped fields fail with actionable decode context.
- Runtime-only `dont_save` is never restored from disk.

### Commit

```bash
git add crates/ecstore/src/pools.rs
git commit -m "fix(metadata): harden legacy pool meta decode"
```

---

## R11: Add Store Init Integration Regression for Decommission/Rebalance Conflict

### Original Fix

F04: Store init recovery ordering.

### Finding

The startup ordering fix has helper tests but lacks an integration-level test that exercises persisted active decommission metadata plus persisted rebalance metadata through `ECStore::init()`.

### Files

- Modify: `crates/ecstore/src/store/init.rs`
- Test: `crates/ecstore/src/store/init.rs`
- Possibly use existing test helpers in: `crates/ecstore/src/pools.rs`, `crates/ecstore/src/rebalance.rs`

### Design

Add a store-init-level regression test that proves:

- active decommission metadata is installed before rebalance auto-start is considered;
- rebalance auto-start is skipped when decommission is active;
- rebalance still auto-starts when only rebalance metadata exists.

Avoid broad test infrastructure rewrites. Use existing init helper seams if full `ECStore::init()` setup is too expensive.

### Implementation Steps

- [ ] Inspect existing `store::init::tests` helpers and identify the narrowest integration seam that loads persisted pool meta and rebalance meta.
- [ ] Add a test for active decommission plus started rebalance metadata and assert rebalance is not started.
- [ ] Add or keep a positive test where only rebalance metadata allows auto-start.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore init --lib
cargo test -p rustfs-ecstore rebalance_meta --lib
cargo test -p rustfs-ecstore decommission --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Future refactors cannot reorder init recovery without failing tests.
- Decommission resume behavior remains unchanged except for blocking concurrent rebalance.
- No production behavior changes are introduced by this test-only task.

### Commit

```bash
git add crates/ecstore/src/store/init.rs
git commit -m "test(init): cover decommission rebalance recovery"
```

---

## R12: Remove Async Saves from Pool Metadata Write Guards

### Original Fix

F03: Decommission pool metadata durability and reload semantics.

### Finding

Several decommission paths call `pool_meta.save(...).await` while holding `self.pool_meta.write().await`. Confirmed locations include start, cancel, failed, complete, bucket-done, and progress-save paths in `crates/ecstore/src/pools.rs`. This can block `is_suspended()`, pool routing, admin status, and progress updates while disk or peer metadata work is pending. It also violates `crates/AGENTS.md`, which forbids holding Tokio write guards across `.await` unless bounded and unavoidable.

### Files

- Modify: `crates/ecstore/src/pools.rs`
- Test: `crates/ecstore/src/pools.rs`

### Design

Convert each affected path to a short lock section:

1. Acquire the write guard.
2. Validate and mutate in-memory metadata.
3. Clone the minimal save snapshot or full `PoolMeta` snapshot needed for persistence.
4. Release the write guard before awaiting disk save or peer reload.
5. On save failure, reacquire a short write guard to roll back or mark an explicit degraded/error state.

Do not broaden this task into a metadata rewrite. Keep the change local to existing decommission save paths.

### Implementation Steps

- [ ] Inventory all `pool_meta.write()` sections in `crates/ecstore/src/pools.rs` that await `save()`.
- [ ] Add a helper only if it materially reduces repeated snapshot/save/error handling across affected paths.
- [ ] Refactor `start_decommission`, `decommission_cancel`, `decommission_failed`, `complete_decommission`, bucket-done, and progress-save paths so no Tokio write guard lives across `save(...).await`.
- [ ] Add a regression test with an instrumented or delayed save seam proving a concurrent read-side operation such as `is_suspended()` or decommission status lookup is not blocked by an in-flight save.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore pool_meta --lib
cargo test -p rustfs pools --lib
cargo fmt --all --check
```

### Acceptance Criteria

- No `self.pool_meta.write().await` guard in decommission paths is held across `pool_meta.save(...).await`.
- Save failures remain visible and do not silently lose required metadata state.
- Pool routing/status reads are not blocked for the duration of slow metadata saves.
- Existing decommission start/cancel/complete behavior remains unchanged except for improved lock scope.

### Commit

```bash
git add crates/ecstore/src/pools.rs
git commit -m "fix(decommission): save pool meta outside write lock"
```

---

## R13: Serialize Rebalance Start Check, Init, and Worker Start

### Original Fix

F05: Rebalance distributed start semantics.

### Finding

The admin handler checks `is_rebalance_conflicting_with_decommission()`, then separately calls `init_rebalance_meta()` and `start_rebalance()`. `init_rebalance_meta()` writes a fresh operation ID after async storage-info and metadata-save work. Two concurrent starts can both pass the initial check, generate different IDs, and return an ID that no longer matches the effective metadata.

### Files

- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Modify: `crates/ecstore/src/rebalance.rs`
- Test: `crates/ecstore/src/rebalance.rs`
- Test if needed: `rustfs/src/admin/handlers/rebalance.rs`

### Design

Make rebalance start a single serialized operation:

- Prefer a dedicated start mutex or metadata namespace lock that covers check, init, and local start.
- Alternatively, make `init_rebalance_meta()` reject existing active metadata with `OperationAborted` semantics before saving a new ID.
- Keep R01 propagation rollback behavior compatible with the chosen guard.

The guard should prevent duplicate local starts and stale returned operation IDs. It should not block unrelated status reads longer than necessary.

### Implementation Steps

- [ ] Add a concurrent-start regression test where two start attempts race and only one operation ID can be accepted.
- [ ] Add or reuse a start guard around check, `init_rebalance_meta()`, and `start_rebalance()`.
- [ ] Ensure the loser receives `OperationAborted` or an equivalent existing admin error instead of a stale success ID.
- [ ] Verify R01 rollback path releases the guard and leaves later starts possible.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore start_rebalance --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Concurrent admin rebalance start requests cannot both return successful different IDs.
- The accepted operation ID matches persisted and in-memory rebalance metadata.
- Failed or rolled-back starts do not permanently block later starts.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs rustfs/src/admin/handlers/rebalance.rs
git commit -m "fix(rebalance): serialize start operations"
```

---

## R14: Prove Last-delete-marker Migration Semantics Against MinIO Behavior

### Original Fix

F01: Delete marker and object-version safety.

### Finding

Both decommission and rebalance skip the last remaining delete marker when replication is not configured, then count that version as complete and may clean up the source entry. The helper-level tests prove the predicate, but they do not prove the user-visible behavior after migration: `ListObjectVersions`, current-object `GET`, and version-specific `GET` can expose whether version metadata was lost. Because this behavior was intentionally hardened earlier, treat it as a compatibility proof task first, not an immediate bug fix.

### Files

- Test: `crates/e2e_test/src/`
- Test if cheaper seam exists: `crates/ecstore/src/pools.rs`
- Test if cheaper seam exists: `crates/ecstore/src/rebalance.rs`

### Design

Add MinIO-compatible regression coverage for versioned buckets:

- Case A: object has only a delete marker.
- Case B: object has a delete marker plus historical data version.
- Case C: repeat A and B with delete-marker replication configured or explicitly absent.

For each case, exercise decommission and rebalance when feasible, then verify:

- `ListObjectVersions` still reports the expected versions/delete markers.
- Current-object `GET` preserves delete-marker not-found semantics.
- Version-specific `GET` preserves access to historical versions.

If the test proves RustFS intentionally differs from MinIO, record the product decision in this plan before changing code.

### Implementation Steps

- [ ] Identify the narrowest existing E2E harness that can create multi-pool RustFS and call admin decommission/rebalance.
- [ ] Add versioned-bucket scenarios for only-delete-marker and delete-marker-plus-history.
- [ ] Add assertions for `ListObjectVersions`, current `GET`, and version-specific `GET` after migration and source cleanup.
- [ ] If the current behavior loses required version metadata, split the actual behavior fix into a new implementation task before broadening R14.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore rebalance_delete_marker --lib
cargo test -p rustfs-ecstore decommission --lib
cargo test -p e2e_test versioning -- --nocapture
cargo fmt --all --check
```

### Acceptance Criteria

- Last-delete-marker migration semantics are covered by user-visible versioning assertions.
- Any RustFS/MinIO behavior difference is either fixed or explicitly documented with product approval.
- Source cleanup cannot remove the only user-visible version metadata without a failing test.

### Commit

```bash
git add crates/e2e_test/src crates/ecstore/src/pools.rs crates/ecstore/src/rebalance.rs
git commit -m "test(data-movement): cover delete marker migration"
```

---

## R15: Define Multi-pool Decommission Compatibility Semantics

### Original Fix

Compatibility follow-up for decommission admin behavior.

### Finding

`rustfs/src/admin/handlers/pools.rs` parses comma-separated pool targets, but `crates/ecstore/src/pools.rs` rejects more than one index with "decommission supports one target pool at a time". This is conservative and not directly dangerous, but MinIO supports submitting multiple pools for queued serial decommission. RustFS should either document the stricter contract or implement queue semantics.

### Files

- Modify: `docs/`
- Modify if implementing compatibility: `crates/ecstore/src/pools.rs`
- Modify if implementing compatibility: `rustfs/src/admin/handlers/pools.rs`
- Test if implementing compatibility: `crates/ecstore/src/pools.rs`
- Test if implementing compatibility: `rustfs/src/admin/handlers/pools.rs`

### Design Options

Choose one implementation before coding:

1. **Document single-pool support for now.**
   - Keep current store behavior.
   - Update admin/API/architecture documentation to state that RustFS accepts only one target pool per decommission request.
   - Keep the existing reject test as the compatibility guard.

2. **Implement MinIO-like queued multi-pool decommission.**
   - Accept multiple target pools.
   - Persist a serial queue, not parallel movement, unless product requirements say otherwise.
   - Ensure only the active pool is suspended/decommissioning at a time.
   - Add status that reports queued, active, completed, failed, and canceled pools.

Recommended first implementation: document the current single-pool contract unless MinIO CLI compatibility is a release blocker.

### Implementation Steps

- [ ] Confirm whether RustFS requires MinIO-compatible multi-pool submission for the target release.
- [ ] If documenting, update the relevant admin/decommission docs and keep the existing rejection behavior.
- [ ] If implementing, add a persisted decommission queue and tests proving serial execution across multiple pools.
- [ ] Add an admin-level test that comma-separated multiple pools either returns the documented error or creates the expected queue.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore start_decommission --lib
cargo test -p rustfs pools --lib
cargo fmt --all --check
```

### Acceptance Criteria

- RustFS behavior for multi-pool decommission is explicit and tested.
- If single-pool-only remains, the error is documented as intentional compatibility scope.
- If queued mode is implemented, only one pool is actively decommissioned at a time and restart recovery preserves queue order.

### Commit

```bash
git add docs crates/ecstore/src/pools.rs rustfs/src/admin/handlers/pools.rs
git commit -m "docs(decommission): define multi-pool support"
```

---

## Follow-up Test Matrix

Run after all R01-R15 tasks are complete:

```bash
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore multipart --lib
cargo test -p rustfs-ecstore metadata --lib
cargo test -p rustfs-ecstore pool_meta --lib
cargo test -p rustfs rebalance --lib
cargo test -p rustfs pools --lib
cargo test -p e2e_test versioning -- --nocapture
scripts/check_logging_guardrails.sh
cargo fmt --all --check
```

Before opening a PR:

```bash
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

After build-based verification, clean generated build artifacts to avoid unnecessary disk usage.

## Review Notes

- R02 should be completed before R03 because overwrite equivalence should compare the metadata that migration actually persists.
- R01 should be completed before R07 because rollback behavior and stopping status share stop semantics.
- R12 should be reviewed before R06 implementation because decommission start rollback/degraded handling should not add new lock-across-await cases.
- R04 should be completed before any wider metadata hardening because it prevents newly strict decode from rejecting metadata produced by the current merge path.
- R06 may require a product decision between rollback and durable degraded state. If that decision cannot be made during implementation, stop before code changes and record the trade-off.
- R14 is a proof task first. If it demonstrates data loss or MinIO-incompatible behavior, create a separate implementation task instead of hiding behavior changes inside the test task.
- R15 needs an explicit product decision before code changes; documentation-only single-pool support is acceptable if queued multi-pool decommission is not a release requirement.
