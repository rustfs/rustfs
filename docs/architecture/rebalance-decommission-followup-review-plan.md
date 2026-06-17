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
- One startup recovery test-depth gap that should run near the distributed-state fixes.
- One P3 MinIO compatibility gap where RustFS is intentionally more conservative today but the product contract is not yet explicit.

The following original items had no new material finding in this review and do not need a follow-up task here:

- F02: decommission unsafe overwrite cleanup safety.
- F06: multipart streaming memory behavior.
- F10: source cleanup preflight placement and not-found idempotence.
- F14: MinIO-like rebalance completion tolerance.

## Validated Additional Findings

The following externally reported issues were checked against the current code and are treated as real planning items:

- Decommission start still writes active local `pool_meta` before peer `reload_pool_meta()`. If reload fails, the admin handler returns an error before worker spawning, leaving an active decommission marker that can suspend the source pool without a running worker. Covered by upgraded R06.
- Rebalance start still performs `check -> init_rebalance_meta -> start_rebalance -> peer load_rebalance_meta(true)` without one atomic start guard. Peer failure is covered by R01; concurrent start identity races and cross-operation start races are covered by R13.
- Multiple decommission paths still hold `pool_meta.write()` across `save(...).await`, including start, cancel, failed, complete, bucket-done, and progress-save paths. Covered by new R12.
- Rebalance stats refresh holds a `rebalance_meta` read guard across `save_rebalance_meta_with_merge(...).await`. Covered by R16.
- Data movement metadata equivalence did not explicitly include user-visible `x-amz-tagging`/`expires` fields that are cleaned out of `user_defined`. Covered by expanded R03.
- Last-delete-marker skip behavior exists in both decommission and rebalance and has helper-level tests, but lacks MinIO-compatible version-listing regression coverage after migration and cleanup. Covered by new R14.
- R14 proof showed that deleting an absent key in a versioned bucket currently creates a delete-marker response without a version ID and `ListObjectVersions` does not report that only delete marker. Covered by new R17.
- The admin handler accepts comma-separated decommission targets, but store validation rejects more than one target pool. This is conservative, but the MinIO compatibility contract is not documented. Covered by R15.
- Source cleanup preflight still treats a prefix-list miss as safe cleanup. Prefix listing is not an exact object-version read, so cleanup should only proceed after exact source metadata verification, or after an explicit object/version not-found result. Covered by R18.
- Transition and restore lookup paths still use the caller's `ObjectOptions` and only contain commented-out `skip_decommissioned` guidance. MinIO skips decommissioned pools for these lifecycle operations. Covered by R19.
- Rebalance delete-marker migration still sets `skip_decommissioned` but not `skip_rebalancing`. MinIO uses `SkipRebalancing=true` for this path to avoid resolving the delete-marker write back to the source pool. Covered by R20.
- Rebalance source cleanup failures are recorded as cleanup warnings and the entry still completes. Non not-found/version-not-found cleanup failures should defer the entry/bucket so retry can converge. Covered by R21.
- Rebalance stop propagation can fail on a subset of peers, and terminal reload currently only loads metadata without cancelling any local worker that missed the stop RPC. Covered by R22 and R23.
- The earlier suggestion that stop/rollback must carry an operation ID is not a MinIO compatibility requirement. The higher-value fix is terminal reload convergence plus propagation observability. Covered by R22 and R23.

## Execution Order

| Order | Task | Original Fix | Priority | Main Risk |
| --- | --- | --- | --- | --- |
| 1 | R13 | F05 | P1 | Rebalance and decommission starts are not serialized across check/init/start |
| 2 | R01 | F05 | P1 | Rebalance start propagation failure can leave accepted peers running |
| 3 | R12 | F03 | P1 | Pool metadata write lock is held across async saves |
| 4 | R06 | F03 | P1 | Failed decommission reload can leave active metadata without a worker |
| 5 | R07 | F05 | P1 | Rebalance stop can release mutual exclusion before workers terminate |
| 6 | R02 | F07 | P1 | Multipart migration can drop per-part checksum metadata |
| 7 | R03 | F09 | P1 | Overwrite convergence can accept metadata-incomplete targets |
| 8 | R04 | F11 | P1 | Cleanup warning metadata can become self-inconsistent and fail decode |
| 9 | R11 | F04 | P2 | Store init recovery lacks full integration-level regression coverage |
| 10 | R16 | F05 | P2 | Rebalance metadata refresh holds a read lock across async save |
| 11 | R05 | F01 | P2 | Rebalance delete marker target write can stall when default placement selects source |
| 12 | R08 | F08/F10 | P2 | Cleanup preflight can reject safely expired versions already removed by lifecycle |
| 13 | R09 | F12 | P2 | Decommission rejected-request logs lack complete audit context |
| 14 | R10 | F13 | P2 | Legacy pool metadata fallback can bypass unknown-field hardening |
| 15 | R14 | F01 | P2 | Last delete marker migration semantics lack MinIO-compatible E2E proof |
| 16 | R17 | Versioning compatibility | P2 | Versioned delete of absent key loses only-delete-marker version metadata |
| 17 | R15 | Compatibility | P3 | Multi-pool decommission support is accepted by API shape but rejected by store |
| 18 | R18 | Source cleanup safety | P1 | Prefix-list miss can allow cleanup without exact object-version verification |
| 19 | R19 | Lifecycle pool selection | P1 | Transition/restore can select a decommissioning pool |
| 20 | R20 | Delete marker placement | P1 | Rebalance delete-marker migration can select a rebalancing source pool |
| 21 | R21 | Cleanup retry | P2 | Rebalance cleanup failure can mark an entry complete instead of retrying |
| 22 | R22 | Stop convergence | P1 | Terminal rebalance reload does not cancel local workers that missed stop RPC |
| 23 | R23 | Stop observability | P1 | Stop peer failures and terminal reload propagation are not visible enough |
| 24 | R24 | Compatibility | P3 | Multi-pool decommission queue semantics need a product-approved design |
| 25 | R25 | Compatibility | P3 | Rebalance single-coordinator semantics need a broader namespace-lock design |
| 26 | R26 | Admin API hardening | P3 | Dangerous admin query parameters are not strictly deserialized |

## Shared Rules

- Read this file before starting each task and restate the selected task in the commit summary or work log.
- Follow the Execution Order table rather than the numeric order of section headings.
- Implement exactly one `Rxx` task per commit.
- Keep unrelated refactors out of scope.
- Preserve the existing code shape unless a small helper is needed for the task.
- For Rust changes, run the focused tests listed under the task plus `cargo fmt --all --check`.
- Leave unrelated untracked files, such as `CLAUDE-FABLE-5.md`, untouched.

---

## R01: Roll Back Cluster Rebalance Start on Propagation Failure

### Original Fix

F05: Rebalance distributed start/stop semantics.

### Finding

`rustfs/src/admin/handlers/rebalance.rs` starts the local rebalance before peer propagation. If `notification_sys.load_rebalance_meta(true)` fails, the admin API returns an error, but the local worker may continue running. The peer call is also fan-out: some peers may already have accepted `start_rebalance = true` and spawned workers before another peer fails.

### Files

- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Modify if needed: `crates/ecstore/src/notification_sys.rs`
- Modify if needed: `crates/ecstore/src/rebalance.rs`
- Test: `rustfs/src/admin/handlers/rebalance.rs`
- Test if needed: `crates/ecstore/src/rebalance.rs`

### Design

On peer propagation failure after local or peer start:

1. Attempt local `store.stop_rebalance().await`.
2. Propagate a cluster-wide stop/rollback to peers that may have accepted the start.
3. Persist stopped metadata through the same path used by explicit stop.
4. Include `rebalance_id` in rollback checks when possible so rollback cannot stop a newer operation created after R13 releases the start guard.
5. Return an admin error that includes propagation failure and rollback failure details if rollback is incomplete.
6. Log `result = "rollback_success"`, `result = "rollback_partial"`, or `result = "rollback_failed"` with `request_id`, masked `actor`, `remote_addr`, `rebalance_id`, and peer failure details.

Do not silently convert propagation failure into success.

### Implementation Steps

- [ ] Add a helper in `rustfs/src/admin/handlers/rebalance.rs` such as `rollback_cluster_rebalance_start(store, notification_sys, rebalance_id)` that calls local `stop_rebalance()` and peer stop/rollback propagation.
- [ ] Add a unit test for formatting rollback failure context so an error includes `failed to propagate rebalance start`, local rollback result, and peer rollback result.
- [ ] Add an admin handler or helper-level test that simulates one peer accepting start and another peer failing, then verifies cluster rollback is attempted.
- [ ] Update the `RebalanceStart` error path to call the rollback helper before returning.
- [ ] If rollback is rebalance-id guarded, add a test where a newer operation ID is present and rollback refuses to stop it.
- [ ] Run:

```bash
cargo test -p rustfs rebalance --lib
cargo test -p rustfs-ecstore stop_rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Admin start does not leave local or peer rebalance workers running after peer propagation failure.
- Partial rollback failure is visible in the returned error, status/logs, and peer details.
- Rollback cannot stop a newer rebalance operation ID.
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

`is_equivalent_data_movement_object()` compares core object fields but does not compare multipart part metadata/checksums, replication/version purge state, or all user-visible metadata. In particular, `ObjectInfo::from_file_info()` extracts `x-amz-tagging` into `user_tags` and parses `expires`, while `clean_metadata()` removes those keys from `user_defined`. A target missing required metadata can be treated as equivalent and allow source cleanup.

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
- object tags / `x-amz-tagging`
- `expires`
- replication status/internal state
- version purge status/internal state
- multipart part count and per-part number, ETag, size, actual size, mod time, index, and checksums

Use exact equality for these fields. A false negative is safer than a false positive.

### Implementation Steps

- [ ] Add a helper such as `is_equivalent_data_movement_part(source, target)` in `crates/ecstore/src/data_movement.rs`.
- [ ] Update `is_equivalent_data_movement_object()` to compare replication/version purge fields, object tags, expires, and call the part helper.
- [ ] Add a failing test where source and target differ only by missing per-part checksum and assert overwrite convergence is rejected.
- [ ] Add a failing test where source and target differ only by version purge status and assert overwrite convergence is rejected.
- [ ] Add failing tests where source and target differ only by object tags or expires and assert overwrite convergence is rejected.
- [ ] Confirm the data movement write path preserves tags and expires; if it does not, extend the write options or create a separate write-path task before allowing source cleanup.
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
- Missing multipart checksum, tags, expires, or replication/version purge state blocks convergence.
- Data movement writes preserve metadata required by the strengthened equivalence check before cleanup can run.
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

Rebalance delete marker movement routes through `ECStore::delete_object()`. In the data-movement branch, `handle_delete_object()` computes a non-source `target_pool_idx` for equivalence checks, but the actual delete marker write still uses `selected_target_pool_idx`. If default placement resolves to the source pool, the path may return `DataMovementOverwriteErr` rather than writing the known non-source fallback target.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify if needed: `crates/ecstore/src/store/object.rs`
- Test: `crates/ecstore/src/rebalance.rs`

### Design

The data-movement delete marker path should:

1. Exclude `src_pool_idx` from target selection.
2. Write the delete marker metadata to the resolved non-source `target_pool_idx`, not the original source-selected pool.
3. Treat overwrite as complete only after strict target equivalence is proven.
4. Return failure if no non-source target is available.

### Implementation Steps

- [ ] Add a failing test where default placement selects the source pool but another target pool is available; rebalance delete marker migration must still write to the non-source target.
- [ ] Fix or prove the `handle_delete_object()` data-movement delete marker branch so it writes through the resolved non-source fallback target.
- [ ] Keep `rebalance_delete_marker()` narrow unless rebalance-specific context is required.
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

`start_decommission()` saves active `pool_meta` before peer reload. Reload failure returns an API error before the admin handler reaches `spawn_decommission_routines`, but the local pool already has active decommission metadata. That state can make `is_suspended()` exclude the pool and `is_decommission_running()` block rebalance while no local decommission worker is moving data until restart recovery happens. Partial peer reload success can also leave remote node memory with active decommission metadata even if the local node later rolls back.

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
   - Propagate the rollback metadata to peers that may have accepted the active state.
   - Report any rollback propagation failure as an explicit partial rollback condition.
   - Safer for user-visible API semantics, but needs careful persistence handling.
   - Do not use cancel semantics if cancel means "decommissioned pool remains out of ordinary placement" in MinIO-compatible behavior.

2. **Persist degraded state.**
   - Add a durable `reload_failed` or `start_degraded` marker to decommission info.
   - Admin status exposes the marker.
   - Store init refuses to auto-resume degraded decommission until reload succeeds or an admin action clears it.
   - The admin handler may still start local workers only if the degraded state is explicit and visible in status/logs.
   - Propagate the degraded marker to peers or report peer memory state as unknown/partial if propagation fails.

Recommended first implementation: rollback if the current pool meta mutation can be reverted locally without losing unrelated state; otherwise persist degraded state and keep local workers running while surfacing the propagation failure. Do not return an error while leaving active metadata with no worker.

### Implementation Steps

- [ ] Add a failing test where `reload_pool_meta()` fails after `pool_meta.save()` and the resulting local state is not active without a worker.
- [ ] Implement the selected rollback or degraded-state behavior.
- [ ] If rollback is chosen, prove `is_suspended()` and `is_decommission_running()` return normal non-decommission behavior after the failed start.
- [ ] If rollback is chosen, prove peers that accepted the active state receive rollback metadata, or status reports partial rollback with affected peers.
- [ ] If degraded-start-with-worker is chosen, prove status/logs expose `reload_pool_meta` failure and local worker startup proceeds.
- [ ] If degraded-start-with-worker is chosen, prove peers either receive the degraded marker or status reports unknown peer state.
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
- Partial peer reload/rollback state is surfaced; no node silently remains active-without-worker.
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

`stop_rebalance_state()` cancels the token and immediately marks started pools as `Stopped`. Admin status can report stopped while workers are still winding down. More importantly, rebalance/decommission mutual exclusion can be released too early if `is_rebalance_in_progress()` treats stop-requested metadata as terminal before worker acknowledgement is persisted.

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
- Treat `stopping = true` as an active conflict for decommission/rebalance start guards until terminal acknowledgement is recorded.

### Implementation Steps

- [ ] Add a test where stop is requested but a pool still has an active worker/cancel token and admin status reports `stopping = true`.
- [ ] Add a test where terminal stop event clears `stopping` and reports stopped.
- [ ] Add a test where decommission start is rejected while rebalance is stopping but not terminal.
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
- Decommission/rebalance mutual exclusion is not released until worker terminal acknowledgement is persisted.
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

The fix must not simply clone whole `PoolMeta` snapshots and save them concurrently. `PoolMeta::save()` persists the full metadata document; if an older lock-free snapshot finishes after a newer one, it can overwrite bucket-done, progress, failed, or complete state. R12 must preserve save ordering or use a merge/epoch strategy.

### Files

- Modify: `crates/ecstore/src/pools.rs`
- Test: `crates/ecstore/src/pools.rs`

### Design

Convert each affected path to a short lock section:

1. Acquire the write guard.
2. Validate and mutate in-memory metadata.
3. Clone the minimal save snapshot or full `PoolMeta` snapshot needed for persistence.
4. Release the write guard before awaiting disk save or peer reload.
5. Serialize full-document saves through a dedicated save mutex, generation/epoch check, or equivalent merge-before-save helper so stale snapshots cannot overwrite newer in-memory state.
6. On save failure, reacquire a short write guard only for conditional handling: roll back if current memory still matches this mutation, otherwise preserve newer state and report the save failure.

Mutation-specific save failure policy:

- `start_decommission`: rollback or mark degraded only if the active start still matches this operation.
- `decommission_cancel`: keep the in-memory cancel request visible; return/report save failure so restart risk is known.
- `decommission_failed` and `complete_decommission`: do not silently revert terminal memory state; report durable-state mismatch if persistence fails.
- bucket-done and progress-save: do not roll back counters or bucket progress; retry or report persistence lag without losing newer progress.

Do not broaden this task into a metadata rewrite. Keep the change local to existing decommission save paths.

### Implementation Steps

- [ ] Inventory all `pool_meta.write()` sections in `crates/ecstore/src/pools.rs` that await `save()`.
- [ ] Add a helper only if it materially reduces repeated snapshot/save/error handling across affected paths.
- [ ] Add a save serialization, generation, or merge-before-save mechanism for full `PoolMeta` persistence.
- [ ] Refactor `start_decommission`, `decommission_cancel`, `decommission_failed`, `complete_decommission`, bucket-done, and progress-save paths so no Tokio write guard lives across `save(...).await`.
- [ ] Add a regression test with an instrumented or delayed save seam proving a concurrent read-side operation such as `is_suspended()` or decommission status lookup is not blocked by an in-flight save.
- [ ] Add a regression test where two saves complete out of order and the older snapshot cannot overwrite newer durable state.
- [ ] Add tests for save failure handling in start and terminal-state paths.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore pool_meta --lib
cargo test -p rustfs pools --lib
cargo fmt --all --check
```

### Acceptance Criteria

- No `self.pool_meta.write().await` guard in decommission paths is held across `pool_meta.save(...).await`.
- Lock-free saves preserve persistence order or merge with current state so stale snapshots cannot overwrite newer metadata.
- Save failures remain visible and do not silently lose required metadata state.
- Save failure handling is mutation-specific and conditional; it does not roll back unrelated or newer updates.
- Pool routing/status reads are not blocked for the duration of slow metadata saves.
- Existing decommission start/cancel/complete behavior remains unchanged except for improved lock scope.

### Commit

```bash
git add crates/ecstore/src/pools.rs
git commit -m "fix(decommission): save pool meta outside write lock"
```

---

## R13: Serialize Rebalance and Decommission Start Gates

### Original Fix

F05: Rebalance distributed start semantics.

### Finding

The admin handler checks `is_rebalance_conflicting_with_decommission()`, then separately calls `init_rebalance_meta()` and `start_rebalance()`. `init_rebalance_meta()` writes a fresh operation ID after async storage-info and metadata-save work. Two concurrent rebalance starts can both pass the initial check, generate different IDs, and return an ID that no longer matches the effective metadata. A decommission start can also race between rebalance metadata initialization and worker start, leaving active-looking rebalance metadata without workers if `start_rebalance()` later rejects the decommission conflict.

### Files

- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Modify: `crates/ecstore/src/rebalance.rs`
- Modify if needed: `crates/ecstore/src/pools.rs`
- Test if needed: `crates/ecstore/src/pools.rs`
- Test: `crates/ecstore/src/rebalance.rs`
- Test if needed: `rustfs/src/admin/handlers/rebalance.rs`

### Design

Make rebalance and decommission starts a single serialized conflict domain:

- Prefer a dedicated start mutex or metadata namespace lock shared by rebalance start and decommission start.
- For rebalance, the guard covers check, `init_rebalance_meta()`, local `start_rebalance()`, and the point where R01 propagation rollback becomes responsible.
- For decommission, the same guard covers decommission/rebalance conflict checks through active metadata save/reload handling.
- Alternatively, make `init_rebalance_meta()` reject existing active metadata with `OperationAborted` semantics before saving a new ID.
- If `start_rebalance()` fails after `init_rebalance_meta()` saved active-looking metadata, roll back or mark that metadata failed before releasing the guard.
- Keep R01 propagation rollback behavior compatible with the chosen guard.

The guard should prevent duplicate starts, stale returned operation IDs, and cross-operation decommission/rebalance start races. It should not block unrelated status reads longer than necessary.

### Implementation Steps

- [ ] Add a concurrent-start regression test where two start attempts race and only one operation ID can be accepted.
- [ ] Add a rebalance-vs-decommission race regression test where decommission attempts to start between rebalance metadata init and worker start.
- [ ] Add or reuse a shared start guard around rebalance check, `init_rebalance_meta()`, and `start_rebalance()`.
- [ ] Apply the same conflict domain to decommission start checks and active-state transition.
- [ ] Ensure the loser receives `OperationAborted` or an equivalent existing admin error instead of a stale success ID.
- [ ] If local rebalance worker start fails after metadata init, roll back or persist a failed state before releasing the guard.
- [ ] Verify R01 rollback path releases the guard and leaves later starts possible.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore start_rebalance --lib
cargo test -p rustfs-ecstore start_decommission --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Concurrent admin rebalance start requests cannot both return successful different IDs.
- The accepted operation ID matches persisted and in-memory rebalance metadata.
- Rebalance and decommission cannot both pass start checks through separate non-atomic windows.
- Rebalance metadata init followed by worker-start failure cannot leave active-looking metadata without workers.
- Failed or rolled-back starts do not permanently block later starts.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs crates/ecstore/src/pools.rs rustfs/src/admin/handlers/rebalance.rs
git commit -m "fix(rebalance): serialize start gates"
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

This is a mandatory proof gate. If the test proves RustFS loses user-visible version metadata or intentionally differs from MinIO, stop after the failing/characterization test and create a separate implementation or product-decision task. Do not hide behavior changes inside R14.

### Implementation Steps

- [ ] Identify the narrowest existing E2E harness that can create multi-pool RustFS and call admin decommission/rebalance.
- [ ] Add versioned-bucket scenarios for only-delete-marker and delete-marker-plus-history.
- [ ] Add assertions for `ListObjectVersions`, current `GET`, and version-specific `GET` after migration and source cleanup.
- [ ] If the current behavior loses required version metadata, commit the proof/characterization test as R14 and add a new follow-up implementation task before changing behavior.
- [ ] Run:

```bash
cargo test -p rustfs-ecstore rebalance_delete_marker --lib
cargo test -p rustfs-ecstore decommission --lib
cargo test -p e2e_test versioning -- --nocapture
cargo fmt --all --check
```

### Acceptance Criteria

- Last-delete-marker migration semantics are covered by user-visible versioning assertions.
- R14 contains tests only; behavior fixes or product documentation are split into a separate task if needed.
- Source cleanup cannot remove the only user-visible version metadata without a failing test.

### Commit

```bash
git add crates/e2e_test/src
git commit -m "test(data-movement): cover delete marker migration"
```

---

## R17: Preserve Only-delete-marker Version Metadata

### Original Fix

Compatibility follow-up discovered by R14.

### Finding

R14 proved a MinIO-incompatible versioning gap before exercising migration: in a versioned bucket, deleting an absent key returns success but does not expose a durable delete-marker version. The delete response lacks `version_id`, and `ListObjectVersions` does not report the only delete marker. Because decommission and rebalance intentionally skip a last remaining delete marker without replication, this baseline gap means migration cleanup can remove or ignore the only user-visible version metadata without an active failing default test.

### Files

- Modify: `crates/ecstore/src/store/object.rs`
- Modify if needed: `crates/ecstore/src/set_disk.rs`
- Test: `crates/e2e_test/src/delete_marker_migration_semantics_test.rs`
- Test if cheaper seam exists: `crates/ecstore/src/store/object.rs`

### Design

Make versioned deletion of an absent key persist and expose an S3/MinIO-compatible delete marker:

1. When bucket versioning is enabled and the target key has no existing data version, create a delete marker with a real version ID.
2. Ensure `ListObjectVersions` reports that delete marker as the latest version.
3. Preserve current-object `GET` delete-marker not-found semantics.
4. Keep delete-marker-plus-history behavior unchanged.
5. Unignore the R14 only-delete-marker compatibility test once the behavior is fixed.

Do not change decommission/rebalance skip policy in this task; first make the underlying versioning semantics compatible and verifiable.

### Implementation Steps

- [x] Reproduce the R14 ignored e2e by running it with `--ignored` and confirm it fails on missing only-delete-marker metadata.
- [x] Trace the versioned delete path for absent keys and identify where delete marker metadata is skipped or not assigned a version ID.
- [x] Add a focused unit/helper test if a cheap seam exists, then fix the delete path to persist the delete marker.
- [x] Unignore `test_versioning_only_delete_marker_has_minio_compatible_visibility_for_migration_proof`.
- [x] Run:

```bash
cargo test -p rustfs-ecstore delete_marker --lib
cargo test -p e2e_test delete_marker_migration_semantics -- --nocapture
cargo fmt --all --check
```

### Acceptance Criteria

- Deleting an absent key in an enabled versioned bucket returns a delete-marker version ID.
- `ListObjectVersions` reports the only delete marker as latest.
- Current-object `GET` still returns delete-marker not-found semantics.
- The R14 only-delete-marker compatibility proof runs by default and passes.

### Commit

```bash
git add crates/ecstore/src/store/object.rs crates/e2e_test/src/delete_marker_migration_semantics_test.rs
git commit -m "fix(versioning): persist only delete markers"
```

---

## R15: Document Single-pool Decommission Compatibility Scope

### Original Fix

Compatibility follow-up for decommission admin behavior.

### Finding

`rustfs/src/admin/handlers/pools.rs` parses comma-separated pool targets, but `crates/ecstore/src/pools.rs` rejects more than one index with "decommission supports one target pool at a time". This is conservative and not directly dangerous, but MinIO supports submitting multiple pools for queued serial decommission. In this remediation plan, RustFS should document and test the stricter single-pool contract. A queued multi-pool implementation requires a separate product decision and a larger design task.

### Files

- Modify: `docs/`
- Test if needed: `crates/ecstore/src/pools.rs`
- Test if needed: `rustfs/src/admin/handlers/pools.rs`

### Design

Document single-pool support for this remediation scope:

- Keep current store behavior.
- Update admin/API/architecture documentation to state that RustFS accepts only one target pool per decommission request.
- Keep the existing reject test as the compatibility guard.
- If product requirements demand MinIO-like queued multi-pool decommission, create a new design task covering persisted queues, restart recovery, active-vs-queued status, and one-active-pool-at-a-time semantics.

### Implementation Steps

- [x] Update the relevant admin/decommission docs with the current single-pool decommission contract.
- [x] Keep or add a test that comma-separated multiple pools return the documented error.
- [x] Add a short note that MinIO-like queued multi-pool decommission is out of scope for R15 and requires a separate product-approved task.
- [x] Run:

```bash
cargo test -p rustfs-ecstore start_decommission --lib
cargo test -p rustfs pools --lib
cargo fmt --all --check
```

### Acceptance Criteria

- RustFS single-pool decommission behavior is explicit and tested.
- The multi-pool rejection error is documented as intentional compatibility scope.
- No queued multi-pool implementation is mixed into this P3 documentation task.

### Commit

```bash
git add docs crates/ecstore/src/pools.rs rustfs/src/admin/handlers/pools.rs
git commit -m "docs(decommission): define multi-pool support"
```

---

## R16: Save Rebalance Metadata Refresh Outside Read Guard

### Original Fix

F05: Rebalance metadata lifecycle and distributed state.

### Finding

`update_rebalance_stats()` takes a `rebalance_meta` read guard and then calls `save_rebalance_meta_with_merge(...).await` while the guard is still live. This is less severe than the decommission `pool_meta.write()` cases, but it can still delay writers that need to update rebalance state. Other rebalance save paths already clone metadata inside a short lock section and save after releasing the guard.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Test: `crates/ecstore/src/rebalance.rs`

### Design

Match the existing `save_rebalance_stats()` pattern:

1. Acquire the read or write guard only long enough to clone the metadata snapshot that needs saving.
2. Release the guard before calling `save_rebalance_meta_with_merge(...).await`.
3. Preserve existing merge behavior and error context.

Do not combine this with R13 unless the same helper naturally covers both paths without broad refactoring.

### Implementation Steps

- [x] Add a regression test or helper-level assertion showing `update_rebalance_stats()` saves after cloning metadata outside the guard.
- [x] Refactor `update_rebalance_stats()` so no `rebalance_meta` read guard is held across async save.
- [x] Run:

```bash
cargo test -p rustfs-ecstore update_rebalance_stats --lib
cargo test -p rustfs-ecstore rebalance_meta --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- `update_rebalance_stats()` does not hold a `rebalance_meta` guard across `save_rebalance_meta_with_merge(...).await`.
- Existing merge-on-save behavior remains unchanged.
- Rebalance metadata writers are not blocked by a long read guard during disk/network save.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs
git commit -m "fix(rebalance): save metadata refresh outside lock"
```

---

## R18: Require Exact Source Version Verification Before Cleanup

### Original Fix

Source cleanup safety follow-up for decommission and rebalance.

### Finding

`ensure_source_cleanup_versions_unchanged()` uses `list_path()` with an object prefix and returns success when the exact object entry is not observed. A prefix-list miss is not the same as an explicit object not-found/version-not-found result. If listing truncation, filtering, cache behavior, or an unexpected prefix collision hides the exact entry, decommission or rebalance cleanup can delete the source without proving that the source versions still match the migrated snapshot.

### Files

- Modify: `crates/ecstore/src/data_movement.rs`
- Modify if needed: `crates/ecstore/src/set_disk.rs`
- Test: `crates/ecstore/src/data_movement.rs`
- Test if needed: `crates/ecstore/src/set_disk.rs`

### Design

Use exact object metadata resolution for cleanup preflight:

1. Read the exact object's version metadata from the source `SetDisks`.
2. Treat only explicit object-not-found/version-not-found as already cleaned.
3. Treat read quorum, decode, unexpected list/cache, and other errors as cleanup preflight failures.
4. Compare the exact current versions against the expected migrated snapshot, allowing only the already-modeled lifecycle-expired identities.
5. Keep decommission and rebalance call sites using the same helper.

Do not rely on prefix listing to prove exact object absence.

### Implementation Steps

- [x] Add an exact source metadata read helper that returns `Ok(None)` only for explicit object-not-found/version-not-found.
- [x] Replace the prefix-list based `load_source_cleanup_versions()` path with the exact helper.
- [x] Add a helper-level test that a miss from a non-exact source would fail closed instead of succeeding.
- [x] Add or update tests where explicit not-found remains idempotent.
- [x] Run:

```bash
cargo test -p rustfs-ecstore source_cleanup --lib
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Source cleanup can proceed only after exact metadata equivalence or explicit not-found/version-not-found.
- Prefix-list absence is no longer a success signal.
- Existing lifecycle-expired allowed-missing behavior is preserved.

### Commit

```bash
git add crates/ecstore/src/data_movement.rs crates/ecstore/src/set_disk.rs
git commit -m "fix(data-movement): require exact cleanup preflight"
```

---

## R19: Skip Decommissioned Pools for Transition and Restore

### Original Fix

MinIO compatibility follow-up for lifecycle transition and restore.

### Finding

`handle_transition_object()` and `handle_restore_transitioned_object()` still use the caller's `ObjectOptions` when locating the object across pools. The code contains commented-out `skip_decommissioned` guidance, but the option is not applied. MinIO explicitly uses `SkipDecommissioned=true` for these lifecycle operations, so RustFS can route transition/restore work to a pool that is being decommissioned.

### Files

- Modify: `crates/ecstore/src/store/object.rs`
- Test: `crates/ecstore/src/store/object.rs`

### Design

Clone the incoming options for multi-pool lookup and operation dispatch:

1. Set `skip_decommissioned = true`.
2. Set `no_lock = true` only if this matches the existing MinIO-compatible lifecycle path and does not bypass a required RustFS lock; otherwise preserve caller `no_lock`.
3. Use the same adjusted options for the pool lookup and the selected pool operation, so lookup and mutation target the same eligibility rules.
4. Keep single-pool behavior unchanged.

### Implementation Steps

- [x] Add a small helper that prepares transition/restore options for pool lookup.
- [x] Update `handle_transition_object()` to use the adjusted options on multi-pool paths.
- [x] Update `handle_restore_transitioned_object()` to use the adjusted options on multi-pool paths.
- [x] Add tests asserting transition and restore options set `skip_decommissioned` and preserve the intended `no_lock` behavior.
- [x] Run:

```bash
cargo test -p rustfs-ecstore transition --lib
cargo test -p rustfs-ecstore restore --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Transition and restore do not select decommissioned pools in multi-pool deployments.
- Lookup and operation dispatch use consistent options.
- Single-pool behavior is unchanged.

### Commit

```bash
git add crates/ecstore/src/store/object.rs
git commit -m "fix(lifecycle): skip decommissioned transition pools"
```

---

## R20: Mark Rebalance Delete-marker Writes as Skip-rebalancing

### Original Fix

F01/R05: Rebalance delete-marker movement.

### Finding

`rebalance_delete_marker_opts()` sets `skip_decommissioned = true` but not `skip_rebalancing = true`. MinIO uses `SkipRebalancing=true` for rebalance delete-marker movement so placement does not resolve back to a source pool that is actively rebalancing.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Test: `crates/ecstore/src/rebalance.rs`

### Design

Add `skip_rebalancing = true` to rebalance delete-marker options while preserving the existing `skip_decommissioned = true` guard. This is a narrow placement fix and should not change delete-marker payload or replication metadata.

### Implementation Steps

- [x] Update `rebalance_delete_marker_opts()` to set `skip_rebalancing = true`.
- [x] Extend the existing helper test to assert both `skip_rebalancing` and `skip_decommissioned`.
- [x] Run:

```bash
cargo test -p rustfs-ecstore rebalance_delete_marker --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Rebalance delete-marker movement skips pools that are currently rebalancing.
- Existing decommission skip and delete-replication state are preserved.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs
git commit -m "fix(rebalance): skip rebalancing delete marker targets"
```

---

## R21: Defer Rebalance Entries on Source Cleanup Failure

### Original Fix

F14/R04: Rebalance cleanup tolerance and warning accounting.

### Finding

`rebalance_entry()` records non not-found source cleanup delete failures as cleanup warnings and then returns `Completed`. That prevents the bucket retry queue from converging cleanup later. For data safety and operational convergence, only object-not-found/version-not-found should be treated as already cleaned. Other cleanup failures should defer the entry and bucket.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Test: `crates/ecstore/src/rebalance.rs`

### Design

Change cleanup delete result handling from warning-only completion to retryable deferral:

1. Keep `Ok(_)`, object-not-found, and version-not-found as successful cleanup outcomes.
2. For all other cleanup delete errors, record the last error and return `RebalanceEntryOutcome::Deferred`.
3. Preserve cleanup warning accounting only for observability if it remains useful, but do not mark the entry complete.
4. Ensure `wait_rebalance_entry_tasks()` and `defer_rebalance_bucket()` continue to put the bucket back on the queue.

### Implementation Steps

- [x] Change cleanup result resolution to return a typed outcome instead of optional warning text, or adjust the call site to treat optional warning as deferral.
- [x] Update tests that currently expect cleanup failures to be ignored.
- [x] Add a test that a transient cleanup failure returns `Deferred` and records the last error.
- [x] Add a test that not-found/version-not-found cleanup remains `Completed`.
- [x] Run:

```bash
cargo test -p rustfs-ecstore resolve_rebalance_entry_cleanup --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Non not-found cleanup failures do not mark entries or buckets complete.
- Cleanup failure is retried through the existing deferred bucket path.
- Cleanup warning/status remains visible without hiding incomplete cleanup.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs
git commit -m "fix(rebalance): defer failed source cleanup"
```

---

## R22: Cancel Local Rebalance Workers on Terminal Reload

### Original Fix

F05/R07: Rebalance distributed stop semantics.

### Finding

If a peer misses `stop_rebalance()` but later receives `load_rebalance_meta(start=false)`, the RPC handler loads stopped/stopping metadata but does not cancel the already-running local worker token. `next_rebal_bucket()` also does not explicitly stop bucket selection for stopped/stopping metadata. This can leave a peer worker running after terminal metadata has propagated.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify: `rustfs/src/storage/rpc/node_service.rs`
- Test: `crates/ecstore/src/rebalance.rs`
- Test: `rustfs/src/storage/rpc/node_service.rs`

### Design

Make terminal reload authoritative for local workers:

1. After loading rebalance metadata, detect terminal stop state (`stopped_at` set or pool status stopping/stopped).
2. If terminal stop state is present, cancel any local in-memory worker token and keep metadata in stopping/stopped state.
3. In `resolve_next_rebalance_bucket()`, return `Ok(None)` for stopped/stopping metadata before selecting any bucket.
4. Keep `start_rebalance=true` behavior unchanged for active metadata.

### Implementation Steps

- [x] Add a helper that applies terminal loaded metadata to the local cancel token.
- [x] Call it from `load_rebalance_meta()` after replacing in-memory metadata.
- [x] Harden `resolve_next_rebalance_bucket()` so stopped/stopping metadata returns no bucket.
- [x] Add tests for terminal reload cancelling an in-memory token.
- [x] Add tests for `next_rebal_bucket()` returning `None` when `stopped_at` or stopping status is present.
- [x] Run:

```bash
cargo test -p rustfs-ecstore load_rebalance_meta --lib
cargo test -p rustfs-ecstore next_rebal_bucket --lib
cargo test -p rustfs rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- A peer that missed stop RPC stops its local worker after terminal metadata reload.
- Stopped/stopping rebalance metadata cannot hand out more buckets.
- Active reload/start behavior remains unchanged.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs rustfs/src/storage/rpc/node_service.rs
git commit -m "fix(rebalance): cancel workers on terminal reload"
```

---

## R23: Expose Rebalance Stop Propagation Failures

### Original Fix

F05/R07: Rebalance distributed stop/status observability.

### Finding

`RebalanceStop` and `NotificationSys::stop_rebalance()` surface aggregate peer errors, but status does not expose enough detail about failed peers, last propagation time, or pending terminal reload. When stop partially fails, operators need to see which peers failed and whether terminal reload propagation has been attempted.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify: `crates/ecstore/src/notification_sys.rs`
- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Test: `crates/ecstore/src/rebalance.rs`
- Test: `rustfs/src/admin/handlers/rebalance.rs`

### Design

Add minimal propagation observability without changing stop semantics:

1. Persist local stopped/stopping metadata even when peer stop propagation has failures.
2. Attempt terminal `load_rebalance_meta(false)` after local stop is persisted, even if some peer stop RPCs failed.
3. Record the last stop propagation attempt time and peer failure strings in rebalance metadata or status response.
4. Expose pending/failed propagation in admin status.

Do not add operation-id requirements for stop/rollback as a compatibility gate.

### Implementation Steps

- [x] Add fields or a status-only structure for last stop propagation time and failed peer details.
- [x] Update stop handler to persist local stopped metadata before returning peer propagation errors.
- [x] Ensure terminal reload broadcast is attempted after local stop persistence.
- [x] Extend rebalance status serialization to include propagation failure details.
- [x] Add tests for partial peer failure visibility.
- [x] Run:

```bash
cargo test -p rustfs rebalance --lib
cargo test -p rustfs-ecstore stop_rebalance --lib
cargo fmt --all --check
```

### Acceptance Criteria

- Partial stop propagation failures remain visible to clients/operators.
- Local stopped/stopping metadata is persisted before stop returns.
- Terminal reload propagation is attempted after stop persistence.
- No operation-id compatibility requirement is introduced for stop.

### Commit

```bash
git add crates/ecstore/src/rebalance.rs crates/ecstore/src/notification_sys.rs rustfs/src/admin/handlers/rebalance.rs
git commit -m "fix(rebalance): expose stop propagation failures"
```

---

## R24: Design Multi-pool Decommission Queue Compatibility

### Original Fix

R15 compatibility follow-up.

### Finding

RustFS currently rejects multiple decommission target pools in one request. MinIO supports submitting multiple pools and processing them serially. This is a compatibility gap, but implementing it safely requires a persisted queue, restart recovery, clear active/queued status, cancellation semantics, and one-active-pool-at-a-time worker scheduling.

### Files

- Modify: `docs/architecture/decommission-compatibility.md`
- Modify: `docs/architecture/rebalance-decommission-followup-review-plan.md`
- Code changes only after product approval.

### Design

Keep R24 as a design/product decision task unless the product requirement is explicitly approved:

1. Define request semantics for comma-separated pools.
2. Define persisted queued metadata shape and legacy decode compatibility.
3. Define serial worker scheduling and restart recovery.
4. Define cancel semantics for active and queued pools.
5. Define status response shape for active/queued/completed pools.

### Implementation Steps

- [ ] Write or update a compatibility design section for queued multi-pool decommission.
- [ ] Do not change store behavior until the design is approved.
- [ ] Run:

```bash
cargo fmt --all --check
```

### Acceptance Criteria

- Multi-pool decommission remains an explicit product decision.
- Implementation is not mixed into P1 data safety work.

### Commit

```bash
git add docs/architecture/decommission-compatibility.md docs/architecture/rebalance-decommission-followup-review-plan.md
git commit -m "docs(decommission): plan multi-pool queue support"
```

---

## R25: Design Rebalance Single-coordinator Start Semantics

### Original Fix

R13 rebalance start serialization.

### Finding

R13 serializes local start gates, but a stronger MinIO-like distributed single-coordinator model may require reading and writing `rebalance.bin` under a namespace lock immediately before start on every coordinator path. This is larger than the current local start guard and should be designed separately from P1 stop convergence.

### Files

- Modify: `docs/architecture/rebalance-decommission-followup-review-plan.md`
- Code changes only after design approval.

### Design

Define whether RustFS should:

1. Keep local start gate plus metadata merge as the supported contract.
2. Add a `rebalance.bin` namespace-lock guarded check/init/start path.
3. Elect or require a single admin coordinator for distributed rebalance starts.

### Implementation Steps

- [ ] Document the current R13 guarantee and the remaining distributed race model.
- [ ] Propose the namespace-lock guarded start design with failure/rollback semantics.
- [ ] Do not change start behavior until the design is approved.
- [ ] Run:

```bash
cargo fmt --all --check
```

### Acceptance Criteria

- The stronger coordinator model is scoped and reviewable before code changes.
- P1 stop/reload fixes are not blocked by this broader compatibility design.

### Commit

```bash
git add docs/architecture/rebalance-decommission-followup-review-plan.md
git commit -m "docs(rebalance): plan coordinator start semantics"
```

---

## R26: Design Strict Query Parsing for Dangerous Admin APIs

### Original Fix

Admin API hardening follow-up.

### Finding

Dangerous admin API query parameters can silently fall back when misspelled or unknown. Tightening this behavior may be compatibility-visible, so it should be designed and rolled out with clear endpoint coverage and error semantics.

### Files

- Modify: `docs/architecture/rebalance-decommission-followup-review-plan.md`
- Code changes only after endpoint scope is approved.

### Design

Define strict parsing for high-risk admin APIs:

1. Identify rebalance/decommission endpoints with destructive or stateful side effects.
2. Define allowed query keys and unknown-key errors.
3. Preserve compatibility for read-only status endpoints unless explicitly approved.
4. Add tests for misspelled dangerous parameters.

### Implementation Steps

- [ ] Document endpoint scope and error contract.
- [ ] Do not change handler parsing until the endpoint list is approved.
- [ ] Run:

```bash
cargo fmt --all --check
```

### Acceptance Criteria

- Strict query parsing is scoped to dangerous admin APIs.
- Compatibility impact is explicit before implementation.

### Commit

```bash
git add docs/architecture/rebalance-decommission-followup-review-plan.md
git commit -m "docs(admin): plan strict query parsing"
```

---

## Follow-up Test Matrix

Run after all R01-R23 implementation tasks are complete:

```bash
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore multipart --lib
cargo test -p rustfs-ecstore metadata --lib
cargo test -p rustfs-ecstore pool_meta --lib
cargo test -p rustfs-ecstore update_rebalance_stats --lib
cargo test -p rustfs-ecstore source_cleanup --lib
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

- R13 should be completed before R01 because rollback must not stop a newer concurrent rebalance operation.
- R01 should be completed before R07 because rollback behavior and stopping status share stop semantics.
- R12 should be completed before R06 because decommission start rollback/degraded handling should use the corrected pool-meta save discipline.
- R11 should run near R06/R13 because it protects startup recovery ordering while distributed start semantics change.
- R02 should be completed before R03 because overwrite equivalence should compare the metadata that migration actually persists.
- R04 should be completed before any wider metadata hardening because it prevents newly strict decode from rejecting metadata produced by the current merge path.
- R06 may require a product decision between rollback and durable degraded state. If that decision cannot be made during implementation, stop before code changes and record the trade-off.
- R14 is a proof task first. If it demonstrates data loss or MinIO-incompatible behavior, create a separate implementation task instead of hiding behavior changes inside the test task.
- R15 is documentation/test-only for the current single-pool contract. MinIO-like queued multi-pool decommission requires a separate product-approved task.
- R18-R21 are data-safety fixes and should be completed before R22/R23 stop convergence unless a stop-specific regression is blocking the cluster.
- R22 should precede R23 because terminal reload cancellation is the actual convergence mechanism; R23 makes partial failure visible.
- R24-R26 are compatibility/design tasks. Do not mix their code changes into P1 data-safety commits without separate product approval.
