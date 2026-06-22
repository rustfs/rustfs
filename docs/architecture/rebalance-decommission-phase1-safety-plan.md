# Rebalance and Decommission Phase 1 Safety Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the highest-risk rebalance/decommission safety defects so object-version semantics and distributed operation state cannot silently diverge.

**Architecture:** Phase 1 keeps the existing ECStore/Rebalance/PoolMeta architecture and introduces narrowly scoped behavior changes. The plan favors fail-closed semantics for unsafe migration, peer propagation, and startup conflicts. Each fix block should be implemented as a separate PR unless a shared helper is explicitly called out.

**Tech Stack:** Rust, Tokio, ECStore, Axum admin handlers, tonic peer RPC, MessagePack metadata, existing RustFS test modules.

---

## Scope

This plan covers:

- F01: rebalance delete marker and remote tiered version migration semantics.
- F02: decommission `DataMovementOverwriteErr` cleanup safety.
- F03: decommission `pool_meta` reload as a start barrier.
- F04: store startup recovery ordering for pool meta and rebalance.
- F05: rebalance distributed start/stop semantics and `stopping` visibility.

This plan does not cover multipart streaming, checksum preservation, lifecycle-expired cleanup, overwrite convergence, cleanup warning UX, audit fields, or metadata decoding hardening. Those belong to later phase plans.

## Shared Principles

- Treat unknown target equivalence as unsafe.
- Do not clean a source entry unless the target state is proven complete or the version is intentionally skipped by a documented policy.
- Do not report distributed admin success when required peers failed.
- Do not auto-run rebalance and decommission together after restart.
- Prefer small local helpers near existing logic over broad refactors.

## F01: Rebalance Delete Marker and Remote Tiered Version Safety

### Decision Needed

Remote tiered versions need a product decision before coding:

- **Recommended short-term behavior:** Match MinIO and skip remote tiered versions during rebalance. This avoids changing remote-tier metadata during a balancing operation and removes the immediate source-cleanup data-loss risk.
- **Later behavior:** Implement explicit cross-pool target metadata movement for remote tiered versions only after tests prove remote references, lifecycle state, and cleanup semantics.

Delete markers should not be skipped. They preserve versioned delete semantics and must be copied to the selected target pool before source cleanup.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Possibly modify: `crates/ecstore/src/data_movement.rs`
- Avoid unless necessary: `crates/ecstore/src/set_disk.rs`
- Test: existing rebalance unit tests in `crates/ecstore/src/rebalance.rs`

### Design

1. Add an ECStore-level helper for rebalance delete marker movement.
   - Input: source pool index, bucket, object name, source `FileInfo`, version ID.
   - Behavior: choose a target pool using the same placement rules as normal data movement, excluding source/rebalancing/decommissioned pools.
   - Write the delete marker metadata to the target pool with the original version ID, mod time, delete marker flag, and replication state.
   - Return success only when the target write succeeds or an equivalent target delete marker is confirmed.

2. Change the delete marker branch in `migrate_entry_version_with_retry_wait`.
   - Do not call `SetDisks::delete_object_for_migration` for rebalance delete markers.
   - Call the ECStore-level helper via the `transfer`/backend abstraction or split delete marker handling out of the `SetDisks` backend path.
   - Do not set `moved = true` unless the target helper succeeds.

3. Change remote tiered branch.
   - Short term: return `ignored = true`, `cleanup_ignored = false`, `moved = false` for remote tiered versions so they do not contribute to full source cleanup.
   - If skipping a remote tiered version prevents source cleanup, record a clear status reason so operators understand why the entry remains.

4. Source cleanup must remain blocked if any version was skipped without cleanup permission.

### Implementation Tasks

- [ ] Write a failing test proving a rebalance delete marker currently does not require target-pool metadata before counting complete.
- [ ] Write a failing test proving remote tiered versions do not allow source cleanup under the short-term skip policy.
- [ ] Implement the delete marker target write helper.
- [ ] Replace the rebalance delete marker source-set branch with the target write helper.
- [ ] Replace the remote tiered branch with the skip-without-cleanup policy.
- [ ] Run focused rebalance tests.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore rebalance_delete_marker --lib
cargo test -p rustfs-ecstore remote_tier --lib
cargo test -p rustfs-ecstore rebalance_entry --lib
```

### Acceptance Criteria

- Rebalance does not clean a source entry when a delete marker target write fails.
- Rebalance preserves versioned delete behavior after source cleanup.
- Remote tiered versions are either safely skipped without source cleanup or explicitly moved with verified target metadata.
- Existing non-delete-marker rebalance behavior remains unchanged.

### Risks

- The existing `MigrationBackend` trait is source-set oriented. If adapting it becomes invasive, prefer an ECStore-specific path in `rebalance_entry` rather than widening the trait for one branch.
- Skipping remote tiered versions may leave more source entries behind. That is safer than deleting unverified metadata and can be improved in a later PR.

---

## F02: Decommission `DataMovementOverwriteErr` Cleanup Safety

### Decision

`DataMovementOverwriteErr` must not be cleanup-safe by default. It can count as complete only after explicit target equivalence verification.

### Files

- Modify: `crates/ecstore/src/pools.rs`
- Possibly modify: `crates/ecstore/src/data_movement.rs`
- Test: decommission tests in `crates/ecstore/src/pools.rs`

### Design

1. Update delete marker and remote tiered decommission error handling.
   - Keep object-not-found and version-not-found handling as cleanup-safe only when existing semantics justify it.
   - Remove `is_err_data_movement_overwrite(&err)` from branches that set `cleanup_ignored = true`.

2. Add a narrow equivalence helper only if an existing target version can be inspected cheaply.
   - Compare bucket, object, version ID, delete marker state, ETag where applicable, mod time, and key metadata.
   - If comparison cannot be done reliably in the current code path, treat overwrite as failure for Phase 1.

3. Preserve source entry on unsafe overwrite.
   - Set `failure = true`.
   - Include a clear error stage in logs/status.
   - Do not increment the decommissioned count.

### Implementation Tasks

- [ ] Write a failing unit test for delete marker decommission where `DataMovementOverwriteErr` must not count complete.
- [ ] Write a failing unit test for remote tiered decommission where `DataMovementOverwriteErr` must not count complete.
- [ ] Remove `is_err_data_movement_overwrite` from cleanup-safe conditions in those branches.
- [ ] Add an explicit helper or comment documenting why overwrite is unsafe without equivalence.
- [ ] Run focused decommission tests.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore DataMovementOverwriteErr --lib
```

### Acceptance Criteria

- `DataMovementOverwriteErr` does not set `cleanup_ignored = true` by default.
- Source cleanup does not run when overwrite equivalence is unknown.
- Logs make unsafe overwrite distinguishable from not-found cleanup-safe cases.

### Risks

- Tightening this behavior may cause more decommission retries/failures in clusters that previously masked unsafe state. That is intended; status must make the failure actionable.

---

## F03: Decommission Pool Meta Reload Barrier

### Decision

A successful decommission start must mean all required peers have acknowledged the updated pool meta or the API must return an explicit failure/degraded result.

### Files

- Modify: `crates/ecstore/src/pools.rs`
- Modify: `crates/ecstore/src/notification_sys.rs`
- Modify: `rustfs/src/admin/handlers/pools.rs`
- Possibly modify: `crates/ecstore/src/rpc/peer_rest_client.rs`
- Possibly modify: `rustfs/src/storage/rpc/node_service.rs`

### Design

1. Change `start_decommission` propagation behavior.
   - After `pool_meta.save`, call `reload_pool_meta`.
   - If peer reload returns an aggregate error, return that error to the admin handler.
   - Do not silently proceed.

2. Decide rollback behavior.
   - Preferred minimal Phase 1: fail the API and leave persisted `pool_meta` in decommission state only if rollback is unsafe or unavailable; status must show reload failure.
   - Stronger option: persist a reverted pool meta before returning failure. This needs careful lock and persistence review and should not be attempted without tests.

3. Ensure decommission workers do not spawn after reload failure.
   - If the caller starts workers after `start_decommission` returns, returning `Err` is sufficient.
   - If any background path can resume from persisted metadata after a failed reload, status must clearly show pending/degraded state.

### Implementation Tasks

- [ ] Write a failing test for `start_decommission` with peer reload failure.
- [ ] Change reload failure from `warn` to returned error.
- [ ] Update admin handler error mapping so clients do not receive success.
- [ ] Add status/log context for peer reload failure.
- [ ] Run focused pool/decommission tests.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore start_decommission --lib
cargo test -p rustfs-ecstore reload_pool_meta --lib
cargo test -p rustfs decommission --lib
```

### Acceptance Criteria

- Admin start decommission does not return success when peer reload fails.
- Decommission workers do not start after reload failure through the admin path.
- Failure output includes enough peer context for operators.

### Risks

- Persisted pool meta may already be updated before reload failure is detected. Avoid adding rollback until the exact persistence safety is reviewed. Fail-closed reporting is the minimum safe fix.

---

## F04: Store Init Recovery Ordering

### Decision

Store init must load and install pool meta before deciding whether rebalance can auto-start.

### Files

- Modify: `crates/ecstore/src/store/init.rs`
- Possibly modify: `crates/ecstore/src/rebalance.rs`
- Possibly modify: `crates/ecstore/src/pools.rs`

### Design

1. Reorder init sequence.
   - Initialize boot time.
   - Load pool meta.
   - Validate and install pool meta.
   - Resolve resumable decommission pools.
   - Load rebalance meta.
   - Start rebalance only if no decommission is active or resumable.

2. Define conflict behavior.
   - If active decommission and started rebalance metadata coexist, decommission wins for Phase 1.
   - Rebalance should not start.
   - Record or expose a clear deferred/conflict reason if existing status structures can carry it without broad schema changes.

3. Preserve existing happy path.
   - Clusters with only rebalance metadata should still auto-start rebalance.
   - Clusters with only decommission metadata should still resume decommission after the existing delay.

### Implementation Tasks

- [ ] Write a failing init test for active decommission plus rebalance metadata.
- [ ] Move pool meta load/validate/install before `load_rebalance_meta` and `start_rebalance`.
- [ ] Add a helper such as `should_auto_start_rebalance_after_init`.
- [ ] Ensure decommission resume still uses the installed pool meta.
- [ ] Run focused init tests.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore init --lib
cargo test -p rustfs-ecstore rebalance_meta --lib
cargo test -p rustfs-ecstore decommission --lib
```

### Acceptance Criteria

- Rebalance does not start when active decommission is loaded from pool meta.
- Rebalance still starts when no decommission is active.
- Decommission resume behavior is unchanged except that it no longer races with an already-started rebalance.

### Risks

- Init ordering can affect existing startup failure behavior. Keep error handling and stage names explicit so operators can identify which stage failed.

---

## F05: Rebalance Distributed Start/Stop Semantics

### Decision

Admin start/stop must not report plain success for partial cluster state. If stop remains asynchronous, status must expose that distinction.

### Files

- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Modify: `rustfs/src/storage/rpc/node_service.rs`
- Modify: `crates/ecstore/src/notification_sys.rs`
- Modify: `crates/ecstore/src/rpc/peer_rest_client.rs`
- Possibly modify: `crates/ecstore/src/rebalance.rs`

### Design

1. Fix peer RPC stop first.
   - `node_service::stop_rebalance` must return `success=false` or gRPC error when `store.stop_rebalance().await` fails.
   - `notification_sys.stop_rebalance` must propagate aggregate peer failure instead of warning and returning `Ok`.

2. Fix start propagation.
   - Avoid returning success from peer `load_rebalance_meta(start=true)` before local start validation has completed.
   - If fully synchronous worker startup is too invasive, split semantics:
     - metadata loaded;
     - start accepted;
     - worker running observable through status.
   - Admin start should fail when any required peer fails load/start acceptance.

3. Define rollback/degraded semantics.
   - Preferred Phase 1: return failure on propagation failure and attempt local stop if local rebalance was already started.
   - If rollback fails, return a degraded error and expose status.

4. Add status language for stop.
   - If cancellation is requested but workers may still be running, status should not claim fully stopped.
   - Minimal approach: add a `stopping` or equivalent field if existing response types allow it.

### Implementation Tasks

- [ ] Write a failing test for peer stop returning success despite local stop error.
- [ ] Update `node_service::stop_rebalance` to return failure details.
- [ ] Write a failing test for `notification_sys.stop_rebalance` aggregate peer failure.
- [ ] Update `notification_sys.stop_rebalance` to return aggregate error.
- [ ] Write a failing admin start propagation test.
- [ ] Update admin start to fail or rollback on propagation failure.
- [ ] Add status distinction for requested stop versus completed stop if response schema permits.
- [ ] Run focused admin/RPC/rebalance tests.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore stop_rebalance --lib
cargo test -p rustfs-ecstore load_rebalance_meta --lib
cargo test -p rustfs rebalance --lib
```

### Acceptance Criteria

- Peer stop failure is visible to callers.
- Admin stop fails or reports degraded state on peer failure.
- Admin start fails or reports degraded state on peer load/start failure.
- Stop status does not mislead operators while workers are still winding down.

### Risks

- API response schema changes may affect clients. Prefer backward-compatible additions where possible, and keep error responses compatible with existing admin error handling.

---

## Phase 1 Test Matrix

Run these once the individual fix tests pass:

```bash
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore pools --lib
cargo test -p rustfs rebalance --lib
cargo test -p rustfs pools --lib
cargo fmt --all --check
```

Before opening a PR with code changes:

```bash
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

After build-based verification, clean generated build artifacts to avoid unnecessary disk usage.

## Suggested PR Split

1. PR 1: F02 only. Smallest fail-closed fix.
2. PR 2: F04 only. Startup ordering and conflict tests.
3. PR 3: F03 only. Decommission peer reload barrier.
4. PR 4: F05 stop semantics first, then start semantics if review scope remains manageable.
5. PR 5: F01 delete marker fix and remote tiered skip policy.

F01 is highest risk, but it may need the most design review. F02 and F04 are good first implementation candidates because they reduce safety risk with narrower code changes.

## Open Questions Before Coding

1. For F01 remote tiered versions, should Phase 1 match MinIO and skip them during rebalance, or should RustFS implement cross-pool remote metadata movement now?
2. For F03 reload failure, should the implementation attempt to rollback persisted pool meta, or fail closed and expose degraded metadata state?
3. For F05 start propagation, is a backward-compatible degraded error response acceptable, or must the API stay strictly compatible with current success/error shapes?
