# Rebalance and Decommission Remediation Plan

> This document turns `docs/architecture/expert-review-analysis.md` into an actionable remediation backlog. It is intentionally split into independent fix blocks so each item can be analyzed, assigned, implemented, and verified separately.

## Scope

This plan covers the confirmed and calibrated issues from the rebalance/decommission expert review:

- Critical and high-risk data integrity issues.
- Distributed state propagation and recovery semantics.
- Data movement resource usage and metadata preservation.
- Operational visibility, auditability, and hardening gaps.

This file is not a code patch. Each block below should be expanded into a focused implementation plan before code changes begin.

## Priority Order

| Order | Fix ID | Source | Priority | Main Risk |
| --- | --- | --- | --- | --- |
| 1 | F01 | P1.1 | Critical | Rebalance may drop delete marker or remote tiered metadata |
| 2 | F02 | P1.5 | High | Decommission may treat unsafe overwrite as cleanup-safe |
| 3 | F03 | P1.4 | High | Retiring pool may remain writable on stale peers |
| 4 | F04 | P1.2 | High | Rebalance and decommission may both resume after restart |
| 5 | F05 | P1.3, A4 | High | Admin start/stop may report success for partial cluster state |
| 6 | F06 | P2.1 | High | Multipart migration can allocate whole parts in memory |
| 7 | F07 | P2.2, A3 | Medium | Migrated objects may lose checksum or other metadata semantics |
| 8 | F08 | P2.4 | Medium | Decommission leaves lifecycle-expired source entries behind |
| 9 | F09 | P2.3 | Medium | Rebalance overwrite races may fail instead of converging |
| 10 | F10 | A2 | Medium | Source cleanup lacks a final version-set guard |
| 11 | F11 | A1 | Medium | Rebalance cleanup failures are completion warnings only |
| 12 | F12 | P3.2 | Low | Admin operations are not sufficiently auditable |
| 13 | F13 | P3.3 | Low | Persisted metadata accepts unknown or inconsistent fields |
| 14 | F14 | P3.4 | Low | Rebalance completion threshold differs from MinIO |

## Phase 1: Data Integrity and Safety

### F01: Fix rebalance delete marker and remote tiered version migration

**Source:** P1.1  
**Decision:** Confirmed fix required.  
**Target priority:** Critical.

**Problem:** `rebalance_entry` marks delete markers and remote tiered versions as moved, but those branches operate through the source `SetDisks` path rather than a confirmed cross-pool target write. Source cleanup can then remove the only metadata that preserves tombstone or tiered-object semantics.

**Primary files:**
- `crates/ecstore/src/rebalance.rs`
- `crates/ecstore/src/data_movement.rs`
- `crates/ecstore/src/set_disk.rs`
- Tests near existing rebalance migration tests in `crates/ecstore/src/rebalance.rs`

**Preferred fix direction:**
- Route rebalance delete marker movement through an `ECStore`-level data movement path that chooses a non-source target pool and writes the exact version metadata to that target.
- For remote tiered versions, either:
  - align with MinIO and skip tiered versions during rebalance, or
  - implement a target-pool metadata move that preserves the remote tier pointer and lifecycle state.
- Do not count the version as rebalanced until the target metadata is confirmed.

**Acceptance criteria:**
- A versioned object with a delete marker remains deleted after rebalance source cleanup.
- A remote tiered version remains readable or correctly listed after rebalance source cleanup.
- Rebalance does not mark delete marker or tiered versions as complete when target metadata was not written.

**Required tests:**
- Multi-pool rebalance test for a versioned object with a delete marker.
- Multi-pool rebalance test for a remote tiered version.
- Failure test where target metadata write fails and source cleanup must not happen.

**Dependencies:** None. This is the first safety fix.

---

### F02: Stop treating `DataMovementOverwriteErr` as cleanup-safe in decommission

**Source:** P1.5  
**Decision:** Confirmed fix required.  
**Target priority:** High.

**Problem:** Decommission treats `DataMovementOverwriteErr` like object-not-found/version-not-found and sets `cleanup_ignored = true`. That error only means source and destination pool are the same; it does not prove an equivalent target version exists.

**Primary files:**
- `crates/ecstore/src/pools.rs`
- `crates/ecstore/src/data_movement.rs`
- `crates/ecstore/src/store/object.rs`

**Preferred fix direction:**
- Remove `DataMovementOverwriteErr` from cleanup-safe branches for delete marker and remote tiered decommission paths.
- If overwrite occurs because an equivalent target version already exists, perform an explicit equivalence check before counting the version complete.
- Otherwise return a migration failure and keep the source entry.

**Acceptance criteria:**
- `DataMovementOverwriteErr` does not increment the decommissioned count unless equivalence is proven.
- Source cleanup is blocked when target equivalence is unknown.
- Logs/status clearly state whether overwrite was equivalent-complete or unsafe.

**Required tests:**
- Delete marker decommission returns `DataMovementOverwriteErr` and does not clean source.
- Remote tiered decommission returns `DataMovementOverwriteErr` and does not clean source.
- Equivalent target version exists; overwrite can be counted complete only after metadata equality check passes.

**Dependencies:** F01 clarifies remote/tombstone movement semantics, but F02 can be implemented independently for decommission.

---

### F03: Make decommission `pool_meta` reload a cluster-wide start barrier

**Source:** P1.4  
**Decision:** Confirmed fix required.  
**Target priority:** High.

**Problem:** `start_decommission` saves pool meta and then calls peer reload. Reload failure is logged but the operation still returns success, leaving stale peers able to write into the retiring pool.

**Primary files:**
- `crates/ecstore/src/pools.rs`
- `crates/ecstore/src/notification_sys.rs`
- `crates/ecstore/src/rpc/peer_rest_client.rs`
- `rustfs/src/admin/handlers/pools.rs`
- `rustfs/src/storage/rpc/node_service.rs`

**Preferred fix direction:**
- Treat reload failure as a failed start unless the system has an explicit partial/degraded operation state.
- Return peer failure information to the admin handler.
- Do not spawn or resume decommission workers until required peers acknowledge the updated pool meta.

**Acceptance criteria:**
- Admin decommission start fails or returns a clearly degraded response when peer reload fails.
- No peer can select the retiring pool for new writes after decommission start is reported successful.
- Status exposes peer reload failure details.

**Required tests:**
- Unit test for `start_decommission` with mocked `reload_pool_meta` failure.
- RPC/admin test showing start does not return plain success on peer failure.
- Write-placement test verifying retiring pools are skipped only after successful cluster reload.

**Dependencies:** None.

---

### F04: Reorder store init recovery to restore pool meta before rebalance

**Source:** P1.2  
**Decision:** Confirmed fix required.  
**Target priority:** High.

**Problem:** Store init loads and starts rebalance before loading persisted pool meta. If unfinished decommission exists on disk, `start_rebalance` can miss it and both processes may resume.

**Primary files:**
- `crates/ecstore/src/store/init.rs`
- `crates/ecstore/src/rebalance.rs`
- `crates/ecstore/src/pools.rs`

**Preferred fix direction:**
- Load and install `PoolMeta` before attempting to start rebalance.
- After pool meta is loaded, explicitly decide:
  - if decommission is active, do not auto-start rebalance;
  - if rebalance metadata exists but conflicts with decommission, mark rebalance as blocked/deferred and expose it in status.
- Keep existing decommission resume delay, but ensure rebalance is not already running when decommission resumes.

**Acceptance criteria:**
- Restart with active decommission metadata and rebalance metadata does not run both workers.
- Startup status explains which operation was deferred and why.
- Existing startup without decommission still resumes rebalance normally.

**Required tests:**
- Init test with persisted active decommission plus started rebalance metadata.
- Init test with only rebalance metadata still resumes rebalance.
- Init test with corrupt or missing pool meta follows existing error policy.

**Dependencies:** None, but coordinate with F05 status semantics.

---

### F05: Make rebalance start/stop distributed semantics explicit and fail-safe

**Source:** P1.3, A4  
**Decision:** Confirmed fix required.  
**Target priority:** High.

**Problem:** Rebalance start can return success even when peer propagation fails or peer background start later fails. Stop can return success while peer stop failed or local stop errors were ignored. Stop is also asynchronous, but admin semantics do not clearly expose `stopping` versus `stopped`.

**Primary files:**
- `rustfs/src/admin/handlers/rebalance.rs`
- `rustfs/src/storage/rpc/node_service.rs`
- `crates/ecstore/src/notification_sys.rs`
- `crates/ecstore/src/rpc/peer_rest_client.rs`
- `crates/ecstore/src/rebalance.rs`

**Preferred fix direction:**
- For start:
  - propagate rebalance metadata to peers before returning success;
  - have peer RPC synchronously validate/start or return a structured failure;
  - on partial failure, rollback local start or report a degraded state.
- For stop:
  - never ignore `store.stop_rebalance()` errors;
  - aggregate peer stop failures and return them to admin;
  - expose `stop_requested`, `stopping`, and `stopped` in status if worker shutdown remains asynchronous.

**Acceptance criteria:**
- Admin start does not return plain success when any required peer fails to load/start.
- Admin stop does not return plain success when any required peer fails to stop.
- Status can distinguish a requested stop from all workers fully stopped.

**Required tests:**
- Peer `load_rebalance_meta(true)` failure.
- Peer background `start_rebalance` failure.
- Peer `stop_rebalance` local save failure.
- Stop requested while a worker is in a long migration operation.

**Dependencies:** Coordinate with F04 so startup and admin semantics use the same conflict model.

---

## Phase 2: Data Movement Correctness and Resource Control

### F06: Stream multipart data movement instead of buffering whole parts

**Source:** P2.1  
**Decision:** Confirmed fix required.  
**Target priority:** High.

**Problem:** Multipart migration allocates `Vec<u8>` for each part and reads the whole part into memory. Large parts plus concurrent workers can OOM the process.

**Primary files:**
- `crates/ecstore/src/data_movement.rs`
- `crates/ecstore/src/store/multipart.rs`
- `crates/ecstore/src/store/object.rs`

**Preferred fix direction:**
- Replace whole-part buffering with bounded streaming.
- Preserve the current part index and checksum/etag behavior through the streaming reader.
- Ensure multipart abort still runs on any failed part or complete call.

**Acceptance criteria:**
- Migrating a large multipart object does not allocate memory proportional to part size.
- Part ETag, size, actual size, and index handling remain correct.
- Failed migration aborts the temporary multipart upload.

**Required tests:**
- Fake large multipart reader proving memory is bounded.
- Multipart migration preserves part order and ETags.
- Failure during part upload aborts the multipart upload.

**Dependencies:** Coordinate with F07 for checksum preservation.

---

### F07: Preserve and verify full data movement metadata

**Source:** P2.2, A3  
**Decision:** Confirmed fix required for checksum; additional metadata requires verification and likely fixes.  
**Target priority:** Medium.

**Problem:** Data movement preserves basic metadata but does not fully prove checksum, multipart checksum, replication state, version purge status, or object-lock metadata equivalence after migration.

**Primary files:**
- `crates/ecstore/src/data_movement.rs`
- `crates/ecstore/src/store_api/types.rs`
- `crates/ecstore/src/set_disk.rs`
- `crates/filemeta/src/fileinfo.rs`
- `crates/filemeta/src/replication.rs`

**Preferred fix direction:**
- For multipart migration, populate `CompletePart` checksum fields when source part checksum exists.
- Preserve or reconstruct object-level checksum metadata.
- Add migration equivalence checks for:
  - ETag;
  - checksum and multipart checksum;
  - replication status and version purge status;
  - object lock retention mode/date and legal hold;
  - version ID and mod time.
- Only change production metadata handling after tests show a real loss path.

**Acceptance criteria:**
- Migrated object metadata matches source for the fields above.
- `GetObjectAttributes` checksum behavior remains correct after migration.
- Object lock retention/legal hold remains unchanged across migration.

**Required tests:**
- Single-part object with checksum metadata.
- Multipart object with per-part checksum metadata.
- Object with replication state and version purge state.
- Object with governance/compliance retention and legal hold.

**Dependencies:** F06 should land first or in the same PR if checksum streaming needs shared reader changes.

---

### F08: Align decommission lifecycle-expired version cleanup semantics

**Source:** P2.4  
**Decision:** Confirmed fix required or explicit design decision required.  
**Target priority:** Medium.

**Problem:** Decommission skips lifecycle-expired versions but does not clean the source entry if any expired version exists. MinIO counts expired versions as completed and can clean the source entry.

**Primary files:**
- `crates/ecstore/src/pools.rs`
- Lifecycle evaluator code under `crates/ecstore/src/bucket/lifecycle/`

**Preferred fix direction:**
- Treat lifecycle-expired versions as completed for decommission source cleanup when they are not required to remain accessible.
- Keep lifecycle/object-lock/replication checks in place before treating a version as expired.
- If RustFS intentionally keeps expired source versions, expose that state in status and exclude the pool from being considered fully clean.

**Acceptance criteria:**
- Decommission can clean a source entry when all remaining versions are either migrated or lifecycle-expired.
- Object-lock or replication-protected versions are not wrongly treated as cleanup-safe.
- Status accurately reflects residual expired-source entries if they are intentionally retained.

**Required tests:**
- Bucket with one migrated version plus one lifecycle-expired version.
- Expired version protected by object lock must not be cleaned.
- Replication-pending version must not be treated as cleanup-safe.

**Dependencies:** Coordinate with F07 metadata tests for object lock and replication.

---

### F09: Handle rebalance overwrite races using explicit equivalence checks

**Source:** P2.3  
**Decision:** Fix likely required; first confirm exact race cases with tests.  
**Target priority:** Medium.

**Problem:** RustFS treats `DataMovementOverwriteErr` as non-transient during rebalance, while MinIO ignores acceptable overwrite races. A strict failure can prevent rebalance convergence when the target already has an equivalent version.

**Primary files:**
- `crates/ecstore/src/rebalance.rs`
- `crates/ecstore/src/data_movement.rs`
- `crates/ecstore/src/store/object.rs`

**Preferred fix direction:**
- Keep unsafe source-equals-target cases as failures.
- If the target version already exists, compare version ID, ETag, size, mod time, checksum, and key metadata.
- Count as complete only when equivalence passes.
- Add last-error/status text when overwrite is unsafe.

**Acceptance criteria:**
- Equivalent target version allows rebalance to continue.
- Non-equivalent target version fails and does not clean source.
- Behavior is documented as compatible with MinIO's acceptable overwrite race handling.

**Required tests:**
- Rebalance overwrite with equivalent target version.
- Rebalance overwrite with mismatched checksum or metadata.
- Rebalance overwrite where source and target pool are the same and no equivalent target exists.

**Dependencies:** F07 defines the metadata equivalence fields.

---

### F10: Add source cleanup preflight verification before deleting migrated entries

**Source:** A2  
**Decision:** Fix recommended as defense-in-depth.  
**Target priority:** Medium.

**Problem:** After versions are migrated, source cleanup deletes the source prefix based on the version list read before migration. A final verification would protect against state propagation failures, recovery races, or repair processes changing metadata before cleanup.

**Primary files:**
- `crates/ecstore/src/rebalance.rs`
- `crates/ecstore/src/pools.rs`
- `crates/ecstore/src/set_disk.rs`

**Preferred fix direction:**
- Before source cleanup, re-read source metadata or a generation marker.
- Verify the version set scheduled for deletion equals the version set that was migrated or intentionally ignored.
- If the version set changed, defer/retry the entry instead of deleting the prefix.

**Acceptance criteria:**
- Cleanup does not delete versions created or discovered after the migration scan.
- Changed source metadata causes a retry/defer state with a clear status error.
- The guard applies to rebalance and decommission where source prefix deletion is used.

**Required tests:**
- Simulate source metadata changing between migration and cleanup.
- Simulate peer state mismatch where a write lands on a rebalancing/decommissioning pool.
- Confirm unchanged metadata still allows cleanup.

**Dependencies:** Works best after F03 and F05 reduce state propagation failures.

---

## Phase 3: Cleanup Semantics, Observability, and Hardening

### F11: Improve rebalance cleanup failure handling and reporting

**Source:** A1  
**Decision:** Fix recommended.  
**Target priority:** Medium.

**Problem:** Rebalance source cleanup failure is converted to a warning and the task can still complete. Only a count and last warning are preserved.

**Primary files:**
- `crates/ecstore/src/rebalance.rs`
- `rustfs/src/admin/handlers/rebalance.rs`
- Rebalance status DTOs and serialization paths

**Preferred fix direction:**
- Preserve a bounded list of cleanup failures, not only the last one.
- Add a clear terminal or partial terminal state when cleanup warnings exist.
- Decide whether cleanup failure should:
  - defer the entry for retry, or
  - allow completion but mark the pool as `completed_with_cleanup_warnings`.

**Acceptance criteria:**
- Admin status reports cleanup warning count and representative object list.
- Operators can identify which objects need manual cleanup or retry.
- Rebalance terminal state is not misleading when source data remains.

**Required tests:**
- Multiple cleanup failures preserve count and bounded object details.
- Status response includes cleanup warning data.
- Completion state reflects cleanup warnings.

**Dependencies:** Coordinate with F10 if cleanup failures become retryable.

---

### F12: Add structured audit fields for rebalance and decommission operations

**Source:** P3.2  
**Decision:** Fix recommended.  
**Target priority:** Low.

**Problem:** Critical admin operations do not consistently log actor, remote address, request ID, pool indices, peer host, result, and partial failure state.

**Primary files:**
- `rustfs/src/admin/handlers/rebalance.rs`
- `rustfs/src/admin/handlers/pools.rs`
- `crates/ecstore/src/notification_sys.rs`
- `crates/ecstore/src/rpc/peer_rest_client.rs`
- Logging guardrail scripts if applicable

**Preferred fix direction:**
- Standardize structured fields for start, stop, cancel, status-affecting propagation, and peer RPC failures.
- Mask sensitive actor material.
- Use warn/error for partial failure rather than info-only logs.

**Acceptance criteria:**
- Start/stop/cancel logs include actor, request ID, pool/rebalance ID, peer, result, and error when present.
- Logs do not expose secrets.
- Partial failures are searchable by stable event fields.

**Required tests:**
- Log-capture tests for success, rejection, and partial failure.
- Guardrail check if logging conventions are enforced by script.

**Dependencies:** F03 and F05 define partial-failure semantics.

---

### F13: Harden persisted rebalance and pool metadata decoding

**Source:** P3.3  
**Decision:** Fix recommended after compatibility review.  
**Target priority:** Low.

**Problem:** Persisted rebalance and pool metadata can accept unknown fields silently. This may be acceptable for compatibility, but it weakens corruption and typo detection.

**Primary files:**
- `crates/ecstore/src/rebalance.rs`
- `crates/ecstore/src/pools.rs`
- Metadata fixture tests near existing metadata tests

**Preferred fix direction:**
- For formats that can be strict, add `deny_unknown_fields`.
- Where backward compatibility requires leniency, detect and warn on unknown fields during legacy decode.
- Add explicit validation for conflicting terminal states.

**Acceptance criteria:**
- Unknown fields either fail decode or produce a warning through a documented compatibility path.
- Missing critical fields fail safely.
- Conflicting states such as completed plus running are rejected.

**Required tests:**
- Unknown field fixture.
- Missing critical field fixture.
- Conflicting terminal state fixture.
- Legacy fixture proving backward compatibility.

**Dependencies:** None.

---

### F14: Decide and document rebalance completion threshold behavior

**Source:** P3.4  
**Decision:** Design decision required.  
**Target priority:** Low.

**Problem:** RustFS uses a stricter rebalance completion goal than MinIO's tolerance-based behavior. This may increase movement work and operational time.

**Primary files:**
- `crates/ecstore/src/rebalance.rs`
- Admin status documentation if present
- Tests around rebalance goal calculation

**Preferred fix direction:**
- Decide whether RustFS should:
  - match MinIO tolerance;
  - keep strict behavior and document it;
  - make tolerance configurable with a conservative default.
- Avoid configurability unless a real operational requirement exists.

**Acceptance criteria:**
- Completion behavior is intentional and covered by tests.
- Status/ETA reporting does not imply MinIO-compatible tolerance if strict behavior remains.
- Large-pool behavior is covered by a deterministic unit test.

**Required tests:**
- Rebalance goal just below target.
- Rebalance goal within MinIO-like tolerance.
- Empty queue completion path remains valid.

**Dependencies:** None.

---

## Cross-Cutting Test Matrix

Each fix should add focused tests, but the following end-to-end scenarios should also be covered before enabling production use:

1. Rebalance a versioned object with normal versions, delete marker, and source cleanup.
2. Rebalance a remote tiered version or verify it is intentionally skipped.
3. Decommission a pool while peer reload fails.
4. Restart with both active decommission metadata and rebalance metadata.
5. Stop rebalance while a large multipart migration is in progress.
6. Migrate a large multipart object without memory proportional to part size.
7. Migrate objects with checksum, replication state, retention, and legal hold metadata.
8. Trigger source cleanup failure and verify admin status exposes it.
9. Trigger overwrite race with equivalent and non-equivalent target versions.
10. Decommission lifecycle-expired versions with object-lock and replication constraints.

## Suggested Execution Strategy

1. Start with F01 and F02 because they protect object version semantics.
2. Implement F03, F04, and F05 before broader rollout because they define distributed operation safety.
3. Implement F06 before running large-scale data movement tests.
4. Implement F07 through F11 as compatibility, convergence, and observability hardening.
5. Keep F12 through F14 as final hardening unless release criteria require them earlier.

Each fix should be developed in a separate PR unless two adjacent fixes share the same core implementation. In particular:

- F01 and F09 should not be merged together unless the equivalence helper is clearly isolated.
- F06 and F07 may be combined only if streaming part migration and checksum preservation share the same reader changes.
- F03 and F05 may share peer failure aggregation utilities, but their admin behavior should be tested separately.

