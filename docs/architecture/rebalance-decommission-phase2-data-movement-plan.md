# Rebalance and Decommission Phase 2 Data Movement Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make shared data movement safe for large objects, metadata-preserving, and resilient to overwrite and cleanup races.

**Architecture:** Phase 2 keeps `crates/ecstore/src/data_movement.rs` as the shared migration core, but tightens its streaming, metadata, and equivalence behavior. Rebalance and decommission should share small helpers where their safety checks are identical, while keeping operation-specific policy decisions in `rebalance.rs` and `pools.rs`.

**Tech Stack:** Rust, Tokio async readers, ECStore object APIs, multipart APIs, lifecycle evaluator, filemeta metadata types, existing crate-local unit tests.

---

## Scope

This plan covers:

- F06: stream multipart data movement instead of buffering whole parts.
- F07: preserve and verify checksum, replication, version purge, and object-lock metadata.
- F08: align or explicitly define decommission lifecycle-expired cleanup behavior.
- F09: handle rebalance overwrite races with explicit target equivalence checks.
- F10: add final source cleanup verification before prefix deletion.

This plan assumes Phase 1 either has landed or its semantics are stable enough to build on.

## Shared Principles

- Streaming must keep memory bounded by buffer size, not part size.
- Metadata equivalence should be explicit and testable.
- A version is complete only when copied, intentionally skipped by policy, or proven equivalent on target.
- Source cleanup must be the last step and must verify the source version set has not changed.
- Fail closed when equivalence cannot be proven.

## F06: Stream Multipart Data Movement

### Decision

Multipart migration must not allocate a buffer equal to `part.size`. It should stream each part into `put_object_part`.

### Files

- Modify: `crates/ecstore/src/data_movement.rs`
- Possibly modify: `crates/ecstore/src/store/multipart.rs`
- Possibly modify: `crates/ecstore/src/store_api/readers.rs`
- Test: data movement tests in `crates/ecstore/src/data_movement.rs`

### Design

1. Replace per-part `Vec<u8>` allocation.
   - Current behavior reads each part with `read_exact` into `chunk`.
   - New behavior should create a bounded reader over the shared object stream for exactly `part.size` bytes.

2. Preserve part boundaries.
   - Each `put_object_part` call must receive a reader that ends exactly at the part boundary.
   - If a part stream ends early, return a staged `read_part` error and abort the multipart upload.

3. Preserve part index behavior.
   - Continue decoding `part.index`.
   - Preserve indexed reader behavior so erasure/indexed metadata remains compatible.

4. Preserve abort behavior.
   - `abort_multipart_upload` must still run if any part upload or complete call fails before completion is marked.

### Implementation Tasks

- [ ] Add a failing data movement test with a fake large multipart stream that panics or fails if code tries to allocate/read the full part into memory.
- [ ] Add a test proving part boundary reads consume exactly the configured part sizes.
- [ ] Introduce a bounded async reader helper for a single multipart part.
- [ ] Replace `vec![0u8; part.size]` and `read_exact` in multipart migration with the bounded reader helper.
- [ ] Ensure part upload still wraps data in `PutObjReader` with correct size, actual size, and index.
- [ ] Keep existing abort-on-error behavior and add a focused test for failure during streamed part upload.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore multipart --lib
```

### Acceptance Criteria

- Data movement memory is bounded and not proportional to multipart part size.
- Multipart part boundaries, sizes, ETags, and indexes remain correct.
- Failed streamed part upload aborts the temporary multipart upload.

### Risks

- Async reader composition can accidentally over-read into the next part. The part-boundary test is mandatory.
- If `put_object_part` requires a concrete reader type, introduce the smallest adapter needed rather than rewriting multipart internals.

---

## F07: Preserve and Verify Full Data Movement Metadata

### Decision

Checksum preservation is a confirmed fix. Replication, version purge, and object-lock behavior must be tested first; production changes should follow only where tests reveal loss or mismatch.

### Files

- Modify: `crates/ecstore/src/data_movement.rs`
- Possibly modify: `crates/ecstore/src/store_api/types.rs`
- Possibly modify: `crates/ecstore/src/set_disk.rs`
- Possibly modify: `crates/filemeta/src/fileinfo.rs`
- Possibly modify: `crates/filemeta/src/replication.rs`
- Test: data movement tests in `crates/ecstore/src/data_movement.rs`

### Design

1. Define metadata equivalence for data movement.
   - Required fields: version ID, ETag, size, actual size, mod time, user metadata, storage class, checksum, multipart checksum, replication state, version purge state, object-lock mode/date, legal hold.
   - Use a helper local to data movement tests first. Promote to production only if F09/F10 need it.

2. Preserve multipart checksum.
   - When source part checksum exists, populate the corresponding `CompletePart` checksum fields.
   - Preserve object-level multipart checksum metadata when complete metadata is written.

3. Validate replication state preservation.
   - Current code copies `user_defined` and `set_disk` reconstructs `replication_state_internal` from metadata.
   - Add tests to prove this works for replication status and version purge status.
   - If tests fail, extend `ObjectOptions` or write path metadata handling narrowly.

4. Validate object lock preservation.
   - Current code copies `user_defined`, which should carry object-lock headers.
   - Add tests for retention mode/date and legal hold.
   - If tests fail, fix the metadata copy path without changing normal user copy semantics.

### Implementation Tasks

- [ ] Add a metadata equivalence assertion helper in data movement tests.
- [ ] Add a failing multipart checksum preservation test.
- [ ] Populate `CompletePart` checksum fields from source part metadata.
- [ ] Add single-part checksum preservation test and fix object-level checksum metadata if needed.
- [ ] Add replication state and version purge state preservation tests.
- [ ] Add object-lock retention and legal hold preservation tests.
- [ ] Apply the minimal production fixes required by those tests.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore checksum --lib
cargo test -p rustfs-ecstore replication --lib
cargo test -p rustfs-ecstore object_lock --lib
```

### Acceptance Criteria

- Migrated single-part and multipart objects preserve checksum behavior.
- Migrated multipart parts preserve per-part checksum metadata where RustFS stores it.
- Replication state and version purge state are equivalent after migration.
- Object-lock retention and legal hold remain unchanged after migration.

### Risks

- Some metadata may be derived rather than stored directly. Tests should compare externally observable object info, not only internal fields.
- Avoid broad `ObjectOptions` expansion unless current metadata copy cannot preserve a required field.

---

## F08: Lifecycle-Expired Version Cleanup Semantics

### Decision

RustFS should either align with MinIO by counting lifecycle-expired versions as decommission-complete, or explicitly document and expose retained expired source entries. The recommended behavior is to align with MinIO when lifecycle/object-lock/replication checks say the version is safe to expire.

### Files

- Modify: `crates/ecstore/src/pools.rs`
- Possibly modify: lifecycle evaluator under `crates/ecstore/src/bucket/lifecycle/`
- Test: decommission tests in `crates/ecstore/src/pools.rs`

### Design

1. Preserve existing lifecycle safety checks.
   - Do not bypass object-lock or replication constraints.
   - Keep `should_skip_lifecycle_for_data_movement` or equivalent evaluator as the source of truth.

2. Count safe expired versions as complete for source cleanup.
   - Replace `expired == 0 && decommissioned == total_versions` with semantics that allow `decommissioned + expired == total_versions` when expired versions are safe.

3. Keep status honest.
   - If a version is retained because expiration is not safe, status should show it as remaining, not completed.

### Implementation Tasks

- [ ] Write a failing test where one migrated version plus one lifecycle-expired version should allow source cleanup.
- [ ] Write a test where object-lock protected expired-looking version must not allow cleanup.
- [ ] Write a test where replication-pending version must not allow cleanup.
- [ ] Update source cleanup predicate to count safe expired versions.
- [ ] Update counters/status if existing fields distinguish expired from decommissioned.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore lifecycle --lib
cargo test -p rustfs-ecstore object_lock --lib
```

### Acceptance Criteria

- Source entry is cleaned when every version is migrated or safely lifecycle-expired.
- Protected versions still block cleanup.
- Decommission status remains understandable for migrated, expired, and blocked versions.

### Risks

- Counting expired versions as complete can hide lifecycle evaluator bugs. Tests must include protected negative cases.

---

## F09: Rebalance Overwrite Race Equivalence

### Decision

Unsafe `DataMovementOverwriteErr` remains a failure. Equivalent target overwrite can be counted complete only after explicit comparison.

### Files

- Modify: `crates/ecstore/src/data_movement.rs`
- Modify: `crates/ecstore/src/rebalance.rs`
- Possibly modify: `crates/ecstore/src/store/object.rs`
- Test: data movement and rebalance tests

### Design

1. Define target equivalence helper.
   - Compare version ID, ETag, size, actual size, mod time, checksum, delete marker state, and selected user metadata.
   - Reuse the test helper from F07 if appropriate, but production helper must be limited to fields needed for safety.

2. Apply helper only to overwrite race cases.
   - If `DataMovementOverwriteErr` occurs and target pool differs from source pool, inspect target object/version.
   - If equivalent, count migration as complete.
   - If not equivalent or cannot inspect target, return failure.

3. Keep source-equals-target unsafe.
   - Do not convert source-equals-target into success without independent target proof.

### Implementation Tasks

- [ ] Add a failing rebalance test where equivalent target version exists and overwrite should converge.
- [ ] Add a failing rebalance test where target version differs and overwrite must fail.
- [ ] Implement a minimal target equivalence helper.
- [ ] Wire the helper into data movement overwrite handling for rebalance.
- [ ] Keep decommission behavior aligned with F02.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore DataMovementOverwriteErr --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs-ecstore data_movement --lib
```

### Acceptance Criteria

- Equivalent overwrite does not block rebalance convergence.
- Non-equivalent overwrite does not clean source.
- Logs/status distinguish equivalent overwrite from unsafe overwrite.

### Risks

- Comparing too few fields can mark a corrupt target complete. Prefer strict comparison in Phase 2.
- Comparing too many volatile fields can prevent convergence. Use tests to calibrate.

---

## F10: Source Cleanup Preflight Verification

### Decision

Before deleting a source prefix, rebalance and decommission should verify the source version set still matches the migrated or intentionally skipped set.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify: `crates/ecstore/src/pools.rs`
- Possibly modify: `crates/ecstore/src/set_disk.rs`
- Test: rebalance and decommission cleanup tests

### Design

1. Capture source version identity at scan time.
   - Version identity should include object name, version ID, delete marker flag, and enough metadata to detect a new or changed version.

2. Re-read before cleanup.
   - Immediately before `delete_prefix`, read current source metadata.
   - Compare current identity set to the expected cleanup set.

3. Defer on mismatch.
   - Rebalance should defer/retry the entry with a clear last error.
   - Decommission should fail the entry or retry according to existing decommission retry policy.

4. Keep not-found idempotent.
   - If source entry is already gone, cleanup remains successful.

### Implementation Tasks

- [ ] Add helper to build a stable version identity set from `FileInfoVersions`.
- [ ] Add rebalance test where source metadata changes between migration and cleanup.
- [ ] Add decommission test where source metadata changes between migration and cleanup.
- [ ] Add cleanup preflight before rebalance source `delete_prefix`.
- [ ] Add cleanup preflight before decommission source `delete_prefix`.
- [ ] Return deferred/failure status with a clear reason on mismatch.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore rebalance_entry --lib
cargo test -p rustfs-ecstore decommission_entry --lib
cargo test -p rustfs-ecstore cleanup --lib
```

### Acceptance Criteria

- Cleanup does not delete a source entry if the version set changed after migration started.
- Already-deleted source remains idempotent.
- Mismatch errors are visible in status or last-error fields.

### Risks

- Extra metadata reads can add I/O to hot migration paths. Keep this as a cleanup-only check.

---

## Phase 2 Test Matrix

Run after individual fix tests pass:

```bash
cargo test -p rustfs-ecstore data_movement --lib
cargo test -p rustfs-ecstore multipart --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs-ecstore decommission --lib
cargo test -p rustfs-ecstore lifecycle --lib
cargo fmt --all --check
```

Before PR:

```bash
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

Clean generated build artifacts after build-based verification.

## Suggested PR Split

1. PR 1: F06 streaming multipart migration.
2. PR 2: F07 checksum and metadata preservation tests/fixes.
3. PR 3: F08 lifecycle-expired cleanup semantics.
4. PR 4: F09 overwrite equivalence.
5. PR 5: F10 cleanup preflight verification.

F06 and F07 may combine only if checksum preservation requires the new streaming reader shape. F09 should wait for F07 if it depends on metadata equivalence helpers.

## Open Questions Before Coding

1. Which metadata fields are considered mandatory for equivalence in Phase 2: strict internal fields or externally observable fields only?
2. Should F08 fully match MinIO cleanup behavior, or should RustFS keep expired source entries but expose them as residual state?
3. Should F10 apply to both rebalance and decommission in the same PR, or should rebalance land first as the higher-risk cleanup path?

