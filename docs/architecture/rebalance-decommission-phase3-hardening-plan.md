# Rebalance and Decommission Phase 3 Hardening Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve operator visibility, auditability, metadata robustness, and documented compatibility after the Phase 1 and Phase 2 safety fixes.

**Architecture:** Phase 3 should not change core data movement semantics unless a hardening test exposes a safety bug. It adds bounded status data, structured logs, metadata validation tests, and an explicit decision around rebalance completion tolerance.

**Tech Stack:** Rust, tracing structured logs, admin status DTOs, serde/rmp metadata decoding, existing guardrail scripts, crate-local unit tests.

---

## Scope

This plan covers:

- F11: improve rebalance cleanup failure reporting.
- F12: add structured audit fields for rebalance/decommission operations.
- F13: harden persisted rebalance and pool metadata decoding.
- F14: decide and document rebalance completion threshold behavior.

This plan assumes the safety semantics from Phase 1 and the data movement behavior from Phase 2 are stable.

## Shared Principles

- Hardening must not hide safety failures.
- Status fields should be bounded to avoid unbounded metadata growth.
- Logs must be structured, searchable, and free of secrets.
- Metadata compatibility must be explicit: strict where possible, legacy-compatible where necessary.

## F11: Rebalance Cleanup Failure Reporting

### Decision

Rebalance cleanup failures should not be invisible completion details. Admin status should expose whether completion had cleanup warnings and provide bounded object-level detail.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Possibly modify: admin response DTOs in the same module or related madmin types
- Test: rebalance status and metadata tests

### Design

1. Extend cleanup warning metadata.
   - Keep existing count.
   - Add a bounded list of recent cleanup failures, for example last 10 entries.
   - Each entry should include bucket, object, message, and timestamp.

2. Keep metadata bounded.
   - Do not store every cleanup failure unboundedly in `rebalance.bin`.
   - Use a ring-buffer style helper or truncate older entries.

3. Make terminal state visible.
   - If rebalance completes with cleanup warnings, status should show warning count and details.
   - Avoid reporting a clean completion when source cleanup failed.

### Implementation Tasks

- [ ] Add a cleanup warning entry struct with bounded retention.
- [ ] Add metadata compatibility defaults for older `rebalance.bin` without the new list.
- [ ] Update `record_rebalance_cleanup_warning_in_meta` to append bounded entries and update count/last fields.
- [ ] Update admin status serialization to expose warning count and entries.
- [ ] Add tests for one warning, multiple warnings, and bounded truncation.
- [ ] Add legacy metadata decode test.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore cleanup_warning --lib
cargo test -p rustfs-ecstore rebalance_meta --lib
cargo test -p rustfs rebalance --lib
```

### Acceptance Criteria

- Multiple cleanup failures are visible through count and bounded details.
- Legacy rebalance metadata still decodes.
- Status distinguishes clean completion from completion with cleanup warnings.

### Risks

- Adding status fields may affect clients if response schemas are strict. Prefer additive fields with defaults.

---

## F12: Structured Audit Fields

### Decision

Rebalance and decommission admin operations should have consistent structured logs for success, rejection, propagation failure, and partial/degraded state.

### Files

- Modify: `rustfs/src/admin/handlers/rebalance.rs`
- Modify: `rustfs/src/admin/handlers/pools.rs`
- Modify: `crates/ecstore/src/notification_sys.rs`
- Modify: `crates/ecstore/src/rpc/peer_rest_client.rs`
- Possibly modify: `scripts/check_logging_guardrails.sh` or related guardrails if logging rules require updates

### Design

1. Define standard fields.
   - `event`
   - `component`
   - `subsystem`
   - `action`
   - `result`
   - `request_id` when available
   - `actor` or masked access key when available
   - `remote_addr` when available
   - `pool_indices` or `rebalance_id`
   - `peer`
   - `error`

2. Apply to admin handlers.
   - Start, stop, cancel, status-affecting operations.
   - Authorization failures should remain handled by existing auth paths but should have enough context if already logged.

3. Apply to peer propagation.
   - Peer success and failure should use stable event names.
   - Partial failure must be warn/error, not info-only.

4. Avoid secret leakage.
   - Never log raw credentials, signatures, tokens, or full auth headers.

### Implementation Tasks

- [ ] Inventory existing rebalance/decommission log events and field names.
- [ ] Define a small local helper or convention for masked actor/request fields if one already exists.
- [ ] Update rebalance admin start/stop/status logs.
- [ ] Update decommission start/cancel/status logs.
- [ ] Update notification peer propagation failure logs.
- [ ] Add log-capture tests for success, partial failure, and rejected operation where feasible.
- [ ] Run logging guardrail script if the repository has one.

### Focused Test Commands

```bash
cargo test -p rustfs rebalance --lib
cargo test -p rustfs pools --lib
cargo test -p rustfs-ecstore notification --lib
scripts/check_logging_guardrails.sh
```

### Acceptance Criteria

- Operators can answer who requested an operation, what resource it targeted, and which peers failed.
- Partial failures are searchable through stable structured fields.
- Logs contain no secrets.

### Risks

- Logging changes can be noisy. Keep high-volume per-object logs out of admin audit events.

---

## F13: Persisted Metadata Decode Hardening

### Decision

Persisted rebalance and pool metadata should reject or surface unknown/corrupt fields where compatibility allows. Legacy compatibility must be tested explicitly.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Modify: `crates/ecstore/src/pools.rs`
- Possibly add fixtures under an existing test fixture directory if one exists
- Test: metadata decode tests in `rebalance.rs` and `pools.rs`

### Design

1. Identify persisted structs.
   - Rebalance metadata and stats structs.
   - Pool metadata and decommission info structs.

2. Choose strictness per struct.
   - Use strict unknown-field rejection where metadata is not expected to carry forward-compatible fields.
   - For legacy-compatible structs, keep decode lenient but log or validate unknown/unsupported state where possible.

3. Add state validation.
   - Reject conflicting terminal states such as complete plus canceled plus running.
   - Reject impossible counters or invalid pool indices where existing validation can catch them.

4. Preserve legacy fixtures.
   - Existing old metadata must still decode if compatibility is required.

### Implementation Tasks

- [ ] List all rebalance and pool metadata structs currently deserialized from persisted bytes.
- [ ] Add fixture tests for unknown fields, missing critical fields, and conflicting terminal states.
- [ ] Add strict serde attributes only where fixtures show compatibility is safe.
- [ ] Add post-decode validation helpers for state conflicts.
- [ ] Update load paths to surface validation errors with actionable messages.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore rebalance_meta --lib
cargo test -p rustfs-ecstore pool_meta --lib
cargo test -p rustfs-ecstore metadata --lib
```

### Acceptance Criteria

- Unknown/corrupt metadata does not silently become an apparently valid operation state.
- Legacy metadata fixtures continue to decode through documented paths.
- Conflicting terminal state is rejected or quarantined.

### Risks

- Strict decode can block startup if old metadata contains benign extra fields. Start with tests and post-decode validation before broad strict attributes.

---

## F14: Rebalance Completion Threshold Decision

### Decision

RustFS must explicitly choose whether to match MinIO's tolerance-based rebalance completion or keep strict completion. The recommended default is to match MinIO unless RustFS has a documented reason to move more data.

### Files

- Modify: `crates/ecstore/src/rebalance.rs`
- Possibly modify: admin docs if present
- Test: rebalance goal/completion tests in `rebalance.rs`

### Design Options

1. **Match MinIO tolerance.**
   - Complete when pool free-space ratio is within the MinIO-like tolerance of the goal.
   - Reduces movement and operational time.
   - Best for compatibility.

2. **Keep strict behavior and document it.**
   - No code behavior change.
   - Add tests proving strict behavior is intentional.
   - Status/docs should avoid implying MinIO-compatible tolerance.

3. **Configurable tolerance.**
   - Most flexible but not recommended unless operators need it.
   - Adds configuration and support burden.

### Recommended Plan

Start with option 1 if compatibility is the goal. If maintainers prefer strictness, implement option 2 with explicit tests and documentation.

### Implementation Tasks

- [ ] Add tests that capture current strict behavior around the completion goal.
- [ ] Add tests for MinIO-like tolerance behavior.
- [ ] Confirm maintainers choose MinIO-compatible or strict behavior.
- [ ] Implement the selected threshold behavior.
- [ ] Update status/docs comments so completion semantics are clear.

### Focused Test Commands

```bash
cargo test -p rustfs-ecstore rebalance_goal --lib
cargo test -p rustfs-ecstore check_if_rebalance_done --lib
cargo test -p rustfs-ecstore rebalance --lib
```

### Acceptance Criteria

- Completion threshold behavior is covered by deterministic tests.
- Operators can understand whether RustFS matches MinIO tolerance.
- Empty-queue completion path remains valid.

### Risks

- Changing threshold may alter operational expectations for existing users. Document the change if behavior changes.

---

## Phase 3 Test Matrix

Run after individual fix tests pass:

```bash
cargo test -p rustfs-ecstore rebalance_meta --lib
cargo test -p rustfs-ecstore pool_meta --lib
cargo test -p rustfs-ecstore rebalance --lib
cargo test -p rustfs rebalance --lib
cargo test -p rustfs pools --lib
cargo fmt --all --check
```

If logging guardrails are relevant:

```bash
scripts/check_logging_guardrails.sh
```

Before PR:

```bash
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

Clean generated build artifacts after build-based verification.

## Suggested PR Split

1. PR 1: F11 cleanup warning status.
2. PR 2: F12 structured audit fields.
3. PR 3: F13 metadata decode hardening.
4. PR 4: F14 completion threshold decision and tests.

F11 may depend on Phase 2 cleanup semantics if F10 changes retry behavior. F12 can be done anytime after Phase 1 defines distributed failure semantics.

## Open Questions Before Coding

1. What is the maximum number of cleanup warning entries to keep in persisted rebalance metadata?
2. Which actor identity is safe and useful to log for admin requests?
3. Should metadata unknown fields fail startup, warn and continue, or quarantine only the affected operation?
4. Should RustFS match MinIO's rebalance completion tolerance by default?

