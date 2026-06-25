# Site Replication Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden RustFS site replication against credential exposure, unsafe diagnostic authorization, incomplete remove cleanup, and weak operational visibility while preserving MinIO-compatible API behavior.

**Architecture:** Start with low-risk guardrails that reduce blast radius, then add deterministic cleanup and status surfaces that support later durable replay work. Keep changes local to existing admin, madmin, and bucket replication modules before introducing larger state-machine refactors.

**Tech Stack:** Rust, Axum admin handlers, serde madmin contracts, ecstore bucket replication metadata, RustFS route policy tests, focused cargo tests.

---

## File Map

- `docs/architecture/site-replication-hardening-pr.md`: PR body draft and implementation log.
- `crates/ecstore/src/bucket/target/bucket_target.rs`: credential redaction helpers and safe `Debug`.
- `rustfs/src/admin/handlers/replication.rs`: remote target response/log redaction.
- `rustfs/src/admin/handlers/bucket_meta.rs`: bucket metadata export redaction.
- `rustfs/src/admin/route_policy.rs`: site replication diagnostic route action mapping.
- `rustfs/src/admin/handlers/site_replication.rs`: diagnostic authorization, remove cleanup, status detail, and focused tests.
- `crates/madmin/src/site_replication.rs`: response DTO fields for peer errors and pending operation visibility when needed.

## Task 1: Documentation Baseline

**Files:**
- Create: `docs/superpowers/plans/2026-06-25-site-replication-hardening.md`
- Create: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Write the implementation plan and PR draft**

Record the phased plan, implementation log, and PR template sections.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/plans/2026-06-25-site-replication-hardening.md docs/architecture/site-replication-hardening-pr.md
git commit -m "docs: plan site replication hardening"
```

## Task 2: Redact Site Replication Credentials

**Files:**
- Modify: `crates/ecstore/src/bucket/target/bucket_target.rs`
- Modify: `rustfs/src/admin/handlers/replication.rs`
- Modify: `rustfs/src/admin/handlers/bucket_meta.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Add safe credential formatting**

Implement custom `Debug` for credential-bearing bucket target structures and expose redacted response/export helpers where existing APIs serialize targets.

- [ ] **Step 2: Replace unsafe logs and admin responses**

Stop returning or logging `secret_key` in remote target listing and bucket metadata export unless an existing privileged import path requires the raw stored form.

- [ ] **Step 3: Add focused tests**

Add assertions that serialized admin/export output and `Debug` output do not contain the configured secret value.

- [ ] **Step 4: Run focused tests**

```bash
cargo test -p rustfs replication --lib
cargo test -p rustfs bucket_meta --lib
cargo test -p rustfs site_replication --lib
```

- [ ] **Step 5: Commit**

```bash
git add crates/ecstore/src/bucket/target/bucket_target.rs rustfs/src/admin/handlers/replication.rs rustfs/src/admin/handlers/bucket_meta.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "fix: redact site replication target secrets"
```

## Task 3: Tighten Diagnostic Route Authorization

**Files:**
- Modify: `rustfs/src/admin/route_policy.rs`
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Move devnull and netperf off read-only info authorization**

Map diagnostic POST endpoints to `SiteReplicationOperationAction` or a stricter existing action and update handlers to validate the same action.

- [ ] **Step 2: Add request bounds**

Ensure diagnostic body or duration inputs cannot be used as unbounded work by a low-privilege caller.

- [ ] **Step 3: Add route policy tests**

Assert `devnull` and `netperf` require operation-level permission.

- [ ] **Step 4: Run focused tests**

```bash
cargo test -p rustfs route_policy --lib
cargo test -p rustfs site_replication --lib
```

- [ ] **Step 5: Commit**

```bash
git add rustfs/src/admin/route_policy.rs rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "fix: require operation access for replication diagnostics"
```

## Task 4: Clean Replication Targets on Site Remove

**Files:**
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Add removed-peer target cleanup**

When a site is removed, traverse buckets and remove `site-repl-{deployment_id}` rules and matching bucket targets before final state is saved.

- [ ] **Step 2: Preserve runtime cache consistency**

After changing `bucket-targets.json`, call existing target refresh paths so replication clients stop using removed peers.

- [ ] **Step 3: Add focused tests**

Create a bucket target/rule fixture and assert remove cleanup prunes only the removed deployment.

- [ ] **Step 4: Run focused tests**

```bash
cargo test -p rustfs site_replication --lib
```

- [ ] **Step 5: Commit**

```bash
git add rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "fix: clean site replication targets on remove"
```

## Task 5: Improve Status Diagnostics

**Files:**
- Modify: `crates/madmin/src/site_replication.rs`
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Expose peer fetch errors**

Add machine-readable peer error fields to status responses without failing the entire status call.

- [ ] **Step 2: Expose pending operations**

Return pending remove or credential rotation state with acked and pending peers.

- [ ] **Step 3: Add focused tests**

Assert status includes peer fetch error details and pending operation summaries.

- [ ] **Step 4: Run focused tests**

```bash
cargo test -p rustfs site_replication --lib
```

- [ ] **Step 5: Commit**

```bash
git add crates/madmin/src/site_replication.rs rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "feat: expose site replication status diagnostics"
```

## Task 6: Compatibility and Release Verification

**Files:**
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Run focused verification**

```bash
cargo fmt --all --check
cargo test -p rustfs route_policy --lib
cargo test -p rustfs route_registration_test --lib
cargo test -p rustfs site_replication --lib
```

- [ ] **Step 2: Run broader gate if feasible**

```bash
make pre-commit
```

- [ ] **Step 3: Record verification and residual risk in PR draft**

Update the PR draft with commands run, failures, skips, and remaining follow-up items.

- [ ] **Step 4: Commit**

```bash
git add docs/architecture/site-replication-hardening-pr.md
git commit -m "docs: finalize site replication hardening notes"
```

## Task 7: Extend Scope for Full Single-PR Completion

**Files:**
- Modify: `docs/superpowers/plans/2026-06-25-site-replication-hardening.md`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Record the expanded single-PR scope**

Add follow-up tasks for MinIO wire compatibility, add preflight validation, bootstrap sync, lifecycle compatibility, durable retry, and repair.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/plans/2026-06-25-site-replication-hardening.md docs/architecture/site-replication-hardening-pr.md
git commit -m "docs: expand site replication hardening scope"
```

## Task 8: MinIO-Compatible Peer Transport Contract

**Files:**
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Add peer admin path mapping**

Route outbound peer requests through MinIO-compatible `/minio/admin/v3/site-replication/...` paths when talking to a MinIO-compatible peer, while preserving RustFS compatibility through the existing alias handling.

- [ ] **Step 2: Add focused tests**

Assert peer path mapping preserves the request path used for signing and URL construction.

- [ ] **Step 3: Commit**

```bash
git add rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "fix: align site replication peer paths with minio"
```

## Task 9: Add-Time Topology Preflight

**Files:**
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [x] **Step 1: Validate self, deployment identity, IDP, and initial data shape**

Before persisting add state, fetch remote metainfo and IDP settings, reject duplicate deployment IDs, missing local site, existing site-replication topology gaps, IDP mismatch, and multiple non-empty initial sites.

- [x] **Step 2: Add focused tests**

Cover pure topology validation for duplicate deployments, missing self, IDP mismatch, and more than one non-empty site.

- [x] **Step 3: Commit**

```bash
git add rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "fix: validate site replication add topology"
```

## Task 10: Full Bootstrap Sync

**Files:**
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [x] **Step 1: Add snapshot bootstrap after add/join**

Use `build_sr_info` as the canonical local snapshot and sync IAM and bucket metadata to peers after add, before object resync.

- [x] **Step 2: Add focused tests**

Cover bootstrap task planning order and item counts without requiring live peers.

- [x] **Step 3: Commit**

```bash
git add rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "feat: bootstrap site replication metadata on add"
```

## Task 11: Lifecycle Compatibility

**Files:**
- Modify: `rustfs/src/app/bucket_usecase.rs`
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Stop default full lifecycle replication**

Only replicate lifecycle expiry metadata when `replicate_ilm_expiry` is enabled for site replication peers.

- [ ] **Step 2: Add focused tests**

Assert lifecycle metadata is skipped by default and included only for expiry-enabled peers.

- [ ] **Step 3: Commit**

```bash
git add rustfs/src/app/bucket_usecase.rs rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "fix: align lifecycle replication with minio semantics"
```

## Task 12: Durable Retry and Repair MVP

**Files:**
- Modify: `crates/madmin/src/site_replication.rs`
- Modify: `rustfs/src/admin/handlers/site_replication.rs`
- Modify: `docs/architecture/site-replication-hardening-pr.md`

- [ ] **Step 1: Add minimal persistent retry queue**

Record failed bucket/IAM/state replication events with peer, path, payload, retry count, and last error; expose pending/failed counts in status.

- [ ] **Step 2: Add repair once operation**

Provide an operation-level admin path that compares local snapshot to peer metainfo and replays missing metadata.

- [ ] **Step 3: Add focused tests**

Cover queue serialization, retry status summaries, and repair task generation.

- [ ] **Step 4: Commit**

```bash
git add crates/madmin/src/site_replication.rs rustfs/src/admin/handlers/site_replication.rs docs/architecture/site-replication-hardening-pr.md
git commit -m "feat: add site replication retry and repair MVP"
```
