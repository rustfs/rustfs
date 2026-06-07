# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-migration-ci-rules`
- Baseline: `upstream/main` at `6f4d0b54a171ff1560b5d892378d2ed407411fb9`
- PR type for this branch: `ci-gate`
- Runtime behavior changes: none
- Rust code changes: none
- CI/script changes: add a migration rule check for PR type vocabulary and
  temporary compatibility marker/register consistency, plus a lightweight
  docs-only workflow so architecture documentation PRs still run the same guard.
- Docs changes: record the new guardrail in the migration handoff.

## Phase 0 Tasks

- [x] `G-001` Refresh `main` and record baseline.
  - Acceptance: baseline commit, title, and branch are recorded.
  - Verification: `git fetch upstream main --prune`; `git rev-parse upstream/main`.
- [x] `G-002` Create migration tracking checklist.
  - Acceptance: this file records task state, context, verification, and handoff.
- [x] `G-003` Classify PR types.
  - Acceptance: [`crate-boundaries.md`](crate-boundaries.md) lists exactly one
    allowed PR type per PR.
- [x] `G-004` Define re-export and wrapper policy.
  - Acceptance: temporary compatibility code must use `RUSTFS_COMPAT_TODO`.
- [x] `G-005` Add dependency direction guard.
  - Acceptance: `./scripts/check_layer_dependencies.sh` passes on current
    `upstream/main` while still rejecting new unaccepted layer dependencies.
- [~] `G-006` Create migration loss-prevention checks.
  - Current branch: add a migration rule check for PR type vocabulary and
    temporary compatibility marker/register consistency, with a dedicated
    architecture-doc trigger that covers `ARCHITECTURE.md` and
    `docs/architecture/**` docs-only PRs.
  - Remaining follow-up: add checks for public re-export, route matrix, and
    storage trait coverage before pure moves.
- [x] `G-007` Create startup timeline table.
  - Acceptance: [`startup-timeline.md`](startup-timeline.md) records current
    binary startup order, side effects, fatal boundaries, and readiness stages.
- [x] `G-008` Capture admin route-action snapshot.
  - Acceptance: [`admin-route-action-snapshot.md`](admin-route-action-snapshot.md)
    records current route families, handler ownership, authorization actions,
    public exceptions, table-catalog routes, and `/minio/admin` compatibility
    alias behavior.
- [x] `G-009` Enforce pre-push three-expert review.
  - Acceptance: [`crate-boundaries.md`](crate-boundaries.md) requires
    quality/architecture, migration-preservation, and testing/verification review
    before push.
- [x] `G-010` Inventory `ecstore::config::{Config, KV, KVS}` consumers.
  - Acceptance:
    [`ecstore-config-consumer-inventory.md`](ecstore-config-consumer-inventory.md)
    records the current model definitions, global accessors, persistence helpers,
    consumer groups, migration risks, and do-not-change contract.
- [x] `TEST-PRTYPE-001` Check PR type enum consistency.
  - Acceptance: `./scripts/check_architecture_migration_rules.sh` parses the
    allowed PR types from [`crate-boundaries.md`](crate-boundaries.md) and fails
    when `ARCHITECTURE.md` or architecture docs reference an unknown PR type.
- [x] `COMPAT-REG-001` Check temporary compatibility cleanup consistency.
  - Acceptance: `./scripts/check_architecture_migration_rules.sh` fails when a
    source `RUSTFS_COMPAT_TODO(<task-id>)` marker lacks a cleanup-register entry,
    when a register entry lacks a source marker, or when a source marker omits a
    removal condition.

## Next PRs

1. `test-only`: add a mechanical admin route matrix guard from the current
   snapshot and `route_registration_test.rs`.
2. `contract`: define the config-model contract surface while preserving the
   existing `Config`, `KV`, and `KVS` behavior.
3. `ci-gate`: add focused checks for public re-export and storage trait coverage
   before pure moves.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Confirmed the guard script is narrow/readable, fixes single-token PR type detection, and reuses one script across CI/make/docs triggers rather than adding a parallel rule system |
| Migration preservation | pass | Confirmed this is a `ci-gate` PR with only CI/config/docs/script changes and no runtime logic, storage hot-path, global-state, compatibility implementation, or crate-split changes |
| Testing/verification | pass | Confirmed positive checks, staged diff check, and temporary negative checks cover unknown PR type and missing cleanup-register entry failure modes |

## Verification Notes

Passed:
- `./scripts/check_architecture_migration_rules.sh`
- `./scripts/check_layer_dependencies.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `git diff --check`
- `git diff --cached --check`
- `bash -n scripts/check_architecture_migration_rules.sh`
- `make architecture-migration-check`
- `ruby -e "require 'yaml'; YAML.load_file('.github/workflows/architecture-migration-rules.yml')"`
- temporary negative check for unknown single-token PR type
- temporary negative check for unknown PR type in `ARCHITECTURE.md`
- temporary negative check for unknown PR type in nested `docs/architecture/**`
- temporary negative check for source compatibility marker without a register entry

## Handoff Notes

- Keep Phase 0 PRs small. Do not move Config, Storage API, Runtime, or ECStore
  code inside this `ci-gate` branch.
- Keep CI checks in a separate `ci-gate` PR so the PR type rule remains enforceable.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
- The next config-model PR must preserve the current tuple-struct shapes and
  persistence behavior before introducing narrower provider contracts.
- Do not move `config::com` until a dedicated helper-user inventory covers
  ECStore-internal persistence paths as well as adjacent non-model users.
