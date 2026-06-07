# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-migration-guardrails`
- Baseline: `upstream/main` at `61f0dfbc40f748be313be84d834d8259cf3e19c9`
- PR type for this branch: `docs-only`
- Runtime behavior changes: none
- Rust code changes: none
- Repository metadata changes: `.gitignore` now allows tracking only
  `docs/architecture/` under the otherwise ignored `docs` tree.

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
- [~] `G-005` Add dependency direction guard.
  - Current branch: documentation only.
  - Next PR: add a `ci-gate` check for forbidden dependency edges.
- [~] `G-006` Create migration loss-prevention checks.
  - Current branch: documentation only.
  - Next PR: add checks for public re-export, route matrix, and storage trait
    coverage before pure moves.
- [x] `G-009` Enforce pre-push three-expert review.
  - Acceptance: [`crate-boundaries.md`](crate-boundaries.md) requires
    quality/architecture, migration-preservation, and testing/verification review
    before push.
- [~] `TEST-PRTYPE-001` Check PR type enum consistency.
  - Current branch: documentation only.
  - Next PR: add a mechanical check that all migration docs use the same PR type
    vocabulary.

## Next PRs

1. `ci-gate`: extend `scripts/check_layer_dependencies.sh` or add a nearby check
   for architecture-migration guardrails.
2. `docs-only` or `test-only`: capture startup timeline and admin route-action
   snapshot.
3. `docs-only`: inventory `ecstore::config::{Config, KV, KVS}` consumers.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Staged diff is scoped to docs, `.gitignore`, and the root architecture index; no over-abstraction or target drift found |
| Migration preservation | pass | No runtime files changed; notify/audit and IAM/KMS no-drift bullets were added after review feedback |
| Testing/verification | pass-with-nonblocking-follow-up | Docs-only verification is sufficient; suggested commands are recorded below |

## Verification Notes

Passed:

- `git diff --cached --check`
- `git diff --cached --name-only -- '*.rs' 'Cargo.toml' 'Cargo.lock' '.github/**' 'scripts/**' 'Makefile' 'Justfile'`
- `git diff --cached --exit-code -- '*.rs' 'Cargo.toml' 'Cargo.lock' '.github/**' 'scripts/**' 'Makefile' 'Justfile'`
- `printf '%s\n' docs/architecture/overview.md docs/foo.md docs/other/file.md | git check-ignore -v --stdin --no-index`
- `git rev-parse upstream/main`
- `git log -1 --format='%H %s' upstream/main`
- `./scripts/check_metrics_migration_refs.sh`

Known unrelated baseline issue:

- `./scripts/check_layer_dependencies.sh` currently fails on `upstream/main`
  because the script output and `scripts/layer-dependency-baseline.txt` format are
  out of sync. Keep that fix in the next `ci-gate` PR.

## Handoff Notes

- Keep Phase 0 PRs small. Do not start Config, Storage API, Runtime, or ECStore
  movement inside this docs branch.
- Keep CI checks in a separate `ci-gate` PR so the PR type rule remains enforceable.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
