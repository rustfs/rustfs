# Crate Boundaries And Migration Guardrails

These rules apply to architecture-migration PRs linked to
[`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660).

## PR Types

Every PR must declare exactly one type:

- `docs-only`
- `test-only`
- `contract`
- `api-extraction`
- `pure-move`
- `consumer-migration`
- `dependency-migration`
- `security-change`
- `behavior-change`
- `ci-gate`

Do not mix directory movement, security tightening, and behavior changes in one PR.

## Dependency Direction

Contract crates must stay below implementation crates. Initial forbidden edges:

- `storage-api -> ecstore`
- `security-governance -> rustfs`
- `extension-schema -> rustfs`
- `extension-schema -> ecstore`

Existing layer checks live in `scripts/check_layer_dependencies.sh`. The next
`ci-gate` PR should extend existing guardrails instead of adding a parallel system.

## Pre-Push Expert Review

Before pushing any PR branch, record three expert reviews in the task notes:

| Expert | Required focus |
|---|---|
| Quality/architecture | Structure, naming, dependency direction, PR type, scope, and over-abstraction risk |
| Migration preservation | Startup order, readiness, quorum, reader semantics, AppContext/global fallback, notify/audit lifecycle, IAM/KMS boundaries, and compatibility |
| Testing/verification | Focused tests, regression tests, commands run, missing coverage, and whether tests are forcing business-logic drift |

Push is allowed only when all three experts return `pass` or
`pass-with-nonblocking-follow-up`. Any `blocker` prevents push until the issue is
fixed and the relevant review is repeated.

## Temporary Compatibility Code

Temporary compatibility code that must be removed later must include a searchable
source comment and a cleanup-register entry.

Use this source-comment format:

```rust
// RUSTFS_COMPAT_TODO(API-005): keep old ecstore::store_api path during storage-api migration. Remove after all consumers use rustfs-storage-api.
```

Rules:

- Add the marker only to temporary compatibility paths, not permanent APIs.
- Include the task ID in the marker.
- State why the compatibility path exists and when it can be removed.
- Use this for temporary re-exports, wrappers, fallbacks, legacy action mappings,
  and old endpoint compatibility layers.
- Delete compatibility layers in their own cleanup PR.

## Config Model First

`ecstore::config::{Config, KV, KVS}` should move before extension config adapters
or config-schema work. First inventory consumers, then decide whether existing
`crates/config` is enough or whether a smaller model crate is required.
