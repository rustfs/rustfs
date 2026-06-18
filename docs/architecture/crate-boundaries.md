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

Existing migration checks live in:

- `scripts/check_layer_dependencies.sh`
- `scripts/check_architecture_migration_rules.sh`

Extend these guardrails instead of adding a parallel system.

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

The current decision is recorded in
[`config-model-boundary-adr.md`](config-model-boundary-adr.md): use the existing
`rustfs-config` package for the pure server-config model, keep persistence and
global server-config state in `ecstore`, and preserve the old
`rustfs_ecstore::config::*` path with a temporary compatibility marker during
the first extraction.

## Loss-Prevention Coverage

Architecture migration checks must keep public contract re-exports and ECStore
compatibility coverage from silently drifting during cleanup PRs.

Required `rustfs-storage-api` public re-exports:

- `pub use admin::{DiskSetSelector, StorageAdminApi};`
- `pub use bucket::{BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp};`
- `pub use error::{StorageErrorCode, StorageResult};`
- `pub use multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo};`
- `pub use object::{HTTPPreconditions, HTTPRangeError, HTTPRangeSpec, ObjectLockRetentionOptions};`
- `pub use object::{ExpirationOptions, TransitionedObject};`
- `pub use object::{HealOperations, MultipartOperations, NamespaceLocking, ObjectIO, ObjectOperations};`
- `pub use object::{ListObjectVersionsInfo, ListObjectsInfo, ListObjectsV2Info, ListOperations, ObjectInfoOrErr};`
- `pub use object::{ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState};`
- `pub use object::{VersionMarker, WalkOptions, WalkVersionsSortOrder};`

ECStore must keep compile-time coverage for `StorageAdminApi`, `HealOperations`,
and the separate `NamespaceLocking` operation group.

The old `StorageAPI` aggregate facade must not reappear in production
`crates/ecstore/src` or `rustfs/src` code after the storage operation groups
have been made explicit.

Outer RustFS/IAM consumers must use `rustfs-storage-api` generic list response
contracts directly for `ListObjectsV2Info`, `ListObjectVersionsInfo`, and
`ObjectInfoOrErr`; ECStore keeps the concrete aliases only for internal
implementation and compatibility.

Outer RustFS/scanner consumers must use `rustfs-storage-api` operation traits
directly for `ObjectIO`, `ObjectOperations`, `ListOperations`,
`MultipartOperations`, `HealOperations`, and `NamespaceLocking`; ECStore keeps
the concrete compatibility traits only for internal implementation and
downstream compatibility.

ECStore internal consumers must use `rustfs-storage-api` lifecycle helper DTOs
directly for `ExpirationOptions` and `TransitionedObject`; ECStore keeps the
old lifecycle paths only as downstream compatibility re-exports.
