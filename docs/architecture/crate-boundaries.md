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
- `pub use capability::{CapabilitySnapshotError, CapabilityState, CapabilityStatus};`
- `pub use error::{StorageErrorCode, StorageResult};`
- `pub use multipart::{CompletePart, ListMultipartsInfo, ListPartsInfo, MultipartInfo, MultipartUploadResult, PartInfo};`
- `pub use observability::{MemorySamplingState, ObservabilitySnapshot, ObservabilitySnapshotProvider, PlatformSupport, UserspaceProfilingCapability};`
- `pub use object::{HTTPPreconditions, HTTPRangeError, HTTPRangeSpec, ObjectLockRetentionOptions};`
- `pub use object::{ExpirationOptions, TransitionedObject};`
- `pub use object::{HealOperations, MultipartOperations, NamespaceLocking, ObjectIO, ObjectOperations};`
- `pub use object::{ListObjectVersionsInfo, ListObjectsInfo, ListObjectsV2Info, ListOperations, ObjectInfoOrErr};`
- `pub use object::{ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState};`
- `pub use object::{VersionMarker, WalkOptions, WalkVersionsSortOrder};`
- `pub use topology::{DiskCapabilities, TopologyCapabilities, TopologyDisk, TopologyLabels, TopologyPool, TopologySet, TopologySnapshot, TopologySnapshotProvider};`

Required `rustfs-concurrency` public workload admission contract re-exports:

- `pub use workload::{AdmissionState, WorkloadAdmissionRegistrySnapshot, WorkloadAdmissionSnapshot, WorkloadAdmissionSnapshotProvider, WorkloadClass};`

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
Outer consumers must not import ECStore directly outside compatibility
boundaries except for temporary trait imports needed for method resolution or
local test trait implementations. Non-trait ECStore surfaces must stay behind
local aliases, constants, or wrapper functions.

Outer compatibility boundary modules must use `rustfs_ecstore::api` for ECStore
public facade surfaces such as layout, storage owner, admin, metrics,
notification, capacity, bucket/config helpers, disk/error contracts, global
state accessors, RPC constants/clients, reader helpers, tier helpers, and
rebalance status contracts. Any non-ECStore `storage_compat.rs` import from
`rustfs_ecstore` must route through the `rustfs_ecstore::api` facade.
The legacy ECStore root `endpoints` and `disks_layout` compatibility modules
must remain crate-private; public layout access goes through
`rustfs_ecstore::api::layout`.
Facade-covered ECStore root modules must remain crate-private after this
boundary is established; outer crates should use `rustfs_ecstore::api::*`
instead of legacy root module paths. This includes storage/layout surfaces as
well as remaining bitrot, erasure coding, object DTO/reader, event, list, and
batch processor root modules once their facade groups exist.
ECStore root `global` re-exports must also stay removed once consumers use
`rustfs_ecstore::api::global` or crate-internal `crate::global` paths.
RustFS root `storage_compat.rs` must expose bucket metadata and quota contracts
as explicit aliases only. Broad `metadata`, `metadata_sys`, and `quota` module
passthroughs are reserved to narrower app/admin/storage compatibility
boundaries that still need module-local owner cleanup.
Root runtime storage config initialization and disk endpoint contracts must also
stay explicit aliases. The root compatibility boundary must not restore `com`,
bare `init`, or grouped `endpoint::Endpoint` passthroughs.
RustFS root `storage_compat.rs` must not re-export ECStore API symbols directly;
remaining root runtime compatibility symbols must be local type aliases,
constants, traits, or wrapper functions so ownership stays visible at the
boundary.
RustFS admin `storage_compat.rs` must expose config IO and default
initialization through explicit aliases. The admin compatibility boundary must
not restore broad `com` or bare `init` passthroughs.
RustFS admin and app `storage_compat.rs` bucket-facing compatibility contracts
must stay explicitly whitelisted. They must not restore broad bucket module,
client object API, client transition API, or storage-class module passthroughs
once a local compatibility boundary has narrowed them to specific aliases.
RustFS storage `storage_compat.rs` must expose bucket metadata, object-lock,
policy, replication, tagging, versioning, object API, and test-only
storage-class config contracts through explicit aliases. The storage
compatibility boundary must not restore broad `metadata`, `metadata_sys`,
`object_lock`, `policy_sys`, `replication`, `tagging`, `utils`, `versioning`,
`versioning_sys`, `object_api_utils`, or `com` passthroughs.
RustFS storage owner `storage_compat.rs` must not re-export ECStore API symbols
directly except temporary trait imports needed for method resolution. Remaining
storage-owner compatibility symbols must be local constants, type aliases, or
wrapper functions so storage-owned global state and helper access stays visible
at the boundary.
RustFS app, admin, and storage outer `storage_compat.rs` object and error
facade aliases must stay anchored on storage-api associated object types and
local `StorageError` aliases. They must not reintroduce raw
`rustfs_ecstore::api::object::{ObjectInfo,ObjectOptions}` or
`rustfs_ecstore::api::error::{Error,Result}` references.
Outer compatibility function signatures must also use local aliases for ECStore
metadata, object-lock, lifecycle journal, monitor, and notification facade
types. The boundary may define the local alias, but call signatures must not
expose the raw ECStore facade path once narrowed.
The RustFS storage owner compatibility boundary must keep raw ECStore facade
paths centralized behind local `ecstore_*` module aliases rather than scattering
`rustfs_ecstore::api::...` references through its aliases and wrappers.
The RustFS app/admin storage compatibility boundaries must likewise route raw
ECStore facade access through their local `ecstore_*` module aliases instead of
scattering `rustfs_ecstore::api::...` paths through compatibility wrappers.
Peripheral consumer storage compatibility boundaries must follow the same
pattern. IAM, heal, scanner, notify, observability, Swift, S3 Select, test, and
fuzz storage compatibility modules keep raw ECStore facade access centralized
behind local `ecstore_*` module aliases.
RustFS root runtime and e2e storage compatibility boundaries must follow the
same pattern, keeping raw ECStore facade access centralized behind local
`ecstore_*` module aliases.
Outer bucket lifecycle, replication, versioning, object-lock, and
restore-request trait method access must stay behind local compatibility traits
or wrapper functions. Non-compat sources must not import those ECStore bucket
API traits directly after the wrapper boundary is established. Remaining
temporary direct ECStore method-resolution imports are limited to disk, RPC peer
client, and warm-backend traits until their hot-path wrappers are isolated.
Scanner, notify, observability, and e2e `storage_compat.rs` boundaries must
also stay narrow. Scanner must not restore grouped bucket compatibility exports
for target, lifecycle, metadata, replication, or versioning modules. Notify
must not restore broad `config`/`global` module imports. Observability must
consume data usage through a local DTO projection instead of re-exporting the
ECStore data-usage loader. The e2e harness must not restore grouped RPC
passthroughs.
Test and fuzz `storage_compat.rs` harnesses must also stay narrow. Heal and
scanner test harnesses must expose ECStore contracts through direct aliases or
local wrappers, and fuzz harnesses must wrap bucket utility entrypoints instead
of restoring grouped ECStore passthrough exports.

ECStore ClusterControlPlane read models must stay owned by the crate-private
`cluster` module. Public access goes through `rustfs_ecstore::api::cluster` so
outer crates cannot depend on ECStore root control-plane internals.
Pool-state, local-node storage, and peer-health status projections are part of
the same facade boundary and must remain read-only until a later controller
slice explicitly wires dynamic health or membership behavior.

RustFS startup internals must stay crate-private after the startup owner split.
Only `startup_entrypoint` remains a public startup module for the binary
entrypoint; IAM bootstrap, optional runtime, and profiling startup shims must
not be re-exported as public library modules. Items inside crate-private
startup modules must also use crate visibility rather than bare public
visibility.

ECStore internal consumers must use `rustfs-storage-api` lifecycle helper DTOs
directly for `ExpirationOptions` and `TransitionedObject`; ECStore keeps the
old lifecycle paths only as downstream compatibility re-exports.
