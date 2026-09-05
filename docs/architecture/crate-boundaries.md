# Crate Boundaries And Migration Guardrails

**Use this when:** you add a crate dependency, move code across crates, touch a `storage_api.rs` boundary file, or need the change-type vocabulary the architecture guard enforces.
**Source of truth:** `scripts/check_architecture_migration_rules.sh` (the enumerated rules; this file is its boundary document) and `scripts/check_layer_dependencies.sh` (layer and edge checks). Extend those guards instead of adding a parallel system.

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

Contract crates stay below implementation crates. Forbidden edges:

| Edge | Why |
|---|---|
| `storage-api -> ecstore` | Storage contracts must not depend on the storage implementation |
| `security-governance -> rustfs` | Governance contracts stay below the binary crate |
| `extension-schema -> rustfs` | The extension schema is consumed by the binary, never the reverse |
| `extension-schema -> ecstore` | The extension schema must not reach storage internals |

- `rustfs-storage-api` exposes storage-facing replication status/state contracts only through `crates/storage-api/src/replication.rs`, so its temporary dependency on `rustfs-filemeta` wire types stays centralized and no `rustfs-replication` / `rustfs-storage-api` cycle appears.
- Leaf crates (`config`, `credentials`, `crypto`, `io-metrics`, `madmin`) may not depend on other `rustfs-*` crates, in either TOML spelling, except the adjudicated edges pinned in the guard's leaf allowlist: `io-metrics -> rustfs-s3-ops` (pure contract crates sharing the `S3Operation` vocabulary) and `madmin -> rustfs-signer` (the SigV4-signed admin SDK client). A new leaf exception must be a pure contract dependency (types and enums only, no I/O, no globals, no non-contract internal dependencies) and land together with its allowlist entry.
- Compile-time source reads follow the same direction: `include_str!` / `include!` of a `.rs` file must not resolve outside the including crate (`scripts/check_layer_dependencies.sh`). Shared source-text expectations belong in a contract surface such as `rustfs_protos::compat_manifest` (`crates/protos/src/compat_manifest.rs`) and are asserted by each owning crate.

## ECStore Access Boundary

Outer crates reach ECStore only through `rustfs_ecstore::api`, and only from one local boundary file per owner (`storage_api.rs`). Boundary files and facade groups are inventoried in [ecstore-api-facade-inventory.md](ecstore-api-facade-inventory.md).

- Inside a boundary file, raw `rustfs_ecstore::api::...` paths are centralized behind local `ecstore_*` module aliases; code outside the boundary sees local type aliases, constants, traits, or wrapper functions, never the raw facade path.
- Non-trait ECStore surfaces (metadata, object-lock, lifecycle journal, monitor, notification types) stay behind local aliases; boundary function signatures do not expose raw ECStore facade types once narrowed. Object and error aliases anchor on storage-api associated object types and a local `StorageError`.
- Outer consumers use `rustfs-storage-api` operation traits (`ObjectIO`, `ObjectOperations`, `ListOperations`, `MultipartOperations`, `HealOperations`, `NamespaceLocking`) and generic list responses (`ListObjectsV2Info`, `ListObjectVersionsInfo`, `ObjectInfoOrErr`) directly; ECStore keeps concrete aliases only for internal implementation and compatibility.
- Bucket lifecycle, replication, versioning, object-lock, restore-request, disk, RPC peer client, and warm-backend trait methods are reached through owner-local compatibility traits or wrapper functions, not by importing ECStore traits outside the boundary.
- The old `StorageAPI` aggregate facade must not reappear in production `crates/ecstore/src` or `rustfs/src` code.
- Facade-covered ECStore root modules (layout, `endpoints`, `disks_layout`, bitrot, erasure, object DTO/reader, event, list, batch processor, `global`) stay crate-private; public access goes through the matching `rustfs_ecstore::api::*` group.
- Cluster control-plane read models stay owned by the crate-private `cluster` module and are published through `rustfs_ecstore::api::cluster`; pool-state, local-node storage, and peer-health projections are read-only.
- RustFS startup internals are crate-private: only `startup_entrypoint` is a public startup module of the `rustfs` library (`rustfs/src/lib.rs`), and items inside the other `startup_*` modules use crate visibility.
- The observability dependency baseline is [obs-ecstore-dependency-inventory.md](obs-ecstore-dependency-inventory.md); observability extraction updates it together with the guard.

## Scanner, Heal, And ECStore

Heal is split by responsibility, not by the shared word "heal". ECStore owns erasure-set repair primitives: quorum metadata arbitration, EC reconstruction, per-disk rename commit, dangling metadata classification, and orphan data-dir reclamation. These stay in ECStore because they share the same object namespace locks, rename commit model, and data-dir cleanup rules as PUT, DELETE, multipart, lifecycle expiry, rebalance, and decommission. Moving those primitives out would split the lock and commit model across crates.

`crates/heal` owns repair orchestration: queueing, deduplication, admission, scheduling, resume, MRF replay, replacement-disk tracking, and the admin-facing status/control surface. It reaches storage through `HealStorageAPI`; ECStore-originated repair requests flow back through typed repair channels rather than a Cargo dependency on the heal crate.

`crates/scanner` owns discovery, data-usage publication, lifecycle/replication scan actions, bitrot scan dispatch, and scanner-driven repair requests. Scanner may request repair through the heal channel, but it must not directly execute erasure-set repair primitives.

`rustfs-scanner-metrics` owns scanner telemetry DTOs, global scanner counters, lifecycle action labels consumed by metrics, and the short-window latency accumulator used by those metrics. ECStore, lifecycle, observability, admin, and scanner code may depend on this crate for metrics only. Scanner storage seams currently live in `rustfs-scanner`'s `storage_api.rs`; if a future `rustfs-scanner-contracts` crate is reintroduced for shared storage or wire contracts, it must not regain metrics, globals, or telemetry implementation.

`remote_scanner` remains scanner-owned for #2219 because it carries the scanner cycle fence, replay protection, stream envelope, and per-bucket scan result protocol. The scanner storage seam exposes the store, set, and disk capabilities needed by the remote execution path, while the wire protocol stays physically owned by scanner. A future split may move remote disk scan execution behind an ECStore storage capability or move the whole remote scanner protocol with scanner; leaving the envelope, fence, replay cache, and execution path split across both sides without a documented owner is not allowed.

The scanner usage authority decision is fixed in [scanner-usage-authority-decision.md](scanner-usage-authority-decision.md): scanner usage remains hard-quota authority. A future scanner storage seam must therefore model the concrete publication, cycle-lock, usage-floor, observed-snapshot, and recovery-marker capabilities described in [scanner-usage-publication.md](scanner-usage-publication.md), not a generic key-value abstraction.

## Loss-Prevention Coverage

The guard pins specific public re-export lines (its `require_source_line` entries) so contract surfaces cannot silently disappear during cleanup. The canonical lists are the guard script and the owning files, not this page:

- `crates/storage-api/src/lib.rs`: admin, bucket, capability, error, multipart, observability, object, and topology contract re-exports;
- `crates/concurrency/src/lib.rs`: workload admission contract re-exports;
- `rustfs/src/lib.rs`: `pub mod startup_entrypoint;`.

ECStore keeps compile-time coverage for `StorageAdminApi`, `HealOperations`, and the separate `NamespaceLocking` operation group (`crates/ecstore/tests/ecstore_contract_compat_test.rs`), and its internal consumers use the `rustfs-storage-api` lifecycle DTOs `ExpirationOptions` and `TransitionedObject` directly.

## Temporary Compatibility Code

Every temporary compatibility path carries a `RUSTFS_COMPAT_TODO(<id>)` source marker with a removal condition and a matching entry in [compat-cleanup-register.md](compat-cleanup-register.md); the guard enforces the match in both directions. Compatibility layers are deleted in their own cleanup change, never bundled with new migration logic.

## Config Model

The server-config model (`Config`, `KV`, `KVS`) and the global server-config snapshot accessors are owned by `rustfs_config::server_config`; ECStore keeps persistence, storage-class state, and startup wiring, and its public facades must not re-export those symbols. See [config-model-boundary-adr.md](config-model-boundary-adr.md).

## Required Architecture Documents

The guard requires the documents and section headings listed in its `require_source_contains` entries (`scripts/check_architecture_migration_rules.sh`); the directory index is [README.md](README.md).

## On-Demand Migration Service

`rustfs/src/on_demand_migration/` owns source clients, pull scheduling, list
merging, runtime state and backfill orchestration. Its `storage_api.rs` is the
only ECStore facade boundary. Object write-back still enters the application's
internal PUT and multipart use cases, including the atomic create-only commit,
delete-marker protection, encryption, quota and notification rules.

ECStore stores the existing ODM bytes and update timestamp without interpreting
the JSON. Every metadata cache install or removal publishes those bytes through
`BUCKET_CONFIG_PUBLISH_HOOK`; the application decodes them and synchronously
withdraws corrupt configurations. Configuration writes validate structure and
deployment constraints in the admin use case before the incarnation-fenced
metadata update. Backfill reads metadata from its store's instance context and
preserves the checkpoint ETag compare-and-set, lease and tail-drained writes.

Observability owns its metric DTOs and accepts application snapshot callbacks;
it does not depend on the ODM runtime. The application registers both bucket
and backfill snapshots during startup, before metadata and metric collection.

This boundary does not change `.metadata.bin`, the ODM wire format or the
backfill checkpoint format. An older binary may still discard unknown metadata
fields when it rewrites a bucket; service relocation does not make mixed-version
configuration writes or rollback preserve ODM configuration.
