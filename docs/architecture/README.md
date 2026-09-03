# Architecture Documentation

**Use this when:** you need the contract, invariant, or boundary rule that governs a change, and you want the one document that owns it.
**Source of truth:** the code and the guards. `scripts/check_architecture_migration_rules.sh` enforces the CI-anchored documents below; `scripts/check_doc_paths.sh` fails the pre-commit gate when any doc under `docs/` cites a repository path that no longer exists.

Two rules keep this directory healthy:

1. **Durable reference only.** One-shot plans, task trackers, dated analyses, status snapshots, and PR-scoped notes do not belong in the repository; keep them in the issue tracker or a local worktree and delete them when the work closes.
2. **No copies of other sources of truth.** Crate lists come from `Cargo.toml`, CI steps from `.github/workflows/`, code structure from the code. Cite a file path plus a symbol name, never a line number, and never paste counts or tables that a command can regenerate.

Every document starts with a `**Use this when:**` line so an agent can decide in one glance whether to read further. The index below repeats those lines.

## CI-anchored core

Required headings and strings in these files are asserted by `scripts/check_architecture_migration_rules.sh`; rename a heading only together with the guard.

| Document | Use this when |
|---|---|
| [crate-boundaries.md](crate-boundaries.md) | you add a crate dependency, move code across crates, touch a `storage_api.rs` boundary file, or need the change-type vocabulary the architecture guard enforces |
| [runtime-lifecycle.md](runtime-lifecycle.md) | moving or reordering anything in `rustfs/src/startup_*.rs`, changing readiness publication, or touching shutdown ordering |
| [readiness-matrix.md](readiness-matrix.md) | changing what a request surface does before storage or IAM is ready, changing probe semantics, or adding a runtime dependency that readiness must wait for |
| [storage-control-data-plane.md](storage-control-data-plane.md) | adding a storage API surface, a cluster read model, or a background-service status/reconcile surface, and you need to know which layer owns it |
| [global-state-crate-split-plan.md](global-state-crate-split-plan.md) | business logic needs runtime state (object store, endpoints, lock clients, lifecycle state, config) and you must pick the right boundary, or you are evaluating a crate split out of ECStore |
| [global-state-inventory.md](global-state-inventory.md) | you meet a `GLOBAL_*` static or an `OnceLock` and need to know whether it is a runtime ownership handle, an owner-local static, or process-global by design |
| [ecstore-module-split-plan.md](ecstore-module-split-plan.md) | you add lifecycle or replication logic and need to know which crate it belongs in, plan to move an operation family out of `SetDisks`, or the guard fails on one of the split rules |
| [ecstore-api-facade-inventory.md](ecstore-api-facade-inventory.md) | you need something from `rustfs_ecstore` in another crate, you are narrowing a `rustfs_ecstore::api` facade group, or the guard reports a facade bypass |
| [obs-ecstore-dependency-inventory.md](obs-ecstore-dependency-inventory.md) | adding, removing, or moving any `rustfs_ecstore` or `rustfs_storage_api` reference inside `crates/obs` |
| [compat-cleanup-register.md](compat-cleanup-register.md) | you add, review, or remove a temporary compatibility path and need the `RUSTFS_COMPAT_TODO` marker format and its removal condition |
| [overview.md](overview.md) | you need the historical framing of the architecture-migration program or the phase names that other contracts refer to |

## Contracts and invariants

| Document | Use this when |
|---|---|
| [erasure-coding.md](erasure-coding.md) | changing anything under `crates/ecstore/src/erasure/`, `crates/filemeta/`, `crates/ecstore/src/set_disk/`, storage-class or layout code, or any decode, quorum, or heal boundary (normative spec) |
| [placement-repair-invariants.md](placement-repair-invariants.md) | changing anything that resolves an object to a pool, set, or disk, or that admits scanner or heal work |
| [heal-concurrency-model.md](heal-concurrency-model.md) | changing heal, PUT/multipart commit, delete, lifecycle expiry, or data-movement code that shares the `(bucket, object)` commit surface, or asking whether RustFS needs a persistent healing marker |
| [unified-object-generation.md](unified-object-generation.md) | adding or changing anything that fences a commit, scopes a read lease, gates old-directory cleanup, binds prepared pool reads, or settles quota against the current object version |
| [ilm-tiering-persistence-contracts.md](ilm-tiering-persistence-contracts.md) | changing an ILM transition, tier configuration mutation, manual transition job, tier-delete recovery path, pool decommission, or any code that can create, transfer, or destroy ownership of a remote-tier object |
| [decommission-compatibility.md](decommission-compatibility.md) | changing pool decommission or rebalance behavior, its admin API shape, the persisted `PoolMeta` fields, or how tier free versions move between pools |
| [ecstore-layout-boundary.md](ecstore-layout-boundary.md) | touching endpoint expansion, `FormatV3`, pool/set layout, or moving files between ECStore's internal directories |
| [runtime-capability-contracts.md](runtime-capability-contracts.md) | changing the read-only observability or topology snapshot contracts in `rustfs-storage-api`, their providers, or the `storage_classes` payload of `GET /rustfs/admin/v4/runtime/capabilities` |
| [workload-admission-contracts.md](workload-admission-contracts.md) | adding a workload class or snapshot provider, or consuming admission state from a background job |
| [background-controller-contract.md](background-controller-contract.md) | adding a status snapshot or reconcile surface for a background service, or being tempted to fold several services into a generic controller |
| [background-services-inventory.md](background-services-inventory.md) | you need one audited background service's desired source, current-status inputs, status surface, and declared side effects |
| [scanner-usage-publication.md](scanner-usage-publication.md) | changing scanner data-usage cache publication, quota-visible usage snapshots, scanner cycle recovery, or the persisted scanner usage artifacts |
| [scanner-usage-authority-decision.md](scanner-usage-authority-decision.md) | deciding whether quota admission depends on scanner data usage, removing scanner publication layers, or designing a scanner storage boundary |
| [config-model-boundary-adr.md](config-model-boundary-adr.md) | touching the server-config model (`Config`, `KV`, `KVS`) or its persistence, or asking which crate owns which part of server configuration |
| [admin-route-action-snapshot.md](admin-route-action-snapshot.md) | adding, moving, or re-authorizing an admin route and needing to know where the route → handler → `AdminAction` contract is enforced |
| [kms-bulk-rekey-contract.md](kms-bulk-rekey-contract.md) | changing the bulk envelope re-wrap sweep, its admin endpoints, the re-wrap primitive, or which objects a rekey may touch |

## Support and compatibility matrices (release-facing, keep current)

| Document | Use this when |
|---|---|
| [s3-compatibility-matrix.md](s3-compatibility-matrix.md) | writing or checking a user-facing S3 compatibility claim, or moving a Ceph s3tests case between lists |
| [s3-tables-support-matrix.md](s3-tables-support-matrix.md) | writing a release note or client-compatibility statement about S3 Tables / Iceberg REST Catalog (cutover procedure: [../operations/s3-tables-cutover-runbook.md](../operations/s3-tables-cutover-runbook.md)) |
| [minio-rustfs-router-compatibility.md](minio-rustfs-router-compatibility.md) | a client or `mc` call that works against MinIO fails against RustFS and you need to know whether the endpoint is missing, stubbed, or deliberately different |
| [minio-file-format-compat.md](minio-file-format-compat.md) | deciding whether a MinIO drive set, bucket-metadata blob, or SSE object can be read or imported by a given RustFS build, or before touching a listed version anchor |

Operations runbooks live in [../operations/](../README.md#operations) and testing references in [../testing/README.md](../testing/README.md).
