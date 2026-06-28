# Global State And Crate Split Plan

This document records the late global-state cleanup plan after the AppContext
foundation, storage API contracts, ECStore layout, runtime lifecycle, and cluster
control-plane boundaries are stable.

As of the Phase 7 closeout, runtime resolver fallbacks have been pushed out of
the root facade and into explicit owner-local boundaries. Future work should
therefore treat broad fallback removal as complete and use this document for the
remaining ECStore-owned bootstrap state and crate-split decisions.

## Remaining Global Owners

| Owner | Current role | Migration stance |
|---|---|---|
| `rustfs/src/app/context.rs` | AppContext-first resolver facade. | Resolver helpers stay context-first and do not construct concrete no-AppContext defaults. |
| `rustfs/src/app/context/runtime_sources.rs` | Default adapters for KMS, IAM, object store, endpoints, config, metrics, and notification state used by AppContext construction. | This is an allowed adapter boundary, not a business logic owner. |
| `rustfs/src/*/runtime_sources.rs` | Root, admin, app, server, startup, and storage owner-local runtime-source boundaries. | Business modules use these boundaries instead of calling global state directly; owner facades own any remaining no-AppContext compatibility defaults. |
| `rustfs/src/*/storage_api.rs` | Root, admin, app, and storage owner-local storage contract/facade boundaries. | Storage helper and ECStore facade access remains visible at local owner boundaries. |
| `crates/*/storage_api.rs` | External crate-local storage facade boundaries for IAM, scanner, heal, notify, observability, Swift, and S3 Select. | External runtime crates consume ECStore runtime state through `rustfs_ecstore::api::runtime` instead of the direct global facade. |
| `crates/ecstore/src/runtime/global.rs` | ECStore bootstrap/runtime state owner. | Keep internal until ECStore has explicit owner handles for all remaining bootstrap state. |
| `crates/ecstore/src/runtime/sources.rs` | ECStore runtime-source adapter over global state. | Preferred ECStore-internal access path while shrinking direct `runtime::global` reads. |

## Runtime Source Boundaries

Runtime-source modules are the allowed compatibility layer between migrated
consumers and process-global state. They must keep these properties:

- context-first lookup when an `AppContext` handle exists;
- explicit fallback to the existing global only where compatibility still
  requires it;
- no hidden service construction in business logic;
- no startup, readiness, IAM, KMS, lock, notification, or storage behavior
  change in inventory or guardrail PRs.

## Guarded Boundary List

The architecture guard snapshots the files currently allowed to reference
`rustfs_ecstore::api::global` directly:

- `rustfs/src/storage/storage_api.rs`

New direct uses must either move behind an existing owner-local boundary or
update this plan and the guard in the same reviewed migration PR.

## Fallback Removal Plan

1. Keep AppContext-first lookup as the stable resolver contract.
2. Keep concrete no-AppContext compatibility defaults only at owner-local
   runtime-source facades that consume them.
3. Do not let business logic call `AppContext` or ECStore globals directly when
   an owner-local runtime-source boundary exists.
4. Keep embedded startup and tests working before deleting any remaining owner
   fallback.
5. Do not remove ECStore bootstrap globals until ownership handles exist for
   local disks, endpoint pools, lock clients, notification state, tier config,
   lifecycle state, and object-store publication.

## GLOB-007 Closeout Boundary

`GLOB-007` is complete when these invariants hold:

- root `rustfs/src/runtime_sources.rs` is an AppContext/root facade entrypoint
  and no longer composes concrete fallback defaults with `unwrap_or`,
  `unwrap_or_else`, direct `init_global`, or direct `new_global` calls;
- private AppContext resolver helpers are context-first and do not hide fallback
  closure parameters;
- admin, app, storage, server, startup, and config owner facades decide when to
  apply no-AppContext compatibility defaults;
- production callers outside runtime-source and storage-api boundary modules do
  not import ECStore global state directly;
- the architecture guard keeps the direct `rustfs_ecstore::api::global`
  boundary list explicit.

Allowed remaining fallbacks are owner compatibility decisions, not resolver
fallback families. They are kept so embedded startup, tests, and no-context
callers preserve the previous behavior while higher layers continue migrating
to explicit AppContext ownership.

## Crate Split Evaluation

`ecstore-erasure` and `storage-cluster` remain proposal-only until dependency
cycles and hot-path risks are proven safe. The Phase 7 evaluation is complete
for now: neither split is ready for code movement in this migration round.
The follow-up ECStore module split plan is recorded in
[`ecstore-module-split-plan.md`](ecstore-module-split-plan.md), including the
remaining `SetDisks`, lifecycle, replication, and facade-shrink boundaries.

### CRATE-001: `ecstore-erasure`

Current coupling:

- erasure decoding depends on disk errors, disk read timeouts, and set-disk
  shard sources;
- set-disk read/write/heal paths construct erasure codecs in hot object I/O
  paths;
- bitrot readers/writers live in ECStore IO support and are used by both
  erasure and set-disk code;
- public compatibility still exposes erasure symbols through
  `rustfs_ecstore::api::erasure`.

Decision: do not split in code yet. The erasure boundary is a candidate only
after the shard-source, disk-error, bitrot, and metrics contracts are explicit
enough to avoid a dependency cycle back into ECStore.

Required evidence before proposing the split:

- `cargo tree -p rustfs-ecstore -e normal --depth 2` snapshot for dependency
  impact;
- focused benchmarks for encode/decode, read reconstruction, bitrot verification,
  and large-object streaming;
- contract sketch for shard sources, disk errors, bitrot IO, metrics, and file
  metadata without importing ECStore implementation modules;
- compatibility plan for `rustfs_ecstore::api::erasure` and test harnesses;
- rollback plan that keeps object read/write quorum and old-version file decode
  behavior unchanged.

### CRATE-002: `storage-cluster`

Current coupling:

- cluster RPC remote disk code depends on disk stores, disk health tracking,
  set-disk buffer sizing, local disk scan guards, internode metrics, and runtime
  credential/signature sources;
- peer S3 and peer REST clients share bucket metadata, disk quorum reduction,
  endpoint layout, local disk initialization, and store helpers;
- control-plane snapshots are separated from data-plane RPC, but remote disk and
  peer clients still own data movement side effects inside ECStore.

Decision: do not split in code yet. The storage-cluster boundary is a candidate
only after remote disk, peer health, lock/quorum, runtime metrics, and endpoint
layout contracts are explicit enough to stand below ECStore without circular
dependencies.

Required evidence before proposing the split:

- dependency graph showing no cycle with ECStore, `rustfs-storage-api`, runtime
  source owners, or cluster control-plane owners;
- RPC contract sketch for remote disk, peer S3, peer REST, auth/signature,
  internode metrics, and cancellation;
- compatibility plan for `rustfs_ecstore::api::cluster`, `api::rpc`, and test
  fixtures that build local disks or endpoint pools;
- focused tests for remote disk error classification, peer health recovery,
  per-pool quorum reduction, lock behavior, and data-stream request paths;
- rollback plan that preserves quorum, remote disk IO, lock, peer health, and
  data movement behavior.

### CRATE-003: `bucket-lifecycle`

Decision: do not split in code yet. Lifecycle remains coupled to ECStore object
operations, bucket metadata, `SetDisks` stale multipart cleanup, tier config,
runtime lifecycle state, scanner metrics, notification/audit side effects, and
replication delete scheduling.

Required evidence before proposing the split:

- contract sketch for lifecycle object operations, metadata access, runtime
  state, replication delete scheduling, and audit/notification sinks;
- dependency graph showing the candidate crate can avoid importing ECStore
  implementation modules;
- focused tests for lifecycle evaluation, expiry, transition, stale multipart
  cleanup, tier journal recovery, and lifecycle-originated replication deletes;
- compatibility plan for `rustfs_ecstore::api::bucket::lifecycle` consumers;
- rollback plan that preserves lifecycle queues, scanner repair accounting,
  tier transitions, object deletion behavior, and notification/audit events.

### CRATE-004: `bucket-replication`

Decision: do not split in code yet. Replication remains coupled to ECStore
object APIs, bucket target clients, metadata systems, file metadata replication
state, runtime replication pool/stat handles, bucket monitor state, scanner
repair classification, lifecycle-originated deletes, and notification events.

Required evidence before proposing the split:

- contract sketch for replication storage operations, metadata/target access,
  runtime pool and stats, event sinks, and lifecycle/heal bridges;
- dependency graph showing the candidate crate can avoid importing ECStore
  implementation modules;
- focused tests for object replication, delete replication, resync state, heal
  repair queueing, target error handling, and queue admission;
- compatibility plan for `rustfs_ecstore::api::bucket::replication` consumers;
- rollback plan that preserves replication queues, MRF/resync state, target
  client behavior, scanner repair, and event emission.

## Preservation Rules

- Do not reintroduce AppContext resolver fallback families in broad cleanup PRs.
- Do not introduce direct global reads in admin, app, server, storage, scanner,
  heal, IAM, notify, observability, Swift, or S3 Select business logic.
- Do not split crates in the same PR that moves runtime state.
- Do not change startup order, readiness, KMS fatal boundaries, IAM recovery,
  lock quorum, object placement, reader behavior, or notification/audit
  lifecycle while shrinking global state.
