# Bucket Engines

**Use this when:** you add a bucket whose data is not stored by the erasure-coded object layer, give a bucket operations beyond S3, add a per-bucket storage profile, or touch the seams a bucket engine plugs into: bucket metadata extension entries, the S3 service builder, custom bucket routes, internode engine calls, engine-owned drive directories, or engine background workers. The framework lives in `crates/bucket-engine` (contract) and `rustfs/src/bucket_engine` (registry, dispatch, peers, lifecycle).

## Problem

Every bucket is served by one global object store, `ECStore`, reached through `AppContext::object_store` from the S3 use cases in `rustfs/src/storage/s3_api/mod.rs`. Behaviour that differs per bucket is hand-wired: table buckets, on-demand migration, the per-bucket ETag algorithm, per-bucket durability, and lockless cache buckets each add their own metadata field, flag checks across handlers, publication code, and admin routes. That works for flags, but not for a bucket whose data lives in a different store with different operations, such as an LLM KV cache. Without a framework, the second such bucket kind would repeat the scatter at a larger scale.

A bucket engine is a built-in implementation that owns a bucket's data plane. The framework gives an engine a stable place to plug in for configuration, request dispatch, custom operations, internode calls, local storage, background work, and bucket lifecycle, while authentication, authorization, bucket metadata, and IAM stay where they are.

## Model

- **Engine:** a compile-time Rust implementation registered by name in an engine registry at startup. Engines run in-process (`ExtensionRuntimeBoundary::Builtin` in `crates/extension-schema`); dynamic native plugins and WASM are out of scope for the data path.
- **Default engine:** `object`, the erasure-coded object layer. Every bucket without an engine entry uses it, unchanged.
- **Bucket engine entry:** a RustFS extension entry in the bucket metadata (alongside `etag.json` and `durability.json` in `crates/ecstore/src/bucket/metadata.rs`) holding the engine name, the engine's config schema version, and its typed config. ECStore stores it as opaque bytes.
- **Binding:** an engine binds to a bucket incarnation. The binding is chosen when the bucket is created and cannot change while the bucket holds data; changing engines means deleting and recreating the bucket.

## Engine contract

An engine declares and implements the following. The contract types live in `crates/bucket-engine` (`rustfs-bucket-engine`), an ECStore-free crate, so that engines and the dispatch layer can depend on them without new reverse edges (see [crate-boundaries.md](crate-boundaries.md)). It defines the engine trait (`BucketEngine`), descriptors and capabilities, the bucket binding (`BucketEngineBinding`, stored as `engine.json`), and the placement view with rendezvous ownership (`PlacementView::owners`).

| Part | Responsibility |
|---|---|
| Descriptor | Name, version, and capabilities: which S3 operations it serves, whether listing is consistent, and which bucket features it rejects (versioning, Object Lock, quota, replication, lifecycle, notifications, tables) |
| Config | Validate a submitted config into a typed config, and accept or refuse a config change on a live bucket |
| Bucket lifecycle | Create, attach after restart, config change, and purge on bucket delete; report whether the bucket holds data so delete-bucket can enforce emptiness |
| S3 data operations | Serve the S3 operations it declares for its buckets, with the same request types the S3 layer already parses, including the NIXL/RDMA transfer token |
| Custom operations | Named bucket-scoped operations (for example a batch prefix query), each declaring the S3 or admin action that authorizes it |
| Internode calls | Handle engine messages from peers addressed to this engine and bucket |
| Local storage | Use the drive directories the framework assigns it, and recover them at startup |
| Background work | Per-bucket workers (for example an evictor) with a side-effect-free status snapshot and declared side effects, per [background-controller-contract.md](background-controller-contract.md) |

## Framework responsibilities

### Configuration and publication

- The engine entry is written at bucket creation (a CreateBucket extension header carrying the engine name, followed by the config) or through an admin route modelled on the per-bucket ETag routes, gated by a config-update admin action and registered in `rustfs/src/admin/route_policy.rs`.
- Every bucket metadata install path publishes the entry to the engine registry, keyed by bucket and incarnation, and peers reload it through the existing bucket metadata notification. Application-owned bucket config entries reach their consumers through a named subscriber registry (`register_bucket_config_subscriber` in `crates/ecstore/src/bucket/metadata_sys.rs`), so engines and on-demand migration observe bucket config changes side by side.
- A cluster accepts engine buckets only after every node reports engine support; an older node that dropped the unknown metadata field would otherwise serve the bucket as an empty object bucket.

### Request dispatch

- **S3 operations:** a dispatch layer wraps the S3 implementation (`impl S3 for FS` in `rustfs/src/storage/ecfs.rs`) where the service is built in `rustfs/src/server/http.rs`. Requests for buckets bound to an engine go to that engine; everything else reaches `FS` unchanged. Operations an engine does not declare fail with one stable error instead of falling through to the object layer.
- **Authorization first:** dispatch happens after SigV4 verification and `S3Access` authorization in `rustfs/src/storage/access.rs`, so bucket policy, IAM, and anonymous-access rules apply to engine buckets exactly as to object buckets.
- **Custom operations:** a bucket-scoped `S3Route` in the style of `MetadataRoute` (`rustfs/src/app/metadata_route.rs`) matches the engine operation in the request, resolves credentials, rejects unsigned `x-amz-*` headers on SigV4 requests, authorizes the operation's declared action with the same evaluation as S3, and only then calls the engine.

### Internode calls

Engines share one internode channel instead of each adding RPCs: a signed engine call carrying the engine name, bucket, incarnation, operation, and an opaque body, served either as a body-digest-bound `NodeService` RPC with an `auth-policy` declaration or as an internode HTTP route beside the NIXL control routes in `rustfs/src/storage/rpc/http_service.rs`. Bulk data never travels in the call body; it moves over NIXL/RDMA directly between the owning node and the client.

### Local storage

- Each engine gets a per-drive directory under the system volume, `.rustfs.sys/engines/<engine>/<bucket-incarnation>/`, on the local drives returned by `all_local_disk` (`crates/ecstore/src/store/peer.rs`). The system-volume prefix keeps the directory out of bucket listing, the scanner, heal, and the delete-recovery volume walk, all of which skip system volumes.
- Engines do their own file I/O under that directory and choose their own durability; they do not go through the object layer's disk API, which pins system volumes to strict durability.
- Bucket purge removes the engine directories for that incarnation on every node.

### Placement services

The framework exposes a placement view built from the cluster topology: the ordered node list from the configured endpoints, the local node identity, the local drives, and a rendezvous hashing helper. Engines place data with it and never reuse object-to-set hashing in `Sets`, so the object layer's placement and quorum rules are untouched.

### Bucket lifecycle integration

- Delete-bucket asks the engine whether the bucket holds data and, for a forced delete, purges the engine's data before removing the bucket metadata.
- The object namespace of an engine bucket stays empty. Scanner usage, quota, and heal see an empty bucket; the engine reports its own usage through its status snapshot and metrics.
- Bucket features an engine rejects (versioning, Object Lock, quota, replication, lifecycle, notifications, tables) are refused when configured on an engine bucket, at the admin or S3 configuration call.

### Observability

Each engine has its own metrics namespace and logging component constants, and appears in the extension catalog (`/rustfs/admin/v4/extensions/catalog`, `rustfs/src/admin/handlers/extensions.rs`) with its name, version, and capabilities.

## Relationship to existing per-bucket features

- **Lockless cache buckets:** the env-listed lockless mode (`crates/ecstore/src/bucket/lockless_cache.rs`) is an option of the object engine, not a separate engine; it can move into the object engine's config once engine entries exist.
- **Per-bucket ETag and durability:** remain object-engine settings.
- **Table buckets and on-demand migration:** stay as they are. They are candidates to move behind the framework later, which would replace their scattered flag checks with a dispatch point.

## Non-goals

- Loading native code or WASM modules into the data path at runtime.
- Changing how the object layer places, protects, or heals objects.
- Making engine buckets visible through replication, lifecycle, or site replication.
