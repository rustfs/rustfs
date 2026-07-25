# Tier / ILM Transition Debugging Guide

How to debug lifecycle tiering (hot → cold transition) issues: inspecting
`xl.meta`, tracing the versionId sent to the remote tier, and known pitfalls.

> Code map (post `#3929` layout):
>
> | Concern | Location |
> |---------|----------|
> | ILM actions (`transition_object`, `expire_transitioned_object`, `get_transitioned_object_reader`, `gen_transition_objname`) | `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` |
> | Erasure-set transition/restore entry points | `crates/ecstore/src/set_disk/` and `crates/ecstore/src/store/` |
> | `WarmBackend` trait (put/get/remove/in_use) | `crates/ecstore/src/services/tier/warm_backend.rs` |
> | Per-provider tier backends (S3, MinIO, GCS, Azure, …) | `crates/ecstore/src/services/tier/warm_backend_*.rs` |
> | Remote-tier sweep (`delete_object_from_remote_tier`) | `crates/ecstore/src/bucket/lifecycle/tier_sweeper.rs` |
> | `ObjectInfo` / `TransitionedObject` types | `crates/ecstore/src/object_api/types.rs` |
> | `FileMeta` / `FileInfo` / version metadata | `crates/filemeta/src/` |
> | Dual-key internal metadata helpers (`insert_bytes` / `get_bytes`) | `crates/utils/src/http/metadata_compat.rs` |

## Metadata key conventions

Internal metadata is stored under **both** `x-rustfs-internal-<suffix>` and
`x-minio-internal-<suffix>` for MinIO interoperability. `get_bytes` prefers
the RustFS key and falls back to the MinIO key.

| Suffix | Meaning |
|--------|---------|
| `transition-status` | `"complete"` when tiered |
| `transitioned-object` | tier key path (stored **without** the tier prefix; `get_dest` adds it) |
| `transitioned-versionID` | S3 version_id returned by tier PUT (16 raw UUID bytes, or absent) |
| `transition-tier` | tier name |
| `tier-free-versionID` | delete-marker version for free-version sweep |

Reading binary values must reject empty/malformed/nil values (regression
covered in `crates/filemeta/src/filemeta/version.rs` tests):

```rust
get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID)
    .and_then(|v| Uuid::from_slice(v.as_slice()).ok())
    .filter(|u| !u.is_nil())
// None for: absent key, wrong-length bytes, nil UUID
```

`transition_version_id == None` means the tier bucket is unversioned; the
GET/DELETE against the tier must then send **no** `versionId` parameter.
A nil UUID (`00000000-…`) sent as `?versionId=` causes `NoSuchVersion`.

## Inspect xl.meta directly

```bash
cargo build -p rustfs-filemeta --example dump_fileinfo
./target/debug/examples/dump_fileinfo /srv/rustfs/data/disk0/{bucket}/{object}/xl.meta
# Shows: transition_status, transition_tier, transitioned_obj, transition_ver_id
```

- `transition_ver_id: <none>` → no versionId will be sent to the tier
  (correct for a non-versioned tier bucket).
- `transition_ver_id: <uuid>` → that UUID will be sent as `?versionId=<uuid>`.

There is one `xl.meta` per erasure shard disk
(`{disk}/{bucket}/{object}/xl.meta`); all shards of a healthy object should
be identical. `dump_versions` (same crate) lists every version in a file.

## Trace the versionId at runtime

```bash
RUST_LOG=rustfs_ecstore::bucket::lifecycle=debug rustfs …
```

- `fetching transitioned object from tier` — DEBUG, before the tier request.
- `tier GET failed` — ERROR, includes `tier_version_id`.

If both `x-rustfs-internal-transitioned-versionID` and
`x-minio-internal-transitioned-versionID` are the **empty string**, the object
was transitioned to a non-versioned tier bucket and no versionId must be sent.

## Manual transition run

Manual transition run is an operator trigger for the existing lifecycle transition evaluator. It does not force objects that are not due under the bucket lifecycle rule, and it does not bypass versioning, replication, delete-marker, directory-marker, tier, or in-flight transition checks.

The admin endpoint is:

```text
POST /rustfs/admin/v3/ilm/transition/run?bucket=<bucket>&prefix=<prefix>&tier=<tier>&dryRun=true&maxObjects=10000&maxDurationSeconds=30
```

Only `bucket` is required. `prefix`, `tier`, `dryRun`, `maxObjects`, and `maxDurationSeconds` narrow or bound the run. `maxObjects` defaults to `10000` and is capped at `100000`. `maxDurationSeconds` is optional, capped at `3600`, and enforced as a best-effort budget checked between listed object versions and pages; an in-flight listing call is not cancelled.

The current contract is `enqueue_only`: the response reports what this bounded scan evaluated and enqueued into the in-memory transition queue. `state=completed` means the bounded scan reached the end of its current scope without queue pressure or configured budget truncation. It does not mean every remote tier PUT has completed. `state=partial` means the run stopped early because it hit `maxObjects`, `maxDurationSeconds`, or queue pressure (`skipped_queue_full`, `skipped_queue_closed`, or `skipped_queue_timeout`).

`job_id` and `status_endpoint` are currently `null`. There is no durable background job, no restart recovery cursor, no cluster-wide single-flight admission, no status polling endpoint, and no cancel endpoint for this trigger yet. Re-running the command is safe only in the normal lifecycle sense: already in-flight object versions are deduplicated in the local process, but separate nodes do not yet share a durable manual-run admission record.

Recommended operator flow:

```bash
rc admin ilm transition run local/mybucket --prefix logs/ --tier cold --dry-run --max-objects 1000 --max-duration-seconds 30
rc admin ilm transition run local/mybucket --prefix logs/ --tier cold --max-objects 1000 --max-duration-seconds 30
```

Inspect the aggregate counters before widening scope. Full object-key lists are intentionally not returned by the admin response. If `RUSTFS_RPC_SECRET` or other credentials were pasted into an issue, chat, log, or ticket while debugging tiering, rotate them on every node, restart the cluster with the new value, and redact the exposed copy before sharing more diagnostics.

## Historical fixes (for context, already merged)

- Expire/GET race (`NoSuchVersion` during expiry of a tiered object):
  `expire_transitioned_object` used to delete the remote tier version
  **before** local metadata, so a concurrent GET between the two steps read a
  stored version_id whose remote version was already gone. Fixed in `#3491`:
  local metadata is deleted **first** (making the object unreachable), and
  remote-tier cleanup is driven by persisted free-version recovery
  (`crates/ecstore/src/bucket/lifecycle/tier_free_version_recovery.rs`).
  **Invariant — keep local-first ordering**: never remove a remote tier
  version while live local metadata still points at it. Regression test:
  `serial_tests::test_expire_transitioned_object_never_races_concurrent_get`
  in `crates/scanner/tests/lifecycle_integration_test.rs` (runs in the CI
  ILM Integration serial lane) pins both the local-first ordering and the
  "concurrent GET never sees `NoSuchVersion`" contract.
- Nil-UUID versionId sent to tier (`NoSuchVersion`): reading code used
  `Uuid::from_slice(..).unwrap_or_default()`, converting an empty metadata
  value into `Uuid::nil()`. Fixed by the `and_then`/`filter` pattern above.
- `warm_backend_s3sdk` ignored the remote version and range options on
  GET/DELETE.
- `copy_object` returned 501 (`NotImplemented`) for tiered objects on
  self-copy with `--storage-class`, blocking de-tiering via
  `mc cp --storage-class STANDARD obj obj`. Fixed by writing the
  tier-fetched `put_object_reader` back through `put_object`.
