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

## Known open issue: expire/GET race

`expire_transitioned_object` deletes the remote tier version **before**
deleting local metadata. A concurrent GET between those two steps reads a
valid stored version_id whose remote version is already gone →
`NoSuchVersion`. The proper fix is to delete local metadata first (making the
object unreachable), then the remote tier version.

## Historical fixes (for context, already merged)

- Nil-UUID versionId sent to tier (`NoSuchVersion`): reading code used
  `Uuid::from_slice(..).unwrap_or_default()`, converting an empty metadata
  value into `Uuid::nil()`. Fixed by the `and_then`/`filter` pattern above.
- `warm_backend_s3sdk` ignored the remote version and range options on
  GET/DELETE.
- `copy_object` returned 501 (`NotImplemented`) for tiered objects on
  self-copy with `--storage-class`, blocking de-tiering via
  `mc cp --storage-class STANDARD obj obj`. Fixed by writing the
  tier-fetched `put_object_reader` back through `put_object`.
