# RustFS — CLAUDE.md

S3-compatible object store in Rust, derived from MinIO. Erasure-coded, multi-pool, supports ILM tiering/lifecycle.

## Commands

```bash
cargo build --release --bin rustfs        # production binary
cargo build                               # dev build
cargo check -p <crate>                    # fast type-check one crate
cargo test -p <crate>                     # test one crate
cargo fmt --all                           # format (required before PR)
make pre-commit                           # full pre-PR gate (fmt + clippy + test)
make build-docker BUILD_OS=ubuntu22.04    # Docker cross-build
```

> Agent/PR rules: see `.github/copilot-instructions.md`.
> Crate membership: `Cargo.toml` `[workspace].members`.
> CI gates: `.github/workflows/ci.yml`.

## Workspace layout

```
rustfs/src/main.rs                          # binary entry point
crates/ecstore/src/
  set_disk.rs                               # ErasureSet: transition_object, restore_transitioned_object
  store.rs / store_api/                     # ECStore trait + ObjectInfo / TransitionedObject types
  bucket/lifecycle/
    bucket_lifecycle_ops.rs                 # ILM actions: transition_object, expire_transitioned_object,
                                            #   get_transitioned_object_reader, gen_transition_objname
    tier_sweeper.rs                         # background sweep: delete_object_from_remote_tier
  tier/
    warm_backend.rs                         # WarmBackend trait (put/get/remove/in_use)
    warm_backend_s3.rs                      # HTTP-client based (TransitionClient) — used for S3/MinIO
    warm_backend_s3sdk.rs                   # aws-sdk-s3 based — alternative S3 backend
    warm_backend_minio.rs / _rustfs.rs / …  # per-provider wrappers (all delegate to _s3 or _s3sdk)
    tier.rs                                 # TierConfigMgr, new_warm_backend dispatch
  client/transition_api.rs                  # TransitionClient HTTP plumbing; UploadInfo, to_object_info
  client/api_put_object_streaming.rs        # put_object_do → UploadInfo (version_id from x-amz-version-id)
crates/filemeta/src/
  filemeta.rs                               # FileMeta (xl.meta top-level), is_skip_meta_key
  filemeta/version.rs                       # FileMetaVersion, MetaObject, MetaDeleteMarker
                                            #   → to_fileinfo() reads transition_version_id
                                            #   → set_transition() writes raw UUID bytes
                                            #   → From<FileInfo> for MetaObject writes all meta
  fileinfo.rs                               # FileInfo struct (transition_version_id: Option<Uuid>)
  examples/
    dump_fileinfo.rs                        # CLI: parse xl.meta, print transition fields + metadata
    dump_versions.rs                        # CLI: list all versions in xl.meta
crates/utils/src/http/metadata_compat.rs    # SUFFIX_* constants, insert_bytes/get_bytes (dual RustFS+MinIO keys)
```

## Metadata key conventions

Internal metadata is stored under **both** `x-rustfs-internal-<suffix>` and `x-minio-internal-<suffix>` for MinIO interoperability. `get_bytes` prefers the RustFS key with MinIO fallback.

Key suffixes (from `metadata_compat.rs`):
| Suffix | Meaning |
|--------|---------|
| `transition-status` | `"complete"` when tiered |
| `transitioned-object` | tier key path (without prefix) |
| `transitioned-versionID` | S3 version_id returned by tier PUT (16 raw UUID bytes, or absent) |
| `transition-tier` | tier name |
| `tier-free-versionID` | delete-marker version for free-version sweep |

## Tier / ILM transition architecture

### Transition flow (hot → cold)
1. `transition_object` (lifecycle_ops) → `ECStore::transition_object` → `set_disk.rs`
2. `gen_transition_objname(bucket)` → `{sha256_hash[0..16]}/{uuid[0..2]}/{uuid[2..4]}/{uuid}` (unique per object version)
3. `tgt_client.put_with_meta(dest_obj, …)` → returns `rv: String` (remote S3 version_id, or `""`)
4. `fi.transition_version_id = if rv.is_empty() { None } else { Some(Uuid::parse_str(&rv)?) }`
5. `fi.transitioned_objname = dest_obj` (without tier prefix)
6. Written to xl.meta via `MetaObject::from(FileInfo)` → `insert_bytes(SUFFIX_TRANSITIONED_VERSION_ID, uuid.as_bytes())` (16 raw bytes)

### Tier GET flow (restore/read)
`get_transitioned_object_reader` (lifecycle_ops):
- reads `oi.transitioned_object.name` (= `fi.transitioned_objname`)
- reads `oi.transitioned_object.version_id` (= `fi.transition_version_id.to_string()` or `""`)
- calls `warm_backend.get(name, version_id, opts)`
- `warm_backend_s3.rs::get`: adds `?versionId=…` only when `rv != ""`

### Tier prefix handling
`WarmBackendS3::get_dest(object)` prepends `self.prefix` to the object name.
`transitioned_objname` is stored **without** the prefix — `get_dest` adds it on every call.

### xl.meta on disk
Path: `{disk}/{bucket}/{object}/xl.meta` — one per erasure shard disk.
All shards should be identical for a healthy object.

## Known bugs & fixes 

### Bug 1: `NoSuchVersion` on tier GET — nil UUID sent as versionId
**Root cause**: `transitioned-versionID` metadata key exists with empty string value (0 bytes). Old reading code:
```rust
// OLD — unwrap_or_default() converts 0-byte or wrong-length slice to Uuid::nil()
get_bytes(…).map(|v| Uuid::from_slice(v.as_slice()).unwrap_or_default())
// → Some(Uuid::nil()) → sends ?versionId=00000000-… → NoSuchVersion
```
**Fix** (version.rs, `MetaObject::to_fileinfo` + `MetaDeleteMarker::to_fileinfo`):
```rust
get_bytes(…)
    .and_then(|v| Uuid::from_slice(v.as_slice()).ok())  // None for wrong-length bytes
    .filter(|u| !u.is_nil())                            // None for nil UUID (old write-back)
```

### Bug 2: `warm_backend_s3sdk.rs` ignored rv and range opts
**Fix**: added `req.version_id(rv)` and `req.range(…)` to GET; `req.version_id(rv)` to DELETE.

### Bug 3 (open): race in `expire_transitioned_object`
Order is: delete remote tier version → delete local object.
A concurrent GET between those two steps fetches a valid stored version_id but the tier version is already gone → `NoSuchVersion`.
The proper fix is to delete local metadata first (making the object unreachable) before deleting the remote tier version.

## Debugging tier issues

### Inspect xl.meta directly
```bash
cargo build -p rustfs-filemeta --example dump_fileinfo
./target/debug/examples/dump_fileinfo /srv/rustfs/data/disk0/{bucket}/{object}/xl.meta
# Shows: transition_status, transition_tier, transitioned_obj, transition_ver_id
```
`transition_ver_id: <none>` → no versionId will be sent to tier (correct for non-versioned tier bucket).
`transition_ver_id: <uuid>` → that UUID will be sent as `?versionId=<uuid>`.

### Check what versionId is being sent at runtime
Enable debug logging:
```bash
RUST_LOG=rustfs_ecstore::bucket::lifecycle=debug rustfs …
```
Log line: `fetching transitioned object from tier` (DEBUG before request).
Log line: `tier GET failed` (ERROR on failure, includes `tier_version_id`).

### Metadata key to watch
```
x-minio-internal-transitioned-versionID=   ← empty string = will cause NoSuchVersion with old code
x-rustfs-internal-transitioned-versionID=  ← same
```
If both are empty string, the object was transitioned to a non-versioned tier bucket. The versionId should NOT be sent (fixed in this branch).

## Common patterns

### Writing internal metadata (binary values)
```rust
insert_bytes(&mut meta_sys, SUFFIX_TRANSITIONED_VERSION_ID, uuid.as_bytes().to_vec());
// stores under both x-rustfs-internal-* and x-minio-internal-* keys
```

### Reading internal metadata (binary values)
```rust
get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID)
    .and_then(|v| Uuid::from_slice(v.as_slice()).ok())
    .filter(|u| !u.is_nil())
// Returns None for: absent, wrong-length bytes, nil UUID
```

### WarmBackend trait
```rust
put_with_meta(object, reader, length, meta) -> Result<String>  // returns S3 version_id or ""
put(object, reader, length) -> Result<String>
get(object, rv, opts) -> Result<ReadCloser>  // rv="" means no versionId
remove(object, rv) -> Result<()>
in_use() -> Result<bool>
```
`rv` = remote version, always pass as empty string when `transition_version_id` is None.
