# Tier / ILM Transition Debugging Guide

**Use this when:** a tiered (transitioned) object returns `NoSuchVersion` or fails to restore, a transition run looks stuck, or you need to inspect `xl.meta` and trace the versionId sent to the remote tier.

**Source of truth:** `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` (transition/expiry actions, fenced free-version cleanup, and durable-job recovery), `crates/ecstore/src/bucket/lifecycle/manual_transition_job.rs` (job/task/result records), `rustfs/src/admin/handlers/ilm_transition.rs` (manual run/status/cancel and transition-transaction reconcile routes), `crates/utils/src/http/metadata_compat.rs` (dual-key helpers), `crates/filemeta/src/filemeta/version.rs` (`SUFFIX_TRANSITIONED_VERSION_ID` read pattern), `crates/filemeta/examples/dump_fileinfo.rs`.

## Code map

| Concern | Location |
|---------|----------|
| ILM actions (`transition_object`, `expire_transitioned_object`, `get_transitioned_object_reader`, `gen_transition_objname`) | `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` |
| Erasure-set transition/restore entry points | `crates/ecstore/src/set_disk/` and `crates/ecstore/src/store/` |
| `WarmBackend` trait (put/get/remove/in_use) | `crates/ecstore/src/services/tier/warm_backend.rs` |
| Per-provider tier backends (S3, MinIO, GCS, Azure, ...) | `crates/ecstore/src/services/tier/warm_backend_*.rs` |
| Remote-tier sweep (`delete_object_from_remote_tier`) | `crates/ecstore/src/bucket/lifecycle/tier_sweeper.rs` |
| Persisted free-version scan and re-enqueue after local-first expiry | `crates/ecstore/src/bucket/lifecycle/tier_free_version_recovery.rs` |
| Fenced free-version remote delete, local-marker cleanup, and rescan | `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` (`cleanup_free_version_exact`) |
| Durable manual transition job/task/result records | `crates/ecstore/src/bucket/lifecycle/manual_transition_job.rs` |
| Manual run/status/cancel and transition-transaction reconcile admin routes | `rustfs/src/admin/handlers/ilm_transition.rs` |
| `ObjectInfo` / `TransitionedObject` types | `crates/ecstore/src/object_api/types.rs` |
| `FileMeta` / `FileInfo` / version metadata | `crates/filemeta/src/` |
| Dual-key internal metadata helpers (`insert_bytes` / `get_bytes`) | `crates/utils/src/http/metadata_compat.rs` |

## Metadata key conventions

Internal metadata is stored under both `x-rustfs-internal-<suffix>` and `x-minio-internal-<suffix>` for MinIO interoperability. `get_bytes` prefers the RustFS key and falls back to the MinIO key.

| Suffix | Meaning |
|--------|---------|
| `transition-status` | `"complete"` when tiered |
| `transitioned-object` | tier key path (stored without the tier prefix; `get_dest` adds it) |
| `transitioned-versionID` | S3 version_id returned by tier PUT (16 raw UUID bytes, or absent) |
| `transition-tier` | tier name |
| `tier-free-versionID` | delete-marker version for free-version sweep |

Reading binary values must reject empty, malformed, and nil values (regression covered in `crates/filemeta/src/filemeta/version.rs` tests):

```rust
get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID)
    .and_then(|v| Uuid::from_slice(v.as_slice()).ok())
    .filter(|u| !u.is_nil())
// None for: absent key, wrong-length bytes, nil UUID
```

`transition_version_id == None` means the tier bucket is unversioned; the GET/DELETE against the tier must then send no `versionId` parameter. A nil UUID (`00000000-...`) sent as `?versionId=` causes `NoSuchVersion`. Do not use `Uuid::from_slice(..).unwrap_or_default()` here: it converts an empty metadata value into `Uuid::nil()`, which is exactly that failure.

## Inspect xl.meta directly

```bash
cargo build -p rustfs-filemeta --example dump_fileinfo
./target/debug/examples/dump_fileinfo /srv/rustfs/data/disk0/{bucket}/{object}/xl.meta
# Shows: transition_status, transition_tier, transitioned_obj, transition_ver_id
```

| Output | Meaning |
|---|---|
| `transition_ver_id: <none>` | No versionId will be sent to the tier (correct for a non-versioned tier bucket). |
| `transition_ver_id: <uuid>` | That UUID will be sent as `?versionId=<uuid>`. |

There is one `xl.meta` per erasure shard disk (`{disk}/{bucket}/{object}/xl.meta`); all shards of a healthy object should be identical. `dump_versions` (same crate) lists every version in a file.

## Trace the versionId at runtime

```bash
RUST_LOG=rustfs_ecstore::bucket::lifecycle=debug rustfs ...
```

| Log line | Level | Meaning |
|---|---|---|
| `fetching transitioned object from tier` | DEBUG | Emitted before the tier request. |
| `tier GET failed` | ERROR | Includes `tier_version_id`. |

If both `x-rustfs-internal-transitioned-versionID` and `x-minio-internal-transitioned-versionID` are the empty string, the object was transitioned to a non-versioned tier bucket and no versionId must be sent.

## Manual transition run

Manual transition run is an operator trigger for the existing lifecycle transition evaluator. It does not force objects that are not due under the bucket lifecycle rule, and it does not bypass versioning, replication, delete-marker, directory-marker, tier, or in-flight transition checks.

```text
POST /rustfs/admin/v3/ilm/transition/run?bucket=<bucket>&prefix=<prefix>&tier=<tier>&dryRun=true&maxObjects=10000&maxDurationSeconds=30
```

| Parameter | Contract |
|---|---|
| `bucket` | Required. |
| `prefix`, `tier`, `dryRun` | Narrow the run. |
| `mode` | `enqueue_only` by default; set `async` for a durable background job. |
| `async` | Compatibility boolean for selecting durable async mode. It must not conflict with `mode`. |
| `maxObjects` | Defaults to `10000`, capped at `100000`. |
| `maxDurationSeconds` | Optional, capped at `3600`; a best-effort budget checked between listed object versions and pages. An in-flight listing call is not cancelled. |

In the default `enqueue_only` mode, the response reports what this bounded scan evaluated and enqueued into the in-memory transition queue. `state=completed` means the bounded scan reached the end of its current scope without queue pressure or budget truncation; it does not mean every remote tier PUT has completed. `state=partial` means the run stopped early on `maxObjects`, `maxDurationSeconds`, or queue pressure (`skipped_queue_full`, `skipped_queue_closed`, or `skipped_queue_timeout`). Its response omits job endpoints.

With `mode=async` or `async=true`, a successful request returns `202 Accepted`, `mode=durable_job`, a UUID `job_id`, and status/cancel endpoints. Durable mode provides bucket-level persisted admission, restart recovery, a lease-fenced listing checkpoint, task-before-enqueue records, worker-result records, and persisted cancellation. It still delegates each object to the normal lifecycle evaluator and transition transaction; a job never bypasses object eligibility or owns remote deletion.

```text
GET    /rustfs/admin/v3/ilm/transition/jobs/<job-id>
DELETE /rustfs/admin/v3/ilm/transition/jobs/<job-id>
```

GET returns the durable job state and report. DELETE persists `cancel_requested`, which stops new scan work for an active executor. Startup recovery reconciles task/results first only when `scan_completed = true`. An expired job whose scan is incomplete and cancellation is requested is directly CAS-terminalized as `cancelled`, so its stored counters may be stale; scope release follows as a best-effort exact delete. Non-cancelled recovery instead CAS-takes over the lease, replaces the scope admission, replays pending tasks, and resumes object-version scanning from the persisted token. A conflicting live durable job for the same bucket and run/dry-run class returns the active job and its endpoint. The async endpoint itself does not query a fleet capability gate and a direct request proceeds to durable job creation. Before enabling durable mode across a mixed fleet, caller/operator orchestration must check the `manual_transition_jobs` runtime capability on every required node and fail closed if any response is unknown or unsupported.

The persisted states, ownership and recovery rules are specified in [../architecture/ilm-tiering-persistence-contracts.md](../architecture/ilm-tiering-persistence-contracts.md). Terminal job/task/result history currently has no automatic retention bound, so status data remains available until a future protocol-level collector is defined.

Recommended operator flow (external `rc` CLI):

```bash
rc admin ilm transition run local/mybucket --prefix logs/ --tier cold --dry-run --max-objects 1000 --max-duration-seconds 30
rc admin ilm transition run local/mybucket --prefix logs/ --tier cold --max-objects 1000 --max-duration-seconds 30
```

Inspect the aggregate counters before widening scope. Full object-key lists are intentionally not returned. If `RUSTFS_RPC_SECRET` or other credentials were pasted into an issue, chat, log, or ticket while debugging tiering, rotate them on every node, restart the cluster with the new value, and redact the exposed copy before sharing more diagnostics.

## Reconcile an unknown transition upload

Historical transition transactions in `upload_outcome_unknown` state can use an explicit two-stage operator workflow when the tier probe is ambiguous and the provider supports exact version deletion. The endpoint refuses transactions that are still inside their ownership window or are in any other state.

1. Inspect the transaction without changing it:

   ```text
   GET /rustfs/admin/v3/ilm/transition/reconcile/<transaction-id>
   ```

2. If independent provider evidence identifies the exact remote version to remove, submit that opaque version identifier with explicit confirmation. This performs only an exact version delete; the response reports whether the transaction journal was still observed afterwards, since background recovery may have finalized the same transaction concurrently:

   ```json
   POST /rustfs/admin/v3/ilm/transition/reconcile/<transaction-id>
   {
     "action": "delete_candidate",
     "confirm": true,
     "remote_version_id": "<exact-provider-version>"
   }
   ```

3. If the journal remains, inspect again and finalize only after the live provider probe proves the candidate is missing:

   ```json
   POST /rustfs/admin/v3/ilm/transition/reconcile/<transaction-id>
   {
     "action": "finalize_missing",
     "confirm": true
   }
   ```

`finalize_missing` re-runs the provider probe and fails closed for `unversioned_present`, `versioned_present`, `ambiguous`, `unsupported`, or probe errors. It never accepts an operator assertion in place of a live `missing` result. Providers without an authoritative probe or exact version deletion remain pending; the endpoint does not infer provider capabilities, accept external absence assertions, or select a candidate automatically.

## Invariant: local-first expiry ordering

`expire_transitioned_object` deletes local metadata first (making the object unreachable) and leaves a persisted free-version for remote-tier cleanup. `tier_free_version_recovery.rs` scans and re-enqueues that record; the lifecycle worker's `cleanup_free_version_exact` in `bucket_lifecycle_ops.rs` performs the fenced remote delete, local-marker cleanup, and rescan. Never remove a remote tier version while live local metadata still points at it: doing so lets a concurrent GET read a stored version_id whose remote version is already gone and fail with `NoSuchVersion`.

Regression test: `serial_tests::test_expire_transitioned_object_never_races_concurrent_get` in `crates/scanner/tests/lifecycle_integration_test.rs` (CI ILM Integration serial lane) pins both the local-first ordering and the "concurrent GET never sees `NoSuchVersion`" contract.
