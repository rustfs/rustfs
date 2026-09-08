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
| Dormant tier validation probe intent format and read-only core inspection | `crates/ecstore/src/services/tier/tier_probe_intent.rs` |
| Manual run/status/cancel and transition-transaction reconcile admin routes | `rustfs/src/admin/handlers/ilm_transition.rs` |
| `ObjectInfo` / `TransitionedObject` types | `crates/ecstore/src/object_api/types.rs` |
| `FileMeta` / `FileInfo` / version metadata | `crates/filemeta/src/` |
| Dual-key internal metadata helpers (`insert_bytes` / `get_bytes`) | `crates/utils/src/http/metadata_compat.rs` |

## Lifecycle rule limits and evaluation

Each lifecycle rule supports at most one `Transition` and one `NoncurrentVersionTransition`. A version can make one initial transition; chaining additional tiers after it reaches `complete` is not supported. Splitting stages across overlapping rules does not enable a transition chain. `PutBucketLifecycleConfiguration` rejects multiple entries in either transition array with `InvalidArgument`, including in disabled rules. Existing stored multi-entry arrays are not executed; replace each with a single intended destination. Independent expiration actions in the rule remain eligible.

`Expiration.Days` and `Expiration.Date` are mutually exclusive. A request containing both is rejected instead of silently selecting the date. When expiration and transition are both eligible, expiration takes precedence; a failed earlier transition does not keep an expired object indefinitely. Deadlines select the earliest action within the same action class.

Noncurrent expiration and transition have independent `NewerNoncurrentVersions` limits. A transition with a positive limit waits for a complete version-group evaluation to establish that enough newer noncurrent versions remain. Single-object evaluation, including the current manual transition and immediate-enqueue paths, conservatively defers these counted transitions to the lifecycle scanner. An unmet expiration retention limit does not suppress a separately eligible transition.

An expired restored local copy can be cleaned up under Object Lock because the retained logical version and remote data remain intact. Cleanup requires a completed transition and still waits for pending or failed replication. The storage layer revalidates the source identity and restore metadata before removing the local copy; restore headers alone do not authorize cleanup.

## Free-version recovery controls

The dedicated free-version recovery loop is enabled by default and is independent of the data scanner and heal switches. Setting `RUSTFS_SCANNER_ENABLED=false` does not stop this repair loop. Set `RUSTFS_TIER_FREE_VERSION_RECOVERY_ENABLED=false` before process startup to disable only the dedicated persisted-marker walk. That setting does not disable lifecycle workers or prevent another scanner path from discovering a free version, and it can leave remote cleanup markers pending for longer, so use it as a break-glass pressure control rather than a cleanup mechanism.

Normal transitioned deletes pass a post-commit receipt directly to the lifecycle queue. The namespace walk is the crash, queue-pressure, mixed-version, and historical-record fallback. It cannot safely skip a bucket merely because that bucket has no current lifecycle rule or the referenced tier was removed: an older `xl.meta` free-version can still be the only owner of a required remote DELETE.

One background recovery page completes at most 10,000 logical objects across at most 100 buckets and enqueues at most 1,000 recoverable free versions. The scanner can decode one additional object to detect truncation; a continuation marker preserves the first unscanned bucket or the last completely scanned object. Every truncated page and follow-up sweep waits at least 60 seconds after the previous page completes; failed pages back off from 60 seconds to 10 minutes, and complete idle sweeps exponentially back off to 10 minutes with jitter. Individual walks have no fixed total timeout, but inherit the drive walk stall timeout so a large healthy bucket can make progress without recreating the old timeout/restart loop.

The structured `lifecycle_worker_state` recovery event reports `duration_ms`, `scanned_entries`, `buckets_scanned`, queue counts, truncation, and continuation markers. `rustfs_internal_stage_duration_ms{stage="lifecycle_free_version_recovery"}` records successful page duration; `stage="lifecycle_free_version_recovery_failed"` records failures.

## Metadata key conventions

Internal metadata is stored under both `x-rustfs-internal-<suffix>` and `x-minio-internal-<suffix>` for MinIO interoperability. `get_bytes` prefers the RustFS key and falls back to the MinIO key.

| Suffix | Meaning |
|--------|---------|
| `transition-status` | `"complete"` when tiered |
| `transitioned-object` | tier key path (stored without the tier prefix; `get_dest` adds it) |
| `transitioned-versionID` | Provider version identifier: current exact UTF-8 text, legacy RustFS raw UUID bytes, MinIO's empty unversioned value, or absent for some historical unversioned records. Interpret it only with `transitioned-version-state` or a live compatibility probe. |
| `transition-tier` | tier name |
| `tier-free-versionID` | delete-marker version for free-version sweep |

Legacy raw UUID values must reject empty, malformed, and nil UUIDs (regression covered in `crates/filemeta/src/filemeta/version.rs` tests):

```rust
get_bytes(&self.meta_sys, SUFFIX_TRANSITIONED_VERSION_ID)
    .and_then(|v| Uuid::from_slice(v.as_slice()).ok())
    .filter(|u| !u.is_nil())
// None for: absent key, wrong-length bytes, nil UUID
```

`transition_version_id == None` means only that no usable legacy UUID projection exists; it does not prove the remote bucket's versioning model. Only an explicit `KnownDisabled` state authorizes ordinary GET/DELETE to omit `versionId`. A missing state with an absent or empty version key remains `Unknown` and requires the bounded compatibility probe or the approved reconcile workflow; it never directly authorizes cleanup. A nil UUID (`00000000-...`) sent as `?versionId=` causes `NoSuchVersion`. Do not use `Uuid::from_slice(..).unwrap_or_default()` here: it converts an empty metadata value into `Uuid::nil()`, which is exactly that failure.

## Inspect xl.meta directly

```bash
cargo build -p rustfs-filemeta --example dump_fileinfo
./target/debug/examples/dump_fileinfo /srv/rustfs/data/disk0/{bucket}/{object}/xl.meta
# Shows: transition_status, transition_tier, transitioned_obj, transition_ver_id
```

| Output | Meaning |
|---|---|
| `transition_ver_id: <none>` | No usable legacy UUID projection exists. Inspect `transitioned-version-state` and the raw compatibility keys; do not infer unversioned semantics. |
| `transition_ver_id: <uuid>` | A legacy UUID representation decoded successfully. It is not destructive authority unless the persisted state or reconcile proof establishes the exact remote model. |

There is one `xl.meta` per erasure shard disk (`{disk}/{bucket}/{object}/xl.meta`); all shards of a healthy object should be identical. `dump_versions` (same crate) lists every version in a file.

## Trace the versionId at runtime

```bash
RUST_LOG=rustfs_ecstore::bucket::lifecycle=debug rustfs ...
```

| Log line | Level | Meaning |
|---|---|---|
| `fetching transitioned object from tier` | DEBUG | Emitted before the tier request. |
| `tier GET failed` | ERROR | Includes `tier_version_id`. |

If the version keys are empty while `transitioned-version-state` is absent, the record has the historical MinIO unversioned shape but still remains `Unknown`; only the compatibility probe or reconcile protocol may prove `KnownDisabled`. If state is explicitly `KnownDisabled`, no `versionId` is sent.

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

## Validation probe crash recovery status

Tier Add, Edit, and Verify currently validate a destination with a unique `rustfs-tier-probe-<uuid>` object and perform bounded compensation while the process remains alive. The `rustfs-tier-probe-intent-v1` decoder, canonical durable namespace, conditional storage primitives, state machine, and crate-level inspection type are present only as a dormant foundation. No validation path writes this record, no startup or periodic recovery scans it, and no admin HTTP route exposes it. V1 requires the owner to remain exactly equal to the immutable creator; takeover would require a new schema with explicit proof. Both durable writing and destructive recovery remain disabled until the fleet capability, operation-generation revalidation, provider timeout, retention, and operator contracts are approved.

Do not search the internal metadata bucket for these records as evidence that validation is crash recoverable: a current server does not create them. If a process is killed after the remote probe PUT but before cleanup, inspect the destination provider manually and retain ambiguous candidates. Never delete an empty or guessed version, and do not hand-create a probe intent to authorize cleanup.

Inspect the aggregate counters before widening scope. Full object-key lists are intentionally not returned. If `RUSTFS_RPC_SECRET` or other credentials were pasted into an issue, chat, log, or ticket while debugging tiering, rotate them on every node, restart the cluster with the new value, and redact the exposed copy before sharing more diagnostics.

## Reconcile an unknown transition upload

Historical transition transactions in `upload_outcome_unknown` state can use an explicit two-stage operator workflow when the tier probe is ambiguous and the provider supports exact version deletion. The endpoint refuses transactions that are still inside their ownership window or are in any other state.

Current fleets can produce two valid v1 state profiles. The legacy profile begins at `upload_started@1` and normally reaches `upload_outcome_unknown@2`, `uploaded`, `local_commit_started`, and `committed`. The compact profile is admitted only while every current member proves `transition_transaction_compaction_v1`; it begins at `upload_outcome_unknown@1` and moves directly to `local_commit_started@2` with a known remote version. Treat `upload_outcome_unknown@1` as a pre-PUT fence, not proof that PUT ran. Treat `local_commit_started@2` as an exact commit fence: if the matching `xl.meta` tuple is complete, recovery removes only the record; otherwise it retains the owner evidence. Do not rewrite either state by hand. An unavailable or older peer automatically makes new transitions use the legacy profile.

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

## Inspect and disposition retained recovery records

Current servers expose the routes below for retained recovery controls. Do not remove internal metadata objects by hand: that loses ETag, all-pool, decommission, export, and audit guarantees.

The approved read-only inventory is bounded and paginated:

```text
GET /rustfs/admin/v3/ilm/recovery/records?protocol=<protocol>&classification=<classification>&limit=<n>&marker=<opaque>
GET /rustfs/admin/v3/ilm/recovery/records/<control-id>
```

List and redacted inspect require `admin:ListTier`. The server reconstructs the canonical source identity, strongly reads every authoritative copy, and reports one logical record with its schema, classification (`retrying`, `retained_ambiguous`, `corrupt`, `operator_required`, `abandoned`, or `terminal`), stable reason code, copy/content digests, retry deadline/counters, fleet readiness, scan completeness, and decommission coverage. It does not return raw legacy bytes, object/version names, endpoints, credentials, or provider error text in the default JSON. Incomplete pool coverage, divergent copies, a missing ETag, corruption, or a truncated page without a continuation marker is fail-closed and cannot produce an actionable receipt.

Inspect returns a 15-minute opaque observation receipt. It binds the authenticated actor, canonical record, every source copy/ETag/digest, topology/fleet generation, requested action class, issue/expiry time, and nonce. The receipt prevents a stale request from widening its target; it is not cleanup authority.

For a strictly decoded v1/v2 tier-delete journal, the approved evidence-preserving flow is:

1. Inspect the exact record and independently decide whether retaining the local cleanup obligation is still useful.
2. With `admin:SetTier`, create an immutable server-side export from the current observation receipt. The export contains the exact raw journal bytes and copy manifest, is installed create-only at the canonical digest-derived export ID, strongly read back, and downloaded through a no-store attachment response:

   ```text
   POST /rustfs/admin/v3/ilm/recovery/records/<control-id>
   { "action": "export", "observation_receipt": "<opaque>" }

   GET /rustfs/admin/v3/ilm/recovery/exports/<export-id>
   ```

3. Only after preserving that export, submit a fresh exact disposition with `admin:SetTier`:

   ```json
   POST /rustfs/admin/v3/ilm/recovery/records/<control-id>
   {
     "action": "abandon_remote_cleanup",
     "confirm": true,
     "acknowledge_remote_cleanup_abandoned": true,
     "observation_receipt": "<opaque>",
     "export_id": "<export-id>",
     "export_sha256": "<sha256>",
     "reason_code": "<bounded-operator-reason>"
   }
   ```

The last action removes only the exact local v1/v2 journal generations by per-copy `If-Match` after a durable `Prepared` disposition receipt and fresh all-member capability proof. The receipt advances `Prepared -> Applying -> Completed` and records a monotonic per-copy `confirmed_absent` set. If the server deletes copy A and crashes before recording progress, recovery may confirm A absent under the unchanged source/control, topology, process-epoch, migration, and decommission proofs, persist that progress, and continue with still-exact copy B. A replacement ETag is always a conflict; recovery never widens the immutable copy manifest.

The action never creates a tier client, probes a backend, or issues remote PUT/GET/DELETE. Its meaning is deliberately narrow: the operator accepts that remote storage may leak and abandons RustFS cleanup after preserving evidence. A changed copy, active decommission, missing member, topology/process restart, incomplete read, or uncertain replacement proof retains the evidence. Success requires every bound copy be durably confirmed absent, a fresh all-member/decommission proof, and the disposition receipt durably `Completed`; response loss resumes only the same canonical operation ID.

Canonical replay of an identical export/disposition consumes no new quota. New operations require a complete artifact inventory and are refused before source mutation when the projected retained total, including the fully encoded candidate, would exceed 10,000 exports, 10,000 disposition receipts, 1 GiB of encoded export data, or 256 MiB of encoded control/disposition data. The quota decision, create-only installation, and exact readback share one cluster-scoped admission WRITE lock. That lock is always acquired before control/source/disposition and physical metadata locks and is released before disposition `Applying` or any source deletion; callers never acquire it while holding those inner guards. A crash before installation consumes no capacity, and lost installation response is resolved by canonical readback under the same serialized order, so concurrent nodes cannot oversubscribe a stale snapshot. Admission is also limited to ten new creations per actor per minute, 100 cluster-wide per minute, 32 concurrent exports, and eight concurrent dispositions. Capacity pressure never evicts recovery evidence or blocks ordinary object I/O; the collector examines at most 100 terminal artifacts per minute.

Malformed/unsupported records and journal v3-v6 cannot use abandon. Known-version and v6 manifest ownership must converge through their normal exact recovery protocol. Operators may inspect, export, and request a bounded retry, but cannot bypass source/free-version proof, manifest membership, topology, or version semantics.

Automatic retry state survives restart. Retryable transport/quorum failures use a 60-second exponential base capped at one hour and a deterministic 80-to-100-percent multiplier, so jitter never increases the capped delay. After 32 consecutive failures or seven days from the first persisted failure, automatic work stops at `operator_required`. Unsupported or ambiguous evidence goes directly to `retained_ambiguous`/`operator_required`; age alone never deletes it. Resolved controls, immutable exports, and completed disposition receipts have minimum 30-day, 90-day, and 365-day retention respectively, and are collected only after exact source absence, decommission, successor, and audit checks.

### Retry a retained transition transaction

For a `transition_transaction` control, inspect returns an additional `transition_retry` object when the exact transaction source and recovery-control generation are still consistent. It contains `retry_ready`, `control_revision`, `source_generation_sha256`, the current classification and counters, and a bounded refusal reason. A missing `transition_retry` with `transition_retry_not_ready_reason=source_or_control_not_ready` means the server could not reconstruct exact live evidence; do not retry from an older response.

First perform a dry-run with the exact revision and source-generation digest returned by the latest inspect:

```json
POST /rustfs/admin/v3/ilm/recovery/records/<control-id>
{
  "action": "retry_transition_recovery",
  "mode": "dry_run",
  "expected_control_revision": 7,
  "expected_source_generation_sha256": "<sha256>"
}
```

After repairing the reported storage, tier, or capability problem, repeat inspect and dry-run, then execute with the newly observed values:

```json
POST /rustfs/admin/v3/ilm/recovery/records/<control-id>
{
  "action": "retry_transition_recovery",
  "mode": "execute",
  "expected_control_revision": 7,
  "expected_source_generation_sha256": "<sha256>",
  "confirm": true
}
```

Execution performs one ETag-CAS update of the exact ownerless `retained_ambiguous` or `operator_required` control to `retrying`. It preserves the lifetime attempt count and failure history, clears only the consecutive-failure backoff, and does not mutate the transaction source or issue a tier PUT, GET, probe, or DELETE. The normal recovery worker then acquires a fresh bounded owner lease and repeats every source and remote proof before any side effect.

A historical v1 `UploadStarted` record can return to `retained_ambiguous` because its bytes do not prove whether PUT reached the provider. `LocalCommitStarted` becomes terminal only when the local object still matches the recorded version ID, data directory, modification time, size, ETag, and exact transitioned remote tuple; otherwise it returns to `operator_required`. Retrying is therefore a bounded re-evaluation after an underlying repair, not an override of missing evidence.

The full schema, lease, mixed-version, retry, privacy, and metric requirements are in [../architecture/ilm-tiering-persistence-contracts.md](../architecture/ilm-tiering-persistence-contracts.md#bounded-recovery-control-and-operator-disposition).

## Reconcile legacy transition-version metadata

This section describes an **approved target that is not implemented yet**. The current server has no admin route that backfills a missing `transitioned-version-state` in `xl.meta`. Do not use the transaction reconcile route above for this purpose: that route owns an upload transaction candidate and may delete it, while legacy metadata reconciliation is non-destructive and may update only the exact local metadata version.

The approved interface is synchronous and accepts exactly one bucket/object/local-version tuple:

```text
GET  /rustfs/admin/v3/ilm/transition/state/reconcile?bucket=<bucket>&object=<object>&versionId=<local-version-id>
POST /rustfs/admin/v3/ilm/transition/state/reconcile?bucket=<bucket>&object=<object>&versionId=<local-version-id>
```

`versionId` is required; use the literal `null` for a locally unversioned object. An omitted or empty selector is invalid. GET requires `admin:ListTier`. It reports the authoritative all-pool tuple, destination identity, fleet/topology readiness, live probe classification, opaque expected-tuple digest, and a machine-readable diagnosis. It returns `ready-to-migrate`, not `migrated`, when a missing state is provable because GET is read-only.

POST requires `admin:SetTier`, `confirm: true`, and the complete immutable source tuple, original per-set missing-state representations, proposed target, and reconciliation digest returned by GET. The server rereads every authoritative copy and repeats the bounded live backend probe; provider console output or an operator-supplied state is diagnostic evidence only, never write authority. A retry accepts only copies that still match their digest-bound original representation or already equal the exact proven target; any other divergence is stale or corrupt. The server may persist only one of these exact state/version pairs, together with the bound destination identity:

| Proven remote model | State | Version value |
|---|---|---|
| Versioning disabled | `KnownDisabled` | Empty/absent; later requests omit `versionId` |
| Versioning suspended null object | `SuspendedNull` | Literal `null` |
| One exact version | `Exact` | Exact nonempty, non-`null` opaque identifier |

The response outcome is `migrated`, `retained-ambiguous`, `corrupt`, or `backend-unavailable`. `migrated` means strong all-pool readback proved the same state and destination identity on every authoritative copy; it can be idempotent with `changed=false`. Ambiguous/missing/multiple probe results are retained, and explicit `Unknown`, malformed or conflicting dual keys, nil identifiers, partial tuples, or copies outside the exact `{original missing representation, proven target}` retry subset fail closed. An unavailable backend, tier generation, metadata quorum, or required strong readback reports `backend-unavailable`; a monotonic partial write is retained for retry and never rolled back.

The POST does not issue remote DELETE or PUT, remove local data, create a free-version, clean a transaction/journal, or change tier configuration. It holds the approved fleet/topology, bucket-lifecycle, exact tier-generation/destination, and stable all-pool object-version fences across authoritative reread and the bounded probe; it rechecks them before quorum writes and after strong readback. A fleet containing a node that cannot preserve the explicit state/destination binding is inspect-only, and a cross-pool first match is never enough.

There is intentionally no bucket, prefix, or fleet selector. Batch repair requires a separate durable, resumable job protocol and remains future work. Until the single-record route is implemented, retain affected metadata, use external inspection only for diagnosis, and never hand-edit `xl.meta` or enable remote cleanup by assuming that an empty version field means an unversioned tier.

The full approved fence, quorum, cross-set retry, destination-binding, and mixed-version contract is specified in [../architecture/ilm-tiering-persistence-contracts.md](../architecture/ilm-tiering-persistence-contracts.md#legacy-transitioned-version-state-reconciliation).

## Invariant: local-first expiry ordering

`expire_transitioned_object` deletes local metadata first (making the object unreachable) and leaves a persisted free-version for remote-tier cleanup. `tier_free_version_recovery.rs` scans and re-enqueues that record; the lifecycle worker's `cleanup_free_version_exact` in `bucket_lifecycle_ops.rs` performs the fenced remote delete, local-marker cleanup, and rescan. Never remove a remote tier version while live local metadata still points at it: doing so lets a concurrent GET read a stored version_id whose remote version is already gone and fail with `NoSuchVersion`.

Regression test: `serial_tests::test_expire_transitioned_object_never_races_concurrent_get` in `crates/scanner/tests/lifecycle_integration_test.rs` (CI ILM Integration serial lane) pins both the local-first ordering and the "concurrent GET never sees `NoSuchVersion`" contract.
