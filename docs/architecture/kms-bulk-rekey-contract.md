# KMS Bulk Rekey Job Contract

**Use this when:** changing the bulk envelope re-wrap sweep (`rustfs/src/kms_rekey.rs`), its admin endpoints (`rustfs/src/admin/handlers/kms_rekey.rs`), the re-wrap primitive, or anything that decides which objects a rekey may touch.
**Source of truth:** `rustfs/src/kms_rekey.rs`, `rustfs/src/admin/handlers/kms_rekey.rs`, `rewrap_object_encryption_metadata` in `rustfs/src/storage/sse.rs`, `KmsManager::rewrap_data_key` / `KmsManager::describe_data_key_wrapping` in `crates/kms/src/manager.rs`, `put_object_metadata` in `crates/ecstore/src/set_disk/ops/object.rs`.

The bulk rekey job re-wraps stored data-key envelopes under the current key-encryption key (KEK) without rewriting object bodies. This document is the acceptance bar; where the shipped v1 sweep deliberately narrows it, [Implementation Status](#implementation-status-v1-sweep) records the deviation.

## Scope

- Applies to: job lifecycle, ownership, idempotency, failure semantics, exclusion rules, and completion evidence for bulk envelope re-wrap.
- Out of scope: the cryptographic definition of a single-object re-wrap (owned by the primitive), master key material migration between backends, a pause state, multi-node parallel execution, destruction of superseded key versions.
- Master key material migration is not this job: Vault Transit, AWS KMS, and HSM backends do not export key material, and the one useful case (Local to Local) is already served by `crates/kms/src/backup/local_export.rs` and `crates/kms/src/backup/local_restore.rs`.

## Implementation Status (v1 Sweep)

The shipped sweep (`POST /rustfs/admin/v3/kms/keys/rekey` plus `/status` and `/cancel`, gated on the cluster-scoped `kms:Rekey` action) narrows the contract as follows:

| Contract item | v1 behavior |
|---|---|
| Ownership / admission | One in-memory slot per process serializes sweeps; a second start request is refused with the running job id. No persisted CAS job record, lease, or crash-recovered ownership; counters are process-local and reset on restart. |
| Resume cursor | None. Recovery from crash, cancel, or partial failure is re-running the sweep; every already-current envelope costs one describe-shaped KMS call and no write. |
| Backend gate | Start refuses with `501` when the backend does not advertise `BackendCapabilities::rewrap` (`crates/kms/src/backends/mod.rs`). Vault KV2 and Vault Transit pass; Local, Static, and AWS are refused. |
| Exclusion counting | Plaintext, SSE-C, and MinIO-sealed envelopes are counted together as `not_applicable`; delete markers and directory entries are skipped without counting. |
| Dry run | Not implemented; the closest capability is `/status` counters from a completed sweep. |
| Admission posture | Exactly one object at a time (one KMS round-trip, then at most one metadata write). No workload-admission integration: [workload-admission-contracts.md](workload-admission-contracts.md) defines an observation-only snapshot surface with no runtime admission API for a background job to join. |

Kept exactly as contracted: work units are `(bucket, object, versionId)` with `latest_only: false`; `mod_time` is never set on the rewrap write; object-lock retention is inherited from `put_object_metadata`; every stored envelope copy is replaced by value match across the RustFS-internal and MinIO-compatible slots, and "no replaceable copy found" is an error, not a silent success; failures are counted and logged per object and never abort the sweep; cancellation is cooperative and terminal.

## Terms

| Term | Meaning |
|---|---|
| Envelope | The sealed data key (DEK) stored on an object version's metadata, with the identifiers needed to unseal it. |
| Re-wrap primitive | Single-object operation that unseals one envelope and re-seals it under the target KEK, changing metadata only: `rewrap_object_encryption_metadata` over `KmsManager::rewrap_data_key`. |
| Rekey job | The scan-and-drive layer defined here, applying the primitive across a scope. |
| Work unit | One `(bucket, object, versionId)` triple. Never `(bucket, object)`: each version carries its own envelope. |
| Scope | The bucket and prefix selector that bounds one job; the unit of admission exclusion. |
| Target state | Envelope sealed under the intended key id at the current KEK version. |

## What The Job Does And Does Not Do

- Re-wraps envelopes only. Erasure-coded shards, part layout, ETag, and storage usage are unchanged; only encryption metadata keys may differ. The metadata-only write is `put_object_metadata` (declared on `ObjectStore` in `crates/ecstore/src/store/mod.rs`, dispatched in `crates/ecstore/src/core/sets.rs`, implemented in `crates/ecstore/src/set_disk/ops/object.rs`).
- **Never destroys a superseded key version.** A half-finished job leaves some envelopes under the new KEK version and some under the old; that state is serviceable only because the old version still decrypts. Destruction stays a separate, human-initiated operation gated on usage evidence.
- Must refuse to start when the target key's retention policy would let the superseded version leave the retention window while the job runs.

## Idempotency Model

Idempotency comes from object metadata itself: the envelope's state is the target state, so a re-run reads what is already correct and skips it. No idempotency table; the job identity is a `job_id: Uuid` for reporting and ownership, following `ManualTransitionJobRecord` in `crates/ecstore/src/bucket/lifecycle/manual_transition_job.rs`.

- **The resume cursor is a performance optimization, not a correctness dependency.** Losing a checkpoint may cause a rescan and a higher skip count, never a wrong result. Checkpoints may therefore be throttled (`PersistThrottle` in `crates/heal/src/heal/resume.rs`).
- **At-least-once with target-state idempotency, never exactly-once.** No design may introduce exactly-once machinery for work units.

### Reading the wrapping KEK version

There is no key-version metadata key. Object metadata carries the key id (`x-rustfs-encryption-key-id`) and the sealed blob under `x-rustfs-encryption-key`; `DecryptResponse` in `crates/kms/src/types.rs` does not report a version either. The version is recoverable because the sealed blob is structured: for every backend that builds one, the ciphertext is the JSON of `DataKeyEnvelope` (`crates/kms/src/encryption/dek.rs`), and the read path already discriminates on it via `is_data_key_envelope` in `rustfs/src/storage/sse.rs`.

| Backend | Rotates | Where the wrapping version lives | Recoverable by a scan |
|---|---|---|---|
| Vault KV2 (`crates/kms/src/backends/vault.rs`) | Yes | `DataKeyEnvelope::master_key_version` | Yes, from the envelope JSON |
| Vault Transit (`crates/kms/src/backends/vault_transit.rs`) | Yes | `vault:vN:` prefix of the ciphertext in `encrypted_key`; the envelope's version field is deliberately `None` | Yes, by parsing that prefix |
| Local (`crates/kms/src/backends/local.rs`) | No, rotation is rejected | Nowhere; hardcoded `None` | Moot while rotation is rejected |
| Static (`crates/kms/src/backends/static_kms.rs`) | No | Nowhere; hardcoded `None` | Moot |
| AWS (`crates/kms/src/backends/aws.rs`) | AWS-managed | Inside the opaque `CiphertextBlob`; no `DataKeyEnvelope` | **No** |

Contract rules that follow:

- **`None` does not mean one thing.** KV2: pre-versioning envelope, resolved by `resolve_envelope_master_key_version` to the key's recorded baseline, never implicitly to "current". Transit: permanent and expected; read the ciphertext prefix. Local/Static: unconditional. Version extraction must be dispatched by backend, never inferred from the field alone.
- **Local's `None` is coupled to the Local blocker.** If Local gains rotation history (`rustfs/backlog#1565`), envelope version recording must land in the same change, or Local becomes a second unreadable backend.
- The primitive exposes the wrapping version through **one backend-dispatched accessor** (`KmsManager::describe_data_key_wrapping`) and reports **"already at target state" as an outcome distinct from "re-wrapped"**.
- **AWS is a scoping exception.** Its ciphertext is opaque, so no scan can skip, report version composition, or self-evidence completion; a re-run would rewrap everything. AWS-backed keys are out of scope and must be refused at admission.
- Skip detection costs a base64 decode plus JSON parse (plus a prefix parse on Transit) per work unit: CPU, not I/O, and part of the rate budget rather than free.

## Failure Semantics

- A partially complete rekey is a valid, serviceable state: no emergency handling, no fail-closed startup guard, no rollback. This is the sharpest difference from KMS backup restore, whose intermediate state is unserviceable and fails closed on startup.
- Precondition: superseded key versions remain decryptable (see Blockers).
- Cancellation is cooperative and terminal; restarting on the same scope skips already-processed objects.
- Pause is not provided. None of the tree's long-running job frameworks (ILM manual transition, heal resume, tier mutation intent, decommission/rebalance, scanner, KMS restore) has a pause state; rate control plus cancel-and-restart deliver what pause is asked for without lease/slot/abandonment state.

## Objects That Cannot Be Rekeyed

Enumerated during the scan and excluded with a counted reason; never a job failure; the execution phase must not touch them.

| Class | Disposition | Reason |
|---|---|---|
| SSE-C objects | Exclude and count | The server never holds the customer key. |
| Objects transitioned to a remote tier | Exclude and count | Body lives remotely; see [tier-ilm-debugging.md](../operations/tier-ilm-debugging.md). |
| In-progress multipart uploads | Exclude and count | Each part carries its own envelope; `crates/kms/src/key_impact.rs` models this as a distinct reference scope. |
| Unencrypted objects | Exclude and count | No envelope. |
| Objects under object-lock retention | Governed by the storage layer (see Metadata Write Contract) | |

Replication destinations are unresolved: propagation depends on `rustfs/backlog#1619`. Until it closes, a rewrap never propagates to a replica and each site runs its own sweep.

## Metadata Write Contract

Three properties of `put_object_metadata` (`crates/ecstore/src/set_disk/ops/object.rs`) constrain the re-wrap write:

- **The merge is additive; it cannot remove keys.** Overwriting a key that keeps its name is safe; a re-wrap that changes *which* keys describe the envelope leaves the old keys behind. This is a live hazard: `parse_minio_managed_sealed_key` in `rustfs/src/storage/sse.rs` selects the MinIO decrypt branch on the mere presence of the MinIO seal-algorithm header, so a RustFS-native envelope written onto MinIO-compatible headers without neutralizing them steers reads down the stale branch. Any envelope-shape change must neutralize superseded keys in the same write.
- **Object-lock retention is enforced before the merge.** `check_object_lock_retention_update` (`crates/ecstore/src/set_disk/mod.rs`) runs first; rekey inherits its decision and must not acquire a bypass.
- **`mod_time` is preserved unless the caller sets it.** The re-wrap path leaves it unset so age-based lifecycle rules are not perturbed.

## Skeleton, Ownership, And Admission

- Structural template: `ManualTransitionJobRecord` (`job_id`, `scope_key`, `owner_id`, `lease_id` with expiry, state machine with explicit `Unknown`, `cancel_requested`, report, queue snapshot), persisted with S3 conditional writes so ownership transitions are compare-and-swap; capability advertisement via `ManualTransitionJobCapabilities` in `rustfs/src/admin/handlers/system.rs`.
- Ownership is scope-scoped: disjoint scopes may run concurrently; same-scope jobs are refused by admission. The scanner leader lock in `crates/scanner/src/scanner.rs` is the wrong granularity (one worker per cluster) and is a fencing reference only.
- First implementation is single-node; multi-node parallelism is a throughput optimization deferred until correctness evidence exists.
- Taken from KMS backup: the durable file commit protocol in `crates/kms/src/backends/local.rs` (`CommitStep` failpoints); the write-receipt ownership proof and `VaultRestoreSequence` phase guard in `crates/kms/src/backup/vault_restore.rs` (a concurrent writer between read and write-back is a conflict-and-skip, never an overwrite); the three-part zero-write dry-run report model in `crates/kms/src/backup/dry_run.rs`. Not taken: the synchronous, empty-target, all-or-nothing restore lifecycle. The job does not belong in `crates/kms` (which must not depend on `rustfs-ecstore`); KMS supplies the primitive only.

## API Surface

- Live surface: the RustFS endpoints above. The MinIO-compatible batch-job surface (`rustfs/src/admin/handlers/batch_job.rs`) still lists `keyrotate` in `KNOWN_JOB_TYPES` and returns a deliberate `NotImplemented` from `start-job`.
- Rule: **one operation must never have two live semantics.** The batch-job `keyrotate` type must keep refusing until it proxies to this engine with full batch-job semantics or is removed; it must never report success while executing nothing.

## Completion Evidence

Completion is proven only when no object in scope still references the superseded key version. The evidence surface is the key usage inventory in `crates/kms/src/key_impact.rs`, which deliberately has no `in_use` / `unreferenced` / `safe_to_delete` field and instead reports which sources were consulted and how completely. Rekey inherits that discipline: an empty result means nothing was found in the sources scanned, never that nothing references the key.

## Blockers

| Item | Status |
|---|---|
| Local rotation history (`rustfs/backlog#1565`) | Resolved by capability gating: Local stays non-production, rotation stays rejected, and the start endpoint refuses any backend without `BackendCapabilities::rewrap`. If Local ever gains rotation, envelope version recording must land in the same change. |
| Execution chain | Resolved: primitive (`rewrap_data_key`, `describe_data_key_wrapping`), object adapter (`rewrap_object_encryption_metadata`), sweep (`rustfs/src/kms_rekey.rs`). |
| Key usage inventory coverage | Open: `key_impact.rs` still reports `ObjectEnvelopes` and `InProgressMultipartUploads` as not scanned, so sweep counters are evidence from that run only. |
| KMS key list pagination | Open; a job enumerating keys would hit it. |
| Replica propagation (`rustfs/backlog#1619`) | Open; no propagation until it closes. |

## Verification Expectations

Acceptance bar for the full contract (dry-run and checkpoint items await those features; a cursor-free sweep satisfies the checkpoint clause vacuously):

1. Dry run performs zero storage writes.
2. Non-rekeyable objects are excluded and counted rather than failing the job.
3. An immediate second run skips every object and writes no metadata, on both a KV2-backed and a Transit-backed scope.
4. A scope on an AWS-backed key is refused at admission.
5. An envelope with no recorded version is classified by backend, not by the bare `None`.
6. Deleting the checkpoint changes only the skip count, not the outcome.
7. A killed and recovered job reaches a terminal state while every object stays readable throughout.
8. A concurrent writer causes conflict-and-skip, not an overwrite.
9. ETag, part layout, and storage usage are unchanged at the `xl.meta` level.
10. Each version of a multi-version object is processed independently with its `versionId` intact.
11. Superseded key versions still exist and still decrypt afterward.
12. Success, skip, exclusion, conflict, and failure counts sum to the number of work units scanned.
