# KMS Bulk Rekey Job Contract

This document defines the contract for the object-side bulk rekey job: a long-running administrative job that re-wraps stored data-key envelopes under the current key-encryption key (KEK) without rewriting object bodies. It is a design contract, not an implementation. No execution engine exists in the tree today.

It tracks [`rustfs/backlog#1642`](https://github.com/rustfs/backlog/issues/1642), which lands the `bulk migrate/rekey` line of [`rustfs/backlog#1562`](https://github.com/rustfs/backlog/issues/1562).

## Scope

- PR type: `docs-only`.
- Baseline: `f34aba1be7`.
- Applies to: the job lifecycle, ownership, idempotency, failure semantics, exclusion rules, and completion evidence for bulk envelope re-wrap.
- Out of scope, and deliberately so: the cryptographic definition of a single-object re-wrap (owned by the re-wrap primitive), master key material migration between KMS backends, a pause state, multi-node parallel execution, and destruction of superseded key versions.

### Why master key material migration is not this job

Vault Transit, AWS KMS, and HSM backends are designed so that key material cannot be exported. There is no path that moves a Local master key into Transit, and the reverse direction would export production key material from an HSM onto local disk, which is a security regression. The one case that is both possible and useful, Local to Local, is already served by the KMS backup and restore bundle in `crates/kms/src/backup/local_export.rs` and `crates/kms/src/backup/local_restore.rs`. Nothing in this contract creates a second, weaker copy of that capability.

## Terms

| Term | Meaning |
|---|---|
| Envelope | The sealed data key (DEK) stored on an object version's metadata, together with the identifiers needed to unseal it. |
| Re-wrap primitive | A single-object operation that unseals one envelope and re-seals it under the target KEK, changing metadata only. It does not exist in the tree yet. |
| Rekey job | The scan-and-drive layer defined by this document, which applies the re-wrap primitive across a scope. |
| Work unit | One `(bucket, object, versionId)` triple. Never `(bucket, object)`: each version carries its own envelope. |
| Scope | The bucket and prefix selector that bounds one job, and the unit of admission exclusion. |
| Target state | The envelope state the job is driving toward: sealed under the intended key id at the current KEK version. |

## What the Job Does And Does Not Do

The job re-wraps envelopes. It never rewrites object bodies. Erasure-coded shards, part layout, ETag, and storage usage must be unchanged across a rekey; only encryption metadata keys may differ. Metadata-only rewrite is supported by the storage layer: `put_object_metadata` is declared on `ObjectStore` in `crates/ecstore/src/store/mod.rs`, dispatched in `crates/ecstore/src/core/sets.rs`, and implemented in `crates/ecstore/src/set_disk/ops/object.rs`, where it takes a namespace write lock, selects the version named by `opts.version_id`, and merges `opts.eval_metadata` into the existing `FileInfo` metadata under read and write quorum.

**The job never destroys a superseded key version.** This is the hardest constraint in this contract, and every other guarantee rests on it. A job that fails halfway leaves some objects wrapped under the new KEK version and some under the old one. That state is fully serviceable — reads and writes both succeed — precisely and only because the old version can still decrypt. Destroying old versions from inside the job would convert a resumable operational action into irreversible data loss on partial failure. Destruction stays a separate, human-initiated operation gated on usage evidence.

A job must therefore refuse to start when the target key's retention policy would allow the superseded version to leave the retention window while the job runs.

## Idempotency Model

Re-running the job must be safe and must converge. The intended source of idempotency is the object metadata itself: the envelope's own state is the target state, so a re-run reads what is already correct and skips it. No separate idempotency table is required, and the job identity is only a `job_id: Uuid` for reporting and ownership, following the ILM manual transition job record in `crates/ecstore/src/bucket/lifecycle/manual_transition_job.rs`.

Two consequences follow, and both are contract requirements:

- **The resume cursor is a performance optimization, not a correctness dependency.** Losing a checkpoint may cause a rescan and a higher skip count, never a wrong result. This is what makes crash recovery cheap: checkpoints may be throttled rather than written per object, following the `PersistThrottle` policy in `crates/heal/src/heal/resume.rs`, which flushes after a bounded number of buffered mutations or a bounded interval, whichever comes first. That module states the same reasoning for heal: because the operation is idempotent, a crash re-does at most one throttle window.
- **The job is at-least-once with target-state idempotency, never exactly-once.** No design may introduce exactly-once machinery for work units.

### Reading the wrapping KEK version

The self-evidencing property above holds only when the wrapping KEK version is observable. It is, for every backend that actually rotates, but not from a dedicated metadata field and not by the same mechanism on each backend.

There is no key-version metadata key: object metadata carries the key **id** (`x-rustfs-encryption-key-id` in `rustfs/src/storage/sse.rs`, defaulting to `default`) and the sealed blob under `x-rustfs-encryption-key`, and nothing else names a version. `DecryptResponse` in `crates/kms/src/types.rs` does not report one either, though `EncryptResponse` does.

The version is nonetheless recoverable, because the sealed blob is structured. `x-rustfs-encryption-key` stores the base64 of the backend ciphertext, and for every backend that builds one that ciphertext is the JSON of `DataKeyEnvelope` (`crates/kms/src/encryption/dek.rs`). Reading it needs no new metadata: base64-decode the value, then parse the JSON. The read path in `rustfs/src/storage/sse.rs` already does exactly this discrimination, calling `is_data_key_envelope` on the decoded blob to pick a provider, so this is an established in-tree pattern rather than a new capability.

Where the version sits inside that structure is backend-specific:

| Backend | Rotates | Where the wrapping version lives | Recoverable by a scan |
|---|---|---|---|
| Vault KV2 (`crates/kms/src/backends/vault.rs`) | Yes | `DataKeyEnvelope::master_key_version`, populated from the key record's version | Yes, from the envelope JSON |
| Vault Transit (`crates/kms/src/backends/vault_transit.rs`) | Yes | The `vault:vN:` prefix of the ciphertext held in the envelope's `encrypted_key`; the envelope's own version field is deliberately `None` because Transit ciphertext self-describes | Yes, by parsing that prefix |
| Local (`crates/kms/src/backends/local.rs`) | No — rotation is rejected | Nowhere; the version field is hardcoded `None` because a key has exactly one material | Moot while rotation is rejected |
| Static (`crates/kms/src/backends/static_kms.rs`) | No — single fixed key | Nowhere; hardcoded `None` | Moot |
| AWS (`crates/kms/src/backends/aws.rs`) | AWS-managed | Inside the opaque `CiphertextBlob`; no `DataKeyEnvelope` is built at all | **No** |

Two traps follow, and both are contract rules.

**`None` does not mean one thing.** On Vault KV2 it means a pre-versioning envelope, and `resolve_envelope_master_key_version` resolves it to the key's recorded baseline version, or to the current version for a key that was never rotated — never implicitly to whatever is current now. On Transit it is permanent and expected, and the version must be read from the ciphertext prefix instead. On Local and Static it is unconditional. A scan that reads `None` as a single condition will misclassify three different situations, so version extraction must be dispatched by backend, never inferred from the field alone.

**Local's `None` is coupled to the blocker below.** The Local backend omits the version specifically because rotation is rejected there. When [`rustfs/backlog#1565`](https://github.com/rustfs/backlog/issues/1565) gives Local a rotation history, that construction must begin recording the wrapping version in the same change, or Local silently becomes a second unreadable backend and loses idempotent skip along with it. This coupling is not obvious from either issue and must not be discovered later.

The requirement this places on the re-wrap primitive is therefore narrower than "record a version", most of which the tree already satisfies:

- The primitive must expose the wrapping version through **one backend-dispatched accessor**. The knowledge is currently split between an envelope field and a ciphertext-prefix convention documented only in a comment and pinned by backend tests. A rekey job driving this from ecstore must not reimplement per-backend parsing, which would also put KMS format knowledge on the wrong side of the crate boundary.
- The primitive must report **"already at target state" as an outcome distinct from "re-wrapped"**, so the job counts a skip instead of inferring one.
- For AWS, neither is achievable by inspection, and the contract must say so rather than pretend otherwise (see below).

### The cost of recognizing the target state

Skipping already-current objects is achievable, and it is not free. Every scanned work unit costs a base64 decode plus a JSON parse of its envelope, and on Transit an additional prefix parse. That is CPU and allocation per object version, not extra I/O: the metadata is already being read by the scan, and no KMS round trip is involved. Envelopes are small, so the cost is bounded per object, but at bulk scale it is the dominant cost of a dry run and of the skip check in a re-run, and it belongs in the rate and admission budget rather than being treated as free.

This cost buys three things, all of which the contract requires and none of which are available without it: a re-run that skips completed work and performs zero metadata writes, a dry run that reports which KEK versions are actually in scope, and the per-object half of completion evidence.

**AWS is the exception, and it is a scoping exception rather than a cost.** Its ciphertext is opaque to RustFS, so no inspection can tell a current envelope from a stale one. A rekey scope on an AWS-backed key therefore cannot skip, cannot report version composition in a dry run, and cannot self-evidence completion; a re-run would re-wrap every object again. AWS also rotates backing key material transparently on decrypt, so the operational need that motivates this job is weaker there to begin with. Until there is a reason to do otherwise, AWS-backed keys are out of scope for bulk rekey, and a job must refuse such a scope at admission rather than start one whose re-runs silently rewrite everything.

## Failure Semantics

A partially complete rekey is a valid, serviceable state, not a damaged one. It requires no emergency handling, no fail-closed startup guard, and no rollback. This is the sharpest difference from KMS backup restore, whose intermediate state genuinely is unserviceable and which therefore fails closed on startup when its commit marker is present.

The precondition is that superseded key versions remain decryptable. Where that precondition does not hold, the whole model collapses (see Blockers).

Cancellation is cooperative and terminal. A canceled job reaches a terminal state with already-processed objects left in the target state; restarting on the same scope skips them.

## Pause Is Not Provided

The originating requirement asked for pause, resume, and idempotent retry. This contract provides cancel, cursor restart, and rate control instead, and does not provide a pause state.

Seven long-running job frameworks exist in the tree — ILM manual transition, heal resume (`crates/heal/src/heal/resume.rs`), tier mutation intent (`crates/ecstore/src/services/tier/tier_mutation_intent.rs`), decommission and rebalance (`crates/ecstore/src/core/pools.rs`), the scanner (`crates/scanner/src/scanner.rs`), and KMS backup restore. None of them has a pause state; each has cancel or stop only. That consistency is a design position, not an oversight. A paused job has to answer what it still holds: whether its lease is renewed, whether it keeps its scope admission slot, and how long it may stay paused before it is abandoned. Each answer adds state and a failure mode.

The two things pause is actually asked for are that the job must not overwhelm the data path, and that stopping it must not throw away progress. Rate and admission control delivers the first; cancel plus cursor restart delivers the second. Both are existing patterns.

## Objects That Cannot Be Rekeyed

These must be enumerated during the scan and excluded with a counted reason. Encountering one is never a job failure, and the execution phase must not touch them.

| Class | Disposition | Reason |
|---|---|---|
| SSE-C objects | Exclude and count | The server never holds the customer key, so it can neither unseal nor re-seal the envelope. |
| Objects transitioned to a remote tier | Exclude and count | The body lives remotely; the relationship between local metadata and the remote object's encryption needs its own analysis first. See [tier-ilm-debugging.md](../operations/tier-ilm-debugging.md). |
| In-progress multipart uploads | Exclude and count | Each part carries its own envelope and an incomplete upload is not a stable work unit. `crates/kms/src/key_impact.rs` already models this as a distinct reference scope. |
| Unencrypted objects | Exclude and count | No envelope to re-wrap. |
| Objects under object-lock retention | Governed by the storage layer, see below | |

Replication destinations are unresolved: whether an envelope metadata rewrite must propagate to a replica depends on [`rustfs/backlog#1619`](https://github.com/rustfs/backlog/issues/1619). Until that closes, this contract does not authorize propagation.

## Metadata Write Contract

Three properties of `put_object_metadata` constrain the re-wrap write, all confirmed in `crates/ecstore/src/set_disk/ops/object.rs`.

**The merge is additive; it cannot remove keys.** `opts.eval_metadata` entries are inserted into the existing metadata map. There is no removal path. Overwriting a key that keeps its name is therefore safe, but a re-wrap that changes *which* metadata keys describe the envelope leaves the old keys behind permanently.

That is a structural hazard, not a theoretical one. `rustfs/src/storage/sse.rs` selects its decrypt branch on the mere presence of the MinIO-compatible seal-algorithm header: `parse_minio_managed_sealed_key` returns a sealed key whenever that header is present with the expected value, and the caller then takes the MinIO branch in preference to the RustFS-native one. A re-wrap that writes a RustFS-native envelope onto an object carrying MinIO-compatible headers, without clearing them, steers subsequent reads down the stale branch. Any re-wrap that changes envelope shape must neutralize the superseded keys in the same write, and cannot rely on deletion to do it.

**Object-lock retention is enforced before the merge.** `check_object_lock_retention_update`, defined in `crates/ecstore/src/set_disk/mod.rs`, runs before `eval_metadata` is applied. Rekey inherits that decision rather than restating it: whatever that check permits for a metadata update, rekey permits; whatever it refuses, rekey counts as an exclusion. Rekey must not acquire a bypass.

**`mod_time` is preserved unless the caller sets it.** The implementation assigns `fi.mod_time` only when `opts.mod_time` is `Some`. The re-wrap path must leave it unset, so that a rekey does not perturb lifecycle rule evaluation — an age-based expiry or transition rule reading a refreshed `mod_time` across a whole bucket would be a cross-feature regression.

## Skeleton, Ownership, And Admission

The ILM manual transition job is the structural template. `ManualTransitionJobRecord` in `crates/ecstore/src/bucket/lifecycle/manual_transition_job.rs` already carries `job_id`, `scope_key`, `owner_id`, `lease_id` with an expiry, a state machine including an explicit `Unknown` state for a corrupt journal, `cancel_requested`, a report, and a queue snapshot. Records are persisted under dedicated metadata-bucket prefixes with a schema string and checksum, and mutated with S3 conditional writes (`if_match` for updates, `if_none_match` for creates) so that ownership transitions are compare-and-swap rather than last-write-wins. Crash recovery, cooperative cancel via `request_manual_transition_job_cancel`, and capability advertisement through `ManualTransitionJobCapabilities` in `rustfs/src/admin/handlers/system.rs` all follow from that shape.

Ownership is scope-scoped, not cluster-scoped. Two jobs on disjoint scopes may run concurrently; two jobs on the same scope must be refused by admission. The scanner's leader lock with epoch fencing in `crates/scanner/src/scanner.rs` is the wrong granularity here because it enforces exactly one worker per cluster; it stays a reference for fencing technique only.

The first implementation is single-node: one owner plus a lease plus recovery is sufficient for correctness. Multi-node parallel execution is a throughput optimization and is out of scope until correctness and its acceptance evidence are both in place.

Because the job runs online, it is subject to admission control alongside the scanner, heal, and decommission, per [workload-admission-contracts.md](workload-admission-contracts.md). It must not contend its way into the foreground data path.

## What Is Taken From KMS Backup, And What Is Not

Four things transfer:

- The durable file commit protocol in `crates/kms/src/backends/local.rs` — write, fsync the file, publish by rename or hard link, fsync the parent directory — together with its injectable `CommitStep` failpoints.
- The write-receipt ownership proof in `crates/kms/src/backup/vault_restore.rs`. Its distinction is the reusable idea: the list of intended targets proves nothing about ownership, and only a receipt recording the version a write actually landed at may authorize touching that record later; everything else is reported as never-written or not-at-written-version. Bulk rekey faces the identical problem when a concurrent writer modifies an object between the job's read and its write-back. Such an object must be counted as a conflict and skipped, never overwritten.
- The sequence guard `VaultRestoreSequence` in the same module: a small, domain-free state machine that makes phase order structural. Rekey's phases are scan, plan, apply, verify.
- The three-part dry-run report model in `crates/kms/src/backup/dry_run.rs` — blockers, conflicts, and external mismatches, with a permission predicate that requires all three to be empty — and its zero-write contract: the report is pure data with no handles and no drop-time side effects.

The lifecycle model does not transfer, and must not be adapted. Backup restore is synchronous, one-shot, single-node, requires an empty target, has no progress surface, and requires the KMS service to be out of `Running` state; its admin layer says as much in `rustfs/src/admin/handlers/kms_backup.rs`. Its commit marker enumerates every file up front, which does not scale to object counts. Its publish primitive is no-clobber, whereas rekey rewrites existing state by definition. Forcing rekey into that four-phase protocol produces an all-or-nothing transaction over the whole scope, which is not operable at this scale.

The job also does not belong in the KMS crate. `crates/kms/Cargo.toml` does not depend on `rustfs-ecstore` and must not: the job body is object scanning and metadata rewriting, which is ecstore and admin territory. The KMS crate supplies the re-wrap primitive only.

## API Surface

The job reuses the existing MinIO-compatible batch-job endpoints in `rustfs/src/admin/handlers/batch_job.rs`. `KNOWN_JOB_TYPES` there already lists `keyrotate` alongside `replicate` and `expire`, and the module documents that RustFS ships no batch-job execution engine: `start-job` validates the declared type and returns a deliberate `NotImplemented`, unknown types get `InvalidRequest`, `list-jobs` returns an empty list, and the status, describe, and cancel endpoints return a no-such-job error. No job is ever accepted, persisted, or faked as successful.

Two rules follow. A second, RustFS-specific REST surface must not be introduced, because it would leave two live semantics for one operation. And the current `NotImplemented` is an external promise: `start-job` must never report success while no engine can execute the job.

Request shapes should track MinIO's `keyrotate` closely enough for `mc admin batch` to work, but compatibility never justifies accepting semantics RustFS cannot execute safely.

## Completion Evidence

A job that reports success has not proven anything until no object in the scope still references the superseded key version. That evidence surface is the key usage inventory, whose typed foundation already exists in `crates/kms/src/key_impact.rs`. That module is deliberately built so a report can never claim a key is unused: it has no `in_use`, no `unreferenced`, and no `safe_to_delete` field, and instead reports which sources were consulted and how completely they could be read. It lists object envelopes and in-progress multipart uploads among its reference scopes and currently marks both as not scanned.

Rekey must inherit that discipline. An empty result means nothing was found in the sources that were scanned, never that nothing references the key. A report that cannot state its own coverage is not completion evidence, and must not be used to authorize destroying anything.

## Blockers

**Hard blocker — no execution path may be implemented until this closes.** [`rustfs/backlog#1565`](https://github.com/rustfs/backlog/issues/1565), specifically the absence of rotation history in the Local backend. `crates/kms/src/backup/local_restore.rs` records this in its own out-of-scope note: remapping stable key ids would require proving that object envelopes migrate in lockstep, bulk rekey is a non-goal there, and Local has no rotation history. If superseded versions are not retained, a rekey interrupted halfway leaves every unprocessed object permanently unreadable after rotation, which falsifies the partial-completion guarantee this entire contract is built on.

**Hard blocker — the job still has nothing to drive.** Half of this has since landed: the envelope-level re-wrap primitive exists as `KmsManager::rewrap_data_key` and `KmsManager::describe_data_key_wrapping` (`crates/kms/src/manager.rs`), gated by `BackendCapabilities::rewrap`, which Vault KV2 and Vault Transit advertise and Local, Static and AWS do not. `DescribeDataKeyWrappingResponse::is_current` is the "already at target state" signal this contract asks for. What is still missing is the object-level adapter: a primitive callable per `(bucket, object, versionId)` that reads the version's envelope, reconstructs its encryption context, re-wraps, and writes the result back through `put_object_metadata`. Until that exists, nothing outside the KMS crate calls the primitive.

**Affects acceptance, not start.** Key usage inventory coverage over object envelopes, without which completion cannot be proven. KMS key list pagination, which a job enumerating keys would hit. And [`rustfs/backlog#1619`](https://github.com/rustfs/backlog/issues/1619), which decides replica propagation.

## Verification Expectations

For this docs-only contract, the architecture guard scripts must pass and no Rust source, Cargo metadata, CI workflow, Makefile, or runtime config may change.

Implementation work under this contract must be able to demonstrate, at minimum: that dry run performs zero storage writes; that non-rekeyable objects are excluded and counted rather than failing the job; that an immediate second run skips every object and writes no metadata, on both a KV2-backed and a Transit-backed scope, since the two recover the wrapping version by different mechanisms; that a scope on an AWS-backed key is refused at admission rather than accepted as a job whose re-runs rewrite everything; that an envelope with no recorded version is classified by backend rather than by the bare `None`; that deleting the checkpoint changes only the skip count, not the outcome; that a killed and recovered job reaches a terminal state while every object remains readable throughout; that a concurrent writer causes a conflict-and-skip rather than an overwrite; that ETag, part layout, and storage usage are unchanged at the `xl.meta` level; that each version of a multi-version object is processed independently with its `versionId` intact; that superseded key versions still exist and still decrypt afterward; and that success, skip, exclusion, conflict, and failure counts sum to the number of work units scanned.
