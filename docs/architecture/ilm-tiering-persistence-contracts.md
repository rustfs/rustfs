# ILM And Tiering Persistence Contracts

**Use this when:** changing an ILM transition, tier configuration mutation, manual transition job, tier-delete recovery path, pool decommission, or any code that can create, transfer, or destroy ownership of a remote-tier object.
**Source of truth:** `TransitionTransaction` and `process_transition_transaction_record` in `crates/ecstore/src/bucket/lifecycle/transition_transaction.rs`; `TierMutationIntent` and its conditional store helpers in `crates/ecstore/src/services/tier/tier_mutation_intent.rs`; `TierConfigMgr::update_candidate_with_config_lock` and mutation recovery in `crates/ecstore/src/services/tier/tier.rs`; `handle_tier_mutation_peer_request` in `crates/ecstore/src/services/tier/tier_mutation_peer.rs`; the record encoders and CAS helpers in `crates/ecstore/src/bucket/lifecycle/manual_transition_job.rs`; manual-job execution/recovery and `cleanup_free_version_exact` in `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs`; `process_tier_delete_journal_entry` and manifest recovery in `crates/ecstore/src/bucket/lifecycle/tier_delete_journal.rs`; the free-version scan/re-enqueue path `recover_tier_free_versions` in `crates/ecstore/src/bucket/lifecycle/tier_free_version_recovery.rs`; `DURABLE_ILM_NAMESPACES` and `validate_durable_ilm_record` in `crates/ecstore/src/bucket/lifecycle/durable_namespace.rs`; and `record_durable_ilm_decommission_progress`, receipt verification, and receipt cleanup in `crates/ecstore/src/core/pools.rs`.

This document separates three kinds of statement:

- **Current** describes behavior enforced by the cited code now.
- **Approved target** is a normative safety requirement for subsequent changes. A target is not evidence that the implementation already satisfies it.
- **Open design** identifies a choice that must be resolved before changing the persisted format or destructive recovery behavior.

The approved targets preserve the safety requirements of the ILM/tiering audit backlog. In particular, later optimizations must not infer a remote version, weaken source identity, or substitute process-local coordination for durable ownership.

## Non-negotiable invariants

These are approved-target invariants. A protocol's explicitly labeled current exception below describes an implementation gap; it does not weaken the target.

1. An unknown, absent, malformed, or unsupported remote-version state is not an unversioned object. Destructive recovery fails closed and never guesses an empty `versionId`.
2. A remote candidate may be deleted only when authoritative metadata proves that no live local owner references it, or when a durable time/fence/owner proof shows that its creator can no longer publish that candidate.
3. The approved transition-commit contract preserves and rechecks source version ID, data directory, modification time, size, and ETag. The current recovery predicate is narrower, as documented below. Bucket incarnation, Object Lock, and replication admission remain independent gates.
4. Free-version cleanup preserves the bucket-lifecycle fence, exact tier generation lease, physical-set locks, authoritative all-pool scan, and before/after fence checks.
5. At most one live protocol owns remote DELETE. Ownership moves from transition transaction to committed `xl.meta`, then to an `xl.meta` free-version on ordinary deletion. Only an authorized v6 journal/manifest operation may replace the free-version owner for a destructive prefix operation.
6. A successful metadata write response is not the only durability proof. Every response-lost or quorum-uncertain write must converge through a strong read of the same key and exact identity/generation; an absent, conflicting, or unreadable readback remains unknown.
7. Unknown namespaces, schemas, states, illegal state edges, checksum failures, identity conflicts, truncated scans without a continuation marker, and mixed-version ambiguity fail closed. They are never cleanup authorization. Current transition path validation, manual-job uppercase alias handling, and transition/manual page-token exceptions are documented below as gaps against this target.

## Ownership chain

| Phase | Authoritative owner | May issue remote DELETE? | Ownership transfer evidence |
|---|---|---:|---|
| Remote PUT is in flight or its response is unknown | Transition transaction | Only cleanup of its own canonical candidate, subject to the transaction recovery predicate | Durable transaction identity plus a known remote-version state; the approved target also requires expiry and durable takeover of the creator fence |
| Local transition commit is complete | Exact transitioned version in `xl.meta` | No | Current recovery finds the transaction's logical bucket/object/version and checks `TRANSITION_COMPLETE` plus the same remote object, tier, and remote version. It does not compare the recorded data directory, modification time, size, or ETag; the approved target adds that full source comparison |
| An ordinary delete removes that transitioned version | Hidden `xl.meta` free-version | Yes | Metadata quorum atomically removes the visible version and preserves its exact tier tuple in the free-version |
| A recursive prefix/delete-all operation cannot preserve per-object markers | v6 journal bound to an immutable single dispatch manifest or a chunk-parent-bound child manifest | Yes, but only after child/manifest completion and all-pool absence proof | `DispatchAuthorized`, exact local destructive mutation, every journal `Committed`, then child/manifest `Completed`; a chunk parent advances only after that child completion |
| Tier configuration mutation, manual job, or decommission receipt | Intent/admission/copy proof only | No | These records gate configuration, scheduling, or migration; they never become remote-object cleanup owners |

An old journal and a free-version can coexist during compatibility recovery. That coexistence is evidence of multiple possible owners, not permission to choose one: the journal path must retain its record until the version-specific recovery rule proves which owner is authoritative.

## Persisted record inventory

All keys below are objects in the internal metadata bucket. The table gives the canonical target form. Transition-transaction runtime recovery extracts the final 32 hexadecimal characters and UUID while ignoring shard directories and accepting uppercase hex. Manual-job runtime recovery requires exactly two shards matching the filename prefix, but accepts an uppercase UUID when the shards use the same uppercase text; it then loads the lowercase canonical job by UUID. The decommission validator recomputes and rejects a noncanonical manual-job path, but currently inherits the weaker transition parser. Exact runtime canonical-path validation for both protocols is an approved target.

| Protocol | Current schema/version | Canonical key | Creator and cleanup owner | Authoritative identity and mutable fields | Current durability point |
|---|---|---|---|---|---|
| Transition transaction | `rustfs-transition-transaction-v1` | `ilm/transition-transactions/records/<aa>/<bb>/<transaction-id>.json` | The transition attempt creates it; transition commit/recovery cleans it | Immutable/fence identity: deployment, transaction, fixed `owner_epoch`, write, source identity, tier/backend fingerprint, canonical remote object, deadline. Mutable: state, remote version, revision. `TransitionCleanupProof` is only a transient admission input to `mark_cleanup_pending`; it is not persisted in the record | Maximum-parity config write. Current create/update/delete calls do not use ETag preconditions |
| Tier mutation peer intent | `rustfs-tier-mutation-intent-v1` | `tier/mutation-intents/records/<aa>/<bb>/<mutation-id>.json` | The receiving peer creates and converges it; the mutation recovery path cleans it | Immutable: mutation ID/kind, old config ETag, candidate digest, sorted affected target identities, expiry. Mutable: revision, state, committed config ETag | Create with `If-None-Match: *`; transition/delete with ETag `If-Match`; maximum parity |
| Tier mutation coordinator intent | `rustfs-tier-mutation-intent-v1` | `tier/mutation-intents/coordinators/<aa>/<bb>/<mutation-id>.json` | The initiating node creates it; coordinator recovery cleans it after peer convergence | Same mutation identity and mutable fields as the peer record | Same conditional-write contract as the peer intent |
| Manual job | `rustfs-manual-transition-job-v1` | `ilm/manual-transition/jobs/<aa>/<bb>/<job-id>.json` | The admin run creates it; the active owner or recovery lease advances it. There is no current record GC owner | Immutable: job ID, bucket-level scope, options, creation time. Mutable: owner/lease, state, cancel bit, cursor, progress/report, queue snapshot, timestamps/error | Initial UUID-key write uses maximum parity without create-only precondition; later updates use ETag CAS |
| Manual scope admission | `rustfs-manual-transition-job-v1` | `ilm/manual-transition/scopes/<aa>/<bb>/<scope-digest>.json` | Job admission creates/renews it; the job owner removes it after terminalization | Immutable bucket/run-vs-dry-run scope; mutable job/lease ownership and expiry | Create-only, renew/delete by ETag CAS |
| Manual task | `rustfs-manual-transition-task-v1` | `ilm/manual-transition/tasks/<job shards>/<job-id>/<task-key>.json` | The scanner persists it before queue admission; no current GC owner | Immutable job plus exact bucket/object/version/tier work identity | Append-only create with `If-None-Match: *` and maximum parity |
| Manual worker result | `rustfs-manual-transition-worker-result-v1` | `ilm/manual-transition/results/<job shards>/<job-id>/<task-key>.json` | The worker persists it after an actual result; no current GC owner | Immutable job/task key and outcome/reason | Append-only create with `If-None-Match: *` and maximum parity |
| Legacy tier-delete journal | Versions 1 through 5 | `ilm/tier-delete-journal/<identity-digest>.json` | The deleting path creates it; version-specific journal recovery cleans it | Remote tuple; v2 adds backend identity, v3 exact version, v4 version state, v5 stable source and transaction state | v5 state changes use ETag CAS; v3/v4 recovery rereads and conditionally cleans. Initial legacy-compatible writes can still be unconditional |
| Sole-owner tier-delete journal | Version 6 | `ilm/tier-delete-journal-v6/<operation-id>/<identity-digest>.json` | The manifest coordinator creates/dispatches it; the journal worker deletes the remote object and cleans the record | Exact remote/source/backend identity plus manifest/operation/topology binding; mutable state `Prepared`/`Dispatched`/`Committed` | Create-only and fenced ETag CAS. Record cleanup writes a terminal receipt first only while a decommission run is active; ordinary recovery without one conditionally deletes the exact ETag directly |
| Tier-delete dispatch manifest | Version 1 | Single dispatch: `ilm/tier-delete-dispatch-manifests/<scope-digest>.json`; chunk child: `ilm/tier-delete-dispatch-manifests/chunks/<scope-digest>/<operation-id>.json` | The prefix-delete coordinator creates it; manifest recovery is its only rollback/completion owner | Immutable operation, bucket/incarnation/prefix, sorted journal set/count/digest, topology generation; mutable manifest state | Create-only and fenced ETag CAS; lost authorization response requires exact strong readback. A child cannot authorize local mutation without the exact active parent binding |
| Tier-delete chunk parent | Version 1 with `record_type = "chunked_parent"` | `ilm/tier-delete-dispatch-manifests/<scope-digest>.json` | The over-limit prefix-delete coordinator creates and advances it; parent recovery advances completed children and removes the terminal parent | Immutable operation, bucket/incarnation/prefix/topology; mutable monotonic revision, next child sequence, completed journal count, one optional exact child binding, and `Active`/`Completed` state | Create-only and fenced ETag CAS. The parent binds a `Preparing` child before it can become `DispatchAuthorized`; final `Completed` follows an error-free, non-truncated empty-candidate rescan and local prefix deletion |
| Decommission durable-namespace receipt | `v2` | `decommission/ilm-receipts/<run-token>/<source-path>/<id-kind>/<id>.json` | The decommission coordinator writes target/source proof and is the only cleanup owner for that run | Source path, namespace and record identity, monotonic checkpoint, optional terminal checkpoint, optional v6 topology generation | Create-only then ETag CAS merge; checksum envelope; maximum parity |
| Decommission expected-receipt manifest | `v1` | `decommission/ilm-manifests/<run-token>.json` | The source-pool decommission coordinator creates and cleans it | Run token plus exact sorted receipt-path count/digest | Create-only, exact readback, and verification before pool removal |

`durable_namespace.rs` registers exactly the two tier-journal namespaces, the dispatch-record namespace shared by single manifests, chunk children, and chunk parents, the transaction namespace, and four manual-job namespaces. A path beginning with `ilm/` that is not in that registry is an error during decommission rather than an ignorable object.

## Durable fences and write primitives

### Current

The internal config layer supplies maximum-parity writes, create-only writes, ETag compare-and-swap, and ETag conditional delete. Their safety roles are distinct:

| Primitive | Safe use |
|---|---|
| Maximum-parity write | Makes bytes durable at the configured metadata quorum; by itself it does not establish unique creation or prevent a stale overwrite |
| `If-None-Match: *` | Installs a UUID/digest-keyed identity once; a conflict must be decoded and compared, never treated as generic success |
| ETag `If-Match` | Advances exactly the observed generation; a precondition failure forces reload and identity/state validation |
| Conditional delete | Removes only the verified terminal generation; if an active decommission covers it, its terminal receipt is written first |
| Strong readback | Resolves a lost response only when key, schema, full immutable identity, state, and expected successor all match |

Tier mutation intents and v6 journal/manifest records use the conditional primitives. Manual job updates, scope admission, and decommission receipts also use CAS after creation. The transition transaction currently carries a fixed `owner_epoch` fence identity and a mutable `revision`, but persists with unconditional writes and deletes; those fields therefore detect some in-memory misuse but are not yet a durable exclusion fence. Changing `owner_epoch` during takeover is not current behavior and remains an open design. The manual job's initial UUID-key write has the same create-only gap, although all later owner/lease updates are CAS-protected.

### Approved target

- Every independently generated record is installed create-only. A duplicate is accepted only after byte-equivalent or semantic-identity validation.
- Every mutable transition is a legal single-generation successor written with ETag CAS. The persisted revision must advance monotonically and the expected owner/lease/epoch must be part of the compared generation.
- Every cleanup reloads the terminal generation, validates identity and cleanup evidence, records any required decommission terminal checkpoint, and conditionally deletes that exact ETag.
- After a timeout, connection loss, or quorum-uncertain response, the caller must strongly reread. Only the exact intended successor is success; predecessor, absence, conflict, corruption, or unavailable readback retains the record and blocks destructive action.
- A process-local mutex, cancellation token, task registry, or cached generation may reduce duplicate work but cannot authorize publication, rollback, or remote deletion.

The exact transition-transaction lease/takeover fields and whether the existing `not_after` becomes the owner expiry are an **open design**. They must be settled with upgrade/downgrade behavior before the v1 schema changes.

## Lock and operation order

Lock ordering is part of the recovery contract. Callers acquire only the locks needed for the row they execute and release them in reverse order.

| Path | Current acquisition order | Operations allowed while held | Operations forbidden while held |
|---|---|---|---|
| Tier edit/remove/clear | Tier-config namespace WRITE lock; dedicated owned `admin_updates` serialization mutex; short `TierConfigMgr` state locks only while accessing manager/runtime state | The dedicated `admin_updates` guard intentionally spans awaited backend validation/probes, peer Prepare/Commit/Abort RPC, reference scans, config CAS, and candidate publication in the current protocol | Ordinary manager `RwLock` and runtime-state `Mutex` guards must not cross awaited network I/O; that rule does not prohibit the dedicated `admin_updates` guard from spanning those awaits. Remote object DELETE is never part of mutation |
| v6 manifest prepare | Caller already holds the bucket-lifecycle WRITE fence; caller acquires a bucket-metadata transaction READ guard covering the Object Lock and bucket-incarnation snapshot and keeps it through local mutation; exact tier-generation leases; fleet/topology proof; for a single dispatch, synthetic manifest-operation WRITE; for a child, parent-operation WRITE then child-operation WRITE | Build and write one immutable bounded journal set and manifest, validate exact set/digest, then authorize local dispatch while both caller-held bucket guards and all leases remain current. A parent binding is durable before child authorization | Remote tier DELETE; per-object worker cleanup; releasing the metadata guard or a required lease before the authorized local mutation completes; child-to-parent nested lock acquisition |
| v6 manifest/parent recovery | Fleet/topology proof; bucket-lifecycle WRITE lock; then exactly one synthetic manifest- or parent-operation WRITE lock | Read/write manifest, parent, and journal metadata; verify exact set/digest/binding; converge or roll back child records; advance a parent only after child completion | Remote tier DELETE; per-object worker cleanup; rollback after authorization; taking a child lock while holding a parent lock in background recovery |
| v5 journal destructive recovery | Synthetic per-journal recovery lock; bucket-lifecycle READ lock; exact tier-generation lease; all physical object READ locks in stable pool/set order | Authoritative source/free-version scan; fenced state CAS; for an eligible terminal state, one bounded remote DELETE; conditional record cleanup | Any delete when a lock or lease is lost; publishing local metadata; selecting an arbitrary backend/version |
| v6 journal destructive recovery | Synthetic per-journal recovery lock; fleet/topology proof; bucket-lifecycle READ lock; exact tier-generation lease; all physical object READ locks in stable pool/set order | Immutable manifest/topology validation, authoritative source/free-version scan, fenced state CAS, and, for an eligible terminal state, one bounded remote DELETE followed by record cleanup | Any delete when a lock, lease, or fleet proof is lost; publishing local metadata; selecting an arbitrary backend/version |
| Free-version cleanup | Bucket-lifecycle READ lock; exact tier-generation lease; all physical object WRITE locks in stable pool/set order | Exact all-pool scan; bounded remote DELETE; local marker removal; post-delete rescan | Deleting before the free-version is the sole owner or after any fence changes |
| Transition commit | Existing object commit locks plus exact source identity and tier-generation checks in the transition path | Publish the exact remote tuple into the matching local version | Publishing a tuple after the source identity or generation changes |
| Transition transaction cleanup | Record validation; exact backend-generation lease inside the probe/delete helper. Current recovery has no explicit bucket-lifecycle/physical-set ownership fence or durable takeover CAS | Identity-bound provider probe and deletion of a known canonical candidate | A tier lease alone does not fence the creator. The approved target requires expired ownership, durable takeover, and exact local/source reread before DELETE |
| Manual job | Initial maximum-parity job write; then persisted bucket-level scope create/CAS; later short job/task/result metadata operations | List, checkpoint, append tasks before enqueue, append results after work, renew/take over lease | Holding metadata guards across remote transition PUT; treating the local active-job map as cluster authority. A crash between the job write and scope claim can leave `Running` without a scope record |
| Decommission receipt | Decommission coordinator's source/target record workflow; record-specific conditional writes | Copy/validate durable record, advance receipts, construct and verify expected manifest, conditionally clean exact covered source | Remote tier DELETE; deleting an uncovered or divergent source record |

The tier mutation lock scope is intentionally recorded as **current**, not ideal. Reducing it is allowed only after a durable `Prepared` intent blocks new reference creators across the fleet, existing tier-operation leases drain, and recovery can reconstruct that block without the initiating process. Which network validation can move outside the namespace lock is an **open design**.

## Transition transaction

### Current contract

`TransitionTransaction` binds the canonical candidate name to a transaction UUID, write UUID, source identity, tier name, backend fingerprint, remote-version state, deadline, fixed `owner_epoch`, and mutable `revision`. The state model permits these ordinary edges:

```text
UploadStarted -> Uploaded -> LocalCommitStarted -> Committed
             \-> UploadOutcomeUnknown -> Uploaded
             \-> AbortedNoRemote
```

Separately, `mark_cleanup_pending` permits proof-checked model edges from `Uploaded`, `UploadOutcomeUnknown`, and `LocalCommitStarted`. Current production code emits `CleanupPending` only when recovery probes `UploadOutcomeUnknown` as `UnversionedPresent` or as `VersionedPresent` with a non-nil identifier. The `Uploaded` abort/recovery path deletes its candidate and transaction record directly, and `LocalCommitStarted` mismatch or missing-source recovery retains the record. The `Uploaded` and `LocalCommitStarted` cleanup edges are currently exercised through the state-machine API and tests, not produced by runtime recovery. States that require a remote delete still require a known `TransitionRemoteVersion` kind. A probed versioned candidate whose identifier parses as a nil UUID is another current special case: recovery exact-deletes it and removes the record without first persisting `CleanupPending`.

The remote candidate itself is named by `canonical_transition_remote_object` under `ilm/transition-transactions/<bucket-hash>/<transaction shards>/<transaction-id>/<write-id>`. That deterministic identity is what a provider probe or exact cleanup must bind; it is distinct from the internal transaction-record key.

The creator owns the canonical remote candidate until local metadata commits the exact tuple. A committed `xl.meta` version then owns reachability. The transaction record is not a second cleanup owner after that transfer.

### Recovery decisions

| Observed durable state/input | Unique current owner | Current recovery decision | Approved destructive admission |
|---|---|---|---|
| `UploadStarted` | Originating transition attempt; current durable exclusion is incomplete | Retain | No delete. The upload may still publish |
| `UploadOutcomeUnknown`; exact provider probe says missing | Transaction recovery, logically; current record writes do not durably exclude a concurrent worker | Delete the record | Strong probe identity must match transaction/backend; no remote delete occurs |
| `UploadOutcomeUnknown`; probe returns `UnversionedPresent` | Transaction recovery, with operator reconcile available after expiry | Persist `CleanupPending`, delete the unversioned candidate, delete the record | Exact transaction/canonical object/backend identity, explicitly unversioned state, durable takeover after owner expiry, current tier lease, and exact reread before cleanup |
| `UploadOutcomeUnknown`; probe returns `VersionedPresent` with a non-nil exact identifier | Transaction recovery, with operator reconcile available after expiry | Persist `CleanupPending`, exact-delete that versioned candidate, delete the record | Exact transaction/canonical object/backend identity and remote version, durable takeover after owner expiry, current tier lease, and exact reread before cleanup |
| `UploadOutcomeUnknown`; probe returns `VersionedPresent` whose identifier is a nil UUID | Transaction recovery | Current code directly exact-deletes that versioned candidate and deletes the record; it does not persist `CleanupPending` | This remains a versioned exact-delete candidate and must not be treated as `UnversionedPresent`. The approved target still requires durable takeover, a current tier lease, and exact reread |
| `UploadOutcomeUnknown`; probe ambiguous, unsupported, or errors | Transaction recovery retains ownership evidence | Retain | No destructive action; operator reconcile may inspect after expiry |
| `Uploaded` | Originating transition attempt; current recovery can race it because the persisted fence is not CAS-protected | Current code immediately deletes the candidate and record | **Current safety gap:** approved behavior must first prove the creator cannot still commit by expired ownership plus durable takeover/CAS, then recheck that no matching local commit exists |
| `LocalCommitStarted`; logical source lookup returns `TRANSITION_COMPLETE` with the same remote object, tier, and remote version | Transition committer until ownership transfers to `xl.meta` | Delete transaction record | Current recovery treats this tuple as ownership transfer. The approved target additionally compares recorded source version ID, data directory, modification time, size, and ETag before conditional terminal cleanup |
| `LocalCommitStarted`; logical source is missing, its transition tuple differs, or the read is uncertain | Transaction record/recovery | Retain | No remote delete without a separate durable cleanup proof |
| `CleanupPending`; logical source lookup returns the same current transition predicate | `xl.meta` is remote reachability owner; recovery owns only record cleanup | Delete transaction record | `xl.meta` is owner; do not delete remote. The approved target adds the full recorded source comparison |
| `CleanupPending`; logical source is absent or its transition tuple differs | Transaction recovery | Delete exact candidate, then record | Cleanup proof, known version state, exact backend lease, durable owner fence, and before/after identity checks |
| `Committed` or `AbortedNoRemote` | Transaction terminal-record cleanup | Delete record | Terminal generation only; no remote delete |
| Path rejected by the current final-component parser, or payload rejected for schema, checksum, identity, state, or remote-version inconsistency | No recovery actor acquires destructive ownership | Preflight fails and retains the bytes; `process_transition_transaction_record` is not called | No remote DELETE or transaction-record cleanup occurs for that rejected input |
| Mis-sharded transition path, extra path components, or uppercase 32-hex filename whose final UUID matches a record | The canonical record selected by that final UUID remains the transaction owner; the listed path is not independently bound | Current parser can accept the final component, reconstruct the canonical key, and process that record, including destructive recovery; a noncanonical listed object without a canonical peer is left behind | **Current path-validation gap:** approved recovery must recompute and require the exact lowercase canonical path before any record side effect |
| Valid cleanup-capable record; remote DELETE succeeds but transaction-record deletion fails | The retained transaction record remains the cleanup owner | Processing returns an error after the remote side effect. A later pass repeats the same exact idempotent remote DELETE and retries record cleanup | This is retryable partial progress, not preflight rejection or evidence that no destructive side effect occurred |

The operator reconcile routes in `rustfs/src/admin/handlers/ilm_transition.rs` accept only expired `UploadOutcomeUnknown` records. Inspection is non-destructive. Exact candidate deletion requires the operator-supplied version to match a live exact provider probe; finalizing missing requires the live probe to return missing. The operator cannot assert absence or choose among ambiguous candidates.

The record persists source version ID, data directory, modification time, size, and ETag. `local_commit_matches_transaction` currently uses the source bucket/object plus versioning mode and source version ID for the logical lookup. Its positive result then compares transition status, remote object name, tier name, and remote version ID; it does not compare the persisted data directory, modification time, size, or ETag. Full source preservation and comparison is therefore an approved target, not a current recovery guarantee.

The current background loop runs every 60 seconds, scans at most 1,000 records per page, and bounds one recovery attempt to 300 seconds. It processes every returned record before checking whether a truncated page supplied a continuation token. If `truncated = true` arrives with no token, that page may already have deleted a remote candidate or transaction record; the loop then stores an empty marker and rescans the first page on its next tick. Cancellation, timeout, or per-record error otherwise retains the affected record. Validating pagination integrity before any page side effect and failing closed on a missing token is an approved target; these work limits do not bound record age.

### Approved target and open design

- Replace unconditional transaction create/update/delete with create-only, ETag CAS, and conditional terminal delete.
- Recovery of `Uploaded` and cleanup-capable states must acquire durable ownership only after the prior owner's expiry. The recovery worker must reread the exact generation after takeover and before remote DELETE.
- Preserve and compare source version ID, data directory, modification time, size, and ETag through local commit and recovery before accepting ownership transfer.
- Recompute and require the exact lowercase sharded path in both transition-transaction and manual-job runtime recovery, and validate a truncated page's continuation token before processing any record from that page.
- **Open:** owner lease duration, clock-skew allowance, takeover revision encoding, and compatibility for existing v1 records that have only `not_after`/`owner_epoch`.
- **Open:** bounded retention and an operator disposition for permanently ambiguous records. Until defined, retained ambiguity is safer than collection.

## Tier mutation intent

### Current contract

`TierMutationIntent` models Add/Edit/Remove/Clear with `Prepared`, `Committed`, and `Aborted`. The identity includes the old tier-config ETag, candidate digest, canonical sorted affected targets and their old/new backend identities, and expiry. Revision and committed config ETag are the only mutable generation data.

New intents use a 15-minute expiry. A peer-only terminal tombstone is retained until that expiry plus five minutes of clock-skew allowance and until no coordinator record remains. Expiry bounds replay protection; it is not config commit/abort evidence.

The coordinator creates its durable record and peer `Prepare` blocks new reference creation, drains exact tier-operation leases, and proves that edit/remove/clear will not strand authoritative references. The coordinator then conditionally writes tier config, commits peers, publishes the runtime candidate, and clears the block. Per-mutation sharded mutexes serialize local phases only; persisted intent plus tier-config ETag is authoritative.

### Recovery decisions

| Observed durable state/input | Unique current owner | Current recovery decision | Destructive/config admission |
|---|---|---|---|
| `Prepared`; current tier-config digest equals candidate | Coordinator recovery; each peer recovery owns only its matching peer record/block | CAS to `Committed`, replay peer Commit and publish | Exact mutation identity and candidate digest; never infer from expiry |
| `Prepared`; current config still proves the old ETag/config | Coordinator recovery | Fan out canonical Abort, then CAS `Aborted` | Abort only the matching intent; a delayed matching Prepare converges to the tombstone |
| `Prepared`; config is a third generation, unreadable, or peer outcome is ambiguous | Coordinator record remains owner of the block | Retain `Prepared` and runtime block | No commit, abort, cleanup, or unblock |
| `Committed` | Coordinator recovery, with peers owning convergence of their local records | Replay peer Commit/runtime publication; clean exact converged records | Config ETag/digest and peer identity must match |
| `Aborted` | Coordinator recovery; peer recovery retains the local tombstone | Replay/confirm Abort and clear matching block; retain peer tombstone until expiry plus clock skew and no coordinator | Never roll back config based on timeout alone |
| Same mutation ID with different identity, illegal edge, corrupt/unknown record, or truncated scan | No actor acquires a conflicting mutation identity | Fail the recovery pass and retain | Fail closed |

Peer protocol v4 accepts current v4 and prior v3 messages. A v4 coordinator recognizes only the exact authenticated unsupported-version response from a v3-only peer and does not retry automatically as v3. Operators pause and drain tier edit/remove/clear for the mixed-version interval and resume only after every topology member supports v4. Ordinary object I/O and existing cleanup remain available.

Intent transitions retry an ETag race at most three times before returning a retryable failure. Recovery fully paginates coordinator and peer prefixes; a decode/read failure or a truncated page without a continuation token fails the pass rather than declaring convergence.

### Approved target and open design

- Keep create-only, ETag CAS, exact identity comparison, canonical Abort tombstones, and lost-response readback.
- Any shorter configuration-lock window must leave a durable `Prepared` fence installed on every required peer before releasing the broad exclusion scope and must prove recovery restores that fence before admitting reference creators.
- **Open:** the exact split between backend validation, peer fanout, reference scan, config CAS, and publication; expiry must never replace config-generation proof.
- **Open:** a dedicated operator reconcile/status surface and bounded retention for irreconcilable coordinator/peer records.

## Manual transition job, task, result, and checkpoint

### Current contract

The admin run defaults to `enqueue_only`; `mode=async` or `async=true` creates a durable job. The persisted scope is bucket-level and separates real and dry-run jobs. A durable async job stores an owner/lease, monotonic cancellation request, listing cursor and progress, queue snapshot, append-only task records written before enqueue, and append-only worker results written after an actual outcome. The local active-job map and cancellation token are accelerators, not ownership.

Job state has only `Running -> {Completed, Partial, Failed, Cancelled, Unknown}` edges. Terminalization persists the job state before a separate best-effort exact scope release. A crash after the terminal CAS, or a scope-delete failure, can therefore leave the admission record behind. Startup recovery immediately skips terminal jobs and does not clean that scope; a later claimant lazily replaces it after loading the referenced terminal job. Task/result records allow restart recovery to identify a task with no corresponding result and replay it through the ordinary transition transaction path. There is no separate checkpoint key. Of the object-version cursor proof, only `report.continuation_token` and `report.scanned` are persisted. `cursor_revision` has `#[serde(default, skip_serializing)]`; current writers omit it, and decode overwrites it with a derived in-memory consistency value.

Scope, task, and result records have no independent state enum. A scope evolves from absent to one live job/lease and then through CAS-protected lease successors. Its exact job/lease may delete it, but a new claimant may also CAS-replace a stale admission after proving that the referenced job is terminal, or after both scope and job leases expire. Task and result evolution is `absent -> immutable present`. Their current terminal history has no cleanup transition or owner.

Creation is not atomic with admission: the admin handler first persists the new `Running` job and only then attempts to claim its bucket-level scope. A crash between those writes can leave a `Running` job with no scope record and no admitted executor. Current recovery does not repair that gap while the job lease is unexpired; after expiry it can CAS-take over the job, tolerate the old scope being absent, and attempt a fresh scope claim.

### Recovery decisions

| Observed durable state/input | Unique current owner | Current recovery decision | Admission/retry rule |
|---|---|---|---|
| Terminal job, with or without a leftover matching scope | Terminal history; no execution owner | Startup recovery skips the job without inspecting or deleting its scope | Terminal states never resume. A future admission claimant can lazily CAS-replace a scope that points to the terminal job |
| `Running` with unexpired lease and matching scope | Exact persisted `(owner_id, lease_id)` holder | Leave it to the current owner | Another process may not enqueue or take over |
| `Running` with unexpired lease but no matching scope | The job record names an intended lease holder, but no scope admission or executor is durable | Skip until lease expiry; the record can remain temporarily stranded | Recovery must not invent admission while the lease is live. After expiry it follows the takeover-and-scope-claim path |
| Expired `Running` with `scan_completed = true` | Recovery worker using the observed lease generation | Reconcile task/result journals before the cancel or takeover branch. Reconciliation may itself terminalize the job | Only this completed-scan branch performs result reconciliation before recovery proceeds |
| Expired `Running` with `scan_completed = false` and cancel requested | Recovery worker that wins the observed job-generation CAS | Make one direct CAS attempt to persist `Cancelled`, without result reconciliation or a replacement lease; success is followed by best-effort exact scope release | The stored counters can be stale. A precondition failure ends this recovery attempt as `Skipped`; this direct CAS does not use the four-try update helper |
| Expired non-cancelled `Running`, after any applicable completed-scan reconciliation | Recovery claimant after a successful owner/lease CAS | Make one direct takeover CAS; on success, remove the prior exact scope if present, claim a fresh scope, replay tasks without results, and continue object-version scanning at the persisted token | A takeover precondition failure ends this attempt as `Skipped`. After takeover, later conflicts or errors can occur after scope claim, replay, or enqueue and therefore do not prove that no side effect occurred |
| Pending task with no result | Job lease holder schedules it; the resulting transition transaction owns remote work | Replay idempotently | The task identity is the retry key; the job never owns remote DELETE |
| `scan_completed = true`; task/result corruption encountered by reconciliation | Observed job lease generation owns terminalization | The reconciliation helpers can CAS terminal `Unknown` under their state predicates, retain journals, and then attempt exact scope release | Corruption reaches `Unknown` only through this reconciliation path; never repair by guessing |
| `scan_completed = false`; task/result corruption encountered while replaying after takeover | Recovery lease remains the current job owner | Return an error and retain the `Running` job and journals; current code does not CAS `Unknown` | Retry later with the same durable evidence; do not silently complete |
| Missing required result or cursor proof discovered after takeover | Recovery lease holder owns terminalization | CAS terminal `Unknown` when its predicates match; retain task/result evidence | Never guess progress or skip a pending object-version page |
| Scope conflict with another live job | Persisted scope admission's job/lease | Reject admission or abandon recovery | Persisted scope, not node-local memory, is authority |
| Job-record page is truncated without a continuation token | Current per-page recovery processes jobs before the outer loop checks the missing token | Jobs on that page may already take over leases, claim scopes, replay/enqueue tasks, or resume scanning; the enclosing pass then returns an error | **Current pagination gap:** the approved target validates the page token before any new claimant or side effect |
| Uppercase 32-hex job filename with shard text matching the same uppercase prefix | The lowercase canonical job selected by the parsed UUID remains authoritative; the alias path is not independently bound | Runtime recovery accepts the alias and loads the canonical job by UUID. If alias and canonical entries are both listed, the same job can be revisited and may repeat takeover, scope, replay, or enqueue work. Decommission canonical-path validation rejects the alias | **Current runtime path gap:** require the exact lowercase canonical job path before recovery side effects |
| Invalid job path/schema/state | No claimant for the invalid record | Retain that record and report failure as applicable | Fail closed for that record |
| Later job-update CAS conflict after recovery has progressed | The successfully claimed recovery lease remains authoritative until fenced or expired | `update_manual_transition_job_record` reloads and retries conflicts up to four times, then returns an error if convergence fails | Scope claim, replay, or enqueue may already have happened; a late conflict is not a no-claimant/no-side-effect result |

The lease interval is 60 seconds. CAS behavior is phase-specific: cancellation terminalization and recovery takeover each use one direct `save_manual_transition_job_record_if_current` attempt, and a precondition failure ends that attempt. Only mutations routed through `update_manual_transition_job_record` reload and retry conflicts, up to four times. A later conflict can occur after a lease or scope was claimed or after replay/enqueue side effects. Startup job-record recovery lists 100 records per page; per-job task and result metadata journals each list 1,000 records per page. Those three metadata-prefix continuation markers exist only in the current scan loop, so a crash restarts the affected scan from the beginning. For manual object-version traversal, the job persists `report.continuation_token` and `report.scanned`; `cursor_revision` is omitted from serialization and derived from those fields after decode for in-memory validation.

### Approved target and open design

- Initial job creation must be create-only and duplicate task/result writes must validate the stored immutable payload before treating a precondition failure as idempotent success.
- Checkpoint monotonicity must continue to cover state, timestamps, progress counters, the persisted report token/scanned pair and its derived in-memory cursor revision, cancellation, and task/result-derived counts.
- Recovery must never turn manual orchestration into a second remote owner; each queued object still uses the transition transaction and ordinary local metadata contracts.
- **Open:** retention/GC for job, task, and result records. Current terminal records are not age- or count-bounded.
- Until persisted-format negotiation exists, caller/operator orchestration is responsible for checking every required node's advertised capability and failing closed when it is unknown or unsupported. The async run handler itself does not perform a fleet capability gate and a direct request proceeds to job creation.
- **Open:** a server-side fleet capability gate and explicit rollout/downgrade negotiation for the v1 job/task/result family.

## Tier-delete journal and dispatch manifest

### Version meaning and ownership

| Journal version | Current read/recovery contract |
|---|---|
| v1 | No backend identity and unknown remote version. Quarantine and retain; never issue remote DELETE |
| v2 | Backend identity is present, but remote version remains unknown. Quarantine and retain |
| v3 | Exact remote version was introduced. Supported legacy exact-delete recovery uses the bound tier generation |
| v4 | Adds explicit transition-version state. Unknown or inconsistent values are retained; known disabled omits `versionId`, suspended null uses `null`, and exact versions use their opaque ID |
| v5 | Adds stable source identity and transaction state. Recovery proves source/free-version presence across physical sets before deciding abort, retain, or commit |
| v6 | Sole-owner record bound to immutable operation/manifest/topology. It is the only new journal format for destructive prefix dispatch |

For a complete source set at or below 200,000 journals, the byte-compatible v1 single manifest remains at the deterministic scope-digest root. For a larger source set, that same root instead contains a strict `chunked_parent` sentinel, and each bounded child uses the unchanged v1 manifest payload at `chunks/<scope-digest>/<operation-id>.json`. A pre-chunking reader rejects the parent schema and the non-root child path, so it cannot start a competing single dispatch while chunking is active.

A v1 single/child manifest binds a bucket incarnation and prefix to an operation UUID, topology generation, and sorted journal names/count/digest. Its legal edges are:

```text
Preparing -> DispatchAuthorized -> Completed
         \-> Aborting -> Aborted
```

The journal edge is `Prepared -> Dispatched -> Committed`. A manifest coordinator owns the whole `Prepared` set and is the only actor that may roll it back or complete the manifest. A per-journal worker cannot remove one prepared member.

The chunk parent stores only monotonic O(1) progress and binds at most one child:

```text
Active(no child) -> Active(bound Preparing child)
Active(bound Completed child) -> Active(no child, next sequence/count)
Active(no child, final empty rescan and local delete complete) -> Completed
```

Child creation is ordered `Preparing` child create, parent binding CAS, journal preparation/dispatch, then child `DispatchAuthorized`. One request exactly replays one bounded child under source-object locks, commits every child journal, marks the child `Completed`, advances the parent, and returns retry-required. A successor request rescans from the prefix start; no listing cursor crosses bucket-lock lifetimes. New or changed source identities are therefore admitted only by a fresh child. Final success requires an error-free, non-truncated scan with no v6 candidate, local prefix deletion under the same bucket fence, and the parent `Completed` CAS.

### Journal recovery decisions

| Record/state and evidence | Unique current owner | Current recovery decision | Remote DELETE admission |
|---|---|---|---|
| v1/v2, any state | Quarantine recovery owns only retention/diagnostics | Quarantine and retain | Never |
| v3/v4 `Committed`, known deletion semantics | Legacy journal recovery worker | Idempotent remote delete under exact tier lease; reread and conditionally delete record | Exact remote/backend identity and current lease. v3 is exact; v4 additionally permits explicit known-disabled, suspended-null, or exact state. These are legacy compatibility paths without v5 source proof |
| v3/v4 unknown, inconsistent, or corrupt semantics | No worker acquires destructive ownership | Retain | Never |
| v5 `Prepared` with authoritative source/free-version present | v5 journal recovery owns exact record abort | Abort exact record | No remote delete |
| v5 `Dispatched`/`Committed` with source/free-version present | `xl.meta` or free-version remains remote owner | Retain | Never |
| v5 `Prepared`/`Dispatched` with exact source absent | v5 journal recovery worker | CAS `Committed`, then delete | Stable source, all-pool absence, bucket READ fence, exact tier lease, physical locks, and before/after checks |
| v5 `Committed` with exact source absent | v5 journal recovery worker | Delete idempotently | Same fenced absence proof |
| v6 `Prepared` | Manifest coordinator for the immutable whole set | Retain for manifest coordinator | Never |
| v6 `Dispatched` with authorized/completed manifest and exact source/free-version absent | Journal recovery owns state advancement; manifest coordinator owns completion | CAS `Committed`, release worker locks, wait for manifest coordinator | No remote delete yet |
| v6 `Committed` with `Completed` manifest and exact source/free-version absent | Exact v6 journal recovery worker | Idempotent remote delete; while an active decommission covers the record, persist its terminal receipt before conditional cleanup; without an active operation, conditionally delete the exact record ETag directly | Matching immutable manifest membership/topology, fleet proof, bucket READ fence, exact tier lease, all physical locks, known version, and before/after fence validation |
| Any source/free-version present, manifest missing/mismatched, topology changed, lost fence, unknown version/state, corruption, or read uncertainty | No journal worker acquires destructive ownership | Retain | Never |

### Manifest recovery decisions

| Manifest state | Unique current owner | Current recovery decision | Authority |
|---|---|---|---|
| `Preparing` | Manifest coordinator under the exact operation and bucket fences | Seal `Aborting`, delete the complete staged set, then mark `Aborted` | Rollback is allowed only before durable authorization |
| `Aborting` | Manifest coordinator | Continue exact whole-set rollback | Never dispatch or delete remote objects |
| `Aborted` | Manifest coordinator owns terminal metadata cleanup | Verify staged journals are absent, then conditionally delete manifest | Terminal local cleanup only |
| `DispatchAuthorized` | Manifest coordinator owns member convergence and local mutation authorization | Wait until all immutable members are `Committed`, then CAS `Completed` | Local destructive prefix mutation was authorized; individual remote deletes still wait for `Completed` |
| `Completed` | Journal workers own member cleanup; coordinator owns final manifest cleanup | Wait for all member records to disappear, then conditionally delete manifest | Journal workers are the remote-delete owners |
| Missing member, set/digest mismatch, wrong incarnation/topology, corrupt state, scan ambiguity, or cancellation | No actor acquires new destructive authority | Retain | Fail closed; an authorized operation never rolls back |

### Chunk-parent recovery decisions

| Parent/child state and evidence | Unique current owner | Current recovery decision | Authority |
|---|---|---|---|
| Active parent with no child | A later prefix-delete retry under bucket WRITE | Retain the parent and rescan from the prefix start | No local or remote deletion |
| Active parent with exact bound `Preparing`/`Aborting`/`Aborted` child | Child manifest coordinator | Retain parent while child recovery rolls back and removes the child | Never authorize or advance that child |
| Active parent with exact bound `DispatchAuthorized` child | The bound child permit or journal recovery | Resume exact-source replay on request; otherwise retain until all journals become `Committed` and the child becomes `Completed` | Only the exact parent-bound child may authorize local replay |
| Active parent with exact bound `Completed` child | Parent coordinator | CAS the next sequence/count and clear the binding | Parent progress only; remote DELETE remains owned by committed child journals |
| Bound child missing and its exact operation journal namespace is non-empty or unreadable | No actor can prove safe abandonment | Retain and fail closed | Never clear the binding |
| Bound child missing and its exact operation journal namespace is proven empty | Parent coordinator | CAS-clear the stale binding and retry from the prefix start | No deletion; the fresh scan reconstructs any remaining source work |
| Completed parent with no active child | Parent recovery | Record terminal decommission evidence when applicable, then conditionally delete the exact parent ETag | Metadata cleanup only |
| Parent identity, child binding, topology, incarnation, sequence/count, CAS generation, or fence mismatches | No actor acquires progress authority | Retain | Fail closed |

Single and child manifest preparation is bounded by 200,000 journals and a 32 MiB record. On the first unique candidate beyond the bound, the physical walks are cancelled and only the retained exact batch can proceed; cancellation fallout is not absence proof. The parent never accumulates child names, and exact local replay uses bounded concurrency. Journal and manifest recovery retain their existing bounded pages, per-entry timeouts, and concurrency. These are work bounds, not retention bounds: v1/v2 quarantine and unresolved v6 operations can remain indefinitely.

### Approved target and open design

- New destructive prefix paths use only v6 plus either one byte-compatible manifest or one parent-bound sequence of byte-compatible child manifests. No new v1-v5 sole-owner records may be created.
- Preserve the two-phase authorization barrier: all prepared records, durable barrier, all dispatched records, durable `DispatchAuthorized`, local mutation, journals committed, durable `Completed`, then remote DELETE.
- Do not downgrade every v6-aware recovery worker while v6 records remain. v5-and-older readers reject and retain v6 records; older nodes may continue producing fallback free-versions until the fleet is homogeneous.
- **Open:** bounded age/count policy and operator disposition for quarantined v1/v2, incomplete manifests, and repeatedly failing exact deletes. Capacity rejection and recovery throughput must not be “fixed” by weakening ownership proof.

## `xl.meta` free-version boundary

### Current

An ordinary delete of a completed transitioned version creates a hidden `rustfs_filemeta::FREE_VERSION` record flagged `XL_FLAG_FREE_VERSION`. It removes the user-visible version locally first and preserves the exact remote tuple for retry. The lifecycle worker owns remote cleanup while that marker exists; GET, listing, restore, transition planning, and usage accounting do not treat it as a visible version.

`tier_free_version_recovery.rs::recover_tier_free_versions` only scans persisted free-version records and re-enqueues them for lifecycle work. The destructive path is `bucket_lifecycle_ops.rs::cleanup_free_version_exact`: it acquires the bucket-lifecycle READ fence, exact backend-generation lease, and every relevant physical object WRITE lock, then performs the authoritative all-pool scan. It revalidates those fences before and after the bounded remote request, removes local markers only after remote cleanup succeeds, and rescans.

Recursive prefix/delete-all is the exception because physical directory removal cannot preserve per-object markers. It may set `skip_tier_free_version` only after the v6 manifest has durably authorized the immutable predecessor set. A v6 journal becomes sole owner only after local source and exact free-version absence are proven. Neither tier mutation intents, manual jobs, nor decommission receipts authorize remote deletion.

### Approved target

- Every code path that removes or overwrites transitioned metadata either atomically leaves an exact free-version owner or enters an already-authorized v6 dispatch.
- If both a journal and free-version are observable, recovery preserves the remote object until version-specific authority and all-pool absence prove a single owner.
- Missing `transitioned-version-state` remains unknown. Compatibility work must not synthesize known-disabled or exact semantics merely from an empty stored version ID.

The following single-record protocol approves how historical objects without RustFS `transitioned-version-state` can be upgraded. It does not approve a bulk scanner or allow destructive cleanup to consume an unproven record.

## Legacy transitioned-version-state reconciliation

### Current

An absent `transitioned-version-state` key decodes as `TransitionVersionState::Unknown`. The current GET and free-version cleanup paths reject that state rather than interpreting an empty remote version as unversioned. There is no admin route that repairs this field in `xl.meta`. The existing transition-transaction reconcile route operates on expired `UploadOutcomeUnknown` transaction records and can exact-delete their canonical candidates; it is a separate protocol and must not be reused for metadata reconciliation.

An explicitly persisted `unknown`, a malformed state, conflicting RustFS/MinIO compatibility keys, an invalid or nil version identifier, and a partial transition tuple are not legacy absence. They remain invalid or ambiguous and fail closed.

### Approved single-record control surface

The approved target is one synchronous, exact logical-version operation. It does not list a bucket or prefix and does not create a durable job:

```text
GET  /rustfs/admin/v3/ilm/transition/state/reconcile?bucket=<bucket>&object=<object>&versionId=<local-version-id>
POST /rustfs/admin/v3/ilm/transition/state/reconcile?bucket=<bucket>&object=<object>&versionId=<local-version-id>
```

`versionId` is required; the literal `null` is the explicit selector for a locally unversioned object, while an omitted or empty selector is invalid. GET requires `admin:ListTier`. It performs an authoritative all-pool metadata read and a bounded server-side live backend probe, but it never writes metadata or mutates the remote tier. Its response includes the canonical immutable source tuple, every original per-set missing-state representation, the proposed target tuple when one is provable, an opaque reconciliation digest over all three, fleet/topology and tier-generation readiness, and whether POST is ready to attempt migration. GET never labels an unmodified legacy record `migrated`.

POST requires `admin:SetTier`. Its body contains `confirm: true`, the complete source and target tuples, every original per-set representation, and their reconciliation digest returned by a recent GET. The server does not trust a client-supplied state or external assertion: it rereads the object, requires every immutable source field to match exactly after canonicalization, repeats the bounded live probe, and derives the target state itself. For the mutable state/version/destination fields, each set must equal either its original missing-state representation bound by the digest or the exact newly proven target; this is the only accepted partial-retry shape. A missing confirmation, any other stale tuple or digest, or a widened selector is rejected before any write.

The expected tuple binds all evidence whose change could redirect the repair:

- bucket name and incarnation; exact object name, local version ID, data directory, modification time, size, and ETag;
- transition completion status, tier name, canonical remote object name, raw remote-version key presence/value, and raw state-key presence/value under both compatibility prefixes;
- tier-config generation and the `transition-tier-destination-id` binding, including backend type, endpoint/bucket/prefix identity, and the credential-independent backend fingerprint;
- topology generation and the generation/digest of every authoritative `xl.meta` copy found across pools and sets.

The only allowed state derivations are:

| Live proof | Persisted state | Persisted remote version | Remote request meaning |
|---|---|---|---|
| Provider proves versioning disabled and the candidate is present | `KnownDisabled` | Absent/empty | Send no `versionId` |
| Provider proves suspended-version null semantics and the exact candidate is present | `SuspendedNull` | Literal `null` | Send the provider's null-version form |
| Provider proves one exact, nonempty, non-`null` opaque version | `Exact` | That exact opaque identifier | Send that exact `versionId` |

Missing, multiple, changing, or unsupported probe results do not select a state. In particular, a preexisting empty remote-version field is not evidence for `KnownDisabled`, and a client may not nominate `Exact` or `SuspendedNull`.

The POST may write only the derived `transitioned-version-state`, its corresponding `transitioned-versionID` value when the proven model requires one, and the exact `transition-tier-destination-id` binding under both compatibility prefixes in the matching `xl.meta` version. It does not issue remote GET beyond the proof probe, remote PUT, remote DELETE, local object DELETE, free-version cleanup, transaction/journal cleanup, tier-config mutation, restore, or source-payload rewrite. Reconciliation establishes metadata meaning; a later ordinary owner may perform cleanup under its own destructive protocol.

### Outcome contract

POST returns exactly one of the following outcomes and whether it changed bytes. GET uses the same diagnostic names for non-applicable cases, returns `ready-to-migrate` when a missing state is provable, and returns `migrated` only when strong readback shows the record was already explicit and converged:

| Outcome | Meaning and permitted effect |
|---|---|
| `migrated` | All authoritative copies already contain, or were monotonically advanced to, the same proven state and destination identity. Only this outcome makes the record eligible for later ordinary read/delete semantics. |
| `retained-ambiguous` | The tuple is structurally legacy-compatible, but the live probe is missing, multiple, changing, unsupported, or otherwise cannot prove exactly one state. No metadata or remote object is changed. |
| `corrupt` | Explicit `Unknown`, malformed/contradictory compatibility keys, nil/invalid identifiers, partial transition metadata, or authoritative copies outside the one allowed `{original missing representation, exact proven target}` retry subset were observed. No backend probe is required after corruption is established, and nothing is changed. |
| `backend-unavailable` | The bound tier generation/destination cannot be acquired, the bounded probe fails, or a metadata quorum/strong readback needed to complete the operation is unavailable. Any already-persisted monotonic subset is retained for an idempotent retry; it is never rolled back. |

HTTP failure detail may distinguish a stale expected tuple, lost fence, timeout, or unavailable quorum, but it must preserve one of these machine-readable outcomes. Logs and audit events include request identity, object identity, tier, generations, outcome, and whether bytes changed; they never include credentials or raw credential-derived configuration.

### Fence, write, and retry order

The approved POST executes the following order. A step that cannot be proven stops the operation without remote mutation:

1. Authenticate `admin:SetTier`, validate the exact single-record selector, `confirm: true`, expected tuple, and digest.
2. Prove every required node advertises the reconciliation format and destination-identity capability; capture the fleet and topology generation. Unknown or unsupported nodes block the writer.
3. Acquire the bucket-lifecycle WRITE fence and validate the bucket incarnation.
4. Acquire the exact tier-config generation lease bound to the expected destination identity.
5. Acquire exact object-version WRITE locks for every owning physical pool/set in stable pool/set order.
6. Perform an authoritative all-pool read, reject duplicate/conflicting ownership, validate every compatibility key, and require each set to match the immutable source tuple plus either its digest-bound original missing-state representation or the exact proposed target.
7. Run one bounded, cancellation-aware live probe through the leased backend. No client or cached probe result is authority.
8. Before writing, revalidate the fleet/topology generation, bucket incarnation and lifecycle fence, tier lease/destination identity, physical owner set, and complete metadata tuple.
9. Write the same derived state and destination identity to each authoritative set with that set's metadata quorum and conditional generation. A timeout or response loss is resolved only by a strong read of that exact set.
10. Strongly reread every authoritative set and revalidate the full tuple, state, destination identity, and topology before returning `migrated`. Release locks and leases in reverse order.

Cross-pool and cross-set partial success is monotonic. The only legal repair edge is `missing state -> one proven {state, remote version, destination identity}`. A retry may accept an already-written subset only when every known copy equals the newly proven target, every remaining copy equals its original missing-state representation captured by the reconciliation digest, and all immutable source fields still match; it then fills only the missing copies. This exact target-plus-original subset is neither stale nor corrupt. The retry never clears a known state, rewrites it to another state, changes destination identity, or rolls a successful set back to missing/`Unknown`. Any other divergent value produces `corrupt`; an unavailable set/readback produces `backend-unavailable`, and destructive cleanup remains blocked until a later strong all-pool read proves complete convergence.

GET takes the same fleet/topology snapshot and authoritative all-pool read but no write locks that imply mutation authority. Because GET is advisory, POST always repeats every fence, read, and live proof rather than promoting the GET result.

### Mixed-version and future batch work

The writer gate requires every node that can serve, rewrite, heal, decommission, or recover the affected `xl.meta` to preserve the explicit state and destination identity. A rolling fleet with an unknown/unsupported node is inspect-only. Downgrade is blocked while reconciled records could be rewritten by readers that erase or misinterpret those fields. Cross-pool movement must either copy the proven tuple unchanged or block reconciliation; a first-match lookup is never sufficient.

Explicit `Unknown`, corruption, and ambiguity remain fail closed for reads that cannot prove non-destructive semantics and for every destructive path. A migrated record becomes ordinary explicit metadata, but reconciliation itself never transfers remote DELETE ownership.

A bucket/prefix/fleet batch reconcile is still an **open design**. It requires a separate durable job identity, create-only admission, lease/CAS checkpoint, bounded pages, per-record expected tuples and outcomes, cancellation/restart semantics, retention, fleet rollout negotiation, and status counters. Implementations must not approximate that protocol by adding a list selector or background loop to the synchronous route.

## Durable namespace receipts during decommission

### Current contract

Decommission cannot treat durable ILM objects as ordinary configuration blobs. `validate_durable_ilm_record` validates namespace, size, schema/checksum, identity, and a protocol-specific checkpoint, and most protocol branches recompute the canonical path. Its transition-transaction branch currently inherits the weaker final-component parser: mismatched shard directories, extra components, and uppercase hex can pass when the final UUID and record contents agree. Exact transition-path validation is therefore an approved target, not a current decommission guarantee. Checkpoint successors enforce journal/manifest legal states, chunk-parent revision/sequence/count/binding progression, transition identity and revision progression, monotonic manual-job progress, scope ownership, and immutable task/result payloads.

The decommission coordinator copies and validates a durable record on a target, persists a receipt for that exact source path/identity/checkpoint, and records the expected receipt set on the source. While a matching decommission operation is active, protocol writers advance receipts as records change and terminal cleanup records a terminal checkpoint before deleting a covered record. Without an active decommission operation, the receipt helper creates no terminal receipt and ordinary protocol recovery proceeds with that protocol's current delete primitive: v6 journal/manifest/parent cleanup uses the exact ETag, while transition-transaction cleanup remains unconditional as documented above. Completion verifies every expected receipt and target checkpoint before the source pool can be removed.

A terminal receipt is proof that an exact target copy reached a terminal checkpoint. It may authorize conditional removal of the matching source record when every active target copy is covered; it never authorizes remote DELETE. A terminal receipt on one target cannot hide a later nonterminal receipt on another target.

Receipts have no enum state. Their legal evolution is `absent -> checkpoint -> monotonic successor -> optional terminal checkpoint`; identity, source path, and v6 topology binding cannot change. The expected-receipt manifest evolves only from absent to one immutable sorted receipt-set digest, then is removed by the completed run's coordinator.

### Recovery decisions

| Observed state | Unique current owner | Current recovery decision | Destructive admission |
|---|---|---|---|
| No active decommission run | Underlying protocol owner | Protocol recovery proceeds normally and no receipt is created. An eligible v6 journal/manifest record is directly removed by exact ETag; transition-transaction cleanup follows its documented current unconditional path | Receipt state grants no remote-delete authority |
| Source and target exact identity/checkpoint agree | Decommission coordinator for the run token | Create or CAS-advance the run-scoped receipt | Successor must be monotonic and topology-bound where required |
| Receipt already covers the same successor | Decommission coordinator for the run token | Treat as idempotent | Exact identity/checkpoint only |
| Conflicting receipt, checksum/schema/path error, divergent target record, missing ETag, or non-successor checkpoint | No decommission actor acquires cleanup authority | Fail decommission and retain source | Never overwrite or guess |
| Source active but an expected target receipt is absent/nonterminal | Decommission coordinator retains the source | Keep source and fail completion verification | No source cleanup |
| Matching terminal receipt covers the exact source and every target copy is terminal/covered | Decommission coordinator for the run token | Conditionally remove the exact source record | Source generation and optional v6 fleet topology must still match |
| Expected-receipt manifest and all receipts validate | Decommission coordinator | Allow the enclosing decommission completion predicate to proceed | Does not itself delete remote tier data |
| Decommission completed | Decommission coordinator owns run-scoped receipt GC | Best-effort clean run receipts and expected manifest | Only the completed run's exact receipt namespace |

The run token is derived from the pool command line and decommission start generation, so a resumed run finds the same namespace. Cancellation leaves receipts intact for resume. Cleanup failures are observable as `decommission_state` with `receipt_cleanup_failed`; unresolved runs currently have no separate age/count retention policy.

Receipt and expected-manifest create/CAS conflicts retry at most three times. Exhaustion, an unavailable strong read, or a non-successor checkpoint fails the decommission pass and leaves the run-scoped evidence intact.

### Approved target and open design

- Any new ILM namespace must register a path parser, size bound, schema decoder, canonical identity, successor relation, and terminal checkpoint before decommission may encounter it.
- Unknown `ilm/` paths and unsupported versions remain completion-blocking. Receipt merging remains create-only/CAS and monotonic.
- **Open:** bounded retention for receipts/manifests left by permanently abandoned decommission runs, including proof that cleanup cannot collide with a resumed run.
- **Open:** rollout/downgrade policy for receipt v2 and new durable namespaces beyond “new readers reject unknown, old nodes must not complete an unsafe decommission.”

## Failure and compatibility

### Approved target failure matrix

The matrix below is the normative approved target, not a blanket description of current implementation. Current exceptions are authoritative only where each protocol section above labels them explicitly. In particular, transition-transaction initial and successor writes and deletes are currently unconditional, while the transition transaction's initial write and the manual job's initial write have neither create-only installation nor mandatory lost-response strong-readback convergence.

| Event | Approved result |
|---|---|
| Crash before record create is durably confirmed | Strong-read exact key; retry create-only if absent; do not assume a candidate is unowned |
| Crash after remote PUT request but before response | Persist/retain unknown outcome; use an identity-bound provider probe or operator workflow; never guess a version |
| Lost response to state CAS or config quorum | Strong-read exact intended successor. Conflict, absence, or unreadable result remains pending |
| Crash after local transition commit | The exact logical `xl.meta` reference, full recorded source identity (version ID, data directory, modification time, size, and ETag), and remote tuple prove ownership transfer; cleanup only the terminal transaction record |
| Crash after remote DELETE but before journal/free-version cleanup | Retry the same exact idempotent DELETE under the same fences, then conditionally clean local evidence |
| Cancellation | Stop issuing new work, persist monotonic cancellation where the protocol has it, and leave ambiguous durable records for recovery. Cancellation is never rollback proof after authorization |
| Rolling upgrade | Gate writers on the minimum capability required by the format. Known older journal/RPC versions follow their explicit compatibility rule; unknown formats are retained |
| Downgrade | Drain v6 journals before removing all v6-aware workers. Do not write a new format until its downgrade reader behavior and writer gate are specified |
| Corrupt or unknown input | Record a diagnosable failure, retain bytes, and block destructive action/completion |

Transition transaction v1, manual job/task/result v1, and receipt v2 do not currently have a complete persisted-format negotiation for rolling downgrade. Until one is designed, caller/operator orchestration must not enable writers whose records required recovery nodes cannot decode. The manual async endpoint does not enforce that fleet gate and a direct request proceeds to job creation. This caller-side fail-closed rule is stricter than treating an unknown record as absent.

### Current format compatibility decisions

| Family/version | Current reader and writer behavior | Upgrade, downgrade, and ignore rule |
|---|---|---|
| Transition transaction v1 | Writers emit v1; the payload decoder rejects another schema, bad checksum, unknown state, or inconsistent transaction/remote identity. The current record-path parser accepts any shard/extra-component layout and uppercase hex when the final 32-hex UUID parses and matches the payload | There is no intentional ignore path, but exact lowercase canonical-path rejection remains an approved fix. A future schema needs a fleet writer gate and an old-reader retention test before rollout; downgrade behavior is open |
| Tier mutation intent v1; peer RPC v3/v4 | Durable readers/writers require intent v1. New peers accept signed/canonical v3 and v4 RPC; old v3 peers return an exact authenticated unsupported response to v4 | Pause and drain edit/remove/clear across the mixed interval; do not automatically retry v4 as v3. Unknown durable intent is retained and blocks recovery |
| Manual job/scope/task/result v1 | Writers emit the v1 family. Manual-job runtime recovery accepts an uppercase UUID path when both shard strings match its uppercase prefix, then loads the lowercase canonical job by UUID; the decommission validator recomputes the canonical path and rejects that alias. Other decoder/path/checksum failures stop reconciliation. Runtime capabilities advertise `enqueue_only` and `async`, but the async run handler does not consult a fleet capability gate and a direct request creates a job | Runtime recovery still needs exact lowercase canonical-path validation to prevent alias-driven duplicate work. Caller/operator orchestration must verify every required node and fail closed when capability is unknown or unsupported. An automatic server-side fleet gate and persisted downgrade negotiation remain open; unknown records are never ignored as completed work |
| Journal v1/v2 | Readers decode but quarantine because remote-version authority is missing; compatibility writers can preserve these forms | Retain indefinitely unless a separately approved, authoritative repair protocol resolves them; never translate empty version ID to known-disabled |
| Journal v3/v4 | Readers recover supported committed records according to exact or explicit version-state semantics; current compatible writes use v4 for known state | Unknown/inconsistent state is retained. These legacy paths are not evidence that a new sole-owner operation may omit v5/v6 source proof |
| Journal v5 | Readers use stable source/all-pool proof; decoded v5 can be checkpointed, while new online sole-owner transactions are not emitted as v5 | Retain and recover conservatively during upgrade. Do not manufacture v5 from older records or use it to bypass v6 manifest authorization |
| Journal v6, dispatch manifest v1, and chunk parent v1 | v6-aware writers/readers require immutable manifest membership and topology. Complete sets at or below 200,000 retain the legacy root manifest bytes; larger sets install a strict parent at that root and operation-scoped v1 child payloads. Pre-chunking v6 readers reject the parent schema and child paths, while v5-and-older readers reject and retain v6 journals | Gate writers on the current fleet capability and retain the root parent for the entire active chunk sequence. Drain v6 before removing all v6-aware workers; do not downgrade by rewriting a live v6 operation |
| Decommission receipt v2 and expected manifest v1 | Current decommission readers validate exact schema/checksum/path/checkpoint and fail completion on unknown input | No ignore path. Mixed-version decommission must not complete unless every participant preserves the registered durable namespace; broader downgrade negotiation is open |

## Reconcile, observability, and retention

| Protocol | Current operator/telemetry surface | Current retention | Required follow-up |
|---|---|---|---|
| Transition transaction | Expired unknown-upload inspect/delete/finalize routes; `lifecycle_transition_transaction_recovery` diagnostics | Terminal records are deleted; ambiguous and unsafe states may remain indefinitely | Backlog age/count/state metrics, bounded policy, and durable takeover status |
| Tier mutation intent | Admin mutation response plus recovery diagnostics; no dedicated reconcile API | Peer aborted tombstone through expiry plus skew; ambiguous coordinator/peer records retained | Status/reconcile view for mutation, peer convergence, config generation, and blocked tiers |
| Manual job | POST run response, GET status, DELETE cancel; runtime capabilities advertise both modes | Job/task/result history is indefinite. Terminalizers only best-effort delete the exact scope; startup skips a terminal job with a leftover scope, which remains until a later admission claimant lazily replaces it | Age/count/bytes limit and a terminal-history/scope GC protocol that preserves recovery evidence |
| Tier-delete journal/manifest | `lifecycle_tier_delete_journal` events, quarantined counter, remote-delete failure/breaker/inflight metrics | Terminal records converge; quarantined/ambiguous records are unbounded by age | Safe operator inspection/disposition, backlog age/count by version/state, bounded recovery without evidence loss |
| Decommission receipt | Decommission state/events including `receipt_cleanup_failed` | Completion triggers only best-effort receipt/manifest cleanup. Delete failures reported as `receipt_cleanup_failed`, as well as abandoned runs, can leave run-scoped records behind | Run-scoped retention and resume-safe cleanup policy |

Retention is a protocol transition, not raw deletion. Any collector must name its unique owner, minimum age/count/bytes bound, exact terminal or quarantine predicate, readback behavior, decommission interaction, and audit/metric output. It may not collect a record solely because it is old.

## Change review checklist

Before changing one of these protocols, reviewers must be able to answer all of the following from code and tests:

1. Which exact persisted generation owns publication, local cleanup, and remote DELETE?
2. What source, remote tuple, backend generation, bucket incarnation, topology, lease, and lock evidence is reread immediately before a destructive action?
3. How does a lost response converge, and what happens when readback is unavailable or conflicting?
4. Which process creates the record, which single actor cleans it, and can a stale process still write after takeover?
5. Which state transitions are legal, terminal, retryable, or permanently quarantined?
6. What do the previous and next binary versions read, write, reject, and retain during rolling upgrade and downgrade?
7. How are cancellation, Object Lock, replication admission, and decommission handled without becoming implicit authorization?
8. What bounded operational surface exposes backlog, age, errors, and a safe reconcile action?

Related runtime and operator context lives in [decommission-compatibility.md](decommission-compatibility.md), [background-controller-contract.md](background-controller-contract.md), [runtime-capability-contracts.md](runtime-capability-contracts.md), and [../operations/tier-ilm-debugging.md](../operations/tier-ilm-debugging.md). Testing changes should follow [../testing/README.md](../testing/README.md); destructive protocol changes require crash/restart and mixed-version fixtures, not timing-only tests.
