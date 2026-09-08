# ILM And Tiering Persistence Contracts

**Use this when:** changing an ILM transition, tier configuration mutation, manual transition job, tier-delete recovery path, pool decommission, or any code that can create, transfer, or destroy ownership of a remote-tier object.
**Source of truth:** `TransitionTransaction` and `process_transition_transaction_record` in `crates/ecstore/src/bucket/lifecycle/transition_transaction.rs`; `TierMutationIntent` and its conditional store helpers in `crates/ecstore/src/services/tier/tier_mutation_intent.rs`; the dormant validation-probe record and conditional primitives in `crates/ecstore/src/services/tier/tier_probe_intent.rs`; `TierConfigMgr::update_candidate_with_config_lock` and mutation recovery in `crates/ecstore/src/services/tier/tier.rs`; `handle_tier_mutation_peer_request` in `crates/ecstore/src/services/tier/tier_mutation_peer.rs`; the record encoders and CAS helpers in `crates/ecstore/src/bucket/lifecycle/manual_transition_job.rs`; manual-job execution/recovery and `cleanup_free_version_exact` in `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs`; `process_tier_delete_journal_entry` and manifest recovery in `crates/ecstore/src/bucket/lifecycle/tier_delete_journal.rs`; the free-version scan/re-enqueue path `recover_tier_free_versions` in `crates/ecstore/src/bucket/lifecycle/tier_free_version_recovery.rs`; `DURABLE_ILM_NAMESPACES` and `validate_durable_ilm_record` in `crates/ecstore/src/bucket/lifecycle/durable_namespace.rs`; and `record_durable_ilm_decommission_progress`, receipt verification, and receipt cleanup in `crates/ecstore/src/core/pools.rs`.

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
| Local transition commit is complete | Exact transitioned version in `xl.meta` | No | Recovery finds the transaction's logical bucket/object/version and requires the complete recorded source identity (version ID, data directory, modification time, size, and ETag), `TRANSITION_COMPLETE`, and the same remote object, tier, and remote version before removing only the transaction record |
| An ordinary delete removes that transitioned version | Hidden `xl.meta` free-version | Yes | Metadata quorum atomically removes the visible version and preserves its exact tier tuple in the free-version |
| A recursive prefix/delete-all operation cannot preserve per-object markers | v6 journal bound to an immutable single dispatch manifest or a chunk-parent-bound child manifest | Yes, but only after child/manifest completion and all-pool absence proof | `DispatchAuthorized`, exact local destructive mutation, every journal `Committed`, then child/manifest `Completed`; a chunk parent advances only after that child completion |
| Tier configuration mutation, manual job, or decommission receipt | Intent/admission/copy proof only | No | These records gate configuration, scheduling, or migration; they never become remote-object cleanup owners |

An old journal and a free-version can coexist during compatibility recovery. That coexistence is evidence of multiple possible owners, not permission to choose one: the journal path must retain its record until the version-specific recovery rule proves which owner is authoritative.

## Persisted record inventory

All keys below are objects in the internal metadata bucket. The table gives the canonical target form. Transition-transaction runtime recovery extracts the final 32 hexadecimal characters and UUID while ignoring shard directories and accepting uppercase hex. Manual-job runtime recovery requires exactly two shards matching the filename prefix, but accepts an uppercase UUID when the shards use the same uppercase text; it then loads the lowercase canonical job by UUID. The decommission validator recomputes and rejects a noncanonical manual-job path, but currently inherits the weaker transition parser. Exact runtime canonical-path validation for both protocols is an approved target.

| Protocol | Current schema/version | Canonical key | Creator and cleanup owner | Authoritative identity and mutable fields | Current durability point |
|---|---|---|---|---|---|
| Transition transaction | `rustfs-transition-transaction-v1`; the compact v1 state profile is fleet-gated; successor v2 is approved below but not implemented | `ilm/transition-transactions/records/<aa>/<bb>/<transaction-id>.json` | The transition attempt creates it; transition commit/recovery cleans it | Immutable/fence identity: deployment, transaction, fixed v1 `owner_epoch`, write, source identity, tier/backend fingerprint, canonical remote object, deadline. Mutable: state, remote version, revision. `TransitionCleanupProof` is only a transient admission input to `mark_cleanup_pending`; it is not persisted in the record | Create-only maximum-parity write; exact record and ETag read before successor `If-Match`; terminal receipt followed by exact ETag conditional delete. A compact success uses two saves and one delete instead of five saves and one delete |
| Tier mutation peer intent | `rustfs-tier-mutation-intent-v1` | `tier/mutation-intents/records/<aa>/<bb>/<mutation-id>.json` | The receiving peer creates and converges it; the mutation recovery path cleans it | Immutable: mutation ID/kind, old config ETag, candidate digest, sorted affected target identities, expiry. Mutable: revision, state, committed config ETag | Create with `If-None-Match: *`; transition/delete with ETag `If-Match`; maximum parity |
| Tier mutation coordinator intent | `rustfs-tier-mutation-intent-v1` | `tier/mutation-intents/coordinators/<aa>/<bb>/<mutation-id>.json` | The initiating node creates it; coordinator recovery cleans it after peer convergence | Same mutation identity and mutable fields as the peer record | Same conditional-write contract as the peer intent |
| Tier validation probe intent | Dormant `rustfs-tier-probe-intent-v1`; no writer or recovery is enabled | `ilm/tier-probe-intents/records/<aa>/<bb>/<probe-id>.json` | No current runtime owner because no path creates the record; v1 permits only the immutable creator as owner | Immutable probe, operation-generation, destination, random remote object, creator identity, and v1 owner fence. Mutable: revision, state, and monotonic remote-version proof | Conditional create/CAS/delete primitives exist but are not called by Add/Edit/Verify or recovery |
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
| Recovery control, export, and disposition | `rustfs-ilm-recovery-control-v1`, `rustfs-ilm-recovery-export-v1`, and `rustfs-ilm-recovery-disposition-v1` are approved below but not implemented | `ilm/recovery-controls/...`, `ilm/recovery-exports/...`, and `ilm/recovery-dispositions/...` under protocol/shard/operation identities | Recovery owns one control for an exact source generation; the authenticated operator creates immutable export/disposition evidence; their collectors never own remote DELETE | Source protocol/path, all-pool copy-set manifest, ETags/content digests, owner lease, retry state, redacted error code, action, actor/reason, and terminal proof | Create-only, ETag CAS, all-pool strong readback, terminal receipt when covered by decommission, and exact conditional cleanup |

`durable_namespace.rs` registers exactly the two tier-journal namespaces, the dispatch-record namespace shared by single manifests, chunk children, and chunk parents, the transition-transaction namespace, the dormant tier-validation-probe namespace, and four manual-job namespaces. A path beginning with `ilm/` that is not in that registry is an error during decommission rather than an ignorable object.

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

Tier mutation intents and v6 journal/manifest records use the conditional primitives. Manual job updates, scope admission, and decommission receipts also use CAS after creation. Transition transaction v1 now uses create-only installation, exact record plus ETag read before each successor CAS, and exact ETag terminal deletion. Its `owner_epoch` and `not_after_unix_nanos` remain immutable, however, so an expired recovery worker claims only the next state generation rather than a renewable durable owner lease. The manual job's initial UUID-key write still lacks create-only installation, although all later owner/lease updates are CAS-protected.

### Approved target

- Every independently generated record is installed create-only. A duplicate is accepted only after byte-equivalent or semantic-identity validation.
- Every mutable transition is a legal single-generation successor written with ETag CAS. The persisted revision must advance monotonically and the expected owner/lease/epoch must be part of the compared generation.
- Every cleanup reloads the terminal generation, validates identity and cleanup evidence, records any required decommission terminal checkpoint, and conditionally deletes that exact ETag.
- After a timeout, connection loss, or quorum-uncertain response, the caller must strongly reread. Only the exact intended successor is success; predecessor, absence, conflict, corruption, or unavailable readback retains the record and blocks destructive action.
- A process-local mutex, cancellation token, task registry, or cached generation may reduce duplicate work but cannot authorize publication, rollback, or remote deletion.

The approved transition-transaction successor, lease/takeover fields, v1 migration, and upgrade/downgrade gates are specified in [Bounded recovery control and operator disposition](#bounded-recovery-control-and-operator-disposition). They require implementation and fleet gating before any v2 writer or destructive v1 takeover is enabled.

## Lock and operation order

Lock ordering is part of the recovery contract. Callers acquire only the locks needed for the row they execute and release them in reverse order.

| Path | Current acquisition order | Operations allowed while held | Operations forbidden while held |
|---|---|---|---|
| Tier add/edit/remove/clear | A short tier-config namespace WRITE lock captures the persisted config ETag, then releases before backend validation. After validation, namespace WRITE then `admin_updates` protect the ETag check and durable coordinator Prepared write. The Prepared runtime block rejects new tier-operation leases while already-issued leases remain current and drain through their complete local cleanup. Both guards are released for that drain, peer Prepare, and reference proof. Only after the proof succeeds does the coordinator revoke the blocked generation, then it reacquires both guards in the same order for final identity checks and config CAS. Both are released again after the coordinator becomes durably Committed | Backend validation, existing leased cleanup, peer fanout, and reference proof run without either exclusive guard. The exact Prepared mutation ID is the only allowance for the late publish transition; its generation drain is expected to be empty because the Prepared block admitted no new lease. Immediately before config CAS the coordinator revalidates the ETag, candidate digest, exact Prepared intent identity, and intent expiry. The durable Committed intent is recovery authority while peer Commit and local publication finish without the exclusive guards | Revoking a generation before already-leased cleanup has removed its exact local ownership marker; admitting a new lease after Prepared; ordinary manager `RwLock` or runtime-state `Mutex` across awaited network I/O. Recovery must retain an unexpired Prepared coordinator whose old ETag is still current; the old ETag alone is not abandonment proof. Remote object DELETE is never part of mutation |
| v6 manifest prepare | Caller already holds the bucket-lifecycle WRITE fence; caller acquires a bucket-metadata transaction READ guard covering the Object Lock and bucket-incarnation snapshot and keeps it through local mutation; exact tier-generation leases; fleet/topology proof; for a single dispatch, synthetic manifest-operation WRITE; for a child, parent-operation WRITE then child-operation WRITE | Build and write one immutable bounded journal set and manifest, validate exact set/digest, then authorize local dispatch while both caller-held bucket guards and all leases remain current. A parent binding is durable before child authorization | Remote tier DELETE; per-object worker cleanup; releasing the metadata guard or a required lease before the authorized local mutation completes; child-to-parent nested lock acquisition |
| v6 manifest/parent recovery | Fleet/topology proof; bucket-lifecycle WRITE lock; then exactly one synthetic manifest- or parent-operation WRITE lock | Read/write manifest, parent, and journal metadata; verify exact set/digest/binding; converge or roll back child records; advance a parent only after child completion | Remote tier DELETE; per-object worker cleanup; rollback after authorization; taking a child lock while holding a parent lock in background recovery |
| v5 journal destructive recovery | Synthetic per-journal recovery lock; bucket-lifecycle READ lock; exact tier-generation lease; all physical object READ locks in stable pool/set order | Authoritative source/free-version scan; fenced state CAS; for an eligible terminal state, one bounded remote DELETE; conditional record cleanup | Any delete when a lock or lease is lost; publishing local metadata; selecting an arbitrary backend/version |
| v6 journal destructive recovery | Synthetic per-journal recovery lock; fleet/topology proof; bucket-lifecycle READ lock; exact tier-generation lease; all physical object READ locks in stable pool/set order | Immutable manifest/topology validation, authoritative source/free-version scan, fenced state CAS, and, for an eligible terminal state, one bounded remote DELETE followed by record cleanup | Any delete when a lock, lease, or fleet proof is lost; publishing local metadata; selecting an arbitrary backend/version |
| Free-version cleanup | Bucket-lifecycle READ lock; exact tier-generation lease; all physical object WRITE locks in stable pool/set order | Exact all-pool scan; bounded remote DELETE; local marker removal; post-delete rescan | Deleting before the free-version is the sole owner or after any fence changes |
| Transition commit | Existing object commit locks plus exact source identity and tier-generation checks in the transition path | Publish the exact remote tuple into the matching local version | Publishing a tuple after the source identity or generation changes |
| Transition transaction cleanup | Record validation; expiry check; a next-state ETag CAS for cleanup ownership; exact backend-generation lease inside the probe/delete helper. Current recovery has no explicit renewable owner lease or bucket-lifecycle/physical-set ownership fence | Identity-bound provider probe and deletion of a known canonical candidate | The cleanup-state CAS fences a stale predecessor, but the approved target also requires an explicit recovery lease and exact all-pool source reread before DELETE |
| Recovery artifact quota admission (approved target) | Cluster-scoped recovery-admission WRITE lock first; then the one canonical control/source operation lock and any source-protocol metadata/physical locks in that protocol's existing stable order | Bounded internal artifact inventory, exact source/control validation, and create-only installation plus strong readback of one fully encoded export or disposition candidate | Acquiring admission while holding a control, source, disposition, bucket, physical, migration, or decommission guard; remote backend I/O; source-journal deletion; disposition `Applying`; releasing admission before candidate installation/readback converges |
| Manual job | Initial maximum-parity job write; then persisted bucket-level scope create/CAS; later short job/task/result metadata operations | List, checkpoint, append tasks before enqueue, append results after work, renew/take over lease | Holding metadata guards across remote transition PUT; treating the local active-job map as cluster authority. A crash between the job write and scope claim can leave `Running` without a scope record |
| Decommission receipt | Decommission coordinator's source/target record workflow; record-specific conditional writes | Copy/validate durable record, advance receipts, construct and verify expected manifest, conditionally clean exact covered source | Remote tier DELETE; deleting an uncovered or divergent source record |

Tier mutation backend validation is outside both exclusive guards and is bound to the persisted ETag snapshot. The initiating task is detached from the admin request so cancellation cannot interrupt validation cleanup. Once the coordinator Prepared record and local fence are durable, lease drain, all-node peer Prepare, and reference proof run without the tier-config namespace or `admin_updates` guard. Recovery treats an unexpired Prepared record as active even while the old config ETag remains current. Both guards are reacquired in namespace-then-admin order, and the ETag, candidate digest, exact intent identity, and expiry are revalidated immediately before config CAS. After the coordinator advances to Committed, the guards are released again for peer Commit and local publication.

## Transition transaction

### Current contract

`TransitionTransaction` binds the canonical candidate name to a transaction UUID, write UUID, source identity, tier name, backend fingerprint, remote-version state, deadline, fixed `owner_epoch`, and mutable `revision`. Without a current homogeneous compaction capability proof, writers use the legacy state profile:

```text
UploadStarted -> Uploaded -> LocalCommitStarted -> Committed
             \-> UploadOutcomeUnknown -> Uploaded
             \-> AbortedNoRemote
```

When every current topology member answers the exact `transition_transaction_compaction_v1` capability challenge, a writer may hold that generation's non-cloneable proof permit and emit the compact v1 profile:

```text
UploadOutcomeUnknown@1 -> LocalCommitStarted@2 -> conditional record delete
```

The create-only `UploadOutcomeUnknown@1` record is durable before the remote PUT. After PUT succeeds, the writer revalidates the source, Object Lock decision, tier generation, and fleet proof before one exact successor CAS to `LocalCommitStarted@2`; that successor carries the known remote version. After the local metadata commit, the writer conditionally deletes that exact record rather than persisting a redundant `Committed` generation. A crash before the PUT leaves an unknown record whose provider probe can prove absence. A crash after PUT leaves the canonical candidate under the unknown record. A crash after the commit fence leaves the exact remote tuple under `LocalCommitStarted`, and a crash after local commit lets recovery prove ownership transfer and remove only the record.

The `UploadOutcomeUnknown@1 -> LocalCommitStarted@2` pair is the only compact direct edge. Legacy `UploadOutcomeUnknown@2 -> LocalCommitStarted@3` remains invalid, while historical receipt jumps keep their separately enumerated revision distances. This distinction lets existing v1 payload readers decode compact records without treating arbitrary skipped history as valid. If any peer is absent, old, unreachable, restarted with a different process epoch, or the topology changes, proof publication/revalidation fails closed and new attempts use the legacy profile. Planned downgrade first disables compact admission and drains live compact records; an older runtime reader retains or safely reconciles the v1 states, but an older decommission checkpoint validator can reject the compact successor and block pool completion.

Separately, `mark_cleanup_pending` permits proof-checked model edges from `Uploaded`, `UploadOutcomeUnknown`, and `LocalCommitStarted`. Current production recovery emits `CleanupPending` after an expired `Uploaded` record wins the exact successor CAS, or when an expired `UploadOutcomeUnknown` probe returns `UnversionedPresent` or `VersionedPresent` with a non-nil identifier. `LocalCommitStarted` mismatch or missing-source recovery retains the record; that cleanup edge is currently exercised through the state-machine API and tests, not produced by runtime recovery. States that require a remote delete still require a known `TransitionRemoteVersion` kind. A probed versioned candidate whose identifier parses as a nil UUID is retained and never authorizes remote deletion.

The remote candidate itself is named by `canonical_transition_remote_object` under `ilm/transition-transactions/<bucket-hash>/<transaction shards>/<transaction-id>/<write-id>`. That deterministic identity is what a provider probe or exact cleanup must bind; it is distinct from the internal transaction-record key.

The creator owns the canonical remote candidate until local metadata commits the exact tuple. A committed `xl.meta` version then owns reachability. The transaction record is not a second cleanup owner after that transfer.

### Recovery decisions

| Observed durable state/input | Unique current owner | Current recovery decision | Approved destructive admission |
|---|---|---|---|
| `UploadStarted` | Originating transition attempt; no current recovery successor is emitted | Retain | No delete. The upload may still publish |
| `UploadOutcomeUnknown`; exact provider probe says missing | Transaction recovery under the exact record generation and recovery lock | Conditionally delete the record | Strong probe identity must match transaction/backend; no remote delete occurs |
| `UploadOutcomeUnknown`; probe returns `UnversionedPresent` | Transaction recovery, with operator reconcile available after expiry | Persist `CleanupPending`, delete the unversioned candidate, delete the record | Exact transaction/canonical object/backend identity, explicitly unversioned state, durable takeover after owner expiry, current tier lease, and exact reread before cleanup |
| `UploadOutcomeUnknown`; probe returns `VersionedPresent` with a non-nil exact identifier | Transaction recovery, with operator reconcile available after expiry | Persist `CleanupPending`, exact-delete that versioned candidate, delete the record | Exact transaction/canonical object/backend identity and remote version, durable takeover after owner expiry, current tier lease, and exact reread before cleanup |
| `UploadOutcomeUnknown`; probe returns `VersionedPresent` whose identifier is a nil UUID | Transaction recovery retains ownership evidence | Retain | A nil identifier is invalid exact-version evidence and never becomes unversioned or remote-delete authority |
| `UploadOutcomeUnknown`; probe ambiguous, unsupported, or errors | Transaction recovery retains ownership evidence | Retain | No destructive action; operator reconcile may inspect after expiry |
| `Uploaded` | Originating transition attempt until expiry; after expiry, the worker that wins `Uploaded -> CleanupPending` by exact ETag CAS | Retain while active; after expiry, persist `CleanupPending`, then recheck and delete the unreferenced candidate or record | Current CAS fences the predecessor, but approved v2 also requires a durable recovery lease, full all-pool source/free-version proof, and before/after fence checks |
| `LocalCommitStarted`; logical source lookup returns the complete recorded source identity and `TRANSITION_COMPLETE` with the same remote object, tier, and remote version | Transition committer until ownership transfers to `xl.meta` | Delete transaction record | Recovery treats only the full source and remote tuple as ownership transfer; a mismatch remains operator-required and cannot authorize remote deletion |
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

- Preserve the current create-only transaction installation, exact record/ETag successor CAS, and conditional terminal delete, and add mandatory exact-successor strong readback for lost or uncertain responses. No v2 work may regress the existing v1 protections.
- Recovery of `Uploaded` and cleanup-capable states must acquire durable ownership only after the prior owner's expiry. The recovery worker must reread the exact generation after takeover and before remote DELETE.
- Preserve and compare source version ID, data directory, modification time, size, and ETag through local commit and recovery before accepting ownership transfer.
- Recompute and require the exact lowercase sharded path in both transition-transaction and manual-job runtime recovery, and validate a truncated page's continuation token before processing any record from that page.
- The approved v2 owner lease uses the duration, clock-skew allowance, takeover revision, and v1 mapping in [Transition transaction v2 and v1 migration](#transition-transaction-v2-and-v1-migration).
- Active automatic retries are bounded by the recovery-control policy below. Ambiguous evidence becomes explicit `retained_ambiguous` or `operator_required`; source evidence is never collected merely because it is old.

## Tier mutation intent

### Current contract

`TierMutationIntent` models Add/Edit/Remove/Clear with `Prepared`, `Committed`, and `Aborted`. The identity includes the old tier-config ETag, candidate digest, canonical sorted affected targets and their old/new backend identities, and expiry. Revision and committed config ETag are the only mutable generation data.

New intents use a 15-minute expiry. A peer-only terminal tombstone is retained until that expiry plus five minutes of clock-skew allowance and until no coordinator record remains. Expiry bounds replay protection; it is not config commit/abort evidence.

The coordinator creates its durable record and peer `Prepare` blocks new reference creation, drains exact tier-operation leases, and proves that edit/remove/clear will not strand authoritative references. Prepare, Commit, and Abort use all-node fanout rather than quorum: independent peer calls use a work-conserving concurrency limit of four, a 30-second per-peer deadline, and a 30-second fanout-wide deadline; Prepare is additionally capped by the intent expiry. The coordinator collects every completed outcome. A timed-out or otherwise ambiguous started Prepare is included in compensating Abort because cancellation does not prove the peer failed to persist its fence; peers not started before the fanout deadline make Prepare fail but do not require Abort. The coordinator then conditionally writes tier config, durably commits the coordinator intent, releases its exclusive guards, requires every prepared peer to commit, publishes the runtime candidate, and clears the block. Per-mutation sharded mutexes serialize local phases only; persisted intent plus tier-config ETag is authoritative.

Terminal recovery does not reinstall a process-local operation fence when the published in-memory manager has the exact committed candidate digest. This exception affects only ordinary tier-operation leases: recovery still replays peer `Commit`, retains and conditionally cleans the durable evidence, and blocks a new tier configuration mutation until the recovery snapshot is quiescent. A different local digest remains fenced until the committed candidate is safely published.

### Recovery decisions

| Observed durable state/input | Unique current owner | Current recovery decision | Destructive/config admission |
|---|---|---|---|
| `Prepared`; current tier-config digest equals candidate | Coordinator recovery; each peer recovery owns only its matching peer record/block | CAS to `Committed`, replay peer Commit and publish | Exact mutation identity and candidate digest; never infer from expiry |
| `Prepared`; intent is unexpired and current config still proves the old ETag/config | The live coordinator remains owner | Retain `Prepared` and the runtime block | Recovery must not abort work that may still be in lock-free peer Prepare or reference proof |
| `Prepared`; intent is expired and current config still proves the old ETag/config | Coordinator recovery | Fan out canonical Abort, then CAS `Aborted` | Abort only the matching intent; a delayed matching Prepare converges to the tombstone |
| `Prepared`; config is a third generation, unreadable, or peer outcome is ambiguous | Coordinator record remains owner of the block | Retain `Prepared` and runtime block | No commit, abort, cleanup, or unblock |
| `Committed` | Coordinator recovery, with peers owning convergence of their local records | Replay peer Commit/runtime publication; clean exact converged records | Config ETag/digest and peer identity must match |
| `Aborted` | Coordinator recovery; peer recovery retains the local tombstone | Replay/confirm Abort and clear matching block; retain peer tombstone until expiry plus clock skew and no coordinator | Never roll back config based on timeout alone |
| Same mutation ID with different identity, illegal edge, corrupt/unknown record, or truncated scan | No actor acquires a conflicting mutation identity | Fail the recovery pass and retain | Fail closed |

Peer protocol v4 accepts current v4 and prior v3 messages. A v4 coordinator recognizes only the exact authenticated unsupported-version response from a v3-only peer and does not retry automatically as v3. Operators pause and drain tier edit/remove/clear for the mixed-version interval and resume only after every topology member supports v4. Ordinary object I/O and existing cleanup remain available.

Intent transitions retry an ETag race at most three times before returning a retryable failure. Recovery fully paginates coordinator and peer prefixes; a decode/read failure or a truncated page without a continuation token fails the pass rather than declaring convergence.

### Approved target and open design

- Keep create-only, ETag CAS, exact identity comparison, canonical Abort tombstones, and lost-response readback.
- The phase split is fixed: ETag snapshot, lock-free backend validation, ETag revalidation, all-node Prepare, reference proof, ETag/digest/intent revalidation, config CAS, all-node Commit, and local publication. Backend validation uses `(old_config_etag, candidate_digest)` as its stable config generation; the durable mutation identity adds `mutation_id`. Runtime driver revisions are not persistence identities and Add does not require one before publication.
- Lease drain, peer Prepare, and reference proof run outside both exclusive guards after the coordinator Prepared record and local fence are durable. The commit path reacquires namespace WRITE then `admin_updates` and repeats the full generation/identity proof. Expiry is checked as an additional rejection boundary and never replaces config-generation proof.
- **Open:** a dedicated operator reconcile/status surface and bounded retention for irreconcilable coordinator/peer records.

## Tier validation probe intent

### Dormant current contract

`TierProbeIntent` defines a strict, checksum-protected `rustfs-tier-probe-intent-v1` envelope and the canonical key `ilm/tier-probe-intents/records/<aa>/<bb>/<probe-id>.json`. The same non-nil probe UUID also determines the remote object name `rustfs-tier-probe-<probe-id>`. The path parser rejects uppercase UUID aliases, wrong shards, extra components, and path/payload/object-name disagreement. The durable namespace registry validates this record so pool decommission cannot silently treat it as an ordinary object.

The format binds Add and Edit to the same durable mutation identity tuple `(mutation_id, old_config_etag, candidate_digest)`. The old config ETag is required even for Add because it identifies the complete persisted tier configuration generation, not whether the named destination tier already exists. Verify instead requires the current persisted config ETag and credential-independent backend identity. These are mutually exclusive tagged variants. Every record also requires the tier name, matching destination identity, immutable creator identity/epoch, positive creation time, and an owner fence with nonempty owner, non-nil epoch, and later `not_after` timestamp. In v1 that owner identity must remain exactly equal to the immutable creator identity. It never persists the process-local driver revision or credential-bearing driver fingerprint.

The dormant state graph is:

```text
UploadOutcomeUnknown -> Uploaded -> CleanupPending -> Completed
                     \-> CleanupPending -> Completed
                     \-> AbortedNoRemote
```

`UploadOutcomeUnknown` and `AbortedNoRemote` carry no remote version. `Uploaded`, `CleanupPending`, and `Completed` carry either explicit unversioned semantics or one exact nonempty opaque version. Once known, that remote version cannot change. Revision advances by one for each edge: `UploadOutcomeUnknown` is revision 1, `Uploaded` and `AbortedNoRemote` are revision 2, `CleanupPending` is revision 2 or 3, and `Completed` is revision 3 or 4. The strict decoder rejects any other state/revision pairing. The direct `UploadOutcomeUnknown -> CleanupPending` edge is reserved for a future authoritative provider probe that discovers the exact cleanup candidate after the original PUT response was lost.

Create-only, ETag CAS, exact-ETag delete, and read-with-ETag primitives exist for the record, plus a crate-level read-only inspection result that always reports both writer and destructive recovery as disabled. No Add, Edit, Verify, startup loop, periodic loop, admin HTTP route, or remote backend operation currently calls these mutation primitives. Consequently this version creates no records and authorizes no remote PUT or DELETE.

### Activation requirements

- Add/Edit must create the mutation identity before validation and reread the same `(mutation_id, old_config_etag, candidate_digest)` before every probe-intent successor. Verify must reread the same config ETag, tier, and backend identity. A credential rotation may supply usable current credentials only when the persisted destination identity remains exact; it may not weaken operation-generation checks.
- Before enabling any writer, every required node must advertise a probe-intent-specific read/retain/recovery capability. This protocol does not reuse the legacy transition-state reconciliation capability or its token. Unknown or older nodes keep validation in the current process-local mode and no durable v1 record is written.
- The v1 owner fence is immutable and must equal the creator identity. Any future takeover requires a new schema with explicit takeover proof, plus an approved lease duration, clock-skew allowance, durable owner/epoch CAS, and revalidation order. Expiry alone never permits remote DELETE.
- Remote-version discovery and deletion must use the provider-bound, destination-bound implementation and bounded request API approved for that target. Unknown, multiple, changing, unsupported, or unavailable results retain the record.
- A successful or response-lost state write must strongly reread the exact record. Remote DELETE requires a current operation-generation proof, exact destination, current credentials for that same destination, a valid fleet fence, durable takeover, and the same known remote version immediately before and after the call.
- Terminal retention, bounded scanning, metrics, and any HTTP inspect/reconcile route remain unapproved. Raw age is never cleanup evidence, and this dormant core API must not be presented as an operator endpoint.

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
- Quarantined v1/v2 records, incomplete manifests, and repeatedly failing exact deletes use the bounded retry, explicit operator state, and single-record control surface approved below. Capacity and recovery throughput must not be “fixed” by weakening ownership proof or age-deleting source evidence.

## Bounded recovery control and operator disposition

This section is an **approved target that is not implemented yet**. It closes the ownership, retry, and operator-disposition design required before backlog recovery work can change destructive behavior. It does not authorize an implementation to enable a v2 writer, take over a v1 transaction, or remove a legacy journal until the fleet and storage gates below exist.

### Recovery-control record

Retry scheduling is persisted separately from the source transaction or journal so legacy bytes remain readable and their cleanup ownership does not move. The control record uses schema `rustfs-ilm-recovery-control-v1`, a checksum envelope, a 16 KiB encoded-size limit, strict unknown-field rejection, and this canonical key:

```text
ilm/recovery-controls/<protocol>/<aa>/<bb>/<source-operation-digest>.json
```

Export envelopes use `ilm/recovery-exports/<protocol>/<aa>/<bb>/<export-id>.json`; disposition receipts use `ilm/recovery-dispositions/<protocol>/<aa>/<bb>/<disposition-id>.json`. `export-id` is the SHA-256 of the control ID plus exact observed source content/copy-set digests; `disposition-id` is the SHA-256 of the export ID plus the bounded action. Repeating an identical export or disposition reuses and strongly validates the same canonical object; different bytes at that ID are a conflict. Both use strict checksum envelopes and create-only installation. A control or disposition is limited to 16 KiB. An export is limited to the source protocol's own maximum encoded record size plus 64 KiB for its copy manifest and envelope; it stores the source bytes once rather than duplicating identical replica bytes.

Admission is fail-closed and cluster-wide. One control may have at most one export creation and one disposition application in flight. The immutable candidate is fully encoded before quota admission. The cluster-scoped recovery-admission WRITE lock is always outermost; a caller already holding any control, source, disposition, bucket, physical, migration, or decommission guard must release it and restart in the order above. While admission is held, a complete artifact inventory must prove that the projected totals, including the candidate, are at most 10,000 export envelopes, 10,000 disposition receipts, 1 GiB of encoded export data, and 256 MiB of encoded control/disposition data. The create-only candidate installation and exact strong readback complete before the lock is released, so neither a single candidate nor a concurrent node can oversubscribe the pre-create snapshot. A crash before installation leaves no artifact; a lost create response is resolved by exact canonical readback while admission remains serialized or after reacquiring it in the same order. Replaying an existing canonical ID consumes no new count, byte, or rate token. New creations are limited to ten per authenticated actor per minute and 100 cluster-wide per minute, with at most 32 export creations and eight disposition applications executing cluster-wide. Exceeding a count, byte, rate, or concurrency limit returns a retryable capacity result before source mutation; it never evicts evidence, interrupts an admitted operation, or blocks ordinary object I/O. The collector examines at most 100 terminal artifacts per minute and never acquires the admission lock while holding an artifact/source guard.

`source-operation-digest` is SHA-256 over the length-delimited source protocol, canonical source path, and stable semantic operation identity: transaction UUID, journal identity, or manifest operation ID. For corrupt bytes whose semantic ID cannot be trusted, the canonical path plus a `corrupt` domain separator is the stable identity; a replacement at that path conflicts with the existing control instead of resetting its history. The control's immutable identity repeats the protocol, path, stable identity, and record class. Its mutable `observed_source_generation` contains the source schema, source ETag/content SHA-256, and sorted all-pool copy-set digest. The copy set records each authoritative pool/set identity, canonical path, ETag, byte length, and content SHA-256; an unreachable pool, missing ETag, divergent copy, or incomplete listing cannot produce an actionable generation. The remaining mutable generation contains:

- `revision`, an ETag-CAS successor counter;
- `classification`: `retrying`, `retained_ambiguous`, `corrupt`, `operator_required`, `abandoned`, or `terminal`;
- `owner_id`, `owner_epoch`, `lease_acquired_at_unix_nanos`, and `lease_expires_at_unix_nanos` while an attempt is owned;
- monotonic `attempt_count`, `consecutive_failure_count`, `first_failure_at_unix_nanos`, `last_failure_at_unix_nanos`, and `next_attempt_at_unix_nanos`;
- one bounded enum `last_error_code`; no free-form provider error, endpoint, credential material, request payload, or response body is persisted;
- for an operator disposition only, the authenticated actor identifier, reason code, confirmation time, exported payload digest, and exact source/control ETags that were confirmed.

The control record is a scheduler and audit fence, not a remote-object owner. It never substitutes for the source record's version, backend, manifest, source-identity, or absence proof. Before every source CAS, local cleanup, or remote request, the worker strongly rereads every authoritative source copy and the control, requires the current observed generation to match, and then applies the source protocol's own locks and proof. A missing, stale, corrupt, divergent, or unavailable control read cannot authorize work.

Control creation is `If-None-Match: *`; every update is exact ETag `If-Match`; response loss requires exact strong readback. A legal source-state CAS does not create a new control. The stable control CAS advances `observed_source_generation` only from the exact predecessor to one source-protocol successor while preserving `first_failure_at_unix_nanos`, lifetime `attempt_count`, and first-seen lineage. If the source CAS succeeded before a crash, recovery accepts the new bytes only after validating that exact legal successor and then converges the old control generation by CAS. A multi-edge jump, semantic-identity change, replacement ETag/content, or unavailable predecessor proof is a conflict and cannot reset counters. Recovery conditionally removes a terminal control only after strongly proving the stable source operation resolved or absent, recording any active decommission terminal receipt, and confirming that no in-flight operator request still names its ETag. `durable_namespace.rs` must register every new namespace, path parser, decoder, size bound, successor relation, and terminal checkpoint before rollout.

### Transition transaction v2 and v1 migration

The successor envelope is `rustfs-transition-transaction-v2`. It preserves the v1 transaction ID, deployment ID, write ID, complete source identity, tier/backend identity, canonical remote object, remote-version state, operation state, and revision. It also records an immutable `origin_format` (`native_v2` or `migrated_v1`), the state-entry revision and predecessor state/revision, and, for migration, the exact source v1 state/revision. These fields make the state history needed for destructive authorization independently checkable instead of inferring it from the current state. The envelope and body reject unknown fields, checksum every field, use no default for a required v2 value, require the existing exact lowercase canonical path, and reject nil UUIDs, nonpositive timestamps, revision zero/overflow, lease inversion, impossible state/revision history, and illegal state/version combinations. It replaces the fixed ownership pair with:

- immutable `creator_epoch` and `creator_not_after_unix_nanos`, copied exactly from v1 `owner_epoch` and `not_after_unix_nanos` during migration;
- optional `created_at_unix_nanos`; migrated v1 records use `None` rather than inventing an age;
- mutable `owner_role` (`creator` or `recovery`), `owner_id`, `owner_epoch`, `owner_generation`, `lease_acquired_at_unix_nanos`, and `lease_expires_at_unix_nanos`. `owner_id` is the authenticated stable fleet-node identity; `owner_epoch` is a fresh UUID for one process claim. A node without both identities cannot create, renew, take over, or act on v2.

A native v2 create uses revision and owner generation 1, a fresh non-nil creator owner/epoch, `UploadStarted`, and an unknown remote version. The first rollout keeps the current seven-day creator ownership window. Creator leases are not renewable; a creator that cannot finish inside the safety window stops publishing and leaves the record for recovery. Recovery-owner leases are 15 minutes. The persisted clock-skew allowance is five minutes: an action may start only when its bounded deadline fits before `lease_expires_at - 5 minutes`, and another owner cannot take over until its local time is at least `lease_expires_at + 5 minutes`. A recovery attempt remains capped at five minutes and every remote call remains subject to its narrower client deadline. A recovery renewal is a same-owner, next-revision CAS that strictly extends expiry; a takeover changes `owner_role`, `owner_id`, and `owner_epoch`, increments `owner_generation` and `revision`, and uses the exact observed ETag. Takeover and state advancement are separate CAS operations; one revision cannot both acquire ownership and claim a recovery outcome.

Before migration can be enabled, the fleet must first deploy a v1 creator fence that strongly rereads the exact transaction path, ETag, state, owner epoch, and source generation immediately before local metadata publication and refuses to publish after any migration/takeover change. The fleet then durably disables new v1 admission and proves that every captured creator/recovery process epoch has either acknowledged quiescence or terminated. A paused or unreachable epoch prevents migration. This drain barrier is distinct from format capability advertisement and remains in force until v2 writer admission is enabled.

Only these checksum-valid v1 state/revision pairs are migration inputs: `UploadStarted@1`; `UploadOutcomeUnknown@2`; `AbortedNoRemote@2`; `Uploaded@2` or `Uploaded@3`; `LocalCommitStarted@3` or `LocalCommitStarted@4`; `Committed@4` or `Committed@5`; and `CleanupPending@3`, `CleanupPending@4`, or `CleanupPending@5`. Any other pair is `corrupt`, inspect-only, and cannot be migrated or authorize a probe, local cleanup, or remote DELETE. A native v2 record must prove a legal predecessor edge at its recorded state-entry revision; ownership-only revisions may increase the outer revision but cannot change the recorded state-entry history. Missing, contradictory, or skipped history is corrupt.

An identity-preserving `v1 -> v2` conversion is one legal successor with `revision + 1` and an unchanged remote tuple. The state is unchanged except that every v1 `UploadStarted` maps conservatively to v2 `UploadOutcomeUnknown`. Historical v1 writers could issue PUT while still in `UploadStarted`; no age, fleet version, or current process observation proves that a retained record came from the later pre-PUT-fence writer. It is permitted only when:

1. every node that can create, commit, recover, heal, or decommission the record advertises both `transition_transaction_v2` and `ilm_recovery_control_v1` for the captured fleet/topology generation, the durable v1-admission stop is active, and the process-epoch drain barrier above is complete;
2. current time is at least the v1 `not_after_unix_nanos` plus five minutes of skew;
3. the canonical path, checksum, full immutable identity, state, remote-version invariant, source record, and ETag all match the observed v1 generation;
4. the migration CAS and strong readback install one fresh recovery lease before any state transition or side effect.

The original creator must successfully CAS `UploadStarted -> UploadOutcomeUnknown` before issuing remote PUT, and must CAS the exact current owner generation to `LocalCommitStarted` before publishing local metadata. A v2 takeover therefore fences a delayed creator. An implementation that can issue PUT or publish after losing this CAS is not compatible with this protocol.

After takeover, recovery applies this matrix:

| State | Approved recovery after exact takeover | Required proof before side effect |
|---|---|---|
| native-v2 `UploadStarted` | CAS `AbortedNoRemote`, then conditionally remove the terminal transaction | Valid native-v2 history proves the creator was fenced before the mandatory pre-PUT `UploadOutcomeUnknown` CAS; migrated v1 never enters this row and no remote request is made |
| `UploadOutcomeUnknown` | Probe under the exact backend lease. Missing becomes terminal cleanup; proven unversioned presence or one nonempty, non-nil exact version becomes `CleanupPending`; nil, ambiguous, or unsupported results become `retained_ambiguous` | Exact transaction/control generations, bounded live probe, current tier destination and lease |
| `Uploaded` | If the exact transitioned tuple or its free-version owns the candidate, remove only the transaction. If the complete original source is still unchanged and no local commit/free-version exists, CAS `CleanupPending`. Otherwise retain | All-pool source/free-version read, full source tuple, bucket incarnation, tier generation, object locks, and post-probe revalidation |
| `LocalCommitStarted` | Exact committed tuple/free-version means record-only cleanup. A fully unchanged original source with no partial committed tuple may move to `CleanupPending`. Missing, divergent, partial, or unavailable metadata is `retained_ambiguous` | Same all-pool proof, including data directory, modification time, size, ETag, transition transaction ID, remote tuple, and destination identity |
| `CleanupPending` | Resume the same exact idempotent candidate delete, or remove only the transaction when local ownership transfer is proven | Current owner lease, source/free-version proof, exact tier lease, physical locks, and before/after fence checks |
| `Committed` | Conditionally remove only the exact terminal transaction when complete local ownership transfer is proven; otherwise classify the record as corrupt or retain it as ambiguous | All-pool strong read matches the complete logical `xl.meta` source identity, transaction ID, remote tuple, destination identity, and any required decommission terminal receipt; no remote DELETE |
| `AbortedNoRemote` | Conditionally remove the exact terminal transaction | Valid native-v2 pre-PUT history or exact migrated v1 `AbortedNoRemote@2`, exact terminal generation, and any required decommission terminal receipt; no remote DELETE |

Remote DELETE is never admitted directly from `UploadStarted`, `UploadOutcomeUnknown`, `Uploaded`, or `LocalCommitStarted`; it first requires a CAS-protected `CleanupPending` generation with known remote-version semantics. A lost source read, mixed all-pool result, expired lease, failed renewal, or source/control CAS conflict retains the evidence and performs no destructive action.

New writers emit v2 only after the homogeneous fleet gate, durable v1-admission stop, and process-epoch drain barrier are complete. During a rolling upgrade, new readers accept v1 but all writers continue v1 and no v1 takeover/migration occurs. A v1 reader rejects and retains v2. Downgrade is blocked until v2 creation is disabled and all v2 transactions and recovery-control records are drained or exported; live v2 bytes are never rewritten to v1.

### Retry and retention policy

An attempt that loses a CAS or discovers a newer source generation reloads instead of recording a remote failure. A retryable transport timeout, backend 5xx/throttle, metadata quorum outage, or bounded remote-delete failure increments the persisted counters and schedules:

```text
min(60 seconds * 2^min(consecutive_failure_count - 1, 6), 1 hour)
```

A deterministic multiplier from 80 to 100 percent, derived from the source-generation digest and attempt count, is applied to that capped base. The jitter can only shorten the delay and therefore never exceeds the one-hour cap; restarts reproduce the same deadline without synchronizing a fleet. Success or a proven source-state advance resets `consecutive_failure_count` but never decreases `attempt_count`. `next_attempt_at_unix_nanos` is only a not-before scheduler hint; ownership and destructive authority still require the lease and source proofs.

After 32 consecutive retryable failures or seven days since `first_failure_at_unix_nanos`, whichever occurs first, the control CAS moves to `operator_required` and automatic attempts stop. Unsupported probes and unknown remote-version semantics move directly to `retained_ambiguous`; corrupt source bytes use `corrupt`; incomplete destructive evidence uses `operator_required`. None is periodically hot-looped. An operator may explicitly request another bounded attempt after the underlying capability or configuration changes, but the request creates a new owner lease and preserves the lifetime attempt count.

This policy bounds automatic work, not evidence lifetime. A source transaction, journal, or manifest is never deleted solely because it is old, numerous, or over a byte threshold. `operator_required` source evidence remains until its protocol reaches a proven terminal state or the legacy-journal disposition below is completed.

Resolved control tombstones are retained for at least 30 days, immutable export envelopes for at least 90 days, and compact completed disposition receipts for at least 365 days. A collector may conditionally remove only a terminal artifact past its floor after proving that the bound source generation is absent where required, no nonterminal successor or active decommission references it, and the terminal audit checkpoint is durable. Capacity pressure blocks new export/disposition work rather than evicting unexpired or nonterminal evidence. Uncertainty retains the artifact; collection never authorizes source or remote deletion.

### Legacy journal and manifest disposition

Journal v1 has neither backend identity nor remote-version authority; v2 has backend identity but still lacks remote-version semantics. Their automatic classification is `retained_ambiguous`, and neither recovery nor an operator action may instantiate a backend or issue remote PUT, GET, probe, or DELETE from those bytes. The approved single-record actions are:

- **inspect**: strictly decode a server-reconstructed canonical journal identity, perform an all-pool strong read, and return a redacted copy-set/content digest, version, quarantine reason, control classification, age information when known, topology readiness, and decommission coverage; it changes nothing and does not return raw object/version fields by default;
- **export**: after a fresh exact inspect, create-only persist an immutable `rustfs-ilm-recovery-export-v1` envelope containing the raw source bytes and sorted copy manifest, then strongly read it back. The response downloads that envelope rather than rereading the live journal, uses no-store/attachment semantics, and never adds credentials or backend configuration;
- **abandon after export**: v1 or v2 only; create a `Prepared` `rustfs-ilm-recovery-disposition-v1` receipt bound to the immutable export and every source copy, conditionally remove only those exact local journal generations, prove every bound copy absent with no replacement, and advance the receipt through `Applying` to `Completed`. This accepts a possible remote storage leak and never asserts that cleanup occurred.

Export and abandon require a fresh all-member capability/topology proof including each member's current process epoch; inspect may remain available in a mixed fleet but returns not-ready for mutation. `abandon after export` uses POST and requires `confirm: true`, `action: abandon_remote_cleanup`, `acknowledge_remote_cleanup_abandoned: true`, the export operation ID/digest, source content and copy-set digests, every source ETag, control ETag, and a bounded operator reason code. It is refused while an active decommission or migration receipt covers either record, while physical copy discovery is incomplete, or when the implementation cannot target every discovered copy with its own `If-Match` condition.

The disposition receipt has immutable action/export/control identities and an immutable sorted copy manifest. Its ETag-CAS generation contains state `Prepared`, `Applying`, or `Completed` and a monotonic sorted `confirmed_absent` set naming only entries from that manifest. Before `Prepared -> Applying`, a fresh all-pool read must find every bound copy at its exact ETag/content digest and repeat the fleet, lock, migration, and decommission checks. The manifest can never be widened, reordered, or replaced.

During `Applying`, recovery treats each manifest entry independently while retaining the original all-pool boundary. An entry already in `confirmed_absent` must still be strongly absent with no successor or replacement generation. For an unconfirmed entry, an exact ETag/content match may be conditionally deleted and then added to `confirmed_absent` only after strong absence readback. If a crash or lost response left that exact path absent before the progress CAS, recovery may add it only after the same strong absence, stable source/control generation, topology, process-epoch, migration, and decommission proofs establish that no replacement exists. A different ETag/content, an unbound copy, an unreadable member, or loss of any proof is a conflict and preserves the current progress. Thus a crash after deleting copy A but before recording its progress can converge and continue with copy B without requiring deleted copy A to reappear.

Immediately before each local metadata deletion, the server repeats the applicable all-pool/fleet/lock checks and conditionally targets only the still-unconfirmed exact ETag. Completion requires every immutable manifest entry in `confirmed_absent`, a fresh all-pool proof that all remain absent without replacement, unchanged fleet/process epochs, and no active decommission or migration coverage. A lost final response is success only when strong readback proves the canonical receipt `Completed`. Recovery may repeat only this canonical operation and never creates a tier client or issues a backend request.

Malformed or unsupported bytes whose outer v1/v2 identity cannot be proven are inspect-only and cannot use abandon. Versions v3-v6 never use `abandon after export`. Their known candidate or manifest ownership must converge through the normal exact protocol. An operator may inspect/export and request a bounded retry, but cannot bypass source/free-version proof, manifest membership, topology, or remote-version validation. `Preparing`/`Aborting` manifests may use their existing whole-set rollback; `DispatchAuthorized`/`Completed`, a missing member, a nonempty operation namespace, or any uncertain binding cannot be manually removed.

### Admin and metrics contract

The approved surface is single-record and uses a protocol-specific expected tuple; it does not reuse the legacy metadata-reconcile digest or create a bucket/prefix job:

```text
GET  /rustfs/admin/v3/ilm/recovery/records?protocol=<protocol>&classification=<classification>&limit=<n>&marker=<opaque>
GET  /rustfs/admin/v3/ilm/recovery/records/<control-id>
POST /rustfs/admin/v3/ilm/recovery/records/<control-id>
GET  /rustfs/admin/v3/ilm/recovery/exports/<export-id>
```

List and redacted inspect require `admin:ListTier`. Raw export creation/download, retry, or abandon require `admin:SetTier` because legacy bytes can reveal bucket, object, tier, and remote-version information. The default page is 100 records and the hard maximum is 1,000. A truncated page without a continuation marker is an error, and counts from an incomplete scan are labeled incomplete rather than reported as zero or complete.

Inspect returns a 15-minute opaque observation receipt bound to the authenticated actor, canonical record identity, source/copy ETags and digests, topology/fleet generation, every member process epoch, action class, issue/expiry time, and nonce. It is observation evidence, not mutation authority. POST requires that receipt and `confirm: true` for terminal actions, and binds the control ETag/revision/classification, requested action, and export digest when applicable. The server repeats all live proofs; a restarted member, membership change, or process-epoch mismatch invalidates the receipt. Client fields are concurrency guards, not authority. Export download reads the immutable envelope, uses TLS plus `Cache-Control: no-store` and attachment disposition, and never logs the raw payload.

Metrics use bounded labels only:

- `rustfs_ilm_recovery_records{protocol,classification,schema}` and `rustfs_ilm_recovery_oldest_age_seconds{protocol,classification,schema}`;
- `rustfs_ilm_recovery_attempts_total{protocol,outcome,error_code}`;
- `rustfs_ilm_recovery_operator_actions_total{protocol,action,outcome}`;
- scan completeness, corrupt-record, and orphan-control counters.

Every label value is a closed enum. `schema` exposes recognized schema identifiers only; an unrecognized raw value maps to `unknown`, while a recognized future schema disabled by the current fleet maps to `unsupported`. `protocol`, `classification`, `outcome`, `error_code`, and `action` likewise map unknown input to one bounded fallback and never expose decoded or operator-provided text.

Object names, bucket names, tier names, transaction IDs, control IDs, endpoints, ETags, error text, and credentials are never metric labels. Admin output may identify the selected record but redacts credentials and raw backend configuration. Audit/log events use the repository ILM event fields, stable reason codes, authenticated actor, source/control generations, action, and outcome; they do not persist or log provider response bodies.

One canonical source generation counts once regardless of its physical copy count or how many recovery passes observed it. Attempt counters increment once per coordinator attempt, not per replica, CAS retry, or page revisit. Aggregate totals and oldest age are authoritative only after a complete all-pool scan; partial coverage reports `incomplete` and never publishes a false zero. A legacy record's first-seen time comes from its durable control record rather than an inferred object modification time.

### Required protocol fixtures

Implementation acceptance requires deterministic crash/restart and mixed-version fixtures, not timing-only tests. At minimum they cover:

- a historical v1 `UploadStarted@1` whose PUT may have reached the provider, proving migration yields `UploadOutcomeUnknown` and never `AbortedNoRemote` or a direct delete;
- every accepted v1 state/revision pair above plus checksum-valid impossible pairs such as `CleanupPending@1`, proving impossible history is inspect-only and makes zero backend calls;
- an in-flight v1 creator interleaved with admission stop, process-epoch drain, migration, takeover, and local publication, proving no stale creator can publish after takeover;
- `Committed` with complete, missing, partial, divergent, and unavailable all-pool `xl.meta` ownership proof, proving only the complete exact tuple permits record cleanup;
- an old reader retaining v2, a mixed fleet blocking v2 writer and migration, and downgrade refusing until v2/control records are drained or exported;
- operator abandon across a crash after one per-copy delete, a lost delete response, a lost progress CAS, a replacement ETag, incomplete topology, and active decommission, proving progress is monotonic, replacements survive, unsafe cases retain evidence, and every case issues zero backend PUT, GET, probe, or DELETE calls;
- canonical export replay, a crash before candidate installation, a lost create response, concurrent admission at the remaining-byte boundary, and actor/cluster count, byte, rate, and concurrency exhaustion, proving duplicate IDs consume no new quota, projected totals never oversubscribe, and admission failure mutates no source evidence.

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

The decommission coordinator copies and validates a durable record on a target, persists a receipt for that exact source path/identity/checkpoint, and records the expected receipt set on the source. While a matching decommission operation is active, protocol writers advance receipts as records change and terminal cleanup records a terminal checkpoint before deleting a covered record. Without an active decommission operation, the receipt helper creates no terminal receipt and ordinary protocol recovery proceeds with that protocol's current exact ETag conditional-delete primitive. Completion verifies every expected receipt and target checkpoint before the source pool can be removed.

A terminal receipt is proof that an exact target copy reached a terminal checkpoint. It may authorize conditional removal of the matching source record when every active target copy is covered; it never authorizes remote DELETE. A terminal receipt on one target cannot hide a later nonterminal receipt on another target.

Receipts have no enum state. Their legal evolution is `absent -> checkpoint -> monotonic successor -> optional terminal checkpoint`; identity, source path, and v6 topology binding cannot change. The expected-receipt manifest evolves only from absent to one immutable sorted receipt-set digest, then is removed by the completed run's coordinator.

### Recovery decisions

| Observed state | Unique current owner | Current recovery decision | Destructive admission |
|---|---|---|---|
| No active decommission run | Underlying protocol owner | Protocol recovery proceeds normally and no receipt is created. Eligible v6 journal/manifest and transition-transaction records are removed only by their exact observed ETag | Receipt state grants no remote-delete authority |
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

The matrix below is the normative approved target, not a blanket description of current implementation. Current exceptions are authoritative only where each protocol section above labels them explicitly. Transition transaction v1 now has create-only installation, exact ETag successor CAS, and conditional terminal deletion, but it still lacks mandatory lost-response exact-successor readback and a renewable durable recovery lease. The manual job's initial write remains non-create-only.

| Event | Approved result |
|---|---|
| Crash before record create is durably confirmed | Strong-read exact key; retry create-only if absent; do not assume a candidate is unowned |
| Crash after remote PUT request but before response | Persist/retain unknown outcome; use an identity-bound provider probe or operator workflow; never guess a version |
| Lost response to state CAS or config quorum | Strong-read exact intended successor. Conflict, absence, or unreadable result remains pending |
| Crash after local transition commit | The exact logical `xl.meta` reference, full recorded source identity (version ID, data directory, modification time, size, and ETag), and remote tuple prove ownership transfer; cleanup only the terminal transaction record |
| Crash after remote DELETE but before journal/free-version cleanup | Retry the same exact idempotent DELETE under the same fences, then conditionally clean local evidence |
| Cancellation | Stop issuing new work, persist monotonic cancellation where the protocol has it, and leave ambiguous durable records for recovery. Cancellation is never rollback proof after authorization |
| Rolling upgrade | Gate writers on the minimum capability required by the format or state profile. Transition compaction falls back to the legacy v1 profile until every current topology member answers the exact capability challenge. Known older journal/RPC versions follow their explicit compatibility rule; unknown formats are retained |
| Downgrade | Disable transition compaction admission and drain compact v1 records before removing capable checkpoint validators. Drain v6 journals and any enabled transition-v2/control protocol before removing their capable workers. Do not write a new format until its downgrade reader behavior and writer gate are specified |
| Corrupt or unknown input | Record a diagnosable failure, retain bytes, and block destructive action/completion |

The compact transition-transaction v1 state profile has an implemented live homogeneous-fleet gate, but transition v2, manual job/task/result v1, and receipt v2 do not currently have a complete persisted-format negotiation for rolling downgrade. The approved transition-v2/control gate above is not current behavior. Until the applicable gate is implemented, caller/operator orchestration must not enable writers whose records required recovery nodes cannot decode. The manual async endpoint does not enforce that fleet gate and a direct request proceeds to job creation. This caller-side fail-closed rule is stricter than treating an unknown record as absent.

### Current format compatibility decisions

| Family/version | Current reader and writer behavior | Upgrade, downgrade, and ignore rule |
|---|---|---|
| Transition transaction v1 | Writers emit v1. A homogeneous live capability permits the compact `UploadOutcomeUnknown@1 -> LocalCommitStarted@2` profile; otherwise writers retain the legacy profile. The payload decoder rejects another schema, bad checksum, unknown state, or inconsistent transaction/remote identity. The current record-path parser accepts any shard/extra-component layout and uppercase hex when the final 32-hex UUID parses and matches the payload | There is no intentional ignore path, but exact lowercase canonical-path rejection remains an approved fix. During rolling upgrade, an unsupported or unavailable peer forces legacy writes. Before downgrade, disable compact admission and drain compact records because older decommission validators may conservatively reject the direct successor. V1 remains the only writer format until the approved v2 fleet gate is implemented; a v2 reader never rewrites an active v1 record |
| Transition transaction v2 and recovery-control/export/disposition v1 | Approved target only; no current reader or writer emits these formats | Roll out read support before the homogeneous writer gate; old readers reject and retain. Disable creation and prove all active records drained before downgrade; never rewrite v2 to v1 |
| Tier mutation intent v1; peer RPC v3/v4 | Durable readers/writers require intent v1. New peers accept signed/canonical v3 and v4 RPC; old v3 peers return an exact authenticated unsupported response to v4 | Pause and drain edit/remove/clear across the mixed interval; do not automatically retry v4 as v3. Unknown durable intent is retained and blocks recovery |
| Manual job/scope/task/result v1 | Writers emit the v1 family. Manual-job runtime recovery accepts an uppercase UUID path when both shard strings match its uppercase prefix, then loads the lowercase canonical job by UUID; the decommission validator recomputes the canonical path and rejects that alias. Other decoder/path/checksum failures stop reconciliation. Runtime capabilities advertise `enqueue_only` and `async`, but the async run handler does not consult a fleet capability gate and a direct request creates a job | Runtime recovery still needs exact lowercase canonical-path validation to prevent alias-driven duplicate work. Caller/operator orchestration must verify every required node and fail closed when capability is unknown or unsupported. An automatic server-side fleet gate and persisted downgrade negotiation remain open; unknown records are never ignored as completed work |
| Journal v1/v2 | Readers decode but quarantine because remote-version authority is missing; compatibility writers can preserve these forms | Never translate empty version ID to known-disabled or authorize remote DELETE. Retain unless the approved exact inspect/export/abandon protocol conditionally removes only the local journal generation |
| Journal v3/v4 | Readers recover supported committed records according to exact or explicit version-state semantics; current compatible writes use v4 for known state | Unknown/inconsistent state is retained. These legacy paths are not evidence that a new sole-owner operation may omit v5/v6 source proof |
| Journal v5 | Readers use stable source/all-pool proof; decoded v5 can be checkpointed, while new online sole-owner transactions are not emitted as v5 | Retain and recover conservatively during upgrade. Do not manufacture v5 from older records or use it to bypass v6 manifest authorization |
| Journal v6, dispatch manifest v1, and chunk parent v1 | v6-aware writers/readers require immutable manifest membership and topology. Complete sets at or below 200,000 retain the legacy root manifest bytes; larger sets install a strict parent at that root and operation-scoped v1 child payloads. Pre-chunking v6 readers reject the parent schema and child paths, while v5-and-older readers reject and retain v6 journals | Gate writers on the current fleet capability and retain the root parent for the entire active chunk sequence. Drain v6 before removing all v6-aware workers; do not downgrade by rewriting a live v6 operation |
| Decommission receipt v2 and expected manifest v1 | Current decommission readers validate exact schema/checksum/path/checkpoint and fail completion on unknown input | No ignore path. Mixed-version decommission must not complete unless every participant preserves the registered durable namespace; broader downgrade negotiation is open |

## Reconcile, observability, and retention

| Protocol | Current operator/telemetry surface | Current retention | Required follow-up |
|---|---|---|---|
| Transition transaction | Expired unknown-upload inspect/delete/finalize routes; `lifecycle_transition_transaction_recovery` diagnostics | Terminal records are deleted; ambiguous and unsafe states may remain indefinitely | Implement the approved v2 lease/takeover, recovery-control status, bounded retry, and backlog metrics |
| Tier mutation intent | Admin mutation response plus recovery diagnostics; no dedicated reconcile API | Peer aborted tombstone through expiry plus skew; ambiguous coordinator/peer records retained | Status/reconcile view for mutation, peer convergence, config generation, and blocked tiers |
| Manual job | POST run response, GET status, DELETE cancel; runtime capabilities advertise both modes | Job/task/result history is indefinite. Terminalizers only best-effort delete the exact scope; startup skips a terminal job with a leftover scope, which remains until a later admission claimant lazily replaces it | Age/count/bytes limit and a terminal-history/scope GC protocol that preserves recovery evidence |
| Tier-delete journal/manifest | `lifecycle_tier_delete_journal` events, quarantined counter, remote-delete failure/breaker/inflight metrics | Terminal records converge; quarantined/ambiguous records are unbounded by age | Implement the approved single-record inspect/export/disposition, bounded retry controls, and logical backlog metrics |
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
