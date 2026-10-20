# Dedicated Artifact Buckets

**Use this when:** changing the creation, persisted identity, or versioning of a
bucket reserved for plugin outputs.

## Identity and creation

`MakeBucketOptions::artifact_base_prefix` in
`crates/storage-api/src/bucket.rs` selects a dedicated artifact bucket at creation.
The bucket incarnation and prefix identify its output namespace. An existing
ordinary bucket cannot be converted into an artifact bucket, and an existing
artifact bucket's prefix cannot be removed or changed. Deleting and recreating a
bucket creates a different incarnation; a bucket name alone is not an identity.

The prefix follows the Runner's artifact-store encoding: at most 1024 bytes,
ASCII letters, digits, `/`, `.`, `_`, and `-`, with a trailing slash and no `.` or
`..` path segment. It is a key prefix, not a filesystem path. Runner's final
attempt prefix must also fit its own length limit.

`ECStore::handle_make_bucket` in `crates/ecstore/src/store/bucket.rs` uses the
existing bucket lifecycle, metadata transaction, and namespace locks. Before
creating the physical bucket it persists the incarnation and creation intent,
including the artifact prefix and enabled versioning. The creation is then
committed through the existing bucket creation protocol. A retry before physical
creation preserves the pending identity instead of falling back to ordinary
bucket creation. If only some disks contain a bucket volume, the existing
incomplete-generation guard requires administrator recovery; this change does
not bypass that guard to finish creation automatically.

On each local disk, volume creation and deletion take the write side of the
existing volume publication lock. Physical filesystem workers retain this
lock after their caller is cancelled, including deferred snapshot cleanup.
Their last owner invalidates volume and file-descriptor caches before releasing
the lock. Linux workers also retain the original mount descriptor; Windows
directory deletion closes its handle before releasing the owner. This local
ordering does not establish the distributed legacy-write admission boundary.

The legacy `WriteAll` and `CompareAndUpdateFile` receivers take a disk-bound
volume read owner before reading authoritative bucket metadata. Ordinary buckets
must still have a live directory on that disk; retained metadata cannot recreate
a missing volume. Their filesystem workers keep the owner through completion,
even after cancellation. Internal volumes retain on-demand directory creation.
The ordinary `WriteMetadata` receiver also prepares its object metadata mutex,
final `xl.meta` namespace mutex, and volume read lock before classification. It
reuses those exact locks for temporary-file writes and final publication; queued
bucket deletion cannot force a second read acquisition. Preparation and execution
retain finite deadlines, while cancelled physical workers keep their ownership.
Ordinary `UpdateMetadata` reuses that prepared publication for its read-modify-write
operation, preserving metadata merge/replacement and durability options. Transition
reconciliation retains its separate authorization path and is not covered by this
admission.
The HTTP file-stream receivers serialize requests for the same target before
admitting the captured disk and live volume. Classification follows admission;
the stream then fixes its target and optional staging paths under namespace
ownership. Authenticated writes verify the staged body before publication, while
legacy writes retain their direct append/truncate behavior. Physical stream
workers, publication and failure cleanup retain the same ownership, including
the HTTP target guard, after the request waiter is cancelled.
Ordinary `RenameData` similarly prepares a single-use disk-bound transaction
before classifying either volume. It acquires the destination metadata and
namespace mutexes, then distinct source and destination volume read locks in
path order. Both ordinary volume directories must still exist. Commit consumes
the sanitized metadata and the same leases, including the quota claim and
captured instance owner; staging, publication and source restoration keep that
ownership through physical completion even after cancellation. Internal staging
retains on-demand directory creation. Preparation and execution have finite
deadlines, and scanner/incarnation-bound requests retain their existing paths.
Ordinary `UndoWrite` prepares metadata admission on that same captured instance
before classification. Backup restoration, remaining-version rewrites and fresh
version rollback retain the admission through physical completion. A single-object
`UndoDelete` with both undo flags and a rollback directory also uses this admission.
Before its first mutation, it freezes the restoration paths and tries all required
extra namespace locks; contention fails before any marker or object changes.
Physical restoration workers retain the complete lock ownership after cancellation.
Optional rollback cleanup skips lock contention. Incarnation-bound deletion keeps
its separate authorization path.
Ordinary `DeleteVersion` without rollback staging uses the same admission. It
covers remaining-version rewrites, final-version removal, and forced delete
markers when metadata is missing. These operations reuse the prepared publication
instead of reacquiring namespace or volume locks. Missing metadata without a
forced marker preserves the original error and leaves optional residue cleanup
to the existing unprepared cleanup path.
Single-object staged deletion also prepares admission before classification. It
freezes the backup, marker, staging and restoration paths, then tries every required
namespace before its first write. Backup publication, reservation, marker commit
and internal failure restoration retain the same complete ownership. Internal
restoration reuses the prepared paths and locks instead of acquiring them again;
optional cleanup remains best effort.
Bulk `DeleteVersions` captures one instance-bound volume admission before
classification. Each object group tries its metadata and namespace locks, then
uses the existing group read-modify-write operation and one original backup.
Contention is reported for that group without blocking later groups. Duplicate
objects retain input order; empty groups keep their existing mutation semantics.
One execution deadline preserves completed group results and marks only unfinished
groups as timed out. Physical workers retain admission after cancellation. Empty
batches preserve their original classification and response behavior.
Ordinary and durable `RenameFile` requests bind both source and destination
volumes to the captured instance before classification. Destination cleanup,
rename, optional ancestor cleanup and required sync workers retain the same
admission until physical completion. Durable acknowledgement follows successful
completion of the destination volume's existing sync policy; a post-rename sync
failure still reports failure even if the destination has already been published.
Part preparation, publication and settlement share the destination part's
namespace admission before acquiring distinct volume read owners. Every required
extra path, including integrity indexes, is frozen before the first write.
Settlement waits for the same destination admission before inspecting the
transaction, so rollback cannot race a timed-out publisher's physical worker.
The existing transaction files and recovery decisions remain authoritative;
a timed-out settlement leaves recoverable state instead of claiming completion.
Admission failures and consumed operations retain their original operation
metrics without counting a successful write twice.
`Delete` and `DeletePaths` bind one volume admission to the captured instance
before classification. Each item tries all required namespaces before its first
mutation, including the encoded-directory cleanup target. Batches retain input
order and stop at the first error without undoing completed items. Scanner
ownership comes from the same instance and remains with the physical workers.
The two residual `DeleteVersion` undo combinations reuse prepared metadata
deletion rather than bypassing admission.

Snapshot-deferred cleanup releases its initial admission while readers remain.
The final snapshot release reacquires volume admission and checks the pending
claim under that ownership before deleting. Claim completion retains the same
owner, so an old cleanup cannot remove a recreated volume's registry entry.
Bucket deletion settles pending cleanup under its existing write owner and tries
required namespaces without waiting for a competing volume reader. Successful
physical volume deletion retires old snapshot state before releasing that owner,
including when the caller has already been cancelled.
The incarnation-bound heal path retains its separate admission. Internal bucket
metadata continues to use its existing owned publication transaction. These
boundaries complement the receiver-side artifact PUT leases; they do not enable
distributed repair or garbage collection.

Inline `RenameData` retains the backup namespace mutex during rollback-backup
publication. Because it already owns a volume read lock, it acquires this second
namespace only without waiting: contention returns `Io(WouldBlock)` through the
existing failure cleanup, leaving the old object metadata intact. This prevents
a competing rename and queued bucket deletion from forming a lock cycle while
preserving exclusion against a delayed backup unlink. The caller may retry after
contention clears; this path does not add automatic retries.

## Persistence and versioning

`BucketMetadata` in `crates/ecstore/src/bucket/metadata.rs` stores the prefix in
the bucket metadata record. Missing fields in older records mean ordinary
buckets; malformed or duplicate artifact identity fields are errors.

Artifact buckets require enabled versioning. Configuration updates cannot
suspend or disable it. Full metadata replacement must enforce the same namespace
and versioning invariants; checking only the versioning configuration handler
would leave another mutation route open. Ordinary configuration changes retain
the artifact identity.

Upgrade all nodes to a version that understands these invariants before creating
artifact buckets. Older binaries may ignore or overwrite the new metadata field;
mixed-version operation and downgrade after activation are not supported by this
contract. Existing ordinary buckets retain their creation semantics.

## Boundary with write sealing and garbage collection

This storage-library creation capability does not expose an S3 creation header
or establish a permanently sealed attempt. An artifact prefix alone does not
prevent later PUT, multipart completion, copy, replication, or restore writes.

`ECStore::seal_artifact_attempt_puts` in `crates/ecstore/src/bucket/artifact.rs`
provides a narrower storage-library operation for ordinary owned ECStore PUTs
on local stores and Unix distributed stores with URL endpoints.
The record binds the bucket incarnation and full attempt prefix. Only a missing
record permits a PUT; a matching record rejects it, and malformed records or
failed reads also reject it. Repeating the seal verifies the same identity.

Artifact bucket creation, inspection, ordinary PUT and PUT sealing use the
configured storage topology and require namespace locking. Distributed operation
requires URL endpoints and Unix physical disk leases on every receiver; merely
changing the setup mode of local file endpoints does not enable it. Repair and
repair sealing use the same supported topology; version retirement remains
local-only. This does not enable distributed garbage collection. The legacy RPC
boundary retains its admission
owner through physical completion, including cancellation and bucket recreation.
A remote timeout, quorum response or expired lock lease cannot prove that a
physical writer stopped.

Each managed PUT now carries its metadata-derived identity into the physical
local disk writer. On Unix, a descriptor-pinned directory under
`.rustfs-artifact-put-seals/<identity digest>/` contains two permanent lock inodes
and a closing marker. The stable digest binds bucket name, incarnation and full
attempt prefix. The admission lock serializes checking the marker with obtaining
a shared active-write lease. Both the mutation and its blocking filesystem
workers retain that lease. Sealing durably writes the marker and syncs all parent
entries before waiting for an exclusive active-write lock. Cancellation during
this wait loses only the confirmation; subsequent admission remains denied.
These files must not be deleted, replaced or omitted during disk restoration.
Non-Unix platforms reject this disk-fence operation.

The dedicated `ArtifactRenameData` and `SealArtifactAttemptPuts` RPCs require
body-bound authentication and a ready listener-owned storage instance. Their
receivers validate the bucket incarnation; seal requests also bind the expected
physical disk UUID. Seal response proofs cover the complete request, actual disk
identity and outcome. Old peers cannot be used through a legacy-method fallback.
The coordinator requires every configured disk, including offline slots, and
rejects missing or mismatched identities, including on retries after the seal
record already exists. Ordinary PUT retains its write quorum; successful sealing
requires confirmation from every configured receiver.

The PUT commit path takes locks in this order: bucket lifecycle, attempt record,
then target object. Same-attempt commits are serialized; uploading a body does
not hold the attempt lock. The actual commit and rename-tail owner retains the
attempt guard after a quorum response or cancellation of the request waiter.
The seal takes the same attempt lock and persists its record with all metadata
writes drained before returning. Thus it waits for admitted ordinary PUTs and
prevents later ordinary PUT publication, including after a restart.

The four-node scenarios in
`crates/e2e_test/src/distributed/artifact_seal_test.rs` exercise cross-node
versioned PUT/GET, full restart, and exact seal retries with an offline receiver,
including retries after a successful seal. Other attempts in the managed bucket
and ordinary buckets remain writable. After a receiver restarts, HTTP readiness
does not imply that peers have re-established its disk connection; retry the
same seal identity until a successful receipt confirms every receiver.

`crates/e2e_test/src/distributed/artifact_put_tail_test.rs` sends a direct signed
ordinary PUT receiver request without a coordinator attempt lock. It pauses the
actual filesystem worker, disconnects the RPC, observes server-side waiter
cancellation, and requires seal refusal until physical completion. Re-signed
requests remain rejected both while draining and after confirmation, without
changing staged or published bytes. Exact-version shard census and GET verify
the completed publication on all four nodes. This test requires the
`e2e-test-hooks` server feature. RPC cancellation does not cancel the admitted
PUT transaction: it may finish metadata publication while retaining its owner.
The first successful seal confirmation is the boundary after which the test
requires published files to remain unchanged.

These scenarios do not inject power loss, disk replacement, or backup
restoration. Successful ordinary PUT sealing is not a repair seal, version
retirement, or GC authorization.

A failed or cancelled seal request may still leave a record for its original
incarnation. Retry idempotently to confirm success; an error is not proof that
the attempt remains writable or that all metadata writes have been cleaned up.

The managed ECStore entry points reject unsupported writes into an artifact
bucket: borrowed-lock and special-purpose PUTs, copy destinations, multipart
creation/parts/completion, data movement, restore, transition, metadata rewrites,
and tag changes. Ordinary single, batch, and exact-version deletion are also
rejected, including delete-marker creation and removal, before any local disk
can stage metadata or rollback state. This applies both at ECStore and public
SetDisks entry points; it does not authorize retirement or GC. Copying from an
artifact bucket to an ordinary bucket remains allowed. Listing and aborting
existing multipart uploads remain available for
cleanup. Classification is bound to the authoritative bucket incarnation, so an
old ordinary-bucket request cannot publish into a recreated artifact bucket.
The guarded entry points only borrow lifecycle authority from a matching snapshot
that retains the actual lock owner. A standalone lock-loss signal or a snapshot
without that owner is rejected; internal callers must supply the owned snapshot
or let the entry point acquire its own lifecycle lock.

Lower-level SetDisks PUTs also resolve authoritative bucket metadata and require
a matching managed target for an artifact object. Missing metadata authority is
an error, not evidence that a bucket is ordinary. Internal callers borrowing a
lifecycle fence must retain its actual owner through physical publication.

Deletion preserves that owner in the existing Object Lock snapshot. Detached
prefix deletion transfers its owned lifecycle lock, rather than borrowing only
a lock-loss signal. The internal free-version cleaner passes its existing lock
owner explicitly, avoiding a second read acquisition behind a queued bucket
writer. It still rejects managed buckets. Transaction-private rollback cleanup
keeps its existing admission contract.

`crates/e2e_test/src/distributed/artifact_delete_test.rs` exercises single and
mixed batch deletion through all four nodes before sealing, after sealing, and
after restart. It includes persisted delete-marker state and ordinary-bucket
success controls. Rejected requests must preserve file bytes and filesystem
identity/change times, so a local mutation followed by rollback is not treated
as a successful rejection.

Object repair takes the same attempt lock before capacity and object locks.
Local rename and metadata-replacement workers retain the lifecycle and attempt
owners until the physical mutation completes, even if the async waiter is
cancelled or times out. On distributed Unix URL topologies, dedicated
`ArtifactRepairRenameData` and `ArtifactRepairWriteMetadata` RPCs bind the
physical disk UUID, bucket incarnation, target and both FileInfo payloads.
The receiver captures its own ready storage instance and retains an independent
repair lease and exact-version retirement guard through physical mutation and
rollback. It does not reacquire the
coordinator's attempt lock or fall back to legacy RPCs on an older peer.

Repair leases use `.rustfs-artifact-repair-seals`, independent of the existing
PUT lease directory. Preserve both directories during disk restoration.
Distributed managed repair rejects destructive cleanup that lacks equivalent
receiver ownership; optional orphan cleanup is skipped. Ordinary-bucket repair
keeps its existing behavior. Repair of an existing version remains allowed after
PUT sealing; it does not reopen logical publication. Ordinary-bucket repair also
retains lifecycle ownership to prevent publication into a recreated bucket.

`ECStore::seal_artifact_attempt_repairs` separately closes repair admission for
the entire attempt, including existing versions and minority metadata repair.
It requires an ordinary PUT seal first. Under lifecycle, attempt, then repair
record locks, it persists an immutable record bound to the incarnation and full
attempt prefix, then closes and drains each configured disk's repair domain.
`SealArtifactAttemptRepairs` has its own request and response proof domains.
Confirmation requires every configured receiver with its expected physical
UUID; quorum is insufficient. Retries repeat this check even when the global
record already exists. An offline member prevents successful confirmation. New repair
requests reject a matching record; corrupt, mismatched, or unreadable records
also fail closed. Retrying confirms the same identity, including after restart.
Existing bytes remain readable while sufficient healthy shards remain. Other
attempts retain their repair behavior.

Repair sealing is irreversible through this API and reduces recoverability:
do not apply it automatically to successful outputs that still need repair.
It is a repair fact, not an all-writers or deletion authorization. Do not
downgrade to a binary that ignores repair seals after using this operation.
Restoring pre-seal storage can rewind the decision and requires reconciliation
before relying on the seal again; records must not expire by age.

`ECStore::retire_artifact_object_version` disables repair of one exact version
inside an already sealed attempt. Retirement still requires local topology.
It requires the current bucket incarnation,
an object strictly inside the attempt prefix, and a non-null version. Retirement
does not delete data: existing bytes remain readable, and other versions remain
repairable. Directory objects use the same canonical identity as storage.

Retirement takes lifecycle, attempt, then record locks. It waits for earlier
physical repair publishers, including workers whose callers were cancelled.
The record transaction retains its owners after request cancellation; retrying
the same identity confirms the durable outcome. An immutable record binds the
bucket incarnation, attempt, object, and version. Unreadable, corrupt, or
mismatched records fail closed. Normal repair and minority metadata regeneration
check the selected version before publication, including when the caller asks
for the latest version. Refusal is not a physical absence proof.

PUT sealing, repair sealing, and version retirement remain separate facts.
Runner must not combine independently obtained narrow receipts into an
all-writes claim. Distributed exact-version retirement still requires
implementation and verification. Retirement records must not expire by age;
restoring storage from before a retirement can rewind that decision and requires
a separate recovery contract.

Before Runner can rely on a seal, storage must fence every supported write path
for the exact bucket incarnation and attempt namespace, retain the fence through
commit and any remaining physical writes, and durably record the seal. Unsupported
write paths must reject writes into the managed namespace. HTTP cancellation or
a cached metadata value cannot substitute for that storage boundary.

Automatic garbage collection remains disabled until storage sealing and the
backup/restore retention contract are both implemented and verified. Unknown
backup/PITR coverage must retain artifacts; a completed plugin execution does not
prove that historical results are safe to delete.

## Combined write sealing

`ECStore::seal_artifact_attempt_writes` closes both supported publication domains
for one managed bucket incarnation and attempt. It persists the existing PUT
and repair records under lifecycle, attempt, then repair-record ownership. It
does not introduce a third record or change either narrow operation's meaning.

Both domains use one captured set of configured endpoints, disk instances, and
non-nil disk UUIDs. Every configured receiver must participate; quorum is not
enough. Local confirmation reads the actual format through the captured disk
root, bypasses the ordinary ID cache, and rejects a changed configured root.
Remote confirmation uses the existing signed, disk-bound seal RPCs. Membership
and physical identity are rechecked before success. A partial failure leaves
durable closure in place; an exact retry must confirm both domains again.

Upgrade every configured node to an implementation of the fresh identity and
shared-root checks before using combined sealing. Mixed-version operation is
unsupported. These existing RPCs do not negotiate that capability: a peer with
the older seal implementation can still return a valid signed response. Peers
without the RPC fail without fallback, but that is not a version gate.

```mermaid
sequenceDiagram
    participant C as Controller
    participant S as Storage coordinator
    participant R as Every configured receiver
    C->>S: seal-writes(version, target)
    S->>S: Own lifecycle and attempt; capture disk IDs; persist PUT record
    S->>R: Close and drain PUT on the captured receivers
    S->>S: Own and persist the repair record
    S->>R: Close and drain repair on the same receivers
    R-->>S: Durable closure and physical drain confirmed
    S->>R: Recheck membership and physical identity
    S-->>C: 200 only after all confirmations
```

The receipt covers the managed profile: ordinary PUT and repair use their
respective domains; unsupported mutation paths reject the bucket. It is not a
cross-disk atomic snapshot, a backup/restore guarantee, or permission to delete
versions. A failed or cancelled caller may leave one or both domains closed;
only a successful exact retry confirms completion. As with repair sealing,
closing an attempt's writes also removes its future repair capability.

## Local sealed-attempt inventory

`LocalDisk::inventory_artifact_attempt` is a crate-private, single-disk physical
candidate census. Its caller supplies the validated managed bucket, incarnation,
attempt prefix and existing `ArtifactPutIdentity`. It opens both existing PUT
and repair closure domains read-only, validates their closed markers, then holds
the actual active-file drain locks in PUT-to-repair order. A durable closed
marker alone does not prove that earlier physical owners have finished. The
scanner never creates a seal or removes residual data.

The scanner retains the initialized root descriptor and both drain owners in its
blocking worker, including after its async waiter is cancelled. It checks fresh
physical disk identity before scanning and before the terminal frame. It walks
all versions directly from strict `xl.meta` decoding, preserves UUID-shaped
namespace children beside referenced data directories, and accounts for known
part files. Unknown residue, corrupt or unsupported metadata, missing referenced
parts, symlinks, identity changes, deadlines and resource limits fail without a
`Complete` frame. This does not verify every shard's content checksum.

Output is one invocation with bounded entry frames and sequence numbers. The
4096-entry traversal quantum only yields work; it is not a total entry limit.
Depth, active handles, metadata size, pending references and frame size are
bounded. Backpressure and cancellation cannot turn a partial stream into
success. Consumers require both the explicit terminal frame and successful
operation completion; ordinary EOF is insufficient. An absent attempt prefix
inside the verified bucket yields an empty census; a missing bucket or disk root
fails. The logical key limit applies to actual metadata objects after decoding
one directory suffix, not to their longer physical payload-directory paths.

The local primitive alone does not expose an HTTP or RPC inventory interface, persist a
Controller inventory, or establish all-member completeness, quorum readability,
backup coverage or permission to collect garbage. Automatic GC remains disabled.

## Verification

`crates/ecstore/src/store/init/tests/artifact_bucket_tests.rs` uses real four-disk
ECStore instances for creation, restart, pending creation retry, configuration
updates, metadata replacement, and versioning rejection. Direct libtest runs
use the same 4 MiB thread stack as the repository's ecstore nextest setup:

```sh
export RUST_MIN_STACK=4194304
cargo test --locked -p rustfs-storage-api --lib artifact
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_bucket -- --test-threads=1
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_put_seal -- --test-threads=1
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_write_entrypoint -- --test-threads=1
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_repair_tests -- --test-threads=1
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_retirement -- --test-threads=1
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_writes_seal -- --test-threads=1
```

`crates/ecstore/src/store/init/tests/artifact_put_seal_tests.rs` pauses real
rename work to check the PUT barrier, including a cancelled request waiter, and
compares on-disk object metadata after rejected writes. It also checks restart,
record corruption, generation mismatch, and ordinary bucket behavior.

`crates/ecstore/src/store/init/tests/artifact_write_entrypoint_tests.rs` checks
unsupported entry points against real four-disk state, ordinary-bucket success
controls, cleanup of preexisting multipart uploads, and deletion/recreation
between request classification and publication.

`crates/ecstore/src/store/init/tests/artifact_repair_tests.rs` exercises direct
PUT admission, sealed-version repair and stale-incarnation rejection on four
real disks. Physical rename and metadata-replacement pauses test that cancellation
cannot release the attempt barrier before publication finishes. The rename case
also drops the repair future at a caller deadline; its guarded disk wrapper
deliberately has no timeout. The metadata case uses public SetDisks repair to
trigger real disk-wrapper timeouts, checks per-disk timeout counters, and verifies
that the physical publishers still retain the attempt barrier.

`crates/ecstore/src/store/init/tests/artifact_retirement_tests.rs` uses real
four-disk storage to check exact-version and latest repair refusal, minority
metadata regeneration, restart, record corruption, directory normalization, and
unretired-version repair. Physical syscall pauses cover both cancelled repair
workers and cancelled retirement publication. Retirement leaves GET available
and never supplies evidence that the version is physically absent.

The narrow-operation checks establish bucket identity, versioning, ordinary PUT sealing,
attempt repair sealing, and exact-version repair retirement. Repair-seal tests
also cover restart, corrupt records, other-attempt isolation, and cancelled
rename/metadata workers whose physical writes must drain before confirmation.
They do not certify all-writes sealing, backup retention, or safe artifact
deletion.

`crates/ecstore/src/store/init/tests/artifact_writes_seal_tests.rs` checks the
combined operation with real storage, including partial closure, exact retries,
record corruption, missing members, and changed disk identity without waiting
for the ordinary format cache to expire. The four-node scenario in
`crates/e2e_test/src/distributed/artifact_writes_test.rs` checks combined
confirmation, offline refusal, restart, and continued PUT/repair rejection.
Its Object Lock requests first enable and read back bucket configuration, so
retention and legal-hold refusal reaches the object mutation path. The admin
handler tests similarly use real historical multipart staging when exercising
the S3 UploadPartCopy entry point.

The distributed repair scenarios in
`crates/e2e_test/src/distributed/artifact_repair_test.rs` verify exact-version
shard and minority metadata recovery after PUT sealing, repair refusal across
restart, offline-member seal retries, and preservation of dangling metadata.
`artifact_repair_tail_test.rs` pauses a real receiver filesystem worker after a
direct authenticated repair RPC. It waits for receiver-side cancellation before
sealing, requires refusal while the worker remains paused, then verifies physical
recovery and successful seal retry. This test requires the `e2e-test-hooks`
server feature; the barrier is absent from production builds.

```sh
python3 scripts/e2e_binary.py build --features e2e-test-hooks
python3 scripts/e2e_binary.py run --features e2e-test-hooks -- cargo test -p e2e_test distributed::artifact_repair -- --test-threads=1
```

## Controller API

The admin router exposes the following SigV4-authenticated POST operations:

- `/rustfs/admin/v3/plugin-artifacts/buckets`, requiring
  `admin:CreatePluginArtifactBucket` on the bucket ARN.
- `/rustfs/admin/v3/plugin-artifacts/buckets/inspect`, requiring
  `admin:GetPluginArtifactBucket` on the bucket ARN.
- `/rustfs/admin/v3/plugin-artifacts/seal-attempt`, requiring
  `admin:SealPluginArtifactAttempt` on the attempt namespace.
- `/rustfs/admin/v3/plugin-artifacts/seal-repairs`, requiring
  `admin:SealPluginArtifactRepairs` on the attempt namespace.
- `/rustfs/admin/v3/plugin-artifacts/seal-writes`, requiring
  `admin:SealPluginArtifactWrites` on the attempt namespace.
- `/rustfs/admin/v3/plugin-artifacts/retire-version`, requiring
  `admin:RetirePluginArtifactVersion` on the exact logical object key.
- `/rustfs/admin/v3/plugin-artifacts/inventory`, requiring
  `admin:InventoryPluginArtifactAttempt` on the complete attempt namespace.

All require a permanent, non-root original IAM user and an installed request
server context. Temporary credentials and service accounts are rejected. The
initial profile requires built-in IAM; external authorization plugins cannot
establish its explicit local Resource contract. No separate signer map is used.
The controller grant is independent of an execution delegation's expiration.

### Sealed-attempt inventory stream

The inventory POST accepts `version: 1`, a non-nil UUID `invocation_id`, and
`target` with the same complete `bucket`, `bucket_incarnation`, and canonical
`attempt_prefix` identity as the seal requests below. Unknown fields and
ambiguous paths are rejected. Its independent action must have an explicit
matching Resource; seal actions, ordinary S3 listing and another attempt's
grant do not authorize inventory. The permanent original-user, built-in IAM
and request-context requirements above apply unchanged.

A successful request starts an `application/x-ndjson` body with `Cache-Control:
no-store`. Each newline-terminated JSON frame repeats `version: 1`, the supplied
`invocation_id`, full `target`, and a continuous zero-based `sequence`.
Its tagged `payload` is one of:

- `{"type":"observation","disk_id":"<non-nil UUID>","key":"<complete logical key>","version_id":"<non-nil UUID>","delete_marker":false}`.
- `{"type":"complete","members":4,"observations":12}`.

The counts above are examples. Observations retain their physical disk identity;
replicas are not additional business versions or a quorum-readability proof.
An empty attempt has no observation frames, but still requires every configured
member and the complete terminal. Internal endpoints and physical paths are
never projected to this wire.

The adapter uses bounded channels and caps each actual encoded line, including
its newline, at 1 MiB. One absolute 30-second deadline covers authorization,
request decoding, all-member inventory, backpressure and output. The existing
16 KiB request bound and 10-second body-read bound also apply. A stalled body
consumer cannot extend that deadline. Disconnecting the HTTP body cancels the
coordinator future; existing physical workers retain their own owners until
they actually exit.

Complete is staged until I2 has both returned success and ended its input
normally. Late storage failure, timeout, corruption or changed membership can
only abort/close the body without Complete; internal errors are replaced with
a generic stream failure. Authentication and validation errors before streaming
use the existing AccessDenied, InvalidRequest or ServiceUnavailable responses.
HTTP 200 and EOF alone do not establish completeness. Consumers must stage all
observations and validate identity, sequence, counts, one final Complete and
clean EOF before accepting the result; a transport error after Complete also
invalidates that result. Never automatically replay a partially emitted stream.

This endpoint uses the existing read-only I2 authority and closure checks. It
creates no seal/identity record, repairs no metadata and changes no object,
bucket or IAM configuration. It neither persists a Controller inventory nor
grants GC, backup retention or deletion permission. Old servers without this
route fail instead of falling back to ordinary listing.

### Sealing requests

All three seal operations use this request:

```json
{
  "version": 1,
  "target": {
    "bucket": "artifacts",
    "bucket_incarnation": "bdbf4d55-7ae3-42f1-a8ea-88cb2836d35d",
    "attempt_prefix": "out/61/8694133a-8a38-4911-b2cc-614556a852a9/301c4039-9739-4a70-a88f-6d83cf721509/"
  }
}
```

Retirement uses the same fields plus `object` (the complete logical key below
that attempt) and `version_id` (a non-nil UUID). Unknown fields are rejected at
both levels. Bodies are limited to 16 KiB and a 10-second read deadline.
The storage call has a 30-second caller deadline.

IAM uses the existing S3 resource grammar, including its path normalization:
the namespace's trailing slash is removed during matching. An exact attempt
controller policy can therefore use these statements (grant repair sealing
separately only to the component making that irreversible decision; the combined
operation likewise needs its own `admin:SealPluginArtifactWrites` grant):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "admin:SealPluginArtifactAttempt",
      "Resource": "arn:aws:s3:::artifacts/out/61/8694133a-8a38-4911-b2cc-614556a852a9/301c4039-9739-4a70-a88f-6d83cf721509"
    },
    {
      "Effect": "Allow",
      "Action": "admin:SealPluginArtifactRepairs",
      "Resource": "arn:aws:s3:::artifacts/out/61/8694133a-8a38-4911-b2cc-614556a852a9/301c4039-9739-4a70-a88f-6d83cf721509"
    },
    {
      "Effect": "Allow",
      "Action": "admin:RetirePluginArtifactVersion",
      "Resource": "arn:aws:s3:::artifacts/out/61/8694133a-8a38-4911-b2cc-614556a852a9/301c4039-9739-4a70-a88f-6d83cf721509/*"
    }
  ]
}
```

An Allow with neither Resource nor NotResource does not grant these artifact actions.
Explicit unscoped Deny, including `admin:*`, remains effective. Existing admin
and table/KMS action semantics are unchanged. A single-object grant cannot seal
its containing attempt. IAM scopes the logical key, not individual versions;
exact bucket generation and version identity are validated by storage.

Only a completed storage operation returns HTTP 200 with `Cache-Control: no-store`.
PUT seal returns `version`, `target`, and `ordinary_put_sealed: true`. Repair seal
returns `version`, `target`, and `attempt_repairs_sealed: true`. Combined sealing
returns `version`, `target`, `ordinary_put_sealed: true`,
`attempt_repairs_sealed: true`, and `all_writes_sealed: true`. The two narrow
permissions do not grant access to the combined operation. Retirement
returns `version`, `target`, `object`, `version_id`, and
`exact_version_repair_retired: true`. These are idempotent confirmations of
specific storage facts. Only the combined operation attests to both write
domains on a common receiver set. No receipt attests that data was deleted or
that GC is permitted.

Malformed requests return InvalidRequest; authentication/resource denial returns
AccessDenied. All storage failures and caller deadlines return a sanitized
ServiceUnavailable response: `artifact operation unconfirmed; retry the same identity`.
This deliberately does not reveal internal record paths or distinguish durable
record corruption from unavailable storage. It also does not mean the operation
was rolled back: the cancellation-safe storage transaction may still complete.
Retry the identical target to obtain confirmation; operators must investigate
persistent failures. Version retirement still rejects distributed topologies.
Ordinary PUT sealing and repair sealing use their separate receiver confirmations
described above.

Bucket creation does not change STS vending. Existing v1
vending requires input and output to share the delegation's bucket;
cross-bucket output credentials need a separate change. Bucket creation alone
does not authorize cleanup or establish backup retention; those require the
explicit protocols below.

### Dedicated bucket creation

POST `/rustfs/admin/v3/plugin-artifacts/buckets` with:

```json
{"version":1,"bucket":"plugin-artifacts","base_prefix":"outputs/"}
```

The controller needs an explicit `admin:CreatePluginArtifactBucket` Allow on
`arn:aws:s3:::plugin-artifacts`. An object-only ARN such as
`arn:aws:s3:::plugin-artifacts/*` does not authorize bucket creation. The same
original-user, built-in IAM, request-instance and body/deadline checks apply.
Unknown fields and invalid bucket names or base prefixes are rejected.

The storage helper requires a supported configured topology (local storage or
Unix distributed URL endpoints) and enabled namespace locking. It invokes the
existing creation transaction with an
immutable artifact namespace and versioning enabled. It does not change the
type of an existing bucket or turn an existing-bucket failure into success.
`BucketExists` maps to `BucketAlreadyExists`; other unconfirmed storage outcomes
retain the sanitized ServiceUnavailable response described above.

After creation returns, the helper acquires the lifecycle read lock and reads
one authoritative metadata snapshot. The namespace, non-nil incarnation and
full versioning configuration must match; excluded folders or prefixes are not
allowed. Success returns HTTP 200 with `version`, `bucket`, `base_prefix`,
`bucket_incarnation`, and `versioning_enabled: true`.

This receipt identifies the current managed generation observed under the
lifecycle lock. It does not assert that this request created that generation:
a legitimate deletion/recreation between creation and observation may produce
a newer matching generation. Every later seal or retirement operation must
still supply and validate the receipt's incarnation. A cancelled or timed-out
creation may have changed durable state; retrying it can return an existing-bucket
error and is not a substitute for confirming identity. No GC authority follows.

The `artifact_bucket_create_*` handler tests reuse the four-disk S4 fixture and
real IAM. They cover creation and persisted identity, existing-bucket failures
without changing type, bucket-resource authorization, request-instance isolation,
rejection of file endpoints mislabeled as distributed before creation, and actual delete/recreate with
old-receipt rejection. The handler recreation test performs two completed creations with the same
prefix, and checks both rejection of the old incarnation and acceptance of the
current one. The storage test `artifact_bucket_create_observes_current_generation`
performs real replacement between creation and the same internal observation
function consumed by the public helper, then reopens storage and verifies the
persisted identity. This is directed coverage of the production observation
phase, not an injected HTTP handler race.

`artifact_bucket_create_observation_rejects_corrupt_authority` checks the
observation's actual authoritative-cache path with damaged name, namespace and
versioning data, plus physical metadata and nil-sidecar corruption through fresh
store instances. The physical cases assert the specific disk parsing error, then
restore the record and reopen again to verify the original identity. The
authoritative snapshot may already be cached from a committed write; observation always holds the lifecycle read lock but does not acquire
metadata and bucket locks on every cache hit. Removing its identity/versioning
validation must fail `ARTIFACT_CREATE_CURRENT_GENERATION` at runtime.
`artifact_bucket_create_requires_namespace_locking_before_creation` uses a
real lock-disabled instance and verifies no physical bucket is created. Local
fast-lock guards cannot lose distributed quorum; this local-only profile does
not claim to inject distributed guard loss. Policy coverage is
`artifact_bucket_create_resource_requires_exact_bucket_authority`.


### Existing bucket identity inspection

POST `/rustfs/admin/v3/plugin-artifacts/buckets/inspect` accepts the same strict
`version: 1`, `bucket`, and `base_prefix` body as creation. It requires its own
`admin:GetPluginArtifactBucket` Allow on `arn:aws:s3:::plugin-artifacts`;
creation permission does not authorize inspection. Original-user and
request-instance checks are unchanged. An object ARN cannot authorize this
bucket operation, and explicit Deny still takes precedence.

Inspection requires local storage and namespace locking. Under the lifecycle
read lock, the existing metadata loader returns one snapshot without performing
legacy incarnation migration or completing a pending creation. The snapshot
must have a committed creation, a non-nil sidecar-backed incarnation, the exact
artifact prefix, and fully enabled versioning without folder or prefix
exclusions. Ordinary buckets, missing buckets, pending intents and mismatching
prefixes fail; the query does not create buckets or write metadata. Unsupported
or unconfirmed storage outcomes return sanitized HTTP 503. They do not mean the
bucket is absent, safe to recreate, or safe to delete.

The HTTP 200 receipt has the same fields as creation. A restarted controller
can observe the current managed incarnation without creating another bucket.
It must compare that identity with its accepted configuration; a matching name
and prefix do not establish continuity across deletion/recreation. Subsequent
seal and retirement calls must carry this exact incarnation and revalidate it.
The response is a current observation, not a permanent seal or GC permission.
Runner persistence and automatic reconciliation are not implemented by this API.

Three `artifact_bucket_inspect_*` handler tests reuse the real IAM/four-disk
fixture for identity and recreation, resource and request-instance isolation,
and rejection without retyping or creation. They dispatch the internal handler,
not a signed network request. The storage tests reopen the store before reading,
assert that a real pending intent's bytes stay unchanged, and verify that legacy
volumes and missing buckets gain no metadata or incarnation records. Namespace
locking and rejection of file endpoints mislabeled as distributed are covered
independently.


## Authenticated all-member inventory stream

`ECStore::inventory_artifact_attempt` observes one attempt under lifecycle READ,
then the existing PUT-attempt and repair-record owners. Both immutable seal
records must already match; inventory creates neither records nor disk closure.
The coordinator captures every configured endpoint, exact Disk Arc and expected
UUID once. Every member, including an empty member, must successfully contribute
a terminal scan result. Observations retain endpoint and disk UUID; replica
duplicates remain observations, with no unbounded global version set or quorum
readability claim. Earlier frames are incomplete staging until global Complete
and the operation's successful return.

`InventoryArtifactAttempt` protocol version 1 uses a fresh invocation UUID and
binds endpoint, expected physical UUID, bucket, incarnation, attempt prefix,
time budget and explicit Scan/Verify phase into the canonical v2 request and
every response proof. Entries, exact version UUIDs, delete-marker flags, sequence,
terminal count and actual disk UUID are authenticated. Wire frames are limited to
256 entries and 64 KiB of actual protobuf encoding, including response proof.
The client enforces contiguous sequence, scope, counts and distinct phase
terminals, and withholds scan Complete until clean EOF. Truncation, post-terminal
frames and terminal-followed-by-transport-error are incomplete. Old peers that
lack this RPC fail; there is no fallback to List or Walk EOF.

Verify is a separate, signed `Verified` terminal; it is never a zero-entry scan
Complete. After scans, the coordinator uses this read-only phase to freshly
recheck each remote physical receiver and both existing drained domains while
other members may have taken longer. Local verification retains the same pinned
root and two-domain owners. The captured member set/Arc and expected IDs, and
actual persisted bucket metadata/incarnation, are checked again before global
completion. These are conservative rechecks, not an atomic snapshot across all
disks or a backup/restore guarantee.

Receiver authority uses bounded `read_config_observation` of existing metadata
and incarnation sidecar: it bypasses cache, suppresses repair, and never migrates
legacy identity. These reads take internal metadata-object locks, without
reentering the coordinator's business-bucket lifecycle READ behind a queued
writer. Each physical scan retains I1's initialized root and real drain owners
through its last read; cancellation or slow consumers cannot release a detached
worker's owner early. Deadline/resource/identity/closure failures omit Complete;
invalid authentication is PermissionDenied, invalid wire input InvalidArgument,
physical scan/closure failure (including a local scanner timeout) FailedPrecondition,
and expiration of the outer handler deadline DeadlineExceeded.
Client protocol, signature or membership failures are errors, regardless of
previous partial output. No public admin route, Controller persistence, deletion,
GC permission or saved scan session is introduced by this layer.

## Exact-version cleanup

`POST /rustfs/admin/v3/plugin-artifacts/delete-version` accepts the same strict
version-1 exact target as `retire-version`: `target` contains `bucket`,
`bucket_incarnation` and the canonical `attempt_prefix`; `object` and non-nil
`version_id` select one exact version. Its independent
`admin:DeletePluginArtifactVersion` permission is scoped to the exact object
resource. Grant it only to the cleanup controller, separately from upload,
inventory, sealing and verification identities.

The storage operation checks the incarnation under its lifecycle owner and
requires both ordinary PUT and repair seals. It preserves the normal Object
Lock checks. A successful response echoes the target, object and version and
sets `exact_version_deleted: true` under the normal RustFS quorum deletion
semantics. This is not proof of physical erasure from every offline disk; retain
the permanent seals and deletion ledger. Retrying the same identity after an unknown
response is safe. A missing bucket, changed incarnation, missing seal, permission
failure or unavailable storage is not a successful deletion receipt. Ordinary
S3 DELETE remains prohibited for managed buckets.

This endpoint executes an already authorized cleanup decision; it does not
establish absence of publication references or backups. The runner must commit
its eligibility decision and exact deletion intent before invoking it. Initial
support is restricted to configured local disks; remote/distributed layouts
reject rather than bypass their managed-write receiver protections.

Before enabling cleanup, request bucket inspection with `version: 2`. It returns
the existing identity fields and `exact_delete_supported`; the Controller must
require `true`. The ordinary `version: 1` inspection response is unchanged, and
bucket creation still accepts only version 1. The capability check is read-only
and uses `admin:GetPluginArtifactBucket`. Every later deletion rechecks its
identity and fences, regardless of this startup observation.

## Permanent backup retention

Backup scheduling is owned by plugin-runner and is disabled by default. Before
creating a restorable snapshot, the Controller must reserve a permanent
`BACKUP_GC_DISABLED` database policy and then enable storage protection. This
storage API does not create a database backup or certify that PostgreSQL WAL is
complete. A database restored to an older time cannot remove this storage fact.

`POST /rustfs/admin/v3/plugin-artifacts/buckets/retain-backups` accepts exactly:

```json
{"version":1,"bucket":"plugin-artifacts","base_prefix":"outputs/","bucket_incarnation":"<UUID>"}
```

It requires the separate bucket-scoped `admin:RetainPluginArtifactBackups`
action. The response echoes those four fields and adds `backup_retained: true`.
Unknown fields, a nil/stale incarnation, a wrong prefix, unavailable locks or
unconfirmed storage fail the request. Retry the same identity after a lost
response; a timeout does not mean the protection was rolled back. There is no
unretain endpoint. Turning off backup scheduling leaves existing protection in
place, and this first mode has no automatic backup expiration or artifact GC.

The storage owner acquires lifecycle WRITE, draining earlier exact DELETE owners,
then the metadata transaction lock. It validates authoritative metadata and the
incarnation sidecar, writes the marker with `TailDrained`, and publishes the
cache only after persistence. The tracked detached owner survives cancellation
of the HTTP waiter. Exact deletion checks protection under lifecycle READ;
bucket deletion, including force deletion of an empty bucket, also rejects a
protected generation. An unreadable metadata record cannot authorize deletion.
Ordinary metadata updates cannot clear the marker.

Only protected buckets use metadata header version 2. Ordinary buckets retain
version 1. Old binaries reject version 2 instead of silently ignoring the new
MessagePack field. The new reader requires agreement between the header and
marker, a committed managed identity and enabled versioning. Upgrade all serving
nodes before enabling this irreversible protection; rolling back to an older
binary does not remove it and may make the protected bucket unavailable.

Inspect with `version: 3` using `admin:GetPluginArtifactBucket`. Its response adds
`backup_retained` to the version-2 shape. A restore must verify this field is
true and match the fixed bucket, prefix and incarnation; configuration alone is
not proof. Version-1 and version-2 response shapes remain unchanged.

This protects versions on the running store against supported cleanup paths. It
does not replace storage replication or protect against destruction of the
underlying disks, manual metadata replacement, or an administrator bypassing the
storage service. Backup/PITR deployment procedures must preserve the same
artifact store identity and isolate old execution authority before reopening a
restored Controller.
