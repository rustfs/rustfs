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
to the existing unprepared cleanup path. Staged deletion and bulk `DeleteVersions`
remain outside this boundary.
The incarnation-bound heal path retains its separate admission. Other legacy
mutation routes and internal metadata publication still need their own admission
boundary before distributed artifact sealing can be enabled.

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
on stores using local disks only.
The record binds the bucket incarnation and full attempt prefix. Only a missing
record permits a PUT; a matching record rejects it, and malformed records or
failed reads also reject it. Repeating the seal verifies the same identity.

Distributed or remote-disk configurations reject artifact PUTs and this seal
operation. Receiver-side fencing is implemented, but the full distributed
admission boundary is not yet enabled. In particular, classification of a legacy
RPC as an ordinary-bucket write must remain valid through physical completion,
including cancellation and bucket deletion/recreation. A remote timeout, quorum
response or expired lock lease cannot prove that a physical writer stopped.

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
rejects missing or mismatched identities. These receiver mechanisms alone do
not authorize enabling distributed artifact buckets or garbage collection.

The PUT commit path takes locks in this order: bucket lifecycle, attempt record,
then target object. Same-attempt commits are serialized; uploading a body does
not hold the attempt lock. The actual commit and rename-tail owner retains the
attempt guard after a quorum response or cancellation of the request waiter.
The seal takes the same attempt lock and persists its record with all metadata
writes drained before returning. Thus it waits for admitted ordinary PUTs and
prevents later ordinary PUT publication, including after a restart.

A failed or cancelled seal request may still leave a record for its original
incarnation. Retry idempotently to confirm success; an error is not proof that
the attempt remains writable or that all metadata writes have been cleaned up.

The managed ECStore entry points reject unsupported writes into an artifact
bucket: borrowed-lock and special-purpose PUTs, copy destinations, multipart
creation/parts/completion, data movement, restore, transition, metadata rewrites,
and tag changes. Copying from an artifact bucket to an ordinary bucket remains
allowed. Listing and aborting existing multipart uploads remain available for
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

Object repair takes the same attempt lock before capacity and object locks.
Its local rename and metadata-replacement workers retain the lifecycle and
attempt owners until the physical mutation completes, even if the async waiter
is cancelled or times out. Repair of an existing version remains allowed after
PUT sealing; it does not reopen logical publication. Ordinary-bucket repair also
retains lifecycle ownership to prevent publication into a recreated bucket.

`ECStore::seal_artifact_attempt_repairs` separately closes repair admission for
the entire attempt, including existing versions and minority metadata repair.
It requires an ordinary PUT seal first. Under lifecycle, attempt, then repair
record locks, it drains admitted physical repair workers before persisting an
immutable record bound to the incarnation and full attempt prefix. New repair
requests reject a matching record; corrupt, mismatched, or unreadable records
also fail closed. Retrying confirms the same identity, including after restart.
Existing bytes remain readable while sufficient healthy shards remain. Other
attempts retain their repair behavior.

Repair sealing is irreversible through this API and reduces recoverability:
do not apply it automatically to successful outputs that still need repair.
It is a local repair fact, not an all-writers or deletion authorization. Do not
downgrade to a binary that ignores repair seals after using this operation.
Restoring pre-seal storage can rewind the decision and requires reconciliation
before relying on the seal again; records must not expire by age.

`ECStore::retire_artifact_object_version` disables repair of one exact version
inside an already sealed attempt. It requires the current bucket incarnation,
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

These operations do not yet prove that every write path is permanently sealed.
Remote physical completion and all-writes sealing still require enforcement
and verification. The controller API below exposes the current local storage
facts; it does not remove those remaining requirements. Runner must not consume PUT sealing or
version retirement alone as permanent all-writes authority. Retirement records
must not expire by age; restoring storage from before a retirement can rewind
that decision and requires a separate recovery contract.

Before Runner can rely on a seal, storage must fence every supported write path
for the exact bucket incarnation and attempt namespace, retain the fence through
commit and any remaining physical writes, and durably record the seal. Unsupported
write paths must reject writes into the managed namespace. HTTP cancellation or
a cached metadata value cannot substitute for that storage boundary.

Automatic garbage collection remains disabled until storage sealing and the
backup/restore retention contract are both implemented and verified. Unknown
backup/PITR coverage must retain artifacts; a completed plugin execution does not
prove that historical results are safe to delete.

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

These checks establish bucket identity, versioning, ordinary PUT sealing,
attempt repair sealing, and exact-version repair retirement. Repair-seal tests
also cover restart, corrupt records, other-attempt isolation, and cancelled
rename/metadata workers whose physical writes must drain before confirmation.
They do not certify all-writes sealing, backup retention, or safe artifact
deletion.

## Controller API

The admin router exposes five SigV4-authenticated POST operations:

- `/rustfs/admin/v3/plugin-artifacts/buckets`, requiring
  `admin:CreatePluginArtifactBucket` on the bucket ARN.
- `/rustfs/admin/v3/plugin-artifacts/buckets/inspect`, requiring
  `admin:GetPluginArtifactBucket` on the bucket ARN.
- `/rustfs/admin/v3/plugin-artifacts/seal-attempt`, requiring
  `admin:SealPluginArtifactAttempt` on the attempt namespace.
- `/rustfs/admin/v3/plugin-artifacts/seal-repairs`, requiring
  `admin:SealPluginArtifactRepairs` on the attempt namespace.
- `/rustfs/admin/v3/plugin-artifacts/retire-version`, requiring
  `admin:RetirePluginArtifactVersion` on the exact logical object key.

All require a permanent, non-root original IAM user and an installed request
server context. Temporary credentials and service accounts are rejected. The
initial profile requires built-in IAM; external authorization plugins cannot
establish its explicit local Resource contract. No separate signer map is used.
The controller grant is independent of an execution delegation's expiration.

Both seal operations use this request:

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
separately only to the component making that irreversible decision):

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
returns `version`, `target`, and `attempt_repairs_sealed: true`. Retirement
returns `version`, `target`, `object`, `version_id`, and
`exact_version_repair_retired: true`. These are idempotent confirmations of
specific storage facts. They do not attest that all writers are sealed, that
data was deleted, or that GC is permitted.

Malformed requests return InvalidRequest; authentication/resource denial returns
AccessDenied. All storage failures and caller deadlines return a sanitized
ServiceUnavailable response: `artifact operation unconfirmed; retry the same identity`.
This deliberately does not reveal internal record paths or distinguish durable
record corruption from unavailable storage. It also does not mean the operation
was rolled back: the cancellation-safe storage transaction may still complete.
Retry the identical target to obtain confirmation; operators must investigate
persistent failures. Current distributed-topology rejection is retained.

Bucket creation does not change STS vending. Existing v1
vending requires input and output to share the delegation's bucket;
cross-bucket output credentials need a separate change. GC remains disabled
until the remaining storage and backup/PITR requirements are satisfied.

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

The storage helper first requires local-only configured endpoints and enabled
namespace locking. It invokes the existing creation transaction with an
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
distributed-topology rejection before creation, and actual delete/recreate with
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
locking and local-topology rejection are covered independently.
