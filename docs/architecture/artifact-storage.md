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
operation. A remote RPC timeout does not prove that its physical write stopped;
the current ownership chain only retains local disk workers. Remote completion
confirmation is required before this capability can be enabled there. Ordinary
buckets retain their existing behavior.

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

This operation does not yet prove that every write path is sealed. Direct
lower-level writes, repair, remote physical completion, and a controlled external
seal interface still require enforcement and verification. Runner must not
consume this PUT-only operation as a permanent all-writes seal.

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
updates, metadata replacement, and versioning rejection. Run:

```sh
cargo test --locked -p rustfs-storage-api --lib artifact
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_bucket -- --test-threads=1
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_put_seal -- --test-threads=1
cargo test --locked -p rustfs-ecstore --lib --features test-util artifact_write_entrypoint -- --test-threads=1
```

`crates/ecstore/src/store/init/tests/artifact_put_seal_tests.rs` pauses real
rename work to check the PUT barrier, including a cancelled request waiter, and
compares on-disk object metadata after rejected writes. It also checks restart,
record corruption, generation mismatch, and ordinary bucket behavior.

`crates/ecstore/src/store/init/tests/artifact_write_entrypoint_tests.rs` checks
unsupported entry points against real four-disk state, ordinary-bucket success
controls, cleanup of preexisting multipart uploads, and deletion/recreation
between request classification and publication.

These checks establish bucket identity, versioning, and ordinary PUT sealing.
They do not certify all-writes sealing, backup retention, or safe artifact
deletion.
