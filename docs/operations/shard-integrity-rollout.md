# Independent shard integrity rollout

Independent shard commitments detect internally valid shards that belong to a
different part generation. The feature preserves the existing checksum frames
and adds optional metadata and proof indexes. It does not authenticate legacy
data retroactively. The [format contract](../architecture/erasure-coding.md#51-independent-shard-commitments)
defines the stored representation.

## Configuration and object lifetime

| Variable | Default | Effect |
|---|---|---|
| `RUSTFS_SHARD_INTEGRITY_WRITE` | `false` | Requests protection for new writes without an inherited object/upload mode. |
| `RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED` | `false` | Operator confirmation that every reader, writer and background coordinator supports the extension. |

Both must be true before new writes opt in. Setting only the request switch
does not enable protection. These flags do not perform peer capability discovery
or stop an older binary from joining the cluster.

A PUT fixes its mode before encoding. An MPU fixes its mode at initiation and
persists it with the upload. Completing an old upload after activation does not
promote it. Disabling the switches during a protected upload does not permit
parts or completion to drop the proof requirement.

Reads always follow the object's metadata. Turning off creation does not disable
verification of existing protected objects. A corrupt, conflicting or incomplete
protection declaration is an error, not a legacy object. Metadata-only COPY
preserves the existing commitment. COPY and physical background rewrites inherit
the source mode; they do not turn an unverified legacy object into a historically
verified object simply by calculating a new hash.

## Upgrade and activation

1. Deploy a version that understands and maintains the extension, leaving both
   switches false. Existing legacy reads, shard repair and explicit-version
   metadata recovery continue under their existing bounds.
2. Validate old/new process combinations for PUT, multipart completion, COPY,
   Heal and restart recovery in the actual deployment. Decoder fixture tests
   alone do not certify these operations.
3. Finish upgrading every reader and write/repair/movement coordinator. Prevent
   old services or rollback images from rejoining before enabling protection.
4. Qualify protected writes, range reads, repair, missing indexes and deployment
   restart/failure behavior. Measure small-object throughput, range latency and
   maximum multipart metadata overhead against the same release configuration.
5. Enable both switches consistently. Verify new objects with an exclusive Deep
   scan before treating the enhanced mode as qualified for production traffic.

Existing rollout or performance gaps remain gates for production activation;
this runbook is not a claim that a distributed upgrade has been exercised.

## Legacy repair and interpretation

Legacy objects remain exposed to substitution by a complete, internally valid
donor shard. Traditional reconstruction can restore missing or detectably
damaged shards, but cannot prove that all surviving bytes are the original
content. It therefore does not create an independent digest or a positive strong
integrity receipt.

Use the heal item's before/after drives and actual repaired-drive count to
observe ordinary recovery. Strong outcome counters may remain unknown/skipped
because object identity is unproven; that is distinct from execution progress.
MRF likewise retains durable legacy repair obligations after physical recovery
when no independent verification receipt can discharge them. An idle attempt
queue does not imply that these persistent obligations have been cleared.

After a qualified all-healthy Deep check, the attempt is `Completed` with an `Unknown` object outcome; it is counted in `unverified_tasks`, not `successful_tasks` or `failed_tasks`. This cumulative attempt count is distinct from the current `held_unverified_legacy` responsibility count. The scheduler reports `verification_unavailable`, and the durable obligation remains subject to the existing explicit recheck or audited operator disposition. Execution completion never supplies a missing integrity receipt.

This also applies when the same Deep attempt has just reconstructed missing legacy shards: successful storage commits and a complete, aligned, all-healthy report for the selected version permit an unverified completion without another Deep pass. Storage failures, incomplete commits, lost locks, invalid receipts and protected-object verification failures do not qualify. `total_unverified_objects_repaired` and `total_unverified_bytes_repaired` record physical repair attempts separately from verified outcomes; bytes are logical object sizes, not disk or network traffic, and repeated repairs of the same object are separate attempts. An already-healthy check adds no physical repair count.

Attempt completion, an in-memory hold and a durable hold are separate steps. The hold survives restart after its companion checkpoint commits. A crash before that commit, a lost completion notice or an unavailable publication result can conservatively replay the earlier active responsibility and repeat Deep. Preserve the prior checkpoint and responsibility until a committed publication or exact readback confirms the transition; no retry or execution completion supplies independent verification.

Partial-write MRF replay uses Deep verification so a protected object's repair
can discharge its obligation after verifying the payload. This adds full-object
read work to those background attempts, including healthy replay targets.
Normal presence scans likewise cannot certify protected payloads; an exact,
all-healthy protected version or delete marker may receive `MetadataHealthy`,
which proves authoritative metadata rather than payload integrity. Legacy
objects receive no positive receipt. Local Heal of transitioned objects
checks metadata without reading the tier payload, so it does not issue a
payload-integrity receipt even if a descriptor remains. An
authoritative historical-version cleanup or absence proof has its own identity
and commit checks and does not depend on a live payload digest.

To protect a legacy version as a trusted migration, validate against an
independently trusted original or suitable end-to-end digest, then reupload.
An ETag is not universally a plaintext MD5. Rehashing existing bytes or checking
RS parity alone does not establish the original identity. Keep this distinction
when measuring migration coverage; an ordinary server-side rewrite is not proof
of historical correctness.

## Rollback

Before any protected objects or uploads exist, a rollback still needs actual
old-version write/repair/restart qualification. Metadata decoding compatibility
is insufficient.

After activation, the supported rollback floor is a version that understands
and preserves the extension. Turning off creation leaves protected objects,
historical versions and in-progress protected uploads in place. It neither
converts them to legacy nor authorizes an arbitrary older reader/writer/repair
coordinator. Preserve recovery snapshots and use an explicitly validated
migration procedure if returning to an older format implementation is required.
