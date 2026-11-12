# Pool metadata upgrade and recovery

**Use this when:** upgrading a cluster to `pool.bin` V3, a node cannot rejoin after a metadata-drive replacement, or startup reports `pool.bin` as incompatible, corrupt, or recovery required.
**Source of truth:** `crates/ecstore/src/core/pools.rs` (`pool.bin` / `pool.bin.identity` reader and writer, the `RUSTFS_POOL_META_V3_WRITE` and `RUSTFS_POOL_META_V3_FLEET_CONFIRMED` gates).

`pool.bin` is cluster state. Do not delete or copy it independently on a live node. Version 3 adds a deployment identity, epoch, durable generation, and a recoverable prepare/commit record on every pool.

## Compatibility matrix

| Reader or writer | V1 | V2 | V3 |
| --- | --- | --- | --- |
| Legacy V1 binary | read/write | reject | reject |
| V2-capable binary | read/write while mixed | read/write after the V2 fleet gate | reject |
| V3-capable binary | read/migrate | read/migrate | read/write; never downgrade |

Leave `RUSTFS_POOL_META_V3_WRITE` and `RUSTFS_POOL_META_V3_FLEET_CONFIRMED` disabled while any running process lacks V3 support. Both must be `true` before an existing cluster migrates. A fresh deployment can initialize directly at V3. Once a committed V3 generation is observed, rollback to a V1/V2-only binary is not supported. Repairing a missing identity on an existing V1/V2 snapshot does not cross the V3 gate; the identity is committed as initialized while `pool.bin` stays on its observed legacy version.

| Startup verdict | Cause | Handling |
| --- | --- | --- |
| **incompatible** | Unsupported version or field layout (unknown fields are not ignored) | Never overwritten |
| **corrupt** | Truncated or invalid payload | Repaired only from a verified committed replica |
| **recovery required** | Conflicting identities, epochs, or transactions at the same generation | Needs an operator-selected source |

## Partial writes

A V3 update first conditionally writes a pending generation containing the last committed snapshot, then conditionally replaces it with the committed record. During initial bootstrap, `pool.bin.identity` remains `initialized=false` and carries a unique fresh-bootstrap nonce until that committed V3 record is verified. Restarting from an initial prepare record finishes generation 1; it never rewrites the record as V1 or V2. On restart:

- prepare-only replicas expose their previous committed snapshot;
- one committed replica makes that transaction authoritative;
- remaining pending or older replicas are repairable by the next fenced save;
- two different committed transactions at one generation stop startup.

Do not hand-edit a pending record or select a replica only because it is in pool zero. Preserve all copies when escalating recovery.

## Committed promotion and peer convergence

`promote_queued_decommission_with_reload` requires the peer reload barrier on every worker attempt, including an unchanged retry. After the barrier returns, `confirm_decommission_peer_convergence` rechecks authoritative identity and replicas, the write gate, namespace fence, durable and local generation, and cancellation before allowing data movement.

When promotion is committed and those checks remain valid, a failed peer reload pauses that active generation for the existing watchdog. The worker releases its exact owner and preserves queued successors; it creates no new progress checkpoint and cannot adopt, copy or clean up objects until a later barrier succeeds. Persisted failed or canceled states and known metadata uncertainty remain authoritative. The notification layer aggregates peer errors, so the returned error preserves that aggregate rather than proving the original remote error type; inspect the failing peer to diagnose the cause.

## Interrupted native metadata publication

A process kill can leave a pool with two PREPARE heads and two COMMIT heads for the same V3 transaction. Repeating the ordinary read cannot repair that split: the latest-head selection still requires its original quorum. Startup reconciliation holds the same distributed `pool.bin` writer fence as runtime recovery.

Conditional native recovery is limited to modern, single-part inline metadata. An ordinary readable COMMIT in another pool authorizes completing that transaction. Recovery requires one matching cluster identity and epoch on every disk and complete readable current and rollback evidence. Every current shard must match the selected transaction; older backup generations remain subject to full integrity validation. Reconstruction verifies bitrot and erasure integrity; two reconstructable shards do not establish commit authority. Missing, conflicting, unsupported, or unexplained higher-generation evidence remains an operator recovery case.

If no ordinary COMMIT selects the interrupted transaction, an ordinary readable V3 PREPARE can authorize restoring its embedded previous V3 COMMIT. Recovery does not promote the prepared state. It repairs unreadable pools first, retaining an ordinary PREPARE until every unreadable pool has been restored. The restored ordinary COMMIT then carries that authority if rewriting the last PREPARE is interrupted. A retained COMMIT backup from the interrupted transaction or competing ordinary PREPARE records prevents rollback. Retained PREPARE backups from an abandoned transaction do not prevent a later transaction from reusing that generation only when both have the same exact predecessor; those backups never grant commit authority.

Recovery checks each disk ID, current SHA and complete backup inventory inside the native metadata and publication leases. Identity shards are validated on every member of their own hashed erasure set before Apply and after normal CAS/readback, under the same global writer fence. Co-located identity bytes are additionally pinned and bound into that disk's inventory SHA; co-location is not required. It publishes the proven COMMIT or acknowledged predecessor with a fresh physical file identity, retains rollback evidence, then performs normal conditional V3 writes and ordinary quorum readback. Partial repair or fence loss keeps writes blocked. A verified identity or transaction conflict retains `RecoveryRequired` in the original write state even if partial repair made the head readable; a later ordinary quorum cannot clear that known conflict. Transport failures, unsupported recovery layouts and missing evidence do not acquire a permanent validation latch merely because recovery could not verify them.

Raw local mutators reject protected source and destination paths before I/O, including accepted volume/path aliases that resolve into the `pool.bin` namespace. The shared `protected_path_kind` helper in `crates/ecstore/src/disk/local/pool_meta_native.rs` follows the native path resolver. Conditional metadata writes and Undo require the canonical object key so aliases cannot create another mutation-lease domain.

Normal `pool.bin` CAS also compares the exact per-disk predecessor under the native mutation lease. Each physical PREPARE and COMMIT uses a separate rollback directory. Undo must match both its own new head and its exact parent backup; late writes, undo and cleanup cannot replace a later acknowledged head. Ordinary object writes retain their existing behavior.

On Unix, guarded inline writes create the predecessor backup directly in that write's owned UUID directory. A fully synchronized head can seed one in-memory durability receipt per disk, holding two read-only descriptors for the head and root. A later write consumes it once and may omit only the backup inode data sync when the current root, inode, complete raw SHA and length, and file identity match, and the backup is proven to be a hardlink to that inode. New head data sync and every backup, head and ancestor directory sync remain required. Published head inodes and their hardlinked backup contents must remain immutable: supported changes stage a new inode and rename it into place. The receipt depends on this invariant; a matching hash alone cannot prove durability after arbitrary in-place rewrites. This receipt carries no commit, identity, quorum or namespace authority.

Restart, a new disk handle, raw-SHA parents, copy fallback, non-inline writes, failed or cancelled publication, Undo and native Apply retain the cold path. They cannot reuse an earlier durability proof. Cleaning an unrelated old backup does not invalidate an unchanged live head receipt.

The native CAS, undo and recovery RPC methods require all participating storage nodes to support them. They never fall back to legacy mutators on `Unimplemented`. Pause pool metadata mutations during a mixed-version rollout and finish upgrading the storage nodes before resuming decommission or rebalance. V3 payloads remain unchanged; downgrading the entire fleet to binaries without native CAS requires a stopped cluster and offline reconciliation.

## Runtime write recovery

A runtime metadata or identity read failure before any write is dispatched rejects that operation but does not permanently block subsequent retries. Errors retain their typed cause; pool metadata unavailability reaches S3 as `503 ServiceUnavailable`, without exposing internal error details. Cancellation during preflight or a rejected first conditional write is also retryable. After any identity, prepare, or commit write may have started, cancellation, an uncertain write result, or abandoned runtime publication blocks further metadata-dependent mutations.

During startup, an unreadable identity or `pool.bin` replica returns typed `ReadUnavailable` before a partial metadata view is installed or the object store is published. Initialization retries from the complete identity and metadata read at the existing five-second retry interval, within the existing startup attempt budget. A later readable result must still pass the original identity, generation, topology, conditional-write and readback checks. Read-only unavailability does not create a permanent startup block; an existing validation or uncertain-write block is never cleared by a clean retry.

The node checks for interrupted pool metadata transactions every five seconds. Failed recovery attempts back off to at most sixty seconds; each attempt has a thirty-second budget and stops on shutdown. Healthy nodes do not read metadata for this worker. Recovery:

1. Cancels old decommission workers and waits for their supervisors to drain them. It does not cancel a separate rebalance operation; an attached rebalance worker prevents recovery until quiescent.
2. Holds the local start/movement gates and distributed `pool.bin` write fence, validates the initialized deployment identity and unchanged pool topology, and selects the authoritative durable transaction.
3. Repairs pending, missing, or lagging copies using conditional writes, then rereads and verifies convergence. A prepare-only first V3 migration commits the predecessor as V3, preserving the observed format floor.
4. Invalidates old movement snapshots, installs the verified durable state, rechecks the fence, and only then clears the block. Speculative in-memory progress is never used as the recovery source. The existing decommission supervisor resumes eligible work afterward.

An unreadable replica, lost fence, or conditional-write conflict leaves the original block in place. Recovery never initializes an all-missing metadata set. Corruption, incompatible layouts, conflicting identities/epochs/transactions, and topology changes require operator reconciliation; restore readability and consistency using the procedures below. Blocks originating in startup validation or storage-format heal are not cleared by the pool transaction worker: restart only after repairing the underlying condition. There is no force-clear switch. If an attached rebalance worker cannot quiesce, collect its status and restart the affected node after verifying the durable metadata; do not manually detach its worker token.

### Diagnostics

- The first block emits `decommission_state` with `state=pool_metadata_blocked`, `reason`, `phase`, and `blocked_since`. A change in recovery failure classification emits `state=pool_metadata_recovery_pending`; successful recovery emits `state=pool_metadata_recovered` with the original timestamp.
- `rustfs_pool_metadata_blocks_total{reason}` and `rustfs_pool_metadata_recoveries_total` count block and recovery transitions. The original cause and phase remain attached to local typed errors; storage/RPC error numbers and on-disk formats are unchanged.
- Cluster-write readiness reports `pool_meta_write_blocked`; node readiness reports the writer only through `details.poolMetadata`. Readiness reads the authority published by the last local metadata writer instead of waiting for the save gate, so an active or queued metadata transaction does not degrade readiness by itself. A block installed by a writer appears when that writer releases the gate. Cluster probes retain their existing cache and overall timeout behavior. Liveness and cluster-read quorum checks are unchanged. The authenticated node-local status and safe error fields are described in [S3 write failure diagnostics](s3-write-failure-diagnostics.md).

Pool metadata CAS explicitly selects `TailDrainedWithOwnerRepair`: it waits for the complete rename result and returns repair responsibility to its existing deployment and transaction owner when write quorum commits but a replica fails or signatures diverge. Cache invalidation and the committed cleanup handoff precede that error. Ordinary system metadata retains quorum-success semantics under `Quorum` and `TailDrained`; draining a tail does not require every disk to succeed. A repair-required completion does not enter rollback cleanup or wait for another temporary-data deletion. The committed bytes are not rolled back. Generic MRF and heal-channel requests cannot supply the deployment and transaction authority needed to repair system metadata; pool CAS retains the uncertain outcome and its existing recovery fence. Ordinary object repair keeps its bucket-incarnation fencing.

An owning identity writer handles `SystemMetadataRepairRequired` before leaving its transaction: it can retry the exact identity up to three times, waiting one, two, then four seconds for disks to reconnect. Each attempt rereads the committed identity, requires byte-for-byte agreement with its intended payload and a usable ETag, and issues a conditional owner-observed write under the original startup or runtime authority. Quorum readback alone does not complete repair. This lets a transient disk failure during pool expansion converge before startup publishes the new topology. Changed or unreadable identity, a rejected repair CAS, lost authority, cancellation, and exhausted repair attempts retain the transaction-unknown gate. Other write errors and `pool.bin` transactions keep their existing recovery behavior.

If a block persists, inspect the first block and subsequent recovery phase, restore disk/peer readability, and verify every metadata and identity copy before restarting. Do not delete metadata to make readiness green.

## Disk replacement and metadata erasure

1. Keep a quorum of nodes online and verify the cluster is ready.
2. Stop the lagging node before replacing or erasing its metadata drive.
3. Restore storage formats and the `pool.bin.identity` marker from the same deployment before rejoining it.
4. Start the node and wait for it to load the verified committed generation and repair its replicas before touching another node.

An initialized identity with every `pool.bin` missing requires metadata recovery. Existing storage formats with neither identity nor `pool.bin` also require recovery unless the pool retains a complete durable format-bootstrap intent. Runtime reload, rebalance activation, and rebalance worker admission preserve this authority check.

Fresh formation first requires every configured disk to explicitly respond that it is unformatted and the legacy-format adoption check to complete. The pool's elected creator persists `.rustfs.sys/format.json.bootstrap` on every configured disk before publishing any format. Each intent contains the deployment UUID, complete erasure topology and assigned disk UUID. After an interruption, that creator can resume only from a complete, consistent intent set whose exact endpoint slots agree with every surviving format. It restores explicitly unformatted slots using the original UUIDs and recovers creator authority before object-level identity reads. Missing, corrupt, conflicting and unreachable intent replicas withhold that authority. A pending `pool.bin.identity` alone continues to require verified creator authority.

After the elected metadata writer commits `pool.bin` and the initialized identity, it durably renames bootstrap intents to `format.json.bootstrap.done`. Every node establishes a durable absence of at least one intent per pool before proceeding with startup. Since replay requires every intent, partial retirement already consumes the authority and a subsequent restart loads the committed metadata normally. The `.done` files are internal tombstones and should remain with the deployment.

Use the same protocol-capable version on all nodes for fresh formation. Finish initialization and intent retirement before rolling back to an older binary, which does not enforce the consumption barrier. Existing deployments retain their current format and identity encodings. Historical interrupted deployments created by older versions, including the format-plus-UUID-temporary-file state in issue #9270, require operator-confirmed recovery because their files contain no durable creator intent. Preserve the drives and follow the metadata-recovery procedure above when any existing data or topology history needs to be retained.
