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

## Runtime write recovery

A runtime metadata or identity read failure before any write is dispatched rejects that operation but does not permanently block subsequent retries. Errors retain their typed cause; pool metadata unavailability reaches S3 as `503 ServiceUnavailable`, without exposing internal error details. Cancellation during preflight or a rejected first conditional write is also retryable. After any identity, prepare, or commit write may have started, cancellation, an uncertain write result, or abandoned runtime publication blocks further metadata-dependent mutations.

The node checks for interrupted pool metadata transactions every five seconds. Failed recovery attempts back off to at most sixty seconds; each attempt has a thirty-second budget and stops on shutdown. Healthy nodes do not read metadata for this worker. Recovery:

1. Cancels old decommission workers and waits for their supervisors to drain them. It does not cancel a separate rebalance operation; an attached rebalance worker prevents recovery until quiescent.
2. Holds the local start/movement gates and distributed `pool.bin` write fence, validates the initialized deployment identity and unchanged pool topology, and selects the authoritative durable transaction.
3. Repairs pending, missing, or lagging copies using conditional writes, then rereads and verifies convergence. A prepare-only first V3 migration commits the predecessor as V3, preserving the observed format floor.
4. Invalidates old movement snapshots, installs the verified durable state, rechecks the fence, and only then clears the block. Speculative in-memory progress is never used as the recovery source. The existing decommission supervisor resumes eligible work afterward.

An unreadable replica, lost fence, or conditional-write conflict leaves the original block in place. Recovery never initializes an all-missing metadata set. Corruption, incompatible layouts, conflicting identities/epochs/transactions, and topology changes require operator reconciliation; restore readability and consistency using the procedures below. Blocks originating in startup validation or storage-format heal are not cleared by the pool transaction worker: restart only after repairing the underlying condition. There is no force-clear switch. If an attached rebalance worker cannot quiesce, collect its status and restart the affected node after verifying the durable metadata; do not manually detach its worker token.

### Diagnostics

- The first block emits `decommission_state` with `state=pool_metadata_blocked`, `reason`, `phase`, and `blocked_since`. A change in recovery failure classification emits `state=pool_metadata_recovery_pending`; successful recovery emits `state=pool_metadata_recovered` with the original timestamp.
- `rustfs_pool_metadata_blocks_total{reason}` and `rustfs_pool_metadata_recoveries_total` count block and recovery transitions. The original cause and phase remain attached to local typed errors; storage/RPC error numbers and on-disk formats are unchanged.
- Node and cluster-write readiness include `pool_meta_write_blocked`. Waiting for the metadata save mutex is bounded to 100 ms and reports `pool_metadata_check_timeout`, not a persistent block. Cluster probes retain their existing cache and overall timeout behavior. Liveness and cluster-read quorum checks are unchanged. The authenticated node-local status and safe error fields are described in [S3 write failure diagnostics](s3-write-failure-diagnostics.md).

If a block persists, inspect the first block and subsequent recovery phase, restore disk/peer readability, and verify every metadata and identity copy before restarting. Do not delete metadata to make readiness green.

## Disk replacement and metadata erasure

1. Keep a quorum of nodes online and verify the cluster is ready.
2. Stop the lagging node before replacing or erasing its metadata drive.
3. Restore storage formats and the `pool.bin.identity` marker from the same deployment before rejoining it.
4. Start the node and wait for it to load the verified committed generation and repair its replicas before touching another node.

An initialized identity with every `pool.bin` missing is recovery required, as are existing storage formats with neither identity nor `pool.bin`. Format creation alone is not fresh-cluster proof: only the elected first topology node may create a durable `initialized=false` bootstrap identity with a fresh-bootstrap nonce, and only after every configured disk explicitly responds that it is unformatted. An unreachable peer, a non-elected distributed node, or an existing format is not sufficient proof. All-missing `pool.bin` replicas are accepted only by the same startup that proved the fresh topology and persisted that pending identity; when every `pool.bin` is missing, a later startup must recover even if the pending identity survived. This prevents a wiped or lagging node from rebuilding empty state and overwriting the cluster. Runtime reload, rebalance activation, and rebalance worker admission fail closed on this missing-authority condition; a clean probe alone cannot clear it.
