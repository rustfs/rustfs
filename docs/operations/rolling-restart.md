# Restarting a multi-node RustFS cluster

**Use this when:** restarting or upgrading nodes of an erasure-coded multi-node cluster, bringing a cluster back after several nodes were down at once, or interpreting `503` degraded-mode responses during startup.

**Source of truth:** `crates/config/src/constants/health.rs` (`DEFAULT_STARTUP_READINESS_MAX_WAIT_SECS`), `rustfs/src/server/readiness.rs` (readiness responses and `Retry-After`), the `iam_bootstrap_retry_failed` log event.

Upgrading the binary or container image does not change the on-disk data format unless an explicitly enabled feature documents a version floor. Replacing the executable and restarting does not run a migration step on startup.

## Version floors that break mixed-version fleets

Each row is a feature whose activation makes older binaries unable to read what newer ones write. Keep every gate in its inactive state throughout a mixed-version rolling upgrade.

| Feature gate | Activation | Consequence once active | Owning doc |
| --- | --- | --- | --- |
| Local SSE wrapped-DEK JSON envelope | The release that replaces the legacy `base64(nonce):base64(ciphertext)` representation with the versioned JSON envelope | Older nodes cannot read objects written with the JSON envelope. Freeze every source of object mutation (client writes, lifecycle, replication), upgrade every node, then resume traffic. Downgrading after new encrypted objects are written is not supported. | [compat-cleanup-register.md](../architecture/compat-cleanup-register.md) (`sse-local-dek-json-v1`), [minio-file-format-compat.md](../architecture/minio-file-format-compat.md) |
| Data-movement part checksums sidecar | `RUSTFS_DATA_MOVEMENT_PART_CHECKSUMS_WRITE=true` and `RUSTFS_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED=true` (inactive unless both) | Enable only after every node that reads or writes object metadata supports the `part-checksums` sidecar and the fleet has adopted that version as its rollback floor. Once rebalance or decommission has migrated a legacy checksummed multipart object with both enabled, rollback is not supported: older readers ignore the sidecar and can report an object checksum in place of the requested part checksum. | This page |
| Pool metadata version 2 | `RUSTFS_POOL_META_V2_WRITE=true` and `RUSTFS_POOL_META_V2_FLEET_CONFIRMED=true` (inactive unless both) | Once a node observes or writes version 2 it never downgrades `pool.bin`; older binaries and rollback builds cannot read it. Unresolved decommission entries fail closed instead of being written in the version 1 format. | [pool-metadata-recovery.md](pool-metadata-recovery.md) |
| Pool metadata version 3 | `RUSTFS_POOL_META_V3_WRITE=true` and `RUSTFS_POOL_META_V3_FLEET_CONFIRMED=true` (inactive on an existing cluster unless both) | Adds durable generations and a recoverable cross-pool commit protocol. Once committed, V1/V2-only binaries cannot rejoin. | [pool-metadata-recovery.md](pool-metadata-recovery.md) (compatibility matrix, disk-replacement order) |

## TL;DR

- Rolling restart (no downtime): restart one node at a time and wait for the restarted node to report `200` on `/health/ready` before touching the next one. The remaining nodes keep serving traffic.
- Sequential cold start (several nodes down): nodes started before the cluster has quorum come up in degraded mode. The process stays alive, answers `503` with the blocking reason, and recovers automatically as soon as enough peers are online. Do not restart-loop them; keep starting the remaining nodes.

## Why a single node cannot serve alone

Erasure coding shards every object, including internal metadata such as IAM users, groups, and policies under `.rustfs.sys`, across the drives of a set. Reading an object back needs a read quorum of shards online. With the drives of one set spread over several nodes, one node alone can never satisfy the read quorum; this is a property of erasure coding, not a bug. The cluster becomes readable once enough nodes are up (for internal configuration objects, written with maximum parity, typically about half the nodes of a set).

Distributed locking similarly needs a majority of nodes' lock RPC endpoints. The startup path does not take namespace locks while loading IAM, so IAM recovery depends only on the storage read quorum.

## Rolling restart procedure

For each node, in any order, one at a time:

1. Restart the node (upgrade the binary/image first if this is an upgrade).
2. Wait until the node reports ready; a ready node returns `200` with `"ready": true` in the JSON body:

   ```bash
   curl -fsS http://<node>:9000/health/ready
   ```

3. Only then move on to the next node.

While one node is down, the rest of the cluster keeps quorum and serves all traffic. Taking a second node down before the first is back can cost some erasure sets their write or even read quorum; that is the situation to avoid.

## Sequential cold start (multiple nodes down)

When the whole cluster (or several nodes) went down and nodes are brought back one at a time:

1. Early nodes come up degraded. The process does not exit. S3 requests receive `503 Service Unavailable` with a `Retry-After: 5` header, an `x-rustfs-readiness-pending` header, and a body naming the blocking dependency:

   | Blocking dependency | Meaning |
   | --- | --- |
   | `storage_quorum` | Waiting for enough nodes/disks for the erasure read quorum. |
   | `iam` | Storage is up; the IAM cache is still loading. |
   | `startup_finalization` | Last startup steps are being published. |

2. Logs say what the node waits for. The IAM recovery loop retries with backoff and logs `event="iam_bootstrap_retry_failed"` with an actionable `hint` field (for example, "storage read quorum not met yet; waiting for enough cluster nodes/disks to come online"). After repeated failures the level escalates from WARN to ERROR; this still does not kill the process.
3. Recovery is automatic. As soon as enough peers are online for the storage read quorum, the pending nodes finish IAM bootstrap on the next retry and flip `/health/ready` to `200` on their own. Restarting them does not speed anything up.
4. Check readiness detail while waiting. `/health/ready` (and `/minio/health/ready`) return per-dependency detail during degradation; the `details` object shows `storage` / `iam` / `lock` readiness and `degradedReasons` lists machine-readable causes such as `storage_quorum_unavailable` or `lock_quorum_unavailable`:

   ```bash
   curl -s http://<node>:9000/health/ready | jq
   ```

## Tuning

| Variable | Default | Effect |
| --- | --- | --- |
| `RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS` | `120` (`DEFAULT_STARTUP_READINESS_MAX_WAIT_SECS`) | How long startup waits for full readiness before continuing in degraded mode with background recovery. Raising it delays the listener during genuinely slow starts; lowering it surfaces degraded mode sooner. Recovery retries continue regardless of this limit. |

## What is not normal

- A node process exiting with a fatal IAM/lock error during startup. That fatal path was removed after v1.0.0-beta.5 (rustfs/rustfs#4304); upgrade if you still see it.
- A node stuck degraded after the whole cluster is back: check network reachability between nodes (peer RPC ports) and per-node clocks, then inspect `degradedReasons` and the `hint` field of the IAM retry logs.
- A node shown offline in the console with no log output is tracked separately (rustfs/backlog#888).
