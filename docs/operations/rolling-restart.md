# Restarting a multi-node RustFS cluster

**Use this when:** restarting or upgrading nodes of an erasure-coded multi-node cluster, bringing a cluster back after several nodes were down at once, or interpreting `503` degraded-mode responses during startup.

**Source of truth:** `crates/config/src/constants/health.rs` (`DEFAULT_STARTUP_READINESS_MAX_WAIT_SECS`), `rustfs/src/server/readiness.rs` (readiness responses and `Retry-After`), `crates/config/src/constants/drive.rs` (remote-drive recovery thresholds), `helm/rustfs/values.yaml` (`minReadySeconds`), `rustfs/src/server/readiness/peer_readmission.rs` (startup peer re-admission gate), the `iam_bootstrap_retry_failed` and `startup_peer_readmission` log events.

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

- Rolling restart (no downtime): restart one node at a time, wait for the restarted node to report `200` on `/health/ready`, and then wait until the other nodes no longer list its drives under `details.storage.unavailableDrives` before touching the next one. The remaining nodes keep serving traffic.
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

3. Wait until every other node has re-admitted it. A distributed node holds its startup readiness until a strict majority of the peers in each of its erasure sets count its drives online (see [startup peer re-admission](#startup-peer-re-admission)), but that gate fails open, so check the remaining nodes before moving on. A node's own `/health/ready` says that it can reach the cluster, not that the cluster can reach it again. On each of the other nodes, `details.storage.unavailableDrives` must no longer list the restarted node's drives. `hostOnline: false` means that node cannot dial the restarted peer yet; `runtimeState` of `offline` or `returning` means the dial works and the drive is still passing its recovery probes:

   ```bash
   curl -s http://<other-node>:9000/health/ready | jq '.details.storage.unavailableDrives'
   ```

4. Only then move on to the next node.

While one node is down, the rest of the cluster keeps quorum and serves all traffic. Taking a second node down before the survivors have re-admitted the first can cost some erasure sets their write or even read quorum; that is the situation to avoid (rustfs/rustfs#8669).

### Why re-admission lags local readiness

A survivor dials a peer by its configured host name on every reconnect and keeps no address cache of its own. After the peer's connections drop, its remote drives become `suspect` on the first failed RPC and `offline` on the second (`RUSTFS_DRIVE_SUSPECT_FAILURE_THRESHOLD`, default `2`). The survivor then probes the peer every `RUSTFS_DRIVE_RETURNING_PROBE_INTERVAL_SECS` (default `2`) and needs `RUSTFS_DRIVE_RETURNING_SUCCESS_THRESHOLD` (default `3`) consecutive successes before the drive counts as `online` again, so re-admission trails the first successful dial by about four to six seconds even on a perfect network.

On Kubernetes the dial itself is the slow part. A replaced pod gets a new IP, but resolvers keep answering with the previous one until the record's TTL expires: kube-dns serves 30-second TTLs, CoreDNS five seconds by default, and NodeLocal DNSCache honours either. Packets to the old IP are usually dropped rather than refused, so every dial runs into the 3-second connect timeout (`RUSTFS_INTERNODE_CONNECT_TIMEOUT_SECS`) and the 1-second budget of the readiness ping (`RUSTFS_HEALTH_LOCK_ONLINE_TIMEOUT_MS`) logs `online check timed out while dialing`. Until the stale record expires, survivors count the replaced pod as unreachable even though it already reports Ready. The Helm chart therefore sets `minReadySeconds` (default `45`) on each StatefulSet so the rolling update does not stop the next pod inside that window. Size it to your DNS TTL plus about ten seconds, or set it to `0` when an external procedure performs step 3 itself.

### Startup peer re-admission

After a distributed node reaches local readiness, it asks the peers in every erasure set it serves how they currently see its drives. It uses the `PeerDriveAdmission` internode RPC, and each peer answers with the same predicate its own `/health/ready` uses for `unavailableDrives`. Startup readiness is published once a strict majority of each set's peers count all of this node's drives in that set as online. In a three-node set that means both peers. While the node waits, `/health/ready` stays `503` with `startup_finalization_pending` and the log shows `event="startup_peer_readmission" state="waiting"`.

The wait always ends in publication:

- `result="admitted"`: the peers re-admitted the node.
- `result="gave_up" reason="deadline_elapsed"`: the startup budget `RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS` ran out. The gate shares that budget with local readiness.
- `result="gave_up" reason="peers_unreachable"`: for 15 seconds the node could not reach enough peers to make the majority possible. A dead peer does not hold a rollout for the whole budget.
- `result="gave_up" reason="peers_cannot_report"`: too many peers run an older release without the RPC, which is normal for the first nodes of a rolling upgrade.
- `result="skipped"`: the gate is disabled, the deployment is not distributed, or no erasure set shares drives with a peer.

The gate covers the delay inside RustFS without relying on the orchestrator. The chart's `minReadySeconds` stays useful as a second margin and for older releases. Set `RUSTFS_STARTUP_PEER_READMISSION_ENABLE=false` to return to local-only startup readiness.

## Sequential cold start (multiple nodes down)

When the whole cluster (or several nodes) went down and nodes are brought back one at a time:

1. Early nodes come up degraded. The process does not exit. S3 requests receive `503 Service Unavailable` with a `Retry-After: 5` header, an `x-rustfs-readiness-pending` header, and a body naming the blocking dependency:

   | Blocking dependency | Meaning |
   | --- | --- |
   | `storage_quorum` | Waiting for enough nodes/disks for the erasure read quorum. |
   | `iam` | Storage is up; the IAM cache is still loading. |
   | `startup_finalization` | Last startup steps are being published. |

2. Logs say what the node waits for. The IAM recovery loop retries with backoff and logs `event="iam_bootstrap_retry_failed"` with an actionable `hint` field (for example, "storage read quorum not met yet; waiting for enough cluster nodes/disks to come online"). After repeated failures the level escalates from WARN to ERROR; this still does not kill the process.
3. Recovery is automatic. Once storage read quorum is available, pending nodes can finish IAM bootstrap on the next retry. `/health/ready` returns `200` when startup has published `FullReady` and the node can still serve reads (storage read quorum, IAM, and shared-lock quorum). Write quorum is reported separately and is required by `/minio/health/cluster`, not by Service membership. Restarting pending nodes does not speed this up.
4. Check readiness detail while waiting. `/health/ready` (and `/minio/health/ready`) separate `storage` / `poolMetadata` / `iam` / `lock` readiness. `storage.ready` follows read quorum (`readinessScope` is `read_quorum`); `storage.readQuorum` and `storage.writeQuorum` show the two observations. `poolMetadata.ready` is the metadata writer and does not by itself withdraw `/health/ready`. `degradedReasons` on this probe names read-path causes such as `storage_quorum_unavailable` and `storage_and_lock_unavailable`. `pool_meta_write_blocked` and `pool_metadata_check_timeout` stay on `/minio/health/cluster`. See the [storage detail contract](../architecture/readiness-matrix.md#storage-detail-contract) for probe scopes and sampling limits:

   ```bash
   curl -s http://<node>:9000/health/ready | jq
   ```

## Tuning

| Variable | Default | Effect |
| --- | --- | --- |
| `RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS` | `120` (`DEFAULT_STARTUP_READINESS_MAX_WAIT_SECS`) | How long startup waits for full readiness before continuing in degraded mode with background recovery. Raising it delays the listener during genuinely slow starts; lowering it surfaces degraded mode sooner. Recovery retries continue regardless of this limit. |
| `RUSTFS_STARTUP_PEER_READMISSION_ENABLE` | `true` (`DEFAULT_STARTUP_PEER_READMISSION_ENABLE`) | Hold startup readiness until a strict majority of each erasure set's peers count this node's drives online. The wait is bounded by `RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS` and fails open. Set `false` to publish on this node's own view only. |
| `RUSTFS_DRIVE_RETURNING_PROBE_INTERVAL_SECS` | `2` (`DEFAULT_DRIVE_RETURNING_PROBE_INTERVAL_SECS`) | Interval between recovery probes to a remote drive that is `offline` or `returning`. |
| `RUSTFS_DRIVE_RETURNING_SUCCESS_THRESHOLD` | `3` (`DEFAULT_DRIVE_RETURNING_SUCCESS_THRESHOLD`) | Consecutive successful probes before an `offline` remote drive is `online` again. Together with the interval this bounds how long survivors keep a restarted peer out after they can dial it. |

## What is not normal

- A node process exiting with a fatal IAM/lock error during startup. That fatal path was removed after v1.0.0-beta.5 (rustfs/rustfs#4304); upgrade if you still see it.
- A node stuck degraded after the whole cluster is back: check network reachability between nodes (peer RPC ports) and per-node clocks, then inspect `degradedReasons` and the `hint` field of the IAM retry logs.
- A node shown offline in the console with no log output is tracked separately (rustfs/backlog#888).
