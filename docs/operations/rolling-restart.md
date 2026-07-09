# Restarting a multi-node RustFS cluster

How to restart nodes of an erasure-coded multi-node cluster without losing
availability, what to expect when several nodes are down at once (sequential
cold start), and how to read the degraded-mode signals. Written for the
failure pattern reported in rustfs/rustfs#4304.

> Upgrading the binary or container image never changes the on-disk data
> format. Replacing the executable and restarting is safe; no migration step
> runs on startup.

## TL;DR

- **Rolling restart (no downtime):** restart **one node at a time**, and wait
  for the restarted node to report `200` on `/health/ready` before touching
  the next one. The remaining nodes keep serving traffic.
- **Sequential cold start (several nodes down):** nodes started before the
  cluster has quorum come up in **degraded mode** — the process stays alive,
  answers `503` with the blocking reason, and recovers **automatically** as
  soon as enough peers are online. Do not restart-loop them; just keep
  starting the remaining nodes.

## Why a single node cannot serve alone

Erasure coding shards every object (including internal metadata such as IAM
users, groups, and policies under `.rustfs.sys`) across the drives of a set.
Reading an object back needs a **read quorum** of shards online. With the
drives of one set spread over several nodes, one node alone can never satisfy
the read quorum — this is a mathematical property of erasure coding, not a
bug. The cluster becomes readable once enough nodes are up (for internal
configuration objects, which are written with maximum parity, that is
typically about half the nodes of a set).

Distributed locking similarly needs a majority of nodes' lock RPC endpoints.
The startup path no longer takes namespace locks while loading IAM
(rustfs/rustfs#4363), so IAM recovery depends only on the storage read
quorum.

## Rolling restart procedure

For each node, in any order, **one at a time**:

1. Restart the node (upgrade the binary/image first if this is an upgrade).
2. Wait until the node reports ready:

   ```bash
   curl -fsS http://<node>:9000/health/ready
   ```

   A ready node returns `200` with `"ready": true` in the JSON body.
3. Only then move on to the next node.

While one node is down, the rest of the cluster keeps quorum and serves all
traffic. If you take a second node down before the first is back, some
erasure sets may lose write or even read quorum and requests start failing —
this is the situation to avoid.

## Sequential cold start (multiple nodes down)

When the whole cluster (or several nodes) went down — power loss, host
maintenance, crash-looping deployment — and nodes are brought back one at a
time:

1. **Early nodes come up degraded.** The process does not exit. S3 requests
   receive `503 Service Unavailable` with a `Retry-After: 5` header, an
   `x-rustfs-readiness-pending` header, and a body naming the blocking
   dependency:

   - `storage_quorum` — waiting for enough nodes/disks for the erasure read
     quorum;
   - `iam` — storage is up, IAM cache is still loading;
   - `startup_finalization` — last startup steps are being published.

2. **Logs say what the node waits for.** The IAM recovery loop retries with
   backoff and logs `event="iam_bootstrap_retry_failed"` with an actionable
   `hint` field (for example, "storage read quorum not met yet; waiting for
   enough cluster nodes/disks to come online"). After repeated failures the
   log level escalates from WARN to ERROR — this still does not kill the
   process.

3. **Recovery is automatic.** As soon as enough peers are online for the
   storage read quorum, the pending nodes finish IAM bootstrap on the next
   retry and flip `/health/ready` to `200` on their own. No manual restart is
   needed, and restarting them does not speed anything up.

4. **Check readiness detail while waiting.** `/health/ready` (and
   `/minio/health/ready`) return per-dependency detail during degradation:

   ```bash
   curl -s http://<node>:9000/health/ready | jq
   ```

   The `details` object shows `storage` / `iam` / `lock` readiness, and
   `degradedReasons` lists machine-readable causes such as
   `storage_quorum_unavailable` or `lock_quorum_unavailable`.

## Tuning

- `RUSTFS_STARTUP_READINESS_MAX_WAIT_SECS` (default `120`): how long startup
  waits for full readiness before continuing in degraded mode with background
  recovery. Raising it delays the listener during genuinely slow starts;
  lowering it surfaces degraded mode sooner. Recovery retries continue
  regardless of this limit.

## What is *not* normal

- A node process **exiting** with a fatal IAM/lock error during startup —
  that fatal path was removed after v1.0.0-beta.5 (rustfs/rustfs#4304);
  upgrade if you still see it.
- A node stuck degraded **after** the whole cluster is back: check network
  reachability between nodes (peer RPC ports) and per-node clocks, then
  inspect `degradedReasons` and the `hint` field of the IAM retry logs.
- A node shown offline in the console with no log output — tracked
  separately, see rustfs/backlog#888.
