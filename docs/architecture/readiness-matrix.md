# Readiness Matrix

**Use this when:** changing what a request surface does before storage or IAM is ready, changing probe semantics, or adding a runtime dependency that readiness must wait for.
**Source of truth:** `rustfs/src/server/readiness.rs` (probe paths, `Retry-After`), `crates/common/src/readiness.rs` (`StorageReady`, `IamReady`, `FullReady`), `crates/config/src/constants/health.rs` (`RUSTFS_HEALTH_*` gates). This matrix is a behavior-preservation baseline, not a new readiness policy.

## Request Behavior Matrix

| Surface | Path examples | Before `FullReady` | After `FullReady` | Dependency notes |
|---|---|---|---|---|
| Health probe | `/health`, `/health/live`, `/health/ready`, `/minio/health/*` | Bypasses the HTTP readiness gate and returns probe-specific liveness or readiness state. | Same path-specific probe behavior. | Probe handlers compute health independently of the outer request gate. |
| Admin and console | `/admin/*`, `/console/*`, `/rustfs/admin/*`, `/minio/admin/*` | Bypasses the outer readiness gate; route auth, handler setup, and handler-specific dependencies still apply. | Same handler behavior without outer gate rejection. | Do not use admin bypasses as proof that storage, IAM, or lock quorum is ready. |
| Internode RPC and gRPC | `/rustfs/rpc/*`, `/minio/rpc/*`, tonic routes | Bypasses the HTTP readiness gate so control-plane peers can communicate during startup. | Same RPC routing behavior. | RPC signature, auth, transport, and handler failures remain authoritative. |
| Table catalog | `/iceberg/*`, `/v1/*`, table-catalog routes | Bypasses the outer readiness gate when the route is registered. | Same registered route behavior. | Catalog handlers keep their own dependency checks. |
| S3 data plane | Bucket/object S3 API routes | Receives `503 Service Unavailable` from the readiness gate with `Retry-After: 5`. | Routed to S3 handlers. | This is the main data-plane behavior protected by `FullReady`. |
| Optional sidecars | FTP, FTPS, SFTP, WebDAV | Governed by protocol-specific startup and shutdown handling. | Governed by protocol-specific runtime handling. | These are not HTTP readiness-gate surfaces. |

## Runtime Dependency Matrix

| Dependency | Ready signal | Blocks `FullReady` | Notes |
|---|---|---|---|
| StorageReady | Storage/global config readiness publication plus runtime storage checks. | Yes. | Startup can mark the stage before later runtime readiness rechecks storage. |
| IamReady | Inline IAM bootstrap or deferred IAM recovery publication. | Yes. | Deferred recovery can publish IAM readiness after HTTP has already started. |
| Lock quorum | Per-set write quorum readiness. | Yes. | Do not replace the distributed lock quorum check with node count or endpoint count. |
| Peer health | `peer_health_ready` runtime status. | Only when `RUSTFS_HEALTH_PEER_READY_CHECK_ENABLE` is enabled. | The gate is disabled by default; unknown peer health degrades readiness only when enabled. |
| KMS compatibility | KMS health compatibility readiness. | Only when `RUSTFS_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE` is enabled (default off; `crates/config/src/constants/health.rs`). | When enabled, `/health/ready` additionally requires the KMS service to be running if a global KMS manager exists. KMS startup fatality and health reporting remain separate from pure docs work. |

Effective `FullReady` is:

```text
storage_ready && iam_ready && lock_quorum_ready && peer_health_ready
```

`peer_health_ready` is true by default unless
`RUSTFS_HEALTH_PEER_READY_CHECK_ENABLE` is enabled. When that flag is enabled,
an unknown or unsupported peer-health snapshot degrades readiness with
`peer_health_unavailable`.

## Probe Semantics

- Liveness reports process availability and must not depend on storage, IAM,
  lock quorum, or peer health.
- Node readiness reports the node's observed storage write quorum, local pool metadata write gate, IAM, and lock quorum. Runtime storage diagnostics do not change the existing startup `FullReady` publication gate or S3 request admission.
- A blocked pool metadata writer degrades node and cluster-write readiness with
  `pool_meta_write_blocked`. Metadata save-gate inspection is bounded to 100 ms;
  contention reports `pool_metadata_check_timeout` without installing a block.
- The authenticated cluster snapshot extends its existing node-local metadata
  gate inspection with safe reason, failure phase, and original block time. It
  distinguishes timeout from a block and changes no admission or recovery
  decision. Runtime readiness and gate status are separate bounded observations.
- Cluster write readiness requires write quorum and the runtime dependency
  readiness used by `FullReady`.
- Cluster read readiness uses the storage read-quorum path and cluster-health timeout behavior. Its lock dependency still uses the per-set majority/write-lock health check. Actual shared namespace locks require `ceil(lock_clients / 2)`, so the cluster read probe is conservative: a four-client set can still admit some reads with two clients while the probe returns 503. This diagnostic change does not lower that probe's lock threshold.
- `HEAD` health probes keep header/status semantics and do not require response
  bodies.

### Storage Detail Contract

The existing `details.storage.ready` boolean and `connected` / `disconnected` status values are retained. `readinessScope` states the condition they summarize:

| Probe | `readinessScope` | `source` |
| --- | --- | --- |
| `/health/ready`, `/minio/health/ready` | `write_quorum_and_pool_metadata` | `local_runtime` |
| `/minio/health/cluster` | `write_quorum_and_pool_metadata` | `storage_inventory` |
| `/minio/health/cluster/read` | `read_quorum` | `storage_inventory` |

Node readiness additionally reports `details.storage.readQuorum`, `details.storage.writeQuorum`, and `details.poolMetadata.ready`. The metadata component's status is `writable` or `unavailable`; existing typed degradation reasons distinguish a write block from an inspection timeout. A healthy metadata writer alone no longer makes the storage component ready.

Node storage quorum uses configured drives per set, all configured pools/sets, and their Standard storage-class data/parity layout. Missing, duplicate, unreachable, or unhealthy disk observations cannot supply extra quorum votes. The read quorum is the data-drive count; the write quorum is that count plus one when data and parity counts are equal. These are observations of available storage slots, not guarantees that a particular object's metadata, shards, or required locks are available.

For a healthy IAM and metadata writer in a four-node, one-drive-per-node EC 2+2 set:

| Surviving nodes | Storage read quorum | Storage write quorum | Pool metadata ready | Node HTTP / top-level ready |
| --- | --- | --- | --- | --- |
| 4 or 3 | true | true | true | 200 / true |
| 2 | true | false | true | 503 / false |
| 1 | false | false | true | 503 / false |
| All restored | true | true | true | 200 / true |

The node path reads local disk-handle health and reuses the same reachable-host observation as its lock dependency, including the existing `RUSTFS_HEALTH_READINESS_CACHE_TTL_MS` cache. Only `Online` drives count; a reachable host with a `Returning` drive does not yet prove data I/O has recovered. It does not call cluster `storage_info`, local `disk_info`, or add disk-info RPCs. The entire storage inventory snapshot has a separate 100 ms wait budget; expiry reports `storage_readiness_check_timeout` and fails closed. Pool metadata inspection retains its own 100 ms budget. These observations are not an atomic cluster snapshot and do not bypass the existing lock-probe timing or cache policy.

The new fields are additive. Their absence in an older response is not evidence of storage quorum. Minimal responses still contain only the existing top-level fields, liveness remains dependency-independent, and HEAD responses remain bodyless.

## Preservation Rules

- Do not move peer-health checks into the S3 data hot path.
- Do not make peer health affect readiness unless
  `RUSTFS_HEALTH_PEER_READY_CHECK_ENABLE` is enabled.
- Do not use this matrix to change the early HTTP listener plus readiness-gate
  split.
- Do not simplify distributed lock quorum, IAM deferred recovery, or KMS fatal
  boundaries in a documentation or guardrail PR.
