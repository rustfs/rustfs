# Readiness Matrix

This document records the current request and dependency behavior around
startup readiness. It is a behavior-preservation baseline for architecture
migration work, not a new readiness policy.

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
| KMS compatibility | KMS health compatibility readiness. | Only when the KMS compatibility readiness check is enabled. | KMS startup fatality and health reporting remain separate from pure docs work. |

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
- Node readiness reports local dependency readiness.
- Cluster write readiness requires write quorum and the runtime dependency
  readiness used by `FullReady`.
- Cluster read readiness may use the read-quorum path and cluster-health
  timeout behavior.
- `HEAD` health probes keep header/status semantics and do not require response
  bodies.

## Preservation Rules

- Do not move peer-health checks into the S3 data hot path.
- Do not make peer health affect readiness unless
  `RUSTFS_HEALTH_PEER_READY_CHECK_ENABLE` is enabled.
- Do not use this matrix to change the early HTTP listener plus readiness-gate
  split.
- Do not simplify distributed lock quorum, IAM deferred recovery, or KMS fatal
  boundaries in a documentation or guardrail PR.
