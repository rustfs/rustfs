# MinIO ↔ RustFS Router Compatibility (Exceptions Only)

**Use this when:** a client or `mc` call that works against MinIO fails against RustFS and you need to know whether the endpoint is missing, stubbed, or deliberately different.
**Source of truth:** S3 plane: the `s3s::S3` trait impl in `rustfs/src/storage/ecfs.rs`. Admin plane: `make_admin_route` in `rustfs/src/admin/mod.rs`, the registration inventory `rustfs/src/admin/route_registration_test.rs`, and the route/action guardrail [admin-route-action-snapshot.md](admin-route-action-snapshot.md).

This document lists only exceptions. Anything not listed here is implemented with MinIO-equivalent behavior. For the s3tests-level claim see [s3-compatibility-matrix.md](s3-compatibility-matrix.md).

## Status Legend

| Status | Meaning |
|---|---|
| 缺失 (missing) | No RustFS route or handler for the MinIO endpoint. |
| 部分兼容 (partial) | Registered and functional, but a documented subset of MinIO behavior is rejected. |
| 已注册未完成 (registered, incomplete) | Route is registered; the handler returns `NotImplemented` as a behavior contract. |
| 行为不一致 (behavior differs) | Implemented, but intentionally diverges from MinIO's response contract. |

Admin paths are relative to the canonical `/rustfs/admin` prefix; `/minio/admin` is accepted as an alias via router canonicalization.

## S3 Data Plane

All `s3s::S3` trait methods are implemented in `rustfs/src/storage/ecfs.rs` except the following.

| S3 operation | Status | Detail |
|---|---|---|
| GetBucketReplicationMetrics | 缺失 | No `get_bucket_replication_metrics`; replication metrics are exposed via admin `/v3/replicationmetrics`. |
| GetBucketOwnershipControls | 缺失 | No handler; s3tests entries remain in `scripts/s3-tests/unimplemented_tests.txt`. |
| PutBucketOwnershipControls, DeleteBucketOwnershipControls | 缺失 | No handler. |
| DeleteBucketNotification, DeleteBucketLogging, DeleteBucketRequestPayment, DeleteBucketAccelerate | 部分兼容 | No distinct DELETE handlers; clear the config by writing an empty configuration through the PUT path. |
| PutBucketAcl, PutObjectAcl | 部分兼容 | Canned-ACL headers only; XML grant bodies return `NotImplemented` (`put_bucket_acl`, `put_object_acl`). |
| GetObjectTorrent | 行为不一致 | `get_object_torrent` returns `404 NoSuchKey` by design, not `501 NotImplemented`, so clients degrade gracefully. |

## Admin Control Plane

Every route asserted in `rustfs/src/admin/route_registration_test.rs` is registered. Exceptions:

| MinIO admin family | Status | Detail |
|---|---|---|
| Batch jobs (`/v3/start-job`, `/v3/list-jobs`, `/v3/status-job`, `/v3/describe-job`, `/v3/cancel-job`) | 已注册未完成 | `rustfs/src/admin/handlers/batch_job.rs`: `start-job` returns `NotImplemented` for known job types (`KNOWN_JOB_TYPES`) and `InvalidRequest` for unknown ones; `list-jobs` returns an empty list; status/describe/cancel return no-such-job. See [kms-bulk-rekey-contract.md](kms-bulk-rekey-contract.md) for why `keyrotate` must keep refusing. |
| Service control (`POST /v3/service`) | 行为不一致 | `ServiceHandle` in `rustfs/src/admin/handlers/system.rs`: `restart` and `stop` both initiate graceful shutdown (the process manager must relaunch; no in-process restart); `freeze` / `unfreeze` toggle a global freeze flag under `ServiceFreezeAdminAction`. `rustfs/src/admin/route_policy.rs` still classifies the route as deferred `NotImplemented`. |
| Inspect data (`GET|POST /v3/inspect-data`) | 行为不一致 | `InspectDataHandler` in `system.rs` returns the raw bytes of one exact `volume` + `file`, size-capped, instead of MinIO's encrypted raw-drive-file archive. The bounded archive lives at `POST /v4/inspect/archive` (`rustfs/src/admin/handlers/inspect_archive.rs`). `route_policy.rs` still classifies the v3 route as deferred `NotImplemented`. |
| Pools decommission / cancel / clear | 部分兼容 | `rustfs/src/admin/handlers/pools.rs` returns `NotImplemented` when endpoints are not initialized (single-pool or uninitialized clusters). |
| `/v3/top/drives`, `/v3/top/net` | 缺失 | Only `/v3/top/locks` is registered (`rustfs/src/admin/handlers/diagnostics.rs`). |
| Bucket / site replication per-object diff | 缺失 | `/v3/replicationmetrics` and site-replication status exist; no diff endpoint. |
| MRF (most-recent-failures) replication metrics breakdown | 缺失 | Only the generic `/v3/metrics` stream and replication metrics wire (`rustfs/src/admin/replication_metrics_wire.rs`). |

Formerly-missing families that are now registered and therefore not exceptions: `/v3/healthinfo`, `/v3/obdinfo`, `/v3/force-unlock`, `/v3/top/locks`, `/v3/speedtest*`, `/v3/log`, `/v3/trace`, `/v3/profile`, `/v3/profiling/*`, `/v3/idp/{ldap|openid}/*`, `/v3/idp-config/*`.

## Update Rule

When an exception above changes state, edit its row here in the same PR that changes the handler, and extend `rustfs/src/admin/route_registration_test.rs` and [admin-route-action-snapshot.md](admin-route-action-snapshot.md) for admin routes. Do not add "implemented" rows to this document; absence from this list is the implemented claim.
