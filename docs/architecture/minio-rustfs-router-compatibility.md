# MinIO ↔ RustFS Router Compatibility Matrix

Tracks how RustFS covers the MinIO HTTP router surface, split into the S3
data-plane router (`cmd/api-router.go` in MinIO: object + bucket APIs) and the
admin control-plane router (`cmd/admin-router.go`: admin `/v3/` and `/v4/`
APIs). Each row records the current RustFS implementation status and the
landing point in the code so the matrix can be re-verified after refactors.

This complements two neighbouring documents and does not duplicate them:

- [s3-compatibility-matrix.md](s3-compatibility-matrix.md) — the release-facing
  S3 compatibility claim and the Ceph s3tests lists that gate it.
- [admin-route-action-snapshot.md](admin-route-action-snapshot.md) — the admin
  route/handler/authorization-action migration guardrail (the source of truth
  for exact route patterns and auth contracts).

Refs rustfs/backlog#596 rustfs/backlog#603.

## Status Legend

| Status | Meaning |
|---|---|
| 已实现 (implemented) | Handler is registered and performs the real operation. |
| 部分兼容 (partial) | Registered and functional, but a documented subset of the MinIO behavior is rejected or unsupported. |
| 已注册未完成 (registered, incomplete) | Route is registered but the handler returns `NotImplemented` (a behavior contract, not a real implementation). |
| 缺失 (missing) | No RustFS route/handler for the MinIO endpoint. |
| 行为不一致 (behavior differs) | Implemented but intentionally diverges from MinIO's response contract. |

Prefixes: RustFS registers admin routes under the canonical `/rustfs/admin`
prefix and accepts `/minio/admin` as a compatibility alias via router
canonicalization (see
[admin-route-action-snapshot.md](admin-route-action-snapshot.md)). Admin paths
below are shown relative to that prefix (e.g. `/v3/info`).

---

## Part 1 — S3 Data Plane (MinIO `cmd/api-router.go`)

RustFS implements the S3 surface through the `s3s` service trait in
`rustfs/src/storage/ecfs.rs`, delegating to use-case layers under
`rustfs/src/app/`. Line numbers are indicative landing points on the branch
this matrix was written against and may drift; the file paths are stable.

### Bucket-level operations

| MinIO / S3 operation | Status | RustFS landing point |
|---|---|---|
| CreateBucket | 已实现 | `rustfs/src/storage/ecfs.rs` (`create_bucket`) |
| DeleteBucket | 已实现 | `rustfs/src/storage/ecfs.rs` (`delete_bucket`) |
| HeadBucket | 已实现 | `rustfs/src/storage/ecfs.rs` (`head_bucket`) |
| ListBuckets | 已实现 | `rustfs/src/storage/ecfs.rs` (`list_buckets`) |
| GetBucketLocation | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_location`) |
| ListObjects (v1) | 已实现 | `rustfs/src/storage/ecfs.rs` (`list_objects`) |
| ListObjectsV2 | 已实现 | `rustfs/src/storage/ecfs.rs` (`list_objects_v2`) |
| ListObjectVersions | 已实现 | `rustfs/src/storage/ecfs.rs` (`list_object_versions`) |
| ListMultipartUploads | 已实现 | `rustfs/src/storage/ecfs.rs` (`list_multipart_uploads`) |
| Get/PutBucketVersioning | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_versioning`, `put_bucket_versioning`) |
| Get/Put/DeleteBucketPolicy | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_policy`, `put_bucket_policy`, `delete_bucket_policy`) |
| GetBucketPolicyStatus | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_policy_status`) |
| Get/Put/DeleteBucketTagging | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_tagging`, `put_bucket_tagging`, `delete_bucket_tagging`) |
| Get/Put/DeleteBucketLifecycle | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_lifecycle_configuration`, `put_bucket_lifecycle_configuration`, `delete_bucket_lifecycle`) |
| Get/Put/DeleteBucketReplication | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_replication`, `put_bucket_replication`, `delete_bucket_replication`) |
| Get/Put/DeleteBucketEncryption | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_encryption`, `put_bucket_encryption`, `delete_bucket_encryption`) |
| Get/PutObjectLockConfiguration | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_object_lock_configuration`, `put_object_lock_configuration`) |
| Get/Put/DeletePublicAccessBlock | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_public_access_block`, `put_public_access_block`, `delete_public_access_block`) |
| Get/Put/DeleteBucketCors | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_cors`, `put_bucket_cors`, `delete_bucket_cors`) |
| GetBucketNotificationConfiguration | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_notification_configuration`) |
| PutBucketNotificationConfiguration | 已实现 | `rustfs/src/storage/ecfs.rs` (`put_bucket_notification_configuration`) |
| Get/PutBucketRequestPayment | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_request_payment`, `put_bucket_request_payment`) |
| Get/PutBucketLogging | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_logging`, `put_bucket_logging`) |
| Get/Put/DeleteBucketWebsite | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_website`, `put_bucket_website`, `delete_bucket_website`) |
| Get/PutBucketAccelerateConfiguration | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_accelerate_configuration`, `put_bucket_accelerate_configuration`) |
| GetBucketAcl | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_bucket_acl`) |
| PutBucketAcl | 部分兼容 | `rustfs/src/storage/ecfs.rs` (`put_bucket_acl`) — canned-ACL headers only; XML grant policies return `NotImplemented`. |
| GetBucketReplicationMetrics | 缺失 | No S3-path handler; replication metrics are exposed via the admin API `/v3/replicationmetrics` instead. |
| GetBucketOwnershipControls | 缺失 | Not implemented (matches the "bucket ownership controls: planned" note in `s3-compatibility-matrix.md`). |
| Put/DeleteBucketOwnershipControls | 缺失 | Not implemented. |

Note on delete verbs: several S3 sub-resource DELETE operations
(DeleteBucketNotification, DeleteBucketLogging, DeleteBucketRequestPayment,
DeleteBucketAccelerate) are not exposed as distinct handlers; the corresponding
config is cleared by writing an empty configuration through the PUT path. Treat
these as 部分兼容 at the client level.

### Object-level operations

| MinIO / S3 operation | Status | RustFS landing point |
|---|---|---|
| GetObject | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_object`) |
| PutObject | 已实现 | `rustfs/src/storage/ecfs.rs` (`put_object`) |
| DeleteObject | 已实现 | `rustfs/src/storage/ecfs.rs` (`delete_object`) |
| DeleteObjects (multi-delete) | 已实现 | `rustfs/src/storage/ecfs.rs` (`delete_objects`) |
| HeadObject | 已实现 | `rustfs/src/storage/ecfs.rs` (`head_object`) |
| CopyObject | 已实现 | `rustfs/src/storage/ecfs.rs` (`copy_object`) |
| GetObjectAcl | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_object_acl`) |
| PutObjectAcl | 部分兼容 | `rustfs/src/storage/ecfs.rs` (`put_object_acl`) — canned-ACL headers only; XML grants return `NotImplemented`. |
| Get/Put/DeleteObjectTagging | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_object_tagging`, `put_object_tagging`, `delete_object_tagging`) |
| GetObjectAttributes | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_object_attributes`) |
| Get/PutObjectLegalHold | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_object_legal_hold`, `put_object_legal_hold`) |
| Get/PutObjectRetention | 已实现 | `rustfs/src/storage/ecfs.rs` (`get_object_retention`, `put_object_retention`) |
| RestoreObject (POST restore) | 已实现 | `rustfs/src/storage/ecfs.rs` (`restore_object`) |
| SelectObjectContent | 已实现 | `rustfs/src/storage/ecfs.rs` (`select_object_content`) → `rustfs/src/app/select_object.rs` |
| CreateMultipartUpload | 已实现 | `rustfs/src/storage/ecfs.rs` (`create_multipart_upload`) |
| UploadPart / UploadPartCopy | 已实现 | `rustfs/src/storage/ecfs.rs` (`upload_part`, `upload_part_copy`) |
| CompleteMultipartUpload | 已实现 | `rustfs/src/storage/ecfs.rs` (`complete_multipart_upload`) |
| AbortMultipartUpload | 已实现 | `rustfs/src/storage/ecfs.rs` (`abort_multipart_upload`) |
| ListParts | 已实现 | `rustfs/src/storage/ecfs.rs` (`list_parts`) |
| PostObject (POST form upload) | 已实现 | Routed via the POST-object marker into the put-object path (`rustfs/src/app/object_usecase.rs`). See the "POST Object form upload checksum handling: planned" note in `s3-compatibility-matrix.md`. |
| GetObjectTorrent | 行为不一致 | `rustfs/src/storage/ecfs.rs` (`get_object_torrent`) — returns `404 NoSuchKey` by design (not `501 NotImplemented`) so clients degrade gracefully. |

For the gate-level view of which of these are covered by executable s3tests,
defer to [s3-compatibility-matrix.md](s3-compatibility-matrix.md); this table is
the router/handler view, not the test-list view.

---

## Part 2 — Admin Control Plane (MinIO `cmd/admin-router.go`)

Router assembly is `rustfs/src/admin/mod.rs::register_admin_routes`; the exact
route patterns, handler ownership, and authorization actions are the guardrail
in [admin-route-action-snapshot.md](admin-route-action-snapshot.md). This table
maps MinIO admin route families to RustFS status.

### Implemented / registered families

| MinIO admin family | Status | RustFS landing point |
|---|---|---|
| STS / is-admin probe | 已实现 | `rustfs/src/admin/handlers/sts.rs`, `is_admin.rs` |
| User lifecycle (list/add/info/remove/status) | 已实现 | `rustfs/src/admin/handlers/user_lifecycle.rs`, `user.rs` |
| Groups | 已实现 | `rustfs/src/admin/handlers/group.rs` |
| Service accounts / access keys | 已实现 | `rustfs/src/admin/handlers/service_account.rs` |
| Canned policies + builtin policy attach/detach + policy-entities | 已实现 | `rustfs/src/admin/handlers/policies.rs` |
| IAM import/export | 已实现 | `rustfs/src/admin/handlers/user_iam.rs`, `user.rs` |
| Account info | 已实现 | `rustfs/src/admin/handlers/account_info.rs` |
| Config KV (get/set/del/help/history/restore + `/v3/config`) | 已实现 | `rustfs/src/admin/handlers/config_admin.rs` |
| Server info / storageinfo / datausageinfo | 已实现 | `rustfs/src/admin/handlers/system.rs` |
| Metrics stream (`/v3/metrics`) | 已实现 | `rustfs/src/admin/handlers/metrics.rs` via `system.rs` |
| Runtime capabilities (`/v4/runtime/capabilities`) | 已实现 | `rustfs/src/admin/handlers/system.rs` |
| Pools list/status | 已实现 | `rustfs/src/admin/handlers/pools.rs` |
| Pools decommission/cancel/clear | 部分兼容 | `rustfs/src/admin/handlers/pools.rs` — returns `NotImplemented` when endpoints are not initialized (single-pool / uninitialized clusters). |
| Rebalance start/status/stop | 已实现 | `rustfs/src/admin/handlers/rebalance.rs` |
| Heal + background-heal status | 已实现 | `rustfs/src/admin/handlers/heal.rs` |
| Tier (list/stats/verify/add/edit/remove/clear) | 已实现 | `rustfs/src/admin/handlers/tier.rs` |
| Quota (legacy + bucket-scoped + stats/check) | 已实现 | `rustfs/src/admin/handlers/quota.rs` |
| Bucket metadata export/import | 已实现 | `rustfs/src/admin/handlers/bucket_meta.rs` |
| Scanner status | 已实现 | `rustfs/src/admin/handlers/scanner.rs` |
| Notification targets (list/arns/put/reset) | 已实现 | `rustfs/src/admin/handlers/event.rs` |
| Audit targets (list/put/reset) | 已实现 | `rustfs/src/admin/handlers/audit.rs` |
| Module switches | 已实现 | `rustfs/src/admin/handlers/module_switch.rs` |
| Plugin catalog + instances (`/v4/plugins/*`) | 已实现 | `rustfs/src/admin/handlers/plugins_catalog.rs`, `plugins_instances.rs` |
| Extension catalog + instances (`/v4/extensions/*`) | 已实现 | `rustfs/src/admin/handlers/extensions.rs` |
| Object ZIP download (`/v3/zip-downloads`) | 已实现 | `rustfs/src/admin/handlers/object_zip_download.rs` |
| Cluster snapshot (`/v4/cluster/snapshot`) | 已实现 | `rustfs/src/admin/handlers/cluster_snapshot.rs` |
| Bucket-level remote targets (list/metrics/set/remove) | 已实现 | `rustfs/src/admin/handlers/replication.rs` |
| Site replication (add/remove/info/status/peer/resync + devnull/netperf) | 已实现 | `rustfs/src/admin/handlers/site_replication.rs` |
| Admin profiling (`/debug/pprof/profile`, `/debug/pprof/status`) | 已实现 | `rustfs/src/admin/handlers/profile_admin.rs`, `profile.rs` |
| TLS debug (`/debug/tls/status`) | 已实现 | `rustfs/src/admin/handlers/tls_debug.rs`, `profile.rs` |
| KMS management / dynamic / keys | 已实现 | `rustfs/src/admin/handlers/kms_management.rs`, `kms_dynamic.rs`, `kms_keys.rs` |
| OIDC public + config | 已实现 | `rustfs/src/admin/handlers/oidc.rs` |
| Table catalog (Iceberg) | 已实现 | `rustfs/src/admin/handlers/table_catalog.rs` |

### Registered-but-incomplete

| MinIO admin family | Status | RustFS landing point |
|---|---|---|
| Service restart/stop (`POST /v3/service`) | 已注册未完成 | `rustfs/src/admin/handlers/system.rs` — handler returns `NotImplemented`. |
| Inspect data (`GET|POST /v3/inspect-data`) | 已注册未完成 | `rustfs/src/admin/handlers/system.rs` — handler returns `NotImplemented`. |

These registered-but-`NotImplemented` routes are behavior contracts; per the
migration rules in [admin-route-action-snapshot.md](admin-route-action-snapshot.md),
implementing or removing them is a behavior-change PR.

---

## Gaps Only — Missing Admin Endpoints (follow-up checklist)

The following MinIO admin `/v3/` route families have **no** RustFS registration
today. This is the actionable checklist for closing admin-API parity. Verified
against `rustfs/src/admin/mod.rs` and `rustfs/src/admin/handlers/` on the branch
this doc was written on.

- [ ] **Server profiling start/stop** — MinIO `/v3/profile` (bulk profiling
  session). RustFS only exposes `/debug/pprof/profile` and
  `/debug/pprof/status`, which are a different, single-shot pprof surface.
- [ ] **Health info** — MinIO `/v3/healthinfo` (cluster health report / subnet
  diagnostics). No RustFS route.
- [ ] **LDAP / generic IDP config CRUD** — MinIO `/v3/idp/{ldap|openid}/...`
  config management. RustFS exposes OIDC config under `/v3/oidc/*` only; there
  is no LDAP IDP config route.
- [ ] **Bucket / site replication diff** — MinIO replication-diff endpoints.
  RustFS exposes `/v3/replicationmetrics` (metrics) and site-replication
  status, but no per-object diff.
- [ ] **MRF metrics** — MinIO's most-recent-failures replication metrics
  breakdown. RustFS has only the generic `/v3/metrics` stream.
- [ ] **Batch jobs** — MinIO `/v3/batch`, `/v3/list-batch-jobs`, job
  describe/cancel. No RustFS batch API.
- [ ] **Distributed locks introspection** — MinIO `/v3/force-unlock` and
  `/v3/top/locks`. No RustFS locks-management API.
- [ ] **Speedtest / perf** — MinIO `/v3/speedtest` (object/drive/net perf).
  RustFS has `netperf`/`devnull` **only** inside the site-replication family,
  not as standalone admin speedtest endpoints.
- [ ] **Console log stream** — MinIO `/v3/log` (kstream / log search). No RustFS
  route.
- [ ] **Top introspection** — MinIO `/v3/top/locks`, `/v3/top/drives`,
  `/v3/top/net`. No RustFS unified `top` family.
- [ ] **Trace stream** — MinIO `/v3/trace`. A `trace.rs` handler skeleton
  exists under `rustfs/src/admin/handlers/` but its registration function is
  **not** called from `register_admin_routes`, so no route is live.

When one of these lands, register it in `rustfs/src/admin/mod.rs`, extend
`rustfs/src/admin/route_registration_test.rs`, update
[admin-route-action-snapshot.md](admin-route-action-snapshot.md) with the
route/handler/action rows, and move the item out of this checklist.
