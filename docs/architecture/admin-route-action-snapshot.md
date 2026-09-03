# Admin Route Action Snapshot

**Use this when:** you add, move, or re-authorize an admin route and need to know where the route → handler → `AdminAction` contract is enforced.
**Source of truth:** `rustfs/src/admin/route_policy.rs` (the `AdminRouteSpec` matrix, checked by `validate_admin_route_policy_specs`), `rustfs/src/admin/route_registration_test.rs` (registration coverage), `rustfs/src/admin/router.rs` (dispatch and credential checks), `rustfs/src/admin/handlers/*.rs` (handler-level authorization calls).

This page is a pointer, not a route table. The machine-checked matrix in `route_policy.rs` lists every admin route with its `AdminAction` and `RouteRiskLevel`; routes that are registered but answered by policy instead of a handler are declared there too through `DeferredRoutePolicyReason`. The `AdminRouteSpec` type lives in `crates/security-governance/src/admin_matrix.rs`.

## Prefix And Alias Contract

| Prefix | Behavior | Rule |
|---|---|---|
| `/rustfs/admin` | Canonical admin prefix used by `make_admin_route` (`rustfs/src/admin/mod.rs`) | The only registered admin prefix |
| `/minio/admin` | Compatibility alias accepted by `S3Router::is_match`; `canonicalize_admin_path` rewrites it to `/rustfs/admin` immediately before route lookup (`rustfs/src/admin/router.rs`) | Never register routes twice; preserve canonicalization |
| `/iceberg/v1` | Table catalog prefix registered by `register_table_catalog_route` (`rustfs/src/admin/handlers/table_catalog/routes.rs`) and accepted by `is_admin_path` | Stays outside `/rustfs/admin`; table actions are authorized per handler |
| `/health`, `/health/ready` | Public health endpoints, registered only when `ENV_HEALTH_ENDPOINT_ENABLE` allows | Preserve the unauthenticated bypass |
| `/profile/cpu`, `/profile/memory` | Registered by the health handler but guarded by profile authorization | Never couple to health-endpoint enablement |

## Public Exceptions

Router-level credential checks (`S3Router::check_access`) are bypassed only for:

- health routes, when they are registered;
- OIDC bootstrap paths matched by `is_oidc_path` (`providers`, `authorize/{provider_id}`, `callback/{provider_id}`, `logout`); the bypass is path-based, so it applies to any method on those paths;
- unsigned STS web-identity form posts to `/` with `application/x-www-form-urlencoded`, which the STS handler validates itself;
- console assets (`/favicon.ico`, `/rustfs/console...`), only while the console is enabled.

Every other admin route requires credentials at the router and a precise `AdminAction` or `S3Action` check in the handler (metrics routes, for example, authorize `GetMetricsAction`). The MinIO alias contract is specified in [minio-rustfs-router-compatibility.md](minio-rustfs-router-compatibility.md).

## Gated Bucket Feature Routes

Some bucket-scoped routes add gates after the `AdminAction` check. The gates are enforced in the handler, so they are invisible to the route matrix and listed here instead.

| Route | Actions | Extra gates after authorization |
|---|---|---|
| `PUT`/`DELETE /rustfs/admin/v3/on-demand-migration/{bucket}` (`?dry-run=true` validates and probes without saving) | `SetBucketOnDemandMigrationAction` (`admin:SetBucketOnDemandMigration`) | bucket must exist (`NoSuchBucket`); `PUT` also requires the `RUSTFS_ON_DEMAND_MIGRATION_ENABLED` module switch (`OnDemandMigrationDisabled`, 400) and the server license (`license_check()`, same mapping as object zip downloads); the source must answer HEAD + a one-key list (`OnDemandMigrationSourceUnreachable`, 400). Handler: `rustfs/src/admin/handlers/on_demand_migration.rs` |
| `GET /rustfs/admin/v3/on-demand-migration/{bucket}` and `GET .../{bucket}/status` | `GetBucketOnDemandMigrationAction` (`admin:GetBucketOnDemandMigration`) | bucket must exist; reads work while the module switch is off so operators can inspect a disabled deployment; `GET` answers `NoSuchConfiguration` (404) when nothing is configured; `status` carries a `backfill` summary of the latest backfill job when one was recorded |
| `POST /rustfs/admin/v3/on-demand-migration/{bucket}/backfill?op=start\|cancel` (body `{prefix?, skip_existing?, dry_run?}` for `start`) | `SetBucketOnDemandMigrationAction` (`admin:SetBucketOnDemandMigration`) | bucket must exist; `op` must be `start` or `cancel` (`InvalidArgument`, 400); `start` also requires the module switch (`OnDemandMigrationDisabled`, 400), the server license, a saved config (`NoSuchConfiguration`, 404) and an enabled bucket state on this node (`OnDemandMigrationDisabled`, 400); a job holding its lease answers `OnDemandMigrationBackfillRunning` (409); `cancel` answers `NoSuchBackfillJob` (404) when the bucket never had a job. Handler: `rustfs/src/admin/handlers/on_demand_migration.rs` |
| `GET /rustfs/admin/v3/on-demand-migration/{bucket}/backfill` | `GetBucketOnDemandMigrationAction` (`admin:GetBucketOnDemandMigration`) | bucket must exist; returns the backfill checkpoint document (`failed_keys` are key hashes, no credentials); `NoSuchBackfillJob` (404) when the bucket never had a job |

Responses on the config routes carry the redacted configuration (`secret_key` and `session_token` replaced by `REDACTED`); the wire shapes of every route in this group, including the backfill checkpoint and the `status` summary, are pinned by the fixtures under `crates/madmin/fixtures/on_demand_migration/`, shared by the server handler tests and the `rustfs-madmin` client tests.
