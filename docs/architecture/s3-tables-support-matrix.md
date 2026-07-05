# S3 Tables Support Matrix

This matrix records the RustFS S3 Tables surfaces that are supported,
previewed, referenced, or intentionally not claimed. It is the release-facing
boundary for the Iceberg REST Catalog work in RustFS.

RustFS S3 Tables is an Iceberg REST Catalog and table-bucket implementation on
top of the RustFS S3 data plane. This document does not claim full parity with
the AWS S3 Tables control-plane API or with every vendor-specific Iceberg
catalog extension.

## Status Labels

| Label | Meaning |
|---|---|
| Automated | Covered by a runnable RustFS script or server test. |
| Manual/live harness | RustFS can generate pinned client package inputs, commands, expected outputs, and CI opt-in gates for a live endpoint, but the live run is not enabled by default in CI. |
| Generated harness | RustFS can generate client configuration or probe input, but live execution is not automated in CI. |
| Supported | Implemented server-side and covered by focused RustFS tests. |
| Preview / controlled | Implemented behind explicit operator action or a run-once endpoint. No automatic background claim is made. |
| Documented, not automated | Configuration or behavior is documented, but the live client run is not automated. |
| Reference only | Kept as a compatibility reference. RustFS does not claim live interoperability yet. |
| Not claimed | Out of scope for the current S3 Tables implementation. |

## Endpoint And Profile Matrix

| Surface | Status | Notes |
|---|---|---|
| `/iceberg/v1` | Supported | Canonical RustFS Iceberg REST Catalog prefix. Default REST signing name is `s3`. |
| `/_iceberg/v1` | Supported compatibility alias | MinIO AIStor-style alias. The smoke profile defaults to REST signing name `s3tables`. |
| S3 object data plane | Supported | Data, metadata, manifest, and delete files remain ordinary S3 objects, with table-aware policy checks for table warehouse paths. |
| Table bucket enablement | Supported | A regular RustFS bucket can be enabled for table catalog use and then addressed as the REST catalog warehouse. |
| Catalog-vended table credentials | Automated when enabled | Disabled by default. When enabled, the credentials endpoint returns short-lived table-scoped S3 credentials. |
| AWS S3 Tables endpoint shape | Profile generator | Generates the AWS catalog URI and S3 Tables warehouse ARN shape for migration docs. Full AWS S3 Tables API parity is not claimed. |
| MinIO AIStor Tables profile | Profile generator plus RustFS alias smoke | RustFS exposes the alias shape, but does not claim all AIStor private extensions. |
| Cloudflare R2 Data Catalog profile | Profile generator | Generates the catalog URI and warehouse-name shape for migration docs. Live RustFS interoperability is not claimed. |
| Alibaba OSS Tables profile | Profile generator | Generates provider endpoint and warehouse shapes for migration docs. Live RustFS interoperability is not claimed. |

## Client And Engine Matrix

| Client or engine | Status | Current RustFS claim |
|---|---|---|
| PyIceberg | Automated | Creates namespace and table, appends rows, reloads, scans, probes metadata-location, refs, views, maintenance, diagnostics, and optional catalog-vended table credentials with an exact-prefix data-plane scope check. |
| Spark Iceberg REST catalog | Manual/live harness | RustFS can generate pinned Spark/Iceberg package inputs, REST catalog properties, SQL, run commands, expected `row_count=2`, and a CI opt-in gate for namespace creation, table creation, append, refresh, count, and cleanup. Live Spark execution and commit-conflict probing are still manual validation items unless explicitly enabled in the runner. |
| Trino Iceberg REST catalog | Manual/live harness | RustFS can generate catalog properties and a read-only `SELECT COUNT(*)` command for a table created by PyIceberg or Spark. Write compatibility is not claimed. |
| DuckDB Iceberg | Manual/live harness | RustFS can generate `httpfs` and `iceberg` SQL using an operator-supplied current metadata location. Write and commit compatibility are not claimed. |
| StarRocks Iceberg REST catalog | Documented, not automated | External catalog read-path reference only. Write compatibility is not claimed. |
| Databend | Manual/live harness | RustFS can generate an S3 stage read probe for table data files. RustFS does not claim Databend Iceberg REST Catalog integration yet. |
| Snowflake Open Catalog / Iceberg integrations | Generated harness | RustFS can generate an operator-adapted external volume/catalog SQL template. Live RustFS interoperability is not claimed. |

## Live Evidence And Operations Matrix

| Area | Status | Current RustFS claim |
|---|---|---|
| Live conformance evidence template | Generated harness | `engine_compatibility.py --print-live-conformance` records required run metadata, per-client result rows, and claim promotion rules before a manual/live result can expand compatibility wording. |
| Production operations guide | Generated harness | `engine_compatibility.py --print-operations-guide` records command, evidence, pass criteria, and fail-closed signals for live conformance, durable backing cutover, maintenance, recovery, permissions, credential vending, and unsupported-claim governance. |
| Client claim promotion | Documented, not automated | PyIceberg remains the automated claim. Spark can be promoted only with recorded manual/live evidence; Trino and DuckDB read probes do not promote write compatibility; Snowflake and vendor profiles remain reference-only without repeatable live evidence. |

## Catalog API Matrix

| Area | Status | Covered behavior |
|---|---|---|
| Catalog config | Supported | `GET /v1/config` advertises RustFS catalog defaults and route capabilities. |
| Table bucket discovery | Supported | `PUT` and `GET /v1/buckets/{warehouse}` enable and inspect table bucket state. |
| Namespaces | Supported | Create, list, load, existence check, and drop namespace routes are registered on both catalog prefixes. |
| Tables | Supported | Create, register, list, load, existence check, commit, metadata-location get/update, and drop table routes are registered on both catalog prefixes. |
| Commit CAS | Supported | Single-table commits validate base metadata, expected version token, referenced object existence, warehouse scope, and Iceberg commit requirements before advancing the current metadata pointer. |
| Commit recovery | Supported | Commit log, idempotency lookup, diagnostics, and recovery routes expose staged/finalization gaps and repair safe idempotency gaps without moving the table pointer. |
| Snapshot refs | Supported | Refs can be listed, created or replaced, and deleted through catalog commits. `main` is protected and refs with explicit retention require forced delete. |
| Iceberg views | Supported | Basic create, list, load, replace, existence check, and drop routes persist view metadata with view-scoped authorization. |
| Table credentials endpoint | Supported | Returns an empty `storage-credentials` list by default. Returns table-scoped temporary credentials only when credential vending is enabled. |
| Catalog diagnostics and export | Supported | Exposes recovery state, consistency state, backing manifest, recoverable commit-log WAL state, strong backing migration target, single-active-writer policy, and scale validation matrix. |
| Catalog import and rollback | Supported | Import/register and rollback use catalog validation and commit paths rather than direct pointer mutation. |
| External catalog bridge | Supported operator path | Operator-supplied metadata pointer sync/import is supported for external catalog identity boundaries. Online vendor SDK polling and policy mirroring are not claimed. |
| Multi-table transactions | Not claimed | RustFS currently claims single-table commit atomicity only. |

## Data Plane And Credential Matrix

| Area | Status | Covered behavior |
|---|---|---|
| Table-aware S3 policy bridge | Supported | Ordinary S3 actions against table warehouse paths are checked through the table data-plane bridge so table policy cannot be bypassed by direct object access. |
| Reserved catalog protection | Supported | Catalog-reserved internal prefixes are protected from ordinary object mutation. |
| Static S3 credentials | Automated | The default PyIceberg smoke path uses configured S3 credentials for REST signing and object data-plane access. |
| Catalog-vended credentials | Automated when enabled | `rustfs-vended-credentials` verifies the returned table prefix, then checks `PutObject`, `HeadObject`, `GetObject`, and `DeleteObject` inside the prefix and denies access outside the prefix. |
| Credential lifetime | Supported | Vended credential TTL is server-side and clamped to a short-lived range. |
| No-long-term-data-credential bootstrap | Not claimed | The current credential-vending flow still uses the configured principal for catalog setup before table-scoped credentials are requested. |

## Maintenance Matrix

| Capability | Status | Current RustFS claim |
|---|---|---|
| Metadata retention dry-run | Supported | Reports retained metadata and deletion candidates without moving the table pointer. |
| Metadata cleanup delete | Supported | Deletes only candidates that pass the safety window and current-pointer checks. |
| Snapshot expiration planning | Supported | Produces expiration plans with retained and candidate snapshots. |
| Snapshot expiration commit | Preview / controlled | Can manually commit safe snapshot expiration through the catalog. Stale plans fail closed. |
| Manifest/data/delete reachability cleanup | Supported | Reads manifest-list and manifest Avro references, reports reachable objects, and deletes only unreferenced table objects that pass the safety window. |
| Maintenance scheduler run endpoint | Preview / controlled | Lets an external scheduler durably queue one maintenance job per table, reuse an active queued job, and recover expired queued leases before requeuing. |
| Maintenance worker run endpoint | Preview / controlled | Supports queued-job claim, run-once execution, current-job backpressure, retry deferral, lease expiry recovery, and heartbeat updates. |
| Maintenance scheduler guardrails | Preview / controlled | Exposes disabled, paused, ready, queued-job handoff, active-job backpressure, retry deferral, quarantine boundary, recommended actions, and recent maintenance job audit timeline state for external schedulers and operators. |
| Maintenance audit events | Preview / controlled | Job reports and scheduler job summaries include structured audit events for planning, worker transitions, heartbeats, lease expiry recovery, and mutating quarantine operations. |
| Maintenance quarantine operations | Preview / controlled | Lets operators inspect, release, retry, or abandon the current quarantined maintenance job without moving the table pointer. |
| Compaction planning | Preview / controlled | Plans partition-local and sort-order-local binpack candidates for Parquet files and does not mix data files from different partition directories or sort orders in one rewrite group. |
| Delete-file or row-level compaction planning | Preview / controlled | Manifests with position or equality delete files produce machine-readable row-level planning and force the compaction report into manual review before any rewrite can run. |
| Compaction commit | Preview / controlled | Can commit a safe partition-local Parquet rewrite through the catalog while preserving Iceberg data file sort order IDs in the rewritten manifest. |
| Built-in periodic scheduler | Not claimed | Operators can trigger scheduler and worker ticks, but continuous in-process scheduling is not claimed. |
| Delete-file or row-level compaction execution | Not claimed | RustFS does not rewrite delete files or execute row-level compaction; those cases remain manual-review maintenance items. |

## Recovery And Strong Backing Matrix

| Area | Status | Current RustFS claim |
|---|---|---|
| Single-table CAS | Supported | The table pointer advances only through expected-token and expected-metadata-location validation. |
| Idempotent retry | Supported | Repeated commit IDs can return the already finalized result or surface recoverable finalization gaps. |
| Post-CAS finalization recovery | Supported | Diagnostics and recovery can repair stale or missing idempotency indexes without changing the current table pointer. |
| Catalog export | Supported | Exposes table state, commit recovery state, and backing migration information for operator inspection. |
| Strong backing migration contract | Supported as a contract | Diagnostics publish object-backed manifest state, recoverable commit-log WAL state, target backing type, replay requirements, and blockers. |
| Durable backing migration dry-run | Supported | `GET /iceberg/v1/{warehouse}/catalog/migration` and the `/_iceberg/v1` alias inspect object-backed catalog inventory, commit recovery blockers, idempotency indexes, warehouse prefix index readiness, recommended actions, and rollback configuration without mutating catalog state. |
| Disaster recovery rehearsal | Manual/live harness | `failure_coverage.py --print-disaster-recovery-rehearsal` generates an operator runbook covering catalog export, diagnostics, safe recovery repair, rollback/import, durable backing migration dry-run, post-recovery loadTable, and table data-plane policy probes. |
| Strong KV/WAL backing cutover | Preview / controlled | Operators can explicitly select durable strong backing with `RUSTFS_TABLE_CATALOG_BACKING=durable-strong`. Run the migration dry-run first and keep the object-backed catalog available for rollback. Object-only advanced operations fail closed in durable strong mode. |
| Single active writer region | Supported policy | Diagnostics publish single-active-writer semantics and read-only replica limits. |
| Active-active multi-region writes | Not claimed | A table must not accept independent concurrent writers in multiple active regions. |

## Durable Backing Cutover Runbook

Use the migration dry-run before changing the table catalog backing for a
warehouse:

1. Run `GET /iceberg/v1/{warehouse}/catalog/migration` with a principal that has
   `GetTableCatalogAction` on the table bucket.
2. Treat any `blockers` entry as fail-closed. Run catalog recovery first when
   commit recovery or idempotency repair is required, and backfill the warehouse
   prefix index before switching backing modes.
3. Take an object-backed catalog snapshot or backup before setting
   `RUSTFS_TABLE_CATALOG_BACKING=durable-strong`.
4. Restart with durable strong backing enabled, then verify catalog config,
   table load, commit recovery diagnostics, and table data-plane policy
   resolution for representative tables.
5. To roll back, restore `RUSTFS_TABLE_CATALOG_BACKING=object-backed` and restart
   while preserving the object-backed catalog objects. Do not delete object-backed
   catalog state until durable strong backing has passed the operator's retention
   window.

## Production Failure Coverage

Positive client smoke proves a client can use a table. Production failure probes
prove RustFS does not silently advance table state when a failure happens.

The tracked failure cases are:

- stale commit token or stale base metadata returns a conflict without advancing
  the table pointer
- missing metadata, manifest, data, or delete objects fail closed before commit
  or maintenance can advance state
- concurrent writers produce a single winning CAS and retryable conflicts for
  stale writers
- table catalog and ordinary S3 permission denials prevent data-plane bypass
- stale maintenance plans fail closed before object deletion or catalog commit
- post-CAS finalization gaps are visible through diagnostics and safe recovery
- external catalog sync conflicts leave pointer, token, and generation unchanged
- backing migration remains blocked until WAL and recovery replay are clean

Do not promote a failure case from a required live probe or load test to an
automated claim until the exact RustFS build, client version, and expected
response shape are recorded.

## Unsupported Or Not Claimed

RustFS does not currently claim:

- full AWS S3 Tables control-plane API parity
- full MinIO AIStor Tables private extension parity
- full Cloudflare R2 Data Catalog interoperability
- full Alibaba OSS Tables interoperability
- built-in periodic maintenance scheduling; external schedulers can queue maintenance jobs and workers can claim them, but RustFS does not claim a continuous in-process scheduler
- active-active multi-region table writes
- multi-table transactions
- no-long-term-data-credential table bootstrap
- online external catalog vendor SDK polling
- external catalog policy mirroring
- delete-file rewrite or row-level compaction execution
- built-in SQL query execution
- Delta Lake or Hudi table format support
- end-to-end SQL row-level DML validation through Spark, Trino, or another SQL engine

## Verification Commands

Use these commands when updating this matrix, release notes, or client
compatibility claims:

```bash
python3 scripts/table-catalog/test_pyiceberg_smoke.py
python3 scripts/table-catalog/test_engine_compatibility.py
python3 scripts/table-catalog/test_failure_coverage.py
python3 scripts/table-catalog/pyiceberg_smoke.py --print-client-matrix
python3 scripts/table-catalog/pyiceberg_smoke.py --print-engine-compatibility
python3 scripts/table-catalog/pyiceberg_smoke.py --print-production-failure-coverage
python3 scripts/table-catalog/pyiceberg_smoke.py --print-vendor-profiles
python3 scripts/table-catalog/pyiceberg_smoke.py --print-production-readiness
python3 scripts/table-catalog/engine_compatibility.py --print-spark-config
python3 scripts/table-catalog/engine_compatibility.py \
  --profile aws-s3tables \
  --region us-east-1 \
  --account-id 123456789012 \
  --table-bucket analytics \
  --print-spark-config
python3 scripts/table-catalog/engine_compatibility.py \
  --metadata-location s3://rustfs-s3table-smoke/tables/table-id/metadata/v1.metadata.json \
  --print-live-conformance \
  --cleanup
python3 scripts/table-catalog/engine_compatibility.py \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --print-operations-guide
python3 scripts/table-catalog/failure_coverage.py \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --print-failure-probes
python3 scripts/table-catalog/failure_coverage.py \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --table-warehouse-location s3://rustfs-s3table-smoke/tables/table-id \
  --print-disaster-recovery-rehearsal
```

## Release Claim Guidance

Use conservative release wording that matches the matrix.

Acceptable wording:

> RustFS includes a core Iceberg REST Catalog-based S3 Tables implementation
> with PyIceberg smoke coverage, table-aware S3 data-plane policy checks,
> controlled maintenance, catalog recovery diagnostics, manual conformance
> input for Spark, Trino, DuckDB, Databend, and Snowflake, production-failure
> probe harnesses, disaster-recovery rehearsal probes, and a machine-readable
> production operations evidence guide.

Do not claim:

> RustFS is fully compatible with AWS S3 Tables.

Any stronger vendor or engine claim needs a repeatable live validation harness,
the exact client versions used, and the expected response shapes recorded in the
table-catalog inventories.

## Related

- [Table catalog conformance scripts](../../scripts/table-catalog/README.md)
- [Admin route action snapshot](admin-route-action-snapshot.md)
- [Runtime capability contracts](runtime-capability-contracts.md)
