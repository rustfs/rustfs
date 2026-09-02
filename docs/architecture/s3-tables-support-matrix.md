# S3 Tables Support Matrix

**Use this when:** writing a release note, README claim, or client-compatibility statement about RustFS S3 Tables / Iceberg REST Catalog, or deciding whether a feature is supported, preview, or not claimed.
**Source of truth:** the table-catalog handlers under `rustfs/src/admin/handlers/table_catalog/`; the conformance scripts and their README in `scripts/table-catalog/`; the durable-backing cutover procedure in [docs/operations/s3-tables-cutover-runbook.md](../operations/s3-tables-cutover-runbook.md).

RustFS S3 Tables is an Iceberg REST Catalog and table-bucket implementation on top of the RustFS S3 data plane. It does not claim parity with the AWS S3 Tables control-plane API or with vendor-specific Iceberg catalog extensions.

## Status Labels

| Label | Meaning |
|---|---|
| Automated | Covered by a runnable RustFS script or server test. |
| Manual/live harness | RustFS generates pinned client inputs, commands, expected outputs, and CI opt-in gates for a live endpoint; the live run is not enabled by default in CI. |
| Generated harness | RustFS generates client configuration or probe input; live execution is not automated. |
| Supported | Implemented server-side and covered by focused RustFS tests. |
| Preview / controlled | Implemented behind explicit operator action or a run-once endpoint; no automatic background claim. |
| Documented, not automated | Behavior is documented; the live client run is not automated. |
| Reference only | Compatibility reference; live interoperability is not claimed. |
| Not claimed | Out of scope for the current implementation. |

## Endpoint And Profile Matrix

| Surface | Status | Notes |
|---|---|---|
| `/iceberg/v1` | Supported | Canonical REST Catalog prefix; default REST signing name `s3`. |
| `/_iceberg/v1` | Supported compatibility alias | MinIO AIStor-style alias; smoke profile defaults to signing name `s3tables`. |
| S3 object data plane | Supported | Data, metadata, manifest, and delete files are ordinary S3 objects with table-aware policy checks on warehouse paths. |
| Table bucket enablement | Supported | A regular bucket is enabled for catalog use and addressed as the REST catalog warehouse. |
| Catalog-vended table credentials | Automated when enabled | Disabled by default. LoadTable vends credentials only when `X-Iceberg-Access-Delegation` contains the exact `vended-credentials` token; the dedicated credentials endpoint uses the same issuer path. |
| AWS S3 Tables endpoint shape | Profile generator | Generates the AWS catalog URI and warehouse ARN shape for migration docs. API parity not claimed. |
| MinIO AIStor Tables profile | Profile generator plus alias smoke | Alias shape only; AIStor private extensions not claimed. |
| Cloudflare R2 Data Catalog profile | Profile generator | Catalog URI and warehouse-name shape only; live interop not claimed. |
| Alibaba OSS Tables profile | Profile generator | Endpoint, `acs:osstables` warehouse ARN, `osstables` signing name, and `https://oss-{region}.aliyuncs.com` S3FileIO endpoint shapes only; live interop not claimed. |

## Client And Engine Matrix

| Client or engine | Status | Claim |
|---|---|---|
| PyIceberg | Automated | Namespace and table create, append, reload, scan, metadata-location, refs, views, maintenance, diagnostics, optional vended credentials with an exact-prefix data-plane scope check. |
| Spark Iceberg REST catalog | Manual/live harness | Pinned package inputs, catalog properties, SQL, expected `row_count=2`, and a CI opt-in gate for create/append/refresh/count/cleanup. Live execution and commit-conflict probing remain manual unless enabled in the runner. |
| Trino Iceberg REST catalog | Manual/live harness | Catalog properties and a read-only `SELECT COUNT(*)` against a PyIceberg- or Spark-created table. Write compatibility not claimed. |
| DuckDB Iceberg 1.5.5 | Automated | `duckdb_smoke.py` covers metadata-location read, single-table create/insert/update/delete/merge, schema evolution, snapshots, concurrent writers, drop, PyIceberg cross-read, and both signing profiles. Staged create, purge-on-drop, and format v3 are verified fail-closed. Two-table mode runs without claiming cross-table atomicity. AWS `ENDPOINT_TYPE S3_TABLES` and vended-credential integration not claimed. |
| StarRocks Iceberg REST catalog | Documented, not automated | External catalog read-path reference only. |
| Databend | Manual/live harness | S3 stage read probe for table data files only; Iceberg REST integration not claimed. |
| Snowflake Open Catalog / Iceberg integrations | Generated harness | Operator-adapted external volume/catalog SQL template only; live interop not claimed. |

## Live Evidence And Operations Matrix

| Area | Status | Claim |
|---|---|---|
| Live conformance evidence | Automated for PyIceberg and DuckDB | `engine_compatibility.py --print-live-evidence-schema` defines the evidence schema and promotion boundaries; both smoke scripts write validated evidence records via `--live-evidence-output`. |
| Production operations guide | Generated harness | `engine_compatibility.py --print-operations-guide` records commands, evidence, pass criteria, and fail-closed signals for conformance, cutover, maintenance, recovery, permissions, credential vending, and claim governance. |
| Vendor compatibility gap audit | Generated harness | `engine_compatibility.py --print-vendor-audit` records provider URLs, path and warehouse shapes, auth models, validation categories, and not-claimed boundaries for the four vendor profiles. |
| Client claim promotion | Automated for scoped clients | PyIceberg and DuckDB claims are bounded by their smoke entrypoints and recorded versions; Spark needs recorded live evidence; Trino stays read-only; Snowflake and vendor profiles stay reference-only. |

## Catalog API Matrix

| Area | Status | Covered behavior |
|---|---|---|
| Catalog config | Supported | `GET /v1/config` advertises defaults and only the supported OpenAPI REST paths in `endpoints`; RustFS extensions (administration, maintenance, migration, diagnostics, refs, metadata-location) are not presented as standard endpoints. |
| Table bucket discovery | Supported | `PUT` / `GET /v1/buckets/{warehouse}` enable and inspect table bucket state. |
| Namespaces | Supported | Create, list, load, exists, drop on both prefixes; `pageSize`/`pageToken` pagination with context-bound tokens; identifiers limited to 512 ASCII characters. |
| Tables | Supported | Create, register, list, load, exists, rename, commit, metadata-location get/update, drop on both prefixes. Rename uses a bucket-scoped persistent fence, recoverable intent, and conditional publication; the source name is reusable only via an ETag-conditional tombstone replacement. Commit identifiers must match the URL; unknown requirements/updates fail as bad requests; staged create, register overwrite, purge-on-drop, and v3-only encryption-key updates return an explicit unsupported-operation response. |
| Commit CAS | Supported | Single-table commits validate base metadata, version token, referenced object existence, warehouse scope, and Iceberg requirements before advancing the pointer; external metadata transitions preserve monotonic assignment watermarks and immutable retained definitions. `idempotency-key-lifetime` is not advertised; mutation-wide `Idempotency-Key` semantics are unsupported. |
| Commit recovery | Supported | Commit log, idempotency lookup, diagnostics, and recovery routes expose and repair finalization gaps without moving the pointer. |
| Snapshot refs | Supported | List, create/replace, delete via commits; `main` is protected; refs with explicit retention need forced delete. |
| Iceberg views | Supported | Create, list, load, replace, exists, drop with view-scoped authorization; only view format version 1. |
| LoadTable and table credentials endpoint | Supported | Vending only on negotiated `vended-credentials`; one temporary session scoped to the warehouse prefix and current metadata location; missing credential permission falls back to metadata-only with an explicit reason; responses carry `Cache-Control: no-store, private`. |
| Catalog diagnostics and export | Supported | Recovery state, consistency, backing manifest, WAL state, migration target, single-active-writer policy, scale validation matrix. |
| Catalog import and rollback | Supported | Import/register and online rollback go through validation and commit paths. Online rollback accepts only forward-safe targets; restoring an older target that lowers watermarks is an offline disaster-recovery operation with all writers stopped. |
| External catalog bridge | Supported operator path | Operator-supplied metadata pointer sync/import. Vendor SDK polling and policy mirroring not claimed. |
| Multi-table transactions | Not claimed | Single-table commit atomicity only. |

## Data Plane And Credential Matrix

| Area | Status | Covered behavior |
|---|---|---|
| Table-aware S3 policy bridge | Supported | Ordinary S3 actions on warehouse paths are checked through the table bridge; table policy cannot be bypassed by direct object access. |
| Reserved catalog protection | Supported | Catalog-reserved prefixes are protected from ordinary object mutation. |
| Static S3 credentials | Automated | Default PyIceberg smoke path. |
| Catalog-vended credentials | Automated when enabled | `rustfs-vended-credentials` verifies the returned prefix, then checks Put/Head/Get/DeleteObject inside it and denies access outside it. |
| Credential lifetime | Supported | Server-side TTL clamped to a short-lived range. |
| No-long-term-data-credential bootstrap | Not claimed | Catalog setup still uses the configured principal before table-scoped credentials are requested. |

## Maintenance Matrix

| Capability | Status | Claim |
|---|---|---|
| Metadata retention dry-run | Supported | Reports retained metadata and deletion candidates without moving the pointer. |
| Metadata cleanup delete | Supported | Deletes only candidates passing the safety window and current-pointer checks. |
| Ordinary bucket lifecycle expiry | Disabled for table buckets | Table bucket objects are excluded from lifecycle expiration, including queued work; snapshot expiration and orphan cleanup stay inside catalog maintenance. |
| Snapshot expiration planning | Supported | Plans with retained and candidate snapshots. |
| Snapshot expiration commit | Preview / controlled | Manual commit through the catalog; stale plans fail closed. |
| Manifest/data/delete reachability cleanup | Supported | Reads manifest-list and manifest Avro references; deletes only unreferenced objects passing the safety window. |
| Maintenance scheduler run endpoint | Preview / controlled | Durably queues one job per table, reuses an active queued job, recovers expired leases. |
| Maintenance worker run endpoint | Preview / controlled | Claim, run-once, backpressure, retry deferral, lease expiry recovery, heartbeats. |
| Maintenance scheduler guardrails | Preview / controlled | Disabled/paused/ready state, handoff, backpressure, quarantine boundary, recommended actions, recent job audit timeline. |
| Maintenance audit events | Preview / controlled | Structured events for planning, worker transitions, heartbeats, lease recovery, quarantine mutations. |
| Maintenance quarantine operations | Preview / controlled | Inspect, release, retry, or abandon the quarantined job without moving the pointer. |
| Compaction planning | Preview / controlled | Partition-local and sort-order-local binpack candidates for Parquet; never mixes partitions or sort orders in one group. |
| Delete-file or row-level compaction planning | Preview / controlled | Position or equality delete files force machine-readable planning into manual review. |
| Compaction commit | Preview / controlled | Commits a safe partition-local Parquet rewrite preserving sort order IDs. |
| Built-in periodic scheduler | Not claimed | Ticks are operator-triggered; no continuous in-process scheduling. |
| Delete-file or row-level compaction execution | Not claimed | Manual-review maintenance item. |

## Recovery And Strong Backing Matrix

| Area | Status | Claim |
|---|---|---|
| Single-table CAS | Supported | Pointer advances only through expected-token and expected-metadata-location validation. |
| Idempotent retry | Supported | Repeated commit IDs return the finalized result or surface recoverable finalization gaps. |
| Commit publication fencing | Supported with rolling-upgrade gate | Exact object guards protect referenced files during publication. Set `RUSTFS_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED=true` only after every serving node supports table and table-bucket fences. In scalable mode, warehouse prefixes must not overlap and enablement, first publication, drop, and relocation are serialized by the table-bucket fence. |
| Post-CAS finalization recovery | Supported | Repairs stale or missing idempotency indexes without changing the pointer. |
| Catalog export | Supported | Table state, commit recovery state, and backing migration information. |
| Strong backing state transfer | Supported | Object-backed catalog state is materialized into the durable strong snapshot deterministically, ETag-CAS protected, idempotent after interrupted finalization, and validated through the restart decoder before publication; conflicts between inactive explicit namespaces and active descendants fail closed. Hydration requires a stable non-empty ETag, caps the snapshot at 64 MiB, and rejects disappearance or format-version rollback after observation. Configured durable-strong mode rejects a missing snapshot on first access; only object-backed migration may initialize an empty target. |
| Durable backing migration preflight | Supported | `GET /iceberg/v1/{warehouse}/catalog/migration` (and the alias) reports inventory, recovery blockers, prefix-index readiness, identifier collisions, fence state, target agreement, and per-bucket cutover readiness. |
| Durable backing migration execution | Preview / controlled | `POST /iceberg/v1/{warehouse}/catalog/migration` fences registry changes, acquires a persistent per-bucket write fence, drains in-flight mutations, materializes the snapshot, and reports `ready_to_enable_durable_strong`. `DELETE` cancels only while the target has not advanced. Both mutations require `admin:MigrateTableCatalog`. Procedure: [s3-tables-cutover-runbook.md](../operations/s3-tables-cutover-runbook.md). |
| Strong snapshot rolling compatibility | Supported | Reads snapshot versions 1 and 2; writes version 1 by default and version 2 only after both `RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2` and `RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2_FLEET_CONFIRMED` are set. A process rejects a lower format after observing a higher one; once v2 is fleet-confirmed, data-plane resolution fails closed until the persisted snapshot is v2. |
| Disaster recovery rehearsal | Manual/live harness | `failure_coverage.py --print-disaster-recovery-rehearsal`. |
| Scale and fault rehearsal | Manual/live harness | `failure_coverage.py --print-scale-fault-rehearsal`. |
| Durable strong snapshot backing cutover | Preview / controlled | `RUSTFS_TABLE_CATALOG_BACKING=durable-strong` only after every table bucket reports `SNAPSHOT_MATERIALIZED` and `ready_to_enable_durable_strong: true`. No separate external KV/WAL service is claimed; the legacy `STRONG_KV_WAL` and `CUT_OVER_LINEARIZABLE_READS` labels in v1 manifests are wire-compatibility labels, not claims. |
| Single active writer region | Supported policy | Diagnostics publish single-active-writer semantics and read-only replica limits. |
| Active-active multi-region writes | Not claimed | A table must not accept independent concurrent writers in multiple regions. |

## Production Failure Coverage

Failure probes prove RustFS does not silently advance table state on failure. Tracked cases: stale commit token or base metadata returns a conflict without advancing the pointer; missing metadata, manifest, data, or delete objects fail closed before commit or maintenance; concurrent writers produce one CAS winner and retryable conflicts; catalog and S3 permission denials prevent data-plane bypass; stale maintenance plans fail closed before deletion or commit; post-CAS finalization gaps are visible and safely recoverable; external catalog sync conflicts leave pointer, token, and generation unchanged; backing migration stays blocked until WAL and recovery replay are clean.

Do not promote a failure case from live probe or load test to an automated claim until the exact RustFS build, client version, and expected response shape are recorded.

## Unsupported Or Not Claimed

Full AWS S3 Tables control-plane parity; full MinIO AIStor private extensions; full Cloudflare R2 Data Catalog or Alibaba OSS Tables interoperability; built-in periodic maintenance scheduling; active-active multi-region writes; multi-table transactions; no-long-term-data-credential bootstrap; online vendor SDK polling; external catalog policy mirroring; delete-file rewrite or row-level compaction execution; built-in SQL execution; Delta Lake or Hudi; end-to-end SQL row-level DML validation through Spark, Trino, or another engine.

## Verification

Commands for updating this matrix, release notes, or client claims are maintained in [scripts/table-catalog/README.md](../../scripts/table-catalog/README.md); the unit tests are `scripts/table-catalog/test_*.py`.

## Release Claim Guidance

Acceptable: "RustFS includes a core Iceberg REST Catalog-based S3 Tables implementation with PyIceberg and DuckDB smoke coverage, table-aware S3 data-plane policy checks, controlled maintenance, catalog recovery diagnostics, manual conformance input for Spark, Trino, Databend, and Snowflake, production-failure probe harnesses, disaster-recovery and scale/fault rehearsal probes, and a machine-readable operations evidence guide."

Do not claim: "RustFS is fully compatible with AWS S3 Tables."

Any stronger vendor or engine claim needs a repeatable live harness, the exact client versions, and the expected response shapes recorded in the table-catalog inventories.

## Related

- [Table catalog conformance scripts](../../scripts/table-catalog/README.md)
- [Durable backing cutover runbook](../operations/s3-tables-cutover-runbook.md)
- [Admin route action snapshot](admin-route-action-snapshot.md)
- [Runtime capability contracts](runtime-capability-contracts.md)
