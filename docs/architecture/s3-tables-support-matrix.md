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
| Alibaba OSS Tables profile | Profile generator | Generates provider endpoint, `acs:osstables` warehouse ARN, `osstables` signing-name, and `https://oss-{region}.aliyuncs.com` S3FileIO endpoint shapes for migration docs. Live RustFS interoperability is not claimed. |

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
| Live conformance evidence | Manual/live harness | `engine_compatibility.py --print-live-evidence-schema` defines the required evidence schema and claim promotion boundaries. `pyiceberg_smoke.py --live-evidence-output` writes a validated PyIceberg evidence record after a successful live smoke run. |
| Production operations guide | Generated harness | `engine_compatibility.py --print-operations-guide` records command, evidence, pass criteria, and fail-closed signals for live conformance, durable backing cutover, maintenance, recovery, permissions, credential vending, and unsupported-claim governance. |
| Vendor compatibility gap audit | Generated harness | `engine_compatibility.py --print-vendor-audit` records provider source URLs, catalog path and warehouse shapes, signing/auth models, error/permission/maintenance validation categories, and not-claimed boundaries for AWS S3 Tables, MinIO AIStor Tables, Cloudflare R2 Data Catalog, and Alibaba OSS Tables. |
| Client claim promotion | Documented, not automated | PyIceberg remains the automated claim. Spark can be promoted only with recorded manual/live evidence; Trino and DuckDB read probes do not promote write compatibility; Snowflake and vendor profiles remain reference-only without repeatable live evidence. |

## Catalog API Matrix

| Area | Status | Covered behavior |
|---|---|---|
| Catalog config | Supported | `GET /v1/config` advertises RustFS catalog defaults and only the supported OpenAPI REST paths in `endpoints`. RustFS administration, maintenance, migration, diagnostics, refs, and metadata-location extensions remain available but are not presented as standard Iceberg REST endpoints. |
| Table bucket discovery | Supported | `PUT` and `GET /v1/buckets/{warehouse}` enable and inspect table bucket state. |
| Namespaces | Supported | Create, list, load, existence check, and drop namespace routes are registered on both catalog prefixes. List responses support Iceberg REST `pageSize`/`pageToken` pagination with context-bound tokens and bounded catalog-store reads. Namespace identifiers are limited to 512 ASCII characters so persisted paths and stateless continuation tokens remain bounded. |
| Tables | Supported | Create, register, list, load, existence check, commit, metadata-location get/update, and drop table routes are registered on both catalog prefixes. Table and view listings support Iceberg REST `pageSize`/`pageToken` pagination with context-bound tokens and bounded catalog-store reads. Commit identifiers must match the URL resource; unknown requirements, updates, and snapshot operations fail as bad requests; staged create, register overwrite, purge-on-drop, and v3-only encryption-key updates return an explicit unsupported-operation response. Standard statistics, partition statistics, and schema/spec cleanup updates are accepted. |
| Commit CAS | Supported | Single-table commits validate base metadata, expected version token, referenced object existence, warehouse scope, and Iceberg commit requirements before advancing the current metadata pointer. Externally supplied metadata transitions preserve monotonic column, partition, and sequence assignment watermarks and immutable definitions for retained schemas, partition specs, sort orders, and snapshots. Standard commits preserve the normal commit-token file name and use an immutable-table-scoped fallback when rename followed by source-name reuse would otherwise collide at the same generation and commit ID. The catalog does not advertise `idempotency-key-lifetime`; clients must treat standard mutation-wide `Idempotency-Key` semantics as unsupported. |
| Commit recovery | Supported | Commit log, idempotency lookup, diagnostics, and recovery routes expose staged/finalization gaps and repair safe idempotency gaps without moving the table pointer. |
| Snapshot refs | Supported | Refs can be listed, created or replaced, and deleted through catalog commits. `main` is protected and refs with explicit retention require forced delete. |
| Iceberg views | Supported | Basic create, list, load, replace, existence check, and drop routes persist view metadata with view-scoped authorization. Replace identifiers must match the URL resource, `schema-id: -1` resolves to the last added schema, one commit timestamp is used consistently, and only Iceberg view format version 1 is accepted. |
| Table credentials endpoint | Supported | Returns an empty `storage-credentials` list by default. Returns table-scoped temporary credentials only when credential vending is enabled. Credential responses set `Cache-Control: no-store, private`, `Pragma: no-cache`, and `Expires: 0`. |
| Catalog diagnostics and export | Supported | Exposes recovery state, consistency state, backing manifest, recoverable commit-log WAL state, strong backing migration target, single-active-writer policy, and scale validation matrix. |
| Catalog import and rollback | Supported | Import/register and online rollback use catalog validation and commit paths rather than direct pointer mutation. Online rollback accepts only a forward-safe metadata target that preserves assignment watermarks and retained definitions. Restoring an older target that lowers those watermarks is an offline disaster-recovery operation and requires every writer to be stopped. |
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
| Ordinary bucket lifecycle expiry | Disabled for table buckets | Table bucket objects are excluded from ordinary lifecycle expiration, including already queued expiry work. Snapshot expiration and orphan cleanup remain catalog maintenance operations so referenced Iceberg files cannot be deleted outside publication fencing. |
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
| Commit publication fencing | Supported with rolling-upgrade gate | Existing deployments retain exact object guards so older writers cannot mutate referenced files during publication. Set `RUSTFS_TABLE_CATALOG_PUBLICATION_FENCE_FLEET_CONFIRMED=true` only after every serving node supports table and table-bucket publication fences. In scalable mode, active table warehouse prefixes must not overlap, ordinary lifecycle expiry remains disabled for table buckets, and first enablement, first publication, drop, and warehouse relocation are serialized by the table-bucket fence. |
| Post-CAS finalization recovery | Supported | Diagnostics and recovery can repair stale or missing idempotency indexes without changing the current table pointer. |
| Catalog export | Supported | Exposes table state, commit recovery state, and backing migration information for operator inspection. |
| Strong backing state transfer | Supported | Object-backed table bucket, namespace, table, view, commit-log, and idempotency state can be materialized into the durable strong snapshot. The transfer is deterministic, ETag-CAS protected, idempotent after an interrupted finalization, validates candidate state through the restart decoder before publication, preserves resource-backed implicit namespaces, and fails closed when an inactive explicit namespace conflicts with active descendants or resources. Snapshot hydration requires a stable non-empty ETag, caps the encoded snapshot at 64 MiB, shares state and reload serialization across requests in one server context, and rejects disappearance or format-version rollback after observation. Configured durable-strong mode rejects a missing snapshot on its first catalog access after startup; only object-backed migration may initialize an empty target. |
| Durable backing migration preflight | Supported | `GET /iceberg/v1/{warehouse}/catalog/migration` and the `/_iceberg/v1` alias inspect object-backed catalog inventory, recovery blockers, warehouse prefix index readiness, active table/view identifier collisions, persistent write-fence state, target snapshot agreement, and whether every table bucket is ready for cutover. |
| Durable backing migration execution | Preview / controlled | `POST /iceberg/v1/{warehouse}/catalog/migration` fences table-bucket registry changes, acquires a persistent per-bucket write fence, records whether a global strong snapshot existed before publication, drains in-flight catalog mutations, materializes the target snapshot, and reports `ready_to_enable_durable_strong`. Retries and `DELETE` may restore a known-absent initial target after an ambiguous first write, but fail closed if a previously existing or materialized global snapshot disappears. `DELETE` releases the bucket fence only while its target state has not advanced, and releases the registry fence after the last bucket is cancelled. Both mutations require `admin:MigrateTableCatalog`. |
| Strong snapshot rolling compatibility | Supported | Durable strong control-plane reads snapshot versions 1 and 2, writes version 1 by default, and writes version 2 only after both the requested and fleet-confirmed gates are enabled. A running process rejects any lower-format snapshot after observing a higher format. Once version 2 is fleet-confirmed, table data-plane resolution fails closed until the persisted snapshot is version 2; a missing table-bucket entry also fails closed instead of bypassing table-aware authorization. |
| Disaster recovery rehearsal | Manual/live harness | `failure_coverage.py --print-disaster-recovery-rehearsal` generates an operator runbook covering catalog export, diagnostics, safe recovery repair, rollback/import, durable backing migration dry-run, post-recovery loadTable, and table data-plane policy probes. |
| Scale and fault rehearsal | Manual/live harness | `failure_coverage.py --print-scale-fault-rehearsal` generates an opt-in runbook for concurrent writer stress, maintenance scheduler lease recovery, durable backing cutover preflight, recovery/rollback/import under load, and post-run evidence capture. |
| Durable strong snapshot backing cutover | Preview / controlled | Operators can select the ETag-CAS snapshot backing with `RUSTFS_TABLE_CATALOG_BACKING=durable-strong` only after every table bucket reports `SNAPSHOT_MATERIALIZED` and `ready_to_enable_durable_strong: true`. This mode does not claim a separate external KV/WAL service, and object-only advanced operations fail closed. Version 1 backing manifests retain the legacy `STRONG_KV_WAL` and `CUT_OVER_LINEARIZABLE_READS` wire labels for client compatibility; those labels do not expand the implementation claim. |
| Single active writer region | Supported policy | Diagnostics publish single-active-writer semantics and read-only replica limits. |
| Active-active multi-region writes | Not claimed | A table must not accept independent concurrent writers in multiple active regions. |

## Durable Backing Cutover Runbook

Use the migration dry-run before changing the table catalog backing for a
warehouse:

1. Take an object-backed catalog backup and record the current metadata pointer
   and version token for representative tables.
2. Run `GET /iceberg/v1/{warehouse}/catalog/migration` with a principal that has
   `GetTableCatalogAction` on each table bucket. Treat every `blockers` entry as
   fail-closed; repair commit recovery state and backfill the warehouse prefix
   index before continuing.
3. Before the migration `POST`, drain every catalog writer that predates the
   durable-backing migration fence and restart it on a fence-aware release. An
   older writer does not recognize the persisted fence and can otherwise
   mutate the object-backed source after the snapshot inventory is captured.
   Keep all catalog writers on the fence-aware release until cutover completes.
4. Inventory object-only advanced operations, including maintenance workers,
   catalog recovery, export, diagnostics, and external catalog bridge writes.
   Quiesce mutating operations before cutover and confirm that each required
   operation is supported by durable-strong mode; unsupported operations fail
   closed after cutover rather than continuing against object-backed state.
5. Run `POST /iceberg/v1/{warehouse}/catalog/migration` with
   `admin:MigrateTableCatalog`. This acquires the exclusive migration fence to
   drain in-flight fence-aware mutations, persists the source fence while
   exclusivity is held, and then copies the catalog state.
6. Repeat the preflight and materialization for every table bucket. Do not set
   `RUSTFS_TABLE_CATALOG_BACKING=durable-strong` until the preflight reports
   `SNAPSHOT_MATERIALIZED`, no blockers, and
   `ready_to_enable_durable_strong: true`.
7. Restart with durable strong backing enabled, then verify catalog config,
   table and view loads, commit idempotency, and table data-plane policy
   resolution before admitting writers.
8. Before restarting into durable-strong mode, `DELETE` on the migration
   endpoint can remove a migration-created target bucket snapshot and release
   the source fence. After the durable-strong state advances, cancellation
   fails closed; recovery requires an operator-selected restore or reverse
   migration instead of restarting against the stale object-backed pointer.
9. Preserve the object-backed catalog backup until durable strong backing has
   passed the operator's retention window.
10. Keep strong snapshot writes on version 1 during a rolling binary upgrade.
   After every catalog writer can read version 2, set both
   `RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2=true` and
   `RUSTFS_TABLE_CATALOG_STRONG_SNAPSHOT_V2_FLEET_CONFIRMED=true`, then restart
   the catalog writers. Perform a controlled catalog write or migration
   materialization and confirm that the persisted snapshot is version 2 before
   serving table data-plane traffic. Setting only one gate does not change the
   write format.
11. After any version 2 snapshot is persisted, do not roll catalog writers back
   to a binary that only reads version 1. Current binaries preserve version 2
   even when the gates are later disabled. A running process rejects restored
   version 1 content after observing version 2, but cannot distinguish an older
   snapshot with the same format version from a deliberate restore. The format
   high-water mark is process-local: restoring any older snapshot and restarting
   every writer is a privileged disaster-recovery rollback that cannot be
   inferred from the restored object alone. Recovery must restore a compatible
   binary and a snapshot selected through the operator recovery procedure.
12. Migration preflight rejects an active table/view identifier collision before
    it writes a migration fence. A pre-existing version 1 strong snapshot with
    such a collision is loaded in cleanup-only quarantine. Ambiguous reads fail
    closed; each cleanup mutation must reduce the collision set, and unrelated
    writes remain blocked until all collisions are removed. Drain catalog
    writers that predate cleanup quarantine before starting this repair, and
    complete cleanup before the first version 2 write. Restoring any version 1
    snapshot after a writer has observed version 2 fails closed instead of
    replacing the in-process catalog state.

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
python3 scripts/table-catalog/engine_compatibility.py --print-vendor-audit
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
python3 scripts/table-catalog/engine_compatibility.py --print-live-evidence-schema
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --endpoint http://127.0.0.1:9000 \
  --bucket rustfs-s3table-smoke \
  --replace \
  --cleanup \
  --rustfs-build rustfs-v1.0.0-beta.8 \
  --git-sha "$(git rev-parse HEAD)" \
  --catalog-backing durable-strong \
  --live-evidence-output /tmp/rustfs-pyiceberg-live-evidence.json
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
python3 scripts/table-catalog/failure_coverage.py \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --table-warehouse-location s3://rustfs-s3table-smoke/tables/table-id \
  --writer-count 8 \
  --maintenance-worker-count 2 \
  --iteration-count 50 \
  --print-scale-fault-rehearsal
```

## Release Claim Guidance

Use conservative release wording that matches the matrix.

Acceptable wording:

> RustFS includes a core Iceberg REST Catalog-based S3 Tables implementation
> with PyIceberg smoke coverage, table-aware S3 data-plane policy checks,
> controlled maintenance, catalog recovery diagnostics, manual conformance
> input for Spark, Trino, DuckDB, Databend, and Snowflake, production-failure
> probe harnesses, disaster-recovery and scale/fault rehearsal probes, and a
> machine-readable production operations evidence guide.

Do not claim:

> RustFS is fully compatible with AWS S3 Tables.

Any stronger vendor or engine claim needs a repeatable live validation harness,
the exact client versions used, and the expected response shapes recorded in the
table-catalog inventories.

## Related

- [Table catalog conformance scripts](../../scripts/table-catalog/README.md)
- [Admin route action snapshot](admin-route-action-snapshot.md)
- [Runtime capability contracts](runtime-capability-contracts.md)
