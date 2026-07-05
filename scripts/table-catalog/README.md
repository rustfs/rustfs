# RustFS S3 Tables Client Conformance

This directory contains repeatable client-facing checks for the RustFS S3 Tables
Iceberg REST Catalog surface. The goal is to keep S3 Tables compatibility claims
grounded in runnable scripts or explicit unsupported entries.

For the release-facing support and limitation matrix, see
[`docs/architecture/s3-tables-support-matrix.md`](../../docs/architecture/s3-tables-support-matrix.md).

## PyIceberg Smoke Test

Install the client dependencies:

```bash
python3 -m pip install 'pyiceberg[pyarrow]' boto3
```

Start RustFS locally, then run:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-s3table-smoke \
  --replace \
  --cleanup
```

The smoke test covers:

- create or reuse the S3 bucket
- enable the RustFS table bucket
- load the PyIceberg REST catalog
- create namespace and table
- append two rows through PyIceberg
- reload and scan the table
- probe direct REST catalog endpoints for metadata-location, table refs,
  Iceberg views, maintenance config, metadata maintenance, worker run, and
  catalog diagnostics
- optionally drop the table and namespace

The default profile uses the canonical RustFS catalog URI:

```text
http://127.0.0.1:9000/iceberg
```

To exercise the MinIO AIStor-style alias exposed by RustFS:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --profile rustfs-compat \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-s3table-smoke \
  --replace \
  --cleanup
```

`rustfs-compat` uses:

```text
catalog URI: http://127.0.0.1:9000/_iceberg
REST signing name: s3tables
```

If the local deployment still requires the standard S3 signing name for the
alias path, override it explicitly:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --profile rustfs-compat \
  --rest-signing-name s3
```

To verify catalog-vended table credentials, enable server-side credential
vending and use the vended credential profile:

```text
RUSTFS_TABLE_CATALOG_CREDENTIAL_VENDING=enabled
```

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --profile rustfs-vended-credentials \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-s3table-smoke \
  --replace \
  --cleanup
```

This profile uses the configured principal to create the bucket, enable the
table bucket, and create the table. After the table exists, it calls the REST
credentials endpoint and reloads the PyIceberg catalog with the returned
table-scoped S3 access key, secret key, and session token before append, reload,
and scan operations.

Before the PyIceberg append, the profile also checks that the returned
credential prefix exactly matches the created table warehouse location after
canonical S3 URI normalization, including percent-decoding equivalent path
encodings. It then runs a direct S3 data-plane scope probe with the returned
temporary credentials:

- `PutObject`, `HeadObject`, `GetObject`, and `DeleteObject` must work inside
  the returned table warehouse prefix.
- `PutObject` and `GetObject` to the same bucket outside that prefix must be
  rejected.

The direct REST catalog probes run by default after the PyIceberg append and
scan. For deployments that intentionally expose only the core Iceberg REST
Catalog table path, skip those probes explicitly:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --skip-catalog-api-probes
```

## Machine-Readable Inventories

The script can print the current conformance inventories without importing
PyIceberg, PyArrow, or boto3:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py --print-client-matrix
python3 scripts/table-catalog/pyiceberg_smoke.py --print-engine-compatibility
python3 scripts/table-catalog/pyiceberg_smoke.py --print-production-failure-coverage
python3 scripts/table-catalog/pyiceberg_smoke.py --print-vendor-profiles
python3 scripts/table-catalog/pyiceberg_smoke.py --print-unsupported-inventory
python3 scripts/table-catalog/pyiceberg_smoke.py --print-production-readiness
```

Use these outputs when updating release notes, PR descriptions, or follow-up
work items. They are intentionally conservative: only PyIceberg is automated by
this script today. Spark has a repeatable manual/live harness with pinned
client package inputs, generated configuration, generated SQL, expected
results, and a CI opt-in gate. Trino, DuckDB, Databend, and Snowflake now have
generated manual probe inputs, but they remain opt-in and do not promote write
or full vendor interoperability claims.

Vendor profiles are machine-readable connection references. They include the
catalog URI shape, warehouse shape, signing name, credential model, namespace
model, pagination model, and explicit not-claimed behavior. They are useful for
building migration docs without turning provider references into compatibility
claims. The `vendor_profiles` object lists every supported reference template;
the `selected_vendor_profile` object renders the active profile with the
provided endpoint, account, bucket, and warehouse arguments.

Spark vendor config generation only emits S3-compatible data-plane endpoint,
path-style, and static S3 credential properties for profiles that explicitly use
the supplied endpoint as object storage, such as RustFS and MinIO AIStor. AWS S3
Tables, Cloudflare R2 Data Catalog, and OSS Tables reference profiles leave
provider object I/O endpoint and credential selection to the provider-specific
Spark/Iceberg runtime configuration instead of inheriting RustFS local defaults.

Generate an AWS S3 Tables-style reference profile:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --profile aws-s3tables \
  --region us-east-1 \
  --account-id 123456789012 \
  --table-bucket analytics \
  --print-vendor-profiles
```

Generate a Cloudflare R2 Data Catalog-style reference profile:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py \
  --profile cloudflare-r2-data-catalog \
  --catalog-uri https://example.account.r2.cloudflarestorage.com/catalog \
  --warehouse-name analytics \
  --print-vendor-profiles
```

The standalone engine helper prints the same compatibility matrix and can also
generate Spark REST catalog input without importing PyIceberg:

```bash
python3 scripts/table-catalog/engine_compatibility.py --print-engine-matrix
python3 scripts/table-catalog/engine_compatibility.py --print-spark-config
python3 scripts/table-catalog/engine_compatibility.py \
  --profile aws-s3tables \
  --region us-east-1 \
  --account-id 123456789012 \
  --table-bucket analytics \
  --print-spark-config
python3 scripts/table-catalog/engine_compatibility.py --print-spark-sql --cleanup
python3 scripts/table-catalog/engine_compatibility.py --print-live-conformance --cleanup
python3 scripts/table-catalog/engine_compatibility.py --print-operations-guide
```

The production failure helper records the negative coverage required before
calling a release production-ready and can generate REST probe steps for a live
RustFS endpoint:

```bash
python3 scripts/table-catalog/failure_coverage.py --print-failure-matrix
python3 scripts/table-catalog/failure_coverage.py \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --rest-path /iceberg \
  --print-failure-probes
python3 scripts/table-catalog/failure_coverage.py \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --table-warehouse-location s3://rustfs-s3table-smoke/tables/table-id \
  --print-disaster-recovery-rehearsal
```

The generated probe plan covers stale-token commit conflicts, missing metadata
object rejection, diagnostics/recovery for finalization gaps, maintenance stale
plan rejection, and external catalog sync conflicts. These steps are meant to be
run against a prepared live table and should be recorded with the exact RustFS
build and client versions used.

`--rest-path` defaults to `/iceberg` and generated probe paths include the
mounted catalog prefix, for example `/iceberg/v1/{warehouse}/...`. Use
`--rest-path /_iceberg` to generate paths for the compatibility alias.

The smoke test also probes catalog-backed advanced Iceberg surfaces:

- table refs can be listed, created or replaced, and deleted through catalog
  commits; refs with explicit retention policy require a forced delete, and
  `main` cannot be deleted
- Iceberg views support basic create, list, load, replace, existence check, and
  drop routes with persisted view metadata and view-scoped authorization
- metadata maintenance supports safe dry-run planning, controlled scheduler
  queueing, worker execution checks, and a scheduler status report with disabled,
  paused, queued, backpressure, retry, quarantine, and audit-timeline state; job reports expose
  structured audit events for planning, worker transitions, heartbeat updates,
  lease expiry, and mutating quarantine operations; quarantined jobs can be
  inspected, released, retried, or abandoned through an operator endpoint
- catalog diagnostics exposes the table recovery and consistency state used by
  operators
- catalog export and diagnostics expose the current catalog backing manifest,
  recoverable commit-log WAL state, strong backing migration target, single
  active writer HA policy, and scale validation matrix

## Client Matrix

| Client | Current status | Claim |
|---|---|---|
| PyIceberg | Automated smoke target | create namespace, create table, append, reload, scan, metadata-location, refs, views, maintenance, diagnostics, optional catalog-vended table credentials with exact-prefix data-plane scope probe |
| Spark Iceberg REST catalog | Manual/live harness | pinned Spark and Iceberg package inputs, configuration, SQL, run command, expected row count, and cleanup can be generated for a running RustFS endpoint; CI execution is opt-in |
| Trino Iceberg REST catalog | Manual/live read probe | generated catalog properties and a read-only SELECT probe for a table created by PyIceberg or Spark; no write compatibility claim yet |
| DuckDB Iceberg | Manual/live read probe | generated httpfs/iceberg SQL using an operator-supplied current metadata location; read-path only |
| StarRocks Iceberg REST catalog | Documented, not automated | external catalog read-path reference only |
| Databend | Manual/live S3 stage probe | generated S3 stage read probe for table data files; Iceberg REST catalog integration is not claimed |
| Snowflake/Open Catalog integrations | Manual reference probe | generated external volume/catalog SQL template; live RustFS interoperability is not claimed |

## Production Failure Coverage

Production failure coverage is tracked separately from positive client
conformance. Positive smoke tests prove a client can create and use a table;
failure probes prove RustFS does not silently advance table state when something
goes wrong.

The current failure matrix covers:

- stale commit tokens and stale base metadata returning a conflict without
  advancing the table pointer
- post-CAS finalization gaps surfacing through diagnostics and safe recovery
  repair without pointer movement
- missing metadata, manifest, data, or delete objects failing closed before a
  commit or maintenance operation advances state
- concurrent writers producing a single winning CAS and retryable conflicts
- table catalog and ordinary S3 object permission denials preventing data-plane
  bypass
- stale maintenance plans failing closed before object deletion or catalog
  commit
- external catalog sync conflicts leaving pointer, token, and generation
  unchanged
- backing migration remaining blocked until WAL/recovery replay is clean

Do not promote a failure case from `probe-required` or `load-test-required` to
an automated claim until the live probe or stress harness is repeatable and its
RustFS build, client version, and expected response shape are recorded.

## Disaster Recovery Rehearsal

The disaster recovery rehearsal plan is a machine-readable operator runbook. It
does not mutate state by itself and does not claim automatic repair. It records
the REST and S3 probes an operator or CI opt-in job should run against a
prepared table when validating recovery behavior.

Generate the rehearsal plan:

```bash
python3 scripts/table-catalog/failure_coverage.py \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --table-warehouse-location s3://rustfs-s3table-smoke/tables/table-id \
  --print-disaster-recovery-rehearsal
```

The plan is gated for CI by:

```text
RUSTFS_TABLE_CATALOG_DR_REHEARSAL=1
```

The generated phases cover:

- baseline capture through catalog export and `loadTable`
- recovery diagnostics and safe idempotency/commit repair
- operator-selected rollback or import of a known metadata location
- durable backing migration dry-run blocker checks
- post-recovery `loadTable` and table warehouse data-plane policy probes

Record the RustFS build, catalog backing mode, table identifier, metadata
location, and expected response status for each run. Treat migration blockers,
manual-review diagnostics, stale rollback/import conflicts, and data-plane
policy failures as fail-closed results that require operator investigation
before cutover or release claims.

## Production Operations Guide

The engine helper can print a machine-readable operations guide that ties client
conformance, durable backing cutover, maintenance, recovery, permissions, and
unsupported-claim governance to the exact evidence operators must record:

```bash
python3 scripts/table-catalog/engine_compatibility.py \
  --endpoint http://127.0.0.1:9000 \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --print-operations-guide
```

Use this output as the release checklist when expanding compatibility language.
Each section records commands, required evidence, pass criteria, and fail-closed
signals. A client or vendor claim should only be promoted when the corresponding
live evidence records the RustFS build, catalog backing mode, client version,
expected status, observed status, and metadata location.

## Vendor Profile References

| Profile | Catalog shape | Warehouse shape | Signing name | RustFS claim |
|---|---|---|---|---|
| `rustfs` | `{endpoint}/iceberg` | `{bucket}` | `s3` | automated smoke target |
| `rustfs-compat` | `{endpoint}/_iceberg` | `{bucket}` | `s3tables` by default | compatibility smoke target |
| `rustfs-vended-credentials` | `{endpoint}/iceberg` | `{bucket}` | `s3` | automated credential smoke target when server vending is enabled |
| `aws-s3tables` | `https://s3tables.{region}.amazonaws.com/iceberg` | `arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}` | `s3tables` | profile generator only; full AWS S3 Tables API parity is not claimed |
| `minio-aistor` | `{endpoint}/_iceberg` | `{warehouse}` | `s3tables` | profile generator plus RustFS alias smoke; full AIStor extension parity is not claimed |
| `cloudflare-r2-data-catalog` | catalog URI returned by R2 | `{warehouse_name}` | `s3` | profile generator only; live RustFS interoperability is not claimed |
| `oss-tables` | provider REST endpoint | `{warehouse}` | `s3` | profile generator only; live RustFS interoperability is not claimed |

## Unsupported Inventory

Unsupported behavior is documented instead of hidden behind internal errors. The
current unsupported inventory is:

- credential vending: automated after table bootstrap with exact-prefix validation and a data-plane scope probe; full no-long-term-data-credential bootstrap is not claimed
- background maintenance worker: controlled scheduler run, worker run-once, heartbeat, quarantine operation, and scheduler status endpoints are registered; disabled/paused/queued/backpressure/retry/quarantine/audit-timeline state and per-job audit events are machine-readable; continuous in-process scheduling is not claimed
- manifest/data reachability cleanup: metadata maintenance reads manifest-list and manifest Avro references, reports manifest/data/delete reachability, and deletes only unreferenced table objects that pass the safety window
- snapshot expiration dry-run planning and manual catalog commit: supported through metadata maintenance reports
- automatic maintenance scheduling: external scheduler hook supported through the scheduler run endpoint, worker run endpoint, and scheduler status report; built-in periodic scheduling is not claimed
- compaction rewrite: controlled run-once support for partition-local and sort-order-preserving Parquet binpack through metadata maintenance; manifests with position or equality delete files produce machine-readable row-level planning and fail closed before rewrite; built-in periodic scheduling, delete-file rewrite, and row-level compaction execution are not claimed
- row-level delete/update/merge commits: standard catalog commit validates append, overwrite, delete, and replace snapshot manifests for table-warehouse scope, referenced object existence, current-live-file deletes, and stale add/delete conflicts; end-to-end SQL DML client coverage remains a compatibility validation item
- external catalog bridges: metadata import/register and operator-supplied metadata pointer sync are supported for Polaris/Glue/DLF/Hive identity boundaries; online vendor SDK polling and policy mirroring are not claimed
- multi-table transactions: not a short-term production claim

## Credential Boundary

RustFS advertises table credential scope metadata without returning reusable
storage secrets by default. `loadTable` includes the table warehouse prefix in
the response config, and the standard credentials endpoint is registered:

```text
GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials
```

The endpoint returns an empty `storage-credentials` list unless table catalog
credential vending is explicitly enabled. When enabled, RustFS issues temporary
table-scoped S3 credentials through the credentials endpoint. Those credentials
are constrained to the table warehouse prefix and include a session token and
expiration.

The `rustfs-vended-credentials` profile verifies the client handoff from the
catalog principal to the table-scoped temporary credentials. It still uses the
configured principal for setup and REST request signing; the vended credentials
are first checked against the created table warehouse location, then checked
with a direct S3 scope probe, and finally applied to PyIceberg S3 data-plane
access after the table has been created.

Enablement is server-side and fail-closed:

```text
RUSTFS_TABLE_CATALOG_CREDENTIAL_VENDING=enabled
RUSTFS_TABLE_CATALOG_CREDENTIAL_TTL_SECONDS=900
```

The TTL is clamped to the supported short-lived range by the server.

## Spark Manual/Live Harness

Spark validation should use the same RustFS endpoint and warehouse bucket as the
PyIceberg smoke test. The harness records default pinned client package inputs,
the exact command shape, and the expected row count. It is manual by default and
should only run in CI when explicitly gated with
`RUSTFS_TABLE_CATALOG_LIVE_CONFORMANCE=1`.

Generate the full live harness document:

```bash
python3 scripts/table-catalog/engine_compatibility.py \
  --endpoint http://127.0.0.1:9000 \
  --warehouse rustfs-s3table-smoke \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --pyiceberg-version 0.10.0 \
  --spark-version 3.5.4 \
  --iceberg-version 1.7.1 \
  --print-live-conformance \
  --cleanup
```

The output includes:

- PyIceberg install and smoke commands
- Spark package coordinates for `iceberg-spark-runtime` and `iceberg-aws-bundle`
- Spark REST catalog properties
- generated Spark SQL
- a `spark-sql` command using the generated properties
- expected `row_count=2` before optional cleanup
- an evidence template for recording RustFS build, catalog backing mode, client
  version, expected status, observed status, metadata location, and claim
  promotion boundary

Generate the configuration properties:

```bash
python3 scripts/table-catalog/engine_compatibility.py \
  --endpoint http://127.0.0.1:9000 \
  --warehouse rustfs-s3table-smoke \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --print-spark-config
```

The generated configuration shape is:

```properties
spark.sql.catalog.rustfs=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.rustfs.type=rest
spark.sql.catalog.rustfs.uri=http://127.0.0.1:9000/iceberg
spark.sql.catalog.rustfs.warehouse=rustfs-s3table-smoke
spark.sql.catalog.rustfs.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.rustfs.s3.endpoint=http://127.0.0.1:9000
spark.sql.catalog.rustfs.s3.path-style-access=true
spark.sql.catalog.rustfs.rest.sigv4-enabled=true
spark.sql.catalog.rustfs.rest.signing-name=s3
spark.sql.catalog.rustfs.rest.signing-region=us-east-1
spark.sql.catalog.rustfs.s3.access-key-id=rustfsadmin
spark.sql.catalog.rustfs.s3.secret-access-key=rustfsadmin
```

Generate the SQL smoke input:

```bash
python3 scripts/table-catalog/engine_compatibility.py \
  --catalog-name rustfs \
  --namespace smoke \
  --table events \
  --print-spark-sql \
  --cleanup
```

The generated SQL covers namespace creation, table creation, append, refresh,
count, and optional cleanup. Until Spark execution is enabled in CI through the
explicit live-conformance gate, do not claim Spark support beyond a manually
verified run with the exact RustFS build, Spark version, Iceberg version, and
expected output recorded.

## Additional Manual Engine Probes

`--print-live-conformance` also generates conservative manual probe input for
engines that are not run by default in RustFS CI:

- Trino: catalog properties and a read-only `SELECT COUNT(*)` command for a
  table already created by PyIceberg or Spark. Trino write compatibility is not
  claimed.
- DuckDB: `httpfs` and `iceberg` SQL using an operator-supplied current Iceberg
  metadata location. DuckDB write and commit compatibility are not claimed.
- Databend: an S3 stage read probe for Parquet data files under the table
  warehouse. Databend Iceberg REST Catalog integration is not claimed.
- Snowflake: an operator-adapted external volume/catalog integration SQL
  template. Live RustFS interoperability is not claimed.

Generate the full probe set:

```bash
python3 scripts/table-catalog/engine_compatibility.py \
  --endpoint http://127.0.0.1:9000 \
  --warehouse rustfs-s3table-smoke \
  --namespace smoke \
  --table events \
  --metadata-location s3://rustfs-s3table-smoke/tables/table-id/metadata/v1.metadata.json \
  --print-live-conformance \
  --cleanup
```

Record the exact engine version, RustFS build, current metadata location, and
expected row count when running any of these probes. Treat failures as
compatibility findings, not as proof that the server can safely claim broader
vendor or engine support.
