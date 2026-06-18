# RustFS S3 Tables Client Conformance

This directory contains repeatable client-facing checks for the RustFS S3 Tables
Iceberg REST Catalog surface. The goal is to keep S3 Tables compatibility claims
grounded in runnable scripts or explicit unsupported entries.

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
this script today. Spark now has generated configuration and SQL smoke input;
other engines are documented until a repeatable harness is added.

The standalone engine helper prints the same compatibility matrix and can also
generate Spark REST catalog input without importing PyIceberg:

```bash
python3 scripts/table-catalog/engine_compatibility.py --print-engine-matrix
python3 scripts/table-catalog/engine_compatibility.py --print-spark-config
python3 scripts/table-catalog/engine_compatibility.py --print-spark-sql --cleanup
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
  --print-failure-probes
```

The generated probe plan covers stale-token commit conflicts, missing metadata
object rejection, diagnostics/recovery for finalization gaps, maintenance stale
plan rejection, and external catalog sync conflicts. These steps are meant to be
run against a prepared live table and should be recorded with the exact RustFS
build and client versions used.

The smoke test also probes catalog-backed advanced Iceberg surfaces:

- table refs can be listed, created or replaced, and deleted through catalog
  commits; refs with explicit retention policy require a forced delete, and
  `main` cannot be deleted
- Iceberg views support basic create, list, load, replace, existence check, and
  drop routes with persisted view metadata and view-scoped authorization
- metadata maintenance supports safe dry-run planning and controlled worker
  execution checks
- catalog diagnostics exposes the table recovery and consistency state used by
  operators
- catalog export and diagnostics expose the current catalog backing manifest,
  recoverable commit-log WAL state, strong backing migration target, single
  active writer HA policy, and scale validation matrix

## Client Matrix

| Client | Current status | Claim |
|---|---|---|
| PyIceberg | Automated smoke target | create namespace, create table, append, reload, scan, metadata-location, refs, views, maintenance, diagnostics, optional catalog-vended table credentials with exact-prefix data-plane scope probe |
| Spark Iceberg REST catalog | Generated smoke harness | configuration and SQL can be generated for create namespace, create table, append, reload, count, and cleanup against a running RustFS endpoint |
| Trino Iceberg REST catalog | Documented, not automated | no write compatibility claim yet |
| DuckDB Iceberg | Documented, not automated | read-path reference only |
| StarRocks Iceberg REST catalog | Documented, not automated | external catalog read-path reference only |
| Databend | Documented, not automated | S3 data-plane reference only; Iceberg REST catalog integration is not claimed |
| Snowflake/Open Catalog integrations | Documented, not automated | reference only |

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

## Vendor Profile References

| Profile | Catalog shape | Signing name | Credential model | RustFS claim |
|---|---|---|---|---|
| `rustfs` | `{endpoint}/iceberg` | `s3` | static S3 credentials | automated smoke target |
| `rustfs-compat` | `{endpoint}/_iceberg` | `s3tables` by default | static S3 credentials | compatibility smoke target |
| `rustfs-vended-credentials` | `{endpoint}/iceberg` | `s3` | catalog-vended table credentials after table creation | automated credential smoke target when server vending is enabled |
| `aws-s3tables` | `https://s3tables.{region}.amazonaws.com/iceberg` | `s3tables` | AWS IAM/session credentials | reference only |
| `minio-aistor` | `{endpoint}/_iceberg` | `s3tables` | policy-scoped S3 credentials | reference only |
| `cloudflare-r2-data-catalog` | catalog URI returned by R2 | `s3` | catalog-vended credentials | reference only |
| `oss-tables` | provider REST endpoint | `s3` | SigV4 S3FileIO credentials | reference only |

## Unsupported Inventory

Unsupported behavior is documented instead of hidden behind internal errors. The
current unsupported inventory is:

- credential vending: automated after table bootstrap with exact-prefix validation and a data-plane scope probe; full no-long-term-data-credential bootstrap is not claimed
- background maintenance worker: controlled run-once and heartbeat endpoints are registered; continuous in-process scheduling is not claimed
- manifest/data reachability cleanup: metadata maintenance reads manifest-list and manifest Avro references, reports manifest/data/delete reachability, and deletes only unreferenced table objects that pass the safety window
- snapshot expiration dry-run planning and manual catalog commit: supported through metadata maintenance reports
- automatic maintenance scheduling: external scheduler hook supported through the worker run endpoint; built-in periodic scheduling is not claimed
- compaction rewrite: controlled run-once support for unpartitioned Parquet binpack through metadata maintenance; built-in periodic scheduling, sort compaction, delete-file rewrite, and row-level compaction are not claimed
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

## Spark Manual Baseline

Spark validation should use the same RustFS endpoint and warehouse bucket as the
PyIceberg smoke test. The exact Spark and Iceberg package versions should be
recorded in the client matrix after each run.

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
count, and optional cleanup. Until Spark execution is automated in CI, do not
claim Spark support beyond a manually verified run with the exact Spark and
Iceberg versions recorded.
