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

## Machine-Readable Inventories

The script can print the current conformance inventories without importing
PyIceberg, PyArrow, or boto3:

```bash
python3 scripts/table-catalog/pyiceberg_smoke.py --print-client-matrix
python3 scripts/table-catalog/pyiceberg_smoke.py --print-vendor-profiles
python3 scripts/table-catalog/pyiceberg_smoke.py --print-unsupported-inventory
```

Use these outputs when updating release notes, PR descriptions, or follow-up
work items. They are intentionally conservative: only PyIceberg is automated by
this script today; other engines are documented until a repeatable harness is
added.

## Client Matrix

| Client | Current status | Claim |
|---|---|---|
| PyIceberg | Automated smoke target | create namespace, create table, append, reload, scan, optional catalog-vended table credentials with exact-prefix data-plane scope probe |
| Spark Iceberg REST catalog | Manual-ready | create/load/append/reload should be verified against a running RustFS endpoint |
| Trino Iceberg REST catalog | Documented, not automated | no write compatibility claim yet |
| DuckDB Iceberg | Documented, not automated | read-path reference only |
| Databend | Documented, not automated | S3 data-plane reference only; Iceberg REST catalog integration is not claimed |
| Snowflake/Open Catalog integrations | Documented, not automated | reference only |

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
- compaction rewrite: unsupported; planning reports fail closed until data file rewrite support is implemented
- Iceberg views: stable unsupported routes are registered and return explicit unsupported JSON
- external catalog bridges: metadata import/register is supported, but Polaris/Glue/DLF/Hive synchronization is unsupported
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
PyIceberg smoke test. The exact package version should be recorded in the client
matrix after each run.

Minimum configuration shape:

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
```

Until Spark is automated, do not claim Spark support beyond a manually verified
run with the exact Spark and Iceberg versions recorded.
