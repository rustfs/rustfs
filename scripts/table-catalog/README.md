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
| PyIceberg | Automated smoke target | create namespace, create table, append, reload, scan |
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
| `aws-s3tables` | `https://s3tables.{region}.amazonaws.com/iceberg` | `s3tables` | AWS IAM/session credentials | reference only |
| `minio-aistor` | `{endpoint}/_iceberg` | `s3tables` | policy-scoped S3 credentials | reference only |
| `cloudflare-r2-data-catalog` | catalog URI returned by R2 | `s3` | catalog-vended credentials | reference only |
| `oss-tables` | provider REST endpoint | `s3` | SigV4 S3FileIO credentials | reference only |

## Unsupported Inventory

Unsupported behavior is documented instead of hidden behind internal errors. The
current unsupported inventory is:

- credential vending: non-secret table scope preview exists; real temporary credentials are not issued
- background maintenance worker: unsupported
- manifest/data reachability cleanup: unsupported
- snapshot expiration and compaction: unsupported
- Iceberg views: unsupported
- multi-table transactions: not a short-term production claim

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
