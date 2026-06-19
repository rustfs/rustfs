#!/usr/bin/env python3
"""Machine-readable Iceberg engine compatibility helpers for RustFS S3 Tables."""

from __future__ import annotations

import argparse
import json
from collections import OrderedDict
from io import StringIO
from typing import Any


def scenario(name: str, status: str, evidence: str) -> dict[str, str]:
    return {
        "name": name,
        "status": status,
        "evidence": evidence,
    }


def engine_compatibility_matrix() -> list[dict[str, Any]]:
    return [
        {
            "client": "PyIceberg",
            "status": "automated-smoke",
            "entrypoint": "scripts/table-catalog/pyiceberg_smoke.py",
            "scenarios": [
                scenario("create-namespace", "automated", "PyIceberg catalog.create_namespace_if_not_exists/create_namespace"),
                scenario("create-table", "automated", "PyIceberg catalog.create_table"),
                scenario("append", "automated", "PyIceberg table.append with PyArrow rows"),
                scenario("reload-table", "automated", "PyIceberg catalog.load_table after append"),
                scenario("scan", "automated", "PyIceberg table.scan().to_arrow"),
                scenario("drop-table", "automated-with-cleanup", "PyIceberg catalog.drop_table when --cleanup or --replace is set"),
                scenario("commit-conflict", "direct-rest-probe-required", "catalog commit conflict remains a follow-up live probe"),
            ],
        },
        {
            "client": "Spark Iceberg REST catalog",
            "status": "generated-smoke-harness",
            "entrypoint": "scripts/table-catalog/engine_compatibility.py --print-spark-sql",
            "scenarios": [
                scenario("create-namespace", "generated-spark-sql", "CREATE NAMESPACE IF NOT EXISTS"),
                scenario("create-table", "generated-spark-sql", "CREATE TABLE USING iceberg"),
                scenario("append", "generated-spark-sql", "INSERT INTO"),
                scenario("reload-table", "generated-spark-sql", "REFRESH TABLE and SELECT COUNT"),
                scenario("drop-table", "generated-spark-sql", "DROP TABLE and optional DROP NAMESPACE"),
                scenario("commit-conflict", "manual-validation-required", "requires a two-writer Spark or REST conflict harness"),
            ],
        },
        {
            "client": "Trino Iceberg REST catalog",
            "status": "documented-read-path",
            "entrypoint": "scripts/table-catalog/README.md",
            "scenarios": [
                scenario("catalog-load", "manual-validation-required", "REST catalog configuration reference"),
                scenario("read-table", "manual-validation-required", "SELECT from a table created by PyIceberg or Spark"),
                scenario("write-table", "not-claimed", "Trino write compatibility is not claimed by this harness"),
            ],
        },
        {
            "client": "DuckDB Iceberg",
            "status": "documented-read-path",
            "entrypoint": "scripts/table-catalog/README.md",
            "scenarios": [
                scenario("catalog-load", "manual-validation-required", "REST catalog extension/configuration reference"),
                scenario("read-table", "manual-validation-required", "read-path verification only"),
                scenario("write-table", "not-claimed", "DuckDB write/commit compatibility is not claimed"),
            ],
        },
        {
            "client": "StarRocks Iceberg REST catalog",
            "status": "documented-read-path",
            "entrypoint": "scripts/table-catalog/README.md",
            "scenarios": [
                scenario("catalog-load", "manual-validation-required", "REST catalog configuration reference"),
                scenario("read-table", "manual-validation-required", "external catalog read-path verification only"),
                scenario("write-table", "not-claimed", "StarRocks write/commit compatibility is not claimed"),
            ],
        },
        {
            "client": "Snowflake Open Catalog / Iceberg integrations",
            "status": "reference-only",
            "entrypoint": "scripts/table-catalog/README.md",
            "scenarios": [
                scenario("catalog-load", "not-claimed", "reference only until a repeatable external integration harness exists"),
            ],
        },
        {
            "client": "Databend",
            "status": "s3-data-plane-reference",
            "entrypoint": "scripts/table-catalog/README.md",
            "scenarios": [
                scenario("s3-data-plane-read", "manual-validation-required", "S3 stage/data-plane reference only"),
                scenario("iceberg-rest-catalog", "not-claimed", "Databend REST catalog integration is not claimed"),
            ],
        },
    ]


def normalized_endpoint(endpoint: str) -> str:
    return endpoint.rstrip("/")


def normalized_rest_path(rest_path: str) -> str:
    stripped = rest_path.strip()
    if not stripped:
        raise ValueError("REST catalog path cannot be empty")
    if not stripped.startswith("/"):
        stripped = f"/{stripped}"
    return stripped.rstrip("/")


def spark_catalog_config(
    *,
    endpoint: str,
    warehouse: str,
    access_key: str,
    secret_key: str,
    region: str,
    catalog_name: str,
    rest_path: str,
    rest_signing_name: str,
) -> OrderedDict[str, str]:
    endpoint = normalized_endpoint(endpoint)
    rest_path = normalized_rest_path(rest_path)
    prefix = f"spark.sql.catalog.{catalog_name}"
    return OrderedDict(
        [
            (prefix, "org.apache.iceberg.spark.SparkCatalog"),
            (f"{prefix}.type", "rest"),
            (f"{prefix}.uri", f"{endpoint}{rest_path}"),
            (f"{prefix}.warehouse", warehouse),
            (f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
            (f"{prefix}.s3.endpoint", endpoint),
            (f"{prefix}.s3.path-style-access", "true"),
            (f"{prefix}.rest.sigv4-enabled", "true"),
            (f"{prefix}.rest.signing-name", rest_signing_name),
            (f"{prefix}.rest.signing-region", region),
            (f"{prefix}.s3.access-key-id", access_key),
            (f"{prefix}.s3.secret-access-key", secret_key),
        ]
    )


def quote_spark_identifier(identifier: str) -> str:
    if not identifier or "`" in identifier or "\n" in identifier or "\r" in identifier:
        raise ValueError("Spark identifier must be non-empty and must not contain backticks or newlines")
    return f"`{identifier}`"


def spark_table_identifier(catalog_name: str, namespace: str, table: str) -> str:
    return ".".join(
        [
            catalog_name,
            quote_spark_identifier(namespace),
            quote_spark_identifier(table),
        ]
    )


def spark_sql_smoke(
    *,
    catalog_name: str,
    namespace: str,
    table: str,
    cleanup: bool = False,
) -> str:
    namespace_identifier = f"{catalog_name}.{quote_spark_identifier(namespace)}"
    table_identifier = spark_table_identifier(catalog_name, namespace, table)
    statements = [
        f"CREATE NAMESPACE IF NOT EXISTS {namespace_identifier};",
        f"DROP TABLE IF EXISTS {table_identifier};",
        f"CREATE TABLE {table_identifier} (id BIGINT, payload STRING) USING iceberg;",
        f"INSERT INTO {table_identifier} VALUES (1, 'alpha'), (2, 'beta');",
        f"REFRESH TABLE {table_identifier};",
        f"SELECT COUNT(*) AS row_count FROM {table_identifier};",
    ]
    if cleanup:
        statements.extend(
            [
                f"DROP TABLE IF EXISTS {table_identifier};",
                f"DROP NAMESPACE IF EXISTS {namespace_identifier};",
            ]
        )
    return "\n".join(statements) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print RustFS S3 Tables Iceberg engine compatibility helpers.")
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--access-key", default="rustfsadmin")
    parser.add_argument("--secret-key", default="rustfsadmin")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--warehouse", default="rustfs-s3table-smoke")
    parser.add_argument("--namespace", default="smoke")
    parser.add_argument("--table", default="events")
    parser.add_argument("--catalog-name", default="rustfs")
    parser.add_argument("--rest-path", default="/iceberg")
    parser.add_argument("--rest-signing-name", default="s3")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--print-engine-matrix", action="store_true")
    parser.add_argument("--print-spark-config", action="store_true")
    parser.add_argument("--print-spark-sql", action="store_true")
    return parser.parse_args(argv)


def print_json(document: Any, output: StringIO | None = None) -> None:
    text = json.dumps(document, indent=2, sort_keys=True)
    if output is None:
        print(text)
    else:
        output.write(f"{text}\n")


def cli_json(argv: list[str]) -> str:
    output = StringIO()
    run(parse_args(argv), output)
    return output.getvalue()


def run(args: argparse.Namespace, output: StringIO | None = None) -> None:
    printed = False
    if args.print_engine_matrix:
        print_json({"engine_compatibility": engine_compatibility_matrix()}, output)
        printed = True
    if args.print_spark_config:
        print_json(
            {
                "spark_config": spark_catalog_config(
                    endpoint=args.endpoint,
                    warehouse=args.warehouse,
                    access_key=args.access_key,
                    secret_key=args.secret_key,
                    region=args.region,
                    catalog_name=args.catalog_name,
                    rest_path=args.rest_path,
                    rest_signing_name=args.rest_signing_name,
                )
            },
            output,
        )
        printed = True
    if args.print_spark_sql:
        sql = spark_sql_smoke(
            catalog_name=args.catalog_name,
            namespace=args.namespace,
            table=args.table,
            cleanup=args.cleanup,
        )
        if output is None:
            print(sql, end="")
        else:
            output.write(sql)
        printed = True
    if not printed:
        print_json({"engine_compatibility": engine_compatibility_matrix()}, output)


def main() -> None:
    run(parse_args())


if __name__ == "__main__":
    main()
