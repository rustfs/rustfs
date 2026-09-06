#!/usr/bin/env python3
"""DuckDB Iceberg REST Catalog smoke test for RustFS S3 Tables."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import engine_compatibility
import pyiceberg_smoke


DEFAULT_DUCKDB_VERSION = engine_compatibility.DEFAULT_DUCKDB_VERSION
IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,47}$")


@dataclass(frozen=True)
class DuckDBExecution:
    returncode: int
    batches: list[list[dict[str, Any]]]
    stdout: str
    stderr: str


@dataclass(frozen=True)
class DuckDBSmokeResult:
    client_version: str
    metadata_location: str
    row_count: int
    cleanup_result: str
    checks: dict[str, str]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    run_id = str(int(time.time()))
    parser = argparse.ArgumentParser(description="Run DuckDB Iceberg REST Catalog conformance against RustFS.")
    parser.add_argument("--endpoint", default=os.getenv("RUSTFS_ENDPOINT", "http://127.0.0.1:9000"))
    parser.add_argument("--access-key", default=os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"))
    parser.add_argument("--secret-key", default=os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin"))
    parser.add_argument("--region", default=os.getenv("RUSTFS_REGION", "us-east-1"))
    parser.add_argument("--bucket", default=os.getenv("RUSTFS_TABLE_BUCKET", "rustfs-duckdb-smoke"))
    parser.add_argument("--namespace", default=os.getenv("RUSTFS_TABLE_NAMESPACE", f"duckdb_smoke_{run_id}"))
    parser.add_argument("--table", default=os.getenv("RUSTFS_TABLE_NAME", "events"))
    parser.add_argument("--duckdb", default=os.getenv("DUCKDB_BIN", "duckdb"))
    parser.add_argument("--duckdb-version", default=DEFAULT_DUCKDB_VERSION)
    parser.add_argument("--timeout", type=float, default=float(os.getenv("RUSTFS_TABLE_SMOKE_TIMEOUT", "60")))
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--replace", action="store_true", help="Drop existing smoke tables with matching identifiers first.")
    parser.add_argument("--insecure", action="store_true")
    parser.add_argument("--live-evidence-output")
    parser.add_argument("--rustfs-build", default=os.getenv("RUSTFS_BUILD", "operator-recorded"))
    parser.add_argument("--git-sha", default=os.getenv("RUSTFS_GIT_SHA", "operator-recorded"))
    parser.add_argument("--catalog-backing", default=os.getenv("RUSTFS_TABLE_CATALOG_BACKING", "operator-recorded"))
    parser.add_argument("--operator", default=os.getenv("USER", "operator-recorded"))
    parser.add_argument("--run-timestamp-utc")
    args = parser.parse_args(argv)
    for label, value in [("namespace", args.namespace), ("table", args.table)]:
        if not IDENTIFIER_RE.fullmatch(value):
            parser.error(f"{label} must start with a letter and contain at most 48 ASCII letters, digits, or underscores")
    return args


def duckdb_path(value: str) -> str:
    resolved = shutil.which(value)
    if resolved is None:
        raise RuntimeError(f"DuckDB executable was not found: {value}")
    return resolved


def duckdb_client_version(executable: str, timeout: float) -> str:
    process = subprocess.run(
        [executable, "-csv", "-noheader", "-c", "SELECT version();"],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(f"DuckDB version probe failed: {process.stderr.strip()}")
    version = process.stdout.strip().removeprefix("v")
    if not version:
        raise RuntimeError("DuckDB version probe returned an empty version")
    return version


def parse_duckdb_json(stdout: str) -> list[list[dict[str, Any]]]:
    batches: list[list[dict[str, Any]]] = []
    decoder = json.JSONDecoder()
    offset = 0
    while offset < len(stdout):
        while offset < len(stdout) and stdout[offset].isspace():
            offset += 1
        if offset == len(stdout):
            break
        value, offset = decoder.raw_decode(stdout, offset)
        if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
            raise RuntimeError("DuckDB JSON output did not contain row objects")
        batches.append(value)
    return batches


def run_duckdb(executable: str, sql: str, timeout: float) -> DuckDBExecution:
    process = subprocess.run(
        [executable, "-json", "-c", sql],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    batches = parse_duckdb_json(process.stdout) if process.returncode == 0 else []
    return DuckDBExecution(process.returncode, batches, process.stdout, process.stderr)


def require_duckdb_success(execution: DuckDBExecution, label: str) -> None:
    if execution.returncode != 0:
        message = execution.stderr.strip() or execution.stdout.strip()
        raise RuntimeError(f"DuckDB {label} failed: {message}")


def require_duckdb_error(execution: DuckDBExecution, label: str, expected: str) -> None:
    if execution.returncode == 0:
        raise RuntimeError(f"DuckDB {label} unexpectedly succeeded")
    message = f"{execution.stdout}\n{execution.stderr}"
    if expected not in message:
        raise RuntimeError(f"DuckDB {label} failed without expected error {expected!r}: {message.strip()}")


def batches_with_column(execution: DuckDBExecution, column: str) -> list[list[dict[str, Any]]]:
    return [batch for batch in execution.batches if batch and column in batch[0]]


def table_name(base: str, suffix: str) -> str:
    return f"{base}_{suffix}"


def table_identifier(catalog: str, namespace: str, table: str) -> str:
    return ".".join(
        [
            engine_compatibility.quote_double_identifier(catalog),
            engine_compatibility.quote_double_identifier(namespace),
            engine_compatibility.quote_double_identifier(table),
        ]
    )


def profile_sql(
    args: argparse.Namespace,
    *,
    catalog: str,
    table: str,
    rest_path: str = "/iceberg",
    signing_name: str = "s3",
) -> str:
    return engine_compatibility.duckdb_rest_catalog_sql(
        endpoint=args.endpoint,
        warehouse=args.bucket,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region=args.region,
        catalog_name=catalog,
        namespace=args.namespace,
        table=table,
        rest_path=rest_path,
        rest_signing_name=signing_name,
    )


def attach_sql(
    args: argparse.Namespace,
    *,
    catalog: str,
    rest_path: str,
    signing_name: str,
    compatibility_options: bool,
    purge_requested: bool = False,
) -> str:
    options = [
        "  TYPE iceberg",
        f"  ENDPOINT {engine_compatibility.sql_string(f'{args.endpoint.rstrip('/')}{rest_path}')}",
        "  AUTHORIZATION_TYPE 'sigv4'",
        "  SECRET 'rustfs_s3'",
        f"  SIGV4_REGION {engine_compatibility.sql_string(args.region)}",
        f"  SIGV4_SERVICE {engine_compatibility.sql_string(signing_name)}",
        "  ACCESS_DELEGATION_MODE 'none'",
    ]
    if compatibility_options:
        options.extend(
            [
                "  STAGE_CREATE_TABLES false",
                "  SKIP_CREATE_TABLE_METADATA_UPDATES true",
                "  DISABLE_MULTI_TABLE_COMMIT true",
                "  REMOVE_FILES_ON_DELETE false",
                f"  PURGE_REQUESTED {'true' if purge_requested else 'false'}",
                "  SUPPORT_NESTED_NAMESPACES false",
            ]
        )
    rendered_options = ",\n".join(options)
    return (
        f"ATTACH {engine_compatibility.sql_string(args.bucket)} "
        f"AS {engine_compatibility.quote_double_identifier(catalog)} (\n{rendered_options}\n);\n"
    )


def canonical_positive_sql(args: argparse.Namespace, seed_table: str, write_table: str, purge_table: str, drop_table: str) -> str:
    catalog = "rustfs_duckdb"
    namespace = ".".join(
        [
            engine_compatibility.quote_double_identifier(catalog),
            engine_compatibility.quote_double_identifier(args.namespace),
        ]
    )
    write_identifier = table_identifier(catalog, args.namespace, write_table)
    purge_identifier = table_identifier(catalog, args.namespace, purge_table)
    drop_identifier = table_identifier(catalog, args.namespace, drop_table)
    return profile_sql(args, catalog=catalog, table=seed_table) + "\n".join(
        [
            f"CREATE SCHEMA IF NOT EXISTS {namespace};",
            f"CREATE TABLE {write_identifier} (id BIGINT, payload VARCHAR);",
            f"INSERT INTO {write_identifier} VALUES (10, 'ten'), (20, 'twenty');",
            f"UPDATE {write_identifier} SET payload = 'TWENTY' WHERE id = 20;",
            f"DELETE FROM {write_identifier} WHERE id = 10;",
            f"ALTER TABLE {write_identifier} ADD COLUMN category VARCHAR;",
            f"INSERT INTO {write_identifier} VALUES (30, 'thirty', 'new');",
            f"MERGE INTO {write_identifier} AS target",
            "USING (VALUES (20, 'twenty-merged', 'merged'), (40, 'forty', 'inserted')) AS source(id, payload, category)",
            "ON target.id = source.id",
            "WHEN MATCHED THEN UPDATE SET payload = source.payload, category = source.category",
            "WHEN NOT MATCHED THEN INSERT (id, payload, category) VALUES (source.id, source.payload, source.category);",
            f"DELETE FROM {write_identifier} WHERE id = 30;",
            f"SELECT id, payload, category FROM {write_identifier} ORDER BY id;",
            f"SELECT count(*) AS snapshot_count FROM iceberg_snapshots({write_identifier});",
            f"CREATE TABLE {drop_identifier} (id BIGINT);",
            f"INSERT INTO {drop_identifier} VALUES (1);",
            f"DROP TABLE {drop_identifier};",
            f"CREATE TABLE {purge_identifier} (id BIGINT);",
            f"INSERT INTO {purge_identifier} VALUES (1);",
            f"SELECT count(*) AS row_count FROM {write_identifier};",
        ]
    ) + "\n"


def alias_sql(args: argparse.Namespace, write_table: str) -> str:
    catalog = "rustfs_compat"
    identifier = table_identifier(catalog, args.namespace, write_table)
    return profile_sql(args, catalog=catalog, table=write_table, rest_path="/_iceberg", signing_name="s3tables") + "\n".join(
        [
            f"INSERT INTO {identifier} VALUES (50, 'fifty', 'compat');",
            f"SELECT count(*) AS alias_row_count FROM {identifier};",
            f"DELETE FROM {identifier} WHERE id = 50;",
            f"SELECT count(*) AS alias_final_row_count FROM {identifier};",
        ]
    ) + "\n"


def concurrent_insert_sql(args: argparse.Namespace, catalog: str, write_table: str, row_id: int) -> str:
    identifier = table_identifier(catalog, args.namespace, write_table)
    return profile_sql(args, catalog=catalog, table=write_table) + f"INSERT INTO {identifier} VALUES ({row_id}, 'writer-{row_id}', 'concurrent');\n"


def multi_table_sql(args: argparse.Namespace, seed_table: str, write_table: str, purge_table: str) -> str:
    catalog = "multi_table"
    first = table_identifier(catalog, args.namespace, write_table)
    second = table_identifier(catalog, args.namespace, purge_table)
    return profile_sql(args, catalog=catalog, table=seed_table) + "\n".join(
        [
            "BEGIN TRANSACTION;",
            f"INSERT INTO {first} VALUES (999, 'multi-a', 'non-atomic');",
            f"INSERT INTO {second} VALUES (999);",
            "COMMIT;",
        ]
    ) + "\n"


def negative_sql(args: argparse.Namespace, *, kind: str, seed_table: str, write_table: str, purge_table: str) -> str:
    bootstrap = f"bootstrap_{kind}"
    sql = profile_sql(args, catalog=bootstrap, table=seed_table)
    sql += f"DETACH {engine_compatibility.quote_double_identifier(bootstrap)};\n"
    if kind == "stage-create":
        catalog = "stage_default"
        sql += attach_sql(
            args,
            catalog=catalog,
            rest_path="/iceberg",
            signing_name="s3",
            compatibility_options=False,
        )
        sql += f"CREATE TABLE {table_identifier(catalog, args.namespace, table_name(args.table, 'stage'))} (id BIGINT);\n"
        return sql
    if kind == "purge":
        catalog = "purge_requested"
        sql += attach_sql(
            args,
            catalog=catalog,
            rest_path="/iceberg",
            signing_name="s3",
            compatibility_options=True,
            purge_requested=True,
        )
        sql += f"DROP TABLE {table_identifier(catalog, args.namespace, purge_table)};\n"
        return sql
    if kind == "format-v3":
        catalog = "format_v3"
        sql += attach_sql(
            args,
            catalog=catalog,
            rest_path="/iceberg",
            signing_name="s3",
            compatibility_options=True,
        )
        identifier = table_identifier(catalog, args.namespace, table_name(args.table, "v3"))
        sql += f"CREATE TABLE {identifier} (id BIGINT) WITH ('format-version' = '3');\n"
        return sql
    raise ValueError(f"unknown negative DuckDB smoke kind: {kind}")


def pyiceberg_args(args: argparse.Namespace) -> argparse.Namespace:
    return argparse.Namespace(
        profile="rustfs",
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        region=args.region,
        bucket=args.bucket,
        warehouse=None,
        table_bucket=None,
        account_id="000000000000",
        warehouse_name=None,
        catalog_uri=None,
        namespace=args.namespace,
        table=args.table,
        catalog_name="rustfs_duckdb_pyiceberg",
        rest_path="/iceberg",
        rest_signing_name="s3",
        require_vended_credentials=False,
        timeout=args.timeout,
        insecure=args.insecure,
    )


def prepare_smoke_tables(catalog: Any, namespace: str, tables: list[str], replace: bool) -> None:
    existing = [table for table in tables if pyiceberg_smoke.table_exists(catalog, (namespace, table))]
    if existing and not replace:
        identifiers = ", ".join(f"{namespace}.{table}" for table in existing)
        raise RuntimeError(f"DuckDB smoke tables already exist: {identifiers}; rerun with --replace to remove them")
    for table in existing:
        catalog.drop_table((namespace, table))


def seed_pyiceberg_table(catalog: Any, args: argparse.Namespace, deps: pyiceberg_smoke.RuntimeDeps, table: str) -> None:
    identifier = (args.namespace, table)
    schema = deps.pyarrow.schema(
        [
            deps.pyarrow.field("id", deps.pyarrow.int64(), nullable=False),
            deps.pyarrow.field("payload", deps.pyarrow.string(), nullable=False),
        ]
    )
    created = catalog.create_table(identifier, schema=schema)
    created.append(
        deps.pyarrow.Table.from_pylist(
            [{"id": 1, "payload": "alpha"}, {"id": 2, "payload": "beta"}],
            schema=schema,
        )
    )


def pyiceberg_rows(catalog: Any, namespace: str, table: str) -> list[dict[str, Any]]:
    rows = catalog.load_table((namespace, table)).scan().to_arrow().to_pylist()
    return sorted(rows, key=lambda row: row["id"])


def run_concurrent_inserts(executable: str, args: argparse.Namespace, write_table: str) -> str:
    probes = [("writer_a", 60), ("writer_b", 70)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(run_duckdb, executable, concurrent_insert_sql(args, catalog, write_table, row_id), args.timeout)
            for catalog, row_id in probes
        ]
    executions = [future.result() for future in futures]
    retried = False
    for (catalog, row_id), execution in zip(probes, executions, strict=True):
        if execution.returncode == 0:
            continue
        error_text = f"{execution.stdout}\n{execution.stderr}".lower()
        if not any(marker in error_text for marker in ["409", "conflict", "version token"]):
            require_duckdb_success(execution, f"concurrent writer {row_id}")
        retried = True
        retry = run_duckdb(executable, concurrent_insert_sql(args, f"{catalog}_retry", write_table, row_id), args.timeout)
        require_duckdb_success(retry, f"concurrent writer retry {row_id}")
    return "passed-with-serial-retry" if retried else "passed-concurrently"


def cleanup_tables(catalog: Any, namespace: str, tables: list[str], *, drop_namespace: bool) -> str:
    cleanup_errors: list[str] = []
    for table in tables:
        try:
            pyiceberg_smoke.drop_table_if_present(catalog, (namespace, table))
        except Exception as error:
            cleanup_errors.append(f"{table}: {error}")
    if drop_namespace:
        try:
            catalog.drop_namespace(namespace)
        except Exception as error:
            cleanup_errors.append(f"namespace: {error}")
    if cleanup_errors:
        raise RuntimeError("DuckDB smoke cleanup failed: " + "; ".join(cleanup_errors))
    return "dropped-tables-and-namespace" if drop_namespace else "dropped-tables-preserved-existing-namespace"


def run_smoke(args: argparse.Namespace, deps: pyiceberg_smoke.RuntimeDeps) -> DuckDBSmokeResult:
    executable = duckdb_path(args.duckdb)
    client_version = duckdb_client_version(executable, args.timeout)
    if client_version != args.duckdb_version:
        raise RuntimeError(f"expected DuckDB {args.duckdb_version}, found {client_version}")

    endpoint = pyiceberg_smoke.normalized_endpoint(args.endpoint)
    pyiceberg_smoke.ensure_local_proxy_bypass(endpoint)
    pyiceberg_smoke.ensure_aws_env(args.access_key, args.secret_key, args.region)
    iceberg_args = pyiceberg_args(args)
    pyiceberg_smoke.ensure_bucket(iceberg_args, deps)
    pyiceberg_smoke.enable_table_bucket(iceberg_args, deps)

    seed_table = table_name(args.table, "seed")
    write_table = table_name(args.table, "write")
    purge_table = table_name(args.table, "purge")
    drop_table = table_name(args.table, "drop")
    stage_table = table_name(args.table, "stage")
    v3_table = table_name(args.table, "v3")
    smoke_tables = [seed_table, write_table, purge_table, drop_table, stage_table, v3_table]
    catalog = pyiceberg_smoke.load_rest_catalog(iceberg_args, deps)
    namespace_preexisting = bool(catalog.namespace_exists(args.namespace))
    prepare_smoke_tables(catalog, args.namespace, smoke_tables, args.replace)
    pyiceberg_smoke.ensure_namespace(catalog, args.namespace)
    seed_pyiceberg_table(catalog, args, deps, seed_table)

    checks: dict[str, str] = {}
    cleanup_result = "not-requested"
    metadata_location = "operator-recorded"
    try:
        positive = run_duckdb(
            executable,
            canonical_positive_sql(args, seed_table, write_table, purge_table, drop_table),
            args.timeout,
        )
        require_duckdb_success(positive, "canonical REST catalog lifecycle")
        row_count_batches = batches_with_column(positive, "row_count")
        if len(row_count_batches) < 2 or row_count_batches[0][0]["row_count"] != 2 or row_count_batches[-1][0]["row_count"] != 2:
            raise RuntimeError("DuckDB canonical REST catalog row counts did not remain at 2")
        result_batches = batches_with_column(positive, "id")
        expected_rows = [
            {"id": 20, "payload": "twenty-merged", "category": "merged"},
            {"id": 40, "payload": "forty", "category": "inserted"},
        ]
        if not result_batches or result_batches[-1] != expected_rows:
            raise RuntimeError(f"DuckDB canonical DML returned unexpected rows: {result_batches[-1] if result_batches else []}")
        snapshot_batches = batches_with_column(positive, "snapshot_count")
        if not snapshot_batches or snapshot_batches[-1][0]["snapshot_count"] < 1:
            raise RuntimeError("DuckDB snapshot metadata probe returned no snapshots")
        if catalog.table_exists((args.namespace, drop_table)):
            raise RuntimeError("DuckDB DROP TABLE did not remove the catalog entry")
        checks["canonical_rest_catalog"] = "pass"
        checks["single_table_ddl_dml"] = "pass"
        checks["schema_evolution"] = "pass"
        checks["snapshot_metadata"] = "pass"

        if pyiceberg_rows(catalog, args.namespace, write_table) != expected_rows:
            raise RuntimeError("PyIceberg did not observe DuckDB-created table rows")
        checks["pyiceberg_cross_read"] = "pass"

        alias = run_duckdb(executable, alias_sql(args, write_table), args.timeout)
        require_duckdb_success(alias, "s3tables compatibility alias")
        alias_counts = batches_with_column(alias, "alias_row_count")
        alias_final_counts = batches_with_column(alias, "alias_final_row_count")
        if not alias_counts or alias_counts[-1][0]["alias_row_count"] != 3:
            raise RuntimeError("DuckDB compatibility alias insert did not produce row_count=3")
        if not alias_final_counts or alias_final_counts[-1][0]["alias_final_row_count"] != 2:
            raise RuntimeError("DuckDB compatibility alias cleanup did not restore row_count=2")
        checks["s3tables_alias"] = "pass"

        negatives = [
            ("stage-create", "stage-create is not supported"),
            ("purge", "purgeRequested=true is not supported"),
            ("format-v3", "unsupported Iceberg table format-version: 3"),
        ]
        for kind, expected_error in negatives:
            execution = run_duckdb(
                executable,
                negative_sql(
                    args,
                    kind=kind,
                    seed_table=seed_table,
                    write_table=write_table,
                    purge_table=purge_table,
                ),
                args.timeout,
            )
            require_duckdb_error(execution, kind, expected_error)
            checks[kind] = "failed-closed"
        if catalog.table_exists((args.namespace, stage_table)) or catalog.table_exists((args.namespace, v3_table)):
            raise RuntimeError("a failed DuckDB create probe left a catalog table behind")
        if not catalog.table_exists((args.namespace, purge_table)):
            raise RuntimeError("purgeRequested=true removed a table despite the expected failure")

        multi_table = run_duckdb(
            executable,
            multi_table_sql(args, seed_table, write_table, purge_table),
            args.timeout,
        )
        require_duckdb_success(multi_table, "multi-table endpoint-disabled mode")
        if not any(row["id"] == 999 for row in pyiceberg_rows(catalog, args.namespace, write_table)):
            raise RuntimeError("DuckDB multi-table endpoint-disabled mode did not commit the first table")
        if not any(row["id"] == 999 for row in pyiceberg_rows(catalog, args.namespace, purge_table)):
            raise RuntimeError("DuckDB multi-table endpoint-disabled mode did not commit the second table")
        multi_cleanup_catalog = "cleanup_multi_table"
        multi_cleanup = profile_sql(args, catalog=multi_cleanup_catalog, table=seed_table) + "\n".join(
            [
                f"DELETE FROM {table_identifier(multi_cleanup_catalog, args.namespace, write_table)} WHERE id = 999;",
                f"DELETE FROM {table_identifier(multi_cleanup_catalog, args.namespace, purge_table)} WHERE id = 999;",
            ]
        )
        require_duckdb_success(
            run_duckdb(executable, multi_cleanup, args.timeout),
            "multi-table endpoint-disabled cleanup",
        )
        checks["multi_table_endpoint_disabled"] = "pass-single-table-atomicity-only"

        checks["concurrent_writers"] = run_concurrent_inserts(executable, args, write_table)
        concurrent_rows = pyiceberg_rows(catalog, args.namespace, write_table)
        if [row["id"] for row in concurrent_rows] != [20, 40, 60, 70]:
            raise RuntimeError(f"concurrent DuckDB writers produced unexpected rows: {concurrent_rows}")
        cleanup_catalog = "cleanup_concurrency"
        cleanup_identifier = table_identifier(cleanup_catalog, args.namespace, write_table)
        cleanup_sql = profile_sql(args, catalog=cleanup_catalog, table=write_table) + "\n".join(
            [
                f"DELETE FROM {cleanup_identifier} WHERE id IN (60, 70);",
                f"SELECT count(*) AS final_row_count FROM {cleanup_identifier};",
            ]
        )
        cleanup_execution = run_duckdb(executable, cleanup_sql, args.timeout)
        require_duckdb_success(cleanup_execution, "concurrency cleanup")
        final_batches = batches_with_column(cleanup_execution, "final_row_count")
        if not final_batches or final_batches[-1][0]["final_row_count"] != 2:
            raise RuntimeError("DuckDB concurrency cleanup did not restore row_count=2")

        final_table = catalog.load_table((args.namespace, write_table))
        final_rows = sorted(final_table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])
        if final_rows != expected_rows:
            raise RuntimeError(f"final PyIceberg cross-read returned unexpected rows: {final_rows}")
        metadata_location = pyiceberg_smoke.table_metadata_location(final_table) or "operator-recorded"
        if metadata_location == "operator-recorded":
            response = pyiceberg_smoke.signed_rest_request(
                argparse.Namespace(**{**vars(iceberg_args), "table": write_table}),
                deps,
                "GET",
                f"/iceberg/v1/{urllib.parse.quote(args.bucket, safe='')}/namespaces/"
                f"{urllib.parse.quote(args.namespace, safe='')}/tables/{urllib.parse.quote(write_table, safe='')}",
            )
            metadata_location = response.get("metadata-location", "operator-recorded")
        if metadata_location == "operator-recorded":
            raise RuntimeError("DuckDB smoke could not resolve the final metadata location")
        metadata_scan = run_duckdb(
            executable,
            engine_compatibility.duckdb_sql_probe(
                endpoint=args.endpoint,
                access_key=args.access_key,
                secret_key=args.secret_key,
                region=args.region,
                metadata_location=metadata_location,
            ),
            args.timeout,
        )
        require_duckdb_success(metadata_scan, "metadata-location scan")
        metadata_counts = batches_with_column(metadata_scan, "row_count")
        if not metadata_counts or metadata_counts[-1][0]["row_count"] != 2:
            raise RuntimeError("DuckDB metadata-location scan did not return row_count=2")
        checks["metadata_location_scan"] = "pass"
    finally:
        if args.cleanup:
            cleanup_result = cleanup_tables(
                catalog,
                args.namespace,
                smoke_tables,
                drop_namespace=not namespace_preexisting,
            )

    return DuckDBSmokeResult(client_version, metadata_location, 2, cleanup_result, checks)


def current_utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_live_evidence(args: argparse.Namespace, result: DuckDBSmokeResult) -> None:
    if not args.live_evidence_output:
        return
    command = pyiceberg_smoke.redacted_command(sys.argv)
    record = engine_compatibility.live_conformance_evidence_record(
        client_name="DuckDB Iceberg",
        client_version=result.client_version,
        scenario="rest-catalog-single-table-read-write-cross-engine-negative-boundaries",
        rustfs_build=args.rustfs_build,
        git_sha=args.git_sha,
        catalog_backing=args.catalog_backing,
        endpoint=args.endpoint,
        warehouse=args.bucket,
        rest_path="/iceberg",
        namespace=args.namespace,
        table=table_name(args.table, "write"),
        metadata_location=result.metadata_location,
        run_timestamp_utc=args.run_timestamp_utc or current_utc_timestamp(),
        operator=args.operator,
        expected_status="pass",
        observed_status="pass",
        row_count=result.row_count,
        cleanup_result=result.cleanup_result,
        claim="automated-rest-catalog-smoke",
        command=command,
    )
    document = {
        "live_conformance_evidence": record,
        "checks": result.checks,
        "validation": engine_compatibility.validate_live_conformance_evidence(record),
    }
    Path(args.live_evidence_output).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    try:
        deps = pyiceberg_smoke.load_runtime_deps()
        result = run_smoke(args, deps)
        write_live_evidence(args, result)
        print(json.dumps({"status": "pass", "row_count": result.row_count, "checks": result.checks}, sort_keys=True))
        return 0
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
