#!/usr/bin/env python3
"""Unit tests for the RustFS DuckDB REST Catalog smoke helper."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import duckdb_smoke


class DuckDBSmokeTest(unittest.TestCase):
    def args(self) -> argparse.Namespace:
        return argparse.Namespace(
            endpoint="http://127.0.0.1:9000",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="us-east-1",
            bucket="rustfs-duckdb-smoke",
            namespace="duckdb_smoke",
            table="events",
            duckdb="duckdb",
            duckdb_version="1.5.5",
            timeout=60.0,
            cleanup=True,
            replace=False,
            insecure=False,
            live_evidence_output=None,
            rustfs_build="rustfs-test",
            git_sha="abc123",
            catalog_backing="object",
            operator="test-operator",
            run_timestamp_utc="2026-08-27T00:00:00Z",
        )

    def test_parse_args_rejects_unsafe_identifiers(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                duckdb_smoke.parse_args(["--namespace", "bad-name"])

    def test_duckdb_client_version_removes_v_prefix(self) -> None:
        process = SimpleNamespace(returncode=0, stdout="v1.5.5\n", stderr="")
        with mock.patch.object(duckdb_smoke.subprocess, "run", return_value=process):
            self.assertEqual(duckdb_smoke.duckdb_client_version("duckdb", 10), "1.5.5")

    def test_parse_duckdb_json_accepts_multiple_query_batches(self) -> None:
        batches = duckdb_smoke.parse_duckdb_json(
            '[{"row_count":2}]\n[{"id":20},\n{"id":40}]\n'
        )

        self.assertEqual(batches[0][0]["row_count"], 2)
        self.assertEqual([row["id"] for row in batches[1]], [20, 40])

    def test_run_duckdb_does_not_parse_json_for_failed_process(self) -> None:
        process = SimpleNamespace(returncode=1, stdout="not-json", stderr="expected failure")
        with mock.patch.object(duckdb_smoke.subprocess, "run", return_value=process):
            execution = duckdb_smoke.run_duckdb("duckdb", "SELECT 1", 10)

        self.assertEqual(execution.returncode, 1)
        self.assertEqual(execution.batches, [])

    def test_concurrent_retry_markers_do_not_accept_generic_commit_errors(self) -> None:
        args = self.args()
        generic_failure = duckdb_smoke.DuckDBExecution(1, [], "", "commit failed: permission denied")
        with mock.patch.object(duckdb_smoke, "run_duckdb", return_value=generic_failure):
            with self.assertRaisesRegex(RuntimeError, "permission denied"):
                duckdb_smoke.run_concurrent_inserts("duckdb", args, "events_write")

    def test_canonical_sql_covers_single_table_lifecycle(self) -> None:
        sql = duckdb_smoke.canonical_positive_sql(
            self.args(),
            "events_seed",
            "events_write",
            "events_purge",
            "events_drop",
        )

        self.assertIn("STAGE_CREATE_TABLES false", sql)
        self.assertIn("SKIP_CREATE_TABLE_METADATA_UPDATES true", sql)
        self.assertIn("CREATE TABLE", sql)
        self.assertIn("INSERT INTO", sql)
        self.assertIn("UPDATE", sql)
        self.assertIn("DELETE FROM", sql)
        self.assertIn("MERGE INTO", sql)
        self.assertIn("ALTER TABLE", sql)
        self.assertIn("iceberg_snapshots", sql)
        self.assertNotIn("DROP TABLE IF EXISTS", sql)
        self.assertIn('DROP TABLE "rustfs_duckdb"."duckdb_smoke"."events_drop"', sql)

    def test_prepare_smoke_tables_refuses_existing_identifiers_without_replace(self) -> None:
        catalog = mock.Mock()
        catalog.table_exists.side_effect = lambda identifier: identifier[1] == "events_write"

        with self.assertRaisesRegex(RuntimeError, "duckdb_smoke.events_write"):
            duckdb_smoke.prepare_smoke_tables(
                catalog,
                "duckdb_smoke",
                ["events_seed", "events_write"],
                replace=False,
            )

        catalog.drop_table.assert_not_called()

    def test_prepare_smoke_tables_replaces_only_existing_identifiers_when_requested(self) -> None:
        catalog = mock.Mock()
        catalog.table_exists.side_effect = lambda identifier: identifier[1] == "events_write"

        duckdb_smoke.prepare_smoke_tables(
            catalog,
            "duckdb_smoke",
            ["events_seed", "events_write"],
            replace=True,
        )

        catalog.drop_table.assert_called_once_with(("duckdb_smoke", "events_write"))

    def test_cleanup_preserves_a_preexisting_namespace(self) -> None:
        catalog = mock.Mock()
        catalog.table_exists.return_value = False

        result = duckdb_smoke.cleanup_tables(
            catalog,
            "duckdb_smoke",
            ["events_seed", "events_write"],
            drop_namespace=False,
        )

        self.assertEqual(result, "dropped-tables-preserved-existing-namespace")
        catalog.drop_namespace.assert_not_called()

    def test_alias_sql_uses_s3tables_signing(self) -> None:
        sql = duckdb_smoke.alias_sql(self.args(), "events_write")

        self.assertIn("ENDPOINT 'http://127.0.0.1:9000/_iceberg'", sql)
        self.assertIn("SIGV4_SERVICE 's3tables'", sql)
        self.assertIn("INSERT INTO", sql)
        self.assertIn("alias_final_row_count", sql)

    def test_boundary_sql_records_required_compatibility_options(self) -> None:
        args = self.args()
        stage_sql = duckdb_smoke.negative_sql(
            args,
            kind="stage-create",
            seed_table="events_seed",
            write_table="events_write",
            purge_table="events_purge",
        )
        stage_attach = stage_sql.split('DETACH "bootstrap_stage-create";', 1)[1]
        self.assertNotIn("STAGE_CREATE_TABLES false", stage_attach)
        self.assertIn("CREATE TABLE", stage_attach)

        purge_sql = duckdb_smoke.negative_sql(
            args,
            kind="purge",
            seed_table="events_seed",
            write_table="events_write",
            purge_table="events_purge",
        )
        self.assertIn("PURGE_REQUESTED true", purge_sql)
        self.assertIn('DROP TABLE "purge_requested"."duckdb_smoke"."events_purge"', purge_sql)

        v3_sql = duckdb_smoke.negative_sql(
            args,
            kind="format-v3",
            seed_table="events_seed",
            write_table="events_write",
            purge_table="events_purge",
        )
        self.assertIn("'format-version' = '3'", v3_sql)

        multi_sql = duckdb_smoke.multi_table_sql(
            args,
            seed_table="events_seed",
            write_table="events_write",
            purge_table="events_purge",
        )
        self.assertIn("DISABLE_MULTI_TABLE_COMMIT true", multi_sql)
        self.assertIn("BEGIN TRANSACTION", multi_sql)
        self.assertIn("'non-atomic'", multi_sql)

    def test_pyiceberg_args_use_canonical_catalog(self) -> None:
        args = duckdb_smoke.pyiceberg_args(self.args())

        self.assertEqual(args.rest_path, "/iceberg")
        self.assertEqual(args.rest_signing_name, "s3")
        self.assertEqual(args.bucket, "rustfs-duckdb-smoke")

    def test_live_evidence_records_automated_duckdb_claim(self) -> None:
        args = self.args()
        result = duckdb_smoke.DuckDBSmokeResult(
            client_version="1.5.5",
            metadata_location="s3://rustfs-duckdb-smoke/metadata/00001.json",
            row_count=2,
            cleanup_result="dropped-tables-and-namespace",
            checks={"canonical_rest_catalog": "pass"},
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "evidence.json"
            args.live_evidence_output = str(output)
            with mock.patch.object(duckdb_smoke.sys, "argv", ["duckdb_smoke.py", "--secret-key", "secret"]):
                duckdb_smoke.write_live_evidence(args, result)
            document = json.loads(output.read_text(encoding="utf-8"))

        evidence = document["live_conformance_evidence"]
        self.assertEqual(evidence["client_name"], "DuckDB Iceberg")
        self.assertEqual(evidence["claim"], "automated-rest-catalog-smoke")
        self.assertIn("--secret-key '<redacted>'", evidence["command"])
        self.assertNotIn("--secret-key secret", evidence["command"])
        self.assertEqual(document["validation"]["status"], "accepted")


if __name__ == "__main__":
    unittest.main()
