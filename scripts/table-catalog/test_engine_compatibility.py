#!/usr/bin/env python3
"""Unit tests for the RustFS table catalog engine compatibility helper."""

from __future__ import annotations

import json
import unittest

import engine_compatibility


class EngineCompatibilityTest(unittest.TestCase):
    def test_matrix_records_required_engine_scenarios_without_overclaiming(self) -> None:
        matrix = engine_compatibility.engine_compatibility_matrix()
        by_client = {entry["client"]: entry for entry in matrix}

        self.assertIn("PyIceberg", by_client)
        self.assertIn("Spark Iceberg REST catalog", by_client)
        self.assertIn("Trino Iceberg REST catalog", by_client)
        self.assertIn("DuckDB Iceberg", by_client)
        self.assertIn("StarRocks Iceberg REST catalog", by_client)

        pyiceberg = by_client["PyIceberg"]
        self.assertEqual(pyiceberg["status"], "automated-smoke")
        self.assertContainsScenario(pyiceberg, "create-namespace", "automated")
        self.assertContainsScenario(pyiceberg, "create-table", "automated")
        self.assertContainsScenario(pyiceberg, "append", "automated")
        self.assertContainsScenario(pyiceberg, "reload-table", "automated")
        self.assertContainsScenario(pyiceberg, "drop-table", "automated-with-cleanup")

        spark = by_client["Spark Iceberg REST catalog"]
        self.assertEqual(spark["status"], "generated-smoke-harness")
        self.assertContainsScenario(spark, "create-namespace", "generated-spark-sql")
        self.assertContainsScenario(spark, "create-table", "generated-spark-sql")
        self.assertContainsScenario(spark, "append", "generated-spark-sql")
        self.assertContainsScenario(spark, "reload-table", "generated-spark-sql")
        self.assertContainsScenario(spark, "drop-table", "generated-spark-sql")
        self.assertContainsScenario(spark, "commit-conflict", "manual-validation-required")

        trino = by_client["Trino Iceberg REST catalog"]
        self.assertEqual(trino["status"], "documented-read-path")
        self.assertContainsScenario(trino, "catalog-load", "manual-validation-required")

    def test_spark_config_uses_rustfs_rest_catalog_and_s3fileio(self) -> None:
        config = engine_compatibility.spark_catalog_config(
            endpoint="http://127.0.0.1:9000",
            warehouse="rustfs-s3table-smoke",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="us-east-1",
            catalog_name="rustfs",
            rest_path="/iceberg",
            rest_signing_name="s3",
        )

        self.assertEqual(config["spark.sql.catalog.rustfs"], "org.apache.iceberg.spark.SparkCatalog")
        self.assertEqual(config["spark.sql.catalog.rustfs.type"], "rest")
        self.assertEqual(config["spark.sql.catalog.rustfs.uri"], "http://127.0.0.1:9000/iceberg")
        self.assertEqual(config["spark.sql.catalog.rustfs.warehouse"], "rustfs-s3table-smoke")
        self.assertEqual(config["spark.sql.catalog.rustfs.io-impl"], "org.apache.iceberg.aws.s3.S3FileIO")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.endpoint"], "http://127.0.0.1:9000")
        self.assertEqual(config["spark.sql.catalog.rustfs.rest.signing-name"], "s3")

    def test_spark_sql_smoke_covers_lifecycle_append_reload_and_cleanup(self) -> None:
        sql = engine_compatibility.spark_sql_smoke(
            catalog_name="rustfs",
            namespace="sales",
            table="orders",
            cleanup=True,
        )

        self.assertIn("CREATE NAMESPACE IF NOT EXISTS rustfs.`sales`", sql)
        self.assertIn("DROP TABLE IF EXISTS rustfs.`sales`.`orders`", sql)
        self.assertIn("CREATE TABLE rustfs.`sales`.`orders`", sql)
        self.assertIn("INSERT INTO rustfs.`sales`.`orders`", sql)
        self.assertIn("REFRESH TABLE rustfs.`sales`.`orders`", sql)
        self.assertIn("SELECT COUNT(*) AS row_count FROM rustfs.`sales`.`orders`", sql)
        self.assertIn("DROP TABLE IF EXISTS rustfs.`sales`.`orders`", sql)
        self.assertIn("DROP NAMESPACE IF EXISTS rustfs.`sales`", sql)

    def test_spark_sql_rejects_unsafe_identifiers(self) -> None:
        with self.assertRaisesRegex(ValueError, "Spark identifier"):
            engine_compatibility.spark_sql_smoke(
                catalog_name="rustfs",
                namespace="sales`prod",
                table="orders",
            )

    def test_cli_prints_machine_readable_engine_matrix(self) -> None:
        payload = engine_compatibility.cli_json(["--print-engine-matrix"])
        document = json.loads(payload)

        self.assertIn("engine_compatibility", document)
        self.assertTrue(document["engine_compatibility"])

    def assertContainsScenario(self, entry: dict[str, object], name: str, status: str) -> None:
        scenarios = entry.get("scenarios")
        self.assertIsInstance(scenarios, list)
        for scenario in scenarios:
            if isinstance(scenario, dict) and scenario.get("name") == name:
                self.assertEqual(scenario.get("status"), status)
                return
        self.fail(f"missing scenario {name!r} in {entry.get('client')!r}")


if __name__ == "__main__":
    unittest.main()
