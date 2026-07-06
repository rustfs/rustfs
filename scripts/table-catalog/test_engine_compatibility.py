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
        self.assertEqual(spark["status"], "manual-live-harness")
        self.assertContainsScenario(spark, "create-namespace", "manual-live-harness")
        self.assertContainsScenario(spark, "create-table", "manual-live-harness")
        self.assertContainsScenario(spark, "append", "manual-live-harness")
        self.assertContainsScenario(spark, "reload-table", "manual-live-harness")
        self.assertContainsScenario(spark, "drop-table", "manual-live-harness")
        self.assertContainsScenario(spark, "commit-conflict", "manual-validation-required")

        trino = by_client["Trino Iceberg REST catalog"]
        self.assertEqual(trino["status"], "manual-live-read-probe")
        self.assertContainsScenario(trino, "catalog-load", "manual-live-probe")

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

    def test_spark_vendor_config_formats_aws_s3tables_profile(self) -> None:
        config = engine_compatibility.spark_vendor_catalog_config(
            profile="aws-s3tables",
            endpoint="https://s3tables.us-east-1.amazonaws.com",
            warehouse="ignored",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="us-east-1",
            catalog_name="rustfs",
            account_id="123456789012",
            table_bucket="analytics",
            catalog_uri=None,
            warehouse_name=None,
        )

        self.assertEqual(config["spark.sql.catalog.rustfs.uri"], "https://s3tables.us-east-1.amazonaws.com/iceberg")
        self.assertEqual(
            config["spark.sql.catalog.rustfs.warehouse"],
            "arn:aws:s3tables:us-east-1:123456789012:bucket/analytics",
        )
        self.assertEqual(config["spark.sql.catalog.rustfs.rest.signing-name"], "s3tables")
        self.assertNotIn("spark.sql.catalog.rustfs.s3.endpoint", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.path-style-access", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.access-key-id", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.secret-access-key", config)

    def test_spark_vendor_config_formats_cloudflare_catalog_profile(self) -> None:
        config = engine_compatibility.spark_vendor_catalog_config(
            profile="cloudflare-r2-data-catalog",
            endpoint="https://example.r2.cloudflarestorage.com",
            warehouse="ignored",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="auto",
            catalog_name="rustfs",
            account_id="000000000000",
            table_bucket="ignored",
            catalog_uri="https://catalog.example.com/iceberg",
            warehouse_name="analytics",
        )

        self.assertEqual(config["spark.sql.catalog.rustfs.uri"], "https://catalog.example.com/iceberg")
        self.assertEqual(config["spark.sql.catalog.rustfs.warehouse"], "analytics")
        self.assertEqual(config["spark.sql.catalog.rustfs.rest.signing-name"], "s3")
        self.assertNotIn("spark.sql.catalog.rustfs.s3.endpoint", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.path-style-access", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.access-key-id", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.secret-access-key", config)

    def test_spark_vendor_config_keeps_endpoint_for_s3_compatible_profiles(self) -> None:
        config = engine_compatibility.spark_vendor_catalog_config(
            profile="minio-aistor",
            endpoint="https://minio.example.com",
            warehouse="analytics",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="us-east-1",
            catalog_name="rustfs",
            account_id="000000000000",
            table_bucket="analytics",
            catalog_uri=None,
            warehouse_name=None,
        )

        self.assertEqual(config["spark.sql.catalog.rustfs.uri"], "https://minio.example.com/_iceberg")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.endpoint"], "https://minio.example.com")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.path-style-access"], "true")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.access-key-id"], "rustfsadmin")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.secret-access-key"], "rustfsadmin")
        self.assertEqual(config["spark.sql.catalog.rustfs.rest.signing-name"], "s3tables")

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

    def test_vendor_compatibility_audit_records_provider_gaps(self) -> None:
        audit = engine_compatibility.vendor_compatibility_audit()
        by_profile = {entry["profile"]: entry for entry in audit["providers"]}

        self.assertEqual(audit["claim_boundary"], "reference profiles and gap audit")
        self.assertEqual(audit["promotion_policy"], "live evidence required before compatibility claims expand")
        self.assertIn("aws-s3tables", by_profile)
        self.assertIn("minio-aistor", by_profile)
        self.assertIn("cloudflare-r2-data-catalog", by_profile)
        self.assertIn("oss-tables", by_profile)

        aws = by_profile["aws-s3tables"]
        self.assertEqual(aws["signing_name"], "s3tables")
        self.assertEqual(aws["warehouse_shape"], "arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}")
        self.assertIn("iceberg-rest-routes", aws["audit_categories"])
        self.assertIn("error-shapes", aws["audit_categories"])
        self.assertIn("permission-actions", aws["audit_categories"])
        self.assertIn("maintenance-semantics", aws["audit_categories"])
        self.assertIn("credential-vending", aws["audit_categories"])
        self.assertEqual(aws["rustfs_claim"], "profile-generator-only")

        minio = by_profile["minio-aistor"]
        self.assertEqual(minio["catalog_base_path"], "/_iceberg")
        self.assertEqual(minio["signing_name"], "s3tables")
        self.assertEqual(minio["rustfs_claim"], "alias-smoke-plus-profile-generator")
        self.assertIn("private-extension-parity", minio["not_claimed"])

        cloudflare = by_profile["cloudflare-r2-data-catalog"]
        self.assertEqual(cloudflare["auth_model"], "api-token")
        self.assertIn("managed-maintenance-parity", cloudflare["not_claimed"])
        self.assertEqual(cloudflare["rustfs_claim"], "profile-generator-only")

        oss = by_profile["oss-tables"]
        self.assertEqual(oss["signing_name"], "osstables")
        self.assertEqual(oss["warehouse_shape"], "acs:osstables:{region}:{account_id}:bucket/{table_bucket}")
        self.assertIn("provider-error-code-parity", oss["not_claimed"])

        self.assertIn("full AWS S3 Tables compatibility", audit["global_not_claimed"])
        self.assertIn("full Cloudflare R2 Data Catalog interoperability", audit["global_not_claimed"])

    def test_vendor_compatibility_audit_has_stable_validation_steps(self) -> None:
        audit = engine_compatibility.vendor_compatibility_audit()
        step_names = {step["name"] for step in audit["required_validation_steps"]}

        self.assertIn("route-and-field-shape", step_names)
        self.assertIn("error-response-shape", step_names)
        self.assertIn("permission-action-mapping", step_names)
        self.assertIn("maintenance-behavior", step_names)
        self.assertIn("credential-model", step_names)
        self.assertIn("client-live-run", step_names)

        route_step = next(step for step in audit["required_validation_steps"] if step["name"] == "route-and-field-shape")
        self.assertIn("request path", route_step["evidence"])
        self.assertIn("response fields", route_step["evidence"])
        self.assertEqual(route_step["status"], "manual-audit-required")

    def test_cli_prints_vendor_compatibility_audit(self) -> None:
        payload = engine_compatibility.cli_json(["--print-vendor-audit"])
        document = json.loads(payload)

        self.assertIn("vendor_compatibility_audit", document)
        profiles = {entry["profile"] for entry in document["vendor_compatibility_audit"]["providers"]}
        self.assertIn("aws-s3tables", profiles)
        self.assertIn("minio-aistor", profiles)
        self.assertIn("cloudflare-r2-data-catalog", profiles)
        self.assertIn("oss-tables", profiles)

    def test_cli_prints_vendor_spark_config(self) -> None:
        payload = engine_compatibility.cli_json(
            [
                "--print-spark-config",
                "--profile",
                "aws-s3tables",
                "--region",
                "us-east-1",
                "--account-id",
                "123456789012",
                "--table-bucket",
                "analytics",
            ]
        )
        document = json.loads(payload)
        config = document["spark_config"]

        self.assertEqual(config["spark.sql.catalog.rustfs.uri"], "https://s3tables.us-east-1.amazonaws.com/iceberg")
        self.assertEqual(
            config["spark.sql.catalog.rustfs.warehouse"],
            "arn:aws:s3tables:us-east-1:123456789012:bucket/analytics",
        )
        self.assertEqual(config["spark.sql.catalog.rustfs.rest.signing-name"], "s3tables")
        self.assertNotIn("spark.sql.catalog.rustfs.s3.endpoint", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.path-style-access", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.access-key-id", config)
        self.assertNotIn("spark.sql.catalog.rustfs.s3.secret-access-key", config)

    def test_cli_prints_oss_tables_spark_config(self) -> None:
        payload = engine_compatibility.cli_json(
            [
                "--print-spark-config",
                "--profile",
                "oss-tables",
                "--endpoint",
                "https://cn-hangzhou.oss-tables.aliyuncs.com",
                "--region",
                "cn-hangzhou",
                "--account-id",
                "123456789012",
                "--table-bucket",
                "analytics",
            ]
        )
        document = json.loads(payload)
        config = document["spark_config"]

        self.assertEqual(config["spark.sql.catalog.rustfs.uri"], "https://cn-hangzhou.oss-tables.aliyuncs.com/iceberg")
        self.assertEqual(
            config["spark.sql.catalog.rustfs.warehouse"],
            "acs:osstables:cn-hangzhou:123456789012:bucket/analytics",
        )
        self.assertEqual(config["spark.sql.catalog.rustfs.rest.signing-name"], "osstables")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.endpoint"], "https://oss-cn-hangzhou.aliyuncs.com")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.access-key-id"], "rustfsadmin")
        self.assertEqual(config["spark.sql.catalog.rustfs.s3.secret-access-key"], "rustfsadmin")
        self.assertNotIn("spark.sql.catalog.rustfs.s3.path-style-access", config)

    def test_cli_spark_config_keeps_explicit_rest_overrides(self) -> None:
        payload = engine_compatibility.cli_json(
            [
                "--print-spark-config",
                "--endpoint",
                "http://127.0.0.1:9000/",
                "--rest-path",
                "/_iceberg",
                "--rest-signing-name",
                "s3tables",
            ]
        )
        document = json.loads(payload)
        config = document["spark_config"]

        self.assertEqual(config["spark.sql.catalog.rustfs.uri"], "http://127.0.0.1:9000/_iceberg")
        self.assertEqual(config["spark.sql.catalog.rustfs.rest.signing-name"], "s3tables")

    def test_live_conformance_harness_pins_clients_and_records_commands(self) -> None:
        harness = engine_compatibility.live_conformance_harness(
            endpoint="http://127.0.0.1:9000",
            warehouse="rustfs-s3table-smoke",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="us-east-1",
            catalog_name="rustfs",
            namespace="smoke",
            table="events",
            rest_path="/iceberg",
            rest_signing_name="s3",
            pyiceberg_version="0.10.0",
            spark_version="3.5.4",
            iceberg_version="1.7.1",
            scala_version="2.12",
            cleanup=True,
        )

        self.assertEqual(harness["mode"], "manual-or-ci-optional")
        self.assertEqual(harness["ci_gate"], "RUSTFS_TABLE_CATALOG_LIVE_CONFORMANCE=1")
        self.assertEqual(harness["expected_results"]["row_count"], 2)
        self.assertIn("RustFS endpoint is reachable", harness["prerequisites"])

        by_client = {client["name"]: client for client in harness["clients"]}
        pyiceberg = by_client["PyIceberg"]
        self.assertEqual(pyiceberg["version"], "0.10.0")
        self.assertIn("pyiceberg[pyarrow]==0.10.0", pyiceberg["install"])
        self.assertIn("scripts/table-catalog/pyiceberg_smoke.py", pyiceberg["command"])
        self.assertIn("--replace", pyiceberg["command"])
        self.assertIn("--cleanup", pyiceberg["command"])

        spark = by_client["Spark Iceberg REST catalog"]
        self.assertEqual(spark["spark_version"], "3.5.4")
        self.assertEqual(spark["iceberg_version"], "1.7.1")
        self.assertIn("iceberg-spark-runtime-3.5_2.12:1.7.1", spark["packages"])
        self.assertIn("iceberg-aws-bundle:1.7.1", spark["packages"])
        self.assertIn("spark-sql", spark["command"])
        self.assertIn("spark.sql.catalog.rustfs.uri=http://127.0.0.1:9000/iceberg", spark["command"])
        self.assertIn("SELECT COUNT(*) AS row_count", spark["sql"])

        trino = by_client["Trino Iceberg REST catalog"]
        self.assertEqual(trino["status"], "manual-live-read-probe")
        self.assertIn("connector.name=iceberg", trino["catalog_properties"])
        self.assertIn("iceberg.catalog.type=rest", trino["catalog_properties"])
        self.assertIn("iceberg.rest-catalog.uri=http://127.0.0.1:9000/iceberg", trino["catalog_properties"])
        self.assertIn("trino", trino["command"])
        self.assertIn("SELECT COUNT(*) AS row_count", trino["sql"])
        self.assertEqual(trino["write_compatibility"], "not-claimed")

        duckdb = by_client["DuckDB Iceberg"]
        self.assertEqual(duckdb["status"], "manual-live-read-probe")
        self.assertIn("LOAD httpfs", duckdb["sql"])
        self.assertIn("LOAD iceberg", duckdb["sql"])
        self.assertIn("iceberg_scan", duckdb["sql"])
        self.assertEqual(duckdb["write_compatibility"], "not-claimed")

        snowflake = by_client["Snowflake Open Catalog / Iceberg integrations"]
        self.assertEqual(snowflake["status"], "manual-reference-probe")
        self.assertIn("CREATE OR REPLACE EXTERNAL VOLUME", snowflake["sql"])
        self.assertEqual(snowflake["live_rustfs_interoperability"], "not-claimed")

        databend = by_client["Databend"]
        self.assertEqual(databend["status"], "manual-live-s3-stage-probe")
        self.assertIn("CREATE STAGE", databend["sql"])
        self.assertIn("SELECT COUNT(*)", databend["sql"])
        self.assertEqual(databend["iceberg_rest_catalog"], "not-claimed")

    def test_live_conformance_harness_records_evidence_contract(self) -> None:
        harness = engine_compatibility.live_conformance_harness(
            endpoint="http://127.0.0.1:9000",
            warehouse="rustfs-s3table-smoke",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="us-east-1",
            catalog_name="rustfs",
            namespace="smoke",
            table="events",
            rest_path="/iceberg",
            rest_signing_name="s3",
            pyiceberg_version="0.10.0",
            spark_version="3.5.4",
            iceberg_version="1.7.1",
            scala_version="2.12",
        )

        evidence = harness["evidence"]
        self.assertEqual(evidence["result"], "operator-recorded")
        self.assertIn("rustfs_build", evidence["required_run_metadata"])
        self.assertIn("catalog_backing", evidence["required_run_metadata"])
        self.assertIn("client_version", evidence["required_run_metadata"])
        self.assertIn("metadata_location", evidence["required_run_metadata"])
        self.assertIn("expected_status", evidence["required_run_metadata"])
        self.assertIn("observed_status", evidence["required_run_metadata"])

        table_by_client = {row["client"]: row for row in evidence["result_table_template"]}
        self.assertEqual(table_by_client["PyIceberg"]["claim_after_pass"], "automated-smoke")
        self.assertEqual(table_by_client["Spark Iceberg REST catalog"]["claim_after_pass"], "manual-live-verified")
        self.assertEqual(table_by_client["Trino Iceberg REST catalog"]["write_claim_after_pass"], "not-claimed")
        self.assertEqual(table_by_client["DuckDB Iceberg"]["write_claim_after_pass"], "not-claimed")
        self.assertIn("manual-live", " ".join(evidence["promotion_rules"]))
        self.assertIn("not-claimed", " ".join(evidence["promotion_rules"]))

    def test_production_operations_guide_covers_release_boundaries(self) -> None:
        guide = engine_compatibility.production_operations_guide(
            endpoint="http://127.0.0.1:9000",
            warehouse="rustfs-s3table-smoke",
            namespace="smoke",
            table="events",
            rest_path="/iceberg",
        )

        self.assertEqual(guide["claim_boundary"], "Iceberg REST Catalog S3 Tables implementation")
        section_by_name = {section["name"]: section for section in guide["sections"]}

        self.assertIn("live-client-conformance", section_by_name)
        self.assertIn("catalog-backing-cutover", section_by_name)
        self.assertIn("maintenance-operations", section_by_name)
        self.assertIn("recovery-and-disaster-rehearsal", section_by_name)
        self.assertIn("permissions-and-credentials", section_by_name)
        self.assertIn("unsupported-claim-governance", section_by_name)

        cutover = section_by_name["catalog-backing-cutover"]
        self.assertIn("GET /iceberg/v1/rustfs-s3table-smoke/catalog/migration", cutover["commands"])
        self.assertIn("blockers is empty", " ".join(cutover["pass_criteria"]))

        maintenance = section_by_name["maintenance-operations"]
        self.assertIn("quarantine", " ".join(maintenance["fail_closed_signals"]))
        self.assertIn("audit-events", " ".join(maintenance["required_evidence"]))

        unsupported = section_by_name["unsupported-claim-governance"]
        self.assertIn("full AWS S3 Tables control-plane API parity", unsupported["not_claimed"])
        self.assertNotIn("fully compatible", json.dumps(guide).lower())

    def test_production_operations_guide_threads_target_args_into_commands(self) -> None:
        guide = engine_compatibility.production_operations_guide(
            endpoint="https://rustfs.example.com",
            warehouse="lakehouse-prod",
            namespace="sales",
            table="orders",
            rest_path="/_iceberg",
        )

        live = {section["name"]: section for section in guide["sections"]}["live-client-conformance"]
        commands = "\n".join(live["commands"])

        self.assertIn("--endpoint https://rustfs.example.com", commands)
        self.assertIn("--bucket lakehouse-prod", commands)
        self.assertIn("--warehouse lakehouse-prod", commands)
        self.assertIn("--namespace sales", commands)
        self.assertIn("--table orders", commands)
        self.assertIn("--rest-path /_iceberg", commands)
        self.assertNotIn("rustfs-s3table-smoke", commands)

    def test_production_operations_guide_encodes_catalog_path_segments(self) -> None:
        guide = engine_compatibility.production_operations_guide(
            endpoint="http://127.0.0.1:9000",
            warehouse="lake bucket",
            namespace="sales/prod",
            table="orders table",
            rest_path="/_iceberg",
        )

        section_by_name = {section["name"]: section for section in guide["sections"]}
        cutover = section_by_name["catalog-backing-cutover"]
        maintenance = section_by_name["maintenance-operations"]
        recovery = section_by_name["recovery-and-disaster-rehearsal"]
        credentials = section_by_name["permissions-and-credentials"]

        self.assertIn("GET /_iceberg/v1/lake%20bucket/catalog/migration", cutover["commands"])
        encoded_table_path = "/_iceberg/v1/lake%20bucket/namespaces/sales%2Fprod/tables/orders%20table"
        for commands in [maintenance["commands"], recovery["commands"], credentials["commands"]]:
            rendered = "\n".join(commands)
            self.assertIn(encoded_table_path, rendered)
            self.assertNotIn("orders table", rendered)
            self.assertNotIn("/namespaces/sales/prod/", rendered)

    def test_cli_prints_live_conformance_harness(self) -> None:
        payload = engine_compatibility.cli_json(
            [
                "--print-live-conformance",
                "--pyiceberg-version",
                "0.10.0",
                "--spark-version",
                "3.5.4",
                "--iceberg-version",
                "1.7.1",
                "--cleanup",
            ]
        )
        document = json.loads(payload)

        self.assertEqual(document["live_conformance"]["mode"], "manual-or-ci-optional")
        self.assertEqual(document["live_conformance"]["expected_results"]["row_count"], 2)
        self.assertIn("evidence", document["live_conformance"])

    def test_cli_prints_production_operations_guide(self) -> None:
        payload = engine_compatibility.cli_json(
            [
                "--print-operations-guide",
                "--warehouse",
                "rustfs-s3table-smoke",
                "--namespace",
                "smoke",
                "--table",
                "events",
            ]
        )
        document = json.loads(payload)

        self.assertEqual(
            document["production_operations_guide"]["claim_boundary"],
            "Iceberg REST Catalog S3 Tables implementation",
        )
        sections = {section["name"] for section in document["production_operations_guide"]["sections"]}
        self.assertIn("live-client-conformance", sections)
        self.assertIn("unsupported-claim-governance", sections)

    def test_live_conformance_harness_sanitizes_sql_file_path(self) -> None:
        harness = engine_compatibility.live_conformance_harness(
            endpoint="http://127.0.0.1:9000",
            warehouse="rustfs-s3table-smoke",
            access_key="rustfsadmin",
            secret_key="rustfsadmin",
            region="us-east-1",
            catalog_name="rustfs",
            namespace="sales/prod",
            table="orders 2026",
            rest_path="/iceberg",
            rest_signing_name="s3",
            pyiceberg_version="0.10.0",
            spark_version="3.5.4",
            iceberg_version="1.7.1",
            scala_version="2.12",
        )

        spark = {client["name"]: client for client in harness["clients"]}["Spark Iceberg REST catalog"]
        self.assertEqual(spark["sql_file"], "/tmp/rustfs-s3tables-sales-prod-orders-2026-spark.sql")

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
