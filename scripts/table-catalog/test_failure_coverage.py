#!/usr/bin/env python3
"""Unit tests for RustFS table catalog production failure coverage helpers."""

from __future__ import annotations

import json
import unittest

import failure_coverage


class FailureCoverageTest(unittest.TestCase):
    def test_matrix_tracks_production_failure_boundaries_without_overclaiming(self) -> None:
        matrix = failure_coverage.production_failure_matrix()
        by_case = {entry["case"]: entry for entry in matrix}

        self.assertIn("commit-cas-conflict", by_case)
        self.assertIn("post-cas-finalization-gap", by_case)
        self.assertIn("missing-referenced-object", by_case)
        self.assertIn("maintenance-stale-plan", by_case)
        self.assertIn("external-bridge-conflict", by_case)
        self.assertIn("backing-migration-blocked", by_case)
        self.assertIn("permission-negative", by_case)

        self.assertEqual(by_case["commit-cas-conflict"]["expected_behavior"], "conflict-without-pointer-advance")
        self.assertEqual(by_case["post-cas-finalization-gap"]["expected_behavior"], "recoverable-diagnostics-and-idempotency-repair")
        for entry in matrix:
            self.assertIn("coverage_status", entry)
            self.assertIn("evidence", entry)
            self.assertNotEqual(entry["coverage_status"], "claimed-automated-live")

    def test_failure_probe_plan_generates_rest_negative_steps(self) -> None:
        plan = failure_coverage.failure_probe_plan(
            warehouse="lake",
            namespace="sales",
            table="orders",
        )
        by_name = {step["name"]: step for step in plan}

        self.assertEqual(
            by_name["stale-token-commit-conflict"]["path"],
            "/iceberg/v1/lake/namespaces/sales/tables/orders",
        )
        self.assertEqual(by_name["stale-token-commit-conflict"]["method"], "POST")
        self.assertEqual(by_name["stale-token-commit-conflict"]["expected_status"], "409")
        self.assertIn("expected-version-token", by_name["stale-token-commit-conflict"]["body"])
        self.assertIn("expected-metadata-location", by_name["stale-token-commit-conflict"]["body"])
        self.assertIn("new-metadata-location", by_name["stale-token-commit-conflict"]["body"])
        self.assertNotIn("base", by_name["stale-token-commit-conflict"]["body"])
        self.assertEqual(
            by_name["diagnostics-after-finalization-gap"]["path"],
            "/iceberg/v1/lake/namespaces/sales/tables/orders/catalog/diagnostics",
        )
        self.assertEqual(by_name["diagnostics-after-finalization-gap"]["method"], "GET")
        self.assertEqual(by_name["recovery-repairs-idempotency-index"]["method"], "POST")
        self.assertIn("does_not_exist.metadata.json", json.dumps(by_name["missing-metadata-object-rejected"]))
        self.assertNotIn("base", by_name["missing-metadata-object-rejected"]["body"])

    def test_cli_prints_failure_matrix_and_probe_plan(self) -> None:
        matrix_payload = failure_coverage.cli_json(["--print-failure-matrix"])
        matrix_document = json.loads(matrix_payload)

        self.assertIn("production_failure_coverage", matrix_document)
        self.assertTrue(matrix_document["production_failure_coverage"])

        probe_payload = failure_coverage.cli_json(
            [
                "--warehouse",
                "lake",
                "--namespace",
                "sales",
                "--table",
                "orders",
                "--rest-path",
                "/_iceberg",
                "--print-failure-probes",
            ]
        )
        probe_document = json.loads(probe_payload)

        self.assertIn("failure_probe_plan", probe_document)
        self.assertTrue(probe_document["failure_probe_plan"])
        self.assertEqual(
            probe_document["failure_probe_plan"][0]["path"],
            "/_iceberg/v1/lake/namespaces/sales/tables/orders",
        )

    def test_disaster_recovery_rehearsal_plan_covers_operator_recovery_path(self) -> None:
        rehearsal = failure_coverage.disaster_recovery_rehearsal_plan(
            warehouse="lake",
            namespace="sales",
            table="orders",
            rest_path="/iceberg",
            table_warehouse_location="s3://lake/tables/orders",
        )

        self.assertEqual(rehearsal["mode"], "manual-or-ci-optional")
        self.assertEqual(rehearsal["ci_gate"], "RUSTFS_TABLE_CATALOG_DR_REHEARSAL=1")
        self.assertIn("record the RustFS build and catalog backing mode before starting", rehearsal["preconditions"])
        self.assertIn("current metadata pointer remains recoverable or deliberately rolled back", rehearsal["expected_invariants"])

        phases = {phase["name"]: phase for phase in rehearsal["phases"]}
        self.assertIn("capture-baseline", phases)
        self.assertIn("diagnose-and-repair", phases)
        self.assertIn("rollback-or-import", phases)
        self.assertIn("migration-preflight", phases)
        self.assertIn("post-recovery-validation", phases)

        baseline_steps = {step["name"]: step for step in phases["capture-baseline"]["steps"]}
        self.assertEqual(
            baseline_steps["export-catalog-state"]["path"],
            "/iceberg/v1/lake/namespaces/sales/tables/orders/catalog/export",
        )
        self.assertEqual(baseline_steps["load-table-before-recovery"]["method"], "GET")

        repair_steps = {step["name"]: step for step in phases["diagnose-and-repair"]["steps"]}
        self.assertEqual(repair_steps["read-recovery-diagnostics"]["path"], "/iceberg/v1/lake/namespaces/sales/tables/orders/catalog/diagnostics")
        self.assertEqual(repair_steps["safe-recovery-repair"]["method"], "POST")
        self.assertEqual(repair_steps["safe-recovery-repair"]["body"], {"mode": "safe-repair"})

        rollback_steps = {step["name"]: step for step in phases["rollback-or-import"]["steps"]}
        self.assertEqual(rollback_steps["rollback-to-known-metadata"]["path"], "/iceberg/v1/lake/namespaces/sales/tables/orders/catalog/rollback")
        self.assertIn("metadata-location", rollback_steps["rollback-to-known-metadata"]["body"])
        self.assertIn("version-token", rollback_steps["rollback-to-known-metadata"]["body"])
        self.assertNotIn("expected-version-token", rollback_steps["rollback-to-known-metadata"]["body"])
        self.assertEqual(rollback_steps["import-known-metadata"]["path"], "/iceberg/v1/lake/namespaces/sales/tables/orders/catalog/import")
        self.assertIn("metadata-location", rollback_steps["import-known-metadata"]["body"])
        self.assertIn("properties", rollback_steps["import-known-metadata"]["body"])
        self.assertNotIn("external-version-token", rollback_steps["import-known-metadata"]["body"])

        migration_steps = {step["name"]: step for step in phases["migration-preflight"]["steps"]}
        self.assertEqual(migration_steps["durable-backing-migration-dry-run"]["path"], "/iceberg/v1/lake/catalog/migration")
        self.assertEqual(migration_steps["durable-backing-migration-dry-run"]["expected_behavior"], "migration blockers must be empty before cutover")

        validation_steps = {step["name"]: step for step in phases["post-recovery-validation"]["steps"]}
        self.assertEqual(validation_steps["load-table-after-recovery"]["path"], "/iceberg/v1/lake/namespaces/sales/tables/orders")
        self.assertEqual(validation_steps["table-data-plane-policy-probe"]["path"], "s3://lake/tables/orders")
        self.assertEqual(validation_steps["table-data-plane-policy-probe"]["method"], "S3-PROBE")

    def test_cli_prints_disaster_recovery_rehearsal_plan(self) -> None:
        payload = failure_coverage.cli_json(
            [
                "--warehouse",
                "lake",
                "--namespace",
                "sales",
                "--table",
                "orders",
                "--rest-path",
                "/_iceberg",
                "--table-warehouse-location",
                "s3://lake/tables/orders",
                "--print-disaster-recovery-rehearsal",
            ]
        )
        document = json.loads(payload)

        self.assertEqual(document["disaster_recovery_rehearsal"]["phases"][0]["name"], "capture-baseline")
        first_step = document["disaster_recovery_rehearsal"]["phases"][0]["steps"][0]
        self.assertEqual(first_step["path"], "/_iceberg/v1/lake/namespaces/sales/tables/orders/catalog/export")


if __name__ == "__main__":
    unittest.main()
