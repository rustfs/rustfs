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
            "/v1/lake/namespaces/sales/tables/orders",
        )
        self.assertEqual(by_name["stale-token-commit-conflict"]["method"], "POST")
        self.assertEqual(by_name["stale-token-commit-conflict"]["expected_status"], "409")
        self.assertEqual(
            by_name["diagnostics-after-finalization-gap"]["path"],
            "/v1/lake/namespaces/sales/tables/orders/catalog/diagnostics",
        )
        self.assertEqual(by_name["diagnostics-after-finalization-gap"]["method"], "GET")
        self.assertEqual(by_name["recovery-repairs-idempotency-index"]["method"], "POST")
        self.assertIn("does_not_exist.metadata.json", json.dumps(by_name["missing-metadata-object-rejected"]))

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
                "--print-failure-probes",
            ]
        )
        probe_document = json.loads(probe_payload)

        self.assertIn("failure_probe_plan", probe_document)
        self.assertTrue(probe_document["failure_probe_plan"])


if __name__ == "__main__":
    unittest.main()
