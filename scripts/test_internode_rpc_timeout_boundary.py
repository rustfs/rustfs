#!/usr/bin/env python3
"""Regression cases for the internode RPC timeout-boundary evidence gate."""

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import check_internode_rpc_timeout_boundary as gate


class TimeoutBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.reports = {
            "under": self.report(30, 10, []),
            "over": self.report(35, 10, [("Ping", "deadline"), ("ReadAll", "deadline")]),
            "recovery": self.report(15, 10, []),
        }
        for name, report in self.reports.items():
            (self.root / f"{name}.json").write_text(json.dumps(report))
        self.faults = {
            "under": {"target": "node4", "requested_stall_ms": 8_000, "actual_stall_ms": 9_100},
            "over": {"target": "node4", "requested_stall_ms": 12_000, "actual_stall_ms": 12_600},
        }
        self.manifest = {
            "schema": 1,
            "scope": "internode-rpc-timeout-boundary",
            "rpc_timeout_ms": 10_000,
            "under_budget": {"fault_receipt": "under-fault.json", "report": "under.json"},
            "over_budget": {"fault_receipt": "over-fault.json", "report": "over.json"},
            "recovery": {"report": "recovery.json"},
        }

    @staticmethod
    def report(duration, rate, failures):
        samples = []
        for sequence in range(duration * rate):
            samples.append({"operation": "Ping", "outcome": "ok", "peer": sequence % 4})
        for sequence in range(8):
            samples.append({"operation": "ReadAll", "outcome": "ok", "peer": sequence % 4})
        for operation, outcome in failures:
            row = next(sample for sample in samples if sample["operation"] == operation and sample["outcome"] == "ok")
            row["outcome"] = outcome
        return {
            "complete": True,
            "errors": len(failures),
            "bulk_coverage_verified": True,
            "offered_duration_secs": duration,
            "control_rps": rate,
            "control_expected": duration * rate,
            "fixture_sha256": "0" * 64,
            "targets": 4,
            "peers": 4,
            "bulk_workers": 2,
            "scope": "Authenticated production RPC clients",
            "samples": samples,
        }

    def write_reports(self):
        for name, report in self.reports.items():
            (self.root / f"{name}.json").write_text(json.dumps(report))
        for name, fault in self.faults.items():
            (self.root / f"{name}-fault.json").write_text(json.dumps(fault))

    def check(self):
        self.write_reports()
        return gate.check(self.manifest, self.root)[0]

    def test_valid_boundary_and_recovery_pass(self):
        result = self.check()
        self.assertTrue(result["observed_pass"])
        self.assertEqual(result["phases"], {"under_budget": True, "over_budget": True, "recovery": True})

    def test_schedule_drop_cannot_substitute_for_deadline(self):
        for report in ("under", "over", "recovery"):
            with self.subTest(report=report):
                original = copy.deepcopy(self.reports[report])
                row = next(sample for sample in self.reports[report]["samples"] if sample["operation"] == "Ping")
                was_ok = row["outcome"] == "ok"
                row["outcome"] = "schedule_drop"
                self.reports[report]["errors"] += was_ok
                self.assertFalse(self.check()["observed_pass"])
                self.reports[report] = original

    def test_under_budget_errors_over_budget_success_and_recovery_errors_fail(self):
        cases = (("under", "deadline", 1), ("over", "ok", -2), ("recovery", "rpc_error", 1))
        for report, outcome, error_delta in cases:
            with self.subTest(report=report):
                original = copy.deepcopy(self.reports[report])
                if report == "over":
                    for sample in self.reports[report]["samples"]:
                        sample["outcome"] = "ok"
                else:
                    next(sample for sample in self.reports[report]["samples"] if sample["operation"] == "Ping")["outcome"] = outcome
                self.reports[report]["errors"] += error_delta
                self.assertFalse(self.check()["observed_pass"])
                self.reports[report] = original

    def test_invalid_stall_receipts_and_control_contract_fail_closed(self):
        edits = (
            lambda: self.faults["under"].update(actual_stall_ms=10_000),
            lambda: self.faults["over"].update(requested_stall_ms=9_000),
            lambda: self.faults["under"].update(requested_stall_ms=9_200),
            lambda: self.reports["under"].update(control_expected=1),
        )
        for edit in edits:
            with self.subTest(edit=edit):
                manifest = copy.deepcopy(self.manifest)
                reports = copy.deepcopy(self.reports)
                faults = copy.deepcopy(self.faults)
                edit()
                with self.assertRaises(ValueError):
                    self.check()
                self.manifest, self.reports, self.faults = manifest, reports, faults

    def test_paths_cannot_escape_or_reuse_evidence(self):
        self.manifest["under_budget"]["report"] = "../outside.json"
        with self.assertRaisesRegex(ValueError, "escapes"):
            self.check()
        self.manifest["under_budget"]["report"] = "under.json"
        self.manifest["recovery"]["report"] = "under.json"
        with self.assertRaisesRegex(ValueError, "reused"):
            self.check()

    def test_probe_workload_identity_cannot_change_between_phases(self):
        for field, value in (("fixture_sha256", "1" * 64), ("targets", 3), ("peers", 3),
                             ("bulk_workers", 1), ("control_rps", 9), ("scope", "different")):
            with self.subTest(field=field):
                self.reports["recovery"][field] = value
                if field == "control_rps":
                    self.reports["recovery"]["control_expected"] = self.reports["recovery"]["offered_duration_secs"] * value
                    self.reports["recovery"]["samples"] = self.report(15, value, [])["samples"]
                with self.assertRaisesRegex(ValueError, "identity changed"):
                    self.check()
                self.reports["recovery"] = self.report(15, 10, [])

    def run_cli(self, output):
        self.write_reports()
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps(self.manifest))
        return subprocess.run([sys.executable, str(Path(gate.__file__)), "--manifest", str(manifest),
                               "--out", str(output)], capture_output=True, text=True)

    def test_entry_point_distinguishes_pass_fail_and_invalid(self):
        output = self.root / "result.json"
        self.assertEqual(self.run_cli(output).returncode, 0)
        self.reports["recovery"]["samples"][0]["outcome"] = "deadline"
        self.reports["recovery"]["errors"] = 1
        self.assertEqual(self.run_cli(output).returncode, 1)
        self.manifest["scope"] = "command"
        self.assertEqual(self.run_cli(output).returncode, 2)

    def test_entry_point_preserves_raw_and_hard_linked_evidence(self):
        raw = self.root / "under.json"
        self.write_reports()
        before = raw.read_bytes()
        self.assertEqual(self.run_cli(raw).returncode, 2)
        self.assertEqual(raw.read_bytes(), before)
        alias = self.root / "alias.json"
        os.link(raw, alias)
        self.assertEqual(self.run_cli(alias).returncode, 2)
        self.assertEqual(raw.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
