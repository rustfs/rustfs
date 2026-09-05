#!/usr/bin/env python3
"""Synthetic adapter and failure-propagation tests; never start a RustFS server."""

import contextlib
import copy
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import scanner_abba as harness


def fake_adapter():
    action, request_path, output_path = sys.argv[1:]
    request = harness.read_json(Path(request_path))
    fault = os.environ.get("SCANNER_ABBA_TEST_FAULT", "")
    if action == "prepare":
        result = {"ready": True}
    elif action == "stop":
        result = {"stopped": True}
    elif action == "oracle":
        if fault == "oracle-exit":
            return 42
        if fault == "missing-oracle":
            return 0
        result = {"complete": True, "errors": 0, "actual": request["expected_oracle"]}
        if fault == "oracle-mismatch":
            result["actual"]["bytes"] += 1
    else:
        if fault == "measure-exit":
            return 42
        result = {key: request[key] for key in ("evidence", "fixed", "build", "data_dir", "background")}
        result.update({"sample_count": 10, "elapsed_seconds": request["duration_seconds"],
                       "metrics": dict.fromkeys(harness.METRICS, 10)})
        baseline = request["comparison"] == "build" and request["leg"].startswith("A")
        result["metrics"].update(p99_ms=10, throughput_ops=100, errors=0, requests=100,
                                 walk_objects=100 if baseline else 20, cold_walk_objects=100 if baseline else 0,
                                 healed_objects=request["expected_healed_objects"])
        result["convergence"] = {"writes_stopped": True, "last_mutation_observed": True,
                                 "first_complete_publication": True, "last_mutation_time": 1,
                                 "last_mutation_observed_time": 2,
                                 "writes_stopped_time": 2, "window_start": 2, "window_end": 3,
                                 "budget_available_seconds": 1, "walk_objects": 110, "full_walk_objects": 100}
        if fault == "zero-samples":
            result["sample_count"] = 0
        elif fault == "request-errors":
            result["metrics"]["errors"] = 1
        elif fault == "load-drift":
            result["fixed"]["offered_load_ops"] += 1
        elif fault == "noise" and request["leg"] == "A2":
            result["metrics"]["p99_ms"] = 20
        elif fault == "zero-requests":
            result["metrics"]["requests"] = 0
        elif fault == "no-publication":
            result["convergence"]["first_complete_publication"] = False
        elif fault == "p2-regression":
            result["convergence"]["walk_objects"] = 121
        elif fault == "latency-regression" and request["leg"].startswith("B"):
            result["metrics"]["p99_ms"] = 12
        elif fault == "exact-thresholds" and request["leg"].startswith("B"):
            result["metrics"].update(p99_ms=10.5, throughput_ops=97)
        elif fault == "just-over-threshold" and request["comparison"] == "build" and request["leg"].startswith("B"):
            result["metrics"]["p99_ms"] = 10.500001
        elif fault == "p1-regression" and not baseline:
            result["metrics"]["walk_objects"] = 30
        elif fault in ("p1-exact-fraction", "p1-over-fraction"):
            result["metrics"].update(walk_objects=9 if baseline else 5 + (fault == "p1-over-fraction"),
                                     cold_walk_objects=5 if baseline else 0)
        elif fault == "unstable-p1-control" and request["comparison"] == "build":
            if request["leg"] == "A1":
                result["metrics"].update(walk_objects=1000, cold_walk_objects=1000)
            elif request["leg"] == "A2":
                result["metrics"].update(walk_objects=10, cold_walk_objects=10)
            elif request["leg"].startswith("B"):
                result["metrics"].update(walk_objects=100, cold_walk_objects=0)
        elif fault == "missing-metric":
            del result["metrics"]["save_bytes"]
        elif fault == "incomplete-repair":
            result["metrics"]["healed_objects"] = 0
    harness.write_json(Path(output_path), result)
    return 0


class ScannerAbbaTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.binary = Path(sys.executable).resolve()
        self.adapter = Path(__file__).resolve()
        self.manifest = {
            "schema": 1, "evidence": "synthetic", "rounds": 3, "duration_seconds": 1, "min_free_bytes": 1,
            "fixed": {"config_sha256": "1" * 64, "dataset_sha256": "2" * 64,
                      "release_flags": "--release", "durability": "drive-sync=on",
                      "disk_type": "synthetic", "cache_state": "cold", "load_command": "fake",
                      "topology": "EC8+4", "offered_load_ops": 100, "resource_isolation": "synthetic"},
            "oracles": {s: {"objects": 10, "versions": 20, "bytes": 30, "sha256": "3" * 64} for s in harness.SCENARIOS},
            "expected_healed_objects": {s: 10 for s in harness.SCENARIOS},
        }
        build = {"binary": str(self.binary), "sha256": harness.digest(self.binary), "revision": "a" * 40}
        self.manifest.update(baseline=build.copy(), candidate=build.copy())

    def run_harness(self, fault=""):
        with patch.dict(os.environ, {"SCANNER_ABBA_TEST_FAULT": fault}), contextlib.redirect_stdout(io.StringIO()):
            return harness.run(copy.deepcopy(self.manifest), self.adapter, self.root / "out", self.root / "data")

    def test_complete_synthetic_matrix_is_not_performance_evidence(self):
        self.assertEqual(self.run_harness(), 0)
        report = harness.read_json(self.root / "out/report.json")
        self.assertEqual((report["status"], report["performance"], report["cells"]), ("synthetic_validated", "pending", 120))
        requests = [harness.read_json(path) for path in (self.root / "out").glob("*/request.json")]
        self.assertEqual(len({r["data_dir"] for r in requests}), 120)
        for scenario in harness.SCENARIOS:
            for comparison in ("build", "background"):
                for round_id in (1, 2, 3):
                    legs = [r for r in requests if (r["scenario"], r["comparison"], r["round"]) == (scenario, comparison, round_id)]
                    self.assertEqual({r["leg"] for r in legs}, set(harness.LEGS))
        self.assertTrue(all(c["p2_max_work_multiple"] == 1.2 for c in report["comparisons"]))

    def test_fail_closed_adapter_and_data_errors(self):
        for fault in ("measure-exit", "oracle-exit", "missing-oracle", "oracle-mismatch", "zero-samples",
                      "zero-requests", "request-errors", "load-drift", "missing-metric", "incomplete-repair"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with self.assertRaises((ValueError, OSError, subprocess.SubprocessError)):
                    self.run_harness(fault)
                report = harness.read_json(self.root / "out/report.json")
                self.assertEqual(report["status"], "failed")
                self.assertTrue(list((self.root / "out").glob("*/stop.json")))

    def test_noise_is_inconclusive_and_nonzero(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("noise"), 3)
        self.assertEqual(harness.read_json(self.root / "out/report.json")["status"], "inconclusive")

    def test_missing_first_publication_is_inconclusive(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("no-publication"), 3)

    def test_performance_regressions_fail(self):
        for fault in ("p1-regression", "p2-regression", "latency-regression"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with patch.object(harness, "SCENARIOS", ("cold-hot",)):
                    self.assertEqual(self.run_harness(fault), 1)

    def test_exact_threshold_boundaries_pass(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("exact-thresholds"), 0)

    def test_just_over_threshold_fails(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("just-over-threshold"), 1)

    def test_p1_fractional_boundary(self):
        for fault, expected in (("p1-exact-fraction", 0), ("p1-over-fraction", 1)):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with patch.object(harness, "SCENARIOS", ("cold-hot",)):
                    self.assertEqual(self.run_harness(fault), expected)

    def test_live_collector_rejects_missing_or_failed_node_metrics(self):
        telemetry = self.root / "telemetry"
        for name in ("status", "heal", "metrics"):
            (telemetry / name).mkdir(parents=True)
        (telemetry / "scanner-summary.csv").write_text("timestamp\n")
        valid = {"errors": [], "final": True, "by_host": {"node-b:9000": {"scanner": {"objects": 10}}}}
        for index in range(16):
            harness.write_json(telemetry / f"status/scanner-status.{index}.json", {"metrics": {"objects": 10}})
            for node in ("node-a", "node-b"):
                harness.write_json(telemetry / f"heal/background-heal-status.{node}.{index}.json",
                                   {"healOperations": {"queueLength": 0}})
                harness.write_json(telemetry / f"metrics/admin-metrics.{node}.{index}.ndjson",
                                   {**valid, "by_host": {f"{node}:9000": {"scanner": {"objects": 10}}}})
        sample = telemetry / "metrics/admin-metrics.node-b.15.ndjson"
        prepared = {"collector": {"alias": "test", "endpoint": "http://node-a:9000",
                                  "metrics_endpoints": "http://node-a:9000,http://node-b:9000"}}
        cases = (
            ("valid", valid, None),
            ("missing", None, "missing distributed metrics samples"),
            ("empty", "", "Expecting value"),
            ("http-error", {"Code": "AccessDenied"}, "distributed metrics errors"),
            ("partial-error", {**valid, "errors": ["node unavailable"]}, "distributed metrics errors"),
            ("unfinished", {**valid, "final": False}, "incomplete distributed metrics"),
            ("missing-host", {**valid, "by_host": {}}, "missing by-host metrics"),
            ("missing-scanner", {**valid, "by_host": {"node-a:9000": {}}}, "missing per-host scanner metrics"),
            ("collector-exit", valid, "scanner collector failed"),
        )
        for name, payload, error in cases:
            with self.subTest(fault=name):
                if payload is None:
                    sample.unlink()
                elif isinstance(payload, str):
                    sample.write_text(payload)
                else:
                    harness.write_json(sample, payload)
                process = Mock(pid=123, wait=Mock(return_value=1 if name == "collector-exit" else 0))
                with patch.object(harness.subprocess, "Popen", return_value=process), \
                        patch.object(harness, "invoke", return_value={"sample_count": 10}), \
                        patch.object(harness.time, "monotonic", side_effect=(0, 900)), \
                        patch.object(harness.os, "killpg"):
                    if error:
                        with self.assertRaisesRegex(ValueError, error):
                            harness.collect_live(prepared, {"duration_seconds": 900}, self.root / "request.json", self.adapter)
                    else:
                        self.assertEqual(harness.collect_live(prepared, {"duration_seconds": 900},
                                                             self.root / "request.json", self.adapter), {"sample_count": 10})

    def test_unstable_p1_work_control_is_inconclusive(self):
        with patch.object(harness, "SCENARIOS", ("cold-hot",)):
            self.assertEqual(self.run_harness("unstable-p1-control"), 3)
        comparison = harness.read_json(self.root / "out/report.json")["comparisons"][0]
        self.assertEqual(comparison["status"], "inconclusive")
        self.assertGreater(comparison["p1"]["repeatability_drift"], 0.05)

    def test_manifest_rejects_missing_build_or_oracle(self):
        for section, key in (("baseline", "binary"), ("oracles", "cold-hot")):
            manifest = copy.deepcopy(self.manifest)
            del manifest[section][key]
            with self.subTest(section=section), self.assertRaises((ValueError, KeyError)):
                harness.validate_manifest(manifest)

    def test_short_measured_window_and_fewer_rounds_rejected(self):
        self.manifest["evidence"] = "measured"
        with self.assertRaisesRegex(ValueError, "duration_seconds"):
            harness.validate_manifest(self.manifest)
        self.manifest["duration_seconds"] = 900
        self.manifest["rounds"] = 2
        with self.assertRaisesRegex(ValueError, "rounds"):
            harness.validate_manifest(self.manifest)

    def test_existing_data_preserved(self):
        (self.root / "data").mkdir()
        marker = self.root / "data/keep"
        marker.write_text("existing")
        with self.assertRaisesRegex(ValueError, "preserved"):
            self.run_harness()
        self.assertEqual(marker.read_text(), "existing")

    def test_invalid_or_live_write_window_does_not_claim_p2(self):
        self.assertIsNone(harness.convergence({"convergence": {"writes_stopped": False}}))
        with self.assertRaises(ValueError):
            harness.convergence({"convergence": {"writes_stopped": True, "last_mutation_observed": True,
                                                 "first_complete_publication": True}})

    def test_nan_and_oversized_samples_rejected(self):
        with self.assertRaises(ValueError):
            harness.number(float("nan"), "latency")
        path = self.root / "oversized.json"
        path.write_bytes(b" " * (harness.MAX_JSON_BYTES + 1))
        with self.assertRaisesRegex(ValueError, "oversized"):
            harness.read_json(path)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] in ("prepare", "measure", "oracle", "stop"):
        sys.exit(fake_adapter())
    unittest.main()
