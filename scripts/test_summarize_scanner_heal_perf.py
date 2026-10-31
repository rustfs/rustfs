#!/usr/bin/env python3

from __future__ import annotations

import copy
import contextlib
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import summarize_scanner_heal_perf as summary


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        hasher = hashlib.sha256()
        while chunk := stream.read(1024 * 1024):
            hasher.update(chunk)
        return hasher.hexdigest()


class ScannerHealPerfSummaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.abba = self.root / "abba"
        self.abba.mkdir()
        self.manifest = {
            "schema": 1,
            "evidence": "measured",
            "rounds": 3,
            "duration_seconds": summary.MIN_MEASURED_RELEASE_DURATION_SECONDS,
            "fixed": {
                "config_sha256": "1" * 64,
                "dataset_sha256": "2" * 64,
                "release_flags": "--profile production",
                "durability": "drive-sync=on",
                "topology": "EC8+4",
                "offered_load_ops": 100,
            },
            "baseline": {"revision": "a" * 40, "sha256": "3" * 64},
            "candidate": {"revision": "b" * 40, "sha256": "4" * 64},
            "adapter_sha256": "5" * 64,
            "collector_sha256": "6" * 64,
            "release_evidence": {
                "topology": {
                    "nodes": 3,
                    "drives_per_node": 4,
                    "pools": 2,
                    "sets_total": 2,
                    "sampled_pools": 2,
                    "sampled_sets": 2,
                    "erasure_set_size": 12,
                    "erasure_data_blocks": 8,
                    "erasure_parity_blocks": 4,
                },
                "distributed": {
                    "metrics_endpoints": ["https://node-1:9000", "https://node-2:9000", "https://node-3:9000"],
                    "failure_domain": "three-node-localhost-lab",
                    "same_window_sampling": True,
                },
                "scheduler": {
                    "bounds": ["admission-retry-idempotency", "deadline-budget", "lock-hold-bound", "minimum-progress"],
                    "max_deferred_items": 128,
                    "max_deferred_bytes": 1048576,
                    "max_retry_age_seconds": 300,
                    "duplicate_task_bound_observed": True,
                },
                "crash_restart": {
                    "fault_modes": ["process-restart", "process-crash-restart"],
                    "unclean_shutdown_marker": True,
                },
                "mixed_version": {
                    "participating_revisions": ["a" * 40, "b" * 40],
                    "reader": True,
                    "writer": True,
                    "rollback_payload": True,
                },
                "profile": {
                    "required_artifacts": ["allocation-profile", "flamegraph", "rss-samples", "save-frequency"],
                    "collector_config_sha256": "7" * 64,
                    "profiler_config_sha256": "8" * 64,
                    "measurements": {
                        "resolved_samples": 120,
                        "allocation_bytes": 4096,
                        "rss_peak_bytes": 10485760,
                        "save_operations": 64,
                        "saved_bytes": 8192,
                    },
                },
                "heal_capacity": {
                    "objects": 96,
                    "versions": 96,
                    "bytes": 12582912,
                    "completed_objects": 96,
                },
                "recovery_window": {
                    "pressure_recovery_window_seconds": 45,
                    "heal_lock_wait_p99_ms": 8,
                    "recovery_p95_ms": 1500,
                    "recovery_p99_ms": 2200,
                },
            },
        }
        self.comparison = {
            "scenario": "cold-hot",
            "comparison": "build",
            "round": 1,
            "status": "pass",
            "foreground_p95_ms": 8.0,
            "foreground_p99_ms": 10.0,
            "foreground_percentiles_ms_by_leg": {
                leg: {"p95": 8.0, "p99": 10.0} for leg in summary.LEGS
            },
            "throughput_ops": 100.0,
            "error_rate": 0.0,
            "p99_regression": 0.02,
            "throughput_change": -0.01,
            "p1": {
                "required_reduction": 0.8,
                "observed_reduction": 0.82,
                "repeatability_drift": 0.01,
                "baseline_walk_objects": 100,
                "baseline_cold_walk_objects": 100,
                "candidate_walk_objects": 20,
                "candidate_cold_walk_objects": 0,
            },
            "p2_post_stop_work_multiples": [None, 1.1, 1.0, None],
            "w10_w11": {
                "foreground_pressure_samples": [10, 10, 10, 10],
                "foreground_pressure_high_samples": [0, 3, 3, 0],
                "foreground_pressure_high_sample_ratios": [0.0, 0.25, 0.25, 0.0],
                "heal_lock_wait_p99_ms": [12.0, 8.0, 9.0, 13.0],
                "attempt_cost_per_healed_object": [None, 1.2, 1.3, None],
                "candidate_attempt_cost_per_healed_object": 1.3,
            },
            "w09": {
                "heal_start_p95_ms": [42.0, 40.0, 41.0, 43.0],
                "heal_duplicate_task_count": [0, 0, 0, 0],
                "heal_lock_hold_p95_ms": [7.0, 6.0, 6.5, 7.5],
            },
            "w10": {
                "status": "not_applicable",
                "pacing_observed": False,
            },
            "w11": {"status": "not_applicable"},
        }
        self.report = {
            "status": "pass",
            "performance": "pass",
            "evidence": "measured",
            "cells": 120,
            "comparisons": self.full_comparisons(),
            "started_at": "2026-09-09T00:00:00Z",
            "finished_at": "2026-09-09T02:30:00Z",
        }
        self.write_inputs()

    def full_comparisons(self):
        comparisons = []
        for scenario in summary.SCENARIOS:
            for comparison in ("build", "background"):
                for round_id in range(1, 4):
                    row = copy.deepcopy(self.comparison)
                    row.update(scenario=scenario, comparison=comparison, round=round_id)
                    if scenario == "running-heal" and comparison == "build":
                        row["w10"] = {
                            "status": "observed",
                            "pacing_observed": True,
                            "candidate_pressure_high_ratio": 0.3,
                            "candidate_delay_events": 3,
                            "foreground_p99_change": -0.02,
                            "foreground_throughput_change": 0.01,
                        }
                        row["w11"] = {
                            "status": "observed",
                            "rss_growth_limit": 0.05,
                            "rss_growth": 0.01,
                            "rss_within_limit": True,
                            "baseline_rss_bytes": 1000000.0,
                            "candidate_rss_bytes": 1010000.0,
                            "baseline_heal_lock_wait_p99_ms": 12.0,
                            "candidate_heal_lock_wait_p99_ms": 8.0,
                            "heal_lock_wait_p99_change": -0.33,
                            "healthy_page_latency_observed": True,
                            "foreground_p99_change": -0.02,
                            "foreground_throughput_change": 0.01,
                            "candidate_attempt_cost_per_healed_object": 1.3,
                        }
                    comparisons.append(row)
        return comparisons

    def write_inputs(self):
        (self.abba / "manifest.json").write_text(json.dumps(self.manifest), encoding="utf-8")
        (self.abba / "report.json").write_text(json.dumps(self.report), encoding="utf-8")

    @mock.patch.object(summary, "measured_provenance_verified", return_value=True)
    def test_measured_pass_writes_quiet_artifacts(self, _verified):
        cache_log = self.root / "cache.log"
        cache_log.write_text(
            "compiler noise\nCACHE_COST "
            + json.dumps({
                "schema": 1,
                "scenario": "small_dirty",
                "cache_wire_bytes": 100,
                "save_body_bytes_per_sample": 200,
                "clone": {"max_ns": 10},
                "encode": {"max_ns": 20},
                "save_inclusive": {"max_ns": 30},
                "build": {"source_revision": "abc", "source_tree": "clean", "test_opt_level_override": "0"},
            })
            + "\nmore noise\n",
            encoding="utf-8",
        )
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": cache_log,
            "require_cache_cost": False,
            "json_out": None,
            "markdown_out": None,
        })
        result = summary.build_summary(args)
        self.assertEqual(result["verdict"], "PASS")
        self.assertEqual(result["abba"]["error_rate"], 0.0)
        self.assertEqual(result["abba"]["provenance"]["manifest_sha256"], sha(self.abba / "manifest.json"))
        self.assertEqual(result["abba"]["w09_duplicate_task_count"], 0.0)
        self.assertEqual(result["abba"]["w09_worst_heal_start_p95_ms"], 43.0)
        self.assertEqual(
            [row["status"] for row in result["abba"]["w11_running_heal_build"]],
            ["observed", "observed", "observed"],
        )
        self.assertIn("w11_running_heal_build_statuses: observed,observed,observed", summary.markdown(result))
        self.assertEqual(result["cache_cost"]["max_save_body_amplification"], 2.0)

    @mock.patch.object(summary, "measured_provenance_verified", return_value=True)
    def test_release_descriptor_rejects_unreconciled_comparison_percentiles(self, _verified):
        profile_paths = []
        for kind in summary.RELEASE_PROFILE_ARTIFACTS:
            artifact = self.root / f"{kind}.artifact"
            artifact.write_text(f"{kind} measured profile\n", encoding="utf-8")
            profile_paths.append(f"{kind}={artifact}")
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "release_bundle_descriptor_out": self.root / "release-descriptor.json",
            "release_source_revision": "b" * 40,
            "release_profile_artifact": profile_paths,
        })
        result = summary.build_summary(args)
        with self.assertRaisesRegex(ValueError, "source evidence is unavailable"):
            summary.write_release_bundle_descriptor(args, result)
        self.assertFalse(args.release_bundle_descriptor_out.exists())
        self.assertFalse((self.root / "release-descriptor-artifacts").exists())

    @mock.patch.object(summary, "measured_provenance_verified", return_value=True)
    def test_release_descriptor_stays_closed_without_profile_artifacts(self, _verified):
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "release_bundle_descriptor_out": self.root / "release-descriptor.json",
            "release_source_revision": "b" * 40,
            "release_profile_artifact": [],
        })
        with self.assertRaisesRegex(ValueError, "source evidence is unavailable"):
            summary.write_release_bundle_descriptor(args, summary.build_summary(args))
        self.assertFalse(args.release_bundle_descriptor_out.exists())

    def test_self_reported_measured_pass_has_unverified_provenance(self):
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "release_bundle_descriptor_out": self.root / "release-descriptor.json",
            "release_source_revision": "b" * 40,
            "release_profile_artifact": [],
        })
        for claim in (None, "verified"):
            with self.subTest(claim=claim):
                if claim is not None:
                    self.report["measurement_provenance"] = claim
                    self.write_inputs()
                result = summary.build_summary(args)
                self.assertEqual(result["verdict"], "FAIL")
                self.assertEqual(result["abba"]["measurement_provenance"], "unverified")
                self.assertIn("not independently verified", result["reason"])
                self.assertTrue(all(row["status"] == "unverified" for row in result["abba"]["w11_running_heal_build"]))
                with self.assertRaisesRegex(ValueError, "requires a measured PASS"):
                    summary.write_release_bundle_descriptor(args, result)

    def test_synthetic_report_fails_as_performance_conclusion(self):
        self.report.update(status="synthetic_validated", performance="pending", evidence="synthetic")
        self.write_inputs()
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "json_out": None,
            "markdown_out": None,
        })
        result = summary.build_summary(args)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertIn("synthetic evidence", result["reason"])

    def test_failed_abba_report_without_comparisons_writes_fail_closed_summary(self):
        self.report = {
            "status": "failed",
            "performance": "pending",
            "completed_cells": 7,
            "error": "collector failed",
        }
        self.write_inputs()
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "json_out": None,
            "markdown_out": None,
        })
        result = summary.build_summary(args)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertEqual(result["abba"]["completed_cells"], 7)
        self.assertEqual(result["abba"]["comparisons_total"], 0)
        self.assertIsNone(result["abba"]["error_rate"])
        self.assertIsNone(result["abba"]["foreground_pressure_samples"])
        self.assertIsNone(result["abba"]["foreground_pressure_high_samples"])
        self.assertIn("error details omitted", result["reason"])
        self.assertIn("- completed_cells: 7", summary.markdown(result))
        self.assertIn("- error: details omitted", summary.markdown(result))

    def test_historical_error_does_not_echo_raw_command(self):
        sentinel = "TEST_ONLY_PRIVATE_ARG"
        self.report = {"status": "failed", "performance": "pending", "evidence": "diagnostic",
                       "completed_cells": 7, "error": f"Command ['client', '{sentinel}'] timed out"}
        self.write_inputs()
        args = type("Args", (), {"abba_dir": self.abba, "cache_cost_log": None,
                                  "require_cache_cost": False, "json_out": None, "markdown_out": None})
        result = summary.build_summary(args)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertNotIn(sentinel, json.dumps(result))
        self.assertNotIn(sentinel, summary.markdown(result))
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "argv", ["summarize_scanner_heal_perf.py", "--abba-dir", str(self.abba)]), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(summary.main(), 1)
        self.assertNotIn(sentinel, out.getvalue() + err.getvalue())

    def test_unknown_report_labels_are_not_echoed(self):
        sentinel = "TEST_ONLY_PRIVATE_LABEL"
        self.report = {"status": sentinel, "performance": sentinel, "evidence": sentinel, "comparisons": []}
        self.write_inputs()
        result = summary.summarize_abba(self.abba)
        self.assertEqual(result["gate_state"], "fail")
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["performance"], "unknown")
        self.assertEqual(result["evidence"], "unknown")
        self.assertNotIn(sentinel, json.dumps(result))

    def test_unverified_provenance_does_not_echo_endpoints_or_flags(self):
        sentinel = "TEST_ONLY_PRIVATE_ENDPOINT_OR_ARG"
        self.report = {"status": "failed", "performance": "pending", "evidence": "diagnostic", "comparisons": []}
        self.manifest["fixed"]["release_flags"] = f"--token={sentinel}"
        self.manifest["fixed"]["durability"] = sentinel
        self.manifest["baseline"]["revision"] = sentinel
        self.manifest["started_at"] = sentinel
        self.manifest["release_evidence"]["distributed"]["metrics_endpoints"] = [f"https://{sentinel}@node.example"]
        self.write_inputs()
        args = type("Args", (), {"abba_dir": self.abba, "cache_cost_log": None,
                                  "require_cache_cost": False, "json_out": None, "markdown_out": None})
        result = summary.build_summary(args)
        self.assertEqual(result["verdict"], "FAIL")
        self.assertNotIn(sentinel, json.dumps(result))
        self.assertNotIn(sentinel, summary.markdown(result))

    def test_unverified_comparison_labels_do_not_echo_free_text(self):
        sentinel = "TEST_ONLY_PRIVATE_COMPARISON"
        self.report.update(status="inconclusive", performance="pending")
        self.report["comparisons"][0]["scenario"] = sentinel
        self.report["comparisons"][0]["p1"]["baseline_walk_objects"] = sentinel
        row = next(row for row in self.report["comparisons"]
                   if row["scenario"] == "running-heal" and row["comparison"] == "build")
        row["w10"]["status"] = sentinel
        row["w10"]["candidate_delay_events"] = sentinel
        row["w11"]["status"] = sentinel
        row["w11"]["rss_growth"] = sentinel
        self.write_inputs()
        result = summary.summarize_abba(self.abba)
        self.assertEqual(result["gate_state"], "fail")
        self.assertNotIn(sentinel, json.dumps(result))

    def test_cache_cost_labels_do_not_echo_free_text(self):
        sentinel = "TEST_ONLY_PRIVATE_CACHE_LABEL"
        self.report = {"status": "failed", "performance": "pending", "evidence": "diagnostic", "comparisons": []}
        self.write_inputs()
        cache_log = self.root / "cache.log"
        cache_log.write_text("CACHE_COST " + json.dumps({"schema": 1, "scenario": sentinel,
            "cache_wire_bytes": 100, "save_body_bytes_per_sample": 200,
            "clone": {"max_ns": 10}, "encode": {"max_ns": 20}, "save_inclusive": {"max_ns": 30},
            "build": {"source_revision": sentinel, "source_tree": sentinel,
                      "test_opt_level_override": sentinel}}) + "\n")
        args = type("Args", (), {"abba_dir": self.abba, "cache_cost_log": cache_log,
                                  "require_cache_cost": False, "json_out": None, "markdown_out": None})
        result = summary.build_summary(args)
        self.assertNotIn(sentinel, json.dumps(result))
        self.assertNotIn(sentinel, summary.markdown(result))

    def test_cli_error_does_not_echo_input_path(self):
        sentinel = "TEST_ONLY_PRIVATE_PATH"
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "argv", ["summarize_scanner_heal_perf.py", "--abba-dir", str(self.root / sentinel)]), \
             contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.assertEqual(summary.main(), 1)
        self.assertNotIn(sentinel, out.getvalue() + err.getvalue())

    def test_unknown_cell_counts_are_not_zero_or_free_text(self):
        sentinel = "TEST_ONLY_PRIVATE_COUNT"
        for cells, completed in ((None, None), (sentinel, sentinel), ("absent", "absent")):
            with self.subTest(cells=cells):
                self.report = {"status": "failed", "performance": "pending", "evidence": "diagnostic",
                               "cells": cells, "completed_cells": completed, "comparisons": []}
                if cells == "absent":
                    self.report.pop("cells")
                    self.report.pop("completed_cells")
                self.write_inputs()
                result = summary.summarize_abba(self.abba)
                self.assertIsNone(result["cells"])
                self.assertIsNone(result["completed_cells"])
                self.assertNotIn(sentinel, json.dumps(result))

    def test_rejected_descriptor_does_not_write_summary_outputs(self):
        json_out = self.root / "summary.json"
        markdown_out = self.root / "summary.md"
        descriptor = self.root / "descriptor.json"
        argv = ["summarize_scanner_heal_perf.py", "--abba-dir", str(self.abba),
                "--json-out", str(json_out), "--markdown-out", str(markdown_out),
                "--release-bundle-descriptor-out", str(descriptor)]
        with mock.patch.object(sys, "argv", argv), contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(summary.main(), 1)
        self.assertFalse(json_out.exists())
        self.assertFalse(markdown_out.exists())
        self.assertFalse(descriptor.exists())

    def test_partial_error_rate_is_unknown(self):
        self.report.update(status="inconclusive", performance="pending")
        for value in (None, "missing"):
            with self.subTest(value=value):
                if value == "missing":
                    self.report["comparisons"][0].pop("error_rate")
                else:
                    self.report["comparisons"][0]["error_rate"] = value
                self.write_inputs()
                self.assertIsNone(summary.summarize_abba(self.abba)["error_rate"])

    def test_partial_w09_measurements_remain_unknown(self):
        self.report.update(status="inconclusive", performance="pending")
        fields = {"heal_start_p95_ms": "w09_worst_heal_start_p95_ms",
                  "heal_duplicate_task_count": "w09_duplicate_task_count",
                  "heal_lock_hold_p95_ms": "w09_worst_lock_hold_p95_ms"}
        for field, output in fields.items():
            for missing in (None, "short", "absent"):
                with self.subTest(field=field, missing=missing):
                    original = copy.deepcopy(self.report["comparisons"][0]["w09"])
                    if missing is None:
                        self.report["comparisons"][0]["w09"][field][1] = None
                    elif missing == "short":
                        self.report["comparisons"][0]["w09"][field] = [0]
                    else:
                        self.report["comparisons"][0]["w09"].pop(field)
                    self.write_inputs()
                    self.assertIsNone(summary.summarize_abba(self.abba)[output])
                    self.report["comparisons"][0]["w09"] = original
        self.report["comparisons"].pop()
        self.write_inputs()
        incomplete = summary.summarize_abba(self.abba)
        for output in fields.values():
            self.assertIsNone(incomplete[output])
        self.assertIsNone(incomplete["worst_p99_regression"])
        self.assertIsNone(incomplete["worst_throughput_loss"])
        self.assertIsNone(incomplete["throughput_ops"])
        self.assertIsNone(incomplete["max_reported_comparison_throughput_ops"])

    def test_partial_candidate_p2_measurements_remain_unknown(self):
        self.report.update(status="inconclusive", performance="pending")
        for missing in ("b1", "b2", "short", "absent", "comparison"):
            with self.subTest(missing=missing):
                original = copy.deepcopy(self.report["comparisons"])
                row = self.report["comparisons"][0]
                if missing == "b1":
                    row["p2_post_stop_work_multiples"][1] = None
                elif missing == "b2":
                    row["p2_post_stop_work_multiples"][2] = None
                elif missing == "short":
                    row["p2_post_stop_work_multiples"] = [None, 1.0]
                elif missing == "absent":
                    row.pop("p2_post_stop_work_multiples")
                else:
                    self.report["comparisons"].pop()
                self.write_inputs()
                self.assertIsNone(summary.summarize_abba(self.abba)["p2_worst_post_stop_work_multiple"])
                self.report["comparisons"] = original

    def test_unverified_reported_percentiles_are_not_a_global_percentile(self):
        self.report.update(status="inconclusive", performance="pending")
        self.write_inputs()
        result = summary.summarize_abba(self.abba)
        self.assertIsNone(result["foreground_p95_ms"])
        self.assertIsNone(result["foreground_p99_ms"])
        self.assertIsNone(result["throughput_ops"])
        self.assertEqual(result["max_reported_comparison_throughput_ops"], 100.0)

    def test_passing_report_requires_each_leg_percentile(self):
        self.report["comparisons"][0]["foreground_percentiles_ms_by_leg"].pop("B2")
        self.write_inputs()
        with self.assertRaisesRegex(ValueError, "foreground percentiles"):
            summary.summarize_abba(self.abba)

    def test_passing_report_rejects_invalid_leg_percentiles(self):
        for p95, p99 in ((-1, 10), (1, float("inf")), (float("nan"), 10), (11, 10)):
            with self.subTest(p95=p95, p99=p99):
                self.report["comparisons"][0]["foreground_percentiles_ms_by_leg"]["B2"] = {"p95": p95, "p99": p99}
                self.write_inputs()
                with self.assertRaisesRegex(ValueError, "foreground percentiles"):
                    summary.summarize_abba(self.abba)

    def test_nonfinite_report_metrics_are_rejected(self):
        self.report.update(status="inconclusive", performance="pending")
        for value in (float("inf"), float("nan")):
            with self.subTest(value=value):
                self.report["comparisons"][0]["p99_regression"] = value
                self.write_inputs()
                with self.assertRaisesRegex(ValueError, "invalid numeric field"):
                    summary.summarize_abba(self.abba)

    def test_per_leg_attempt_ratios_are_not_attempt_samples(self):
        self.report.update(status="inconclusive", performance="pending")
        self.write_inputs()
        result = summary.summarize_abba(self.abba)
        self.assertIsNone(result["attempt_cost_samples"])
        self.assertGreater(result["reported_attempt_cost_ratio_count"], 0)
        self.report["comparisons"][0]["w10_w11"].pop("attempt_cost_per_healed_object")
        self.write_inputs()
        self.assertIsNone(summary.summarize_abba(self.abba)["reported_attempt_cost_ratio_count"])

    def test_error_rate_cannot_be_negative_or_exceed_one(self):
        self.report.update(status="inconclusive", performance="pending")
        for value in (-0.1, 1.1):
            with self.subTest(value=value):
                self.report["comparisons"][0]["error_rate"] = value
                self.write_inputs()
                with self.assertRaisesRegex(ValueError, "error_rate"):
                    summary.summarize_abba(self.abba)

    def test_passing_report_cannot_contain_request_errors(self):
        self.report["comparisons"][0]["error_rate"] = 0.01
        self.write_inputs()
        with self.assertRaisesRegex(ValueError, "error_rate"):
            summary.summarize_abba(self.abba)

    def test_partial_pressure_samples_are_unknown(self):
        self.report.update(status="inconclusive", performance="pending")
        self.report["comparisons"][0]["w10_w11"].pop("foreground_pressure_samples")
        for high_samples in ([0, 0, 0], [0, 0, 0, None]):
            with self.subTest(high_samples=high_samples):
                self.report["comparisons"][1]["w10_w11"]["foreground_pressure_high_samples"] = high_samples
                self.write_inputs()
                result = summary.summarize_abba(self.abba)
                self.assertIsNone(result["foreground_pressure_samples"])
                self.assertIsNone(result["foreground_pressure_high_samples"])

    def test_complete_zero_pressure_samples_remain_zero(self):
        self.report.update(status="inconclusive", performance="pending")
        for row in self.report["comparisons"]:
            row["w10_w11"]["foreground_pressure_high_samples"] = [0, 0, 0, 0]
        self.write_inputs()
        self.assertEqual(summary.summarize_abba(self.abba)["foreground_pressure_high_samples"], 0)

    def test_missing_comparison_keeps_matrix_totals_unknown(self):
        self.report.update(status="inconclusive", performance="pending")
        self.report["comparisons"].pop()
        self.write_inputs()
        result = summary.summarize_abba(self.abba)
        self.assertIsNone(result["error_rate"])
        self.assertIsNone(result["foreground_pressure_samples"])
        self.assertIsNone(result["foreground_pressure_high_samples"])

    def test_passing_report_requires_error_and_pressure_samples(self):
        for field in ("error_rate", "foreground_pressure_samples", "foreground_pressure_high_samples"):
            with self.subTest(field=field):
                self.setUp()
                row = self.report["comparisons"][0]
                if field == "error_rate":
                    row.pop(field)
                else:
                    row["w10_w11"].pop(field)
                self.write_inputs()
                with self.assertRaisesRegex(ValueError, "must be measured"):
                    summary.summarize_abba(self.abba)

    def test_passing_abba_report_requires_comparisons(self):
        del self.report["comparisons"]
        self.write_inputs()
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "json_out": None,
            "markdown_out": None,
        })
        with self.assertRaisesRegex(ValueError, "passing report requires comparisons"):
            summary.build_summary(args)

    def test_passing_abba_report_requires_complete_matrix(self):
        cases = {
            "trimmed": lambda: self.report["comparisons"].pop(),
            "duplicate": lambda: self.report["comparisons"].__setitem__(1, copy.deepcopy(self.report["comparisons"][0])),
            "bad cells": lambda: self.report.update(cells=119),
            "bad evidence": lambda: self.manifest.update(evidence="synthetic"),
            "outside": lambda: self.report["comparisons"][0].update(round=99),
            "failed comparison": lambda: self.report["comparisons"][0].update(status="inconclusive"),
        }
        for name, mutate in cases.items():
            with self.subTest(fault=name):
                self.setUp()
                mutate()
                self.write_inputs()
                args = type("Args", (), {
                    "abba_dir": self.abba,
                    "cache_cost_log": None,
                    "require_cache_cost": False,
                    "json_out": None,
                    "markdown_out": None,
                })
                with self.assertRaisesRegex(ValueError, "ABBA matrix|manifest/report evidence|comparison"):
                    summary.build_summary(args)

    def test_passing_measured_report_requires_two_hour_window(self):
        self.manifest["duration_seconds"] = summary.MIN_MEASURED_RELEASE_DURATION_SECONDS - 1
        self.write_inputs()
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "json_out": None,
            "markdown_out": None,
        })
        with self.assertRaisesRegex(ValueError, "two hours"):
            summary.build_summary(args)

    def test_passing_abba_report_requires_w10_w11_evidence(self):
        for fault in ("missing", "pressure", "lock", "attempt", "length", "range", "w11-missing", "w11-pending"):
            with self.subTest(fault=fault):
                self.setUp()
                target = self.report["comparisons"][0]
                if fault == "missing":
                    del target["w10_w11"]
                elif fault == "pressure":
                    del target["w10_w11"]["foreground_pressure_high_sample_ratios"]
                elif fault == "lock":
                    del target["w10_w11"]["heal_lock_wait_p99_ms"]
                elif fault == "attempt":
                    del target["w10_w11"]["attempt_cost_per_healed_object"]
                elif fault == "length":
                    target["w10_w11"]["attempt_cost_per_healed_object"] = [None]
                elif fault == "w11-missing":
                    running_heal = next(
                        comparison for comparison in self.report["comparisons"]
                        if comparison["scenario"] == "running-heal" and comparison["comparison"] == "build"
                    )
                    del running_heal["w11"]
                elif fault == "w11-pending":
                    running_heal = next(
                        comparison for comparison in self.report["comparisons"]
                        if comparison["scenario"] == "running-heal" and comparison["comparison"] == "build"
                    )
                    running_heal["w11"]["status"] = "pending"
                else:
                    target["w10_w11"]["foreground_pressure_high_sample_ratios"] = [1.5, 0.0, 0.0, 0.0]
                self.write_inputs()
                args = type("Args", (), {
                    "abba_dir": self.abba,
                    "cache_cost_log": None,
                    "require_cache_cost": False,
                    "json_out": None,
                    "markdown_out": None,
                })
                with self.assertRaisesRegex(ValueError, "W10/W11|W11|performance evidence|length mismatch|above maximum"):
                    summary.build_summary(args)

    def test_passing_abba_report_requires_w09_evidence(self):
        cases = {
            "missing": lambda row: row.pop("w09"),
            "start": lambda row: row["w09"].pop("heal_start_p95_ms"),
            "zero start": lambda row: row["w09"].update(heal_start_p95_ms=[0, 40.0, 41.0, 43.0]),
            "duplicates": lambda row: row["w09"].update(heal_duplicate_task_count=[0, 1, 0, 0]),
            "unknown duplicates": lambda row: row["w09"].update(heal_duplicate_task_count=[None, 0, 0, 0]),
            "lock": lambda row: row["w09"].pop("heal_lock_hold_p95_ms"),
            "zero lock": lambda row: row["w09"].update(heal_lock_hold_p95_ms=[0, 6.0, 6.5, 7.5]),
            "length": lambda row: row["w09"].update(heal_lock_hold_p95_ms=[1]),
        }
        for name, mutate in cases.items():
            with self.subTest(fault=name):
                self.setUp()
                mutate(self.report["comparisons"][0])
                self.write_inputs()
                args = type("Args", (), {
                    "abba_dir": self.abba,
                    "cache_cost_log": None,
                    "require_cache_cost": False,
                    "json_out": None,
                    "markdown_out": None,
                })
                with self.assertRaisesRegex(ValueError, "W09|performance evidence|above maximum|length mismatch|must be measured"):
                    summary.build_summary(args)

    def test_passing_measured_report_requires_release_evidence_manifest(self):
        del self.manifest["release_evidence"]
        self.write_inputs()
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": False,
            "json_out": None,
            "markdown_out": None,
        })
        with self.assertRaisesRegex(ValueError, "release_evidence"):
            summary.build_summary(args)

    def test_requires_cache_profile_when_requested(self):
        args = type("Args", (), {
            "abba_dir": self.abba,
            "cache_cost_log": None,
            "require_cache_cost": True,
            "json_out": None,
            "markdown_out": None,
        })
        with self.assertRaisesRegex(ValueError, "cache-cost profile log is required"):
            summary.build_summary(args)

    def test_cli_prints_one_line_and_exits_nonzero_for_pending_performance(self):
        self.report.update(status="inconclusive", performance="inconclusive")
        self.write_inputs()
        stdout = io.StringIO()
        stderr = io.StringIO()
        argv = [
            "summarize_scanner_heal_perf.py",
            "--abba-dir",
            str(self.abba),
            "--json-out",
            str(self.root / "summary.json"),
            "--markdown-out",
            str(self.root / "summary.md"),
        ]
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with mock.patch.object(sys, "argv", argv):
                code = summary.main()
        self.assertEqual(code, 1)
        self.assertEqual(stdout.getvalue().count("\n"), 1)
        self.assertTrue(stdout.getvalue().startswith("FAIL scanner_heal_perf "))
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(summary.read_json(self.root / "summary.json")["verdict"], "FAIL")
        self.assertIn("worst_p99_regression", (self.root / "summary.md").read_text(encoding="utf-8"))

    def test_invalid_cache_cost_lines_fail_closed(self):
        cache_log = self.root / "cache.log"
        cache_log.write_text("CACHE_COST {\"schema\": 2}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unsupported schema"):
            summary.summarize_cache_cost(cache_log)

    def test_script_entrypoint_is_quiet(self):
        script = Path(__file__).with_name("summarize_scanner_heal_perf.py")
        process = subprocess.run(
            [sys.executable, str(script), "--abba-dir", str(self.abba)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(process.returncode, 1, process.stderr)
        self.assertTrue(process.stdout.startswith("FAIL scanner_heal_perf "))
        self.assertEqual(process.stdout.count("\n"), 1)
        self.assertEqual(process.stderr, "")


if __name__ == "__main__":
    unittest.main()
