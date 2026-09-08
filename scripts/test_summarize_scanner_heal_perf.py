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
                },
            },
        }
        self.comparison = {
            "scenario": "cold-hot",
            "comparison": "build",
            "round": 1,
            "status": "pass",
            "p99_regression": 0.02,
            "throughput_change": -0.01,
            "p1": {"required_reduction": 0.8, "observed_reduction": 0.82, "repeatability_drift": 0.01},
            "p2_post_stop_work_multiples": [None, 1.1, 1.0, None],
            "w10_w11": {
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
            "w11": {"status": "not_applicable"},
        }
        self.report = {
            "status": "pass",
            "performance": "pass",
            "evidence": "measured",
            "cells": 120,
            "comparisons": self.full_comparisons(),
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

    def test_measured_pass_writes_quiet_artifacts(self):
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
        self.assertEqual(result["abba"]["provenance"]["manifest_sha256"], sha(self.abba / "manifest.json"))
        self.assertEqual(result["abba"]["w09_duplicate_task_count"], 0.0)
        self.assertEqual(result["abba"]["w09_worst_heal_start_p95_ms"], 43.0)
        self.assertEqual(
            [row["status"] for row in result["abba"]["w11_running_heal_build"]],
            ["observed", "observed", "observed"],
        )
        self.assertIn("w11_running_heal_build_statuses: observed,observed,observed", summary.markdown(result))
        self.assertEqual(result["cache_cost"]["max_save_body_amplification"], 2.0)

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
        self.assertIn("collector failed", result["reason"])
        self.assertIn("- completed_cells: 7", summary.markdown(result))
        self.assertIn("- error: collector failed", summary.markdown(result))

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
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(process.stdout.count("\n"), 1)
        self.assertEqual(process.stderr, "")


if __name__ == "__main__":
    unittest.main()
