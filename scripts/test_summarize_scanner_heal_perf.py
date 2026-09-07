#!/usr/bin/env python3

from __future__ import annotations

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
        }
        self.report = {
            "status": "pass",
            "performance": "pass",
            "evidence": "measured",
            "cells": 120,
            "comparisons": [self.comparison],
        }
        self.write_inputs()

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
