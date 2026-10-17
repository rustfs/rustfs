#!/usr/bin/env python3
"""Verify that paired conditional-read evidence cannot bypass acceptance gates."""

import copy
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import MagicMock, patch

from run_conditional_read_abba import compare, parse_rows, run_case


def row():
    return {"size": 4096, "kind": "miss", "cache": False, "slowtail_ms": "0",
            "ops_per_sec": 1000, "p95_ms": 2, "p99_ms": 3,
            "lock_p95_ms": 1, "lock_p99_ms": 2}


class AcceptanceTests(unittest.TestCase):
    def test_stable_equivalent_legs_pass(self):
        self.assertTrue(compare([{("case",): row()} for _ in range(4)])["accepted"])

    def test_baseline_drift_blocks_candidate_attribution(self):
        legs = [{("case",): row()} for _ in range(4)]
        legs[3][("case",)]["ops_per_sec"] = 800
        result = compare(legs)
        self.assertFalse(result["accepted"])
        self.assertEqual(result["reason"], "baseline_drift")
        self.assertNotIn("cases", result)

    def test_miss_throughput_and_lock_regressions_fail(self):
        for metric, value in [("ops_per_sec", 900), ("lock_p99_ms", 4)]:
            legs = [{("case",): row()} for _ in range(4)]
            for index in (1, 2):
                legs[index][("case",)][metric] = value
            self.assertFalse(compare(legs)["accepted"], metric)

    def test_missing_workload_is_rejected(self):
        legs = [{("case",): row()} for _ in range(4)]
        legs[1] = {}
        with self.assertRaises(ValueError):
            compare(legs)

    def test_complete_rows_include_first_libtest_output_line(self):
        rows = []
        for size in (4096, 1_300_000):
            for kind in ("unconditional", "hit", "miss"):
                value = copy.copy(row())
                value.update(size=size, kind=kind)
                rows.append("CONDITIONAL_BENCH " + json.dumps(value))
        output = "test app::benchmark ... " + "\n".join(rows)
        self.assertEqual(len(parse_rows(output)), 6)
        with self.assertRaises(ValueError):
            parse_rows(output + "\n" + rows[0])

    def test_wrong_shape_or_nonfinite_metrics_are_rejected(self):
        rows = []
        for size in (4096, 1_300_000):
            for kind in ("unconditional", "hit", "miss"):
                value = copy.copy(row())
                value.update(size=size, kind=kind)
                rows.append(value)
        for field, value in [("kind", "unknown"), ("p99_ms", float("nan"))]:
            changed = copy.deepcopy(rows)
            changed[0][field] = value
            output = "\n".join("CONDITIONAL_BENCH " + json.dumps(item) for item in changed)
            with self.assertRaises(ValueError):
                parse_rows(output)


class IsolationTests(unittest.TestCase):
    def process(self):
        process = MagicMock()
        process.__enter__.return_value = process
        process.communicate.return_value = ("saved output", None)
        process.returncode = 0
        return process

    def test_changed_executable_is_rejected_before_launch(self):
        with patch("run_conditional_read_abba.digest", return_value="changed"), \
                patch("run_conditional_read_abba.subprocess.Popen") as launch:
            with self.assertRaisesRegex(RuntimeError, "before execution"):
                run_case(Path("binary"), "expected", {})
            launch.assert_not_called()

    def test_changed_executable_is_rejected_after_launch(self):
        with patch("run_conditional_read_abba.digest", side_effect=["expected", "changed"]), \
                patch("run_conditional_read_abba.check_resource_isolation"), \
                patch("run_conditional_read_abba.subprocess.Popen", return_value=self.process()):
            with self.assertRaisesRegex(RuntimeError, "during execution"):
                run_case(Path("binary"), "expected", {})

    def test_final_case_checks_resource_isolation_after_exit(self):
        with patch("run_conditional_read_abba.digest", return_value="expected"), \
                patch("run_conditional_read_abba.check_resource_isolation", side_effect=RuntimeError("concurrent build")), \
                patch("run_conditional_read_abba.subprocess.Popen", return_value=self.process()):
            with self.assertRaisesRegex(RuntimeError, "concurrent build"):
                run_case(Path("binary"), "expected", {})

    def test_concurrent_build_during_execution_kills_case_and_saves_log(self):
        process = self.process()
        process.communicate.side_effect = [subprocess.TimeoutExpired("binary", 1), ("saved output", None)]
        log = MagicMock()
        with patch("run_conditional_read_abba.digest", return_value="expected"), \
                patch("run_conditional_read_abba.check_resource_isolation", side_effect=RuntimeError("concurrent build")) as check, \
                patch("run_conditional_read_abba.subprocess.Popen", return_value=process):
            with self.assertRaisesRegex(RuntimeError, "concurrent build"):
                run_case(Path("binary"), "expected", {}, log)
            check.assert_called_once_with(check_load=False)
            process.kill.assert_called_once()
            log.write_text.assert_called_once_with("saved output")


if __name__ == "__main__":
    unittest.main()
