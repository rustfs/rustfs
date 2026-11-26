#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("warp_raw_quality.py")
SPEC = importlib.util.spec_from_file_location("warp_raw_quality", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


HEADER = "thread\top\tn_objects\tbytes\tendpoint\terror\tstart\tend\tduration_ns\n"


def row(thread: int, endpoint: str, start: int, end: int, error: str = "", *, duration_ns: int | None = None) -> str:
    def stamp(second: int) -> str:
        minute, second = divmod(second, 60)
        return f"2026-01-01T00:{minute:02d}:{second:02d}Z"

    duration = (end - start) * 1_000_000_000 if duration_ns is None else duration_ns
    return f"{thread}\tPUT\t1\t100\t{endpoint}\t{error}\t{stamp(start)}\t{stamp(end)}\t{duration}\n"


class WarpRawQualityTest(unittest.TestCase):
    def fixture(self, error: bool = False) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "warp.tsv"
        content = HEADER
        content += row(0, "a", 0, 1)
        content += row(0, "a", 1, 3)
        content += row(0, "a", 3, 5)
        content += row(0, "a", 5, 10)
        content += row(1, "b", 0, 2, "boom" if error else "")
        content += row(1, "b", 2, 4)
        content += row(1, "b", 4, 6)
        content += row(1, "b", 6, 9)
        path.write_text(content)
        return path

    def test_common_window_and_host_counts(self) -> None:
        result = MODULE.parse(self.fixture(), {"a", "b"})
        self.assertEqual(result["measure_duration_secs"], 3)
        self.assertEqual(result["completed_objects"], 3)
        self.assertEqual(result["completed_requests"], 3)
        self.assertEqual(result["host_counts"], {"a": 4, "b": 4})
        self.assertAlmostEqual(result["reqps"], 1.0)

    def run_cli(self, source: Path, *flags: str) -> tuple[int, dict]:
        output = source.with_name("quality.json")
        result = subprocess.run(
            [sys.executable, str(MODULE_PATH), "--input", str(source), "--output", str(output), *flags],
            check=False,
        )
        return result.returncode, json.loads(output.read_text())

    def test_steady_rate_uses_exact_completions_and_accepts_the_minimum(self) -> None:
        source = self.fixture()
        status, result = self.run_cli(source, "--steady-after-seconds", "1", "--min-steady-rps", "1")
        self.assertEqual(status, 0)
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["steady"]["completed_objects"], 2)
        self.assertEqual(result["steady"]["completed_requests"], 2)
        self.assertEqual(result["steady"]["p99_ms"], 2000)
        self.assertEqual(result["steady"]["start_ns"], result["measure_start_ns"] + 1_000_000_000)
        self.assertEqual(result["steady"]["duration_secs"], 2)
        self.assertEqual(result["steady"]["completed_objects_per_second"], 1)

    def test_steady_shortfall_fails_even_when_whole_window_rate_meets_threshold(self) -> None:
        source = self.fixture()
        intervals = ((0, 1), (1, 2), (2, 3), (3, 4), (4, 9), (9, 10))
        source.write_text(HEADER + "".join(row(0, "a", start, end) for start, end in intervals))
        status, old_result = self.run_cli(source)
        self.assertEqual(status, 0)
        self.assertGreaterEqual(old_result["reqps"], 0.5)
        self.assertNotIn("steady", old_result)
        status, result = self.run_cli(source, "--steady-after-seconds", "4", "--min-steady-rps", "0.5")
        self.assertEqual(status, 1)
        self.assertEqual(result["reason"], "steady completion rate below minimum")
        self.assertEqual(result["steady"]["completed_objects"], 0)
        self.assertIsNone(result["steady"]["p99_ms"])
        self.assertEqual(result["reqps"], old_result["reqps"])

    def test_steady_rate_upper_bound_rejects_overshoot_and_accepts_equality(self) -> None:
        source = self.fixture()
        flags = ("--steady-after-seconds", "1", "--min-steady-rps", "0.5", "--max-steady-rps")
        status, result = self.run_cli(source, *flags, "0.9")
        self.assertEqual(status, 1)
        self.assertEqual(result["reason"], "steady completion rate above maximum")
        self.assertEqual(result["steady"]["completed_objects_per_second"], 1)
        self.assertEqual(result["steady"]["max_completed_objects_per_second"], 0.9)
        status, result = self.run_cli(source, *flags, "1")
        self.assertEqual(status, 0)
        self.assertEqual(result["status"], "pass")

    def test_steady_p99_excludes_completions_at_right_edge(self) -> None:
        source = self.fixture()
        intervals = ((0, 0, 8), (0, 8, 10), (0, 10, 15), (0, 15, 35), (1, 0, 1), (1, 1, 2), (1, 2, 15), (1, 20, 40))
        source.write_text(HEADER + "".join(row(thread, "a", start, end) for thread, start, end in intervals))
        result = MODULE.parse(source, {"a"}, steady_after_seconds=2)
        self.assertEqual(result["steady"]["completed_requests"], 1)
        self.assertEqual(result["steady"]["p99_ms"], 2000)

    def test_steady_p99_ranks_recorded_request_durations(self) -> None:
        source = self.fixture()
        # Use different wall-clock spans and recorded durations to verify both the
        # source of latency and the 99th (not the largest) of 100 observations.
        samples = (row(0, "a", i, i + 1, duration_ns=i * 1_000_000) for i in range(1, 101))
        source.write_text(HEADER + row(0, "a", 0, 1) + "".join(samples) + row(0, "a", 102, 103))
        result = MODULE.parse(source, {"a"}, steady_after_seconds=1)
        self.assertEqual(result["steady"]["completed_requests"], 100)
        self.assertEqual(result["steady"]["p99_ms"], 99)

    def test_invalid_steady_gate_rejects_missing_empty_and_nonfinite_options(self) -> None:
        source = self.fixture()
        cases = (
            ("--steady-after-seconds", "1"),
            ("--steady-after-seconds", "3", "--min-steady-rps", "1"),
            ("--steady-after-seconds", "-1", "--min-steady-rps", "1"),
            ("--steady-after-seconds", "1", "--min-steady-rps", "nan"),
            ("--max-steady-rps", "1"),
            ("--steady-after-seconds", "1", "--min-steady-rps", "1", "--max-steady-rps", "0.9"),
            ("--steady-after-seconds", "1", "--min-steady-rps", "1", "--max-steady-rps", "inf"),
        )
        for args in cases:
            with self.subTest(args=args):
                status, result = self.run_cli(source, *args)
                self.assertEqual(status, 1)
                self.assertEqual(result["status"], "fail")

    def test_any_raw_error_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains request errors"):
            MODULE.parse(self.fixture(error=True), {"a", "b"})

    def test_expected_hosts_are_exact(self) -> None:
        with self.assertRaisesRegex(ValueError, "expected host set"):
            MODULE.parse(self.fixture(), {"a", "b", "c"})

    def test_timestamp_parser_preserves_nanoseconds(self) -> None:
        self.assertEqual(
            MODULE.timestamp_ns("1970-01-01T00:00:01.123456789Z"),
            1_123_456_789,
        )

    def test_truncated_row_fails_as_malformed_input(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "warp.tsv"
        path.write_text(HEADER + "0\tPUT\t1\t100\ta\t\t2026-01-01T00:00:00Z\t2026-01-01T00:00:01Z\n")
        with self.assertRaisesRegex(ValueError, "row width differs"):
            MODULE.parse(path, {"a"})


if __name__ == "__main__":
    unittest.main()
