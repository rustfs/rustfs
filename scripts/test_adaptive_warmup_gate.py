#!/usr/bin/env python3

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("adaptive_warmup_gate.py")


def window(avg: float, p90: float, p99: float, *, errors: int = 0, successful_puts: int = 5127) -> dict:
    per_host, remainder = divmod(successful_puts, 4)
    return {
        "successful_puts": successful_puts,
        "errors": errors,
        "reqps": 250,
        "avg_ms": avg,
        "p90_ms": p90,
        "p99_ms": p99,
        "host_counts": {
            "a": per_host + (remainder > 0),
            "b": per_host + (remainder > 1),
            "c": per_host + (remainder > 2),
            "d": per_host,
        },
    }


class AdaptiveWarmupGateTest(unittest.TestCase):
    def run_gate(self, rows: list[dict], *extra: str) -> tuple[int, dict]:
        with tempfile.TemporaryDirectory() as directory:
            input_path = Path(directory) / "input.json"
            output_path = Path(directory) / "output.json"
            input_path.write_text(json.dumps(rows))
            result = subprocess.run(
                [str(SCRIPT), "--input", str(input_path), "--output", str(output_path), *extra],
                check=False,
            )
            return result.returncode, json.loads(output_path.read_text())

    def test_three_stable_windows_pass(self) -> None:
        code, result = self.run_gate([window(14, 20, 100), window(14.2, 20.5, 102), window(13.9, 20.2, 99)])
        self.assertEqual(code, 0)
        self.assertEqual(result["status"], "pass")

    def test_unstable_tail_continues_before_limit(self) -> None:
        code, result = self.run_gate([window(16, 20, 126), window(14, 15, 98), window(13, 15, 70)])
        self.assertEqual(code, 2)
        self.assertEqual(result["reason"], "recent_windows_unstable")

    def test_unstable_tail_fails_at_limit(self) -> None:
        rows = [window(16 if index % 2 == 0 else 13, 20, 126 if index % 2 == 0 else 70) for index in range(12)]
        code, result = self.run_gate(rows)
        self.assertEqual(code, 1)
        self.assertEqual(result["reason"], "max_windows_unstable")

    def test_request_error_fails_closed(self) -> None:
        code, result = self.run_gate([window(14, 20, 100, errors=1)])
        self.assertEqual(code, 1)
        self.assertEqual(result["status"], "fail")

    def test_exact_scheduled_count_is_valid_without_tail_completions(self) -> None:
        code, result = self.run_gate(
            [window(14, 20, 100, successful_puts=5000)] * 3,
        )
        self.assertEqual(code, 0)
        self.assertEqual(result["status"], "pass")

    def test_host_count_mismatch_fails_closed(self) -> None:
        row = window(14, 20, 100)
        row["host_counts"]["a"] -= 1
        code, result = self.run_gate([row])
        self.assertEqual(code, 1)
        self.assertEqual(result["reason"], "warmup host counts do not match successful PUT count")

    def test_non_finite_threshold_fails_closed(self) -> None:
        code, result = self.run_gate([window(14, 20, 100)], "--max-spread-pct", "inf")
        self.assertEqual(code, 1)
        self.assertEqual(result["reason"], "warmup numeric arguments must be finite")


if __name__ == "__main__":
    unittest.main()
