#!/usr/bin/env python3

import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("adaptive_warmup_runner.py")
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("adaptive_warmup_runner", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class AdaptiveWarmupRunnerTest(unittest.TestCase):
    def test_latency_extractor_rejects_unknown_schema(self) -> None:
        with self.assertRaises((OSError, RuntimeError, ValueError)):
            MODULE.extract_latency_metrics(Path("/does/not/exist"), Path("/does/not/exist"))

    def test_gate_evaluator_requires_recent_consecutive_windows(self) -> None:
        stable = {"reqps": 250.0, "avg_ms": 14.0, "p90_ms": 20.0, "p99_ms": 100.0}
        unstable = {"reqps": 250.0, "avg_ms": 20.0, "p90_ms": 30.0, "p99_ms": 180.0}
        result = MODULE.adaptive_warmup_gate.evaluate([stable, stable, unstable], 3, 12, 5)
        self.assertEqual(result["status"], "continue")

    def test_unstable_exhaustion_has_non_retryable_exit_code(self) -> None:
        result = {"status": "fail", "reason": "max_windows_unstable"}
        self.assertEqual(MODULE.result_exit_code(result), 95)

    def test_quality_failure_keeps_generic_failure_exit_code(self) -> None:
        result = {"status": "fail", "reason": "warmup request rate is outside tolerance"}
        self.assertEqual(MODULE.result_exit_code(result), 1)


if __name__ == "__main__":
    unittest.main()
