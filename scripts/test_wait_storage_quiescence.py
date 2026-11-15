import json
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("wait_storage_quiescence.py")


def sample(timestamp, dirty, writeback, io_total, cpu_total):
    return {
        "captured_at_ms": timestamp,
        "mem_total_kib": 8 * 1024 * 1024,
        "dirty_kib": dirty,
        "writeback_kib": writeback,
        "io_psi_total_us": io_total,
        "cpu_psi_total_us": cpu_total,
    }


class QuiescenceGateTests(unittest.TestCase):
    def run_fixture(self, rows, consecutive=3):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory) / "samples.jsonl"
            fixture.write_text("".join(json.dumps(row) + "\n" for row in rows))
            return subprocess.run(
                [str(SCRIPT), "--sample-file", str(fixture), "--consecutive", str(consecutive)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    def test_stable_window_passes(self):
        result = self.run_fixture(
            [sample(1000, 4096, 0, 1000, 1000), sample(2000, 4096, 0, 2000, 2000), sample(3000, 4096, 0, 3000, 3000)]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["status"], "pass")

    def test_high_dirty_never_passes(self):
        result = self.run_fixture(
            [sample(1000, 500_000, 0, 1000, 1000), sample(2000, 500_000, 0, 2000, 2000), sample(3000, 500_000, 0, 3000, 3000)]
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout)["status"], "inconclusive")

    def test_pressure_burst_resets_window(self):
        result = self.run_fixture(
            [
                sample(1000, 4096, 0, 1000, 1000),
                sample(2000, 4096, 0, 500_000, 2000),
                sample(3000, 4096, 0, 501_000, 3000),
                sample(4000, 4096, 0, 502_000, 4000),
                sample(5000, 4096, 0, 503_000, 5000),
            ]
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["observed_samples"], 5)

    def test_counter_reset_fails_closed(self):
        result = self.run_fixture([sample(1000, 4096, 0, 2000, 2000), sample(2000, 4096, 0, 1000, 3000)])
        self.assertEqual(result.returncode, 2)
        self.assertIn("counter reset", result.stderr)


if __name__ == "__main__":
    unittest.main()
