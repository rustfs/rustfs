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

    def run_live_gate(self, delay_stage):
        with tempfile.TemporaryDirectory() as directory:
            fake_docker = Path(directory) / "docker"
            fake_docker.write_text(
                "#!/bin/sh\n"
                "set -eu\n"
                f'if [ "{delay_stage}" = "context" ] && [ "$1 $2" = "context show" ]; then sleep 2; fi\n'
                'if [ "$1 $2" = "context show" ]; then printf "test-context\\n"; exit 0; fi\n'
                f'if [ "{delay_stage}" = "exec" ] && [ "$3" = "exec" ]; then sleep 2; fi\n'
                'if [ "$3" = "inspect" ]; then printf "container-id\\n"; exit 0; fi\n'
                'if [ "$3" = "exec" ]; then\n'
                '  printf "mem_total_kib=8388608\\ndirty_kib=4096\\nwriteback_kib=0\\n"\n'
                '  printf "io_psi_total_us=1000\\ncpu_psi_total_us=1000\\n"\n'
                "  exit 0\n"
                "fi\n"
                "exit 64\n"
            )
            fake_docker.chmod(0o755)
            return subprocess.run(
                [
                    str(SCRIPT),
                    "--docker-bin",
                    str(fake_docker),
                    "--docker-context",
                    "test-context",
                    "--container",
                    "target",
                    "--timeout-secs",
                    "1",
                    "--consecutive",
                    "1",
                    "--interval-secs",
                    "0.01",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
            )

    def test_initial_docker_lookup_uses_gate_deadline(self):
        result = self.run_live_gate("context")
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("deadline expired", result.stderr)
        self.assertNotIn('"status": "pass"', result.stdout)

    def test_docker_exec_uses_remaining_gate_deadline(self):
        result = self.run_live_gate("exec")
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("deadline expired", result.stderr)
        self.assertNotIn('"status": "pass"', result.stdout)


if __name__ == "__main__":
    unittest.main()
