"""Regression checks for benchmark evidence and drift verdicts."""

import json
from pathlib import Path
import tempfile
import unittest

import delete_recovery_perf as perf


class EvidenceTests(unittest.TestCase):
    def test_expired_operation_window_cannot_be_attributed_to_the_candidate(self):
        with self.assertRaisesRegex(ValueError, "expired"):
            perf.stage_delta({"prepare": {"count": 4, "ns": 100}}, {"prepare": {"count": 3, "ns": 90}})

    def test_operation_service_times_keep_their_units(self):
        self.assertEqual(perf.stage_delta({}, {"prepare": {"count": 4, "ns": 2_000_000}}),
                         {"prepare": {"calls": 4, "service_ms": 2.0}})

    def test_binary_identity_and_release_configuration_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            binary, receipt = Path(directory) / "binary", Path(directory) / "receipt.json"
            binary.write_bytes(b"candidate")
            value = {"binary_sha256": perf.binary_digest(binary), "source_head": "a" * 40,
                     "build": {"profile": "debug", "opt_level": 0}}
            receipt.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "release"):
                perf.binary_receipt(binary, receipt)
            binary.write_bytes(b"different executable")
            with self.assertRaisesRegex(ValueError, "does not match"):
                perf.binary_receipt(binary, receipt)

    def test_control_or_candidate_drift_withholds_every_improvement_claim(self):
        for unstable in ("A2", "B2"):
            samples = self.samples()
            for sample in samples[unstable]:
                sample["wall_seconds"] *= 2
            result = perf.comparison_report(samples, 10)
            self.assertEqual(result["verdict"], "inconclusive")
            self.assertNotIn("wall_time_reduction_percent", result)

    def test_candidate_startup_timeout_withholds_improvement_claims(self):
        samples = self.samples()
        samples["B1"][0]["clean_restart"]["discovery_within_30_seconds"] = False
        result = perf.comparison_report(samples, 10)
        self.assertFalse(result["candidate_discovery_within_30_seconds"])
        self.assertEqual(result["verdict"], "inconclusive")
        self.assertNotIn("wall_time_reduction_percent", result)

    def test_stable_controls_allow_a_quantified_result(self):
        result = perf.comparison_report(self.samples(), 10)
        self.assertEqual(result["verdict"], "comparable")
        self.assertAlmostEqual(result["wall_time_reduction_percent"]["inline-batch"], 30)

    @staticmethod
    def samples():
        return {cohort: [{"case": case, "wall_seconds": value, "clean_restart": {"discovery_within_30_seconds": True}} for case in
                         ("inline-batch", "inline-single", "external-batch") for _ in range(3)]
                for cohort, value in (("A1", 10), ("B1", 7), ("B2", 7), ("A2", 10))}


if __name__ == "__main__":
    unittest.main()
