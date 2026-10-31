#!/usr/bin/env python3
"""Tests for incomplete Scanner/Heal evidence plans."""

import tempfile
import unittest
from pathlib import Path

import run_scanner_heal_linux_evidence_plan as planner


class ScannerHealLinuxEvidencePlanTest(unittest.TestCase):
    def test_p2_stays_pending_without_an_independent_oracle_producer(self):
        steps = planner.performance_steps()
        p2 = next(step for step in steps if step["id"] == "p2-independent-evidence")
        plan = {"source_revision": "a" * 40, "stages": [{"id": "performance", "steps": [p2]}]}
        with tempfile.TemporaryDirectory() as tmp:
            status = planner.build_status(plan, Path(tmp))
        self.assertEqual(status["decision"], "blocked")
        self.assertIn("P2", status["pending_gates"])
        self.assertTrue(status["next_step"]["reason"])

    def test_scheduler_pressure_plan_does_not_claim_p2(self):
        step = next(step for step in planner.performance_steps() if step["id"] == "scheduler-pressure")
        self.assertNotIn("P2", step["covers"]["gates"])
        command = step["commands"][0]["command"]
        for flag in ("--started-at", "--finished-at", "--run-id", "--measurement-window-id"):
            self.assertIn(flag, command)


if __name__ == "__main__":
    unittest.main()
