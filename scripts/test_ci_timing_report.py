#!/usr/bin/env python3
"""Timing reports must preserve missing evidence and non-successful runs."""
import copy
from datetime import datetime, timezone
import unittest
from unittest import mock

from ci_timing_report import collect, summarize


class TimingTests(unittest.TestCase):
    def setUp(self):
        self.job = {"id": 2, "name": "Workspace Test and Lint", "status": "completed", "conclusion": "success",
                    "created_at": "2026-09-01T00:10:00Z", "started_at": "2026-09-01T00:15:00Z", "completed_at": "2026-09-01T00:25:00Z",
                    "steps": [{"name": "Run nextest tests", "status": "completed", "conclusion": "success", "started_at": "2026-09-01T00:16:00Z", "completed_at": "2026-09-01T00:24:00Z"}]}
        self.run = {"id": 1, "run_attempt": 2, "head_sha": "a" * 40, "status": "completed", "conclusion": "success",
                    "created_at": "2026-09-01T00:00:00Z", "run_started_at": "2026-09-01T00:00:00Z", "jobs": [self.job]}

    def test_parallel_minutes_are_not_wall_time(self):
        self.run["jobs"].append({**self.job, "id": 3, "name": "Other"})
        result = summarize([self.run])
        self.assertEqual(result["successful_code_wall"]["median_minutes"], 25)
        self.assertEqual(result["successful_code_job_sum"]["median_minutes"], 20)
        self.assertEqual(result["jobs"][self.job["name"]]["queue"]["median_minutes"], 5)
        self.assertEqual(result["jobs"][self.job["name"]]["steps"]["Run nextest tests"]["median_minutes"], 8)

    def test_cancelled_incomplete_and_docs_runs_are_not_success_samples(self):
        cancelled = {**self.run, "status": "completed", "conclusion": "cancelled"}
        pending = {**self.run, "status": "queued", "conclusion": None, "jobs": []}
        docs = {**self.run, "jobs": [{**self.job, "name": "Quick Checks"}]}
        result = summarize([self.run, cancelled, pending, docs])
        self.assertEqual(result["successful_code_runs"], 1)
        self.assertEqual(result["cancelled_fraction"], 0.25)
        self.assertEqual(result["completed_fraction"], 0.75)

    def test_rerun_wall_time_excludes_time_before_the_attempt(self):
        self.run["created_at"] = "2026-08-01T00:00:00Z"
        self.assertEqual(summarize([self.run])["successful_code_wall"]["median_minutes"], 25)
        del self.run["run_started_at"]
        self.assertIsNone(summarize([self.run])["successful_code_wall"]["median_minutes"])

    def test_old_workspace_job_name_requires_an_executed_test_step(self):
        self.job["name"] = "Test and Lint"
        self.assertEqual(summarize([self.run])["successful_code_runs"], 1)
        self.job["steps"] = []
        self.assertEqual(summarize([self.run])["successful_code_runs"], 0)

    def test_collection_rejects_out_of_range_api_results(self):
        with mock.patch("ci_timing_report.api", return_value={"workflow_runs": [self.run]}):
            with self.assertRaisesRegex(ValueError, "outside the requested date range"):
                collect("owner/repo", 1, datetime(2026, 9, 2, tzinfo=timezone.utc))

    def test_missing_times_are_unknown_not_zero(self):
        self.job["created_at"] = None
        self.job["completed_at"] = None
        result = summarize([self.run])
        self.assertIsNone(result["successful_code_wall"]["median_minutes"])
        self.assertIsNone(result["successful_code_job_sum"]["median_minutes"])
        self.assertEqual(result["jobs"][self.job["name"]]["queue"]["missing"], 1)

    def test_collection_binds_the_attempt_and_rejects_reruns(self):
        source = copy.deepcopy(self.run)
        del source["jobs"]
        with mock.patch("ci_timing_report.api", side_effect=[{"workflow_runs": [source]}, {"jobs": [self.job]}, source]) as api:
            snapshot = collect("owner/repo", 1, datetime(2026, 8, 1, tzinfo=timezone.utc))
            self.assertIn("/attempts/2/jobs?", api.call_args_list[1].args[0])
            self.assertEqual(snapshot["runs"][0]["run_attempt"], 2)
        with mock.patch("ci_timing_report.api", side_effect=[{"workflow_runs": [source]}, {"jobs": [self.job]}, {**source, "run_attempt": 3}]):
            with self.assertRaisesRegex(ValueError, "changed during collection"):
                collect("owner/repo", 1, datetime(2026, 8, 1, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
