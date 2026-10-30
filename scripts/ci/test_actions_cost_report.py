#!/usr/bin/env python3
"""Accounting fixtures preserve failed, cancelled, shared and unknown work."""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import actions_cost_report as report


class AccountingTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = json.loads((Path(__file__).parent / "fixtures/cost/attempts.json").read_text())

    def test_all_attempts_and_cross_workflow_allocation_conserve_cost(self):
        summary = report.summarize(self.snapshot)
        self.assertEqual(summary["attempts"], 6)
        self.assertEqual(summary["attempt_statuses"], {"failure": 1, "success": 3, "cancelled": 2})
        self.assertEqual(summary["total_unallocated"]["known_runner_minutes"], 33)
        self.assertEqual(summary["pull_requests"]["11"]["known_runner_minutes"], 25)
        self.assertEqual(summary["pull_requests"]["12"]["known_runner_minutes"], 3)
        self.assertEqual(summary["pull_requests"]["11"]["known_queue_minutes"], 6)
        allocated = sum(pr["known_runner_minutes"] for pr in summary["pull_requests"].values())
        self.assertEqual(allocated + summary["periodic_unallocated"]["known_runner_minutes"]
                         + summary["unattributed"]["known_runner_minutes"], 33)
        self.assertFalse(summary["pull_requests"]["11"]["lifecycle_complete"])
        self.assertIsNone(summary["runner_models"]["fixed_capacity"]["monetary_cost"])

    def test_success_sample_uses_attempt_start_and_preserves_unknown_phases(self):
        summary = report.summarize(self.snapshot)
        self.assertEqual(summary["successful_code_pr"]["runner_sum"]["median_minutes"], 10)
        self.assertEqual(summary["successful_code_pr"]["feedback"]["median_minutes"], 11)
        self.assertEqual(summary["unknowns"]["runner_minutes"], 1)
        self.assertEqual(summary["unknowns"]["queue_minutes"], 2)
        self.assertEqual(summary["unknowns"]["phase_breakdown_jobs"], 6)
        self.assertIsNone(summary["job_observations"][1]["phases_minutes"]["compile"])

    def test_unknown_group_membership_stays_unallocated(self):
        self.snapshot["attributions"] = {}
        self.snapshot["runs"][2]["head_branch"] = "gh-readonly-queue/main/pr-11-fixture"
        summary = report.summarize(self.snapshot)
        self.assertEqual(summary["unattributed"]["known_runner_minutes"], 6)
        self.assertNotIn("12", summary["pull_requests"])

    def test_foreign_repository_pr_numbers_cannot_be_attributed_locally(self):
        run = self.snapshot["runs"][0]
        run["pull_requests"].append({"number": 12, "url": "https://api.github.com/repos/foreign/repo/pulls/12"})
        self.assertEqual(report.repository_members(run, "owner/repo"), [11])
        self.assertEqual(report.attribution(run, self.snapshot), ([11], "event_pull_requests"))
        run["pull_requests"] = [{"number": 11, "url": "https://api.github.com/repos/foreign/repo/pulls/11"}]
        self.assertEqual(report.attribution(run, self.snapshot), ([], "unattributed"))

    def test_manifest_is_bound_to_run_sha_and_cannot_allocate_periodic_work(self):
        row = copy.deepcopy(self.snapshot["attributions"]["102"])
        manifest = {"schema": 1, "repository": "owner/repo", "runs": [row]}
        report.add_attributions(self.snapshot, manifest)
        for changes in ({"head_sha": "d" * 40}, {"pull_requests": [11, 11]},
                        {"run_id": 103, "head_sha": "c" * 40}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                report.add_attributions(self.snapshot, {**manifest, "runs": [{**row, **changes}]})

    def test_duplicate_attempt_or_job_is_rejected(self):
        for duplicate_attempt in (True, False):
            snapshot = copy.deepcopy(self.snapshot)
            if duplicate_attempt:
                snapshot["runs"].append(copy.deepcopy(snapshot["runs"][0]))
            else:
                snapshot["runs"][0]["jobs"] *= 2
            with self.assertRaises(ValueError):
                report.summarize(snapshot)
        self.snapshot["runs"][1]["jobs"][0]["id"] = self.snapshot["runs"][0]["jobs"][0]["id"]
        with self.assertRaisesRegex(ValueError, "duplicate job"):
            report.summarize(self.snapshot)

    def test_pagination_requires_complete_unique_inventory(self):
        first = [{"id": number} for number in range(100)]
        with patch.object(report, "api", side_effect=[{"total_count": 101, "jobs": first}, {"total_count": 101, "jobs": [{"id": 100}]}]) as api:
            self.assertEqual(len(report.pages("fixture", "jobs")), 101)
            self.assertIn("page=2", api.call_args.args[0])
        for second in ({"total_count": 101, "jobs": []}, {"total_count": 101, "jobs": [{"id": 0}]},
                       {"total_count": 102, "jobs": [{"id": 100}]}):
            with patch.object(report, "api", side_effect=[{"total_count": 101, "jobs": first}, second]), self.assertRaises(ValueError):
                report.pages("fixture", "jobs")

    def collect_fixture(self, *, missing_attempt=False, changed=False):
        attempts = self.snapshot["runs"][:2]
        latest = {key: value for key, value in attempts[1].items() if key != "jobs"}
        calls = []
        def api(path):
            calls.append(path)
            clean = urlsplit(path).path
            query = parse_qs(urlsplit(path).query)
            if clean.endswith("/actions/runs"):
                self.assertNotIn("event", query)
                self.assertNotIn("status", query)
                return {"workflow_runs": [latest], "total_count": 1}
            if clean.endswith("/pulls/11"):
                return self.snapshot["pull_requests"]["11"]
            if clean.endswith("/attempts/1") and missing_attempt:
                raise RuntimeError("expired")
            if "/attempts/" in clean:
                attempt = int(clean.split("/attempts/")[1].split("/")[0])
                if clean.endswith("/jobs"):
                    return {"jobs": attempts[attempt - 1]["jobs"], "total_count": 1}
                return attempts[attempt - 1]
            return {**latest, "run_attempt": 3} if changed else latest
        with patch.object(report, "api", side_effect=api):
            snapshot = report.collect("owner/repo", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")
        return snapshot, calls

    def test_collector_fetches_each_attempt_and_retains_api_failures(self):
        snapshot, calls = self.collect_fixture()
        self.assertTrue(snapshot["complete"])
        self.assertEqual([run["run_attempt"] for run in snapshot["runs"]], [1, 2])
        self.assertTrue(any("/attempts/1/jobs?" in call for call in calls))
        self.assertTrue(any("/attempts/2/jobs?" in call for call in calls))
        partial, _ = self.collect_fixture(missing_attempt=True)
        self.assertFalse(partial["complete"])
        self.assertEqual(len(partial["runs"]), 2)
        self.assertFalse(partial["runs"][0]["complete"])
        self.assertTrue(partial["runs"][1]["complete"])
        self.assertEqual(partial["issues"][0]["scope"], "attempt")

    def test_changed_latest_attempt_and_capped_inventory_are_incomplete(self):
        snapshot, _ = self.collect_fixture(changed=True)
        self.assertFalse(snapshot["complete"])
        with patch.object(report, "api", return_value={"total_count": 1001, "workflow_runs": []}):
            snapshot = report.collect("owner/repo", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")
        self.assertFalse(snapshot["complete"])
        self.assertEqual(snapshot["issues"][0]["scope"], "run_inventory")


if __name__ == "__main__":
    unittest.main()
