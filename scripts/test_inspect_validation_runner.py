"""Runner diagnostics must remain read-only when the shared fleet is unavailable."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import inspect_validation_runner as inspection
from ci_gate import workflow_document


class RunnerInspectionTests(unittest.TestCase):
    def request(self):
        return {"validation_runner_inspect_only": True, "validation_case": "",
                "validation_tools_only": False, "i05_diagnostic_branch": "source-plan",
                "validation_tools_run_attempt": "1"}

    def test_workflow_default_allows_inspection_request(self):
        workflow = Path(__file__).resolve().parents[1] / ".github/workflows/rustfs-functional-chain.yml"
        definitions = workflow_document(workflow.read_text())['on']['workflow_dispatch']['inputs']
        defaults = {key: value['default'] == 'true' if value.get('type') == 'boolean' else value['default']
                    for key, value in definitions.items() if 'default' in value}
        inspection.validate_request({**defaults, **self.request()})

    def test_exclusive_request_rejects_every_execution_mode_before_api_access(self):
        inspection.validate_request(self.request())
        for key, value in [("validation_runner_inspect_only", False),
                           ("validation_case", "environment.inspect"), ("build_run_id", "123"),
                           ("validation_package_sha", "a" * 40), ("validation_tools_only", True),
                           ("validation_native_tools", True), ("validation_shared_tools", True),
                           ("validation_manager_tools", True), ("validation_cleanup_run_id", "123"),
                           ("auto_testing_sha", "a" * 40), ("validation_tools_run_id", "123"),
                           ("i05_diagnostic_branch", "queued-successor"),
                           ("i05_diagnostic_branch", "timeout"),
                           ("validation_plan", "weekly"), ("suites", "s3"),
                           ("validation_tools_run_attempt", "2")]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                inspection.validate_request({**self.request(), key: value})

    def fixture(self, runners, *, unknown_jobs=False):
        def api(path):
            self.assertTrue(path.startswith("repos/rustfs/rustfs/actions/"))
            if "/runners?" in path:
                return {"runners": runners}
            if "/runs?status=queued&" in path:
                return {"workflow_runs": [{"id": 123, "name": "RustFS Functional Chain",
                    "status": "queued", "created_at": "2026-10-26T00:00:00Z", "html_url": "run-url"}]}
            if "/runs?status=" in path:
                return {"workflow_runs": []}
            self.assertIn("/runs/123/jobs?", path)
            return {"jobs": [] if unknown_jobs else [
                {"id": 456, "name": "validation", "status": "queued", "runner_name": "",
                 "labels": ["smoke-testing"], "html_url": "job-url", "steps": [{"secret": "excluded"}]},
                {"id": 457, "name": "native", "status": "in_progress", "labels": ["ubuntu-latest"]},
                {"id": 458, "name": "prior", "status": "completed", "labels": ["smoke-testing"]}]}
        return api

    def test_offline_idle_busy_and_unregistered_are_distinct_with_only_fleet_jobs(self):
        runner = {"id": 7, "name": "rustfs-smoke-testing", "status": "offline", "busy": False,
                  "labels": [{"name": "smoke-testing"}], "extra": {"secret": "excluded"}}
        for state, busy, expected in [("offline", False, "offline"), ("online", True, "online-busy"),
                                       ("online", False, "online-idle"), (None, False, "unregistered")]:
            with self.subTest(expected=expected):
                rows = [{**runner, "status": state, "busy": busy}] if state else []
                report = {}
                with patch.object(inspection, "api", side_effect=self.fixture(rows)):
                    inspection.collect(report)
                self.assertEqual(report["runnerAvailability"], expected)
                self.assertTrue(report["complete"])
                self.assertEqual([row["jobId"] for row in report["fleetJobs"]], [456])
                self.assertNotIn("secret", json.dumps(report))

    def test_jobless_workflow_is_retained_without_inventing_a_runner_assignment(self):
        report = {}
        with patch.object(inspection, "api", side_effect=self.fixture([], unknown_jobs=True)):
            inspection.collect(report)
        self.assertEqual(report["fleetJobs"], [])
        self.assertEqual(report["runsWithoutJobs"][0]["runId"], 123)

    def test_inventory_pagination_is_bounded_and_cannot_claim_complete_when_truncated(self):
        row = {"id": 7}
        with patch.object(inspection, "api", return_value={"runners": [row] * 100}) as api:
            with self.assertRaisesRegex(ValueError, "bounded inspection"):
                inspection.pages("repos/rustfs/rustfs/actions/runners", "runners")
        self.assertEqual(api.call_count, 10)

    def test_api_failure_keeps_partial_report_without_subprocess_diagnostics_or_fleet_claims(self):
        for inputs, expected_calls in [(self.request(), 1), ({**self.request(), "validation_tools_only": True}, 0)]:
            with self.subTest(inputs=inputs), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "report.json"
                with patch.dict(os.environ, RUNNER_INSPECTION_INPUTS=json.dumps(inputs)), \
                     patch.object(inspection.sys, "argv", ["inspect", "--output", str(output)]), \
                     patch.object(inspection, "api", side_effect=subprocess.CalledProcessError(
                         1, ["gh", "api"], stderr=b"secret-token")) as api:
                    self.assertEqual(inspection.main(), 1)
                report = json.loads(output.read_text())
                self.assertFalse(report["complete"])
                for field in ("mutated", "fleetAccessed", "cleanStateCertified",
                              "sharedWorkflowIdleCertified", "canonicalCasePassClaimed"):
                    self.assertIs(report[field], False)
                self.assertNotIn("secret-token", output.read_text())
                self.assertEqual(api.call_count, expected_calls)

    def test_workflow_is_hosted_and_uses_a_separate_concurrency_group(self):
        workflow = Path(__file__).resolve().parents[1] / ".github/workflows/rustfs-functional-chain.yml"
        source = workflow.read_text()
        job = source.split("  validation-runner-inspection:\n", 1)[1].split("  validation-package-request:", 1)[0]
        self.assertIn("runs-on: ubuntu-latest", job)
        self.assertIn("timeout-minutes: 10", job)
        self.assertIn("RUNNER_INSPECTION_INPUTS: ${{ toJSON(inputs) }}", job)
        self.assertIn("if: ${{ always() }}", job)
        self.assertNotIn("RUSTFS_SECRET_KEY", job)
        concurrency = source.split("concurrency:\n", 1)[1].split("jobs:\n", 1)[0]
        self.assertLess(concurrency.index("validation_runner_inspect_only"), concurrency.index("validation_package_sha"))
        self.assertIn("rustfs-functional-validation-runner-inspection", concurrency)
        for name in ("validation-package-request", "validation-package", "prepare", "validation"):
            job = source.split("  " + name + ":\n", 1)[1].split("\n    runs-on:", 1)[0].split("\n    uses:", 1)[0]
            self.assertIn("!inputs.validation_runner_inspect_only", job)


if __name__ == "__main__":
    unittest.main()
