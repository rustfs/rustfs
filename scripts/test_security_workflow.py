#!/usr/bin/env python3
"""Run the security workflow's evidence and result steps without remote VMs."""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

from check_test_wiring import yaml_block


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/rustfs-security-test.yml"
CASE_ROW = "| IAM-101 | user CRUD lifecycle | PASS |"


class SecurityWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = WORKFLOW.read_text()
        self.job = yaml_block(self.source.splitlines(), "security-test", 2)
        self.assertIsNotNone(self.job)
        starts = [i for i, line in enumerate(self.job) if line.startswith("      - name: ")]
        self.steps = {
            self.job[start].split(": ", 1)[1].strip('"'): self.job[start:end]
            for start, end in zip(starts, starts[1:] + [len(self.job)])
        }
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.context = {
            "runner.temp": self.temp.name,
            "github.server_url": "https://github.com",
            "github.repository": "rustfs/rustfs",
            "github.run_id": "314159",
            "github.run_attempt": "2",
            "github.sha": "0123456789abcdef0123456789abcdef01234567",
            "github.event_name": "workflow_dispatch",
            "github.workspace": self.temp.name,
            "inputs.package_url": "",
            "inputs.rustfs_version": "test-version",
            "inputs.topology": "all",
            "inputs.oidc_live": "false",
            "steps.evidence.outcome": "skipped",
            "steps.test.outcome": "skipped",
            "steps.report.outcome": "skipped",
        }
        self.env = {
            **os.environ, "GITHUB_STEP_SUMMARY": str(self.directory / "summary.md"),
            "GITHUB_ENV": str(self.directory / "github-env"), "RUNNER_TEMP": self.temp.name, "TMPDIR": self.temp.name,
        }
        for key in ("server_url", "repository", "run_id", "run_attempt", "sha", "event_name"):
            self.env[f"GITHUB_{key.upper()}"] = self.context[f"github.{key}"]
        self.context["env.SECURITY_ARTIFACTS_DIR"] = ""
        self.artifacts = self.directory / "rustfs-security-314159-2"
        suite = self.directory / "auto-testing/rustfs-security-test.sh"
        suite.parent.mkdir()
        suite.write_text(
            '#!/usr/bin/env bash\nset -euo pipefail\n'
            'log_dir=$(mktemp -d "$TMPDIR/rustfs-security.XXXXXX")\n'
            'echo "CURRENT SUITE LOG" > "$log_dir/suite.log"\n'
            'case "$FAKE_REPORT" in\n'
            f'  present) printf "%s\\n" "CURRENT SUITE DIAGNOSTIC" "{CASE_ROW}" > "$REPORT_FILE" ;;\n'
            '  empty) : > "$REPORT_FILE" ;;\n'
            'esac\n'
            'echo "UNWRAPPED SUITE SUMMARY" >> "$GITHUB_STEP_SUMMARY"\n'
            'exit "$FAKE_EXIT"\n'
        )

    def render(self, value: str) -> str:
        return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda match: self.context[match[1]], value)

    def step_env(self, lines: list[str], indent: int = 8) -> dict[str, str]:
        result = {}
        for line in yaml_block(lines, "env", indent) or []:
            if line.strip() and not line.lstrip().startswith("#"):
                key, value = line.strip().split(": ", 1)
                result[key] = self.render(value.strip("'\""))
        return result

    def run_step(self, name: str) -> subprocess.CompletedProcess[str]:
        lines = self.steps[name]
        start = lines.index("        run: |") + 1
        shell_lines = []
        for line in lines[start:]:
            if line.strip() and not line.startswith("          "):
                break
            shell_lines.append(line[10:])
        self.assertTrue(shell_lines, f"missing literal shell body: {name}")
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", self.render("\n".join(shell_lines))],
            cwd=self.directory, env={**self.env, **self.step_env(lines)}, capture_output=True, text=True,
        )
        for line in lines:
            if line.startswith("        id: "):
                self.context[f"steps.{line.split(': ', 1)[1]}.outcome"] = "failure" if result.returncode else "success"
        if Path(self.env["GITHUB_ENV"]).exists():
            for line in Path(self.env["GITHUB_ENV"]).read_text().splitlines():
                key, value = line.split("=", 1)
                self.env[key] = value
                self.context[f"env.{key}"] = value
        return result

    def test_workflow_wiring(self) -> None:
        names = list(self.steps)
        self.assertLess(names.index("Checkout repository (for the OIDC live gate script)"), names.index("Checkout auto-testing scripts (with retry)"))
        self.assertNotIn("    continue-on-error: true", self.job)
        self.assertIn("        continue-on-error: true", self.steps["Run security suite"])
        for name in ("Initialize security evidence", "Generate report"):
            self.assertNotIn("        continue-on-error: true", self.steps[name])
        self.assertIn("        if: ${{ always() && steps.evidence.outcome == 'success' }}", self.steps["Generate report"])
        self.assertNotIn("/tmp/rustfs-security", self.source)
        for name in ("Upload functional report to dashboard", "File failure issue in rustfs/backlog"):
            report = next(line for line in self.steps[name] if line.strip().startswith("REPORT_FILE:"))
            self.assertIn("${{ env.SECURITY_ARTIFACTS_DIR }}/report.md", report)
        for name in ("Upload functional report to dashboard", "Upload report and logs"):
            self.assertIn("        if: ${{ always() && steps.evidence.outcome == 'success' }}", self.steps[name])
        artifact_settings = yaml_block(self.steps["Upload report and logs"], "with", 8)
        self.assertIn("          path: ${{ env.SECURITY_ARTIFACTS_DIR }}/", artifact_settings)
        self.assertIn("          if-no-files-found: error", artifact_settings)

    def test_suite_report_and_result_matrix(self) -> None:
        for outcome, mode, exit_code in (
            ("success", "present", 0), ("failure", "present", 7), ("failure", "missing", 7),
            ("success", "missing", 0), ("success", "empty", 0),
            ("skipped", "missing", 0), ("skipped", "present", 0),
            ("cancelled", "missing", 0), ("cancelled", "present", 0),
        ):
            with self.subTest(outcome=outcome, report=mode):
                self.setUp()
                initialized = self.run_step("Initialize security evidence")
                self.assertEqual(initialized.returncode, 0, initialized.stderr)
                self.assertEqual(self.env["SECURITY_ARTIFACTS_DIR"], str(self.artifacts))
                self.env.update(FAKE_REPORT=mode, FAKE_EXIT=str(exit_code))
                if outcome != "skipped" or mode == "present":
                    suite = self.run_step("Run security suite")
                    self.assertEqual(suite.returncode, exit_code, suite.stderr)
                    logs = list(self.artifacts.glob("rustfs-security.*/suite.log"))
                    self.assertEqual(len(logs), 1)
                    self.assertEqual(logs[0].read_text(), "CURRENT SUITE LOG\n")
                self.context["steps.test.outcome"] = outcome
                report = self.run_step("Generate report")
                success = outcome == "success" and mode == "present"
                self.assertEqual(report.returncode == 0, success, report.stderr)
                contents = (self.artifacts / "report.md").read_text()
                for expected in (
                    "https://github.com/rustfs/rustfs/actions/runs/314159", "Attempt: 2",
                    f"Workflow Commit: {self.context['github.sha']}", "Trigger: workflow_dispatch",
                    f"Test Step Outcome: {'success' if success else 'failure'}", f"Suite Step Outcome: {outcome}",
                ):
                    self.assertIn(expected, contents)
                self.assertEqual(CASE_ROW in contents, success)
                self.assertEqual("CURRENT SUITE DIAGNOSTIC" in contents, success)
                if mode == "present":
                    raw = (self.artifacts / "suite-report.md").read_text()
                    self.assertEqual(raw, f"CURRENT SUITE DIAGNOSTIC\n{CASE_ROW}\n")
                summary = Path(self.env["GITHUB_STEP_SUMMARY"]).read_text()
                self.assertEqual(summary, contents)
                self.assertNotIn("UNWRAPPED SUITE SUMMARY", summary)

    def test_existing_evidence_directory_is_rejected(self) -> None:
        self.artifacts.mkdir()
        stale = self.artifacts / "suite-report.md"
        stale.write_text("OLD RUN REPORT")
        self.assertNotEqual(self.run_step("Initialize security evidence").returncode, 0)
        self.assertEqual(stale.read_text(), "OLD RUN REPORT")
        self.assertFalse(Path(self.env["GITHUB_ENV"]).exists())
        (self.artifacts / "report.md").write_text("OLD RUN REPORT")
        self.context.update({
            "env.SECURITY_ARTIFACTS_DIR": str(self.artifacts), "secrets.PF_TESTING_GH_TOKEN": "fake-local-token",
        })
        fake_bin = self.directory / "bin"
        fake_bin.mkdir()
        gh = fake_bin / "gh"
        gh.write_text(
            '#!/usr/bin/env bash\nset -euo pipefail\n'
            'if [ "$1 $2" = "issue create" ]; then\n'
            '  while [ "$#" -gt 0 ]; do\n'
            '    if [ "$1" = "--body-file" ]; then cat "$2" > "$CAPTURE_BODY"; fi\n'
            '    shift\n'
            '  done\n'
            'fi\n'
        )
        gh.chmod(0o755)
        body = self.directory / "issue-body.md"
        self.env.update(PATH=f"{fake_bin}{os.pathsep}{os.environ['PATH']}", CAPTURE_BODY=str(body))
        result = self.run_step("File failure issue in rustfs/backlog")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("OLD RUN REPORT", body.read_text())
        self.assertIn("https://github.com/rustfs/rustfs/actions/runs/314159", body.read_text())


if __name__ == "__main__":
    unittest.main()
