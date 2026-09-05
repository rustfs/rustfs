#!/usr/bin/env python3
"""Exercise functional chain dispatch and security evidence without remote VMs."""

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


def named_steps(job: list[str]) -> dict[str, list[str]]:
    starts = [i for i, line in enumerate(job) if line.startswith("      - name: ")]
    return {
        job[start].split(": ", 1)[1].strip('"'): job[start:end]
        for start, end in zip(starts, starts[1:] + [len(job)])
    }


class SecurityWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = WORKFLOW.read_text()
        self.job = yaml_block(self.source.splitlines(), "security-test", 2)
        self.assertIsNotNone(self.job)
        self.steps = named_steps(self.job)
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

    def test_all_ten_suites_hold_the_shared_lock_for_manual_and_chain_runs(self) -> None:
        for suite in ("upgrade", "s3-compat", "kms", "tier", "storage", "heal", "pool-expand", "security", "replication", "performance"):
            with self.subTest(suite=suite):
                source = (ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text().splitlines()
                # Workflow-level concurrency covers every job, including cleanup,
                # regardless of trigger or the runner hosting the job.
                self.assertEqual([
                    line.strip() for line in yaml_block(source, "concurrency", 0)
                    if line.strip() and not line.lstrip().startswith("#")
                ], [
                    "group: rustfs-shared-functional-tests", "cancel-in-progress: false",
                ])
                self.assertIsNotNone(yaml_block(source, "workflow_dispatch", 2))
                self.assertIsNotNone(yaml_block(source, "repository_dispatch", 2))
                cleanup_name = "Reset test environment (after)" if suite == "performance" else "Cleanup environment (after)"
                cleanup = named_steps(yaml_block(source, "jobs", 0))[cleanup_name]
                self.assertTrue(any(line.startswith("        if:") and "always()" in line for line in cleanup))

    def test_root_dispatches_only_upgrade_and_replication_hands_off_after_failure(self) -> None:
        for failed_attempts, issue_exit, token in ((0, 0, "fixture"), (2, 0, "fixture"), (3, 0, "fixture"), (3, 7, "fixture"), (0, 0, "")):
            with self.subTest(failed_attempts=failed_attempts, issue_exit=issue_exit, token=bool(token)):
                self.setUp()
                fake_bin = self.directory / "bin"
                fake_bin.mkdir()
                commands = {
                    "gh": '''#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = api ]; then
  printf '%s\\n' "$*" >> "$DISPATCHES"
  attempt=$(wc -l < "$DISPATCHES")
  [ "$attempt" -gt "$FAILED_ATTEMPTS" ]
elif [ "$1 $2" = 'issue create' ]; then
  printf 'issue\\n' >> "$EXECUTED"
  while [ "$#" -gt 0 ]; do
    if [ "$1" = --body-file ]; then
      cat "$2" > "$CAPTURE_BODY"
      printf '%s\\n' "$2" > "$CAPTURE_BODY_PATH"
    fi
    shift
  done
  exit "$ISSUE_EXIT"
else
  exit 99
fi
''',
                    "sleep": '#!/bin/sh\nprintf "sleep %s\\n" "$1" >> "$EXECUTED"\n',
                    "ssh": '#!/bin/sh\nprintf "cleanup\\n" >> "$EXECUTED"\n',
                }
                for name, contents in commands.items():
                    command = fake_bin / name
                    command.write_text(contents)
                    command.chmod(0o755)
                dispatches = self.directory / "dispatches"
                executed = self.directory / "executed"
                body = self.directory / "issue-body.md"
                body_path = self.directory / "issue-body-path"
                self.env.update(
                    PATH=f"{fake_bin}{os.pathsep}{os.environ['PATH']}", DISPATCHES=str(dispatches),
                    EXECUTED=str(executed), CAPTURE_BODY=str(body), CAPTURE_BODY_PATH=str(body_path),
                    FAILED_ATTEMPTS="0", ISSUE_EXIT=str(issue_exit),
                    RUSTFS_NODES="fixture-node", RUSTFS_SSH_USER="fixture-user",
                    RUSTFS_NIGHTLY_PACKAGE_URL="https://example.invalid/package.deb",
                )
                self.context.update({"secrets.PF_TESTING_GH_TOKEN": "fixture", "inputs.suite": "all"})
                driver = (ROOT / ".github/workflows/rustfs-functional-chain.yml").read_text()
                self.steps = named_steps(yaml_block(driver.splitlines(), "start-chain", 2))
                self.assertEqual(list(self.steps), ["Dispatch first suite (upgrade)"])
                started = self.run_step("Dispatch first suite (upgrade)")
                self.assertEqual(started.returncode, 0, started.stderr)
                self.assertEqual(dispatches.read_text().splitlines(), [
                    "api --method POST repos/rustfs/rustfs/dispatches -f event_type=rustfs-chain-upgrade -F client_payload[from_suite]=nightly-build",
                ])
                dispatches.unlink()

                replication = (ROOT / ".github/workflows/rustfs-replication-test.yml").read_text()
                job = yaml_block(replication.splitlines(), "replication-test", 2)
                self.assertFalse(any(line.startswith("    continue-on-error:") for line in job))
                self.steps = named_steps(job)
                handoff = "Continue functional chain (next: Performance)"
                self.assertIn("        if: ${{ always() && github.event_name == 'repository_dispatch' }}", self.steps[handoff])
                self.assertFalse(any(line.strip().startswith("continue-on-error:") for line in self.steps[handoff]))
                self.assertIn("        if: always()", self.steps["Cleanup environment (after)"])
                self.assertLess(list(self.steps).index("Cleanup environment (after)"), list(self.steps).index(handoff))
                suite = self.directory / "auto-testing/rustfs-replication-test.sh"
                suite.write_text('#!/bin/sh\nprintf "suite failed\\n" >> "$EXECUTED"\nexit 17\n')
                failed = self.run_step("Run replication suite")
                self.assertEqual(failed.returncode, 17, failed.stderr)
                cleaned = self.run_step("Cleanup environment (after)")
                self.assertEqual(cleaned.returncode, 0, cleaned.stderr)
                self.assertEqual(executed.read_text().splitlines(), ["suite failed", "cleanup"])
                self.env["FAILED_ATTEMPTS"] = str(failed_attempts)
                self.context["secrets.PF_TESTING_GH_TOKEN"] = token
                forwarded = self.run_step(handoff)
                self.assertEqual(forwarded.returncode == 0, bool(token) and failed_attempts < 3, forwarded.stderr)
                calls = dispatches.read_text().splitlines() if dispatches.exists() else []
                self.assertEqual(calls, [
                    "api --method POST repos/rustfs/rustfs/dispatches -f event_type=rustfs-chain-performance -F client_payload[from_suite]=replication",
                ] * (min(failed_attempts + 1, 3) if token else 0))
                if failed_attempts == 3:
                    self.assertIn("could not hand off from **replication** to **Performance**", body.read_text())
                    self.assertIn("rustfs-chain-performance", body.read_text())
                    self.assertEqual(executed.read_text().splitlines().count("issue"), 2 if issue_exit else 1)
                    self.assertFalse(Path(body_path.read_text().strip()).exists())


if __name__ == "__main__":
    unittest.main()
