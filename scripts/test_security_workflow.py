#!/usr/bin/env python3
"""Exercise functional workflow failures and security evidence without remote VMs."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from check_test_wiring import yaml_block
from functional_case_report import generate_report


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/rustfs-security-test.yml"
CASE_ROW = "| IAM-101 | user CRUD lifecycle | PASS |"


def named_steps(job: list[str]) -> dict[str, list[str]]:
    starts = [i for i, line in enumerate(job) if line.startswith("      - name: ")]
    return {
        job[start].split(": ", 1)[1].strip('"'): job[start:end]
        for start, end in zip(starts, starts[1:] + [len(job)])
    }


def shell_body(lines: list[str]) -> str:
    start = lines.index("        run: |") + 1
    shell_lines = []
    for line in lines[start:]:
        if line.strip() and not line.startswith("          "):
            break
        shell_lines.append(line[10:])
    if not shell_lines:
        raise ValueError("missing literal shell body")
    return "\n".join(shell_lines)


class WorkflowSteps:
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
        result = subprocess.run(
            ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", self.render(shell_body(lines))],
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


class SecurityWorkflowTests(WorkflowSteps, unittest.TestCase):
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


class FunctionalWorkflowTests(unittest.TestCase):
    JOBS = {
        "kms": "kms-test", "storage": "storage-test", "s3-compat": "s3-compat-test",
        "upgrade": "upgrade-test", "replication": "replication-test", "heal": "heal-test",
        "tier": "tier-test", "pool-expand": "pool-expansion-test", "performance": "performance-test",
    }
    DIRECT_TESTS = {
        "kms": "Run KMS suite", "storage": "Run storage engine suite",
        "s3-compat": "Run S3 compatibility suite", "upgrade": "Run upgrade compatibility suite",
        "replication": "Run replication suite",
    }

    def test_failure_and_always_step_wiring(self) -> None:
        for suite, job_id in self.JOBS.items():
            with self.subTest(suite=suite):
                source = (ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text()
                job = yaml_block(source.splitlines(), job_id, 2)
                self.assertIsNotNone(job)
                self.assertNotRegex("\n".join(job), r'''(?m)^    ["']?continue-on-error["']?\s*:''')
                steps = named_steps(job)
                if suite in self.DIRECT_TESTS:
                    test = steps[self.DIRECT_TESTS[suite]]
                    self.assertNotRegex("\n".join(test), r'''(?m)^        ["']?continue-on-error["']?\s*:''')
                    self.assertIn("        if: ${{ always() && steps.evidence.outcome == 'success' }}", steps["Generate report"])
                cleanup = steps["Reset test environment (after)" if suite == "performance" else "Cleanup environment (after)"]
                condition = next(line.strip() for line in cleanup if line.startswith("        if:"))
                self.assertIn(condition, (
                    "if: always()",
                    "if: ${{ always() && inputs.cleanup_after != 'false' }}",
                    "if: ${{ always() && (inputs.cleanup_after != 'false' || github.event_name != 'workflow_dispatch') }}",
                ))
                if suite != "performance":
                    handoff = steps["Chain complete"] if suite == "replication" else next(
                        value for name, value in steps.items() if name.startswith("Continue functional chain")
                    )
                    self.assertIn("        if: ${{ always() && github.event_name == 'repository_dispatch' }}", handoff)

    def test_failed_suite_preserves_exit_and_cleanup_and_dispatch_execute(self) -> None:
        for suite, test_name in self.DIRECT_TESTS.items():
            with self.subTest(suite=suite), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "auto-testing").mkdir()
                script = root / f"auto-testing/rustfs-{suite}-test.sh"
                script.write_text('#!/bin/sh\nprintf "partial suite diagnostics\\n"\nexit 17\n')
                script.chmod(0o755)
                fake_bin = root / "bin"
                fake_bin.mkdir()
                for command, marker in (("ssh", "cleanup"), ("gh", "dispatch")):
                    fake = fake_bin / command
                    fake.write_text(f'#!/bin/sh\nprintf "{marker}\\n" >> "$EXECUTED"\n')
                    fake.chmod(0o755)
                env = {
                    **os.environ, "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
                    "EXECUTED": str(root / "executed"), "RUSTFS_NODES": "fixture-node",
                    "RUSTFS_SSH_USER": "fixture-user", "RUSTFS_NIGHTLY_PACKAGE_URL": "https://example.invalid/package.deb",
                    "GH_TOKEN": "local-fixture", "GITHUB_EVENT_NAME": "repository_dispatch", "GITHUB_RUN_ID": "314159",
                }
                source = (ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text()
                steps = named_steps(yaml_block(source.splitlines(), self.JOBS[suite], 2))
                context = {"github.event_name": "repository_dispatch", "steps.test.outcome": "failure"}
                for expression in re.findall(r"\$\{\{\s*(.*?)\s*\}\}", source):
                    if expression.startswith("inputs.") and re.fullmatch(r"inputs\.\w+", expression):
                        context[expression] = ""
                def execute(name):
                    lines = steps[name]
                    rendered = re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda match: context[match[1]], shell_body(lines))
                    return subprocess.run(
                        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", rendered],
                        cwd=root, env={**env, "LOG_FILE": str(root / "suite.log")}, capture_output=True, text=True,
                    )
                failed = execute(test_name)
                self.assertEqual(failed.returncode, 17, failed.stderr)
                self.assertIn("partial suite diagnostics", failed.stdout)
                cleanup = execute("Cleanup environment (after)")
                self.assertEqual(cleanup.returncode, 0, cleanup.stderr)
                handoff_name = "Chain complete" if suite == "replication" else next(
                    name for name in steps if name.startswith("Continue functional chain")
                )
                handoff = execute(handoff_name)
                self.assertEqual(handoff.returncode, 0, handoff.stderr)
                markers = (root / "executed").read_text().splitlines()
                self.assertEqual(markers, ["cleanup"] if suite == "replication" else ["cleanup", "dispatch"])


class FunctionalCaseReportTests(unittest.TestCase):
    def report(self, text: str | None, matrix: bool = False) -> tuple[bool, str, str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "suite.log"
            if text is not None:
                log.write_text(text)
            valid = generate_report(log, root / "cases.md", root / "matrix.md" if matrix else None)
            return valid, (root / "cases.md").read_text(), (root / "matrix.md").read_text() if matrix else ""

    def test_repeated_case_executions_preserve_failure_and_context(self):
        # log() from rustfs/auto-testing@6120aa0a76de, rustfs-kms-test.sh:131.
        log = subprocess.check_output(["bash", "-c", r'''
log() { printf '\033[1;36m[INFO]\033[0m %s\n' "$*"; }
log '== topology: single-single  kms-backend: local =='
printf '\033[32m--- KMS-101 roundtrip ---\033[0m\n[FAIL] KMS-101\n'
log '== topology: single-multi  kms-backend: vault-kv2 =='
printf '%s\n' '--- KMS-101 roundtrip ---' '[PASS] KMS-101'
printf '%s\n' '--- KMS-101 roundtrip ---' '[UNSUPPORTED] KMS-101'
'''], text=True)
        valid, cases, _ = self.report(log)
        self.assertFalse(valid)
        self.assertEqual(cases.count("| KMS-101 |"), 3)
        self.assertIn("- Total: 3\n- PASS: 1\n- FAIL: 1\n- UNSUPPORTED: 1\n- RUNNING: 0\n", cases)
        self.assertIn("roundtrip (topology: single-single  kms-backend: local) | FAIL |", cases)
        self.assertIn("roundtrip (topology: single-multi  kms-backend: vault-kv2) | PASS |", cases)
        self.assertNotIn("\\n", cases)

    def test_missing_empty_unfinished_and_orphan_results_are_not_success(self):
        for text in (None, "", "setup failed\n", "--- KMS-101 roundtrip ---\n", "[PASS] KMS-101\n",
                     "--- KMS-101 first ---\n--- KMS-101 second ---\n[PASS] KMS-101\n",
                     "--- KMS-101 first ---\n[FAIL] KMS-101\n[PASS] KMS-101\n"):
            with self.subTest(log=text):
                valid, cases, _ = self.report(text)
                self.assertFalse(valid)
                self.assertIn("## Case Summary", cases)
        valid, cases, _ = self.report("[INFO] == suite: bucket replication (REP-*) ==\n--- REP-101 unsupported ---\n[UNSUPPORTED] REP-101\n")
        self.assertTrue(valid)
        self.assertIn("suite: bucket replication", cases)
        self.assertIn("- UNSUPPORTED: 1\n", cases)

    def test_upgrade_matrix_is_preserved_and_required_for_complete_report(self):
        case = "--- UPG-101 upgrade ---\n[PASS] UPG-101\n"
        for suffix, expected in (("", False), ("[UPG-TOPO] single-single local v1 v2 PASS=1 FAIL=0\n", True),
                                 ("[UPG-TOPO] single-single local v1 v2 PASS=1 FAIL=1\n", False)):
            with self.subTest(matrix=suffix):
                valid, _, matrix = self.report(case + suffix, matrix=True)
                self.assertEqual(valid, expected)
                self.assertIn("| Topology | KMS Backend | From Version | To Version | Result |", matrix)
                self.assertIn("| single-single | local | v1 | v2 |" if suffix else "NOT RUN", matrix)

    def test_s3_case_identifiers_include_digits(self):
        valid, cases, _ = self.report("--- S3C-101 CreateBucket ---\n[PASS] S3C-101\n")
        self.assertTrue(valid)
        self.assertIn("| S3C-101 | CreateBucket (context not recorded) | PASS |", cases)


class FunctionalEvidenceTests(WorkflowSteps, unittest.TestCase):
    SUITES = (*FunctionalWorkflowTests.DIRECT_TESTS, "heal", "performance")

    def prepare(self, suite: str) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.source = (ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text()
        self.steps = named_steps(yaml_block(self.source.splitlines(), FunctionalWorkflowTests.JOBS[suite], 2))
        self.context = {expression: "" for expression in re.findall(r"\$\{\{\s*(.*?)\s*\}\}", self.source)}
        self.context.update({
            "github.server_url": "https://github.com", "github.repository": "rustfs/rustfs",
            "github.run_id": "314159", "github.run_attempt": "2", "github.sha": "0123456789abcdef0123456789abcdef01234567",
            "github.event_name": "repository_dispatch", "steps.test.outcome": "success",
            "secrets.PF_TESTING_GH_TOKEN": "local-fixture", "env.PF_TESTING_GH_TOKEN": "local-fixture",
        })
        self.artifacts = self.directory / f"rustfs-{suite}-314159-2"
        self.env = {
            **os.environ, "GITHUB_ENV": str(self.directory / "github-env"), "RUNNER_TEMP": self.temp.name,
            "GITHUB_STEP_SUMMARY": str(self.directory / "summary.md"), "RUSTFS_NODES": "fixture-node",
            "RUSTFS_NIGHTLY_PACKAGE_URL": "https://example.invalid/package.deb", "CAPTURE_BODY": str(self.directory / "issue.md"),
        }
        for key in ("server_url", "repository", "run_id", "run_attempt", "sha", "event_name"):
            self.env[f"GITHUB_{key.upper()}"] = self.context[f"github.{key}"]
        (self.directory / "scripts").mkdir()
        (self.directory / "scripts/functional_case_report.py").symlink_to(ROOT / "scripts/functional_case_report.py")
        fake_bin = self.directory / "bin"
        fake_bin.mkdir()
        (fake_bin / "python3").symlink_to(sys.executable)
        for command, body in (
            ("ssh", 'printf "fixture-version\\n"\n'),
            ("gh", 'if [ "$1 $2" = "issue create" ]; then\n'
                   '  while [ "$#" -gt 0 ]; do\n'
                   '    if [ "$1" = "--body-file" ]; then cat "$2" > "$CAPTURE_BODY"; fi\n'
                   '    shift\n'
                   '  done\n'
                   'elif [ "$1 $2" = "api --method" ]; then cat >/dev/null; fi\n'),
        ):
            script = fake_bin / command
            script.write_text("#!/bin/sh\n" + body)
            script.chmod(0o755)
        self.env["PATH"] = f"{fake_bin}{os.pathsep}{os.environ['PATH']}"

    def test_evidence_wiring_and_failed_initialization_cannot_publish_stale_files(self):
        for suite in self.SUITES:
            with self.subTest(suite=suite):
                self.prepare(suite)
                self.assertNotIn("/tmp/rustfs-", self.source)
                names = list(self.steps)
                self.assertLess(names.index("Initialize functional evidence"), names.index("Checkout auto-testing scripts (with retry)"))
                if suite in FunctionalWorkflowTests.DIRECT_TESTS:
                    self.assertLess(names.index("Checkout repository (for report parser)"), names.index("Checkout auto-testing scripts (with retry)"))
                for name, lines in self.steps.items():
                    if name in ("Generate report", "Upload functional report to dashboard") or any("uses: actions/upload-artifact@" in line for line in lines):
                        self.assertIn("        if: ${{ always() && steps.evidence.outcome == 'success' }}", lines)
                    if any("uses: actions/upload-artifact@" in line for line in lines):
                        self.assertIn("          path: ${{ env.FUNCTIONAL_ARTIFACTS_DIR }}/", lines)
                        self.assertIn("          if-no-files-found: error", lines)
                self.artifacts.mkdir()
                for filename in ("report.md", "suite.log"):
                    (self.artifacts / filename).write_text("OLD RUN EVIDENCE")
                self.env.update(REPORT_FILE=str(self.artifacts / "report.md"), LOG_FILE=str(self.artifacts / "suite.log"))
                initialized = self.run_step("Initialize functional evidence")
                self.assertNotEqual(initialized.returncode, 0)
                self.assertFalse(Path(self.env["GITHUB_ENV"]).exists())
                issue = self.run_step("File failure issue in rustfs/backlog")
                self.assertEqual(issue.returncode, 0, issue.stderr)
                body = Path(self.env["CAPTURE_BODY"]).read_text()
                self.assertNotIn("OLD RUN EVIDENCE", body)
                self.assertIn("no report or log file was produced", body)
                self.assertEqual((self.artifacts / "report.md").read_text(), "OLD RUN EVIDENCE")

    def test_reports_use_only_current_complete_suite_evidence(self):
        for suite in self.SUITES[:-1]:
            good = "--- KMS-101 roundtrip ---\n[PASS] KMS-101\n"
            partial = "--- KMS-101 roundtrip ---\n[PASS] KMS-101\n--- KMS-102 unfinished ---\n"
            if suite == "s3-compat":
                good, partial = good.replace("KMS-", "S3C-"), partial.replace("KMS-", "S3C-")
            if suite == "upgrade":
                good += "[UPG-TOPO] single-single local v1 v2 PASS=1 FAIL=0\n"
            if suite == "heal":
                good = "".join(f"[HEAL-STEP] {step} fixture PASS\n" for step in range(1, 8))
                partial = "[HEAL-STEP] 1 fixture PASS\n"
            for outcome, log in (("success", good), ("failure", good), ("success", partial), ("success", ""),
                                 ("success", None), ("skipped", None), ("cancelled", good)):
                with self.subTest(suite=suite, outcome=outcome, log=log):
                    self.prepare(suite)
                    stale = self.directory / "old-suite.log"
                    stale.write_text("OLD RUN EVIDENCE\n" + good)
                    self.env.update(LOG_FILE=str(stale), REPORT_FILE=str(stale))
                    self.assertEqual(self.run_step("Initialize functional evidence").returncode, 0)
                    self.assertEqual(self.env["LOG_FILE"], str(self.artifacts / "suite.log"))
                    self.assertEqual(self.env["TMPDIR"], str(self.artifacts))
                    if log is not None:
                        Path(self.env["LOG_FILE"]).write_text(log)
                    self.context["steps.test.outcome"] = outcome
                    report = self.run_step("Generate report")
                    success = outcome == "success" and log == good
                    self.assertEqual(report.returncode == 0, success, report.stderr)
                    contents = Path(self.env["REPORT_FILE"]).read_text()
                    self.assertNotIn("OLD RUN EVIDENCE", contents)
                    self.assertEqual("| PASS |" in contents, success)
                    for value in ("actions/runs/314159", "Attempt: 2", "Workflow Commit: " + self.context["github.sha"],
                                  f"Test Step Outcome: {'success' if success else 'failure'}", f"Suite Step Outcome: {outcome}"):
                        self.assertIn(value, contents)
                    self.assertEqual(Path(self.env["GITHUB_STEP_SUMMARY"]).read_text(), contents)
                    evidence = (self.artifacts / ("steps.md" if suite == "heal" else "cases.md")).read_text()
                    if log in (good, partial):
                        self.assertIn("| PASS |", evidence)
                    self.assertNotIn("OLD RUN EVIDENCE", evidence)

    def test_actual_suite_commands_pass_the_current_log_and_scratch_paths(self):
        for suite in self.SUITES:
            with self.subTest(suite=suite):
                self.prepare(suite)
                self.assertEqual(self.run_step("Initialize functional evidence").returncode, 0)
                scripts = self.directory / "auto-testing"
                scripts.mkdir()
                filename = f"rustfs_{suite}_test.sh" if suite in ("heal", "performance") else f"rustfs-{suite}-test.sh"
                script = scripts / filename
                script.write_text(
                    '#!/bin/bash\nset -euo pipefail\nlog=""\n'
                    'while [ "$#" -gt 0 ]; do\n'
                    '  if [ "$1" = "--log-file" ]; then log="$2"; shift; fi\n'
                    '  shift\n'
                    'done\n'
                    '[ "$log" = "$LOG_FILE" ] || exit 31\n'
                    'printf "CURRENT SUITE LOG\\n" > "$log"\n'
                    'scratch=$(mktemp -d "$TMPDIR/fixture.XXXXXX")\n'
                    'printf "CURRENT SCRATCH\\n" > "$scratch/trace.log"\n'
                    'if [ -n "${RUSTFS_RESULT_DIR:-}" ]; then\n'
                    '  mkdir -p "$RUSTFS_RESULT_DIR"\n'
                    '  printf "CURRENT RESULTS\\n" > "$RUSTFS_RESULT_DIR/summary.md"\n'
                    'fi\n'
                )
                script.chmod(0o755)
                name = FunctionalWorkflowTests.DIRECT_TESTS.get(suite) or (
                    "Run benchmark (GET/PUT/MIXED)" if suite == "performance" else "Run heal test (write -> outage -> heal -> verify)"
                )
                result = self.run_step(name)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual((self.artifacts / "suite.log").read_text(), "CURRENT SUITE LOG\n")
                self.assertEqual(len(list(self.artifacts.glob("fixture.*/trace.log"))), 1)
                if suite == "performance":
                    self.assertEqual((self.artifacts / "results/summary.md").read_text(), "CURRENT RESULTS\n")

    def test_heal_accumulates_actual_staged_steps_without_overwriting_failures(self):
        self.prepare("heal")
        self.assertEqual(self.run_step("Initialize functional evidence").returncode, 0)
        script = self.directory / "auto-testing/rustfs_heal_test.sh"
        script.parent.mkdir()
        # Result printf and full-run condition from auto-testing@6120aa0a76de:143,1163-1168.
        script.write_text(r'''#!/bin/bash
set -euo pipefail
SELECTED_STEPS=()
PREFLIGHT=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --steps) IFS=',' read -ra SELECTED_STEPS <<< "$2"; shift ;;
    --log-file) LOG_FILE="$2"; shift ;;
    --preflight) PREFLIGHT=1 ;;
  esac
  shift
done
if [ "$PREFLIGHT" -eq 1 ]; then
  printf '\n' >> "$INVOKED_STEPS"
  exit 0
fi
printf '%s\n' "${SELECTED_STEPS[*]}" >> "$INVOKED_STEPS"
emit_step_result() {
  local n="$1" desc="$2" status="$3"
  printf '[HEAL-STEP] %s %s %s\n' "${n}" "${desc}" "${status}"
}
{
  for step in "${SELECTED_STEPS[@]}"; do
    emit_step_result "$step" "fixture step $step" PASS
  done
  want_all=1
  for s in 1 2 3 4 5 6 7; do
    [[ " ${SELECTED_STEPS[*]} " == *" ${s} "* ]] || want_all=0
  done
  if [ "${want_all}" -eq 1 ]; then
    printf '[HEAL-RESULT] PASS all steps passed\n'
  fi
} >> "$LOG_FILE"
''')
        script.chmod(0o755)
        self.env["INVOKED_STEPS"] = str(self.directory / "invoked-steps")
        for name in ("Install RustFS package & start cluster", "Preflight checks", "Run heal test (write -> outage -> heal -> verify)"):
            result = self.run_step(name)
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(Path(self.env["INVOKED_STEPS"]).read_text().splitlines(), ["1 2", "", "3 4 5 6 7"])
        log = Path(self.env["LOG_FILE"]).read_text()
        self.assertNotIn("[HEAL-RESULT]", log)
        self.assertEqual(log.count("[HEAL-STEP]"), 7)
        report = self.run_step("Generate report")
        self.assertEqual(report.returncode, 0, report.stderr)
        failed_logs = ["\n".join(line for line in log.splitlines() if not line.startswith(f"[HEAL-STEP] {step} ")) + "\n"
                       for step in range(1, 8)]
        failed_logs += [
            log.replace("[HEAL-STEP] 3", "[HEAL-STEP] 3 original failure FAIL\n[HEAL-STEP] 3"),
            log + "[HEAL-STEP] 3 later step failure FAIL\n",
            log + "[HEAL-RESULT] FAIL earlier failure\n[HEAL-RESULT] PASS later success\n",
            log.replace("[HEAL-STEP] 4 fixture step 4 PASS", "[HEAL-STEP] 4 fixture step 4 SKIP"),
        ]
        for failed_log in failed_logs:
            with self.subTest(log=failed_log):
                Path(self.env["LOG_FILE"]).write_text(failed_log)
                report = self.run_step("Generate report")
                self.assertNotEqual(report.returncode, 0, report.stderr)
                contents = Path(self.env["REPORT_FILE"]).read_text()
                self.assertIn("Test Step Outcome: failure", contents)
                self.assertNotIn("| PASS |", contents)
                if "original failure" in failed_log:
                    self.assertIn("| 3 | original failure | FAIL |", (self.artifacts / "steps.md").read_text())
                if "later step failure" in failed_log:
                    self.assertIn("| 3 | later step failure | FAIL |", (self.artifacts / "steps.md").read_text())

    def test_performance_results_version_and_report_are_bound_to_the_run(self):
        self.prepare("performance")
        initialized = self.run_step("Initialize functional evidence")
        self.assertEqual(initialized.returncode, 0, initialized.stderr)
        self.assertEqual(self.env["RUSTFS_RESULT_DIR"], str(self.artifacts / "results"))
        self.assertEqual(self.env["VERSION_FILE"], str(self.artifacts / "version.txt"))
        version = self.run_step("Collect RustFS version info")
        self.assertEqual(version.returncode, 0, version.stderr)
        self.assertIn("fixture-version", Path(self.env["VERSION_FILE"]).read_text())
        old_summary = self.directory / "old-results/summary.md"
        old_summary.parent.mkdir()
        old_summary.write_text("OLD RUN EVIDENCE")
        upload = "Upload report to dashboard (reports/YYYY-MM-DD.md)"
        self.assertNotEqual(self.run_step(upload).returncode, 0)
        self.assertFalse(Path(self.env["REPORT_FILE"]).exists())
        results = Path(self.env["RUSTFS_RESULT_DIR"])
        results.mkdir()
        (results / "summary.md").write_text("CURRENT PERFORMANCE RESULTS\n")
        report = self.run_step(upload)
        self.assertEqual(report.returncode, 0, report.stderr)
        contents = Path(self.env["REPORT_FILE"]).read_text()
        for value in ("actions/runs/314159", "**Attempt**: 2", "**Workflow Commit**: " + self.context["github.sha"],
                      "CURRENT PERFORMANCE RESULTS", "fixture-version"):
            self.assertIn(value, contents)
        self.assertNotIn("OLD RUN EVIDENCE", contents)


if __name__ == "__main__":
    unittest.main()
