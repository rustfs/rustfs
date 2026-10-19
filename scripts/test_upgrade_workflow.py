#!/usr/bin/env python3
"""Exercise both pinned upgrade sources without building RustFS."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

from check_test_wiring import yaml_block
from test_security_workflow import named_steps, shell_body


ROOT = Path(__file__).resolve().parents[1]


class UpgradeWorkflowTests(unittest.TestCase):
    def setUp(self):
        source = (ROOT / ".github/workflows/e2e-upgrade.yml").read_text().splitlines()
        self.job = yaml_block(source, "upgrade", 2)
        self.steps = named_steps(self.job)
        self.step = self.steps["Run upgrade compatibility tests"]
        # Explicit bash enables pipefail in Actions, so tee cannot hide a failure.
        self.assertIn("        shell: bash", self.step)
        self.assertIn("      fail-fast: false", self.job)
        self.assertEqual(re.findall(r"- version: ([^\s]+)", "\n".join(self.job)), ["1.0.0-rc.5", "1.0.1"])
        rust = (ROOT / "crates/e2e_test/src/upgrade_compatibility_test.rs").read_text()
        self.expected = set(re.findall(r'#\[ignore[^\n]*\]\s*async fn (\w+)\(', rust))
        self.assertTrue(self.expected)

    def run_cases(self, version, failed_test="", fail_tee=False):
        excluded = ("stable_upgrade_deletes_foreign_reservations_without_losing_live_objects"
                    if version == "1.0.0-rc.5" else "rc5_rollback_requires_restoring_odm_configuration")
        expected = self.expected - {excluded}
        self.assertEqual(len(expected), 7)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = root / "commands.jsonl"
            fake_python = root / "python3"
            fake_python.write_text(f"#!{sys.executable}\n" + '''import json, os, sys
from pathlib import Path
args = sys.argv[1:]
assert args[:8] == ["scripts/e2e_binary.py", "run", "--", "cargo", "test", "--locked", "-p", "e2e_test"]
assert args[9:] == ["--", "--ignored", "--exact", "--nocapture"]
test = args[8].removeprefix("upgrade_compatibility_test::")
assert args[8] != test
assert os.environ["no_proxy"] == "127.0.0.1,localhost,::1"
with open(os.environ["COMMANDS"], "a") as output:
    output.write(json.dumps({"test": test, "logs": os.environ["RUSTFS_E2E_LOG_DIR"],
                             "evidence": os.environ["RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR"]}) + "\\n")
Path(os.environ["RUSTFS_SCANNER_HEAL_G09_EVIDENCE_DIR"]).mkdir(parents=True)
print("executed " + test)
sys.exit(17 if test == os.environ["FAILED_TEST"] else 0)
''')
            fake_python.chmod(0o755)
            if fail_tee:
                tee = root / "tee"
                tee.write_text("#!/bin/sh\ncat >/dev/null\nexit 23\n")
                tee.chmod(0o755)
            env = dict(os.environ, PATH=f"{root}{os.pathsep}{os.environ['PATH']}",
                       RUNNER_TEMP=str(root), COMMANDS=str(commands), FAILED_TEST=failed_test,
                       NO_PROXY="127.0.0.1,localhost,::1", UPGRADE_SOURCE_VERSION=version)
            result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", shell_body(self.step)],
                                    cwd=root, env=env, capture_output=True, text=True)
            calls = [json.loads(line) for line in commands.read_text().splitlines()]
            self.assertEqual({call["test"] for call in calls}, expected)
            self.assertEqual(len(calls), len(expected))
            for field in ("logs", "evidence"):
                self.assertEqual(len({call[field] for call in calls}), len(calls))
                self.assertTrue(all(Path(call[field]).is_dir() for call in calls))
            if not fail_tee:
                for call in calls:
                    self.assertIn(call["test"], (Path(call["logs"]) / "test.log").read_text())
            return result

    def test_all_ignored_upgrade_cases_run_once_with_separate_evidence(self):
        for version in ("1.0.0-rc.5", "1.0.1"):
            with self.subTest(version=version):
                result = self.run_cases(version)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_early_or_late_failure_does_not_hide_remaining_cases(self):
        for version, last in (("1.0.0-rc.5", "rc5_rollback_requires_restoring_odm_configuration"),
                              ("1.0.1", "stable_upgrade_deletes_foreign_reservations_without_losing_live_objects")):
            for name in ("direct_upgrade_from_previous_release_preserves_object_contracts", last):
                with self.subTest(version=version, name=name):
                    result = self.run_cases(version, failed_test=name)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn(f"::error::Upgrade compatibility failed: {name}", result.stdout)

    def test_log_write_failure_fails_the_job(self):
        for version in ("1.0.0-rc.5", "1.0.1"):
            self.assertNotEqual(self.run_cases(version, fail_tee=True).returncode, 0)

    def test_reports_are_uploaded_even_after_failure(self):
        for name, directory in (("Upload scanner/heal G09 evidence", "rustfs-upgrade-g09-evidence"),
                                ("Upload server logs", "rustfs-upgrade-logs")):
            step = self.steps[name]
            self.assertIn("        if: always()", step)
            self.assertIn("          path: ${{ runner.temp }}/" + directory, step)
            self.assertTrue(any("name:" in line and "matrix.source.version" in line for line in step))


if __name__ == "__main__":
    unittest.main()
