#!/usr/bin/env python3
"""Exercise the pinned upgrade sources without building RustFS."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest

from check_test_wiring import yaml_block
from test_security_workflow import named_steps, shell_body


ROOT = Path(__file__).resolve().parents[1]
SOURCE_CASES = {
    "1.0.0-rc.5": ("rc5_rollback_requires_restoring_odm_configuration",
                   "rc5_checkpoint_upgrade_rewinds_signed_progress_without_losing_objects"),
    "1.0.1": ("stable_upgrade_deletes_foreign_reservations_without_losing_live_objects",),
    "1.0.5": (),
}


class UpgradeWorkflowTests(unittest.TestCase):
    def setUp(self):
        source = (ROOT / ".github/workflows/e2e-upgrade.yml").read_text().splitlines()
        self.job = yaml_block(source, "upgrade", 2)
        self.steps = named_steps(self.job)
        self.step = self.steps["Run upgrade compatibility tests"]
        # Explicit bash enables pipefail in Actions, so tee cannot hide a failure.
        self.assertIn("        shell: bash", self.step)
        self.assertIn("      fail-fast: false", self.job)
        self.assertEqual(re.findall(r"- version: ([^\s]+)", "\n".join(self.job)), list(SOURCE_CASES))
        rust = (ROOT / "crates/e2e_test/src/upgrade_compatibility_test.rs").read_text()
        self.expected = set(re.findall(r'#\[ignore[^\n]*\]\s*async fn (\w+)\(', rust))
        self.assertTrue(self.expected)

    def run_cases(self, version, failed_test="", fail_tee=False):
        shared = self.expected.difference(*SOURCE_CASES.values())
        self.assertEqual(len(shared), 6)
        expected = shared | set(SOURCE_CASES[version])
        self.assertTrue(expected <= self.expected)
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
        for version in SOURCE_CASES:
            with self.subTest(version=version):
                result = self.run_cases(version)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_early_or_late_failure_does_not_hide_remaining_cases(self):
        for version, specialized in SOURCE_CASES.items():
            last = specialized[-1] if specialized else "previous_release_baseline_replicates_multipart_layouts"
            for name in ("direct_upgrade_from_previous_release_preserves_object_contracts", last):
                with self.subTest(version=version, name=name):
                    result = self.run_cases(version, failed_test=name)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertIn(f"::error::Upgrade compatibility failed: {name}", result.stdout)

    def test_log_write_failure_fails_the_job(self):
        for version in SOURCE_CASES:
            self.assertNotEqual(self.run_cases(version, fail_tee=True).returncode, 0)

    def test_official_reference_matches_the_workflow_and_release_plan(self):
        reference = tomllib.loads((ROOT / ".config/reference-binary.toml").read_text())
        platform = reference["platforms"]["linux-x86_64-gnu"]
        pin = ("1.0.5", platform["asset"], platform["sha256"])
        self.assertEqual(reference["version"], pin[0])
        matrix = re.findall(r"- version: (\S+)\s+asset: (\S+)\s+sha256: (\w+)", "\n".join(self.job))
        self.assertIn(pin, matrix)
        plan = json.loads((ROOT / ".config/release-validation-plan.json").read_text())
        self.assertEqual(set(matrix), {(s["version"], s["artifact_name"], s["artifact_sha256"])
                                       for s in plan["upgrade_sources"]["sources"]})
        legacy_mpu = "\n".join(self.steps["Run mixed-version MPU recovery"])
        self.assertIn("matrix.source.version != '1.0.5'", legacy_mpu)
        self.assertIn("mixed_version_failed_complete_requires_current_coordinator_for_abort", legacy_mpu)

    def test_gateway_evidence_requires_both_roles_and_rollback_for_the_pinned_pair(self):
        step = self.steps["Verify gateway baseline G09 evidence"]
        self.assertIn("matrix.source.version == '1.0.5'", "\n".join(step))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "rustfs-upgrade-g09-evidence"
            env = dict(os.environ, RUNNER_TEMP=directory, GITHUB_SHA="c" * 40, UPGRADE_SOURCE_REVISION="a" * 40)
            for field in ("mixed_version_reader_evidence", "mixed_version_writer_evidence", "rollback_payload_evidence"):
                folder = root / ("bucket-config-rollback" if field == "rollback_payload_evidence" else "mixed-version-upgrade")
                folder.mkdir(exist_ok=True, parents=True)
                payload = dict(gate="G09", field=field, current_revision=env["GITHUB_SHA"],
                               previous_revision=env["UPGRADE_SOURCE_REVISION"], rollback_payload_replayed=True)
                (folder / f"G09-{field}.json").write_text(json.dumps(payload))
            multipart = root / "bucket-config-rollback/inflight-multipart-rollback.json"
            payload = dict(current_revision=env["GITHUB_SHA"], previous_revision=env["UPGRADE_SOURCE_REVISION"],
                           part_sizes=[5242880, 5242880, 4096], bytes=10489856,
                           rollback_read_sha256="d" * 64, roll_forward_read_sha256="d" * 64)
            multipart.write_text(json.dumps(payload))
            command = ["bash", "-e", "-o", "pipefail", "-c", shell_body(step)]
            self.assertEqual(subprocess.run(command, env=env, capture_output=True).returncode, 0)
            wrong_pair = dict(env, UPGRADE_SOURCE_REVISION="b" * 40)
            self.assertNotEqual(subprocess.run(command, env=wrong_pair, capture_output=True).returncode, 0)
            multipart.write_text(json.dumps(dict(payload, roll_forward_read_sha256="e" * 64)))
            self.assertNotEqual(subprocess.run(command, env=env, capture_output=True).returncode, 0)
            multipart.write_text(json.dumps(payload))
            (root / "mixed-version-upgrade/G09-mixed_version_reader_evidence.json").unlink()
            self.assertNotEqual(subprocess.run(command, env=env, capture_output=True).returncode, 0)

    def test_reports_are_uploaded_even_after_failure(self):
        for name, directory in (("Upload scanner/heal G09 evidence", "rustfs-upgrade-g09-evidence"),
                                ("Upload server logs", "rustfs-upgrade-logs")):
            step = self.steps[name]
            self.assertIn("        if: always()", step)
            self.assertIn("          path: ${{ runner.temp }}/" + directory, step)
            self.assertTrue(any("name:" in line and "matrix.source.version" in line for line in step))


if __name__ == "__main__":
    unittest.main()
