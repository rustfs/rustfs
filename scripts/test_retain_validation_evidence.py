"""Interrupted evidence is read-only and never an implicit recovery or PASS."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import retain_validation_evidence as retain


class RetainEvidenceTests(unittest.TestCase):
    def fixture(self, directory):
        temporary = Path(directory).resolve() / "_work/_temp"
        temporary.mkdir(parents=True)
        parent = temporary.parent / "rustfs-validation-evidence"
        parent.mkdir(mode=0o700)
        root = parent / "rustfs-validation-77-1"
        root.mkdir(mode=0o700)
        (root / "runner-storage-preflight.json").write_text(json.dumps({
            "runId": "77", "runAttempt": "1", "evidenceRoot": str(root),
            "persistentOutsideRunnerTemp": True}))
        (root / "original.json").write_bytes(b'{"verdict":"RUNNING"}')
        env = {"RUNNER_TEMP": str(temporary), "GITHUB_RUN_ID": "88", "GITHUB_RUN_ATTEMPT": "1",
               "RUNNER_NAME": "owned-runner", "GITHUB_ENV": str(temporary / "env"),
               "EVIDENCE_RETAIN_INPUTS": json.dumps({"validation_retain_evidence_run_id": "77",
                    "i05_diagnostic_branch": "timeout", "validation_tools_run_attempt": "1"})}
        run = {"id": 77, "run_attempt": 1, "status": "completed", "conclusion": "failure",
               "event": "workflow_dispatch", "repository": {"full_name": "rustfs/rustfs"},
               "path": ".github/workflows/rustfs-functional-chain.yml", "head_sha": "a" * 40,
               "html_url": "source-url"}
        jobs = {"total_count": 1, "jobs": [{"name": "validation", "runner_name": "owned-runner"}]}
        return env, root, run, jobs

    def test_existing_bytes_and_original_running_verdict_remain_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            env, root, run, jobs = self.fixture(directory)
            before = {p.name: p.read_bytes() for p in root.iterdir()}
            report = {}
            with patch.object(retain, "api", side_effect=[run, jobs]) as api:
                self.assertEqual(retain.prepare(env, report), root)
            self.assertTrue(report["complete"])
            self.assertEqual({p.name: p.read_bytes() for p in root.iterdir()}, before)
            self.assertEqual({row["path"]: row["sha256"] for row in report["files"]},
                             {name: hashlib.sha256(raw).hexdigest() for name, raw in before.items()})
            self.assertEqual([call.args[0] for call in api.call_args_list], [
                "repos/rustfs/rustfs/actions/runs/77/attempts/1",
                "repos/rustfs/rustfs/actions/runs/77/attempts/1/jobs?per_page=100"])

    def test_mixed_execution_requests_and_unsafe_run_ids_refuse_before_api(self):
        with tempfile.TemporaryDirectory() as directory:
            env, root, run, jobs = self.fixture(directory)
            inputs = json.loads(env["EVIDENCE_RETAIN_INPUTS"])
            for field, value in [("validation_case", "heal.admin-healthy-noop"),
                                 ("validation_runner_inspect_only", True), ("validation_tools_only", True),
                                 ("build_run_id", "1"), ("validation_retain_evidence_run_id", "../77"),
                                 ("validation_retain_evidence_run_id", "88")]:
                with self.subTest(field=field, value=value), patch.object(retain, "api") as api:
                    altered = {**env, "EVIDENCE_RETAIN_INPUTS": json.dumps({**inputs, field: value})}
                    with self.assertRaises(ValueError): retain.prepare(altered, {})
                    api.assert_not_called()

    def test_wrong_repository_running_source_and_different_runner_refuse(self):
        for mutation in ("repo", "running", "success", "runner", "attempt"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                env, root, run, jobs = self.fixture(directory)
                if mutation == "repo": run["repository"]["full_name"] = "foreign/repo"
                elif mutation == "running": run["status"] = "in_progress"
                elif mutation == "success": run["conclusion"] = "success"
                elif mutation == "attempt": run["run_attempt"] = 2
                else: jobs["jobs"][0]["runner_name"] = "other-runner"
                with patch.object(retain, "api", side_effect=[run, jobs]), self.assertRaises(ValueError):
                    retain.prepare(env, {})

    def test_links_special_files_and_byte_budget_do_not_escape_the_original_root(self):
        for mutation in ("symlink", "hardlink", "fifo", "budget", "public-root"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                env, root, run, jobs = self.fixture(directory)
                if mutation == "symlink": (root / "foreign").symlink_to(Path(directory) / "secret")
                elif mutation == "hardlink": os.link(root / "original.json", root / "alias")
                elif mutation == "fifo": os.mkfifo(root / "fifo")
                elif mutation == "public-root": root.chmod(0o755)
                with patch.object(retain, "api", side_effect=[run, jobs]), \
                     patch.object(retain, "MAX_BYTES", 1 if mutation == "budget" else retain.MAX_BYTES), \
                     self.assertRaises(ValueError):
                    retain.prepare(env, {})

    def test_file_changes_and_foreign_storage_provenance_are_rejected(self):
        for mutation in ("write", "marker"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                env, root, run, jobs = self.fixture(directory)
                if mutation == "marker":
                    (root / "runner-storage-preflight.json").write_text('{"runId":"foreign"}')
                real = os.read
                changed = []
                def read(fd, size):
                    raw = real(fd, size)
                    if mutation == "write" and not changed:
                        changed.append(True)
                        with (root / "original.json").open("ab") as stream: stream.write(b"changed")
                    return raw
                with patch.object(retain, "api", side_effect=[run, jobs]), patch.object(retain.os, "read", side_effect=read), \
                     self.assertRaises(ValueError):
                    retain.prepare(env, {})

    def test_cli_exports_only_admitted_root_and_emits_no_case_or_recovery_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            env, root, run, jobs = self.fixture(directory)
            output = Path(directory) / "receipt.json"
            with patch.dict(os.environ, env), patch.object(retain, "api", side_effect=[run, jobs]), \
                 patch.object(retain.sys, "argv", ["retain", "--report", str(output)]):
                self.assertEqual(retain.main(), 0)
            value = json.loads(output.read_text())
            for field in ("fleetAccessed", "originalEvidenceModified", "sourceVerdictRewritten", "canonicalCasePassClaimed"):
                self.assertIs(value[field], False)
            self.assertIn("VALIDATION_RETAIN_ROOT=" + str(root), Path(env["GITHUB_ENV"]).read_text())

    def test_cli_keeps_incomplete_receipt_without_exporting_a_missing_original_root(self):
        with tempfile.TemporaryDirectory() as directory:
            env, root, run, jobs = self.fixture(directory)
            env["EVIDENCE_RETAIN_INPUTS"] = '{"validation_retain_evidence_run_id":"99"}'
            run["id"] = 99
            output = Path(directory) / "receipt.json"
            with patch.dict(os.environ, env), patch.object(retain, "api", side_effect=[run, jobs]), \
                 patch.object(retain.sys, "argv", ["retain", "--report", str(output)]):
                self.assertEqual(retain.main(), 1)
            value = json.loads(output.read_text())
            self.assertFalse(value["complete"])
            self.assertEqual(value["errorType"], "FileNotFoundError")
            self.assertFalse(Path(env["GITHUB_ENV"]).exists())


if __name__ == "__main__":
    unittest.main()
