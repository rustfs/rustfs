#!/usr/bin/env python3
"""Exercise candidate substitution and complete-chain acceptance boundaries."""
import copy
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import zipfile

import functional_chain_evidence as evidence
import resolve_functional_candidate as candidate


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.run = {"id": 123, "run_attempt": 2, "head_sha": "a" * 40}
        self.manifest = {"schema": 2, "workflow_sha": "a" * 40, "source_sha": "b" * 40, "source_ref": "release",
                         "build_run_id": 123, "build_run_attempt": 2, "package_sha256": "c" * 64,
                         "package_url": "https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/123/2/" + "c" * 64 + "/rustfs.deb"}

    def archive(self, names=None):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for name in names or ["nightly-candidate-123-2.json"]:
                archive.writestr(name, json.dumps(self.manifest))
        payload = output.getvalue()
        artifact = {"id": 789, "name": "nightly-candidate-123-2", "expired": False, "size_in_bytes": len(payload),
                    "digest": "sha256:" + hashlib.sha256(payload).hexdigest(), "workflow_run": {"id": 123, "head_sha": "a" * 40}}
        return payload, artifact

    def test_distinct_build_source_preserves_both_identities(self):
        payload, artifact = self.archive()
        result = candidate.read_manifest(payload, artifact, self.run)
        self.assertEqual(result["source_sha"], "b" * 40)
        self.assertEqual(result["workflow_sha"], "a" * 40)
        self.assertEqual(result["source_ref"], "release")

    def test_legacy_requires_the_build_and_workflow_sha_to_agree(self):
        self.manifest["schema"] = 1
        del self.manifest["workflow_sha"], self.manifest["source_ref"]
        with self.assertRaisesRegex(ValueError, "legacy"):
            candidate.validate_manifest(self.manifest, self.run)
        self.manifest["source_sha"] = self.run["head_sha"]
        candidate.validate_manifest(self.manifest, self.run)

    def test_manifest_substitutions_fail(self):
        for key, value in (("workflow_sha", "d" * 40), ("build_run_id", 124), ("build_run_attempt", 1),
                           ("package_sha256", "d" * 64), ("package_url", "https://example.com/package.deb"),
                           ("schema", True), ("source_ref", "release\nFORGED=value")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                candidate.validate_manifest({**self.manifest, key: value}, self.run)

    def test_artifact_substitutions_and_archive_members_fail(self):
        payload, artifact = self.archive()
        for key, value in (("expired", True), ("name", "nightly-candidate-123-1"), ("size_in_bytes", 1),
                           ("digest", "sha256:" + "d" * 64), ("workflow_run", {"id": 124, "head_sha": "a" * 40})):
            with self.subTest(key=key), self.assertRaises(ValueError):
                candidate.read_manifest(payload, {**artifact, key: value}, self.run)
        for names in (["../nightly-candidate-123-2.json"], ["nightly-candidate-123-2.json", "extra.json"]):
            payload, artifact = self.archive(names)
            with self.assertRaises(ValueError):
                candidate.read_manifest(payload, artifact, self.run)

    def test_resolver_uses_attempt_metadata_and_does_not_resolve_moving_branch(self):
        payload, artifact = self.archive()
        run = {**self.run, "path": ".github/workflows/nightly-gnu.yml", "head_branch": "main", "head_repository": {"full_name": "rustfs/rustfs"},
               "event": "schedule", "status": "completed", "conclusion": "success", "run_started_at": "2026-09-12T00:00:00Z"}
        with mock.patch.object(candidate, "api", side_effect=[run, {"artifacts": [artifact]}, payload]) as api:
            result = candidate.resolve(123, 2)
        self.assertTrue(api.call_args_list[0].args[0].endswith("/attempts/2"))
        self.assertFalse(any("branches/" in call.args[0] for call in api.call_args_list))
        self.assertEqual(result["artifact_id"], 789)
        self.assertEqual(result["manifest"]["source_sha"], "b" * 40)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.chain = {"run_id": 456, "attempt": 1, "candidate": {"source_sha": "a" * 40, "workflow_sha": "b" * 40}}
        self.needs = {suite: {"result": "success"} for suite in evidence.SUITES}
        for suite in evidence.SUITES:
            value = {"schema": 1, "suite": suite, "chain": self.chain, "valid": True, "report_sha256": "c" * 64,
                     "counts": {"PASS": 1, "FAIL": 0, "SKIP": 0, "UNSUPPORTED": 0, "RUNNING": 0}}
            (self.directory / (suite + ".json")).write_text(json.dumps(value))

    def test_complete_chain_retains_source_identity(self):
        result = evidence.aggregate(self.chain, self.directory, self.needs)
        self.assertTrue(result["complete"])
        self.assertEqual(result["chain"], self.chain)
        self.assertEqual(len(result["suites"]), 10)

    def test_missing_failed_cancelled_or_skipped_lane_never_passes(self):
        for state in ("failure", "cancelled", "skipped", "pending"):
            with self.subTest(state=state), self.assertRaises(ValueError):
                evidence.aggregate(self.chain, self.directory, {**self.needs, "s3": {"result": state}})
        del self.needs["s3"]
        with self.assertRaises(ValueError):
            evidence.aggregate(self.chain, self.directory, self.needs)

    def test_partial_rerun_missing_artifact_and_zero_test_fail(self):
        path = self.directory / "s3.json"
        original = json.loads(path.read_text())
        values = [{**original, "chain": {**self.chain, "attempt": 2}},
                  {**original, "counts": {**original["counts"], "PASS": 0}},
                  {**original, "counts": {**original["counts"], "FAIL": 1}},
                  {**original, "valid": False}, {**original, "report_sha256": ""}]
        for value in values:
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                evidence.aggregate(self.chain, self.directory, self.needs)
        path.unlink()
        with self.assertRaises(ValueError):
            evidence.aggregate(self.chain, self.directory, self.needs)

    def test_reports_count_case_status_not_cleanup_status(self):
        text = "| Topology | Case | Name | Status | Cleanup |\n| --- | --- | --- | --- | --- |\n| sns | TIER-1 | test | UNSUPPORTED | PASS |\n| sns | TIER-2 | test | FAIL | PASS |\n"
        counts = evidence.report_counts(text)
        self.assertEqual(counts["PASS"], 0)
        self.assertEqual(counts["FAIL"], 1)
        self.assertEqual(counts["UNSUPPORTED"], 1)
        self.assertEqual(evidence.report_counts("")["PASS"], 0)

    def test_performance_needs_real_complete_metrics(self):
        header = "method\tsize\tthroughput\tobj_per_s\treq_avg\treq_p50\treq_p90\treq_p99\n"
        row = "put\t1MiB\t100MiB/s\t100\t1ms\t1ms\t2ms\t3ms\n"
        self.assertEqual(evidence.report_counts(header + row, True)["PASS"], 1)
        self.assertEqual(evidence.report_counts(header, True)["PASS"], 0)
        with self.assertRaises(ValueError):
            evidence.report_counts(header + row + row, True)
        with self.assertRaises(ValueError):
            evidence.report_counts(header + "put\t1MiB\t\t\t\t\t\t\n", True)


class EnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / ".config").mkdir()
        (self.root / ".config/functional-script-revision.txt").write_text("d" * 40)
        manifest = {"schema": 2, "workflow_sha": "a" * 40, "source_sha": "b" * 40, "source_ref": "release",
                    "build_run_id": 123, "build_run_attempt": 2, "package_sha256": "c" * 64,
                    "package_url": "https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/123/2/" + "c" * 64 + "/rustfs.deb"}
        self.chain = {"schema": 1, "run_id": 456, "attempt": 3, "workflow_sha": "e" * 40, "testing_sha": "d" * 40,
                      "candidate": {"manifest": manifest, "artifact_id": 789, "artifact_digest": "sha256:" + "f" * 64,
                                    "workflow_sha": "a" * 40, "workflow_ref": "main", "build_started_at": "2026-09-12T00:00:00Z"}}
        self.env = {"CHAIN_MANIFEST": json.dumps(self.chain), "GITHUB_RUN_ID": "456", "GITHUB_RUN_ATTEMPT": "3", "GITHUB_SHA": "e" * 40,
                    "GITHUB_ENV": str(self.root / "env"), "GITHUB_OUTPUT": str(self.root / "output"),
                    "CHAIN_JOB_STATUS": "success", "CHAIN_TEST_OUTCOME": "success", "CHAIN_REPORT_OUTCOME": "success"}

    def test_consumer_exports_the_same_package_and_checksum_to_installers(self):
        with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, self.env), mock.patch.object(evidence.subprocess, "check_output", return_value="e" * 40):
            evidence.consume(evidence.current_chain())
        variables = dict(line.split("=", 1) for line in (self.root / "env").read_text().splitlines())
        self.assertEqual(variables["PACKAGE_SHA256"], "c" * 64)
        self.assertEqual(variables["TO_SHA256"], "c" * 64)
        self.assertEqual(variables["RUSTFS_NIGHTLY_PACKAGE_URL"], self.chain["candidate"]["manifest"]["package_url"])

    def test_partial_rerun_and_wrong_lane_checkout_fail_before_install(self):
        for changes, head in (({"GITHUB_RUN_ATTEMPT": "4"}, "e" * 40), ({}, "f" * 40), ({"GITHUB_RUN_ID": "457"}, "e" * 40)):
            with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, {**self.env, **changes}), mock.patch.object(evidence.subprocess, "check_output", return_value=head), self.assertRaises(ValueError):
                evidence.current_chain()
        self.assertFalse((self.root / "env").exists())

    def test_report_or_swallowed_test_failure_cannot_produce_valid_evidence(self):
        report = self.root / "cases.md"
        report.write_text("| Case | Name | Status |\n| --- | --- | --- |\n| KMS-1 | fixture | PASS |\n")
        for index, (key, status) in enumerate((("CHAIN_TEST_OUTCOME", "failure"), ("CHAIN_REPORT_OUTCOME", "failure"), ("CHAIN_JOB_STATUS", "cancelled"))):
            output = self.root / str(index) / "kms.json"
            with mock.patch.dict(evidence.os.environ, {**self.env, key: status}), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40), self.assertRaises(ValueError):
                evidence.record(self.chain, "kms", report, output)
            self.assertFalse(json.loads(output.read_text())["valid"])
        output = self.root / "success" / "kms.json"
        with mock.patch.dict(evidence.os.environ, self.env), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40):
            evidence.record(self.chain, "kms", report, output)
        self.assertTrue(json.loads(output.read_text())["valid"])

    def test_unknown_status_cannot_hide_among_passing_cases(self):
        text = "| Case | Name | Status |\n| --- | --- | --- |\n| KMS-1 | fixture | PASS |\n| KMS-2 | fixture | NOT RUN |\n"
        with self.assertRaises(ValueError):
            evidence.report_counts(text)

    def test_driver_passes_one_manifest_and_runs_every_lane_after_failure(self):
        from check_test_wiring import yaml_block
        lines = (candidate.ROOT / ".github/workflows/rustfs-functional-chain.yml").read_text().splitlines()
        previous = None
        for suite in evidence.SUITES:
            job = "\n".join(yaml_block(lines, suite, 2))
            self.assertIn("needs: [prepare" + (", " + previous if previous else "") + "]", job)
            self.assertIn("if: ${{ always() && needs.prepare.result == 'success' }}", job)
            self.assertIn("chain_manifest: ${{ needs.prepare.outputs.manifest }}", job)
            previous = suite
        complete = "\n".join(yaml_block(lines, "complete-chain", 2))
        self.assertIn("needs: [prepare, " + ", ".join(evidence.SUITES) + "]", complete)
        self.assertIn("functional_chain_evidence.py aggregate", complete)


if __name__ == "__main__":
    unittest.main()
