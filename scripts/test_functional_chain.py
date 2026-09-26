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
        self.assertEqual([suite["suite"] for suite in result["suites"]], [
            "upgrade", "s3", "kms", "tier", "storage", "heal", "pool", "security", "replication",
            "fault-tolerance", "table", "performance"])

    def test_release_suites_cannot_be_missing_failed_or_have_missing_artifacts(self):
        for suite in ("fault-tolerance", "table"):
            with self.subTest(suite=suite):
                missing = {key: value for key, value in self.needs.items() if key != suite}
                with self.assertRaises(ValueError):
                    evidence.aggregate(self.chain, self.directory, missing)
                with self.assertRaises(ValueError):
                    evidence.aggregate(self.chain, self.directory, {**self.needs, suite: {"result": "failure"}})
                path = self.directory / (suite + ".json")
                original = path.read_text()
                path.unlink()
                with self.assertRaises(ValueError):
                    evidence.aggregate(self.chain, self.directory, self.needs)
                path.write_text(original)

    def test_failure_report_preserves_counts_without_weakening_success_gate(self):
        path = self.directory / "s3.json"
        record = json.loads(path.read_text())
        record.update(valid=False, counts={**record["counts"], "FAIL": 2}, error="failed cases")
        path.write_text(json.dumps(record))
        needs = {**self.needs, "s3": {"result": "failure"}, "performance": {"result": "cancelled"}}
        (self.directory / "performance.json").unlink()
        result = evidence.summarize(self.chain, self.directory, needs)
        self.assertFalse(result["complete"])
        self.assertEqual(result["lanes"][1]["evidence"]["counts"]["FAIL"], 2)
        self.assertEqual(result["lanes"][-1]["result"], "cancelled")
        self.assertIsNone(result["lanes"][-1]["evidence"])
        with self.assertRaises(ValueError):
            evidence.aggregate(self.chain, self.directory, needs)

    def test_preparation_failure_still_has_all_required_lanes(self):
        result = evidence.summarize(None, self.directory / "absent", {"prepare": {"result": "failure"}})
        self.assertFalse(result["complete"])
        self.assertEqual([lane["suite"] for lane in result["lanes"]], list(evidence.SUITES))
        self.assertIn("| performance | missing | missing |", evidence.render_summary(result))
        self.assertIn("Preparation: failure", evidence.render_summary(result))

    def test_malformed_or_cross_attempt_record_is_not_reused(self):
        for payload in ("broken json", "[]", json.dumps({"suite": "s3", "chain": {"attempt": 9}})):
            (self.directory / "s3.json").write_text(payload)
            result = evidence.summarize(self.chain, self.directory, self.needs)
            self.assertFalse(result["complete"])
            self.assertIsNone(result["lanes"][1]["evidence"])

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

    def test_fault_tolerance_requires_complete_known_verdicts(self):
        passing = "FT-CASE: A-read verdict=pass observed expected result\n"
        divergent = "FT-CASE: C2-read verdict=known-divergence lock majority unavailable\n"
        summary = "FT-SUMMARY: unexpected=0 known-divergence=1 strict=0\n"
        counts = evidence.fault_tolerance_counts(passing + divergent + summary)
        self.assertEqual(counts["PASS"], 1)
        self.assertEqual(counts["UNSUPPORTED"], 1)
        for invalid in (passing, passing + passing + summary, passing + divergent + summary.replace("strict=0", "strict=1"),
                        passing + divergent + summary.replace("known-divergence=1", "known-divergence=0"),
                        passing.replace("verdict=pass", "verdict=UNKNOWN") + summary):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                evidence.fault_tolerance_counts(invalid)


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

    def test_testing_sha_fallback_is_accepted_while_garbage_is_rejected(self):
        # prepare's >24h staleness fallback legitimately sets testing_sha to
        # auto-testing main HEAD, which differs from the committed pin; only
        # the sha format is an invariant now.
        for testing_sha, ok in (("d" * 40, True), ("1" * 40, True), ("xyz", False), ("", False)):
            chain = dict(self.chain, testing_sha=testing_sha)
            env = dict(self.env, CHAIN_MANIFEST=json.dumps(chain))
            if ok:
                with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, env), mock.patch.object(evidence.subprocess, "check_output", return_value="e" * 40):
                    evidence.consume(evidence.current_chain())
            else:
                with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, env), mock.patch.object(evidence.subprocess, "check_output", return_value="e" * 40), self.assertRaises(ValueError):
                    evidence.current_chain()

    def test_report_or_swallowed_test_failure_cannot_produce_valid_evidence(self):
        report = self.root / "cases.md"
        report.write_text("| Case | Name | Status |\n| --- | --- | --- |\n| KMS-1 | fixture | PASS |\n")
        for index, (key, status) in enumerate((("CHAIN_TEST_OUTCOME", "failure"), ("CHAIN_REPORT_OUTCOME", "failure"), ("CHAIN_JOB_STATUS", "cancelled"))):
            output = self.root / str(index) / "kms.json"
            Path(self.env["GITHUB_OUTPUT"]).unlink(missing_ok=True)
            with mock.patch.dict(evidence.os.environ, {**self.env, key: status}), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40), self.assertRaises(ValueError):
                evidence.record(self.chain, "kms", report, output)
            self.assertFalse(json.loads(output.read_text())["valid"])
            self.assertIn("error", json.loads(output.read_text()))
            self.assertEqual(Path(self.env["GITHUB_OUTPUT"]).read_text(), "written=true\n")
        output = self.root / "success" / "kms.json"
        with mock.patch.dict(evidence.os.environ, self.env), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40):
            evidence.record(self.chain, "kms", report, output)
        self.assertTrue(json.loads(output.read_text())["valid"])

    def test_collision_or_failed_write_never_authorizes_evidence_upload(self):
        report = self.root / "cases.md"
        report.write_text("| Case | Status |\n| --- | --- |\n| KMS-1 | PASS |\n")
        output = self.root / "stale" / "kms.json"
        output.parent.mkdir()
        output.write_text("OLD RUN EVIDENCE")
        with mock.patch.dict(evidence.os.environ, self.env), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40):
            with self.assertRaises(FileExistsError):
                evidence.record(self.chain, "kms", report, output)
            self.assertEqual(output.read_text(), "OLD RUN EVIDENCE")
            self.assertFalse(Path(self.env["GITHUB_OUTPUT"]).exists())
            with mock.patch.object(Path, "write_text", side_effect=OSError("disk full")), self.assertRaises(OSError):
                evidence.record(self.chain, "kms", report, self.root / "new" / "kms.json")
            self.assertFalse(Path(self.env["GITHUB_OUTPUT"]).exists())

    def test_unknown_status_cannot_hide_among_passing_cases(self):
        text = "| Case | Name | Status |\n| --- | --- | --- |\n| KMS-1 | fixture | PASS |\n| KMS-2 | fixture | NOT RUN |\n"
        with self.assertRaises(ValueError):
            evidence.report_counts(text)

    def test_release_suite_records_require_successful_execution_and_complete_reports(self):
        reports = {
            "table": "| Case | Name | Status |\n| --- | --- | --- |\n| TBL-101 | Iceberg smoke | PASS |\n",
            "fault-tolerance": "FT-CASE: A-read verdict=pass read succeeded\nFT-SUMMARY: unexpected=0 known-divergence=0 strict=0\n",
        }
        for suite, text in reports.items():
            report = self.root / (suite + ".txt")
            report.write_text(text)
            for status in ("success", "failure"):
                with self.subTest(suite=suite, status=status):
                    output = self.root / (suite + "-" + status) / (suite + ".json")
                    with mock.patch.dict(evidence.os.environ, {**self.env, "CHAIN_TEST_OUTCOME": status}), \
                            mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40):
                        if status == "success":
                            evidence.record(self.chain, suite, report, output)
                        else:
                            with self.assertRaises(ValueError):
                                evidence.record(self.chain, suite, report, output)
                    self.assertEqual(json.loads(output.read_text())["valid"], status == "success")

    def test_driver_passes_one_manifest_and_runs_every_lane_after_failure(self):
        from check_test_wiring import yaml_block
        lines = (candidate.ROOT / ".github/workflows/rustfs-functional-chain.yml").read_text().splitlines()
        previous = None
        for suite in evidence.SUITES:
            job = "\n".join(yaml_block(lines, suite, 2))
            self.assertIn("needs: [prepare" + (", " + previous if previous else "") + "]", job)
            if suite == "performance":
                # gated on the preflight probe: runs only when its fleet is online
                self.assertIn("if: ${{ always() && needs.prepare.result == 'success' && needs.prepare.outputs.performance_ready == 'online' }}", job)
            else:
                self.assertIn("if: ${{ always() && needs.prepare.result == 'success' }}", job)
            self.assertIn("chain_manifest: ${{ needs.prepare.outputs.manifest }}", job)
            previous = suite
        complete = "\n".join(yaml_block(lines, "complete-chain", 2))
        self.assertIn("needs: [prepare, " + ", ".join(evidence.SUITES) + "]", complete)
        self.assertIn("functional_chain_evidence.py aggregate", complete)
        self.assertIn("functional_chain_evidence.py summarize", complete)
        self.assertIn("functional-chain-report-", complete)
        self.assertIn("needs.prepare.result != 'skipped'", complete)
        prepare = "\n".join(yaml_block(lines, "prepare", 2))
        self.assertIn("Check shared functional fleet runner before scheduling suites", prepare)
        self.assertIn("check_functional_runners.py smoke-testing", prepare)
        self.assertIn("Probe performance fleet runner", prepare)
        self.assertIn("check_functional_runners.py pf-testing", prepare)
        self.assertIn("performance_ready: ${{ steps.perf_probe.outputs.performance_ready }}", prepare)

    def test_every_lane_retains_failed_evidence_and_identifies_its_own_attempt(self):
        paths = list((candidate.ROOT / ".github/workflows").glob("rustfs-*-test.yml"))
        lanes = [path for path in paths if "name: Upload chain evidence" in path.read_text()]
        self.assertEqual(len(lanes), len(evidence.SUITES))
        for path in lanes:
            with self.subTest(path=path.name):
                text = path.read_text()
                self.assertIn("if: ${{ always() && steps.chain_record.outputs.written == 'true' }}", text)
                self.assertIn("python3 auto-testing/scripts/issue_manager.py handle", text)
                self.assertIn('--run-id "${GITHUB_RUN_ID}"', text)
                self.assertIn('--attempt "${GITHUB_RUN_ATTEMPT}"', text)
                run_url = next(line for line in text.splitlines() if "--run-url" in line)
                self.assertIn('/attempts/${GITHUB_RUN_ATTEMPT}#summary"', run_url)

    def test_table_issue_manager_receives_outcome_and_case_evidence(self):
        text = (candidate.ROOT / ".github/workflows/rustfs-table-test.yml").read_text()
        self.assertIn("--outcome '${{ steps.test.outcome }}'", text)
        self.assertIn('--report "${FUNCTIONAL_ARTIFACTS_DIR}/cases.md"', text)

    def test_pool_topology_is_checked_before_cleanup_with_four_node_defaults(self):
        text = (candidate.ROOT / ".github/workflows/rustfs-pool-expand-test.yml").read_text()
        self.assertIn("http://rustfs-node4:9000", text)
        self.assertIn("vars.RUSTFS_NODES || 'vm000 vm001 vm002'", text)
        self.assertLess(text.index("name: Validate pool topology"), text.index("name: Cleanup environment (before)"))
        self.assertIn("steps.topology.outcome == 'success' && inputs.cleanup_after", text)


class RunnerTests(unittest.TestCase):
    def test_offline_or_missing_runner_fails_before_dispatch(self):
        import check_functional_runners as runners
        for inventory in ([], [{"status": "offline", "labels": [{"name": "pf-testing"}]}]):
            with mock.patch.object(runners, "api", return_value={"runners": inventory}), self.assertRaisesRegex(ValueError, "pf-testing"):
                runners.check(["pf-testing"])

    def test_busy_online_runner_is_available_and_inventory_is_paginated(self):
        import check_functional_runners as runners
        pages = [{"runners": [{"status": "online", "labels": []}] * 100},
                 {"runners": [{"status": "online", "busy": True, "labels": [{"name": "pf-testing"}]}]}]
        with mock.patch.object(runners, "api", side_effect=pages) as api:
            runners.check(["pf-testing"])
        self.assertIn("page=2", api.call_args.args[0])

    def test_unavailable_inventory_does_not_assume_online(self):
        import check_functional_runners as runners
        with mock.patch.object(runners, "api", side_effect=OSError("forbidden")), self.assertRaises(OSError):
            runners.check(["pf-testing"])

    def test_release_lanes_use_pinned_scripts_and_verified_package_before_recording(self):
        for suite, report in (("fault-tolerance", "suite.log"), ("table", "cases.md")):
            with self.subTest(suite=suite):
                workflow = (candidate.ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text()
                self.assertIn("  workflow_call:", workflow)
                self.assertIn("  workflow_dispatch:", workflow)
                self.assertIn("group: rustfs-shared-functional-tests-v2", workflow)
                expected_ref = (
                    "ref: ${{ steps.chain.outputs.testing_sha || inputs.auto_testing_ref || 'main' }}"
                    if suite == "fault-tolerance"
                    else "ref: ${{ steps.chain.outputs.testing_sha || 'main' }}"
                )
                self.assertIn(expected_ref, workflow)
                self.assertIn("steps.chain_package.outputs.package_url || inputs.package_url", workflow)
                self.assertLess(workflow.index("prepare_functional_package.py prepare"), workflow.index("id: test"))
                self.assertIn("prepare_functional_package.py cleanup", workflow)
                self.assertIn(f"functional_chain_evidence.py record --suite {suite}", workflow)
                self.assertIn('--report "${FUNCTIONAL_ARTIFACTS_DIR}/' + report + '"', workflow)
                self.assertIn("CHAIN_TEST_OUTCOME: ${{ steps.test.outcome }}", workflow)
                self.assertIn("CHAIN_REPORT_OUTCOME: ${{ steps.chain_report.outcome }}", workflow)


class WorkflowTimeoutTests(unittest.TestCase):
    def test_non_performance_suites_have_hard_and_step_deadlines(self):
        from check_test_wiring import yaml_block
        from test_security_workflow import FunctionalWorkflowTests, named_steps
        jobs = {**FunctionalWorkflowTests.JOBS, "security": "security-test"}
        jobs.pop("performance")
        self.assertEqual(len(jobs), 11)
        for suite, job_id in jobs.items():
            with self.subTest(suite=suite):
                source = (candidate.ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text()
                job = yaml_block(source.splitlines(), job_id, 2)
                self.assertIn("    timeout-minutes: 60", job)
                steps = named_steps(job)
                primary = [step for step in steps.values() if any(
                    line in ("        id: test", "        id: pool_test") for line in step)]
                self.assertEqual(len(primary), 1)
                self.assertIn("        timeout-minutes: 45", primary[0])
                cleanup = "Cleanup environment"
                for phase in ("before", "after"):
                    self.assertIn("        timeout-minutes: 5", steps[f"{cleanup} ({phase})"])
                self.assertTrue(any("always()" in line for line in steps[f"{cleanup} (after)"]))
                for name, step in steps.items():
                    if name.startswith(("Generate report", "Upload functional report", "File failure issue",
                                        "Upload report", "Upload test logs", "Manage backlog issues")):
                        self.assertIn("        timeout-minutes: 2", step, name)
                if suite in ("heal", "pool-expand"):
                    install = next(step for name, step in steps.items() if name.startswith("Install RustFS package"))
                    self.assertIn("        timeout-minutes: 5", install)
                    self.assertIn("        timeout-minutes: 5", steps["Preflight checks"])

    def test_performance_is_exempt_from_functional_timeouts(self):
        from check_test_wiring import yaml_block
        source = (candidate.ROOT / ".github/workflows/rustfs-performance-test.yml").read_text()
        job = "\n".join(yaml_block(source.splitlines(), "performance-test", 2))
        self.assertIn("    timeout-minutes: 900", job)
        self.assertNotIn("        timeout-minutes:", job)
        self.assertIn("RUSTFS_WARP_DURATION: ${{ inputs.warp_duration || '5m' }}", job)
        self.assertNotIn("RUSTFS_WARP_SLEEP:", job)
        self.assertIn("RUSTFS_WARP_METHODS: ${{ inputs.test_method }}", job)
        self.assertIn("RUSTFS_WARP_SIZES: ${{ inputs.object_size }}", job)
        self.assertIn("default: '5m'", source)


if __name__ == "__main__":
    unittest.main()
