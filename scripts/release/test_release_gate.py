#!/usr/bin/env python3
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Contract tests for scripts/release/release_gate.py (rustfs/backlog#2662).

Every fixture under testdata/release-gate/ is a contract rehearsal only: these
tests prove the gate's fail-closed orchestration and must never be read as
product release evidence or as R02 live verification results.
"""

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATE = ROOT / "scripts" / "release" / "release_gate.py"
FIXTURES = ROOT / "testdata" / "release-gate"
PLAN = FIXTURES / "plan.json"
CANDIDATE = FIXTURES / "candidate.json"
ARTIFACT = FIXTURES / "artifacts" / "rustfs-linux-x86_64-gnu-v9.9.9-fixture.zip"
ARTIFACT_NAME = "rustfs-linux-x86_64-gnu-v9.9.9-fixture.zip"
REAL_PLAN = ROOT / ".config" / "release-validation-plan.json"


def run_gate(*arguments):
    result = subprocess.run(
        [sys.executable, str(GATE), *map(str, arguments)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return result


def run_validate(results, output, plan=PLAN, candidate=CANDIDATE):
    return run_gate(
        "validate",
        "--plan", plan,
        "--candidate", candidate,
        "--results", results,
        "--output", output,
    )


def run_promote(output, plan=PLAN, candidate=CANDIDATE, summary=None, artifact=ARTIFACT,
                artifact_name=ARTIFACT_NAME, artifact_sha256=None):
    arguments = [
        "verify-promotion",
        "--plan", plan,
        "--candidate", candidate,
        "--summary", summary or output,
    ]
    if artifact is not None:
        arguments += ["--artifact", artifact]
    if artifact_name is not None:
        arguments += ["--artifact-name", artifact_name]
    if artifact_sha256 is not None:
        arguments += ["--artifact-sha256", artifact_sha256]
    if output is not None:
        arguments += ["--output", output]
    return run_gate(*arguments)


def load_summary(output):
    return json.loads((Path(output) / "release-gate-summary.json").read_text())


def finding_codes(summary):
    return {finding["code"] for finding in summary["findings"]}


class GateFixtureTests(unittest.TestCase):
    """Assertion matrix rows from rustfs/backlog#2662 exercised on the fixtures."""

    def setUp(self):
        self._temporary = tempfile.TemporaryDirectory(prefix="release-gate-test.")
        self.addCleanup(self._temporary.cleanup)
        self.output = Path(self._temporary.name)

    def validate_fixture(self, name, expect):
        output = self.output / name
        result = run_validate(FIXTURES / name, output)
        self.assertEqual(result.returncode, expect, result.stderr)
        return result, load_summary(output)

    def test_all_required_pass_and_identity_consistent(self):
        # Matrix: all required PASS and identity consistent -> validate and
        # verify-promotion return 0.
        result, summary = self.validate_fixture("pass", 0)
        self.assertEqual(summary["state"], "pass")
        self.assertTrue(summary["publish_allowed"])
        self.assertEqual(len(summary["required_cases"]), 5)
        self.assertEqual(len(summary["observed"]), 5)
        self.assertEqual(summary["findings"], [])
        expansion = json.loads((self.output / "pass" / "plan-expansion.json").read_text())
        self.assertEqual(len(expansion["required_cases"]), 5)
        import hashlib
        artifact = self.output / ARTIFACT_NAME
        artifact.write_bytes(b"release gate contract payload\n")
        candidate = json.loads(CANDIDATE.read_text())
        old_sha = candidate["deliverables"][0]["sha256"]
        new_sha = hashlib.sha256(artifact.read_bytes()).hexdigest()
        candidate["deliverables"][0].update(sha256=new_sha, size=artifact.stat().st_size)
        candidate_path = self.output / "candidate.json"
        candidate_path.write_text(json.dumps(candidate))
        results = self.output / "actual-results"
        results.mkdir()
        for path in (FIXTURES / "pass").glob("*.json"):
            (results / path.name).write_text(path.read_text().replace(old_sha, new_sha))
        result = run_validate(results, self.output / "actual-certification", candidate=candidate_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        promote = run_promote(self.output / "actual-certification", candidate=candidate_path, artifact=artifact)
        self.assertEqual(promote.returncode, 0, promote.stderr)

    def test_cross_platform_same_case_and_split_shas_accepted(self):
        # Matrix: the same case name on two platforms, and a workflow SHA that
        # differs from the (legally bound) product SHA, must not be rejected.
        result, summary = self.validate_fixture("cross-platform", 0)
        self.assertEqual(summary["state"], "pass")
        product_commit = json.loads(CANDIDATE.read_text())["product_commit"]
        workflows = []
        for name in sorted((FIXTURES / "cross-platform").glob("*.json")):
            if name.name == "batch.json":
                continue
            envelope = json.loads(name.read_text())
            workflows.append(envelope["workflow"]["workflow_sha"])
            self.assertNotEqual(envelope["workflow"]["workflow_sha"], product_commit)
        self.assertEqual(len(workflows), 5)
        case_names = [entry["case_id"] for entry in summary["observed"]]
        self.assertEqual(case_names.count("startup"), 2)
        self.assertEqual(case_names.count("artifact-identity"), 2)

    def test_missing_case_names_the_case(self):
        result, summary = self.validate_fixture("missing-case", 1)
        self.assertIn("missing_case", finding_codes(summary))
        missing = [f for f in summary["findings"] if f["code"] == "missing_case"]
        self.assertTrue(any(f.get("case_id") == "startup" for f in missing), missing)
        self.assertFalse(summary["publish_allowed"])

    def test_duplicate_key_names_the_case(self):
        result, summary = self.validate_fixture("duplicate-key", 1)
        self.assertIn("duplicate_key", finding_codes(summary))
        duplicates = [f for f in summary["findings"] if f["code"] == "duplicate_key"]
        self.assertTrue(any(f.get("case_id") == "startup" for f in duplicates), duplicates)
        self.assertFalse(summary["publish_allowed"])

    def test_extra_unplanned_case_names_the_case(self):
        result, summary = self.validate_fixture("extra-case", 1)
        self.assertIn("extra_case", finding_codes(summary))
        extra = [f for f in summary["findings"] if f["code"] == "extra_case"]
        self.assertTrue(any(f.get("case_id") == "surprise-case" for f in extra), extra)
        self.assertFalse(summary["publish_allowed"])

    def test_non_pass_required_items_block(self):
        # Matrix: any skipped / infra_error / quarantined required item blocks
        # and no official publish write is allowed.
        for name in ("non-pass-skipped", "non-pass-infra-error", "non-pass-quarantined"):
            with self.subTest(name=name):
                result, summary = self.validate_fixture(name, 1)
                self.assertIn("non_pass", finding_codes(summary))
                self.assertFalse(summary["publish_allowed"])

    def test_child_workflow_cancellation_blocks(self):
        result, summary = self.validate_fixture("child-cancelled", 1)
        self.assertIn("cancelled", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_batch_timeout_blocks(self):
        result, summary = self.validate_fixture("timeout", 1)
        self.assertIn("timeout", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_same_tag_different_bytes_is_rejected(self):
        result, summary = self.validate_fixture("same-tag-different-bytes", 1)
        self.assertIn("digest_mismatch", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_stale_attempt_cannot_be_reused(self):
        result, summary = self.validate_fixture("stale-attempt", 1)
        self.assertIn("stale_attempt", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_wrong_platform_is_rejected(self):
        result, summary = self.validate_fixture("wrong-platform", 1)
        self.assertIn("platform_mismatch", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_wrong_harness_sha_cannot_be_reused(self):
        result, summary = self.validate_fixture("wrong-harness", 1)
        self.assertIn("harness_mismatch", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_zero_tests_block(self):
        result, summary = self.validate_fixture("zero-tests", 1)
        self.assertIn("zero_tests", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_untrusted_source_blocks(self):
        # A PR-uploaded or foreign JSON set must never gain release authority.
        result, summary = self.validate_fixture("untrusted-source", 1)
        self.assertIn("untrusted_source", finding_codes(summary))
        self.assertFalse(summary["publish_allowed"])

    def test_swapped_package_after_verification_fails_promotion(self):
        # Matrix: swapping a published package after verification must fail
        # verify-promotion.
        output = self.output / "certified"
        self.assertEqual(run_validate(FIXTURES / "pass", output).returncode, 0)
        promote = run_promote(
            output,
            artifact=FIXTURES / "promotion-swap" / ARTIFACT_NAME,
            summary=output,
        )
        self.assertEqual(promote.returncode, 1, promote.stdout)
        self.assertIn("artifact_mismatch", promote.stderr)
        report = json.loads((output / "promotion-check.json").read_text())
        self.assertEqual(report["state"], "blocked")
        self.assertEqual(finding_codes(report), {"artifact_mismatch"})

    def test_edited_plan_after_verification_fails_promotion(self):
        # Matrix: editing the plan after verification must fail
        # verify-promotion; the old certification cannot be reused.
        output = self.output / "certified"
        self.assertEqual(run_validate(FIXTURES / "pass", output).returncode, 0)
        promote = run_promote(
            output,
            plan=FIXTURES / "promotion-edit" / "plan.json",
            summary=output,
        )
        self.assertEqual(promote.returncode, 1, promote.stdout)
        self.assertIn("plan_edited", promote.stderr)

    def test_promotion_without_certification_fails(self):
        output = self.output / "blocked"
        self.assertEqual(run_validate(FIXTURES / "missing-case", output).returncode, 1)
        promote = run_promote(output, summary=output)
        self.assertEqual(promote.returncode, 1, promote.stdout)
        self.assertIn("certification_missing", promote.stderr)

    def test_summary_junit_and_expansion_are_written(self):
        output = self.output / "pass"
        self.assertEqual(run_validate(FIXTURES / "pass", output).returncode, 0)
        summary = load_summary(output)
        self.assertEqual(summary["plan_sha256"], json.loads(CANDIDATE.read_text())["plan_sha256"])
        junit = (output / "release-gate.junit.xml").read_text()
        self.assertIn("release-gate", junit)
        self.assertIn("identity/linux-x86_64-gnu/startup", junit)
        markdown = (output / "release-gate-summary.md").read_text()
        self.assertIn("state: pass", markdown)

    def test_error_messages_carry_no_credentials(self):
        for name in ("missing-case", "stale-attempt", "untrusted-source", "zero-tests"):
            output = self.output / name
            run_validate(FIXTURES / name, output)
            text = output.joinpath("release-gate-summary.json").read_text()
            text += (output / "release-gate-summary.md").read_text()
            lowered = text.lower()
            for marker in ("secret", "password", "access_key", "akia", "bearer "):
                self.assertNotIn(marker, lowered, f"{name} leaked a credential-shaped string")


class PlanContractTests(unittest.TestCase):
    def test_real_plan_is_explicit_about_sources_and_rollback(self):
        sys.path.insert(0, str(GATE.parent))
        import release_gate

        plan = json.loads(REAL_PLAN.read_text())
        info = release_gate.validate_plan(plan)
        sources = info["upgrade_sources"]
        self.assertGreaterEqual(len(sources["sources"]), 1)
        self.assertFalse(sources["rollback_supported"])
        self.assertTrue(sources["basis"].strip())
        self.assertTrue(sources["rollback_basis"].strip())
        for source in sources["sources"]:
            self.assertEqual(len(source["artifact_sha256"]), 64)

    def test_plan_requires_trusted_workflows_and_frozen_harness(self):
        sys.path.insert(0, str(GATE.parent))
        import release_gate

        plan = json.loads(REAL_PLAN.read_text())
        plan["trusted_workflows"] = []
        with self.assertRaises(release_gate.GateError):
            release_gate.validate_plan(plan)
        plan = json.loads(REAL_PLAN.read_text())
        plan["lanes"][0]["harness_sha"] = "not-a-sha"
        with self.assertRaises(release_gate.GateError):
            release_gate.validate_plan(plan)
        plan = json.loads(REAL_PLAN.read_text())
        plan["upgrade_sources"].pop("rollback_supported")
        with self.assertRaises(release_gate.GateError):
            release_gate.validate_plan(plan)

    def test_plan_sha256_is_semantic_and_detects_required_set_edits(self):
        sys.path.insert(0, str(GATE.parent))
        import release_gate

        plan = json.loads(PLAN.read_text())
        baseline = release_gate.canonical_sha256(plan)
        self.assertEqual(release_gate.canonical_sha256(json.loads(json.dumps(plan))), baseline)
        edited = json.loads(PLAN.read_text())
        edited["lanes"][0]["cases"] = ["artifact-identity"]
        self.assertNotEqual(release_gate.canonical_sha256(edited), baseline)


class PublicationWiringTests(unittest.TestCase):
    """The publisher re-runs full certification over trusted downloaded receipts."""

    def test_single_publisher_revalidates_the_complete_seal(self):
        source = (ROOT / "scripts/release/publish.py").read_text()
        self.assertIn('pipeline.certify(inputs, directory,', source)
        self.assertIn('"full")', source)
        self.assertIn('summary["publish_allowed"]', source)
        self.assertIn('pipeline.verify_deliveries(inputs, directory)', source)
        workflow = (ROOT / ".github/workflows/release-publish.yml").read_text()
        self.assertIn('environment: release-publication', workflow)
        self.assertIn('needs: [verify, version]', workflow)

    def test_builders_do_not_have_publication_permissions(self):
        for name in ("build.yml", "docker.yml", "package.yml", "helm-package.yml"):
            source = (ROOT / ".github/workflows" / name).read_text()
            self.assertNotIn("contents: write", source)
            self.assertNotIn("packages: write", source)


if __name__ == "__main__":
    unittest.main()
