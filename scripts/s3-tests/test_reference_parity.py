#!/usr/bin/env python3
# Copyright 2026 RustFS Team
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
"""Contract tests for reference_parity.py: target list, rerun merge, tolerated entries and exit codes."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parent / "reference_parity.py"
REPO = Path(__file__).resolve().parents[2]
PREFIX = "s3tests/functional/test_s3.py::"
CLASSNAME = "s3tests.functional.test_s3"
TARGETS = [{"name": "reference", "binary": "reference", "env": {}},
           {"name": "candidate-legacy", "binary": "candidate", "env": {}}]
ISSUE = "https://github.com/rustfs/rustfs/issues/1"


def junit(states):
    cases = []
    for name, state in states.items():
        body = {"pass": "", "fail": "<failure message='x'/>", "error": "<error message='x'/>",
                "skip": "<skipped/>"}[state]
        cases.append(f'<testcase classname="{CLASSNAME}" name="{name}">{body}</testcase>')
    return f'<testsuites><testsuite name="pytest">{"".join(cases)}</testsuite></testsuites>'


def run(*args):
    proc = subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


class Lane:
    """A temporary reports directory with one sub-directory per target."""

    def __init__(self, case):
        self.temp = tempfile.TemporaryDirectory()
        case.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.reports = self.root / "reports"
        self.tolerated = self.root / "tolerated.txt"
        self.tolerated.write_text("# none\n")

    def target(self, name, states, selected=None, rerun=None):
        directory = self.reports / name
        directory.mkdir(parents=True)
        names = selected if selected is not None else list(states)
        (directory / "selected-nodeids.txt").write_text("".join(PREFIX + n + "\n" for n in names))
        (directory / "junit.xml").write_text(junit(states))
        if rerun is not None:
            (directory / "rerun-junit.xml").write_text(junit(rerun))
        return directory

    def compare(self, targets=None):
        summary = self.root / "summary.json"
        code, out, err = run("compare", "--targets-json", json.dumps(targets or TARGETS),
                             "--reports", str(self.reports), "--tolerated", str(self.tolerated),
                             "--json", str(summary))
        data = json.loads(summary.read_text()) if summary.exists() else None
        return code, out, err, data


BASE = {"test_a": "pass", "test_b": "pass", "test_c": "skip"}


class CompareTests(unittest.TestCase):
    # --- negative cases ---

    def test_pass_versus_fail_is_a_difference(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_a": "fail"})
        code, out, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["differences"], [{"target": "candidate-legacy", "nodeid": PREFIX + "test_a",
                                                "reference": "pass", "candidate": "fail", "tolerated": None}])
        self.assertIn("Result: **FAIL**", out)

    def test_error_counts_as_fail(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_b": "error"})
        code, _, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["differences"][0]["candidate"], "fail")

    def test_reference_fail_candidate_pass_is_also_a_difference(self):
        lane = Lane(self)
        lane.target("reference", {**BASE, "test_a": "fail"})
        lane.target("candidate-legacy", BASE)
        code, _, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["differences"][0]["reference"], "fail")

    def test_skip_versus_pass_is_a_difference(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_c": "pass"})
        code, _, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["differences"][0]["nodeid"], PREFIX + "test_c")

    def test_case_absent_from_candidate_report_is_missing_and_incomplete(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {"test_a": "pass", "test_c": "skip"}, selected=list(BASE))
        code, out, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["differences"][0]["candidate"], "missing")
        self.assertEqual(data["incomplete"], {"candidate-legacy": [PREFIX + "test_b"]})
        self.assertIn("Incomplete runs", out)

    def test_case_missing_from_both_reports_still_fails(self):
        lane = Lane(self)
        partial = {"test_a": "pass", "test_c": "skip"}
        lane.target("reference", partial, selected=list(BASE))
        lane.target("candidate-legacy", partial, selected=list(BASE))
        code, _, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["differences"], [])
        self.assertEqual(set(data["incomplete"]), {"reference", "candidate-legacy"})

    def test_failure_that_also_fails_the_rerun_stays_a_difference(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_a": "fail"}, rerun={"test_a": "fail"})
        code, _, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["flaky"]["candidate-legacy"], [])

    def test_rerun_of_a_case_that_did_not_fail_is_rejected(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_a": "fail"}, rerun={"test_b": "pass"})
        code, _, err, _ = lane.compare()
        self.assertEqual(code, 2)
        self.assertIn("rerun without failing first", err)

    def test_missing_target_report_fails_rather_than_skips(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        code, _, err, data = lane.compare()
        self.assertEqual(code, 2)
        self.assertIsNone(data)
        self.assertIn("candidate-legacy: report directory is missing", err)

    def test_missing_junit_fails(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        (lane.target("candidate-legacy", BASE) / "junit.xml").unlink()
        code, _, err, _ = lane.compare()
        self.assertEqual(code, 2)
        self.assertIn("cannot read", err)

    def test_different_selected_lists_are_rejected(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {"test_a": "pass", "test_b": "pass"})
        code, _, err, _ = lane.compare()
        self.assertEqual(code, 2)
        self.assertIn("selected node ids differ", err)

    def test_unselected_case_in_junit_is_rejected(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_extra": "pass"}, selected=list(BASE))
        code, _, err, _ = lane.compare()
        self.assertEqual(code, 2)
        self.assertIn("unselected cases", err)

    def test_stale_tolerated_entry_fails(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", BASE)
        lane.tolerated.write_text(f"candidate-legacy | {PREFIX}test_a | {ISSUE} | known drift\n")
        code, out, _, data = lane.compare()
        self.assertEqual(code, 1)
        self.assertEqual(data["stale_tolerated"][0]["nodeid"], PREFIX + "test_a")
        self.assertIn("Stale tolerated entries", out)

    def test_tolerated_entry_for_another_target_does_not_cover_the_difference(self):
        lane = Lane(self)
        targets = TARGETS + [{"name": "candidate-gateway", "binary": "candidate", "env": {}}]
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_a": "fail"})
        lane.target("candidate-gateway", {**BASE, "test_a": "fail"})
        lane.tolerated.write_text(f"candidate-gateway | {PREFIX}test_a | {ISSUE} | gateway gap\n")
        code, _, _, data = lane.compare(targets)
        self.assertEqual(code, 1)
        untolerated = [d["target"] for d in data["differences"] if not d["tolerated"]]
        self.assertEqual(untolerated, ["candidate-legacy"])

    def test_tolerated_entry_without_issue_url_is_rejected(self):
        for line in (f"candidate-legacy | {PREFIX}test_a | rustfs#1 | reason",
                     f"candidate-legacy | {PREFIX}test_a | {ISSUE} | ",
                     f"candidate-legacy | {PREFIX}test_a | {ISSUE}",
                     f"reference | {PREFIX}test_a | {ISSUE} | reason",
                     f"candidate-legacy | test_a | {ISSUE} | reason"):
            with self.subTest(line=line):
                lane = Lane(self)
                lane.target("reference", BASE)
                lane.target("candidate-legacy", {**BASE, "test_a": "fail"})
                lane.tolerated.write_text(line + "\n")
                code, _, err, _ = lane.compare()
                self.assertEqual(code, 2, err)

    def test_duplicate_tolerated_entry_is_rejected(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_a": "fail"})
        entry = f"candidate-legacy | {PREFIX}test_a | {ISSUE} | reason\n"
        lane.tolerated.write_text(entry * 2)
        code, _, err, _ = lane.compare()
        self.assertEqual(code, 2)
        self.assertIn("duplicate entry", err)

    def test_missing_tolerated_file_fails(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", BASE)
        lane.tolerated.unlink()
        code, _, err, _ = lane.compare()
        self.assertEqual(code, 2)
        self.assertIn("tolerated-difference file is missing", err)

    # --- positive cases ---

    def test_identical_runs_pass(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", BASE)
        code, out, _, data = lane.compare()
        self.assertEqual(code, 0)
        self.assertEqual(data["counts"]["candidate-legacy"], {"pass": 2, "fail": 0, "skip": 1, "missing": 0})
        self.assertIn("No differences.", out)
        self.assertIn("Result: **PASS**", out)

    def test_failure_that_passes_the_rerun_is_flaky_not_a_difference(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_a": "fail"}, rerun={"test_a": "pass"})
        code, out, _, data = lane.compare()
        self.assertEqual(code, 0)
        self.assertEqual(data["flaky"]["candidate-legacy"], [PREFIX + "test_a"])
        self.assertIn("Flaky", out)

    def test_tolerated_difference_passes_and_is_labelled(self):
        lane = Lane(self)
        lane.target("reference", BASE)
        lane.target("candidate-legacy", {**BASE, "test_a": "fail"})
        lane.tolerated.write_text(f"# comment\ncandidate-legacy | {PREFIX}test_a | {ISSUE} | known drift\n")
        code, out, _, data = lane.compare()
        self.assertEqual(code, 0)
        self.assertEqual(data["differences"][0]["tolerated"], ISSUE)
        self.assertIn(ISSUE, out)


class TargetTests(unittest.TestCase):
    def write(self, text):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        path = Path(temp.name) / "targets.toml"
        path.write_text(text)
        return path

    def test_invalid_target_lists_are_rejected(self):
        ref = '[[targets]]\nname = "reference"\nbinary = "reference"\n'
        cand = '[[targets]]\nname = "candidate-legacy"\nbinary = "candidate"\n'
        for label, text in {
            "no schema": ref + cand,
            "only reference": "schema = 1\n" + ref,
            "two references": "schema = 1\n" + ref + ref.replace('"reference"\nbinary', '"other"\nbinary'),
            "no reference": "schema = 1\n" + cand + cand.replace("candidate-legacy", "candidate-two"),
            "duplicate name": "schema = 1\n" + ref + cand + cand,
            "unknown binary": "schema = 1\n" + ref + cand.replace('binary = "candidate"', 'binary = "main"'),
            "bad name": "schema = 1\n" + ref + cand.replace("candidate-legacy", "Candidate Legacy"),
            "foreign env key": "schema = 1\n" + ref + cand + 'env = { PATH = "/tmp" }\n',
            "env injection": "schema = 1\n" + ref + cand + 'env = { RUSTFS_X = "a\\nB=c" }\n',
            "unknown key": "schema = 1\n" + ref + cand + 'stack = "gateway"\n',
        }.items():
            with self.subTest(label):
                code, _, _ = run("targets", "--config", str(self.write(text)))
                self.assertEqual(code, 2)

    def test_missing_target_file_fails(self):
        code, _, err = run("targets", "--config", "/nonexistent/targets.toml")
        self.assertEqual(code, 2)
        self.assertIn("target list is missing", err)

    def test_export_env_rejects_foreign_keys(self):
        code, _, _ = run("export-env", "--target-json", json.dumps({"env": {"LD_PRELOAD": "x"}}))
        self.assertEqual(code, 2)

    def test_repository_target_list_is_valid(self):
        code, out, err = run("targets", "--config", str(REPO / ".config/reference-parity-targets.toml"))
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out), TARGETS)

    def test_export_env_prints_validated_lines(self):
        code, out, _ = run("export-env", "--target-json", json.dumps({"env": {"RUSTFS_S3_STACK": "gateway"}}))
        self.assertEqual((code, out), (0, "RUSTFS_S3_STACK=gateway\n"))


class FailedTests(unittest.TestCase):
    def test_failed_lists_only_failures_in_test_list_form(self):
        lane = Lane(self)
        directory = lane.target("reference", {**BASE, "test_a": "fail", "test_d": "error"})
        code, out, _ = run("failed", "--report", str(directory))
        self.assertEqual((code, out), (0, "test_a\ntest_d\n"))

    def test_failed_rejects_a_missing_report(self):
        code, _, err = run("failed", "--report", "/nonexistent/report")
        self.assertEqual(code, 2)
        self.assertIn("report directory is missing", err)


if __name__ == "__main__":
    unittest.main()
