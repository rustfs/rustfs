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
"""Contract tests for dual_stack.py: the profile table, the stack proof and the per-profile summary."""

import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "dual_stack.py"
CONFIG = ROOT / ".config" / "dual-stack-profiles.toml"
NEXTEST = ROOT / ".config" / "nextest.toml"
REFUSAL = "gateway stack is not assembled yet (rustfs/backlog#2734 T2.5)"

sys.path.insert(0, str(ROOT / "scripts"))
import dual_stack  # noqa: E402


def run(*args):
    proc = subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def server_log(*events):
    return "\n".join(json.dumps(event) for event in events) + "\n"


def started(stack=None):
    event = {"level": "INFO", "event": "server_starting", "message": "Starting RustFS server"}
    if stack is not None:
        event["s3_stack"] = stack
    return event


def junit(cases):
    body = ""
    for name, state in cases.items():
        inner = {"pass": "", "fail": "<failure message='x'/>", "skip": "<skipped/>"}[state]
        body += f"<testcase classname='e2e' name='{name}'>{inner}</testcase>"
    return f"<testsuites><testsuite name='s'>{body}</testsuite></testsuites>"


class Tree:
    """A downloaded-legs directory with one profile and both of its legs."""

    def __init__(self, tmp, profile="e2e-smoke"):
        self.root = Path(tmp)
        self.profile = profile
        self.config = self.root / "profiles.toml"
        self.config.write_text(
            "schema = 1\n[[profile]]\n"
            f'name = "{profile}"\nsource = "s"\nrunner = "r"\ntimeout_minutes = 1\n'
            'features = ""\nsetup = []\nnextest_args = ""\nfilter = ""\n'
        )

    def leg(self, stack):
        return self.root / "legs" / f"dual-stack-leg-{self.profile}-{stack}"

    def write(self, stack, cases, evidence=None):
        directory = self.leg(stack)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "junit.xml").write_text(junit(cases))
        logs = self.root / f"logs-{stack}"
        logs.mkdir(exist_ok=True)
        (logs / "node.log").write_text(server_log(started(stack.capitalize())) if evidence is None else evidence)
        code, _, _ = run("stack", "--expect", stack, "--out", str(directory / "stack.json"), str(logs))
        return code

    def summary(self):
        code, out, err = run("summary", "--config", str(self.config), "--legs", str(self.root / "legs"),
                             "--date", "2026-11-13", "--out-dir", str(self.root / "out"))
        report = self.root / "out" / "2026-11-13.json"
        return code, out + err, json.loads(report.read_text()) if report.is_file() else None


class ProfileTableTests(unittest.TestCase):
    def test_table_lists_exactly_the_e2e_profiles_of_nextest(self):
        declared = set(re.findall(r"^\[profile\.(e2e-[a-z0-9-]+)\]$", NEXTEST.read_text(), re.MULTILINE))
        listed = {item["name"] for item in dual_stack.load_profiles(CONFIG)}
        self.assertEqual(listed, declared)

    def test_matrix_runs_every_profile_on_both_stacks(self):
        include = dual_stack.matrix(dual_stack.load_profiles(CONFIG))["include"]
        pairs = {(item["profile"], item["stack"]) for item in include}
        self.assertEqual(len(include), len(pairs))
        self.assertEqual({stack for _, stack in pairs}, {"legacy", "gateway"})
        self.assertEqual(len(pairs), 2 * len(dual_stack.load_profiles(CONFIG)))

    def test_malformed_tables_are_refused(self):
        entry = ('name = "e2e-a"\nsource = "s"\nrunner = "r"\ntimeout_minutes = 1\n'
                 'features = ""\nsetup = []\nnextest_args = ""\nfilter = ""\n')
        bad = {
            "unknown key": f"schema = 1\n[[profile]]\n{entry}extra = 1\n",
            "duplicate name": f"schema = 1\n[[profile]]\n{entry}[[profile]]\n{entry}",
            "not an e2e name": f"schema = 1\n[[profile]]\n{entry.replace('e2e-a', 'ci')}",
            "unknown setup tag": f"schema = 1\n[[profile]]\n{entry.replace('setup = []', 'setup = [\"x\"]')}",
            "zero timeout": f"schema = 1\n[[profile]]\n{entry.replace('timeout_minutes = 1', 'timeout_minutes = 0')}",
            "no profiles": "schema = 1\n",
            "wrong schema": f"schema = 2\n[[profile]]\n{entry}",
        }
        for label, text in bad.items():
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "p.toml"
                path.write_text(text)
                code, _, err = run("matrix", "--config", str(path))
                self.assertEqual(code, 2, (label, err))


class StackProofTests(unittest.TestCase):
    def prove(self, expect, files):
        with tempfile.TemporaryDirectory() as tmp:
            for name, text in files.items():
                (Path(tmp) / name).write_text(text)
            out = Path(tmp) / "evidence.json"
            code, _, err = run("stack", "--expect", expect, "--out", str(out), tmp)
            return code, json.loads(out.read_text()) if out.is_file() else None, err

    def test_a_server_of_the_other_stack_fails_the_proof(self):
        files = {"a.log": server_log(started("Legacy")), "b.log": server_log(started("Gateway"))}
        code, evidence, _ = self.prove("legacy", files)
        self.assertEqual(code, 1)
        self.assertEqual(evidence["foreign_starts"], 1)

    def test_the_other_direction_fails_too(self):
        code, evidence, _ = self.prove("gateway", {"a.log": server_log(started("Legacy"), started("Gateway"))})
        self.assertEqual((code, evidence["foreign_starts"]), (1, 1))

    def test_a_refused_start_proves_nothing_and_keeps_the_message(self):
        # The shape a refusing server logs: the cause is in `error`, `message` is generic.
        log = server_log({"level": "INFO", "message": "obs ready"},
                         {"level": "ERROR", "message": "Server runtime failed", "event": "server_runtime_failed", "error": REFUSAL})
        code, evidence, _ = self.prove("gateway", {"a.log": log})
        self.assertEqual(code, 1)
        self.assertEqual(evidence["not_started"], [{"message": REFUSAL, "logs": 1}])

    def test_unstamped_starts_alone_prove_nothing(self):
        code, evidence, _ = self.prove("legacy", {"old.log": server_log(started())})
        self.assertEqual((code, evidence["unstamped_starts"]), (1, 1))

    def test_empty_directory_is_an_input_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, _, err = run("stack", "--expect", "legacy", "--out", str(Path(tmp) / "e.json"), tmp)
            self.assertEqual(code, 2)
            self.assertIn("nothing proves", err)

    def test_missing_path_is_an_input_error(self):
        code, _, _ = run("stack", "--expect", "legacy", "--out", "/dev/null", "/nonexistent-dual-stack-logs")
        self.assertEqual(code, 2)

    def test_every_server_on_the_requested_stack_passes(self):
        files = {"a.log": server_log(started("Legacy")), "b.log": server_log(started("Legacy"), started("Legacy"))}
        code, evidence, _ = self.prove("legacy", files)
        self.assertEqual((code, evidence["started"]), (0, {"legacy": 3}))

    def test_older_release_servers_are_counted_not_failed(self):
        files = {"new.log": server_log(started("Gateway")), "old.log": server_log(started())}
        code, evidence, _ = self.prove("gateway", files)
        self.assertEqual((code, evidence["unstamped_starts"]), (0, 1))


class SummaryTests(unittest.TestCase):
    CASES = {"alpha": "pass", "beta": "pass", "gamma": "fail", "delta": "skip"}

    def tree(self, tmp, legacy=None, gateway=None):
        tree = Tree(tmp)
        tree.write("legacy", legacy or self.CASES)
        tree.write("gateway", gateway or self.CASES)
        return tree

    def test_missing_gateway_report_is_no_result_and_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = self.tree(tmp)
            (tree.leg("gateway") / "junit.xml").unlink()
            code, text, report = tree.summary()
        self.assertEqual(code, 1)
        self.assertIn("no result for gateway", text)
        self.assertIsNone(report["profiles"][0]["diff"])

    def test_empty_legacy_report_is_no_result_and_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = self.tree(tmp)
            (tree.leg("legacy") / "junit.xml").write_text("")
            code, text, _ = tree.summary()
        self.assertEqual(code, 1)
        self.assertIn("no result for legacy", text)

    def test_a_leg_that_never_uploaded_is_no_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = Tree(tmp)
            tree.write("legacy", self.CASES)
            code, text, _ = tree.summary()
        self.assertEqual(code, 1)
        self.assertIn("no result for gateway", text)

    def test_gateway_that_never_started_is_reported_not_compared(self):
        refusal = server_log({"level": "ERROR", "message": REFUSAL})
        with tempfile.TemporaryDirectory() as tmp:
            tree = Tree(tmp)
            tree.write("legacy", self.CASES)
            tree.write("gateway", {name: "fail" for name in self.CASES}, evidence=refusal)
            code, text, report = tree.summary()
        self.assertEqual(code, 0, text)
        self.assertIn(f"server did not start: {REFUSAL}", text)
        self.assertNotIn("###", text)
        self.assertEqual(report["profiles"][0]["gateway"]["status"], "not_started")
        self.assertIsNone(report["profiles"][0]["diff"])

    def test_legacy_that_never_started_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = Tree(tmp)
            tree.write("legacy", self.CASES, evidence=server_log({"level": "ERROR", "message": "boom"}))
            tree.write("gateway", self.CASES)
            code, text, _ = tree.summary()
        self.assertEqual(code, 1)
        self.assertIn("server did not start: boom", text)

    def test_a_leg_on_the_wrong_stack_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = Tree(tmp)
            tree.write("legacy", self.CASES)
            tree.write("gateway", self.CASES, evidence=server_log(started("Legacy")))
            code, text, report = tree.summary()
        self.assertEqual(code, 1)
        self.assertEqual(report["profiles"][0]["gateway"]["status"], "wrong_stack")
        self.assertNotIn("No differences", text)

    def test_injected_gateway_only_failure_is_named(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = self.tree(tmp, gateway={**self.CASES, "alpha": "fail"})
            code, text, report = tree.summary()
        self.assertEqual(code, 0, text)
        self.assertIn("Gateway-only failures (1): `e2e::alpha`", text)
        diff = report["profiles"][0]["diff"]
        self.assertEqual((diff["gateway_only_failures"], diff["legacy_only_failures"]), (["e2e::alpha"], []))
        self.assertEqual((diff["cases"], diff["passed_on_both"], diff["failed_on_both"]), (4, 1, 1))

    def test_legacy_only_failure_is_named(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = self.tree(tmp, legacy={**self.CASES, "beta": "fail"})
            _, text, report = tree.summary()
        self.assertIn("Legacy-only failures (1): `e2e::beta`", text)
        self.assertEqual(report["profiles"][0]["diff"]["gateway_only_failures"], [])

    def test_pass_against_skip_and_a_missing_case_are_other_differences(self):
        gateway = {"alpha": "skip", "beta": "pass", "gamma": "fail", "extra": "pass"}
        with tempfile.TemporaryDirectory() as tmp:
            tree = self.tree(tmp, gateway=gateway)
            _, _, report = tree.summary()
        diff = report["profiles"][0]["diff"]
        self.assertEqual(len(diff["other_differences"]), 3)
        self.assertEqual((diff["legacy_only_failures"], diff["gateway_only_failures"]), ([], []))

    def test_named_failures_are_capped_and_counted(self):
        legacy = {f"case{n:03}": "pass" for n in range(dual_stack.NAMED_LIMIT + 5)}
        with tempfile.TemporaryDirectory() as tmp:
            tree = self.tree(tmp, legacy=legacy, gateway={name: "fail" for name in legacy})
            _, text, report = tree.summary()
        self.assertIn("(+5 more in the JSON report)", text)
        self.assertEqual(len(report["profiles"][0]["diff"]["gateway_only_failures"]), dual_stack.NAMED_LIMIT + 5)

    def test_identical_legs_have_no_differences(self):
        with tempfile.TemporaryDirectory() as tmp:
            tree = self.tree(tmp)
            code, text, report = tree.summary()
        self.assertEqual(code, 0, text)
        diff = report["profiles"][0]["diff"]
        self.assertEqual((diff["cases"], diff["passed_on_both"], diff["failed_on_both"]), (4, 2, 1))
        self.assertNotIn("###", text)


if __name__ == "__main__":
    unittest.main()
