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
"""Contract tests for mint_stack_diff.py: the failing-set gate and the sides that have no result."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "mint_stack_diff.py"
REFUSAL = "gateway stack is not assembled yet (rustfs/backlog#2734 T2.5)"


def entry(suite, function, status):
    return json.dumps({"name": suite, "function": function, "status": status})


def start_log(*events):
    return "\n".join(json.dumps(event) for event in events) + "\n"


STARTED = {"event": "server_starting"}


class Leg:
    """One downloaded mint leg: its log.json lines and the server log its stack proof is read from."""

    def __init__(self, root, stack, results, server=None):
        self.directory = Path(root) / stack
        (self.directory / "mint-log").mkdir(parents=True)
        if results is not None:
            (self.directory / "mint-log" / "log.json").write_text("\n".join(results) + "\n")
        logs = Path(root) / f"{stack}-logs"
        logs.mkdir()
        events = server if server is not None else [{**STARTED, "s3_stack": stack.capitalize()}]
        (logs / "rustfs.log").write_text(start_log(*events))
        subprocess.run([sys.executable, str(SCRIPT.parent / "dual_stack.py"), "stack", "--expect", stack,
                        "--out", str(self.directory / "stack.json"), str(logs)], capture_output=True, check=False)


def compare(legacy, gateway, **sides):
    with tempfile.TemporaryDirectory() as tmp:
        legs = {"legacy": Leg(tmp, "legacy", legacy, sides.get("legacy_server")),
                "gateway": Leg(tmp, "gateway", gateway, sides.get("gateway_server"))}
        proc = subprocess.run([sys.executable, str(SCRIPT), str(legs["legacy"].directory), str(legs["gateway"].directory)],
                              capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


BASE = [entry("awscli", "put", "PASS"), entry("awscli", "get", "FAIL"), entry("mc", "ls", "PASS")]


class MintStackDiffTests(unittest.TestCase):
    def test_a_case_failing_only_on_gateway_breaks_the_gate_and_is_named(self):
        code, out, _ = compare(BASE, [entry("awscli", "put", "FAIL"), entry("awscli", "get", "FAIL"), entry("mc", "ls", "PASS")])
        self.assertEqual(code, 1)
        self.assertIn("Passing on legacy, failing on gateway (1):\n  awscli::put", out)
        self.assertIn("Gate NOT met", out)

    def test_missing_gateway_result_never_satisfies_the_gate(self):
        code, out, _ = compare(BASE, None)
        self.assertEqual(code, 1)
        self.assertIn("no result for gateway", out)
        self.assertNotIn("Gate met", out)

    def test_empty_gateway_result_never_satisfies_the_gate(self):
        code, out, _ = compare(BASE, [""])
        self.assertEqual(code, 1)
        self.assertIn("no result for gateway", out)

    def test_missing_legacy_result_fails_the_gate(self):
        code, out, _ = compare(None, BASE)
        self.assertEqual(code, 1)
        self.assertIn("no result for legacy", out)

    def test_gateway_that_refuses_startup_is_skipped_with_its_message(self):
        refused = [{"level": "ERROR", "message": REFUSAL}]
        code, out, _ = compare(BASE, None, gateway_server=refused)
        self.assertEqual(code, 1)
        self.assertIn(f"gateway: skipped: gateway stack refuses startup: {REFUSAL}", out)

    def test_a_gateway_that_ran_mint_without_a_proven_start_is_not_skipped(self):
        code, out, _ = compare(BASE, BASE, gateway_server=[{"level": "ERROR", "message": REFUSAL}])
        self.assertEqual(code, 1)
        self.assertIn(f"gateway: server did not start: {REFUSAL}", out)

    def test_results_from_a_server_of_the_wrong_stack_are_not_a_result(self):
        code, out, _ = compare(BASE, BASE, gateway_server=[{**STARTED, "s3_stack": "Legacy"}])
        self.assertEqual(code, 1)
        self.assertIn("started on another stack", out)
        self.assertNotIn("Gate met", out)

    def test_a_case_with_one_failing_entry_fails(self):
        gateway = [entry("awscli", "put", "PASS"), entry("awscli", "put", "FAIL"), entry("awscli", "get", "FAIL")]
        code, out, _ = compare(BASE, gateway)
        self.assertEqual(code, 1)
        self.assertIn("awscli::put", out)

    def test_truncated_gateway_result_does_not_hold_the_gate(self):
        legacy = [entry("s", f"f{n}", "PASS") for n in range(100)] + [entry("s", "bad", "FAIL")]
        code, out, _ = compare(legacy, [entry("s", f"f{n}", "PASS") for n in range(3)])
        self.assertEqual(code, 1)
        self.assertIn("Passing on legacy, not run on gateway (97):", out)

    def test_all_na_gateway_result_does_not_hold_the_gate(self):
        legacy = [entry("s", f"f{n}", "PASS") for n in range(100)]
        code, out, _ = compare(legacy, [entry("s", f"f{n}", "NA") for n in range(100)])
        self.assertEqual(code, 1)
        self.assertIn("Passing on legacy, n/a on gateway (100):", out)

    def test_unknown_gateway_status_is_bad_input(self):
        code, _, err = compare(BASE, [entry("awscli", "put", "ERROR")])
        self.assertEqual(code, 2)
        self.assertIn("unknown status", err)

    def test_unparsable_result_line_is_bad_input(self):
        code, _, err = compare(BASE, [entry("awscli", "put", "PASS"), '{"name": "mc", "function": "ls", "sta'])
        self.assertEqual(code, 2)
        self.assertIn("unparsable", err)

    def test_a_new_gateway_failure_where_legacy_never_ran_the_case_breaks_the_gate(self):
        code, out, _ = compare([entry("s", "a", "PASS")], [entry("s", "a", "PASS"), entry("s", "b", "FAIL")])
        self.assertEqual(code, 1)
        self.assertIn("Failing on gateway, not run or n/a on legacy (1):\n  s::b", out)

    def test_plain_progress_lines_in_the_result_file_are_ignored(self):
        code, _, _ = compare(["Dependency validation complete"] + BASE, BASE)
        self.assertEqual(code, 0)

    def test_gateway_failures_equal_to_legacy_hold_the_gate(self):
        code, out, _ = compare(BASE, BASE)
        self.assertEqual(code, 0)
        self.assertIn("Gate met", out)

    def test_gateway_failing_less_holds_the_gate_and_lists_the_difference(self):
        code, out, _ = compare(BASE, [entry("awscli", "put", "PASS"), entry("awscli", "get", "PASS"), entry("mc", "ls", "PASS")])
        self.assertEqual(code, 0)
        self.assertIn("Failing on legacy, passing on gateway (1):\n  awscli::get", out)


if __name__ == "__main__":
    unittest.main()
