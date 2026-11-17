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
"""Contract tests for junit_diff.py: exit codes, the difference table and the JSON summary."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent / "junit_diff.py"
FIXTURES = Path(__file__).resolve().parent / "tests" / "junit_diff"


def run(a, b, *extra):
    """Run the script on two fixture names; return (exit code, stdout, stderr)."""
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), str(FIXTURES / a), str(FIXTURES / b), *extra],
        capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


class JunitDiffTests(unittest.TestCase):
    # --- negative cases: differences and bad input ---

    def test_pass_vs_fail_is_difference(self):
        code, out, _ = run("base.xml", "pass_fail.xml")
        self.assertEqual(code, 1)
        self.assertIn("rustfs::s3::alpha", out)
        self.assertRegex(out, r"rustfs::s3::alpha\s+pass\s+fail")

    def test_pass_vs_error_counts_as_fail(self):
        code, out, _ = run("base.xml", "pass_error.xml")
        self.assertEqual(code, 1)
        self.assertRegex(out, r"rustfs::s3::alpha\s+pass\s+fail")

    def test_pass_vs_skip_is_difference(self):
        code, out, _ = run("base.xml", "pass_skip.xml")
        self.assertEqual(code, 1)
        self.assertRegex(out, r"rustfs::s3::alpha\s+pass\s+skip")

    def test_case_missing_on_b_is_difference(self):
        code, out, _ = run("base.xml", "missing_case.xml")
        self.assertEqual(code, 1)
        self.assertRegex(out, r"rustfs::s3::beta\s+pass\s+missing")

    def test_case_missing_on_a_is_difference(self):
        code, out, _ = run("missing_case.xml", "base.xml")
        self.assertEqual(code, 1)
        self.assertRegex(out, r"rustfs::s3::beta\s+missing\s+pass")

    def test_rerun_failure_final_fail_is_difference(self):
        code, out, _ = run("base.xml", "rerun_fail.xml")
        self.assertEqual(code, 1)
        self.assertRegex(out, r"rustfs::s3::alpha\s+pass\s+fail")

    def test_malformed_xml_exits_2(self):
        for a, b in (("malformed.xml", "base.xml"), ("base.xml", "malformed.xml")):
            code, out, err = run(a, b)
            self.assertEqual(code, 2, (out, err))
            self.assertNotIn("No differences", out)

    def test_empty_file_exits_2(self):
        code, _, err = run("base.xml", "empty.xml")
        self.assertEqual(code, 2)
        self.assertIn("empty.xml", err)

    def test_report_without_cases_exits_2(self):
        code, _, _ = run("no_cases.xml", "no_cases.xml")
        self.assertEqual(code, 2)

    def test_unexpected_root_exits_2(self):
        code, _, _ = run("wrong_root.xml", "base.xml")
        self.assertEqual(code, 2)

    def test_nonexistent_file_exits_2(self):
        code, _, _ = run("base.xml", "does_not_exist.xml")
        self.assertEqual(code, 2)

    def test_absent_side_is_named_by_its_label(self):
        labels = ("--label-a", "legacy", "--label-b", "gateway")
        absent = ("does_not_exist.xml", "empty.xml", "no_cases.xml")
        for name in absent:
            for a, b, stack in (("base.xml", name, "gateway"), (name, "base.xml", "legacy")):
                code, out, err = run(a, b, *labels)
                self.assertEqual(code, 2, (a, b, out, err))
                self.assertIn(f"no result for {stack}", err)
                self.assertNotIn("No differences", out)

    def test_unusable_report_is_not_reported_as_absent(self):
        code, _, err = run("base.xml", "malformed.xml", "--label-a", "legacy", "--label-b", "gateway")
        self.assertEqual(code, 2)
        self.assertNotIn("no result", err)

    def test_stale_allow_entry_fails_run(self):
        code, out, _ = run("base.xml", "pass_fail.xml", "--allow", str(FIXTURES / "allow_stale.txt"))
        self.assertEqual(code, 1)
        self.assertIn("rustfs::s3::nonexistent", out)
        self.assertIn("stale", out.lower())

    def test_allow_entry_without_any_difference_is_stale(self):
        code, out, _ = run("base.xml", "base.xml", "--allow", str(FIXTURES / "allow_ok.txt"))
        self.assertEqual(code, 1)
        self.assertIn("stale", out.lower())

    def test_allow_does_not_cover_other_differences(self):
        code, out, _ = run("base.xml", "missing_case.xml", "--allow", str(FIXTURES / "allow_ok.txt"))
        self.assertEqual(code, 1)

    def test_unreadable_allow_file_exits_2(self):
        code, _, _ = run("base.xml", "base.xml", "--allow", str(FIXTURES / "nope.txt"))
        self.assertEqual(code, 2)

    # --- positive cases ---

    def test_identical_files_exit_0(self):
        code, out, _ = run("base.xml", "base.xml")
        self.assertEqual(code, 0)
        self.assertIn("No differences", out)

    def test_bare_testsuite_root_matches_nested(self):
        code, _, _ = run("base.xml", "bare_suite.xml")
        self.assertEqual(code, 0)

    def test_allowed_difference_exits_0(self):
        code, out, _ = run("base.xml", "pass_fail.xml", "--allow", str(FIXTURES / "allow_ok.txt"))
        self.assertEqual(code, 0)
        self.assertIn("allowed", out.lower())

    def test_allowed_missing_case_exits_0(self):
        code, _, _ = run("base.xml", "missing_case.xml", "--allow", str(FIXTURES / "allow_missing_case.txt"))
        self.assertEqual(code, 0)

    def test_flaky_pass_listed_not_a_difference(self):
        code, out, _ = run("base.xml", "flaky_pass.xml")
        self.assertEqual(code, 0)
        self.assertIn("rustfs::s3::alpha", out)
        self.assertIn("flaky", out.lower())

    # --- options and summary ---

    def test_labels_appear_in_table(self):
        code, out, _ = run("base.xml", "pass_skip.xml", "--label-a", "reference", "--label-b", "candidate")
        self.assertEqual(code, 1)
        self.assertIn("reference", out)
        self.assertIn("candidate", out)

    def test_json_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.json"
            code, _, _ = run("base.xml", "flaky_pass.xml", "--json", str(path),
                             "--label-a", "reference", "--label-b", "candidate")
            self.assertEqual(code, 0)
            data = json.loads(path.read_text())
        self.assertEqual(data["counts"]["reference"], {"pass": 3, "fail": 0, "skip": 1})
        self.assertEqual(data["counts"]["candidate"], {"pass": 3, "fail": 0, "skip": 1})
        self.assertEqual(data["differences"], [])
        self.assertEqual(data["flaky"]["candidate"], ["rustfs::s3::alpha"])
        self.assertEqual(data["flaky"]["reference"], [])
        self.assertEqual(data["stale_allow"], [])

    def test_json_records_differences(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.json"
            code, _, _ = run("base.xml", "missing_case.xml", "--json", str(path))
            self.assertEqual(code, 1)
            data = json.loads(path.read_text())
        self.assertEqual(data["differences"],
                         [{"case": "rustfs::s3::beta", "a": "pass", "b": "missing", "allowed": False}])
        self.assertEqual(data["counts"]["b"], {"pass": 2, "fail": 0, "skip": 1})


if __name__ == "__main__":
    unittest.main()
