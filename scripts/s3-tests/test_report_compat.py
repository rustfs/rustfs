#!/usr/bin/env python3
"""Regression tests for the S3 compatibility report."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPORT_PATH = Path(__file__).with_name("report_compat.py")
SPEC = importlib.util.spec_from_file_location("report_compat", REPORT_PATH)
assert SPEC and SPEC.loader
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


class ReportCompatTests(unittest.TestCase):
    def test_upstream_names_expose_incomplete_junit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            collected = directory / "collected.txt"
            collected.write_text("s3tests/functional/test_s3.py::test_one[a]\ns3tests/functional/test_s3.py::test_one[b]\n")
            junit = directory / "junit.xml"
            junit.write_text('<testsuite><testcase name="test_one[a]" /></testsuite>')

            expected = REPORT.load_collected_nodeids(collected)
            results, _, _ = REPORT.parse_junit(junit)

            self.assertEqual(expected - results.keys(), {"test_one[b]"})

    def test_cli_fails_an_incomplete_sweep(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            collected = directory / "collected.txt"
            collected.write_text("s3tests/functional/test_s3.py::test_one\ns3tests/functional/test_s3.py::test_two\n")
            junit = directory / "junit.xml"
            junit.write_text('<testsuite><testcase name="test_one" /></testsuite>')
            for filename in REPORT.LIST_FILES.values():
                (directory / filename).write_text("")
            (directory / "implemented_tests.txt").write_text("test_one\n")

            result = subprocess.run(
                [
                    sys.executable,
                    str(REPORT_PATH),
                    "--junit",
                    str(junit),
                    "--lists-dir",
                    str(directory),
                    "--collected-nodeids",
                    str(collected),
                    "--fail-on-regression",
                    "--fail-on-unclassified",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1)
            self.assertIn("1 missing result(s)", result.stdout)

    def test_preflight_rejects_missing_and_stale_classifications(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            collected = directory / "collected.txt"
            collected.write_text("s3tests/functional/test_s3.py::test_known[a]\ntest_new\n")
            for filename in REPORT.LIST_FILES.values():
                (directory / filename).write_text("")
            (directory / "implemented_tests.txt").write_text("test_known\ntest_stale\ntest_stale\n")

            result = subprocess.run(
                [
                    sys.executable,
                    str(REPORT_PATH),
                    "--lists-dir",
                    str(directory),
                    "--collected-nodeids",
                    str(collected),
                    "--check-classifications-only",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1)
            self.assertIn("[UNCLASSIFIED] test_new", result.stdout)
            self.assertIn("[STALE] test_stale", result.stdout)
            self.assertIn("[INVALID] implemented_tests.txt has duplicates: test_stale", result.stdout)

    def test_timeout_fails_even_when_test_is_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            junit = directory / "junit.xml"
            junit.write_text(
                '<testsuite><testcase name="test_slow"><failure message="Failed: Timeout (&gt;300.0s)" /></testcase></testsuite>'
            )
            for filename in REPORT.LIST_FILES.values():
                (directory / filename).write_text("")
            (directory / "excluded_tests.txt").write_text("test_slow\n")

            result = subprocess.run(
                [sys.executable, str(REPORT_PATH), "--junit", str(junit), "--lists-dir", str(directory)],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1)
            self.assertIn("1 timeout(s)", result.stdout)


if __name__ == "__main__":
    unittest.main()
