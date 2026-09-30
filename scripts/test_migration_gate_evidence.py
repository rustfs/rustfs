#!/usr/bin/env python3
"""Check that reusing core test evidence cannot silently drop migration proofs."""

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET

from check_migration_gate_evidence import NAME_PARTS, SUITE, verify


class MigrationEvidenceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.listing = self.root / "core.json"
        self.junit = self.root / "junit.xml"
        self.floor = self.root / "floor.txt"
        self.floor.write_text("# Existing floor\n2\n")
        self.names = ("store::rebalance_commits", "object::delete_marker_preserves_version")
        self.suite = {
            "package-name": SUITE, "binary-id": SUITE, "kind": "lib", "status": "listed",
            "testcases": {name: self.runnable_case() for name in self.names},
        }
        self.data = {"rust-suites": {SUITE: self.suite}}
        self.write_listing()
        self.write_junit(self.names)

    @staticmethod
    def runnable_case():
        return {"kind": "test", "ignored": False, "filter-match": {"status": "matches"}}

    def write_listing(self):
        self.listing.write_text(json.dumps(self.data))

    def write_junit(self, names):
        report = ET.Element("testsuites", tests=str(len(names)), failures="0", errors="0")
        suite = ET.SubElement(report, "testsuite", name=SUITE)
        for name in names:
            ET.SubElement(suite, "testcase", name=name, classname=SUITE)
        ET.ElementTree(report).write(self.junit)

    def check(self):
        return verify(self.listing, self.junit, self.floor)

    def test_successful_core_evidence_meets_the_existing_floor(self):
        self.assertEqual(self.check(), (2, 2))

    def test_substring_selection_matches_the_canonical_nextest_filter(self):
        names = [f"nested::prefix_{part}_suffix" for part in NAME_PARTS]
        self.suite["testcases"] = {name: self.runnable_case() for name in names}
        for name in ("nested::Rebalance", "nested::rebalancing", "nested::ordinary_test"):
            self.suite["testcases"][name] = self.runnable_case()
        self.data["rust-suites"]["other-package"] = copy.deepcopy(self.suite)
        self.write_listing()
        self.write_junit(names)
        self.assertEqual(self.check(), (5, 2))
        script = Path(__file__).with_name("check_migration_gate_evidence.py")
        result = subprocess.run([sys.executable, str(script), "--filter"], capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout.strip(),
                         "test(data_movement) or test(rebalance) or test(decommission) or test(source_cleanup) or test(delete_marker)")

    def test_ignored_tests_do_not_count_towards_the_floor(self):
        self.suite["testcases"]["store::rebalance_ignored"] = dict(self.runnable_case(), ignored=True)
        self.write_listing()
        self.assertEqual(self.check(), (2, 2))
        self.suite["testcases"][self.names[0]]["ignored"] = True
        self.write_listing()
        with self.assertRaisesRegex(ValueError, "below the committed floor"):
            self.check()

    def test_filtered_migration_test_is_rejected_even_above_the_floor(self):
        case = self.runnable_case()
        case["filter-match"] = {"status": "mismatch", "reason": "expression"}
        self.suite["testcases"]["store::rebalance_filtered"] = case
        self.write_listing()
        with self.assertRaisesRegex(ValueError, "filtered"):
            self.check()

    def test_wrong_library_identity_and_unlisted_suite_are_rejected(self):
        for key, value in (("package-name", "impostor"), ("binary-id", "other"), ("kind", "test"), ("status", "skipped")):
            with self.subTest(key=key):
                bad = copy.deepcopy(self.data)
                bad["rust-suites"][SUITE][key] = value
                self.listing.write_text(json.dumps(bad))
                with self.assertRaisesRegex(ValueError, "library test binary"):
                    self.check()

    def test_empty_malformed_and_duplicate_listing_inputs_fail(self):
        for value in ("", "[]", "{}", '{"rust-suites":{},"rust-suites":{}}'):
            with self.subTest(value=value):
                self.listing.write_text(value)
                with self.assertRaises((ValueError, KeyError, TypeError)):
                    self.check()
        self.suite["testcases"] = {}
        self.write_listing()
        with self.assertRaisesRegex(ValueError, "below the committed floor"):
            self.check()

    def test_missing_duplicate_and_wrong_junit_test_identity_fail(self):
        for fault in ("missing", "duplicate", "wrong-class", "wrong-suite", "duplicate-suite"):
            with self.subTest(fault=fault):
                self.write_junit(self.names)
                report = ET.parse(self.junit)
                suite = report.getroot().find("testsuite")
                case = suite.find("testcase")
                if fault == "missing":
                    suite.remove(case)
                elif fault == "duplicate":
                    suite.append(copy.deepcopy(case))
                elif fault == "wrong-class":
                    case.set("classname", "impostor")
                elif fault == "wrong-suite":
                    suite.set("name", "impostor")
                else:
                    report.getroot().append(copy.deepcopy(suite))
                report.write(self.junit)
                with self.assertRaises(ValueError):
                    self.check()

    def test_failed_skipped_or_retried_proofs_are_not_successes(self):
        for tag in ("failure", "error", "skipped", "rerunFailure", "rerunError", "flakyFailure", "flakyError"):
            with self.subTest(tag=tag):
                self.write_junit(self.names)
                report = ET.parse(self.junit)
                ET.SubElement(report.getroot().find("testsuite/testcase"), tag)
                report.write(self.junit)
                with self.assertRaisesRegex(ValueError, "failed, skipped, or required a retry"):
                    self.check()

    def test_nonempty_successful_junit_and_positive_floor_are_required(self):
        for content in ("", "<testsuites/>", '<testsuites tests="2" failures="1" errors="0"/>'):
            self.junit.write_text(content)
            with self.assertRaises((ValueError, ET.ParseError)):
                self.check()
        self.write_junit(self.names)
        for content in ("", "0", "-1", "2\n3", "invalid"):
            self.floor.write_text(content)
            with self.assertRaisesRegex(ValueError, "positive integer"):
                self.check()

    def test_evidence_shell_mode_never_invokes_cargo(self):
        scripts = self.root / "scripts"
        scripts.mkdir()
        source_dir = Path(__file__).resolve().parent
        for name in ("check_migration_gate_count.sh", "check_migration_gate_evidence.py"):
            shutil.copy(source_dir / name, scripts / name)
        (self.root / ".config").mkdir()
        shutil.copy(self.floor, self.root / ".config/migration-gate-floor.txt")
        commands = self.root / "commands"
        commands.mkdir()
        cargo = commands / "cargo"
        cargo.write_text("#!/bin/sh\necho unexpected cargo invocation >&2\nexit 99\n")
        cargo.chmod(0o755)
        env = dict(os.environ, PATH=f"{commands}{os.pathsep}{os.environ['PATH']}")
        result = subprocess.run(["bash", str(scripts / "check_migration_gate_count.sh"), "evidence",
                                 str(self.listing), str(self.junit)], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("2 proofs passed without retries", result.stdout)


if __name__ == "__main__":
    unittest.main()
