#!/usr/bin/env python3
"""Verify migration proofs already passed in the workspace nextest run."""

import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


SUITE = "rustfs-ecstore"
NAME_PARTS = ("data_movement", "rebalance", "decommission", "source_cleanup", "delete_marker")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def library_suite(path):
    listing = json.loads(Path(path).read_text(), object_pairs_hook=unique_object)
    suite = listing["rust-suites"][SUITE]
    if any(suite.get(key) != value for key, value in {
        "package-name": SUITE, "binary-id": SUITE, "kind": "lib", "status": "listed",
    }.items()) or not isinstance(suite.get("testcases"), dict):
        raise ValueError(f"{path}: expected the listed {SUITE} library test binary")
    return suite["testcases"]


def verify(core_path, junit_path, floor_path):
    floor_lines = [line.strip() for line in Path(floor_path).read_text().splitlines()
                   if line.strip() and not line.lstrip().startswith("#")]
    if len(floor_lines) != 1 or not floor_lines[0].isdigit() or int(floor_lines[0]) <= 0:
        raise ValueError("migration floor must contain one positive integer")
    floor = int(floor_lines[0])
    expected = set()
    for name, case in library_suite(core_path).items():
        if not any(part in name for part in NAME_PARTS) or case.get("ignored") is True:
            continue
        if (case.get("kind") != "test" or case.get("ignored") is not False
                or case.get("filter-match", {}).get("status") != "matches"):
            raise ValueError(f"{name}: migration test is malformed or filtered from the core run")
        expected.add(name)
    if len(expected) < floor:
        raise ValueError(f"migration selection has {len(expected)} tests, below the committed floor {floor}")

    report = ET.parse(junit_path).getroot()
    if (report.tag != "testsuites" or int(report.get("tests", "0")) < len(expected)
            or report.get("failures") != "0" or report.get("errors") != "0"):
        raise ValueError("core JUnit must report a successful nonempty nextest run")
    suites = [suite for suite in report.findall("testsuite") if suite.get("name") == SUITE]
    if len(suites) != 1:
        raise ValueError("core JUnit must contain exactly one migration library suite")
    suite_cases = set(suites[0].findall("testcase"))
    executions = {}
    for case in report.iter("testcase"):
        if case.get("classname") == SUITE:
            executions.setdefault(case.get("name"), []).append(case)
    for name in sorted(expected):
        cases = executions.get(name, [])
        if len(cases) != 1 or cases[0] not in suite_cases:
            raise ValueError(f"{name}: expected exactly one core JUnit execution in the migration library suite")
        case = cases[0]
        if (case.get("status", "passed") != "passed"
                or any(child.tag not in ("system-out", "system-err", "properties") for child in case)):
            raise ValueError(f"{name}: migration proof failed, skipped, or required a retry")
    return len(expected), floor


def main():
    if sys.argv[1:] == ["--filter"]:
        print(" or ".join(f"test({part})" for part in NAME_PARTS))
        return 0
    if len(sys.argv) != 4:
        print("usage: check_migration_gate_evidence.py CORE_LISTING JUNIT FLOOR | --filter", file=sys.stderr)
        return 2
    try:
        count, floor = verify(*sys.argv[1:])
    except (OSError, ValueError, TypeError, KeyError, AttributeError, ET.ParseError) as error:
        print(f"migration gate evidence failed: {error}", file=sys.stderr)
        return 1
    print(f"migration gate evidence OK: {count} proofs passed without retries (floor: {floor})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
