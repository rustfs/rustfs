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

"""Diff a ceph/s3-tests junit.xml result against the RustFS test classification lists.

Classifies every executed test into:
  - regressions:            listed in implemented_tests.txt but failed/errored
  - promotion candidates:   passed but listed in unimplemented_tests.txt / excluded_tests.txt
  - unclassified passes:    passed but not present in any list (new upstream tests)
  - unclassified failures:  failed and not present in any list (new upstream tests)

Writes a markdown report and prints a summary to stdout. Exit code is 0 unless
--fail-on-regression is given and at least one regression was found.
"""

from __future__ import annotations

import argparse
import pathlib
import sys
import xml.etree.ElementTree as ET

LIST_FILES = {
    "implemented": "implemented_tests.txt",
    "unimplemented": "unimplemented_tests.txt",
    "excluded": "excluded_tests.txt",
}


def load_list(path: pathlib.Path) -> set[str]:
    names: set[str] = set()
    if not path.is_file():
        return names
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.add(line)
    return names


def base_name(testcase_name: str) -> str:
    """Strip pytest parametrization (test_foo[param]) to match list entries."""
    return testcase_name.split("[", 1)[0]


def parse_junit(path: pathlib.Path) -> dict[str, str]:
    """Return {test name: status} with status in passed/failed/error/skipped.

    Parametrized cases collapse onto their base name; any failing variant marks
    the whole test failed.
    """
    results: dict[str, str] = {}
    severity = {"skipped": 0, "passed": 1, "failed": 2, "error": 2}
    root = ET.parse(path).getroot()
    for case in root.iter("testcase"):
        name = base_name(case.get("name", ""))
        if not name:
            continue
        if case.find("failure") is not None:
            status = "failed"
        elif case.find("error") is not None:
            status = "error"
        elif case.find("skipped") is not None:
            status = "skipped"
        else:
            status = "passed"
        prev = results.get(name)
        if prev is None or severity[status] > severity[prev]:
            results[name] = status
    return results


def render_section(title: str, rows: list[str], hint: str = "") -> list[str]:
    lines = [f"## {title} ({len(rows)})", ""]
    if hint:
        lines += [hint, ""]
    if rows:
        lines += [f"- `{name}`" for name in sorted(rows)]
    else:
        lines.append("_none_")
    lines.append("")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit", required=True, type=pathlib.Path, help="junit.xml produced by pytest")
    parser.add_argument(
        "--lists-dir",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parent,
        help="directory containing the *_tests.txt classification files",
    )
    parser.add_argument("--output", type=pathlib.Path, help="write the markdown report to this path")
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="exit non-zero when a test from implemented_tests.txt failed",
    )
    args = parser.parse_args()

    if not args.junit.is_file():
        print(f"[ERROR] junit file not found: {args.junit}", file=sys.stderr)
        return 2

    lists = {key: load_list(args.lists_dir / fname) for key, fname in LIST_FILES.items()}
    results = parse_junit(args.junit)

    regressions: list[str] = []
    promotions: dict[str, list[str]] = {"unimplemented": [], "excluded": []}
    unclassified_passed: list[str] = []
    unclassified_failed: list[str] = []
    counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0}

    for name, status in results.items():
        counts[status] += 1
        if status in ("failed", "error"):
            if name in lists["implemented"]:
                regressions.append(name)
            elif name not in lists["unimplemented"] and name not in lists["excluded"]:
                unclassified_failed.append(name)
        elif status == "passed":
            if name in lists["unimplemented"]:
                promotions["unimplemented"].append(name)
            elif name in lists["excluded"]:
                promotions["excluded"].append(name)
            elif name not in lists["implemented"]:
                unclassified_passed.append(name)

    lines = [
        "# S3 compatibility report",
        "",
        f"Executed: {len(results)} tests — "
        f"{counts['passed']} passed, {counts['failed']} failed, "
        f"{counts['error']} errored, {counts['skipped']} skipped.",
        "",
    ]
    lines += render_section(
        "Regressions",
        regressions,
        "Listed in `implemented_tests.txt` but failing — these gate PRs and must be fixed.",
    )
    lines += render_section(
        "Promotion candidates (from unimplemented_tests.txt)",
        promotions["unimplemented"],
        "Now passing — move to `implemented_tests.txt` to lock in the coverage.",
    )
    lines += render_section(
        "Promotion candidates (from excluded_tests.txt)",
        promotions["excluded"],
        "Passing despite being excluded — re-evaluate the exclusion.",
    )
    lines += render_section(
        "Unclassified passes",
        unclassified_passed,
        "Passing but absent from every list — add to `implemented_tests.txt`.",
    )
    lines += render_section(
        "Unclassified failures",
        unclassified_failed,
        "Failing and absent from every list — triage into `unimplemented_tests.txt` or `excluded_tests.txt`.",
    )

    report = "\n".join(lines)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"[INFO] Report written to {args.output}")

    print(
        f"[INFO] {len(regressions)} regression(s), "
        f"{len(promotions['unimplemented']) + len(promotions['excluded']) + len(unclassified_passed)} promotion candidate(s), "
        f"{len(unclassified_failed)} unclassified failure(s)"
    )
    for name in sorted(regressions):
        print(f"[REGRESSION] {name}")

    if args.fail_on_regression and regressions:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
