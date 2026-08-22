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

Writes a markdown report and prints a summary to stdout. Optional gates reject
regressions, unclassified tests, stale classifications, and incomplete node-ID
execution.
"""

from __future__ import annotations

import argparse
from collections import Counter
import pathlib
import re
import sys
import xml.etree.ElementTree as ET

LIST_FILES = {
    "implemented": "implemented_tests.txt",
    "unimplemented": "unimplemented_tests.txt",
    "excluded": "excluded_tests.txt",
    # Lifecycle behavior lane (backlog#1148 ilm-10): gated by its own ci.yml job
    # against a debug-accelerated server. In the full `scope=all` sweep these run
    # against the plain server (no scanner / no RUSTFS_ILM_DEBUG_DAY_SECS) and are
    # EXPECTED to fail there, so a failure here is neither a regression nor an
    # unclassified failure.
    "behavior": "lifecycle_behavior_tests.txt",
}


def load_entries(path: pathlib.Path) -> list[str]:
    names: list[str] = []
    if not path.is_file():
        return names
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.append(line)
    return names


def classification_errors(entries: dict[str, list[str]]) -> list[str]:
    errors: list[str] = []
    lists = {key: set(names) for key, names in entries.items()}
    for key, names in entries.items():
        duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
        if duplicates:
            errors.append(f"{LIST_FILES[key]} has duplicates: {', '.join(duplicates)}")
    keys = tuple(lists)
    for index, left in enumerate(keys):
        for right in keys[index + 1 :]:
            overlap = sorted(lists[left] & lists[right])
            if overlap:
                errors.append(f"{left}/{right} classifications overlap: {', '.join(overlap)}")
    return errors


def base_name(testcase_name: str) -> str:
    """Strip pytest parametrization (test_foo[param]) to match list entries."""
    return testcase_name.split("[", 1)[0]


def parse_junit(path: pathlib.Path) -> tuple[dict[str, str], list[str], list[tuple[str, str, str, str]]]:
    """Return exact statuses, pytest-timeout cases, and failure summaries."""
    results: dict[str, str] = {}
    timed_out: list[str] = []
    failures: list[tuple[str, str, str, str]] = []
    severity = {"skipped": 0, "passed": 1, "failed": 2, "error": 2}
    root = ET.parse(path).getroot()
    for case in root.iter("testcase"):
        name = case.get("name", "")
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
        node = case.find("failure") if status == "failed" else case.find("error")
        if node is not None:
            details = " ".join(filter(None, [node.get("message", ""), node.text or ""]))
            message = node.get("message") or next(iter((node.text or "").strip().splitlines()), "")
            failures.append((case.get("classname", ""), name, case.get("time", "0"), message))
            if re.search(r"\bTimeout\s*(?:>|\()", details, re.IGNORECASE):
                timed_out.append(name)
    return results, timed_out, failures


def collapse_results(results: dict[str, str]) -> dict[str, str]:
    """Collapse parametrized cases for classification-level reporting."""
    collapsed: dict[str, str] = {}
    severity = {"skipped": 0, "passed": 1, "failed": 2, "error": 2}
    for exact_name, status in results.items():
        name = base_name(exact_name)
        previous = collapsed.get(name)
        if previous is None or severity[status] > severity[previous]:
            collapsed[name] = status
    return collapsed


def load_collected_nodeids(path: pathlib.Path) -> set[str]:
    names: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        nodeid = line.strip()
        if nodeid:
            names.add(nodeid.rsplit("::", 1)[-1])
    return names


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
    parser.add_argument("--junit", type=pathlib.Path, help="junit.xml produced by pytest")
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
    parser.add_argument(
        "--fail-on-unclassified",
        action="store_true",
        help="exit non-zero when an executed test is absent from every classification",
    )
    parser.add_argument(
        "--collected-nodeids",
        type=pathlib.Path,
        help="exact pytest node IDs from the pinned suite's collect-only pass",
    )
    parser.add_argument(
        "--check-classifications-only",
        action="store_true",
        help="validate classification names against collected node IDs without reading JUnit",
    )
    args = parser.parse_args()

    entries = {key: load_entries(args.lists_dir / fname) for key, fname in LIST_FILES.items()}
    lists = {key: set(names) for key, names in entries.items()}
    invalid_classifications = classification_errors(entries)
    collected: set[str] = set()
    if args.collected_nodeids:
        collected = load_collected_nodeids(args.collected_nodeids)
        collected_base = {base_name(name) for name in collected}
        classified = set().union(*lists.values())
        missing_classifications = sorted(collected_base - classified)
        stale_classifications = sorted(classified - collected_base)
    else:
        missing_classifications = []
        stale_classifications = []

    if args.check_classifications_only:
        if not args.collected_nodeids:
            parser.error("--check-classifications-only requires --collected-nodeids")
        for error in invalid_classifications:
            print(f"[INVALID] {error}")
        for name in missing_classifications:
            print(f"[UNCLASSIFIED] {name}")
        for name in stale_classifications:
            print(f"[STALE] {name}")
        return 1 if invalid_classifications or missing_classifications or stale_classifications else 0

    if invalid_classifications:
        for error in invalid_classifications:
            print(f"[ERROR] {error}", file=sys.stderr)
        return 2

    if not args.junit or not args.junit.is_file():
        print(f"[ERROR] junit file not found: {args.junit}", file=sys.stderr)
        return 2

    exact_results, timed_out, failures = parse_junit(args.junit)
    results = collapse_results(exact_results)
    missing_results = sorted(collected - exact_results.keys()) if collected else []

    regressions: list[str] = []
    promotions: dict[str, list[str]] = {"unimplemented": [], "excluded": []}
    unclassified_passed: list[str] = []
    unclassified_failed: list[str] = []
    behavior_passed: list[str] = []
    counts = {"passed": 0, "failed": 0, "error": 0, "skipped": 0}

    for name, status in results.items():
        counts[status] += 1
        if status in ("failed", "error"):
            if name in lists["implemented"]:
                regressions.append(name)
            elif (
                name not in lists["unimplemented"]
                and name not in lists["excluded"]
                and name not in lists["behavior"]
            ):
                unclassified_failed.append(name)
        elif status == "passed":
            if name in lists["unimplemented"]:
                promotions["unimplemented"].append(name)
            elif name in lists["excluded"]:
                promotions["excluded"].append(name)
            elif name in lists["behavior"]:
                behavior_passed.append(name)
            elif name not in lists["implemented"]:
                unclassified_passed.append(name)

    lines = [
        "# S3 compatibility report",
        "",
        f"Executed: {len(exact_results)} exact cases across {len(results)} classified tests.",
        "",
        "Classification status — "
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
        "Lifecycle behavior lane (passing in this run)",
        behavior_passed,
        "Gated by the dedicated `s3-lifecycle-behavior-tests` lane; expected to "
        "fail in the plain `scope=all` sweep (no debug day / scanner).",
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
    lines += render_section(
        "Missing results",
        missing_results,
        "Present in the pinned upstream suite but absent from JUnit — the sweep was incomplete.",
    )
    lines += render_section(
        "Timed out",
        timed_out,
        "Per-test timeout is an infrastructure failure regardless of compatibility classification.",
    )

    report = "\n".join(lines)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
        print(f"[INFO] Report written to {args.output}")

    print(
        f"[INFO] {len(regressions)} regression(s), "
        f"{len(promotions['unimplemented']) + len(promotions['excluded']) + len(unclassified_passed)} promotion candidate(s), "
        f"{len(unclassified_failed)} unclassified failure(s), "
        f"{len(missing_results)} missing result(s), "
        f"{len(timed_out)} timeout(s)"
    )
    for name in sorted(regressions):
        print(f"[REGRESSION] {name}")
    if failures:
        print("[ERROR] s3-tests failed testcase summary:")
        for classname, name, duration, message in failures[:20]:
            nodeid = f"{classname}::{name}" if classname else name
            print(f"[ERROR] - {nodeid} ({duration}s): {message}")
        if len(failures) > 20:
            print(f"[ERROR] - ... {len(failures) - 20} additional failed testcases omitted")

    if args.fail_on_regression and regressions:
        return 1
    if args.fail_on_unclassified and (unclassified_passed or unclassified_failed):
        return 1
    if args.collected_nodeids and missing_results:
        return 1
    if timed_out:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
