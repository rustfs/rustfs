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
"""Compare two JUnit XML reports case by case.

Cases are aligned on classname + name and reduced to pass / fail / skip. A case
present in only one report is reported as `missing` on the other side. A
`<flakyFailure>` case that finally passed is a pass and is listed separately.

A side that is missing, empty or holds no test case produced no result: that is
exit 2 with `no result for <label>`, never an empty comparison that reads as
"No differences".

Exit codes: 0 no unallowed differences and no stale allow-list entry; 1 at least
one difference or stale allow-list entry; 2 no result for a side, or unreadable or
malformed input.
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET

PASS, FAIL, SKIP, MISSING = "pass", "fail", "skip", "missing"
_RETRY_TAGS = {"flakyFailure", "flakyError", "rerunFailure", "rerunError"}


class InputError(Exception):
    """Input that cannot be trusted as a report; maps to exit code 2."""


class NoResult(InputError):
    """A report that is missing, empty or holds no test case: the side ran nothing observable."""


def case_id(classname, name):
    return f"{classname}::{name}" if classname else name


def parse_report(path):
    """Return ({case_id: state}, [flaky case ids]) for one JUnit file."""
    try:
        with open(path, "rb") as handle:
            data = handle.read()
    except FileNotFoundError as exc:
        raise NoResult(f"{path}: cannot read: file is missing") from exc
    except OSError as exc:
        raise InputError(f"{path}: cannot read: {exc.strerror or exc}") from exc
    if not data.strip():
        raise NoResult(f"{path}: file is empty")
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise InputError(f"{path}: malformed XML: {exc}") from exc
    if root.tag not in ("testsuites", "testsuite"):
        raise InputError(f"{path}: unexpected root element <{root.tag}>")
    states, flaky = {}, []
    for case in root.iter("testcase"):
        name = case.get("name")
        if not name:
            raise InputError(f"{path}: <testcase> without a name")
        cid = case_id(case.get("classname", ""), name)
        if cid in states:
            raise InputError(f"{path}: duplicate case {cid}")
        tags = {child.tag for child in case}
        if tags & {"failure", "error"}:
            state = FAIL
        elif "skipped" in tags:
            state = SKIP
        else:
            state = PASS
        states[cid] = state
        if state == PASS and tags & _RETRY_TAGS:
            flaky.append(cid)
    if not states:
        raise NoResult(f"{path}: report contains no test cases")
    return states, sorted(flaky)


def load_report(path, label):
    """parse_report, naming the side whose report is absent."""
    try:
        return parse_report(path)
    except NoResult as exc:
        raise InputError(f"no result for {label}: {exc}") from exc


def read_allow(path):
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except (OSError, UnicodeDecodeError) as exc:
        raise InputError(f"{path}: cannot read allow-list: {exc}") from exc
    return {line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")}


def counts(states):
    result = {PASS: 0, FAIL: 0, SKIP: 0}
    for state in states.values():
        result[state] += 1
    return result


def compare(a, b, allow):
    diffs = []
    for cid in sorted(set(a) | set(b)):
        sa, sb = a.get(cid, MISSING), b.get(cid, MISSING)
        if sa != sb:
            diffs.append({"case": cid, "a": sa, "b": sb, "allowed": cid in allow})
    stale = sorted(allow - {d["case"] for d in diffs})
    return diffs, stale


def render(args, diffs, stale, flaky_a, flaky_b, counts_a, counts_b):
    out = []
    for label, c in ((args.label_a, counts_a), (args.label_b, counts_b)):
        out.append(f"{label}: pass={c[PASS]} fail={c[FAIL]} skip={c[SKIP]}")
    for label, flaky in ((args.label_a, flaky_a), (args.label_b, flaky_b)):
        if flaky:
            out.append(f"\nFlaky in {label} (passed after retry, not a difference):")
            out.extend(f"  {cid}" for cid in flaky)
    if diffs:
        width = max(len("case"), *(len(d["case"]) for d in diffs))
        wa = max(len(args.label_a), 7)
        out.append("")
        out.append(f"{'case':<{width}}  {args.label_a:<{wa}}  {args.label_b}")
        for d in diffs:
            note = "  (allowed)" if d["allowed"] else ""
            out.append(f"{d['case']:<{width}}  {d['a']:<{wa}}  {d['b']}{note}")
    if stale:
        out.append("\nStale allow-list entries (no matching difference; remove them):")
        out.extend(f"  {cid}" for cid in stale)
    if not diffs and not stale:
        out.append("\nNo differences.")
    return "\n".join(out)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("a", help="first JUnit XML report")
    parser.add_argument("b", help="second JUnit XML report")
    parser.add_argument("--label-a", default="a")
    parser.add_argument("--label-b", default="b")
    parser.add_argument("--json", metavar="PATH", help="write a machine-readable summary")
    parser.add_argument("--allow", metavar="FILE", help="case ids (one per line) whose difference is tolerated")
    args = parser.parse_args(argv)
    if args.label_a == args.label_b:
        print("error: --label-a and --label-b must differ", file=sys.stderr)
        return 2
    try:
        states_a, flaky_a = load_report(args.a, args.label_a)
        states_b, flaky_b = load_report(args.b, args.label_b)
        allow = read_allow(args.allow) if args.allow else set()
        diffs, stale = compare(states_a, states_b, allow)
        counts_a, counts_b = counts(states_a), counts(states_b)
        if args.json:
            summary = {
                "labels": {"a": args.label_a, "b": args.label_b},
                "counts": {args.label_a: counts_a, args.label_b: counts_b},
                "differences": diffs,
                "flaky": {args.label_a: flaky_a, args.label_b: flaky_b},
                "stale_allow": stale,
            }
            with open(args.json, "w", encoding="utf-8") as handle:
                json.dump(summary, handle, indent=2, sort_keys=True)
                handle.write("\n")
    except (InputError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(render(args, diffs, stale, flaky_a, flaky_b, counts_a, counts_b))
    return 1 if stale or any(not d["allowed"] for d in diffs) else 0


if __name__ == "__main__":
    sys.exit(main())
