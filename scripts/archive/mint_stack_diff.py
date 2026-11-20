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
"""Compare the mint results of the legacy and the gateway stack (.github/workflows/mint.yml).

Each argument is the directory of one mint leg: `mint-log/log.json` and `stack.json`
(scripts/dual_stack.py stack). A case is `<suite>::<function>` and fails when any entry fails.

The gate: every case legacy passes is present and passing on gateway, and no case fails on gateway
that does not fail on legacy. A case the gateway did not run, reported `NA`, or ran to an unknown
status never passes, so a truncated or empty gateway result cannot satisfy it. A side without a
result never satisfies it either (`skipped: gateway stack refuses startup`). A `{` line of log.json
that does not parse, and a status other than PASS, FAIL or NA, are bad input.

Exit codes: 0 the gate holds; 1 it does not, or a side has no result; 2 bad input.
"""

import argparse
import json
from pathlib import Path
import sys

import dual_stack

STACKS = dual_stack.STACKS
RANK = {"FAIL": 2, "PASS": 1, "NA": 0}


def read_cases(log):
    """Return {case: PASS|FAIL|NA} from a mint log.json, or None when it holds no result."""
    try:
        lines = log.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    cases = {}
    for number, line in enumerate(lines, 1):
        if not line.startswith("{"):
            continue  # mint also writes plain progress lines into this file
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{log}:{number}: unparsable result line: {exc}") from exc
        status = str(entry.get("status", "")).upper()
        if status not in RANK:
            raise ValueError(f"{log}:{number}: unknown status {entry.get('status')!r}")
        case = f"{entry.get('name', 'unknown')}::{entry.get('function', '')}"
        cases[case] = max(cases.get(case, status), status, key=RANK.get)
    return cases or None


def load(directory, stack):
    """Return (cases, None) for a proven leg with a result, else (None, why there is none)."""
    state = dual_stack.evidence_state(directory, stack, "mint-log/log.json")
    if state["status"] != "ok":
        return None, state["detail"]
    cases = read_cases(directory / "mint-log" / "log.json")
    if cases is None:
        return None, f"no result for {stack}: mint wrote no log.json entries"
    return cases, None


def compare(legacy, gateway):
    """Return the report lines and whether the gate holds."""
    sides = {stack: load(Path(directory), stack) for stack, directory in zip(STACKS, (legacy, gateway))}
    lines = ["Mint stack comparison"]
    for stack, (cases, why) in sides.items():
        if cases is None:
            lines.append(f"{stack}: {why}")
        else:
            counts = {status: list(cases.values()).count(status) for status in RANK}
            lines.append(f"{stack}: {len(cases)} cases, {counts['PASS']} pass, {counts['FAIL']} fail, {counts['NA']} n/a")
    if any(cases is None for cases, _ in sides.values()):
        return lines + ["Gate NOT met: a side has no result, so no failing set can be compared"], False
    (legacy_cases, _), (gateway_cases, _) = sides.values()
    lost = {case: gateway_cases.get(case) for case, status in legacy_cases.items() if status == "PASS" and gateway_cases.get(case) != "PASS"}
    new_failures = sorted(case for case, status in gateway_cases.items() if status == "FAIL" and legacy_cases.get(case) != "FAIL" and case not in lost)
    lists = {
        "Passing on legacy, failing on gateway": sorted(case for case, status in lost.items() if status == "FAIL"),
        "Passing on legacy, not run on gateway": sorted(case for case, status in lost.items() if status is None),
        "Passing on legacy, n/a on gateway": sorted(case for case, status in lost.items() if status == "NA"),
        "Failing on gateway, not run or n/a on legacy": new_failures,
    }
    for title, cases in lists.items():
        lines.append(f"{title} ({len(cases)}):")
        lines.extend(f"  {case}" for case in cases)
    fixed = sorted(case for case, status in legacy_cases.items() if status == "FAIL" and gateway_cases.get(case) == "PASS")
    lines.append(f"Failing on legacy, passing on gateway ({len(fixed)}):")
    lines.extend(f"  {case}" for case in fixed)
    holds = not any(lists.values())
    lines.append("Gate met: every legacy pass passes on gateway and no new gateway failure" if holds
                 else "Gate NOT met: the gateway does not pass everything legacy passes")
    return lines, holds


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("legacy", help="directory of the legacy mint leg")
    parser.add_argument("gateway", help="directory of the gateway mint leg")
    args = parser.parse_args(argv)
    try:
        lines, holds = compare(args.legacy, args.gateway)
    except (OSError, KeyError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print("\n".join(lines))
    return 0 if holds else 1


if __name__ == "__main__":
    sys.exit(main())
