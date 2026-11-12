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

Each argument is the directory of one mint leg: `mint-log/log.json` (mint's result file, one JSON
object per line) and `stack.json` (scripts/dual_stack.py stack). A case is `<suite>::<function>`
and fails when any of its entries fails.

The gate is that the cases failing on the gateway stack are a subset of the cases failing on the
legacy stack. The set difference is printed either way. A side without a result never satisfies
the gate: an empty failing set is only evidence when the stack was proven to have run, so a
gateway server that never started is `no result for gateway`, not an empty difference.

Exit codes: 0 the gate holds; 1 the gate does not hold, or a side has no result; 2 bad input.
"""

import argparse
import json
from pathlib import Path
import sys

import dual_stack

STACKS = dual_stack.STACKS
RANK = {"FAIL": 2, "PASS": 1}


def read_cases(log):
    """Return {case: PASS|FAIL|NA} from a mint log.json, or None when it holds no result."""
    try:
        lines = log.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    cases = {}
    for line in lines:
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict) or "status" not in entry:
            continue
        case = f"{entry.get('name', 'unknown')}::{entry.get('function', '')}"
        status = str(entry["status"]).upper()
        cases[case] = max(cases.get(case, status), status, key=lambda value: RANK.get(value, 0))
    return cases or None


def load(directory, stack):
    """Return (cases, None) for a proven leg with a result, else (None, why there is none)."""
    state = dual_stack.evidence_state(directory, stack)
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
            counts = {status: list(cases.values()).count(status) for status in ("PASS", "FAIL", "NA")}
            lines.append(f"{stack}: {len(cases)} cases, {counts['PASS']} pass, {counts['FAIL']} fail, {counts['NA']} n/a")
    if any(cases is None for cases, _ in sides.values()):
        return lines + ["Gate NOT met: a side has no result, so no failing set can be compared"], False
    failing = {stack: {case for case, status in cases.items() if status == "FAIL"} for stack, (cases, _) in sides.items()}
    gateway_only = sorted(failing["gateway"] - failing["legacy"])
    legacy_only = sorted(failing["legacy"] - failing["gateway"])
    lines.append(f"Failing on gateway but not on legacy ({len(gateway_only)}):")
    lines.extend(f"  {case}" for case in gateway_only)
    lines.append(f"Failing on legacy but not on gateway ({len(legacy_only)}):")
    lines.extend(f"  {case}" for case in legacy_only)
    holds = not gateway_only
    lines.append("Gate met: the gateway failures are a subset of the legacy failures" if holds else "Gate NOT met: the gateway fails cases that legacy passes")
    return lines, holds


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("legacy", help="directory of the legacy mint leg")
    parser.add_argument("gateway", help="directory of the gateway mint leg")
    args = parser.parse_args(argv)
    try:
        lines, holds = compare(args.legacy, args.gateway)
    except (OSError, KeyError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print("\n".join(lines))
    return 0 if holds else 1


if __name__ == "__main__":
    sys.exit(main())
