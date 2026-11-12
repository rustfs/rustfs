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
"""Plan, prove and summarize the nightly both-stack lane (.github/workflows/dual-stack-nightly.yml).

Subcommands:
  matrix   print the profile x stack matrix of .config/dual-stack-profiles.toml as JSON
  stack    prove, from the servers' own start-up output, which S3 stack one leg ran
  summary  diff the two legs of every profile with junit_diff.py and write the dated summary

`stack` reads the `server_starting` event every server logs, which carries the stack the server
selected (`s3_stack`); it never reads the variable the workflow set. It fails when a server of the
other stack started, when no server of the requested stack started, and when there is no server
output at all, and it records the message of every log that never reached a start (the server
refused to start) so the summary can say why a leg proved nothing.

`summary` expects `<legs>/dual-stack-leg-<profile>-<stack>/{junit.xml,stack.json}` for every
profile and stack of the config. A side whose JUnit is absent or whose stack was not proven is
reported as such and never as a comparison; the gateway side not starting is reported, and is not
a failure of the summary until the gateway stack is required (T2.10).

Exit codes: 0 ok; 1 a leg that did not prove its stack, or a side without a result; 2 bad input.
"""

import argparse
import contextlib
import io
import json
from collections import Counter
from pathlib import Path
import sys
import tomllib

import junit_diff

STACKS = ("legacy", "gateway")
START_EVENT = "server_starting"
NAMED_LIMIT = 20
MESSAGE_LIMIT = 200
PROFILE_KEYS = {"name", "source", "runner", "timeout_minutes", "features", "setup", "nextest_args", "filter"}
SETUP_TAGS = {"awscurl", "full-tools", "protocols-tools", "pools", "minio"}


class InputError(Exception):
    """Input that cannot be trusted; maps to exit code 2."""


# --- profile list ----------------------------------------------------------

def load_profiles(path):
    try:
        data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise InputError(f"{path}: {exc}") from exc
    profiles = data.get("profile")
    if data.get("schema") != 1 or not isinstance(profiles, list) or not profiles:
        raise InputError(f"{path}: expected schema = 1 and at least one [[profile]]")
    seen = set()
    for item in profiles:
        name = item.get("name")
        if set(item) != PROFILE_KEYS:
            raise InputError(f"{path}: profile {name!r} must have exactly the keys {sorted(PROFILE_KEYS)}")
        if not isinstance(name, str) or not name.startswith("e2e-") or name in seen:
            raise InputError(f"{path}: profile name {name!r} must be a unique e2e-* name")
        seen.add(name)
        if not isinstance(item["setup"], list) or not set(item["setup"]) <= SETUP_TAGS:
            raise InputError(f"{path}: {name}: setup must be a list drawn from {sorted(SETUP_TAGS)}")
        if not isinstance(item["timeout_minutes"], int) or item["timeout_minutes"] <= 0:
            raise InputError(f"{path}: {name}: timeout_minutes must be a positive integer")
        for key in ("source", "runner", "features", "nextest_args", "filter"):
            if not isinstance(item[key], str):
                raise InputError(f"{path}: {name}: {key} must be a string")
    return profiles


def matrix(profiles):
    return {"include": [{**profile, "profile": profile["name"], "stack": stack} for profile in profiles for stack in STACKS]}


# --- which stack did the servers select ------------------------------------

def log_files(paths):
    files = []
    for path in map(Path, paths):
        if path.is_dir():
            files.extend(sorted(item for item in path.rglob("*") if item.is_file()))
        elif path.is_file():
            files.append(path)
        else:
            raise InputError(f"{path}: not found")
    if not files:
        raise InputError("no server output found: nothing proves which stack ran")
    return files


def prove_stack(expect, paths):
    started, unstamped, without_start = Counter(), 0, Counter()
    files = log_files(paths)
    for path in files:
        reached_start, last = False, ""
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            event = None
            if line.startswith("{"):
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    pass
            if not isinstance(event, dict):
                last = line
            elif event.get("event") == START_EVENT:
                reached_start = True
                if "s3_stack" in event:
                    started[str(event["s3_stack"]).lower()] += 1
                else:
                    unstamped += 1
            elif event.get("level") == "ERROR":
                last = str(event.get("error") or event.get("message") or line)
        if not reached_start:
            without_start[last[:MESSAGE_LIMIT]] += 1
    foreign = sum(count for stack, count in started.items() if stack != expect)
    if foreign:
        verdict = f"{foreign} server(s) started on another stack although {expect} was requested"
    elif not started[expect]:
        verdict = f"no server started on the {expect} stack"
    else:
        verdict = "ok"
    return {
        "expected": expect,
        "verdict": verdict,
        "started": dict(started),
        "foreign_starts": foreign,
        "unstamped_starts": unstamped,
        "logs_scanned": len(files),
        "logs_without_start": sum(without_start.values()),
        "not_started": [{"message": message, "logs": count} for message, count in without_start.most_common(5)],
    }


# --- per-profile comparison ------------------------------------------------

def run_junit_diff(legacy, gateway, diff_path):
    """Run junit_diff.py on one profile's two reports; return (exit code, parsed --json summary)."""
    out, err = io.StringIO(), io.StringIO()
    summary_path = Path(str(diff_path) + ".json")
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = junit_diff.main([str(legacy), str(gateway), "--label-a", "legacy", "--label-b", "gateway", "--json", str(summary_path)])
    diff_path.write_text(out.getvalue() + err.getvalue(), encoding="utf-8")
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.is_file() else None
    summary_path.unlink(missing_ok=True)
    return code, summary


def evidence_state(directory, stack):
    """What a leg's stack.json proves: ok, not_started, wrong_stack or no_result, with a detail to print."""
    evidence_path = directory / "stack.json"
    if not evidence_path.is_file():
        return {"status": "no_result", "detail": f"no result for {stack}: the leg left no server evidence"}
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    if evidence["foreign_starts"]:
        return {"status": "wrong_stack", "detail": f"{evidence['foreign_starts']} server(s) started on another stack"}
    if evidence["verdict"] != "ok":
        reasons = "; ".join(item["message"] + (f" ({item['logs']} logs)" if item["logs"] > 1 else "") for item in evidence["not_started"] if item["message"])
        return {"status": "not_started", "detail": f"server did not start: {reasons or 'its logs show no start'}"}
    return {"status": "ok", "detail": f"ran; {evidence['started'][stack]} server start(s) on {stack}"}


def leg_state(directory, stack):
    """evidence_state, and for a proven leg a JUnit that holds results."""
    state = evidence_state(directory, stack)
    if state["status"] != "ok":
        return state
    try:
        junit_diff.parse_report(directory / "junit.xml")
    except junit_diff.NoResult as exc:
        return {"status": "no_result", "detail": f"no result for {stack}: {exc}"}
    except junit_diff.InputError as exc:
        return {"status": "no_result", "detail": f"unusable result for {stack}: {exc}"}
    return state


def compare_profile(legs, profile, out_dir):
    name = profile["name"]
    leg = lambda stack: legs / f"dual-stack-leg-{name}-{stack}"
    result = {"profile": name, "source": profile["source"], "filter": profile["filter"], "diff": None}
    result.update({stack: leg_state(leg(stack), stack) for stack in STACKS})
    if all(result[stack]["status"] == "ok" for stack in STACKS):
        code, summary = run_junit_diff(leg("legacy") / "junit.xml", leg("gateway") / "junit.xml", out_dir / "diffs" / f"{name}.txt")
        if summary is None:
            raise InputError(f"{name}: junit_diff.py produced no summary (exit {code})")
        counts, differences = summary["counts"]["legacy"], summary["differences"]
        on_legacy = lambda state: sum(d["a"] == state for d in differences)
        result["diff"] = {
            "cases": sum(counts.values()) + on_legacy(junit_diff.MISSING),
            "passed_on_both": counts[junit_diff.PASS] - on_legacy(junit_diff.PASS),
            "failed_on_both": counts[junit_diff.FAIL] - on_legacy(junit_diff.FAIL),
            "legacy_only_failures": [d["case"] for d in differences if d["a"] == junit_diff.FAIL],
            "gateway_only_failures": [d["case"] for d in differences if d["b"] == junit_diff.FAIL],
            "other_differences": [
                f"{d['case']}: legacy {d['a']}, gateway {d['b']}" for d in differences if junit_diff.FAIL not in (d["a"], d["b"])
            ],
        }
    return result


def broken(result):
    """True when the measurement itself failed: a side without a result or on the wrong stack, or no legacy proof."""
    statuses = {stack: result[stack]["status"] for stack in STACKS}
    return any(status in ("no_result", "wrong_stack") for status in statuses.values()) or statuses["legacy"] != "ok"


def named(label, cases):
    if not cases:
        return []
    shown = ", ".join(f"`{case}`" for case in cases[:NAMED_LIMIT])
    more = f" (+{len(cases) - NAMED_LIMIT} more in the JSON report)" if len(cases) > NAMED_LIMIT else ""
    return [f"- {label} ({len(cases)}): {shown}{more}"]


def render(date, results):
    lines = [
        f"## Both-stack e2e diff {date}",
        "",
        "| Profile | Legacy leg | Gateway leg (report-only until T2.10) | Cases | Passed on both | Failed on both "
        "| Legacy-only failures | Gateway-only failures | Other differences |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        diff = result["diff"]
        counts = (
            [diff["cases"], diff["passed_on_both"], diff["failed_on_both"], len(diff["legacy_only_failures"]),
             len(diff["gateway_only_failures"]), len(diff["other_differences"])]
            if diff else ["n/a"] * 6
        )
        lines.append("| " + " | ".join(
            [result["profile"], result["legacy"]["detail"].replace("|", "/"), result["gateway"]["detail"].replace("|", "/"),
             *map(str, counts)]) + " |")
    for result in results:
        diff = result["diff"]
        if diff:
            body = (named("Legacy-only failures", diff["legacy_only_failures"])
                    + named("Gateway-only failures", diff["gateway_only_failures"])
                    + named("Other differences", diff["other_differences"]))
            if body:
                lines += ["", f"### {result['profile']}", "", *body]
    return "\n".join(lines) + "\n"


def summarize(legs, profiles, date, out_dir):
    (out_dir / "diffs").mkdir(parents=True, exist_ok=True)
    results = [compare_profile(Path(legs), profile, out_dir) for profile in profiles]
    markdown = render(date, results)
    (out_dir / f"{date}.json").write_text(json.dumps({"date": date, "profiles": results}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / "summary.md").write_text(markdown, encoding="utf-8")
    return markdown, any(broken(result) for result in results)


# --- command line ----------------------------------------------------------

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("matrix")
    plan.add_argument("--config", required=True)
    stack = commands.add_parser("stack")
    stack.add_argument("--expect", choices=STACKS, required=True)
    stack.add_argument("--out", required=True, help="write the evidence JSON here")
    stack.add_argument("paths", nargs="+", help="server log files or directories")
    summary = commands.add_parser("summary")
    summary.add_argument("--config", required=True)
    summary.add_argument("--legs", required=True, help="directory of downloaded leg artifacts")
    summary.add_argument("--date", required=True)
    summary.add_argument("--out-dir", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "matrix":
            print(json.dumps(matrix(load_profiles(args.config))))
            return 0
        if args.command == "stack":
            evidence = prove_stack(args.expect, args.paths)
            Path(args.out).write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(json.dumps(evidence, indent=2, sort_keys=True))
            return 0 if evidence["verdict"] == "ok" else 1
        out_dir = Path(args.out_dir)
        markdown, failed = summarize(args.legs, load_profiles(args.config), args.date, out_dir)
        print(markdown)
        return 1 if failed else 0
    except (InputError, OSError, KeyError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
