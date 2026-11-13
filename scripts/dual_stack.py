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
"""Prove which S3 stack servers ran and summarize the both-stack legs of a workflow run.

  stack      prove, from the servers' own `server_starting` line (`s3_stack`), which stack one leg ran;
             fails on a server of the other stack, on no server of the requested stack, and on no output
  preflight  start a built binary once on the gateway stack and prove whether it starts; a gateway leg
             that cannot prove a start does not run, and is summarized as `skipped: gateway stack
             refuses startup: <why>`
  summary    diff the two legs of every profile with junit_diff.py and write the dated summary from
             `<legs>/dual-stack-leg-<profile>-<stack>/{junit.xml,stack.json}`; a side without a JUnit
             or a proven stack is reported as such and never as a comparison

A skipped or non-starting gateway side is tolerated by `summary` until T2.10.
Exit codes: 0 ok; 1 a stack not proven, or a side without a result; 2 bad input.
"""

import argparse
import contextlib
import io
import json
import os
import socket
import subprocess
import tempfile
import time
from collections import Counter
from pathlib import Path
import sys

import junit_diff

STACKS = ("legacy", "gateway")
START_EVENT = "server_starting"
NAMED_LIMIT = 20
MESSAGE_LIMIT = 200
PREFLIGHT_SECONDS = 120


class InputError(Exception):
    """Input that cannot be trusted; maps to exit code 2."""


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


def log_event(line):
    """The JSON event of a log line, ignoring a plain-text prefix glued to it; None for plain text."""
    start = line.find("{")
    if start < 0:
        return None
    try:
        event = json.loads(line[start:])
    except json.JSONDecodeError:
        return None
    return event if isinstance(event, dict) else None


def prove_stack(expect, paths):
    started, unstamped, without_start, proven = Counter(), 0, Counter(), 0
    files = log_files(paths)
    for path in files:
        starts, saw_event, last = set(), False, ""
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            event = log_event(line)
            if event is None:
                last = line or last
                continue
            saw_event = True
            if event.get("event") == START_EVENT:
                if "s3_stack" in event:
                    started[str(event["s3_stack"]).lower()] += 1
                    starts.add(str(event["s3_stack"]).lower())
                else:
                    unstamped += 1
                    starts.add(None)
            elif event.get("level") == "ERROR":
                last = str(event.get("error") or event.get("message") or line)
        proven += expect in starts
        # Text that is not a server log (a test runner's output) has no event and is not a server.
        if not starts and saw_event:
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
        "logs_proven": proven,
        "logs_without_start": sum(without_start.values()),
        "not_started": [{"message": message, "logs": count} for message, count in without_start.most_common(5)],
    }


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def preflight(binary, seconds=PREFLIGHT_SECONDS):
    """Start `binary` once on the gateway stack, stop it at its start-up line or exit, and prove what it did."""
    with tempfile.TemporaryDirectory() as tmp:
        log = Path(tmp) / "server.log"
        env = {**os.environ, "RUSTFS_S3_STACK": "gateway", "RUST_LOG": "rustfs=info", "RUSTFS_CONSOLE_ENABLE": "false"}
        with open(log, "wb") as output:
            server = subprocess.Popen([binary, "--address", f"127.0.0.1:{free_port()}", str(Path(tmp) / "data")],
                                      stdout=output, stderr=subprocess.STDOUT, env=env)
            deadline = time.monotonic() + seconds
            while server.poll() is None and time.monotonic() < deadline and START_EVENT not in log.read_text(errors="replace"):
                time.sleep(0.5)
            server.terminate()
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
        return prove_stack("gateway", [log])


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


def evidence_state(directory, stack, result):
    """What a leg's stack.json proves: ok, skipped, not_started, wrong_stack or no_result, with a detail to print.

    A gateway leg with no `result` file never ran its suite: it is `skipped`.
    """
    evidence_path = directory / "stack.json"
    if not evidence_path.is_file():
        return {"status": "no_result", "detail": f"no result for {stack}: the leg left no server evidence"}
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    if evidence["foreign_starts"]:
        return {"status": "wrong_stack", "detail": f"{evidence['foreign_starts']} server(s) started on another stack"}
    if evidence["verdict"] != "ok":
        top = next((item["message"] for item in evidence["not_started"] if item["message"]), "its logs show no start")
        if stack == "gateway" and not (directory / result).is_file():
            return {"status": "skipped", "detail": f"skipped: gateway stack refuses startup: {top}"}
        return {"status": "not_started", "detail": f"server did not start: {top}"}
    return {"status": "ok", "detail": (
        f"ran; {evidence['logs_proven']} of {evidence['logs_scanned']} server logs prove {stack}, "
        f"{evidence['logs_without_start']} without a start, {evidence['unstamped_starts']} unstamped starts")}


def leg_state(directory, stack):
    """evidence_state, and for a proven leg a JUnit that holds results."""
    state = evidence_state(directory, stack, "junit.xml")
    if state["status"] != "ok":
        return state
    try:
        junit_diff.parse_report(directory / "junit.xml")
    except junit_diff.NoResult as exc:
        return {"status": "no_result", "detail": f"no result for {stack}: {exc}"}
    except junit_diff.InputError as exc:
        return {"status": "no_result", "detail": f"unusable result for {stack}: {exc}"}
    return state


def compare_profile(legs, name, out_dir):
    leg = lambda stack: legs / f"dual-stack-leg-{name}-{stack}"
    result = {"profile": name, "diff": None}
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
    """True when the measurement itself failed: a side without a result or on the wrong stack, or no legacy proof.

    A gateway side that refuses to start, or whose suite was skipped for it, is reported and tolerated until T2.10.
    """
    statuses = {stack: result[stack]["status"] for stack in STACKS}
    return any(status in ("no_result", "wrong_stack") for status in statuses.values()) or statuses["legacy"] != "ok"


def named(label, cases):
    shown = ", ".join(f"`{case}`" for case in cases[:NAMED_LIMIT]) + (f" (+{len(cases) - NAMED_LIMIT} more in the JSON report)" if len(cases) > NAMED_LIMIT else "")
    return [f"- {label} ({len(cases)}): {shown}"] if cases else []


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
    stack = commands.add_parser("stack")
    stack.add_argument("--expect", choices=STACKS, required=True)
    stack.add_argument("--out", required=True, help="write the evidence JSON here")
    stack.add_argument("paths", nargs="+", help="server log files or directories")
    pre = commands.add_parser("preflight")
    pre.add_argument("--binary", required=True)
    pre.add_argument("--out", required=True, help="write the evidence JSON here")
    summary = commands.add_parser("summary")
    summary.add_argument("--profiles", required=True, help="comma-separated profile names")
    summary.add_argument("--legs", required=True, help="directory of downloaded leg artifacts")
    summary.add_argument("--date", required=True)
    summary.add_argument("--out-dir", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command in ("stack", "preflight"):
            evidence = prove_stack(args.expect, args.paths) if args.command == "stack" else preflight(args.binary)
            Path(args.out).parent.mkdir(parents=True, exist_ok=True)
            Path(args.out).write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(json.dumps(evidence, indent=2, sort_keys=True))
            return 0 if evidence["verdict"] == "ok" else 1
        markdown, failed = summarize(args.legs, args.profiles.split(","), args.date, Path(args.out_dir))
        print(markdown)
        return 1 if failed else 0
    except (InputError, OSError, KeyError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
