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
"""Prove which S3 stack the servers of one e2e leg ran, from their own start-up output.

  stack  read the `server_starting` line every server logs (`s3_stack`) in the given logs; fails on a
         server of the other stack, on no server of the requested stack, and on no server output at all

The stack is never taken from the variable a workflow set. Servers of older releases log no stack
and are counted as unstamped. Exit codes: 0 proven; 1 not proven; 2 bad input.
"""

import argparse
import json
from collections import Counter
from pathlib import Path
import sys

STACKS = ("legacy", "gateway")
START_EVENT = "server_starting"
MESSAGE_LIMIT = 200


class InputError(Exception):
    """Input that cannot be trusted; maps to exit code 2."""


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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    stack = commands.add_parser("stack")
    stack.add_argument("--expect", choices=STACKS, required=True)
    stack.add_argument("--out", required=True, help="write the evidence JSON here")
    stack.add_argument("paths", nargs="+", help="server log files or directories")
    args = parser.parse_args(argv)
    try:
        evidence = prove_stack(args.expect, args.paths)
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(evidence, indent=2, sort_keys=True))
        return 0 if evidence["verdict"] == "ok" else 1
    except (InputError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
