#!/usr/bin/env python3
"""Strict restart diagnostic using the real scanner libtest worker, not a walker model."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

WORKER = "scanner_folder::tests::enumeration_restart::enumeration_restart_worker"
MAX_REPORT_BYTES = 16384


def bounded_int(low, high):
    def parse(value):
        number = int(value)
        if not low <= number <= high:
            raise argparse.ArgumentTypeError(f"must be between {low} and {high}")
        return number
    return parse


def validate_report(report, *, round_number, pid, objects, budget):
    if not isinstance(report, dict):
        raise ValueError("worker report must be an object")
    expected = {"schema": 1, "round": round_number, "pid": pid,
                "objects_expected": objects, "raw_entry_budget": budget}
    for key, value in expected.items():
        if type(report.get(key)) is not int or report[key] != value:
            raise ValueError(f"worker report mismatch: {key}")
    for key in ("raw_entries", "raw_name_bytes", "objects_before", "objects_retained",
                "versions_retained", "bytes_retained", "objects_processed"):
        if type(report.get(key)) is not int or not 0 <= report[key] <= 1048576:
            raise ValueError(f"invalid bounded counter: {key}")
    if report["raw_entries"] == 0:
        raise ValueError("nonempty fixture must observe raw entries; budget hook may not have run")
    if report["raw_entries"] > budget:
        raise ValueError("raw-entry budget exceeded; no unbudgeted tail is permitted")
    for key in ("raw_first_entry", "raw_last_entry"):
        value = report.get(key)
        if type(value) is not str or not 0 < len(value.encode("utf-8")) <= 512:
            raise ValueError(f"invalid raw entry marker: {key}")
    if type(report.get("snapshot_complete")) is not bool:
        raise ValueError("missing explicit completeness")
    if report.get("outcome") not in ("complete", "partial", "cancelled_without_cache"):
        raise ValueError("unexpected scanner outcome")


def converged(report, objects):
    return (report["snapshot_complete"] and report["outcome"] == "complete"
            and all(report[key] == objects for key in
                    ("objects_retained", "versions_retained", "bytes_retained")))


def replays_raw_window(previous, current):
    return (previous["raw_first_entry"] == current["raw_first_entry"]
            and previous["raw_last_entry"] == current["raw_last_entry"]
            and previous["objects_retained"] == current["objects_before"]
            and current["objects_retained"] == previous["objects_retained"])


def run(args):
    binary = args.test_binary.resolve(strict=True)
    listed = subprocess.run([str(binary), WORKER, "--exact", "--list"],
                            check=True, capture_output=True, text=True, timeout=30)
    if f"{WORKER}: test" not in listed.stdout.splitlines():
        raise ValueError("binary does not contain the exact scanner worker test")
    workspace = args.output.resolve()
    workspace.mkdir()  # Refuse reuse/overwrite of previous evidence or customer data.
    reports = []
    replayed_raw_window = False
    for round_number in range(args.rounds):
        request = {"workspace": str(workspace), "objects": args.objects,
                   "raw_entry_budget": args.raw_entry_budget, "round": round_number}
        request_path = workspace / "request.json"
        request_path.write_text(json.dumps(request), encoding="utf-8")
        env = dict(os.environ, RUSTFS_ENUMERATION_REQUEST=str(request_path),
                   RUST_MIN_STACK="4194304", NO_PROXY="localhost,127.0.0.1,::1",
                   no_proxy="localhost,127.0.0.1,::1")
        with subprocess.Popen([str(binary), WORKER, "--exact", "--test-threads=1"],
                              env=env, stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL) as worker:
            try:
                status = worker.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait()
                raise ValueError(f"worker round {round_number} timed out") from None
            if status:
                raise ValueError(f"real scanner worker round {round_number} exited {status}")
            report_path = workspace / f"round-{round_number}.json"
            with report_path.open("rb") as handle:
                raw = handle.read(MAX_REPORT_BYTES + 1)
            if len(raw) > MAX_REPORT_BYTES:
                raise ValueError("oversized worker report")
            report = json.loads(raw)
            validate_report(report, round_number=round_number, pid=worker.pid,
                            objects=args.objects, budget=args.raw_entry_budget)
        if reports and report["objects_before"] != reports[-1]["objects_retained"]:
            raise ValueError("cache coverage did not survive the process boundary")
        if reports and replays_raw_window(reports[-1], report):
            replayed_raw_window = True
        reports.append(report)
        print(json.dumps(report, sort_keys=True), flush=True)
        if converged(report, args.objects):
            print("PASS: bounded scanner-worker restart convergence for this fixture only")
            return 0
    reason = "replayed raw enumeration window" if replayed_raw_window else "no bounded restart convergence"
    print(f"FAIL: fixed-budget restart convergence not established ({reason}); R-E gate remains unmet",
          file=sys.stderr)
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", type=Path, required=True,
                        help="compiled rustfs-scanner libtest executable")
    parser.add_argument("--output", type=Path, required=True, help="new evidence directory (must not exist)")
    parser.add_argument("--objects", type=bounded_int(1, 1024), default=128)
    parser.add_argument("--raw-entry-budget", type=bounded_int(1, 4096), default=8)
    parser.add_argument("--rounds", type=bounded_int(1, 64), default=8)
    parser.add_argument("--timeout", type=bounded_int(1, 120), default=60,
                        help="per-worker watchdog seconds, not the scan work budget")
    args = parser.parse_args()
    try:
        return run(args)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
