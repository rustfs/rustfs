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
                "versions_retained", "bytes_retained", "objects_processed",
                "raw_page_index_committed_entries", "raw_page_index_indexed_entries"):
        if type(report.get(key)) is not int or not 0 <= report[key] <= 1048576:
            raise ValueError(f"invalid bounded counter: {key}")
    made_budgeted_object_progress = report["objects_processed"] > 0
    if report["raw_entries"] == 0 and not made_budgeted_object_progress:
        raise ValueError("nonempty fixture must observe raw entries; budget hook may not have run")
    if report["raw_entries"] > budget:
        raise ValueError("raw-entry budget exceeded; no unbudgeted tail is permitted")
    if report["objects_processed"] > budget:
        raise ValueError("object budget exceeded; no unbudgeted scan tail is permitted")
    for key in ("raw_first_entry", "raw_last_entry"):
        value = report.get(key)
        if report["raw_entries"] == 0 and made_budgeted_object_progress and value is None:
            continue
        if type(value) is not str or not 0 < len(value.encode("utf-8")) <= 512:
            raise ValueError(f"invalid raw entry marker: {key}")
    if "raw_page_index_parent" not in report:
        raise ValueError("missing raw page index parent")
    raw_page_index_parent = report.get("raw_page_index_parent")
    if raw_page_index_parent is not None and (type(raw_page_index_parent) is not str
                                              or not 0 < len(raw_page_index_parent.encode("utf-8")) <= 512):
        raise ValueError("invalid raw page index parent")
    if type(report.get("raw_page_index_complete")) is not bool:
        raise ValueError("missing raw page index completeness")
    if type(report.get("snapshot_complete")) is not bool:
        raise ValueError("missing explicit completeness")
    if report.get("outcome") not in ("complete", "partial", "cancelled_without_cache"):
        raise ValueError("unexpected scanner outcome")
    if report["raw_page_index_committed_entries"] > report["raw_page_index_indexed_entries"]:
        raise ValueError("raw page index committed entries exceed indexed entries")
    if report["raw_page_index_parent"] == "bucket" and report["raw_page_index_indexed_entries"] > objects:
        raise ValueError("raw page index exceeds fixture object count")
    if report["objects_retained"] > report["objects_before"] + report["objects_processed"]:
        raise ValueError("retained coverage advanced beyond classified object work")


def converged(report, objects):
    return (report["snapshot_complete"] and report["outcome"] == "complete"
            and all(report[key] == objects for key in
                    ("objects_retained", "versions_retained", "bytes_retained")))


def replays_raw_window(previous, current):
    return (previous["raw_first_entry"] == current["raw_first_entry"]
            and previous["raw_last_entry"] == current["raw_last_entry"]
            and previous["objects_retained"] == current["objects_before"]
            and current["objects_retained"] == previous["objects_retained"])


def validate_recoverable_quantum(reports, *, objects, budget, require_converged):
    if not reports:
        raise ValueError("no scanner restart reports were produced")
    previous = None
    made_enumeration_progress = False
    made_raw_page_commit_progress = False
    made_classification_progress = False
    made_durable_progress = False
    for index, report in enumerate(reports):
        validate_report(report, round_number=index, pid=report["pid"], objects=objects, budget=budget)
        if report["raw_page_index_parent"] == "bucket" and report["raw_page_index_committed_entries"] > 0:
            made_raw_page_commit_progress = True
        if previous is not None:
            if report["objects_before"] != previous["objects_retained"]:
                raise ValueError("durable retained coverage did not survive process restart")
            if report["objects_retained"] < previous["objects_retained"]:
                raise ValueError("durable retained coverage regressed across restart")
            if replays_raw_window(previous, report):
                raise ValueError("raw enumeration window replayed without durable coverage")
            if (report["raw_page_index_parent"] == previous["raw_page_index_parent"]
                    and report["raw_page_index_committed_entries"] < previous["raw_page_index_committed_entries"]
                    and not previous["raw_page_index_complete"]):
                raise ValueError("committed raw enumeration page coverage regressed before completion")
        made_enumeration_progress |= report["raw_entries"] > 0 or report["raw_page_index_indexed_entries"] > 0
        made_classification_progress |= report["objects_processed"] > 0
        made_durable_progress |= report["objects_retained"] > report["objects_before"]
        previous = report
    if not made_enumeration_progress:
        raise ValueError("restart proof did not exercise raw enumeration")
    if not made_raw_page_commit_progress:
        raise ValueError("restart proof did not commit a durable raw enumeration page")
    if not made_classification_progress:
        raise ValueError("restart proof did not exercise object classification")
    if not made_durable_progress:
        raise ValueError("restart proof did not persist processed object coverage")
    if require_converged and not converged(reports[-1], objects):
        raise ValueError("fixed-budget restart convergence was not established")


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
        if reports and replays_raw_window(reports[-1], report):
            replayed_raw_window = True
        reports.append(report)
        print(json.dumps(report, sort_keys=True), flush=True)
        if converged(report, args.objects):
            validate_recoverable_quantum(reports, objects=args.objects, budget=args.raw_entry_budget, require_converged=True)
            print("PASS: bounded scanner-worker restart convergence with enumeration/classification/processing evidence")
            return 0
    validate_recoverable_quantum(reports, objects=args.objects, budget=args.raw_entry_budget, require_converged=False)
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
