#!/usr/bin/env python3
"""Recompute fixed-rate Warp quality evidence from raw TSV benchdata."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


TIMESTAMP = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$")
REQUIRED = {"thread", "op", "n_objects", "bytes", "endpoint", "error", "start", "end", "duration_ns"}


def timestamp_ns(value: str) -> int:
    match = TIMESTAMP.fullmatch(value)
    if not match:
        raise ValueError(f"invalid RFC3339Nano timestamp: {value}")
    import calendar
    import datetime

    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    base = calendar.timegm(datetime.datetime(year, month, day, hour, minute, second).timetuple())
    fraction = (match.group(7) or "").ljust(9, "0")
    return base * 1_000_000_000 + int(fraction or "0")


def open_text(path: Path):
    if path.suffix != ".zst":
        return path.open(newline="")
    process = subprocess.Popen(
        ["zstd", "--decompress", "--stdout", "--quiet", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdout is None:
        raise RuntimeError("zstd stdout is unavailable")
    return io.TextIOWrapper(process.stdout, newline=""), process


def parse(path: Path, expected_hosts: set[str] | None, *, steady_after_seconds: int | None = None) -> dict:
    if steady_after_seconds is not None and steady_after_seconds < 0:
        raise ValueError("steady cutoff must be nonnegative")
    opened = open_text(path)
    process = None
    if isinstance(opened, tuple):
        stream, process = opened
    else:
        stream = opened
    try:
        reader = csv.DictReader((line for line in stream if not line.startswith("#")), delimiter="\t", strict=True)
        if reader.fieldnames is None or not REQUIRED.issubset(reader.fieldnames):
            raise ValueError("Warp raw header is missing required fields")
        rows = []
        first_end: dict[int, int] = {}
        last_start: dict[int, int] = {}
        raw_errors = 0
        raw_successful_objects = 0
        host_counts: dict[str, int] = defaultdict(int)
        for row in reader:
            if None in row or any(row.get(field) is None for field in REQUIRED):
                raise ValueError("Warp raw row width differs from header")
            if row["op"] != "PUT":
                raise ValueError("Warp raw input contains a non-PUT operation")
            thread = int(row["thread"])
            objects = int(row["n_objects"])
            size_bytes = int(row["bytes"])
            duration_ns = int(row["duration_ns"])
            if objects <= 0 or size_bytes < 0 or duration_ns < 0 or not row["endpoint"]:
                raise ValueError("Warp raw row contains an invalid numeric or endpoint field")
            start = timestamp_ns(row["start"])
            end = timestamp_ns(row["end"])
            if end < start:
                raise ValueError("Warp raw operation ends before it starts")
            error = bool(row["error"])
            raw_errors += int(error)
            if not error:
                raw_successful_objects += objects
                host_counts[row["endpoint"]] += objects
            first_end[thread] = min(first_end.get(thread, end), end)
            last_start[thread] = max(last_start.get(thread, start), start)
            rows.append((start, end, objects, duration_ns))
        if not rows or raw_errors:
            raise ValueError("Warp raw input is empty or contains request errors")
        if expected_hosts is not None and set(host_counts) != expected_hosts:
            raise ValueError("Warp raw endpoints differ from the expected host set")
        window_start = max(first_end.values())
        window_end = min(last_start.values())
        if window_end <= window_start:
            raise ValueError("Warp common active window is not positive")
        completed_objects = sum(objects for _, end, objects, _ in rows if window_start <= end < window_end)
        completed_requests = sum(1 for _, end, _, _ in rows if window_start <= end < window_end)
        fractional_objects = 0.0
        for start, end, objects, _ in rows:
            if end <= start:
                continue
            overlap = max(0, min(end, window_end) - max(start, window_start))
            fractional_objects += objects * overlap / (end - start)
        duration_secs = (window_end - window_start) / 1_000_000_000
        result = {
            "schema_version": 1,
            "source": str(path),
            "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "operation": "PUT",
            "threads": len(first_end),
            "raw_requests": len(rows),
            "successful_puts": raw_successful_objects,
            "errors": raw_errors,
            "host_counts": dict(sorted(host_counts.items())),
            "completed_objects": completed_objects,
            "completed_requests": completed_requests,
            "fractional_objects": fractional_objects,
            "measure_start_ns": window_start,
            "measure_end_exclusive_ns": window_end,
            "measure_duration_secs": duration_secs,
            "reqps": fractional_objects / duration_secs,
            "window_status": "warp_v1.8.2_common_active_window",
        }
        if steady_after_seconds is not None:
            steady_start = window_start + steady_after_seconds * 1_000_000_000
            if steady_start >= window_end:
                raise ValueError("steady cutoff leaves no common active window")
            steady_duration = (window_end - steady_start) / 1_000_000_000
            steady_objects = 0
            steady_latencies_ns = []
            for _, end, objects, duration_ns in rows:
                if steady_start <= end < window_end:
                    steady_objects += objects
                    steady_latencies_ns.append(duration_ns)
            steady_latencies_ns.sort()
            steady_requests = len(steady_latencies_ns)
            # Nearest-rank p99 over the same successful requests used for the completion rate.
            p99_ns = steady_latencies_ns[(99 * steady_requests + 99) // 100 - 1] if steady_requests else None
            result["steady"] = {
                "start_ns": steady_start,
                "end_exclusive_ns": window_end,
                "duration_secs": steady_duration,
                "completed_objects": steady_objects,
                "completed_objects_per_second": steady_objects / steady_duration,
                "completed_requests": steady_requests,
                "p99_ms": p99_ns / 1_000_000 if p99_ns is not None else None,
            }
        return result
    finally:
        stream.close()
        if process is not None:
            stderr = process.stderr.read().decode() if process.stderr is not None else ""
            if process.wait() != 0:
                raise RuntimeError(f"zstd failed: {stderr.strip()}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--expected-host", action="append")
    parser.add_argument("--steady-after-seconds", type=int, help="Seconds to exclude from the start of the common active window")
    parser.add_argument("--min-steady-rps", type=float, help="Minimum successful object completions per second in the steady window")
    parser.add_argument("--max-steady-rps", type=float, help="Optional maximum steady completion rate, used with the minimum")
    args = parser.parse_args()
    try:
        if (args.steady_after_seconds is None) != (args.min_steady_rps is None):
            raise ValueError("--steady-after-seconds and --min-steady-rps must be used together")
        if args.min_steady_rps is not None and (not math.isfinite(args.min_steady_rps) or args.min_steady_rps <= 0):
            raise ValueError("minimum steady rate must be finite and positive")
        if args.max_steady_rps is not None and (
            args.min_steady_rps is None or not math.isfinite(args.max_steady_rps) or args.max_steady_rps < args.min_steady_rps
        ):
            raise ValueError("maximum steady rate must be finite and at least the configured minimum")
        result = parse(
            Path(args.input),
            set(args.expected_host) if args.expected_host else None,
            steady_after_seconds=args.steady_after_seconds,
        )
    except (OSError, RuntimeError, ValueError, csv.Error) as error:
        Path(args.output).write_text(json.dumps({"schema_version": 1, "status": "fail", "reason": str(error)}, indent=2) + "\n")
        return 1
    result["status"] = "pass"
    if args.min_steady_rps is not None:
        steady = result["steady"]
        steady["min_completed_objects_per_second"] = args.min_steady_rps
        if args.max_steady_rps is not None:
            steady["max_completed_objects_per_second"] = args.max_steady_rps
        if steady["completed_objects_per_second"] < args.min_steady_rps:
            result.update(status="fail", reason="steady completion rate below minimum")
        elif args.max_steady_rps is not None and steady["completed_objects_per_second"] > args.max_steady_rps:
            result.update(status="fail", reason="steady completion rate above maximum")
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return int(result["status"] == "fail")


if __name__ == "__main__":
    sys.exit(main())
