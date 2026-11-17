#!/usr/bin/env python3
"""Recompute fixed-rate Warp quality evidence from raw TSV benchdata."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
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


def parse(path: Path, expected_hosts: set[str] | None) -> dict:
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
            if None in row:
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
            rows.append((start, end, objects, error))
        if not rows or raw_errors:
            raise ValueError("Warp raw input is empty or contains request errors")
        if expected_hosts is not None and set(host_counts) != expected_hosts:
            raise ValueError("Warp raw endpoints differ from the expected host set")
        window_start = max(first_end.values())
        window_end = min(last_start.values())
        if window_end <= window_start:
            raise ValueError("Warp common active window is not positive")
        completed_objects = sum(objects for _, end, objects, error in rows if not error and window_start <= end < window_end)
        completed_requests = sum(1 for _, end, _, error in rows if not error and window_start <= end < window_end)
        fractional_objects = 0.0
        for start, end, objects, error in rows:
            if error or end <= start:
                continue
            overlap = max(0, min(end, window_end) - max(start, window_start))
            fractional_objects += objects * overlap / (end - start)
        duration_secs = (window_end - window_start) / 1_000_000_000
        return {
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
    args = parser.parse_args()
    try:
        result = parse(Path(args.input), set(args.expected_host) if args.expected_host else None)
    except (OSError, RuntimeError, ValueError, csv.Error) as error:
        Path(args.output).write_text(json.dumps({"schema_version": 1, "status": "fail", "reason": str(error)}, indent=2) + "\n")
        return 1
    result["status"] = "pass"
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
