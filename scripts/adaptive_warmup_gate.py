#!/usr/bin/env python3
"""Fail-closed stability gate for fixed-rate benchmark warmup windows."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path


def positive_number(row: dict, name: str) -> float:
    value = row.get(name)
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"window field {name} must be a positive number")
    return float(value)


def validate_window(row: dict, target_rps: float, duration_secs: float, concurrency: int, tolerance_pct: float) -> dict:
    successful_puts = positive_number(row, "successful_puts")
    errors = row.get("errors")
    if not isinstance(errors, int) or isinstance(errors, bool) or errors < 0:
        raise ValueError("window field errors must be a non-negative integer")
    if errors:
        raise ValueError("warmup window contains request errors")
    reqps = positive_number(row, "reqps")
    scheduled = target_rps * duration_secs
    tolerance = tolerance_pct / 100
    minimum_puts = scheduled * (1 - tolerance)
    maximum_puts = (scheduled + concurrency - 1) * (1 + tolerance)
    if not minimum_puts <= successful_puts <= maximum_puts:
        raise ValueError("warmup successful PUT count is outside tolerance")
    if abs(reqps - target_rps) / target_rps * 100 > tolerance_pct:
        raise ValueError("warmup request rate is outside tolerance")
    host_counts = row.get("host_counts")
    if not isinstance(host_counts, dict) or len(host_counts) < 2:
        raise ValueError("warmup host_counts must contain at least two hosts")
    counts = [value for value in host_counts.values() if isinstance(value, int) and not isinstance(value, bool) and value >= 0]
    if len(counts) != len(host_counts) or sum(counts) <= 0:
        raise ValueError("warmup host_counts contains invalid values")
    if sum(counts) != successful_puts:
        raise ValueError("warmup host counts do not match successful PUT count")
    ideal = sum(counts) / len(counts)
    host_deviation_pct = max(abs(value - ideal) / ideal * 100 for value in counts)
    if host_deviation_pct > tolerance_pct:
        raise ValueError("warmup host distribution is outside tolerance")
    return {
        "successful_puts": int(successful_puts),
        "errors": errors,
        "reqps": reqps,
        "avg_ms": positive_number(row, "avg_ms"),
        "p90_ms": positive_number(row, "p90_ms"),
        "p99_ms": positive_number(row, "p99_ms"),
        "host_deviation_pct": host_deviation_pct,
    }


def spread_pct(values: list[float]) -> float:
    median = statistics.median(values)
    return (max(values) - min(values)) / median * 100


def evaluate(rows: list[dict], required_windows: int, max_windows: int, max_spread_pct: float) -> dict:
    if len(rows) < required_windows:
        return {"status": "continue", "reason": "insufficient_windows", "windows": rows}
    recent = rows[-required_windows:]
    spreads = {name: spread_pct([row[name] for row in recent]) for name in ("reqps", "avg_ms", "p90_ms", "p99_ms")}
    stable = all(value <= max_spread_pct for value in spreads.values())
    if stable:
        return {"status": "pass", "reason": "stable", "windows": rows, "recent_spread_pct": spreads}
    if len(rows) >= max_windows:
        return {"status": "fail", "reason": "max_windows_unstable", "windows": rows, "recent_spread_pct": spreads}
    return {"status": "continue", "reason": "recent_windows_unstable", "windows": rows, "recent_spread_pct": spreads}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--target-rps", type=float, default=250)
    parser.add_argument("--duration-secs", type=float, default=20)
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--required-windows", type=int, default=3)
    parser.add_argument("--max-windows", type=int, default=12)
    parser.add_argument("--quality-tolerance-pct", type=float, default=2)
    parser.add_argument("--max-spread-pct", type=float, default=5)
    args = parser.parse_args()
    output = Path(args.output)
    try:
        raw = json.loads(Path(args.input).read_text())
        if not isinstance(raw, list):
            raise ValueError("warmup input must be a JSON array")
        if not 1 <= args.required_windows <= args.max_windows:
            raise ValueError("required windows must be between one and max windows")
        numeric_args = (args.target_rps, args.duration_secs, args.quality_tolerance_pct, args.max_spread_pct)
        if not all(math.isfinite(value) for value in numeric_args):
            raise ValueError("warmup numeric arguments must be finite")
        if args.target_rps <= 0 or args.duration_secs <= 0 or args.concurrency <= 0:
            raise ValueError("target RPS, duration, and concurrency must be positive")
        if args.quality_tolerance_pct < 0 or args.max_spread_pct < 0:
            raise ValueError("tolerances must be non-negative")
        if len(raw) > args.max_windows:
            raise ValueError("warmup input exceeds max windows")
        windows = [
            validate_window(row, args.target_rps, args.duration_secs, args.concurrency, args.quality_tolerance_pct)
            for row in raw
        ]
        result = evaluate(windows, args.required_windows, args.max_windows, args.max_spread_pct)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        result = {"status": "fail", "reason": str(error), "windows": []}
    result["schema_version"] = 1
    result["target_rps"] = args.target_rps
    result["duration_secs"] = args.duration_secs
    result["concurrency"] = args.concurrency
    result["required_windows"] = args.required_windows
    result["max_windows"] = args.max_windows
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if result["status"] == "pass":
        return 0
    if result["status"] == "continue":
        return 2
    return 1


if __name__ == "__main__":
    sys.exit(main())
