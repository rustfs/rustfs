#!/usr/bin/env python3
"""Run fixed-rate Warp warmup windows until the stability gate passes."""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import adaptive_warmup_gate
import warp_raw_quality

WARMUP_UNSTABLE_EXIT = 95


def result_exit_code(result: dict) -> int:
    if result.get("status") == "pass":
        return 0
    if result.get("reason") == "max_windows_unstable":
        return WARMUP_UNSTABLE_EXIT
    return 1


def preserve_window_evidence(validated: dict, row: dict) -> dict:
    return {**validated, "host_counts": row["host_counts"], "raw_quality": row["raw_quality"]}


def extract_latency_metrics(extractor: Path, log_path: Path) -> dict:
    result = subprocess.run(
        [
            str(extractor),
            "--tool",
            "warp",
            "--warp-mode",
            "put",
            "--extract-metrics-from-log",
            str(log_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError("Warp metrics extractor failed")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) != 2:
        raise ValueError("Warp metrics extractor returned an unexpected schema")
    rows = list(csv.DictReader(lines))
    if len(rows) != 1:
        raise ValueError("Warp metrics extractor returned an unexpected row count")
    row = rows[0]
    try:
        return {
            "avg_ms": float(row["latency_ms"]),
            "p90_ms": float(row["req_p90_ms"]),
            "p99_ms": float(row["req_p99_ms"]),
        }
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("Warp metrics extractor omitted a latency field") from error


def write_gate(path: Path, result: dict, args: argparse.Namespace) -> None:
    output = dict(result)
    output.update(
        schema_version=1,
        target_rps=args.target_rps,
        duration_secs=args.duration_secs,
        concurrency=args.concurrency,
        required_windows=args.required_windows,
        max_windows=args.max_windows,
        bucket=args.bucket,
        hosts=args.host,
    )
    path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warp-bin", required=True)
    parser.add_argument("--metrics-extractor", required=True)
    parser.add_argument("--host", action="append", required=True)
    parser.add_argument("--access-key-env", default="RUSTFS_ACCESS_KEY")
    parser.add_argument("--secret-key-env", default="RUSTFS_SECRET_KEY")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--size", default="100KiB")
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--target-rps", type=int, default=250)
    parser.add_argument("--duration-secs", type=int, default=20)
    parser.add_argument("--required-windows", type=int, default=3)
    parser.add_argument("--max-windows", type=int, default=12)
    parser.add_argument("--quality-tolerance-pct", type=float, default=2)
    parser.add_argument("--max-spread-pct", type=float, default=5)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if not 1 <= args.required_windows <= args.max_windows:
        parser.error("required windows must be between one and max windows")
    if not math.isfinite(args.quality_tolerance_pct) or not math.isfinite(args.max_spread_pct):
        parser.error("tolerances must be finite")
    if args.target_rps <= 0 or args.duration_secs <= 0 or args.concurrency <= 0:
        parser.error("target RPS, duration, and concurrency must be positive")
    if args.quality_tolerance_pct < 0 or args.max_spread_pct < 0:
        parser.error("tolerances must be non-negative")
    access_key = os.environ.get(args.access_key_env)
    secret_key = os.environ.get(args.secret_key_env)
    if not access_key or not secret_key:
        parser.error("credential environment variables must be set")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    windows_path = output_dir / "windows.json"
    gate_path = output_dir / "warmup-gate.json"
    windows = []
    for index in range(1, args.max_windows + 1):
        window_dir = output_dir / f"window-{index:02d}"
        window_dir.mkdir()
        log_path = window_dir / "warp.log"
        benchdata_prefix = window_dir / "warp"
        command = [
            args.warp_bin,
            "put",
            "--host",
            ",".join(args.host),
            "--access-key",
            access_key,
            "--secret-key",
            secret_key,
            "--bucket",
            args.bucket,
            "--region",
            args.region,
            "--obj.size",
            args.size,
            "--concurrent",
            str(args.concurrency),
            "--duration",
            f"{args.duration_secs}s",
            "--rps-limit",
            str(args.target_rps),
            "--host-select",
            "roundrobin",
            "--full",
            "--noclear",
            "--no-color",
            "--analyze.v",
            "--benchdata",
            str(benchdata_prefix),
        ]
        try:
            with log_path.open("w") as log:
                run = subprocess.run(
                    command,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=False,
                    timeout=args.duration_secs + 60,
                )
        except subprocess.TimeoutExpired:
            result = {"status": "fail", "reason": "warp_command_timeout", "windows": windows}
            write_gate(gate_path, result, args)
            return 1
        if run.returncode:
            result = {"status": "fail", "reason": "warp_command_failed", "windows": windows}
            write_gate(gate_path, result, args)
            return 1
        try:
            raw = warp_raw_quality.parse(Path(f"{benchdata_prefix}.csv.zst"), set(args.host))
            latency = extract_latency_metrics(Path(args.metrics_extractor), log_path)
            row = {
                "successful_puts": raw["successful_puts"],
                "errors": raw["errors"],
                "reqps": raw["reqps"],
                "host_counts": raw["host_counts"],
                **latency,
                "raw_quality": str(window_dir / "raw-quality.json"),
            }
            validated = adaptive_warmup_gate.validate_window(
                row,
                args.target_rps,
                args.duration_secs,
                args.concurrency,
                args.quality_tolerance_pct,
            )
            validated = preserve_window_evidence(validated, row)
            (window_dir / "raw-quality.json").write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n")
            windows.append(validated)
            windows_path.write_text(json.dumps(windows, indent=2, sort_keys=True) + "\n")
            result = adaptive_warmup_gate.evaluate(
                windows,
                args.required_windows,
                args.max_windows,
                args.max_spread_pct,
            )
        except (OSError, RuntimeError, ValueError, csv.Error) as error:
            result = {"status": "fail", "reason": str(error), "windows": windows}
            write_gate(gate_path, result, args)
            return 1
        write_gate(gate_path, result, args)
        if result["status"] == "pass":
            return result_exit_code(result)
        if result["status"] == "fail":
            return result_exit_code(result)
    return WARMUP_UNSTABLE_EXIT


if __name__ == "__main__":
    sys.exit(main())
