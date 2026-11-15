#!/usr/bin/env python3
"""Fail closed until Docker VM storage and pressure counters stay quiet."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Sample:
    captured_at_ms: int
    mem_total_kib: int
    dirty_kib: int
    writeback_kib: int
    io_psi_total_us: int
    cpu_psi_total_us: int

    @classmethod
    def from_mapping(cls, value: dict[str, object]) -> "Sample":
        try:
            sample = cls(**{field: int(value[field]) for field in cls.__dataclass_fields__})
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"invalid quiescence sample: {error}") from error
        if sample.captured_at_ms < 0 or sample.mem_total_kib <= 0:
            raise ValueError("invalid quiescence sample bounds")
        if min(sample.dirty_kib, sample.writeback_kib, sample.io_psi_total_us, sample.cpu_psi_total_us) < 0:
            raise ValueError("quiescence counters must be non-negative")
        return sample


def sample_is_quiet(previous: Sample | None, current: Sample) -> tuple[bool, dict[str, float]]:
    dirty_cap_kib = max(64 * 1024, current.mem_total_kib * 0.02)
    writeback_cap_kib = max(8 * 1024, current.mem_total_kib * 0.0025)
    details = {
        "dirty_cap_kib": dirty_cap_kib,
        "writeback_cap_kib": writeback_cap_kib,
        "dirty_rate_kib_s": 0.0,
        "io_psi_rate_us_s": 0.0,
        "cpu_psi_rate_us_s": 0.0,
    }
    quiet = current.dirty_kib <= dirty_cap_kib and current.writeback_kib <= writeback_cap_kib
    if previous is None:
        return quiet, details
    elapsed_s = (current.captured_at_ms - previous.captured_at_ms) / 1000.0
    if elapsed_s <= 0:
        raise ValueError("quiescence sample timestamps must increase")
    if current.io_psi_total_us < previous.io_psi_total_us or current.cpu_psi_total_us < previous.cpu_psi_total_us:
        raise ValueError("pressure counter reset during quiescence gate")
    details["dirty_rate_kib_s"] = abs(current.dirty_kib - previous.dirty_kib) / elapsed_s
    details["io_psi_rate_us_s"] = (current.io_psi_total_us - previous.io_psi_total_us) / elapsed_s
    details["cpu_psi_rate_us_s"] = (current.cpu_psi_total_us - previous.cpu_psi_total_us) / elapsed_s
    quiet = quiet and details["dirty_rate_kib_s"] <= 8 * 1024
    quiet = quiet and details["io_psi_rate_us_s"] <= 100_000
    quiet = quiet and details["cpu_psi_rate_us_s"] <= 200_000
    return quiet, details


def remaining_budget(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("quiescence gate deadline expired")
    return remaining


def run(command: list[str], deadline: float) -> str:
    try:
        return subprocess.run(
            command,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=remaining_budget(deadline),
        ).stdout.strip()
    except subprocess.TimeoutExpired as error:
        raise TimeoutError(f"quiescence gate deadline expired while running {command[0]}") from error


def live_sample(docker: str, context: str, container: str, expected_id: str, deadline: float) -> Sample:
    current_id = run([docker, "--context", context, "inspect", "--format", "{{.Id}}", container], deadline)
    if current_id != expected_id:
        raise RuntimeError("container identity changed during quiescence gate")
    script = r'''
set -eu
awk '/^MemTotal:/{print "mem_total_kib=" $2} /^Dirty:/{print "dirty_kib=" $2} /^Writeback:/{print "writeback_kib=" $2}' /proc/meminfo
awk '/^some /{for(i=1;i<=NF;i++) if($i ~ /^total=/){sub(/^total=/,"",$i); print "io_psi_total_us=" $i}}' /proc/pressure/io
awk '/^some /{for(i=1;i<=NF;i++) if($i ~ /^total=/){sub(/^total=/,"",$i); print "cpu_psi_total_us=" $i}}' /proc/pressure/cpu
'''
    output = run([docker, "--context", context, "exec", container, "sh", "-c", script], deadline)
    remaining_budget(deadline)
    values: dict[str, object] = {"captured_at_ms": int(time.time() * 1000)}
    for line in output.splitlines():
        key, separator, value = line.partition("=")
        if not separator:
            raise RuntimeError(f"invalid telemetry line: {line}")
        values[key] = value
    return Sample.from_mapping(values)


def fixture_samples(path: Path) -> list[Sample]:
    samples = []
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            samples.append(Sample.from_mapping(json.loads(line)))
        except (json.JSONDecodeError, ValueError) as error:
            raise ValueError(f"{path}:{line_number}: {error}") from error
    return samples


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--container")
    parser.add_argument("--docker-context")
    parser.add_argument("--docker-bin", default="docker")
    parser.add_argument("--sample-file", type=Path)
    parser.add_argument("--consecutive", type=int, default=10)
    parser.add_argument("--timeout-secs", type=int, default=120)
    parser.add_argument("--interval-secs", type=float, default=1.0)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    if args.consecutive <= 0 or args.timeout_secs <= 0 or args.interval_secs <= 0:
        parser.error("consecutive, timeout and interval must be positive")
    if bool(args.sample_file) == bool(args.container):
        parser.error("provide exactly one of --sample-file or --container")
    if args.container and not args.docker_context:
        parser.error("--docker-context is required with --container")
    return args


def main() -> int:
    args = parse_args()
    deadline = time.monotonic() + args.timeout_secs
    expected_id = None
    samples = None
    if args.sample_file:
        samples = iter(fixture_samples(args.sample_file))
    else:
        active_context = run([args.docker_bin, "context", "show"], deadline)
        if active_context != args.docker_context:
            raise RuntimeError(f"docker context changed: expected {args.docker_context}, active {active_context}")
        expected_id = run(
            [args.docker_bin, "--context", args.docker_context, "inspect", "--format", "{{.Id}}", args.container],
            deadline,
        )

    previous = None
    consecutive = 0
    observed = 0
    last_details: dict[str, float] = {}
    while time.monotonic() <= deadline:
        try:
            sample = next(samples) if samples is not None else live_sample(
                args.docker_bin, args.docker_context, args.container, expected_id, deadline
            )
        except StopIteration:
            break
        quiet, last_details = sample_is_quiet(previous, sample)
        remaining_budget(deadline)
        observed += 1
        consecutive = consecutive + 1 if quiet else 0
        previous = sample
        if consecutive >= args.consecutive:
            result = {"status": "pass", "observed_samples": observed, "consecutive_samples": consecutive, **last_details}
            if args.output_json:
                args.output_json.write_text(json.dumps(result, sort_keys=True) + "\n")
            print(json.dumps(result, sort_keys=True))
            return 0
        if samples is None:
            time.sleep(min(args.interval_secs, remaining_budget(deadline)))

    result = {"status": "inconclusive", "observed_samples": observed, "consecutive_samples": consecutive, **last_details}
    if args.output_json:
        args.output_json.write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"quiescence gate failed: {error}", file=sys.stderr)
        raise SystemExit(2)
