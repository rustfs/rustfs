#!/usr/bin/env python3
"""Run the identical application-read test harness in serial ABBA processes.

This measures local EC reads, not network/authentication or deployment capacity.
Use matching toolchain, features and profiles for both executable artifacts.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import time


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def parse_rows(output):
    rows = {}
    prefix = "CONDITIONAL_BENCH "
    for line in output.splitlines():
        if prefix in line:
            row = json.loads(line.split(prefix, 1)[1])
            key = (row["size"], row["kind"], row["cache"], row["slowtail_ms"])
            if key in rows:
                raise ValueError(f"duplicate benchmark row {key}")
            rows[key] = row
    if len(rows) != 6:
        raise ValueError(f"expected six benchmark rows, received {len(rows)}")
    if {(key[0], key[1]) for key in rows} != {
        (size, kind) for size in (4096, 1_300_000) for kind in ("unconditional", "hit", "miss")
    }:
        raise ValueError("benchmark workload matrix is incomplete")
    for row in rows.values():
        for metric in ("ops_per_sec", "p95_ms", "p99_ms", "lock_p95_ms", "lock_p99_ms"):
            if not math.isfinite(row[metric]) or row[metric] <= 0:
                raise ValueError(f"invalid benchmark metric {metric}")
    return rows


def check_resource_isolation(check_load=True):
    commands = subprocess.check_output(["ps", "-A", "-o", "comm="], text=True)
    if any(Path(command.strip()).name in ("cargo", "rustc") for command in commands.splitlines()):
        raise RuntimeError("stop concurrent Cargo builds before measuring")
    load = os.getloadavg()
    if check_load and load[0] > (os.cpu_count() or 1) * 0.5:
        raise RuntimeError(f"host load too high for paired measurements: {load[0]}")
    return load


def run_case(executable, expected_digest, env, log_path=None):
    if digest(executable) != expected_digest:
        raise RuntimeError("benchmark executable changed before execution")
    # Poll for concurrent builds without attributing this benchmark's own CPU
    # use to external host load. Load gates run before and after the process.
    with subprocess.Popen([str(executable.resolve()), "conditional_read_paired_benchmark",
                           "--ignored", "--nocapture", "--test-threads=1"],
                          env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as run:
        try:
            while True:
                try:
                    output, _ = run.communicate(timeout=1)
                    break
                except subprocess.TimeoutExpired:
                    check_resource_isolation(check_load=False)
        except BaseException:
            run.kill()
            output, _ = run.communicate()
            if log_path is not None:
                log_path.write_text(output)
            raise
        returncode = run.returncode
    if log_path is not None:
        log_path.write_text(output)
    check_resource_isolation()
    if digest(executable) != expected_digest:
        raise RuntimeError("benchmark executable changed during execution")
    return returncode, output


def drift_passes(first, last):
    if abs(last["ops_per_sec"] / first["ops_per_sec"] - 1) > 0.10:
        return False
    return all(
        abs(last[metric] - first[metric]) <= max(first[metric] * 0.15, 0.25)
        for metric in ("p95_ms", "p99_ms", "lock_p95_ms", "lock_p99_ms")
    )


def compare(rows):
    if not all(rows[0].keys() == leg.keys() for leg in rows):
        raise ValueError("ABBA legs have different workload matrices")
    unstable = [key for key in rows[0] if not drift_passes(rows[0][key], rows[3][key])]
    if unstable:
        return {"accepted": False, "reason": "baseline_drift", "unstable_cases": unstable}
    results = []
    for key, first in rows[0].items():
        baseline = {metric: math.sqrt(first[metric] * rows[3][key][metric]) for metric in
                    ("ops_per_sec", "p95_ms", "p99_ms", "lock_p95_ms", "lock_p99_ms")}
        candidate = {metric: math.sqrt(rows[1][key][metric] * rows[2][key][metric]) for metric in baseline}
        # Protect ordinary reads and condition misses; 304 savings alone do not pass.
        passed = candidate["ops_per_sec"] >= baseline["ops_per_sec"] * 0.95 and all(
            candidate[metric] <= baseline[metric] + max(baseline[metric] * 0.10, 0.25)
            for metric in ("p95_ms", "p99_ms", "lock_p95_ms", "lock_p99_ms")
        )
        results.append({"case": key, "baseline": baseline, "candidate": candidate,
                        "within_regression_budget": passed})
    return {"accepted": all(row["within_regression_budget"] for row in results), "cases": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--cooldown-seconds", type=float, default=5)
    args = parser.parse_args()
    if args.iterations < 100 or args.concurrency < 1 or args.cooldown_seconds < 0:
        parser.error("iterations must be >=100, concurrency >0, cooldown >=0")
    args.out_dir.mkdir(parents=True, exist_ok=False)
    executables = {}
    for name, source in (("baseline", args.baseline), ("candidate", args.candidate)):
        original_digest = digest(source)
        snapshot = args.out_dir / f"{name}.binary"
        shutil.copyfile(source, snapshot)
        snapshot.chmod(0o555)
        if digest(snapshot) != original_digest:
            raise RuntimeError("source executable changed while taking its snapshot")
        executables[name] = snapshot
    manifest = {
        "baseline_revision": args.baseline_revision, "candidate_revision": args.candidate_revision,
        "baseline_sha256": digest(executables["baseline"]), "candidate_sha256": digest(executables["candidate"]),
        "harness_sha256": digest(args.harness), "profile": args.profile,
        "iterations": args.iterations, "concurrency": args.concurrency,
        "order": ["A1", "B1", "B2", "A2"], "slowtail_ms": [0, 20], "cache": [False, True],
        "drift_budget": "throughput 10%; latency and lock percentiles 15% or 0.25ms",
        "regression_budget": "throughput -5%; latency and lock percentiles +10% or 0.25ms",
        "cpu_count": os.cpu_count(), "load_at_start": check_resource_isolation(),
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    legs = []
    for name, artifact in (("A1", "baseline"), ("B1", "candidate"),
                           ("B2", "candidate"), ("A2", "baseline")):
        executable = executables[artifact]
        cases = {}
        for cache in (False, True):
            for delay in (0, 20):
                check_resource_isolation()
                env = os.environ.copy()
                # Keep ambient experiments and proxies out of localhost-sensitive runs.
                for variable in tuple(env):
                    if variable.startswith("RUSTFS_") or variable.lower().endswith("_proxy"):
                        del env[variable]
                env.update({
                    "NO_PROXY": "*", "no_proxy": "*", "RUSTFS_OBJECT_LOCK_DIAG_ENABLE": "true",
                    "RUSTFS_OBJECT_DATA_CACHE_ENABLE": str(cache).lower(),
                    "RUSTFS_OBJECT_DATA_CACHE_MODE": "fill_materialize_enabled",
                    "RUSTFS_OBJECT_DATA_CACHE_MAX_BYTES": "8388608",
                    "RUSTFS_OBJECT_DATA_CACHE_MAX_ENTRY_BYTES": "2097152",
                    "RUSTFS_OBJECT_DATA_CACHE_MIN_FREE_MEMORY_PERCENT": "0",
                    "RUSTFS_CONDITIONAL_BENCH_ITERATIONS": str(args.iterations),
                    "RUSTFS_CONDITIONAL_BENCH_CONCURRENCY": str(args.concurrency),
                    "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DELAY_MS": str(delay),
                    "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_DISKS": "3",
                    "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_BUCKET": "conditional-bench",
                    "RUSTFS_GET_METADATA_SLOWTAIL_FAULT_OBJECT_PREFIX": "bench-",
                })
                returncode, output = run_case(executable, manifest[f"{artifact}_sha256"], env,
                                             args.out_dir / f"{name}-cache{int(cache)}-delay{delay}.log")
                if returncode:
                    raise RuntimeError(f"{name} cache={cache} delay={delay} failed; see saved log")
                for key, row in parse_rows(output).items():
                    if (row["cache"] != cache or row["slowtail_ms"] != str(delay)
                            or row["iterations"] != args.iterations or row["concurrency"] != args.concurrency
                            or row["metadata_cache_hits"] != 0 or not row["metadata_cache_bypassed"]
                            or row["lock_samples"] <= 0):
                        raise ValueError("benchmark did not observe requested environment")
                    cases[key] = row
                print(f"{name}: cache={cache} slowtail={delay}ms complete", flush=True)
        legs.append(cases)
        time.sleep(args.cooldown_seconds)
    result = compare(legs)
    result["load_at_end"] = os.getloadavg()
    (args.out_dir / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if result["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
