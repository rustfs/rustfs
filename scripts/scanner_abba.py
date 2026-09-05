#!/usr/bin/env python3
"""Run isolated scanner/heal ABBA cells through a deployment-specific adapter."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

SCENARIOS = ("cold-hot", "fresh-hot", "multi-hot-new", "running-heal", "mrf-replay")
LEGS = ("A1", "B1", "B2", "A2")
MAX_JSON_BYTES = 1024 * 1024
METRICS = (
    "p99_ms", "throughput_ops", "rss_bytes", "cpu_seconds", "iops", "rpc_count",
    "cache_clone_bytes", "encode_bytes", "save_bytes", "oldest_age_seconds",
    "walk_objects", "cold_walk_objects", "healed_objects", "errors", "requests",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, name, minimum=0):
    require(type(value) in (float, int) and math.isfinite(value) and value >= minimum,
            f"invalid {name}")
    return value


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path):
    require(path.stat().st_size <= MAX_JSON_BYTES, f"oversized JSON: {path.name}")
    with path.open() as stream:
        value = json.load(stream)
    require(isinstance(value, dict), f"expected JSON object: {path.name}")
    return value


def write_json(path, value):
    data = json.dumps(value, indent=2, allow_nan=False) + "\n"
    require(len(data.encode()) <= MAX_JSON_BYTES, "oversized result")
    path.write_text(data)


def sha(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def validate_manifest(manifest):
    require(manifest.get("schema") == 1, "unsupported manifest schema")
    require(manifest.get("evidence") in ("synthetic", "measured"), "missing evidence type")
    fixed = manifest["fixed"]
    for key in ("config_sha256", "dataset_sha256"):
        require(sha(fixed.get(key)), f"invalid fixed.{key}")
    for key in ("release_flags", "durability", "disk_type", "cache_state", "load_command", "resource_isolation"):
        require(isinstance(fixed.get(key), str) and fixed[key].strip(), f"missing fixed.{key}")
    require(fixed.get("topology") == "EC8+4", "formal matrix requires EC8+4")
    number(fixed.get("offered_load_ops"), "offered load", 1)
    require(type(manifest.get("rounds")) is int and 3 <= manifest["rounds"] <= 10,
            "rounds must be 3..10")
    minimum = 900 if manifest["evidence"] == "measured" else 1
    require(type(manifest.get("duration_seconds")) is int and
            minimum <= manifest["duration_seconds"] <= 86400, "invalid duration_seconds")
    number(manifest.get("min_free_bytes"), "min_free_bytes", 1)
    for phase in ("baseline", "candidate"):
        build = manifest[phase]
        path = Path(build["binary"]).resolve(strict=True)
        require(path.is_file() and os.access(path, os.X_OK), f"missing executable {phase} build")
        require(sha(build.get("sha256")) and digest(path) == build["sha256"], f"{phase} binary hash mismatch")
        require(isinstance(build.get("revision"), str) and len(build["revision"]) == 40 and
                all(c in "0123456789abcdef" for c in build["revision"]), f"invalid {phase} revision")
        build["binary"] = str(path)
    for scenario in SCENARIOS:
        expected = manifest["oracles"][scenario]
        for key in ("objects", "versions", "bytes"):
            require(type(expected.get(key)) is int and expected[key] > 0, f"missing {scenario} oracle {key}")
        require(sha(expected.get("sha256")), f"missing {scenario} content/version digest")
        number(manifest["expected_healed_objects"].get(scenario), f"{scenario} expected repairs")
        if scenario in ("running-heal", "mrf-replay"):
            require(manifest["expected_healed_objects"][scenario] > 0, f"{scenario} requires repairs")


def invoke(adapter, action, request, timeout):
    """The adapter writes bounded JSON separately; stderr/stdout remain raw evidence."""
    output = request.parent / f"{action}.json"
    with (request.parent / f"{action}.log").open("wb") as log:
        process = subprocess.Popen([str(adapter), action, str(request), str(output)],
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            returncode = process.wait(timeout=timeout)
            if returncode:
                raise subprocess.CalledProcessError(returncode, [str(adapter), action])
        finally:
            if process.poll() != 0:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
    return read_json(output)


def validate_result(result, request, expected):
    require(result.get("evidence") == request["evidence"], "adapter evidence type mismatch")
    require(result.get("fixed") == request["fixed"], "offered load/config/cache/durability drift")
    require(result.get("build") == request["build"], "deployed build provenance mismatch")
    require(result.get("data_dir") == request["data_dir"], "adapter data isolation mismatch")
    require(result.get("background") == request["background"], "background mode mismatch")
    require(type(result.get("sample_count")) is int and 1 <= result["sample_count"] <= 3600,
            "sample_count must be 1..3600")
    number(result.get("elapsed_seconds"), "elapsed_seconds", request["duration_seconds"])
    metrics = result["metrics"]
    for key in METRICS:
        number(metrics.get(key), key)
    for key in ("requests", "p99_ms", "throughput_ops"):
        require(metrics[key] > 0, f"zero {key}")
    require(metrics["errors"] == 0, "workload request errors")
    require(metrics["cold_walk_objects"] <= metrics["walk_objects"], "cold walk exceeds total walk")
    require(result.get("oracle") == expected, "object/version/byte oracle mismatch")
    if request["background"] == "on":
        require(metrics["walk_objects"] > 0, "zero background walk")
        require(metrics["healed_objects"] == request["expected_healed_objects"], "incomplete repair oracle")
        if request["scenario"] in ("running-heal", "mrf-replay"):
            require(metrics["healed_objects"] > 0, "zero completed repairs")
    return result


def convergence(result):
    window = result.get("convergence")
    if not window or window.get("writes_stopped") is not True or window.get("last_mutation_observed") is not True or window.get("first_complete_publication") is not True:
        return None
    for key in ("last_mutation_time", "last_mutation_observed_time", "writes_stopped_time", "window_start", "window_end", "walk_objects", "full_walk_objects", "budget_available_seconds"):
        number(window.get(key), f"convergence.{key}")
    require(window["last_mutation_time"] <= window["writes_stopped_time"] <= window["window_start"] < window["window_end"],
            "invalid post-mutation convergence window")
    require(window["last_mutation_time"] <= window["last_mutation_observed_time"] <= window["window_start"],
            "convergence started before last mutation was observed")
    require(window["full_walk_objects"] > 0, "zero full walk reference")
    require(0 < window["budget_available_seconds"] <= window["window_end"] - window["window_start"],
            "invalid convergence budget window")
    return window["walk_objects"] / window["full_walk_objects"]


def evaluate(cells):
    comparisons = []
    inconclusive = False
    failed = False
    for offset in range(0, len(cells), 4):
        group = cells[offset:offset + 4]
        require([cell["leg"] for cell in group] == list(LEGS), "incomplete ABBA group")
        a1, b1, b2, a2 = (cell["result"]["metrics"] for cell in group)
        control = group[0]["comparison"] == "background"
        drift = max(abs(a2[k] / a1[k] - 1) for k in ("p99_ms", "throughput_ops"))
        repeat_drift = max(abs(b2[k] / b1[k] - 1) for k in ("p99_ms", "throughput_ops"))
        noise = max(drift, repeat_drift) > 0.05
        a = {key: (a1[key] + a2[key]) / 2 for key in METRICS}
        b = {key: (b1[key] + b2[key]) / 2 for key in METRICS}
        p99 = b["p99_ms"] / a["p99_ms"] - 1
        throughput = b["throughput_ops"] / a["throughput_ops"] - 1
        thresholds = {"p99_regression": 0.10 if control else 0.05,
                      "throughput_loss": 0.05 if control else 0.03}
        passed = p99 <= thresholds["p99_regression"] and throughput >= -thresholds["throughput_loss"]
        p1 = None
        if not control:
            if group[0]["scenario"] == "cold-hot":
                require(a["cold_walk_objects"] > 0, "cold-hot baseline has no cold walk samples")
            required = a["cold_walk_objects"] / a["walk_objects"] * 0.80
            reduction = 1 - b["walk_objects"] / a["walk_objects"]
            p1 = {"required_reduction": required, "observed_reduction": reduction}
            if group[0]["scenario"] == "cold-hot":
                passed &= reduction >= required
        p2 = [convergence(cell["result"]) if cell["background"] == "on" else None for cell in group]
        candidate_p2 = [value for cell, value in zip(group, p2) if cell["leg"].startswith("B")]
        p2_pending = any(value is None for value in candidate_p2)
        passed &= all(value <= 1.2 for value in candidate_p2 if value is not None)
        inconclusive |= noise or p2_pending
        if not noise and not passed:
            failed = True
        comparisons.append({"scenario": group[0]["scenario"], "comparison": group[0]["comparison"],
                            "round": group[0]["round"], "status": "inconclusive" if noise else ("fail" if not passed else "inconclusive" if p2_pending else "pass"),
                            "a2_a1_drift": drift, "b2_b1_drift": repeat_drift,
                            "p99_regression": p99, "throughput_change": throughput,
                            "thresholds": thresholds, "p1": p1, "p2_max_work_multiple": 1.2,
                            "p2_post_stop_work_multiples": p2})
    return ("fail" if failed else "inconclusive" if inconclusive else "pass"), comparisons


def collect_live(prepared, request, request_path, adapter):
    collector = Path(__file__).with_name("run_scanner_validation_harness.sh")
    # Only allow connection fields here; the runner owns cadence and output paths.
    connection = prepared["collector"]
    require(set(connection) == {"alias", "endpoint", "metrics_endpoints"}, "invalid collector connection")
    require(all(isinstance(value, str) and value for value in connection.values()), "missing collector endpoint")
    output = request_path.parent / "telemetry"
    args = ["bash", str(collector), "--alias", connection["alias"], "--endpoint", connection["endpoint"],
            "--metrics-endpoints", connection["metrics_endpoints"], "--deployment", "distributed",
            "--samples", str(request["duration_seconds"] // 60 + 1), "--interval-secs", "60",
            "--out-dir", str(output)]
    with (request_path.parent / "collector.log").open("wb") as log:
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            started = time.monotonic()
            result = invoke(adapter, "measure", request_path, request["duration_seconds"] + 300)
            require(time.monotonic() - started >= request["duration_seconds"], "measurement ended before required window")
            require(process.wait(timeout=120) == 0, "scanner collector failed")
            require(output.joinpath("scanner-summary.csv").stat().st_size > 0, "missing collector samples")
            samples = list((output / "status").glob("scanner-status.*.json"))
            require(len(samples) == request["duration_seconds"] // 60 + 1, "missing scanner samples")
            for sample in samples:
                status = read_json(sample)
                require(isinstance(status.get("metrics"), dict) and status["metrics"], "invalid scanner status response")
            heals = list((output / "heal").glob("background-heal-status.*.json"))
            require(bool(heals), "missing heal samples")
            for sample in heals:
                status = read_json(sample)
                require(isinstance(status.get("healOperations"), dict) and status["healOperations"], "invalid heal status response")
            return result
        finally:
            # Stop telemetry children as well when measurement fails or times out.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()


def run(manifest, adapter, output, data_root):
    validate_manifest(manifest)
    require(adapter.is_file() and os.access(adapter, os.X_OK), "missing executable adapter")
    require(not output.exists() and not data_root.exists(), "output/data root must be new; existing data is preserved")
    require(output != data_root and output not in data_root.parents and data_root not in output.parents,
            "output and data roots must not overlap")
    output.mkdir(parents=True)
    data_root.mkdir(parents=True)
    require(shutil.disk_usage(data_root).free >= manifest["min_free_bytes"], "insufficient free disk space")
    manifest["adapter_sha256"] = digest(adapter)
    manifest["collector_sha256"] = digest(Path(__file__).with_name("run_scanner_validation_harness.sh"))
    write_json(output / "manifest.json", manifest)
    cells = []
    write_json(output / "report.json", {"status": "incomplete", "performance": "pending"})
    try:
        for scenario in SCENARIOS:
            for comparison in ("build", "background"):
                for round_id in range(1, manifest["rounds"] + 1):
                    for leg in LEGS:
                        phase = "baseline" if comparison == "build" and leg.startswith("A") else "candidate"
                        background = "off" if comparison == "background" and leg.startswith("A") else "on"
                        name = f"{scenario}-{comparison}-{round_id}-{leg}"
                        cell_dir = output / name
                        cell_dir.mkdir()
                        data_dir = data_root / name
                        data_dir.mkdir()
                        request = {"schema": 1, "scenario": scenario, "comparison": comparison, "round": round_id,
                                   "leg": leg, "background": background, "build": manifest[phase],
                                   "evidence": manifest["evidence"], "fixed": manifest["fixed"],
                                   "duration_seconds": manifest["duration_seconds"], "data_dir": str(data_dir),
                                   "expected_healed_objects": manifest["expected_healed_objects"][scenario],
                                   "expected_oracle": manifest["oracles"][scenario]}
                        require(digest(Path(request["build"]["binary"])) == request["build"]["sha256"], "binary changed during run")
                        require(digest(adapter) == manifest["adapter_sha256"], "adapter changed during run")
                        require(shutil.disk_usage(data_root).free >= manifest["min_free_bytes"], "insufficient free disk space")
                        request_path = cell_dir / "request.json"
                        write_json(request_path, request)
                        print(name, flush=True)
                        try:
                            prepared = invoke(adapter, "prepare", request_path, 300)
                            require(prepared.get("ready") is True, "deployment not ready")
                            if manifest["evidence"] == "measured":
                                result = collect_live(prepared, request, request_path, adapter)
                            else:
                                result = invoke(adapter, "measure", request_path, 300)
                            # An independent operation must enumerate all object versions and bytes.
                            oracle = invoke(adapter, "oracle", request_path, 300)
                            require(oracle.get("complete") is True and oracle.get("errors") == 0, "correctness oracle failed")
                            require(type(oracle.get("errors")) is int, "invalid oracle error count")
                            result["oracle"] = oracle["actual"]
                            validate_result(result, request, request["expected_oracle"])
                            cells.append({**request, "result": result})
                        finally:
                            stopped = invoke(adapter, "stop", request_path, 300)
                            require(stopped.get("stopped") is True, "adapter failed to stop deployment")
        status, comparisons = evaluate(cells)
        synthetic = manifest["evidence"] == "synthetic"
        report = {"status": "synthetic_validated" if synthetic and status == "pass" else status,
                  "evidence": manifest["evidence"], "performance": "pending" if synthetic else status,
                  "cells": len(cells), "comparisons": comparisons}
        write_json(output / "report.json", report)
        return 0 if status == "pass" else 3 if status == "inconclusive" else 1
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        write_json(output / "report.json", {"status": "failed", "performance": "pending",
                                            "completed_cells": len(cells), "error": str(error)})
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--adapter", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        return run(read_json(args.manifest), args.adapter.resolve(), args.out_dir.resolve(), args.data_root.resolve())
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
