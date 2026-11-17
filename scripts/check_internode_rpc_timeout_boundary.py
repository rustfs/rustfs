#!/usr/bin/env python3
"""Verify recorded internode RPC timeout-boundary and recovery evidence."""

import argparse
import hashlib
import json
from pathlib import Path
import sys


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_REPORT_BYTES = 512 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def uint(value, name):
    require(type(value) is int and value >= 0, f"invalid {name}")
    return value


def load(path, limit):
    require(path.is_file() and path.stat().st_size <= limit, f"missing or oversized evidence: {path.name}")
    raw = path.read_bytes()
    return json.loads(raw, object_pairs_hook=unique_object), hashlib.sha256(raw).hexdigest()


def evidence_path(root, value, seen):
    require(isinstance(value, str) and value, "missing evidence path")
    path = (root / value).resolve()
    require(path == root or root in path.parents, "evidence path escapes manifest directory")
    require(path not in seen, "evidence file reused across phases")
    seen.add(path)
    return path


def samples(report, operation):
    rows = report.get("samples")
    require(isinstance(rows, list) and rows, "missing probe samples")
    return [row for row in rows if row.get("operation") == operation]


def report_status(report):
    require(report.get("complete") is True, "probe report is incomplete")
    require(report.get("bulk_coverage_verified") is True, "probe report lacks validated bulk coverage")
    duration = uint(report.get("offered_duration_secs"), "offered duration")
    rate = uint(report.get("control_rps"), "control rate")
    expected = uint(report.get("control_expected"), "expected control count")
    require(duration > 0 and rate > 0 and expected == duration * rate, "invalid control offering contract")
    ping = samples(report, "Ping")
    read_all = samples(report, "ReadAll")
    require(len(ping) == expected and read_all, "probe sample coverage does not match its contract")
    allowed = {"ok", "deadline", "schedule_drop", "rpc_error"}
    require(all(row.get("outcome") in allowed for row in ping + read_all), "unknown probe outcome")
    observed_errors = sum(row.get("outcome") != "ok" for row in ping + read_all)
    require(uint(report.get("errors"), "probe error count") == observed_errors, "probe error count does not match samples")
    fixture = report.get("fixture_sha256")
    require(isinstance(fixture, str) and len(fixture) == 64 and all(char in "0123456789abcdef" for char in fixture),
            "invalid fixture identity")
    targets = uint(report.get("targets"), "target count")
    peers = uint(report.get("peers"), "peer count")
    workers = uint(report.get("bulk_workers"), "bulk worker count")
    scope = report.get("scope")
    require(targets > 0 and peers > 0 and workers > 0 and isinstance(scope, str) and scope,
            "invalid probe workload identity")
    identity = {"fixture_sha256": fixture, "targets": targets, "peers": peers, "bulk_workers": workers,
                "control_rps": rate, "scope": scope}
    return ping, read_all, identity


def phase(report, expected):
    ping, read_all, identity = report_status(report)
    outcomes = [row["outcome"] for row in ping + read_all]
    if expected == "success":
        return not outcomes.count("deadline") and not outcomes.count("schedule_drop") and not outcomes.count("rpc_error"), identity
    require(expected == "deadline", "unknown timeout-boundary expectation")
    return outcomes.count("deadline") > 0 and not outcomes.count("schedule_drop") and not outcomes.count("rpc_error"), identity


def check(manifest, root):
    root = root.resolve()
    require(isinstance(manifest, dict) and manifest.get("schema") == 1, "unsupported timeout-boundary schema")
    require(manifest.get("scope") == "internode-rpc-timeout-boundary", "wrong timeout-boundary scope")
    timeout = uint(manifest.get("rpc_timeout_ms"), "RPC timeout")
    require(timeout > 0, "RPC timeout must be positive")
    seen = set()
    receipts = []
    verdicts = {}
    workload_identity = None
    phases = (("under_budget", "success"), ("over_budget", "deadline"), ("recovery", "success"))
    for name, expectation in phases:
        descriptor = manifest.get(name)
        require(isinstance(descriptor, dict), f"missing {name} descriptor")
        if name != "recovery":
            fault_path = evidence_path(root, descriptor.get("fault_receipt"), seen)
            fault, fault_digest = load(fault_path, MAX_MANIFEST_BYTES)
            require(isinstance(fault, dict) and isinstance(fault.get("target"), str) and fault["target"],
                    f"invalid {name} fault target")
            requested = uint(fault.get("requested_stall_ms"), f"{name} requested stall")
            actual = uint(fault.get("actual_stall_ms"), f"{name} actual stall")
            require(0 < requested <= actual, f"invalid {name} stall receipt")
            if name == "under_budget":
                require(actual < timeout, "under-budget stall crossed the RPC timeout")
            else:
                require(requested > timeout and actual > timeout, "over-budget stall did not cross the RPC timeout")
            receipts.append({"phase": name, "kind": "fault", "path": descriptor["fault_receipt"],
                             "sha256": fault_digest})
        path = evidence_path(root, descriptor.get("report"), seen)
        report, digest = load(path, MAX_REPORT_BYTES)
        verdicts[name], identity = phase(report, expectation)
        if workload_identity is None:
            workload_identity = identity
        else:
            require(identity == workload_identity, "probe workload identity changed across timeout phases")
        receipts.append({"phase": name, "kind": "report", "path": descriptor["report"], "sha256": digest})
    return {
        "observed_pass": all(verdicts.values()),
        "scope": manifest["scope"],
        "rpc_timeout_ms": timeout,
        "phases": verdicts,
        "workload": workload_identity,
        "reports": receipts,
        "limits": "Recorded request outcomes only; deployment ownership, fault delivery, Prometheus windows and performance attribution remain separate evidence.",
    }, seen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    try:
        require(args.manifest.is_file() and args.manifest.stat().st_size <= MAX_MANIFEST_BYTES,
                "missing or oversized timeout-boundary manifest")
        raw = args.manifest.read_bytes()
        manifest = json.loads(raw, object_pairs_hook=unique_object)
        result, inputs = check(manifest, args.manifest.resolve().parent)
        output = args.out.resolve()
        require(output not in inputs | {args.manifest.resolve()}, "output would overwrite raw evidence")
        if args.out.exists():
            require(not any(args.out.samefile(path) for path in inputs | {args.manifest.resolve()}),
                    "output aliases raw evidence")
        result["manifest_sha256"] = hashlib.sha256(raw).hexdigest()
        args.out.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
        return 0 if result["observed_pass"] else 1
    except (ValueError, OSError, TypeError, KeyError, AttributeError, json.JSONDecodeError) as error:
        print(f"Invalid internode RPC timeout-boundary evidence: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
