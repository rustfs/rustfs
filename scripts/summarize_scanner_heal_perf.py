#!/usr/bin/env python3
"""Summarize Scanner/Heal ABBA and cache-cost profile artifacts quietly."""

from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

from scanner_abba import (
    LEGS,
    MIN_MEASURED_RELEASE_DURATION_SECONDS,
    SCENARIOS,
    validate_release_evidence_manifest,
)

MAX_JSON_BYTES = 1024 * 1024
CACHE_COST_PREFIX = "CACHE_COST "
PASS_STATES = {"pass"}
FAIL_STATES = {"fail", "failed"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_json(path: Path) -> dict[str, Any]:
    require(path.is_file(), f"missing JSON artifact: {path}")
    require(path.stat().st_size <= MAX_JSON_BYTES, f"oversized JSON artifact: {path}")
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    require(isinstance(value, dict), f"expected JSON object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        if hasattr(hashlib, "file_digest"):
            return hashlib.file_digest(stream, "sha256").hexdigest()
        hasher = hashlib.sha256()
        while chunk := stream.read(1024 * 1024):
            hasher.update(chunk)
        return hasher.hexdigest()


def number(value: Any, name: str) -> Decimal:
    require(type(value) in (float, int), f"invalid numeric field: {name}")
    return Decimal(str(value))


def maybe_number(value: Any, name: str) -> Decimal | None:
    if value is None:
        return None
    return number(value, name)


def pct(value: Decimal | None) -> str:
    if value is None:
        return "pending"
    return f"{float(value * Decimal('100')):.2f}%"


def ratio(value: Decimal | None) -> str:
    if value is None:
        return "pending"
    return f"{float(value):.3f}x"


def max_decimal(values: list[Decimal | None]) -> Decimal | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return max(present)


def require_metric_series(value: Any, name: str, minimum: Decimal | None = None,
                          maximum: Decimal | None = None) -> list[Decimal | None]:
    require(isinstance(value, list) and value, f"missing performance evidence field: {name}")
    parsed = [maybe_number(item, name) for item in value]
    for item in parsed:
        if item is None:
            continue
        if minimum is not None:
            require(item >= minimum, f"{name} below minimum")
        if maximum is not None:
            require(item <= maximum, f"{name} above maximum")
    return parsed


def require_measured_comparison_evidence(comparison: dict[str, Any], index: int) -> None:
    w10_w11 = comparison.get("w10_w11")
    require(isinstance(w10_w11, dict), f"comparison {index} missing W10/W11 evidence")
    pressure = require_metric_series(
        w10_w11.get("foreground_pressure_high_sample_ratios"),
        f"comparison {index} foreground_pressure_high_sample_ratios",
        Decimal("0"),
        Decimal("1"),
    )
    lock_wait = require_metric_series(
        w10_w11.get("heal_lock_wait_p99_ms"),
        f"comparison {index} heal_lock_wait_p99_ms",
        Decimal("0"),
    )
    attempt_cost = require_metric_series(
        w10_w11.get("attempt_cost_per_healed_object"),
        f"comparison {index} attempt_cost_per_healed_object",
        Decimal("0"),
    )
    require(len(pressure) == len(lock_wait) == len(attempt_cost),
            f"comparison {index} W10/W11 evidence length mismatch")
    candidate_attempt_cost = maybe_number(
        w10_w11.get("candidate_attempt_cost_per_healed_object"),
        f"comparison {index} candidate_attempt_cost_per_healed_object",
    )
    require(candidate_attempt_cost is None or candidate_attempt_cost >= 0,
            f"comparison {index} candidate attempt cost below minimum")


def require_complete_abba_matrix(manifest: dict[str, Any], report: dict[str, Any], comparisons: list[dict[str, Any]]) -> None:
    require(report.get("evidence") == manifest.get("evidence"), "manifest/report evidence mismatch")
    require(type(manifest.get("duration_seconds")) is int and
            manifest["duration_seconds"] >= MIN_MEASURED_RELEASE_DURATION_SECONDS,
            "measured ABBA duration_seconds requires at least two hours")
    rounds = manifest.get("rounds")
    require(type(rounds) is int and 3 <= rounds <= 10, "invalid manifest.rounds")
    expected_cells = len(SCENARIOS) * 2 * rounds * len(LEGS)
    require(
        report.get("cells") == expected_cells,
        f"ABBA matrix cell count mismatch: expected {expected_cells}, got {report.get('cells')}",
    )
    expected_keys = {
        (scenario, comparison, round_id)
        for scenario in SCENARIOS
        for comparison in ("build", "background")
        for round_id in range(1, rounds + 1)
    }
    observed_keys = []
    for index, comparison in enumerate(comparisons):
        key = (comparison.get("scenario"), comparison.get("comparison"), comparison.get("round"))
        require(key in expected_keys, f"comparison {index} is outside the ABBA matrix")
        require(comparison.get("status") in PASS_STATES, f"comparison {index} did not pass")
        observed_keys.append(key)
    observed_set = set(observed_keys)
    require(len(observed_keys) == len(observed_set), "duplicate ABBA matrix comparison")
    missing = sorted(expected_keys - observed_set)
    require(not missing, f"missing ABBA matrix comparison: {missing[0] if missing else ''}")


def summarize_abba(abba_dir: Path) -> dict[str, Any]:
    manifest_path = abba_dir / "manifest.json"
    report_path = abba_dir / "report.json"
    manifest = read_json(manifest_path)
    report = read_json(report_path)
    report_state = report.get("status")
    performance_state = report.get("performance")
    require(isinstance(report_state, str) and report_state, "report.status missing")
    require(isinstance(performance_state, str) and performance_state, "report.performance missing")
    comparisons = report.get("comparisons")
    if comparisons is None:
        require(report_state not in PASS_STATES, "passing report requires comparisons")
        comparisons = []
    require(isinstance(comparisons, list), "report.comparisons must be a list")

    counts = Counter()
    p99_regressions: list[Decimal] = []
    throughput_losses: list[Decimal] = []
    p1_rows = []
    p2_values: list[Decimal | None] = []
    for index, comparison in enumerate(comparisons):
        require(isinstance(comparison, dict), f"comparison {index} must be an object")
        state = comparison.get("status")
        require(isinstance(state, str) and state, f"comparison {index} missing status")
        counts[state] += 1
        p99_regressions.append(number(comparison.get("p99_regression"), f"comparison {index} p99_regression"))
        throughput_change = number(comparison.get("throughput_change"), f"comparison {index} throughput_change")
        throughput_losses.append(max(Decimal("0"), -throughput_change))
        p1 = comparison.get("p1")
        if isinstance(p1, dict):
            p1_rows.append({
                "scenario": comparison.get("scenario"),
                "comparison": comparison.get("comparison"),
                "round": comparison.get("round"),
                "required_reduction": float(number(p1.get("required_reduction"), "p1.required_reduction")),
                "observed_reduction": float(number(p1.get("observed_reduction"), "p1.observed_reduction")),
                "repeatability_drift": (
                    None if p1.get("repeatability_drift") is None
                    else float(number(p1.get("repeatability_drift"), "p1.repeatability_drift"))
                ),
            })
        p2 = comparison.get("p2_post_stop_work_multiples")
        if isinstance(p2, list):
            p2_values.extend(maybe_number(value, "p2_post_stop_work_multiple") for value in p2)

    measured = report.get("evidence") == "measured"
    passed = report_state in PASS_STATES and performance_state in PASS_STATES and measured
    if passed:
        require_complete_abba_matrix(manifest, report, comparisons)
        validate_release_evidence_manifest({**manifest, "evidence": "measured"})
        for index, comparison in enumerate(comparisons):
            require_measured_comparison_evidence(comparison, index)
    gate_state = "pass" if passed else "fail"
    if report_state == "synthetic_validated":
        reason = "synthetic evidence validates the harness only; measured performance remains pending"
    elif report_state in FAIL_STATES and isinstance(report.get("error"), str) and report["error"]:
        reason = f"ABBA report status is {report_state}: {report['error']}"
    elif report_state not in PASS_STATES:
        reason = f"ABBA report status is {report_state}"
    elif performance_state not in PASS_STATES:
        reason = f"performance status is {performance_state}"
    elif not measured:
        reason = "measured evidence is required for a performance conclusion"
    else:
        reason = "measured ABBA report passed"

    fixed = manifest.get("fixed", {})
    require(isinstance(fixed, dict), "manifest.fixed must be an object")
    return {
        "gate_state": gate_state,
        "reason": reason,
        "status": report_state,
        "performance": performance_state,
        "evidence": report.get("evidence"),
        "cells": report.get("cells", 0),
        "completed_cells": report.get("completed_cells"),
        "error": report.get("error"),
        "comparisons_total": len(comparisons),
        "comparison_status_counts": dict(sorted(counts.items())),
        "worst_p99_regression": None if not p99_regressions else float(max(p99_regressions)),
        "worst_throughput_loss": None if not throughput_losses else float(max(throughput_losses)),
        "p2_worst_post_stop_work_multiple": None if max_decimal(p2_values) is None else float(max_decimal(p2_values)),
        "p1_reductions": p1_rows,
        "provenance": {
            "abba_dir": str(abba_dir.resolve()),
            "manifest_sha256": digest(manifest_path),
            "report_sha256": digest(report_path),
            "baseline_revision": manifest.get("baseline", {}).get("revision"),
            "baseline_sha256": manifest.get("baseline", {}).get("sha256"),
            "candidate_revision": manifest.get("candidate", {}).get("revision"),
            "candidate_sha256": manifest.get("candidate", {}).get("sha256"),
            "adapter_sha256": manifest.get("adapter_sha256"),
            "collector_sha256": manifest.get("collector_sha256"),
            "config_sha256": fixed.get("config_sha256"),
            "dataset_sha256": fixed.get("dataset_sha256"),
            "release_flags": fixed.get("release_flags"),
            "durability": fixed.get("durability"),
            "topology": fixed.get("topology"),
            "offered_load_ops": fixed.get("offered_load_ops"),
            "release_evidence": manifest.get("release_evidence"),
        },
    }


def cache_cost_records(path: Path) -> list[dict[str, Any]]:
    require(path.is_file(), f"missing cache-cost log: {path}")
    records = []
    with path.open(encoding="utf-8", errors="replace") as stream:
        for line_no, line in enumerate(stream, start=1):
            if CACHE_COST_PREFIX not in line:
                continue
            payload = line.split(CACHE_COST_PREFIX, 1)[1].strip()
            value = json.loads(payload)
            require(isinstance(value, dict), f"cache-cost line {line_no} is not a JSON object")
            require(value.get("schema") == 1, f"cache-cost line {line_no} has unsupported schema")
            records.append(value)
    require(records, f"no {CACHE_COST_PREFIX.strip()} records found in {path}")
    return records


def summarize_cache_cost(path: Path) -> dict[str, Any]:
    records = cache_cost_records(path)
    scenarios = Counter()
    max_wire = Decimal("0")
    max_save_amp = Decimal("0")
    max_clone_ns = Decimal("0")
    max_encode_ns = Decimal("0")
    max_save_ns = Decimal("0")
    build_sources = set()
    for index, record in enumerate(records):
        scenarios[str(record.get("scenario"))] += 1
        wire = number(record.get("cache_wire_bytes"), f"cache_cost {index} cache_wire_bytes")
        save_body = number(record.get("save_body_bytes_per_sample"), f"cache_cost {index} save_body_bytes_per_sample")
        require(wire > 0, f"cache_cost {index} has zero wire bytes")
        max_wire = max(max_wire, wire)
        max_save_amp = max(max_save_amp, save_body / wire)
        for field, target in (("clone", "max_clone_ns"), ("encode", "max_encode_ns"), ("save_inclusive", "max_save_ns")):
            quantiles = record.get(field)
            require(isinstance(quantiles, dict), f"cache_cost {index} missing {field} quantiles")
            value = number(quantiles.get("max_ns"), f"cache_cost {index} {field}.max_ns")
            if target == "max_clone_ns":
                max_clone_ns = max(max_clone_ns, value)
            elif target == "max_encode_ns":
                max_encode_ns = max(max_encode_ns, value)
            else:
                max_save_ns = max(max_save_ns, value)
        build = record.get("build", {})
        require(isinstance(build, dict), f"cache_cost {index} build must be an object")
        build_sources.add((build.get("source_revision"), build.get("source_tree"), build.get("test_opt_level_override")))
    return {
        "records": len(records),
        "scenario_counts": dict(sorted(scenarios.items())),
        "max_cache_wire_bytes": int(max_wire),
        "max_save_body_amplification": float(max_save_amp),
        "max_clone_ns": int(max_clone_ns),
        "max_encode_ns": int(max_encode_ns),
        "max_save_inclusive_ns": int(max_save_ns),
        "build_sources": [
            {"source_revision": revision, "source_tree": tree, "test_opt_level_override": opt}
            for revision, tree, opt in sorted(build_sources, key=lambda item: tuple("" if part is None else str(part) for part in item))
        ],
        "provenance": {
            "cache_cost_log": str(path.resolve()),
            "cache_cost_log_sha256": digest(path),
        },
    }


def markdown(summary: dict[str, Any]) -> str:
    abba = summary["abba"]
    p2 = None if abba["p2_worst_post_stop_work_multiple"] is None else Decimal(str(abba["p2_worst_post_stop_work_multiple"]))
    p99 = None if abba["worst_p99_regression"] is None else Decimal(str(abba["worst_p99_regression"]))
    throughput = None if abba["worst_throughput_loss"] is None else Decimal(str(abba["worst_throughput_loss"]))
    lines = [
        f"# Scanner/Heal Performance Summary",
        "",
        f"- verdict: {summary['verdict']}",
        f"- reason: {summary['reason']}",
        f"- abba: status={abba['status']} performance={abba['performance']} evidence={abba['evidence']} cells={abba['cells']} comparisons={abba['comparisons_total']}",
        f"- worst_p99_regression: {pct(p99)}",
        f"- worst_throughput_loss: {pct(throughput)}",
        f"- p2_worst_post_stop_work_multiple: {ratio(p2)}",
    ]
    if abba.get("completed_cells") is not None:
        lines.append(f"- completed_cells: {abba['completed_cells']}")
    if abba.get("error"):
        lines.append(f"- error: {abba['error']}")
    if summary.get("cache_cost") is not None:
        cache = summary["cache_cost"]
        lines.extend([
            f"- cache_cost_records: {cache['records']}",
            f"- max_cache_wire_bytes: {cache['max_cache_wire_bytes']}",
            f"- max_save_body_amplification: {cache['max_save_body_amplification']:.3f}x",
            f"- max_clone_ns: {cache['max_clone_ns']}",
            f"- max_encode_ns: {cache['max_encode_ns']}",
            f"- max_save_inclusive_ns: {cache['max_save_inclusive_ns']}",
        ])
    lines.extend([
        "",
        "## Provenance",
        "",
    ])
    for key, value in abba["provenance"].items():
        lines.append(f"- {key}: {value}")
    if summary.get("cache_cost") is not None:
        for key, value in summary["cache_cost"]["provenance"].items():
            lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def build_summary(args: argparse.Namespace) -> dict[str, Any]:
    abba = summarize_abba(args.abba_dir)
    cache = summarize_cache_cost(args.cache_cost_log) if args.cache_cost_log else None
    if args.require_cache_cost and cache is None:
        raise ValueError("cache-cost profile log is required")
    verdict = "PASS" if abba["gate_state"] == "pass" else "FAIL"
    reason = abba["reason"]
    return {"schema": 1, "verdict": verdict, "reason": reason, "abba": abba, "cache_cost": cache}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--abba-dir", type=Path, required=True, help="Directory containing manifest.json and report.json")
    parser.add_argument("--cache-cost-log", type=Path, help="Rust test output containing CACHE_COST JSON lines")
    parser.add_argument("--require-cache-cost", action="store_true", help="Fail when --cache-cost-log is missing")
    parser.add_argument("--json-out", type=Path, help="Write the normalized summary JSON artifact")
    parser.add_argument("--markdown-out", type=Path, help="Write a compact Markdown summary artifact")
    args = parser.parse_args()
    try:
        summary = build_summary(args)
        if args.json_out:
            write_json(args.json_out, summary)
        if args.markdown_out:
            args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
            args.markdown_out.write_text(markdown(summary), encoding="utf-8")
        abba = summary["abba"]
        print(
            f"{summary['verdict']} scanner_heal_perf "
            f"status={abba['status']} performance={abba['performance']} evidence={abba['evidence']} "
            f"comparisons={abba['comparisons_total']} reason={summary['reason']}"
        )
        return 0 if summary["verdict"] == "PASS" else 1
    except (ValueError, OSError, json.JSONDecodeError) as error:
        print(f"FAIL scanner_heal_perf error={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
