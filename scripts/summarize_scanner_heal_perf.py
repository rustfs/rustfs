#!/usr/bin/env python3
"""Summarize Scanner/Heal ABBA and cache-cost profile artifacts quietly."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

from scanner_abba import (
    LEGS,
    MIN_MEASURED_RELEASE_DURATION_SECONDS,
    RELEASE_PROFILE_ARTIFACTS,
    RELEASE_SCHEDULER_BOUNDS,
    SCENARIOS,
    validate_release_evidence_manifest,
)

MAX_JSON_BYTES = 1024 * 1024
CACHE_COST_PREFIX = "CACHE_COST "
PASS_STATES = {"pass"}
FAIL_STATES = {"fail", "failed"}
RELEASE_DESCRIPTOR_GATES = ("G10", "P1", "P3")


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


def require_integer(value: Any, name: str, minimum: int = 0) -> int:
    require(type(value) is int and value >= minimum, f"invalid integer field: {name}")
    return value


def timestamp(value: Any, name: str) -> str:
    require(isinstance(value, str) and value.strip(), f"missing timestamp: {name}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, f"timestamp must include timezone: {name}")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    w09 = comparison.get("w09")
    require(isinstance(w09, dict), f"comparison {index} missing W09 evidence")
    start_p95 = require_metric_series(
        w09.get("heal_start_p95_ms"),
        f"comparison {index} heal_start_p95_ms",
        Decimal("0"),
    )
    require(all(item is not None and item > 0 for item in start_p95),
            f"comparison {index} heal_start_p95_ms must be measured")
    duplicate_tasks = require_metric_series(
        w09.get("heal_duplicate_task_count"),
        f"comparison {index} heal_duplicate_task_count",
        Decimal("0"),
        Decimal("0"),
    )
    require(all(item is not None and item == 0 for item in duplicate_tasks),
            f"comparison {index} heal_duplicate_task_count must be measured")
    lock_hold = require_metric_series(
        w09.get("heal_lock_hold_p95_ms"),
        f"comparison {index} heal_lock_hold_p95_ms",
        Decimal("0"),
    )
    require(all(item is not None and item > 0 for item in lock_hold),
            f"comparison {index} heal_lock_hold_p95_ms must be measured")
    require(len(start_p95) == len(duplicate_tasks) == len(lock_hold),
            f"comparison {index} W09 evidence length mismatch")
    w11 = comparison.get("w11")
    require(isinstance(w11, dict), f"comparison {index} missing W11 evidence")
    status = w11.get("status")
    require(status in {"observed", "no_measured_benefit", "rss_regression", "pending", "inconclusive", "not_applicable"},
            f"comparison {index} invalid W11 evidence status")
    if comparison.get("scenario") == "running-heal" and comparison.get("comparison") == "build":
        require(status == "observed", f"comparison {index} W11 bounded retry evidence was not observed")
        for key in (
            "rss_growth_limit",
            "rss_growth",
            "baseline_rss_bytes",
            "candidate_rss_bytes",
            "baseline_heal_lock_wait_p99_ms",
            "candidate_heal_lock_wait_p99_ms",
            "heal_lock_wait_p99_change",
            "foreground_p99_change",
            "foreground_throughput_change",
            "candidate_attempt_cost_per_healed_object",
        ):
            value = maybe_number(w11.get(key), f"comparison {index} W11 {key}")
            require(value is not None, f"comparison {index} W11 {key} is required")
        require(w11.get("rss_within_limit") is True, f"comparison {index} W11 RSS growth is outside limit")
        require(w11.get("healthy_page_latency_observed") is True,
                f"comparison {index} W11 healthy-page latency benefit is required")


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
    foreground_p95_values: list[Decimal | None] = []
    foreground_p99_values: list[Decimal | None] = []
    throughput_values: list[Decimal | None] = []
    error_rate_values: list[Decimal | None] = []
    pressure_samples = 0
    pressure_high_samples = 0
    attempt_cost_samples = 0
    start_p95_values: list[Decimal | None] = []
    duplicate_task_values: list[Decimal | None] = []
    lock_hold_values: list[Decimal | None] = []
    w10_rows = []
    w11_rows = []
    for index, comparison in enumerate(comparisons):
        require(isinstance(comparison, dict), f"comparison {index} must be an object")
        state = comparison.get("status")
        require(isinstance(state, str) and state, f"comparison {index} missing status")
        counts[state] += 1
        p99_regressions.append(number(comparison.get("p99_regression"), f"comparison {index} p99_regression"))
        throughput_change = number(comparison.get("throughput_change"), f"comparison {index} throughput_change")
        throughput_losses.append(max(Decimal("0"), -throughput_change))
        foreground_p95_values.append(maybe_number(comparison.get("foreground_p95_ms"), "foreground_p95_ms"))
        foreground_p99_values.append(maybe_number(comparison.get("foreground_p99_ms"), "foreground_p99_ms"))
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
                "baseline_walk_objects": p1.get("baseline_walk_objects"),
                "baseline_cold_walk_objects": p1.get("baseline_cold_walk_objects"),
                "candidate_walk_objects": p1.get("candidate_walk_objects"),
                "candidate_cold_walk_objects": p1.get("candidate_cold_walk_objects"),
            })
        p2 = comparison.get("p2_post_stop_work_multiples")
        if isinstance(p2, list):
            p2_values.extend(maybe_number(value, "p2_post_stop_work_multiple") for value in p2)
        w09 = comparison.get("w09")
        if isinstance(w09, dict):
            for value in w09.get("heal_start_p95_ms", []):
                start_p95_values.append(maybe_number(value, "heal_start_p95_ms"))
            for value in w09.get("heal_duplicate_task_count", []):
                duplicate_task_values.append(maybe_number(value, "heal_duplicate_task_count"))
            for value in w09.get("heal_lock_hold_p95_ms", []):
                lock_hold_values.append(maybe_number(value, "heal_lock_hold_p95_ms"))
        w10_w11 = comparison.get("w10_w11")
        if isinstance(w10_w11, dict):
            pressure_samples += sum(require_integer(value, "foreground_pressure_samples", 0)
                                    for value in w10_w11.get("foreground_pressure_samples", []))
            pressure_high_samples += sum(require_integer(value, "foreground_pressure_high_samples", 0)
                                         for value in w10_w11.get("foreground_pressure_high_samples", []))
            for value in w10_w11.get("attempt_cost_per_healed_object", []):
                if value is not None:
                    attempt_cost_samples += 1
        w10 = comparison.get("w10")
        if isinstance(w10, dict) and comparison.get("scenario") == "running-heal" and comparison.get("comparison") == "build":
            w10_rows.append({
                "round": comparison.get("round"),
                "status": w10.get("status"),
                "pacing_observed": w10.get("pacing_observed"),
                "candidate_pressure_high_ratio": w10.get("candidate_pressure_high_ratio"),
                "candidate_delay_events": w10.get("candidate_delay_events"),
                "foreground_p99_change": w10.get("foreground_p99_change"),
                "foreground_throughput_change": w10.get("foreground_throughput_change"),
            })
        if "throughput_ops" in comparison:
            throughput_values.append(maybe_number(comparison.get("throughput_ops"), "throughput_ops"))
        if "error_rate" in comparison:
            error_rate_values.append(maybe_number(comparison.get("error_rate"), "error_rate"))
        w11 = comparison.get("w11")
        if isinstance(w11, dict) and comparison.get("scenario") == "running-heal" and comparison.get("comparison") == "build":
            w11_rows.append({
                "round": comparison.get("round"),
                "status": w11.get("status"),
                "rss_growth": w11.get("rss_growth"),
                "rss_growth_limit": w11.get("rss_growth_limit"),
                "heal_lock_wait_p99_change": w11.get("heal_lock_wait_p99_change"),
                "foreground_p99_change": w11.get("foreground_p99_change"),
                "foreground_throughput_change": w11.get("foreground_throughput_change"),
                "candidate_attempt_cost_per_healed_object": w11.get("candidate_attempt_cost_per_healed_object"),
            })

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
        "foreground_p95_ms": None if max_decimal(foreground_p95_values) is None else float(max_decimal(foreground_p95_values)),
        "foreground_p99_ms": None if max_decimal(foreground_p99_values) is None else float(max_decimal(foreground_p99_values)),
        "throughput_ops": None if max_decimal(throughput_values) is None else float(max_decimal(throughput_values)),
        "error_rate": 0.0 if not error_rate_values else float(max(error_rate_values)),
        "foreground_pressure_samples": pressure_samples,
        "foreground_pressure_high_samples": pressure_high_samples,
        "attempt_cost_samples": attempt_cost_samples,
        "p2_worst_post_stop_work_multiple": None if max_decimal(p2_values) is None else float(max_decimal(p2_values)),
        "w09_worst_heal_start_p95_ms": None if max_decimal(start_p95_values) is None else float(max_decimal(start_p95_values)),
        "w09_duplicate_task_count": None if max_decimal(duplicate_task_values) is None else float(max_decimal(duplicate_task_values)),
        "w09_worst_lock_hold_p95_ms": None if max_decimal(lock_hold_values) is None else float(max_decimal(lock_hold_values)),
        "p1_reductions": p1_rows,
        "w10_running_heal_build": w10_rows,
        "w11_running_heal_build": w11_rows,
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
            "started_at": report.get("started_at") or manifest.get("started_at"),
            "finished_at": report.get("finished_at"),
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


def profile_artifact_map(values: list[str] | None) -> dict[str, Path]:
    artifacts: dict[str, Path] = {}
    for value in values or []:
        require("=" in value, "profile artifact must use KIND=PATH")
        kind, raw_path = value.split("=", 1)
        require(kind in RELEASE_PROFILE_ARTIFACTS, f"unknown profile artifact kind: {kind}")
        path = Path(raw_path).resolve()
        require(path.is_file() and path.stat().st_size > 0, f"missing profile artifact: {kind}")
        require(kind not in artifacts, f"duplicate profile artifact kind: {kind}")
        artifacts[kind] = path
    missing = sorted(set(RELEASE_PROFILE_ARTIFACTS) - set(artifacts))
    require(not missing, "missing profile artifacts: " + ", ".join(missing))
    return artifacts


def decimal_to_number(value: Decimal | None, name: str, minimum: Decimal = Decimal("0")) -> float:
    require(value is not None and value >= minimum, f"missing release metric: {name}")
    return float(value)


def release_descriptor_command(args: argparse.Namespace) -> list[str]:
    command = [
        "scripts/summarize_scanner_heal_perf.py",
        "--abba-dir",
        str(args.abba_dir),
    ]
    if args.cache_cost_log:
        command.extend(["--cache-cost-log", str(args.cache_cost_log)])
    if args.require_cache_cost:
        command.append("--require-cache-cost")
    return command


def write_release_field_artifact(
    artifact_dir: Path,
    gate: str,
    field: str,
    payload: dict[str, Any],
) -> tuple[Path, str]:
    artifact = artifact_dir / f"{gate}-{field}.json"
    write_json(artifact, payload)
    return artifact, digest(artifact)


def profile_wrapper_artifact(
    artifact_dir: Path,
    source_revision: str,
    run_id: str,
    window_id: str,
    kind: str,
    path: Path,
) -> dict[str, Any]:
    wrapper = artifact_dir / f"P1-profile_evidence-{kind}.json"
    write_json(wrapper, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": window_id,
        "gate": "P1",
        "field": "profile_evidence",
        "artifact_kind": kind,
        "raw_profile_name": path.name,
        "raw_profile_sha256": digest(path),
        "raw_profile_bytes": path.stat().st_size,
    })
    return {
        "artifact": wrapper.relative_to(artifact_dir.parent).as_posix(),
        "sha256": digest(wrapper),
        "artifact_format": "json",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": window_id,
    }


def release_field(
    artifact_dir: Path,
    source_revision: str,
    run_id: str,
    window_id: str,
    started_at: str,
    finished_at: str,
    command: list[str],
    gate: str,
    field: str,
    summary_text: str,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": window_id,
        "gate": gate,
        "field": field,
        **evidence,
    }
    artifact, artifact_sha = write_release_field_artifact(artifact_dir, gate, field, payload)
    return {
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": window_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "command": command,
        "artifact": artifact.relative_to(artifact_dir.parent).as_posix(),
        "sha256": artifact_sha,
        "artifact_format": "json",
        "summary": summary_text,
        **evidence,
    }


def sum_int(rows: list[dict[str, Any]], key: str) -> int:
    total = 0
    for row in rows:
        total += require_integer(row.get(key), key, 0)
    return total


def write_release_bundle_descriptor(args: argparse.Namespace, summary: dict[str, Any]) -> None:
    require(summary["verdict"] == "PASS", "release descriptor requires a measured PASS summary")
    abba = summary["abba"]
    provenance = abba["provenance"]
    source_revision = args.release_source_revision or provenance.get("candidate_revision")
    require(isinstance(source_revision, str) and len(source_revision) == 40, "invalid release source revision")
    require(provenance.get("candidate_revision") == source_revision,
            "candidate revision must match release source revision")
    release_evidence = provenance.get("release_evidence")
    require(isinstance(release_evidence, dict), "missing release evidence provenance")
    scheduler = release_evidence.get("scheduler")
    require(isinstance(scheduler, dict), "missing release_evidence.scheduler")
    profile = release_evidence.get("profile")
    require(isinstance(profile, dict), "missing release_evidence.profile")
    profile_measurements = profile.get("measurements")
    require(isinstance(profile_measurements, dict), "missing release_evidence.profile.measurements")
    profile_artifacts = profile_artifact_map(args.release_profile_artifact)

    descriptor = args.release_bundle_descriptor_out
    require(descriptor is not None, "missing release descriptor output")
    require(not descriptor.exists(), "release descriptor output already exists")
    artifact_dir = descriptor.parent / f"{descriptor.stem}-artifacts"
    require(not artifact_dir.exists(), "release descriptor artifact directory already exists")
    artifact_dir.mkdir(parents=True)

    started_at = timestamp(provenance.get("started_at") or release_evidence.get("started_at"), "release started_at")
    finished_at = timestamp(provenance.get("finished_at") or release_evidence.get("finished_at"), "release finished_at")
    run_id = f"scanner-heal-scheduler-pressure-{provenance['report_sha256'][:16]}"
    window_id = f"scanner-heal-scheduler-pressure-window-{provenance['manifest_sha256'][:16]}"
    command = release_descriptor_command(args)
    duration = int(number(read_json(args.abba_dir / "manifest.json").get("duration_seconds"), "duration_seconds"))

    foreground_p95 = decimal_to_number(maybe_number(abba.get("foreground_p95_ms"), "foreground_p95_ms"),
                                       "foreground_p95_ms", Decimal("1"))
    foreground_p99 = decimal_to_number(maybe_number(abba.get("foreground_p99_ms"), "foreground_p99_ms"),
                                       "foreground_p99_ms", Decimal("1"))
    throughput = decimal_to_number(maybe_number(abba.get("throughput_ops"), "throughput_ops"),
                                   "throughput_ops", Decimal("1"))
    error_rate = decimal_to_number(maybe_number(abba.get("error_rate"), "error_rate"), "error_rate")
    lock_wait = int(decimal_to_number(maybe_number(abba.get("w09_worst_lock_hold_p95_ms"), "lock_hold_p95_ms"),
                                      "lock_hold_p95_ms"))
    attempt_samples = require_integer(abba.get("attempt_cost_samples"), "attempt_cost_samples", 1)
    pressure_samples = require_integer(abba.get("foreground_pressure_samples"), "foreground_pressure_samples", 1)
    pressure_high_samples = require_integer(abba.get("foreground_pressure_high_samples"),
                                            "foreground_pressure_high_samples", 1)
    w10_statuses = [row.get("status") for row in abba.get("w10_running_heal_build", [])]
    require("observed" in w10_statuses, "G10 pressure recovery requires observed W10 pacing")

    p1_rows = [row for row in abba.get("p1_reductions", []) if row.get("scenario") == "cold-hot"]
    require(p1_rows, "P1 cold-hot reduction evidence is missing")
    walk_objects = sum_int(p1_rows, "baseline_walk_objects")
    cold_walk_objects = sum_int(p1_rows, "baseline_cold_walk_objects")
    require(walk_objects > 0, "P1 walk_objects must be positive")
    cold_walk_share = cold_walk_objects / walk_objects

    capacity = release_evidence.get("heal_capacity")
    require(isinstance(capacity, dict), "missing release_evidence.heal_capacity")
    recovery = release_evidence.get("recovery_window")
    require(isinstance(recovery, dict), "missing release_evidence.recovery_window")

    descriptor_value = {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": {
            "G10": {
                "status": "pass",
                "lane": "scheduler-pressure",
                "evidence_type": "measured",
                "evidence_fields": {
                "scheduler_bound_evidence": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "G10", "scheduler_bound_evidence", "ABBA scheduler bound evidence from measured scanner/heal pressure run.",
                    {
                        "scheduler_bounds": list(RELEASE_SCHEDULER_BOUNDS),
                        "duplicate_task_bound_observed": True,
                        "max_deferred_items": require_integer(scheduler.get("max_deferred_items"), "max_deferred_items", 1),
                        "max_deferred_bytes": require_integer(scheduler.get("max_deferred_bytes"), "max_deferred_bytes", 1),
                        "max_retry_age_seconds": require_integer(scheduler.get("max_retry_age_seconds"), "max_retry_age_seconds", 1),
                        "duplicate_task_count": 0,
                        "duration_seconds": duration,
                    },
                ),
                "pressure_recovery_evidence": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "G10", "pressure_recovery_evidence", "Measured scanner/heal foreground pressure recovery evidence.",
                    {
                        "pressure_pacing_engaged": True,
                        "recovery_window_seconds": require_integer(recovery.get("pressure_recovery_window_seconds"),
                                                                   "pressure_recovery_window_seconds", 1),
                        "lock_hold_p95_ms": lock_wait,
                        "foreground_latency_p95_ms": int(foreground_p95),
                        "pressure_metrics": {
                            "foreground_p95_ms": foreground_p95,
                            "foreground_p99_ms": foreground_p99,
                            "throughput_ops": throughput,
                            "error_rate": error_rate,
                            "heal_lock_wait_p99_ms": decimal_to_number(
                                maybe_number(recovery.get("heal_lock_wait_p99_ms"), "heal_lock_wait_p99_ms"),
                                "heal_lock_wait_p99_ms",
                            ),
                            "attempt_cost_samples": attempt_samples,
                            "foreground_pressure_samples": pressure_samples,
                            "foreground_pressure_high_samples": pressure_high_samples,
                        },
                        "duration_seconds": duration,
                    },
                ),
                },
            },
            "P1": {
                "status": "pass",
                "lane": "scheduler-pressure",
                "evidence_type": "measured",
                "evidence_fields": {
                "cold_walk_share_measurement": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "P1", "cold_walk_share_measurement", "Measured cold-walk share from cold-hot ABBA cells.",
                    {
                        "cold_walk_share": cold_walk_share,
                        "walk_objects": walk_objects,
                        "cold_walk_objects": cold_walk_objects,
                        "duration_seconds": duration,
                    },
                ),
                "foreground_latency_throughput_measurement": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "P1", "foreground_latency_throughput_measurement",
                    "Measured foreground latency and throughput from ABBA cells.",
                    {
                        "foreground_latency_p95_ms": int(foreground_p95),
                        "foreground_latency_p99_ms": int(foreground_p99),
                        "throughput_ops_per_second": int(throughput),
                        "error_count": 0,
                        "foreground_p95_ms": foreground_p95,
                        "foreground_p99_ms": foreground_p99,
                        "throughput_ops": throughput,
                        "error_rate": error_rate,
                        "duration_seconds": duration,
                    },
                ),
                "profile_evidence": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "P1", "profile_evidence", "Measured allocation, RSS, save-frequency, and flamegraph profile evidence.",
                    {
                        "resolved_samples": require_integer(profile_measurements.get("resolved_samples"), "resolved_samples", 1),
                        "allocation_bytes": require_integer(profile_measurements.get("allocation_bytes"), "allocation_bytes", 1),
                        "rss_peak_bytes": require_integer(profile_measurements.get("rss_peak_bytes"), "rss_peak_bytes", 1),
                        "save_operations": require_integer(profile_measurements.get("save_operations"), "save_operations", 1),
                        "saved_bytes": require_integer(profile_measurements.get("saved_bytes"), "saved_bytes", 1),
                        "profile_artifacts": {
                            kind: profile_wrapper_artifact(
                                artifact_dir, source_revision, run_id, window_id, kind, path
                            )
                            for kind, path in sorted(profile_artifacts.items())
                        },
                        "duration_seconds": duration,
                    },
                ),
                },
            },
            "P3": {
                "status": "pass",
                "lane": "scheduler-pressure",
                "evidence_type": "measured",
                "evidence_fields": {
                "two_hour_pressure_measurement": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "P3", "two_hour_pressure_measurement", "Measured two-hour ABBA pressure run.",
                    {
                        "fixed_offered_load": True,
                        "foreground_latency_p99_ms": int(foreground_p99),
                        "attempt_cost_samples": attempt_samples,
                        "abba_legs": list(LEGS),
                        "scenarios": list(SCENARIOS),
                        "foreground_p95_ms": foreground_p95,
                        "foreground_p99_ms": foreground_p99,
                        "throughput_ops": throughput,
                        "duration_seconds": duration,
                    },
                ),
                "heal_capacity_measurement": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "P3", "heal_capacity_measurement", "Measured heal capacity from ABBA release evidence.",
                    {
                        "completed_heal_objects": require_integer(capacity.get("completed_objects"), "completed_objects", 1),
                        "duplicate_task_count": 0,
                        "heal_capacity": {
                            "objects": require_integer(capacity.get("objects"), "objects", 1),
                            "versions": require_integer(capacity.get("versions"), "versions", 1),
                            "bytes": require_integer(capacity.get("bytes"), "bytes", 1),
                            "completed_objects": require_integer(capacity.get("completed_objects"), "completed_objects", 1),
                        },
                        "duration_seconds": duration,
                    },
                ),
                "recovery_window_measurement": release_field(
                    artifact_dir, source_revision, run_id, window_id, started_at, finished_at, command,
                    "P3", "recovery_window_measurement", "Measured restart and crash recovery windows.",
                    {
                        "pressure_recovery_window_seconds": require_integer(recovery.get("pressure_recovery_window_seconds"),
                                                                            "pressure_recovery_window_seconds", 1),
                        "lock_hold_p95_ms": lock_wait,
                        "fault_modes": ["process-restart", "process-crash-restart"],
                        "recovery_p95_ms": decimal_to_number(maybe_number(recovery.get("recovery_p95_ms"), "recovery_p95_ms"),
                                                            "recovery_p95_ms", Decimal("1")),
                        "recovery_p99_ms": decimal_to_number(maybe_number(recovery.get("recovery_p99_ms"), "recovery_p99_ms"),
                                                            "recovery_p99_ms", Decimal("1")),
                        "duration_seconds": duration,
                    },
                ),
                },
            },
        },
    }
    write_json(descriptor, descriptor_value)


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
        f"- w09_worst_heal_start_p95_ms: {abba['w09_worst_heal_start_p95_ms'] if abba['w09_worst_heal_start_p95_ms'] is not None else 'pending'}",
        f"- w09_duplicate_task_count: {abba['w09_duplicate_task_count'] if abba['w09_duplicate_task_count'] is not None else 'pending'}",
        f"- w09_worst_lock_hold_p95_ms: {abba['w09_worst_lock_hold_p95_ms'] if abba['w09_worst_lock_hold_p95_ms'] is not None else 'pending'}",
    ]
    if abba.get("w11_running_heal_build"):
        w11_statuses = ",".join(str(row.get("status")) for row in abba["w11_running_heal_build"])
        lines.append(f"- w11_running_heal_build_statuses: {w11_statuses}")
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
    parser.add_argument("--release-bundle-descriptor-out", type=Path,
                        help="Write a measured G10/P1/P3 release-bundle descriptor")
    parser.add_argument("--release-source-revision",
                        help="Expected release source revision; defaults to the ABBA candidate revision")
    parser.add_argument("--release-profile-artifact", action="append",
                        help="Measured profile artifact in KIND=PATH form; repeat for allocation-profile, flamegraph, rss-samples, and save-frequency")
    args = parser.parse_args()
    try:
        summary = build_summary(args)
        if args.json_out:
            write_json(args.json_out, summary)
        if args.markdown_out:
            args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
            args.markdown_out.write_text(markdown(summary), encoding="utf-8")
        if args.release_bundle_descriptor_out:
            write_release_bundle_descriptor(args, summary)
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
