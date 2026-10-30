#!/usr/bin/env python3
"""Summarize Scanner/Heal ABBA and cache-cost profile artifacts quietly."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
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
    SCENARIOS,
    measured_provenance_verified,
    validate_release_evidence_manifest,
)

MAX_JSON_BYTES = 1024 * 1024
CACHE_COST_PREFIX = "CACHE_COST "
PASS_STATES = {"pass"}
FAIL_STATES = {"fail", "failed"}
REPORT_STATES = PASS_STATES | FAIL_STATES | {"inconclusive", "incomplete", "pending", "synthetic_validated", "unknown"}
PERFORMANCE_STATES = PASS_STATES | FAIL_STATES | {"inconclusive", "pending", "unknown"}
EVIDENCE_TYPES = {"measured", "synthetic", "diagnostic"}
OBSERVATION_STATES = {"observed", "no_measured_benefit", "rss_regression", "pending", "inconclusive", "not_applicable", "unknown"}


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


def read_json_snapshot(path: Path) -> tuple[dict[str, Any], str]:
    with path.open("rb") as stream:
        data = stream.read(MAX_JSON_BYTES + 1)
    require(len(data) <= MAX_JSON_BYTES, "oversized JSON artifact")
    value = json.loads(data)
    require(isinstance(value, dict), "expected JSON object")
    return value, hashlib.sha256(data).hexdigest()


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
    result = Decimal(str(value))
    require(result.is_finite(), f"invalid numeric field: {name}")
    return result


def maybe_number(value: Any, name: str) -> Decimal | None:
    if value is None:
        return None
    return number(value, name)


def safe_observed_number(value: Any) -> int | float | None:
    if type(value) not in (int, float) or not Decimal(str(value)).is_finite():
        return None
    return value


def safe_count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def safe_observation_state(value: Any) -> str:
    return value if isinstance(value, str) and value in OBSERVATION_STATES else "unknown"


def safe_hex(value: Any, length: int) -> str | None:
    return value if isinstance(value, str) and len(value) == length and all(c in "0123456789abcdef" for c in value) else None


def safe_timestamp(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value if parsed.tzinfo is not None else None


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
    percentiles = comparison.get("foreground_percentiles_ms_by_leg")
    require(isinstance(percentiles, dict) and set(percentiles) == set(LEGS),
            f"comparison {index} missing foreground percentiles by leg")
    for leg in LEGS:
        row = percentiles[leg]
        require(isinstance(row, dict), f"comparison {index} invalid {leg} foreground percentiles")
        p95 = number(row.get("p95"), f"comparison {index} {leg} foreground percentiles p95")
        p99 = number(row.get("p99"), f"comparison {index} {leg} foreground percentiles p99")
        require(p95.is_finite() and p99.is_finite() and p95 >= 0 and p99 > 0 and p95 <= p99,
                f"comparison {index} invalid {leg} foreground percentiles")
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


def has_complete_abba_matrix(manifest: dict[str, Any], report: dict[str, Any], comparisons: list[dict[str, Any]]) -> bool:
    rounds = manifest.get("rounds")
    if type(rounds) is not int or not 3 <= rounds <= 10:
        return False
    expected = {(scenario, comparison, round_id) for scenario in SCENARIOS
                for comparison in ("build", "background") for round_id in range(1, rounds + 1)}
    observed = []
    for row in comparisons:
        if not isinstance(row, dict) or not isinstance(row.get("scenario"), str) or not isinstance(row.get("comparison"), str) \
                or type(row.get("round")) is not int:
            return False
        observed.append((row["scenario"], row["comparison"], row["round"]))
    return (len(observed) == len(expected) and set(observed) == expected
            and report.get("cells") == len(expected) * len(LEGS))


def summarize_abba(abba_dir: Path) -> dict[str, Any]:
    manifest_path = abba_dir / "manifest.json"
    report_path = abba_dir / "report.json"
    manifest, manifest_sha256 = read_json_snapshot(manifest_path)
    report, report_sha256 = read_json_snapshot(report_path)
    report_state = report.get("status")
    performance_state = report.get("performance")
    require(isinstance(report_state, str) and report_state, "report.status missing")
    require(isinstance(performance_state, str) and performance_state, "report.performance missing")
    report_state = report_state if report_state in REPORT_STATES else "unknown"
    performance_state = performance_state if performance_state in PERFORMANCE_STATES else "unknown"
    evidence_type = report.get("evidence")
    evidence_type = evidence_type if isinstance(evidence_type, str) and evidence_type in EVIDENCE_TYPES else "unknown"
    comparisons = report.get("comparisons")
    if comparisons is None:
        require(report_state not in PASS_STATES, "passing report requires comparisons")
        comparisons = []
    require(isinstance(comparisons, list), "report.comparisons must be a list")
    matrix_complete = has_complete_abba_matrix(manifest, report, comparisons)

    counts = Counter()
    p99_regressions: list[Decimal] = []
    throughput_losses: list[Decimal] = []
    p1_rows = []
    p2_values: list[Decimal | None] = []
    p2_complete = matrix_complete
    foreground_p95_values: list[Decimal | None] = []
    foreground_p99_values: list[Decimal | None] = []
    throughput_values: list[Decimal | None] = []
    error_rate_values: list[Decimal | None] = []
    pressure_samples = 0
    pressure_high_samples = 0
    pressure_samples_complete = matrix_complete
    pressure_high_samples_complete = matrix_complete
    attempt_cost_ratio_count = 0
    attempt_cost_ratios_complete = matrix_complete
    start_p95_values: list[Decimal | None] = []
    duplicate_task_values: list[Decimal | None] = []
    lock_hold_values: list[Decimal | None] = []
    w09_complete = {field: matrix_complete for field in
                    ("heal_start_p95_ms", "heal_duplicate_task_count", "heal_lock_hold_p95_ms")}
    w10_rows = []
    w11_rows = []
    for index, comparison in enumerate(comparisons):
        require(isinstance(comparison, dict), f"comparison {index} must be an object")
        state = comparison.get("status")
        require(isinstance(state, str) and state, f"comparison {index} missing status")
        counts[state if state in REPORT_STATES else "unknown"] += 1
        p99_regressions.append(number(comparison.get("p99_regression"), f"comparison {index} p99_regression"))
        throughput_change = number(comparison.get("throughput_change"), f"comparison {index} throughput_change")
        throughput_losses.append(max(Decimal("0"), -throughput_change))
        foreground_p95_values.append(maybe_number(comparison.get("foreground_p95_ms"), "foreground_p95_ms"))
        foreground_p99_values.append(maybe_number(comparison.get("foreground_p99_ms"), "foreground_p99_ms"))
        p1 = comparison.get("p1")
        if isinstance(p1, dict):
            p1_rows.append({
                "scenario": comparison.get("scenario") if comparison.get("scenario") in SCENARIOS else "unknown",
                "comparison": comparison.get("comparison") if comparison.get("comparison") in ("build", "background") else "unknown",
                "round": safe_count(comparison.get("round")),
                "required_reduction": float(number(p1.get("required_reduction"), "p1.required_reduction")),
                "observed_reduction": float(number(p1.get("observed_reduction"), "p1.observed_reduction")),
                "repeatability_drift": (
                    None if p1.get("repeatability_drift") is None
                    else float(number(p1.get("repeatability_drift"), "p1.repeatability_drift"))
                ),
                "baseline_walk_objects": safe_count(p1.get("baseline_walk_objects")),
                "baseline_cold_walk_objects": safe_count(p1.get("baseline_cold_walk_objects")),
                "candidate_walk_objects": safe_count(p1.get("candidate_walk_objects")),
                "candidate_cold_walk_objects": safe_count(p1.get("candidate_cold_walk_objects")),
            })
        p2 = comparison.get("p2_post_stop_work_multiples")
        if isinstance(p2, list) and len(p2) == len(LEGS):
            candidate = [maybe_number(p2[index], "p2_post_stop_work_multiple") for index in (1, 2)]
            p2_values.extend(candidate)
            if any(value is None for value in candidate):
                p2_complete = False
        else:
            p2_complete = False
        w09 = comparison.get("w09")
        for field, values in (("heal_start_p95_ms", start_p95_values),
                              ("heal_duplicate_task_count", duplicate_task_values),
                              ("heal_lock_hold_p95_ms", lock_hold_values)):
            samples = w09.get(field) if isinstance(w09, dict) else None
            if isinstance(samples, list):
                values.extend(maybe_number(value, field) for value in samples)
            if not isinstance(samples, list) or len(samples) != len(LEGS) or any(value is None for value in samples):
                w09_complete[field] = False
        w10_w11 = comparison.get("w10_w11")
        if isinstance(w10_w11, dict):
            samples = w10_w11.get("foreground_pressure_samples")
            if isinstance(samples, list) and len(samples) == len(LEGS) and all(value is not None for value in samples):
                pressure_samples += sum(require_integer(value, "foreground_pressure_samples", 0) for value in samples)
            else:
                pressure_samples_complete = False
            high_samples = w10_w11.get("foreground_pressure_high_samples")
            if isinstance(high_samples, list) and len(high_samples) == len(LEGS) and all(value is not None for value in high_samples):
                pressure_high_samples += sum(require_integer(value, "foreground_pressure_high_samples", 0) for value in high_samples)
            else:
                pressure_high_samples_complete = False
            costs = w10_w11.get("attempt_cost_per_healed_object")
            if isinstance(costs, list) and len(costs) == len(LEGS):
                attempt_cost_ratio_count += sum(value is not None for value in costs)
            else:
                attempt_cost_ratios_complete = False
        else:
            pressure_samples_complete = False
            pressure_high_samples_complete = False
            attempt_cost_ratios_complete = False
        w10 = comparison.get("w10")
        if isinstance(w10, dict) and comparison.get("scenario") == "running-heal" and comparison.get("comparison") == "build":
            w10_rows.append({
                "round": safe_count(comparison.get("round")),
                "status": safe_observation_state(w10.get("status")),
                "pacing_observed": w10.get("pacing_observed") if type(w10.get("pacing_observed")) is bool else None,
                "candidate_pressure_high_ratio": safe_observed_number(w10.get("candidate_pressure_high_ratio")),
                "candidate_delay_events": safe_count(w10.get("candidate_delay_events")),
                "foreground_p99_change": safe_observed_number(w10.get("foreground_p99_change")),
                "foreground_throughput_change": safe_observed_number(w10.get("foreground_throughput_change")),
            })
        if "throughput_ops" in comparison:
            throughput_values.append(maybe_number(comparison.get("throughput_ops"), "throughput_ops"))
        error_rate = maybe_number(comparison.get("error_rate"), "error_rate")
        require(error_rate is None or Decimal("0") <= error_rate <= Decimal("1"), "invalid error_rate")
        error_rate_values.append(error_rate)
        w11 = comparison.get("w11")
        if isinstance(w11, dict) and comparison.get("scenario") == "running-heal" and comparison.get("comparison") == "build":
            w11_rows.append({
                "round": safe_count(comparison.get("round")),
                "status": safe_observation_state(w11.get("status")),
                "rss_growth": safe_observed_number(w11.get("rss_growth")),
                "rss_growth_limit": safe_observed_number(w11.get("rss_growth_limit")),
                "heal_lock_wait_p99_change": safe_observed_number(w11.get("heal_lock_wait_p99_change")),
                "foreground_p99_change": safe_observed_number(w11.get("foreground_p99_change")),
                "foreground_throughput_change": safe_observed_number(w11.get("foreground_throughput_change")),
                "candidate_attempt_cost_per_healed_object": safe_observed_number(w11.get("candidate_attempt_cost_per_healed_object")),
            })

    measured = report.get("evidence") == "measured"
    reported_pass = report_state in PASS_STATES and performance_state in PASS_STATES and measured
    verified = measured and measured_provenance_verified(abba_dir)
    if reported_pass:
        require_complete_abba_matrix(manifest, report, comparisons)
        validate_release_evidence_manifest({**manifest, "evidence": "measured"})
        for index, comparison in enumerate(comparisons):
            require_measured_comparison_evidence(comparison, index)
        require(all(value is not None for value in error_rate_values), "error_rate must be measured")
        require(all(value == 0 for value in error_rate_values), "passing report has nonzero error_rate")
        require(pressure_samples_complete, "foreground_pressure_samples must be measured")
        require(pressure_high_samples_complete, "foreground_pressure_high_samples must be measured")
    passed = reported_pass and verified
    if not verified and report.get("evidence") != "synthetic":
        for row in w10_rows + w11_rows:
            if row["status"] == "observed":
                row["status"] = "unverified"
    gate_state = "pass" if passed else "fail"
    if report_state == "synthetic_validated":
        reason = "synthetic evidence validates the harness only; measured performance remains pending"
    elif report_state in FAIL_STATES and isinstance(report.get("error"), str) and report["error"]:
        reason = f"ABBA report status is {report_state}; error details omitted"
    elif report_state not in PASS_STATES:
        reason = f"ABBA report status is {report_state}"
    elif performance_state not in PASS_STATES:
        reason = f"performance status is {performance_state}"
    elif not measured:
        reason = "measured evidence is required for a performance conclusion"
    elif not verified:
        reason = "measured source windows and client percentiles are not independently verified"
    else:
        reason = "measured ABBA report passed"

    fixed = manifest.get("fixed", {})
    require(isinstance(fixed, dict), "manifest.fixed must be an object")
    baseline = manifest.get("baseline") if isinstance(manifest.get("baseline"), dict) else {}
    candidate = manifest.get("candidate") if isinstance(manifest.get("candidate"), dict) else {}
    return {
        "gate_state": gate_state,
        "reason": reason,
        "status": report_state,
        "performance": performance_state,
        "evidence": evidence_type,
        "measurement_provenance": "verified" if verified else "unverified",
        "cells": safe_count(report.get("cells")),
        "completed_cells": safe_count(report.get("completed_cells")),
        # Historical reports may contain unredacted subprocess arguments.
        "error": "details omitted" if isinstance(report.get("error"), str) and report["error"] else None,
        "comparisons_total": len(comparisons),
        "comparison_status_counts": dict(sorted(counts.items())),
        "worst_p99_regression": float(max(p99_regressions)) if matrix_complete and p99_regressions else None,
        "worst_throughput_loss": float(max(throughput_losses)) if matrix_complete and throughput_losses else None,
        # Comparisons describe different legs and windows. Their maxima are
        # not a percentile of one measured request population.
        "foreground_p95_ms": None,
        "foreground_p99_ms": None,
        "max_reported_comparison_p95_ms": (None if max_decimal(foreground_p95_values) is None
                                           else float(max_decimal(foreground_p95_values))),
        "max_reported_comparison_p99_ms": (None if max_decimal(foreground_p99_values) is None
                                           else float(max_decimal(foreground_p99_values))),
        "throughput_ops": None,
        "max_reported_comparison_throughput_ops": (float(max(throughput_values))
                                                   if matrix_complete and len(throughput_values) == len(comparisons)
                                                   and all(value is not None for value in throughput_values) else None),
        "error_rate": (None if not matrix_complete or any(value is None for value in error_rate_values)
                       else float(max_decimal(error_rate_values))),
        "foreground_pressure_samples": pressure_samples if pressure_samples_complete else None,
        "foreground_pressure_high_samples": pressure_high_samples if pressure_high_samples_complete else None,
        "attempt_cost_samples": None,
        "reported_attempt_cost_ratio_count": attempt_cost_ratio_count if attempt_cost_ratios_complete else None,
        "p2_worst_post_stop_work_multiple": float(max(p2_values)) if p2_complete and p2_values else None,
        "w09_worst_heal_start_p95_ms": (float(max(start_p95_values))
                                         if w09_complete["heal_start_p95_ms"] and start_p95_values else None),
        "w09_duplicate_task_count": (float(max(duplicate_task_values))
                                     if w09_complete["heal_duplicate_task_count"] and duplicate_task_values else None),
        "w09_worst_lock_hold_p95_ms": (float(max(lock_hold_values))
                                       if w09_complete["heal_lock_hold_p95_ms"] and lock_hold_values else None),
        "p1_reductions": p1_rows,
        "w10_running_heal_build": w10_rows,
        "w11_running_heal_build": w11_rows,
        "provenance": {
            "abba_dir": str(abba_dir.resolve()),
            "manifest_sha256": manifest_sha256,
            "report_sha256": report_sha256,
            "baseline_revision": safe_hex(baseline.get("revision"), 40),
            "baseline_sha256": safe_hex(baseline.get("sha256"), 64),
            "candidate_revision": safe_hex(candidate.get("revision"), 40),
            "candidate_sha256": safe_hex(candidate.get("sha256"), 64),
            "adapter_sha256": safe_hex(manifest.get("adapter_sha256"), 64),
            "collector_sha256": safe_hex(manifest.get("collector_sha256"), 64),
            "config_sha256": safe_hex(fixed.get("config_sha256"), 64),
            "dataset_sha256": safe_hex(fixed.get("dataset_sha256"), 64),
            "release_flags_present": isinstance(fixed.get("release_flags"), str) and bool(fixed["release_flags"]),
            "durability": fixed.get("durability") if fixed.get("durability") in ("drive-sync=on", "drive-sync=off") else None,
            "topology": fixed.get("topology") if fixed.get("topology") == "EC8+4" else None,
            "offered_load_ops": safe_observed_number(fixed.get("offered_load_ops")),
            "release_evidence_present": isinstance(manifest.get("release_evidence"), dict),
            "started_at": safe_timestamp(report.get("started_at")) or safe_timestamp(manifest.get("started_at")),
            "finished_at": safe_timestamp(report.get("finished_at")),
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
        scenario = record.get("scenario")
        scenarios[scenario if isinstance(scenario, str) and scenario in ("unchanged", "small_dirty", "all_dirty") else "unknown"] += 1
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
        revision = build.get("source_revision")
        revision = (revision if isinstance(revision, str) and 1 <= len(revision) <= 40
                    and all(c in "0123456789abcdef" for c in revision) else None)
        tree = build.get("source_tree")
        tree = tree if tree in ("clean", "dirty", "unknown") else None
        opt = build.get("test_opt_level_override")
        opt = opt if opt in ("0", "1", "2", "3", "s", "z") else None
        build_sources.add((revision, tree, opt))
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


def write_release_bundle_descriptor(_args: argparse.Namespace, summary: dict[str, Any]) -> None:
    require(summary["verdict"] == "PASS", "release descriptor requires a measured PASS summary")
    raise ValueError("release descriptor source evidence is unavailable")


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
        f"- abba: status={abba['status']} performance={abba['performance']} evidence={abba['evidence']} cells={abba['cells'] if abba['cells'] is not None else 'pending'} comparisons={abba['comparisons_total']}",
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
                        help="Reserved; verified source evidence is not yet available")
    parser.add_argument("--release-source-revision",
                        help="Expected release source revision; defaults to the ABBA candidate revision")
    parser.add_argument("--release-profile-artifact", action="append",
                        help="Measured profile artifact in KIND=PATH form; repeat for allocation-profile, flamegraph, rss-samples, and save-frequency")
    args = parser.parse_args()
    try:
        summary = build_summary(args)
        if args.release_bundle_descriptor_out:
            write_release_bundle_descriptor(args, summary)
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
        print(f"FAIL scanner_heal_perf error={type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
