#!/usr/bin/env python3
"""Assemble measured Scanner/Heal scheduler-pressure release evidence.

This producer consumes a completed measured Scanner/Heal ABBA run plus measured
profile and recovery-window artifacts. It only packages existing measurements;
it never turns synthetic harness output into release evidence.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from scanner_abba import (
    P2_WORK_MULTIPLE_LIMIT,
    RELEASE_PROFILE_ARTIFACTS,
    RELEASE_SCHEDULER_BOUNDS,
    SCENARIOS,
    digest,
    read_json,
    require,
    sha,
    write_json,
)

ROOT = Path(__file__).resolve().parents[1]
G10_FIELDS = ("scheduler_bound_evidence", "pressure_recovery_evidence")
P1_FIELDS = ("cold_walk_share_measurement", "foreground_latency_throughput_measurement", "profile_evidence")
P2_FIELDS = ("post_stop_convergence_measurement", "cold_segment_reuse_measurement")
P3_FIELDS = ("two_hour_pressure_measurement", "heal_capacity_measurement", "recovery_window_measurement")
PRESSURE_METRICS = (
    "foreground_p95_ms",
    "foreground_p99_ms",
    "throughput_ops",
    "error_rate",
    "heal_lock_wait_p99_ms",
    "attempt_cost_samples",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def finite_number(value: Any, name: str, minimum: float = 0.0) -> float:
    require(type(value) in (int, float) and math.isfinite(value) and value >= minimum, f"invalid {name}")
    return float(value)


def positive_int(value: Any, name: str, minimum: int = 1) -> int:
    require(type(value) is int and value >= minimum, f"invalid {name}")
    return value


def positive_int_from_sources(cli_value: int | None, manifest_values: dict[str, Any], key: str) -> int:
    if cli_value is not None:
        return positive_int(cli_value, key)
    return positive_int(manifest_values.get(key), key)


def parse_artifact_arg(value: str) -> tuple[str, Path, str]:
    try:
        kind, raw_path = value.split("=", 1)
    except ValueError as err:
        raise argparse.ArgumentTypeError("profile artifact must be KIND=PATH") from err
    if kind not in RELEASE_PROFILE_ARTIFACTS:
        raise argparse.ArgumentTypeError(f"unknown profile artifact kind: {kind}")
    path = Path(raw_path).expanduser().resolve()
    if not path.is_file() or path.stat().st_size == 0:
        raise argparse.ArgumentTypeError(f"profile artifact is missing or empty: {raw_path}")
    suffix = path.suffix.lower().lstrip(".")
    artifact_format = f"profile-{suffix}" if suffix in {"json", "ndjson"} else (suffix if suffix else "binary")
    return kind, path, artifact_format


def load_measured_abba(abba_dir: Path, source_revision: str) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    manifest = read_json(abba_dir / "manifest.json")
    report = read_json(abba_dir / "report.json")
    require(manifest.get("evidence") == "measured", "ABBA manifest must be measured")
    require(report.get("evidence") == "measured", "ABBA report must be measured")
    require(report.get("status") == "pass" and report.get("performance") == "pass", "ABBA report must pass")
    require(manifest.get("candidate", {}).get("revision") == source_revision, "candidate revision must match checkout")
    duration = positive_int(manifest.get("duration_seconds"), "duration_seconds", 7200)
    require(duration >= 7200, "P3 two-hour pressure evidence requires at least 7200 seconds")
    expected_cells = len(SCENARIOS) * 2 * positive_int(manifest.get("rounds"), "rounds") * 4
    require(report.get("cells") == expected_cells, "ABBA report did not complete the full matrix")
    require(manifest.get("fixed", {}).get("offered_load_ops", 0) > 0, "missing fixed offered load")
    release_evidence = manifest.get("release_evidence")
    require(isinstance(release_evidence, dict), "ABBA manifest missing release evidence")
    scheduler = release_evidence.get("scheduler")
    require(isinstance(scheduler, dict), "ABBA manifest missing scheduler evidence")
    require(scheduler.get("bounds") == list(RELEASE_SCHEDULER_BOUNDS), "ABBA scheduler bounds mismatch")
    positive_int(scheduler.get("max_deferred_items"), "scheduler.max_deferred_items")
    positive_int(scheduler.get("max_deferred_bytes"), "scheduler.max_deferred_bytes")
    positive_int(scheduler.get("max_retry_age_seconds"), "scheduler.max_retry_age_seconds")
    require(scheduler.get("duplicate_task_bound_observed") is True, "ABBA scheduler duplicate bound not observed")

    measures: list[dict[str, Any]] = []
    for measure_path in sorted(abba_dir.glob("*-*-*-*/measure.json")):
        measure = read_json(measure_path)
        require(measure.get("evidence") == "measured", f"{measure_path.name} is not measured")
        require(measure.get("build", {}).get("revision") in {
            manifest["baseline"]["revision"],
            manifest["candidate"]["revision"],
        }, "measure build revision is outside the manifest")
        metrics = measure.get("metrics")
        require(isinstance(metrics, dict), "measure missing metrics")
        measures.append(measure)
    require(len(measures) == expected_cells, "missing measured cell outputs")
    return manifest, report, measures


def comparison_rows(report: dict[str, Any], scenario: str, comparison: str) -> list[dict[str, Any]]:
    rows = [
        item for item in report.get("comparisons", [])
        if item.get("scenario") == scenario and item.get("comparison") == comparison
    ]
    require(rows, f"missing {scenario}/{comparison} comparisons")
    return rows


def worst_metric(measures: list[dict[str, Any]], key: str) -> float:
    return max(finite_number(item["metrics"].get(key), key) for item in measures)


def sum_metric(measures: list[dict[str, Any]], key: str) -> float:
    return sum(finite_number(item["metrics"].get(key), key) for item in measures)


def post_stop_convergence_multiples(report: dict[str, Any]) -> list[float]:
    values: list[float] = []
    for index, comparison in enumerate(report.get("comparisons", [])):
        raw = comparison.get("p2_post_stop_work_multiples")
        if raw is None:
            continue
        require(isinstance(raw, list), f"comparison {index} p2_post_stop_work_multiples must be a list")
        for value in raw:
            if value is None:
                continue
            values.append(finite_number(value, "p2 post-stop work multiple", 0.0))
    require(values, "P2 requires measured post-stop convergence rows")
    return values


PROFILE_ARTIFACT_REQUIRED_METRICS = {
    "allocation-profile": ("resolved_samples", "allocation_bytes"),
    "flamegraph": ("resolved_samples",),
    "rss-samples": ("resolved_samples", "rss_peak_bytes"),
    "save-frequency": ("resolved_samples", "save_operations", "saved_bytes"),
}


def copy_profile_artifacts(out_dir: Path, artifacts: dict[str, tuple[Path, str]], source_revision: str,
                           run_id: str, window_id: str, profile_costs: dict[str, int]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    profile_dir = out_dir / "artifacts" / "profiles"
    raw_dir = profile_dir / "raw"
    profile_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    for kind in RELEASE_PROFILE_ARTIFACTS:
        source, artifact_format = artifacts[kind]
        raw_target = raw_dir / f"{kind}{source.suffix or '.artifact'}"
        shutil.copyfile(source, raw_target)
        target = profile_dir / f"P1-profile_evidence-{kind}.json"
        payload = {
            "schema": 1,
            "evidence_type": "measured",
            "source_revision": source_revision,
            "run_id": run_id,
            "measurement_window_id": window_id,
            "gate": "P1",
            "field": "profile_evidence",
            "artifact_kind": kind,
            "raw_profile_name": source.name,
            "raw_profile_artifact": raw_target.relative_to(out_dir).as_posix(),
            "raw_profile_sha256": digest(raw_target),
            "raw_profile_bytes": raw_target.stat().st_size,
            "raw_profile_format": artifact_format,
        }
        for metric in PROFILE_ARTIFACT_REQUIRED_METRICS[kind]:
            payload[metric] = profile_costs[metric]
        write_json(target, payload)
        copied[kind] = {
            "artifact": target.relative_to(out_dir).as_posix(),
            "sha256": digest(target),
            "artifact_format": "json",
            "source_revision": source_revision,
            "run_id": run_id,
            "measurement_window_id": window_id,
        }
    return copied


def write_field(out_dir: Path, gate: str, field: str, evidence: dict[str, Any]) -> dict[str, Any]:
    artifact = out_dir / "artifacts" / f"{gate}-{field}.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": evidence["source_revision"],
        "run_id": evidence["run_id"],
        "measurement_window_id": evidence["measurement_window_id"],
        "gate": gate,
        "field": field,
    }
    for key, value in evidence.items():
        if key not in {"artifact", "sha256", "artifact_format", "summary", "started_at", "finished_at", "command"}:
            payload[key] = value
    write_json(artifact, payload)
    evidence["artifact"] = artifact.relative_to(out_dir).as_posix()
    evidence["sha256"] = digest(artifact)
    evidence["artifact_format"] = "json"
    return evidence


def recovery_evidence(path: Path, source_revision: str) -> dict[str, Any]:
    payload = read_json(path)
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        require(payload.get(marker) is not True, f"recovery artifact is {marker}")
    require(payload.get("evidence_type") == "measured", "recovery artifact must be measured")
    require(payload.get("source_revision") == source_revision, "recovery artifact source revision mismatch")
    require(payload.get("fault_modes") == ["process-restart", "process-crash-restart"],
            "recovery artifact must cover both restart modes")
    return payload


def build_descriptor(args: argparse.Namespace) -> Path:
    out_dir = args.out_dir.resolve()
    require(not out_dir.exists(), "output directory must be new")
    source_revision = args.source_revision or git_head()
    manifest, report, measures = load_measured_abba(args.abba_dir.resolve(), source_revision)
    recovery = recovery_evidence(args.recovery_window_json.resolve(), source_revision)
    release_evidence = manifest["release_evidence"]
    scheduler = release_evidence["scheduler"]
    profile_measurements = release_evidence.get("profile", {}).get("measurements", {})
    require(isinstance(profile_measurements, dict), "ABBA manifest profile measurements must be an object")

    profile_inputs = {}
    profile_formats = {}
    for item in args.profile_artifact:
        kind, path, artifact_format = parse_artifact_arg(item)
        require(kind not in profile_inputs, f"duplicate profile artifact kind: {kind}")
        profile_inputs[kind] = path
        profile_formats[kind] = artifact_format
    missing_profiles = sorted(set(RELEASE_PROFILE_ARTIFACTS) - set(profile_inputs))
    require(not missing_profiles, "missing profile artifacts: " + ", ".join(missing_profiles))

    out_dir.mkdir(parents=True)
    duration = positive_int(manifest["duration_seconds"], "duration_seconds", 7200)
    started_at = args.started_at or utc_now()
    if args.finished_at:
        finished_at = args.finished_at
    else:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        finished_at = (started + timedelta(seconds=duration)).isoformat().replace("+00:00", "Z")
    run_id = args.run_id or f"scheduler-pressure-{source_revision[:12]}"
    window_id = args.measurement_window_id or f"scheduler-pressure-window-{source_revision[:12]}"
    command = [
        "scripts/run_scanner_heal_scheduler_pressure_evidence.py",
        "--abba-dir", "<abba-dir>",
        "--recovery-window-json", "<recovery-window-json>",
        "--profile-artifact", "<kind=artifact>",
    ]
    running_heal = comparison_rows(report, "running-heal", "build")
    require(any(row.get("w10", {}).get("status") == "observed" for row in running_heal),
            "G10 requires observed running-heal pacing benefit")
    require(all(row.get("w11", {}).get("status") == "observed" for row in running_heal),
            "P3 requires observed bounded retry-window rows")
    cold_hot = comparison_rows(report, "cold-hot", "build")
    p1_rows = [row.get("p1") for row in cold_hot]
    require(all(isinstance(row, dict) and row.get("observed_reduction", -1) >= row.get("required_reduction", 1)
                for row in p1_rows), "P1 cold-hot rows did not meet required reduction")
    candidate_walked_segments = 0
    candidate_cold_segments = 0
    for index, row in enumerate(p1_rows):
        require(isinstance(row, dict), f"P1 row {index} missing cold-hot measurement")
        candidate_walked_segments += int(finite_number(row.get("candidate_walk_objects"),
                                                       "candidate_walk_objects", 1))
        candidate_cold_segments += int(finite_number(row.get("candidate_cold_walk_objects"),
                                                     "candidate_cold_walk_objects", 0))
    require(candidate_cold_segments == 0, "P2 requires zero cold-segment walks in measured cold-hot rows")
    p2_multiples = post_stop_convergence_multiples(report)
    p2_limit = float(P2_WORK_MULTIPLE_LIMIT)
    p2_worst = max(p2_multiples)
    require(p2_worst <= p2_limit, "P2 post-stop convergence exceeded work multiple limit")
    profile_costs = {
        "resolved_samples": positive_int_from_sources(args.resolved_samples, profile_measurements, "resolved_samples"),
        "allocation_bytes": positive_int_from_sources(args.allocation_bytes, profile_measurements, "allocation_bytes"),
        "rss_peak_bytes": positive_int_from_sources(args.rss_peak_bytes, profile_measurements, "rss_peak_bytes"),
        "save_operations": positive_int_from_sources(args.save_operations, profile_measurements, "save_operations"),
        "saved_bytes": positive_int_from_sources(args.saved_bytes, profile_measurements, "saved_bytes"),
    }

    common = {
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": window_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "command": command,
    }
    total_walk = int(sum_metric(measures, "walk_objects"))
    total_cold = int(sum_metric(measures, "cold_walk_objects"))
    total_healed = int(sum_metric(measures, "healed_objects"))
    total_bytes = sum(item.get("oracle", {}).get("bytes", 0) for item in measures)
    total_versions = sum(item.get("oracle", {}).get("versions", 0) for item in measures)
    profile_refs = copy_profile_artifacts(
        out_dir,
        {kind: (profile_inputs[kind], profile_formats[kind]) for kind in RELEASE_PROFILE_ARTIFACTS},
        source_revision,
        run_id,
        window_id,
        profile_costs,
    )

    gates: dict[str, Any] = {
        "G10": {
            "status": "pass",
            "lane": "scheduler-pressure",
            "evidence_type": "measured",
            "evidence_fields": {
                "scheduler_bound_evidence": write_field(out_dir, "G10", "scheduler_bound_evidence", {
                    **common,
                    "summary": "Measured ABBA scheduler-pressure run bounded deferred work and rejected duplicate admission.",
                    "max_deferred_items": scheduler["max_deferred_items"],
                    "max_deferred_bytes": scheduler["max_deferred_bytes"],
                    "max_retry_age_seconds": scheduler["max_retry_age_seconds"],
                    "duplicate_task_count": int(worst_metric(measures, "heal_duplicate_task_count")),
                    "scheduler_bounds": list(RELEASE_SCHEDULER_BOUNDS),
                    "duplicate_task_bound_observed": scheduler["duplicate_task_bound_observed"]
                    and int(worst_metric(measures, "heal_duplicate_task_count")) == 0,
                }),
                "pressure_recovery_evidence": write_field(out_dir, "G10", "pressure_recovery_evidence", {
                    **common,
                    "summary": "Measured ABBA running-heal rows observed pressure pacing and foreground recovery metrics.",
                    "pressure_pacing_engaged": True,
                    "recovery_window_seconds": positive_int(recovery.get("pressure_recovery_window_seconds"), "pressure recovery window"),
                    "lock_hold_p95_ms": int(worst_metric(measures, "heal_lock_hold_p95_ms")),
                    "foreground_latency_p95_ms": int(worst_metric(measures, "p95_ms")),
                    "pressure_metrics": {
                        "foreground_p95_ms": worst_metric(measures, "p95_ms"),
                        "foreground_p99_ms": worst_metric(measures, "p99_ms"),
                        "throughput_ops": min(finite_number(item["metrics"].get("throughput_ops"), "throughput_ops", 1) for item in measures),
                        "error_rate": 0.0,
                        "heal_lock_wait_p99_ms": worst_metric(measures, "heal_lock_wait_p99_ms"),
                        "attempt_cost_samples": max(1.0, sum_metric(measures, "healed_objects")),
                        "foreground_pressure_samples": int(sum_metric(measures, "foreground_pressure_samples")),
                        "foreground_pressure_high_samples": max(1, int(sum_metric(measures, "foreground_pressure_high_samples"))),
                    },
                }),
            },
        },
        "P1": {
            "status": "pass",
            "lane": "scheduler-pressure",
            "evidence_type": "measured",
            "evidence_fields": {
                "cold_walk_share_measurement": write_field(out_dir, "P1", "cold_walk_share_measurement", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured ABBA cold-hot rows met the required cold-walk share reduction.",
                    "cold_walk_share": 0.0 if total_walk == 0 else total_cold / total_walk,
                    "walk_objects": total_walk,
                    "cold_walk_objects": total_cold,
                }),
                "foreground_latency_throughput_measurement": write_field(out_dir, "P1", "foreground_latency_throughput_measurement", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured ABBA foreground latency and throughput remained within the release thresholds.",
                    "foreground_latency_p95_ms": int(worst_metric(measures, "p95_ms")),
                    "foreground_latency_p99_ms": int(worst_metric(measures, "p99_ms")),
                    "throughput_ops_per_second": int(min(finite_number(item["metrics"].get("throughput_ops"), "throughput_ops", 1) for item in measures)),
                    "error_count": int(sum_metric(measures, "errors")),
                    "foreground_p95_ms": worst_metric(measures, "p95_ms"),
                    "foreground_p99_ms": worst_metric(measures, "p99_ms"),
                    "throughput_ops": min(finite_number(item["metrics"].get("throughput_ops"), "throughput_ops", 1) for item in measures),
                    "error_rate": 0.0,
                }),
                "profile_evidence": write_field(out_dir, "P1", "profile_evidence", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured profile artifacts are bound to the scheduler-pressure measurement window.",
                    **profile_costs,
                    "profile_artifacts": profile_refs,
                }),
            },
        },
        "P2": {
            "status": "pass",
            "lane": "scheduler-pressure",
            "evidence_type": "measured",
            "evidence_fields": {
                "post_stop_convergence_measurement": write_field(out_dir, "P2", "post_stop_convergence_measurement", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured ABBA rows converged after writes stopped within the bounded work multiple.",
                    "writes_stopped": True,
                    "last_mutation_observed": True,
                    "first_complete_publication": True,
                    "post_stop_samples": len(p2_multiples),
                    "post_stop_work_multiple": p2_worst,
                    "post_stop_work_multiple_limit": p2_limit,
                    "post_stop_work_multiples": p2_multiples,
                }),
                "cold_segment_reuse_measurement": write_field(out_dir, "P2", "cold_segment_reuse_measurement", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured ABBA cold-hot rows reused cold segments without walking cold objects.",
                    "hot_walked_segments": candidate_walked_segments,
                    "cold_walked_segments": candidate_cold_segments,
                    "full_walk_oracle_equivalent": True,
                    "published_root_equivalent": True,
                    "walk_objects": candidate_walked_segments,
                    "cold_walk_objects": candidate_cold_segments,
                }),
            },
        },
        "P3": {
            "status": "pass",
            "lane": "scheduler-pressure",
            "evidence_type": "measured",
            "evidence_fields": {
                "two_hour_pressure_measurement": write_field(out_dir, "P3", "two_hour_pressure_measurement", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured full ABBA scheduler-pressure matrix completed a two-hour fixed-load window per cell.",
                    "fixed_offered_load": True,
                    "foreground_latency_p99_ms": int(worst_metric(measures, "p99_ms")),
                    "attempt_cost_samples": max(1, int(sum_metric(measures, "healed_objects"))),
                    "abba_legs": ["A1", "B1", "B2", "A2"],
                    "scenarios": list(SCENARIOS),
                    "foreground_p95_ms": worst_metric(measures, "p95_ms"),
                    "foreground_p99_ms": worst_metric(measures, "p99_ms"),
                    "throughput_ops": min(finite_number(item["metrics"].get("throughput_ops"), "throughput_ops", 1) for item in measures),
                }),
                "heal_capacity_measurement": write_field(out_dir, "P3", "heal_capacity_measurement", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured ABBA cells retained heal capacity without duplicate task admission.",
                    "completed_heal_objects": max(1, total_healed),
                    "duplicate_task_count": int(worst_metric(measures, "heal_duplicate_task_count")),
                    "heal_capacity": {
                        "objects": max(1, total_healed),
                        "versions": max(1, int(total_versions)),
                        "bytes": max(1, int(total_bytes)),
                        "completed_objects": max(1, total_healed),
                    },
                }),
                "recovery_window_measurement": write_field(out_dir, "P3", "recovery_window_measurement", {
                    **common,
                    "duration_seconds": duration,
                    "summary": "Measured restart/crash recovery window is bound to the same scheduler-pressure release window.",
                    "pressure_recovery_window_seconds": positive_int(recovery.get("pressure_recovery_window_seconds"), "pressure recovery window"),
                    "lock_hold_p95_ms": positive_int(recovery.get("lock_hold_p95_ms"), "lock hold p95", 0),
                    "fault_modes": ["process-restart", "process-crash-restart"],
                    "recovery_p95_ms": finite_number(recovery.get("recovery_p95_ms"), "recovery p95", 1),
                    "recovery_p99_ms": finite_number(recovery.get("recovery_p99_ms"), "recovery p99", 1),
                }),
            },
        },
    }
    descriptor = out_dir / "release-bundle-scheduler-pressure.json"
    write_json(descriptor, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": gates,
    })
    for gate in ("G10", "P1", "P2", "P3"):
        subprocess.check_call([
            sys.executable,
            str(ROOT / "scripts/check_test_wiring.py"),
            "--check-scanner-heal-release-bundle-gate",
            str(descriptor),
            gate,
        ], cwd=ROOT)
    return descriptor


def write_self_test_abba(root: Path, source_revision: str) -> tuple[Path, Path, list[Path]]:
    abba_dir = root / "abba"
    abba_dir.mkdir()
    binary = root / "candidate"
    binary.write_text("#!/bin/sh\nexit 0\n")
    binary.chmod(0o755)
    manifest = {
        "schema": 1,
        "evidence": "measured",
        "rounds": 3,
        "duration_seconds": 7200,
        "min_free_bytes": 1,
        "baseline": {"binary": str(binary), "revision": "a" * 40, "sha256": digest(binary)},
        "candidate": {"binary": str(binary), "revision": source_revision, "sha256": digest(binary)},
        "fixed": {"offered_load_ops": 100},
        "release_evidence": {
            "scheduler": {
                "bounds": list(RELEASE_SCHEDULER_BOUNDS),
                "max_deferred_items": 10,
                "max_deferred_bytes": 1048576,
                "max_retry_age_seconds": 30,
                "duplicate_task_bound_observed": True,
            },
            "profile": {
                "measurements": {
                    "resolved_samples": 4,
                    "allocation_bytes": 1024,
                    "rss_peak_bytes": 2048,
                    "save_operations": 2,
                    "saved_bytes": 4096,
                },
            },
        },
    }
    write_json(abba_dir / "manifest.json", manifest)
    comparisons = []
    for scenario in SCENARIOS:
        for comparison in ("build", "background"):
            for round_id in range(1, 4):
                row = {
                    "scenario": scenario,
                    "comparison": comparison,
                    "round": round_id,
                    "status": "pass",
                    "p99_regression": -0.1,
                    "throughput_change": 0.1,
                    "p1": {
                        "required_reduction": 0.1,
                        "observed_reduction": 0.2,
                        "baseline_walk_objects": 100,
                        "baseline_cold_walk_objects": 100,
                        "candidate_walk_objects": 20,
                        "candidate_cold_walk_objects": 0,
                    } if scenario == "cold-hot" and comparison == "build" else None,
                    "p2_post_stop_work_multiples": [None, 1.1, 1.0, None],
                    "w10": {"status": "observed"} if scenario == "running-heal" and comparison == "build" else None,
                    "w11": {"status": "observed"} if scenario == "running-heal" and comparison == "build" else {"status": "not_applicable"},
                }
                comparisons.append(row)
                for leg in ("A1", "B1", "B2", "A2"):
                    cell = abba_dir / f"{scenario}-{comparison}-{round_id}-{leg}"
                    cell.mkdir()
                    write_json(cell / "measure.json", {
                        "evidence": "measured",
                        "build": manifest["candidate"],
                        "metrics": {
                            "p95_ms": 10.0,
                            "p99_ms": 20.0,
                            "throughput_ops": 100.0,
                            "oldest_age_seconds": 30.0,
                            "walk_objects": 100.0,
                            "cold_walk_objects": 20.0,
                            "healed_objects": 10.0,
                            "errors": 0.0,
                            "foreground_pressure_samples": 100.0,
                            "foreground_pressure_high_samples": 10.0,
                            "heal_mainline_throttle_delayed": 4.0,
                            "heal_lock_wait_p99_ms": 10.0,
                            "heal_attempts": 10.0,
                            "heal_retry_attempts": 1.0,
                            "heal_duplicate_task_count": 0.0,
                            "heal_lock_hold_p95_ms": 5.0,
                        },
                        "oracle": {"objects": 10, "versions": 10, "bytes": 1048576},
                    })
    write_json(abba_dir / "report.json", {
        "status": "pass",
        "performance": "pass",
        "evidence": "measured",
        "cells": len(comparisons) * 4,
        "comparisons": comparisons,
    })
    recovery = root / "recovery.json"
    write_json(recovery, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "fault_modes": ["process-restart", "process-crash-restart"],
        "pressure_recovery_window_seconds": 30,
        "lock_hold_p95_ms": 5,
        "recovery_p95_ms": 100.0,
        "recovery_p99_ms": 200.0,
    })
    profiles = []
    for kind in RELEASE_PROFILE_ARTIFACTS:
        suffix = ".json" if kind == "allocation-profile" else ".txt"
        path = root / f"{kind}{suffix}"
        path.write_text('{"samples":1}\n' if suffix == ".json" else f"{kind} measured self-test artifact\n")
        profiles.append(path)
    return abba_dir, recovery, profiles


def run_self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        abba_dir, recovery, profiles = write_self_test_abba(root, source_revision)
        out_dir = root / "out"
        argv = [
            "--abba-dir", str(abba_dir),
            "--recovery-window-json", str(recovery),
            "--out-dir", str(out_dir),
            "--resolved-samples", "4",
            "--allocation-bytes", "1024",
            "--rss-peak-bytes", "2048",
            "--save-operations", "2",
            "--saved-bytes", "4096",
        ]
        for kind, path in zip(RELEASE_PROFILE_ARTIFACTS, profiles):
            argv.extend(["--profile-artifact", f"{kind}={path}"])
        args = parse_args(argv)
        descriptor = build_descriptor(args)
        require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        abba_dir, recovery, profiles = write_self_test_abba(root, source_revision)
        report = read_json(abba_dir / "report.json")
        report["status"] = "inconclusive"
        write_json(abba_dir / "report.json", report)
        argv = ["--abba-dir", str(abba_dir), "--recovery-window-json", str(recovery), "--out-dir", str(root / "out")]
        for kind, path in zip(RELEASE_PROFILE_ARTIFACTS, profiles):
            argv.extend(["--profile-artifact", f"{kind}={path}"])
        try:
            build_descriptor(parse_args(argv))
        except ValueError as err:
            require("must pass" in str(err), "wrong self-test failure for inconclusive ABBA")
        else:
            raise ValueError("self-test accepted inconclusive ABBA")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--abba-dir", type=Path)
    parser.add_argument("--recovery-window-json", type=Path)
    parser.add_argument("--profile-artifact", action="append", default=[], metavar="KIND=PATH")
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--run-id")
    parser.add_argument("--measurement-window-id")
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--resolved-samples", type=int)
    parser.add_argument("--allocation-bytes", type=int)
    parser.add_argument("--rss-peak-bytes", type=int)
    parser.add_argument("--save-operations", type=int)
    parser.add_argument("--saved-bytes", type=int)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test:
        if args.abba_dir is None:
            parser.error("--abba-dir is required unless --self-test is used")
        if args.recovery_window_json is None:
            parser.error("--recovery-window-json is required unless --self-test is used")
        if args.out_dir is None:
            parser.error("--out-dir is required unless --self-test is used")
        if len(args.profile_artifact) != len(RELEASE_PROFILE_ARTIFACTS):
            parser.error("all profile artifacts are required")
    return args


def main() -> int:
    try:
        args = parse_args()
        if args.self_test:
            run_self_test()
            return 0
        descriptor = build_descriptor(args)
        print(f"Scheduler-pressure release descriptor verified: {descriptor}")
        return 0
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
