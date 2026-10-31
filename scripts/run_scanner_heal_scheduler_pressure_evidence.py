#!/usr/bin/env python3
"""Assemble measured Scanner/Heal scheduler-pressure release evidence.

This producer consumes a completed measured Scanner/Heal ABBA run plus measured
profile and recovery-window artifacts. It only packages existing measurements;
it never turns synthetic harness output into release evidence.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from scanner_abba import (
    RELEASE_PROFILE_ARTIFACTS,
    RELEASE_SCHEDULER_BOUNDS,
    SCENARIOS,
    digest,
    measured_provenance_verified,
    read_json,
    require,
    sha,
    write_json,
)

ROOT = Path(__file__).resolve().parents[1]
G10_FIELDS = ("scheduler_bound_evidence", "pressure_recovery_evidence")
P1_FIELDS = ("cold_walk_share_measurement", "foreground_latency_throughput_measurement", "profile_evidence")
P3_FIELDS = ("two_hour_pressure_measurement", "heal_capacity_measurement", "recovery_window_measurement")

def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


@dataclass(frozen=True)
class VerifiedPressureObservations:
    source_revision: str
    window: tuple[str, str, str, str]
    fields: dict[str, dict[str, dict[str, Any]]]
    profile_costs: dict[str, int]
    profile_identities: dict[str, tuple[str, int]]


def pressure_inputs_verified(_abba_dir: Path, _recovery_path: Path, _profiles: dict[str, Path],
                             _window: tuple[str, str, str, str]) -> VerifiedPressureObservations | None:
    # The verifier must derive each field from original, independently bound
    # observations. A self-reported flag or a count from a different window is
    # not a verified observation.
    return None


def positive_int(value: Any, name: str, minimum: int = 1) -> int:
    require(type(value) is int and value >= minimum, f"invalid {name}")
    return value


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
    require(measured_provenance_verified(abba_dir), "ABBA source windows and client percentiles are not independently verified")
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


PROFILE_ARTIFACT_REQUIRED_METRICS = {
    "allocation-profile": ("resolved_samples", "allocation_bytes"),
    "flamegraph": ("resolved_samples",),
    "rss-samples": ("resolved_samples", "rss_peak_bytes"),
    "save-frequency": ("resolved_samples", "save_operations", "saved_bytes"),
}


def copy_profile_artifacts(out_dir: Path, artifacts: dict[str, tuple[Path, str]], source_revision: str,
                           run_id: str, window_id: str, profile_costs: dict[str, int],
                           profile_identities: dict[str, tuple[str, int]]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    profile_dir = out_dir / "artifacts" / "profiles"
    raw_dir = profile_dir / "raw"
    profile_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    for kind in RELEASE_PROFILE_ARTIFACTS:
        source, artifact_format = artifacts[kind]
        raw_target = raw_dir / f"{kind}{source.suffix or '.artifact'}"
        shutil.copyfile(source, raw_target)
        expected_hash, expected_size = profile_identities[kind]
        require(raw_target.stat().st_size == expected_size and digest(raw_target) == expected_hash,
                f"profile artifact differs from verified observation: {kind}")
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
    manifest, _, _ = load_measured_abba(args.abba_dir.resolve(), source_revision)
    recovery_evidence(args.recovery_window_json.resolve(), source_revision)

    profile_inputs = {}
    profile_formats = {}
    for item in args.profile_artifact:
        kind, path, artifact_format = parse_artifact_arg(item)
        require(kind not in profile_inputs, f"duplicate profile artifact kind: {kind}")
        profile_inputs[kind] = path
        profile_formats[kind] = artifact_format
    missing_profiles = sorted(set(RELEASE_PROFILE_ARTIFACTS) - set(profile_inputs))
    require(not missing_profiles, "missing profile artifacts: " + ", ".join(missing_profiles))

    duration = positive_int(manifest["duration_seconds"], "duration_seconds", 7200)
    started_at, finished_at = args.started_at, args.finished_at
    run_id, window_id = args.run_id, args.measurement_window_id
    require(all(isinstance(value, str) and value.strip() for value in (started_at, finished_at, run_id, window_id)),
            "original measurement window timestamps and identifiers are required")
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid original measurement window") from None
    require(started.tzinfo is not None and finished.tzinfo is not None and (finished - started).total_seconds() >= duration,
            "invalid original measurement window")
    window = (run_id, window_id, started_at, finished_at)
    observations = pressure_inputs_verified(args.abba_dir.resolve(), args.recovery_window_json.resolve(), profile_inputs, window)
    require(isinstance(observations, VerifiedPressureObservations) and observations.window == window
            and observations.source_revision == source_revision,
            "independent verified pressure observations are unavailable")
    expected_fields = {"G10": G10_FIELDS, "P1": P1_FIELDS, "P3": P3_FIELDS}
    require(type(observations.fields) is dict and set(observations.fields) == set(expected_fields),
            "verified pressure observations do not cover required gates")
    reserved = {"artifact", "sha256", "artifact_format", "schema", "source_revision", "run_id",
                "measurement_window_id", "started_at", "finished_at", "command", "evidence_type",
                "gate", "field", "profile_artifacts", "resolved_samples", "allocation_bytes",
                "rss_peak_bytes", "save_operations", "saved_bytes"}
    for gate, names in expected_fields.items():
        rows = observations.fields[gate]
        require(type(rows) is dict and set(rows) == set(names), f"verified {gate} observations are incomplete")
        for field in names:
            row = rows[field]
            require(type(row) is dict and not reserved.intersection(row)
                    and isinstance(row.get("summary"), str) and row["summary"].strip(),
                    f"invalid verified {gate}.{field} observation")

    profile_keys = ("resolved_samples", "allocation_bytes", "rss_peak_bytes", "save_operations", "saved_bytes")
    require(type(observations.profile_costs) is dict and set(observations.profile_costs) == set(profile_keys),
            "verified profile observations are incomplete")
    profile_costs = {key: positive_int(observations.profile_costs[key], key) for key in profile_keys}
    require(type(observations.profile_identities) is dict
            and set(observations.profile_identities) == set(RELEASE_PROFILE_ARTIFACTS),
            "verified profile identities are incomplete")
    for kind, identity in observations.profile_identities.items():
        require(type(identity) is tuple and len(identity) == 2 and sha(identity[0])
                and type(identity[1]) is int and identity[1] > 0, f"invalid verified profile identity: {kind}")
    for key in profile_keys:
        supplied = getattr(args, key)
        require(supplied is None or supplied == profile_costs[key], f"profile {key} differs from verified observation")

    command = ["scripts/run_scanner_heal_scheduler_pressure_evidence.py", "--abba-dir", "<abba-dir>",
               "--recovery-window-json", "<recovery-window-json>", "--profile-artifact", "<kind=artifact>"]
    common = {"evidence_type": "measured", "source_revision": source_revision, "run_id": run_id,
              "measurement_window_id": window_id, "started_at": started_at, "finished_at": finished_at,
              "command": command}
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".scheduler-pressure-", dir=out_dir.parent) as temporary:
        staging = Path(temporary) / "bundle"
        staging.mkdir()
        profile_refs = copy_profile_artifacts(
            staging, {kind: (profile_inputs[kind], profile_formats[kind]) for kind in RELEASE_PROFILE_ARTIFACTS},
            source_revision, run_id, window_id, profile_costs, observations.profile_identities,
        )
        gates: dict[str, Any] = {}
        for gate, names in expected_fields.items():
            fields = {}
            for field in names:
                evidence = {**common, **observations.fields[gate][field]}
                if gate == "P1" and field == "profile_evidence":
                    evidence.update(profile_costs, profile_artifacts=profile_refs)
                fields[field] = write_field(staging, gate, field, evidence)
            gates[gate] = {"status": "pass", "lane": "scheduler-pressure", "evidence_type": "measured",
                           "evidence_fields": fields}
        descriptor = staging / "release-bundle-scheduler-pressure.json"
        write_json(descriptor, {"schema": 1, "evidence": "measured", "source_revision": source_revision, "gates": gates})
        out_dir.mkdir(mode=0o700)
        # Both directories are on the same filesystem. Move the artifacts
        # without a second copy, then check the actual destination before
        # publishing the descriptor.
        os.rename(staging / "artifacts", out_dir / "artifacts")
        pending = out_dir / ".release-bundle-scheduler-pressure.pending.json"
        os.link(descriptor, pending)
        try:
            for gate in ("G10", "P1", "P3"):
                subprocess.check_call([
                    sys.executable,
                    str(ROOT / "scripts/check_test_wiring.py"),
                    "--check-scanner-heal-release-bundle-gate",
                    str(pending),
                    gate,
                ], cwd=ROOT)
            final = out_dir / descriptor.name
            os.link(pending, final)
        finally:
            pending.unlink()
    return final


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


def self_test_observations(source_revision: str, profiles: dict[str, Path]) -> VerifiedPressureObservations:
    """Synthetic packaging fixture; it is never produced by the verifier."""
    pressure = {"foreground_p95_ms": 10.0, "foreground_p99_ms": 20.0, "throughput_ops": 100.0,
                "error_rate": 0.0, "heal_lock_wait_p99_ms": 10.0, "attempt_cost_samples": 10,
                "foreground_pressure_samples": 100, "foreground_pressure_high_samples": 10}
    profile_costs = {"resolved_samples": 4, "allocation_bytes": 1024, "rss_peak_bytes": 2048,
                     "save_operations": 2, "saved_bytes": 4096}
    fields = {
        "G10": {
            "scheduler_bound_evidence": {"summary": "Fixture bounds", "max_deferred_items": 10,
                "max_deferred_bytes": 1048576, "max_retry_age_seconds": 30, "duplicate_task_count": 0,
                "scheduler_bounds": list(RELEASE_SCHEDULER_BOUNDS), "duplicate_task_bound_observed": True},
            "pressure_recovery_evidence": {"summary": "Fixture pressure", "pressure_pacing_engaged": True,
                "recovery_window_seconds": 30, "lock_hold_p95_ms": 5, "foreground_latency_p95_ms": 10,
                "pressure_metrics": pressure},
        },
        "P1": {
            "cold_walk_share_measurement": {"summary": "Fixture cold share", "duration_seconds": 7200,
                "cold_walk_share": 0.2, "walk_objects": 100, "cold_walk_objects": 20},
            "foreground_latency_throughput_measurement": {"summary": "Fixture foreground", "duration_seconds": 7200,
                "foreground_latency_p95_ms": 10, "foreground_latency_p99_ms": 20,
                "throughput_ops_per_second": 100, "error_count": 0, "foreground_p95_ms": 10.0,
                "foreground_p99_ms": 20.0, "throughput_ops": 100.0, "error_rate": 0.0},
            "profile_evidence": {"summary": "Fixture profile", "duration_seconds": 7200},
        },
        "P3": {
            "two_hour_pressure_measurement": {"summary": "Fixture two-hour pressure", "duration_seconds": 7200,
                "fixed_offered_load": True, "foreground_latency_p99_ms": 20, "attempt_cost_samples": 10,
                "abba_legs": ["A1", "B1", "B2", "A2"], "scenarios": list(SCENARIOS),
                "foreground_p95_ms": 10.0, "foreground_p99_ms": 20.0, "throughput_ops": 100.0},
            "heal_capacity_measurement": {"summary": "Fixture heal capacity", "duration_seconds": 7200,
                "completed_heal_objects": 10, "duplicate_task_count": 0,
                "heal_capacity": {"objects": 10, "versions": 10, "bytes": 4096, "completed_objects": 10}},
            "recovery_window_measurement": {"summary": "Fixture recovery", "duration_seconds": 7200,
                "pressure_recovery_window_seconds": 30, "lock_hold_p95_ms": 5,
                "fault_modes": ["process-restart", "process-crash-restart"], "recovery_p95_ms": 100.0,
                "recovery_p99_ms": 200.0},
        },
    }
    identities = {kind: (digest(path), path.stat().st_size) for kind, path in profiles.items()}
    return VerifiedPressureObservations(source_revision, ("fixture-run-0001", "fixture-window-0001",
                                        "2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z"), fields, profile_costs, identities)


def run_self_test() -> None:
    import tempfile
    from unittest import mock
    import check_test_wiring as wiring

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
            "--started-at", "2026-01-01T00:00:00Z",
            "--finished-at", "2026-01-01T02:00:00Z",
            "--run-id", "fixture-run-0001",
            "--measurement-window-id", "fixture-window-0001",
        ]
        for kind, path in zip(RELEASE_PROFILE_ARTIFACTS, profiles):
            argv.extend(["--profile-artifact", f"{kind}={path}"])
        args = parse_args(argv)
        for claim in (None, "verified"):
            if claim is not None:
                report = read_json(abba_dir / "report.json")
                report["measurement_provenance"] = claim
                write_json(abba_dir / "report.json", report)
            try:
                load_measured_abba(abba_dir, source_revision)
            except ValueError as err:
                require("not independently verified" in str(err), "wrong self-test failure for unverified ABBA")
            else:
                raise ValueError("self-test accepted unverified ABBA")
        with mock.patch(f"{__name__}.measured_provenance_verified", return_value=True):
            try:
                build_descriptor(args)
            except ValueError as err:
                require("verified pressure observations" in str(err),
                        "wrong self-test failure for unverified pressure inputs")
            else:
                raise ValueError("self-test accepted unverified pressure inputs")
        def verify_fixture(command, cwd):
            require(cwd == ROOT and command[2] == "--check-scanner-heal-release-bundle-gate",
                    "unexpected self-test checker invocation")
            with mock.patch.object(wiring, "measured_provenance_verified", return_value=True):
                status = wiring.scanner_heal_release_bundle_gate_status(ROOT, Path(command[3]), command[4])
            require(status["decision"] == "verified", "fixture gate validation failed")

        # Exercise packaging with a simulated verifier; this fixture is not
        # measurement evidence and the output stays inside the temp directory.
        with mock.patch(f"{__name__}.measured_provenance_verified", return_value=True), \
             mock.patch(f"{__name__}.pressure_inputs_verified", return_value=self_test_observations(
                 source_revision, dict(zip(RELEASE_PROFILE_ARTIFACTS, profiles)))), \
             mock.patch.object(subprocess, "check_call", side_effect=verify_fixture):
            descriptor = build_descriptor(args)
        require(descriptor.is_file(), "self-test descriptor missing")
        require("P2" not in read_json(descriptor)["gates"], "self-test manufactured P2 evidence")
        for gate in ("P1", "P3"):
            checked = subprocess.run([sys.executable, str(ROOT / "scripts/check_test_wiring.py"),
                                      "--check-scanner-heal-release-bundle-gate", str(descriptor), gate],
                                     cwd=ROOT, capture_output=True, text=True, check=False)
            require(checked.returncode == 1 and "not independently verified" in checked.stdout,
                    f"self-test accepted unverified {gate} evidence")

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
