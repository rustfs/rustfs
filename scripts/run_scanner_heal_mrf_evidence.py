#!/usr/bin/env python3
"""Assemble measured Scanner/Heal G07/G08/P4 MRF release evidence.

The producer consumes raw W13 MRF JSON artifacts emitted by the measured Rust
test and packages them into the common release-bundle descriptor shape. It is
usable both by the W13 shell runner and by operators who need to re-check an
already collected MRF evidence directory.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import check_test_wiring as wiring

ROOT = Path(__file__).resolve().parents[1]
FIELD_ARTIFACTS = {
    "G07": {
        "mrf_responsibility_oracle": "g07-mrf-responsibility/G07-mrf_responsibility_oracle.json",
        "commit_boundary_crash_matrix": "g07-mrf-responsibility/G07-commit_boundary_crash_matrix.json",
    },
    "G08": {
        "mrf_capacity_evidence": "g08-mrf-capacity/G08-mrf_capacity_evidence.json",
        "disk_full_matrix": "g08-mrf-capacity/G08-disk_full_matrix.json",
        "replica_loss_matrix": "g08-mrf-capacity/G08-replica_loss_matrix.json",
    },
    "P4": {
        "mrf_scale_measurement": "p4-mrf-soak/P4-mrf_scale_measurement.json",
        "mrf_replay_cost_measurement": "p4-mrf-soak/P4-mrf_replay_cost_measurement.json",
        "retained_responsibility_evidence": "p4-mrf-soak/P4-retained_responsibility_evidence.json",
        "mrf_cleanup_gc_soak_evidence": "p4-mrf-soak/P4-mrf_cleanup_gc_soak_evidence.json",
    },
}
SELECTION_GATES = {
    "all": ("G07", "G08", "P4"),
    "g07": ("G07",),
    "g08": ("G08",),
    "p4": ("P4",),
}


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def source_revision_for(run_dir: Path, override: str | None) -> str:
    if override:
        return override
    stamp = run_dir / "source-revision.txt"
    if stamp.is_file():
        value = stamp.read_text().strip()
        wiring.require(value, "source-revision.txt is empty")
        return value
    return git_head()


def timestamp(value: Any, name: str) -> str:
    wiring.require(isinstance(value, str) and value.strip(), f"missing {name}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    wiring.require(parsed.tzinfo is not None, f"{name} must include timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def selected_artifacts(selection: str) -> dict[str, dict[str, Path]]:
    return {
        gate: {field: Path(relative) for field, relative in FIELD_ARTIFACTS[gate].items()}
        for gate in SELECTION_GATES[selection]
    }


def descriptor_path_for(run_dir: Path, output: Path | None) -> Path:
    if output is not None:
        return output.resolve()
    return run_dir / "release-bundle-w13.json"


def relative_to_descriptor(path: Path, descriptor: Path) -> str:
    return path.resolve(strict=True).relative_to(descriptor.parent.resolve()).as_posix()


def measured_payload(path: Path, source_revision: str, gate: str, field: str) -> dict[str, Any]:
    payload = wiring.read_json(path.resolve())
    wiring.require(isinstance(payload, dict), f"{gate}.{field} artifact must be a JSON object")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        wiring.require(payload.get(marker) is not True, f"{gate}.{field} artifact is {marker}")
    wiring.require(payload.get("schema") == 1, f"{gate}.{field} artifact schema must be 1")
    wiring.require(payload.get("evidence_type") == "measured", f"{gate}.{field} artifact must be measured")
    wiring.require(payload.get("source_revision") == source_revision, f"{gate}.{field} source revision mismatch")
    wiring.require(payload.get("gate") == gate, f"{gate}.{field} artifact gate mismatch")
    wiring.require(payload.get("field") == field, f"{gate}.{field} artifact field mismatch")
    for key in ("run_id", "measurement_window_id"):
        value = payload.get(key)
        wiring.require(isinstance(value, str) and value.strip(), f"{gate}.{field} missing {key}")
    expected_kind = wiring.SCANNER_HEAL_RELEASE_MRF_ARTIFACT_KINDS[(gate, field)]
    wiring.require(payload.get("artifact_kind") == expected_kind, f"{gate}.{field} artifact kind mismatch")
    timestamp(payload.get("started_at"), f"{gate}.{field}.started_at")
    timestamp(payload.get("finished_at"), f"{gate}.{field}.finished_at")
    command = payload.get("command")
    wiring.require(
        isinstance(command, list) and command and all(isinstance(item, str) and item.strip() for item in command),
        f"{gate}.{field} artifact missing command provenance",
    )
    summary = payload.get("summary")
    wiring.require(isinstance(summary, str) and summary.strip(), f"{gate}.{field} artifact missing summary")
    if gate == "G08" and field == "disk_full_matrix":
        for observed in (
            "journal_write_enospc_observed",
            "committed_checkpoint_enospc_observed",
            "cleanup_delete_on_full_filesystem_observed",
        ):
            wiring.release_bundle_bool_true(payload.get(observed), f"{gate}.{field}.{observed}")
        wiring.evidence_integer(payload.get("enospc_filler_bytes"), f"{gate}.{field}.enospc_filler_bytes", 1, 2**63 - 1)
    return payload


def validate_mrf_field(gate: str, field: str, evidence: dict[str, Any]) -> None:
    if gate == "G07":
        case_field = {
            "mrf_responsibility_oracle": "mrf_responsibility_cases",
            "commit_boundary_crash_matrix": "commit_crash_cases",
        }[field]
        wiring.release_bundle_exact_strings(
            evidence.get(case_field),
            wiring.SCANNER_HEAL_RELEASE_G07_REQUIRED_CASES[field],
            f"{gate}.{field}.{case_field}",
        )
        wiring.evidence_integer(evidence.get("replayed_records"), f"{gate}.{field}.replayed_records", 1, 2**63 - 1)
        wiring.release_bundle_bool_true(
            evidence.get("responsibility_anchor_retained"),
            f"{gate}.{field}.responsibility_anchor_retained",
        )
        wiring.release_bundle_bool_true(
            evidence.get("successor_snapshot_published"),
            f"{gate}.{field}.successor_snapshot_published",
        )
    if gate == "G08":
        case_field = {
            "mrf_capacity_evidence": "capacity_cases",
            "disk_full_matrix": "disk_full_cases",
            "replica_loss_matrix": "replica_loss_cases",
        }[field]
        wiring.release_bundle_exact_strings(
            evidence.get(case_field),
            wiring.SCANNER_HEAL_RELEASE_G08_REQUIRED_CASES[field],
            f"{gate}.{field}.{case_field}",
        )
    if gate == "P4":
        duration = wiring.evidence_integer(evidence.get("duration_seconds"), f"{gate}.{field}.duration_seconds", 1, 86400)
        wiring.require(duration >= 900, f"{gate}.{field} requires at least 900 seconds")
        if field != "mrf_scale_measurement":
            wiring.evidence_integer(evidence.get("replayed_records"), f"{gate}.{field}.replayed_records", 1, 2**63 - 1)
            wiring.release_bundle_bool_true(
                evidence.get("responsibility_anchor_retained"),
                f"{gate}.{field}.responsibility_anchor_retained",
            )
            wiring.release_bundle_bool_true(
                evidence.get("successor_snapshot_published"),
                f"{gate}.{field}.successor_snapshot_published",
            )
        if field == "retained_responsibility_evidence":
            wiring.release_bundle_exact_strings(
                evidence.get("retained_responsibility_cases"),
                wiring.SCANNER_HEAL_RELEASE_P4_RETAINED_RESPONSIBILITY_CASES,
                f"{gate}.{field}.retained_responsibility_cases",
            )
            retention_window = wiring.evidence_integer(
                evidence.get("retention_window_seconds"),
                f"{gate}.{field}.retention_window_seconds",
                7200,
                86400,
            )
            wiring.require(duration >= retention_window, f"{gate}.{field} duration must cover retention window")
            wiring.release_bundle_bool_true(evidence.get("idle_cleanup_observed"), f"{gate}.{field}.idle_cleanup_observed")
            wiring.release_bundle_bool_true(
                evidence.get("verified_proof_discharge_observed"),
                f"{gate}.{field}.verified_proof_discharge_observed",
            )
        if field == "mrf_cleanup_gc_soak_evidence":
            wiring.require(duration >= 7200, f"{gate}.{field} requires at least two hours")
            wiring.release_bundle_exact_strings(
                evidence.get("cleanup_gc_cases"),
                wiring.SCANNER_HEAL_RELEASE_MRF_CLEANUP_GC_SOAK_CASES,
                f"{gate}.{field}.cleanup_gc_cases",
            )
            wiring.release_bundle_bool_true(
                evidence.get("verified_idle_gc_observed"),
                f"{gate}.{field}.verified_idle_gc_observed",
            )
            wiring.require(evidence.get("pending_responsibilities_after_gc") == 0,
                           f"{gate}.{field} requires zero pending responsibilities after GC")
            wiring.require(evidence.get("stale_journals_after_gc") == 0,
                           f"{gate}.{field} requires zero stale journals after GC")


def field_evidence(run_dir: Path, descriptor: Path, source_revision: str, gate: str, field: str, relative: Path) -> dict[str, Any]:
    artifact = run_dir / relative
    payload = measured_payload(artifact, source_revision, gate, field)
    evidence = {
        "artifact": relative_to_descriptor(artifact, descriptor),
        "sha256": wiring.digest(artifact),
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": payload["run_id"],
        "measurement_window_id": payload["measurement_window_id"],
        "started_at": payload["started_at"],
        "finished_at": payload["finished_at"],
        "command": payload["command"],
        "artifact_format": "json",
        "summary": payload["summary"],
    }
    for mirror in wiring.release_bundle_json_artifact_mirrored_fields(gate, field):
        wiring.require(mirror in payload, f"{gate}.{field} artifact missing {mirror}")
        evidence[mirror] = payload[mirror]
    if gate == "P4":
        evidence["duration_seconds"] = payload["duration_seconds"]
    validate_mrf_field(gate, field, evidence)
    return evidence


def build_descriptor(args: argparse.Namespace) -> Path:
    run_dir = args.run_dir.resolve()
    wiring.require(run_dir.is_dir(), "run directory is missing")
    descriptor = descriptor_path_for(run_dir, args.out_file)
    wiring.require(descriptor.parent == run_dir or descriptor.parent.is_relative_to(run_dir),
                   "descriptor must be written under the run directory")
    source_revision = source_revision_for(run_dir, args.source_revision)
    registry = wiring.read_json(ROOT / ".config/scanner-heal-required-tests.json")
    requirements = {item["gate"]: item for item in registry["release_requirements"]}
    gates: dict[str, Any] = {}
    for gate, artifacts in selected_artifacts(args.test).items():
        gates[gate] = {
            "status": "pass",
            "lane": requirements[gate]["lane"],
            "evidence_type": "measured",
            "evidence_fields": {
                field: field_evidence(run_dir, descriptor, source_revision, gate, field, relative)
                for field, relative in artifacts.items()
            },
        }
    wiring.write_json(descriptor, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": gates,
    })
    for gate in gates:
        subprocess.check_call([
            sys.executable,
            str(ROOT / "scripts/check_test_wiring.py"),
            "--check-scanner-heal-release-bundle-gate",
            str(descriptor),
            gate,
        ], cwd=ROOT)
    return descriptor


def base_payload(source_revision: str, gate: str, field: str, duration_seconds: int = 7200) -> dict[str, Any]:
    started = datetime.now(timezone.utc).replace(microsecond=0)
    finished = started + timedelta(seconds=duration_seconds)
    return {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": f"w13-mrf-{gate.lower()}-{source_revision[:12]}",
        "measurement_window_id": f"w13-mrf-window-{gate.lower()}-{source_revision[:12]}",
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "finished_at": finished.isoformat().replace("+00:00", "Z"),
        "command": ["scripts/run_scanner_heal_w13_mrf_evidence.sh", "--test", gate.lower()],
        "summary": f"Measured Scanner/Heal {gate}.{field} MRF evidence.",
        "gate": gate,
        "field": field,
        "artifact_kind": wiring.SCANNER_HEAL_RELEASE_MRF_ARTIFACT_KINDS[(gate, field)],
    }


def write_self_test_artifacts(run_dir: Path, source_revision: str) -> None:
    for gate, artifacts in selected_artifacts("all").items():
        for field, relative in artifacts.items():
            payload = base_payload(source_revision, gate, field)
            if gate == "G07":
                payload["crash_points"] = ["before-commit", "after-commit"]
                payload["replayed_records"] = 2
                payload["responsibility_anchor_retained"] = True
                payload["successor_snapshot_published"] = True
                if field == "mrf_responsibility_oracle":
                    payload["mrf_responsibility_cases"] = list(wiring.SCANNER_HEAL_RELEASE_G07_REQUIRED_CASES[field])
                else:
                    payload["commit_crash_cases"] = list(wiring.SCANNER_HEAL_RELEASE_G07_REQUIRED_CASES[field])
            elif gate == "G08":
                case_field = {
                    "mrf_capacity_evidence": "capacity_cases",
                    "disk_full_matrix": "disk_full_cases",
                    "replica_loss_matrix": "replica_loss_cases",
                }[field]
                payload[case_field] = list(wiring.SCANNER_HEAL_RELEASE_G08_REQUIRED_CASES[field])
                if field == "disk_full_matrix":
                    payload.update({
                        "journal_write_enospc_observed": True,
                        "committed_checkpoint_enospc_observed": True,
                        "cleanup_delete_on_full_filesystem_observed": True,
                        "enospc_filler_bytes": 1024,
                    })
            elif gate == "P4":
                payload["duration_seconds"] = 7200
                if field != "mrf_scale_measurement":
                    payload["replayed_records"] = 2
                    payload["responsibility_anchor_retained"] = True
                    payload["successor_snapshot_published"] = True
                if field == "retained_responsibility_evidence":
                    payload.update({
                        "retained_responsibility_cases": list(wiring.SCANNER_HEAL_RELEASE_P4_RETAINED_RESPONSIBILITY_CASES),
                        "retention_window_seconds": 7200,
                        "idle_cleanup_observed": True,
                        "verified_proof_discharge_observed": True,
                    })
                if field == "mrf_cleanup_gc_soak_evidence":
                    payload.update({
                        "cleanup_gc_cases": list(wiring.SCANNER_HEAL_RELEASE_MRF_CLEANUP_GC_SOAK_CASES),
                        "verified_idle_gc_observed": True,
                        "pending_responsibilities_after_gc": 0,
                        "stale_journals_after_gc": 0,
                    })
            path = run_dir / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            wiring.write_json(path, payload)


def expect_failure(args: list[str], needle: str) -> None:
    try:
        build_descriptor(parse_args(args))
    except (ValueError, subprocess.CalledProcessError) as err:
        wiring.require(needle in str(err), f"wrong self-test failure: {err}")
    else:
        raise ValueError("self-test accepted invalid MRF evidence")


def run_self_test() -> None:
    import tempfile

    source_revision = git_head()
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "run"
        run_dir.mkdir()
        (run_dir / "source-revision.txt").write_text(source_revision + "\n")
        write_self_test_artifacts(run_dir, source_revision)
        descriptor = build_descriptor(parse_args(["--run-dir", str(run_dir)]))
        wiring.require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "run"
        run_dir.mkdir()
        write_self_test_artifacts(run_dir, source_revision)
        path = run_dir / FIELD_ARTIFACTS["G07"]["mrf_responsibility_oracle"]
        payload = wiring.read_json(path)
        payload["mrf_responsibility_cases"] = payload["mrf_responsibility_cases"][:-1]
        wiring.write_json(path, payload)
        expect_failure(["--run-dir", str(run_dir), "--source-revision", source_revision, "--test", "g07"], "missing cases")

    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "run"
        run_dir.mkdir()
        write_self_test_artifacts(run_dir, source_revision)
        path = run_dir / FIELD_ARTIFACTS["G08"]["disk_full_matrix"]
        payload = wiring.read_json(path)
        payload["journal_write_enospc_observed"] = False
        wiring.write_json(path, payload)
        expect_failure(["--run-dir", str(run_dir), "--source-revision", source_revision, "--test", "g08"], "journal_write_enospc_observed")

    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "run"
        run_dir.mkdir()
        write_self_test_artifacts(run_dir, source_revision)
        path = run_dir / FIELD_ARTIFACTS["P4"]["mrf_cleanup_gc_soak_evidence"]
        payload = wiring.read_json(path)
        payload["duration_seconds"] = 900
        payload["finished_at"] = payload["started_at"]
        wiring.write_json(path, payload)
        expect_failure(["--run-dir", str(run_dir), "--source-revision", source_revision, "--test", "p4"], "two hours")

    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp) / "run"
        run_dir.mkdir()
        write_self_test_artifacts(run_dir, source_revision)
        path = run_dir / FIELD_ARTIFACTS["G08"]["replica_loss_matrix"]
        payload = wiring.read_json(path)
        payload["synthetic"] = True
        wiring.write_json(path, payload)
        expect_failure(["--run-dir", str(run_dir), "--source-revision", source_revision, "--test", "g08"], "synthetic")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--out-file", type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--test", choices=tuple(SELECTION_GATES), default="all")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test and args.run_dir is None:
        parser.error("--run-dir is required unless --self-test is used")
    return args


def main() -> int:
    try:
        args = parse_args()
        if args.self_test:
            run_self_test()
            return 0
        descriptor = build_descriptor(args)
        print(f"Scanner/Heal MRF release descriptor verified: {descriptor}")
        return 0
    except (ValueError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
