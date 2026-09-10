#!/usr/bin/env python3
"""Assemble measured Scanner/Heal status-and-outcome release evidence.

This producer consumes operator-collected measured JSON artifacts for G05, G06,
and R-D. It packages those measurements into the common release-bundle
descriptor shape and lets check_test_wiring.py validate each gate.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from check_test_wiring import (
    SCANNER_HEAL_RELEASE_G05_PER_OBJECT_OUTCOME_CASES,
    SCANNER_HEAL_RELEASE_G05_TERMINAL_RETENTION_CASES,
    SCANNER_HEAL_RELEASE_G06_CONCURRENT_STATUS_CASES,
    SCANNER_HEAL_RELEASE_G06_LEGACY_CLIENT_CASES,
    SCANNER_HEAL_RELEASE_G06_TRUNCATION_CASES,
    SCANNER_HEAL_RELEASE_RD_EVENT_CASES,
    SCANNER_HEAL_RELEASE_RD_GRACE_CASES,
    SCANNER_HEAL_RELEASE_RD_LEDGER_CASES,
    SCANNER_HEAL_RELEASE_RD_MANAGER_CASES,
    release_bundle_exact_strings,
)
from scanner_abba import digest, read_json, require, write_json


ROOT = Path(__file__).resolve().parents[1]
G05_FIELDS = ("per_object_outcome_oracle", "terminal_retention_bounds")
G06_FIELDS = ("concurrent_status_evidence", "legacy_client_compatibility", "truncation_behavior")
RD_FIELDS = ("manager_disposition_evidence", "event_disposition_evidence", "ledger_disposition_evidence", "grace_handling")


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def positive_int(value: Any, name: str, minimum: int = 1, maximum: int = 2**63 - 1) -> int:
    require(type(value) is int and minimum <= value <= maximum, f"invalid {name}")
    return value


def identity_string(value: Any, name: str) -> str:
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}", value), f"invalid {name}")
    return value


def timestamp(value: Any, name: str) -> str:
    require(isinstance(value, str) and value.endswith("Z"), f"invalid {name}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, f"{name} must include timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def bool_true(value: Any, name: str) -> None:
    require(value is True, f"{name} must be true")


def load_measured_json(path: Path, source_revision: str, label: str) -> dict[str, Any]:
    payload = read_json(path.resolve())
    require(isinstance(payload, dict), f"{label} must be a JSON object")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        require(payload.get(marker) is not True, f"{label} is {marker}")
    require(payload.get("schema") == 1, f"{label} schema must be 1")
    require(payload.get("evidence_type") == "measured", f"{label} must be measured")
    require(payload.get("source_revision") == source_revision, f"{label} source revision mismatch")
    identity_string(payload.get("run_id"), f"{label}.run_id")
    identity_string(payload.get("measurement_window_id"), f"{label}.measurement_window_id")
    require(payload["measurement_window_id"] != payload["run_id"], f"{label} must separate run/window identities")
    started_at = timestamp(payload.get("started_at"), f"{label}.started_at")
    finished_at = timestamp(payload.get("finished_at"), f"{label}.finished_at")
    require(
        datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
        >= datetime.fromisoformat(started_at.replace("Z", "+00:00")),
        f"{label}.finished_at precedes started_at",
    )
    require(isinstance(payload.get("command"), list) and payload["command"], f"{label} missing command provenance")
    return payload


def shared_raw_identity(payloads: list[tuple[str, dict[str, Any]]]) -> dict[str, str]:
    first_label, first = payloads[0]
    identity = {
        "run_id": first["run_id"],
        "measurement_window_id": first["measurement_window_id"],
        "started_at": timestamp(first["started_at"], f"{first_label}.started_at"),
        "finished_at": timestamp(first["finished_at"], f"{first_label}.finished_at"),
    }
    for label, payload in payloads[1:]:
        for key, expected in identity.items():
            observed = timestamp(payload[key], f"{label}.{key}") if key.endswith("_at") else payload[key]
            require(observed == expected, f"{label}.{key} does not match status-and-outcome run identity")
    return identity


def validate_status_outcome(payload: dict[str, Any]) -> None:
    release_bundle_exact_strings(
        payload.get("per_object_outcome_cases"),
        SCANNER_HEAL_RELEASE_G05_PER_OBJECT_OUTCOME_CASES,
        "status outcome per_object_outcome_cases",
    )
    outcomes = payload.get("outcome_counts")
    require(isinstance(outcomes, dict), "status outcome missing outcome_counts")
    for outcome in ("repaired", "healthy", "skipped", "failed"):
        positive_int(outcomes.get(outcome), f"outcome_counts.{outcome}")
    bool_true(payload.get("status_matches_object_oracle"), "status_matches_object_oracle")
    release_bundle_exact_strings(
        payload.get("terminal_retention_cases"),
        SCANNER_HEAL_RELEASE_G05_TERMINAL_RETENTION_CASES,
        "status outcome terminal_retention_cases",
    )
    window = positive_int(payload.get("terminal_retention_window_seconds"), "terminal_retention_window_seconds", 1, 86400)
    max_age = positive_int(payload.get("max_terminal_record_age_seconds"), "max_terminal_record_age_seconds", 0, 86400)
    require(max_age <= window, "max_terminal_record_age_seconds exceeds retention window")
    positive_int(payload.get("terminal_records_pruned_after_window"), "terminal_records_pruned_after_window")


def validate_status_compat(payload: dict[str, Any]) -> None:
    release_bundle_exact_strings(
        payload.get("concurrent_status_cases"),
        SCANNER_HEAL_RELEASE_G06_CONCURRENT_STATUS_CASES,
        "status compat concurrent_status_cases",
    )
    positive_int(payload.get("status_samples"), "status_samples", 2)
    bool_true(payload.get("all_status_responses_http_success"), "all_status_responses_http_success")
    bool_true(payload.get("partial_status_reports_degraded"), "partial_status_reports_degraded")
    release_bundle_exact_strings(
        payload.get("legacy_client_cases"),
        SCANNER_HEAL_RELEASE_G06_LEGACY_CLIENT_CASES,
        "status compat legacy_client_cases",
    )
    bool_true(payload.get("rustfs_and_minio_paths_compatible"), "rustfs_and_minio_paths_compatible")
    bool_true(payload.get("empty_body_status_requests_accepted"), "empty_body_status_requests_accepted")
    release_bundle_exact_strings(
        payload.get("truncation_cases"),
        SCANNER_HEAL_RELEASE_G06_TRUNCATION_CASES,
        "status compat truncation_cases",
    )
    bool_true(payload.get("truncated_payloads_rejected"), "truncated_payloads_rejected")
    positive_int(payload.get("max_status_payload_bytes"), "max_status_payload_bytes", 1, 2**20)


def validate_disposition(payload: dict[str, Any]) -> None:
    release_bundle_exact_strings(
        payload.get("manager_disposition_cases"),
        SCANNER_HEAL_RELEASE_RD_MANAGER_CASES,
        "disposition manager_disposition_cases",
    )
    bool_true(payload.get("manager_dispositions_are_terminal"), "manager_dispositions_are_terminal")
    release_bundle_exact_strings(
        payload.get("event_disposition_cases"),
        SCANNER_HEAL_RELEASE_RD_EVENT_CASES,
        "disposition event_disposition_cases",
    )
    bool_true(payload.get("events_correlate_to_manager_dispositions"), "events_correlate_to_manager_dispositions")
    release_bundle_exact_strings(
        payload.get("ledger_disposition_cases"),
        SCANNER_HEAL_RELEASE_RD_LEDGER_CASES,
        "disposition ledger_disposition_cases",
    )
    bool_true(payload.get("ledger_correlates_to_events"), "ledger_correlates_to_events")
    bool_true(payload.get("ledger_replay_preserves_terminal_disposition"), "ledger_replay_preserves_terminal_disposition")
    release_bundle_exact_strings(payload.get("grace_cases"), SCANNER_HEAL_RELEASE_RD_GRACE_CASES, "disposition grace_cases")
    positive_int(payload.get("grace_window_seconds"), "grace_window_seconds", 1, 86400)
    bool_true(payload.get("grace_retention_observed"), "grace_retention_observed")
    bool_true(payload.get("grace_expiry_pruned_terminal_records"), "grace_expiry_pruned_terminal_records")


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


def common_evidence(args: argparse.Namespace, source_revision: str, identity: dict[str, str]) -> dict[str, Any]:
    duration = positive_int(args.duration_seconds, "duration_seconds", 1, 86400)
    started_at = timestamp(args.started_at, "--started-at") if args.started_at else identity["started_at"]
    finished_at = timestamp(args.finished_at, "--finished-at") if args.finished_at else identity["finished_at"]
    run_id = args.run_id or identity["run_id"]
    measurement_window_id = args.measurement_window_id or identity["measurement_window_id"]
    require(run_id == identity["run_id"], "--run-id must match raw status-and-outcome artifacts")
    require(measurement_window_id == identity["measurement_window_id"],
            "--measurement-window-id must match raw status-and-outcome artifacts")
    require(started_at == identity["started_at"], "--started-at must match raw status-and-outcome artifacts")
    require(finished_at == identity["finished_at"], "--finished-at must match raw status-and-outcome artifacts")
    return {
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": measurement_window_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration,
        "command": [
            "scripts/run_scanner_heal_status_outcome_evidence.py",
            "--status-outcome-json", "<status-outcome-json>",
            "--status-compat-json", "<status-compat-json>",
            "--disposition-json", "<disposition-json>",
        ],
    }


def build_descriptor(args: argparse.Namespace) -> Path:
    out_dir = args.out_dir.resolve()
    require(not out_dir.exists(), "output directory must be new")
    source_revision = args.source_revision or git_head()
    status_outcome_path = args.status_outcome_json.resolve()
    status_compat_path = args.status_compat_json.resolve()
    disposition_path = args.disposition_json.resolve()
    status_outcome = load_measured_json(status_outcome_path, source_revision, "status outcome artifact")
    status_compat = load_measured_json(status_compat_path, source_revision, "status compatibility artifact")
    disposition = load_measured_json(disposition_path, source_revision, "disposition artifact")
    identity = shared_raw_identity([
        ("status outcome artifact", status_outcome),
        ("status compatibility artifact", status_compat),
        ("disposition artifact", disposition),
    ])
    validate_status_outcome(status_outcome)
    validate_status_compat(status_compat)
    validate_disposition(disposition)

    out_dir.mkdir(parents=True)
    common = common_evidence(args, source_revision, identity)
    source_artifacts = {
        "status_outcome_source_sha256": digest(status_outcome_path),
        "status_compat_source_sha256": digest(status_compat_path),
        "disposition_source_sha256": digest(disposition_path),
    }
    gates: dict[str, Any] = {
        "G05": {
            "status": "pass",
            "lane": "status-and-outcome",
            "evidence_type": "measured",
            "evidence_fields": {
                "per_object_outcome_oracle": write_field(out_dir, "G05", "per_object_outcome_oracle", {
                    **common,
                    "summary": "Measured per-object heal outcomes matched the object oracle.",
                    "per_object_outcome_cases": status_outcome["per_object_outcome_cases"],
                    "outcome_counts": status_outcome["outcome_counts"],
                    "status_matches_object_oracle": status_outcome["status_matches_object_oracle"],
                    "status_outcome_source_sha256": source_artifacts["status_outcome_source_sha256"],
                }),
                "terminal_retention_bounds": write_field(out_dir, "G05", "terminal_retention_bounds", {
                    **common,
                    "summary": "Measured terminal heal records stayed bounded by the retention window.",
                    "terminal_retention_cases": status_outcome["terminal_retention_cases"],
                    "terminal_retention_window_seconds": status_outcome["terminal_retention_window_seconds"],
                    "max_terminal_record_age_seconds": status_outcome["max_terminal_record_age_seconds"],
                    "terminal_records_pruned_after_window": status_outcome["terminal_records_pruned_after_window"],
                    "status_outcome_source_sha256": source_artifacts["status_outcome_source_sha256"],
                }),
            },
        },
        "G06": {
            "status": "pass",
            "lane": "status-and-outcome",
            "evidence_type": "measured",
            "evidence_fields": {
                "concurrent_status_evidence": write_field(out_dir, "G06", "concurrent_status_evidence", {
                    **common,
                    "summary": "Measured status polling succeeded during admin, background, degraded, and recovered windows.",
                    "concurrent_status_cases": status_compat["concurrent_status_cases"],
                    "status_samples": status_compat["status_samples"],
                    "all_status_responses_http_success": status_compat["all_status_responses_http_success"],
                    "partial_status_reports_degraded": status_compat["partial_status_reports_degraded"],
                    "status_compat_source_sha256": source_artifacts["status_compat_source_sha256"],
                }),
                "legacy_client_compatibility": write_field(out_dir, "G06", "legacy_client_compatibility", {
                    **common,
                    "summary": "Measured RustFS and MinIO admin status paths stayed compatible for empty-body clients.",
                    "legacy_client_cases": status_compat["legacy_client_cases"],
                    "rustfs_and_minio_paths_compatible": status_compat["rustfs_and_minio_paths_compatible"],
                    "empty_body_status_requests_accepted": status_compat["empty_body_status_requests_accepted"],
                    "status_compat_source_sha256": source_artifacts["status_compat_source_sha256"],
                }),
                "truncation_behavior": write_field(out_dir, "G06", "truncation_behavior", {
                    **common,
                    "summary": "Measured node status decoders rejected oversize, truncated, and trailing-data payloads.",
                    "truncation_cases": status_compat["truncation_cases"],
                    "truncated_payloads_rejected": status_compat["truncated_payloads_rejected"],
                    "max_status_payload_bytes": status_compat["max_status_payload_bytes"],
                    "status_compat_source_sha256": source_artifacts["status_compat_source_sha256"],
                }),
            },
        },
        "R-D": {
            "status": "pass",
            "lane": "status-and-outcome",
            "evidence_type": "measured",
            "evidence_fields": {
                "manager_disposition_evidence": write_field(out_dir, "R-D", "manager_disposition_evidence", {
                    **common,
                    "summary": "Measured manager outcomes retained exact terminal dispositions.",
                    "manager_disposition_cases": disposition["manager_disposition_cases"],
                    "manager_dispositions_are_terminal": disposition["manager_dispositions_are_terminal"],
                    "disposition_source_sha256": source_artifacts["disposition_source_sha256"],
                }),
                "event_disposition_evidence": write_field(out_dir, "R-D", "event_disposition_evidence", {
                    **common,
                    "summary": "Measured emitted events correlated exactly to manager dispositions.",
                    "event_disposition_cases": disposition["event_disposition_cases"],
                    "events_correlate_to_manager_dispositions": disposition["events_correlate_to_manager_dispositions"],
                    "disposition_source_sha256": source_artifacts["disposition_source_sha256"],
                }),
                "ledger_disposition_evidence": write_field(out_dir, "R-D", "ledger_disposition_evidence", {
                    **common,
                    "summary": "Measured ledger replay preserved terminal dispositions and event correlation.",
                    "ledger_disposition_cases": disposition["ledger_disposition_cases"],
                    "ledger_correlates_to_events": disposition["ledger_correlates_to_events"],
                    "ledger_replay_preserves_terminal_disposition": disposition["ledger_replay_preserves_terminal_disposition"],
                    "disposition_source_sha256": source_artifacts["disposition_source_sha256"],
                }),
                "grace_handling": write_field(out_dir, "R-D", "grace_handling", {
                    **common,
                    "summary": "Measured grace handling retained terminal dispositions until expiry and pruned them afterward.",
                    "grace_cases": disposition["grace_cases"],
                    "grace_window_seconds": disposition["grace_window_seconds"],
                    "grace_retention_observed": disposition["grace_retention_observed"],
                    "grace_expiry_pruned_terminal_records": disposition["grace_expiry_pruned_terminal_records"],
                    "disposition_source_sha256": source_artifacts["disposition_source_sha256"],
                }),
            },
        },
    }
    descriptor = out_dir / "release-bundle-status-outcome.json"
    write_json(descriptor, {"schema": 1, "evidence": "measured", "source_revision": source_revision, "gates": gates})
    for gate in ("G05", "G06", "R-D"):
        subprocess.check_call([
            sys.executable,
            str(ROOT / "scripts/check_test_wiring.py"),
            "--check-scanner-heal-release-bundle-gate",
            str(descriptor),
            gate,
        ], cwd=ROOT)
    return descriptor


def write_self_test_inputs(root: Path, source_revision: str) -> tuple[Path, Path, Path]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    common = {
        "run_id": f"status-outcome-{source_revision[:12]}",
        "measurement_window_id": f"status-outcome-window-{source_revision[:12]}",
        "started_at": now.isoformat().replace("+00:00", "Z"),
        "finished_at": now.isoformat().replace("+00:00", "Z"),
        "command": ["scripts/run_live_status_outcome_probe.sh", "--measured"],
    }
    status_outcome = root / "status-outcome.json"
    write_json(status_outcome, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        **common,
        "per_object_outcome_cases": list(SCANNER_HEAL_RELEASE_G05_PER_OBJECT_OUTCOME_CASES),
        "outcome_counts": {"repaired": 4, "healthy": 3, "skipped": 2, "failed": 1},
        "status_matches_object_oracle": True,
        "terminal_retention_cases": list(SCANNER_HEAL_RELEASE_G05_TERMINAL_RETENTION_CASES),
        "terminal_retention_window_seconds": 3600,
        "max_terminal_record_age_seconds": 3599,
        "terminal_records_pruned_after_window": 2,
    })
    status_compat = root / "status-compat.json"
    write_json(status_compat, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        **common,
        "concurrent_status_cases": list(SCANNER_HEAL_RELEASE_G06_CONCURRENT_STATUS_CASES),
        "status_samples": 4,
        "all_status_responses_http_success": True,
        "partial_status_reports_degraded": True,
        "legacy_client_cases": list(SCANNER_HEAL_RELEASE_G06_LEGACY_CLIENT_CASES),
        "rustfs_and_minio_paths_compatible": True,
        "empty_body_status_requests_accepted": True,
        "truncation_cases": list(SCANNER_HEAL_RELEASE_G06_TRUNCATION_CASES),
        "truncated_payloads_rejected": True,
        "max_status_payload_bytes": 4096,
    })
    disposition = root / "disposition.json"
    write_json(disposition, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        **common,
        "manager_disposition_cases": list(SCANNER_HEAL_RELEASE_RD_MANAGER_CASES),
        "manager_dispositions_are_terminal": True,
        "event_disposition_cases": list(SCANNER_HEAL_RELEASE_RD_EVENT_CASES),
        "events_correlate_to_manager_dispositions": True,
        "ledger_disposition_cases": list(SCANNER_HEAL_RELEASE_RD_LEDGER_CASES),
        "ledger_correlates_to_events": True,
        "ledger_replay_preserves_terminal_disposition": True,
        "grace_cases": list(SCANNER_HEAL_RELEASE_RD_GRACE_CASES),
        "grace_window_seconds": 300,
        "grace_retention_observed": True,
        "grace_expiry_pruned_terminal_records": True,
    })
    return status_outcome, status_compat, disposition


def run_self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        status_outcome, status_compat, disposition = write_self_test_inputs(root, source_revision)
        descriptor = build_descriptor(parse_args([
            "--status-outcome-json", str(status_outcome),
            "--status-compat-json", str(status_compat),
            "--disposition-json", str(disposition),
            "--out-dir", str(root / "out"),
            "--duration-seconds", "60",
        ]))
        require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        status_outcome, status_compat, disposition = write_self_test_inputs(root, source_revision)
        payload = read_json(status_compat)
        payload["truncation_cases"].remove("truncated-node-status-reject")
        write_json(status_compat, payload)
        try:
            build_descriptor(parse_args([
                "--status-outcome-json", str(status_outcome),
                "--status-compat-json", str(status_compat),
                "--disposition-json", str(disposition),
                "--out-dir", str(root / "out"),
            ]))
        except ValueError as err:
            require("truncation_cases missing cases" in str(err), "wrong self-test failure for missing truncation")
        else:
            raise ValueError("self-test accepted incomplete truncation evidence")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        status_outcome, status_compat, disposition = write_self_test_inputs(root, source_revision)
        payload = read_json(disposition)
        payload["measurement_window_id"] = "status-outcome-stale-window"
        write_json(disposition, payload)
        try:
            build_descriptor(parse_args([
                "--status-outcome-json", str(status_outcome),
                "--status-compat-json", str(status_compat),
                "--disposition-json", str(disposition),
                "--out-dir", str(root / "out"),
            ]))
        except ValueError as err:
            require("does not match status-and-outcome run identity" in str(err),
                    "wrong self-test failure for mismatched raw identity")
        else:
            raise ValueError("self-test accepted mismatched raw status/outcome provenance")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        status_outcome, status_compat, disposition = write_self_test_inputs(root, source_revision)
        try:
            build_descriptor(parse_args([
                "--status-outcome-json", str(status_outcome),
                "--status-compat-json", str(status_compat),
                "--disposition-json", str(disposition),
                "--out-dir", str(root / "out"),
                "--run-id", "status-outcome-other-run",
            ]))
        except ValueError as err:
            require("--run-id must match raw status-and-outcome artifacts" in str(err),
                    "wrong self-test failure for run id relabel")
        else:
            raise ValueError("self-test accepted command-line relabeling of raw status/outcome evidence")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--status-outcome-json", type=Path)
    parser.add_argument("--status-compat-json", type=Path)
    parser.add_argument("--disposition-json", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--run-id")
    parser.add_argument("--measurement-window-id")
    parser.add_argument("--started-at")
    parser.add_argument("--finished-at")
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test:
        if args.status_outcome_json is None:
            parser.error("--status-outcome-json is required unless --self-test is used")
        if args.status_compat_json is None:
            parser.error("--status-compat-json is required unless --self-test is used")
        if args.disposition_json is None:
            parser.error("--disposition-json is required unless --self-test is used")
        if args.out_dir is None:
            parser.error("--out-dir is required unless --self-test is used")
    return args


def main() -> int:
    try:
        args = parse_args()
        if args.self_test:
            run_self_test()
            return 0
        descriptor = build_descriptor(args)
        print(f"Status-and-outcome release descriptor verified: {descriptor}")
        return 0
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
