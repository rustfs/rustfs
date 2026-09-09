#!/usr/bin/env python3
"""Assemble measured Scanner/Heal G11/G13 maintenance release evidence.

The producer consumes operator-collected measured JSON. It only packages and
checks the evidence fields; it does not run the distributed workload or approve
the full release gate by itself.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import check_test_wiring as wiring

ROOT = Path(__file__).resolve().parents[1]
G11_FIELDS = (
    "maintenance_producer_matrix",
    "complete_producer_inventory",
    "segment_activation_preflight",
)
G13_FIELDS = (
    "quorum_minus_one_matrix",
    "unknown_disk_remount_matrix",
    "object_lock_dry_run_grace_evidence",
)


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def timestamp(value: Any, name: str) -> str:
    wiring.require(isinstance(value, str) and value.strip(), f"missing {name}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    wiring.require(parsed.tzinfo is not None, f"{name} must include timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def measured_proof(path: Path, source_revision: str) -> dict[str, Any]:
    proof = wiring.read_json(path)
    wiring.require(proof.get("schema") == 1, "proof schema must be 1")
    wiring.require(proof.get("evidence_type") == "measured", "proof must be measured")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        wiring.require(proof.get(marker) is not True, f"proof is {marker}")
    wiring.require(proof.get("source_revision") == source_revision, "proof source revision mismatch")
    timestamp(proof.get("started_at"), "proof.started_at")
    timestamp(proof.get("finished_at"), "proof.finished_at")
    wiring.evidence_string(proof.get("run_id"), "proof.run_id", r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
    wiring.evidence_string(
        proof.get("measurement_window_id"),
        "proof.measurement_window_id",
        r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}",
    )
    wiring.require(proof["measurement_window_id"] != proof["run_id"], "proof must separate run/window identities")
    return proof


def field_from_proof(proof: dict[str, Any], field: str) -> dict[str, Any]:
    value = proof.get(field)
    wiring.require(isinstance(value, dict), f"proof missing {field}")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        wiring.require(value.get(marker) is not True, f"{field} is {marker}")
    if "evidence_type" in value:
        wiring.require(value["evidence_type"] == "measured", f"{field} must be measured")
    return dict(value)


def write_field(out_dir: Path, gate: str, field: str, common: dict[str, Any], field_evidence: dict[str, Any]) -> dict[str, Any]:
    evidence = {
        **common,
        **field_evidence,
        "evidence_type": "measured",
        "summary": field_evidence.get("summary") or f"Measured Scanner/Heal {gate}.{field} evidence.",
    }
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
    wiring.write_json(artifact, payload)
    evidence["artifact"] = artifact.relative_to(out_dir).as_posix()
    evidence["sha256"] = wiring.digest(artifact)
    evidence["artifact_format"] = "json"
    return evidence


def build_descriptor(args: argparse.Namespace) -> Path:
    out_dir = args.out_dir.resolve()
    wiring.require(not out_dir.exists(), "output directory must be new")
    source_revision = args.source_revision or git_head()
    proof = measured_proof(args.proof_json.resolve(), source_revision)
    out_dir.mkdir(parents=True)
    common = {
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": proof["run_id"],
        "measurement_window_id": proof["measurement_window_id"],
        "started_at": timestamp(proof["started_at"], "proof.started_at"),
        "finished_at": timestamp(proof["finished_at"], "proof.finished_at"),
        "command": [
            "scripts/run_scanner_heal_maintenance_evidence.py",
            "--proof-json",
            "<proof-json>",
        ],
    }
    gates = {
        "G11": {
            "status": "pass",
            "lane": "maintenance-producers",
            "evidence_type": "measured",
            "evidence_fields": {
                field: write_field(out_dir, "G11", field, common, field_from_proof(proof, field))
                for field in G11_FIELDS
            },
        },
        "G13": {
            "status": "pass",
            "lane": "maintenance-producers",
            "evidence_type": "measured",
            "evidence_fields": {
                field: write_field(out_dir, "G13", field, common, field_from_proof(proof, field))
                for field in G13_FIELDS
            },
        },
    }
    descriptor = out_dir / "release-bundle-maintenance.json"
    wiring.write_json(descriptor, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": gates,
    })
    for gate in ("G11", "G13"):
        subprocess.check_call([
            sys.executable,
            str(ROOT / "scripts/check_test_wiring.py"),
            "--check-scanner-heal-release-bundle-gate",
            str(descriptor),
            gate,
        ], cwd=ROOT)
    return descriptor


def write_self_test_proof(path: Path, source_revision: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    proof = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": f"maintenance-{source_revision[:12]}",
        "measurement_window_id": f"maintenance-window-{source_revision[:12]}",
        "started_at": now.isoformat().replace("+00:00", "Z"),
        "finished_at": now.isoformat().replace("+00:00", "Z"),
        "maintenance_producer_matrix": {
            "producer_identities": list(wiring.SCANNER_HEAL_REQUIRED_PRODUCER_IDENTITIES),
            "producer_families": list(wiring.SCANNER_HEAL_REQUIRED_PRODUCER_FAMILIES),
            "matrix_cases": list(wiring.SCANNER_HEAL_RELEASE_G11_REQUIRED_CASES["maintenance_producer_matrix"]),
            "durable_identity_observed": True,
            "generation_window_bound": True,
            "restart_gap_absent": True,
            "overflow_absent": True,
        },
        "complete_producer_inventory": {
            "required_producer_identities": list(wiring.SCANNER_HEAL_REQUIRED_PRODUCER_IDENTITIES),
            "observed_producer_identities": list(wiring.SCANNER_HEAL_REQUIRED_PRODUCER_IDENTITIES),
            "required_producer_families": list(wiring.SCANNER_HEAL_REQUIRED_PRODUCER_FAMILIES),
            "observed_producer_families": list(wiring.SCANNER_HEAL_REQUIRED_PRODUCER_FAMILIES),
            "missing_producer_identities": [],
            "unknown_producer_excluded": True,
        },
        "segment_activation_preflight": {
            "production_activation": False,
            "scanner_segment_reuse_activated": False,
            "proof_inputs": list(wiring.SCANNER_HEAL_SEGMENT_ACTIVATION_PROOF_INPUTS),
            "fail_closed_checks": list(wiring.SCANNER_HEAL_SEGMENT_ACTIVATION_FAIL_CLOSED_CHECKS),
        },
        "quorum_minus_one_matrix": {
            "quorum_cases": list(wiring.SCANNER_HEAL_RELEASE_G13_REQUIRED_CASES["quorum_minus_one_matrix"]),
            "no_success_at_quorum_minus_one": True,
            "exact_quorum_restored": True,
        },
        "unknown_disk_remount_matrix": {
            "remount_cases": list(wiring.SCANNER_HEAL_RELEASE_G13_REQUIRED_CASES["unknown_disk_remount_matrix"]),
            "unknown_disks_excluded": True,
            "remounted_disks_revalidated": True,
            "stale_incarnation_rejected": True,
        },
        "object_lock_dry_run_grace_evidence": {
            "grace_cases": list(wiring.SCANNER_HEAL_RELEASE_G13_REQUIRED_CASES["object_lock_dry_run_grace_evidence"]),
            "object_lock_denials_preserved": True,
            "dry_run_mutation_count": 0,
            "grace_outcomes_retained": True,
        },
    }
    wiring.write_json(path, proof)


def run_self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "maintenance-proof.json"
        write_self_test_proof(proof, source_revision)
        descriptor = build_descriptor(parse_args([
            "--proof-json", str(proof),
            "--out-dir", str(root / "out"),
        ]))
        wiring.require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "maintenance-proof.json"
        write_self_test_proof(proof, source_revision)
        payload = wiring.read_json(proof)
        payload["evidence_type"] = "synthetic"
        wiring.write_json(proof, payload)
        try:
            build_descriptor(parse_args(["--proof-json", str(proof), "--out-dir", str(root / "out")]))
        except ValueError as err:
            wiring.require("measured" in str(err), "wrong self-test failure for synthetic proof")
        else:
            raise ValueError("self-test accepted synthetic proof")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof-json", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test:
        if args.proof_json is None:
            parser.error("--proof-json is required unless --self-test is used")
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
        print(f"Maintenance release descriptor verified: {descriptor}")
        return 0
    except (ValueError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
