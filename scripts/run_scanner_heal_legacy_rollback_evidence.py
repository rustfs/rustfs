#!/usr/bin/env python3
"""Assemble measured Scanner/Heal R-L legacy rollback release evidence.

This producer consumes one operator-collected measured JSON proof. It packages
legacy source-conflict, migration-gap, and crash-safe source-retirement evidence
into the common release-bundle descriptor shape.
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

import check_test_wiring as wiring

ROOT = Path(__file__).resolve().parents[1]
RL_FIELDS = (
    "legacy_source_conflict_evidence",
    "migration_gap_evidence",
    "crash_safe_source_retirement_evidence",
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
    started_at = timestamp(proof.get("started_at"), "proof.started_at")
    finished_at = timestamp(proof.get("finished_at"), "proof.finished_at")
    wiring.require(
        datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
        >= datetime.fromisoformat(started_at.replace("Z", "+00:00")),
        "proof timestamps are inverted",
    )
    wiring.evidence_string(proof.get("run_id"), "proof.run_id", r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
    wiring.evidence_string(
        proof.get("measurement_window_id"),
        "proof.measurement_window_id",
        r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}",
    )
    wiring.require(proof["measurement_window_id"] != proof["run_id"], "proof must separate run/window identities")
    versions = proof.get("versions")
    wiring.require(
        isinstance(versions, list)
        and len(set(versions)) >= 2
        and all(isinstance(version, str) and re.fullmatch(r"[0-9a-f]{40}", version) for version in versions),
        "proof requires two source revisions",
    )
    wiring.require(source_revision in versions, "proof versions omit tested source revision")
    return proof


def field_from_proof(proof: dict[str, Any], field: str) -> dict[str, Any]:
    value = proof.get(field)
    wiring.require(isinstance(value, dict), f"proof missing {field}")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        wiring.require(value.get(marker) is not True, f"{field} is {marker}")
    if "evidence_type" in value:
        wiring.require(value["evidence_type"] == "measured", f"{field} must be measured")
    if "source_revision" in value:
        wiring.require(value["source_revision"] == proof["source_revision"], f"{field} source revision mismatch")
    if "run_id" in value:
        wiring.require(value["run_id"] == proof["run_id"], f"{field} run_id mismatch")
    if "measurement_window_id" in value:
        wiring.require(
            value["measurement_window_id"] == proof["measurement_window_id"],
            f"{field} measurement window mismatch",
        )
    evidence = {
        **value,
        "versions": proof["versions"],
        "mixed_version_role": wiring.SCANNER_HEAL_RELEASE_MIXED_VERSION_ROLES[("R-L", field)],
    }
    if "crash_points" not in evidence:
        evidence["crash_points"] = proof.get("crash_points")
    wiring.validate_release_bundle_domain_evidence("R-L", field, evidence)
    return evidence


def write_field(out_dir: Path, common: dict[str, Any], field: str, field_evidence: dict[str, Any]) -> dict[str, Any]:
    evidence = {
        **field_evidence,
        **common,
        "evidence_type": "measured",
        "summary": field_evidence.get("summary") or f"Measured Scanner/Heal R-L {field} evidence.",
    }
    artifact = out_dir / "artifacts" / f"R-L-{field}.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": evidence["source_revision"],
        "run_id": evidence["run_id"],
        "measurement_window_id": evidence["measurement_window_id"],
        "gate": "R-L",
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
            "scripts/run_scanner_heal_legacy_rollback_evidence.py",
            "--proof-json",
            "<proof-json>",
        ],
    }
    descriptor = out_dir / "release-bundle-legacy-rollback.json"
    gates = {
        "R-L": {
            "status": "pass",
            "lane": "mixed-version-rollback",
            "evidence_type": "measured",
            "evidence_fields": {
                field: write_field(out_dir, common, field, field_from_proof(proof, field))
                for field in RL_FIELDS
            },
        },
    }
    wiring.write_json(descriptor, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": gates,
    })
    subprocess.check_call([
        sys.executable,
        str(ROOT / "scripts/check_test_wiring.py"),
        "--check-scanner-heal-release-bundle-gate",
        str(descriptor),
        "R-L",
    ], cwd=ROOT)
    return descriptor


def write_self_test_proof(path: Path, source_revision: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    proof = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": f"legacy-rollback-{source_revision[:12]}",
        "measurement_window_id": f"legacy-rollback-window-{source_revision[:12]}",
        "started_at": now.isoformat().replace("+00:00", "Z"),
        "finished_at": now.isoformat().replace("+00:00", "Z"),
        "versions": ["a" * 40, source_revision],
        "crash_points": ["before-successor-manifest", "after-successor-before-retire"],
        "legacy_source_conflict_evidence": {
            "legacy_source_conflict_cases": list(
                wiring.SCANNER_HEAL_RELEASE_RL_REQUIRED_CASES["legacy_source_conflict_evidence"]
            ),
            "source_conflicts_rejected": True,
            "takeover_identity_bound": True,
            "legacy_checksum_gap_rejected": True,
        },
        "migration_gap_evidence": {
            "migration_gap_cases": list(wiring.SCANNER_HEAL_RELEASE_RL_REQUIRED_CASES["migration_gap_evidence"]),
            "migration_gap_closed": True,
            "legacy_sources_fail_closed": True,
            "prior_responsibilities_inherited": True,
        },
        "crash_safe_source_retirement_evidence": {
            "source_retirement_cases": list(
                wiring.SCANNER_HEAL_RELEASE_RL_REQUIRED_CASES["crash_safe_source_retirement_evidence"]
            ),
            "source_retirement_is_crash_safe": True,
            "old_source_retained_until_successor": True,
            "recovered_pending_migration": True,
        },
    }
    wiring.write_json(path, proof)


def run_self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "legacy-rollback-proof.json"
        write_self_test_proof(proof, source_revision)
        descriptor = build_descriptor(parse_args([
            "--proof-json", str(proof),
            "--out-dir", str(root / "out"),
        ]))
        wiring.require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "legacy-rollback-proof.json"
        write_self_test_proof(proof, source_revision)
        payload = wiring.read_json(proof)
        payload["migration_gap_evidence"]["migration_gap_closed"] = False
        wiring.write_json(proof, payload)
        try:
            build_descriptor(parse_args(["--proof-json", str(proof), "--out-dir", str(root / "out")]))
        except ValueError as err:
            wiring.require("migration_gap_closed" in str(err), "wrong self-test failure for R-L proof")
        else:
            raise ValueError("self-test accepted incomplete R-L proof")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "legacy-rollback-proof.json"
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

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "legacy-rollback-proof.json"
        write_self_test_proof(proof, source_revision)
        payload = wiring.read_json(proof)
        payload["versions"] = ["not-a-source-revision".ljust(40, "x"), source_revision]
        wiring.write_json(proof, payload)
        try:
            build_descriptor(parse_args(["--proof-json", str(proof), "--out-dir", str(root / "out")]))
        except ValueError as err:
            wiring.require("source revisions" in str(err), "wrong self-test failure for invalid version")
        else:
            raise ValueError("self-test accepted invalid source revision")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "legacy-rollback-proof.json"
        write_self_test_proof(proof, source_revision)
        payload = wiring.read_json(proof)
        payload["finished_at"] = "2026-09-08T00:00:00Z"
        payload["started_at"] = "2026-09-09T00:00:00Z"
        wiring.write_json(proof, payload)
        try:
            build_descriptor(parse_args(["--proof-json", str(proof), "--out-dir", str(root / "out")]))
        except ValueError as err:
            wiring.require("timestamps are inverted" in str(err), "wrong self-test failure for inverted timestamps")
        else:
            raise ValueError("self-test accepted inverted timestamps")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = root / "legacy-rollback-proof.json"
        write_self_test_proof(proof, source_revision)
        payload = wiring.read_json(proof)
        payload["legacy_source_conflict_evidence"]["run_id"] = "legacy-rollback-different-run"
        wiring.write_json(proof, payload)
        try:
            build_descriptor(parse_args(["--proof-json", str(proof), "--out-dir", str(root / "out")]))
        except ValueError as err:
            wiring.require("run_id mismatch" in str(err), "wrong self-test failure for nested run_id")
        else:
            raise ValueError("self-test accepted nested run_id mismatch")


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
        print(f"Legacy rollback release descriptor verified: {descriptor}")
        return 0
    except (ValueError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
