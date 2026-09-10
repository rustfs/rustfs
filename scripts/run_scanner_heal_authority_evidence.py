#!/usr/bin/env python3
"""Assemble measured Scanner/Heal G01 authority release evidence.

The producer consumes operator-collected measured JSON. It packages root and
quota authority measurements into the common release-bundle descriptor shape
and lets check_test_wiring.py validate G01 without approving the full release.
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


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def timestamp(value: Any, name: str) -> str:
    wiring.require(isinstance(value, str) and value.strip(), f"missing {name}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    wiring.require(parsed.tzinfo is not None, f"{name} must include timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def measured_json(path: Path, source_revision: str, label: str) -> dict[str, Any]:
    payload = wiring.read_json(path.resolve())
    wiring.require(isinstance(payload, dict), f"{label} must be a JSON object")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        wiring.require(payload.get(marker) is not True, f"{label} is {marker}")
    wiring.require(payload.get("schema") == 1, f"{label} schema must be 1")
    wiring.require(payload.get("evidence_type") == "measured", f"{label} must be measured")
    wiring.require(payload.get("source_revision") == source_revision, f"{label} source revision mismatch")
    return payload


def common_evidence(args: argparse.Namespace, source_revision: str) -> dict[str, Any]:
    duration = wiring.evidence_integer(args.duration_seconds, "duration_seconds", 1, 86400)
    started_at = timestamp(args.started_at, "started_at") if args.started_at else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if args.finished_at:
        finished_at = timestamp(args.finished_at, "finished_at")
    else:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        finished_at = (started + timedelta(seconds=duration)).isoformat().replace("+00:00", "Z")
    wiring.require(
        datetime.fromisoformat(finished_at.replace("Z", "+00:00")) >= datetime.fromisoformat(started_at.replace("Z", "+00:00")),
        "authority measurement timestamps are inverted",
    )
    run_id = args.run_id or f"authority-{source_revision[:12]}"
    window_id = args.measurement_window_id or f"authority-window-{source_revision[:12]}"
    wiring.evidence_string(run_id, "run_id", r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
    wiring.evidence_string(window_id, "measurement_window_id", r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
    wiring.require(run_id != window_id, "authority proof must separate run/window identities")
    return {
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": window_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "command": [
            "scripts/run_scanner_heal_authority_evidence.py",
            "--root-authority-json",
            "<root-authority-json>",
            "--quota-authority-json",
            "<quota-authority-json>",
        ],
    }


def validate_root_authority(payload: dict[str, Any]) -> None:
    wiring.release_bundle_exact_strings(
        payload.get("root_authority_cases"),
        wiring.SCANNER_HEAL_RELEASE_G01_ROOT_AUTHORITY_CASES,
        "root_authority_cases",
    )
    wiring.release_bundle_bool_true(payload.get("root_cas_observed"), "root_cas_observed")
    wiring.release_bundle_bool_true(payload.get("root_readback_observed"), "root_readback_observed")
    wiring.release_bundle_bool_true(payload.get("incomplete_root_rejected"), "incomplete_root_rejected")
    wiring.release_bundle_bool_true(payload.get("stale_root_rejected"), "stale_root_rejected")


def validate_quota_authority(payload: dict[str, Any]) -> None:
    wiring.release_bundle_exact_strings(
        payload.get("quota_authority_cases"),
        wiring.SCANNER_HEAL_RELEASE_G01_QUOTA_AUTHORITY_CASES,
        "quota_authority_cases",
    )
    wiring.release_bundle_bool_true(payload.get("quota_floor_readback_observed"), "quota_floor_readback_observed")
    wiring.release_bundle_bool_true(payload.get("over_limit_put_rejected"), "over_limit_put_rejected")
    wiring.release_bundle_bool_true(payload.get("rejected_object_invisible"), "rejected_object_invisible")
    wiring.release_bundle_bool_true(payload.get("quota_fails_closed_without_authority"), "quota_fails_closed_without_authority")


def write_field(out_dir: Path, field: str, common: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    evidence = {
        **payload,
        **common,
        "evidence_type": "measured",
        "summary": payload.get("summary") or f"Measured Scanner/Heal G01.{field} evidence.",
    }
    artifact = out_dir / "artifacts" / f"G01-{field}.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact_payload = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": evidence["source_revision"],
        "run_id": evidence["run_id"],
        "measurement_window_id": evidence["measurement_window_id"],
        "gate": "G01",
        "field": field,
    }
    for key, value in evidence.items():
        if key not in {"artifact", "sha256", "artifact_format", "summary", "started_at", "finished_at", "command"}:
            artifact_payload[key] = value
    wiring.write_json(artifact, artifact_payload)
    evidence["artifact"] = artifact.relative_to(out_dir).as_posix()
    evidence["sha256"] = wiring.digest(artifact)
    evidence["artifact_format"] = "json"
    return evidence


def build_descriptor(args: argparse.Namespace) -> Path:
    out_dir = args.out_dir.resolve()
    wiring.require(not out_dir.exists(), "output directory must be new")
    source_revision = args.source_revision or git_head()
    root_authority = measured_json(args.root_authority_json, source_revision, "root authority artifact")
    quota_authority = measured_json(args.quota_authority_json, source_revision, "quota authority artifact")
    validate_root_authority(root_authority)
    validate_quota_authority(quota_authority)
    out_dir.mkdir(parents=True)
    common = common_evidence(args, source_revision)
    fields = {
        "root_authority_evidence": write_field(out_dir, "root_authority_evidence", common, root_authority),
        "quota_authority_evidence": write_field(out_dir, "quota_authority_evidence", common, quota_authority),
    }
    descriptor = out_dir / "release-bundle-authority.json"
    wiring.write_json(descriptor, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": {
            "G01": {
                "status": "pass",
                "lane": "authority-coverage",
                "evidence_type": "measured",
                "evidence_fields": fields,
            },
        },
    })
    subprocess.check_call([
        sys.executable,
        str(ROOT / "scripts/check_test_wiring.py"),
        "--check-scanner-heal-release-bundle-gate",
        str(descriptor),
        "G01",
    ], cwd=ROOT)
    return descriptor


def write_self_test_inputs(root: Path, source_revision: str) -> tuple[Path, Path]:
    root_json = root / "root-authority.json"
    quota_json = root / "quota-authority.json"
    wiring.write_json(root_json, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "root_authority_cases": list(wiring.SCANNER_HEAL_RELEASE_G01_ROOT_AUTHORITY_CASES),
        "root_cas_observed": True,
        "root_readback_observed": True,
        "incomplete_root_rejected": True,
        "stale_root_rejected": True,
    })
    wiring.write_json(quota_json, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "quota_authority_cases": list(wiring.SCANNER_HEAL_RELEASE_G01_QUOTA_AUTHORITY_CASES),
        "quota_floor_readback_observed": True,
        "over_limit_put_rejected": True,
        "rejected_object_invisible": True,
        "quota_fails_closed_without_authority": True,
    })
    return root_json, quota_json


def run_self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        root_json, quota_json = write_self_test_inputs(root, source_revision)
        descriptor = build_descriptor(parse_args([
            "--root-authority-json", str(root_json),
            "--quota-authority-json", str(quota_json),
            "--out-dir", str(root / "out"),
            "--duration-seconds", "60",
        ]))
        wiring.require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        root_json, quota_json = write_self_test_inputs(root, source_revision)
        payload = wiring.read_json(root_json)
        payload["synthetic"] = True
        wiring.write_json(root_json, payload)
        try:
            build_descriptor(parse_args([
                "--root-authority-json", str(root_json),
                "--quota-authority-json", str(quota_json),
                "--out-dir", str(root / "out"),
            ]))
        except ValueError as err:
            wiring.require("synthetic" in str(err), "wrong self-test failure for synthetic root authority")
        else:
            raise ValueError("self-test accepted synthetic root authority")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        root_json, quota_json = write_self_test_inputs(root, source_revision)
        payload = wiring.read_json(quota_json)
        payload["quota_authority_cases"] = payload["quota_authority_cases"][:-1]
        wiring.write_json(quota_json, payload)
        try:
            build_descriptor(parse_args([
                "--root-authority-json", str(root_json),
                "--quota-authority-json", str(quota_json),
                "--out-dir", str(root / "out"),
            ]))
        except ValueError as err:
            wiring.require("missing cases" in str(err), "wrong self-test failure for incomplete quota authority")
        else:
            raise ValueError("self-test accepted incomplete quota authority")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root-authority-json", type=Path)
    parser.add_argument("--quota-authority-json", type=Path)
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
        if args.root_authority_json is None:
            parser.error("--root-authority-json is required unless --self-test is used")
        if args.quota_authority_json is None:
            parser.error("--quota-authority-json is required unless --self-test is used")
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
        print(f"Authority release descriptor verified: {descriptor}")
        return 0
    except (ValueError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
