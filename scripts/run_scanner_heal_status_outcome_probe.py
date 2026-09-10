#!/usr/bin/env python3
"""Collect Scanner/Heal status-and-outcome raw evidence from live observations.

This helper normalizes operator-collected live observation JSON into the three
measured raw artifacts consumed by run_scanner_heal_status_outcome_evidence.py.
It does not approve a release bundle by itself.
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


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def timestamp(value: Any, name: str) -> str:
    wiring.require(isinstance(value, str) and value.endswith("Z"), f"invalid {name}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    wiring.require(parsed.tzinfo is not None, f"{name} must include timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def identity_string(value: Any, name: str) -> str:
    wiring.require(
        isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}", value),
        f"invalid {name}",
    )
    return value


def measured_observation(path: Path, source_revision: str) -> dict[str, Any]:
    payload = wiring.read_json(path.resolve())
    wiring.require(isinstance(payload, dict), "observation must be a JSON object")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        wiring.require(payload.get(marker) is not True, f"observation is {marker}")
    wiring.require(payload.get("schema") == 1, "observation schema must be 1")
    wiring.require(payload.get("evidence_type") == "measured", "observation must be measured")
    wiring.require(payload.get("source_revision") == source_revision, "observation source revision mismatch")
    run_id = identity_string(payload.get("run_id"), "run_id")
    window_id = identity_string(payload.get("measurement_window_id"), "measurement_window_id")
    wiring.require(run_id != window_id, "run/window identities must differ")
    started = timestamp(payload.get("started_at"), "started_at")
    finished = timestamp(payload.get("finished_at"), "finished_at")
    wiring.require(
        datetime.fromisoformat(finished.replace("Z", "+00:00"))
        >= datetime.fromisoformat(started.replace("Z", "+00:00")),
        "finished_at precedes started_at",
    )
    wiring.require(isinstance(payload.get("command"), list) and payload["command"], "missing command provenance")
    return payload


def object_items(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    items = payload.get(key)
    wiring.require(isinstance(items, list) and items, f"missing {key}")
    for index, item in enumerate(items):
        wiring.require(isinstance(item, dict), f"{key}[{index}] must be an object")
    return items


def case_map(items: list[dict[str, Any]], key: str, expected: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    observed: dict[str, dict[str, Any]] = {}
    for item in items:
        case = item.get("case")
        wiring.require(isinstance(case, str) and case, f"{key} item missing case")
        wiring.require(case not in observed, f"{key} duplicate case: {case}")
        observed[case] = item
    missing = [case for case in expected if case not in observed]
    unknown = [case for case in observed if case not in expected]
    wiring.require(not missing, f"{key} missing cases: {', '.join(missing)}")
    wiring.require(not unknown, f"{key} unknown cases: {', '.join(unknown)}")
    return observed


def bool_true(value: Any, name: str) -> None:
    wiring.release_bundle_bool_true(value, name)


def status_outcome(payload: dict[str, Any], common: dict[str, Any]) -> dict[str, Any]:
    outcome_items = case_map(
        object_items(payload, "status_outcomes"),
        "status_outcomes",
        wiring.SCANNER_HEAL_RELEASE_G05_PER_OBJECT_OUTCOME_CASES,
    )
    counts = {"repaired": 0, "healthy": 0, "skipped": 0, "failed": 0}
    for case, item in outcome_items.items():
        outcome = item.get("outcome")
        wiring.require(outcome in counts, f"status_outcomes.{case} has invalid outcome")
        bool_true(item.get("status_matches_object_oracle"), f"status_outcomes.{case}.status_matches_object_oracle")
        counts[outcome] += 1
    for outcome, count in counts.items():
        wiring.require(count > 0, f"missing measured {outcome} outcome")

    retention = case_map(
        object_items(payload, "terminal_retention_samples"),
        "terminal_retention_samples",
        wiring.SCANNER_HEAL_RELEASE_G05_TERMINAL_RETENTION_CASES,
    )
    window = wiring.evidence_integer(
        payload.get("terminal_retention_window_seconds"),
        "terminal_retention_window_seconds",
        1,
        86400,
    )
    max_age = 0
    pruned = 0
    for case, item in retention.items():
        bool_true(item.get("retained_until_window"), f"terminal_retention_samples.{case}.retained_until_window")
        max_age = max(
            max_age,
            wiring.evidence_integer(
                item.get("max_age_seconds"),
                f"terminal_retention_samples.{case}.max_age_seconds",
                0,
                86400,
            ),
        )
        if item.get("pruned_after_window") is True:
            pruned += 1
    wiring.require(max_age <= window, "terminal retention max age exceeds window")
    wiring.require(pruned > 0, "no terminal records were pruned after the retention window")

    return {
        **common,
        "per_object_outcome_cases": list(wiring.SCANNER_HEAL_RELEASE_G05_PER_OBJECT_OUTCOME_CASES),
        "outcome_counts": counts,
        "status_matches_object_oracle": True,
        "terminal_retention_cases": list(wiring.SCANNER_HEAL_RELEASE_G05_TERMINAL_RETENTION_CASES),
        "terminal_retention_window_seconds": window,
        "max_terminal_record_age_seconds": max_age,
        "terminal_records_pruned_after_window": pruned,
    }


def status_compat(payload: dict[str, Any], common: dict[str, Any]) -> dict[str, Any]:
    status = case_map(
        object_items(payload, "status_samples"),
        "status_samples",
        wiring.SCANNER_HEAL_RELEASE_G06_CONCURRENT_STATUS_CASES,
    )
    status_samples = 0
    degraded = False
    for case, item in status.items():
        bool_true(item.get("http_success"), f"status_samples.{case}.http_success")
        status_samples += wiring.evidence_integer(item.get("samples"), f"status_samples.{case}.samples", 1, 2**31 - 1)
        degraded = degraded or item.get("degraded") is True

    legacy = case_map(
        object_items(payload, "legacy_client_samples"),
        "legacy_client_samples",
        wiring.SCANNER_HEAL_RELEASE_G06_LEGACY_CLIENT_CASES,
    )
    for case, item in legacy.items():
        bool_true(item.get("accepted"), f"legacy_client_samples.{case}.accepted")
    rustfs_and_minio = (
        legacy["rustfs-admin-v3-background-heal-status"].get("path_compatible") is True
        and legacy["minio-admin-v3-background-heal-status"].get("path_compatible") is True
    )
    empty_body = legacy["heal-client-token-empty-body"].get("empty_body_accepted") is True
    bool_true(rustfs_and_minio, "rustfs and minio status path compatibility")
    bool_true(empty_body, "empty body heal status request")

    truncation = case_map(
        object_items(payload, "truncation_samples"),
        "truncation_samples",
        wiring.SCANNER_HEAL_RELEASE_G06_TRUNCATION_CASES,
    )
    max_payload = 1
    for case, item in truncation.items():
        bool_true(item.get("rejected"), f"truncation_samples.{case}.rejected")
        max_payload = max(
            max_payload,
            wiring.evidence_integer(item.get("payload_bytes"), f"truncation_samples.{case}.payload_bytes", 1, 2**20),
        )

    return {
        **common,
        "concurrent_status_cases": list(wiring.SCANNER_HEAL_RELEASE_G06_CONCURRENT_STATUS_CASES),
        "status_samples": status_samples,
        "all_status_responses_http_success": True,
        "partial_status_reports_degraded": degraded,
        "legacy_client_cases": list(wiring.SCANNER_HEAL_RELEASE_G06_LEGACY_CLIENT_CASES),
        "rustfs_and_minio_paths_compatible": rustfs_and_minio,
        "empty_body_status_requests_accepted": empty_body,
        "truncation_cases": list(wiring.SCANNER_HEAL_RELEASE_G06_TRUNCATION_CASES),
        "truncated_payloads_rejected": True,
        "max_status_payload_bytes": max_payload,
    }


def disposition(payload: dict[str, Any], common: dict[str, Any]) -> dict[str, Any]:
    managers = case_map(
        object_items(payload, "manager_dispositions"),
        "manager_dispositions",
        wiring.SCANNER_HEAL_RELEASE_RD_MANAGER_CASES,
    )
    for case, item in managers.items():
        bool_true(item.get("terminal"), f"manager_dispositions.{case}.terminal")

    events = case_map(
        object_items(payload, "event_dispositions"),
        "event_dispositions",
        wiring.SCANNER_HEAL_RELEASE_RD_EVENT_CASES,
    )
    for case, item in events.items():
        bool_true(item.get("correlates_to_manager"), f"event_dispositions.{case}.correlates_to_manager")

    ledgers = case_map(
        object_items(payload, "ledger_dispositions"),
        "ledger_dispositions",
        wiring.SCANNER_HEAL_RELEASE_RD_LEDGER_CASES,
    )
    for case, item in ledgers.items():
        bool_true(item.get("correlates_to_events"), f"ledger_dispositions.{case}.correlates_to_events")
        bool_true(item.get("replay_preserves_terminal"), f"ledger_dispositions.{case}.replay_preserves_terminal")

    grace = case_map(
        object_items(payload, "grace_samples"),
        "grace_samples",
        wiring.SCANNER_HEAL_RELEASE_RD_GRACE_CASES,
    )
    grace_window = wiring.evidence_integer(payload.get("grace_window_seconds"), "grace_window_seconds", 1, 86400)
    retained = False
    pruned = False
    for item in grace.values():
        retained = retained or item.get("retention_observed") is True
        pruned = pruned or item.get("expiry_pruned_terminal_records") is True
    bool_true(retained, "grace retention observed")
    bool_true(pruned, "grace expiry pruned terminal records")

    return {
        **common,
        "manager_disposition_cases": list(wiring.SCANNER_HEAL_RELEASE_RD_MANAGER_CASES),
        "manager_dispositions_are_terminal": True,
        "event_disposition_cases": list(wiring.SCANNER_HEAL_RELEASE_RD_EVENT_CASES),
        "events_correlate_to_manager_dispositions": True,
        "ledger_disposition_cases": list(wiring.SCANNER_HEAL_RELEASE_RD_LEDGER_CASES),
        "ledger_correlates_to_events": True,
        "ledger_replay_preserves_terminal_disposition": True,
        "grace_cases": list(wiring.SCANNER_HEAL_RELEASE_RD_GRACE_CASES),
        "grace_window_seconds": grace_window,
        "grace_retention_observed": retained,
        "grace_expiry_pruned_terminal_records": pruned,
    }


def common_raw(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": payload["source_revision"],
        "run_id": payload["run_id"],
        "measurement_window_id": payload["measurement_window_id"],
        "started_at": payload["started_at"],
        "finished_at": payload["finished_at"],
        "command": payload["command"],
    }


def collect(args: argparse.Namespace) -> tuple[Path, Path, Path]:
    out_dir = args.out_dir.resolve()
    wiring.require(not out_dir.exists(), "output directory must be new")
    source_revision = args.source_revision or git_head()
    observed = measured_observation(args.observations_json, source_revision)
    common = common_raw(observed)
    out_dir.mkdir(parents=True)
    status_outcome_path = out_dir / "status-outcome.json"
    status_compat_path = out_dir / "status-compat.json"
    disposition_path = out_dir / "disposition.json"
    wiring.write_json(status_outcome_path, status_outcome(observed, common))
    wiring.write_json(status_compat_path, status_compat(observed, common))
    wiring.write_json(disposition_path, disposition(observed, common))
    return status_outcome_path, status_compat_path, disposition_path


def write_self_test_observation(path: Path, source_revision: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": f"status-probe-{source_revision[:12]}",
        "measurement_window_id": f"status-probe-window-{source_revision[:12]}",
        "started_at": now,
        "finished_at": now,
        "command": ["scripts/run_scanner_heal_status_outcome_probe.py", "--observations-json", "<live-observations>"],
        "terminal_retention_window_seconds": 3600,
        "grace_window_seconds": 300,
        "status_outcomes": [
            {"case": "object-repaired", "outcome": "repaired", "status_matches_object_oracle": True},
            {"case": "object-already-healthy", "outcome": "healthy", "status_matches_object_oracle": True},
            {"case": "object-skipped-by-policy", "outcome": "skipped", "status_matches_object_oracle": True},
            {"case": "object-failed-and-retained", "outcome": "failed", "status_matches_object_oracle": True},
        ],
        "terminal_retention_samples": [
            {"case": "finished-retained-until-window", "retained_until_window": True, "max_age_seconds": 1200},
            {"case": "failed-retained-until-window", "retained_until_window": True, "max_age_seconds": 1300},
            {"case": "canceled-retained-until-window", "retained_until_window": True, "max_age_seconds": 1400},
            {
                "case": "expired-terminal-pruned-after-window",
                "retained_until_window": True,
                "max_age_seconds": 3599,
                "pruned_after_window": True,
            },
        ],
        "status_samples": [
            {"case": "status-during-admin-heal", "samples": 2, "http_success": True},
            {"case": "status-during-background-heal", "samples": 2, "http_success": True},
            {"case": "status-while-peer-down", "samples": 2, "http_success": True, "degraded": True},
            {"case": "status-after-peer-rejoin", "samples": 2, "http_success": True},
        ],
        "legacy_client_samples": [
            {"case": "rustfs-admin-v3-background-heal-status", "accepted": True, "path_compatible": True},
            {"case": "minio-admin-v3-background-heal-status", "accepted": True, "path_compatible": True},
            {"case": "heal-client-token-empty-body", "accepted": True, "empty_body_accepted": True},
            {"case": "node-heal-status-v1-wire", "accepted": True},
        ],
        "truncation_samples": [
            {"case": "oversize-node-status-reject", "rejected": True, "payload_bytes": 1048576},
            {"case": "truncated-node-status-reject", "rejected": True, "payload_bytes": 4096},
            {"case": "trailing-data-node-status-reject", "rejected": True, "payload_bytes": 4096},
        ],
        "manager_dispositions": [
            {"case": "accepted", "terminal": True},
            {"case": "coalesced-duplicate", "terminal": True},
            {"case": "rejected-policy", "terminal": True},
            {"case": "terminal-retained", "terminal": True},
        ],
        "event_dispositions": [
            {"case": "event-repaired", "correlates_to_manager": True},
            {"case": "event-failed", "correlates_to_manager": True},
            {"case": "event-skipped", "correlates_to_manager": True},
            {"case": "event-grace-retained", "correlates_to_manager": True},
        ],
        "ledger_dispositions": [
            {"case": "ledger-recorded", "correlates_to_events": True, "replay_preserves_terminal": True},
            {"case": "ledger-replayed", "correlates_to_events": True, "replay_preserves_terminal": True},
            {"case": "ledger-discharged", "correlates_to_events": True, "replay_preserves_terminal": True},
            {"case": "ledger-pruned-after-grace", "correlates_to_events": True, "replay_preserves_terminal": True},
        ],
        "grace_samples": [
            {"case": "grace-open-retains-disposition", "retention_observed": True},
            {"case": "grace-expired-prunes-terminal", "expiry_pruned_terminal_records": True},
            {"case": "restart-preserves-grace-clock", "retention_observed": True},
        ],
    }
    wiring.write_json(path, payload)


def run_self_test() -> None:
    import tempfile

    source_revision = git_head()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        observation = root / "observation.json"
        write_self_test_observation(observation, source_revision)
        raw_paths = collect(parse_args([
            "--observations-json",
            str(observation),
            "--out-dir",
            str(root / "raw"),
        ]))
        descriptor = root / "descriptor"
        subprocess.check_call([
            sys.executable,
            str(ROOT / "scripts/run_scanner_heal_status_outcome_evidence.py"),
            "--status-outcome-json",
            str(raw_paths[0]),
            "--status-compat-json",
            str(raw_paths[1]),
            "--disposition-json",
            str(raw_paths[2]),
            "--out-dir",
            str(descriptor),
        ], cwd=ROOT)

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        observation = root / "observation.json"
        write_self_test_observation(observation, source_revision)
        payload = wiring.read_json(observation)
        payload["status_outcomes"].pop()
        wiring.write_json(observation, payload)
        try:
            collect(parse_args(["--observations-json", str(observation), "--out-dir", str(root / "raw")]))
        except ValueError as err:
            wiring.require("status_outcomes missing cases" in str(err), "wrong self-test failure for missing outcome")
        else:
            raise ValueError("self-test accepted incomplete status outcome observations")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        observation = root / "observation.json"
        write_self_test_observation(observation, source_revision)
        payload = wiring.read_json(observation)
        payload["synthetic"] = True
        wiring.write_json(observation, payload)
        try:
            collect(parse_args(["--observations-json", str(observation), "--out-dir", str(root / "raw")]))
        except ValueError as err:
            wiring.require("observation is synthetic" in str(err), "wrong self-test failure for synthetic observation")
        else:
            raise ValueError("self-test accepted synthetic status/outcome observations")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        observation = root / "observation.json"
        write_self_test_observation(observation, source_revision)
        payload = wiring.read_json(observation)
        payload["measurement_window_id"] = payload["run_id"]
        wiring.write_json(observation, payload)
        try:
            collect(parse_args(["--observations-json", str(observation), "--out-dir", str(root / "raw")]))
        except ValueError as err:
            wiring.require("run/window identities must differ" in str(err), "wrong self-test failure for identity reuse")
        else:
            raise ValueError("self-test accepted reused run/window identities")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations-json", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test:
        if args.observations_json is None:
            parser.error("--observations-json is required unless --self-test is used")
        if args.out_dir is None:
            parser.error("--out-dir is required unless --self-test is used")
    return args


def main() -> None:
    args = parse_args()
    if args.self_test:
        run_self_test()
        return
    paths = collect(args)
    json.dump(
        {
            "status_outcome_json": str(paths[0]),
            "status_compat_json": str(paths[1]),
            "disposition_json": str(paths[2]),
        },
        sys.stdout,
        indent=2,
        allow_nan=False,
    )
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
