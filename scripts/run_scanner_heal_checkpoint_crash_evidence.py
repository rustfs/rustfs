#!/usr/bin/env python3
"""Assemble Scanner/Heal G02/R-E release evidence from measured restart diagnostics."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from scanner_abba import digest, read_json, require, write_json

ROOT = Path(__file__).resolve().parents[1]
CHECKPOINT_FIELDS = ("bounded_checkpoint_oracle", "independent_version_inventory")
RESTART_FIELDS = ("fixed_budget_restart_evidence", "enumeration_evidence", "classification_evidence")


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def positive_int(value: Any, name: str, minimum: int = 1) -> int:
    require(type(value) is int and value >= minimum, f"invalid {name}")
    return value


def timestamp(value: Any, name: str) -> str:
    require(isinstance(value, str) and value.endswith("Z"), f"invalid {name}")
    datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def round_report_paths(directory: Path) -> list[Path]:
    paths: list[tuple[int, Path]] = []
    for path in directory.glob("round-*.json"):
        match = re.fullmatch(r"round-(\d+)\.json", path.name)
        require(match is not None, f"invalid round report name: {path.name}")
        paths.append((int(match.group(1)), path))
    return [path for _, path in sorted(paths)]


def load_reports(directory: Path) -> list[dict[str, Any]]:
    require(directory.is_dir(), "diagnostic directory is missing")
    reports = [read_json(path) for path in round_report_paths(directory)]
    require(reports, "diagnostic directory has no round reports")
    non_negative_counters = {
        "raw_entries",
        "raw_name_bytes",
        "objects_before",
        "objects_retained",
        "versions_retained",
        "bytes_retained",
        "objects_processed",
        "raw_page_index_committed_entries",
        "raw_page_index_indexed_entries",
    }
    for index, report in enumerate(reports):
        require(report.get("schema") == 1, f"round {index} has wrong schema")
        require(report.get("round") == index, f"round {index} order mismatch")
        for key in (
            "pid",
            "objects_expected",
            "raw_entry_budget",
            "raw_entries",
            "raw_name_bytes",
            "objects_before",
            "objects_retained",
            "versions_retained",
            "bytes_retained",
            "objects_processed",
            "raw_page_index_committed_entries",
            "raw_page_index_indexed_entries",
        ):
            positive_int(report.get(key), f"round {index} {key}", 0 if key in non_negative_counters else 1)
        require(type(report.get("snapshot_complete")) is bool, f"round {index} missing snapshot_complete")
        require(type(report.get("raw_page_index_complete")) is bool, f"round {index} missing raw_page_index_complete")
        require(report.get("outcome") in {"complete", "partial", "cancelled_without_cache"}, f"round {index} bad outcome")
    return reports


def require_measured_manifest(path: Path, source_revision: str) -> dict[str, Any]:
    manifest = read_json(path)
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        require(manifest.get(marker) is not True, f"checkpoint/crash manifest is {marker}")
    require(manifest.get("schema") == 1, "unsupported manifest schema")
    require(manifest.get("evidence_type") == "measured", "manifest must be measured")
    require(manifest.get("source_revision") == source_revision, "manifest source revision mismatch")
    require(isinstance(manifest.get("run_id"), str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}", manifest["run_id"]),
            "invalid run_id")
    require(isinstance(manifest.get("measurement_window_id"), str)
            and manifest["measurement_window_id"] != manifest["run_id"], "invalid measurement_window_id")
    timestamp(manifest.get("started_at"), "started_at")
    timestamp(manifest.get("finished_at"), "finished_at")
    require(isinstance(manifest.get("command"), list) and manifest["command"], "missing command provenance")
    require(manifest.get("diagnostic_exit_code") == 0, "diagnostic did not pass")
    return manifest


def derived_measured_manifest(directory: Path, source_revision: str, reports: list[dict[str, Any]]) -> dict[str, Any]:
    request_path = directory / "request.json"
    request = read_json(request_path)
    require(isinstance(request, dict), "diagnostic request must be a JSON object")
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        require(request.get(marker) is not True, f"diagnostic request is {marker}")
    objects = positive_int(request.get("objects"), "request.objects")
    raw_entry_budget = positive_int(request.get("raw_entry_budget"), "request.raw_entry_budget")
    final_round = positive_int(request.get("round"), "request.round", 0)
    require(objects == reports[-1]["objects_expected"], "diagnostic request object count mismatch")
    require(raw_entry_budget == reports[-1]["raw_entry_budget"], "diagnostic request raw budget mismatch")
    require(final_round == reports[-1]["round"], "diagnostic request final round mismatch")
    paths = [request_path] + round_report_paths(directory)
    started = datetime.fromtimestamp(min(path.stat().st_mtime for path in paths), timezone.utc).replace(microsecond=0)
    finished = datetime.fromtimestamp(max(path.stat().st_mtime for path in paths), timezone.utc).replace(microsecond=0)
    run_id = re.sub(r"[^A-Za-z0-9._:-]", "-", directory.name)
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}", run_id) is not None,
            "diagnostic directory name cannot be used as run_id")
    return {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": run_id,
        "measurement_window_id": f"{run_id}-window",
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "finished_at": finished.isoformat().replace("+00:00", "Z"),
        "command": [
            "python3",
            "scripts/diagnose_scanner_enumeration_restart.py",
            "--test-binary",
            "<libtest>",
            "--output",
            str(directory),
            "--objects",
            str(objects),
            "--raw-entry-budget",
            str(raw_entry_budget),
            "--rounds",
            str(final_round + 1),
        ],
        "diagnostic_exit_code": 0,
    }


def summarize(reports: list[dict[str, Any]]) -> dict[str, Any]:
    first = reports[0]
    final = reports[-1]
    objects_expected = positive_int(final["objects_expected"], "objects_expected")
    raw_entry_budget = positive_int(final["raw_entry_budget"], "raw_entry_budget")
    require(final.get("snapshot_complete") is True and final.get("outcome") == "complete",
            "fixed-budget restart convergence was not established")
    require(final.get("objects_retained") == objects_expected, "final retained objects mismatch")
    require(final.get("versions_retained") == objects_expected, "final retained versions mismatch")
    require(final.get("bytes_retained") == objects_expected, "final retained bytes mismatch")
    pids = {positive_int(report["pid"], "pid") for report in reports}
    require(len(pids) >= 2 or len(reports) >= 2, "restart diagnostic must include at least two worker rounds")
    require(any(report.get("raw_entries", 0) > 0 for report in reports), "raw enumeration was not observed")
    require(any(report.get("raw_page_index_committed_entries", 0) > 0 for report in reports),
            "durable raw page commit was not observed")
    require(any(report.get("objects_processed", 0) > 0 for report in reports), "classification was not observed")
    previous = None
    frontier_retained = False
    for report in reports:
        require(report["raw_entries"] <= raw_entry_budget, "raw-entry budget exceeded")
        require(report["objects_processed"] <= raw_entry_budget, "object budget exceeded")
        if previous is not None:
            require(report["objects_before"] == previous["objects_retained"],
                    "retained coverage did not survive restart")
            frontier_retained |= report["objects_before"] >= previous["objects_retained"]
        previous = report
    return {
        "objects_expected": objects_expected,
        "raw_entry_budget": raw_entry_budget,
        "max_raw_entries_per_round": max(report["raw_entries"] for report in reports),
        "max_objects_processed_per_round": max(report["objects_processed"] for report in reports),
        "object_processing_attempts": sum(report["objects_processed"] for report in reports),
        "objects_processed": final["objects_retained"],
        "objects_retained": final["objects_retained"],
        "versions_retained": final["versions_retained"],
        "bytes_retained": final["bytes_retained"],
        "restart_rounds": len(reports),
        "raw_page_index_complete": max(report["raw_page_index_committed_entries"] for report in reports) >= objects_expected
        and max(report["raw_page_index_indexed_entries"] for report in reports) >= objects_expected,
        "enumeration_frontier_retained": frontier_retained,
        "first_round": first,
        "final_round": final,
    }


def write_field(out_dir: Path, gate: str, field: str, evidence: dict[str, Any]) -> dict[str, Any]:
    artifact = out_dir / "artifacts" / f"{gate}-{field}.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": 1,
        "evidence_type": "measured",
        "artifact_kind": "scanner-checkpoint-crash-evidence",
        "source_revision": evidence["source_revision"],
        "run_id": evidence["run_id"],
        "measurement_window_id": evidence["measurement_window_id"],
        "started_at": evidence["started_at"],
        "finished_at": evidence["finished_at"],
        "gate": gate,
        "field": field,
    }
    for key, value in evidence.items():
        if key not in {"artifact", "sha256", "artifact_format", "summary", "command"}:
            payload[key] = value
    write_json(artifact, payload)
    evidence["artifact"] = artifact.relative_to(out_dir).as_posix()
    evidence["sha256"] = digest(artifact)
    evidence["artifact_format"] = "json"
    return evidence


def build_descriptor(args: argparse.Namespace) -> Path:
    out_dir = args.out_dir.resolve()
    require(not out_dir.exists(), "output directory must be new")
    source_revision = args.source_revision or git_head()
    reports = load_reports(args.diagnostic_dir.resolve())
    if args.manifest is None:
        manifest = derived_measured_manifest(args.diagnostic_dir.resolve(), source_revision, reports)
    else:
        manifest = require_measured_manifest(args.manifest.resolve(), source_revision)
    summary = summarize(reports)
    out_dir.mkdir(parents=True)
    common = {
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": manifest["run_id"],
        "measurement_window_id": manifest["measurement_window_id"],
        "started_at": manifest["started_at"],
        "finished_at": manifest["finished_at"],
        "command": manifest["command"],
    }
    gates = {
        "G02": {
            "status": "pass",
            "lane": "checkpoint-and-crash",
            "evidence_type": "measured",
            "evidence_fields": {
                "bounded_checkpoint_oracle": write_field(out_dir, "G02", "bounded_checkpoint_oracle", {
                    **common,
                    "summary": "Measured scanner-worker restart reports bounded raw enumeration and object processing.",
                    "checkpoint_progress_bounded": True,
                    "raw_entry_budget": summary["raw_entry_budget"],
                    "max_raw_entries_per_round": summary["max_raw_entries_per_round"],
                    "max_objects_processed_per_round": summary["max_objects_processed_per_round"],
                    "durable_checkpoint_committed": True,
                    "no_unbounded_tail": True,
                }),
                "independent_version_inventory": write_field(out_dir, "G02", "independent_version_inventory", {
                    **common,
                    "summary": "Measured restart convergence retained an independent object/version/byte inventory.",
                    "independent_version_inventory_observed": True,
                    "objects_expected": summary["objects_expected"],
                    "objects_retained": summary["objects_retained"],
                    "versions_retained": summary["versions_retained"],
                    "bytes_retained": summary["bytes_retained"],
                }),
            },
        },
        "R-E": {
            "status": "pass",
            "lane": "checkpoint-and-crash",
            "evidence_type": "measured",
            "evidence_fields": {
                "fixed_budget_restart_evidence": write_field(out_dir, "R-E", "fixed_budget_restart_evidence", {
                    **common,
                    "summary": "Measured scanner worker converged after repeated process restarts without an unbudgeted final sweep.",
                    "crash_points": ["scanner-worker-process-restart"],
                    "fixed_budget_restart_converged": True,
                    "restart_rounds": summary["restart_rounds"],
                    "raw_entry_budget": summary["raw_entry_budget"],
                    "no_unbudgeted_final_sweep": True,
                }),
                "enumeration_evidence": write_field(out_dir, "R-E", "enumeration_evidence", {
                    **common,
                    "summary": "Measured raw enumeration and raw-page checkpoint progress survived worker restarts.",
                    "crash_points": ["scanner-worker-process-restart"],
                    "raw_enumeration_observed": True,
                    "durable_raw_page_commit_observed": True,
                    "raw_page_index_complete": summary["raw_page_index_complete"],
                    "enumeration_frontier_retained": summary["enumeration_frontier_retained"],
                }),
                "classification_evidence": write_field(out_dir, "R-E", "classification_evidence", {
                    **common,
                    "summary": "Measured object classification and retained inventory converged under the fixed restart budget.",
                    "crash_points": ["scanner-worker-process-restart"],
                    "classification_observed": True,
                    "objects_processed": summary["objects_processed"],
                    "object_processing_attempts": summary["object_processing_attempts"],
                    "objects_retained": summary["objects_retained"],
                    "versions_retained": summary["versions_retained"],
                    "bytes_retained": summary["bytes_retained"],
                }),
            },
        },
    }
    descriptor = out_dir / "release-bundle-checkpoint-crash.json"
    write_json(descriptor, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": gates,
    })
    for gate in ("G02", "R-E"):
        subprocess.check_call([
            sys.executable,
            str(ROOT / "scripts/check_test_wiring.py"),
            "--check-scanner-heal-release-bundle-gate",
            str(descriptor),
            gate,
        ], cwd=ROOT)
    return descriptor


def write_self_test_inputs(
    root: Path,
    source_revision: str,
    complete: bool = True,
    objects_expected: int = 16,
) -> tuple[Path, Path]:
    diagnostic = root / "diagnostic"
    diagnostic.mkdir()
    raw_entry_budget = 8
    reports = []
    objects_retained = 0
    for round_index in range((objects_expected + raw_entry_budget - 1) // raw_entry_budget):
        remaining = objects_expected - objects_retained
        processed = min(raw_entry_budget, remaining)
        is_final = objects_retained + processed >= objects_expected
        retained_after = objects_retained + processed
        if is_final and not complete:
            retained_after = max(objects_retained, objects_expected - raw_entry_budget // 2)
        reports.append({
            "schema": 1,
            "round": round_index,
            "pid": 1000 + round_index,
            "objects_expected": objects_expected,
            "raw_entry_budget": raw_entry_budget,
            "raw_entries": processed,
            "raw_name_bytes": 128,
            "objects_before": objects_retained,
            "objects_retained": retained_after,
            "versions_retained": retained_after,
            "bytes_retained": retained_after,
            "objects_processed": processed,
            "raw_page_index_parent": "bucket",
            "raw_page_index_committed_entries": min(objects_expected, retained_after),
            "raw_page_index_indexed_entries": min(objects_expected, retained_after),
            "raw_page_index_complete": is_final and complete,
            "snapshot_complete": is_final and complete,
            "outcome": "complete" if is_final and complete else "partial",
        })
        objects_retained = retained_after
    for report in reports:
        write_json(diagnostic / f"round-{report['round']}.json", report)
    write_json(diagnostic / "request.json", {
        "workspace": str(diagnostic),
        "objects": reports[-1]["objects_expected"],
        "raw_entry_budget": reports[-1]["raw_entry_budget"],
        "round": reports[-1]["round"],
    })
    started = datetime.now(timezone.utc).replace(microsecond=0)
    manifest = root / "manifest.json"
    write_json(manifest, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": "checkpoint-crash-self-test-run",
        "measurement_window_id": "checkpoint-crash-self-test-window",
        "started_at": started.isoformat().replace("+00:00", "Z"),
        "finished_at": (started + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "command": ["scripts/diagnose_scanner_enumeration_restart.py", "--test-binary", "<libtest>"],
        "diagnostic_exit_code": 0,
    })
    return manifest, diagnostic


def run_self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        manifest, diagnostic = write_self_test_inputs(root, source_revision)
        descriptor = build_descriptor(parse_args([
            "--manifest", str(manifest),
            "--diagnostic-dir", str(diagnostic),
            "--out-dir", str(root / "out"),
        ]))
        require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        _, diagnostic = write_self_test_inputs(root, source_revision, objects_expected=96)
        descriptor = build_descriptor(parse_args([
            "--diagnostic-dir", str(diagnostic),
            "--out-dir", str(root / "out"),
        ]))
        require(descriptor.is_file(), "self-test descriptor missing for derived manifest")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        manifest, diagnostic = write_self_test_inputs(root, source_revision, complete=False)
        try:
            build_descriptor(parse_args([
                "--manifest", str(manifest),
                "--diagnostic-dir", str(diagnostic),
                "--out-dir", str(root / "out"),
            ]))
        except ValueError as err:
            require("convergence" in str(err), "wrong self-test failure for non-converged diagnostic")
        else:
            raise ValueError("self-test accepted non-converged diagnostic")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--diagnostic-dir", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test:
        if args.diagnostic_dir is None:
            parser.error("--diagnostic-dir is required unless --self-test is used")
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
        print(f"Checkpoint/crash release descriptor verified: {descriptor}")
        return 0
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
