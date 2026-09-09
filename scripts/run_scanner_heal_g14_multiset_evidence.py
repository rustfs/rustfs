#!/usr/bin/env python3
"""Assemble measured Scanner/Heal G14 multi-set/multi-pool release evidence.

The input proof must come from one same-window distributed measurement that
already observed EC8+4, at least two sets, at least two pools, and distributed
segment invalidation. This script packages that proof into the release-bundle
field shape enforced by check_test_wiring.py.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from scanner_abba import digest, read_json, require, write_json

ROOT = Path(__file__).resolve().parents[1]
G14_FIELDS = (
    "same_window_field_evidence",
    "ec8_4_evidence",
    "multi_set_evidence",
    "multi_pool_evidence",
    "distributed_segment_invalidation_evidence",
)


def git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_from_epoch(value: Any, name: str) -> str:
    require(type(value) in (int, float) and value > 0, f"invalid {name}")
    return datetime.fromtimestamp(float(value), timezone.utc).isoformat().replace("+00:00", "Z")


def positive_int(value: Any, name: str, minimum: int = 1) -> int:
    require(type(value) is int and value >= minimum, f"invalid {name}")
    return value


def parse_case_dir_arg(value: str) -> tuple[str | None, Path]:
    if "=" in value:
        case_id, raw_path = value.split("=", 1)
        require(bool(case_id.strip()), "case directory case id is empty")
        return case_id.strip(), Path(raw_path).expanduser().resolve()
    return None, Path(value).expanduser().resolve()


def load_proof(path: Path, source_revision: str) -> dict[str, Any]:
    proof = read_json(path)
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        require(proof.get(marker) is not True, f"G14 proof is {marker}")
    require(proof.get("schema") == 1, "unsupported G14 proof schema")
    require(proof.get("evidence_type") == "measured", "G14 proof must be measured")
    require(proof.get("source_revision") == source_revision, "G14 proof source revision mismatch")
    require(isinstance(proof.get("run_id"), str) and len(proof["run_id"]) >= 8, "missing G14 proof run_id")
    require(isinstance(proof.get("measurement_window_id"), str) and len(proof["measurement_window_id"]) >= 8,
            "missing G14 measurement window")
    require(proof["run_id"] != proof["measurement_window_id"], "G14 run/window identities must differ")
    require(isinstance(proof.get("command"), list) and proof["command"], "missing G14 command provenance")
    topology = proof.get("topology")
    require(isinstance(topology, dict), "G14 proof missing topology")
    require(topology.get("erasure") == "EC8+4", "G14 proof must record EC8+4")
    positive_int(topology.get("nodes"), "topology.nodes", 3)
    positive_int(topology.get("drives_per_node"), "topology.drives_per_node", 4)
    positive_int(proof.get("sets"), "sets", 2)
    positive_int(proof.get("pools"), "pools", 2)
    require(proof.get("invalidation_domain") == "distributed-ec", "G14 proof must be distributed EC invalidation")
    require(proof.get("distributed_ec_invalidation") is True, "G14 proof missing peer invalidation")
    positive_int(proof.get("peer_count"), "peer_count", 3)
    require(proof.get("same_window_remote_proof") is True, "G14 proof missing same-window remote proof")
    require(proof.get("all_peers_bound_to_generation_window") is True,
            "G14 proof missing peer generation-window binding")
    samples = proof.get("case_evidence")
    require(isinstance(samples, list) and samples, "G14 proof must reference measured case evidence")
    for index, sample in enumerate(samples):
        require(isinstance(sample, dict), f"G14 case evidence {index} must be an object")
        require(isinstance(sample.get("case"), str) and sample["case"].strip(), f"G14 case evidence {index} missing case")
        require(isinstance(sample.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", sample["sha256"]),
                f"G14 case evidence {index} missing sha256")
        if "source_revision" in sample:
            require(sample["source_revision"] == source_revision, f"G14 case evidence {index} source revision mismatch")
        if "measurement_window_id" in sample:
            require(sample["measurement_window_id"] == proof["measurement_window_id"],
                    f"G14 case evidence {index} measurement window mismatch")
    return proof


def load_case_directory(raw_value: str, source_revision: str) -> dict[str, Any]:
    expected_case, directory = parse_case_dir_arg(raw_value)
    require(directory.is_dir(), f"G14 case directory is missing: {directory}")
    run = read_json(directory / "run.json")
    execution = read_json(directory / "execution.json")
    require(run.get("schema") == 1, "G14 case run schema mismatch")
    require(isinstance(run.get("run_id"), str) and re.fullmatch(r"[0-9a-f]{32}", run["run_id"]),
            "invalid G14 case run id")
    require(run.get("source_revision") == source_revision, "G14 case source revision mismatch")
    require(execution.get("run_id") == run["run_id"], "G14 case execution belongs to another run")
    require(execution.get("exit_code") == 0, "G14 case execution did not pass")
    artifacts = execution.get("artifacts")
    require(isinstance(artifacts, dict), "G14 case execution missing artifacts")
    started_at = iso_from_epoch(run.get("started_at"), "case started_at")
    finished_at = iso_from_epoch(execution.get("finished_at"), "case finished_at")

    oracle_items = []
    for name, expected_sha in sorted(artifacts.items()):
        if not name.endswith(".json") or name in {"run.json", "execution.json", "release-status.json"}:
            continue
        path = directory / name
        if not path.is_file():
            continue
        require(isinstance(expected_sha, str) and re.fullmatch(r"[0-9a-f]{64}", expected_sha),
                f"G14 case artifact {name} has an invalid hash")
        require(digest(path) == expected_sha, f"G14 case artifact hash mismatch: {name}")
        item = read_json(path)
        if item.get("schema") == 1 and item.get("case") and item.get("run_id") == run["run_id"]:
            oracle_items.append((name, path, expected_sha, item))
    require(len(oracle_items) == 1, "G14 case directory must contain exactly one case oracle")
    name, path, expected_sha, oracle = oracle_items[0]
    if expected_case is not None:
        require(oracle.get("case") == expected_case, "G14 case directory case id mismatch")
    require(oracle.get("source_revision") == source_revision, "G14 oracle source revision mismatch")
    require(oracle.get("evidence") in {"process-restart", "process-crash-restart"}, "G14 oracle has wrong evidence type")
    topology = oracle.get("topology")
    require(isinstance(topology, dict), "G14 oracle missing topology")
    positive_int(topology.get("nodes"), "oracle topology.nodes", 3)
    positive_int(topology.get("drives_per_node"), "oracle topology.drives_per_node", 4)
    require(oracle.get("erasure_set_drive_count") == 12, "G14 oracle must use EC8+4 set width")
    positive_int(oracle.get("sets"), "oracle sets", 1)
    positive_int(oracle.get("pools"), "oracle pools", 1)
    if oracle.get("sets", 1) > 1 or oracle.get("pools", 1) > 1:
        require(oracle.get("distributed_ec_invalidation") is True, "G14 oracle missing distributed invalidation")
        positive_int(oracle.get("peer_count"), "oracle peer_count", 3)
        require(oracle.get("same_window_remote_proof") is True, "G14 oracle missing same-window remote proof")
        require(oracle.get("all_peers_bound_to_generation_window") is True,
                "G14 oracle missing peer generation-window binding")
    return {
        "case": oracle["case"],
        "artifact_name": name,
        "artifact_path": path,
        "sha256": expected_sha,
        "run_id": run["run_id"],
        "started_at": started_at,
        "finished_at": finished_at,
        "oracle": oracle,
    }


def copy_case_artifacts(out_dir: Path, records: list[dict[str, Any]], window_id: str,
                        source_revision: str) -> list[dict[str, Any]]:
    case_dir = out_dir / "artifacts" / "cases"
    case_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for record in records:
        target = case_dir / record["artifact_name"]
        if target.exists():
            target = case_dir / f"{record['case']}-{record['artifact_name']}"
        shutil.copyfile(record["artifact_path"], target)
        copied.append({
            "case": record["case"],
            "artifact": target.relative_to(out_dir).as_posix(),
            "sha256": digest(target),
            "source_revision": source_revision,
            "measurement_window_id": window_id,
        })
    return copied


def proof_from_case_directories(raw_values: list[str], out_dir: Path, source_revision: str) -> dict[str, Any]:
    require(raw_values, "missing G14 case directories")
    records = [load_case_directory(value, source_revision) for value in raw_values]
    covering = [
        record for record in records
        if record["oracle"].get("erasure_set_drive_count") == 12
        and record["oracle"].get("sets", 0) >= 2
        and record["oracle"].get("pools", 0) >= 2
        and record["oracle"].get("distributed_ec_invalidation") is True
    ]
    require(covering, "G14 case evidence must include one same-window EC8+4 multi-set/multi-pool proof")
    selected = covering[0]
    oracle = selected["oracle"]
    window_id = f"g14-case-window-{selected['run_id']}"
    return {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": selected["run_id"],
        "measurement_window_id": window_id,
        "started_at": selected["started_at"],
        "finished_at": selected["finished_at"],
        "command": ["scripts/run_scanner_heal_g14_multiset_evidence.py", "--case-dir", "<case=dir>"],
        "summary": "Measured G14 descriptor assembled from Scanner/Heal e2e case evidence.",
        "topology": {
            "erasure": "EC8+4",
            "nodes": oracle["topology"]["nodes"],
            "drives_per_node": oracle["topology"]["drives_per_node"],
        },
        "sets": oracle["sets"],
        "pools": oracle["pools"],
        "invalidation_domain": "distributed-ec",
        "distributed_ec_invalidation": oracle["distributed_ec_invalidation"],
        "peer_count": oracle["peer_count"],
        "same_window_remote_proof": oracle["same_window_remote_proof"],
        "all_peers_bound_to_generation_window": oracle["all_peers_bound_to_generation_window"],
        "case_evidence": copy_case_artifacts(out_dir, [selected], window_id, source_revision),
    }


def write_field(out_dir: Path, field: str, proof: dict[str, Any], source_revision: str) -> dict[str, Any]:
    started_at = proof.get("started_at") or utc_now()
    finished_at = proof.get("finished_at") or started_at
    evidence: dict[str, Any] = {
        "artifact": "",
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": proof["run_id"],
        "measurement_window_id": proof["measurement_window_id"],
        "started_at": started_at,
        "finished_at": finished_at,
        "command": proof["command"],
        "artifact_format": "json",
        "summary": proof.get("summary") or "Measured G14 same-window EC8+4 multi-set/multi-pool proof.",
    }
    if field == "same_window_field_evidence":
        evidence["same_window_fields"] = [item for item in G14_FIELDS if item != field]
    elif field == "ec8_4_evidence":
        evidence["topology"] = proof["topology"]
    elif field == "multi_set_evidence":
        evidence["sets"] = proof["sets"]
    elif field == "multi_pool_evidence":
        evidence["pools"] = proof["pools"]
    elif field == "distributed_segment_invalidation_evidence":
        for key in (
            "invalidation_domain",
            "distributed_ec_invalidation",
            "peer_count",
            "same_window_remote_proof",
            "all_peers_bound_to_generation_window",
        ):
            evidence[key] = proof[key]
    artifact = out_dir / "artifacts" / f"G14-{field}.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": proof["run_id"],
        "measurement_window_id": proof["measurement_window_id"],
        "gate": "G14",
        "field": field,
        "case_evidence": proof["case_evidence"],
    }
    for key, value in evidence.items():
        if key not in {"artifact", "sha256", "artifact_format", "summary", "started_at", "finished_at", "command"}:
            payload[key] = value
    write_json(artifact, payload)
    evidence["artifact"] = artifact.relative_to(out_dir).as_posix()
    evidence["sha256"] = digest(artifact)
    return evidence


def build_descriptor(args: argparse.Namespace) -> Path:
    out_dir = args.out_dir.resolve()
    require(not out_dir.exists(), "output directory must be new")
    source_revision = args.source_revision or git_head()
    out_dir.mkdir(parents=True)
    if args.proof_json is not None:
        proof = load_proof(args.proof_json.resolve(), source_revision)
    else:
        proof = proof_from_case_directories(args.case_dir, out_dir, source_revision)
    fields = {field: write_field(out_dir, field, proof, source_revision) for field in G14_FIELDS}
    descriptor = out_dir / "release-bundle-g14.json"
    write_json(descriptor, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": {
            "G14": {
                "status": "pass",
                "lane": "ec8-4-multiset",
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
        "G14",
    ], cwd=ROOT)
    return descriptor


def write_self_test_proof(root: Path, source_revision: str) -> Path:
    proof = root / "proof.json"
    write_json(proof, {
        "schema": 1,
        "evidence_type": "measured",
        "source_revision": source_revision,
        "run_id": "g14-self-test-run",
        "measurement_window_id": "g14-self-test-window",
        "started_at": "2026-09-09T00:00:00Z",
        "finished_at": "2026-09-09T00:30:00Z",
        "command": ["scripts/run_scanner_heal_g14_multiset_evidence.py", "--proof-json", "proof.json"],
        "summary": "Measured parser self-test proof.",
        "topology": {"erasure": "EC8+4", "nodes": 3, "drives_per_node": 4},
        "sets": 2,
        "pools": 2,
        "invalidation_domain": "distributed-ec",
        "distributed_ec_invalidation": True,
        "peer_count": 3,
        "same_window_remote_proof": True,
        "all_peers_bound_to_generation_window": True,
        "case_evidence": [
            {"case": "ec84-target-drive-restart", "sha256": "a" * 64},
            {"case": "multi-set-distributed-invalidation", "sha256": "b" * 64},
            {"case": "multi-pool-distributed-invalidation", "sha256": "c" * 64},
        ],
    })
    return proof


def write_self_test_case_dir(root: Path, source_revision: str, case: str, sets: int, pools: int) -> Path:
    directory = root / case
    directory.mkdir()
    started = datetime(2026, 9, 9, 0, 0, 0, tzinfo=timezone.utc).timestamp()
    finished = datetime(2026, 9, 9, 0, 30, 0, tzinfo=timezone.utc).timestamp()
    run_id = ("a" if pools > 1 else "b") * 32
    write_json(directory / "run.json", {
        "schema": 1,
        "run_id": run_id,
        "source_revision": source_revision,
        "started_at": started,
    })
    oracle = {
        "schema": 1,
        "case": case,
        "evidence": "process-crash-restart" if pools > 1 else "process-restart",
        "run_id": run_id,
        "source_revision": source_revision,
        "topology": {"nodes": 3, "drives_per_node": 12 if pools > 1 else 8},
        "erasure_set_drive_count": 12,
        "sets": sets,
        "pools": pools,
        "distributed_ec_invalidation": True,
        "peer_count": 3,
        "same_window_remote_proof": True,
        "all_peers_bound_to_generation_window": True,
    }
    oracle_name = f"{case}.json"
    write_json(directory / oracle_name, oracle)
    write_json(directory / "execution.json", {
        "run_id": run_id,
        "exit_code": 0,
        "finished_at": finished,
        "artifacts": {oracle_name: digest(directory / oracle_name)},
    })
    return directory


def run_self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = write_self_test_proof(root, source_revision)
        descriptor = build_descriptor(parse_args([
            "--proof-json", str(proof),
            "--out-dir", str(root / "out"),
        ]))
        require(descriptor.is_file(), "self-test descriptor missing")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        proof = write_self_test_proof(root, source_revision)
        data = read_json(proof)
        data["pools"] = 1
        write_json(proof, data)
        try:
            build_descriptor(parse_args(["--proof-json", str(proof), "--out-dir", str(root / "out")]))
        except ValueError as err:
            require("pools" in str(err), "wrong self-test failure for single-pool proof")
        else:
            raise ValueError("self-test accepted single-pool G14 proof")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        multi_set = write_self_test_case_dir(root, source_revision, "background-target-restart-ec8-4-multi-set", 2, 1)
        multi_pool = write_self_test_case_dir(root, source_revision, "background-target-crash-ec8-4-multi-pool", 3, 3)
        descriptor = build_descriptor(parse_args([
            "--case-dir", f"background-target-restart-ec8-4-multi-set={multi_set}",
            "--case-dir", f"background-target-crash-ec8-4-multi-pool={multi_pool}",
            "--out-dir", str(root / "out"),
        ]))
        require(descriptor.is_file(), "self-test case-dir descriptor missing")
        data = read_json(descriptor)
        evidence = data["gates"]["G14"]["evidence_fields"]["same_window_field_evidence"]
        artifact = descriptor.parent / evidence["artifact"]
        case_evidence = read_json(artifact)["case_evidence"]
        require(len(case_evidence) == 1, "self-test case-dir descriptor copied non-covering case evidence")
        require(case_evidence[0]["case"] == "background-target-crash-ec8-4-multi-pool",
                "self-test case-dir descriptor selected the wrong covering case")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_revision = git_head()
        multi_set = write_self_test_case_dir(root, source_revision, "background-target-restart-ec8-4-multi-set", 2, 1)
        try:
            build_descriptor(parse_args([
                "--case-dir", f"background-target-restart-ec8-4-multi-set={multi_set}",
                "--out-dir", str(root / "out"),
            ]))
        except ValueError as err:
            require("multi-set/multi-pool" in str(err), "wrong self-test failure for missing multi-pool case")
        else:
            raise ValueError("self-test accepted case evidence without multi-pool proof")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof-json", type=Path)
    parser.add_argument("--case-dir", action="append", default=[],
                        help="Measured e2e evidence run directory, optionally CASE=DIR; repeatable")
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--source-revision")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if not args.self_test:
        if (args.proof_json is None) == (not args.case_dir):
            parser.error("provide exactly one of --proof-json or --case-dir unless --self-test is used")
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
        print(f"G14 release descriptor verified: {descriptor}")
        return 0
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
