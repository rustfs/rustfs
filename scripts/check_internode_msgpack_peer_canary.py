#!/usr/bin/env python3
"""Validate measured mixed-version evidence for the internode msgpack peer gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys
from typing import Any


PHASES = ("mixed-old-peer", "all-new", "rollback-old-peer")
TOPOLOGIES = {
    "mixed-old-peer": (3, 1),
    "all-new": (4, 0),
    "rollback-old-peer": (3, 1),
}
OLD_NODES = {
    "mixed-old-peer": {"node4"},
    "all-new": set(),
    "rollback-old-peer": {"node2"},
}
ARTIFACTS = ("decisions", "fallback", "decode_errors", "rpc_errors")
DECISIONS = {"msgpack_only", "dual_write"}
REASONS = {"manual_fleet", "peer_capable", "peer_unknown", "peer_revoked", "not_requested"}
REVISION = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
MAX_DOCUMENT_BYTES = 1024 * 1024


class EvidenceError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceError(message)


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_json(path: Path) -> tuple[dict[str, Any], str]:
    require(path.is_file(), f"missing evidence artifact: {path}")
    size = path.stat().st_size
    require(0 < size <= MAX_DOCUMENT_BYTES, f"invalid evidence artifact size: {path}")
    raw = path.read_bytes()
    document = json.loads(raw, object_pairs_hook=unique_object)
    require(isinstance(document, dict), f"evidence artifact must be an object: {path}")
    return document, hashlib.sha256(raw).hexdigest()


def load_raw(path: Path) -> str:
    require(path.is_file(), f"missing evidence artifact: {path}")
    size = path.stat().st_size
    require(0 < size <= MAX_DOCUMENT_BYTES, f"invalid evidence artifact size: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def bounded_number(value: Any, name: str) -> float:
    require(type(value) in (int, float), f"{name} must be numeric")
    result = float(value)
    require(math.isfinite(result) and result >= 0, f"{name} must be finite and non-negative")
    return result


def uint(value: Any, name: str) -> int:
    require(type(value) is int and value >= 0, f"{name} must be a non-negative integer")
    return value


def timestamped_value(row: dict[str, Any], start: float, end: float, lag: float, name: str) -> float:
    value = row.get("value")
    require(isinstance(value, list) and len(value) == 2, f"{name} must contain one Prometheus value")
    timestamp = bounded_number(value[0], f"{name} timestamp")
    require(end <= timestamp <= end + lag, f"{name} timestamp is outside its phase completion window")
    try:
        number = float(value[1])
    except (TypeError, ValueError) as error:
        raise EvidenceError(f"{name} value is not numeric") from error
    require(math.isfinite(number) and number >= 0, f"{name} value must be finite and non-negative")
    return number


def prometheus_rows(document: dict[str, Any], name: str) -> list[dict[str, Any]]:
    require(document.get("status") == "success", f"{name} Prometheus query failed")
    data = document.get("data")
    require(isinstance(data, dict) and data.get("resultType") == "vector", f"{name} must be a Prometheus vector")
    rows = data.get("result")
    require(isinstance(rows, list) and len(rows) <= 64, f"{name} has invalid vector cardinality")
    require(all(isinstance(row, dict) for row in rows), f"{name} contains a non-object result")
    return rows


def decision_values(document: dict[str, Any], start: float, end: float, lag: float) -> dict[tuple[str, str], float]:
    result: dict[tuple[str, str], float] = {}
    for index, row in enumerate(prometheus_rows(document, "decisions")):
        labels = row.get("metric")
        require(isinstance(labels, dict) and set(labels) == {"decision", "reason"},
                "decision evidence must contain only decision/reason labels")
        decision = labels.get("decision")
        reason = labels.get("reason")
        require(decision in DECISIONS and reason in REASONS, "decision evidence contains an unbounded label")
        key = (decision, reason)
        require(key not in result, f"duplicate decision series: {key}")
        result[key] = timestamped_value(row, start, end, lag, f"decision[{index}]")
    return result


def scalar_sum(document: dict[str, Any], start: float, end: float, lag: float, name: str) -> float:
    total = 0.0
    rows = prometheus_rows(document, name)
    require(len(rows) == 1, f"{name} scalar query must return exactly one row")
    for index, row in enumerate(rows):
        labels = row.get("metric")
        require(labels == {}, f"{name} scalar query must not retain labels")
        total += timestamped_value(row, start, end, lag, f"{name}[{index}]")
    return total


def artifact_path(root: Path, relative: Any, used: set[Path], name: str) -> Path:
    require(isinstance(relative, str) and relative and not Path(relative).is_absolute(), f"invalid {name} artifact path")
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise EvidenceError(f"{name} artifact escapes the evidence directory") from error
    require(path not in used, f"artifact reused across phases: {relative}")
    used.add(path)
    return path


def bool_value(value: Any, name: str) -> bool:
    require(type(value) is bool, f"{name} must be boolean")
    return value


def exact_env(document: Any, name: str, old: bool) -> None:
    require(isinstance(document, dict), f"{name} environment must be an object")
    expected = {
        "msgpack_only": not old,
        "peer_capability": not old,
        "fleet_confirmed": False,
    }
    require(document == expected, f"{name} environment does not match the peer-gate canary contract")


def expected_queries(window: int, cluster_id: str) -> dict[str, str]:
    selector = f'{{rustfs_cluster_id="{cluster_id}"}}'
    grpc_selector = f'{{rustfs_cluster_id="{cluster_id}",backend="grpc",operation!="grpc_read_version"}}'
    read_version_selector = (
        f'{{rustfs_cluster_id="{cluster_id}",backend="grpc",operation="grpc_read_version",'
        'classification!="remote_response"}'
    )
    return {
        "decisions": (
            "sum by (decision, reason) "
            f"(increase(rustfs_system_network_internode_msgpack_send_decisions_total{selector}[{window}s]))"
        ),
        "fallback": f"sum(increase(rustfs_system_network_internode_msgpack_json_fallback_total{selector}[{window}s])) or vector(0)",
        "decode_errors": (
            f"sum(increase(rustfs_system_network_internode_msgpack_json_decode_error_total{selector}[{window}s])) or vector(0)"
        ),
        "rpc_errors": (
            f"(sum(increase(rustfs_system_network_internode_operation_errors_total{grpc_selector}[{window}s])) "
            "or vector(0)) + "
            "(sum(increase(rustfs_system_network_internode_operation_classified_errors_total"
            f"{read_version_selector}[{window}s])) or vector(0))"
        ),
    }


def check(manifest: dict[str, Any], root: Path) -> dict[str, Any]:
    root = root.resolve()
    require(type(manifest.get("schema")) is int and manifest.get("schema") == 1, "unsupported canary evidence schema")
    require(manifest.get("scope") == "internode-msgpack-peer-canary", "unexpected evidence scope")
    source = manifest.get("source_revision")
    baseline = manifest.get("baseline_revision")
    require(isinstance(source, str) and REVISION.fullmatch(source) is not None, "invalid source revision")
    require(isinstance(baseline, str) and REVISION.fullmatch(baseline) is not None, "invalid baseline revision")
    require(source != baseline, "candidate and baseline revisions must differ")
    require(bool_value(manifest.get("stable_connection_affinity"), "stable_connection_affinity"),
            "stable connection-to-peer affinity is required")
    cluster_id = manifest.get("cluster_id")
    require(isinstance(cluster_id, str) and re.fullmatch(r"msgpack-[0-9a-f]{32}", cluster_id) is not None,
            "invalid canary cluster identity")
    lag = bounded_number(manifest.get("max_metric_lag_secs"), "max_metric_lag_secs")
    require(0 <= lag <= 300, "max_metric_lag_secs exceeds the bounded limit")
    phases = manifest.get("phases")
    require(isinstance(phases, list) and len(phases) == len(PHASES), "exactly three canary phases are required")

    used: set[Path] = set()
    receipts: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    violations: list[str] = []
    image_path = artifact_path(root, manifest.get("images"), used, "images")
    image_digest = load_raw(image_path)
    images = json.loads(image_path.read_bytes(), object_pairs_hook=unique_object)
    require(isinstance(images, list) and len(images) == 2, "expected candidate and baseline image inspections")
    image_ids = []
    revision_evidence = []
    for image, revision in zip(images, (source, baseline)):
        require(isinstance(image, dict), "invalid image inspection")
        image_id = image.get("Id")
        require(isinstance(image_id, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is not None,
                "invalid immutable image ID")
        require(image.get("RevisionEvidence") in {"config-labels", "immutable-history"},
                "invalid image revision evidence source")
        config = image.get("Config")
        require(isinstance(config, dict), "invalid image config")
        labels = config.get("Labels")
        require(isinstance(labels, dict), "invalid image labels")
        observed = [labels[key] for key in ("vcs-ref", "org.opencontainers.image.revision") if key in labels]
        require(bool(observed) and all(value == revision for value in observed), "image revision mismatch")
        image_ids.append(image_id)
        revision_evidence.append(image["RevisionEvidence"])
    require(image_ids[0] != image_ids[1], "candidate and baseline resolve to the same image")
    receipts.append({"kind": "images", "path": manifest["images"], "sha256": image_digest})
    previous_end = 0.0
    for expected_name, phase in zip(PHASES, phases):
        require(isinstance(phase, dict) and phase.get("name") == expected_name, "canary phases are missing or out of order")
        start = uint(phase.get("start_unix"), f"{expected_name} start")
        end = uint(phase.get("end_unix"), f"{expected_name} end")
        require(previous_end < start < end and end - start <= 3600, f"invalid {expected_name} measurement window")
        previous_end = end
        topology = phase.get("topology")
        require(isinstance(topology, dict), f"{expected_name} topology must be an object")
        expected_new, expected_old = TOPOLOGIES[expected_name]
        nodes = topology.get("nodes")
        require(isinstance(nodes, dict) and set(nodes) == {"node1", "node2", "node3", "node4"},
                f"{expected_name} node inventory is incomplete")
        expected_nodes = {
            node: baseline if node in OLD_NODES[expected_name] else source
            for node in ("node1", "node2", "node3", "node4")
        }
        require(nodes == expected_nodes, f"{expected_name} node revisions do not match the mixed-version matrix")
        running_path = artifact_path(root, phase.get("images"), used, f"{expected_name}.images")
        running, running_digest = load_json(running_path)
        expected_images = {
            node: {"image_id": image_ids[1] if node in OLD_NODES[expected_name] else image_ids[0], "revision": revision}
            for node, revision in expected_nodes.items()
        }
        require(running == expected_images, f"{expected_name} running images do not match verified identities")
        receipts.append({"phase": expected_name, "kind": "running-images", "path": phase["images"],
                         "sha256": running_digest})
        new_nodes = uint(topology.get("new_nodes"), f"{expected_name} new_nodes")
        old_nodes = uint(topology.get("old_nodes"), f"{expected_name} old_nodes")
        require(new_nodes == expected_new and old_nodes == expected_old,
                f"{expected_name} topology counts do not match the four-node matrix")
        environments = phase.get("environment")
        require(isinstance(environments, dict), f"{expected_name} environment is missing")
        exact_env(environments.get("new_nodes"), f"{expected_name} new_nodes", old=False)
        exact_env(environments.get("old_nodes"), f"{expected_name} old_nodes", old=True)
        queries = phase.get("queries")
        require(queries == expected_queries(end - start, cluster_id), f"{expected_name} PromQL does not match its exact window")
        workload_path = artifact_path(root, phase.get("workload"), used, f"{expected_name}.workload")
        workload, workload_digest = load_json(workload_path)
        summary_path = artifact_path(root, workload.get("summary"), used, f"{expected_name}.workload-summary")
        summary_digest = load_raw(summary_path)
        require(workload.get("summary_sha256") == summary_digest, f"{expected_name} workload summary hash mismatch")
        workload_success = workload.get("status") == "success"
        requests = uint(workload.get("requests"), f"{expected_name} workload requests")
        require(requests > 0, f"{expected_name} workload has no requests")
        if not workload_success:
            violations.append(f"{expected_name}: workload failed")
        rollback_readback = bool_value(workload.get("rollback_readback"), f"{expected_name} rollback_readback")
        if expected_name != "rollback-old-peer":
            require(not rollback_readback, f"{expected_name} cannot claim rollback readback")
        if expected_name == "rollback-old-peer" and not rollback_readback:
            violations.append(f"{expected_name}: rollback readback failed")
        source_digest = workload.get("readback_source_sha256")
        downloaded_digest = workload.get("readback_download_sha256")
        if expected_name == "rollback-old-peer":
            require(isinstance(source_digest, str) and SHA256.fullmatch(source_digest) is not None,
                    "rollback source digest is invalid")
            require(source_digest == downloaded_digest, "rollback readback digest mismatch")
        else:
            require(source_digest is None and downloaded_digest is None,
                    f"{expected_name} cannot claim rollback content digests")
        receipts.append({"phase": expected_name, "kind": "workload", "path": phase["workload"],
                         "sha256": workload_digest})
        receipts.append({"phase": expected_name, "kind": "workload-summary", "path": workload["summary"],
                         "sha256": summary_digest})

        artifacts = phase.get("artifacts")
        require(isinstance(artifacts, dict) and set(artifacts) == set(ARTIFACTS), f"{expected_name} artifact set is incomplete")
        documents: dict[str, dict[str, Any]] = {}
        for artifact in ARTIFACTS:
            path = artifact_path(root, artifacts[artifact], used, f"{expected_name}.{artifact}")
            document, digest = load_json(path)
            documents[artifact] = document
            receipts.append({"phase": expected_name, "kind": artifact, "path": artifacts[artifact], "sha256": digest})

        decisions = decision_values(documents["decisions"], start, end, lag)
        peer_capable = decisions.get(("msgpack_only", "peer_capable"), 0.0)
        compatibility = decisions.get(("dual_write", "peer_unknown"), 0.0) + decisions.get(
            ("dual_write", "peer_revoked"), 0.0
        )
        if peer_capable <= 0:
            violations.append(f"{expected_name}: no peer_capable msgpack-only decision")
        if expected_old > 0 and compatibility <= 0:
            violations.append(f"{expected_name}: no old/unknown peer dual-write decision")
        if decisions.get(("msgpack_only", "manual_fleet"), 0.0) > 0:
            violations.append(f"{expected_name}: manual fleet path was unexpectedly active")
        if decisions.get(("dual_write", "not_requested"), 0.0) > 0:
            violations.append(f"{expected_name}: peer gate was not active on an eligible new-node request")

        errors = {
            "fallback": scalar_sum(documents["fallback"], start, end, lag, "fallback"),
            "decode_errors": scalar_sum(documents["decode_errors"], start, end, lag, "decode_errors"),
            "rpc_errors": scalar_sum(documents["rpc_errors"], start, end, lag, "rpc_errors"),
        }
        for name, value in errors.items():
            if value != 0:
                violations.append(f"{expected_name}: nonzero {name} ({value})")
        summaries.append({
            "name": expected_name,
            "start_unix": start,
            "end_unix": end,
            "topology": topology,
            "workload_requests": requests,
            "peer_capable_decisions": peer_capable,
            "compatibility_decisions": compatibility,
            **errors,
        })

    return {
        "observed_pass": not violations,
        "scope": manifest["scope"],
        "source_revision": source,
        "baseline_revision": baseline,
        "cluster_id": cluster_id,
        "image_revision_evidence": {
            "candidate": revision_evidence[0],
            "baseline": revision_evidence[1],
        },
        "phases": summaries,
        "violations": violations,
        "artifacts": receipts,
        "limits": "Measured four-node compatibility evidence only; production soak and performance drift gates remain required.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    try:
        root = args.manifest.resolve().parent
        manifest, digest = load_json(args.manifest.resolve())
        result = check(manifest, root)
        inputs = {args.manifest.resolve()} | {(root / item["path"]).resolve() for item in result["artifacts"]}
        require(args.out.resolve() not in inputs, "output would overwrite raw canary evidence")
        result["manifest_sha256"] = digest
        args.out.write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        return 0 if result["observed_pass"] else 1
    except (EvidenceError, OSError, TypeError, KeyError, AttributeError, UnicodeError, json.JSONDecodeError) as error:
        print(f"Invalid internode msgpack peer canary evidence: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
