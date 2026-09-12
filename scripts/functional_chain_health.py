#!/usr/bin/env python3
"""Report the latest chain attempt separately from verified complete successes."""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess

from functional_chain_evidence import validate_records
from resolve_functional_candidate import REPOSITORY, api, read_json_artifact, require, resolve, sha

WORKFLOW = "rustfs-functional-chain.yml"
MAX_AGE = timedelta(hours=36)


def timestamp(value):
    require(isinstance(value, str), "missing evidence timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "evidence timestamp has no timezone")
    return parsed


def validate_summary(summary, run):
    require(isinstance(summary, dict) and type(summary.get("schema")) is int and summary["schema"] == 1 and summary.get("complete") is True, "unsupported complete-chain evidence")
    chain = summary["chain"]
    require(chain["run_id"] == run["id"] and chain["attempt"] == run["run_attempt"] and chain["workflow_sha"] == run["head_sha"], "complete evidence belongs to another run attempt")
    require(sha(chain["testing_sha"]), "missing test-script pin")
    validate_records(chain, summary["suites"])
    candidate = chain["candidate"]
    manifest = candidate["manifest"]
    require(resolve(manifest["build_run_id"], manifest["build_run_attempt"]) == candidate, "producer candidate identity changed")
    config = api(f"repos/{REPOSITORY}/contents/.config/functional-script-revision.txt?ref={run['head_sha']}")
    require(base64.b64decode(config["content"]).decode().strip() == chain["testing_sha"], "private pin differs from workflow source")
    completed = timestamp(summary["completed_at"])
    require(timestamp(run["run_started_at"]) <= completed <= datetime.now(timezone.utc) + timedelta(minutes=5), "invalid completion timestamp")
    source_ref = manifest.get("source_ref", candidate["workflow_ref"])
    return {"run_id": run["id"], "attempt": run["run_attempt"], "url": run["html_url"],
            "workflow_sha": chain["workflow_sha"], "testing_sha": chain["testing_sha"],
            "candidate": candidate, "source_ref": source_ref, "source_sha": manifest["source_sha"],
            "completed_at": completed.isoformat(), "expires_at": (timestamp(candidate["build_started_at"]) + MAX_AGE).isoformat(),
            "verified_at": datetime.now(timezone.utc).isoformat(), "evidence_schema": 1}


def complete_success(run):
    require(run["path"] == ".github/workflows/" + WORKFLOW and run["head_branch"] == "main", "unexpected chain workflow source")
    require((run.get("head_repository") or {}).get("full_name") == REPOSITORY, "chain came from another repository")
    require(run.get("status") == "completed" and run.get("conclusion") == "success", "chain has not completed successfully")
    name = f"functional-chain-complete-{run['id']}-{run['run_attempt']}"
    payload = api(f"repos/{REPOSITORY}/actions/runs/{run['id']}/artifacts?per_page=100")
    require(payload["total_count"] <= 100, "chain artifact listing is incomplete")
    artifacts = [item for item in payload["artifacts"] if item.get("name") == name]
    require(len(artifacts) == 1, "missing or ambiguous complete-chain artifact")
    artifact = artifacts[0]
    require(type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= 1024 * 1024, "complete evidence size is invalid")
    archive = api(f"repos/{REPOSITORY}/actions/artifacts/{artifact['id']}/zip", binary=True)
    summary = read_json_artifact(archive, artifact, run, name, "chain-complete.json", max_json=128 * 1024)
    result = validate_summary(summary, run)
    result["artifact_id"] = artifact["id"]
    result["artifact_digest"] = artifact["digest"]
    return result


def collect(limit=20):
    observed = datetime.now(timezone.utc)
    workflow = api(f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW}")
    runs = api(f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW}/runs?branch=main&per_page={limit}")["workflow_runs"]
    runs.sort(key=lambda run: timestamp(run["run_started_at"]), reverse=True)
    result = {"schema": 1, "observed_at": observed.isoformat(), "workflow_state": workflow["state"],
              "owner": "@overtrue", "scan_limit": limit, "inspection_complete": True,
              "latest_attempt": None, "last_complete_success": {}, "healthy": False}
    for index, listed in enumerate(runs):
        run = api(f"repos/{REPOSITORY}/actions/runs/{listed['id']}/attempts/{listed['run_attempt']}")
        if index == 0:
            result["latest_attempt"] = {"run_id": run["id"], "attempt": run["run_attempt"], "url": run["html_url"],
                                        "status": run["status"], "conclusion": run["conclusion"], "verification": "not_complete"}
        if run.get("conclusion") != "success":
            continue
        try:
            complete = complete_success(run)
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            if index == 0:
                result["latest_attempt"]["verification"] = "invalid"
            result["inspection_complete"] = False
            continue
        source = complete["source_ref"]
        if source.startswith("refs/heads/"):
            source = source[len("refs/heads/"):]
        if index == 0:
            result["latest_attempt"]["verification"] = "complete"
            result["latest_attempt"]["source_ref"] = source
            result["latest_attempt"]["source_sha"] = complete["source_sha"]
        previous = result["last_complete_success"].get(source)
        if previous is None or timestamp(complete["completed_at"]) > timestamp(previous["completed_at"]):
            result["last_complete_success"][source] = complete
    # Reject a snapshot if a new attempt started while artifacts were checked.
    refreshed = api(f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW}/runs?branch=main&per_page={limit}")["workflow_runs"]
    fields = ("id", "run_attempt", "status", "conclusion", "head_sha", "run_started_at")
    snapshot = lambda values: sorted(tuple(item.get(key) for key in fields) for item in values)
    require(snapshot(runs) == snapshot(refreshed), "chain attempts changed during inspection; retry collection")
    for complete in result["last_complete_success"].values():
        complete["fresh"] = observed <= timestamp(complete["expires_at"])
    latest = result["latest_attempt"] or {}
    result["healthy"] = (result["workflow_state"] == "active" and latest.get("verification") == "complete"
                         and latest.get("source_ref") == "main" and result["last_complete_success"].get("main", {}).get("fresh") is True)
    return result


def merge_history(current, previous):
    require(isinstance(previous, dict) and type(previous.get("schema")) is int and previous["schema"] == 1, "unsupported existing dashboard state")
    require(timestamp(previous["observed_at"]) <= timestamp(current["observed_at"]), "refusing an older dashboard observation")
    for source, value in previous.get("last_complete_success", {}).items():
        if source not in current["last_complete_success"] or timestamp(value["completed_at"]) > timestamp(current["last_complete_success"][source]["completed_at"]):
            value = dict(value)
            value["fresh"] = timestamp(current["observed_at"]) <= timestamp(value["expires_at"])
            value["retained_history"] = True
            current["last_complete_success"][source] = value
    return current


def publish(result):
    endpoint = "repos/rustfs/dashboard/contents/src/chain-health.json"
    existing = api(endpoint)
    previous = json.loads(base64.b64decode(existing["content"]))
    merge_history(result, previous)
    body = {"message": "chore(ci): update functional chain health", "sha": existing["sha"],
            "content": base64.b64encode((json.dumps(result, indent=2) + "\n").encode()).decode()}
    subprocess.run(["gh", "api", "--method", "PUT", endpoint, "--input", "-"], input=json.dumps(body), text=True, check=True, capture_output=True, timeout=60)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    result = collect()
    if args.publish:
        publish(result)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("workflow_state", "inspection_complete", "healthy", "latest_attempt")}))
    return 0 if result["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
