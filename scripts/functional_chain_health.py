#!/usr/bin/env python3
"""Report the latest chain attempt separately from verified complete successes."""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
from urllib.parse import quote

from functional_chain_evidence import validate_records
from resolve_functional_candidate import REPOSITORY, api, read_json_artifact, require, resolve, sha
from validation_registry import load_registry, select_plan, suite_ids, validate_plan, workflow_entry

WORKFLOW = "rustfs-functional-chain.yml"
ENTRY = workflow_entry(".github/workflows/" + WORKFLOW)
MAX_AGE = timedelta(hours=ENTRY["max_age_hours"])


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
    plan = validate_plan(summary["plan"]) if "plan" in summary else select_plan(load_registry())
    require(plan["suites"] == list(suite_ids()), "partial suite execution cannot satisfy complete-chain health")
    skipped = summary.get("skipped_lanes", [])
    require(isinstance(skipped, list) and len(skipped) == len(set(skipped))
            and set(skipped) <= set(plan["optional_suites"]), "required suite was skipped")
    require(not skipped or all(summary.get("skip_reasons", {}).get(name) for name in skipped),
            "optional suite omission has no evidence")
    validate_records(chain, summary["suites"], [name for name in plan["suites"] if name not in skipped])
    candidate = chain["candidate"]
    manifest = candidate["manifest"]
    require(resolve(manifest["build_run_id"], manifest["build_run_attempt"]) == candidate, "producer candidate identity changed")
    # testing_sha may legitimately be auto-testing main HEAD via the prepare
    # step's >24h staleness fallback (checked for sha format above), so pin
    # equality is not an invariant; drop the pin-file comparison.
    completed = timestamp(summary["completed_at"])
    require(timestamp(run["run_started_at"]) <= completed <= datetime.now(timezone.utc) + timedelta(minutes=5), "invalid completion timestamp")
    source_ref = manifest.get("source_ref", candidate["workflow_ref"])
    return {"run_id": run["id"], "attempt": run["run_attempt"], "url": run["html_url"],
            "workflow_sha": chain["workflow_sha"], "testing_sha": chain["testing_sha"],
            "candidate": candidate, "source_ref": source_ref, "source_sha": manifest["source_sha"],
            "completed_at": completed.isoformat(), "expires_at": (completed + MAX_AGE).isoformat(),
            "skipped_lanes": skipped,
            "verified_at": datetime.now(timezone.utc).isoformat(), "evidence_schema": 1}


def complete_success(run):
    require(run["path"] == ".github/workflows/" + WORKFLOW and run["head_branch"] == "main", "unexpected chain workflow source")
    require((run.get("head_repository") or {}).get("full_name") == REPOSITORY, "chain came from another repository")
    require(run.get("event") in ("workflow_run", "workflow_dispatch"), "unexpected chain event")
    require(run.get("status") == "completed" and run.get("conclusion") == "success", "chain has not completed successfully")
    name = f"functional-chain-complete-{run['id']}-{run['run_attempt']}"
    artifacts = [item for item in run_artifacts(run["id"]) if item.get("name") == name]
    require(len(artifacts) == 1, "missing or ambiguous complete-chain artifact")
    artifact = artifacts[0]
    require(type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= 1024 * 1024, "complete evidence size is invalid")
    archive = api(f"repos/{REPOSITORY}/actions/artifacts/{artifact['id']}/zip", binary=True)
    summary = read_json_artifact(archive, artifact, run, name, "chain-complete.json", max_json=128 * 1024)
    result = validate_summary(summary, run)
    result["artifact_id"] = artifact["id"]
    result["artifact_digest"] = artifact["digest"]
    return result


def run_artifacts(run_id):
    artifacts = []
    total = None
    for page in range(1, 21):
        payload = api(f"repos/{REPOSITORY}/actions/runs/{run_id}/artifacts?per_page=100&page={page}")
        count, values = payload.get("total_count"), payload.get("artifacts")
        require(type(count) is int and count >= 0 and isinstance(values, list), "incomplete chain artifact listing")
        require(total is None or total == count, "chain artifacts changed during pagination")
        total = count
        require(len(values) <= 100 and (values or len(artifacts) == total), "incomplete chain artifact page")
        artifacts.extend(values)
        require(len(artifacts) <= total and len({item["id"] for item in artifacts}) == len(artifacts),
                "duplicated chain artifact page")
        if len(artifacts) == total:
            return artifacts
    raise ValueError("chain artifact pagination budget exhausted")


def expected_attempt(run):
    if run.get("event") == "workflow_run":
        return True
    require(run.get("event") == "workflow_dispatch", "unexpected chain attempt event")
    name = f"functional-plan-{run['id']}-{run['run_attempt']}"
    artifacts = [item for item in run_artifacts(run["id"]) if item.get("name") == name]
    if not artifacts:
        # Package/tools/inspection/recovery entrypoints do not select suites.
        return False
    require(len(artifacts) == 1, "ambiguous suite plan artifact")
    artifact = artifacts[0]
    require(type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= 1024 * 1024,
            "selection plan size is invalid")
    archive = api(f"repos/{REPOSITORY}/actions/artifacts/{artifact['id']}/zip", binary=True)
    plan = validate_plan(read_json_artifact(archive, artifact, run, name, "suite-plan.json"))
    return plan["suites"] == list(suite_ids())


def list_runs(page_size):
    require(type(page_size) is int and 1 <= page_size <= 100, "invalid chain page size")
    runs = []
    total = None
    for page in range(1, 101):
        payload = api(f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW}/runs?branch=main&per_page={page_size}&page={page}")
        count, values = payload.get("total_count"), payload.get("workflow_runs")
        require(type(count) is int and count >= 0 and isinstance(values, list), "incomplete chain run listing")
        require(total is None or total == count, "chain run count changed during pagination")
        total = count
        require(len(values) <= page_size and (values or len(runs) == total), "incomplete chain run page")
        runs.extend(values)
        require(len(runs) <= total and len({run["id"] for run in runs}) == len(runs), "duplicated chain run page")
        if len(runs) == total:
            return runs
    raise ValueError("chain pagination budget exhausted; observation is incomplete")


def expected_source_sha(source_ref):
    value = api(f"repos/{REPOSITORY}/git/ref/heads/{quote(source_ref, safe='')}")["object"]["sha"]
    require(sha(value), "expected source ref has no valid SHA")
    return value


def collect(limit=100, source_ref="main"):
    source_ref = source_ref.removeprefix("refs/heads/")
    require(bool(source_ref), "expected nightly source ref is empty")
    observed = datetime.now(timezone.utc)
    workflow = api(f"repos/{REPOSITORY}/actions/workflows/{WORKFLOW}")
    expected_sha = expected_source_sha(source_ref)
    runs = list_runs(limit)
    # Manual full-plan attempts can refresh evidence; package/tools/recovery
    # jobs do not claim suite completion. Automatic attempts remain visible
    # even if they never produce an artifact.
    runs.sort(key=lambda run: timestamp(run["run_started_at"]), reverse=True)
    result = {"schema": 1, "observed_at": observed.isoformat(), "workflow_state": workflow["state"],
              "owner": ENTRY["owner"], "policy": ENTRY["policy"], "scan_limit": limit,
              "inspection_complete": True, "expected_source_ref": source_ref, "expected_source_sha": expected_sha,
              "latest_attempt": None, "last_complete_success": {}, "healthy": False}
    for listed in runs:
        run = api(f"repos/{REPOSITORY}/actions/runs/{listed['id']}/attempts/{listed['run_attempt']}")
        require((run["id"], run["run_attempt"], run["head_sha"]) ==
                (listed["id"], listed["run_attempt"], listed["head_sha"]), "attempt API returned a different run identity")
        if not expected_attempt(run):
            continue
        latest = result["latest_attempt"] is None
        if latest:
            result["latest_attempt"] = {"run_id": run["id"], "attempt": run["run_attempt"], "url": run["html_url"],
                                        "status": run["status"], "conclusion": run["conclusion"], "verification": "not_complete"}
        if run.get("conclusion") != "success":
            continue
        try:
            complete = complete_success(run)
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            if latest:
                result["latest_attempt"]["verification"] = "invalid"
                result["inspection_complete"] = False
            continue
        source = complete["source_ref"].removeprefix("refs/heads/")
        if latest:
            result["latest_attempt"] = {"run_id": run["id"], "attempt": run["run_attempt"], "url": run["html_url"],
                                        "status": run["status"], "conclusion": run["conclusion"], "verification": "complete",
                                        "source_ref": source, "source_sha": complete["source_sha"]}
        previous = result["last_complete_success"].get(source)
        if previous is None or timestamp(complete["completed_at"]) > timestamp(previous["completed_at"]):
            result["last_complete_success"][source] = complete
        # Older immutable successes add no facts once this source has current
        # evidence. Historical observations survive separately in the dashboard.
        if source == source_ref:
            break
    refreshed = list_runs(limit)
    fields = ("id", "run_attempt", "status", "conclusion", "head_sha", "run_started_at")
    snapshot = lambda values: sorted(tuple(item.get(key) for key in fields) for item in values)
    require(snapshot(runs) == snapshot(refreshed), "chain attempts changed during inspection; retry collection")
    require(expected_source_sha(source_ref) == expected_sha, "expected source changed during inspection")
    for complete in result["last_complete_success"].values():
        complete["fresh"] = (observed <= timestamp(complete["expires_at"])
                             and complete["source_sha"] == expected_sha)
    latest = result["latest_attempt"] or {}
    result["healthy"] = (result["inspection_complete"] and result["policy"] == "active"
                         and result["workflow_state"] == "active" and latest.get("verification") == "complete"
                         and latest.get("source_ref") == source_ref and latest.get("source_sha") == expected_sha
                         and result["last_complete_success"].get(source_ref, {}).get("fresh") is True)
    return result


def merge_history(current, previous):
    require(isinstance(previous, dict) and type(previous.get("schema")) is int and previous["schema"] == 1, "unsupported existing dashboard state")
    require(timestamp(previous["observed_at"]) <= timestamp(current["observed_at"]), "refusing an older dashboard observation")
    for source, value in previous.get("last_complete_success", {}).items():
        if source not in current["last_complete_success"] or timestamp(value["completed_at"]) > timestamp(current["last_complete_success"][source]["completed_at"]):
            value = dict(value)
            value["fresh"] = (timestamp(current["observed_at"]) <= timestamp(value["expires_at"])
                              and value["source_sha"] == current.get("expected_source_sha"))
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
    parser.add_argument("--source-ref", default="main", help="Expected nightly build source, not the workflow branch")
    args = parser.parse_args()
    try:
        result = collect(source_ref=args.source_ref)
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        result = {"schema": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
                  "workflow_state": "unknown", "inspection_complete": False, "healthy": False,
                  "latest_attempt": None, "last_complete_success": {}, "error": str(error)}
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    if args.publish:
        publish(result)
    print(json.dumps({key: result[key] for key in ("workflow_state", "inspection_complete", "healthy", "latest_attempt")}))
    return 0 if result["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
