#!/usr/bin/env python3
"""Read all Actions attempts in a creation window and account for observed work."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci_timing_report import minutes, summarize as timing_summary, timestamp

RUN_FIELDS = ("id", "run_attempt", "workflow_id", "path", "name", "event", "head_sha", "head_branch",
              "created_at", "run_started_at", "updated_at", "status", "conclusion", "html_url", "pull_requests")
JOB_FIELDS = ("id", "run_id", "run_attempt", "name", "status", "conclusion", "created_at", "started_at",
              "completed_at", "labels", "runner_name", "runner_group_name", "steps", "html_url")
PHASES = ("setup", "compile", "test", "archive", "transfer")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def api(path):
    # Every request is explicitly GET; stderr may contain credentials or URLs.
    result = subprocess.run(["gh", "api", "--method", "GET", path], capture_output=True, text=True, timeout=90)
    if result.returncode:
        raise RuntimeError("GitHub read failed")
    return json.loads(result.stdout)


def pages(path, field, *, cap=10000):
    rows, seen, expected = [], set(), None
    for page in range(1, math.ceil(cap / 100) + 1):
        response = api(f"{path}{'&' if '?' in path else '?'}per_page=100&page={page}")
        batch = response.get(field)
        total = response.get("total_count")
        require(isinstance(batch, list) and type(total) is int and 0 <= total <= cap,
                "missing count or inventory exceeds limit; narrow the collection window")
        require(expected is None or total == expected, "inventory changed during pagination")
        expected = total
        for row in batch:
            require(type(row.get("id")) is int and row["id"] not in seen, "duplicate or missing inventory identity")
            seen.add(row["id"])
        rows.extend(batch)
        if len(rows) >= total or len(batch) < 100:
            require(len(rows) == total, "incomplete pagination")
            return rows
    raise ValueError("pagination limit reached")


def repository_members(run, repository):
    # Actions can associate a shared SHA with PRs targeting other forks. A PR
    # number is only meaningful inside its base repository.
    return sorted({row["number"] for row in (run.get("pull_requests") or [])
                   if type(row.get("number")) is int and row["number"] > 0
                   and row.get("url", "").lower() == f"https://api.github.com/repos/{repository}/pulls/{row['number']}".lower()})


def collect(repository, since, until):
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "invalid repository")
    require(timestamp(since) < timestamp(until), "collection interval must increase")
    snapshot = {"schema": 1, "repository": repository, "observed_at": datetime.now(timezone.utc).isoformat(),
                "created_since": since, "created_until": until, "complete": False, "issues": [],
                "runs": [], "pull_requests": {}, "attributions": {}}
    prefix = f"repos/{repository}"
    try:
        query = urlencode({"created": f"{since}..{until}", "exclude_pull_requests": "false"})
        inventory = pages(f"{prefix}/actions/runs?{query}", "workflow_runs", cap=1000)
    except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        snapshot["issues"].append({"scope": "run_inventory", "error_type": type(error).__name__})
        return snapshot
    for run in inventory:
        run_id = run["id"]
        try:
            require(timestamp(since) <= timestamp(run.get("created_at")) <= timestamp(until), "run outside interval")
            latest = run.get("run_attempt")
            require(type(latest) is int and 1 <= latest <= 1000, "invalid attempt count")
            endpoint = f"{prefix}/actions/runs/{run_id}"
            for attempt in range(1, latest + 1):
                sample = {"id": run_id, "run_attempt": attempt, "jobs": [], "complete": False}
                snapshot["runs"].append(sample)
                try:
                    detail = api(f"{endpoint}/attempts/{attempt}")
                    require(detail.get("id") == run_id and detail.get("run_attempt") == attempt
                            and detail.get("head_sha") == run["head_sha"], "attempt identity mismatch")
                    sample.update({key: detail.get(key) for key in RUN_FIELDS})
                    jobs = pages(f"{endpoint}/attempts/{attempt}/jobs", "jobs")
                    for job in jobs:
                        require(job.get("run_id") == run_id and job.get("run_attempt", attempt) == attempt,
                                "job belongs to a different attempt")
                    sample["jobs"] = [{key: job.get(key) for key in JOB_FIELDS} for job in jobs]
                    sample["complete"] = True
                except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
                    snapshot["issues"].append({"scope": "attempt", "run_id": run_id, "attempt": attempt,
                                               "error_type": type(error).__name__})
            observed = api(endpoint)
            require(all(observed.get(key) == run.get(key) for key in ("run_attempt", "head_sha", "status", "conclusion", "updated_at")),
                    "run changed while collecting")
        except (ValueError, TypeError, RuntimeError, subprocess.TimeoutExpired) as error:
            snapshot["issues"].append({"scope": "run", "run_id": run_id, "error_type": type(error).__name__})
    members = {number for run in snapshot["runs"] for number in repository_members(run, repository)}
    for number in sorted(members):
        try:
            detail = api(f"{prefix}/pulls/{number}")
            require(detail.get("number") == number, "PR identity mismatch")
            snapshot["pull_requests"][str(number)] = {key: detail.get(key)
                for key in ("number", "created_at", "merged_at", "merge_commit_sha", "html_url")}
        except (ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
            snapshot["issues"].append({"scope": "pull_request", "number": number, "error_type": type(error).__name__})
    snapshot["complete"] = not snapshot["issues"]
    return snapshot


def add_attributions(snapshot, manifest):
    """Bind operator-reviewed missing event provenance to immutable run identities."""
    require(manifest.get("schema") == 1 and manifest.get("repository") == snapshot["repository"],
            "attribution manifest repository/schema mismatch")
    rows = manifest.get("runs", [])
    require(isinstance(rows, list), "attribution runs must be a list")
    seen = set()
    for row in rows:
        run_id = row.get("run_id")
        require(type(run_id) is int and run_id not in seen, "duplicate or invalid attribution run")
        seen.add(run_id)
        matches = [run for run in snapshot["runs"] if run["id"] == run_id]
        require(matches and all(run.get("head_sha") == row.get("head_sha") for run in matches), "attribution SHA mismatch")
        members = row.get("pull_requests")
        require(isinstance(members, list) and members and all(type(value) is int and value > 0 for value in members)
                and len(set(members)) == len(members), "attribution requires unique PR members")
        require(isinstance(row.get("evidence"), str) and row["evidence"].startswith("https://github.com/"),
                "attribution requires a reviewable evidence URL")
        require(not any(run.get("event") == "schedule" for run in matches), "periodic work must remain separate")
        snapshot["attributions"][str(run_id)] = row
    for number, metadata in manifest.get("pull_requests", {}).items():
        snapshot["pull_requests"].setdefault(number, metadata)


def runner_model(labels):
    labels = labels or []
    if any(label.startswith(("sm-", "dind-", "smoke-", "pf-")) for label in labels):
        return "fixed_capacity"
    if any(label.startswith("ubicloud-") for label in labels):
        return "metered"
    if "self-hosted" not in labels and any(label.startswith(("ubuntu-", "windows-", "macos-")) for label in labels):
        return "standard_hosted"
    return "unknown"


def safe_minutes(start, end):
    try:
        return minutes(start, end)
    except (ValueError, TypeError, AttributeError):
        return None


def job_measurement(job):
    skipped = job.get("conclusion") == "skipped"
    duration = 0 if skipped else (safe_minutes(job.get("started_at"), job.get("completed_at"))
                                 if job.get("status") == "completed" else None)
    wait = 0 if skipped else safe_minutes(job.get("created_at"), job.get("started_at"))
    phases = {phase: [] for phase in PHASES}
    steps = []
    for step in job.get("steps") or []:
        phase = step.get("name", "").removeprefix("CI phase: ")
        phase = phase if phase in PHASES else "unknown"
        elapsed = safe_minutes(step.get("started_at"), step.get("completed_at")) if step.get("status") == "completed" else None
        steps.append({"name": step.get("name"), "phase": phase, "minutes": elapsed})
        if phase in phases:
            phases[phase].append(elapsed)
    return {"runner_minutes": duration, "queue_minutes": wait,
            "runner_model": runner_model(job.get("labels")),
            "phases_minutes": {phase: sum(values) if values and all(value is not None for value in values) else None
                               for phase, values in phases.items()}, "steps": steps}


def attribution(run, snapshot):
    if run.get("event") == "schedule":
        return [], "periodic"
    explicit = snapshot.get("attributions", {}).get(str(run["id"]))
    if explicit:
        require(explicit["head_sha"] == run.get("head_sha"), "stored attribution SHA mismatch")
        return explicit["pull_requests"], "reviewed_manifest"
    # Merge-group membership and workflow_run provenance cannot be recovered
    # safely from a synthetic branch name or a matching current SHA alone.
    if run.get("event") in ("pull_request", "pull_request_target"):
        members = repository_members(run, snapshot["repository"])
        if members:
            return members, "event_pull_requests"
    if run.get("event") == "push":
        merged = [int(number) for number, pr in snapshot.get("pull_requests", {}).items()
                  if pr.get("merged_at") and pr.get("merge_commit_sha") == run.get("head_sha")]
        if merged:
            return sorted(merged), "exact_merge_commit"
    return [], "unattributed"


def new_bucket():
    return {"known_runner_minutes": 0.0, "known_queue_minutes": 0.0,
            "unknown_runner_jobs": 0, "unknown_queue_jobs": 0, "job_share": 0.0,
            "per_job_rounded_known_minutes": 0.0, "monetary_cost": None}


def accumulate(bucket, metric, weight=1):
    bucket["job_share"] += weight
    for kind in ("runner", "queue"):
        duration = metric[f"{kind}_minutes"]
        if duration is None:
            bucket[f"unknown_{kind}_jobs"] += weight
        else:
            bucket[f"known_{kind}_minutes"] += duration * weight
    if metric["runner_minutes"] is not None:
        bucket["per_job_rounded_known_minutes"] += math.ceil(metric["runner_minutes"]) * weight


def summarize(snapshot):
    total, periodic, unattributed = new_bucket(), new_bucket(), new_bucket()
    pools, prs = defaultdict(new_bucket), defaultdict(new_bucket)
    statuses, unknowns, observations, identities = Counter(), Counter(), [], set()
    all_jobs = set()
    for run in snapshot["runs"]:
        identity = (run["id"], run["run_attempt"])
        require(identity not in identities, "duplicate run attempt in snapshot")
        identities.add(identity)
        statuses[run.get("conclusion") or run.get("status") or "unknown"] += 1
        members, source = attribution(run, snapshot)
        for job in run["jobs"]:
            require(job["id"] not in all_jobs, "duplicate job across the snapshot")
            all_jobs.add(job["id"])
            metric = job_measurement(job)
            accumulate(total, metric)
            accumulate(pools[metric["runner_model"]], metric)
            if members:
                for member in members:
                    accumulate(prs[str(member)], metric, 1 / len(members))
            else:
                accumulate(periodic if source == "periodic" else unattributed, metric)
            for key in ("runner_minutes", "queue_minutes"):
                unknowns[key] += metric[key] is None
            unknowns["runner_model"] += metric["runner_model"] == "unknown"
            unknowns["phase_breakdown_jobs"] += any(value is None for value in metric["phases_minutes"].values())
            observations.append({"run_id": run["id"], "run_attempt": run["run_attempt"], "head_sha": run.get("head_sha"),
                                 "workflow": run.get("path"), "event": run.get("event"), "job_id": job["id"],
                                 "job_name": job.get("name"), "conclusion": job.get("conclusion"),
                                 "pull_requests": members, "attribution_source": source, **metric})
    for number, bucket in prs.items():
        metadata = snapshot.get("pull_requests", {}).get(number, {})
        bucket["merged_at"] = metadata.get("merged_at")
        bucket["created_at"] = metadata.get("created_at")
        # A creation-window sample cannot prove retained historical runs or all
        # externally triggered validation were available. Never call it a bill.
        bucket["lifecycle_complete"] = False
    successful = timing_summary([run for run in snapshot["runs"]
                                 if run.get("path") == ".github/workflows/ci.yml" and run.get("complete")])
    return {"collection_complete": snapshot.get("complete", False), "attempts": len(identities),
            "attempt_statuses": dict(statuses), "unknowns": dict(unknowns), "total_unallocated": total,
            "runner_models": dict(pools), "pull_requests": dict(prs), "periodic_unallocated": periodic,
            "unattributed": unattributed, "job_observations": observations,
            "successful_code_pr": {"runner_sum": successful["successful_code_job_sum"], "feedback": successful["successful_code_wall"]},
            "allocation_rule": "Each job and its queue wait are shared equally by recorded PR members; periodic work stays separate.",
            "limits": ["Known minutes are partial sums; unknown timestamps and missing attempts are never zero-filled.",
                       "Creation-to-start wait does not separate scheduler and dependency delay and is not runner occupancy.",
                       "Creation windows omit older runs rerun inside the window; retained API history may be incomplete.",
                       "Only explicit CI phase step labels split compile, test and transfer; combined steps remain unknown.",
                       "Rounded minutes are a comparison model, not a provider invoice. Prices and fixed capacity bills are unknown.",
                       "Cache state, image identity and compiler attribution require benchmark or build receipts; labels do not prove them."]}


def markdown(report):
    total = report["total_unallocated"]
    return ("# Actions resource accounting\n\n"
            f"Collection complete: {report['collection_complete']}. Attempts: {report['attempts']}.\n\n"
            f"Known runner occupancy: {total['known_runner_minutes']:.2f} minutes; "
            f"jobs with unknown occupancy: {total['unknown_runner_jobs']}.\n\n"
            f"Known creation-to-start wait sum: {total['known_queue_minutes']:.2f} minutes; "
            f"jobs with unknown wait: {total['unknown_queue_jobs']}.\n\n"
            "| Allocation | Known runner minutes | Unknown runner job shares |\n|---|---:|---:|\n"
            + "".join(f"| {name} | {bucket['known_runner_minutes']:.2f} | {bucket['unknown_runner_jobs']:.2f} |\n"
                      for name, bucket in [(f"PR #{number}", value) for number, value in sorted(report["pull_requests"].items())]
                      + [("Periodic", report["periodic_unallocated"]), ("Unattributed", report["unattributed"])])
            + "\n" + "\n".join(f"- {limit}" for limit in report["limits"]) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default="rustfs/rustfs")
    parser.add_argument("--since", help="inclusive run creation timestamp, with timezone")
    parser.add_argument("--until", help="inclusive run creation timestamp, with timezone")
    parser.add_argument("--input", type=Path, help="recompute an existing snapshot offline")
    parser.add_argument("--attributions", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    if not args.input and not (args.since and args.until):
        parser.error("provide --input or both --since and --until")
    snapshot = json.loads(args.input.read_text()) if args.input else collect(args.repository, args.since, args.until)
    require(snapshot.get("schema") == 1 and isinstance(snapshot.get("runs"), list), "unsupported snapshot")
    if args.attributions:
        add_attributions(snapshot, json.loads(args.attributions.read_text()))
    snapshot["summary"] = summarize(snapshot)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(snapshot, indent=2) + "\n")
    args.summary.write_text(markdown(snapshot["summary"]))
    print(f"{len(snapshot['runs'])} attempts; collection_complete={snapshot['complete']}; see {args.output}")
    return 0 if snapshot["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
