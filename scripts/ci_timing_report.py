#!/usr/bin/env python3
"""Collect bounded Actions timing samples without changing CI selection."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import statistics
import subprocess
from urllib.parse import urlencode


def timestamp(value):
    if not value:
        return None
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Actions timestamp has no timezone")
    return result


def minutes(start, end):
    start, end = timestamp(start), timestamp(end)
    if start is None or end is None or end < start:
        return None
    return (end - start).total_seconds() / 60


def distribution(values):
    known = [value for value in values if value is not None]
    return {"samples": len(values), "known": len(known), "missing": len(values) - len(known),
            "median_minutes": statistics.median(known) if known else None}


def summarize(runs):
    statuses = Counter()
    jobs_by_name = defaultdict(list)
    successful_code_runs = []
    for run in runs:
        statuses[run.get("conclusion") or run.get("status", "unknown")] += 1
        jobs = run["jobs"]
        for job in jobs:
            if job.get("conclusion") != "skipped":
                jobs_by_name[job["name"]].append(job)
        # Exclude docs-only and pull_request.closed cancellation-handler greens.
        if (run.get("status") == "completed" and run.get("conclusion") == "success" and jobs
                and any(job["name"] in ("Workspace Test and Lint", "Test and Lint") and job.get("conclusion") == "success"
                        and any(step.get("name") == "Run nextest tests" and step.get("conclusion") == "success" for step in (job.get("steps") or [])) for job in jobs)
                and all(job.get("status") == "completed" for job in jobs)):
            successful_code_runs.append(run)

    wall, job_time = [], []
    for run in successful_code_runs:
        ends = [timestamp(job.get("completed_at")) for job in run["jobs"] if job.get("conclusion") != "skipped"]
        end = max(ends).isoformat() if ends and all(ends) else None
        start = run.get("run_started_at") if run.get("run_attempt", 1) > 1 else run.get("created_at")
        wall.append(minutes(start, end))
        durations = [minutes(job.get("started_at"), job.get("completed_at")) for job in run["jobs"] if job.get("conclusion") != "skipped"]
        job_time.append(sum(durations) if durations and all(value is not None for value in durations) else None)

    job_summary = {}
    for name, jobs in sorted(jobs_by_name.items()):
        steps = defaultdict(list)
        for job in jobs:
            for step in (job.get("steps") or []):
                if step.get("conclusion") != "skipped":
                    duration = minutes(step.get("started_at"), step.get("completed_at")) if step.get("status") == "completed" else None
                    steps[step["name"]].append(duration)
        job_summary[name] = {
            "queue": distribution([minutes(job.get("created_at"), job.get("started_at")) for job in jobs]),
            "execution": distribution([minutes(job.get("started_at"), job.get("completed_at")) if job.get("status") == "completed" else None for job in jobs]),
            "steps": {name: distribution(values) for name, values in sorted(steps.items())},
        }
    total = len(runs)
    return {"runs": total, "statuses": dict(statuses),
            "cancelled_fraction": statuses["cancelled"] / total if total else None,
            "completed_fraction": sum(run.get("status") == "completed" for run in runs) / total if total else None,
            "successful_code_runs": len(successful_code_runs),
            "successful_code_wall": distribution(wall), "successful_code_job_sum": distribution(job_time),
            "jobs": job_summary,
            "limits": ["Live collection samples completed runs; ongoing queue depth is not measured.",
                       "Job creation-to-start is scheduler wait; dependency delay is not included.",
                       "Step times can combine setup, compilation and tests; they do not isolate compiler time.",
                       "Job sums are unweighted runner minutes, not billing or wall time.",
                       "Actions timing does not establish functional completeness, escaped regressions or quarantine health."]}


def api(path):
    result = subprocess.run(["gh", "api", path], check=True, capture_output=True, text=True, timeout=60)
    return json.loads(result.stdout)


def collect(repository, limit, since=None):
    since = since or datetime.now(timezone.utc) - timedelta(days=7)
    runs = []
    page = 1
    while len(runs) < limit:
        query = urlencode({"event": "pull_request", "status": "completed", "per_page": 100, "page": page, "created": ">=" + since.isoformat()})
        batch = api(f"repos/{repository}/actions/workflows/ci.yml/runs?{query}")["workflow_runs"]
        if any(timestamp(run.get("created_at")) is None or timestamp(run["created_at"]) < since for run in batch):
            raise ValueError("GitHub returned runs outside the requested date range")
        runs.extend(batch[:limit - len(runs)])
        if len(batch) < 100:
            break
        page += 1
    samples = []
    for run in runs:
        attempt = run["run_attempt"]
        endpoint = f"repos/{repository}/actions/runs/{run['id']}/attempts/{attempt}"
        jobs, page = [], 1
        while True:
            batch = api(f"{endpoint}/jobs?per_page=100&page={page}")["jobs"]
            jobs.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        # A rerun or completion while collecting must not mix snapshots.
        observed = api(f"repos/{repository}/actions/runs/{run['id']}")
        if any(observed.get(key) != run.get(key) for key in ("run_attempt", "head_sha", "status", "conclusion")):
            raise ValueError(f"run {run['id']} changed during collection; collect a fresh snapshot")
        sample = {key: run.get(key) for key in ("id", "run_attempt", "head_sha", "created_at", "run_started_at", "status", "conclusion", "html_url")}
        sample["jobs"] = [{key: job.get(key) for key in ("id", "name", "created_at", "started_at", "completed_at", "status", "conclusion", "steps")} for job in jobs]
        samples.append(sample)
    return {"schema": 1, "repository": repository, "observed_at": datetime.now(timezone.utc).isoformat(), "created_since": since.isoformat(), "runs": samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default="rustfs/rustfs")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--input", type=Path, help="summarize a previously collected JSON sample")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.limit <= 200:
        parser.error("--limit must be between 1 and 200")
    snapshot = json.loads(args.input.read_text()) if args.input else collect(args.repository, args.limit)
    if snapshot.get("schema") != 1 or not isinstance(snapshot.get("runs"), list):
        parser.error("unsupported timing snapshot")
    snapshot["summary"] = summarize(snapshot["runs"])
    args.output.write_text(json.dumps(snapshot, indent=2) + "\n")
    print(json.dumps({key: value for key, value in snapshot["summary"].items() if key != "jobs"}, indent=2))


if __name__ == "__main__":
    main()
