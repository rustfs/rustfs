#!/usr/bin/env python3
"""Inspect GitHub runner registration and queued jobs without accessing the fleet."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

from resolve_functional_candidate import REPOSITORY, api, require


LABEL = "smoke-testing"
ACTIVE_STATUSES = ("queued", "in_progress", "pending", "waiting", "requested")


def validate_request(inputs):
    require(isinstance(inputs, dict) and inputs.get("validation_runner_inspect_only") is True,
            "runner inspection must be explicitly selected")
    defaults = {"validation_runner_inspect_only": True,
                "validation_retain_evidence_run_attempt": "1",
                "i05_diagnostic_branch": "source-plan", "validation_tools_run_attempt": "1"}
    require(all(value == defaults[key] if key in defaults else value in ("", False, None)
                for key, value in inputs.items()),
            "runner inspection cannot select a candidate, tools, fleet case or recovery")


def pages(path, field):
    rows = []
    for page in range(1, 11):
        separator = "&" if "?" in path else "?"
        batch = api(f"{path}{separator}per_page=100&page={page}").get(field)
        require(isinstance(batch, list), "GitHub inventory response is incomplete")
        rows.extend(batch)
        if len(batch) < 100:
            return rows
    raise ValueError("GitHub inventory exceeds the bounded inspection limit")


def collect(report):
    prefix = f"repos/{REPOSITORY}/actions"
    report["stage"] = "runner-inventory"
    runners = pages(prefix + "/runners", "runners")
    report["runners"] = [
        {"id": row["id"], "name": row["name"], "status": row["status"],
         "busy": row["busy"], "labels": [label["name"] for label in row["labels"]]}
        for row in runners if LABEL in {label["name"] for label in row["labels"]}]
    online = [row for row in report["runners"] if row["status"] == "online"]
    report["runnerAvailability"] = (
        "online-idle" if any(row["busy"] is False for row in online) else
        "online-busy" if online else "offline" if report["runners"] else "unregistered")
    report["stage"] = "active-workflows"
    runs = {}
    for status in ACTIVE_STATUSES:
        for row in pages(prefix + f"/runs?status={status}", "workflow_runs"):
            runs[row["id"]] = row
    report["fleetJobs"] = []
    report["runsWithoutJobs"] = []
    for run_id, row in sorted(runs.items()):
        report["stage"] = "workflow-jobs"
        jobs = pages(prefix + f"/runs/{run_id}/jobs", "jobs")
        identity = {"runId": run_id, "workflow": row["name"], "status": row["status"],
                    "createdAt": row["created_at"], "url": row["html_url"]}
        if not jobs:
            report["runsWithoutJobs"].append(identity)
        for job in jobs:
            if LABEL in job.get("labels", []) and job["status"] != "completed":
                report["fleetJobs"].append({**identity, "jobId": job["id"], "jobName": job["name"],
                    "jobStatus": job["status"], "runnerName": job.get("runner_name"),
                    "labels": job["labels"], "jobUrl": job["html_url"]})
    report.update(stage="complete", complete=True, activeWorkflowCount=len(runs))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), "retain the original runner inspection report")
    report = {"schemaVersion": 1, "repository": REPOSITORY, "label": LABEL,
              "observedAt": datetime.now(timezone.utc).isoformat(), "complete": False,
              "runId": os.environ.get("GITHUB_RUN_ID"), "runAttempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
              "workflowCommit": os.environ.get("GITHUB_SHA"),
              "runnerAvailability": "unknown", "stage": "request-validation",
              "fleetAccessed": False, "mutated": False,
              "cleanStateCertified": False, "sharedWorkflowIdleCertified": False,
              "canonicalCasePassClaimed": False}
    try:
        validate_request(json.loads(os.environ["RUNNER_INSPECTION_INPUTS"]))
        collect(report)
    except Exception as error:
        # Subprocess diagnostics can include configuration or credentials.
        report["errorType"] = type(error).__name__
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({key: report[key] for key in ("complete", "stage", "runnerAvailability")}))
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
