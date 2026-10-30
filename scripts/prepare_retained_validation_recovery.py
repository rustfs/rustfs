#!/usr/bin/env python3
"""Copy verified local evidence into a recovery input without claiming an old artifact."""

import argparse
import json
import os
from pathlib import Path
import re
import sys

from resolve_functional_candidate import REPOSITORY, api, require
from retain_validation_evidence import inventory, prepare


def stage(env, report):
    source = env["FAILED_RUN_ID"]
    require(re.fullmatch(r"[1-9][0-9]{0,19}", source), "exact failed run required")
    run = api(f"repos/{REPOSITORY}/actions/runs/{source}")
    require(run.get("head_branch") == env["GITHUB_REF_NAME"]
            and type(run.get("run_attempt")) is int and run["run_attempt"] > 0,
            "local recovery source branch or attempt differs")
    root = prepare({**env, "EVIDENCE_RETAIN_INPUTS": json.dumps({
        "validation_retain_evidence_run_id": source,
        "validation_retain_evidence_run_attempt": str(run["run_attempt"])})}, report)
    report.update(complete=False, stage="stable-recovery-copy")
    destination = Path(env["VALIDATION_ROOT"]) / "failed-artifact"
    require(destination.parent.is_absolute() and destination.parent.resolve(strict=True) == destination.parent,
            "recovery evidence root is not canonical")
    destination.mkdir(mode=0o700, exist_ok=False)
    copied = destination / "retained-source-evidence"
    files, size = inventory(root, copy_to=copied)
    require(files == report["files"] and size == report["bytes"]
            and inventory(copied) == (files, size), "retained evidence changed during recovery copy")
    report.update(complete=True, stage="complete", recoveryInput=str(copied))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = {"schemaVersion": 1, "collectionRunId": os.environ["GITHUB_RUN_ID"],
              "collectionAttempt": os.environ["GITHUB_RUN_ATTEMPT"], "complete": False,
              "stage": "source-workflow",
              "collectionKind": "runner-local-recovery-input-copy", "sourceArtifactClaimed": False,
              "fleetAccessed": False, "originalEvidenceModified": False,
              "sourceVerdictRewritten": False, "canonicalCasePassClaimed": False}
    try:
        stage(os.environ, report)
    except Exception as error:
        report.update(complete=False, errorType=type(error).__name__)
    with args.report.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({key: report[key] for key in ("complete", "stage")}))
    return 0 if report["complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
