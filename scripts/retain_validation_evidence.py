#!/usr/bin/env python3
"""Admit an interrupted run's existing runner-local evidence for read-only upload."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

from resolve_functional_candidate import REPOSITORY, api, require


MAX_FILES = 200000
MAX_BYTES = 4 * 1024**3


def stable(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid,
            info.st_nlink, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def inventory(root, *, copy_to=None):
    files, total, entries = [], 0, 0
    device = root.lstat().st_dev
    if copy_to is not None:
        require(copy_to.is_absolute() and copy_to.parent.resolve(strict=True) == copy_to.parent,
                "evidence copy parent is not canonical")
        copy_to.mkdir(mode=0o700, exist_ok=False)

    def visit(fd, prefix):
        nonlocal total, entries
        before = os.fstat(fd)
        require(stat.S_ISDIR(before.st_mode) and before.st_uid == os.getuid() and before.st_dev == device,
                "evidence directory has a different owner or type")
        names = sorted(os.listdir(fd))
        require(len(names) <= MAX_FILES, "evidence directory exceeds the entry budget")
        for name in names:
            entries += 1
            require(entries <= MAX_FILES, "evidence exceeds the entry budget")
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            path = prefix + name
            require(info.st_uid == os.getuid() and info.st_dev == device, "evidence entry has a different owner or device")
            if stat.S_ISDIR(info.st_mode):
                if copy_to is not None:
                    (copy_to / path).mkdir(mode=0o700, exist_ok=False)
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    require(stable(os.fstat(child)) == stable(info), "evidence directory changed before inspection")
                    visit(child, path + "/")
                finally:
                    os.close(child)
            else:
                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                        and len(files) < MAX_FILES and total + info.st_size <= MAX_BYTES,
                        "evidence contains links, special files or exceeds its byte/file budget")
                child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                target = None
                try:
                    require(stable(os.fstat(child)) == stable(info), "evidence file changed before inspection")
                    if copy_to is not None:
                        target = os.open(copy_to / path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                    digest, size = hashlib.sha256(), 0
                    while chunk := os.read(child, 1024**2):
                        size += len(chunk)
                        require(size <= info.st_size, "evidence file grew during inspection")
                        digest.update(chunk)
                        if target is not None:
                            remaining = memoryview(chunk)
                            while remaining:
                                written = os.write(target, remaining)
                                require(written > 0, "evidence copy made no progress")
                                remaining = remaining[written:]
                    require(size == info.st_size and stable(os.fstat(child)) == stable(info),
                            "evidence file changed during inspection")
                finally:
                    os.close(child)
                    if target is not None:
                        os.close(target)
                total += size
                files.append({"path": path, "size": size, "sha256": digest.hexdigest()})
            require(stable(os.stat(name, dir_fd=fd, follow_symlinks=False)) == stable(info),
                    "evidence entry changed during inspection")
        require(sorted(os.listdir(fd)) == names
                and stable(os.fstat(fd)) == stable(before), "evidence population changed during inspection")

    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        visit(fd, "")
    finally:
        os.close(fd)
    return files, total


def prepare(env, report):
    inputs = json.loads(env["EVIDENCE_RETAIN_INPUTS"])
    source = inputs.get("validation_retain_evidence_run_id")
    attempt = inputs.get("validation_retain_evidence_run_attempt", "1")
    require(isinstance(source, str) and re.fullmatch(r"[1-9][0-9]{0,19}", source), "exact source run ID required")
    require(isinstance(attempt, str) and re.fullmatch(r"[1-9][0-9]{0,9}", attempt), "exact source run attempt required")
    defaults = {"validation_retain_evidence_run_id": source, "validation_plan": "nightly",
                "validation_retain_evidence_run_attempt": attempt,
                "i05_diagnostic_branch": "source-plan", "validation_tools_run_attempt": "1"}
    require(all(value == defaults[key] if key in defaults else value in ("", False, None)
                for key, value in inputs.items()), "evidence retention is exclusive with all execution modes")
    require(source != env["GITHUB_RUN_ID"], "cannot retain the running collection job")
    report["stage"] = "source-workflow"
    run = api(f"repos/{REPOSITORY}/actions/runs/{source}/attempts/{attempt}")
    require(run.get("id") == int(source) and type(run.get("run_attempt")) is int and run["run_attempt"] == int(attempt)
            and run.get("repository", {}).get("full_name") == REPOSITORY
            and run.get("status") == "completed" and run.get("conclusion") in {"failure", "cancelled", "timed_out"}
            and run.get("event") == "workflow_dispatch"
            and run.get("path") == ".github/workflows/rustfs-functional-chain.yml",
            "source is not one completed interrupted validation workflow")
    jobs = api(f"repos/{REPOSITORY}/actions/runs/{source}/attempts/{run['run_attempt']}/jobs?per_page=100")
    require(jobs.get("total_count", 101) <= 100
            and len([job for job in jobs.get("jobs", []) if job.get("name") == "validation"
                     and job.get("runner_name") == env["RUNNER_NAME"]]) == 1,
            "source validation did not execute on this runner")
    report["sourceRun"] = {key: run[key] for key in ("id", "run_attempt", "head_sha", "status", "conclusion", "html_url")}
    report["stage"] = "owned-persistent-root"
    temporary = Path(env["RUNNER_TEMP"])
    require(temporary.is_absolute() and temporary.resolve(strict=True) == temporary
            and temporary.is_dir(), "runner temporary path is not canonical")
    parent = temporary.parent / "rustfs-validation-evidence"
    root = parent / f"rustfs-validation-{source}-{run['run_attempt']}"
    for path in (parent, root):
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
                and stat.S_IMODE(info.st_mode) == 0o700 and path.resolve(strict=True) == path,
                "persistent evidence must be an existing private runner-owned directory")
    report["stage"] = "stable-file-inventory"
    files, size = inventory(root)
    marker = root / "runner-storage-preflight.json"
    require(any(row["path"] == marker.name for row in files) and marker.stat().st_size < 1024**2,
            "source storage provenance is absent or oversized")
    storage = json.loads(marker.read_text())
    require(storage.get("runId") == source and storage.get("runAttempt") == str(run["run_attempt"])
            and storage.get("evidenceRoot") == str(root)
            and storage.get("persistentOutsideRunnerTemp") is True,
            "evidence storage provenance belongs to another run or root")
    require(not any(character in str(root) for character in ("\n", "\r")), "unsafe evidence export path")
    report.update(complete=True, stage="complete", evidenceRoot=str(root), files=files, bytes=size)
    return root


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = {"schemaVersion": 1, "collectionRunId": os.environ["GITHUB_RUN_ID"],
              "collectionAttempt": os.environ["GITHUB_RUN_ATTEMPT"], "complete": False,
              "observedAt": datetime.now(timezone.utc).isoformat(),
              "stage": "request-validation", "fleetAccessed": False, "originalEvidenceModified": False,
              "sourceVerdictRewritten": False, "canonicalCasePassClaimed": False}
    root = None
    try:
        root = prepare(os.environ, report)
    except Exception as error:
        report["errorType"] = type(error).__name__
    args.report.parent.mkdir(parents=True, exist_ok=True)
    with args.report.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    if root is not None:
        with Path(os.environ["GITHUB_ENV"]).open("a") as stream:
            stream.write("VALIDATION_RETAIN_ROOT=" + str(root) + "\n")
            stream.write("VALIDATION_RETAIN_SOURCE_RUN=" + str(report["sourceRun"]["id"]) + "\n")
            stream.write("VALIDATION_RETAIN_SOURCE_ATTEMPT=" + str(report["sourceRun"]["run_attempt"]) + "\n")
    print(json.dumps({key: report[key] for key in ("complete", "stage")}))
    return 0 if root is not None else 1


if __name__ == "__main__":
    sys.exit(main())
