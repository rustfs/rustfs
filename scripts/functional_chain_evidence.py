#!/usr/bin/env python3
"""Bind functional-suite evidence to one candidate and one chain attempt."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess

from resolve_functional_candidate import ROOT, positive, require, sha, validate_manifest

SUITES = ("upgrade", "s3", "kms", "tier", "storage", "heal", "pool", "security", "replication", "performance")
MAX_REPORT = 8 * 1024 * 1024


def current_chain():
    chain = json.loads(os.environ["CHAIN_MANIFEST"])
    require(isinstance(chain, dict) and set(chain) == {"schema", "run_id", "attempt", "workflow_sha", "testing_sha", "candidate"}, "invalid chain envelope")
    require(type(chain["schema"]) is int and chain["schema"] == 1, "unsupported chain schema")
    require(positive(chain["run_id"]) and positive(chain["attempt"]), "invalid chain run identity")
    require(chain["run_id"] == int(os.environ["GITHUB_RUN_ID"]) and chain["attempt"] == int(os.environ["GITHUB_RUN_ATTEMPT"]), "chain belongs to another run attempt; rerun all jobs")
    require(sha(chain["workflow_sha"]) and chain["workflow_sha"] == os.environ["GITHUB_SHA"], "chain workflow source mismatch")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    require(head == chain["workflow_sha"], "lane checkout differs from chain workflow source")
    require(chain["testing_sha"] == (ROOT / ".config/functional-script-revision.txt").read_text().strip() and sha(chain["testing_sha"]), "private script pin differs from chain")
    candidate = chain["candidate"]
    require(isinstance(candidate, dict) and set(candidate) == {"manifest", "artifact_id", "artifact_digest", "workflow_sha", "workflow_ref", "build_started_at"}, "invalid candidate envelope")
    manifest = candidate["manifest"]
    require(positive(candidate["artifact_id"]) and isinstance(candidate["artifact_digest"], str) and bool(re.fullmatch(r"sha256:[0-9a-f]{64}", candidate["artifact_digest"])), "invalid candidate artifact identity")
    require(sha(candidate["workflow_sha"]) and candidate["workflow_ref"] == "main", "candidate workflow source is invalid")
    validate_manifest(manifest, {"id": manifest["build_run_id"], "run_attempt": manifest["build_run_attempt"], "head_sha": candidate["workflow_sha"]})
    return chain


def consume(chain):
    manifest = chain["candidate"]["manifest"]
    with open(os.environ["GITHUB_ENV"], "a") as output:
        # Every pinned installer already verifies these hashes before dpkg.
        for key, value in (("RUSTFS_NIGHTLY_PACKAGE_URL", manifest["package_url"]),
                           ("PACKAGE_SHA256", manifest["package_sha256"]), ("TO_SHA256", manifest["package_sha256"])):
            output.write(key + "=" + value + "\n")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write("testing_sha=" + chain["testing_sha"] + "\n")


def report_counts(text, performance=False):
    counts = {"PASS": 0, "FAIL": 0, "SKIP": 0, "UNSUPPORTED": 0, "RUNNING": 0}
    if performance:
        rows = list(csv.DictReader(io.StringIO(text), delimiter="\t"))
        seen = set()
        for row in rows:
            key = (row.get("method"), row.get("size"))
            require(key[0] in ("get", "put", "mixed") and key[1] and key not in seen, "invalid or duplicate performance round")
            seen.add(key)
            fields = ("throughput", "obj_per_s", "req_avg", "req_p50")
            if key[0] != "mixed":
                fields += ("req_p90", "req_p99")
            require(all(isinstance(row.get(field), str) and row[field].strip() for field in fields), "missing benchmark metrics")
        counts["PASS"] = len(rows)
        return counts
    column = None
    for line in text.splitlines():
        if not line.startswith("|"):
            column = None
            continue
        cells = [cell.strip().strip("*") for cell in line.strip().strip("|").split("|")]
        for label in ("Status", "Result"):
            if cells[0] in ("ID", "Case", "Topology", "Step") and label in cells:
                column = cells.index(label)
                break
        else:
            if column is not None:
                require(len(cells) > column, "incomplete report row")
                status = cells[column]
                if re.fullmatch(r":?-+:?", status):
                    continue
                require(status in counts, "unknown case result")
                counts[status] += 1
    return counts


def record(chain, suite, report, output):
    require(suite in SUITES, "unknown suite")
    result = {"schema": 1, "suite": suite, "chain": chain, "valid": False, "counts": {}, "report_sha256": None}
    error = None
    try:
        private_head = subprocess.check_output(["git", "-C", "auto-testing", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        require(private_head == chain["testing_sha"], "suite used a different private script revision")
        require(report.is_file() and 0 < report.stat().st_size <= MAX_REPORT, "missing, empty or oversized report")
        data = report.read_bytes()
        result["report_sha256"] = hashlib.sha256(data).hexdigest()
        result["counts"] = report_counts(data.decode("utf-8"), suite == "performance")
        require(result["counts"]["PASS"] > 0 and not result["counts"]["FAIL"] and not result["counts"]["RUNNING"], "no passing executions or incomplete/failed cases")
        require(all(os.environ[key] == "success" for key in ("CHAIN_JOB_STATUS", "CHAIN_TEST_OUTCOME", "CHAIN_REPORT_OUTCOME")), "suite, report or job did not succeed")
        result["valid"] = True
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        error = exc
    output.parent.mkdir(parents=True, exist_ok=False)
    output.write_text(json.dumps(result, sort_keys=True) + "\n")
    if error:
        raise error


def aggregate(chain, directory, needs):
    require(set(needs) == set(SUITES), "aggregate is missing a required lane")
    require(all(value.get("result") == "success" for value in needs.values()), "a required suite did not succeed")
    require({path.name for path in directory.iterdir()} == {suite + ".json" for suite in SUITES}, "missing or unexpected suite evidence")
    records = [json.loads((directory / (suite + ".json")).read_text()) for suite in SUITES]
    validate_records(chain, records)
    return {"schema": 1, "chain": chain, "suites": records, "complete": True, "completed_at": datetime.now(timezone.utc).isoformat()}


def validate_records(chain, records):
    require(isinstance(records, list) and len(records) == len(SUITES), "missing suite evidence")
    require([record.get("suite") for record in records] == list(SUITES), "missing, duplicate or reordered suite evidence")
    for suite, result in zip(SUITES, records):
        require(type(result.get("schema")) is int and result["schema"] == 1 and result.get("suite") == suite and result.get("chain") == chain, "suite evidence identity mismatch")
        require(result.get("valid") is True and sha(result.get("report_sha256"), 64), "suite evidence is invalid")
        counts = result.get("counts", {})
        require(set(counts) == {"PASS", "FAIL", "SKIP", "UNSUPPORTED", "RUNNING"}, "missing suite counts")
        require(all(type(value) is int and value >= 0 for value in counts.values()) and counts["PASS"] > 0 and counts["FAIL"] == counts["RUNNING"] == 0, "suite has no complete passing evidence")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("consume", "record", "aggregate"))
    parser.add_argument("--suite", choices=SUITES)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    chain = current_chain()
    if args.mode == "consume":
        consume(chain)
    elif args.mode == "record":
        record(chain, args.suite, args.report, args.output)
    else:
        needs = json.loads(os.environ["CHAIN_NEEDS"])
        needs.pop("prepare", None)
        result = aggregate(chain, args.directory, needs)
        args.output.write_text(json.dumps(result, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
