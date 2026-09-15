#!/usr/bin/env python3
"""Resolve a nightly manifest from its exact GitHub build attempt and artifact."""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import zipfile

REPOSITORY = "rustfs/rustfs"
ROOT = Path(__file__).resolve().parents[1]
MAX_ARCHIVE = 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(value, length=40):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value) is not None


def positive(value):
    return type(value) is int and value > 0


def api(path, binary=False):
    result = subprocess.run(["gh", "api", path], check=True, capture_output=True, timeout=60)
    return result.stdout if binary else json.loads(result.stdout)


def validate_manifest(manifest, run):
    require(isinstance(manifest, dict), "manifest must be an object")
    common = {"schema", "source_sha", "build_run_id", "build_run_attempt", "package_url", "package_sha256"}
    version = manifest.get("schema")
    require(type(version) is int and version in (1, 2), "unsupported candidate schema")
    require(set(manifest) == (common if version == 1 else common | {"workflow_sha", "source_ref"}), "unexpected candidate fields")
    require(positive(manifest["build_run_id"]) and positive(manifest["build_run_attempt"]), "invalid build identity")
    require((manifest["build_run_id"], manifest["build_run_attempt"]) == (run["id"], run["run_attempt"]), "candidate belongs to another build attempt")
    require(sha(manifest["source_sha"]) and sha(manifest["package_sha256"], 64), "invalid candidate hash")
    if version == 1:
        require(manifest["source_sha"] == run["head_sha"], "legacy manifest cannot identify a different build source")
    else:
        require(manifest["workflow_sha"] == run["head_sha"] and sha(manifest["workflow_sha"]), "candidate workflow SHA differs from artifact provenance")
        require(isinstance(manifest["source_ref"], str) and bool(re.fullmatch(r"[A-Za-z0-9_./-]{1,200}", manifest["source_ref"])), "invalid build source ref")
    expected = (f"https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/{run['id']}/"
                f"{run['run_attempt']}/{manifest['package_sha256']}/rustfs.deb")
    require(manifest["package_url"] == expected, "package URL does not bind the run, attempt and checksum")
    return manifest


def read_json_artifact(archive, artifact, run, expected_name, member, max_json=16384):
    require(artifact.get("expired") is False, "candidate artifact expired")
    require(positive(artifact.get("id")), "invalid artifact id")
    require(artifact.get("name") == expected_name, "candidate artifact belongs to another attempt")
    provenance = artifact.get("workflow_run") or {}
    require(provenance.get("id") == run["id"] and provenance.get("head_sha") == run["head_sha"], "candidate artifact belongs to another workflow run")
    require(0 < len(archive) <= MAX_ARCHIVE and artifact.get("size_in_bytes") == len(archive), "candidate artifact size mismatch")
    require(artifact.get("digest") == "sha256:" + hashlib.sha256(archive).hexdigest(), "candidate artifact checksum mismatch")
    with zipfile.ZipFile(io.BytesIO(archive)) as source:
        files = source.infolist()
        require(len(files) == 1 and files[0].filename == member, "unexpected candidate archive members")
        require(0 < files[0].file_size <= max_json and not files[0].is_dir(), "candidate manifest too large or empty")
        manifest = json.loads(source.read(files[0]))
    return manifest


def read_manifest(archive, artifact, run):
    name = f"nightly-candidate-{run['id']}-{run['run_attempt']}"
    return validate_manifest(read_json_artifact(archive, artifact, run, name, name + ".json"), run)


def resolve(run_id, attempt):
    require(positive(run_id) and positive(attempt), "build run and attempt are required positive integers")
    endpoint = f"repos/{REPOSITORY}/actions/runs/{run_id}"
    run = api(f"{endpoint}/attempts/{attempt}")
    require(run.get("id") == run_id and run.get("run_attempt") == attempt, "GitHub returned a different build attempt")
    require(run.get("path") == ".github/workflows/nightly-gnu.yml" and run.get("head_branch") == "main", "candidate must come from nightly-gnu on main")
    require((run.get("head_repository") or {}).get("full_name") == REPOSITORY, "candidate came from another repository")
    require(run.get("event") in ("schedule", "workflow_dispatch") and run.get("status") == "completed" and run.get("conclusion") == "success", "nightly attempt has not completed successfully")
    name = f"nightly-candidate-{run_id}-{attempt}"
    artifacts = []
    for page in range(1, 11):
        batch = api(f"{endpoint}/artifacts?per_page=100&page={page}")["artifacts"]
        artifacts.extend(item for item in batch if item.get("name") == name)
        if len(batch) < 100:
            break
    else:
        raise ValueError("too many build artifacts to resolve safely")
    require(len(artifacts) == 1, "missing or ambiguous candidate artifact")
    artifact = artifacts[0]
    require(type(artifact.get("size_in_bytes")) is int and 0 < artifact["size_in_bytes"] <= MAX_ARCHIVE, "candidate artifact size is invalid")
    archive = api(f"repos/{REPOSITORY}/actions/artifacts/{artifact['id']}/zip", binary=True)
    manifest = read_manifest(archive, artifact, run)
    return {"manifest": manifest, "artifact_id": artifact["id"], "artifact_digest": artifact["digest"],
            "workflow_sha": run["head_sha"], "workflow_ref": run["head_branch"], "build_started_at": run["run_started_at"]}


def prepare():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    if os.environ["GITHUB_EVENT_NAME"] == "workflow_run":
        build = event["workflow_run"]
        require(build.get("event") == "schedule", "automatic chain requires a scheduled build")
        run_id, attempt = build["id"], build["run_attempt"]
    else:
        run_id, attempt = int(os.environ["BUILD_RUN_ID"]), int(os.environ["BUILD_RUN_ATTEMPT"])
    candidate = resolve(run_id, attempt)
    revision = (ROOT / ".config/functional-script-revision.txt").read_text().strip()
    require(sha(revision), "private test script revision must be pinned")
    chain = {"schema": 1, "run_id": int(os.environ["GITHUB_RUN_ID"]), "attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]),
             "workflow_sha": os.environ["GITHUB_SHA"], "testing_sha": revision, "candidate": candidate}
    encoded = json.dumps(chain, sort_keys=True, separators=(",", ":"))
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write("manifest=" + encoded + "\n")
    Path(os.environ["CHAIN_OUTPUT"]).write_text(encoded + "\n")


if __name__ == "__main__":
    prepare()
