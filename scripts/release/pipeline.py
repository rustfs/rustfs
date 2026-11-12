#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Candidate orchestration. GitHub run identity is checked before trusting bytes."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import urllib.error
import zipfile

import candidate as c
import release_gate


def current_run(workflow):
    c.require(os.environ.get("GITHUB_REPOSITORY") == c.REPOSITORY and os.environ.get("GITHUB_REF") == "refs/heads/main",
              "release orchestration must run on the upstream main branch")
    run = c.validate_run(c.github(f"repos/{c.REPOSITORY}/actions/runs/{int(os.environ['GITHUB_RUN_ID'])}"), workflow, completed=False)
    c.require(run["head_sha"] == os.environ["GITHUB_SHA"], "workflow source changed")
    return run


def get_run(run_id, workflow):
    run = c.github(f"repos/{c.REPOSITORY}/actions/runs/{int(run_id)}")
    active = str(run_id) == os.environ.get("GITHUB_RUN_ID")
    return c.validate_run(run, workflow, completed=not active)


def manifest_env():
    return c.validate_inputs(json.loads(os.environ["CANDIDATE_MANIFEST"]))


def output_inputs(inputs):
    c.emit_output("manifest", json.dumps(inputs, separators=(",", ":")))
    for field in ("candidate_id", "source_sha", "workflow_sha", "version"):
        c.emit_output(field, inputs[field])


def source_ci(source, version):
    runs = c.github(f"repos/{c.REPOSITORY}/actions/workflows/ci.yml/runs?head_sha={source}&status=success&per_page=10")["workflow_runs"]
    required = {"Quick Checks", "Test and Lint", "End-to-End Tests (full merge gate)"}
    for run in runs:
        if (run.get("head_sha") != source or run.get("path") != ".github/workflows/ci.yml"
                or run.get("head_branch") not in ("main", f"release/{version}")
                or run.get("event") not in ("push", "workflow_dispatch")
                or run.get("conclusion") != "success" or run.get("status") != "completed"
                or (run.get("repository") or {}).get("full_name") != c.REPOSITORY):
            continue
        jobs = []
        for page in range(1, 11):
            batch = c.github(f"repos/{c.REPOSITORY}/actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100&page={page}")["jobs"]
            jobs.extend(batch)
            if len(batch) < 100:
                break
        selected = {name: [job for job in jobs if job.get("name") == name] for name in required}
        if all(len(found) == 1 and found[0].get("status") == "completed" and found[0].get("conclusion") == "success" for found in selected.values()):
            return {"run_id": run["id"], "run_attempt": run["run_attempt"]}
    raise ValueError("no complete exact-source CI found; run ci.yml on main or the release branch before creating a candidate")


def unused_version(version):
    # Reject a spent version before allocating native builders. A partial
    # publication resumes its existing seal instead of making a new candidate.
    for tag in (version, "v" + version):
        for endpoint in ("git/ref/tags", "releases/tags"):
            try:
                c.github(f"repos/{c.REPOSITORY}/{endpoint}/{tag}")
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    continue
                raise
            raise ValueError("version already has a tag or release; resume its approved seal or choose a new version")


def prepare(version, source, directory):
    run = current_run("release-candidate.yml")
    c.require(c.HEX40.fullmatch(source) and c.VERSION.fullmatch(version), "use a full source commit and exact version")
    directory.mkdir(parents=True, exist_ok=True)
    name = "release-inputs"
    exists = any(a["name"] == name for a in c.artifact_list(run["id"]))
    if exists:
        c.download_artifact(run, name, directory)
        inputs = c.validate_inputs(c.read_json(directory / "inputs.json"))
        c.require(inputs["source_sha"] == source and inputs["version"] == version and inputs["workflow_sha"] == run["head_sha"], "retry changed candidate inputs")
    else:
        unused_version(version)
        c.command(["git", "fetch", "--no-tags", "origin", source], cwd=c.ROOT)
        ci = source_ci(source, version)
        c.command(["rustup", "update", "stable", "--no-self-update"])
        toolchain = c.command(["rustc", "+stable", "--version"]).split()[1]
        inputs = c.freeze(version, source, run["head_sha"], toolchain, c.github("repos/rustfs/console/releases/latest"),
                          run["id"], int(os.environ["GITHUB_RUN_NUMBER"]), c.github("repos/rustfs/cli/releases/latest"),
                          c.github("repos/rustfs/rustfs/releases/latest"), ci)
        c.write_json(directory / "inputs.json", inputs)
    output_inputs(inputs)
    c.emit_output("reused", str(exists).lower())


def reuse_platform(target):
    inputs = manifest_env()
    workflow = os.environ["GITHUB_WORKFLOW_REF"].split("@", 1)[0].rsplit("/", 1)[-1]
    run = current_run(workflow)
    name = f"candidate-{inputs['candidate_id']}-{target}"
    exists = any(a["name"] == name for a in c.artifact_list(run["id"]))
    if exists:
        with tempfile.TemporaryDirectory() as directory:
            c.download_artifact(run, name, directory)
            records = [c.read_json(Path(directory) / f"record-{target}.json")]
            c.verify_records(inputs, records, directory, (target,))
            verify_evidence(inputs, Path(directory), {run["id"]: run})
    c.emit_output("reused", str(exists).lower())


def load_linux(run_id, expected, directory):
    run = get_run(run_id, "release-candidate.yml")
    c.download_artifact(run, "release-linux", directory)
    inputs = c.validate_inputs(c.read_json(directory / "inputs.json"))
    c.require(inputs["origin_run_id"] == run["id"] and inputs["workflow_sha"] == run["head_sha"], "candidate inputs belong to another run")
    c.require(c.HEX64.fullmatch(expected or "") and c.file_digest(directory / "linux-manifest.json") == expected,
              "Linux approval digest does not match the downloaded manifest")
    verify_seal_files(c.read_json(directory / "linux-manifest.json"), directory)
    check_recipe(inputs)
    return inputs, run


def check_recipe(inputs):
    # Unrelated main changes do not invalidate a frozen candidate. A changed
    # producer/verifier/publisher does, even when the product source is unchanged.
    c.require(c.recipe_digest(os.environ["GITHUB_SHA"]) == inputs["recipe_sha256"], "release recipe changed; create a new candidate")


def evidence_files(directory):
    return sorted(list(directory.glob("identity-*.json")) + list(directory.glob("upgrade-*.json")))


def verify_evidence(inputs, directory, runs):
    for path in evidence_files(directory):
        value = c.read_json(path)
        workflow = value.get("workflow", {})
        run = runs.get(int(workflow.get("run_id", 0)))
        c.require(run is not None and workflow.get("repository") == c.REPOSITORY
                  and workflow.get("workflow_id") == run["path"].rsplit("/", 1)[-1]
                  and workflow.get("workflow_sha") == run["head_sha"]
                  and type(workflow.get("run_attempt")) is int and 0 < workflow["run_attempt"] <= run["run_attempt"],
                  "receipt was not produced by the downloaded trusted run")
        c.require(value.get("harness_commit") == inputs["workflow_sha"], "receipt harness changed")
        for item in value.get("evidence", []):
            name = item.get("path", "")
            c.require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,200}", name), "unsafe evidence path")
            log = directory / name
            c.require(log.is_file() and not log.is_symlink() and log.stat().st_size == item.get("size")
                      and c.file_digest(log) == item.get("sha256"), "receipt evidence was removed or substituted")


def certify(inputs, directory, runs, scope):
    records = [c.read_json(directory / f"record-{target}.json") for target in c.targets("linux" if scope == "linux" else "all")]
    candidate = c.certification_candidate(inputs, records, directory, c.targets("linux" if scope == "linux" else "all"))
    plan = c.read_json(c.ROOT / ".config/release-validation-plan.json")
    c.require(c.digest(plan) == inputs["plan_sha256"], "validation plan changed")
    verify_evidence(inputs, directory, runs)
    with tempfile.TemporaryDirectory() as temp:
        temp = Path(temp)
        results = temp / "results"
        results.mkdir()
        for path in evidence_files(directory):
            shutil.copyfile(path, results / path.name)
        c.write_json(temp / "candidate.json", candidate)
        c.write_json(temp / "plan.json", plan)
        status = release_gate.run_validate(temp / "plan.json", temp / "candidate.json", results, temp / "gate", scope)
        summary = c.read_json(temp / "gate/release-gate-summary.json")
        c.require(status == 0, "certification failed: " + json.dumps(summary["findings"]))
    return candidate, summary


def file_records(directory, excluded=()):
    result = []
    for path in sorted(directory.iterdir()):
        if path.name in excluded:
            continue
        c.require(path.is_file() and not path.is_symlink() and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,200}", path.name), "unexpected staged file")
        result.append({"name": path.name, "sha256": c.file_digest(path), "size": path.stat().st_size})
    return result


def verify_seal_files(seal, directory):
    c.require(isinstance(seal, dict) and seal.get("schema") == 1 and seal.get("stage") in ("linux", "complete"), "invalid release seal")
    filename = "linux-manifest.json" if seal["stage"] == "linux" else "release-manifest.json"
    c.require(seal.get("files") == file_records(directory, (filename,)), "sealed files changed, are missing, or contain extras")


def aliases(inputs):
    return {f"rustfs-{target}-latest.zip": f"rustfs-{target}-v{inputs['version']}.zip" for target in c.TARGETS}


def linux_seal(directory):
    run = current_run("release-candidate.yml")
    if any(a["name"] == "release-linux" for a in c.artifact_list(run["id"])):
        c.download_artifact(run, "release-linux", directory)
        inputs = c.validate_inputs(c.read_json(directory / "inputs.json"))
        verify_seal_files(c.read_json(directory / "linux-manifest.json"), directory)
        certify(inputs, directory, {run["id"]: run}, "linux")
        c.emit_output("reused", "true")
        c.emit_output("manifest_sha256", c.file_digest(directory / "linux-manifest.json"))
        return
    c.download_artifact(run, "release-inputs", directory)
    inputs = c.validate_inputs(c.read_json(directory / "inputs.json"))
    for target in c.LINUX:
        c.download_artifact(run, f"candidate-{inputs['candidate_id']}-{target}", directory)
    c.download_artifact(run, "release-upgrade", directory)
    _, summary = certify(inputs, directory, {run["id"]: run}, "linux")
    c.require(not summary["publish_allowed"], "partial certification must not grant publication authority")
    c.write_json(directory / "linux-manifest.json", {"schema": 1, "stage": "linux", "inputs_sha256": c.digest(inputs),
                                                   "files": file_records(directory), "summary": summary})
    c.emit_output("manifest_sha256", c.file_digest(directory / "linux-manifest.json"))


def check_acceptance(url):
    c.require(re.fullmatch(r"https://github\.com/rustfs/backlog/issues/[1-9][0-9]*(?:#issuecomment-[1-9][0-9]*)?", url),
              "acceptance must link to a RustFS backlog report")


def complete_prepare(run_id, expected, report, directory):
    current_run("release-complete.yml")
    check_acceptance(report)
    inputs, run = load_linux(run_id, expected, directory)
    certify(inputs, directory, {run["id"]: run}, "linux")
    output_inputs(inputs)


def complete_seal(run_id, expected, report, directory):
    current = current_run("release-complete.yml")
    if any(a["name"] == "release-seal" for a in c.artifact_list(current["id"])):
        c.download_artifact(current, "release-seal", directory)
        inputs = c.validate_inputs(c.read_json(directory / "inputs.json"))
        seal = c.read_json(directory / "release-manifest.json")
        verify_seal_files(seal, directory)
        c.require(inputs["origin_run_id"] == run_id and seal["acceptance"] == {"linux_manifest_sha256": expected, "report": report}, "completion retry changed acceptance")
        origin = get_run(run_id, "release-candidate.yml")
        certify(inputs, directory, {origin["id"]: origin, current["id"]: current}, "full")
        verify_deliveries(inputs, directory)
        c.emit_output("reused", "true")
        c.emit_output("manifest_sha256", c.file_digest(directory / "release-manifest.json"))
        return
    inputs, origin = load_linux(run_id, expected, directory)
    check_acceptance(report)
    (directory / "linux-manifest.json").unlink()
    for target in c.DESKTOP:
        c.download_artifact(current, f"candidate-{inputs['candidate_id']}-{target}", directory)
    for target in c.LINUX:
        c.download_artifact(current, f"packages-{target}", directory)
    for variant in ("musl", "gnu"):
        c.download_artifact(current, f"image-{variant}", directory)
    c.download_artifact(current, "helm-package", directory)
    candidate, summary = certify(inputs, directory, {origin["id"]: origin, current["id"]: current}, "full")
    verify_deliveries(inputs, directory)
    # Persist the small proof bundle with the public release; Actions retention
    # must not be the only way to audit an already published version.
    evidence = directory / "release-evidence.zip"
    with zipfile.ZipFile(evidence, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(directory.iterdir()):
            if path.suffix in (".json", ".log", ".sarif"):
                archive.write(path, path.name)
        archive.writestr("certification.json", json.dumps(summary, indent=2, sort_keys=True))
        archive.writestr("validation-plan.json", (c.ROOT / ".config/release-validation-plan.json").read_bytes())
    with tempfile.TemporaryDirectory() as temp:
        lock = Path(temp) / "Cargo.lock"
        lock.write_bytes(c.git_file(inputs["source_sha"], "Cargo.lock"))
        c.require(c.file_digest(lock) == inputs["lock_sha256"], "SBOM dependency lock changed")
        subprocess.run(["python3", str(c.ROOT / "scripts/security/generate_release_supply_chain_assets.py"),
                        "--asset-dir", str(directory), "--version", inputs["version"], "--tag", inputs["version"],
                        "--build-type", candidate["release_profile"], "--repository", c.REPOSITORY,
                        "--ref", "refs/heads/main", "--sha", inputs["source_sha"], "--run-id", str(current["id"]),
                        "--run-attempt", str(current["run_attempt"]), "--cargo-lock", str(lock),
                        "--workflow", "release-complete.yml"], check=True, timeout=60)
    provenance = directory / f"rustfs-{inputs['version']}.provenance.json"
    value = c.read_json(provenance)
    value["predicate"]["buildDefinition"]["externalParameters"]["candidateInputs"] = inputs
    provenance.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    assets = {p.name: p for p in directory.iterdir() if p.suffix in (".zip", ".deb", ".rpm", ".tgz") or p.name.endswith((".sbom.cdx.json", ".provenance.json"))}
    assets.update({name: directory / source for name, source in aliases(inputs).items()})
    for algorithm in ("sha256", "sha512"):
        checksums = []
        for name, path in sorted(assets.items()):
            with path.open("rb") as stream:
                checksum = hashlib.file_digest(stream, algorithm).hexdigest()
            checksums.append(f"{checksum}  {name}")
        (directory / f"{algorithm.upper()}SUMS").write_text("\n".join(checksums) + "\n")
    c.write_json(directory / "release-manifest.json", {
        "schema": 1, "stage": "complete", "inputs_sha256": c.digest(inputs), "candidate": candidate,
        "summary": summary, "files": file_records(directory), "complete_run_id": current["id"],
        "aliases": aliases(inputs),
        "acceptance": {"linux_manifest_sha256": expected, "report": report},
    })
    c.emit_output("manifest_sha256", c.file_digest(directory / "release-manifest.json"))


def verify_deliveries(inputs, directory):
    chart = c.read_json(directory / "chart.json")
    c.require(chart.get("version") == inputs["version"] and chart.get("inputs_sha256") == c.digest(inputs)
              and chart.get("name") == f"rustfs-{inputs['version']}.tgz", "chart identity changed")
    path = directory / chart["name"]
    c.require(path.stat().st_size == chart["size"] and c.file_digest(path) == chart["sha256"], "chart bytes changed")
    for target in c.LINUX:
        binary = c.read_json(directory / f"record-{target}.json")["binary_sha256"]
        record = c.read_json(directory / f"packages-{target}.json")
        c.require(record.get("inputs_sha256") == c.digest(inputs) and record.get("binary_sha256") == binary, "package wraps a different candidate")
        c.require(len(record.get("files", [])) == 2 and {Path(f["name"]).suffix for f in record["files"]} == {".deb", ".rpm"}, "missing package format")
        for item in record["files"]:
            path = directory / item["name"]
            c.require(path.name == item["name"] and path.is_file() and path.stat().st_size == item["size"] and c.file_digest(path) == item["sha256"], "package changed")
    for variant in ("musl", "gnu"):
        record = c.read_json(directory / f"image-{variant}.json")
        c.require(record.get("inputs_sha256") == c.digest(inputs), "image inputs changed")
        expected = {arch: c.read_json(directory / f"record-linux-{arch}-{variant}.json")["binary_sha256"] for arch in ("x86_64", "aarch64")}
        c.require(record.get("binaries") == expected, "image contains an unverified binary")
        path = directory / f"image-{variant}.tar"
        c.require(c.file_digest(path) == record.get("sha256") and path.stat().st_size == record.get("size")
                  and re.fullmatch(r"sha256:[0-9a-f]{64}", record.get("digest", "")), "image archive changed")
        c.require(record.get("smoke") == "pass" and record.get("scan") == "completed", "image verification did not finish")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--version", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--directory", type=Path, required=True)
    p = sub.add_parser("reuse-platform")
    p.add_argument("--target", choices=c.TARGETS, required=True)
    p = sub.add_parser("linux-seal")
    p.add_argument("--directory", type=Path, required=True)
    for name in ("complete-prepare", "complete-seal"):
        p = sub.add_parser(name)
        p.add_argument("--run-id", type=int, required=True)
        p.add_argument("--digest", required=True)
        p.add_argument("--report", required=True)
        p.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if args.mode == "prepare":
        prepare(args.version, args.source, args.directory)
    elif args.mode == "reuse-platform":
        reuse_platform(args.target)
    elif args.mode == "linux-seal":
        linux_seal(args.directory)
    elif args.mode == "complete-prepare":
        complete_prepare(args.run_id, args.digest, args.report, args.directory)
    else:
        complete_seal(args.run_id, args.digest, args.report, args.directory)


if __name__ == "__main__":
    main()
