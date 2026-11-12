#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Frozen release inputs and artifact identity; publication never builds binaries."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
from urllib.parse import urlsplit
import zipfile

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "rustfs/rustfs"
SCHEMA = 1
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:alpha|beta|rc)\.(0|[1-9][0-9]*))?\Z")
TARGETS = ("linux-x86_64-musl", "linux-aarch64-musl", "linux-x86_64-gnu", "linux-aarch64-gnu", "macos-aarch64", "windows-x86_64")
LINUX = TARGETS[:4]
DESKTOP = TARGETS[4:]
MAX_MEMBER = 2 * 1024**3
RECIPE_PATHS = (
    ".github/workflows/build.yml",
    ".github/workflows/release-candidate.yml",
    ".github/workflows/release-complete.yml",
    ".github/workflows/release-publish.yml",
    ".github/workflows/docker.yml",
    ".github/workflows/package.yml",
    ".github/actions/setup/action.yml",
    ".github/actions/setup/download_release_asset.py",
    "scripts/release/candidate.py",
    "scripts/release/pipeline.py",
    "scripts/release/verify.py",
    "scripts/release/deliver.py",
    "scripts/release/publish.py",
    "scripts/release/release_gate.py",
    "scripts/release/package_versions.sh",
    "scripts/release/package_service_scripts.sh",
    "scripts/release/build_deb.sh",
    "scripts/release/build_rpm.sh",
    "scripts/release/check_glibc.py",
    ".config/release-validation-plan.json",
    "Dockerfile", "Dockerfile.glibc", "entrypoint.sh", "deploy/build/rustfs.service",
    ".github/workflows/helm-package.yml", "scripts/test_helm_templates.sh",
    "scripts/security/generate_release_supply_chain_assets.py",
    ".github/workflows/ci.yml", "scripts/ci_gate.py",
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def stream_digest(stream):
    result = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        result.update(chunk)
    return result.hexdigest()


def file_digest(path):
    with Path(path).open("rb") as stream:
        return stream_digest(stream)


def read_json(path):
    path = Path(path)
    require(path.is_file() and path.stat().st_size <= 4 * 1024**2, "missing or oversized JSON record")
    def unique(pairs):
        value = {}
        for name, item in pairs:
            require(name not in value, "duplicate JSON field")
            value[name] = item
        return value
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique)


def write_json(path, value):
    """Existing identity records are immutable, including across job retries."""
    path = Path(path)
    if path.exists():
        require(read_json(path) == value, "refusing to replace an existing identity record")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, ensure_ascii=False)
        stream.write("\n")


def command(args, *, input_text=None, cwd=None):
    return subprocess.run(args, input=input_text, text=True, capture_output=True, check=True, cwd=cwd, timeout=120).stdout.strip()


class ArtifactRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, url):
        require(urlsplit(url).scheme == "https", "artifact redirect must use HTTPS")
        redirected = super().redirect_request(request, fp, code, message, headers, url)
        if redirected is not None and urlsplit(request.full_url).netloc != urlsplit(url).netloc:
            redirected.remove_header("Authorization")
        return redirected


def github_stream(path):
    require(path.startswith("repos/") and ":" not in path and not path.startswith("/"), "invalid GitHub API path")
    token = os.environ.get("GH_TOKEN") or command(["gh", "auth", "token"])
    request = urllib.request.Request("https://api.github.com/" + path,
                                     headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
                                              "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "rustfs-release"})
    return urllib.request.build_opener(ArtifactRedirect()).open(request, timeout=600)


def github(path):
    with github_stream(path) as response:
        return json.load(response)


def git_file(revision, path, root=ROOT):
    require(HEX40.fullmatch(revision) is not None, "source must be a full commit SHA")
    return subprocess.check_output(["git", "show", f"{revision}:{path}"], cwd=root, timeout=60)


def recipe_digest(revision, root=ROOT):
    files = {path: hashlib.sha256(git_file(revision, path, root)).hexdigest() for path in RECIPE_PATHS}
    for directory in ("bootstrap", "helm"):
        paths = command(["git", "ls-tree", "-r", "--name-only", revision, "--", directory], cwd=root).splitlines()
        require(bool(paths), "missing release recipe directory")
        files.update({path: hashlib.sha256(git_file(revision, path, root)).hexdigest() for path in paths})
    return digest(files)


def console_asset(release):
    require(isinstance(release, dict) and release.get("draft") is False and release.get("prerelease") is False,
            "Console must be a published stable release")
    tag = release.get("tag_name")
    require(isinstance(tag, str) and re.fullmatch(r"v?[0-9]+\.[0-9]+\.[0-9]+", tag), "invalid Console version")
    assets = [asset for asset in release.get("assets", [])
              if re.fullmatch(r"rustfs-console-[A-Za-z0-9._-]+\.zip", asset.get("name", ""))]
    require(len(assets) == 1, "Console must have exactly one unambiguous ZIP asset")
    asset = assets[0]
    checksum = asset.get("digest", "")
    require(isinstance(checksum, str) and checksum.startswith("sha256:") and HEX64.fullmatch(checksum[7:]),
            "Console asset must have a SHA-256 digest")
    expected = f"https://github.com/rustfs/console/releases/download/{tag}/{asset['name']}"
    require(asset.get("browser_download_url") == expected, "Console URL does not match its release identity")
    return {"tag": tag, "name": asset["name"], "url": expected, "sha256": checksum[7:]}


def release_asset(release, repository, name):
    require(release.get("draft") is False and release.get("prerelease") is False, "dependency must be a published stable release")
    tag = release.get("tag_name", "")
    require(re.fullmatch(r"v?[0-9]+\.[0-9]+\.[0-9]+", tag), "invalid dependency tag")
    matches = [a for a in release.get("assets", []) if a.get("name") == name]
    require(len(matches) == 1, "missing or ambiguous pinned dependency")
    asset = matches[0]
    url = f"https://github.com/{repository}/releases/download/{tag}/{name}"
    require(asset.get("browser_download_url") == url and re.fullmatch(r"sha256:[0-9a-f]{64}", asset.get("digest", "")), "invalid dependency identity")
    return {"version": tag, "name": name, "url": url, "sha256": asset["digest"][7:]}


def validate_dependency(asset, repository):
    require(isinstance(asset, dict) and set(asset) == {"version", "name", "url", "sha256"}, "invalid dependency record")
    require(re.fullmatch(r"[A-Za-z0-9._-]+", asset["name"]), "unsafe dependency name")
    normalized = release_asset({"draft": False, "prerelease": False, "tag_name": asset["version"],
                                "assets": [{"name": asset["name"], "browser_download_url": asset["url"],
                                            "digest": "sha256:" + asset["sha256"]}]}, repository, asset["name"])
    require(normalized == asset, "dependency record changed")


def validate_inputs(value):
    require(isinstance(value, dict), "candidate inputs must be an object")
    fields = {"schema", "candidate_id", "version", "source_sha", "workflow_sha", "recipe_sha256", "lock_sha256",
              "plan_sha256", "toolchain", "console", "cli", "previous", "ci", "origin_run_id", "origin_run_number"}
    require(set(value) == fields and type(value["schema"]) is int and value["schema"] == SCHEMA,
            "unsupported candidate input schema")
    require(isinstance(value["version"], str) and VERSION.fullmatch(value["version"]), "invalid release version")
    for name in ("source_sha", "workflow_sha"):
        require(isinstance(value[name], str) and HEX40.fullmatch(value[name]), f"invalid {name}")
    for name in ("recipe_sha256", "lock_sha256", "plan_sha256"):
        require(isinstance(value[name], str) and HEX64.fullmatch(value[name]), f"invalid {name}")
    for name in ("origin_run_id", "origin_run_number"):
        require(type(value[name]) is int and value[name] > 0, f"invalid {name}")
    require(value["candidate_id"] == f"{value['version']}-preview.{value['origin_run_number']}", "candidate name differs from its immutable run identity")
    require(isinstance(value["toolchain"], str) and re.fullmatch(r"[1-9][0-9]*\.[0-9]+\.[0-9]+", value["toolchain"]),
            "candidate toolchain must be an exact stable Rust version")
    console = value["console"]
    require(isinstance(console, dict) and set(console) == {"tag", "name", "url", "sha256"}, "invalid Console identity")
    normalized = console_asset({"draft": False, "prerelease": False, "tag_name": console["tag"],
                                "assets": [{"name": console["name"], "browser_download_url": console["url"],
                                            "digest": "sha256:" + str(console["sha256"])}]})
    require(normalized == console, "invalid pinned Console asset")
    require(isinstance(value["cli"], dict) and set(value["cli"]) == set(LINUX), "missing CLI platform")
    for asset in value["cli"].values():
        validate_dependency(asset, "rustfs/cli")
    validate_dependency(value["previous"], REPOSITORY)
    require(value["previous"]["version"].lstrip("v") != value["version"], "previous release must differ from the candidate")
    require(isinstance(value["ci"], dict) and set(value["ci"]) == {"run_id", "run_attempt"}
            and all(type(item) is int and item > 0 for item in value["ci"].values()), "invalid exact-source CI identity")
    return value


def freeze(version, source_sha, workflow_sha, toolchain, release, run_id, run_number, cli, previous, ci, root=ROOT):
    require(VERSION.fullmatch(version) is not None, "invalid release version")
    source = tomllib.loads(git_file(source_sha, "Cargo.toml", root).decode())
    require(source["workspace"]["package"]["version"] == version, "Cargo workspace version differs from the target")
    recipe = recipe_digest(workflow_sha, root)
    require(recipe_digest(source_sha, root) == recipe, "candidate must contain the reviewed release recipe")
    plan = json.loads(git_file(workflow_sha, ".config/release-validation-plan.json", root))
    value = {"schema": SCHEMA, "version": version, "candidate_id": f"{version}-preview.{run_number}",
             "source_sha": source_sha, "workflow_sha": workflow_sha, "recipe_sha256": recipe,
             "lock_sha256": hashlib.sha256(git_file(source_sha, "Cargo.lock", root)).hexdigest(),
             "plan_sha256": digest(plan), "toolchain": toolchain, "console": console_asset(release),
             "origin_run_id": run_id, "origin_run_number": run_number, "ci": ci,
             "cli": {target: release_asset(cli, "rustfs/cli", f"rustfs-cli-linux-{'amd64' if 'x86_64' in target else 'arm64'}{'-gnu' if target.endswith('gnu') else ''}-{cli['tag_name']}.tar.gz") for target in LINUX},
             "previous": release_asset(previous, REPOSITORY, f"rustfs-linux-x86_64-gnu-v{previous['tag_name'].lstrip('v')}.zip")}
    return validate_inputs(value)


def targets(selection):
    if selection == "linux":
        return LINUX
    if selection == "desktop":
        return DESKTOP
    if selection == "all":
        return TARGETS
    selected = tuple(selection.split(","))
    require(selected and len(set(selected)) == len(selected) and all(item in TARGETS for item in selected),
            "unknown or duplicate build targets")
    return selected


def archive_record(path, target, inputs):
    validate_inputs(inputs)
    require(target in TARGETS, "unknown binary platform")
    path = Path(path)
    name = f"rustfs-{target}-v{inputs['version']}.zip"
    require(path.name == name and path.is_file() and not path.is_symlink(), "archive name does not match the target version")
    binary = "rustfs.exe" if target == "windows-x86_64" else "rustfs"
    expected = {binary}
    if target.endswith("-gnu"):
        expected.update(("rustfs.cpu-symbol-catalog.json", "rustfs.cpu-symbol-catalog.sha256"))
    with zipfile.ZipFile(path) as archive:
        members = archive.infolist()
        require(len(members) == len(expected) and {m.filename for m in members} == expected,
                "unexpected or duplicate release archive member")
        for member in members:
            require(not member.is_dir() and 0 < member.file_size <= MAX_MEMBER and (member.external_attr >> 16) & 0o170000 != 0o120000,
                    "invalid release archive member")
        with archive.open(binary) as stream:
            binary_sha = stream_digest(stream)
    return {"target": target, "name": name, "kind": "zip", "sha256": file_digest(path), "size": path.stat().st_size,
            "binary_sha256": binary_sha, "inputs_sha256": digest(inputs)}


def verify_records(inputs, records, directory, required):
    validate_inputs(inputs)
    require(isinstance(records, list) and len(records) == len(required), "candidate is missing required platform artifacts")
    seen = set()
    for record in records:
        require(isinstance(record, dict) and record.get("target") in required, "unexpected artifact platform")
        target = record["target"]
        require(target not in seen, "duplicate artifact platform")
        seen.add(target)
        expected = archive_record(Path(directory) / f"rustfs-{target}-v{inputs['version']}.zip", target, inputs)
        require(record == expected, "candidate artifact or input identity changed")
    require(seen == set(required), "incomplete candidate platform set")
    return sorted(records, key=lambda record: record["target"])


def certification_candidate(inputs, records, directory, required=TARGETS):
    """Candidate identity survives adding desktop files; test receipts bind bytes."""
    records = verify_records(inputs, records, directory, required)
    return {"schema_version": 1, "product_commit": inputs["source_sha"], "version": inputs["version"],
            "release_profile": "prerelease" if "-" in inputs["version"] else "stable", "features": ["pyroscope"],
            "plan_sha256": inputs["plan_sha256"], "workflow_sha": inputs["workflow_sha"],
            "validation_batch": {"run_id": inputs["candidate_id"], "run_attempt": 1},
            "deliverables": [{"kind": "zip", "name": r["name"], "sha256": r["sha256"], "size": r["size"],
                              "binary_sha256": r["binary_sha256"],
                              "location": f"https://github.com/{REPOSITORY}/actions/runs/{inputs['origin_run_id']}#artifact-{r['sha256']}"}
                             for r in records]}


def validate_run(run, workflow, *, completed=True):
    require(isinstance(run, dict) and (run.get("repository") or {}).get("full_name") == REPOSITORY,
            "build run belongs to another repository")
    require(run.get("path") == f".github/workflows/{workflow}" and run.get("event") == "workflow_dispatch"
            and run.get("head_branch") == "main", "candidate must originate from the trusted main workflow")
    require(type(run.get("id")) is int and run["id"] > 0 and type(run.get("run_attempt")) is int and run["run_attempt"] > 0,
            "invalid workflow run identity")
    require(isinstance(run.get("head_sha"), str) and HEX40.fullmatch(run["head_sha"]), "invalid workflow source SHA")
    if completed:
        require(run.get("status") == "completed" and run.get("conclusion") == "success", "candidate run did not succeed")
    return run


def validate_artifact(artifact, run, name):
    require(artifact.get("name") == name and artifact.get("expired") is False, "missing or expired candidate artifact")
    origin = artifact.get("workflow_run") or {}
    require(origin.get("id") == run["id"] and origin.get("head_sha") == run["head_sha"], "artifact belongs to another build run")
    require(type(artifact.get("id")) is int and artifact["id"] > 0 and type(artifact.get("size_in_bytes")) is int
            and 0 < artifact["size_in_bytes"] <= 8 * MAX_MEMBER, "invalid artifact identity or size")
    require(isinstance(artifact.get("digest"), str) and re.fullmatch(r"sha256:[0-9a-f]{64}", artifact["digest"]),
            "artifact has no immutable digest")
    return artifact


def artifact_list(run_id):
    require(type(run_id) is int and run_id > 0, "invalid run ID")
    result = []
    for page in range(1, 101):
        batch = github(f"repos/{REPOSITORY}/actions/runs/{run_id}/artifacts?per_page=100&page={page}")["artifacts"]
        result.extend(batch)
        if len(batch) < 100:
            return result
    raise ValueError("too many candidate artifacts")


def download_artifact(run, name, destination):
    matches = [item for item in artifact_list(run["id"]) if item.get("name") == name]
    require(len(matches) == 1, "candidate artifact is absent or ambiguous")
    artifact = validate_artifact(matches[0], run, name)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp:
        archive_path = Path(temp) / "artifact.zip"
        with archive_path.open("wb") as stream, github_stream(f"repos/{REPOSITORY}/actions/artifacts/{artifact['id']}/zip") as response:
            received = 0
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                received += len(chunk)
                require(received <= artifact["size_in_bytes"], "artifact response exceeds its declared size")
                stream.write(chunk)
        require(archive_path.stat().st_size == artifact["size_in_bytes"] and
                "sha256:" + file_digest(archive_path) == artifact["digest"], "downloaded artifact digest or size mismatch")
        with zipfile.ZipFile(archive_path) as archive:
            members = archive.infolist()
            require(0 < len(members) <= 256 and len({m.filename for m in members}) == len(members), "invalid artifact members")
            require(sum(member.file_size for member in members) <= 8 * MAX_MEMBER, "expanded artifact exceeds the candidate size limit")
            for member in members:
                require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,200}", member.filename)
                        and not member.is_dir() and 0 < member.file_size <= MAX_MEMBER
                        and (member.external_attr >> 16) & 0o170000 != 0o120000, "unsafe artifact member")
                target = destination / member.filename
                require(not target.exists(), "artifact extraction would overwrite a file")
                with archive.open(member) as source, target.open("xb") as sink:
                    import shutil
                    shutil.copyfileobj(source, sink)
    return artifact


def emit_output(name, value):
    require("\n" not in str(value) and "\r" not in str(value), "invalid workflow output")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
        stream.write(f"{name}={value}\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("freeze")
    for name in ("version", "source-sha", "workflow-sha", "toolchain", "console", "cli", "previous", "output"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--run-number", type=int, required=True)
    p.add_argument("--ci-run-id", type=int, required=True)
    p.add_argument("--ci-run-attempt", type=int, required=True)
    p = sub.add_parser("validate-inputs")
    p.add_argument("--inputs", required=True)
    p.add_argument("--check-checkout", action="store_true")
    p = sub.add_parser("record")
    p.add_argument("--inputs", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--archive", required=True)
    p.add_argument("--output", required=True)
    p = sub.add_parser("assemble")
    p.add_argument("--inputs", required=True)
    p.add_argument("--directory", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--platforms", choices=("linux", "all"), default="all")
    p = sub.add_parser("download")
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--workflow", choices=("release-candidate.yml", "release-complete.yml"), required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--destination", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze":
        write_json(args.output, freeze(args.version, args.source_sha, args.workflow_sha, args.toolchain,
                                      read_json(args.console), args.run_id, args.run_number, read_json(args.cli), read_json(args.previous),
                                      {"run_id": args.ci_run_id, "run_attempt": args.ci_run_attempt}))
    elif args.command == "validate-inputs":
        inputs = validate_inputs(read_json(args.inputs))
        if args.check_checkout:
            head = command(["git", "rev-parse", "HEAD"], cwd=ROOT)
            require(head == inputs["source_sha"], "checkout differs from the frozen source")
            require(recipe_digest(head) == inputs["recipe_sha256"], "release recipe differs from frozen inputs")
            require(file_digest(ROOT / "Cargo.lock") == inputs["lock_sha256"], "dependency lock differs from frozen inputs")
        print(digest(inputs))
    elif args.command == "record":
        write_json(args.output, archive_record(args.archive, args.target, read_json(args.inputs)))
    elif args.command == "assemble":
        inputs = validate_inputs(read_json(args.inputs))
        directory = Path(args.directory)
        records = [read_json(path) for path in sorted(directory.glob("record-*.json"))]
        write_json(args.output, certification_candidate(inputs, records, directory, targets(args.platforms)))
    elif args.command == "download":
        run = validate_run(github(f"repos/{REPOSITORY}/actions/runs/{args.run_id}"), args.workflow)
        download_artifact(run, args.name, args.destination)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError, zipfile.BadZipFile, subprocess.CalledProcessError) as error:
        print(f"candidate rejected: {error}", file=sys.stderr)
        raise SystemExit(1) from None
