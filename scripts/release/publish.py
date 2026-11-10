#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Promote an approved seal. This module never compiles or packages a deliverable."""
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

import candidate as c
import pipeline

REGISTRIES = ("docker.io/rustfs/rustfs", "ghcr.io/rustfs/rustfs", "quay.io/rustfs/rustfs")


def api(path, method="GET", body=None, missing=False):
    args = ["gh", "api", "--method", method, path]
    if body is not None:
        args += ["--input", "-"]
    result = subprocess.run(args, input=json.dumps(body) if body is not None else None, text=True,
                            capture_output=True, timeout=120)
    if result.returncode:
        if missing and "(HTTP 404)" in result.stderr:
            return None
        raise ValueError(f"GitHub {method} failed for {path}: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def verify(run_id, expected, directory):
    current = pipeline.current_run("release-publish.yml")
    complete = pipeline.get_run(run_id, "release-complete.yml")
    c.download_artifact(complete, "release-seal", directory)
    manifest_path = directory / "release-manifest.json"
    c.require(c.HEX64.fullmatch(expected or "") and c.file_digest(manifest_path) == expected, "publication approval does not match the sealed manifest")
    seal = c.read_json(manifest_path)
    pipeline.verify_seal_files(seal, directory)
    c.require(seal["stage"] == "complete" and seal.get("complete_run_id") == complete["id"], "seal belongs to another completion run")
    inputs = c.validate_inputs(c.read_json(directory / "inputs.json"))
    c.require(seal.get("inputs_sha256") == c.digest(inputs), "seal inputs changed")
    c.require(seal.get("aliases") == pipeline.aliases(inputs), "release aliases differ from the approved versioned archives")
    pipeline.check_recipe(inputs)
    origin = pipeline.get_run(inputs["origin_run_id"], "release-candidate.yml")
    c.require(origin["head_sha"] == inputs["workflow_sha"], "candidate producer changed")
    candidate, summary = pipeline.certify(inputs, directory, {origin["id"]: origin, complete["id"]: complete}, "full")
    c.require(candidate == seal.get("candidate") and summary == seal.get("summary") and summary["publish_allowed"], "full certification does not match the seal")
    pipeline.verify_deliveries(inputs, directory)
    pipeline.check_acceptance(seal["acceptance"]["report"])
    c.require(c.HEX64.fullmatch(seal["acceptance"].get("linux_manifest_sha256", "")), "missing Linux acceptance digest")
    c.emit_output("version", inputs["version"])
    c.emit_output("source_sha", inputs["source_sha"])
    return inputs, seal, current


def ensure_tag(inputs, manifest_sha):
    version, source = inputs["version"], inputs["source_sha"]
    message = f"RustFS {version}\nRelease manifest SHA-256: {manifest_sha}\n"
    path = f"repos/{c.REPOSITORY}/git/ref/tags/{version}"
    reference = api(path, missing=True)
    if reference is None:
        tag = api(f"repos/{c.REPOSITORY}/git/tags", "POST", {"tag": version, "message": message, "object": source, "type": "commit"})
        api(f"repos/{c.REPOSITORY}/git/refs", "POST", {"ref": f"refs/tags/{version}", "sha": tag["sha"]})
        reference = api(path)
    c.require(reference["object"]["type"] == "tag", "existing release tag is not an annotated candidate tag")
    tag = api(f"repos/{c.REPOSITORY}/git/tags/{reference['object']['sha']}")
    c.require(tag.get("tag") == version and tag.get("object", {}).get("sha") == source and tag.get("object", {}).get("type") == "commit"
              and tag.get("message", "").rstrip() == message.rstrip(), "public tag already identifies different source or bytes; never move it")


def public_files(directory):
    names = {"release-manifest.json", "SHA256SUMS", "SHA512SUMS", "release-evidence.zip"}
    return sorted(p for p in directory.iterdir() if p.name in names or p.suffix in (".zip", ".deb", ".rpm", ".tgz") or p.name.endswith((".sbom.cdx.json", ".provenance.json")))


def release(inputs, manifest_sha):
    endpoint = f"repos/{c.REPOSITORY}/releases"
    value = api(endpoint + "/tags/" + inputs["version"], missing=True)
    marker = f"Release manifest SHA-256: {manifest_sha}"
    if value is None:
        notes = api(endpoint + "/generate-notes", "POST", {"tag_name": inputs["version"], "target_commitish": inputs["source_sha"],
                                                          "previous_tag_name": inputs["previous"]["version"]})
        value = api(endpoint, "POST", {"tag_name": inputs["version"], "target_commitish": inputs["source_sha"],
                                      "name": f"RustFS {inputs['version']}", "body": marker + "\n\n" + notes["body"],
                                      "draft": True, "prerelease": "-" in inputs["version"], "make_latest": "false"})
    c.require(marker in value.get("body", ""), "existing release belongs to another sealed candidate")
    return value


def github_assets(inputs, value, paths):
    assets = []
    for page in range(1, 101):
        batch = api(f"repos/{c.REPOSITORY}/releases/{value['id']}/assets?per_page=100&page={page}")
        assets.extend(batch)
        if len(batch) < 100:
            break
    known = {a["name"]: a for a in assets}
    c.require(len(known) == len(assets) and set(known) <= {p.name for p in paths}, "release has unsealed or duplicate assets")
    for path in paths:
        old = known.get(path.name)
        if old is None:
            # No --clobber: a race or interrupted upload must fail closed.
            subprocess.run(["gh", "release", "upload", inputs["version"], str(path), "--repo", c.REPOSITORY], check=True, timeout=600)
        else:
            c.require(old.get("digest") == "sha256:" + c.file_digest(path) and old.get("size") == path.stat().st_size,
                      "existing release asset differs; never overwrite accepted bytes")
    actual = api(f"repos/{c.REPOSITORY}/releases/{value['id']}/assets?per_page=100")
    c.require(len(actual) == len(paths), "release asset set incomplete")
    for path in paths:
        match = [a for a in actual if a["name"] == path.name]
        c.require(len(match) == 1 and match[0].get("digest") == "sha256:" + c.file_digest(path)
                  and match[0].get("size") == path.stat().st_size, "uploaded release asset verification failed")


def aws(*args, capture=True):
    return subprocess.run(["aws", "--endpoint-url", os.environ["R2_ENDPOINT"], *args],
                          text=True, capture_output=capture, check=False, timeout=600)


def r2_file(path, key, *, mutable=False):
    bucket = os.environ["R2_BUCKET"]
    expected = c.file_digest(path)
    head = aws("s3api", "head-object", "--bucket", bucket, "--key", key)
    if head.returncode and "(404)" not in head.stderr:
        raise ValueError("R2 lookup failed; refusing to treat an unavailable destination as empty")
    if head.returncode == 0:
        with tempfile.TemporaryDirectory() as temp:
            old = Path(temp) / "object"
            result = aws("s3", "cp", f"s3://{bucket}/{key}", str(old), "--only-show-errors")
            c.require(result.returncode == 0, "could not verify existing R2 object")
            if c.file_digest(old) == expected:
                return
            c.require(mutable, "existing versioned R2 object differs; never overwrite it")
    args = ["s3api", "put-object", "--bucket", bucket, "--key", key, "--body", str(path), "--metadata", json.dumps({"sha256": expected})]
    if not mutable:
        args += ["--if-none-match", "*"]
    result = aws(*args)
    c.require(result.returncode == 0, "R2 upload failed; retry the same seal after checking destination state")
    with tempfile.TemporaryDirectory() as temp:
        actual = Path(temp) / "object"
        result = aws("s3", "cp", f"s3://{bucket}/{key}", str(actual), "--only-show-errors")
        c.require(result.returncode == 0 and c.file_digest(actual) == expected, "R2 read-back digest mismatch")


def image_digest(reference, *, missing=False):
    result = subprocess.run(["skopeo", "inspect", "--authfile", os.environ["REGISTRY_AUTH_FILE"], "--raw", f"docker://{reference}"],
                            capture_output=True, timeout=120)
    if result.returncode:
        error = result.stderr.decode(errors="replace").lower()
        if missing and ("manifest unknown" in error or "manifest_unknown" in error or "name unknown" in error):
            return None
        raise ValueError(f"cannot verify registry manifest for {reference}")
    return "sha256:" + hashlib.sha256(result.stdout).hexdigest()


def copy_image(archive, reference, expected, *, mutable=False):
    old = image_digest(reference, missing=True)
    if old == expected:
        return
    c.require(old is None or mutable, "existing image tag identifies different bytes; never replace it")
    subprocess.run(["skopeo", "copy", "--authfile", os.environ["REGISTRY_AUTH_FILE"], "--all", "--preserve-digests",
                    f"oci-archive:{archive}", f"docker://{reference}"], check=True, timeout=1800)
    c.require(image_digest(reference) == expected, "registry changed the accepted image digest")


def images(inputs, directory, *, latest=False):
    for variant, suffix in (("musl", ""), ("gnu", "-glibc")):
        record = c.read_json(directory / f"image-{variant}.json")
        for registry in REGISTRIES:
            tag = ("latest" if latest else inputs["version"]) + suffix
            copy_image(directory / f"image-{variant}.tar", f"{registry}:{tag}", record["digest"], mutable=latest)


def publish_version(inputs, seal, directory, manifest_sha):
    ensure_tag(inputs, manifest_sha)
    value = release(inputs, manifest_sha)
    paths = public_files(directory)
    with tempfile.TemporaryDirectory() as temp:
        alias_paths = []
        for name, source in seal["aliases"].items():
            path = Path(temp) / name
            shutil.copyfile(directory / source, path)
            alias_paths.append(path)
        github_assets(inputs, value, paths + alias_paths)
    for path in paths:
        prefix = "artifacts/rustfs/release/"
        if path.name in ("release-manifest.json", "release-evidence.zip", "SHA256SUMS", "SHA512SUMS"):
            prefix += inputs["version"] + "/"
        r2_file(path, prefix + path.name)
    images(inputs, directory)
    # Every versioned destination is verified before the draft is made public.
    if value["draft"]:
        api(f"repos/{c.REPOSITORY}/releases/{value['id']}", "PATCH",
            {"draft": False, "prerelease": "-" in inputs["version"], "make_latest": "false"})


def stable_key(version):
    c.require(isinstance(version, str) and re.fullmatch(r"v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version), "invalid stable channel version")
    return tuple(map(int, version.lstrip("v").split(".")))


def helm(inputs, directory):
    record = c.read_json(directory / "chart.json")
    package = directory / record["name"]
    c.require(c.file_digest(package) == record["sha256"], "Helm package changed")
    env = dict(os.environ, GH_TOKEN=os.environ["HELM_TOKEN"])
    subprocess.run(["gh", "auth", "setup-git", "--hostname", "github.com"], env=env, check=True, timeout=60)
    with tempfile.TemporaryDirectory() as temp:
        subprocess.run(["git", "clone", "--depth", "1", "--branch", "main", "https://github.com/rustfs/helm.git", temp], env=env, check=True, timeout=120)
        path = Path(temp) / package.name
        if path.exists():
            c.require(c.file_digest(path) == record["sha256"], "published Helm version already has different bytes")
            c.require((Path(temp) / "index.yaml").is_file(), "existing Helm chart has no index")
            return
        shutil.copyfile(package, path)
        c.command(["helm", "repo", "index", ".", "--url", "https://charts.rustfs.com"], cwd=temp)
        c.command(["git", "config", "user.name", "RustFS Release"], cwd=temp)
        c.command(["git", "config", "user.email", "dev@rustfs.com"], cwd=temp)
        c.command(["git", "add", package.name, "index.yaml"], cwd=temp)
        c.command(["git", "commit", "-m", f"chore: publish chart {inputs['version']}"], cwd=temp)
        subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=temp, env=env, check=True, timeout=120)


def channels(inputs, seal, directory, manifest_sha):
    # Recheck all immutable destinations, including when resuming a failed job.
    publish_version(inputs, seal, directory, manifest_sha)
    # The versioned Helm catalog entry must still be completed when a newer
    # release already owns the mutable channels.
    helm(inputs, directory)
    if "-" in inputs["version"]:
        return
    latest = api(f"repos/{c.REPOSITORY}/releases/latest", missing=True)
    if latest and stable_key(latest["tag_name"]) > stable_key(inputs["version"]):
        print("A newer stable release owns the channels; keeping its pointers.")
        return
    value = release(inputs, manifest_sha)
    # GitHub Latest also acts as a monotonic marker during partial channel
    # failures. Versioned assets and images are already fully available here.
    api(f"repos/{c.REPOSITORY}/releases/{value['id']}", "PATCH", {"make_latest": "true"})
    images(inputs, directory, latest=True)
    for target in c.TARGETS:
        path = directory / f"rustfs-{target}-v{inputs['version']}.zip"
        r2_file(path, f"artifacts/rustfs/release/rustfs-{target}-latest.zip", mutable=True)
    pointer = {"version": inputs["version"], "tag": inputs["version"], "release_date": value.get("published_at"),
               "release_type": "stable", "download_url": f"https://github.com/{c.REPOSITORY}/releases/tag/{inputs['version']}"}
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "latest.json"
        c.write_json(path, pointer)
        subprocess.run([os.environ["OSSUTIL_BIN"], "cp", str(path), "oss://rustfs-version/latest.json", "--force"], check=True, timeout=120)
        actual = Path(temp) / "actual.json"
        subprocess.run([os.environ["OSSUTIL_BIN"], "cp", "oss://rustfs-version/latest.json", str(actual)], check=True, timeout=120)
        c.require(c.read_json(actual) == pointer, "installer pointer did not match the published release")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("verify", "version", "channels"))
    p.add_argument("--run-id", type=int, required=True)
    p.add_argument("--digest", required=True)
    p.add_argument("--directory", type=Path, required=True)
    args = p.parse_args()
    inputs, seal, current = verify(args.run_id, args.digest, args.directory)
    if args.mode != "verify":
        for name in ("R2_ENDPOINT", "R2_BUCKET", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "REGISTRY_AUTH_FILE"):
            c.require(bool(os.environ.get(name)), f"missing publication setting: {name}")
        if args.mode == "channels":
            c.require(os.environ.get("HELM_TOKEN") and os.environ.get("OSSUTIL_BIN"), "missing channel publication settings")
        {"version": publish_version, "channels": channels}[args.mode](inputs, seal, args.directory, args.digest)
        c.write_json("publication.json", {"schema": 1, "version": inputs["version"], "manifest_sha256": args.digest,
                                         "source_sha": inputs["source_sha"], "complete_run_id": args.run_id,
                                         "publish_run_id": current["id"], "stage": args.mode})


if __name__ == "__main__":
    main()
