#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Wrap accepted binaries, then inspect the actual packages and OCI images."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
import time

import candidate as c
import pipeline


def run(args, **kwargs):
    return subprocess.run(args, check=True, timeout=1800, **kwargs)


def binary(inputs, target, output):
    origin = pipeline.get_run(inputs["origin_run_id"], "release-candidate.yml")
    c.download_artifact(origin, f"candidate-{inputs['candidate_id']}-{target}", output)
    record = c.read_json(output / f"record-{target}.json")
    c.verify_records(inputs, [record], output, (target,))
    pipeline.verify_evidence(inputs, output, {origin["id"]: origin})
    import verify
    path, _ = verify.unpack(output / record["name"], target, inputs, output / "bin")
    return path, record


def reuse(kind, target):
    inputs = pipeline.manifest_env()
    current = pipeline.current_run("release-complete.yml")
    name = f"{kind}-{target}"
    exists = any(a["name"] == name for a in c.artifact_list(current["id"]))
    if exists:
        with tempfile.TemporaryDirectory() as temp:
            c.download_artifact(current, name, temp)
            record = c.read_json(Path(temp) / f"{name}.json")
            c.require(record.get("inputs_sha256") == c.digest(inputs), "existing delivery belongs to another candidate")
            # Successful jobs are immutable. The final seal independently checks
            # all sizes and digests before they can be approved for publication.
    c.emit_output("reused", str(exists).lower())


def package(inputs, target, output):
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp:
        temp = Path(temp)
        executable, record = binary(inputs, target, temp / "source")
        arch, libc = target.split("-")[1:]
        build_type = "prerelease" if "-" in inputs["version"] else "release"
        values = c.command(["bash", "scripts/release/package_versions.sh", build_type, inputs["version"], "", arch, libc], cwd=c.ROOT)
        versions = dict(line.split("=", 1) for line in values.splitlines())
        # Existing package recipes operate in a private working directory.
        for name in ("scripts", "deploy"):
            (temp / name).symlink_to(c.ROOT / name, target_is_directory=True)
        for name in ("LICENSE", "README.md"):
            shutil.copyfile(c.ROOT / name, temp / name)
        (temp / "bin").mkdir()
        shutil.copyfile(executable, temp / "bin/rustfs")
        (temp / "bin/rustfs").chmod(0o755)
        env = dict(os.environ, **{key.upper(): value for key, value in versions.items()}, LIBC=libc,
                   DEB_ARCH="amd64" if arch == "x86_64" else "arm64", RPM_ARCH=arch,
                   DEB_DEPENDS="libc6 (>= 2.31)" if libc == "gnu" else "",
                   RPM_DEPENDS="glibc >= 2.31" if libc == "gnu" else "", GITHUB_OUTPUT=str(temp / "outputs"))
        for kind in ("deb", "rpm"):
            run(["bash", str(c.ROOT / f"scripts/release/build_{kind}.sh")], cwd=temp, env=env)
            path = temp / versions[f"{kind}_file"]
            unpacked = temp / f"unpacked-{kind}"
            unpacked.mkdir()
            if kind == "deb":
                run(["dpkg-deb", "--extract", str(path), str(unpacked)])
            else:
                with subprocess.Popen(["rpm2cpio", str(path)], stdout=subprocess.PIPE) as source:
                    run(["cpio", "--extract", "--make-directories", "--quiet", "./usr/bin/rustfs"], stdin=source.stdout, cwd=unpacked)
                    source.stdout.close()
                    c.require(source.wait(timeout=60) == 0, "RPM extraction failed")
            c.require(c.file_digest(unpacked / "usr/bin/rustfs") == record["binary_sha256"], "package binary differs from accepted ZIP")
            shutil.copyfile(path, output / path.name)
        files = pipeline.file_records(output)
        c.write_json(output / f"packages-{target}.json", {"inputs_sha256": c.digest(inputs), "binary_sha256": record["binary_sha256"], "files": files})


def image_prepare(inputs, variant, output):
    output.mkdir(parents=True, exist_ok=True)
    binaries = {}
    for arch, docker_arch in (("x86_64", "amd64"), ("aarch64", "arm64")):
        target = f"linux-{arch}-{variant}"
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            executable, record = binary(inputs, target, temp / "source")
            directory = c.ROOT / ".release-inputs" / docker_arch
            directory.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(executable, directory / "rustfs")
            (directory / "rustfs").chmod(0o755)
            cli = inputs["cli"][target]
            archive = temp / "cli.tar.gz"
            run(["curl", "--fail", "--location", "--retry", "3", "--output", str(archive), cli["url"]])
            c.require(c.file_digest(archive) == cli["sha256"], "CLI dependency changed")
            with tarfile.open(archive, "r:gz") as package:
                members = [m for m in package.getmembers() if m.name == "rc"]
                c.require(len(members) == 1 and members[0].isfile() and 0 < members[0].size <= c.MAX_MEMBER, "invalid CLI archive")
                with package.extractfile(members[0]) as source, (directory / "rc").open("wb") as dest:
                    shutil.copyfileobj(source, dest)
            (directory / "rc").chmod(0o755)
            binaries[arch] = record["binary_sha256"]
    c.write_json(output / f"image-{variant}-inputs.json", {"inputs_sha256": c.digest(inputs), "binaries": binaries})


def image_smoke(inputs, variant, output):
    archive = output / f"image-{variant}.tar"
    record = c.read_json(output / f"image-{variant}-inputs.json")
    scan_directory = Path(str(output) + "-scan")
    scan_directory.mkdir(parents=True, exist_ok=True)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for arch, docker_arch in (("x86_64", "amd64"), ("aarch64", "arm64")):
        image = f"rustfs-acceptance:{variant}-{docker_arch}"
        run(["skopeo", "copy", "--override-os", "linux", "--override-arch", docker_arch, f"oci-archive:{archive}", f"docker-daemon:{image}"])
        container = c.command(["docker", "create", "--platform", f"linux/{docker_arch}", image])
        try:
            with tempfile.TemporaryDirectory() as temp:
                path = Path(temp) / "rustfs"
                run(["docker", "cp", f"{container}:/usr/bin/rustfs", str(path)])
                c.require(c.file_digest(path) == record["binaries"][arch], "image wraps a different executable")
        finally:
            run(["docker", "rm", "-v", container])
        version = c.command(["docker", "run", "--rm", "--platform", f"linux/{docker_arch}", "--entrypoint", "/usr/bin/rustfs", image, "--version"])
        c.require(version.splitlines()[0] == f"rustfs {inputs['version']}", "container displays wrong version")
        container = c.command(["docker", "run", "-d", "--platform", f"linux/{docker_arch}", "-p", "127.0.0.1::9001",
                               "-e", "RUSTFS_ACCESS_KEY=release-smoke", "-e", "RUSTFS_SECRET_KEY=release-smoke-test-secret",
                               "-e", "RUSTFS_CONSOLE_ENABLE=true", image])
        log = output / f"image-{variant}-{docker_arch}.log"
        try:
            address = c.command(["docker", "port", container, "9001/tcp"]).strip()
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                try:
                    with opener.open(f"http://{address}/rustfs/console/", timeout=2) as response:
                        c.require(response.status == 200 and "text/html" in response.headers.get("Content-Type", ""), "image Console did not return HTML")
                    break
                except OSError:
                    time.sleep(0.5)
            else:
                raise ValueError("container startup timed out")
        finally:
            with log.open("w") as stream:
                subprocess.run(["docker", "logs", container], stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=60)
            run(["docker", "rm", "-f", "-v", container])
        with log.open("a") as stream:
            stream.write("\nConsole: HTTP 200 text/html; wrapped executable SHA-256 and version verified\n")
        # Trivy's local archive reader does not reliably select a platform from
        # a multi-architecture OCI index. Export the image just verified above.
        scan_archive = scan_directory / f"image-{variant}-{docker_arch}.tar"
        run(["docker", "save", "--output", str(scan_archive), image])
        record.setdefault("scan_inputs", {})[docker_arch] = c.file_digest(scan_archive)
    raw = subprocess.check_output(["skopeo", "inspect", "--raw", f"oci-archive:{archive}"], timeout=60)
    import hashlib
    record.update(digest="sha256:" + hashlib.sha256(raw).hexdigest(), sha256=c.file_digest(archive), size=archive.stat().st_size, smoke="pass")
    c.write_json(output / f"image-{variant}-smoke.json", record)


def image_record(inputs, variant, output):
    record = c.read_json(output / f"image-{variant}-smoke.json")
    c.require(record["inputs_sha256"] == c.digest(inputs), "image inputs changed")
    for arch in ("amd64", "arm64"):
        scan_archive = Path(str(output) + "-scan") / f"image-{variant}-{arch}.tar"
        c.require(record.get("scan_inputs", {}).get(arch) == c.file_digest(scan_archive), "scan input changed after image verification")
        report = c.read_json(output / f"scan-{variant}-{arch}.sarif")
        c.require(report.get("version") == "2.1.0" and report.get("runs"), "container scan did not produce a SARIF report")
    record["scan"] = "completed"
    c.write_json(output / f"image-{variant}.json", record)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("reuse", "package", "image-prepare", "image-smoke", "image-record"))
    p.add_argument("--kind", choices=("packages", "image"))
    p.add_argument("--target", required=True)
    p.add_argument("--output", type=Path, default=Path("delivery"))
    args = p.parse_args()
    inputs = pipeline.manifest_env()
    if args.mode == "reuse":
        reuse(args.kind, args.target)
    elif args.mode == "package":
        c.require(args.target in c.LINUX, "unknown package platform")
        package(inputs, args.target, args.output)
    else:
        c.require(args.target in ("musl", "gnu"), "unknown image variant")
        {"image-prepare": image_prepare, "image-smoke": image_smoke, "image-record": image_record}[args.mode](inputs, args.target, args.output)


if __name__ == "__main__":
    main()
