#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Run release checks against the archived executable, never a rebuilt server."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import hashlib
import hmac
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import urllib.request
import zipfile

import candidate as c

TRIPLES = {
    "linux-x86_64-musl": "x86_64-unknown-linux-musl", "linux-aarch64-musl": "aarch64-unknown-linux-musl",
    "linux-x86_64-gnu": "x86_64-unknown-linux-gnu", "linux-aarch64-gnu": "aarch64-unknown-linux-gnu",
    "macos-aarch64": "aarch64-apple-darwin", "windows-x86_64": "x86_64-pc-windows-msvc",
}
UPGRADES = {
    "direct-upgrade": "direct_upgrade_from_previous_release_preserves_object_contracts",
    "mixed-version-upgrade": "rolling_upgrade_from_previous_release_preserves_mixed_version_contracts",
    "bucket-config-upgrade": "direct_upgrade_from_previous_release_preserves_bucket_configuration",
    "bucket-config-rollback": "rollback_to_previous_release_reads_current_bucket_metadata",
    "multipart-layout-upgrade": "direct_upgrade_from_previous_release_preserves_multipart_layouts",
    "multipart-layout-baseline": "previous_release_baseline_replicates_multipart_layouts",
}


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def envelope(inputs, record, lane, case, logs, started, output):
    """Only called after the real check succeeds; logs remain part of the seal."""
    target = record["target"].split("-")
    platform = dict(zip(("os", "arch", "abi"), target))
    platform.setdefault("abi", "msvc" if platform["os"] == "windows" else "none")
    workflow = os.environ["GITHUB_WORKFLOW_REF"].split("@", 1)[0].rsplit("/", 1)[-1]
    c.require(workflow in ("release-candidate.yml", "release-complete.yml"), "untrusted verifier workflow")
    value = {
        "schema_version": 1, "evidence_profile": "release-candidate", "tier": "release",
        "run_id": inputs["candidate_id"], "run_attempt": 1, "plan_sha256": inputs["plan_sha256"],
        "lane": lane, "case_id": case, "product_commit": inputs["source_sha"],
        "harness_commit": inputs["workflow_sha"], "target_fingerprint": record["binary_sha256"],
        "platform": platform, "started_at": started, "finished_at": now(), "state": "pass",
        "reason_code": "CHECK_PASSED", "reason_class": "NOT_APPLICABLE",
        "artifact": {"archive_sha256": record["sha256"], "binary_sha256": record["binary_sha256"]},
        "evidence": [{"path": p.name, "sha256": c.file_digest(p), "size": p.stat().st_size} for p in logs],
        "workflow": {"repository": c.REPOSITORY, "workflow_id": workflow, "run_id": os.environ["GITHUB_RUN_ID"],
                     "run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"]), "workflow_sha": os.environ["GITHUB_SHA"],
                     "status": "completed"},
    }
    c.write_json(output / f"{lane}-{record['target']}-{case}.json", value)


def unpack(archive, target, inputs, directory):
    record = c.archive_record(archive, target, inputs)
    name = "rustfs.exe" if target.startswith("windows") else "rustfs"
    with zipfile.ZipFile(archive) as package:
        package.extract(name, directory)
    binary = (Path(directory) / name).resolve()
    binary.chmod(0o755)
    return binary, record


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def s3_request(opener, endpoint, method, path, access, secret, body=b""):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date = timestamp[:8]
    host = endpoint.removeprefix("http://")
    payload = hashlib.sha256(body).hexdigest()
    headers = f"host:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{timestamp}\n"
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"{method}\n{path}\n\n{headers}\n{signed}\n{payload}"
    scope = f"{date}/us-east-1/s3/aws4_request"
    to_sign = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = ("AWS4" + secret).encode()
    for part in (date, "us-east-1", "s3", "aws4_request"):
        key = hmac.digest(key, part.encode(), "sha256")
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request(endpoint + path, data=body if method == "PUT" else None, method=method,
                                     headers={"Host": host, "X-Amz-Content-Sha256": payload, "X-Amz-Date": timestamp,
                                              "Authorization": f"AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed}, Signature={signature}"})
    with opener.open(request, timeout=10) as response:
        c.require(response.status in (200, 204), "S3 smoke request failed")
        return response.read(1024 * 1024)


def native(inputs, archive, target, output):
    started = now()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temp:
        binary, record = unpack(archive, target, inputs, temp)
        version = subprocess.check_output([str(binary), "--version"], text=True, timeout=60)
        c.require(version.splitlines()[0] == f"rustfs {inputs['version']}", "binary displays a different product version")
        for label, expected in (("build target", TRIPLES[target]), ("git commit", inputs["source_sha"])):
            c.require(re.search(rf"(?m)^{label}\s*:\s*{re.escape(expected)}\s*$", version), f"binary {label} mismatch")
        c.require(re.search(rf"(?m)^rust version\s*:\s*(?:rustc )?{re.escape(inputs['toolchain'])}(?:\s|$)", version), "binary toolchain mismatch")
        log = output / f"identity-{target}.log"
        log.write_text(version, encoding="utf-8")
        envelope(inputs, record, "identity", "artifact-identity", [log], started, output)
        data = Path(temp) / "data"
        data.mkdir()
        api, console = port(), port()
        while console == api:
            console = port()
        log = output / f"startup-{target}.log"
        access, secret = "release-" + os.urandom(8).hex(), os.urandom(24).hex()
        env = dict(os.environ, NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with log.open("w", encoding="utf-8") as stream:
            process = subprocess.Popen([str(binary), "server", "--address", f"127.0.0.1:{api}", "--console-enable",
                                        "--console-address", f"127.0.0.1:{console}", "--access-key", access,
                                        "--secret-key", secret, str(data)],
                                       stdout=stream, stderr=subprocess.STDOUT, env=env)
            try:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    c.require(process.poll() is None, "candidate exited before becoming ready")
                    try:
                        with opener.open(f"http://127.0.0.1:{api}/health/ready", timeout=2) as response:
                            c.require(response.status == 200, "S3 listener is not ready")
                        with opener.open(f"http://127.0.0.1:{console}/rustfs/console/", timeout=2) as response:
                            c.require(response.status == 200 and "text/html" in response.headers.get("Content-Type", ""), "Console did not return HTML")
                        break
                    except OSError:
                        time.sleep(0.5)
                else:
                    raise ValueError("candidate did not become ready within 60 seconds")
                bucket = "/release-" + os.urandom(8).hex()
                endpoint = f"http://127.0.0.1:{api}"
                content = os.urandom(1024)
                s3_request(opener, endpoint, "PUT", bucket, access, secret)
                s3_request(opener, endpoint, "PUT", bucket + "/roundtrip", access, secret, content)
                c.require(s3_request(opener, endpoint, "GET", bucket + "/roundtrip", access, secret) == content, "S3 round-trip bytes differ")
                s3_request(opener, endpoint, "DELETE", bucket + "/roundtrip", access, secret)
                s3_request(opener, endpoint, "DELETE", bucket, access, secret)
                c.require(process.poll() is None, "candidate exited during startup verification")
            finally:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=15)
        c.require(c.file_digest(binary) == record["binary_sha256"], "server bytes changed during native verification")
        with log.open("a", encoding="utf-8") as stream:
            stream.write("\nConsole: HTTP 200 text/html; S3: ready, bucket/object CRUD and byte-identical round trip\n")
        envelope(inputs, record, "identity", "startup", [log], started, output)


def upgrade(inputs, archive, output):
    output.mkdir(parents=True, exist_ok=True)
    plan = c.read_json(c.ROOT / ".config/release-validation-plan.json")
    c.require(c.digest(plan) == inputs["plan_sha256"], "upgrade plan changed")
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        binary, record = unpack(archive, "linux-x86_64-gnu", inputs, root)
        metadata = binary.stat()
        receipt = root / "receipt.json"
        receipt.write_text(json.dumps({"schema": 1, "workspace": str(c.ROOT.resolve()), "binary": str(binary),
                                      "size": metadata.st_size, "modified_ns": metadata.st_mtime_ns, "features": ["pyroscope"]}))
        env = dict(os.environ, CARGO_BIN_EXE_rustfs=str(binary), RUSTFS_E2E_BINARY_RECEIPT=str(receipt),
                   RUSTFS_BUILD_FEATURES="pyroscope", NO_PROXY="127.0.0.1,localhost,::1", no_proxy="127.0.0.1,localhost,::1",
                   HTTP_PROXY="", HTTPS_PROXY="", ALL_PROXY="", http_proxy="", https_proxy="", all_proxy="")
        logs = {case: [] for case in UPGRADES}
        started = now()
        sources = list(plan["upgrade_sources"]["sources"])
        previous = inputs["previous"]
        if previous["version"] not in {s["version"] for s in sources}:
            sources.append({"version": previous["version"], "artifact_name": previous["name"], "artifact_sha256": previous["sha256"]})
        for source in sources:
            directory = root / source["version"]
            directory.mkdir()
            package = directory / source["artifact_name"]
            url = f"https://github.com/{c.REPOSITORY}/releases/download/{source['version']}/{source['artifact_name']}"
            subprocess.run(["curl", "--fail", "--location", "--retry", "3", "--output", str(package), url], check=True, timeout=600)
            c.require(c.file_digest(package) == source["artifact_sha256"], "previous release digest mismatch")
            with zipfile.ZipFile(package) as previous:
                c.require(previous.namelist().count("rustfs") == 1, "ambiguous previous server")
                previous.extract("rustfs", directory)
            (directory / "rustfs").chmod(0o755)
            env["RUSTFS_UPGRADE_SOURCE_BINARY"] = str(directory / "rustfs")
            for case, test in UPGRADES.items():
                log = output / f"upgrade-{source['version']}-{case}.log"
                env["RUSTFS_E2E_LOG_DIR"] = str(root / f"logs-{source['version']}-{case}")
                with log.open("w", encoding="utf-8") as stream:
                    subprocess.run(["cargo", "test", "--locked", "-p", "e2e_test", f"upgrade_compatibility_test::{test}",
                                    "--", "--ignored", "--exact", "--nocapture"], cwd=c.ROOT, env=env,
                                   stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=1800)
                c.require(re.search(r"test result: ok\. 1 passed; 0 failed;", log.read_text()), "upgrade command did not run exactly one passing test")
                c.require(c.file_digest(binary) == record["binary_sha256"], "candidate changed during upgrade tests")
                logs[case].append(log)
        for case, files in logs.items():
            envelope(inputs, record, "upgrade", case, files, started, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("native", "upgrade"))
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--target", choices=c.TARGETS)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inputs = c.validate_inputs(c.read_json(args.inputs))
    if args.mode == "native":
        c.require(args.target is not None, "native verification needs a platform")
        native(inputs, args.archive, args.target, args.output)
    else:
        upgrade(inputs, args.archive, args.output)


if __name__ == "__main__":
    main()
