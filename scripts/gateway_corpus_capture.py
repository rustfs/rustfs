#!/usr/bin/env python3
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Record ordinary RustFS and Ceph tests on disposable local servers for #2777."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
from urllib.request import ProxyHandler, build_opener


CEPH_TESTS = (
    "test_bucket_listv2_prefix_basic",
    "test_bucket_listv2_continuationtoken",
    "test_object_head_zero_bytes",
    "test_object_write_file",
    "test_multipart_copy_small",
    "test_multipart_upload_resend_part",
    "test_multipart_upload_contents",
)
MINIMUM_ENTRIES = 1000


def ceph_pin():
    runner = Path("scripts/s3-tests/run.sh").read_text()
    (rev,) = re.findall(r'^S3TESTS_REV="\${S3TESTS_REV:-([0-9a-f]{40})}"$', runner, re.M)
    return rev


def gateway_pin():
    with Path("Cargo.toml").open("rb") as manifest:
        dependencies = tomllib.load(manifest)["workspace"]["dependencies"]
    revisions = {spec.get("rev") for spec in dependencies.values()
                 if isinstance(spec, dict) and spec.get("git") == "https://github.com/rustfs/gateway.git"}
    if len(revisions) != 1 or not re.fullmatch(r"[0-9a-f]{40}", str(next(iter(revisions)))):
        raise ValueError("Cargo.toml must pin rustfs/gateway at exactly one full revision")
    return revisions.pop()


def revision(path):
    return subprocess.check_output(["git", "-C", path, "rev-parse", "HEAD"], text=True).strip()


def run_ceph(checkout, temp, env):
    """Run the implemented upstream tests on a fresh single-node fixture."""
    implemented = set(Path("scripts/s3-tests/implemented_tests.txt").read_text().splitlines())
    if not set(CEPH_TESTS) <= implemented:
        raise ValueError("Selected Ceph tests are missing from the implemented manifest")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = str(sock.getsockname()[1])
    addr = f"127.0.0.1:{port}"
    access = "rustfsadmin-ci"
    secret = secrets.token_urlsafe(36)
    data = temp / "ceph-data"
    data.mkdir()
    config = Path(".github/s3tests/s3tests.conf").read_text()
    for key, value in {
        "S3_HOST": "127.0.0.1", "S3_PORT": port, "S3_ACCESS_KEY": access, "S3_SECRET_KEY": secret,
        "S3_ALT_ACCESS_KEY": "rustfsalt", "S3_ALT_SECRET_KEY": secrets.token_urlsafe(36),
    }.items():
        config = config.replace("${" + key + "}", value)
    conf_file = temp / "s3tests.conf"
    conf_file.write_text(config)
    server_env = dict(env, RUSTFS_ADDRESS=addr, RUSTFS_VOLUMES=str(data),
                      RUSTFS_ACCESS_KEY=access, RUSTFS_SECRET_KEY=secret, RUSTFS_CONSOLE_ENABLE="false")
    with (temp / "ceph.log").open("w") as log:
        srv = subprocess.Popen([env["CARGO_BIN_EXE_rustfs"]], env=server_env, stdout=log, stderr=log)
        try:
            opener = build_opener(ProxyHandler({}))
            for _ in range(120):
                if srv.poll() is not None:
                    raise RuntimeError("Ceph fixture exited before readiness")
                try:
                    with opener.open(f"http://{addr}/health/ready", timeout=1) as res:
                        if res.status == 200:
                            break
                except OSError:
                    pass
                time.sleep(0.5)
            else:
                raise RuntimeError("Ceph fixture did not become ready")
            # Use upstream's own assertions. Each test owns its bucket in this disposable store.
            command = [sys.executable, "-c", """
import sys
from s3tests import functional
from s3tests.functional import test_s3
functional.configure()
for test in sys.argv[1:]:
    getattr(test_s3, test)()
""", *CEPH_TESTS]
            subprocess.run(command, cwd=checkout, env=dict(env, S3TEST_CONF=str(conf_file)),
                           stdout=log, stderr=log, check=True)
        finally:
            srv.terminate()
            try:
                srv.wait(timeout=15)
            except subprocess.TimeoutExpired:
                srv.kill()
                srv.wait()


def count_entries(files, sources):
    counts = Counter()
    for path in files:
        with path.open() as entries:
            for line in entries:
                entry = json.loads(line)
                if entry["sut"] != "rustfs-server" or entry["src"] not in sources:
                    raise ValueError("Unexpected corpus origin")
                counts[entry["src"]] += 1
    if counts.keys() != sources:
        raise ValueError("Missing corpus source")
    return counts


def collect(args, temp):
    gateway_rev = gateway_pin()
    if revision(args.gateway) != gateway_rev:
        raise ValueError("Gateway checkout differs from the Cargo pin")
    ceph_rev = ceph_pin()
    if revision(args.ceph) != ceph_rev:
        raise ValueError("Ceph checkout differs from the test runner pin")
    e2e_source = f"rustfs-e2e@{revision('.')}"
    ceph_source = f"s3-tests@{ceph_rev}"
    raw = temp / "raw" / "corpus"
    raw.mkdir(parents=True, mode=0o700)
    env = dict(os.environ, TMPDIR=str(temp), RUSTFS_CORPUS_DIR=str(raw), RUSTFS_CORPUS_RECORD="1",
               NO_PROXY="localhost,127.0.0.1,::1", no_proxy="localhost,127.0.0.1,::1")
    print("Recording e2e-full listing regression tests", flush=True)
    with (temp / "e2e.log").open("w") as log:
        subprocess.run(
            ["cargo", "nextest", "run", "--locked", "-p", "e2e_test", "--profile", "e2e-full",
             "-E", "test(listing_regression_test::)", "--no-tests", "fail"],
            env=dict(env, RUSTFS_CORPUS_RECORD_SRC=e2e_source), stdout=log, stderr=log, check=True,
        )
    print(f"Recording {len(CEPH_TESTS)} implemented Ceph tests", flush=True)
    run_ceph(args.ceph, temp, dict(env, RUSTFS_CORPUS_RECORD_SRC=ceph_source))
    report = verify_capture(temp, args, {e2e_source, ceph_source})
    # The enclosing e2e_binary.py run verifies this identity before and after capture.
    identity = json.loads(Path(env["CARGO_BIN_EXE_rustfs"] + ".e2e.json").read_text())
    report.update(gateway_revision=gateway_rev, server_sha256=identity["binary_sha256"])
    return report


def verify_capture(temp, args, sources):
    raw = temp / "raw" / "corpus"
    files = sorted(raw.glob("*.jsonl"))
    dest = temp / "checked" / "corpus"
    dest.parent.mkdir()
    scanner = args.gateway / "scripts/check_corpus_no_secrets.sh"
    subprocess.run(["bash", scanner], env=dict(os.environ, GATEWAY_CHECK_ROOT=str(raw.parent)), check=True)
    raw_counts = count_entries(files, sources)
    subprocess.run([args.tool, "ingest", *files, "--into", dest], check=True)
    subprocess.run([args.tool, "verify", dest, "--strict"], check=True)
    subprocess.run(["bash", scanner], env=dict(os.environ, GATEWAY_CHECK_ROOT=str(dest.parent)), check=True)
    # Deduplication operates across both sources. Identical observations may collapse into one.
    counts = count_entries(sorted(dest.rglob("*.jsonl")), sources)
    total = sum(counts.values())
    return {
        "scope": "candidate recording subset", "e2e_suite": "e2e-full/listing_regression_test",
        "ceph_tests": list(CEPH_TESTS), "raw_counts": dict(raw_counts), "retained_counts": dict(counts),
        "retained_total": total, "minimum_entries": MINIMUM_ENTRIES, "size_gate_met": total >= MINIMUM_ENTRIES,
        "official_reference_replay": "not run", "full_suite_coverage": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("pins")
    gate = sub.add_parser("check-size")
    gate.add_argument("summary", type=Path)
    record = sub.add_parser("record")
    for option in ("ceph", "gateway", "tool", "summary"):
        record.add_argument(f"--{option}", type=lambda value: Path(value).resolve(), required=True)
    args = parser.parse_args(argv)
    if args.cmd == "pins":
        print(f"rev={gateway_pin()}")
        print(f"ceph-rev={ceph_pin()}")
        return 0
    if args.cmd == "check-size":
        total = json.loads(args.summary.read_text())["retained_total"]
        print(f"Retained {total}/{MINIMUM_ENTRIES} required unique entries. "
              "Only selected scenarios ran. Official-reference replay is outstanding.")
        return 0 if total >= MINIMUM_ENTRIES else 1
    # Only this numeric/provenance report may leave the temporary run directory.
    args.summary.unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix="gateway-corpus-") as temp:
        report = collect(args, Path(temp).resolve())
    args.summary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
