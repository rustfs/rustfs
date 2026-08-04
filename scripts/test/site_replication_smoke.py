#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
# Copyright 2024 RustFS Team
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
"""Local two-site replication lab: manages two rustfs processes and pairs them.

Standalone replacement for the "Site Replication: A + B" compound in
.vscode/launch.json — it spawns site A (:9000) and site B (:9020) from
target/debug/rustfs, waits for both to come up, then calls the admin API to
configure site replication (idempotent: skipped when the pair already exists).

Usage:
    ./scripts/test/site_replication_smoke.py            # up: start both + pair
    ./scripts/test/site_replication_smoke.py status     # process + pair status
    ./scripts/test/site_replication_smoke.py smoke      # bidirectional object check
    ./scripts/test/site_replication_smoke.py logs       # tail both server logs
    ./scripts/test/site_replication_smoke.py down       # stop both processes
    ./scripts/test/site_replication_smoke.py clean      # down + wipe site data

State lives under target/: volumes in target/volume/site-{a,b}/test{1..4},
logs and pidfiles in target/logs/site-{a,b}. Build the server first with
`cargo build --bin rustfs`. Requests are SigV4-signed the same way as
crates/e2e_test (service "s3", region "us-east-1", UNSIGNED-PAYLOAD).
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ADMIN_PREFIX = "/rustfs/admin/v3"
REGION = "us-east-1"
SERVICE = "s3"
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
STOP_GRACE_SECONDS = 10.0


@dataclass
class Site:
    name: str
    port: int
    console_port: int
    access_key: str
    secret_key: str

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def volume_dir(self) -> Path:
        return REPO_ROOT / "target" / "volume" / self.name

    @property
    def log_dir(self) -> Path:
        return REPO_ROOT / "target" / "logs" / self.name

    @property
    def stdout_log(self) -> Path:
        return self.log_dir / "stdout.log"

    @property
    def pid_file(self) -> Path:
        return self.log_dir / "rustfs.pid"


# ---------------------------------------------------------------------------
# SigV4 signing (stdlib only)
# ---------------------------------------------------------------------------


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _uri_encode(value: str, encode_slash: bool) -> str:
    safe = "-._~" + ("" if encode_slash else "/")
    return urllib.parse.quote(value, safe=safe)


def _canonical_query(query: str) -> str:
    if not query:
        return ""
    pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
    encoded = sorted((_uri_encode(k, True), _uri_encode(v, True)) for k, v in pairs)
    return "&".join(f"{k}={v}" for k, v in encoded)


def signed_request(
    site: Site,
    method: str,
    path: str,
    query: str = "",
    body: bytes | None = None,
    content_type: str | None = None,
    timeout: float = 15.0,
) -> tuple[int, bytes]:
    """Send a SigV4-signed request; returns (status_code, body_bytes)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    headers = {
        "host": f"127.0.0.1:{site.port}",
        "x-amz-content-sha256": UNSIGNED_PAYLOAD,
        "x-amz-date": amz_date,
    }
    if content_type:
        headers["content-type"] = content_type

    signed_names = ";".join(sorted(headers))
    canonical_headers = "".join(f"{k}:{headers[k].strip()}\n" for k in sorted(headers))
    canonical_request = "\n".join(
        [
            method,
            _uri_encode(path, False),
            _canonical_query(query),
            canonical_headers,
            signed_names,
            UNSIGNED_PAYLOAD,
        ]
    )

    scope = f"{date_stamp}/{REGION}/{SERVICE}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    key = _hmac(_hmac(_hmac(_hmac(f"AWS4{site.secret_key}".encode(), date_stamp), REGION), SERVICE), "aws4_request")
    signature = hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={site.access_key}/{scope}, SignedHeaders={signed_names}, Signature={signature}"
    )

    url = f"{site.endpoint}{urllib.parse.quote(path, safe='/-._~')}"
    if query:
        url += f"?{query}"
    request = urllib.request.Request(url, data=body, method=method)
    for name, value in headers.items():
        if name != "host":
            request.add_header(name, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()


def admin(site: Site, method: str, subpath: str, query: str = "", payload: object | None = None) -> tuple[int, bytes]:
    body = None
    content_type = None
    if payload is not None:
        body = json.dumps(payload).encode()
        content_type = "application/json"
    return signed_request(site, method, f"{ADMIN_PREFIX}/{subpath}", query, body, content_type)


# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------


def read_pid(site: Site) -> int | None:
    try:
        pid = int(site.pid_file.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        site.pid_file.unlink(missing_ok=True)
        return None
    return pid


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def start_site(site: Site, binary: Path, console: bool) -> None:
    if (pid := read_pid(site)) is not None:
        print(f"[ok] {site.name} already running (pid {pid}, {site.endpoint})")
        return
    if port_in_use(site.port):
        raise SystemExit(
            f"[fail] port {site.port} is in use but not managed by this script; "
            f"stop the other process first (lsof -iTCP:{site.port} -sTCP:LISTEN)"
        )

    for index in range(1, 5):
        (site.volume_dir / f"test{index}").mkdir(parents=True, exist_ok=True)
    site.log_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.setdefault("RUST_LOG", "rustfs=info,ecstore=warn,s3s=warn,iam=info")
    env.update(
        {
            "RUSTFS_ACCESS_KEY": site.access_key,
            "RUSTFS_SECRET_KEY": site.secret_key,
            "RUSTFS_VOLUMES": f"./target/volume/{site.name}/test{{1...4}}",
            "RUSTFS_ADDRESS": f":{site.port}",
            "RUSTFS_SERVER_DOMAINS": f"127.0.0.1:{site.port}",
            "RUSTFS_CONSOLE_ENABLE": "true" if console else "false",
            "RUSTFS_CONSOLE_ADDRESS": f"127.0.0.1:{site.console_port}",
            "RUSTFS_OBS_LOG_DIRECTORY": f"./target/logs/{site.name}",
            "RUSTFS_UNSAFE_BYPASS_DISK_CHECK": "true",
            "RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET": "true",
            # Let a locally running console dev server (pnpm dev) reach the S3/admin API.
            "RUSTFS_CORS_ALLOWED_ORIGINS": "http://localhost:3000,http://127.0.0.1:3000",
        }
    )

    with site.stdout_log.open("ab") as log:
        process = subprocess.Popen(
            [str(binary)],
            cwd=REPO_ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    site.pid_file.write_text(f"{process.pid}\n")
    print(f"[ok] started {site.name} (pid {process.pid}, {site.endpoint}, log {site.stdout_log.relative_to(REPO_ROOT)})")


def stop_site(site: Site) -> None:
    pid = read_pid(site)
    if pid is None:
        print(f"[ok] {site.name} not running")
        return
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + STOP_GRACE_SECONDS
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.2)
    else:
        print(f"[warn] {site.name} (pid {pid}) ignored SIGTERM, sending SIGKILL")
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    site.pid_file.unlink(missing_ok=True)
    print(f"[ok] stopped {site.name} (pid {pid})")


def wait_ready(sites: list[Site], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    for site in sites:
        while True:
            if read_pid(site) is None:
                raise SystemExit(f"[fail] {site.name} exited during startup; check {site.stdout_log.relative_to(REPO_ROOT)}")
            try:
                status, _ = signed_request(site, "GET", "/", timeout=3.0)
                if status < 500:
                    print(f"[ok] {site.name} is ready at {site.endpoint}")
                    break
            except (urllib.error.URLError, OSError, TimeoutError):
                pass
            if time.monotonic() > deadline:
                raise SystemExit(f"[fail] {site.name} ({site.endpoint}) not ready within {timeout:.0f}s")
            time.sleep(1.0)


# ---------------------------------------------------------------------------
# Site replication
# ---------------------------------------------------------------------------


def pair_state(site: Site) -> dict:
    status, body = admin(site, "GET", "site-replication/info")
    if status != 200:
        raise SystemExit(f"[fail] site-replication info: HTTP {status} {body.decode(errors='replace')}")
    return json.loads(body)


def ensure_pair(site_a: Site, site_b: Site) -> None:
    info = pair_state(site_a)
    if info.get("enabled") and len(info.get("sites", [])) >= 2:
        endpoints = ", ".join(peer.get("endpoint", "?") for peer in info["sites"])
        print(f"[ok] site replication already configured ({endpoints})")
        return

    peers = [
        {"name": s.name, "endpoints": s.endpoint, "accessKey": s.access_key, "secretKey": s.secret_key}
        for s in (site_a, site_b)
    ]
    status, body = admin(site_a, "PUT", "site-replication/add", "replicateILMExpiry=false", peers)
    if status != 200:
        text = body.decode(errors="replace")
        hint = ""
        if "non-empty" in text:
            hint = f"\n  hint: both sites already hold data; run `{sys.argv[0]} clean` for a fresh pair"
        raise SystemExit(f"[fail] site-replication add: HTTP {status} {text}{hint}")
    result = json.loads(body)
    if not result.get("success", False):
        raise SystemExit(f"[fail] site-replication add rejected: {json.dumps(result, indent=2)}")
    print(f"[ok] site replication configured: {result.get('status', '')}")


def remove_pair(site: Site) -> None:
    status, body = admin(site, "PUT", "site-replication/remove", payload={"all": True})
    if status != 200:
        raise SystemExit(f"[fail] site-replication remove: HTTP {status} {body.decode(errors='replace')}")
    print(f"[ok] site replication removed: {body.decode(errors='replace')}")


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------


def put_object(site: Site, bucket: str, key: str, data: bytes) -> None:
    status, body = signed_request(site, "PUT", f"/{bucket}/{key}", body=data, content_type="application/octet-stream")
    if status != 200:
        raise SystemExit(f"[fail] PUT {site.name}/{bucket}/{key}: HTTP {status} {body.decode(errors='replace')}")


def wait_object(site: Site, bucket: str, key: str, expected: bytes, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last = "no response yet"
    while time.monotonic() < deadline:
        status, body = signed_request(site, "GET", f"/{bucket}/{key}")
        if status == 200 and body == expected:
            print(f"[ok] {key} replicated to {site.name}")
            return
        last = f"HTTP {status}" if status != 200 else "body mismatch"
        time.sleep(1.0)
    raise SystemExit(f"[fail] {key} did not appear on {site.name} within {timeout:.0f}s (last: {last})")


def smoke(site_a: Site, site_b: Site, timeout: float) -> None:
    bucket = f"sr-smoke-{uuid.uuid4().hex[:8]}"
    status, body = signed_request(site_a, "PUT", f"/{bucket}")
    if status != 200:
        raise SystemExit(f"[fail] create bucket {bucket} on {site_a.name}: HTTP {status} {body.decode(errors='replace')}")
    print(f"[ok] created bucket {bucket} on {site_a.name}")

    # Bucket creation itself must replicate before objects can flow.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, _ = signed_request(site_b, "GET", f"/{bucket}", query="location=")
        if status == 200:
            print(f"[ok] bucket {bucket} replicated to {site_b.name}")
            break
        time.sleep(1.0)
    else:
        raise SystemExit(f"[fail] bucket {bucket} did not replicate to {site_b.name} within {timeout:.0f}s")

    payload_ab = f"hello from {site_a.name} {uuid.uuid4()}".encode()
    put_object(site_a, bucket, "from-a.txt", payload_ab)
    wait_object(site_b, bucket, "from-a.txt", payload_ab, timeout)

    payload_ba = f"hello from {site_b.name} {uuid.uuid4()}".encode()
    put_object(site_b, bucket, "from-b.txt", payload_ba)
    wait_object(site_a, bucket, "from-b.txt", payload_ba, timeout)

    print(f"[ok] bidirectional replication verified via bucket {bucket}")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_up(sites: list[Site], binary: Path, console: bool, timeout: float) -> None:
    if not binary.is_file():
        raise SystemExit(f"[fail] {binary} not found; build it first: cargo build --bin rustfs")
    for site in sites:
        start_site(site, binary, console)
    wait_ready(sites, timeout)
    ensure_pair(sites[0], sites[1])
    print("[ok] lab is up:")
    for site in sites:
        print(f"      {site.name}: {site.endpoint}  (admin {site.access_key}/{site.secret_key})")


def cmd_status(sites: list[Site]) -> None:
    any_up = False
    for site in sites:
        pid = read_pid(site)
        if pid is not None:
            any_up = True
            print(f"[ok] {site.name}: running (pid {pid}, {site.endpoint})")
        else:
            print(f"[--] {site.name}: stopped")
    if not any_up:
        return
    try:
        info = pair_state(sites[0])
    except SystemExit as err:
        print(err)
        return
    if info.get("enabled"):
        print(f"[ok] site replication enabled, peers: {', '.join(p.get('endpoint', '?') for p in info.get('sites', []))}")
    else:
        print("[--] site replication not configured")


def cmd_logs(sites: list[Site], lines: int) -> None:
    for site in sites:
        print(f"===== {site.name} ({site.stdout_log.relative_to(REPO_ROOT)}) =====")
        try:
            content = site.stdout_log.read_text(errors="replace").splitlines()
        except FileNotFoundError:
            print("(no log yet)")
            continue
        for line in content[-lines:]:
            print(line)


def cmd_clean(sites: list[Site]) -> None:
    for site in sites:
        stop_site(site)
    for site in sites:
        if site.volume_dir.exists():
            shutil.rmtree(site.volume_dir)
            print(f"[ok] wiped {site.volume_dir.relative_to(REPO_ROOT)}")
    print("[ok] clean; next `up` starts a fresh pair")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "command",
        nargs="?",
        default="up",
        choices=["up", "down", "restart", "status", "logs", "smoke", "info", "remove", "clean"],
    )
    parser.add_argument("--port-a", type=int, default=9000, help="site A S3 port (default: %(default)s)")
    parser.add_argument("--port-b", type=int, default=9020, help="site B S3 port (default: %(default)s)")
    parser.add_argument("--access-key", default="rustfsadmin")
    parser.add_argument("--secret-key", default="rustfsadmin")
    parser.add_argument("--binary", type=Path, default=REPO_ROOT / "target" / "debug" / "rustfs")
    parser.add_argument("--console", action="store_true", help="also start the web console (ports 9001/9021)")
    parser.add_argument("--timeout", type=float, default=60.0, help="per-step wait timeout in seconds")
    parser.add_argument("--lines", type=int, default=30, help="log lines per site for `logs`")
    args = parser.parse_args()

    site_a = Site("site-a", args.port_a, args.port_a + 1, args.access_key, args.secret_key)
    site_b = Site("site-b", args.port_b, args.port_b + 1, args.access_key, args.secret_key)
    sites = [site_a, site_b]

    if args.command == "up":
        cmd_up(sites, args.binary, args.console, args.timeout)
    elif args.command == "down":
        for site in sites:
            stop_site(site)
    elif args.command == "restart":
        for site in sites:
            stop_site(site)
        cmd_up(sites, args.binary, args.console, args.timeout)
    elif args.command == "status":
        cmd_status(sites)
    elif args.command == "logs":
        cmd_logs(sites, args.lines)
    elif args.command == "smoke":
        smoke(site_a, site_b, args.timeout)
    elif args.command == "info":
        print(json.dumps(pair_state(site_a), indent=2, ensure_ascii=False))
    elif args.command == "remove":
        remove_pair(site_a)
    elif args.command == "clean":
        cmd_clean(sites)


if __name__ == "__main__":
    main()
