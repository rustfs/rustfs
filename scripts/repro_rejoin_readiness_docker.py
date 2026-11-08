#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# Licensed under the Apache License, Version 2.0.
"""Reproduce recovered-node admission on an isolated five-node HTTPS Docker pool.

Run on a Linux AMD64 or ARM64 Docker daemon, with an image matching the daemon::

    python3 scripts/repro_rejoin_readiness_docker.py run \
        --image IMAGE --expected-source COMMIT_SHA --out-dir /tmp/rejoin-case \
        --gate cluster --payload-mode distinct

The image must carry io.rustfs.repro.source_revision and
io.rustfs.repro.binary_sha256 labels. It is resolved to a local immutable image
ID; nothing is pulled. The image needs Python 3, getent and /usr/bin/rustfs.
The host needs Docker, OpenSSL and curl with built-in AWS SigV4 support.

Evidence and generated credentials/CA keys live in a new private directory.
That directory also retains the driver source snapshot and its SHA256. A failed
immediate recovery batch is followed by one separate node-write-gated read-only
convergence sample; that later sample never changes the original failure.
Do not upload that directory wholesale. Failures retain stopped Docker resources
after recovery and evidence capture, even with --cleanup-on-success. Explicit cleanup verifies every resource's exact
name and ownership before deleting any::

    python3 scripts/repro_rejoin_readiness_docker.py cleanup --out-dir /tmp/rejoin-case

The evidence records the host and Docker-daemon architectures. All five nodes
share one Docker daemon; their volumes do not establish independent physical
failure domains. Image/daemon architecture mismatches are rejected rather than
silently running the server under architecture emulation.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import inspect
import io
import json
import os
from pathlib import Path
import platform
import re
import secrets
import signal
import subprocess
import sys
import tarfile
import threading
import time
from datetime import datetime, timezone
from typing import Any


PROJECT_RE = re.compile(r"rustfs-rejoin-repro-[0-9a-f]{16}\Z")
SOURCE_RE = re.compile(r"[0-9a-f]{40}\Z")
IMAGE_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA_RE = re.compile(r"[0-9a-f]{64}\Z")
LABEL_PREFIX = "io.rustfs.repro."
OWNER = "rejoin-readiness-v1"
PORT = 19000
ORIGINAL_PAYLOAD = b"same payload for reference and each degraded overwrite\n"
GATE_PATHS = {
    "cluster": ("/minio/health/cluster",),
    "node-write": ("/health/ready", "/minio/health/cluster"),
}


class ReproError(RuntimeError):
    """A failed precondition or experiment assertion."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    path.chmod(0o600)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def elf_architecture(header: bytes, expected_arch: str = "") -> str:
    """Validate a Linux executable's ELF64 header and optional native architecture."""
    if len(header) < 64 or header[:7] != b"\x7fELF\x02\x01\x01":
        raise ValueError("binary must have an ELF64 little-endian version-one header")
    if (int.from_bytes(header[16:18], "little") not in (2, 3)
            or int.from_bytes(header[20:24], "little") != 1
            or int.from_bytes(header[52:54], "little") != 64):
        raise ValueError("invalid executable ELF64 header")
    machines = {183: "arm64", 62: "amd64"}
    machine = int.from_bytes(header[18:20], "little")
    if machine not in machines:
        raise ValueError("unsupported ELF machine; expected AArch64 or x86_64")
    architecture = machines[machine]
    if expected_arch and architecture != expected_arch:
        raise ValueError("binary ELF architecture does not match the Docker daemon architecture")
    return architecture


def payload_for(mode: str, generation: int, reference: bool = False) -> bytes:
    if mode == "original":
        return ORIGINAL_PAYLOAD
    if mode != "distinct" or generation < 0:
        raise ReproError("invalid payload mode or generation")
    label = f"{'reference' if reference else 'overwrite'}:{generation:08d}:"
    if len(label) >= len(ORIGINAL_PAYLOAD):
        raise ReproError("payload generation is too large")
    return label.ljust(len(ORIGINAL_PAYLOAD) - 1, ".").encode() + b"\n"


def curl_quote(value: str) -> str:
    if any(c in value for c in "\r\n\0"):
        raise ReproError("curl configuration values cannot contain line breaks")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def parse_http_status(stdout: bytes) -> int:
    value = stdout.decode("ascii", errors="replace").strip()
    if not re.fullmatch(r"[0-9]{3}", value):
        raise ReproError("curl returned an invalid HTTP status")
    return int(value)


def response_cause(status: int, headers: str, body: bytes) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for line in headers.splitlines():
        name, separator, value = line.partition(":")
        if separator and name.lower() in ("retry-after", "x-rustfs-readiness-pending", "x-rustfs-error-cause", "x-amz-request-id"):
            result[name.lower()] = value.strip()
    text = body.decode("utf-8", errors="replace")
    code = re.search(r"<Code>([^<]{1,120})</Code>", text)
    if code:
        result["s3_code"] = code.group(1)
    if status == 503 and "x-rustfs-readiness-pending" in result:
        result["classification"] = "startup_admission"
    elif status == 503:
        result["classification"] = "service_unavailable_without_startup_header"
    elif status == 0:
        result["classification"] = "transport_failure"
    return result


def canonical_architecture(value: Any) -> str:
    aliases = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}
    if not isinstance(value, str) or value not in aliases:
        raise ReproError("unsupported architecture; the experiment requires AMD64 or ARM64")
    return aliases[value]


def validate_image(info: dict[str, Any], source: str, expected_binary: str | None = None,
                   *, expected_arch: str) -> dict[str, str]:
    if not SOURCE_RE.fullmatch(source):
        raise ReproError("expected source must be a lowercase 40-character commit SHA")
    image_id = info.get("Id", "")
    labels = (info.get("Config") or {}).get("Labels") or {}
    binary = labels.get(LABEL_PREFIX + "binary_sha256", "")
    if not IMAGE_RE.fullmatch(image_id):
        raise ReproError("image inspection did not return an immutable image ID")
    architecture = canonical_architecture(info.get("Architecture"))
    if info.get("Os") != "linux" or architecture != canonical_architecture(expected_arch):
        raise ReproError("the experiment requires a Linux image matching the Docker daemon architecture")
    inherited = (info.get("Config") or {}).get("Env") or ()
    if any(entry.partition("=")[0].startswith(("RUSTFS_", "MINIO_")) or entry.partition("=")[0] == "RUST_LOG"
           for entry in inherited):
        raise ReproError("the image must not inherit RUSTFS_*, MINIO_* or RUST_LOG overrides")
    if labels.get(LABEL_PREFIX + "source_revision") != source:
        raise ReproError("image source revision does not match --expected-source")
    if not SHA_RE.fullmatch(binary):
        raise ReproError("image requires a valid binary SHA256 label")
    if expected_binary is not None and binary != expected_binary:
        raise ReproError("image binary SHA256 does not match the expected binary")
    return {"id": image_id, "source_revision": source, "binary_sha256": binary, "architecture": architecture, "os": "linux",
            "build_profile": labels.get(LABEL_PREFIX + "build_profile", "unknown")}


def expected_resources(project: str) -> list[tuple[str, str, str]]:
    if not PROJECT_RE.fullmatch(project):
        raise ReproError("invalid experiment project name")
    resources = [("container", f"{project}-init", "init")]
    resources += [("container", f"{project}-storage{n:02}", "node") for n in range(1, 6)]
    resources += [("volume", f"{project}-{kind}{n:02}", kind) for n in range(1, 6) for kind in ("data", "config")]
    return resources + [("network", project, "network")]


def resource_labels(project: str, source: str, kind: str) -> dict[str, str]:
    return {LABEL_PREFIX + "owner": OWNER, LABEL_PREFIX + "project": project,
            LABEL_PREFIX + "source_revision": source, LABEL_PREFIX + "kind": kind}


def verify_ownership(info: dict[str, Any], resource: str, name: str, kind: str, project: str, source: str) -> None:
    labels = ((info.get("Config") or {}).get("Labels") or {}) if resource == "container" else (info.get("Labels") or {})
    actual_name = info.get("Name", "").removeprefix("/")
    if actual_name != name or any(labels.get(key) != value for key, value in resource_labels(project, source, kind).items()):
        raise ReproError(f"refusing to mutate unowned {resource} {name}")


def is_missing_resource(resource: str, name: str, error: str) -> bool:
    subject = re.escape(name.lower())
    forms = {
        "container": rf"no such container:\s*{subject}",
        "volume": rf"(?:no such volume:\s*{subject}|get {subject}:\s*no such volume)",
        "network": rf"(?:no such network:\s*{subject}|network {subject} not found)",
        "image": rf"no such image:\s*{subject}(?::latest)?",
    }
    if resource not in forms:
        return False
    pattern = r"(?:error(?: response from daemon)?:\s*)?" + forms[resource]
    lines = [line.strip().lower() for line in error.splitlines() if line.strip()]
    return bool(lines) and all(re.fullmatch(pattern, line) is not None for line in lines)


class Commands:
    def __init__(self) -> None:
        self.redactions: list[str] = []

    def redact(self, value: str) -> str:
        for secret in self.redactions:
            value = value.replace(secret, "[REDACTED]")
        return value

    def run(self, args: list[str], *, data: bytes | None = None, timeout: float = 120,
            check: bool = True) -> subprocess.CompletedProcess[bytes]:
        try:
            result = subprocess.run(args, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=timeout, check=False)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ReproError(f"{args[0]} {args[1] if len(args) > 1 else ''} failed: {type(error).__name__}") from error
        if check and result.returncode:
            detail = self.redact(result.stderr.decode(errors="replace"))[-1500:]
            raise ReproError(f"{args[0]} {args[1] if len(args) > 1 else ''} failed ({result.returncode}): {detail}")
        return result

    def docker(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        return self.run(["docker", *args], **kwargs)

    def inspect(self, resource: str, name: str, *, optional: bool = False) -> dict[str, Any] | None:
        result = self.docker(resource, "inspect", name, check=False)
        if result.returncode:
            error = result.stderr.decode(errors="replace").lower()
            missing = is_missing_resource(resource, name, error)
            if optional and missing:
                return None
            raise ReproError(f"could not inspect {resource} {name}")
        value = json.loads(result.stdout)
        if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
            raise ReproError(f"unexpected inspection result for {resource}")
        return value[0]


def cleanup_resources(commands: Commands, manifest: dict[str, Any]) -> None:
    project, source = manifest["project"], manifest["source_revision"]
    if not isinstance(project, str) or not isinstance(source, str) or not SOURCE_RE.fullmatch(source):
        raise ReproError("invalid source revision in manifest")
    owned = []
    # Verify the entire set before deleting anything; a bad manifest cannot
    # partially tear down another experiment.
    for resource, name, kind in expected_resources(project):
        info = commands.inspect(resource, name, optional=True)
        if info is not None:
            verify_ownership(info, resource, name, kind, project, source)
            owned.append((resource, name, info.get("Id", name)))
    for resource, name, identifier in owned:
        if resource == "container":
            commands.docker("container", "rm", "--force", identifier)
        elif resource == "network":
            commands.docker(resource, "rm", identifier)
        else:
            commands.docker(resource, "rm", name)


INIT_SCRIPT = r'''
import hashlib, json, os, pathlib, stat, subprocess, sys, tarfile
root = pathlib.Path('/stage')
for n in range(1, 6):
    for kind in ('data', 'config'):
        path = root / f'{kind}{n:02}'
        os.chmod(path, 0o700)
        os.chown(path, 10001, 10001)
with tarfile.open(fileobj=sys.stdin.buffer, mode='r|') as archive:
    for member in archive:
        parts = pathlib.PurePosixPath(member.name).parts
        if (not member.isfile() or len(parts) != 3 or
            parts[0] not in [f'config{n:02}' for n in range(1, 6)] or
            parts[1] not in ('tls', 'secrets') or
            parts[2] not in ('ca.crt', 'rustfs_cert.pem', 'rustfs_key.pem', 'access-key', 'secret-key')):
            raise RuntimeError('unexpected configuration archive entry')
        path = root.joinpath(*parts)
        path.parent.mkdir(mode=0o700, exist_ok=True)
        os.chown(path.parent, 10001, 10001)
        data = archive.extractfile(member).read()
        with open(path, 'xb') as output:
            output.write(data)
        os.chmod(path, 0o600)
        os.chown(path, 10001, 10001)
with open('/usr/bin/rustfs', 'rb') as binary:
    binary_architecture = elf_architecture(binary.read(64), sys.argv[1])
    binary.seek(0)
    digest = hashlib.file_digest(binary, 'sha256').hexdigest() if hasattr(hashlib, 'file_digest') else None
    if digest is None:
        binary.seek(0)
        h = hashlib.sha256()
        for block in iter(lambda: binary.read(1024 * 1024), b''):
            h.update(block)
        digest = h.hexdigest()
if digest != sys.argv[2]:
    raise RuntimeError('binary SHA256 does not match its image label')
filesystems = []
for n in range(1, 6):
    path = f'/stage/data{n:02}'
    kind = subprocess.check_output(['stat', '-f', '-c', '%T', path], text=True).strip()
    filesystems.append({'node': n, 'filesystem_type': kind, 'uid': os.stat(path).st_uid,
                        'mode': oct(stat.S_IMODE(os.stat(path).st_mode))})
os_release = {}
with open('/etc/os-release') as release:
    for line in release:
        key, separator, value = line.strip().partition('=')
        if separator and key in ('PRETTY_NAME', 'ID', 'VERSION_ID'):
            os_release[key] = value.strip('"')
version = subprocess.check_output(['/usr/bin/rustfs', '--version'], text=True).strip()
print(json.dumps({'binary_sha256': digest, 'binary_architecture': binary_architecture, 'binary_version': version,
                  'runtime_os': os_release, 'data_volumes': filesystems}))
'''

NODE_ENTRYPOINT = r'''
set -eu
attempt=0
while :; do
    all_ready=true
    for peer in storage01 storage02 storage03 storage04 storage05; do
        getent hosts "$peer" >/dev/null || all_ready=false
    done
    [ "$all_ready" = true ] && break
    attempt=$((attempt + 1))
    [ "$attempt" -lt 60 ] || exit 1
    sleep 1
done
exec /usr/bin/rustfs
'''

MAX_RUNTIME_LOG_BYTES = 32 * 1024 * 1024
MAX_RUNTIME_LOG_FILES = 256
REQUIRED_DIAGNOSTIC_EVENTS = (
    "object_namespace_lock/acquired", "object_namespace_lock/guard_dropped", "namespace_lock_release/released",
)

LOG_ARCHIVE_SCRIPT = r'''
import os, pathlib, stat, sys, tarfile
root = pathlib.Path(sys.argv[1])
remaining, max_files = int(sys.argv[2]), int(sys.argv[3])
if not root.is_dir():
    raise RuntimeError('runtime log directory is unavailable')
files = entries = 0
with tarfile.open(fileobj=sys.stdout.buffer, mode='w|') as archive:
    for directory, dirs, names, descriptor in os.fwalk(root, follow_symlinks=False):
        entries += len(dirs) + len(names)
        if entries > max_files * 2:
            raise RuntimeError('runtime log entry limit exceeded')
        dirs.sort()
        for name in sorted(names):
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
            with os.fdopen(fd, 'rb') as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode):
                    continue
                files += 1
                if files > max_files or info.st_size > remaining:
                    raise RuntimeError('runtime log capture budget exceeded')
                remaining -= info.st_size
                member = tarfile.TarInfo(str(pathlib.Path(directory).relative_to(root) / name))
                member.size, member.mode = info.st_size, 0o600
                archive.addfile(member, source)
'''


def diagnostic_event_counts(data: bytes) -> dict[str, int]:
    counts = dict.fromkeys(REQUIRED_DIAGNOSTIC_EVENTS, 0)
    events = {"object_namespace_lock", "object_namespace_lock_peer", "namespace_lock_release", "distributed_lock_cleanup"}
    for line in io.BytesIO(data):
        line = line.lstrip()
        if not line.startswith(b"{"):
            _, separator, line = line.partition(b" ")  # docker logs --timestamps
            if not separator:
                continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        fields = row.get("fields") if isinstance(row.get("fields"), dict) else row
        event, state = fields.get("event"), fields.get("state")
        if isinstance(event, str) and event in events and isinstance(state, str) and len(state) <= 128:
            key = event + "/" + state
            counts[key] = counts.get(key, 0) + 1
    return counts


class Experiment:
    def __init__(self, args: argparse.Namespace, commands: Commands) -> None:
        self.args, self.commands = args, commands
        self.out = args.out_dir.resolve()
        self.project = "rustfs-rejoin-repro-" + secrets.token_hex(8)
        self.source = args.expected_source
        self.nodes: dict[int, dict[str, Any]] = {}
        self.acknowledged: dict[str, bytes] = {}
        self.first_failure: dict[str, Any] | None = None
        self.request_number = 0
        self.record_lock = threading.Lock()
        self.started = False
        self.summary: dict[str, Any] = {"project": self.project, "source_revision": self.source,
            "gate": args.gate, "payload_mode": args.payload_mode, "stress_iterations": args.stress_iterations,
            "outage_wait_seconds": args.outage_wait, "host_platform": platform.system(), "host_architecture": platform.machine(),
            "declared_build_profile": args.build_profile, "diagnostics_enabled": args.diagnostics,
            "experiment_scope": "five HTTPS containers on one Docker daemon; their volumes do not establish independent physical failure domains",
            "started_at": utc_now(), "object_request_retries": 0}

    def event(self, value: dict[str, Any]) -> None:
        with self.record_lock:
            with (self.out / "events.jsonl").open("a") as output:
                output.write(self.commands.redact(json.dumps(value, sort_keys=True)) + "\n")

    def labels(self, kind: str) -> list[str]:
        return [item for key, value in resource_labels(self.project, self.source, kind).items()
                for item in ("--label", f"{key}={value}")]

    def fail(self, phase: str, reason: str, request: dict[str, Any] | None = None) -> None:
        failure = {"phase": phase, "reason": self.commands.redact(reason), "at": utc_now()}
        if request:
            failure["request"] = request
        if self.first_failure is None:
            self.first_failure = failure
        self.event({"failure": failure})

    def preflight(self) -> None:
        self.out.mkdir(mode=0o700, parents=False, exist_ok=False)
        os.umask(0o077)
        private = self.out / "private"
        private.mkdir(mode=0o700)
        driver_source = Path(__file__).resolve().read_bytes()
        driver_snapshot = private / "driver.py"
        driver_snapshot.write_bytes(driver_source)
        driver_snapshot.chmod(0o600)
        self.summary["driver"] = {"source_sha256": sha256(driver_source), "snapshot": "private/driver.py"}
        help_result = self.commands.run(["curl", "--disable", "--help", "all"])
        if b"--aws-sigv4" not in help_result.stdout:
            raise ReproError("host curl requires built-in --aws-sigv4 support")
        self.summary["host_tools"] = {
            "curl_version": self.commands.run(["curl", "--disable", "--version"]).stdout.decode(errors="replace").strip(),
            "openssl_version": self.commands.run(["openssl", "version"]).stdout.decode(errors="replace").strip(),
        }
        server = json.loads(self.commands.docker("info", "--format", "{{json .}}").stdout)
        if server.get("OSType") != "linux":
            raise ReproError("this harness requires a Linux Docker daemon")
        daemon_arch = canonical_architecture(server.get("Architecture"))
        image = self.commands.inspect("image", self.args.image)
        assert image is not None
        self.identity = validate_image(image, self.source, self.args.expected_binary_sha256, expected_arch=daemon_arch)
        if self.args.build_profile is not None and self.identity["build_profile"] != self.args.build_profile:
            raise ReproError("image build profile label does not match --build-profile")
        self.summary["image"] = self.identity
        self.summary["docker"] = {key: server.get(key) for key in
            ("ServerVersion", "OperatingSystem", "OSType", "Architecture", "KernelVersion", "Driver", "NCPU", "MemTotal")}
        self.summary["docker"]["canonical_architecture"] = daemon_arch
        self.summary["experiment_scope"] = (
            f"five HTTPS {daemon_arch} containers on one Linux Docker daemon, controlled from "
            f"{self.summary['host_platform']}/{self.summary['host_architecture']}; "
            "their volumes do not establish independent physical failure domains"
        )
        self.summary["runtime_worker_threads"] = self.args.workers
        write_json(self.out / "manifest.json", {"schema": 1, "project": self.project,
            "source_revision": self.source, "image": self.identity})

    def make_configuration(self) -> bytes:
        private = self.out / "private"
        self.access, self.secret = secrets.token_hex(12), secrets.token_hex(24)
        self.commands.redactions = [self.secret, self.access]
        (private / "access-key").write_text(self.access + "\n")
        (private / "secret-key").write_text(self.secret + "\n")
        self.commands.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-sha256",
            "-subj", "/CN=RustFS-isolated-rejoin-CA", "-keyout", str(private / "ca.key"), "-out", str(private / "ca.crt")])
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            for n in range(1, 6):
                node = f"storage{n:02}"
                directory = private / node
                directory.mkdir(mode=0o700)
                self.commands.run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={node}",
                    "-keyout", str(directory / "rustfs_key.pem"), "-out", str(directory / "request.csr")])
                extensions = directory / "extensions"
                extensions.write_text(f"subjectAltName=DNS:{node},IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
                self.commands.run(["openssl", "x509", "-req", "-days", "2", "-sha256", "-in", str(directory / "request.csr"),
                    "-CA", str(private / "ca.crt"), "-CAkey", str(private / "ca.key"), "-CAcreateserial", "-extfile", str(extensions),
                    "-out", str(directory / "rustfs_cert.pem")])
                for name, source in (("tls/ca.crt", private / "ca.crt"), ("tls/rustfs_cert.pem", directory / "rustfs_cert.pem"),
                    ("tls/rustfs_key.pem", directory / "rustfs_key.pem"), ("secrets/access-key", private / "access-key"),
                    ("secrets/secret-key", private / "secret-key")):
                    data = source.read_bytes()
                    info = tarfile.TarInfo(f"config{n:02}/{name}")
                    info.size, info.mode, info.uid, info.gid = len(data), 0o600, 10001, 10001
                    archive.addfile(info, io.BytesIO(data))
        return buffer.getvalue()

    def setup(self) -> None:
        configuration = self.make_configuration()
        self.commands.docker("network", "create", "--driver", "bridge", *self.labels("network"), self.project)
        network = self.commands.inspect("network", self.project)
        if network is None:
            raise ReproError("could not inspect the experiment network")
        verify_ownership(network, "network", self.project, "network", self.project, self.source)
        self.summary["network"] = {key: network.get(key) for key in ("Id", "Name", "Driver", "Internal")}
        if network.get("Driver") != "bridge" or network.get("Internal") is not False:
            raise ReproError("the experiment requires its declared non-internal bridge network")
        for n in range(1, 6):
            for kind in ("data", "config"):
                name = f"{self.project}-{kind}{n:02}"
                self.commands.docker("volume", "create", *self.labels(kind), name)
                volume = self.commands.inspect("volume", name)
                if volume is None:
                    raise ReproError("could not inspect a created experiment volume")
                # Docker may reuse an existing name without applying labels.
                # Validate every volume before staging can mount or chown it.
                verify_ownership(volume, "volume", name, kind, self.project, self.source)
        mounts = [item for n in range(1, 6) for kind in ("data", "config")
                  for item in ("--mount", f"type=volume,src={self.project}-{kind}{n:02},dst=/stage/{kind}{n:02},volume-nocopy")]
        init_name = self.project + "-init"
        init_id = self.commands.docker("container", "create", "--pull=never", "--name", init_name, "--network", "none",
            "--user", "0:0", "--interactive", *self.labels("init"), *mounts,
            "--entrypoint", "python3", self.identity["id"], "-c", inspect.getsource(elf_architecture) + "\n" + INIT_SCRIPT,
            self.identity["architecture"], self.identity["binary_sha256"]).stdout.decode().strip()
        staged = json.loads(self.commands.docker("container", "start", "--attach", "--interactive", init_id, data=configuration).stdout)
        if staged["binary_architecture"] != self.identity["architecture"]:
            raise ReproError("the binary ELF architecture does not match the image and daemon")
        if staged["binary_sha256"] != self.identity["binary_sha256"]:
            raise ReproError("the image binary does not match its SHA256 label")
        self.summary["staging"] = staged
        info = self.commands.inspect("container", init_id)
        assert info is not None
        verify_ownership(info, "container", init_name, "init", self.project, self.source)
        self.commands.docker("container", "rm", init_id)
        environment = {
            "RUSTFS_VOLUMES": " ".join(f"https://storage{n:02}:{PORT}/data" for n in range(1, 6)),
            "RUSTFS_ADDRESS": f"0.0.0.0:{PORT}", "RUSTFS_CONSOLE_ADDRESS": "0.0.0.0:19001", "RUSTFS_CONSOLE_ENABLE": "true",
            "RUSTFS_TLS_ENABLED": "true", "RUSTFS_TLS_PATH": "/config/tls", "RUSTFS_EXTRA_CA_CERT": "/config/tls/ca.crt",
            "RUSTFS_STORAGE_CLASS_STANDARD": "EC:2", "RUSTFS_STORAGE_CLASS_RRS": "EC:2", "RUSTFS_ERASURE_SET_DRIVE_COUNT": "5",
            "RUSTFS_ACCESS_KEY_FILE": "/config/secrets/access-key", "RUSTFS_SECRET_KEY_FILE": "/config/secrets/secret-key",
            "RUSTFS_OBS_LOG_DIRECTORY": "/logs", "RUSTFS_OBS_LOGGER_LEVEL": "warn", "RUSTFS_HEAL_MAX_CONCURRENT_HEALS": "1",
            "RUSTFS_HEAL_MAX_CONCURRENT_PER_SET": "1", "RUSTFS_SCANNER_SPEED": "slow", "RUSTFS_SCANNER_IDLE_MODE": "true",
            "RUSTFS_SCANNER_MAX_CONCURRENT_SET_SCANS": "1", "RUSTFS_SCANNER_MAX_CONCURRENT_DISK_SCANS": "1",
            "RUSTFS_HEALTH_CLUSTER_TIMEOUT_MS": "15000",
        }
        if self.args.workers is not None:
            environment["RUSTFS_RUNTIME_WORKER_THREADS"] = str(self.args.workers)
        if self.args.diagnostics:
            environment.update({"RUSTFS_OBJECT_LOCK_DIAG_ENABLE": "true", "RUSTFS_OBJECT_LOCK_DIAG_SLOW_ACQUIRE_MS": "100",
                "RUSTFS_OBJECT_LOCK_DIAG_SLOW_HOLD_MS": "100",
                "RUSTFS_OBS_LOG_DIRECTORY": "", "RUSTFS_OBS_LOG_STDOUT_ENABLED": "true",
                "RUST_LOG": "warn,rustfs_ecstore::object_lock_diag=trace,rustfs_lock::namespace_owner=trace,"
                            "rustfs_ecstore::cluster::rpc::remote_locker=debug,rustfs_lock::distributed_lock=debug"})
        self.summary["environment"] = environment
        self.summary["logging"] = {"application_sink": "stdout" if self.args.diagnostics else "/logs",
            "docker_driver": "json-file", "docker_max_size": "20m", "docker_max_files": 5,
            "runtime_max_bytes": MAX_RUNTIME_LOG_BYTES, "runtime_max_files": MAX_RUNTIME_LOG_FILES}
        for n in range(1, 6):
            name = f"{self.project}-storage{n:02}"
            args = ["container", "create", "--pull=never", "--name", name, "--hostname", f"storage{n:02}",
                *self.labels("node"), "--network", self.project, "--network-alias", f"storage{n:02}", "--user", "10001:10001",
                "--read-only", "--cpus", "1", "--memory", "1g", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                "--log-driver", "json-file", "--log-opt", "max-size=20m", "--log-opt", "max-file=5",
                "--tmpfs", "/tmp:mode=1777,size=64m", "--tmpfs", "/logs:uid=10001,gid=10001,mode=0700,size=32m",
                "--publish", f"127.0.0.1::{PORT}", "--mount", f"type=volume,src={self.project}-data{n:02},dst=/data,volume-nocopy",
                "--mount", f"type=volume,src={self.project}-config{n:02},dst=/config,readonly,volume-nocopy"]
            args += [item for key, value in environment.items() for item in ("--env", f"{key}={value}")]
            args += ["--entrypoint", "/bin/sh", self.identity["id"], "-c", NODE_ENTRYPOINT]
            identifier = self.commands.docker(*args).stdout.decode().strip()
            if not re.fullmatch(r"[0-9a-f]{64}", identifier):
                raise ReproError("Docker returned an invalid container ID")
            self.nodes[n] = {"id": identifier, "name": name}
        self.started = True
        self.commands.docker("container", "start", *(node["id"] for node in self.nodes.values()))
        self.snapshot("initial-before-gate")

    def snapshot(self, phase: str) -> None:
        snapshot = []
        for n, node in self.nodes.items():
            info = self.commands.inspect("container", node["id"])
            assert info is not None
            verify_ownership(info, "container", node["name"], "node", self.project, self.source)
            bindings = (info.get("NetworkSettings", {}).get("Ports", {}).get(f"{PORT}/tcp") or [])
            node["running"] = info["State"]["Running"]
            if node["running"]:
                if len(bindings) != 1 or bindings[0].get("HostIp") != "127.0.0.1":
                    raise ReproError("S3 must be published on one loopback-only ephemeral port")
                node["port"] = int(bindings[0]["HostPort"])
            node["generation"] = {"container_id": info["Id"], "started_at": info["State"]["StartedAt"],
                                  "restart_count": info.get("RestartCount")}
            limits = info["HostConfig"]
            snapshot.append({"node": n, "image": info["Image"], "generation": node["generation"], "state": info["State"],
                "limits": {key: limits.get(key) for key in ("NanoCpus", "Memory", "ReadonlyRootfs", "CapDrop", "SecurityOpt", "Tmpfs", "LogConfig")},
                "mounts": [{key: mount.get(key) for key in ("Type", "Name", "Destination", "RW", "Driver")} for mount in info["Mounts"]]})
        write_json(self.out / f"containers-{phase}.json", snapshot)

    def request(self, node: int, method: str, path: str, *, payload: bytes | None = None,
                signed: bool = True, phase: str = "probe", timeout: float = 30) -> dict[str, Any]:
        with self.record_lock:
            self.request_number += 1
            request_id = self.request_number
        prefix = f"request-{request_id:05}"
        port = self.nodes[node]["port"]
        url = f"https://storage{node:02}:{port}{path}"
        resolution = f"storage{node:02}:{port}:127.0.0.1"
        body_file, header_file = self.out / f"{prefix}.body", self.out / f"{prefix}.headers"
        config = ["silent", "show-error", 'proto = "=https"', 'noproxy = "*"', "connect-timeout = 5", f"max-time = {timeout}",
            "retry = 0", f"url = {curl_quote(url)}", f"resolve = {curl_quote(resolution)}",
            f"cacert = {curl_quote(str(self.out / 'private/ca.crt'))}", f"request = {curl_quote(method)}",
            f"output = {curl_quote(str(body_file))}", f"dump-header = {curl_quote(str(header_file))}", 'write-out = "%{http_code}"']
        if signed:
            config += ['aws-sigv4 = "aws:amz:us-east-1:s3"', f"user = {curl_quote(self.access + ':' + self.secret)}",
                       f"header = {curl_quote('x-amz-content-sha256: ' + sha256(payload or b''))}"]
        if payload is not None:
            payload_file = self.out / "private" / f"{prefix}.payload"
            payload_file.write_bytes(payload)
            config.append(f"upload-file = {curl_quote(str(payload_file))}")
        config_file = self.out / "private" / f"{prefix}.curl"
        config_file.write_text("\n".join(config) + "\n")
        began, wall = time.monotonic(), utc_now()
        result = self.commands.run(["curl", "--disable", "--config", str(config_file)], check=False, timeout=timeout + 10)
        status = parse_http_status(result.stdout)
        body = body_file.read_bytes() if body_file.exists() else b""
        headers = header_file.read_text(errors="replace") if header_file.exists() else ""
        for secret in self.commands.redactions:
            body = body.replace(secret.encode(), b"[REDACTED]")
        headers = self.commands.redact(headers)
        body_file.write_bytes(body)
        header_file.write_text(headers)
        record = {"id": request_id, "phase": phase, "node": node, "method": method, "path": path,
            "http": status, "curl_exit": result.returncode, "started_at": wall, "duration_seconds": time.monotonic() - began,
            "body": body_file.name, "headers": header_file.name, "body_sha256": sha256(body),
            "generation": self.nodes[node]["generation"], "cause": response_cause(status, headers, body)}
        if result.stderr:
            record["transport_error"] = self.commands.redact(result.stderr.decode(errors="replace"))[-1000:]
        self.event(record)
        return record

    def wait_gate(self, phase: str, *, gate: str | None = None) -> dict[str, Any]:
        selected_gate = self.args.gate if gate is None else gate
        deadline = time.monotonic() + self.args.gate_timeout
        while time.monotonic() < deadline:
            all_ready = True
            for node in range(1, 6):
                for path in GATE_PATHS[selected_gate]:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise ReproError(f"{phase} gate timed out")
                    result = self.request(node, "GET", path, signed=False, phase=phase + "-gate", timeout=min(20, remaining))
                    all_ready &= result["curl_exit"] == 0 and result["http"] == 200
            if all_ready:
                # No sleep, diagnostic probe, or Docker inspection may be added
                # between this return and the immediate recovery object reads.
                return {"gate": selected_gate, "paths": list(GATE_PATHS[selected_gate]), "completed_at": utc_now(),
                        "monotonic": time.monotonic(), "last_request_id": self.request_number}
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        raise ReproError(f"{phase} gate timed out")

    def require(self, record: dict[str, Any], phase: str, expected: bytes | None = None) -> None:
        reason = None
        if record["curl_exit"] or record["http"] != 200:
            reason = "object request failed"
        elif expected is not None and record["body_sha256"] != sha256(expected):
            reason = "object payload did not match the last acknowledged payload"
        if reason:
            self.fail(phase, reason, record)
            raise ReproError(reason)

    def sequence(self) -> None:
        self.summary["initial_gate"] = self.wait_gate("initial")
        deadline = time.monotonic() + self.args.gate_timeout
        while True:
            result = self.request(1, "PUT", "/repro", phase="create-bucket")
            if result["http"] == 200 and not result["curl_exit"]:
                break
            if result["http"] != 503 or result["curl_exit"] or time.monotonic() >= deadline:
                self.require(result, "create-bucket")
            time.sleep(2)
        reference = payload_for(self.args.payload_mode, 0, reference=True)
        self.require(self.request(1, "PUT", "/repro/reference-object", payload=reference, phase="initial-put"), "initial-put")
        self.acknowledged["reference-object"] = reference
        self.require(self.request(3, "GET", "/repro/reference-object", phase="initial-get"), "initial-get", reference)
        generation = 0
        for stopped, peer in ((5, 2), (2, 3)):
            self.commands.docker("container", "stop", "-t", "10", self.nodes[stopped]["id"], timeout=30)
            time.sleep(self.args.outage_wait)
            self.require(self.request(1, "GET", "/repro/reference-object", phase=f"down-{stopped}-reference"), "degraded-reference", reference)
            for iteration in range(self.args.stress_iterations):
                generation += 1
                payload = payload_for(self.args.payload_mode, generation)
                phase = f"down-{stopped}-iteration-{iteration + 1}"
                self.require(self.request(1, "PUT", "/repro/degraded-object", payload=payload, phase=phase), phase)
                self.acknowledged["degraded-object"] = payload
                self.require(self.request(peer, "GET", "/repro/degraded-object", phase=phase), phase, payload)
        self.summary["sequence_passed"] = True

    def read_object_batch(self, phase: str, gate: dict[str, Any]) -> dict[str, Any]:
        started, wall = time.monotonic(), utc_now()
        failed = []
        for key, expected in self.acknowledged.items():
            for node in range(1, 6):
                record = self.request(node, "GET", f"/repro/{key}", phase=phase)
                try:
                    self.require(record, phase, expected)
                except ReproError:
                    failed.append(record["id"])
        return {"failed_request_ids": failed, "count": len(self.acknowledged) * 5,
                "result": "failed" if failed else "passed", "started_at": wall, "finished_at": utc_now(),
                "duration_seconds": time.monotonic() - started,
                "gate_to_batch_start_seconds": started - gate["monotonic"]}

    def recover(self) -> None:
        if not self.started:
            self.summary["recovery"] = "not started: setup did not complete"
            return
        self.snapshot("before-recovery")
        if self.first_failure:
            # Observe the still-failing generation before any stopped peer is
            # restarted or log copying delays volatile lock observations.
            failure_request = self.first_failure.get("request", {})
            cause = failure_request.get("cause", {})
            if cause.get("retry-after") == "1" or cause.get("x-rustfs-error-cause") in (
                    "namespace_lock_timeout", "namespace_lock_unavailable"):
                for sample in range(self.args.lock_samples):
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        futures = [executor.submit(self.request, node, "GET", "/rustfs/admin/v3/top/locks?count=1000",
                            phase=f"pre-recovery-locks-{sample + 1}", timeout=10)
                            for node in self.nodes if self.nodes[node]["running"]]
                        for future in futures:
                            try:
                                record = future.result()
                                status = record["http"]
                                self.event({"lock_snapshot_request_id": record["id"], "sample": sample + 1,
                                    "api_result": "unsupported_or_not_registered" if status in (404, 405, 501)
                                    else "available" if status == 200 else "request_failed", "http": status})
                            except ReproError as error:
                                self.event({"lock_snapshot_error": str(error), "sample": sample + 1})
                    if sample + 1 < self.args.lock_samples:
                        time.sleep(1)
            try:
                self.capture("before-recovery")
            except (Exception, KeyboardInterrupt) as error:
                self.fail("pre-recovery-capture", str(error) or "interrupted")
        restart = [node["id"] for node in self.nodes.values() if not node["running"]]
        if restart:
            self.commands.docker("container", "start", *restart)
        self.snapshot("restarted-before-gate")
        self.summary["recovery_gate"] = self.wait_gate("recovery")
        self.summary["recovery_gets"] = self.read_object_batch("immediate-recovery", self.summary["recovery_gate"])
        # Classification probes run only after the immediate object batch.
        for node in range(1, 6):
            for path in ("/health/ready", "/minio/health/cluster", "/minio/health/cluster/read"):
                self.request(node, "GET", path, signed=False, phase="post-recovery-classification", timeout=20)
        if self.summary["recovery_gets"]["failed_request_ids"]:
            convergence = {"started_at": utc_now()}
            self.summary["convergence"] = convergence
            try:
                convergence["gate"] = self.wait_gate("convergence", gate="node-write")
            except ReproError as error:
                self.fail("convergence-gate", str(error))
                convergence.update(result="gate_failed", finished_at=utc_now())
                return
            convergence["gets"] = self.read_object_batch("later-convergence", convergence["gate"])
            convergence.update(result=convergence["gets"]["result"], finished_at=utc_now())

    def capture(self, phase: str = "final") -> None:
        captures = self.summary.setdefault("log_captures", [])
        totals = dict.fromkeys(REQUIRED_DIAGNOSTIC_EVENTS, 0)
        for node in self.nodes.values():
            info = self.commands.inspect("container", node["id"])
            if info is None:
                raise ReproError("could not inspect a log capture source")
            verify_ownership(info, "container", node["name"], "node", self.project, self.source)
            record = {"phase": phase, "container_id": info["Id"], "node": node["name"], "running": info["State"]["Running"],
                      "started_at": info["State"].get("StartedAt"), "captured_at": utc_now()}
            captures.append(record)
            # The server's explicit json-file retention (20m * 5) bounds this
            # history and preserves it across restarts; no tail hides early events.
            result = self.commands.docker("container", "logs", "--timestamps", node["id"], check=False)
            if result.returncode:
                self.fail("capture", f"Docker stdout capture failed for {node['name']}: "
                          + self.commands.redact(result.stderr.decode(errors="replace"))[-500:])
            history = result.stdout + result.stderr
            for secret in self.commands.redactions:
                history = history.replace(secret.encode(), b"[REDACTED]")
            destination = node["name"].rsplit("-", 1)[-1] + f"-{phase}.log"
            (self.out / destination).write_bytes(history)
            counts = diagnostic_event_counts(history)
            record["docker_history"] = {"method": "docker container logs --timestamps", "path": destination,
                "bytes": len(history), "exit_code": result.returncode, "event_counts": counts}
            runtime = {"method": "docker container exec python3 tar", "bytes": 0, "files": [], "event_counts": {}}
            record["runtime_files"] = runtime
            if self.args.diagnostics:
                runtime.update(status="not_configured", reason="diagnostics_use_stdout_only")
            elif not info["State"]["Running"]:
                runtime.update(status="unavailable", reason="stopped_container_tmpfs_cannot_be_read")
                self.fail("capture", f"stopped container runtime tmpfs is unavailable for {node['name']} during {phase}")
            else:
                # docker cp can omit live tmpfs mounts. Read /logs from inside
                # the running container, without changing its logging state.
                archive = self.commands.docker("container", "exec", node["id"], "python3", "-c", LOG_ARCHIVE_SCRIPT,
                    "/logs", str(MAX_RUNTIME_LOG_BYTES), str(MAX_RUNTIME_LOG_FILES), check=False)
                runtime["archive_bytes"] = len(archive.stdout)
                if archive.returncode:
                    runtime.update(status="error", exit_code=archive.returncode)
                    self.fail("capture", f"live runtime log capture failed for {node['name']}: "
                              + self.commands.redact(archive.stderr.decode(errors="replace"))[-500:])
                else:
                    log_dir = self.out / f"runtime-logs-{phase}" / node["name"].rsplit("-", 1)[-1]
                    log_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
                    try:
                        logs = tarfile.open(fileobj=io.BytesIO(archive.stdout))
                        with logs:
                            for member in logs:
                                if member.isdir():
                                    continue
                                if not member.isfile() or len(runtime["files"]) >= MAX_RUNTIME_LOG_FILES or member.size > MAX_RUNTIME_LOG_BYTES - runtime["bytes"]:
                                    raise ReproError("runtime log archive exceeds its regular-file capture budget")
                                relative = Path(member.name)
                                if relative.is_absolute() or ".." in relative.parts:
                                    raise ReproError("unexpected runtime log archive path")
                                target = log_dir / relative
                                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                                data = logs.extractfile(member).read()
                                for secret in self.commands.redactions:
                                    data = data.replace(secret.encode(), b"[REDACTED]")
                                target.write_bytes(data)
                                runtime["bytes"] += len(data)
                                events = diagnostic_event_counts(data)
                                runtime["files"].append({"path": str(target.relative_to(self.out)), "bytes": len(data)})
                                for event, count in events.items():
                                    runtime["event_counts"][event] = runtime["event_counts"].get(event, 0) + count
                        runtime["status"] = "captured" if runtime["files"] else "empty"
                    except (ReproError, OSError, tarfile.TarError) as error:
                        runtime.update(status="error", reason=self.commands.redact(str(error)))
                        self.fail("capture", str(error))
            for event, count in counts.items():
                totals[event] = totals.get(event, 0) + count
            self.event({"log_capture": record})
        if self.started:
            self.snapshot("final")
        if phase == "final":
            missing = [event for event in REQUIRED_DIAGNOSTIC_EVENTS if not totals[event]]
            status = "missing" if len(missing) == len(REQUIRED_DIAGNOSTIC_EVENTS) else "partial" if missing else "complete"
            self.summary["diagnostic_coverage"] = {"requested": self.args.diagnostics,
                "status": status if self.args.diagnostics else "not_requested", "event_counts": totals, "missing_events": missing,
                "scope": "final pool aggregate of captured retained Docker history; counts do not prove per-owner release"}
            if self.args.diagnostics and missing:
                self.fail("diagnostic-coverage", "required lock trace categories were not captured: " + ", ".join(missing))

    def stop_retained_nodes(self, *, allow_missing: bool = False) -> None:
        running = []
        existing = []
        by_name = {node["name"]: node for node in self.nodes.values()}
        # Include the staging container and partially created nodes. A Docker
        # CLI timeout can leave an owned container alive before its ID is saved.
        # Verify the complete inventory before stopping any immutable ID.
        for resource, name, kind in expected_resources(self.project):
            if resource != "container":
                continue
            node = by_name.get(name)
            optional = allow_missing or node is None
            info = self.commands.inspect("container", node["id"] if node else name, optional=optional)
            if info is None:
                if optional:
                    continue
                raise ReproError("a retained experiment node could not be inspected")
            verify_ownership(info, "container", name, kind, self.project, self.source)
            existing.append((info["Id"], name, kind))
            if info["State"]["Running"]:
                running.append(info["Id"])
        if running:
            self.commands.docker("container", "stop", "-t", "10", *running, timeout=80)
        snapshot = []
        for identifier, name, kind in existing:
            info = self.commands.inspect("container", identifier)
            if info is None:
                raise ReproError("could not confirm a retained container stopped")
            verify_ownership(info, "container", name, kind, self.project, self.source)
            snapshot.append({"id": identifier, "name": name, "kind": kind, "state": info["State"]})
            if name in by_name:
                by_name[name]["running"] = info["State"]["Running"]
        write_json(self.out / "containers-retained-stopped.json", snapshot)
        if any(info["state"]["Running"] for info in snapshot):
            raise ReproError("an owned experiment container is still running after retention stop")
        self.summary["retained_nodes_stopped"] = True

    def run(self) -> int:
        self.preflight()
        try:
            self.setup()
            self.sequence()
        except (Exception, KeyboardInterrupt) as error:
            self.fail("sequence-or-setup", str(error) or "interrupted")
        finally:
            try:
                self.recover()
            except (Exception, KeyboardInterrupt) as error:
                self.fail("recovery", str(error) or "interrupted")
            try:
                self.capture()
            except (Exception, KeyboardInterrupt) as error:
                self.fail("capture", str(error) or "interrupted")
            self.summary["acknowledged_objects"] = {key: sha256(value) for key, value in self.acknowledged.items()}
            self.summary["resources_retained"] = True
            cleanup_failed = False
            if self.first_failure is None and self.args.cleanup_on_success:
                try:
                    cleanup_resources(self.commands, json.loads((self.out / "manifest.json").read_text()))
                    self.summary["resources_retained"] = False
                except (Exception, KeyboardInterrupt) as error:
                    cleanup_failed = True
                    self.fail("cleanup", str(error) or "interrupted")
                    self.summary["cleanup"] = "failed; resources may be partially removed"
            if self.summary["resources_retained"]:
                try:
                    self.stop_retained_nodes(allow_missing=cleanup_failed)
                except (Exception, KeyboardInterrupt) as error:
                    self.fail("retention-stop", str(error) or "interrupted")
                    self.summary["retained_nodes_stopped"] = False
            self.summary["first_failure"] = self.first_failure
            self.summary["result"] = "failed" if self.first_failure else "passed"
            self.summary["finished_at"] = utc_now()
            write_json(self.out / "summary.json", self.summary)
        print(json.dumps({"result": self.summary["result"], "project": self.project,
                          "resources_retained": self.summary["resources_retained"], "summary": str(self.out / "summary.json")}))
        return 2 if self.first_failure else 0


def positive_int(value: str) -> int:
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return result


def lock_sample_count(value: str) -> int:
    result = positive_int(value)
    if result > 5:
        raise argparse.ArgumentTypeError("lock snapshots are bounded to at most five samples")
    return result


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = result.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="create an isolated pool and run the fault/recovery experiment")
    run.add_argument("--image", required=True)
    run.add_argument("--expected-source", required=True)
    run.add_argument("--expected-binary-sha256")
    run.add_argument("--build-profile", choices=("dev", "debug", "release"), help="require this image build-profile label; the actual label is always recorded")
    run.add_argument("--diagnostics", action="store_true", help="enable the bounded lock diagnostic profile; default keeps warn logging")
    run.add_argument("--lock-samples", type=lock_sample_count, default=2, help="bounded pre-recovery lock snapshots after Retry-After: 1")
    run.add_argument("--out-dir", type=Path, required=True, help="new private evidence directory (must not exist)")
    run.add_argument("--gate", choices=GATE_PATHS, default="cluster")
    run.add_argument("--payload-mode", choices=("original", "distinct"), default="distinct")
    run.add_argument("--stress-iterations", type=positive_int, default=1, help="overwrite/read pairs per outage")
    run.add_argument("--workers", type=positive_int, help="set only for the two-worker comparison")
    run.add_argument("--outage-wait", type=positive_int, default=40)
    run.add_argument("--gate-timeout", type=positive_int, default=300)
    run.add_argument("--cleanup-on-success", action="store_true")
    clean = sub.add_parser("cleanup", help="remove only resources with this evidence manifest's exact ownership")
    clean.add_argument("--out-dir", type=Path, required=True)
    return result


def interrupt_run(signum: int, _frame: Any) -> None:
    # Use the existing interrupt-aware recovery, retention and summary path.
    raise KeyboardInterrupt(signal.Signals(signum).name)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    commands = Commands()
    try:
        if args.command == "cleanup":
            manifest = json.loads((args.out_dir / "manifest.json").read_text())
            cleanup_resources(commands, manifest)
            print(json.dumps({"cleaned_project": manifest["project"]}))
            return 0
        previous_sigterm = signal.signal(signal.SIGTERM, interrupt_run)
        try:
            return Experiment(args, commands).run()
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm)
    except (ReproError, OSError, ValueError, KeyError, KeyboardInterrupt) as error:
        print(commands.redact(f"reproduction failed: {str(error) or 'interrupted'}"), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
