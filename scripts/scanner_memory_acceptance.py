#!/usr/bin/env python3
"""Run owned S3 scanner-memory acceptance phases; service lifecycle stays external."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
import urllib.request
import uuid

SCHEMA = 1
OWNER_KEY = ".scanner-memory-owner.json"
LABEL = "rustfs.scanner-memory.run-id"
GIB = 1024 ** 3
SAFE_ENV = frozenset({
    "RUSTFS_SCANNER_ENABLED", "RUSTFS_HEAL_ENABLED", "RUSTFS_SCANNER_SPEED",
    "RUSTFS_SCANNER_START_DELAY_SECS", "RUSTFS_SCANNER_CYCLE", "RUSTFS_SCANNER_IDLE_MODE",
    "RUSTFS_SCANNER_CYCLE_MAX_OBJECTS", "RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS",
    "RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES", "RUSTFS_SCANNER_DISK_IO_BUDGET_IOPS",
    "RUSTFS_SCANNER_DISK_IO_BUDGET_BYTES_PER_SEC",
    "RUSTFS_CONSOLE_ENABLE", "RUSTFS_ERASURE_SET_DRIVE_COUNT", "RUSTFS_DURABILITY_MODE",
    "RUSTFS_MEMORY_OBSERVABILITY_INTERVAL_SECS", "RUSTFS_OBS_METRICS_EXPORT_ENABLED",
})
STATUS_FIELDS = (
    "current_cycle", "current_cycle_active", "current_cycle_objects_scanned",
    "current_cycle_directories_scanned", "last_cycle_result", "last_cycle_partial_reason",
    "last_cycle_end_unix_secs", "last_cycle_objects_scanned", "failed_cycles",
    "current_disk_bucket_scans_active", "last_cycle_bucket_drive_failures", "usage_publication",
)


class AcceptanceError(ValueError):
    """A public-safe diagnostic created by this runner, never by an SDK."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise AcceptanceError("redirected observation endpoints are not supported")


def require(condition, message):
    if not condition:
        raise AcceptanceError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def local_url(value):
    parsed = urlsplit(value)
    require(parsed.scheme in ("http", "https") and parsed.hostname in ("127.0.0.1", "::1")
            and parsed.port is not None and not parsed.username and not parsed.password
            and not parsed.query and not parsed.fragment and parsed.path in ("", "/"),
            "endpoint must use literal 127.0.0.1 or ::1 with an explicit HTTP(S) port")
    return value.rstrip("/")


@contextmanager
def open_owned(path, mode="r"):
    require(mode in ("r", "a"), "unsupported evidence file mode")
    flags = os.O_RDONLY if mode == "r" else os.O_WRONLY | os.O_CREAT | os.O_APPEND
    try:
        fd = os.open(path, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    except OSError:
        raise AcceptanceError("evidence file is not an owned regular file") from None
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
                and info.st_mode & 0o077 == 0, "evidence file must be private, owned and single-link")
        if mode == "a":
            os.fchmod(fd, 0o600)
        stream = os.fdopen(fd, mode, encoding="utf-8")
        fd = None
        with stream:
            yield stream
    finally:
        if fd is not None:
            os.close(fd)


def read_json(path, limit=1024 * 1024):
    with open_owned(path) as stream:
        text = stream.read(limit + 1)
    require(len(text) <= limit, "oversized evidence file")
    return json.loads(text)


def atomic_json(path, value):
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def object_key(plan, index):
    require(type(index) is int and 0 <= index < plan["objects"], "object index outside manifest")
    prefix = index % plan["root_prefixes"]
    ordinal = index // plan["root_prefixes"]
    suffix = f"source:{prefix:06d}/{ordinal:08d}.webp"
    if plan["layout"] == "wide":
        return suffix
    return f"photos/{prefix % 256:02x}/{(prefix // 256) % 256:02x}/" + suffix


def key_index(plan, key):
    pattern = r"source:([0-9]{6,})/([0-9]{8,})\.webp"
    if plan["layout"] == "nested":
        pattern = r"photos/[0-9a-f]{2}/[0-9a-f]{2}/" + pattern
    match = re.fullmatch(pattern, key)
    require(match is not None, "inventory contains a foreign object key")
    prefix, ordinal = map(int, match.groups())
    index = ordinal * plan["root_prefixes"] + prefix
    require(prefix < plan["root_prefixes"] and index < plan["objects"] and object_key(plan, index) == key,
            "inventory key does not match the immutable layout")
    return index


def payload(plan):
    seed = uuid.UUID(plan["run_id"]).bytes
    blocks = (hashlib.sha256(seed + index.to_bytes(8, "big")).digest()
              for index in range((plan["object_bytes"] + 31) // 32))
    return b"".join(blocks)[:plan["object_bytes"]]


def owner_body(manifest):
    return canonical({"schema": SCHEMA, "run_id": manifest["plan"]["run_id"], "plan_sha256": manifest["plan_sha256"]})


def validate_manifest(manifest):
    require(type(manifest) is dict and set(manifest) == {"schema", "plan", "plan_sha256"}
            and manifest["schema"] == SCHEMA, "invalid manifest envelope")
    plan = manifest["plan"]
    require(type(plan) is dict and set(plan) == {
        "run_id", "created_at", "endpoint", "source_sha", "binary_sha256", "layout", "objects",
        "object_bytes", "root_prefixes", "memory_bytes", "data_root", "data_volume", "bucket",
    }, "invalid immutable plan fields")
    require(digest(plan) == manifest["plan_sha256"], "manifest digest mismatch")
    require(str(uuid.UUID(plan["run_id"])) == plan["run_id"], "invalid run identity")
    require(plan["bucket"] == "scanner-memory-" + uuid.UUID(plan["run_id"]).hex, "bucket is not run-owned")
    local_url(plan["endpoint"])
    require(re.fullmatch(r"[0-9a-f]{40}", plan["source_sha"]) and re.fullmatch(r"[0-9a-f]{64}", plan["binary_sha256"]),
            "invalid source or binary identity")
    require(plan["layout"] in ("wide", "nested"), "invalid layout")
    for name, maximum in (("objects", 20_000_000), ("object_bytes", 1024 * 1024),
                          ("root_prefixes", 2_000_000), ("memory_bytes", 64 * GIB)):
        require(type(plan[name]) is int and 0 < plan[name] <= maximum, "invalid workload bound")
    require(plan["root_prefixes"] <= plan["objects"], "root prefix count exceeds object count")
    require(bool(plan["data_root"]) != bool(plan["data_volume"]), "select one immutable storage source")
    if plan["data_volume"]:
        require(isinstance(plan["data_volume"], str) and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", plan["data_volume"]),
                "invalid owned data volume name")
    else:
        path = Path(plan["data_root"])
        require(path.is_absolute() and path == path.resolve() and path.is_dir(), "data root must be an existing canonical directory")
    return plan


class Evidence:
    def __init__(self, root):
        self.root = root.resolve()
        require(root == self.root and root.is_dir() and not root.is_symlink(), "run directory must be canonical")
        require(root.stat().st_uid == os.getuid() and root.stat().st_mode & 0o077 == 0, "run directory must be private and owned")
        self.manifest = read_json(root / "manifest.json")
        self.plan = validate_manifest(self.manifest)
        self.lock = threading.Lock()

    @contextmanager
    def exclusive(self):
        fd = os.open(self.root / ".lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
                    and info.st_mode & 0o077 == 0, "invalid evidence lock ownership")
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield

    def append(self, kind, **fields):
        row = {"schema": SCHEMA, "at": utc(), "monotonic_ns": time.monotonic_ns(), "kind": kind,
               "run_id": self.plan["run_id"], **fields}
        with self.lock, open_owned(self.root / "observations.jsonl", "a") as stream:
            stream.write(json.dumps(row, sort_keys=True) + "\n")
            stream.flush()
        return row

    def state(self):
        path = self.root / "state.json"
        state = read_json(path) if path.exists() or path.is_symlink() else {"plan_sha256": self.manifest["plan_sha256"], "next_object": 0}
        require(state.get("plan_sha256") == self.manifest["plan_sha256"], "resume state belongs to another plan")
        require(type(state.get("next_object")) is int and 0 <= state["next_object"] <= self.plan["objects"], "invalid resume marker")
        return state

    def save(self, state):
        atomic_json(self.root / "state.json", state)


class S3:
    def __init__(self, evidence, workers):
        import boto3
        from botocore.config import Config
        self.evidence = evidence
        self.plan = evidence.plan
        self.session = boto3.Session()
        require(os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"),
                "set disposable AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment")
        self.client = self.session.client("s3", endpoint_url=self.plan["endpoint"], region_name="us-east-1", config=Config(
            signature_version="s3v4", s3={"addressing_style": "path"}, proxies={}, connect_timeout=5, read_timeout=30,
            retries={"total_max_attempts": 2}, max_pool_connections=workers))

    def admin(self, path):
        from botocore.auth import S3SigV4Auth
        from botocore.awsrequest import AWSRequest
        request = AWSRequest(method="GET", url=self.plan["endpoint"] + path)
        S3SigV4Auth(self.session.get_credentials().get_frozen_credentials(), "s3", "us-east-1").add_auth(request)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        with opener.open(urllib.request.Request(request.url, headers=dict(request.headers)), timeout=15) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
        require(len(raw) <= 4 * 1024 * 1024, "admin response exceeds bound")
        return json.loads(raw)

    def status(self, enabled):
        status = self.admin("/rustfs/admin/v3/scanner/status")
        require(status.get("enabled") is enabled, "scanner enabled state differs from required phase")
        metrics = status.get("metrics")
        require(type(metrics) is dict, "scanner metrics are unavailable")
        return {"enabled": enabled, "metrics": {key: metrics.get(key) for key in STATUS_FIELDS}}

    def owned(self, create=False):
        from botocore.exceptions import ClientError
        bucket = self.plan["bucket"]
        try:
            self.client.head_bucket(Bucket=bucket)
        except ClientError as error:
            require(error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404 and create,
                    "owned bucket is unavailable")
            self.client.create_bucket(Bucket=bucket)
            self.client.put_object(Bucket=bucket, Key=OWNER_KEY, Body=owner_body(self.evidence.manifest),
                                   ContentType="application/json")
        response = self.client.get_object(Bucket=bucket, Key=OWNER_KEY)
        with response["Body"] as body:
            marker = body.read(4097)
        require(marker == owner_body(self.evidence.manifest), "bucket ownership marker mismatch")
        require(self.client.get_bucket_versioning(Bucket=bucket).get("Status") is None,
                "acceptance bucket must remain unversioned")

    def verify(self, samples=16, deadline=None, guard=None):
        deadline = deadline or time.monotonic() + 3600
        def check():
            require(time.monotonic() <= deadline, "inventory verification time budget exhausted")
            if guard:
                guard()
        check()
        self.owned()
        plan = self.plan
        seen = bytearray((plan["objects"] + 7) // 8)
        count = size = markers = 0
        token = None
        while True:
            check()
            args = {"Bucket": plan["bucket"], "MaxKeys": 1000}
            if token:
                args["ContinuationToken"] = token
            page = self.client.list_objects_v2(**args)
            for entry in page.get("Contents", []):
                if entry["Key"] == OWNER_KEY:
                    markers += 1
                    require(entry["Size"] == len(owner_body(self.evidence.manifest)), "ownership object size changed")
                    continue
                index = key_index(plan, entry["Key"])
                mask = 1 << (index % 8)
                require(not seen[index // 8] & mask, "listing contains a duplicate object")
                seen[index // 8] |= mask
                require(entry["Size"] == plan["object_bytes"], "object size mismatch")
                count += 1
                size += entry["Size"]
            if not page.get("IsTruncated"):
                break
            next_token = page.get("NextContinuationToken")
            require(isinstance(next_token, str) and next_token and next_token != token, "listing cursor did not advance")
            token = next_token
        require(count == plan["objects"] and markers == 1 and size == count * plan["object_bytes"],
                "exact object/byte inventory does not match manifest")
        expected = payload(plan)
        indices = sorted({index * (plan["objects"] - 1) // max(1, samples - 1) for index in range(samples)})
        for index in indices:
            check()
            response = self.client.get_object(Bucket=plan["bucket"], Key=object_key(plan, index))
            with response["Body"] as body:
                actual = body.read(plan["object_bytes"] + 1)
            require(actual == expected, "sampled payload differs from fixture")
        result = {"objects": count, "logical_bytes": size, "ownership_objects": markers,
                  "scanner_expected_objects": count + 1,
                  "scanner_expected_bytes": size + len(owner_body(self.evidence.manifest)), "payload_samples": len(indices)}
        check()
        self.evidence.append("inventory", **result)
        return result

    def usage(self, expected):
        account = self.admin("/rustfs/admin/v3/accountinfo")
        matches = [bucket for bucket in account.get("buckets", []) if bucket.get("name") == self.plan["bucket"]]
        require(len(matches) == 1, "bucket usage is unavailable")
        bucket = matches[0]
        return {"objects": bucket.get("objects"), "size": bucket.get("size"),
                "converged": account.get("usage_snapshot_converged"),
                "matches_inventory": bucket.get("objects") == expected["scanner_expected_objects"]
                and bucket.get("size") == expected["scanner_expected_bytes"]}


def command(argv, timeout=30):
    result = subprocess.run(argv, capture_output=True, timeout=timeout, check=False)
    require(result.returncode == 0 and len(result.stdout) <= 1024 * 1024, "runtime observation command failed")
    return result.stdout


def key_values(raw, colon=False):
    result = {}
    for line in raw.splitlines():
        pieces = line.split(":" if colon else None, 1)
        if len(pieces) != 2:
            continue
        value = pieces[1].strip().split()
        if value and value[0].isdigit():
            result[pieces[0]] = int(value[0]) * (1024 if len(value) > 1 and value[1] == "kB" else 1)
    return result


def parse_proc_stat(raw):
    end = raw.rfind(")")
    require(end > 0, "invalid process identity")
    fields = raw[end + 1:].split()
    require(len(fields) >= 20 and fields[19].isdigit(), "invalid process start time")
    return int(fields[19])


def address_matches(bound, endpoint):
    try:
        actual, expected = ipaddress.ip_address(bound), ipaddress.ip_address(urlsplit(endpoint).hostname)
    except (ValueError, TypeError):
        return False
    return actual.version == expected.version and (actual == expected or actual.is_unspecified)


def docker_endpoint_matches(bindings, endpoint):
    port = str(urlsplit(endpoint).port)
    return any(protocol.endswith("/tcp") and binding.get("HostPort") == port
               and address_matches(binding.get("HostIp"), endpoint)
               for protocol, entries in bindings.items() for binding in (entries or []))


def native_endpoint_matches(tables, owned_sockets, endpoint):
    port = urlsplit(endpoint).port
    found = False
    for protocol, table in tables.items():
        for row in table.splitlines()[1:]:
            fields = row.split()
            if len(fields) <= 9 or fields[3] != "0A":
                continue
            address, encoded_port = fields[1].rsplit(":", 1)
            if int(encoded_port, 16) != port:
                continue
            width = 8 if protocol == "tcp" else 32
            require(len(address) == width, "invalid native listener address")
            packed = b"".join(int(address[index:index + 8], 16).to_bytes(4, sys.byteorder)
                              for index in range(0, width, 8))
            bound = ipaddress.ip_address(packed)
            expected = ipaddress.ip_address(urlsplit(endpoint).hostname)
            ambiguous_v4 = bound.version == 6 and expected.version == 4 and (bound.is_unspecified or bound.ipv4_mapped == expected)
            if ambiguous_v4 and fields[9] not in owned_sockets:
                return False
            if not address_matches(str(bound), endpoint):
                continue
            if fields[9] not in owned_sockets:
                return False
            found = True
    return found


class Runtime:
    def __init__(self, evidence, container=None, pid=None, metrics_url=None):
        require(bool(container) != bool(pid), "select exactly one owned container or native PID")
        self.evidence, self.plan = evidence, evidence.plan
        self.container, self.pid = container, pid or 1
        require(container is None or re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", container), "invalid container identifier")
        require(type(self.pid) is int and self.pid > 0, "invalid process PID")
        self.metrics_url = metrics_url
        if metrics_url:
            parsed = urlsplit(metrics_url)
            local_url(f"{parsed.scheme}://{parsed.netloc}")
            require(not parsed.query and not parsed.fragment, "metrics URL must not contain query or fragment")
        self.verified = set()

    def read_files(self):
        names = ("stat", "status", "smaps_rollup", "cgroup", "environ")
        if self.container:
            raw = json.loads(command(["docker", "inspect", self.container]))
            require(len(raw) == 1, "ambiguous container identity")
            item = raw[0]
            require((item["Config"].get("Labels") or {}).get(LABEL) == self.plan["run_id"], "container is not owned by this run")
            require(item["State"]["Running"] and not item["State"]["OOMKilled"], "owned container stopped or was OOM-killed")
            require(item["HostConfig"]["Memory"] == self.plan["memory_bytes"], "Docker memory limit differs from manifest")
            bindings = item.get("NetworkSettings", {}).get("Ports", {})
            require(docker_endpoint_matches(bindings, self.plan["endpoint"]),
                    "S3 TCP endpoint address is not published by the observed container")
            if self.plan["data_volume"]:
                mounts = [mount for mount in item.get("Mounts", [])
                          if mount.get("Type") == "volume" and mount.get("Name") == self.plan["data_volume"]]
                volume = json.loads(command(["docker", "volume", "inspect", self.plan["data_volume"]]))
                require(len(volume) == 1 and (volume[0].get("Labels") or {}).get(LABEL) == self.plan["run_id"],
                        "data volume is not owned by this run")
            else:
                mounts = [mount for mount in item.get("Mounts", [])
                          if mount.get("Type") == "bind" and Path(mount["Source"]).resolve() == Path(self.plan["data_root"])]
            require(len(mounts) == 1 and mounts[0].get("RW") is True, "container storage does not match the immutable manifest")
            storage_path = mounts[0]["Destination"]
            identity = {"container_id": item["Id"], "started_at": item["State"]["StartedAt"],
                        "restart_count": item["RestartCount"], "pid": self.pid}
            script = 'for n in stat status smaps_rollup cgroup environ; do printf "\\n__FILE_%s__\\n" "$n"; cat "/proc/$1/$n" || exit 1; done; '
            script += 'printf "\\n__FILE_exe_inode__\\n"; stat -Lc "%d:%i" "/proc/$1/exe"; '
            script += 'for n in memory.current memory.max memory.peak memory.stat memory.events memory.swap.current memory.swap.max; do printf "\\n__FILE_%s__\\n" "$n"; cat "/sys/fs/cgroup/$n" 2>/dev/null || true; done'
            script += '; printf "\\n__FILE_storage__\\n"; df -Pk "$2" | tail -n 1'
            data = command(["docker", "exec", item["Id"], "sh", "-c", script, "sh", str(self.pid), storage_path])
            chunks = re.split(rb"\n__FILE_([A-Za-z_.]+)__\n", data)
            files = {chunks[index].decode(): chunks[index + 1].strip(b"\n") for index in range(1, len(chunks), 2)}
        else:
            require(self.plan["data_volume"] is None, "native PID sampling requires a host data root")
            root = Path("/proc") / str(self.pid)
            require(root.stat().st_uid == os.getuid(), "native process is not owned by this user")
            sockets = set()
            for descriptor in (root / "fd").iterdir():
                try:
                    target = os.readlink(descriptor)
                except FileNotFoundError:
                    continue
                match = re.fullmatch(r"socket:\[([0-9]+)\]", target)
                if match:
                    sockets.add(match[1])
            tables = {protocol: (root / "net" / protocol).read_text() for protocol in ("tcp", "tcp6")
                      if (root / "net" / protocol).exists()}
            require(native_endpoint_matches(tables, sockets, self.plan["endpoint"]),
                    "S3 TCP endpoint is not exclusively bound by the observed PID")
            files = {name: (root / name).read_bytes() for name in names}
            groups = files["cgroup"].decode().splitlines()
            require(len(groups) == 1 and groups[0].startswith("0::/") and groups[0] != "0::/", "dedicated cgroup v2 is required")
            relative = groups[0][3:].lstrip("/")
            require(".." not in Path(relative).parts, "invalid cgroup path")
            group = Path("/sys/fs/cgroup") / relative
            require(set((group / "cgroup.procs").read_text().split()) == {str(self.pid)}, "native cgroup contains other processes")
            for name in ("memory.current", "memory.max", "memory.peak", "memory.stat", "memory.events", "memory.swap.current", "memory.swap.max"):
                files[name] = (group / name).read_bytes() if (group / name).exists() else b""
            info = (root / "exe").stat()
            files["exe_inode"] = f"{info.st_dev}:{info.st_ino}".encode()
            files["storage"] = f"native 0 0 {shutil.disk_usage(self.plan['data_root']).free // 1024} 0 /".encode()
            identity = {"boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(), "pid": self.pid}
        identity.update(start_ticks=parse_proc_stat(files["stat"].decode()), executable_inode=files["exe_inode"].decode())
        key = digest(identity)
        if key not in self.verified:
            if self.container:
                observed = command(["docker", "exec", identity["container_id"], "sha256sum", f"/proc/{self.pid}/exe"], timeout=60).decode().split()[0]
            else:
                with (Path("/proc") / str(self.pid) / "exe").open("rb") as stream:
                    observed = hashlib.file_digest(stream, "sha256").hexdigest()
            require(observed == self.plan["binary_sha256"], "running binary does not match manifest")
            self.verified.add(key)
        environment = {}
        for pair in files["environ"].split(b"\0"):
            name, _, value = pair.partition(b"=")
            if name.decode(errors="replace") in SAFE_ENV:
                environment[name.decode()] = value.decode()
        return identity, files, environment

    def sample(self):
        identity, files, environment = self.read_files()
        def number(name):
            value = files.get(name, b"").strip()
            return int(value) if value.isdigit() else None
        require(number("memory.max") == self.plan["memory_bytes"], "kernel memory limit differs from manifest")
        status = key_values(files["status"].decode(), colon=True)
        smaps = key_values(files["smaps_rollup"].decode(), colon=True)
        events = key_values(files["memory.events"].decode())
        require("oom" in events and "oom_kill" in events, "cgroup OOM counters are unavailable")
        require(all(events.get(name, 0) == 0 for name in ("oom", "oom_kill", "oom_group_kill")), "cgroup reported an OOM event")
        require(status.get("VmRSS") is not None and number("memory.current") is not None, "required memory readings are missing")
        storage = files["storage"].split()
        require(len(storage) >= 6 and storage[3].isdigit(), "storage free-space observation is unavailable")
        result = {"storage_available_bytes": int(storage[3]) * 1024, "identity": identity, "identity_sha256": digest(identity), "configuration": environment,
                  "rss_bytes": status["VmRSS"], "rss_anon_bytes": status.get("RssAnon"),
                  "smaps_anonymous_bytes": smaps.get("Anonymous"), "cgroup_current_bytes": number("memory.current"),
                  "cgroup_peak_bytes": number("memory.peak"), "cgroup_limit_bytes": number("memory.max"),
                  "cgroup_swap_bytes": number("memory.swap.current"), "cgroup_swap_limit_bytes": number("memory.swap.max"),
                  "cgroup_stat": key_values(files["memory.stat"].decode()), "cgroup_events": events,
                  "active_heap_bytes": None, "active_heap_status": "unknown: allocator metrics not supplied",
                  "allocator_endpoint_attribution": "operator-supplied; process binding unverified" if self.metrics_url else None}
        if self.metrics_url:
            try:
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
                with opener.open(self.metrics_url, timeout=5) as response:
                    text = response.read(4 * 1024 * 1024 + 1).decode()
                require(len(text) <= 4 * 1024 * 1024, "oversized metrics response")
                values = {}
                for name in ("rustfs_memory_allocator_malloc_requested_bytes_available", "rustfs_memory_allocator_malloc_requested_bytes"):
                    matches = re.findall(r"^" + name + r"(?:\{[^\n]*\})?\s+([0-9.eE+-]+)\s*$", text, re.MULTILINE)
                    if len(matches) == 1:
                        values[name] = float(matches[0])
                active = values.get("rustfs_memory_allocator_malloc_requested_bytes")
                if values.get("rustfs_memory_allocator_malloc_requested_bytes_available") == 1 and active is not None and math.isfinite(active) and active >= 0:
                    result.update(active_heap_bytes=int(active), active_heap_status="observed: allocator requested current bytes")
                else:
                    result["active_heap_status"] = "unknown: live allocator statistic unavailable"
            except Exception:
                result["active_heap_status"] = "unknown: allocator metrics request failed"
        return result


class Phase:
    """One process identity and deadline cover work, verification and the last sample."""

    def __init__(self, evidence, runtime, api, label, enabled, args):
        self.evidence, self.runtime, self.api = evidence, runtime, api
        self.label, self.enabled, self.interval = label, enabled, args.interval
        self.phase_id = str(uuid.uuid4())
        self.started = time.monotonic()
        self.deadline = self.started + args.seconds
        self.stop = threading.Event()
        self.sample_lock = threading.Lock()
        self.error = None
        self.identity = None
        self.latest = None
        self.initial = None
        self.stage = "scanner" if enabled else "workload"

    def check(self):
        require(self.error is None, self.error or "observation failed")
        require(time.monotonic() <= self.deadline, "phase time budget exhausted; retain evidence and resume explicitly")

    def once(self):
        with self.sample_lock:
            self.check()
            memory = self.runtime.sample()
            if self.identity is None:
                self.identity = memory["identity_sha256"]
            require(memory["identity_sha256"] == self.identity, "process changed during a phase; start a new phase after restart")
            status = self.api.status(self.enabled)
            self.check()
            row = self.evidence.append("sample", phase=self.label, phase_id=self.phase_id, stage=self.stage, memory=memory, scanner=status)
            self.latest = row
            return row

    def __enter__(self):
        self.evidence.append("phase_start", phase=self.label, phase_id=self.phase_id,
                             time_budget_seconds=self.deadline - self.started)
        self.initial = self.once()
        def sample_loop():
            while not self.stop.wait(self.interval):
                try:
                    self.once()
                except Exception:
                    self.error = "memory/status observation failed; phase is inconclusive"
                    self.evidence.append("observation_error", phase=self.label, phase_id=self.phase_id)
                    self.stop.set()
        self.thread = threading.Thread(target=sample_loop, daemon=True)
        self.thread.start()
        return self

    def enter_stage(self, stage):
        with self.sample_lock:
            self.stage = stage
        self.once()

    def verify(self):
        self.enter_stage("verification")
        return self.api.verify(deadline=self.deadline, guard=self.check)

    def result(self, result):
        return {**result, "phase_id": self.phase_id, "process_identity": self.identity,
                "elapsed_seconds": time.monotonic() - self.started}

    def __exit__(self, exception_type, *_):
        self.stop.set()
        self.thread.join(timeout=60)
        require(not self.thread.is_alive(), "sampler did not stop")
        if exception_type is None:
            self.check()
            self.once()


def free_space(plan, remaining, available=None):
    required = GIB + math.ceil(remaining * (plan["object_bytes"] + 16384) * 1.2)
    if available is None:
        require(plan["data_root"] is not None, "named-volume capacity must be observed inside the owned container")
        available = shutil.disk_usage(plan["data_root"]).free
    require(type(available) is int and available >= required, "insufficient free space for remaining objects and metadata reserve")


def run_load(evidence, api, runtime, args):
    state = evidence.state()
    data = payload(evidence.plan)
    with Phase(evidence, runtime, api, "load", False, args) as phase, ThreadPoolExecutor(args.workers) as pool:
        free_space(evidence.plan, evidence.plan["objects"] - state["next_object"], phase.latest["memory"]["storage_available_bytes"])
        api.owned(create=state["next_object"] == 0)
        while state["next_object"] < evidence.plan["objects"]:
            phase.check()
            start = state["next_object"]
            end = min(start + args.batch, evidence.plan["objects"])
            free_space(evidence.plan, evidence.plan["objects"] - start, phase.latest["memory"]["storage_available_bytes"])
            api.owned()
            def put(index):
                phase.check()
                api.client.put_object(Bucket=evidence.plan["bucket"], Key=object_key(evidence.plan, index), Body=data)
            list(pool.map(put, range(start, end)))
            state["next_object"] = end
            evidence.save(state)
            evidence.append("load_batch", phase_id=phase.phase_id, completed=end, expected=evidence.plan["objects"])
            phase.check()
        inventory = phase.verify()
    state["load_complete"] = True
    evidence.save(state)
    return phase.result({"phase": "load", "complete": True, "inventory": inventory})


def run_probe(evidence, api, runtime, args):
    require(evidence.state().get("load_complete") is True, "finish and verify load before probing")
    completed = set()
    with Phase(evidence, runtime, api, args.label, True, args) as phase:
        api.owned()
        first_end = phase.initial["scanner"]["metrics"].get("last_cycle_end_unix_secs") or 0
        while True:
            phase.check()
            metrics = phase.latest["scanner"]["metrics"]
            ended = metrics.get("last_cycle_end_unix_secs")
            if type(ended) is int and ended > first_end and metrics.get("last_cycle_result") == "success":
                completed.add(ended)
            if args.stop_after_progress is not None:
                progress = metrics.get("current_cycle_objects_scanned")
                if metrics.get("current_cycle_active") is True and type(progress) is int and progress >= args.stop_after_progress:
                    result = {"phase": args.label, "active_before_external_restart": True,
                              "completed_cycles": len(completed), "complete": False}
                    break
            elif len(completed) >= args.min_completed_cycles:
                inventory = phase.verify()
                usage = api.usage(inventory)
                phase.check()
                require(usage["matches_inventory"] and usage["converged"] is True,
                        "scanner usage is not converged to the exact S3 inventory")
                result = {"phase": args.label, "completed_cycles": len(completed), "inventory": inventory,
                          "usage": usage, "complete": True}
                break
            require(time.monotonic() < phase.deadline, "required scanner progress was not observed within the phase budget")
            phase.stop.wait(min(args.interval, max(0, phase.deadline - time.monotonic())))
    return phase.result(result)


def run_burst(evidence, api, runtime, args):
    require(evidence.state().get("load_complete") is True, "finish load before scanner-off burst")
    state = evidence.state()
    count = args.burst_objects or evidence.plan["objects"]
    require(0 < count <= evidence.plan["objects"], "burst count exceeds owned fixture")
    burst = state.setdefault("burst", {"id": str(uuid.uuid4()), "next": 0, "objects": count})
    require(burst["objects"] == count and str(uuid.UUID(burst["id"])) == burst["id"]
            and type(burst["next"]) is int and 0 <= burst["next"] <= count, "burst resume parameters changed")
    initial_cursor = burst["next"]
    evidence.save(state)
    with Phase(evidence, runtime, api, "burst", False, args) as phase, ThreadPoolExecutor(args.workers) as pool:
        api.owned()
        while burst["next"] < count:
            phase.check()
            api.owned()
            end = min(burst["next"] + args.batch, count)
            free_space(evidence.plan, args.workers, phase.latest["memory"]["storage_available_bytes"])
            def operation(index):
                phase.check()
                key = f".scanner-memory-burst/{burst['id']}/{index:08d}"
                api.client.copy_object(Bucket=evidence.plan["bucket"], Key=key,
                                       CopySource={"Bucket": evidence.plan["bucket"], "Key": object_key(evidence.plan, index)})
                phase.check()
                head = api.client.head_object(Bucket=evidence.plan["bucket"], Key=key)
                require(head["ContentLength"] == evidence.plan["object_bytes"], "copied object size mismatch")
                phase.check()
                api.client.delete_object(Bucket=evidence.plan["bucket"], Key=key)
            list(pool.map(operation, range(burst["next"], end)))
            burst["next"] = end
            evidence.save(state)
            evidence.append("burst_batch", phase_id=phase.phase_id, completed=end, expected=count)
            phase.check()
        inventory = phase.verify()
        phase.enter_stage("idle")
        idle_deadline = time.monotonic() + args.idle_seconds
        require(idle_deadline <= phase.deadline, "insufficient phase budget for the requested idle observation")
        evidence.append("idle_start", phase="burst", phase_id=phase.phase_id, idle_seconds=args.idle_seconds)
        while time.monotonic() < idle_deadline:
            phase.check()
            phase.stop.wait(min(args.interval, max(0, idle_deadline - time.monotonic())))
    return phase.result({"phase": "burst", "complete": True, "objects": count, "burst_id": burst["id"],
                         "cursor_start": initial_cursor, "cursor_end": burst["next"],
                         "newly_completed_objects": burst["next"] - initial_cursor,
                         "new_logical_copy_head_delete_requests": (burst["next"] - initial_cursor) * 3,
                         "cumulative_completed_objects": burst["next"],
                         "cumulative_logical_copy_head_delete_requests": burst["next"] * 3,
                         "observed_http_request_count": None, "idle_seconds": args.idle_seconds, "inventory": inventory,
                         "reclamation_verdict": "measurement only; RSS retention alone does not prove a leak"})


def summarize(evidence):
    samples = {}
    results = []
    errors = 0
    path = evidence.root / "observations.jsonl"
    if path.exists() or path.is_symlink():
        with open_owned(path) as stream:
            for line in stream:
                require(len(line) <= 1024 * 1024, "oversized evidence row")
                row = json.loads(line)
                require(row.get("run_id") == evidence.plan["run_id"], "foreign run evidence")
                if row["kind"] == "sample":
                    memory = row["memory"]
                    key = row["phase_id"] + ":" + memory["identity_sha256"] + ":" + row["stage"]
                    group = samples.setdefault(key, {"phase": row["phase"], "phase_id": row["phase_id"],
                                                     "process_identity": memory["identity_sha256"], "stage": row["stage"], "samples": 0,
                                                     "rss_peak": 0, "sampled_current_peak_bytes": 0,
                                                     "kernel_reported_peak_bytes": None, "first_rss": memory["rss_bytes"],
                                                     "active_heap_samples": 0})
                    group["samples"] += 1
                    group["rss_peak"] = max(group["rss_peak"], memory["rss_bytes"])
                    group["sampled_current_peak_bytes"] = max(group["sampled_current_peak_bytes"], memory["cgroup_current_bytes"])
                    if memory.get("cgroup_peak_bytes") is not None:
                        group["kernel_reported_peak_bytes"] = max(group["kernel_reported_peak_bytes"] or 0, memory["cgroup_peak_bytes"])
                    group["last_rss"] = memory["rss_bytes"]
                    group["active_heap_samples"] += memory["active_heap_bytes"] is not None
                elif row["kind"] == "phase_result":
                    results.append(row["result"])
                elif row["kind"] in ("phase_error", "observation_error"):
                    errors += 1
    interrupted = set()
    restart_observed = False
    for result in results:
        identity = result.get("process_identity")
        if result.get("active_before_external_restart"):
            interrupted.add(identity)
        elif interrupted and identity and identity not in interrupted and result.get("complete") and result.get("completed_cycles", 0) > 0:
            restart_observed = True
    summary = {"schema": SCHEMA, "plan_sha256": evidence.manifest["plan_sha256"], "plan": evidence.plan,
               "phases": samples, "results": results, "observation_errors": errors,
               "restart_window_observed": restart_observed,
               "full_scale": evidence.plan["objects"] == 2_000_000 and evidence.plan["object_bytes"] >= 50 * 1024,
               "allocator_leak_verdict": "unknown; correlate live heap, anonymous RSS and cgroup file/slab across idle windows",
               "overall_acceptance_passed": False}
    atomic_json(evidence.root / "summary.json", summary)
    return summary


def prepare(args):
    require(not args.run_dir.exists(), "prepare requires a new run directory")
    identifier = str(uuid.uuid4())
    plan = {"run_id": identifier, "created_at": utc(), "endpoint": local_url(args.endpoint),
            "source_sha": args.source_sha, "binary_sha256": args.binary_sha256, "layout": args.layout,
            "objects": args.objects, "object_bytes": args.object_bytes, "root_prefixes": args.root_prefixes,
            "memory_bytes": args.memory_bytes, "data_root": str(args.data_root.resolve()) if args.data_root else None,
            "data_volume": args.data_volume,
            "bucket": "scanner-memory-" + uuid.UUID(identifier).hex}
    manifest = {"schema": SCHEMA, "plan": plan, "plan_sha256": digest(plan)}
    validate_manifest(manifest)
    if plan["data_root"]:
        free_space(plan, plan["objects"])
    args.run_dir.mkdir(mode=0o700)
    atomic_json(args.run_dir / "manifest.json", manifest)
    return {"run_id": identifier, "bucket": plan["bucket"], "container_label": LABEL + "=" + identifier,
            "manifest": str(args.run_dir / "manifest.json")}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "load", "verify", "probe", "burst", "summary"))
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--source-sha")
    parser.add_argument("--binary-sha256")
    parser.add_argument("--layout", choices=("wide", "nested"), default="wide")
    parser.add_argument("--objects", type=int, default=2_000_000)
    parser.add_argument("--object-bytes", type=int, default=50 * 1024)
    parser.add_argument("--root-prefixes", type=int, default=170_000)
    parser.add_argument("--memory-bytes", type=int, default=4 * GIB)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--data-volume")
    parser.add_argument("--container")
    parser.add_argument("--pid", type=int)
    parser.add_argument("--allocator-metrics-url")
    parser.add_argument("--workers", type=int, default=48)
    parser.add_argument("--batch", type=int, default=256)
    parser.add_argument("--seconds", type=float, default=3600)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--label", default="scanner_probe")
    parser.add_argument("--min-completed-cycles", type=int, default=2)
    parser.add_argument("--stop-after-progress", type=int)
    parser.add_argument("--burst-objects", type=int)
    parser.add_argument("--idle-seconds", type=float, default=1800)
    args = parser.parse_args(argv)
    evidence = None
    try:
        args.run_dir = args.run_dir.absolute()
        require(1 <= args.workers <= 256 and 1 <= args.batch <= 4096, "invalid client concurrency or batch bound")
        require(math.isfinite(args.seconds) and 0 < args.seconds <= 7 * 86400
                and math.isfinite(args.interval) and 0.1 <= args.interval <= 300
                and math.isfinite(args.idle_seconds) and 0 < args.idle_seconds <= 86400, "invalid time bounds")
        require(re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", args.label), "invalid phase label")
        require(args.min_completed_cycles >= 0 and (args.stop_after_progress is None or args.stop_after_progress > 0),
                "invalid cycle/progress bounds")
        if args.action == "prepare":
            require(args.endpoint and args.source_sha and args.binary_sha256 and (args.data_root or args.data_volume),
                    "prepare requires endpoint, source, binary and storage identity")
            result = prepare(args)
        else:
            evidence = Evidence(args.run_dir)
            with evidence.exclusive():
                if args.action == "summary":
                    result = summarize(evidence)
                else:
                    api = S3(evidence, args.workers)
                    if args.action == "verify":
                        result = api.verify(deadline=time.monotonic() + args.seconds)
                    else:
                        runtime = Runtime(evidence, args.container, args.pid, args.allocator_metrics_url)
                        result = {"load": run_load, "probe": run_probe, "burst": run_burst}[args.action](evidence, api, runtime, args)
                    evidence.append("phase_result", result=result)
                    summarize(evidence)
        print(json.dumps(result, sort_keys=True))
        return 0
    except AcceptanceError as error:
        message = str(error)
    except Exception:
        message = "acceptance phase failed; raw credentials and server errors were withheld"
    if evidence:
        try:
            evidence.append("phase_error", phase=args.action, reason=message)
            summarize(evidence)
        except Exception:
            pass
    print(message, file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
