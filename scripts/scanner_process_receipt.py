#!/usr/bin/env python3
"""Read Linux process evidence for an owned scanner benchmark service."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

from scanner_service import Service, local_path, require

PROC = Path("/proc")
SAFE_ENVIRONMENT = frozenset({
    "RUSTFS_ADDRESS", "RUSTFS_CONSOLE_ENABLE", "RUSTFS_ERASURE_SET_DRIVE_COUNT",
    "RUSTFS_STORAGE_CLASS_STANDARD", "RUSTFS_DURABILITY_MODE", "RUSTFS_NEW_BUCKET_DURABILITY_MODE",
    "RUSTFS_SCANNER_ENABLED", "RUSTFS_HEAL_ENABLED", "RUSTFS_SCANNER_SPEED", "RUSTFS_SCANNER_IDLE_MODE",
    "RUSTFS_SCANNER_START_DELAY_SECS", "RUSTFS_SCANNER_CYCLE", "RUSTFS_SCANNER_PREFIX_ATTRIBUTION",
    "HOTPATH_FOCUS", "HOTPATH_ALLOC_CUMULATIVE", "HOTPATH_PROMETHEUS_HOST", "HOTPATH_PROMETHEUS_PORT",
    "HOTPATH_METRICS_PORT", "HOTPATH_OUTPUT_PATH", "HOTPATH_DRAIN_INTERVAL",
    "RUSTFS_HEAL_ADAPTIVE_ADMISSION",
    "RUSTFS_OBJECT_LOCK_DIAG_ENABLE", "RUSTFS_OBS_LOGS_EXPORT_ENABLED", "RUSTFS_OBS_TRACES_EXPORT_ENABLED",
    "RUSTFS_OBS_METRICS_EXPORT_ENABLED", "RUST_LOG", "NO_PROXY", "no_proxy",
})
PRIVATE_ENVIRONMENT = frozenset({
    "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY", "RUSTFS_ROOT_USER", "RUSTFS_ROOT_PASSWORD",
})


def read_bounded(path, limit=65536):
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, "oversized process input")
    return data


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def process_state(pid):
    root = PROC / str(pid)
    require(root.stat().st_uid == os.getuid(), "process owner mismatch")
    stat = read_bounded(root / "stat").decode()
    # comm is parenthesized and may contain spaces or parentheses.
    end = stat.rfind(")")
    require(end >= 0 and stat.split(" ", 1)[0] == str(pid), "invalid process stat")
    fields = stat[end + 1:].split()
    require(len(fields) >= 20 and int(fields[19]) > 0, "invalid process start time")
    return {"start_ticks": int(fields[19]),
            "arguments": read_bounded(root / "cmdline"),
            "environment": read_bounded(root / "environ"),
            "cgroup": read_bounded(root / "cgroup"),
            "cwd": os.readlink(root / "cwd"),
            "executable": os.readlink(root / "exe")}


def environment_digest(raw, expected):
    require(type(expected) is dict and set(expected) <= SAFE_ENVIRONMENT
            and all(type(value) is str for value in expected.values()), "unsupported expected environment")
    observed = {}
    for entry in raw.split(b"\0"):
        if not entry:
            continue
        key, value = entry.split(b"=", 1)
        key = key.decode()
        if key in PRIVATE_ENVIRONMENT:
            continue
        if key.startswith(("RUSTFS_", "HOTPATH_")) or key in SAFE_ENVIRONMENT:
            require(key in SAFE_ENVIRONMENT and key not in observed, "unrecognized or duplicate benchmark environment")
            observed[key] = value.decode()
    require(observed == expected, "benchmark environment mismatch")
    return fingerprint(observed)


def attest(service, expected_environment, data_roots):
    started = time.time_ns()
    monotonic_started = time.monotonic_ns()
    boot_id = str(uuid.UUID((PROC / "sys/kernel/random/boot_id").read_text().strip()))
    before = service.observe()
    require(before["active"] == "active" and before["pid"] > 0, "service is not active")
    pid = before["pid"]
    state = process_state(pid)
    require(state["cwd"] == str(service.root) and state["executable"] == str(service.binary),
            "process path mismatch")
    require(state["cgroup"] == ("0::/system.slice/" + service.unit + "\n").encode(), "process cgroup mismatch")
    arguments = [str(service.binary), *service.plan["args"]]
    require(state["arguments"] == b"\0".join(arg.encode() for arg in arguments) + b"\0", "process arguments mismatch")
    config_hash = environment_digest(state["environment"], expected_environment)
    with (PROC / str(pid) / "exe").open("rb") as stream:
        identity = os.fstat(stream.fileno())
        binary_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    require(binary_hash == service.plan["binary_sha256"], "running binary digest mismatch")
    require(type(data_roots) is list and data_roots and len(data_roots) == len(set(data_roots)), "invalid data roots")
    directories = []
    for name in data_roots:
        path = local_path(name)
        info = path.stat()
        require(path.is_dir() and info.st_uid == os.getuid(), "data root must be an owned directory")
        usage = os.statvfs(path)
        directories.append({"path": str(path), "device": info.st_dev, "inode": info.st_ino,
                            "total_bytes": usage.f_blocks * usage.f_frsize,
                            "available_bytes": usage.f_bavail * usage.f_frsize})
    require(len({(directory["device"], directory["inode"]) for directory in directories}) == len(directories),
            "duplicate data directory identity")
    require(process_state(pid) == state and service.observe() == before, "process or resource limits changed during observation")
    current = (PROC / str(pid) / "exe").stat()
    require((current.st_dev, current.st_ino) == (identity.st_dev, identity.st_ino), "running executable changed")
    for directory in directories:
        current = local_path(directory["path"]).stat()
        require((current.st_dev, current.st_ino) == (directory["device"], directory["inode"]),
                "data directory changed during observation")
    ended = time.time_ns()
    require(ended >= started, "wall clock moved backwards")
    return {"schema": 1, "kind": "process_receipt", "boot_id": boot_id,
            "observed_start_unix_ns": started, "observed_end_unix_ns": ended,
            "elapsed_monotonic_ns": time.monotonic_ns() - monotonic_started,
            "pid": pid, "process_start_ticks": state["start_ticks"],
            "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
            "binary_sha256": binary_hash, "executable_device": identity.st_dev, "executable_inode": identity.st_ino,
            "arguments_sha256": fingerprint(arguments), "environment_sha256": config_hash,
            "environment_keys": sorted(expected_environment), "service": before, "data_roots": directories,
            "effective_server_configuration_verified": False, "data_roots_bound_to_server": False,
            "storage_acceptance_passed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("environment", type=Path, help="expected non-secret environment JSON; private values are excluded")
    parser.add_argument("--data-root", action="append", required=True)
    args = parser.parse_args()
    try:
        service = Service(json.loads(read_bounded(args.plan)))
        result = attest(service, json.loads(read_bounded(args.environment)), args.data_root)
    except (ValueError, TypeError, AttributeError, KeyError, OSError, IndexError):
        print("process evidence failed; preserve inputs and reconcile service state", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
