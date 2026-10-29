#!/usr/bin/env python3
"""Inspect runner capabilities and measure isolated, fixed-source Cargo builds."""

from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, timezone
import errno
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time


def command(arguments, *, timeout=15, cwd=None):
    try:
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return result.stdout.strip() if result.returncode == 0 else None
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def read(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return None


def memory_limit():
    memory = read("/proc/meminfo") or ""
    match = re.search(r"^MemTotal:\s+(\d+) kB$", memory, re.MULTILINE)
    limits = [int(match[1]) * 1024] if match else []
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        value = read(path)
        if value and value.isdigit():
            limits.append(int(value))
    return min(limits) if limits else None


def cpu_limit():
    limits = [float(os.cpu_count() or 1)]
    if hasattr(os, "sched_getaffinity"):
        limits.append(float(len(os.sched_getaffinity(0))))
    quota = read("/sys/fs/cgroup/cpu.max")
    if quota:
        fields = quota.split()
        if len(fields) == 2 and all(field.isdigit() for field in fields) and int(fields[1]) > 0:
            limits.append(int(fields[0]) / int(fields[1]))
    else:
        quota = read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period = read("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        if quota and period and quota.isdigit() and period.isdigit() and int(period) > 0:
            limits.append(int(quota) / int(period))
    return min(limits)


def io_uring():
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "aarch64"):
        return {"available": None, "reason": "unsupported_probe_platform"}
    libc = ctypes.CDLL(None, use_errno=True)
    # A zeroed io_uring_params requests no features and one queue entry. The
    # syscall creates only a descriptor; it does not submit I/O or map memory.
    parameters = ctypes.create_string_buffer(256)
    result = libc.syscall(ctypes.c_long(425), ctypes.c_uint(1), ctypes.byref(parameters))
    if result >= 0:
        os.close(result)
        return {"available": True, "reason": "setup_succeeded"}
    return {"available": False, "reason": errno.errorcode.get(ctypes.get_errno(), "UNKNOWN")}


def inspect(root):
    root = root.resolve(strict=True)
    usage = shutil.disk_usage(root)
    mount_raw = command(["findmnt", "--json", "--target", str(root), "--output", "TARGET,SOURCE,FSTYPE,OPTIONS"])
    mount = json.loads(mount_raw)["filesystems"][0] if mount_raw else None
    docker = command(["docker", "info", "--format", "{{.ServerVersion}}"])
    return {"schema": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
            "architecture": platform.machine(), "system": platform.system(), "kernel": platform.release(),
            "libc": list(platform.libc_ver()), "effective_cpu_count": cpu_limit(), "memory_limit_bytes": memory_limit(),
            "disk": {"root": str(root), "free_bytes": usage.free, "total_bytes": usage.total, "mount": mount},
            "docker": {"available": docker is not None, "server_version": docker}, "io_uring": io_uring(),
            "tools": {tool: command(arguments) for tool, arguments in {
                "rustc": ["rustc", "-Vv"], "cargo": ["cargo", "--version"], "cc": ["cc", "--version"],
                "cmake": ["cmake", "--version"], "protoc": ["protoc", "--version"],
                "clang": ["clang", "--version"], "pkg-config": ["pkg-config", "--version"]}.items()},
            "image": {"hosted_image": os.environ.get("ImageOS"), "hosted_version": os.environ.get("ImageVersion"),
                      "operator_image_digest": os.environ.get("RUSTFS_RUNNER_IMAGE_DIGEST")},
            "run": {"id": os.environ.get("GITHUB_RUN_ID"), "attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
                    "runner_name": os.environ.get("RUNNER_NAME")},
            "storage_locality_verified": False, "worker_destruction_verified": False}


def preflight(report, *, min_free_gib=0, min_memory_gib=0, architecture=None, docker=False, uring=False):
    failures = []
    if report["system"] != "Linux":
        failures.append("Linux is required for this pilot")
    if architecture and report["architecture"] != architecture:
        failures.append("architecture does not match the requested artifact/build target")
    if report["disk"]["free_bytes"] < min_free_gib * 1024 ** 3:
        failures.append("insufficient free disk")
    if min_memory_gib and (report["memory_limit_bytes"] is None or report["memory_limit_bytes"] < min_memory_gib * 1024 ** 3):
        failures.append("insufficient or unknown memory limit")
    if docker and not report["docker"]["available"]:
        failures.append("Docker daemon unavailable")
    if uring and report["io_uring"]["available"] is not True:
        failures.append("io_uring setup unavailable")
    return failures


def check_storage_attestation(path, report):
    """Check an operator assertion without treating nonrotational disks as local."""
    metadata = path.stat()
    if metadata.st_uid != 0 or metadata.st_mode & 0o022:
        raise ValueError("storage attestation must be root-owned and not group/world-writable")
    attestation = json.loads(path.read_text())
    mount = report["disk"]["mount"] or {}
    if not (attestation.get("schema") == 1 and attestation.get("storage") == "local-ssd"
            and attestation.get("mount_source") == mount.get("source")
            and attestation.get("mount_target") == mount.get("target")
            and mount.get("fstype") in ("ext4", "xfs", "btrfs")
            and re.fullmatch(r"sha256:[a-f0-9]{64}", attestation.get("image_digest", ""))):
        raise ValueError("local SSD attestation does not match this mount and image identity")
    if report["image"]["operator_image_digest"] != attestation["image_digest"]:
        raise ValueError("running image digest must match the operator storage attestation")
    report["storage_attestation"] = attestation
    # Local block devices can still be backed by network storage. This is an
    # operator assertion, never an independent physical-locality measurement.
    report["storage_locality_source"] = "operator_attestation"


def build_commands(workload):
    if workload == "server":
        return ["build", "--locked", "-p", "rustfs", "--bin", "rustfs"]
    if workload == "workspace-tests":
        return ["test", "--locked", "--workspace", "--all-targets", "--no-run"]
    raise ValueError("unsupported workload")


def measure(arguments, environment, cwd, log, time_file, timeout):
    start = time.monotonic()
    # GNU time records peak RSS and I/O for the Cargo process tree. Signal
    # handling kills that entire group before the task-owned target is removed.
    with log.open("w") as output:
        process = subprocess.Popen(["/usr/bin/time", "-v", "-o", str(time_file), *arguments],
                                   cwd=cwd, env=environment, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        timed_out = False
        try:
            result = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                result = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                result = process.wait(timeout=10)
            # The leader can exit before a child that ignores TERM. Do not
            # remove its target directory while that process group survives.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    timings = read(time_file) or ""
    def field(name):
        match = re.search(r"^\s*" + re.escape(name) + r":\s+(\d+)$", timings, re.MULTILINE)
        return int(match[1]) if match else None
    return {"elapsed_seconds": time.monotonic() - start, "exit_code": result, "timed_out": timed_out,
            "max_rss_kib": field("Maximum resident set size (kbytes)"),
            "filesystem_inputs": field("File system inputs"), "filesystem_outputs": field("File system outputs")}


def benchmark(source, root, output, sha, toolchain, workload, jobs, timeout, *, measure_fn=measure):
    if not re.fullmatch(r"[a-f0-9]{40}", sha) or not re.fullmatch(r"\d+\.\d+\.\d+", toolchain):
        raise ValueError("benchmark requires an exact source SHA and Rust toolchain version")
    if command(["git", "rev-parse", "HEAD"], cwd=source) != sha:
        raise ValueError("source checkout does not match the requested SHA")
    if command(["git", "status", "--porcelain", "--untracked-files=no"], cwd=source) != "":
        raise ValueError("source checkout has tracked modifications")
    version = command(["rustup", "run", toolchain, "rustc", "-Vv"])
    if not version or f"release: {toolchain}\n" not in version + "\n":
        raise ValueError("the exact requested Rust toolchain is not installed")
    if command(["/usr/bin/time", "--version"]) is None:
        raise ValueError("GNU time is required for resource measurements")
    output.mkdir(parents=True, exist_ok=True)
    report = {"schema": 1, "source_sha": sha, "rustc": version, "workload": workload,
              "profile": "dev", "features": "default", "build_jobs": jobs, "measurements": [],
              "cache_policy": "fresh Cargo target for cold; same target for warm; registry downloads may be warm",
              "passed": False, "task_directory_removed": False}
    target = Path(tempfile.mkdtemp(prefix="rustfs-runner-pilot-", dir=root))
    try:
        # Compare the checked-in build configuration, not ambient tuning left
        # by a previous job or interactive shell. Registry/network settings may
        # remain, but all known compiler/profile/target overrides are removed.
        environment = {key: value for key, value in os.environ.items()
                       if not key.startswith(("CARGO_PROFILE_", "CARGO_TARGET_", "CARGO_BUILD_"))
                       and key not in {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTDOCFLAGS", "CARGO_ENCODED_RUSTDOCFLAGS",
                                       "RUSTC", "RUSTDOC", "CC", "CXX", "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS",
                                       "RUSTFS_BUILD_VERSION"}}
        environment.update(CARGO_TARGET_DIR=str(target / "target"), CARGO_BUILD_JOBS=str(jobs),
                           CARGO_INCREMENTAL="0", RUSTC_WRAPPER="", RUSTC_WORKSPACE_WRAPPER="")
        arguments = ["rustup", "run", toolchain, "cargo", *build_commands(workload)]
        report["command"] = arguments
        for cache in ("cold", "warm"):
            log, time_file = output / f"{cache}.log", output / f"{cache}.time.txt"
            metric = measure_fn(arguments, environment, source, log, time_file, timeout)
            report["measurements"].append({"cache": cache, **metric})
            (output / "benchmark.json").write_text(json.dumps(report, indent=2) + "\n")
            if metric["exit_code"] != 0 or metric["timed_out"]:
                break
        report["passed"] = len(report["measurements"]) == 2 and all(item["exit_code"] == 0 and not item["timed_out"] for item in report["measurements"])
    finally:
        shutil.rmtree(target)
        report["task_directory_removed"] = not target.exists()
        (output / "benchmark.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="existing scratch directory on the disk under test")
    parser.add_argument("--output", type=Path, required=True, help="durable evidence directory outside scratch")
    parser.add_argument("--min-free-gib", type=float, default=0)
    parser.add_argument("--min-memory-gib", type=float, default=0)
    parser.add_argument("--architecture", choices=("x86_64", "aarch64"))
    parser.add_argument("--require-docker", action="store_true")
    parser.add_argument("--require-io-uring", action="store_true")
    parser.add_argument("--local-ssd-attestation", type=Path)
    parser.add_argument("--benchmark", choices=("server", "workspace-tests"))
    parser.add_argument("--source", type=Path, default=Path.cwd())
    parser.add_argument("--source-sha")
    parser.add_argument("--toolchain")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    args = parser.parse_args()
    if not (1 <= args.jobs <= 16 and 60 <= args.timeout_seconds <= 7200
            and 0 <= args.min_free_gib <= 10000 and 0 <= args.min_memory_gib <= 1000):
        parser.error("resource requirements are outside the bounded pilot limits")
    if args.benchmark and not (args.source_sha and args.toolchain):
        parser.error("--benchmark requires --source-sha and --toolchain")
    args.output.mkdir(parents=True, exist_ok=True)
    report = inspect(args.root)
    failures = preflight(report, min_free_gib=args.min_free_gib, min_memory_gib=args.min_memory_gib,
                         architecture=args.architecture, docker=args.require_docker, uring=args.require_io_uring)
    if args.local_ssd_attestation:
        try:
            check_storage_attestation(args.local_ssd_attestation, report)
        except (ValueError, OSError) as error:
            failures.append(str(error))
    report["preflight_failures"] = failures
    (args.output / "capabilities.json").write_text(json.dumps(report, indent=2) + "\n")
    if failures:
        print("Runner preflight failed: " + "; ".join(failures), file=sys.stderr)
        return 1
    if args.benchmark:
        def cancel(_signum, _frame):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, cancel)
        result = benchmark(args.source.resolve(), args.root.resolve(), args.output.resolve(), args.source_sha,
                           args.toolchain, args.benchmark, args.jobs, args.timeout_seconds)
        return 0 if result["passed"] else 1
    print("Runner preflight passed; see capabilities.json for observed capabilities and unverified infrastructure properties")
    return 0


if __name__ == "__main__":
    sys.exit(main())
