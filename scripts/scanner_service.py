#!/usr/bin/env python3
"""Node-local service control for isolated scanner benchmarks (Linux/systemd)."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import uuid


def require(condition, message):
    if not condition:
        raise ValueError(message)


def positive(value, name):
    require(type(value) is int and value > 0, "invalid " + name)
    return value


def local_path(value):
    require(isinstance(value, str) and re.fullmatch(r"/[A-Za-z0-9_./-]+", value), "invalid local path")
    path = Path(value)
    require(path == path.resolve(), "local path must be canonical")
    return path


def command(*args):
    # Never include argv, environment-file contents or command output in errors.
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=45, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError("service command failed or timed out; reconcile with status/stop") from None
    require(result.returncode == 0, "service command failed; reconcile with status/stop")
    return result.stdout


class Service:
    def __init__(self, plan):
        require(type(plan) is dict and set(plan) == {
            "run_id", "root", "binary", "binary_sha256", "environment_file", "args",
            "runtime_seconds", "cpu_percent", "memory_bytes",
        }, "invalid service plan fields")
        require(str(uuid.UUID(plan["run_id"])) == plan["run_id"], "invalid run ID")
        require(os.getuid() != 0, "run as the benchmark user, not root")
        self.plan = plan
        self.root = local_path(plan["root"])
        root_stat = self.root.stat()
        require(self.root.is_dir() and root_stat.st_uid == os.getuid()
                and stat.S_IMODE(root_stat.st_mode) == 0o700, "benchmark root must be private and owned")
        self.binary = local_path(plan["binary"])
        self.environment = local_path(plan["environment_file"])
        require(self.binary.is_relative_to(self.root) and self.environment.is_relative_to(self.root),
                "binary and environment file must be inside benchmark root")
        require(isinstance(plan["binary_sha256"], str)
                and re.fullmatch(r"[0-9a-f]{64}", plan["binary_sha256"]), "invalid binary digest")
        require(type(plan["args"]) is list and all(isinstance(arg, str) and arg
                and not any(ord(c) < 32 or c == "%" for c in arg) for arg in plan["args"]), "invalid service arguments")
        for key in ("runtime_seconds", "cpu_percent", "memory_bytes"):
            positive(plan[key], key)
        identity = hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.unit = "rustfs-scanner-" + plan["run_id"] + ".service"
        self.description = "RustFS scanner acceptance " + identity

    def state(self):
        raw = command("systemctl", "show", self.unit,
                      "--property=LoadState,ActiveState,MainPID,ControlGroup,Description,User,WorkingDirectory,Transient")
        state = dict(line.split("=", 1) for line in raw.splitlines() if "=" in line)
        if state.get("LoadState") != "not-found":
            require(state.get("Description") == self.description and state.get("User") == str(os.getuid())
                    and state.get("WorkingDirectory") == str(self.root) and state.get("Transient") == "yes",
                    "service ownership mismatch; refusing operation")
        return state

    def observe(self):
        state = self.state()
        pid = int(state["MainPID"])
        result = {"unit": self.unit, "active": state["ActiveState"], "pid": pid,
                  "stopped": state["ActiveState"] in ("inactive", "failed") and pid == 0,
                  "cpu_max": None, "memory_max": None}
        if state["ActiveState"] == "active":
            require(pid > 0 and state["ControlGroup"] == "/system.slice/" + self.unit, "unexpected service cgroup")
            cgroup = Path("/sys/fs/cgroup") / state["ControlGroup"].lstrip("/")
            result["cpu_max"] = (cgroup / "cpu.max").read_text().strip()
            result["memory_max"] = int((cgroup / "memory.max").read_text())
        return result

    def start(self):
        require(self.state()["LoadState"] == "not-found", "service already exists; use status/stop to reconcile")
        for path in (self.binary, self.environment):
            info = path.stat()
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                    and not info.st_mode & 0o022, "service input must be an owned regular file")
        require(stat.S_IMODE(self.environment.stat().st_mode) == 0o600, "environment file must have mode 0600")
        require(os.access(self.binary, os.X_OK), "binary is not executable")
        with self.binary.open("rb") as source:
            require(hashlib.file_digest(source, "sha256").hexdigest() == self.plan["binary_sha256"], "binary digest mismatch")
        command("sudo", "-n", "systemd-run", "--quiet", "--collect", "--service-type=exec",
                "--expand-environment=no", "--unit=" + self.unit, "--description=" + self.description,
                "--uid=" + str(os.getuid()), "--gid=" + str(os.getgid()),
                "--property=WorkingDirectory=" + str(self.root),
                "--property=EnvironmentFile=" + str(self.environment),
                "--property=KillMode=control-group", "--property=SendSIGKILL=yes",
                "--property=TimeoutStopSec=30", "--property=Restart=no", "--property=NoNewPrivileges=yes",
                "--property=RuntimeMaxSec=" + str(self.plan["runtime_seconds"]),
                "--property=CPUAccounting=yes", "--property=MemoryAccounting=yes", "--property=IOAccounting=yes",
                "--property=CPUQuota=" + str(self.plan["cpu_percent"]) + "%",
                "--property=MemoryMax=" + str(self.plan["memory_bytes"]),
                "--", str(self.binary), *self.plan["args"])
        result = self.observe()
        require(result["active"] == "active", "service exited before observation")
        return result

    def resize(self, cpu_percent, memory_bytes):
        positive(cpu_percent, "cpu_percent")
        positive(memory_bytes, "memory_bytes")
        before = self.observe()
        require(before["active"] == "active", "cannot resize inactive service")
        command("sudo", "-n", "systemctl", "set-property", "--runtime", self.unit,
                "CPUQuota=" + str(cpu_percent) + "%", "MemoryMax=" + str(memory_bytes))
        after = self.observe()
        require(after["active"] == "active" and after["pid"] == before["pid"], "process changed during resize")
        quota, period = map(int, after["cpu_max"].split())
        require(quota * 100 == period * cpu_percent and after["memory_max"] == memory_bytes,
                "kernel resource limits do not match request")
        return after

    def stop(self):
        if self.state()["LoadState"] != "not-found":
            command("sudo", "-n", "systemctl", "stop", self.unit)
        result = self.observe()
        require(result["stopped"], "service has not stopped")
        return result

    def execute(self, action, cpu_percent=None, memory_bytes=None):
        # Separate SSH controllers serialize mutations without relying on a PID file.
        lock_path = self.root / ("." + self.unit + ".lock")
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if action == "start":
                return self.start()
            if action == "stop":
                return self.stop()
            if action == "resize":
                return self.resize(cpu_percent, memory_bytes)
            require(action == "status", "unknown action")
            return self.observe()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "status", "resize", "stop"))
    parser.add_argument("plan", type=Path, help="immutable plan; credentials belong in its private environment file")
    parser.add_argument("--cpu-percent", type=int)
    parser.add_argument("--memory-bytes", type=int)
    args = parser.parse_args()
    try:
        require(args.plan.stat().st_size <= 65536, "service plan is too large")
        result = Service(json.loads(args.plan.read_text())).execute(args.action, args.cpu_percent, args.memory_bytes)
    except (ValueError, TypeError, AttributeError, KeyError, OSError):
        # A malformed plan or a tool error may contain secret-bearing input.
        print("service control failed; retain the plan and reconcile with status/stop", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
