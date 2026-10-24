#!/usr/bin/env python3
"""Fail-closed tests for benchmark service ownership and recovery."""

import copy
import concurrent.futures
import fcntl
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid

import scanner_service as control


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.root.chmod(0o700)
        self.binary = self.root / "binary"
        self.binary.write_bytes(b"executable fixture")
        self.binary.chmod(0o700)
        self.environment = self.root / "private.env"
        self.environment.write_text("SECRET=do-not-output\n")
        self.environment.chmod(0o600)
        self.plan = {"run_id": str(uuid.uuid4()), "root": str(self.root), "binary": str(self.binary),
                     "binary_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                     "environment_file": str(self.environment), "args": ["600"],
                     "runtime_seconds": 60, "cpu_percent": 200, "memory_bytes": 268435456}
        self.service = control.Service(self.plan)
        self.calls = []
        self.state = {"LoadState": "not-found", "ActiveState": "inactive", "MainPID": "0",
                      "ControlGroup": "", "Description": "", "User": "", "WorkingDirectory": "", "Transient": "no"}

    def command(self, *args):
        self.calls.append(args)
        if args[:2] == ("systemctl", "show"):
            return "\n".join(key + "=" + value for key, value in self.state.items())
        if args[:4] == ("sudo", "-n", "systemctl", "stop"):
            self.state.update(ActiveState="inactive", MainPID="0", ControlGroup="")
            return ""
        raise AssertionError("unexpected mutation")

    def owned(self):
        self.state.update(LoadState="loaded", ActiveState="active", MainPID="123",
                          Description=self.service.description, User=str(os.getuid()),
                          WorkingDirectory=str(self.root), Transient="yes")

    def test_recover_stop_with_lost_controller_and_missing_inputs(self):
        self.owned()
        self.binary.unlink()
        self.environment.unlink()
        # A new controller needs the immutable plan, not the original process or input files.
        recovered = control.Service(copy.deepcopy(self.plan))
        with patch.object(control, "command", side_effect=self.command):
            self.assertTrue(recovered.execute("stop")["stopped"])
            self.assertTrue(recovered.execute("stop")["stopped"])

    def test_never_started_stop_has_no_mutation(self):
        with patch.object(control, "command", side_effect=self.command):
            self.assertTrue(self.service.execute("stop")["stopped"])
        self.assertTrue(all(call[0] == "systemctl" for call in self.calls))

    def test_controllers_serialize_before_observing_or_mutating(self):
        path = self.root / ("." + self.service.unit + ".lock")
        entering = threading.Event()
        acquired = threading.Event()
        flock = fcntl.flock

        def observe_lock(fd, operation):
            entering.set()
            flock(fd, operation)
            acquired.set()

        with path.open("w") as owner:
            flock(owner, fcntl.LOCK_EX)
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool, \
                    patch.object(control.fcntl, "flock", side_effect=observe_lock), \
                    patch.object(control, "command", side_effect=self.command):
                pending = pool.submit(self.service.execute, "stop")
                try:
                    self.assertTrue(entering.wait(5))
                    self.assertFalse(acquired.is_set())
                    self.assertEqual(self.calls, [])
                finally:
                    flock(owner, fcntl.LOCK_UN)
                self.assertTrue(pending.result(timeout=5)["stopped"])

    def test_foreign_service_and_changed_plan_refuse_stop(self):
        self.owned()
        for key, value in (("Description", "another task"), ("User", "0"),
                           ("WorkingDirectory", "/"), ("Transient", "no")):
            with self.subTest(key=key), patch.dict(self.state, {key: value}), \
                    patch.object(control, "command", side_effect=self.command):
                with self.assertRaisesRegex(ValueError, "ownership"):
                    self.service.execute("stop")
        changed = copy.deepcopy(self.plan)
        changed["args"] = ["different"]
        with patch.object(control, "command", side_effect=self.command), self.assertRaisesRegex(ValueError, "ownership"):
            control.Service(changed).execute("stop")
        self.assertTrue(all(call[0] == "systemctl" for call in self.calls))

    def test_duplicate_start_is_not_a_restart(self):
        self.owned()
        with patch.object(control, "command", side_effect=self.command), self.assertRaisesRegex(ValueError, "already exists"):
            self.service.execute("start")
        self.assertTrue(all(call[0] == "systemctl" for call in self.calls))

    def test_bad_digest_and_public_environment_refuse_start(self):
        with patch.object(control, "command", side_effect=self.command):
            self.binary.write_bytes(b"replaced")
            with self.assertRaisesRegex(ValueError, "digest"):
                self.service.execute("start")
            self.environment.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "0600"):
                self.service.execute("start")
        self.assertTrue(all(call[0] == "systemctl" for call in self.calls))

    def test_path_and_plan_validation(self):
        link = self.root / "link"
        link.symlink_to(self.binary)
        cases = {"binary": str(link), "environment_file": "/outside.env", "cpu_percent": True,
                 "runtime_seconds": 0, "memory_bytes": -1, "args": ["%n"], "run_id": "../other"}
        for key, value in cases.items():
            with self.subTest(key=key), self.assertRaises(ValueError):
                control.Service(dict(self.plan, **{key: value}))
        with self.assertRaises(ValueError):
            control.Service(dict(self.plan, unexpected="secret"))

    def test_resize_requires_same_process_and_kernel_limits(self):
        before = {"active": "active", "pid": 123}
        for after in ({"active": "active", "pid": 124},
                      {"active": "active", "pid": 123, "cpu_max": "200000 100000", "memory_max": 123},
                      {"active": "inactive", "pid": 0}):
            with self.subTest(after=after), patch.object(self.service, "observe", side_effect=[before, after]), \
                    patch.object(control, "command"), self.assertRaises(ValueError):
                self.service.execute("resize", 100, 123)

    def test_subprocess_error_does_not_expose_argv_or_stderr(self):
        result = subprocess.CompletedProcess([], 1, "private stdout", "SECRET=do-not-output")
        with patch.object(subprocess, "run", return_value=result), self.assertRaises(ValueError) as caught:
            control.command("sensitive-argument")
        self.assertNotIn("private", str(caught.exception))
        self.assertNotIn("SECRET", str(caught.exception))
        with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired(["sensitive"], 45)), \
                self.assertRaises(ValueError) as caught:
            control.command("sensitive-argument")
        self.assertNotIn("sensitive", str(caught.exception))

    def test_cli_malformed_private_plan_does_not_echo_input(self):
        plan = self.root / "plan.json"
        plan.write_text('{"SECRET=do-not-output":broken}')
        result = subprocess.run(["python3", str(Path(control.__file__)), "status", str(plan)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("SECRET", result.stderr)


if __name__ == "__main__":
    unittest.main()
