#!/usr/bin/env python3
"""Reject incomplete or changing process evidence without exposing configuration."""

import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

import scanner_process_receipt as receipt
from scanner_service import Service


class ProcessReceiptTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.root.chmod(0o700)
        self.binary = self.root / "binary"
        self.binary.write_bytes(b"actual executable fixture")
        self.data = self.root / "data"
        self.data.mkdir()
        self.plan = {"run_id": str(uuid.uuid4()), "root": str(self.root), "binary": str(self.binary),
                     "binary_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest(),
                     "environment_file": str(self.root / "private.env"), "args": [str(self.data)],
                     "runtime_seconds": 60, "cpu_percent": 200, "memory_bytes": 268435456}
        self.service = Service(self.plan)
        self.proc = self.root / "proc"
        self.pid = self.proc / "123"
        self.pid.mkdir(parents=True)
        boot = self.proc / "sys/kernel/random/boot_id"
        boot.parent.mkdir(parents=True)
        boot.write_text(str(uuid.uuid4()))
        self.start_ticks = 892351
        (self.pid / "stat").write_text("123 (name with ) parentheses) " +
                                      " ".join(["S"] + ["0"] * 18 + [str(self.start_ticks)] + ["0"] * 10))
        (self.pid / "exe").symlink_to(self.binary)
        (self.pid / "cwd").symlink_to(self.root)
        (self.pid / "cmdline").write_bytes((str(self.binary) + "\0" + str(self.data) + "\0").encode())
        (self.pid / "cgroup").write_text("0::/system.slice/" + self.service.unit + "\n")
        self.public = {"RUSTFS_SCANNER_ENABLED": "false", "NO_PROXY": "*"}
        self.environment = b"RUSTFS_SCANNER_ENABLED=false\0NO_PROXY=*\0RUSTFS_SECRET_KEY=private-fixture\0PATH=/bin\0"
        (self.pid / "environ").write_bytes(self.environment)
        self.observation = {"unit": self.service.unit, "active": "active", "pid": 123, "stopped": False,
                            "cpu_max": "200000 100000", "memory_max": 268435456}
        self.addCleanup(patch.stopall)
        patch.object(receipt, "PROC", self.proc).start()
        self.observe = patch.object(self.service, "observe", side_effect=lambda: copy.deepcopy(self.observation)).start()

    def attest(self):
        return receipt.attest(self.service, self.public, [str(self.data)])

    def test_real_file_identity_and_public_digest_without_private_values(self):
        result = self.attest()
        self.assertEqual(result["binary_sha256"], self.plan["binary_sha256"])
        self.assertEqual(result["executable_inode"], self.binary.stat().st_ino)
        self.assertEqual(result["data_roots"][0]["inode"], self.data.stat().st_ino)
        self.assertEqual(result["process_start_ticks"], self.start_ticks)
        self.assertEqual(result["environment_sha256"], receipt.fingerprint(self.public))
        self.assertEqual(result["service"], self.observation)
        self.assertEqual(result["clock_ticks_per_second"], os.sysconf("SC_CLK_TCK"))
        self.assertFalse(result["effective_server_configuration_verified"])
        self.assertFalse(result["data_roots_bound_to_server"])
        self.assertFalse(result["storage_acceptance_passed"])
        self.assertLessEqual(result["observed_start_unix_ns"], result["observed_end_unix_ns"])
        self.assertNotIn("private-fixture", json.dumps(result))
        self.assertNotIn("SECRET", json.dumps(result))

    def test_selected_environment_must_match_exactly(self):
        cases = [self.environment.replace(b"false", b"true"),
                 self.environment.replace(b"NO_PROXY=*\0", b""),
                 self.environment + b"RUSTFS_UNKNOWN=private-fixture\0",
                 self.environment + b"NO_PROXY=*\0"]
        for raw in cases:
            with self.subTest(raw_hash=hashlib.sha256(raw).hexdigest()), self.assertRaises(ValueError):
                receipt.environment_digest(raw, self.public)
        for expected in ({"RUSTFS_SECRET_KEY": "private-fixture"}, {"NO_PROXY": 1}, []):
            with self.assertRaises(ValueError):
                receipt.environment_digest(self.environment, expected)

    def test_credentials_do_not_change_public_digest(self):
        changed = self.environment.replace(b"private-fixture", b"different-private-fixture")
        self.assertEqual(receipt.environment_digest(changed, self.public),
                         receipt.environment_digest(self.environment, self.public))

    def test_scanner_schedule_is_bound_to_actual_process_environment(self):
        schedule = {"RUSTFS_SCANNER_START_DELAY_SECS": "1", "RUSTFS_SCANNER_CYCLE": "5"}
        self.public.update(schedule)
        raw = self.environment + b"RUSTFS_SCANNER_START_DELAY_SECS=1\0RUSTFS_SCANNER_CYCLE=5\0"
        (self.pid / "environ").write_bytes(raw)
        result = self.attest()
        self.assertEqual(result["environment_sha256"], receipt.fingerprint(self.public))
        self.assertTrue(set(schedule) <= set(result["environment_keys"]))
        for key in schedule:
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "mismatch"):
                receipt.environment_digest(raw, {**self.public, key: "60"})
        with self.assertRaises(ValueError):
            receipt.environment_digest(raw + b"RUSTFS_SCANNER_UNKNOWN=1\0", self.public)

    def test_inactive_service_is_rejected_before_process_access(self):
        self.observation.update(active="inactive", pid=0)
        with self.assertRaisesRegex(ValueError, "not active"):
            self.attest()

    def test_wrong_binary_content_is_rejected(self):
        self.binary.write_bytes(b"replaced executable")
        with self.assertRaisesRegex(ValueError, "binary digest"):
            self.attest()

    def test_arguments_cgroup_and_paths_are_checked(self):
        for name, replacement in (("cmdline", b"another-command\0"), ("cgroup", b"0::/another.service\n")):
            path = self.pid / name
            original = path.read_bytes()
            with self.subTest(name=name), self.assertRaises(ValueError):
                path.write_bytes(replacement)
                self.attest()
            path.write_bytes(original)
        for name in ("cwd", "exe"):
            path = self.pid / name
            original = os.readlink(path)
            path.unlink()
            path.symlink_to(self.data)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "path mismatch"):
                self.attest()
            path.unlink()
            path.symlink_to(original)

    def test_pid_reuse_and_process_changes_during_hash_are_rejected(self):
        original = receipt.process_state
        for key, value in (("start_ticks", self.start_ticks + 1), ("arguments", b"changed\0"),
                           ("environment", self.environment + b"PRIVATE=changed\0"),
                           ("cgroup", b"0::/changed\n")):
            first = original(123)
            second = dict(first, **{key: value})
            with self.subTest(key=key), patch.object(receipt, "process_state", side_effect=[first, second]), \
                    self.assertRaisesRegex(ValueError, "changed during observation"):
                self.attest()

    def test_changed_pid_or_kernel_limit_is_rejected(self):
        for key, value in (("pid", 124), ("cpu_max", "100000 100000"), ("memory_max", 134217728)):
            self.observe.side_effect = [self.observation, dict(self.observation, **{key: value})]
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "changed during observation"):
                self.attest()

    def test_replaced_executable_is_rejected_even_with_same_bytes(self):
        digest = receipt.hashlib.file_digest

        def replace_after_hash(stream, algorithm):
            result = digest(stream, algorithm)
            old = self.root / "previous-binary"
            self.binary.rename(old)
            self.binary.write_bytes(old.read_bytes())
            return result

        with patch.object(receipt.hashlib, "file_digest", side_effect=replace_after_hash), \
                self.assertRaisesRegex(ValueError, "executable changed"):
            self.attest()

    def test_replaced_data_directory_during_observation_is_rejected(self):
        def observe_after_data():
            if self.observe.call_count == 2:
                self.data.rename(self.root / "previous-data")
                self.data.mkdir()
            return copy.deepcopy(self.observation)

        self.observe.side_effect = observe_after_data
        with self.assertRaisesRegex(ValueError, "data directory changed"):
            self.attest()

    def test_missing_duplicate_or_aliased_data_roots_are_rejected(self):
        link = self.root / "alias"
        link.symlink_to(self.data)
        for paths in ([], [str(self.data)] * 2, [str(self.data), str(self.data) + "/."],
                      [str(link)], [str(self.binary)], [str(self.root / "missing")]):
            with self.subTest(paths=paths), self.assertRaises((ValueError, OSError)):
                receipt.attest(self.service, self.public, paths)

    def test_bad_stat_and_oversized_inputs_are_rejected(self):
        path = self.pid / "stat"
        for value in ("123 (truncated) S 0", "999 (wrong pid) S " + "0 " * 30,
                      "123 (no start) S " + "0 " * 30):
            path.write_text(value)
            with self.assertRaises(ValueError):
                receipt.process_state(123)
        path.write_bytes(b"x" * 65537)
        with self.assertRaisesRegex(ValueError, "oversized"):
            receipt.read_bounded(path)

    def test_cli_failure_does_not_echo_secret_or_produce_receipt(self):
        plan = self.root / "plan.json"
        plan.write_text(json.dumps(self.plan))
        public = self.root / "public.json"
        public.write_text('{"private-fixture":broken}')
        result = subprocess.run(["python3", receipt.__file__, str(plan), str(public), "--data-root", str(self.data)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("private-fixture", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
