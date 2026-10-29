#!/usr/bin/env python3
"""Runner pilots refuse unsupported capabilities and preserve failed build evidence."""

import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import runner_pilot as pilot


class PilotTests(unittest.TestCase):
    def setUp(self):
        self.capabilities = {"system": "Linux", "architecture": "x86_64", "memory_limit_bytes": 8 * 1024 ** 3,
                             "disk": {"free_bytes": 100 * 1024 ** 3, "mount": {"source": "/dev/nvme0n1", "target": "/scratch", "fstype": "xfs"}},
                             "docker": {"available": True}, "io_uring": {"available": True},
                             "image": {"operator_image_digest": "sha256:" + "a" * 64}}
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.scratch = self.root / "scratch"
        self.scratch.mkdir()
        self.output = self.root / "evidence"

    def test_preflight_refuses_disk_memory_abi_and_required_daemon(self):
        self.assertEqual(pilot.preflight(self.capabilities, min_free_gib=80, min_memory_gib=8, docker=True, uring=True), [])
        broken = copy.deepcopy(self.capabilities)
        broken.update(architecture="aarch64", memory_limit_bytes=None)
        broken["docker"]["available"] = False
        broken["io_uring"]["available"] = None
        failures = pilot.preflight(broken, min_free_gib=101, min_memory_gib=8, architecture="x86_64", docker=True, uring=True)
        self.assertEqual(len(failures), 5)

    def test_memory_limit_obeys_container_limit(self):
        values = {"/proc/meminfo": "MemTotal:       67108864 kB",
                  "/sys/fs/cgroup/memory.max": str(8 * 1024 ** 3)}
        with patch.object(pilot, "read", side_effect=values.get):
            self.assertEqual(pilot.memory_limit(), 8 * 1024 ** 3)

    def test_local_ssd_requires_matching_operator_mount_and_image(self):
        attestation = self.root / "storage.json"
        row = {"schema": 1, "storage": "local-ssd", "mount_source": "/dev/nvme0n1", "mount_target": "/scratch", "image_digest": "sha256:" + "a" * 64}
        attestation.write_text(json.dumps(row))
        metadata = type("Metadata", (), {"st_uid": 0, "st_mode": 0o100644})()
        with patch.object(Path, "stat", return_value=metadata):
            pilot.check_storage_attestation(attestation, self.capabilities)
            self.assertEqual(self.capabilities["storage_locality_source"], "operator_attestation")
            attestation.write_text(json.dumps({**row, "mount_source": "/dev/other"}))
            with self.assertRaises(ValueError):
                pilot.check_storage_attestation(attestation, self.capabilities)
        metadata.st_mode = 0o100666
        with patch.object(Path, "stat", return_value=metadata), self.assertRaises(ValueError):
            pilot.check_storage_attestation(attestation, self.capabilities)

    def invoke_benchmark(self, *, fail=False, raise_error=False):
        environments = []
        def command(arguments, **kwargs):
            if arguments[:2] == ["git", "rev-parse"]:
                return "a" * 40
            if arguments[:2] == ["git", "status"]:
                return ""
            if arguments[0] == "rustup":
                return "rustc 1.92.0\nrelease: 1.92.0\nhost: x86_64-unknown-linux-gnu"
            return "GNU time"
        def measure(arguments, environment, source, log, time_file, timeout):
            environments.append(environment)
            target = Path(environment["CARGO_TARGET_DIR"])
            target.mkdir(exist_ok=True)
            (target / "compiled").write_text("fixture")
            log.write_text("controlled build output")
            time_file.write_text("fixture resources")
            if raise_error:
                raise OSError("controlled build failure")
            return {"exit_code": 1 if fail else 0, "timed_out": False, "elapsed_seconds": 1,
                    "max_rss_kib": 1024, "filesystem_inputs": 1, "filesystem_outputs": 2}
        with patch.object(pilot, "command", side_effect=command):
            result = pilot.benchmark(self.root, self.scratch, self.output, "a" * 40, "1.92.0", "server", 2, 60, measure_fn=measure)
        return result, environments

    def test_cold_warm_share_only_task_target_and_cleanup(self):
        with patch.dict(os.environ, {"CARGO_PROFILE_DEV_OVERFLOW_CHECKS": "false", "RUSTFLAGS": "-C opt-level=3"}):
            result, environments = self.invoke_benchmark()
        self.assertTrue(result["passed"])
        self.assertEqual([row["cache"] for row in result["measurements"]], ["cold", "warm"])
        self.assertEqual(environments[0]["CARGO_TARGET_DIR"], environments[1]["CARGO_TARGET_DIR"])
        self.assertEqual(environments[0]["RUSTC_WRAPPER"], "")
        self.assertEqual(environments[0]["CARGO_INCREMENTAL"], "0")
        self.assertNotIn("CARGO_PROFILE_DEV_OVERFLOW_CHECKS", environments[0])
        self.assertNotIn("RUSTFLAGS", environments[0])
        self.assertTrue(result["task_directory_removed"])
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_failed_build_has_no_warm_success_and_keeps_receipt(self):
        result, _ = self.invoke_benchmark(fail=True)
        self.assertFalse(result["passed"])
        self.assertEqual(len(result["measurements"]), 1)
        self.assertTrue(result["task_directory_removed"])
        self.assertEqual(json.loads((self.output / "benchmark.json").read_text()), result)

    def test_unexpected_failure_still_removes_only_task_directory(self):
        unrelated = self.scratch / "unrelated"
        unrelated.mkdir()
        with self.assertRaises(OSError):
            self.invoke_benchmark(raise_error=True)
        self.assertEqual(list(self.scratch.iterdir()), [unrelated])
        self.assertFalse(json.loads((self.output / "benchmark.json").read_text())["passed"])

    def test_requires_pinned_source_and_toolchain(self):
        for sha, version in (("main", "1.92.0"), ("a" * 40, "stable")):
            with self.assertRaises(ValueError):
                pilot.benchmark(self.root, self.scratch, self.output, sha, version, "server", 2, 60)
        with patch.object(pilot, "command", return_value="b" * 40), self.assertRaises(ValueError):
            pilot.benchmark(self.root, self.scratch, self.output, "a" * 40, "1.92.0", "server", 2, 60)


if __name__ == "__main__":
    unittest.main()
