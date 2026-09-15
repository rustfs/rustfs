#!/usr/bin/env python3
"""Verify candidate bytes before a suite can install them on any shared node."""
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import prepare_functional_package as package


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.payload = b"verified package bytes"
        self.digest = hashlib.sha256(self.payload).hexdigest()
        self.chain = {"run_id": 123, "attempt": 2, "candidate": {"manifest": {
            "package_sha256": self.digest, "package_url": "https://example.invalid/rustfs.deb"}}}
        self.env = {"RUSTFS_NODES": "vm000 vm001", "RUSTFS_SSH_USER": "tester"}

    def test_fetch_failure_or_hash_mismatch_never_contacts_a_node(self):
        def mismatch(args, **kwargs):
            Path(args[args.index("--output") + 1]).write_bytes(b"wrong package")
        for effect, expected in ((subprocess.CalledProcessError(22, "curl"), subprocess.CalledProcessError),
                                 (mismatch, ValueError)):
            with self.subTest(effect=effect), mock.patch.dict(os.environ, self.env), \
                    mock.patch.object(package.subprocess, "run", side_effect=effect) as run, \
                    self.assertRaises(expected):
                package.prepare(self.chain)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.args[0][0], "curl")

    def test_every_node_receives_the_same_checked_bytes(self):
        transfers = []
        def run(args, **kwargs):
            if args[0] == "curl":
                Path(args[args.index("--output") + 1]).write_bytes(self.payload)
            else:
                transfers.append((args[-2], kwargs["stdin"].read(), args[-1]))
        with mock.patch.dict(os.environ, self.env), mock.patch.object(package.subprocess, "run", side_effect=run):
            url = package.prepare(self.chain)
        self.assertEqual(url, f"file:///var/cache/rustfs-functional/123-2/{self.digest}.deb")
        self.assertEqual([peer for peer, _, _ in transfers], ["tester@vm000", "tester@vm001"])
        for _, payload, command in transfers:
            self.assertEqual(payload, self.payload)
            self.assertIn("sha256sum -c - >/dev/null", command)
            self.assertIn(self.digest, command)

    def test_remote_hash_mismatch_preserves_previous_package_and_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cache" / "package.deb"
            path.parent.mkdir()
            path.write_bytes(self.payload)
            script = package.install_script(path, self.digest)
            failed = subprocess.run(["sh", "-c", script], input=b"corrupt transfer", capture_output=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertEqual(path.read_bytes(), self.payload)
            self.assertEqual(list(path.parent.iterdir()), [path])
            good = subprocess.run(["sh", "-c", script], input=self.payload, capture_output=True)
            self.assertEqual(good.returncode, 0, good.stderr)
            self.assertEqual(path.read_bytes(), self.payload)

    def test_cleanup_attempts_every_node_and_reports_failure(self):
        with mock.patch.dict(os.environ, self.env), \
                mock.patch.object(package, "ssh", side_effect=[subprocess.CalledProcessError(1, "ssh"), None]) as ssh, \
                self.assertRaisesRegex(ValueError, "tester@vm000"):
            package.cleanup(self.chain)
        self.assertEqual([call.args[0] for call in ssh.call_args_list], ["tester@vm000", "tester@vm001"])


if __name__ == "__main__":
    unittest.main()
