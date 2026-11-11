#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Linux integration tests using the real DEB/RPM recipes and package managers."""
import hashlib
import importlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent / "release"))
deliver = importlib.import_module("deliver")
c = importlib.import_module("candidate")
fixtures = importlib.import_module("test_candidate")


class PackageIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        missing = [tool for tool in ("dpkg-deb", "fakeroot", "rpm", "rpm2cpio", "cpio", "fpm") if not shutil.which(tool)]
        if sys.platform != "linux" or missing:
            raise RuntimeError(f"Run on Linux with real packaging tools; missing: {missing}")

    def test_each_target_wraps_its_accepted_bytes(self):
        for target in c.LINUX:
            with self.subTest(target=target), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                executable = root / "rustfs"
                executable.write_bytes(b"package contract fixture only: " + target.encode())
                expected = hashlib.sha256(executable.read_bytes()).hexdigest()
                value = fixtures.inputs()
                with patch.object(deliver, "binary", return_value=(executable, {"binary_sha256": expected})):
                    deliver.package(value, target, root / "output")
                record = c.read_json(root / "output" / f"packages-{target}.json")
                self.assertEqual(record["binary_sha256"], expected)
                self.assertEqual(record["inputs_sha256"], c.digest(value))
                self.assertEqual({Path(item["name"]).suffix for item in record["files"]}, {".deb", ".rpm"})

    def test_real_package_with_substituted_payload_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            executable = root / "rustfs"
            executable.write_bytes(b"accepted fixture")
            expected = c.file_digest(executable)
            real_run = deliver.run
            def mutate_deb(args, **kwargs):
                result = real_run(args, **kwargs)
                if any(str(arg).endswith("build_deb.sh") for arg in args):
                    work = Path(kwargs["cwd"])
                    package = work / kwargs["env"]["DEB_FILE"]
                    unpacked = work / "corrupted-package"
                    subprocess.run(["dpkg-deb", "--raw-extract", str(package), str(unpacked)], check=True)
                    (unpacked / "usr/bin/rustfs").write_bytes(b"wrong server")
                    subprocess.run(["dpkg-deb", "--build", str(unpacked), str(package)], check=True)
                return result
            with patch.object(deliver, "binary", return_value=(executable, {"binary_sha256": expected})), patch.object(deliver, "run", side_effect=mutate_deb):
                with self.assertRaisesRegex(ValueError, "package binary differs"):
                    deliver.package(fixtures.inputs(), "linux-x86_64-musl", root / "output")
            self.assertFalse(list((root / "output").glob("packages-*.json")))


if __name__ == "__main__":
    unittest.main()
