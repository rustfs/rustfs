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
import zipfile
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

    def test_manual_packages_match_release_names_and_bytes(self):
        script = Path(__file__).resolve().parent / "release" / "package_manual_artifacts.sh"
        cases = (
            ("9.9.9-rc.8", "x86_64", "gnu", True),
            ("9.9.9", "aarch64", "musl", False),
        )
        for version, arch, libc, with_catalog in cases:
            with self.subTest(version=version, arch=arch, libc=libc), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                payload = f"manual-package:{version}:{arch}:{libc}\n".encode()
                binary = root / "rustfs"
                binary.write_bytes(payload)
                command = ["bash", str(script), "package", "--version", version, "--arch", arch, "--libc", libc,
                           "--binary", str(binary), "--output", str(root / "dist")]
                if with_catalog:
                    for name in ("rustfs.cpu-symbol-catalog.json", "rustfs.cpu-symbol-catalog.sha256"):
                        (root / name).write_text(name + "\n", encoding="utf-8")
                        command.extend(["--zip-extra", str(root / name)])
                subprocess.run(command, check=True, text=True, timeout=180)
                stem = f"rustfs-linux-{arch}-{libc}-v{version}"
                dist = root / "dist"
                self.assertEqual(sorted(path.name for path in dist.iterdir()),
                                 [f"{stem}.deb", f"{stem}.rpm", f"{stem}.zip"])
                expected_hash = hashlib.sha256(payload).hexdigest()
                with zipfile.ZipFile(dist / f"{stem}.zip") as archive:
                    names = sorted(archive.namelist())
                    self.assertEqual(hashlib.sha256(archive.read("rustfs")).hexdigest(), expected_hash)
                if with_catalog:
                    self.assertEqual(names, ["rustfs", "rustfs.cpu-symbol-catalog.json", "rustfs.cpu-symbol-catalog.sha256"])
                else:
                    self.assertEqual(names, ["rustfs"])
                deb = dist / f"{stem}.deb"
                self.assertEqual(subprocess.check_output(["dpkg-deb", "-f", deb, "Version"], text=True).strip(),
                                 version.replace("-", "~"))
                self.assertEqual(subprocess.check_output(["dpkg-deb", "-f", deb, "Architecture"], text=True).strip(),
                                 "amd64" if arch == "x86_64" else "arm64")
                depends = subprocess.check_output(["dpkg-deb", "-f", deb, "Depends"], text=True).strip()
                self.assertEqual(depends, "libc6 (>= 2.31)" if libc == "gnu" else "")
                unpacked = root / "deb"
                subprocess.run(["dpkg-deb", "--extract", str(deb), str(unpacked)], check=True)
                self.assertEqual(hashlib.sha256((unpacked / "usr/bin/rustfs").read_bytes()).hexdigest(), expected_hash)
                rpm = dist / f"{stem}.rpm"
                identity = subprocess.check_output(
                    ["rpm", "-qp", "--qf", "%{NAME} %{VERSION} %{RELEASE} %{ARCH}", str(rpm)], text=True)
                self.assertEqual(identity, f"rustfs {version.replace('-', '~')} 1 {arch}")
                requires = subprocess.check_output(["rpm", "-qp", "--requires", str(rpm)], text=True)
                if libc == "gnu":
                    self.assertIn("glibc >= 2.31", requires)
                else:
                    self.assertNotIn("glibc", requires)


if __name__ == "__main__":
    unittest.main()
