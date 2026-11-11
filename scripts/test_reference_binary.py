#!/usr/bin/env python3
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Exercise the reference-binary pin resolver and unpacker without network access."""

import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github/actions/reference-binary/reference_binary.py"
PIN = '''schema = 1
repository = "rustfs/rustfs"
version = "1.0.5"
tag = "1.0.5"

[platforms.linux-x86_64-gnu]
asset = "rustfs-linux-x86_64-gnu-v1.0.5.zip"
sha256 = "{sha}"
executable = "rustfs"
'''
SHA = "a" * 64


def run(*args):
    proc = subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, check=False)
    return proc.returncode, proc.stdout, proc.stderr


class Base(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def pin(self, text):
        path = self.root / "pin.toml"
        path.write_text(text)
        return path

    def archive(self, members):
        path = self.root / "rustfs.zip"
        with zipfile.ZipFile(path, "w") as bundle:
            for name, data in members.items():
                bundle.writestr(name, data)
        return path, hashlib.sha256(path.read_bytes()).hexdigest()


class ResolveTests(Base):
    def test_missing_pin_file_fails(self):
        code, _, err = run("resolve", "--pin", str(self.root / "absent.toml"), "--platform", "linux-x86_64-gnu")
        self.assertEqual(code, 1)
        self.assertIn("reference pin file is missing", err)

    def test_unpinned_platform_fails(self):
        code, _, err = run("resolve", "--pin", str(self.pin(PIN.format(sha=SHA))), "--platform", "linux-aarch64-gnu")
        self.assertEqual(code, 1)
        self.assertIn("no pin for platform", err)

    def test_malformed_pins_fail(self):
        good = PIN.format(sha=SHA)
        for label, text in {
            "short digest": good.replace(SHA, "a" * 63),
            "uppercase digest": good.replace(SHA, "A" * 64),
            "no digest": good.replace(f'sha256 = "{SHA}"\n', ""),
            "path in asset": good.replace('asset = "rustfs-', 'asset = "../rustfs-'),
            "non-zip asset": good.replace("v1.0.5.zip", "v1.0.5.deb"),
            "asset of another version": good.replace("v1.0.5.zip", "v1.0.4.zip"),
            "path in executable": good.replace('executable = "rustfs"', 'executable = "bin/rustfs"'),
            "wrong schema": good.replace("schema = 1", "schema = 2"),
            "boolean schema": good.replace("schema = 1", "schema = true"),
            "no repository": good.replace('repository = "rustfs/rustfs"\n', ""),
            "unknown key": good + 'url = "https://example.invalid"\n',
            "not toml": "schema = \n",
        }.items():
            with self.subTest(label):
                code, _, _ = run("resolve", "--pin", str(self.pin(text)), "--platform", "linux-x86_64-gnu")
                self.assertEqual(code, 1)

    def test_resolves_identity_as_step_outputs(self):
        code, out, err = run("resolve", "--pin", str(self.pin(PIN.format(sha=SHA))), "--platform", "linux-x86_64-gnu")
        self.assertEqual(code, 0, err)
        self.assertEqual(out.splitlines(), ["repository=rustfs/rustfs", "version=1.0.5", "tag=1.0.5",
                                            "asset=rustfs-linux-x86_64-gnu-v1.0.5.zip", f"sha256={SHA}",
                                            "executable=rustfs"])

    def test_repository_pin_is_valid(self):
        code, out, err = run("resolve", "--pin", str(ROOT / ".config/reference-binary.toml"),
                             "--platform", "linux-x86_64-gnu")
        self.assertEqual(code, 0, err)
        self.assertIn("version=1.0.5", out.splitlines())


class UnpackTests(Base):
    def unpack(self, archive, sha):
        return run("unpack", "--archive", str(archive), "--sha256", sha, "--executable", "rustfs",
                   "--destination", str(self.root / "out"))

    def test_digest_mismatch_fails_and_extracts_nothing(self):
        archive, _ = self.archive({"rustfs": b"binary"})
        code, _, err = self.unpack(archive, SHA)
        self.assertEqual(code, 1)
        self.assertIn("does not match the pinned digest", err)
        self.assertFalse((self.root / "out").exists())

    def test_missing_archive_fails(self):
        code, _, err = self.unpack(self.root / "absent.zip", SHA)
        self.assertEqual(code, 1)
        self.assertIn("missing or unreadable", err)

    def test_archive_without_top_level_executable_fails(self):
        archive, sha = self.archive({"bin/rustfs": b"binary"})
        code, _, err = self.unpack(archive, sha)
        self.assertEqual(code, 1)
        self.assertIn("exactly one top-level", err)

    def test_empty_executable_fails(self):
        archive, sha = self.archive({"rustfs": b""})
        code, _, _ = self.unpack(archive, sha)
        self.assertEqual(code, 1)

    def test_non_zip_archive_fails(self):
        archive = self.root / "rustfs.zip"
        archive.write_bytes(b"not a zip")
        code, _, err = self.unpack(archive, hashlib.sha256(b"not a zip").hexdigest())
        self.assertEqual(code, 1)
        self.assertIn("not a valid zip", err)

    def test_extracts_only_the_executable(self):
        archive, sha = self.archive({"rustfs": b"binary", "../escape": b"x", "README": b"r"})
        code, out, err = self.unpack(archive, sha)
        self.assertEqual(code, 0, err)
        target = self.root / "out" / "rustfs"
        self.assertEqual(out.strip(), f"path={target}")
        self.assertEqual(target.read_bytes(), b"binary")
        self.assertTrue(target.stat().st_mode & 0o111)
        self.assertEqual(sorted(p.name for p in (self.root / "out").iterdir()), ["rustfs"])
        self.assertFalse((self.root / "escape").exists())


if __name__ == "__main__":
    unittest.main()
