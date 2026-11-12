#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for manual origin/main packaging."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "release" / "package_manual_artifacts.sh"
WORKFLOW = ROOT / ".github" / "workflows" / "manual-main-packages.yml"
sys.path.insert(0, str(ROOT / "scripts"))
from ci_gate import workflow_document  # noqa: E402
import validation_registry as registry  # noqa: E402


def run_script(*args, check=False):
    return subprocess.run(["bash", str(SCRIPT), *args], cwd=ROOT, text=True,
                          capture_output=True, timeout=60, check=check)


def trigger(text):
    match = re.search(r"^on:\n(.*?)(?=^[^ #\n]|\Z)", text, re.M | re.S)
    if match is None:
        raise AssertionError("workflow has no event declaration")
    return re.findall(r"^  (\w+):", match.group(1), re.M)


class VersionAndMatrixTests(unittest.TestCase):
    def test_release_and_prerelease_names_come_from_package_versions(self):
        cases = {
            ("1.2.3", "x86_64", "gnu"): """\
build_type=release
deb_version=1.2.3
rpm_version=1.2.3
rpm_release=1
deb_file=rustfs-linux-x86_64-gnu-v1.2.3.deb
rpm_file=rustfs-linux-x86_64-gnu-v1.2.3.rpm
zip_file=rustfs-linux-x86_64-gnu-v1.2.3.zip
""",
            ("1.2.3-alpha.1", "aarch64", "gnu"): """\
build_type=prerelease
deb_version=1.2.3~alpha.1
rpm_version=1.2.3~alpha.1
rpm_release=1
deb_file=rustfs-linux-aarch64-gnu-v1.2.3-alpha.1.deb
rpm_file=rustfs-linux-aarch64-gnu-v1.2.3-alpha.1.rpm
zip_file=rustfs-linux-aarch64-gnu-v1.2.3-alpha.1.zip
""",
            ("1.2.3-beta.2", "x86_64", "musl"): """\
build_type=prerelease
deb_version=1.2.3~beta.2
rpm_version=1.2.3~beta.2
rpm_release=1
deb_file=rustfs-linux-x86_64-musl-v1.2.3-beta.2.deb
rpm_file=rustfs-linux-x86_64-musl-v1.2.3-beta.2.rpm
zip_file=rustfs-linux-x86_64-musl-v1.2.3-beta.2.zip
""",
            ("1.2.3-rc.4", "aarch64", "musl"): """\
build_type=prerelease
deb_version=1.2.3~rc.4
rpm_version=1.2.3~rc.4
rpm_release=1
deb_file=rustfs-linux-aarch64-musl-v1.2.3-rc.4.deb
rpm_file=rustfs-linux-aarch64-musl-v1.2.3-rc.4.rpm
zip_file=rustfs-linux-aarch64-musl-v1.2.3-rc.4.zip
""",
        }
        for (version, arch, libc), expected in cases.items():
            with self.subTest(version=version, arch=arch, libc=libc):
                result = run_script("validate", "--version", version, "--arch", arch, "--libc", libc)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected)

    def test_invalid_versions_fail_without_stdout(self):
        for version in ("", "v1.2.3", "1.2", "01.2.3", "1.2.3-preview.1", "1.2.3-alpha",
                        "1.2.3-rc.01", "1.2.3-rc.1-extra", "1.2.3 ", "1.2.3$(id)", "dev-abcdef"):
            with self.subTest(version=version):
                result = run_script("validate", "--version", version, "--arch", "x86_64", "--libc", "gnu")
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("invalid package version", result.stderr)
                self.assertIn("not a release:", result.stderr)
                self.assertIn("not a prerelease:", result.stderr)

    def test_invalid_version_does_not_create_packages(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "rustfs"
            binary.write_bytes(b"not packaged")
            output = root / "dist"
            result = run_script("package", "--version", "v1.2.3", "--arch", "x86_64", "--libc", "gnu",
                                "--binary", str(binary), "--output", str(output))
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertFalse(output.exists())

    def test_catalog_members_must_be_the_official_pair(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            binary = root / "rustfs"
            binary.write_bytes(b"not packaged")
            catalog = root / "rustfs.cpu-symbol-catalog.json"
            catalog.write_text("{}\n", encoding="utf-8")
            result = run_script("package", "--version", "1.2.3", "--arch", "x86_64", "--libc", "gnu",
                                "--binary", str(binary), "--output", str(root / "dist"),
                                "--zip-extra", str(catalog))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("packaged together", result.stderr)
            self.assertFalse((root / "dist").exists())
            other = root / "notes.txt"
            other.write_text("nope\n", encoding="utf-8")
            rejected = run_script("package", "--version", "1.2.3", "--arch", "x86_64", "--libc", "musl",
                                  "--binary", str(binary), "--output", str(root / "dist"),
                                  "--zip-extra", str(other))
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("unsupported zip member", rejected.stderr)

    def test_architecture_matrix_matches_official_linux_rows(self):
        source = (ROOT / ".github" / "workflows" / "build.yml").read_text(encoding="utf-8")
        official = json.loads(re.search(r"all='(\{.*?\})'", source, re.S).group(1))["include"]
        linux = [row for row in official if row["platform"] == "linux"]

        def derived(row):
            arch = "aarch64" if row["target"].startswith("aarch64-") else "x86_64"
            libc = row["target_id"].rsplit("-", 1)[1]
            return {**row, "arch": arch, "libc": libc, "build_os": f"linux-{arch}"}

        expected = {
            "all": [derived(row) for row in linux],
            "x86": [derived(row) for row in linux if row["target"].startswith("x86_64-")],
            "arm": [derived(row) for row in linux if row["target"].startswith("aarch64-")],
        }
        self.assertEqual([row["target_id"] for row in expected["x86"]],
                         ["linux-x86_64-musl", "linux-x86_64-gnu"])
        self.assertEqual([row["target_id"] for row in expected["arm"]],
                         ["linux-aarch64-musl", "linux-aarch64-gnu"])
        for choice, rows in expected.items():
            with self.subTest(choice=choice):
                result = run_script("matrix", choice, check=True)
                self.assertEqual(json.loads(result.stdout), {"include": rows})
        for choice in ("", "ALL", "linux", "x86_64", "x86,arm"):
            with self.subTest(choice=choice):
                result = run_script("matrix", choice)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("unsupported architecture", result.stderr)


class WorkflowContractTests(unittest.TestCase):
    def test_dispatch_builds_origin_main_and_only_uploads_artifacts(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(trigger(text), ["workflow_dispatch"])
        document = workflow_document(text)
        events = document.get("on", document.get(True))
        self.assertEqual(set(events), {"workflow_dispatch"})
        inputs = events["workflow_dispatch"]["inputs"]
        self.assertEqual(inputs["architecture"]["type"], "choice")
        self.assertEqual(inputs["architecture"]["options"], ["x86", "arm", "all"])
        self.assertTrue(inputs["architecture"]["required"])
        self.assertEqual(inputs["version"]["type"], "string")
        self.assertTrue(inputs["version"]["required"])
        self.assertEqual(document["permissions"], {"contents": "read"})
        self.assertEqual(re.findall(r"^\s+ref:\s*(.+)$", text, re.M),
                         ["main", "${{ needs.prepare.outputs.source_sha }}"])
        self.assertIn("refs/heads/main", text)
        self.assertNotIn("github.sha", text)
        self.assertIn("package_manual_artifacts.sh validate", text)
        self.assertIn("package_manual_artifacts.sh matrix", text)
        self.assertIn("package_manual_artifacts.sh package", text)
        self.assertIn("RUSTFS_BUILD_VERSION: ${{ needs.prepare.outputs.version }}", text)
        self.assertIn("cargo zigbuild --release --locked", text)
        self.assertIn("cargo build --release --locked", text)
        self.assertIn("scripts/release/check_glibc.py", text)
        self.assertIn('cache-save-if: \'false\'', text)
        self.assertIn("actions/upload-artifact@", text)
        self.assertIn("dist/*", text)
        for forbidden in ("contents: write", "aws s3", "gh release", "softprops/action-gh-release",
                          "docker push", "R2_ACCESS_KEY_ID", "workflow_call:"):
            self.assertNotIn(forbidden, text)
        self.assertEqual(text.count("persist-credentials: false"), text.count("actions/checkout@"))
        item = registry.workflow_entry(".github/workflows/manual-main-packages.yml")
        self.assertEqual(item["name"], "Manual main packages")
        self.assertEqual(item["events"], ["workflow_dispatch"])
        self.assertEqual(item["schedule"], [])
        self.assertEqual(item["monitor"], "none")
        self.assertNotEqual(item["origin"], "baseline")

    def test_registry_accepts_the_added_workflow(self):
        self.assertTrue(registry.check_wiring(registry.load_registry()))


class PackageToolTests(unittest.TestCase):
    def test_zip_deb_and_rpm_wrap_the_same_binary_when_tools_exist(self):
        required = ("dpkg-deb", "fakeroot", "rpm", "rpm2cpio", "cpio", "fpm", "zip", "unzip")
        missing = [tool for tool in required if shutil.which(tool) is None]
        if missing or os.name != "posix":
            self.skipTest("packaging tools not installed: " + ", ".join(missing))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            payload = b"manual main package fixture\n"
            binary = root / "rustfs"
            binary.write_bytes(payload)
            result = run_script("package", "--version", "9.9.9-rc.8", "--arch", "x86_64", "--libc", "gnu",
                                "--binary", str(binary), "--output", str(root / "dist"))
            self.assertEqual(result.returncode, 0, result.stderr)
            dist = root / "dist"
            self.assertEqual(sorted(path.name for path in dist.iterdir()), [
                "rustfs-linux-x86_64-gnu-v9.9.9-rc.8.deb",
                "rustfs-linux-x86_64-gnu-v9.9.9-rc.8.rpm",
                "rustfs-linux-x86_64-gnu-v9.9.9-rc.8.zip",
            ])
            self.assertIn("deb_version=9.9.9~rc.8", result.stdout)
            self.assertIn("zip_file=rustfs-linux-x86_64-gnu-v9.9.9-rc.8.zip", result.stdout)


if __name__ == "__main__":
    unittest.main()
