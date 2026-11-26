#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Checksum refresh and the legacy deb/rpm release follow-up."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "release"))
from ci_gate import workflow_document  # noqa: E402
import refresh_release_checksums as checksums  # noqa: E402
import validation_registry as registry  # noqa: E402

WORKFLOW = ROOT / ".github" / "workflows" / "package-deb-rpm.yml"


def step_runs(job):
    return [step.get("run", "") for step in job.get("steps", [])]


class ChecksumRefreshTests(unittest.TestCase):
    def test_packages_are_appended_and_zip_lines_stay(self):
        existing = "\n".join([
            "a" * 64 + "  rustfs-linux-x86_64-gnu-latest.zip",
            "a" * 64 + "  rustfs-linux-x86_64-gnu-v1.0.7.zip",
            "b" * 64 + "  rustfs-linux-x86_64-gnu-v1.0.7.deb",
            "",
        ])
        updated = checksums.refresh(existing, [
            ("dist/rustfs-linux-x86_64-gnu-v1.0.7.rpm", "c" * 64),
            ("dist/rustfs-linux-x86_64-gnu-v1.0.7.deb", "d" * 64),
        ], 64)
        lines = updated.splitlines()
        self.assertEqual(lines[0], "a" * 64 + "  rustfs-linux-x86_64-gnu-latest.zip")
        self.assertEqual(lines[1], "a" * 64 + "  rustfs-linux-x86_64-gnu-v1.0.7.zip")
        self.assertEqual(lines[2:], [
            "d" * 64 + "  rustfs-linux-x86_64-gnu-v1.0.7.deb",
            "c" * 64 + "  rustfs-linux-x86_64-gnu-v1.0.7.rpm",
        ])
        self.assertTrue(updated.endswith("\n"))

    def test_github_tilde_asset_names_match_the_stored_release_name(self):
        self.assertEqual(
            checksums.github_asset_name("rustfs-linux-x86_64-gnu-v1.0.0~rc.5.deb"),
            "rustfs-linux-x86_64-gnu-v1.0.0.rc.5.deb")

    def test_malformed_or_empty_checksums_are_rejected(self):
        with self.assertRaises(ValueError):
            checksums.parse_checksums("not-a-checksum\n", 64)
        with self.assertRaises(ValueError):
            checksums.refresh("aa  only-one-space\n", [("pkg.deb", "d" * 64)], 64)
        with self.assertRaises(ValueError):
            checksums.refresh("", [("pkg.deb", "d" * 64)], 64)

    def test_cli_verifies_a_published_zip_and_rewrites_packages(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = root / "rustfs-linux-x86_64-musl-v1.0.7.deb"
            package.write_bytes(b"package-bytes")
            digest = checksums.file_digest(package, "sha256")
            checksum = root / "SHA256SUMS"
            checksum.write_text("e" * 64 + "  rustfs-linux-x86_64-musl-v1.0.7.zip\n", encoding="utf-8")
            script = ROOT / "scripts" / "release" / "refresh_release_checksums.py"
            verified = subprocess.run(
                [sys.executable, str(script), "verify", "--checksum", str(checksum),
                 "--name", "rustfs-linux-x86_64-musl-v1.0.7.zip", "--digest", "e" * 64,
                 "--digest-length", "64"],
                capture_output=True, text=True, check=False)
            self.assertEqual(verified.returncode, 0, verified.stderr)
            mismatch = subprocess.run(
                [sys.executable, str(script), "verify", "--checksum", str(checksum),
                 "--name", "rustfs-linux-x86_64-musl-v1.0.7.zip", "--digest", "f" * 64,
                 "--digest-length", "64"],
                capture_output=True, text=True, check=False)
            self.assertEqual(mismatch.returncode, 1)
            output = root / "next"
            rewritten = subprocess.run(
                [sys.executable, str(script), "refresh", "--checksum", str(checksum),
                 "--output", str(output), "--algorithm", "sha256", "--digest-length", "64",
                 str(package)],
                capture_output=True, text=True, check=False)
            self.assertEqual(rewritten.returncode, 0, rewritten.stderr)
            self.assertIn("e" * 64 + "  rustfs-linux-x86_64-musl-v1.0.7.zip", output.read_text(encoding="utf-8"))
            self.assertIn(digest + "  rustfs-linux-x86_64-musl-v1.0.7.deb", output.read_text(encoding="utf-8"))


class PackageWorkflowTests(unittest.TestCase):
    def test_tag_releases_attach_packages_and_refresh_checksums(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        document = workflow_document(text)
        events = document.get("on", document.get(True))
        self.assertEqual(list(events), ["workflow_run", "workflow_dispatch"])
        self.assertEqual(events["workflow_run"]["workflows"], ["Build and Release"])
        self.assertEqual(events["workflow_run"]["types"], ["completed"])
        self.assertEqual(events["workflow_dispatch"]["inputs"]["tag"]["required"], "true")
        self.assertEqual(document["permissions"], {"contents": "write", "actions": "read"})
        self.assertIn("github.repository == 'rustfs/rustfs'", document["jobs"]["resolve"]["if"])
        self.assertIn("github.event.workflow_run.event == 'push'", document["jobs"]["resolve"]["if"])
        self.assertIn("head_branch != 'main'", document["jobs"]["resolve"]["if"])
        package_runs = "\n".join(step_runs(document["jobs"]["package"]))
        self.assertIn("scripts/release/build_deb.sh", package_runs)
        self.assertIn("scripts/release/build_rpm.sh", package_runs)
        self.assertIn("fpm --version 1.17.0", package_runs)
        self.assertIn("refresh_release_checksums.py verify", package_runs)
        self.assertNotIn("--clobber", package_runs)
        self.assertIn("different digest", package_runs)
        checksum_runs = "\n".join(step_runs(document["jobs"]["checksums"]))
        self.assertIn("refresh_release_checksums.py refresh", checksum_runs)
        self.assertIn("SHA256SUMS", checksum_runs)
        self.assertIn("SHA512SUMS", checksum_runs)
        self.assertEqual(checksum_runs.count("--clobber"), 2)
        self.assertIn('"${#files[@]}" -eq 8', checksum_runs)
        self.assertEqual(
            {item["arch"] + "-" + item["libc"] for item in document["jobs"]["package"]["strategy"]["matrix"]["include"]},
            {"x86_64-gnu", "x86_64-musl", "aarch64-gnu", "aarch64-musl"})
        item = registry.workflow_entry(".github/workflows/package-deb-rpm.yml")
        self.assertEqual(item["name"], "Package DEB/RPM")
        self.assertEqual(item["events"], ["workflow_run", "workflow_dispatch"])


if __name__ == "__main__":
    unittest.main()
