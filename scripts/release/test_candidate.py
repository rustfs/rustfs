#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Exercise release identity, substitution and retry boundaries without publishing."""
from __future__ import annotations

import copy
import io
import os
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import urllib.request
import zipfile

import candidate


class TransportTests(unittest.TestCase):
    def test_signed_download_redirect_does_not_leak_the_github_token(self):
        request = urllib.request.Request("https://api.github.com/repos/rustfs/rustfs/actions/artifacts/1/zip", headers={"Authorization": "Bearer test-token"})
        redirected = candidate.ArtifactRedirect().redirect_request(request, None, 302, "Found", {}, "https://example.blob.core.windows.net/artifact.zip?signature=fixture")
        self.assertIsNone(redirected.get_header("Authorization"))
        same_host = candidate.ArtifactRedirect().redirect_request(request, None, 302, "Found", {}, "https://api.github.com/another-path")
        self.assertEqual(same_host.get_header("Authorization"), "Bearer test-token")
        with self.assertRaises(ValueError):
            candidate.ArtifactRedirect().redirect_request(request, None, 302, "Found", {}, "http://example.invalid/artifact.zip")

    def test_download_checks_real_bytes_and_rejects_traversal(self):
        import hashlib
        run = {"id": 70, "head_sha": "a" * 40}
        def blob(name):
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, "w") as package:
                package.writestr(name, b"accepted")
            return stream.getvalue()
        for name, valid in (("record.json", True), ("../escape", False)):
            content = blob(name)
            artifact = {"id": 90, "name": "candidate", "expired": False, "workflow_run": run,
                        "size_in_bytes": len(content), "digest": "sha256:" + hashlib.sha256(content).hexdigest()}
            with tempfile.TemporaryDirectory() as temp, patch.object(candidate, "artifact_list", return_value=[artifact]), patch.object(candidate, "github_stream", side_effect=lambda _: io.BytesIO(content)):
                if valid:
                    candidate.download_artifact(run, "candidate", temp)
                    self.assertEqual((Path(temp) / name).read_bytes(), b"accepted")
                    artifact["digest"] = "sha256:" + "0" * 64
                    with self.assertRaisesRegex(ValueError, "digest"):
                        candidate.download_artifact(run, "candidate", Path(temp) / "changed")
                else:
                    with self.assertRaisesRegex(ValueError, "unsafe"):
                        candidate.download_artifact(run, "candidate", temp)

    def test_duplicate_json_fields_are_not_silently_accepted(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.json"
            path.write_text('{"schema": 1, "schema": 2}')
            with self.assertRaisesRegex(ValueError, "duplicate"):
                candidate.read_json(path)


class FreezeRepositoryTests(unittest.TestCase):
    def test_freeze_reads_real_commit_and_rejects_recipe_or_version_drift(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in candidate.RECIPE_PATHS:
                path = root / name; path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("recipe\n" if name != ".config/release-validation-plan.json" else "{}\n")
            for directory in ("bootstrap", "helm"):
                (root / directory).mkdir()
                (root / directory / "input.txt").write_text("runtime input\n")
            (root / "Cargo.toml").write_text('[workspace.package]\nversion = "1.2.3"\n')
            (root / "Cargo.lock").write_text("version = 4\n")
            env = dict(os.environ, GIT_AUTHOR_NAME="Fixture", GIT_AUTHOR_EMAIL="fixture@example.invalid", GIT_COMMITTER_NAME="Fixture", GIT_COMMITTER_EMAIL="fixture@example.invalid")
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, env=env, stderr=subprocess.DEVNULL, text=True).strip()
            git("init", "-q"); git("add", "."); git("commit", "-qm", "fixture")
            original = git("rev-parse", "HEAD")
            value = inputs()
            console = {"draft": False, "prerelease": False, "tag_name": value["console"]["tag"],
                       "assets": [{"name": value["console"]["name"], "browser_download_url": value["console"]["url"], "digest": "sha256:" + value["console"]["sha256"]}]}
            cli = {"draft": False, "prerelease": False, "tag_name": "v0.1.0", "assets": []}
            for arch in ("amd64", "arm64"):
                for abi in ("", "-gnu"):
                    name = f"rustfs-cli-linux-{arch}{abi}-v0.1.0.tar.gz"
                    cli["assets"].append({"name": name, "browser_download_url": f"https://github.com/rustfs/cli/releases/download/v0.1.0/{name}", "digest": "sha256:" + "f" * 64})
            previous = {"draft": False, "prerelease": False, "tag_name": "1.2.2", "assets": [{"name": value["previous"]["name"], "browser_download_url": value["previous"]["url"], "digest": "sha256:" + value["previous"]["sha256"]}]}
            def freeze(source, workflow, version="1.2.3"):
                return candidate.freeze(version, source, workflow, "1.92.0", console, 70, 7, cli, previous, {"run_id": 60, "run_attempt": 1}, root)
            frozen = freeze(original, original)
            (root / "unrelated.txt").write_text("main may advance\n")
            git("add", "."); git("commit", "-qm", "unrelated")
            advanced = git("rev-parse", "HEAD")
            self.assertEqual(freeze(original, advanced)["recipe_sha256"], frozen["recipe_sha256"])
            (root / ".github/workflows/build.yml").write_text("different recipe\n")
            git("add", "."); git("commit", "-qm", "recipe change")
            with self.assertRaisesRegex(ValueError, "recipe"):
                freeze(original, git("rev-parse", "HEAD"))
            with self.assertRaisesRegex(ValueError, "version"):
                freeze(original, original, "1.2.4")


def inputs():
    value = {
        "schema": 1, "candidate_id": "1.2.3-preview.7", "version": "1.2.3", "ci": {"run_id": 60, "run_attempt": 1},
        "source_sha": "a" * 40, "workflow_sha": "b" * 40, "recipe_sha256": "c" * 64,
        "lock_sha256": "d" * 64, "plan_sha256": "e" * 64, "toolchain": "1.92.0",
        "console": {"tag": "v0.1.41", "name": "rustfs-console-v0.1.41.zip",
                    "url": "https://github.com/rustfs/console/releases/download/v0.1.41/rustfs-console-v0.1.41.zip",
                    "sha256": "f" * 64}, "origin_run_id": 70, "origin_run_number": 7,
    }
    def asset(repo, version, name):
        return {"version": version, "name": name, "url": f"https://github.com/{repo}/releases/download/{version}/{name}", "sha256": "f" * 64}
    value["previous"] = asset("rustfs/rustfs", "1.2.2", "rustfs-linux-x86_64-gnu-v1.2.2.zip")
    value["cli"] = {target: asset("rustfs/cli", "v0.1.0", "rustfs-cli.tar.gz") for target in candidate.LINUX}
    return value


def archive(directory, target, value=None, *, extra=None, binary=b"exact candidate bytes"):
    value = value or inputs()
    path = Path(directory) / f"rustfs-{target}-v{value['version']}.zip"
    members = {"rustfs.exe" if target == "windows-x86_64" else "rustfs": binary}
    if target.endswith("-gnu"):
        members.update({"rustfs.cpu-symbol-catalog.json": b"{}", "rustfs.cpu-symbol-catalog.sha256": b"catalog"})
    if extra:
        members.update(extra)
    with zipfile.ZipFile(path, "w") as output:
        for name, content in members.items():
            output.writestr(name, content)
    return path


class InputIdentityTests(unittest.TestCase):
    def test_valid_frozen_candidate(self):
        self.assertEqual(candidate.validate_inputs(inputs()), inputs())

    def test_moving_or_injected_inputs_rejected(self):
        changes = [("source_sha", "main"), ("version", "1.2.3\nTAG=bad"), ("version", "01.2.3"),
                   ("candidate_id", "1.2.3-preview.8"), ("toolchain", "stable"), ("origin_run_id", True),
                   ("schema", True), ("recipe_sha256", "c" * 63)]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                record = inputs(); record[key] = value
                with self.assertRaises(ValueError):
                    candidate.validate_inputs(record)

    def test_console_asset_cannot_redirect_or_be_ambiguous(self):
        record = inputs()
        record["console"]["url"] = "https://example.invalid/console.zip"
        with self.assertRaises(ValueError):
            candidate.validate_inputs(record)
        asset = {"name": inputs()["console"]["name"], "browser_download_url": inputs()["console"]["url"], "digest": "sha256:" + "f" * 64}
        for release in ({"draft": True, "prerelease": False, "tag_name": "v0.1.41", "assets": [asset]},
                        {"draft": False, "prerelease": False, "tag_name": "v0.1.41", "assets": [asset, asset]}):
            with self.assertRaises(ValueError):
                candidate.console_asset(release)

    def test_frozen_record_is_retryable_but_not_replaceable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inputs.json"
            candidate.write_json(path, inputs())
            before = path.read_bytes()
            candidate.write_json(path, inputs())
            self.assertEqual(before, path.read_bytes())
            changed = inputs(); changed["source_sha"] = "f" * 40
            with self.assertRaisesRegex(ValueError, "replace"):
                candidate.write_json(path, changed)
            self.assertEqual(before, path.read_bytes())

    def test_platform_selection_keeps_desktop_out_of_linux(self):
        self.assertEqual(candidate.targets("linux"), candidate.LINUX)
        self.assertEqual(candidate.targets("desktop"), candidate.DESKTOP)
        for value in ("", "linux-x86_64-gnu,linux-x86_64-gnu", "linux,windows", "all,linux"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                candidate.targets(value)


class ArtifactIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inputs = inputs()
        self.records = [candidate.archive_record(archive(self.root, target), target, self.inputs) for target in candidate.TARGETS]

    def test_all_six_final_artifacts_bind_exact_bytes(self):
        value = candidate.certification_candidate(self.inputs, self.records, self.root)
        self.assertEqual(len(value["deliverables"]), 6)
        self.assertEqual(value["version"], "1.2.3")
        self.assertEqual(value["release_profile"], "stable")
        self.assertEqual(value["validation_batch"], {"run_id": "1.2.3-preview.7", "run_attempt": 1})
        self.assertNotIn("latest", json.dumps(value))

    def test_partial_linux_bundle_cannot_be_published_as_complete(self):
        linux = self.records[:4]
        self.assertEqual(len(candidate.certification_candidate(self.inputs, linux, self.root, candidate.LINUX)["deliverables"]), 4)
        with self.assertRaisesRegex(ValueError, "missing"):
            candidate.certification_candidate(self.inputs, linux, self.root)

    def test_same_version_different_bytes_rejected(self):
        archive(self.root, candidate.TARGETS[0], binary=b"replacement")
        with self.assertRaisesRegex(ValueError, "changed"):
            candidate.verify_records(self.inputs, self.records, self.root, candidate.TARGETS)

    def test_different_inputs_cannot_share_receipts(self):
        changed = copy.deepcopy(self.inputs); changed["console"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "changed"):
            candidate.verify_records(changed, self.records, self.root, candidate.TARGETS)

    def test_duplicate_or_mislabeled_platform_rejected(self):
        for replacement in (self.records[:-1] + [self.records[0]], self.records + [self.records[0]]):
            with self.assertRaises(ValueError):
                candidate.verify_records(self.inputs, replacement, self.root, candidate.TARGETS)
        wrong = copy.deepcopy(self.records); wrong[0]["name"] = "../replacement.zip"
        with self.assertRaises(ValueError):
            candidate.verify_records(self.inputs, wrong, self.root, candidate.TARGETS)

    def test_archive_members_are_strict(self):
        for name in ("../escape", "rustfs-cli", "nested/rustfs"):
            with self.subTest(name=name):
                path = archive(self.root, candidate.TARGETS[0], extra={name: b"extra"})
                with self.assertRaises(ValueError):
                    candidate.archive_record(path, candidate.TARGETS[0], self.inputs)

    def test_binary_symlink_rejected(self):
        path = self.root / f"rustfs-{candidate.TARGETS[0]}-v1.2.3.zip"
        with zipfile.ZipFile(path, "w") as output:
            entry = zipfile.ZipInfo("rustfs"); entry.external_attr = 0o120777 << 16
            output.writestr(entry, "../../untrusted")
        with self.assertRaises(ValueError):
            candidate.archive_record(path, candidate.TARGETS[0], self.inputs)

    def test_real_cli_fails_without_overwriting_existing_output(self):
        candidate.write_json(self.root / "inputs.json", self.inputs)
        for item in self.records:
            candidate.write_json(self.root / f"record-{item['target']}.json", item)
        output = self.root / "candidate.json"
        command = [sys.executable, str(Path(candidate.__file__)), "assemble", "--inputs", str(self.root / "inputs.json"),
                   "--directory", str(self.root), "--output", str(output)]
        good = subprocess.run(command, text=True, capture_output=True)
        self.assertEqual(good.returncode, 0, good.stderr)
        original = output.read_bytes()
        archive(self.root, candidate.TARGETS[1], binary=b"bad retry")
        bad = subprocess.run(command, text=True, capture_output=True)
        self.assertNotEqual(bad.returncode, 0)
        self.assertEqual(output.read_bytes(), original)


class ProvenanceTests(unittest.TestCase):
    def run_record(self):
        return {"repository": {"full_name": "rustfs/rustfs"}, "path": ".github/workflows/release-candidate.yml",
                "event": "workflow_dispatch", "head_branch": "main", "id": 70, "run_attempt": 1,
                "head_sha": "b" * 40, "status": "completed", "conclusion": "success"}

    def test_trusted_source_and_completion_required(self):
        run = self.run_record()
        self.assertEqual(candidate.validate_run(run, "release-candidate.yml"), run)
        for field, value in (("head_branch", "unreviewed"), ("event", "pull_request"), ("conclusion", "failure"),
                             ("status", "in_progress"), ("repository", {"full_name": "fork/rustfs"}),
                             ("path", ".github/workflows/other.yml")):
            with self.subTest(field=field):
                changed = {**run, field: value}
                with self.assertRaises(ValueError):
                    candidate.validate_run(changed, "release-candidate.yml")

    def test_artifact_must_match_exact_origin(self):
        run = self.run_record()
        artifact = {"name": "candidate", "expired": False, "id": 1, "size_in_bytes": 100,
                    "digest": "sha256:" + "c" * 64, "workflow_run": {"id": 70, "head_sha": "b" * 40}}
        candidate.validate_artifact(artifact, run, "candidate")
        for field, value in (("expired", True), ("name", "other"), ("digest", ""), ("size_in_bytes", True),
                             ("workflow_run", {"id": 71, "head_sha": "b" * 40})):
            with self.subTest(field=field), self.assertRaises(ValueError):
                candidate.validate_artifact({**artifact, field: value}, run, "candidate")


if __name__ == "__main__":
    unittest.main()
