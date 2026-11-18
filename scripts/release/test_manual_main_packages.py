#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for manual Linux packaging from origin/main or a git ref."""
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


def run_script(*args, cwd=None, check=False):
    return subprocess.run(["bash", str(SCRIPT), *args], cwd=cwd or ROOT, text=True,
                          capture_output=True, timeout=60, check=check)


def run_git(cwd, *args, check=True):
    result = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True, check=False)
    if check and result.returncode != 0:
        raise AssertionError(f"git {' '.join(args)} failed:\n{result.stderr}")
    return result


def git_head(cwd):
    return run_git(cwd, "rev-parse", "HEAD").stdout.strip()


def output_map(stdout):
    data = {}
    for line in stdout.splitlines():
        key, value = line.split("=", 1)
        data[key] = value
    return data


def init_packaging_remote(root, *, clone_head):
    """Build a bare origin and a clone that has not fetched its tags.

    The tagged base commit has no packaging script. The main tip does, which
    is the recipe used when a historical tag is packaged. ``clone_head`` is
    ``stale`` when the clone must still be on the base commit while origin/main
    has moved, or ``main`` when the clone is already at that tip.
    """
    origin = root / "origin.git"
    seed = root / "seed"
    run_git(root, "init", "--bare", "-b", "main", str(origin))
    run_git(root, "init", "-b", "main", str(seed))
    run_git(seed, "config", "user.email", "packaging-tests@example.com")
    run_git(seed, "config", "user.name", "Packaging Tests")
    run_git(seed, "config", "commit.gpgsign", "false")
    run_git(seed, "config", "tag.gpgSign", "false")
    (seed / "README").write_text("base\n", encoding="utf-8")
    run_git(seed, "add", "README")
    run_git(seed, "commit", "-m", "base")
    base = git_head(seed)
    run_git(seed, "remote", "add", "origin", str(origin))
    run_git(seed, "push", "origin", "main")
    work = root / "work"
    if clone_head == "stale":
        run_git(root, "clone", "--no-tags", str(origin), str(work))
    script = seed / "scripts" / "release" / "package_manual_artifacts.sh"
    script.parent.mkdir(parents=True)
    script.write_text("# packaging recipe without confirmation\npackage)\n  ;;\n", encoding="utf-8")
    run_git(seed, "add", "scripts/release/package_manual_artifacts.sh")
    run_git(seed, "commit", "-m", "add packaging script")
    legacy = git_head(seed)
    script.write_text("# packaging recipe\nconfirm-source)\n  ;;\n", encoding="utf-8")
    run_git(seed, "add", "scripts/release/package_manual_artifacts.sh")
    run_git(seed, "commit", "-m", "add confirm-source")
    main = git_head(seed)
    run_git(seed, "checkout", "--orphan", "side")
    run_git(seed, "rm", "-rf", ".")
    (seed / "SIDE").write_text("side\n", encoding="utf-8")
    run_git(seed, "add", "SIDE")
    run_git(seed, "commit", "-m", "side")
    side = git_head(seed)
    run_git(seed, "checkout", "main")
    run_git(seed, "tag", "-a", "v1.2.3", "-m", "annotated", base)
    run_git(seed, "tag", "1.2.3-beta.2", base)
    run_git(seed, "tag", "v1.2.3-rc.1", base)
    run_git(seed, "tag", "v8.8.8", legacy)
    run_git(seed, "tag", "v9.9.9", main)
    run_git(seed, "tag", "v2.0.0", side)
    run_git(seed, "tag", "not-a-version", base)
    run_git(seed, "tag", "vv1.2.3", base)
    run_git(seed, "branch", "topic", base)
    run_git(seed, "branch", "release/1.2", base)
    # Same name as the annotated tag, pointing at a different commit.
    run_git(seed, "branch", "v1.2.3", main)
    run_git(seed, "push", "origin", "main")
    run_git(seed, "push", "origin", "refs/heads/topic:refs/heads/topic")
    run_git(seed, "push", "origin", "refs/heads/release/1.2:refs/heads/release/1.2")
    run_git(seed, "push", "origin", "refs/heads/v1.2.3:refs/heads/v1.2.3")
    for tag in ("v1.2.3", "1.2.3-beta.2", "v1.2.3-rc.1", "v8.8.8", "v9.9.9", "v2.0.0", "not-a-version", "vv1.2.3"):
        run_git(seed, "push", "origin", f"refs/tags/{tag}:refs/tags/{tag}")
    run_git(origin, "config", "uploadpack.allowReachableSHA1InWant", "true")
    if clone_head == "main":
        run_git(root, "clone", "--no-tags", str(origin), str(work))
    elif clone_head != "stale":
        raise AssertionError(f"unsupported clone_head {clone_head}")
    return {"work": work, "origin": origin, "seed": seed, "base": base, "legacy": legacy, "main": main, "side": side}


def confirm_step_script():
    document = workflow_document(WORKFLOW.read_text(encoding="utf-8"))
    steps = document["jobs"]["package"]["steps"]
    step = next(item for item in steps if item["name"] == "Confirm the pinned source")
    return step["run"]


def run_confirm_step(cwd, kind, name, sha):
    env = os.environ.copy()
    env.update({"SOURCE_KIND": kind, "SOURCE_NAME": name, "SOURCE_SHA": sha})
    return subprocess.run(["bash", "-c", confirm_step_script()], cwd=cwd, env=env, text=True,
                          capture_output=True, timeout=60)


def install_confirm_helper(root):
    destination = root / "packaging-tools" / "scripts" / "release" / "package_manual_artifacts.sh"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(SCRIPT.read_bytes())
    destination.chmod(0o755)


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
    def test_dispatch_packages_main_or_a_tag_and_only_uploads_artifacts(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertEqual(trigger(text), ["workflow_dispatch"])
        document = workflow_document(text)
        events = document.get("on", document.get(True))
        self.assertEqual(set(events), {"workflow_dispatch"})
        inputs = events["workflow_dispatch"]["inputs"]
        self.assertEqual(set(inputs), {"architecture", "ref", "version"})
        self.assertEqual(inputs["architecture"]["type"], "choice")
        self.assertEqual(inputs["architecture"]["options"], ["x86", "arm", "all"])
        self.assertEqual(inputs["architecture"]["required"], "true")
        self.assertEqual(inputs["ref"]["type"], "string")
        self.assertEqual(inputs["ref"]["required"], "false")
        self.assertEqual(inputs["ref"]["default"], "")
        self.assertIn("origin/main", inputs["ref"]["description"])
        self.assertIn("tag", inputs["ref"]["description"].lower())
        self.assertEqual(inputs["version"]["type"], "string")
        # Empty is allowed so a tag can supply the version. The resolver
        # still rejects an empty version when the ref input is empty.
        # workflow_document keeps YAML booleans as strings.
        self.assertEqual(inputs["version"]["required"], "false")
        self.assertEqual(inputs["version"]["default"], "")
        self.assertIn("No leading v", inputs["version"]["description"])
        self.assertIn("Required for origin/main", inputs["version"]["description"])
        self.assertEqual(document["permissions"], {"contents": "read"})
        self.assertEqual(re.findall(r"^[ \t]+ref:[ \t]+(.+)$", text, re.M), [
            "main",
            "${{ needs.prepare.outputs.source_sha }}",
            "${{ needs.prepare.outputs.tool_sha }}",
        ])
        self.assertNotIn("ref: ${{ inputs.ref }}", text)
        self.assertNotIn("github.sha", text)
        self.assertIn("refs/heads/main", SCRIPT.read_text(encoding="utf-8"))
        self.assertIn("package_manual_artifacts.sh resolve-source", text)
        confirm_run = confirm_step_script()
        self.assertIn("packaging-tools/scripts/release/package_manual_artifacts.sh", confirm_run)
        self.assertIn("confirm-source)", confirm_run)
        self.assertIn('bash "$helper" confirm-source', confirm_run)
        self.assertIn("needs.prepare.outputs.confirm_from == 'tool'", text)
        self.assertIn("package_manual_artifacts.sh matrix", text)
        self.assertIn('bash "$script" package', text)
        self.assertIn('SOURCE_REF: ${{ inputs.ref }}', text)
        self.assertIn('PACKAGE_VERSION: ${{ inputs.version }}', text)
        self.assertIn("RUSTFS_BUILD_VERSION: ${{ needs.prepare.outputs.version }}", text)
        self.assertIn("cargo zigbuild --release --locked", text)
        self.assertIn("cargo build --release --locked", text)
        self.assertIn("scripts/release/check_glibc.py", text)
        self.assertIn('cache-save-if: \'false\'', text)
        self.assertIn("actions/upload-artifact@", text)
        self.assertIn("dist/*", text)
        self.assertIn("retention-days: 30", text)
        self.assertIn("compression-level: 0", text)
        self.assertIn("sm-standard-4", text)
        self.assertIn("ubicloud-standard-16-arm", text)
        names = re.findall(r"^      - name: (.+)$", text, re.M)
        self.assertLess(names.index("Checkout the pinned source commit"),
                        names.index("Checkout packaging recipe"))
        self.assertLess(names.index("Checkout packaging recipe"),
                        names.index("Confirm the pinned source"))
        for forbidden in ("contents: write", "aws s3", "gh release", "softprops/action-gh-release",
                          "docker push", "R2_ACCESS_KEY_ID", "workflow_call:"):
            self.assertNotIn(forbidden, text)
        self.assertEqual(text.count("persist-credentials: false"), text.count("actions/checkout@"))
        item = registry.workflow_entry(".github/workflows/manual-main-packages.yml")
        self.assertEqual(item["name"], "Manual Linux packages")
        self.assertEqual(item["events"], ["workflow_dispatch"])
        self.assertEqual(item["schedule"], [])
        self.assertEqual(item["monitor"], "none")
        self.assertNotEqual(item["origin"], "baseline")
        self.assertIn("git tag", item["disposition"])
        self.assertIn("origin/main", item["disposition"])
        self.assertIn("No GitHub Release", item["disposition"])

    def test_registry_accepts_the_added_workflow(self):
        self.assertTrue(registry.check_wiring(registry.load_registry()))


class SourceResolutionTests(unittest.TestCase):
    def resolve(self, repo, source, version):
        return run_script("resolve-source", "--source", source, "--version", version, cwd=repo["work"])

    def confirm(self, repo, kind, name, sha):
        return run_script("confirm-source", "--kind", kind, "--name", name, "--sha", sha, cwd=repo["work"])

    def test_empty_ref_packages_the_fetched_main_tip(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="stale")
            self.assertEqual(git_head(repo["work"]), repo["base"])
            result = self.resolve(repo, "", "1.2.3")
            self.assertEqual(result.returncode, 0, result.stderr)
            data = output_map(result.stdout)
            self.assertEqual(data, {
                "source_sha": repo["main"],
                "source_kind": "main",
                "source_name": "main",
                "version": "1.2.3",
                "packaging": "source",
                "tool_sha": repo["base"],
                "confirm_from": "source",
            })
            self.assertEqual(git_head(repo["work"]), repo["main"])
            self.assertEqual(self.confirm(repo, "main", "main", repo["main"]).returncode, 0)

    def test_main_requires_a_version_and_rejects_a_leading_v(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="stale")
            missing = self.resolve(repo, "", "")
            self.assertNotEqual(missing.returncode, 0)
            self.assertEqual(missing.stdout, "")
            self.assertIn("version is required when packaging the tip of origin/main", missing.stderr)
            self.assertEqual(git_head(repo["work"]), repo["base"])
            leading = self.resolve(repo, "", "v1.2.3")
            self.assertNotEqual(leading.returncode, 0)
            self.assertEqual(leading.stdout, "")
            self.assertIn("invalid package version: v1.2.3", leading.stderr)
            self.assertNotIn("derived from tag", leading.stderr)
            self.assertEqual(git_head(repo["work"]), repo["base"])

    def test_annotated_tag_is_checked_out_by_peeled_commit_and_derives_version(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            self.assertEqual(run_git(repo["work"], "tag", "--list").stdout, "")
            before = git_head(repo["work"])
            result = self.resolve(repo, "v1.2.3", "")
            self.assertEqual(result.returncode, 0, result.stderr)
            data = output_map(result.stdout)
            tag_object = run_git(repo["work"], "rev-parse", "refs/tags/v1.2.3").stdout.strip()
            self.assertNotEqual(tag_object, repo["base"])
            self.assertEqual(data["source_sha"], repo["base"])
            self.assertEqual(data["source_kind"], "tag")
            self.assertEqual(data["source_name"], "v1.2.3")
            self.assertEqual(data["version"], "1.2.3")
            self.assertEqual(data["packaging"], "tool")
            self.assertEqual(data["confirm_from"], "tool")
            self.assertEqual(data["tool_sha"], repo["main"])
            self.assertEqual(git_head(repo["work"]), before)
            run_git(repo["work"], "checkout", "--detach", repo["base"])
            self.assertEqual(self.confirm(repo, "tag", "v1.2.3", repo["base"]).returncode, 0)

    def test_prefixed_tag_prerelease_and_untagged_history_resolve(self):
        cases = (
            ("refs/tags/v1.2.3", "", "v1.2.3", "1.2.3", "base"),
            ("v1.2.3-rc.1", "", "v1.2.3-rc.1", "1.2.3-rc.1", "base"),
            ("1.2.3-beta.2", "", "1.2.3-beta.2", "1.2.3-beta.2", "base"),
            ("v2.0.0", "", "v2.0.0", "2.0.0", "side"),
            ("v9.9.9", "", "v9.9.9", "9.9.9", "main"),
        )
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            for source, version, name, derived, commit in cases:
                with self.subTest(source=source):
                    result = self.resolve(repo, source, version)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    data = output_map(result.stdout)
                    self.assertEqual(data["source_sha"], repo[commit])
                    self.assertEqual(data["source_kind"], "tag")
                    self.assertEqual(data["source_name"], name)
                    self.assertEqual(data["version"], derived)
                    self.assertEqual(git_head(repo["work"]), repo["main"])
            side = self.resolve(repo, "v2.0.0", "")
            self.assertEqual(output_map(side.stdout)["packaging"], "tool")
            self.assertEqual(output_map(side.stdout)["confirm_from"], "tool")
            current = self.resolve(repo, "v9.9.9", "")
            self.assertEqual(output_map(current.stdout)["packaging"], "source")
            self.assertEqual(output_map(current.stdout)["confirm_from"], "source")
            run_git(repo["work"], "checkout", "--detach", repo["side"])
            confirmed = self.confirm(repo, "tag", "v2.0.0", repo["side"])
            self.assertEqual(confirmed.returncode, 0, confirmed.stderr)

    def test_explicit_version_wins_and_a_leading_v_is_not_stripped(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            chosen = self.resolve(repo, "v1.2.3", "9.8.7")
            self.assertEqual(chosen.returncode, 0, chosen.stderr)
            data = output_map(chosen.stdout)
            self.assertEqual(data["source_sha"], repo["base"])
            self.assertEqual(data["version"], "9.8.7")
            self.assertNotIn("1.2.3", data["version"])
            leading = self.resolve(repo, "v1.2.3", "v9.8.7")
            self.assertNotEqual(leading.returncode, 0)
            self.assertEqual(leading.stdout, "")
            self.assertIn("invalid package version: v9.8.7", leading.stderr)
            self.assertNotIn("derived from tag", leading.stderr)
            self.assertEqual(git_head(repo["work"]), repo["main"])

    def test_missing_tag_fails_before_a_local_tag_can_be_used(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            run_git(repo["work"], "tag", "v7.7.7")
            before = git_head(repo["work"])
            missing = self.resolve(repo, "v0.0.9", "")
            self.assertNotEqual(missing.returncode, 0)
            self.assertEqual(missing.stdout, "")
            self.assertIn("git ref does not exist: v0.0.9", missing.stderr)
            local_only = self.resolve(repo, "v7.7.7", "")
            self.assertNotEqual(local_only.returncode, 0)
            self.assertEqual(local_only.stdout, "")
            self.assertIn("git ref does not exist: v7.7.7", local_only.stderr)
            self.assertNotIn(repo["main"], local_only.stdout)
            self.assertEqual(git_head(repo["work"]), before)

    def test_invalid_derived_versions_fail_with_the_tag_name(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            for source, derived in (("not-a-version", "not-a-version"), ("vv1.2.3", "v1.2.3")):
                with self.subTest(source=source):
                    result = self.resolve(repo, source, "")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, "")
                    self.assertIn(f"version derived from tag {source} is invalid: {derived}", result.stderr)
                    self.assertIn("invalid package version", result.stderr)
                    self.assertEqual(git_head(repo["work"]), repo["main"])

    def test_branch_and_full_sha_require_an_explicit_version(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            cases = (
                ("topic", repo["base"], "branch", "topic"),
                ("release/1.2", repo["base"], "branch", "release/1.2"),
                ("refs/heads/topic", repo["base"], "branch", "topic"),
                (repo["base"], repo["base"], "commit", repo["base"]),
            )
            for source, sha, kind, name in cases:
                with self.subTest(source=source):
                    result = self.resolve(repo, source, "3.0.0")
                    self.assertEqual(result.returncode, 0, result.stderr)
                    data = output_map(result.stdout)
                    self.assertEqual(data["source_sha"], sha)
                    self.assertEqual(data["source_kind"], kind)
                    self.assertEqual(data["source_name"], name)
                    self.assertEqual(data["version"], "3.0.0")
                    self.assertEqual(git_head(repo["work"]), repo["main"])
            for source in ("topic", repo["base"]):
                with self.subTest(source=source, version=""):
                    missing = self.resolve(repo, source, "")
                    self.assertNotEqual(missing.returncode, 0)
                    self.assertEqual(missing.stdout, "")
                    self.assertIn("version is required when packaging a branch or commit", missing.stderr)
            absent = self.resolve(repo, "0123456789abcdef0123456789abcdef01234567", "3.0.0")
            self.assertNotEqual(absent.returncode, 0)
            self.assertEqual(absent.stdout, "")
            self.assertIn("git ref does not exist: 0123456789abcdef0123456789abcdef01234567", absent.stderr)

    def test_tag_wins_when_a_branch_has_the_same_name(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            result = self.resolve(repo, "v1.2.3", "4.5.6")
            self.assertEqual(result.returncode, 0, result.stderr)
            data = output_map(result.stdout)
            self.assertEqual(data["source_kind"], "tag")
            self.assertEqual(data["source_sha"], repo["base"])
            self.assertNotEqual(data["source_sha"], repo["main"])

    def test_unsafe_refs_are_rejected_before_fetch(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            for source in ("../v1.2.3", "-h", "main:refs/heads/evil", "v1.2.3.lock",
                            "refs/tags/../v1.2.3", " v1.2.3", "v1.2.3 "):
                with self.subTest(source=source):
                    result = self.resolve(repo, source, "1.2.3")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, "")
                    self.assertIn("unsupported git ref:", result.stderr)
                    self.assertNotIn("does not exist", result.stderr)
                    self.assertEqual(git_head(repo["work"]), repo["main"])

    def test_confirm_step_uses_the_tooling_helper_when_the_tag_cannot_confirm(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            historical = self.resolve(repo, "v1.2.3", "")
            self.assertEqual(historical.returncode, 0, historical.stderr)
            self.assertEqual(output_map(historical.stdout)["confirm_from"], "tool")
            self.assertEqual(output_map(historical.stdout)["packaging"], "tool")
            legacy = self.resolve(repo, "v8.8.8", "")
            self.assertEqual(legacy.returncode, 0, legacy.stderr)
            legacy_data = output_map(legacy.stdout)
            self.assertEqual(legacy_data["source_sha"], repo["legacy"])
            self.assertEqual(legacy_data["packaging"], "source")
            self.assertEqual(legacy_data["confirm_from"], "tool")

            run_git(repo["work"], "checkout", "--detach", repo["base"])
            direct = subprocess.run(
                ["bash", "scripts/release/package_manual_artifacts.sh", "confirm-source",
                 "--kind", "tag", "--name", "v1.2.3", "--sha", repo["base"]],
                cwd=repo["work"], text=True, capture_output=True, timeout=60)
            self.assertEqual(direct.returncode, 127, direct.stderr)
            missing_helper = run_confirm_step(repo["work"], "tag", "v1.2.3", repo["base"])
            self.assertNotEqual(missing_helper.returncode, 0)
            self.assertIn("no packaging script implements confirm-source", missing_helper.stderr)
            install_confirm_helper(repo["work"])
            confirmed = run_confirm_step(repo["work"], "tag", "v1.2.3", repo["base"])
            self.assertEqual(confirmed.returncode, 0, confirmed.stderr)

            stub = repo["work"] / "scripts" / "release" / "package_manual_artifacts.sh"
            stub.parent.mkdir(parents=True, exist_ok=True)
            stub.write_text("#!/bin/bash\ncase \"${1:-}\" in\n  package) ;;\n  *) exit 2 ;;\nesac\n", encoding="utf-8")
            helper = repo["work"] / "packaging-tools" / "scripts" / "release" / "package_manual_artifacts.sh"
            helper.unlink()
            old_script = run_confirm_step(repo["work"], "tag", "v1.2.3", repo["base"])
            self.assertNotEqual(old_script.returncode, 0)
            self.assertIn("no packaging script implements confirm-source", old_script.stderr)
            install_confirm_helper(repo["work"])
            confirmed = run_confirm_step(repo["work"], "tag", "v1.2.3", repo["base"])
            self.assertEqual(confirmed.returncode, 0, confirmed.stderr)

            helper.unlink()
            install_confirm_helper(repo["work"])
            source_helper = repo["work"] / "scripts" / "release" / "package_manual_artifacts.sh"
            source_helper.write_bytes(SCRIPT.read_bytes())
            source_helper.chmod(0o755)
            (repo["work"] / "packaging-tools").rename(repo["work"] / "packaging-tools-absent")
            confirmed = run_confirm_step(repo["work"], "tag", "v1.2.3", repo["base"])
            self.assertEqual(confirmed.returncode, 0, confirmed.stderr)

    def test_recipe_must_exist_on_the_tooling_checkout_when_the_commit_lacks_it(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="stale")
            result = self.resolve(repo, "v1.2.3", "")
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "")
            self.assertIn("does not contain scripts/release/package_manual_artifacts.sh", result.stderr)
            self.assertEqual(git_head(repo["work"]), repo["base"])

    def test_confirm_fails_when_a_tag_is_missing_or_moves(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="main")
            run_git(repo["work"], "checkout", "--detach", repo["base"])
            run_git(repo["seed"], "tag", "-f", "-a", "v1.2.3", "-m", "moved", repo["main"])
            run_git(repo["seed"], "push", "--force", "origin", "refs/tags/v1.2.3:refs/tags/v1.2.3")
            moved = self.confirm(repo, "tag", "v1.2.3", repo["base"])
            self.assertNotEqual(moved.returncode, 0)
            self.assertEqual(moved.stdout, "")
            self.assertIn(f"git tag v1.2.3 points at {repo['main']}, not pinned commit {repo['base']}", moved.stderr)
            run_git(repo["seed"], "push", "origin", ":refs/tags/v1.2.3")
            missing = self.confirm(repo, "tag", "v1.2.3", repo["base"])
            self.assertNotEqual(missing.returncode, 0)
            self.assertEqual(missing.stdout, "")
            self.assertIn("git tag does not exist: v1.2.3", missing.stderr)

    def test_confirm_rejects_a_main_commit_removed_from_the_branch(self):
        with tempfile.TemporaryDirectory() as temp:
            repo = init_packaging_remote(Path(temp), clone_head="stale")
            resolved = self.resolve(repo, "", "1.2.3")
            self.assertEqual(resolved.returncode, 0, resolved.stderr)
            run_git(repo["seed"], "checkout", "--orphan", "rewritten")
            (repo["seed"] / "README").write_text("rewritten\n", encoding="utf-8")
            run_git(repo["seed"], "add", "README")
            run_git(repo["seed"], "commit", "-m", "rewritten")
            run_git(repo["seed"], "push", "--force", "origin", "rewritten:main")
            rejected = self.confirm(repo, "main", "main", repo["main"])
            self.assertNotEqual(rejected.returncode, 0)
            self.assertEqual(rejected.stdout, "")
            self.assertIn("is not on origin/main", rejected.stderr)
            unsafe = self.confirm(repo, "tag", "../v1.2.3", git_head(repo["work"]))
            self.assertNotEqual(unsafe.returncode, 0)
            self.assertIn("unsupported git ref:", unsafe.stderr)


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
