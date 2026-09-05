#!/usr/bin/env python3
"""Exercise the E2E build/run boundary without compiling RustFS."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest


class BinaryProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "scripts").mkdir()
        shutil.copy(Path(__file__).with_name("e2e_binary.py"), self.root / "scripts/e2e_binary.py")
        (self.root / "Cargo.toml").write_text("[workspace]\n")
        (self.root / "source.rs").write_text("original source\n")
        (self.root / ".gitignore").write_text("/target/\n/rustfs/static/\n")
        (self.root / ".agents/skills").mkdir(parents=True)
        (self.root / ".agents/skills/SKILL.md").write_text("tracked instructions\n")
        (self.root / ".claude").mkdir()
        (self.root / ".claude/skills").symlink_to("../.agents/skills", target_is_directory=True)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        for args in (["add", "."], ["-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "fixture"]):
            subprocess.run(["git", "-C", str(self.root), *args], check=True)
        self.commands = self.root / "target/commands"
        self.commands.mkdir(parents=True)
        cargo = self.commands / "cargo"
        cargo.write_text(f"#!{sys.executable}\n" + '''import json, os, pathlib, sys
if os.environ.get("FAKE_BUILD_FAIL"):
    raise SystemExit(23)
args = sys.argv[1:]
target = pathlib.Path(args[args.index("--target-dir") + 1])
binary = target / ("release" if "--release" in args else "debug") / "rustfs"
binary.parent.mkdir(parents=True, exist_ok=True)
binary.write_text("#!/bin/sh\\nexit 0\\n")
binary.chmod(0o755)
features = ["default", "ftps", "webdav"]
if "--features" in args:
    features.extend(args[args.index("--features") + 1].split(","))
if "full" in features:
    features.extend(["sftp", "swift", "metrics-gpu", "pyroscope"])
print(json.dumps({"reason": "compiler-artifact", "target": {"name": "rustfs", "kind": ["bin"]}, "executable": str(binary), "features": sorted(set(features))}))
if os.environ.get("FAKE_BUILD_MUTATE"):
    pathlib.Path("source.rs").write_text("changed during build")
''')
        cargo.chmod(0o755)
        rustc = self.commands / "rustc"
        rustc.write_text("#!/bin/sh\nprintf 'rustc fixture\\nhost: fixture\\n'\n")
        rustc.chmod(0o755)
        self.env = dict(os.environ, PATH=f"{self.commands}{os.pathsep}{os.environ['PATH']}")
        for name in ("CARGO_TARGET_DIR", "CARGO_BIN_EXE_rustfs", "RUSTFS_BUILD_FEATURES", "RUSTFS_E2E_BINARY_RECEIPT"):
            self.env.pop(name, None)
        self.binary = self.root / "target/debug/rustfs"
        self.sidecar = self.binary.with_name("rustfs.e2e.json")

    def invoke(self, *args, env=None):
        return subprocess.run([sys.executable, str(self.root / "scripts/e2e_binary.py"), *args], cwd=self.root, env=env or self.env, text=True, capture_output=True)

    def build(self, features=""):
        result = self.invoke("build", "--features", features)
        self.assertEqual(result.returncode, 0, result.stderr)

    def run_code(self, code="pass", features="", env=None):
        return self.invoke("run", "--features", features, "--", sys.executable, "-c", code, env=env)

    def test_build_run_and_receipt_cleanup(self):
        self.build("full,e2e-test-hooks")
        result = self.run_code("import os,pathlib; print(os.environ['RUSTFS_E2E_BINARY_RECEIPT']); assert pathlib.Path(os.environ['CARGO_BIN_EXE_rustfs']).is_file(); assert 'sftp' in os.environ['RUSTFS_BUILD_FEATURES']", "e2e-test-hooks,full")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(Path(result.stdout.strip()).exists(), "run receipts must not survive their command")
        self.assertIn("sftp", json.loads(self.sidecar.read_text())["features"])

    def test_source_changes_are_not_hidden_by_timestamps_or_head(self):
        self.build()
        path = self.root / "source.rs"
        old = path.stat()
        path.write_text("different bytes\n")
        os.utime(path, ns=(old.st_atime_ns, old.st_mtime_ns))
        self.assertNotEqual(self.run_code().returncode, 0)

    def test_deleted_untracked_and_ignored_embedded_inputs(self):
        for mutation in ("delete", "untracked", "static"):
            with self.subTest(mutation=mutation):
                self.build()
                path = self.root / "source.rs"
                if mutation == "delete":
                    path.unlink()
                elif mutation == "untracked":
                    (self.root / "new.rs").write_text("new source")
                else:
                    static = self.root / "rustfs/static"
                    static.mkdir(parents=True)
                    (static / "index.html").write_text("embedded content")
                self.assertNotEqual(self.run_code().returncode, 0)
                path.write_text("original source\n")

    def test_wrong_binary_features_and_manifest_fail_closed(self):
        self.build("sftp")
        self.assertNotEqual(self.run_code(features="webdav").returncode, 0)
        self.binary.write_text("old server")
        self.assertNotEqual(self.run_code(features="sftp").returncode, 0)
        self.sidecar.write_text("{}")
        self.assertNotEqual(self.run_code(features="sftp").returncode, 0)
        self.sidecar.unlink()
        self.assertNotEqual(self.run_code(features="sftp").returncode, 0)

    def test_build_failure_or_source_race_does_not_leave_a_receipt(self):
        for failure in ("FAKE_BUILD_FAIL", "FAKE_BUILD_MUTATE"):
            self.build()
            result = self.invoke("build", env=dict(self.env, **{failure: "1"}))
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(self.sidecar.exists())

    def test_child_failure_and_changes_during_run_fail(self):
        self.build()
        failed = self.run_code("raise SystemExit(37)")
        self.assertEqual(failed.returncode, 37, failed.stderr)
        for code in ("import pathlib; pathlib.Path('source.rs').write_text('changed while testing')", "import pathlib; pathlib.Path('target/debug/rustfs').write_text('different server')"):
            self.build()
            self.assertNotEqual(self.run_code(code).returncode, 0)

    def test_override_cannot_select_an_unverified_server(self):
        self.build()
        result = self.run_code(env=dict(self.env, CARGO_BIN_EXE_rustfs="/some/old/server"))
        self.assertNotEqual(result.returncode, 0)

    def test_artifact_moves_between_clean_checkouts(self):
        self.build()
        with tempfile.TemporaryDirectory() as destination:
            clone = Path(destination) / "clone"
            subprocess.run(["git", "clone", "-q", str(self.root), str(clone)], check=True)
            (clone / "target/debug").mkdir(parents=True)
            shutil.copy2(self.binary, clone / "target/debug/rustfs")
            shutil.copy2(self.sidecar, clone / "target/debug/rustfs.e2e.json")
            result = subprocess.run([sys.executable, str(clone / "scripts/e2e_binary.py"), "run", "--", sys.executable, "-c", "pass"], cwd=clone, env=self.env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_target_directory_and_profile_are_explicit(self):
        env = dict(self.env, CARGO_TARGET_DIR="target/custom")
        built = self.invoke("build", "--profile", "release", env=env)
        self.assertEqual(built.returncode, 0, built.stderr)
        run = self.invoke("run", "--profile", "release", "--", sys.executable, "-c", "pass", env=env)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertNotEqual(self.invoke("run", "--", sys.executable, "-c", "pass", env=env).returncode, 0)

    def test_target_directory_cannot_hide_source_inputs(self):
        for target in (str(self.root), str(self.root / "crates"), str(self.root.parent)):
            with self.subTest(target=target):
                result = self.invoke("build", env=dict(self.env, CARGO_TARGET_DIR=target))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("CARGO_TARGET_DIR", result.stderr)
        tracked = self.root / "target/tracked.rs"
        tracked.write_text("tracked build input")
        subprocess.run(["git", "add", "-f", "target/tracked.rs"], cwd=self.root, check=True)
        self.build()
        tracked.write_text("changed tracked build input")
        self.assertNotEqual(self.run_code().returncode, 0)

    def test_unsupported_embedded_directory_links_fail_closed(self):
        self.build()
        destination = self.root / "target/embedded-assets"
        destination.mkdir()
        (destination / "index.html").write_text("untracked embedded input")
        static = self.root / "rustfs/static"
        static.mkdir(parents=True)
        (static / "linked-assets").symlink_to(destination, target_is_directory=True)
        self.assertNotEqual(self.run_code().returncode, 0)

    def test_directory_aliases_cannot_hide_unrecorded_inputs(self):
        self.build()
        target = self.root / ".agents/skills/SKILL.md"
        target.write_text("changed instructions\n")
        self.assertNotEqual(self.run_code().returncode, 0)
        self.build()
        (target.parent / ".gitignore").write_text("hidden.rs\n")
        (target.parent / "hidden.rs").write_text("ignored build input\n")
        result = self.invoke("build")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unrecorded input", result.stderr)
        alias = self.root / ".claude/skills"
        alias.unlink()
        with tempfile.TemporaryDirectory() as external:
            alias.symlink_to(external, target_is_directory=True)
            result = self.invoke("build")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("escapes the source inventory", result.stderr)

    def test_directory_alias_indirection_is_part_of_the_identity(self):
        for name in ("first", "second"):
            directory = self.root / name
            directory.mkdir()
            (directory / "input.rs").write_text(name)
        selection = self.root / "target/selection"
        selection.symlink_to(self.root / "first", target_is_directory=True)
        (self.root / "source-alias").symlink_to("target/selection", target_is_directory=True)
        self.build()
        selection.unlink()
        selection.symlink_to(self.root / "second", target_is_directory=True)
        self.assertNotEqual(self.run_code().returncode, 0)

    def test_existing_embedded_files_and_symlink_targets_are_hashed(self):
        static = self.root / "rustfs/static"
        static.mkdir(parents=True)
        index = static / "index.html"
        index.write_text("embedded version one")
        external = self.root / "target/embedded-file"
        external.write_text("linked version one")
        (static / "linked.html").symlink_to(external)
        self.build()
        index.write_text("embedded version two")
        self.assertNotEqual(self.run_code().returncode, 0)
        self.build()
        external.write_text("linked version two")
        self.assertNotEqual(self.run_code().returncode, 0)

    def test_each_run_hashes_binary_twice_and_never_calls_cargo(self):
        script = self.root / "scripts/e2e_binary.py"
        script.write_text(script.read_text().replace("def file_hash(path):\n", "def file_hash(path):\n    if path.name == 'rustfs':\n        with (ROOT / 'target/hash-count').open('a') as count:\n            count.write('hash\\n')\n"))
        self.build()
        count = self.root / "target/hash-count"
        count.write_text("")
        result = self.run_code(env=dict(self.env, FAKE_BUILD_FAIL="1"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(count.read_text().splitlines(), ["hash", "hash"])

    def test_concurrent_build_or_run_is_rejected(self):
        self.build()
        command = [sys.executable, str(self.root / "scripts/e2e_binary.py"), "run", "--", sys.executable, "-c", "print('ready', flush=True); input()"]
        with subprocess.Popen(command, cwd=self.root, env=self.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
            self.assertEqual(process.stdout.readline().strip(), "ready")
            try:
                for args in (("build", "--features", "sftp"), ("run", "--", sys.executable, "-c", "pass")):
                    rejected = self.invoke(*args)
                    self.assertNotEqual(rejected.returncode, 0)
                    self.assertIn("Another E2E build/run", rejected.stderr)
            finally:
                output, error = process.communicate("\n", timeout=10)
            self.assertEqual(process.returncode, 0, error + output)
        self.assertFalse(self.binary.with_name("rustfs.e2e.lock").exists())

    def test_interruption_cleans_receipt_and_releases_ownership(self):
        self.build()
        for signum in (signal.SIGINT, signal.SIGTERM):
            command = [sys.executable, str(self.root / "scripts/e2e_binary.py"), "run", "--", sys.executable, "-c", "import os; print(os.environ['RUSTFS_E2E_BINARY_RECEIPT'], flush=True); input()"]
            with subprocess.Popen(command, cwd=self.root, env=self.env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as process:
                receipt = Path(process.stdout.readline().strip())
                self.assertTrue(receipt.is_file())
                process.send_signal(signum)
                process.communicate(timeout=10)
                self.assertNotEqual(process.returncode, 0)
            self.assertFalse(receipt.exists())
            self.assertFalse(self.binary.with_name("rustfs.e2e.lock").exists())


if __name__ == "__main__":
    unittest.main()
