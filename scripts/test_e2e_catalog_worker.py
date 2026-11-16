#!/usr/bin/env python3
"""Exercise native worker provenance through its CLI without building RustFS."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class CatalogWorkerProvenanceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        for name in ("e2e_binary.py", "e2e_catalog_worker.py"):
            shutil.copy(Path(__file__).with_name(name), scripts / name)
        (self.root / ".gitignore").write_text("/target/\n__pycache__/\n/rustfs/static/\n")
        (self.root / "source.rs").write_text("original source\n")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        for args in (["add", "."], ["-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "fixture"]):
            subprocess.run(["git", "-C", str(self.root), *args], check=True)
        commands = self.root / "target/commands"
        commands.mkdir(parents=True)
        cargo = commands / "cargo"
        cargo.write_text(f"#!{sys.executable}\n" + '''import json, os, pathlib, subprocess, sys
args = sys.argv[1:]
assert args[:6] == ["test", "--locked", "-p", "rustfs", "--lib", "--no-run"], args
if os.environ.get("FAKE_BUILD_FAIL"):
    raise SystemExit(23)
target = pathlib.Path(args[args.index("--target-dir") + 1])
assert os.environ["RUSTC_WRAPPER"] == ""
assert os.environ["RUSTC_WORKSPACE_WRAPPER"] == ""
compiler = subprocess.check_output([os.environ["RUSTC"], "-Vv"], text=True)
(target / "compiler-used.txt").write_text(compiler)
worker = target / ("release" if "--release" in args else "debug") / "deps/rustfs-fixture"
worker.parent.mkdir(parents=True, exist_ok=True)
worker.write_text("#!" + sys.executable + "\\nimport json, os, sys\\nassert not any(key.startswith('RUSTFS_E2E_BUILD_') for key in os.environ)\\nprint(json.dumps(sys.argv[1:]))\\n")
worker.chmod(0o755)
features = ["default", "ftps", "webdav"]
if "--features" in args:
    features.extend(args[args.index("--features") + 1].split(","))
print(json.dumps({"reason":"compiler-artifact", "target":{"name":"rustfs", "kind":[os.environ.get("FAKE_TARGET_KIND", "lib")]}, "profile":{"test":True}, "executable":str(worker), "features":sorted(set(features))}))
if os.environ.get("FAKE_BUILD_MUTATE"):
    pathlib.Path("source.rs").write_text("changed during build")
if os.environ.get("FAKE_COMPILER_MUTATE"):
    pathlib.Path(os.environ["RUSTC"]).write_text("#!/bin/sh\\nprintf 'changed compiler\\\\n'\\n")
''')
        cargo.chmod(0o755)
        rustc = commands / "rustc"
        rustc.write_text("#!/bin/sh\nprintf 'rustc fixture\\nhost: fixture\\n'\n")
        rustc.chmod(0o755)
        self.env = dict(os.environ, PATH=f"{commands}{os.pathsep}{os.environ['PATH']}")
        for name in ("CARGO_TARGET_DIR", "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTFS_CATALOG_WORKER_LAUNCHER", "RUSTFS_CATALOG_WORKER_RECEIPT"):
            self.env.pop(name, None)
        self.manifest = self.root / "target/debug/rustfs-catalog-worker.json"

    def invoke(self, *args, env=None):
        return subprocess.run(
            [sys.executable, str(self.root / "scripts/e2e_catalog_worker.py"), *args],
            cwd=self.root, env=env or self.env, text=True, capture_output=True,
        )

    def build(self, features=""):
        result = self.invoke("build", "--features", features)
        self.assertEqual(result.returncode, 0, result.stderr)

    def run_code(self, code, features="", env=None):
        return self.invoke("run", "--features", features, "--", sys.executable, "-c", code, env=env)

    def test_build_run_launcher_and_receipt_cleanup(self):
        self.build("e2e-test-hooks")
        result = self.run_code('''import json, os, pathlib, subprocess
receipt = pathlib.Path(os.environ["RUSTFS_CATALOG_WORKER_RECEIPT"])
data = json.loads(receipt.read_text())
assert receipt.stat().st_mode & 0o777 == 0o600
assert receipt.parent.stat().st_mode & 0o777 == 0o700
assert pathlib.Path(data["launcher_path"]).stat().st_mode & 0o777 == 0o700
out = subprocess.check_output([os.environ["RUSTFS_CATALOG_WORKER_LAUNCHER"]], text=True)
assert json.loads(out) == ["--exact", "startup_entrypoint::tests::catalog_worker_native_lifecycle", "--nocapture", "--test-threads=1"]
print(receipt)
''', "e2e-test-hooks")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(Path(result.stdout.strip()).exists())
        self.assertFalse(self.manifest.with_name(self.manifest.name + ".e2e.lock").exists())

    def test_failed_build_removes_previous_identity(self):
        self.build()
        result = self.invoke("build", env=dict(self.env, FAKE_BUILD_FAIL="1"))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.manifest.exists())

    def test_rustc_override_is_the_compiler_recorded_and_invoked(self):
        compiler = self.root / "target/commands/custom-rustc"
        compiler.write_text("#!/bin/sh\nprintf 'rustc custom-compiler\\nhost: fixture\\n'\n")
        compiler.chmod(0o755)
        env = dict(self.env, RUSTC=str(compiler), RUSTC_WRAPPER="unsupported-wrapper", RUSTC_WORKSPACE_WRAPPER="another-wrapper")
        result = self.invoke("build", env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        record = json.loads(self.manifest.read_text())
        self.assertEqual(record["rustc"], (self.root / "target/compiler-used.txt").read_text())
        self.assertIn("custom-compiler", record["rustc"])

    def test_changed_compiler_invalidates_build(self):
        result = self.invoke("build", env=dict(self.env, FAKE_COMPILER_MUTATE="1"))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.manifest.exists())

    def test_run_removes_host_server_and_telemetry_configuration(self):
        self.build()
        env = dict(self.env, RUSTFS_NOTIFY_ENDPOINT="https://outside.invalid", MINIO_NOTIFY_ENDPOINT="https://outside.invalid", OTEL_EXPORTER_OTLP_ENDPOINT="https://outside.invalid", RUSTFS_CATALOG_FAULT_ARTIFACT_DIR="/explicit-artifacts")
        result = self.run_code('''import os, subprocess
assert "RUSTFS_NOTIFY_ENDPOINT" not in os.environ
assert "MINIO_NOTIFY_ENDPOINT" not in os.environ
assert "OTEL_EXPORTER_OTLP_ENDPOINT" not in os.environ
assert os.environ["RUSTFS_CATALOG_FAULT_ARTIFACT_DIR"] == "/explicit-artifacts"
subprocess.run([os.environ["RUSTFS_CATALOG_WORKER_LAUNCHER"]], check=True)
''', env=env)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_launcher_rejects_unregistered_server_settings(self):
        self.build()
        result = self.run_code('''import os, subprocess
for key in ("RUSTFS_NOTIFY_ENDPOINT", "RUSTFS_E2E_BUILD_UNEXPECTED"):
    env = dict(os.environ, **{key: "unexpected"})
    result = subprocess.run([os.environ["RUSTFS_CATALOG_WORKER_LAUNCHER"]], env=env, capture_output=True)
    assert result.returncode != 0
    assert b"Unexpected inherited" in result.stderr
''')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_launcher_removes_cargo_build_metadata_before_worker(self):
        self.build()
        result = self.run_code('''import os, subprocess
keys = ("COMMIT", "DIRTY", "FEATURES", "LOCK", "PROFILE", "RUSTFLAGS_HEX", "TARGET")
env = dict(os.environ, **{"RUSTFS_E2E_BUILD_" + key: "cargo-metadata" for key in keys})
subprocess.run([os.environ["RUSTFS_CATALOG_WORKER_LAUNCHER"]], env=env, check=True)
''')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_non_libtest_artifact_is_rejected(self):
        result = self.invoke("build", env=dict(self.env, FAKE_TARGET_KIND="bin"))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.manifest.exists())

    def test_source_change_during_build_is_rejected(self):
        result = self.invoke("build", env=dict(self.env, FAKE_BUILD_MUTATE="1"))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.manifest.exists())

    def test_source_binary_and_feature_mismatches_are_rejected(self):
        self.build("e2e-test-hooks")
        self.assertNotEqual(self.run_code("pass").returncode, 0)
        source = self.root / "source.rs"
        source.write_text("new source\n")
        self.assertNotEqual(self.run_code("pass", "e2e-test-hooks").returncode, 0)
        source.write_text("original source\n")
        record = json.loads(self.manifest.read_text())
        worker = Path(record["worker_path"])
        worker.write_text(worker.read_text() + "# tamper\n")
        self.assertNotEqual(self.run_code("pass", "e2e-test-hooks").returncode, 0)

    def test_child_failure_is_preserved(self):
        self.build()
        self.assertEqual(self.run_code("raise SystemExit(37)").returncode, 37)

    def test_inputs_changed_during_run_invalidate_success_and_preserve_failure(self):
        for status in (0, 37):
            with self.subTest(status=status):
                (self.root / "source.rs").write_text("original source\n")
                self.build()
                result = self.run_code(f"import pathlib; pathlib.Path('source.rs').write_text('mutation'); raise SystemExit({status})")
                self.assertEqual(result.returncode, status or 1)

    def test_receipt_and_launcher_tampering_are_rejected(self):
        for field in ("worker_test", "worker_sha256", "launcher"):
            with self.subTest(field=field):
                self.build()
                result = self.run_code('''import json, os, pathlib, subprocess, sys
p = pathlib.Path(os.environ["RUSTFS_CATALOG_WORKER_RECEIPT"])
r = json.loads(p.read_text())
field = ''' + repr(field) + '''
if field == "launcher":
    launcher = pathlib.Path(r["launcher_path"])
    launcher.write_text(launcher.read_text() + "# changed\\n")
else:
    r[field] = "invalid"
    p.write_text(json.dumps(r))
out = subprocess.run([os.environ["RUSTFS_CATALOG_WORKER_LAUNCHER"]], capture_output=True)
assert out.returncode != 0, out.stdout
''')
                self.assertNotEqual(result.returncode, 0, "parent must invalidate a tampered receipt even when the child observed rejection")

    def test_duplicate_and_unknown_manifest_fields_are_rejected(self):
        self.build()
        original = self.manifest.read_text()
        for suffix in (',"version":1}', ',"extra":true}'):
            with self.subTest(suffix=suffix):
                self.manifest.write_text(original.rstrip().removesuffix("}") + suffix)
                self.assertNotEqual(self.run_code("pass").returncode, 0)

    def test_missing_receipt_and_unseparated_command_are_errors(self):
        self.assertNotEqual(self.invoke("launch").returncode, 0)
        self.assertNotEqual(self.invoke("run", "true").returncode, 0)

    def test_existing_marker_rejects_concurrent_build(self):
        self.manifest.parent.mkdir(parents=True)
        marker = self.manifest.with_name(self.manifest.name + ".e2e.lock")
        marker.write_text("pid=fixture\n")
        result = self.invoke("build")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(marker.read_text(), "pid=fixture\n")


if __name__ == "__main__":
    unittest.main()
