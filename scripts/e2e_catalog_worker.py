#!/usr/bin/env python3
"""Build and run the native Catalog fault-test worker with verified inputs."""

import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid

import e2e_binary as provenance

ROOT = provenance.ROOT
WORKER_TEST = "startup_entrypoint::tests::catalog_worker_native_lifecycle"
RECEIPT_ENV = "RUSTFS_CATALOG_WORKER_RECEIPT"
LAUNCHER_ENV = "RUSTFS_CATALOG_WORKER_LAUNCHER"
ARTIFACT_ENV = "RUSTFS_CATALOG_FAULT_ARTIFACT_DIR"
NODE_ENV = {
    RECEIPT_ENV, LAUNCHER_ENV, ARTIFACT_ENV,
    "RUSTFS_VOLUMES", "RUSTFS_ADDRESS", "RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY",
    "RUSTFS_CONSOLE_ENABLE", "RUSTFS_STORAGE_CLASS_STANDARD", "RUSTFS_TABLE_CATALOG_BACKING",
    "RUSTFS_RPC_SECRET", "RUSTFS_CATALOG_WORKER_SOCKET", "RUSTFS_CATALOG_WORKER_NODE_ID",
    "RUSTFS_CATALOG_WORKER_NONCE",
}
# Cargo injects these build.rs fields into test processes. Keep them out of node configuration.
TEST_BUILD_ENV = {
    "RUSTFS_E2E_BUILD_COMMIT", "RUSTFS_E2E_BUILD_DIRTY", "RUSTFS_E2E_BUILD_FEATURES",
    "RUSTFS_E2E_BUILD_LOCK", "RUSTFS_E2E_BUILD_PROFILE", "RUSTFS_E2E_BUILD_RUSTFLAGS_HEX",
    "RUSTFS_E2E_BUILD_TARGET",
}
MANIFEST_KEYS = {
    "version", "kind", "workspace_root", "source", "worker_path",
    "worker_sha256", "worker_test", "requested_features", "features",
    "profile", "rustc",
}
RECEIPT_KEYS = {
    "version", "workspace_root", "source", "worker_path", "worker_sha256",
    "worker_test", "launcher_path", "launcher_sha256", "build_manifest_path",
    "build_manifest_sha256", "profile", "features", "run_id",
}


def read_json(path):
    def unique_fields(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate JSON field: {key}")
            result[key] = value
        return result

    if path.stat().st_size > 1024 * 1024:
        raise ValueError(f"Oversized worker identity: {path}")
    return json.loads(path.read_text(), object_pairs_hook=unique_fields)


def manifest_path(target, profile):
    return target / profile / "rustfs-catalog-worker.json"


def write_private(path, contents, mode=0o600):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "w") as output:
        output.write(contents)


def build(target, profile, requested):
    manifest = manifest_path(target, profile)
    manifest.unlink(missing_ok=True)
    before = provenance.source_identity()
    compiler = shutil.which(os.environ.get("RUSTC", "rustc"))
    if compiler is None:
        raise ValueError("The selected Catalog worker compiler is not executable")
    # Keep the invocation name: resolving a rustup shim symlink changes dispatch.
    compiler = os.path.abspath(compiler)
    compiler_env = dict(os.environ, RUSTC=compiler, RUSTC_WRAPPER="", RUSTC_WORKSPACE_WRAPPER="")
    compiler_version = subprocess.check_output([compiler, "-Vv"], cwd=ROOT, env=compiler_env, text=True)
    command = [
        "cargo", "test", "--locked", "-p", "rustfs", "--lib", "--no-run",
        "--target-dir", str(target), "--message-format=json-render-diagnostics",
    ]
    if profile == "release":
        command.append("--release")
    if requested:
        command.extend(["--features", ",".join(requested)])
    artifact = None
    with subprocess.Popen(command, cwd=ROOT, env=compiler_env, stdout=subprocess.PIPE, text=True, start_new_session=True) as process:
        try:
            for line in process.stdout:
                message = json.loads(line)
                if message.get("reason") == "compiler-message":
                    print(message["message"].get("rendered", ""), end="", file=sys.stderr)
                if (
                    message.get("reason") == "compiler-artifact"
                    and message.get("target", {}).get("name") == "rustfs"
                    and "lib" in message.get("target", {}).get("kind", [])
                    and message.get("profile", {}).get("test") is True
                    and message.get("executable")
                ):
                    artifact = message
            if process.wait() != 0:
                raise ValueError("Catalog worker build failed; no build identity was recorded")
        except BaseException:
            provenance.terminate_command(process)
            raise
    if artifact is None:
        raise ValueError("Cargo did not produce the RustFS library test executable")
    worker = Path(artifact["executable"]).resolve(strict=True)
    if target not in worker.parents or not worker.is_file() or not os.access(worker, os.X_OK):
        raise ValueError("Catalog worker executable is outside the requested target or is not executable")
    if provenance.source_identity() != before:
        raise ValueError("Catalog worker inputs changed during compilation")
    if subprocess.check_output([compiler, "-Vv"], cwd=ROOT, env=compiler_env, text=True) != compiler_version:
        raise ValueError("Catalog worker compiler identity changed during compilation")
    record = {
        "version": 1,
        "kind": "rustfs-catalog-libtest",
        "workspace_root": str(ROOT),
        "source": before,
        "worker_path": str(worker),
        "worker_sha256": provenance.file_hash(worker),
        "worker_test": WORKER_TEST,
        "requested_features": requested,
        "features": sorted(artifact["features"]),
        "profile": profile,
        "rustc": compiler_version,
    }
    write_private(manifest, json.dumps(record, sort_keys=True) + "\n")
    print(f"Catalog worker: {worker}\nBuild identity: {manifest}", file=sys.stderr)


def verify_manifest(manifest, profile=None, requested=None):
    record = read_json(manifest)
    if (
        not isinstance(record, dict) or set(record) != MANIFEST_KEYS
        or type(record["version"]) is not int or record["version"] != 1
        or record["kind"] != "rustfs-catalog-libtest"
        or record["worker_test"] != WORKER_TEST
        or record["workspace_root"] != str(ROOT)
        or record["profile"] not in ("debug", "release")
        or not isinstance(record["rustc"], str) or not record["rustc"].strip()
    ):
        raise ValueError("Invalid Catalog worker build identity")
    for name in ("features", "requested_features"):
        values = record[name]
        if not isinstance(values, list) or not all(isinstance(value, str) for value in values) or values != sorted(set(values)):
            raise ValueError(f"Invalid Catalog worker {name}")
    if not set(record["requested_features"]) <= set(record["features"]):
        raise ValueError("Catalog worker resolved features omit a requested feature")
    if profile is not None and profile != record["profile"]:
        raise ValueError("Catalog worker build profile differs from this invocation")
    if requested is not None and requested != record["requested_features"]:
        raise ValueError("Catalog worker build features differ from this invocation")
    if record["source"] != provenance.source_identity():
        raise ValueError("Catalog worker source identity changed; rebuild before testing")
    worker = Path(record["worker_path"])
    if str(worker.resolve(strict=True)) != str(worker) or not worker.is_file() or not os.access(worker, os.X_OK):
        raise ValueError("Invalid Catalog worker executable path")
    if provenance.file_hash(worker) != record["worker_sha256"]:
        raise ValueError("Catalog worker binary content changed")
    return record


def launch(receipt_path, extra):
    if extra:
        raise ValueError("Catalog worker launcher accepts configuration through its environment only")
    worker_env = {key: value for key, value in os.environ.items() if key not in TEST_BUILD_ENV}
    unexpected = sorted(key for key in worker_env if (key.startswith("RUSTFS_") and key not in NODE_ENV) or key.startswith(("MINIO_", "OTEL_")))
    if unexpected:
        raise ValueError("Unexpected inherited Catalog worker settings: " + ", ".join(unexpected))
    if os.environ.get(RECEIPT_ENV) != str(receipt_path):
        raise ValueError("Catalog worker receipt differs from the parent test invocation")
    if receipt_path.is_symlink() or stat.S_IMODE(receipt_path.stat().st_mode) != 0o600:
        raise ValueError("Catalog worker receipt requires a private regular file")
    if receipt_path.stat().st_uid != os.geteuid() or stat.S_IMODE(receipt_path.parent.stat().st_mode) != 0o700:
        raise ValueError("Catalog worker receipt directory is not private to the test process")
    receipt = read_json(receipt_path)
    if not isinstance(receipt, dict) or set(receipt) != RECEIPT_KEYS or type(receipt["version"]) is not int or receipt["version"] != 1:
        raise ValueError("Invalid Catalog worker run receipt")
    uuid.UUID(receipt["run_id"])
    manifest = Path(receipt["build_manifest_path"])
    if provenance.file_hash(manifest) != receipt["build_manifest_sha256"]:
        raise ValueError("Catalog worker build manifest changed during the test invocation")
    record = verify_manifest(manifest)
    for key in ("workspace_root", "source", "worker_path", "worker_sha256", "worker_test", "profile", "features"):
        if receipt[key] != record[key]:
            raise ValueError(f"Catalog worker receipt does not match its build: {key}")
    launcher = Path(receipt["launcher_path"])
    if (
        os.environ.get(LAUNCHER_ENV) != str(launcher)
        or launcher.parent != receipt_path.parent
        or launcher.is_symlink()
        or str(launcher.resolve(strict=True)) != str(launcher)
        or stat.S_IMODE(launcher.stat().st_mode) != 0o700
        or provenance.file_hash(launcher) != receipt["launcher_sha256"]
    ):
        raise ValueError("Catalog worker launcher identity changed")
    worker = record["worker_path"]
    os.execve(worker, [worker, "--exact", WORKER_TEST, "--nocapture", "--test-threads=1"], worker_env)


def run(manifest, profile, requested, command):
    if not command:
        raise ValueError("run requires a test command after --")
    record = verify_manifest(manifest, profile, requested)
    before_manifest = provenance.file_hash(manifest)
    with tempfile.TemporaryDirectory(prefix="catalog-worker-run-") as temporary:
        directory = Path(temporary).resolve()
        directory.chmod(0o700)
        receipt = directory / "receipt.json"
        launcher = directory / "launcher"
        launcher_command = [sys.executable, str(Path(__file__).resolve()), "launch", "--receipt", str(receipt)]
        write_private(launcher, "#!/bin/sh\nexec " + shlex.join(launcher_command) + ' "$@"\n', 0o700)
        run_record = {key: record[key] for key in ("workspace_root", "source", "worker_path", "worker_sha256", "worker_test", "profile", "features")}
        run_record.update({
            "version": 1,
            "launcher_path": str(launcher),
            "launcher_sha256": provenance.file_hash(launcher),
            "build_manifest_path": str(manifest),
            "build_manifest_sha256": before_manifest,
            "run_id": str(uuid.uuid4()),
        })
        write_private(receipt, json.dumps(run_record, sort_keys=True) + "\n")
        env = {key: value for key, value in os.environ.items() if not key.startswith(("RUSTFS_", "MINIO_", "OTEL_"))}
        if ARTIFACT_ENV in os.environ:
            env[ARTIFACT_ENV] = os.environ[ARTIFACT_ENV]
        env.update({RECEIPT_ENV: str(receipt), LAUNCHER_ENV: str(launcher)})
        with subprocess.Popen(command, cwd=ROOT, env=env, start_new_session=True) as process:
            try:
                status = process.wait()
            except BaseException:
                provenance.terminate_command(process)
                raise
        try:
            if (
                verify_manifest(manifest, profile, requested) != record
                or provenance.file_hash(manifest) != before_manifest
                or read_json(receipt) != run_record
                or provenance.file_hash(launcher) != run_record["launcher_sha256"]
            ):
                raise ValueError("Catalog worker run identity changed during testing")
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print(f"Catalog worker validation invalidated: {error}", file=sys.stderr)
            return status if status else 1
        return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("build", "run", "launch"))
    parser.add_argument("--features", default="")
    parser.add_argument("--profile", choices=("debug", "release"), default="debug")
    parser.add_argument("--receipt", type=Path)
    arguments = sys.argv[1:]
    command = []
    if "--" in arguments:
        separator = arguments.index("--")
        command = arguments[separator + 1:]
        arguments = arguments[:separator]
    options = parser.parse_args(arguments)
    try:
        if os.name != "posix":
            raise ValueError("The native Catalog worker requires Unix sockets")
        if options.mode == "launch":
            if options.receipt is None:
                raise ValueError("launch requires a run receipt")
            launch(options.receipt, command)
            return 1
        if options.receipt is not None:
            raise ValueError("--receipt is reserved for the private launcher")
        target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
        provenance.validate_target_directory(target)
        manifest = manifest_path(target, options.profile)
        manifest.parent.mkdir(parents=True, exist_ok=True)
        with provenance.exclusive_binary(manifest):
            requested = provenance.feature_set(options.features)
            if options.mode == "build":
                if command:
                    raise ValueError("build does not accept a test command")
                build(target, options.profile, requested)
                return 0
            return run(manifest, options.profile, requested, command)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Catalog worker prerequisite failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
