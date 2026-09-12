#!/usr/bin/env python3
"""Build an identified E2E server and verify it around one test invocation."""

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import signal
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
RECEIPT_ENV = "RUSTFS_E2E_BINARY_RECEIPT"


def feature_set(value):
    return sorted(set(part.strip() for part in value.split(",") if part.strip()))


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_identity():
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    tracked = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT)
    paths = set(tracked.decode("utf-8").rstrip("\0").split("\0")) - {""}
    # RustEmbed consumes ignored console assets as well as tracked Rust sources.
    static_dir = ROOT / "rustfs/static"
    if static_dir.is_symlink():
        raise ValueError("The embedded static directory must not be a symlink")
    if static_dir.is_dir():
        for path in static_dir.rglob("*"):
            if path.is_symlink() and path.is_dir():
                raise ValueError(f"Unsupported embedded directory symlink: {path}")
            if not path.is_dir():
                paths.add(str(path.relative_to(ROOT)))
    elif static_dir.exists():
        paths.add("rustfs/static")
    digest = hashlib.sha256()
    digest.update(b"static-present\0" if static_dir.is_dir() else b"static-absent\0")
    for name in sorted(paths):
        path = ROOT / name
        digest.update(name.encode("utf-8") + b"\0")
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            digest.update(b"deleted\0")
            continue
        if stat.S_ISLNK(metadata.st_mode):
            digest.update(b"symlink\0" + os.fsencode(os.readlink(path)) + b"\0")
            if path.is_dir():
                target = path.resolve()
                if ROOT not in target.parents:
                    raise ValueError(f"Directory link escapes the source inventory: {name}")
                # Directory aliases such as .claude/skills share already-hashed inputs.
                for child in target.rglob("*"):
                    if child.is_dir() and not child.is_symlink():
                        continue
                    if child.is_dir() or str(child.relative_to(ROOT)) not in paths:
                        raise ValueError(f"Directory link contains an unrecorded input: {child}")
                digest.update(b"directory\0" + str(target.relative_to(ROOT)).encode("utf-8") + b"\0")
                continue
        elif not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"Unsupported build input: {name}")
        digest.update(str(metadata.st_mode & 0o111).encode() + b"\0")
        digest.update(file_hash(path).encode() + b"\0")
    return {"head": head, "sha256": digest.hexdigest()}


def sidecar_path(binary):
    return binary.with_name(binary.name + ".e2e.json")


def validate_target_directory(target_dir):
    if target_dir == ROOT or target_dir in ROOT.parents:
        raise ValueError("CARGO_TARGET_DIR must not contain the source workspace")
    if ROOT in target_dir.parents:
        ignored = subprocess.run(["git", "check-ignore", "--quiet", "--no-index", str(target_dir.relative_to(ROOT))], cwd=ROOT)
        if ignored.returncode != 0:
            raise ValueError("An in-workspace CARGO_TARGET_DIR must be Git-ignored; use target/ or an external directory")


@contextmanager
def exclusive_binary(binary):
    marker = binary.with_name(binary.name + ".e2e.lock")
    try:
        descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise ValueError(f"Another E2E build/run owns {marker}; do not share a target directory between concurrent runs") from error
    try:
        identity = os.fstat(descriptor)
        with os.fdopen(descriptor, "w") as lock:
            lock.write(f"pid={os.getpid()}\n")
        yield
    finally:
        current = marker.stat()
        if (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino):
            raise ValueError("The E2E ownership marker changed during the command")
        marker.unlink()


def terminate_command(process):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def build(binary, target_dir, profile, requested, all_bins):
    sidecar = sidecar_path(binary)
    sidecar.unlink(missing_ok=True)
    before = source_identity()
    command = ["cargo", "build", "--locked", "-p", "rustfs", "--target-dir", str(target_dir), "--message-format=json-render-diagnostics"]
    command.extend(["--bins"] if all_bins else ["--bin", "rustfs"])
    if requested:
        command.extend(["--features", ",".join(requested)])
    if profile == "release":
        command.append("--release")
    artifact = None
    with subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, text=True, start_new_session=True) as process:
        try:
            for line in process.stdout:
                message = json.loads(line)
                if message.get("reason") == "compiler-message":
                    print(message["message"].get("rendered", ""), end="", file=sys.stderr)
                if message.get("reason") == "compiler-artifact" and message.get("target", {}).get("name") == "rustfs" and "bin" in message.get("target", {}).get("kind", []):
                    artifact = message
            if process.wait() != 0:
                raise ValueError("RustFS build failed; no E2E identity was recorded")
        except BaseException:
            terminate_command(process)
            raise
    if not artifact or Path(artifact.get("executable", "")).resolve() != binary:
        raise ValueError("Cargo did not produce the requested RustFS executable")
    if source_identity() != before:
        raise ValueError("Build inputs changed during compilation; finish preparing embedded assets and rebuild in an isolated worktree")
    record = {
        "schema": 1,
        "source": before,
        "requested_features": requested,
        "features": sorted(artifact["features"]),
        "profile": profile,
        "rustc": subprocess.check_output(["rustc", "-Vv"], text=True),
        "binary_sha256": file_hash(binary),
    }
    sidecar.write_text(json.dumps(record, sort_keys=True) + "\n")
    print(f"Built E2E server: {binary}\nIdentity: {sidecar}", file=sys.stderr)


def verify(binary, profile, requested):
    record = json.loads(sidecar_path(binary).read_text())
    if not isinstance(record, dict) or set(record) != {"schema", "source", "requested_features", "features", "profile", "rustc", "binary_sha256"} or type(record["schema"]) is not int or record["schema"] != 1:
        raise ValueError("Missing or unsupported E2E binary identity; run the build command")
    if not isinstance(record["rustc"], str) or not record["rustc"].strip():
        raise ValueError("Missing E2E build toolchain identity")
    if record["requested_features"] != requested or record["profile"] != profile:
        raise ValueError("E2E binary build features/profile differ from this test invocation")
    if not isinstance(record["features"], list) or not all(isinstance(item, str) for item in record["features"]) or not set(requested) <= set(record["features"]):
        raise ValueError("Invalid resolved E2E binary features")
    if record["source"] != source_identity():
        raise ValueError("E2E binary was built from different inputs; rebuild before testing")
    if record["binary_sha256"] != file_hash(binary):
        raise ValueError("E2E binary content differs from its build identity")
    return record


def run(binary, profile, requested, command):
    if not command:
        raise ValueError("run requires a test command after --")
    override = os.environ.get("CARGO_BIN_EXE_rustfs")
    if override and Path(override).resolve() != binary:
        raise ValueError("CARGO_BIN_EXE_rustfs selects a different server; use --binary explicitly")
    record = verify(binary, profile, requested)
    metadata = binary.stat()
    with tempfile.TemporaryDirectory(prefix="rustfs-e2e-receipt-") as directory:
        receipt = Path(directory) / "receipt.json"
        receipt.write_text(json.dumps({
            "schema": 1,
            "workspace": str(ROOT),
            "binary": str(binary),
            "size": metadata.st_size,
            "modified_ns": metadata.st_mtime_ns,
            "features": record["features"],
        }))
        env = dict(os.environ, CARGO_BIN_EXE_rustfs=str(binary), RUSTFS_BUILD_FEATURES=",".join(record["features"]))
        env[RECEIPT_ENV] = str(receipt)
        with subprocess.Popen(command, cwd=ROOT, env=env, start_new_session=True) as process:
            try:
                status = process.wait()
            except (KeyboardInterrupt, SystemExit):
                terminate_command(process)
                raise
        try:
            if verify(binary, profile, requested) != record:
                raise ValueError("E2E build identity changed during testing")
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print(f"E2E validation invalidated: {error}", file=sys.stderr)
            return status if status else 1
        return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("build", "run"))
    parser.add_argument("--features", default="", help="additional Cargo features; defaults remain enabled")
    parser.add_argument("--profile", choices=("debug", "release"), default="debug")
    parser.add_argument("--binary", type=Path, help="prebuilt server path for run")
    parser.add_argument("--bins", action="store_true", help="build all RustFS binary targets, preserving the CI build matrix")
    # Parse the child command separately so its options are never interpreted here.
    args = sys.argv[1:]
    separator = args.index("--") if "--" in args else len(args)
    command = args[separator + 1:] if separator < len(args) else []
    options = parser.parse_args(args[:separator])
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    binary = (options.binary or target_dir / options.profile / ("rustfs.exe" if os.name == "nt" else "rustfs")).resolve()
    try:
        validate_target_directory(target_dir)
        requested = feature_set(options.features)
        if options.mode == "build":
            binary.parent.mkdir(parents=True, exist_ok=True)
        with exclusive_binary(binary):
            if options.mode == "build":
                if options.binary or command:
                    raise ValueError("build does not accept --binary or a child command")
                build(binary, target_dir, options.profile, requested, options.bins)
                return 0
            if options.bins:
                raise ValueError("--bins is a build option")
            return run(binary, options.profile, requested, command)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"E2E prerequisite failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
    raise SystemExit(main())
