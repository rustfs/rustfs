#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import platform
import secrets
import shutil
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from lab import (
    FixtureCase,
    LabPaths,
    S3Client,
    build_payload_file,
    head_case,
    stop_process,
    store_case_artifacts,
    upload_case,
    wait_for_s3_ready,
)


RELEASE = "1.0.0-beta.5"
CASE_ID = "rustfs-beta5-sse-kms-singlepart-64k"
KMS_KEY_ID = "beta5-test-key"
LINUX_X86_64_ASSET = "rustfs-linux-x86_64-gnu-v1.0.0-beta.5.zip"
LINUX_X86_64_SHA256 = "73529e732adc3c2c5c78c7f2c2e4e331a20e5bee1e2db341ee7c762299e4b327"
DEFAULT_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "rustfs-beta5-generated"


def release_asset_url(asset: str) -> str:
    return f"https://github.com/rustfs/rustfs/releases/download/{RELEASE}/{asset}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_binary(explicit_binary: Path | None, workdir: Path) -> Path:
    if explicit_binary is not None:
        binary = explicit_binary.resolve()
        if not binary.is_file():
            raise FileNotFoundError(f"RustFS beta.5 binary not found: {binary}")
        return binary

    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
        raise RuntimeError("--rustfs-binary is required outside Linux x86_64")

    archive = workdir / LINUX_X86_64_ASSET
    urllib.request.urlretrieve(release_asset_url(LINUX_X86_64_ASSET), archive)
    actual = sha256_file(archive)
    if actual != LINUX_X86_64_SHA256:
        raise RuntimeError(f"RustFS beta.5 archive SHA-256 mismatch: expected {LINUX_X86_64_SHA256}, got {actual}")
    with zipfile.ZipFile(archive) as bundle:
        bundle.extract("rustfs", workdir)
    binary = workdir / "rustfs"
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    return binary


def write_local_kms_key(key_dir: Path) -> None:
    key_dir.mkdir(parents=True, mode=0o700)
    key_path = key_dir / f"{KMS_KEY_ID}.key"
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    payload = {
        "key_id": KMS_KEY_ID,
        "version": 1,
        "algorithm": "AES_256",
        "usage": "EncryptDecrypt",
        "status": "Active",
        "description": None,
        "metadata": {},
        "created_at": now,
        "rotated_at": None,
        "created_by": "fixture-lab",
        "encrypted_key_material": base64.b64encode(secrets.token_bytes(32)).decode("ascii"),
        "nonce": [],
    }
    key_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    key_path.chmod(0o600)


def wait_for_process_s3(client: S3Client, process: subprocess.Popen[str], timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("RustFS beta.5 exited before its S3 API became ready")
        try:
            client.list_buckets()
            return
        except (RuntimeError, urllib.error.URLError):
            time.sleep(1)
    wait_for_s3_ready(client, 1)


def capture(args: argparse.Namespace) -> Path:
    root = args.root.resolve()
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)

    with tempfile.TemporaryDirectory(prefix="rustfs-beta5-fixture-") as temporary:
        workdir = Path(temporary)
        binary = resolve_binary(args.rustfs_binary, workdir)
        key_dir = workdir / "kms"
        write_local_kms_key(key_dir)
        disks = [workdir / f"disk{index}" for index in range(1, 5)]
        for disk in disks:
            disk.mkdir()

        endpoint = args.endpoint
        parsed = urlparse(endpoint)
        if parsed.scheme != "http" or not parsed.netloc:
            raise ValueError("beta.5 fixture endpoint must be an http:// URL")
        command = [
            str(binary),
            "server",
            "--address",
            parsed.netloc,
            "--access-key",
            "minioadmin",
            "--secret-key",
            "minioadmin",
            "--kms-enable",
            "--kms-backend",
            "local",
            "--kms-key-dir",
            str(key_dir),
            "--kms-default-key-id",
            KMS_KEY_ID,
            *(str(disk) for disk in disks),
        ]
        environment = os.environ.copy()
        environment["RUSTFS_UNSAFE_BYPASS_DISK_CHECK"] = "true"
        server_log_path = workdir / "server.log"
        case = FixtureCase(
            case_id=CASE_ID,
            bucket="demo",
            object_name="dir/object.bin",
            encryption="SSE-KMS",
            size_bytes=64 * 1024,
            multipart=False,
            kms_key_id=KMS_KEY_ID,
        )
        payload_file = workdir / "payload.bin"
        plaintext_sha256 = build_payload_file(payload_file, case.size_bytes)

        with server_log_path.open("w", encoding="utf-8") as server_log:
            process = subprocess.Popen(
                command,
                cwd=str(workdir),
                env=environment,
                stdout=server_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            try:
                client = S3Client(endpoint)
                wait_for_process_s3(client, process, args.timeout_seconds)
                client.create_bucket(case.bucket)
                request_payload = upload_case(client, case, payload_file)
                head_payload = head_case(client, case)
            finally:
                stop_process(process)

        export_root = workdir / "export"
        export_root.mkdir()
        for index, disk in enumerate(disks, start=1):
            shutil.copytree(disk, export_root / f"disk{index}")
        shutil.copytree(key_dir, root / "kms")
        return store_case_artifacts(
            paths=LabPaths(root=root, cases=root / "cases"),
            case_id=case.case_id,
            bucket=case.bucket,
            object_name=case.object_name,
            source_tree=export_root,
            version_id=head_payload.get("VersionId"),
            request_payload=request_payload,
            head_payload=head_payload,
            plaintext_sha256=plaintext_sha256,
            notes="Captured from the pinned RustFS 1.0.0-beta.5 release with the production local KMS backend.",
            capture_payload={
                "release": RELEASE,
                "binary_sha256": sha256_file(binary),
                "disk_count": len(disks),
                "kms_key_id": KMS_KEY_ID,
            },
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture a real RustFS beta.5 SSE-KMS fixture")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--rustfs-binary", type=Path)
    parser.add_argument("--endpoint", default="http://127.0.0.1:19011")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    return parser


def main() -> int:
    case_dir = capture(build_parser().parse_args())
    print(case_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
