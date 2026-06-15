#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import ipaddress
import json
import os
import shutil
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_ROOT = Path("artifacts/minio-fixture-lab")
DEFAULT_WORK_ROOT = DEFAULT_ROOT / "_runner"
DEFAULT_MINIO_BINARY = (
    Path(__file__).resolve().parents[4]
    / "tmp"
    / "minio.darwin-arm64.RELEASE.2025-09-07T16-13-09Z"
)
DEFAULT_KMS_KEY_ID = "minio-default-key"
DEFAULT_KMS_SECRET_KEY = (
    "minio-default-key:IyqsU3kMFloCNup4BsZtf/rmfHVcTgznO2F25CkEH1g="
)
LAB_KMS_SECRET_KEY_ENV = "MINIO_FIXTURE_LAB_KMS_SECRET_KEY"
DEFAULT_ENDPOINT = "http://127.0.0.1:9000"
DEFAULT_DISK_COUNT = 4
DEFAULT_ACCESS_KEY = "minioadmin"
DEFAULT_SECRET_KEY = "minioadmin"
DEFAULT_REGION = "us-east-1"
MULTIPART_CHUNK_SIZE = 5 * 1024 * 1024
DEFAULT_BUCKET = "demo"
DEFAULT_OBJECT = "dir/object.bin"
DEFAULT_SSE_C_KEY_BYTES = bytes(range(32))
DEFAULT_SSE_C_KEY_B64 = base64.b64encode(DEFAULT_SSE_C_KEY_BYTES).decode("ascii")
DEFAULT_SSE_C_KEY_MD5_B64 = base64.b64encode(hashlib.md5(DEFAULT_SSE_C_KEY_BYTES).digest()).decode("ascii")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def copy_optional_file(source: Path | None, destination: Path) -> str | None:
    if source is None:
        return None
    if not source.is_file():
        raise FileNotFoundError(f"expected file: {source}")
    shutil.copy2(source, destination)
    return destination.name


def copy_tree(source: Path, destination: Path) -> list[str]:
    if not source.exists():
        raise FileNotFoundError(f"source tree does not exist: {source}")
    if source.is_file():
        raise ValueError(f"source tree must be a directory: {source}")
    if destination.exists():
        shutil.rmtree(destination)
    shutil.copytree(source, destination)
    return sorted(
        str(path.relative_to(destination)).replace("\\", "/")
        for path in destination.rglob("*")
        if path.is_file()
    )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def format_context_header(context: dict[str, str] | None) -> str | None:
    if not context:
        return None
    json_bytes = json.dumps(context, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(json_bytes).decode("ascii")


def format_size_label(size_bytes: int) -> str:
    if size_bytes == 64 * 1024:
        return "64k"
    if size_bytes == 8 * 1024 * 1024:
        return "8m"
    return f"{size_bytes}b"


@dataclass
class LabPaths:
    root: Path
    cases: Path


@dataclass
class MinioLauncher:
    kind: str
    command: list[str]
    workdir: Path


@dataclass
class FixtureCase:
    case_id: str
    bucket: str
    object_name: str
    encryption: str
    size_bytes: int
    multipart: bool
    kms_key_id: str | None = None
    kms_context: dict[str, str] | None = None


@dataclass(frozen=True)
class KmsSecretKeyConfig:
    secret_key: str
    key_id: str


def resolve_lab_paths(root: Path) -> LabPaths:
    root = root.resolve()
    return LabPaths(root=root, cases=root / "cases")


def resolve_existing_command(name: str) -> Path:
    resolved = shutil.which(name)
    if resolved is None:
        raise FileNotFoundError(f"required command not found in PATH: {name}")
    return Path(resolved).resolve()


def discover_minio_launcher(
    explicit_binary: Path | None, minio_root: Path | None
) -> MinioLauncher:
    if explicit_binary is not None:
        binary = explicit_binary.resolve()
        if not binary.is_file():
            raise FileNotFoundError(f"minio binary not found: {binary}")
        return MinioLauncher(kind="binary", command=[str(binary)], workdir=binary.parent)

    resolved_root = minio_root.resolve() if minio_root is not None else None
    if resolved_root is not None:
        candidate_binary = resolved_root / "minio.exe"
        if candidate_binary.is_file():
            return MinioLauncher(
                kind="binary",
                command=[str(candidate_binary.resolve())],
                workdir=candidate_binary.resolve().parent,
            )

    if DEFAULT_MINIO_BINARY.is_file():
        binary = DEFAULT_MINIO_BINARY.resolve()
        return MinioLauncher(kind="binary", command=[str(binary)], workdir=binary.parent)

    path_binary = shutil.which("minio")
    if path_binary is not None:
        binary = Path(path_binary).resolve()
        return MinioLauncher(kind="binary", command=[str(binary)], workdir=binary.parent)

    raise FileNotFoundError(
        "unable to locate MinIO. Provide --minio-binary, point --minio-root at a directory "
        "containing minio.exe, or ensure minio is in PATH."
    )


def parse_kms_secret_key(secret_key: str) -> KmsSecretKeyConfig:
    key_id, separator, raw_key = secret_key.partition(":")
    if not separator or not key_id or not raw_key:
        raise ValueError(
            "kms secret key must be in the form '<key-id>:<base64-32byte-key>'"
        )
    return KmsSecretKeyConfig(secret_key=secret_key, key_id=key_id)


def resolve_kms_secret_key(explicit_secret_key: str | None) -> KmsSecretKeyConfig:
    secret_key = (
        explicit_secret_key
        or os.environ.get(LAB_KMS_SECRET_KEY_ENV)
        or os.environ.get("MINIO_KMS_SECRET_KEY")
        or DEFAULT_KMS_SECRET_KEY
    )
    return parse_kms_secret_key(secret_key)


def build_default_cases(kms_key_id: str = DEFAULT_KMS_KEY_ID) -> list[FixtureCase]:
    kms_context = {"project": "rio-v2", "stage": "fixture"}
    cases = []
    for encryption in ("SSE-S3", "SSE-KMS", "SSE-C"):
        for multipart, size_bytes in ((False, 64 * 1024), (True, 8 * 1024 * 1024)):
            shape = "multipart" if multipart else "singlepart"
            case = FixtureCase(
                case_id=f"{encryption.lower()}-{shape}-{format_size_label(size_bytes)}",
                bucket=DEFAULT_BUCKET,
                object_name=DEFAULT_OBJECT,
                encryption=encryption,
                size_bytes=size_bytes,
                multipart=multipart,
                kms_key_id=kms_key_id if encryption == "SSE-KMS" else None,
                kms_context=kms_context if encryption == "SSE-KMS" else None,
            )
            cases.append(case)
    return cases


def build_request_record(case: FixtureCase) -> dict[str, Any]:
    headers: dict[str, str] = {}
    if case.encryption == "SSE-S3":
        headers["x-amz-server-side-encryption"] = "AES256"
    elif case.encryption == "SSE-KMS":
        headers["x-amz-server-side-encryption"] = "aws:kms"
        if case.kms_key_id is not None:
            headers["x-amz-server-side-encryption-aws-kms-key-id"] = case.kms_key_id
        context_header = format_context_header(case.kms_context)
        if context_header is not None:
            headers["x-amz-server-side-encryption-context"] = context_header
    elif case.encryption == "SSE-C":
        headers["x-amz-server-side-encryption-customer-algorithm"] = "AES256"
        headers["x-amz-server-side-encryption-customer-key"] = DEFAULT_SSE_C_KEY_B64
        headers["x-amz-server-side-encryption-customer-key-md5"] = DEFAULT_SSE_C_KEY_MD5_B64
    else:
        raise ValueError(f"unsupported encryption mode: {case.encryption}")

    record: dict[str, Any] = {
        "case_id": case.case_id,
        "bucket": case.bucket,
        "object": case.object_name,
        "encryption": case.encryption,
        "multipart": case.multipart,
        "size_bytes": case.size_bytes,
        "headers": headers,
    }
    if case.multipart:
        record["multipart_chunk_size_bytes"] = MULTIPART_CHUNK_SIZE
    return record


def cmd_init(args: argparse.Namespace) -> int:
    paths = resolve_lab_paths(args.root)
    ensure_layout(paths)
    print(str(paths.root))
    return 0


def ensure_layout(paths: LabPaths) -> None:
    ensure_dir(paths.root)
    ensure_dir(paths.cases)
    write_json(
        paths.root / "layout.json",
        {
            "schema_version": 1,
            "created_at_utc": utc_now(),
            "root": ".",
            "description": "Captured MinIO fixture cases for RustFS compatibility validation.",
        },
    )


def store_case_artifacts(
    *,
    paths: LabPaths,
    case_id: str,
    bucket: str,
    object_name: str,
    source_tree: Path,
    version_id: str | None,
    request_payload: dict[str, Any] | None,
    head_payload: dict[str, Any] | None,
    plaintext_sha256: str | None,
    notes: str | None,
    capture_payload: dict[str, Any] | None = None,
) -> Path:
    ensure_dir(paths.cases)
    case_dir = paths.cases / case_id
    backend_dir = case_dir / "backend"
    if case_dir.exists():
        shutil.rmtree(case_dir)
    ensure_dir(case_dir)

    exported_files = copy_tree(source_tree.resolve(), backend_dir)
    request_file = None
    if request_payload is not None:
        write_json(case_dir / "request.json", request_payload)
        request_file = "request.json"

    head_file = None
    if head_payload is not None:
        write_json(case_dir / "head.json", head_payload)
        head_file = "head.json"

    plaintext_file = None
    if plaintext_sha256 is not None:
        (case_dir / "plaintext.sha256").write_text(plaintext_sha256 + "\n", encoding="utf-8")
        plaintext_file = "plaintext.sha256"

    manifest = {
        "schema_version": 1,
        "case_id": case_id,
        "created_at_utc": utc_now(),
        "bucket": bucket,
        "object": object_name,
        "version_id": version_id,
        "source_tree": str(source_tree).replace("\\", "/"),
        "notes": notes,
        "artifacts": {
            "backend_dir": "backend",
            "request_json": request_file,
            "head_json": head_file,
            "plaintext_sha256": plaintext_file,
        },
        "backend_files": exported_files,
    }
    if capture_payload is not None:
        manifest["capture"] = capture_payload
    write_json(case_dir / "manifest.json", manifest)
    return case_dir


def cmd_add_case(args: argparse.Namespace) -> int:
    paths = resolve_lab_paths(args.root)
    case_dir = store_case_artifacts(
        paths=paths,
        case_id=args.case_id,
        bucket=args.bucket,
        object_name=args.object,
        source_tree=args.source_tree,
        version_id=args.version_id,
        request_payload=read_optional_json(args.request_json.resolve() if args.request_json else None),
        head_payload=read_optional_json(args.head_json.resolve() if args.head_json else None),
        plaintext_sha256=(
            args.plaintext_sha256.resolve().read_text(encoding="utf-8").strip()
            if args.plaintext_sha256
            else None
        ),
        notes=args.notes,
    )
    print(str(case_dir))
    return 0


def parse_endpoint(endpoint: str) -> tuple[str, int]:
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported endpoint scheme: {endpoint}")
    if parsed.hostname is None or parsed.port is None:
        raise ValueError(f"endpoint must include host and port: {endpoint}")
    return parsed.hostname, parsed.port


def is_https_endpoint(endpoint: str) -> bool:
    return urlparse(endpoint).scheme == "https"


def ensure_local_tls_certificates(certs_dir: Path, hostname: str) -> None:
    public_crt = certs_dir / "public.crt"
    private_key = certs_dir / "private.key"
    if public_crt.is_file() and private_key.is_file():
        return

    ensure_dir(certs_dir)
    openssl = resolve_existing_command("openssl")
    san_entries = ["DNS:localhost", "IP:127.0.0.1"]
    try:
        ipaddress.ip_address(hostname)
        san_entries.append(f"IP:{hostname}")
    except ValueError:
        san_entries.append(f"DNS:{hostname}")
    config_path = certs_dir / "openssl.cnf"
    config_path.write_text(
        "\n".join(
            [
                "[req]",
                "distinguished_name = req_distinguished_name",
                "x509_extensions = v3_req",
                "prompt = no",
                "[req_distinguished_name]",
                f"CN = {hostname}",
                "[v3_req]",
                f"subjectAltName = {','.join(san_entries)}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    subprocess.run(
        [
            str(openssl),
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "30",
            "-keyout",
            str(private_key),
            "-out",
            str(public_crt),
            "-config",
            str(config_path),
            "-extensions",
            "v3_req",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def build_server_command(
    launcher: MinioLauncher, endpoint: str, disk_paths: list[Path], certs_dir: Path | None = None
) -> list[str]:
    host, port = parse_endpoint(endpoint)
    normalized_disk_paths = [str(path.resolve()).replace("\\", "/") for path in disk_paths]
    command = launcher.command + [
        "server",
        "--address",
        f"{host}:{port}",
        "--console-address",
        f"{host}:{port + 1}",
    ]
    if certs_dir is not None:
        command += ["--certs-dir", str(certs_dir.resolve())]
    command += normalized_disk_paths
    return command


def create_disk_paths(workdir: Path, disk_count: int) -> list[Path]:
    disks = []
    for index in range(1, disk_count + 1):
        disk = ensure_dir(workdir / "backend" / f"disk{index}")
        disks.append(disk)
    return disks


def build_payload_file(path: Path, size_bytes: int) -> str:
    hasher = hashlib.sha256()
    remaining = size_bytes
    pattern = bytes(range(251))
    with path.open("wb") as handle:
        while remaining > 0:
            chunk = pattern[: min(len(pattern), remaining)]
            handle.write(chunk)
            hasher.update(chunk)
            remaining -= len(chunk)
    return hasher.hexdigest()


def minio_env(kms_secret_key: KmsSecretKeyConfig) -> dict[str, str]:
    env = dict(os.environ)
    env["CI"] = "on"
    env["MINIO_CI_CD"] = "1"
    env["MINIO_ROOT_USER"] = "minioadmin"
    env["MINIO_ROOT_PASSWORD"] = "minioadmin"
    env["MINIO_KMS_SECRET_KEY"] = kms_secret_key.secret_key
    env["MINIO_UPDATE"] = "off"
    return env


class S3Client:
    def __init__(
        self,
        endpoint: str,
        access_key: str = DEFAULT_ACCESS_KEY,
        secret_key: str = DEFAULT_SECRET_KEY,
        region: str = DEFAULT_REGION,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.parsed_endpoint = urlparse(self.endpoint)
        self.ssl_context = ssl._create_unverified_context() if is_https_endpoint(endpoint) else None

    def request(
        self,
        method: str,
        bucket: str | None = None,
        key: str | None = None,
        *,
        query: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
    ) -> tuple[int, dict[str, str], bytes]:
        path = "/"
        if bucket is not None:
            path += urllib.parse.quote(bucket, safe="")
            if key is not None:
                path += "/" + urllib.parse.quote(key, safe="/")
        query = query or {}
        headers = dict(headers or {})
        signed_headers = self.sign_headers(method, path, query, headers, body)
        url = self.endpoint + path
        if query:
            url += "?" + urllib.parse.urlencode(sorted(query.items()), quote_via=urllib.parse.quote)
        request = urllib.request.Request(url, data=body if method != "HEAD" else None, method=method)
        for name, value in signed_headers.items():
            request.add_header(name, value)
        try:
            with urllib.request.urlopen(request, timeout=30, context=self.ssl_context) as response:
                return response.status, {k.lower(): v for k, v in response.headers.items()}, response.read()
        except urllib.error.HTTPError as err:
            payload = err.read()
            raise RuntimeError(
                f"S3 request failed: {method} {url}\nstatus: {err.code}\nbody:\n{payload.decode('utf-8', errors='replace')}"
            ) from err

    def sign_headers(
        self,
        method: str,
        canonical_uri: str,
        query: dict[str, str],
        headers: dict[str, str],
        body: bytes,
    ) -> dict[str, str]:
        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body).hexdigest()
        host = self.parsed_endpoint.netloc
        signed_headers = {k.lower(): v.strip() for k, v in headers.items()}
        signed_headers["host"] = host
        signed_headers["x-amz-content-sha256"] = payload_hash
        signed_headers["x-amz-date"] = amz_date

        canonical_query = "&".join(
            f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(v, safe='-_.~')}"
            for k, v in sorted(query.items())
        )
        canonical_headers = "".join(f"{k}:{signed_headers[k]}\n" for k in sorted(signed_headers))
        signed_header_names = ";".join(sorted(signed_headers))
        canonical_request = "\n".join(
            [
                method,
                canonical_uri,
                canonical_query,
                canonical_headers,
                signed_header_names,
                payload_hash,
            ]
        )
        scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = hmac.new(
            self.signing_key(date_stamp),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed_headers["authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={self.access_key}/{scope}, "
            f"SignedHeaders={signed_header_names}, Signature={signature}"
        )
        return signed_headers

    def signing_key(self, date_stamp: str) -> bytes:
        key = ("AWS4" + self.secret_key).encode("utf-8")
        for value in (date_stamp, self.region, "s3", "aws4_request"):
            key = hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()
        return key

    def list_buckets(self) -> None:
        self.request("GET")

    def create_bucket(self, bucket: str) -> None:
        self.request("PUT", bucket=bucket)

    def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes,
        headers: dict[str, str],
    ) -> None:
        self.request("PUT", bucket=bucket, key=key, headers=headers, body=body)

    def create_multipart_upload(self, bucket: str, key: str, headers: dict[str, str]) -> str:
        _, _, body = self.request("POST", bucket=bucket, key=key, query={"uploads": ""}, headers=headers)
        root = ET.fromstring(body)
        upload_id = root.findtext("{*}UploadId")
        if upload_id is None:
            raise RuntimeError(f"missing UploadId in create multipart response: {body!r}")
        return upload_id

    def upload_part(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        part_number: int,
        body: bytes,
        headers: dict[str, str],
    ) -> str:
        _, response_headers, _ = self.request(
            "PUT",
            bucket=bucket,
            key=key,
            query={"partNumber": str(part_number), "uploadId": upload_id},
            headers=headers,
            body=body,
        )
        etag = response_headers.get("etag")
        if etag is None:
            raise RuntimeError("missing ETag in upload part response")
        return etag

    def complete_multipart_upload(
        self,
        bucket: str,
        key: str,
        upload_id: str,
        parts: list[dict[str, Any]],
    ) -> None:
        body = build_complete_multipart_xml(parts)
        self.request(
            "POST",
            bucket=bucket,
            key=key,
            query={"uploadId": upload_id},
            headers={"content-type": "application/xml"},
            body=body,
        )

    def head_object(self, bucket: str, key: str, headers: dict[str, str]) -> dict[str, Any]:
        _, response_headers, _ = self.request("HEAD", bucket=bucket, key=key, headers=headers)
        return {
            "ContentLength": int(response_headers.get("content-length", "0")),
            "ServerSideEncryption": response_headers.get("x-amz-server-side-encryption"),
            "SSECustomerAlgorithm": response_headers.get("x-amz-server-side-encryption-customer-algorithm"),
            "SSECustomerKeyMD5": response_headers.get("x-amz-server-side-encryption-customer-key-md5"),
            "SSEKMSKeyId": response_headers.get("x-amz-server-side-encryption-aws-kms-key-id"),
            "VersionId": response_headers.get("x-amz-version-id"),
        }


def wait_for_minio(endpoint: str, process: subprocess.Popen[str], timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    health_url = endpoint.rstrip("/") + "/minio/health/live"
    ssl_context = ssl._create_unverified_context() if is_https_endpoint(endpoint) else None
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("MinIO exited before becoming healthy")
        try:
            with urllib.request.urlopen(health_url, timeout=2, context=ssl_context) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(1)
    raise TimeoutError(f"timed out waiting for MinIO health endpoint: {health_url}")


def wait_for_s3_ready(client: S3Client, timeout_seconds: int) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            client.list_buckets()
            return
        except RuntimeError:
            time.sleep(1)
    raise TimeoutError(f"timed out waiting for S3 API readiness: {client.endpoint}")


def create_bucket(client: S3Client, case: FixtureCase) -> None:
    client.create_bucket(case.bucket)


def build_complete_multipart_xml(parts: list[dict[str, Any]]) -> bytes:
    root = ET.Element("CompleteMultipartUpload")
    for part in parts:
        part_element = ET.SubElement(root, "Part")
        ET.SubElement(part_element, "PartNumber").text = str(part["PartNumber"])
        ET.SubElement(part_element, "ETag").text = part["ETag"]
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def upload_case(client: S3Client, case: FixtureCase, payload_file: Path) -> dict[str, Any]:
    request_record = build_request_record(case)
    headers = request_record["headers"]
    headers["content-type"] = "binary/octet-stream"

    if not case.multipart:
        client.put_object(case.bucket, case.object_name, payload_file.read_bytes(), headers)
        return request_record

    upload_id = client.create_multipart_upload(case.bucket, case.object_name, headers)

    parts = []
    with payload_file.open("rb") as handle:
        part_number = 1
        while True:
            chunk = handle.read(MULTIPART_CHUNK_SIZE)
            if not chunk:
                break
            part_file = payload_file.parent / f"part-{part_number}.bin"
            part_file.write_bytes(chunk)
            part_headers = headers if case.encryption == "SSE-C" else {}
            etag = client.upload_part(
                case.bucket,
                case.object_name,
                upload_id,
                part_number,
                chunk,
                part_headers,
            )
            parts.append({"ETag": etag, "PartNumber": part_number})
            part_number += 1

    client.complete_multipart_upload(case.bucket, case.object_name, upload_id, parts)
    return request_record


def head_case(client: S3Client, case: FixtureCase) -> dict[str, Any]:
    headers = build_request_record(case)["headers"] if case.encryption == "SSE-C" else {}
    return client.head_object(case.bucket, case.object_name, headers)


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=15)


def capture_case(
    *,
    paths: LabPaths,
    launcher: MinioLauncher,
    endpoint: str,
    case: FixtureCase,
    work_root: Path,
    disk_count: int,
    timeout_seconds: int,
    preserve_workdir: bool,
    kms_secret_key: KmsSecretKeyConfig,
) -> Path:
    case_workdir = work_root / case.case_id
    if case_workdir.exists():
        shutil.rmtree(case_workdir)
    ensure_dir(case_workdir)

    payload_file = case_workdir / "payload.bin"
    plaintext_sha256 = build_payload_file(payload_file, case.size_bytes)
    disk_paths = create_disk_paths(case_workdir, disk_count)
    certs_dir = None
    if is_https_endpoint(endpoint):
        host, _ = parse_endpoint(endpoint)
        certs_dir = case_workdir / "certs"
        ensure_local_tls_certificates(certs_dir, host)
    server_command = build_server_command(launcher, endpoint, disk_paths, certs_dir)
    server_log_path = case_workdir / "server.log"

    with server_log_path.open("w", encoding="utf-8") as server_log:
        process = subprocess.Popen(
            server_command,
            cwd=str(launcher.workdir),
            env=minio_env(kms_secret_key),
            stdout=server_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            client = S3Client(endpoint)
            wait_for_minio(endpoint, process, timeout_seconds)
            wait_for_s3_ready(client, timeout_seconds)
            create_bucket(client, case)
            request_payload = upload_case(client, case, payload_file)
            head_payload = head_case(client, case)
        finally:
            stop_process(process)

    case_dir = store_case_artifacts(
        paths=paths,
        case_id=case.case_id,
        bucket=case.bucket,
        object_name=case.object_name,
        source_tree=case_workdir / "backend",
        version_id=head_payload.get("VersionId") if "head_payload" in locals() else None,
        request_payload=request_payload if "request_payload" in locals() else None,
        head_payload=head_payload if "head_payload" in locals() else None,
        plaintext_sha256=plaintext_sha256,
        notes="Captured automatically from a local disposable MinIO run.",
        capture_payload={
            "endpoint": endpoint,
            "launcher_kind": launcher.kind,
            "disk_count": disk_count,
            "kms_key_id": kms_secret_key.key_id,
        },
    )

    if not preserve_workdir:
        shutil.rmtree(case_workdir)
    return case_dir


def select_cases(case_ids: list[str] | None) -> list[FixtureCase]:
    cases = build_default_cases()
    if not case_ids:
        return cases
    selected = [case for case in cases if case.case_id in set(case_ids)]
    if len(selected) != len(set(case_ids)):
        known = ", ".join(case.case_id for case in cases)
        missing = sorted(set(case_ids) - {case.case_id for case in selected})
        raise ValueError(f"unknown case id(s): {', '.join(missing)}; known: {known}")
    return selected


def cmd_capture_matrix(args: argparse.Namespace) -> int:
    paths = resolve_lab_paths(args.root)
    ensure_layout(paths)
    ensure_dir(args.work_root)

    launcher = discover_minio_launcher(args.minio_binary, args.minio_root)
    kms_secret_key = resolve_kms_secret_key(args.kms_secret_key)
    cases = build_default_cases(kms_key_id=kms_secret_key.key_id)
    if args.case_id:
        selected_ids = set(args.case_id)
        cases = [case for case in cases if case.case_id in selected_ids]
        if len(cases) != len(selected_ids):
            known = ", ".join(case.case_id for case in build_default_cases())
            missing = sorted(selected_ids - {case.case_id for case in cases})
            raise ValueError(f"unknown case id(s): {', '.join(missing)}; known: {known}")
    for case in cases:
        case_dir = capture_case(
            paths=paths,
            launcher=launcher,
            endpoint=args.endpoint,
            case=case,
            work_root=args.work_root,
            disk_count=args.disk_count,
            timeout_seconds=args.timeout_seconds,
            preserve_workdir=args.preserve_workdir,
            kms_secret_key=kms_secret_key,
        )
        print(f"{case.case_id}: {case_dir}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture real MinIO backend fixtures into a stable local layout."
    )
    parser.set_defaults(func=None)
    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser("init", help="initialize the fixture lab root")
    init_parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="fixture lab root directory")
    init_parser.set_defaults(func=cmd_init)

    add_case_parser = subparsers.add_parser("add-case", help="capture one fixture case")
    add_case_parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="fixture lab root directory")
    add_case_parser.add_argument("--case-id", required=True, help="stable fixture case identifier")
    add_case_parser.add_argument("--bucket", required=True, help="bucket name")
    add_case_parser.add_argument("--object", required=True, help="object key")
    add_case_parser.add_argument("--version-id", help="object version id")
    add_case_parser.add_argument("--source-tree", type=Path, required=True, help="backend object tree to capture")
    add_case_parser.add_argument("--request-json", type=Path, help="request metadata JSON")
    add_case_parser.add_argument("--head-json", type=Path, help="HEAD object metadata JSON")
    add_case_parser.add_argument("--plaintext-sha256", type=Path, help="plaintext sha256 file")
    add_case_parser.add_argument("--notes", help="free-form notes")
    add_case_parser.set_defaults(func=cmd_add_case)

    capture_parser = subparsers.add_parser(
        "capture-matrix",
        help="run a disposable local MinIO instance and capture the default fixture matrix",
    )
    capture_parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="fixture lab root directory")
    capture_parser.add_argument(
        "--work-root",
        type=Path,
        default=DEFAULT_WORK_ROOT,
        help="temporary runner workspace",
    )
    capture_parser.add_argument(
        "--minio-binary",
        type=Path,
        help="explicit path to a minio binary",
    )
    capture_parser.add_argument(
        "--minio-root",
        type=Path,
        help="optional directory containing minio.exe",
    )
    capture_parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help="local MinIO endpoint used for the disposable run",
    )
    capture_parser.add_argument(
        "--kms-secret-key",
        help=(
            "static MinIO KMS secret key in the form "
            "'<key-id>:<base64-32byte-key>'; overrides "
            f"{LAB_KMS_SECRET_KEY_ENV}, MINIO_KMS_SECRET_KEY, and the built-in default"
        ),
    )
    capture_parser.add_argument(
        "--disk-count",
        type=int,
        default=DEFAULT_DISK_COUNT,
        help="number of backend disks to provision per case",
    )
    capture_parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="startup timeout for MinIO health checks",
    )
    capture_parser.add_argument(
        "--preserve-workdir",
        action="store_true",
        help="keep per-case runner work directories after capture",
    )
    capture_parser.add_argument(
        "--case-id",
        action="append",
        help="capture only specific default case ids",
    )
    capture_parser.set_defaults(func=cmd_capture_matrix)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.func is None:
        parser.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
