"""Core library of the SSE black-box suite: one real server process, signed HTTP, and what a client can observe.

Everything a case does to the product goes through `signed_request`, which also records the exchange as an
observation (see observe.py). Standard library only, so the suite runs with `uv run --no-project`.
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import http.client
import json
import os
import re
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

import observe

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
STATE_DIR = Path(os.environ.get("SSE_BB_STATE_DIR", REPO_ROOT / "target" / "sse-bb"))

ADMIN_PREFIX = "/rustfs/admin/v3"
REGION = "us-east-1"
SERVICE = "s3"
UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"
# Added by the client to a response whose body ended before the announced length; never sent by the server.
TRUNCATED = "x-harness-body-truncated"
ACCESS_KEY = "rustfsadmin"
SECRET_KEY = "rustfsadmin"
# The port is a local-development choice; CI and other machines override it.
DEFAULT_PORT = int(os.environ.get("SSE_BB_PORT", "20105"))

STARTUP_TIMEOUT = 90.0
STOP_GRACE = 10.0
KMS_PROBE_INTERVAL = 5  # the product floors RUSTFS_KMS_PROBE_INTERVAL_SECS at 5 seconds

DEFAULT_KEY = "bb-default"
KEY_1 = "bb-key-1"
KEY_2 = "bb-key-2"
MISSING_KEY = "bb-key-that-was-never-created"

KIB = 1024
MIB = 1024 * 1024


class CheckFailed(Exception):
    """A hard prerequisite of a case does not hold; the case cannot continue."""


class EnvError(Exception):
    """The environment, not the product, is at fault: the site is down or a dependency is missing."""


# ---------------------------------------------------------------------------
# Deterministic test data
# ---------------------------------------------------------------------------


def plain(seed: str, size: int) -> bytes:
    """Incompressible bytes fully determined by `seed`, so two runs expect the same body hashes."""
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(f"sse-blackbox plaintext {seed} {counter}".encode()).digest()
        counter += 1
    return bytes(out[:size])


def derived_key(label: str, size: int = 32) -> bytes:
    """Key material derived at run time; the repository carries no key literal."""
    return hashlib.sha256(f"sse-blackbox key {label}".encode()).digest()[:size]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Site
# ---------------------------------------------------------------------------


@dataclass
class Site:
    name: str
    root: Path
    port: int = DEFAULT_PORT
    access_key: str = ACCESS_KEY
    secret_key: str = SECRET_KEY
    host: str = "127.0.0.1"
    env: dict = field(default_factory=dict)
    proc: subprocess.Popen | None = None

    @property
    def endpoint(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def volume_dir(self) -> Path:
        return self.root / "volume"

    @property
    def log_dir(self) -> Path:
        return self.root / "logs"

    @property
    def stdout_log(self) -> Path:
        return self.log_dir / "stdout.log"

    @property
    def app_log(self) -> Path:
        """JSON-lines application log written under RUSTFS_OBS_LOG_DIRECTORY."""
        return self.log_dir / "rustfs.log"

    @property
    def key_dir(self) -> Path:
        return self.root / "kms-keys"

    def drives(self) -> list[Path]:
        return [self.volume_dir / f"test{i}" for i in range(1, 5)]


# ---------------------------------------------------------------------------
# SigV4 and HTTP
# ---------------------------------------------------------------------------


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _uri_encode(value: str, encode_slash: bool) -> str:
    return urllib.parse.quote(value, safe="-._~" + ("" if encode_slash else "/"))


def _canonical_query(query: str) -> str:
    if not query:
        return ""
    pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
    return "&".join(f"{k}={v}" for k, v in sorted((_uri_encode(k, True), _uri_encode(v, True)) for k, v in pairs))


def _signing_key(secret_key: str, date_stamp: str) -> bytes:
    return _hmac(_hmac(_hmac(_hmac(f"AWS4{secret_key}".encode(), date_stamp), REGION), SERVICE), "aws4_request")


def _http(url: str, method: str, body: bytes | None, headers: dict[str, str], timeout: float):
    request = urllib.request.Request(url, data=body, method=method)
    for name, value in headers.items():
        if name != "host":
            request.add_header(name, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response_headers = {k.lower(): v for k, v in response.headers.items()}
            try:
                return response.status, response_headers, response.read()
            except http.client.IncompleteRead as err:
                # The server answered and then cut the body short. That is the product's doing, not the
                # environment's, so the status stands and the truncation is reported next to it.
                return response.status, {**response_headers, TRUNCATED: str(len(err.partial))}, err.partial
    except urllib.error.HTTPError as err:
        return err.code, {k.lower(): v for k, v in err.headers.items()}, err.read()
    except (urllib.error.URLError, OSError, TimeoutError, http.client.HTTPException) as err:
        # Status 0 is a transport failure: the site is down or hung. It is never a product refusal.
        return 0, {}, str(err).encode()


def signed_request(
    s: Site,
    method: str,
    path: str,
    query: str = "",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    creds: tuple[str, str] | None = None,
    timeout: float = 120.0,
    label: str | None = None,
) -> tuple[int, dict[str, str], bytes]:
    """SigV4 header-signed request. Returns (status, lower-cased headers, body) and never raises on HTTP errors."""
    access_key, secret_key = creds if creds else (s.access_key, s.secret_key)
    now = datetime.datetime.now(datetime.UTC)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    all_headers = {"host": f"{s.host}:{s.port}", "x-amz-content-sha256": UNSIGNED_PAYLOAD, "x-amz-date": amz_date}
    if headers:
        all_headers.update({k.lower(): v for k, v in headers.items()})

    signed_names = ";".join(sorted(all_headers))
    canonical_headers = "".join(f"{k}:{all_headers[k].strip()}\n" for k in sorted(all_headers))
    canonical_request = "\n".join(
        [method, _uri_encode(path, False), _canonical_query(query), canonical_headers, signed_names, UNSIGNED_PAYLOAD]
    )
    scope = f"{date_stamp}/{REGION}/{SERVICE}/aws4_request"
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, sha256_hex(canonical_request.encode())])
    signature = hmac.new(_signing_key(secret_key, date_stamp), string_to_sign.encode(), hashlib.sha256).hexdigest()
    all_headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_names}, Signature={signature}"
    )
    url = f"{s.endpoint}{urllib.parse.quote(path, safe='/-._~')}"
    if query:
        url += f"?{query}"
    status, response_headers, response_body = _http(url, method, body, all_headers, timeout)
    observe.exchange(label, method, path, query, status, response_headers, response_body)
    if status == 0 and observe.in_case():
        raise EnvError(f"{method} {path}: no HTTP response ({response_body[:160].decode(errors='replace')})")
    return status, response_headers, response_body


def admin(s: Site, method: str, subpath: str, query: str = "", payload: object | None = None,
          creds: tuple[str, str] | None = None, raw_body: bytes | None = None,
          label: str | None = None) -> tuple[int, object]:
    """Signed admin call; returns (status, parsed JSON or text)."""
    body = json.dumps(payload).encode() if payload is not None else raw_body
    headers = {"content-type": "application/json"} if body is not None else None
    status, _, raw = signed_request(s, method, f"{ADMIN_PREFIX}/{subpath.lstrip('/')}", query, body, headers, creds,
                                    label=label)
    try:
        return status, json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return status, raw.decode(errors="replace")


def s3_error(body: bytes) -> tuple[str, str]:
    """(Code, Message) from an S3 XML error body; ('', '') when the body is not such a document."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return "", ""
    return root.findtext("{*}Code") or root.findtext("Code") or "", root.findtext("{*}Message") or root.findtext("Message") or ""


# ---------------------------------------------------------------------------
# SSE request headers and response projections
# ---------------------------------------------------------------------------

SSE = "x-amz-server-side-encryption"
SSE_KEY_ID = f"{SSE}-aws-kms-key-id"
SSE_CONTEXT = f"{SSE}-context"
SSEC = f"{SSE}-customer-"
COPY_SSEC = "x-amz-copy-source-server-side-encryption-customer-"


def sse_s3() -> dict[str, str]:
    return {SSE: "AES256"}


def sse_kms(key_id: str | None = None, context: dict | None = None) -> dict[str, str]:
    headers = {SSE: "aws:kms"}
    if key_id:
        headers[SSE_KEY_ID] = key_id
    if context is not None:
        headers[SSE_CONTEXT] = base64.b64encode(json.dumps(context, separators=(",", ":")).encode()).decode()
    return headers


def key_md5(key: bytes) -> str:
    return base64.b64encode(hashlib.md5(key).digest()).decode()


def ssec(key: bytes, prefix: str = SSEC) -> dict[str, str]:
    """The SSE-C triple. Pass `COPY_SSEC` as the prefix for the copy-source variant."""
    return {f"{prefix}algorithm": "AES256", f"{prefix}key": base64.b64encode(key).decode(), f"{prefix}key-md5": key_md5(key)}


def projection(headers: dict[str, str]) -> dict[str, str]:
    """The SSE headers actually present in a response."""
    return {k: v for k, v in headers.items() if k.startswith(SSE)}


def projection_s3() -> dict[str, str]:
    return {SSE: "AES256"}


def projection_kms(key_id: str) -> dict[str, str]:
    return {SSE: "aws:kms", SSE_KEY_ID: key_id}


def projection_c(key: bytes) -> dict[str, str]:
    return {f"{SSEC}algorithm": "AES256", f"{SSEC}key-md5": key_md5(key)}


# ---------------------------------------------------------------------------
# S3 operations
# ---------------------------------------------------------------------------


def make_bucket(s: Site, bucket: str) -> None:
    """Create a bucket, riding out the few seconds after startup in which a fresh site still answers 503."""
    deadline = time.monotonic() + 30.0
    with observe.quiet():
        while True:
            status, _, body = signed_request(s, "PUT", f"/{bucket}")
            if status in (200, 409) or (0 < status < 500) or time.monotonic() > deadline:
                break
            time.sleep(0.5)
    if status == 0:
        raise EnvError(f"CreateBucket {bucket}: no HTTP response")
    if status not in (200, 409):
        raise CheckFailed(f"CreateBucket {bucket}: HTTP {status} {s3_error(body)}")


def put(s: Site, bucket: str, key: str, data: bytes, headers=None, creds=None, label=None):
    return signed_request(s, "PUT", f"/{bucket}/{key}", body=data,
                          headers={"content-type": "application/octet-stream", **(headers or {})}, creds=creds,
                          label=label or "PutObject")


def get(s: Site, bucket: str, key: str, headers=None, creds=None, range_: str | None = None, label=None):
    request_headers = dict(headers or {})
    if range_:
        request_headers["range"] = range_
    return signed_request(s, "GET", f"/{bucket}/{key}", headers=request_headers or None, creds=creds,
                          label=label or "GetObject")


def head(s: Site, bucket: str, key: str, headers=None, creds=None, label=None):
    status, response_headers, _ = signed_request(s, "HEAD", f"/{bucket}/{key}", headers=headers, creds=creds,
                                                 label=label or "HeadObject")
    return status, response_headers


def copy(s: Site, bucket: str, key: str, src_bucket: str, src_key: str, headers=None, creds=None, label=None):
    """CopyObject. `headers` carries destination SSE headers and the copy-source SSE-C triple."""
    request_headers = {"x-amz-copy-source": f"/{src_bucket}/{urllib.parse.quote(src_key)}", **(headers or {})}
    return signed_request(s, "PUT", f"/{bucket}/{key}", headers=request_headers, creds=creds, label=label or "CopyObject")


def create_multipart(s: Site, bucket: str, key: str, headers=None, creds=None):
    status, response_headers, body = signed_request(s, "POST", f"/{bucket}/{key}", query="uploads=", headers=headers,
                                                    creds=creds, label="CreateMultipartUpload")
    upload_id = None
    if status == 200:
        upload_id = ET.fromstring(body).findtext("{*}UploadId") or ET.fromstring(body).findtext("UploadId")
    return status, response_headers, upload_id, body


def upload_part(s: Site, bucket: str, key: str, upload_id: str, part_no: int, data: bytes, headers=None, creds=None):
    return signed_request(s, "PUT", f"/{bucket}/{key}", query=f"partNumber={part_no}&uploadId={urllib.parse.quote(upload_id)}",
                          body=data, headers=headers, creds=creds, label="UploadPart")


def complete_multipart(s: Site, bucket: str, key: str, upload_id: str, parts: list[tuple[int, str]], headers=None,
                       creds=None):
    parts_xml = "".join(f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in parts)
    return signed_request(s, "POST", f"/{bucket}/{key}", query=f"uploadId={urllib.parse.quote(upload_id)}",
                          body=f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>".encode(),
                          headers={"content-type": "application/xml", **(headers or {})}, creds=creds,
                          label="CompleteMultipartUpload")


def list_parts(s: Site, bucket: str, key: str, upload_id: str):
    return signed_request(s, "GET", f"/{bucket}/{key}", query=f"uploadId={urllib.parse.quote(upload_id)}", label="ListParts")


def list_objects(s: Site, bucket: str, prefix: str = "") -> dict[str, int]:
    """{key: size} from ListObjectsV2."""
    query = "list-type=2" + (f"&prefix={urllib.parse.quote(prefix)}" if prefix else "")
    status, _, body = signed_request(s, "GET", f"/{bucket}", query=query, label="ListObjectsV2")
    if status != 200:
        raise CheckFailed(f"ListObjectsV2 {bucket}: HTTP {status} {s3_error(body)}")
    out = {}
    for contents in ET.fromstring(body).findall("{*}Contents"):
        out[contents.findtext("{*}Key")] = int(contents.findtext("{*}Size"))
    return out


def put_bucket_encryption(s: Site, bucket: str, algorithm: str, kms_key_id: str | None = None):
    key_xml = f"<KMSMasterKeyID>{kms_key_id}</KMSMasterKeyID>" if kms_key_id else ""
    payload = ('<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule>'
               f"<ApplyServerSideEncryptionByDefault><SSEAlgorithm>{algorithm}</SSEAlgorithm>{key_xml}"
               "</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>").encode()
    return signed_request(s, "PUT", f"/{bucket}", query="encryption=", body=payload,
                          headers={"content-type": "application/xml"}, label="PutBucketEncryption")


def get_bucket_encryption(s: Site, bucket: str) -> tuple[int, dict, bytes]:
    """(status, {'algorithm', 'kms_key_id'}, body)."""
    status, _, body = signed_request(s, "GET", f"/{bucket}", query="encryption=", label="GetBucketEncryption")
    config = {}
    if status == 200:
        root = ET.fromstring(body)
        config = {"algorithm": root.findtext(".//{*}SSEAlgorithm"), "kms_key_id": root.findtext(".//{*}KMSMasterKeyID")}
    return status, config, body


def delete_bucket_encryption(s: Site, bucket: str):
    return signed_request(s, "DELETE", f"/{bucket}", query="encryption=", label="DeleteBucketEncryption")


# ---------------------------------------------------------------------------
# Admin: KMS and IAM
# ---------------------------------------------------------------------------


def kms_status(s: Site):
    return admin(s, "GET", "kms/status", label="KmsStatus")


def kms_stop(s: Site):
    return admin(s, "POST", "kms/stop", label="KmsStop")


def kms_start(s: Site):
    return admin(s, "POST", "kms/start", payload={}, label="KmsStart")


def key_create(s: Site, name: str):
    return admin(s, "POST", "kms/keys", payload={"tags": {"name": name}}, label="KmsCreateKey")


def key_list(s: Site) -> list[str]:
    status, response = admin(s, "GET", "kms/keys", query="limit=100", label="KmsListKeys")
    if status != 200 or not isinstance(response, dict):
        raise CheckFailed(f"ListKeys: HTTP {status} {str(response)[:200]}")
    return sorted(str(k.get("key_id")) for k in response.get("keys", []))


def write_local_key_file(key_dir: Path, key_id: str) -> None:
    """Pre-create a Local-backend key file in the development format, with material generated for this run."""
    key_dir.mkdir(parents=True, exist_ok=True)
    created = datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    stored = {
        "key_id": key_id, "version": 1, "algorithm": "AES_256", "usage": "EncryptDecrypt", "status": "Active",
        "metadata": {}, "created_at": f"{created}[UTC]", "rotated_at": None, "created_by": "sse-blackbox",
        "encrypted_key_material": base64.b64encode(os.urandom(32)).decode(), "nonce": [],
    }
    path = key_dir / f"{key_id}.key"
    path.write_text(json.dumps(stored, indent=2))
    path.chmod(0o600)


def wait_kms_healthy(s: Site, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    with observe.quiet():
        while True:
            status, body = kms_status(s)
            if status == 200 and isinstance(body, dict) and body.get("backend_status") == "healthy":
                return
            if time.monotonic() > deadline:
                raise EnvError(f"KMS not healthy within {timeout:.0f}s: HTTP {status} {str(body)[:200]}")
            time.sleep(0.5)


def ensure_key(s: Site, name: str) -> None:
    """Create the named key unless the key list already has it."""
    with observe.quiet():
        if name in key_list(s):
            return
        status, response = key_create(s, name)
        if status not in (200, 201):
            raise EnvError(f"CreateKey {name}: HTTP {status} {str(response)[:200]}")


def iam_add_user(s: Site, user: str, secret: str):
    return admin(s, "PUT", "add-user", query=f"accessKey={urllib.parse.quote(user)}",
                 payload={"secretKey": secret, "status": "enabled"}, label="AddUser")


def iam_add_policy(s: Site, name: str, policy: dict):
    return admin(s, "PUT", "add-canned-policy", query=f"name={urllib.parse.quote(name)}",
                 raw_body=json.dumps(policy).encode(), label="AddPolicy")


def iam_attach_policy(s: Site, user: str, policy: str):
    query = f"policyName={urllib.parse.quote(policy)}&userOrGroup={urllib.parse.quote(user)}&isGroup=false"
    return admin(s, "PUT", "set-user-or-group-policy", query=query, label="AttachPolicy")


# ---------------------------------------------------------------------------
# Process control
# ---------------------------------------------------------------------------

BASE_SERVER_ENV = {
    "RUST_LOG": "rustfs=info,rustfs_sse=info,rustfs_kms=info,rustfs_iam=info,rustfs_ecstore=warn,rustfs_scanner=warn,rustfs_heal=warn,s3s=warn",
    "RUSTFS_CONSOLE_ENABLE": "false",
    "RUSTFS_UNSAFE_BYPASS_DISK_CHECK": "true",
}
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy")
SECRET_ENV_MARKERS = ("TOKEN", "SECRET", "MASTER_KEY", "PASSWORD")


def clean_env() -> dict[str, str]:
    """The inherited environment without RUSTFS_* and proxy variables: ambient configuration must not reach a run."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("RUSTFS_") and k not in PROXY_VARS}
    env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost"
    return env


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) == 0


def is_running(s: Site) -> bool:
    return s.proc is not None and s.proc.poll() is None


def start_site(s: Site, binary: Path, env_overrides: dict[str, str]) -> None:
    """Start `s` on a scrubbed environment. A busy port is an error: the suite neither moves on nor kills the holder."""
    if is_running(s):
        raise EnvError(f"{s.name} is already running")
    if port_in_use(s.port):
        raise EnvError(f"port {s.port} is already in use; find the holder with: lsof -nP -iTCP:{s.port} -sTCP:LISTEN")
    if not binary.exists():
        raise EnvError(f"server binary missing: {binary}")
    for drive in s.drives():
        drive.mkdir(parents=True, exist_ok=True)
    s.log_dir.mkdir(parents=True, exist_ok=True)
    env = clean_env()
    env.update(BASE_SERVER_ENV)
    env.update({
        "RUSTFS_ACCESS_KEY": s.access_key,
        "RUSTFS_SECRET_KEY": s.secret_key,
        "RUSTFS_VOLUMES": str(s.volume_dir / "test{1...4}"),
        "RUSTFS_ADDRESS": f"127.0.0.1:{s.port}",
        "RUSTFS_SERVER_DOMAINS": f"127.0.0.1:{s.port}",
        "RUSTFS_OBS_LOG_DIRECTORY": str(s.log_dir),
    })
    env.update(env_overrides)
    s.env = dict(env_overrides)
    shown = {k: ("<redacted>" if any(m in k for m in SECRET_ENV_MARKERS) else v) for k, v in s.env.items()}
    with s.stdout_log.open("ab") as log:
        log.write(f"\n===== start {datetime.datetime.now().isoformat(timespec='seconds')} env={json.dumps(shown)}\n".encode())
        s.proc = subprocess.Popen([str(binary)], cwd=str(s.root), env=env, stdout=log, stderr=subprocess.STDOUT,
                                  start_new_session=True)
    (s.log_dir / "rustfs.pid").write_text(f"{s.proc.pid}\n")
    wait_ready(s)


def wait_ready(s: Site, timeout: float = STARTUP_TIMEOUT) -> None:
    """Ready means a signed ListBuckets answers; a listening port alone proves nothing."""
    deadline = time.monotonic() + timeout
    with observe.quiet():
        while True:
            if not is_running(s):
                raise EnvError(f"{s.name} exited during startup; log tail:\n{tail_log(s)}")
            status, _, _ = signed_request(s, "GET", "/", timeout=3.0)
            if 200 <= status < 500:
                return
            if time.monotonic() > deadline:
                raise EnvError(f"{s.name} not ready within {timeout:.0f}s; log tail:\n{tail_log(s)}")
            time.sleep(0.5)


def stop_site(s: Site) -> None:
    """SIGTERM, then SIGKILL after a grace period. Only the process this driver started is ever signalled."""
    if s.proc is None:
        return
    if s.proc.poll() is None:
        s.proc.send_signal(signal.SIGTERM)
        try:
            s.proc.wait(timeout=STOP_GRACE)
        except subprocess.TimeoutExpired:
            s.proc.kill()
            s.proc.wait(timeout=STOP_GRACE)
    s.proc = None
    deadline = time.monotonic() + 30.0
    while port_in_use(s.port):
        if time.monotonic() > deadline:
            raise EnvError(f"{s.name} still listening on {s.port} after stop")
        time.sleep(0.2)


def tail_log(s: Site, lines: int = 15) -> str:
    out = []
    for path in (s.stdout_log, s.app_log):
        try:
            text = path.read_text(errors="replace")
        except FileNotFoundError:
            continue
        out.append(f"--- {path.name} ---\n" + "\n".join(line[:400] for line in text.splitlines()[-lines:]))
    return "\n".join(out) or "<no log>"


def log_events(s: Site, event: str) -> list[dict]:
    """Structured log records whose `event` field equals `event`, in the order they were written."""
    records = []
    try:
        text = s.app_log.read_text(errors="replace")
    except FileNotFoundError:
        return records
    for line in text.splitlines():
        if f'"{event}"' not in line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        fields = record.get("fields") if isinstance(record.get("fields"), dict) else {}
        merged = {**record, **fields}
        if merged.get("event") == event:
            records.append(merged)
    return records


def log_text(s: Site) -> str:
    return "".join(p.read_text(errors="replace") for p in (s.stdout_log, s.app_log) if p.exists())


# ---------------------------------------------------------------------------
# Stored form (layer 2): metadata key names, read from the raw xl.meta
# ---------------------------------------------------------------------------

_STORED_KEY = re.compile(
    rb"(?i)(x-rustfs-encryption-[a-z0-9-]+|x-rustfs-internal-[a-z0-9-]+|x-minio-internal-[a-z0-9-]+"
    rb"|x-amz-server-side-encryption[a-z0-9-]*)"
)


def stored_encryption_keys(s: Site, bucket: str, key: str) -> list[str] | None:
    """Encryption-related metadata key names stored for an object, lower-cased and sorted; None when not found.

    Key names are plain strings inside xl.meta, so a byte scan finds them without a format-aware reader. It reads
    names only; values need the dedicated layer-2 tooling.
    """
    for drive in s.drives():
        xl = drive / bucket / key / "xl.meta"
        if xl.exists():
            return sorted({m.group(0).decode().lower() for m in _STORED_KEY.finditer(xl.read_bytes())})
    return None


def binary_identity(binary: Path) -> dict:
    try:
        version = subprocess.run([str(binary), "--version"], capture_output=True, text=True, timeout=30, check=False,
                                 env=clean_env()).stdout.strip()
    except (OSError, subprocess.SubprocessError) as err:
        version = f"<error {err}>"
    return {"path": str(binary), "sha256": sha256_hex(binary.read_bytes()), "version": version.splitlines()[0] if version else ""}
