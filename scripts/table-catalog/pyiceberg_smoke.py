#!/usr/bin/env python3
"""Manual PyIceberg smoke test for the RustFS Iceberg REST catalog."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

import engine_compatibility
import failure_coverage

DEFAULT_PROFILE = "rustfs"
CATALOG_VENDED_PROFILE = "rustfs-vended-credentials"
VENDED_CREDENTIAL_PROFILES = {CATALOG_VENDED_PROFILE}
REQUIRED_STORAGE_CREDENTIAL_KEYS = (
    "s3.access-key-id",
    "s3.secret-access-key",
    "s3.session-token",
)
TABLE_MAINTENANCE_CONFIG_VERSION = 1
IDENTIFIER_SEGMENT_MAX_LEN = 64

PROFILE_DEFAULTS: dict[str, dict[str, Any]] = {
    "rustfs": {
        "catalog_uri": "{endpoint}/iceberg",
        "rest_path": "/iceberg",
        "rest_signing_name": "s3",
        "warehouse": "{bucket}",
        "credential_mode": "static-s3-credentials",
        "status": "automated-smoke-target",
        "compatibility_stage": "automated",
        "catalog_uri_shape": "{endpoint}/iceberg",
        "warehouse_shape": "{bucket}",
        "namespace_model": "single-level",
        "pagination_model": "rustfs",
        "not_claimed": [],
    },
    "rustfs-compat": {
        "catalog_uri": "{endpoint}/_iceberg",
        "rest_path": "/_iceberg",
        "rest_signing_name": "s3tables",
        "warehouse": "{bucket}",
        "credential_mode": "static-s3-credentials",
        "status": "compatibility-smoke-target",
        "compatibility_stage": "automated",
        "catalog_uri_shape": "{endpoint}/_iceberg",
        "warehouse_shape": "{bucket}",
        "namespace_model": "single-level",
        "pagination_model": "rustfs",
        "not_claimed": ["full MinIO AIStor private extension parity"],
    },
    CATALOG_VENDED_PROFILE: {
        "catalog_uri": "{endpoint}/iceberg",
        "rest_path": "/iceberg",
        "rest_signing_name": "s3",
        "warehouse": "{bucket}",
        "credential_mode": "catalog-vended-temporary-credentials",
        "status": "automated-credential-smoke-target",
        "compatibility_stage": "automated-when-enabled",
        "catalog_uri_shape": "{endpoint}/iceberg",
        "warehouse_shape": "{bucket}",
        "namespace_model": "single-level",
        "pagination_model": "rustfs",
        "not_claimed": ["no-long-term-data-credential bootstrap"],
    },
    "aws-s3tables": {
        "catalog_uri": "https://s3tables.{region}.amazonaws.com/iceberg",
        "rest_path": "/iceberg",
        "rest_signing_name": "s3tables",
        "warehouse": "arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}",
        "credential_mode": "aws-iam-or-session-credentials",
        "status": "reference-only",
        "compatibility_stage": "reference-only",
        "catalog_uri_shape": "https://s3tables.{region}.amazonaws.com/iceberg",
        "warehouse_shape": "arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}",
        "namespace_model": "single-level",
        "pagination_model": "vendor-specific",
        "not_claimed": ["full AWS S3 Tables API parity", "AWS IAM/Lake Formation policy parity"],
    },
    "minio-aistor": {
        "catalog_uri": "{endpoint}/_iceberg",
        "rest_path": "/_iceberg",
        "rest_signing_name": "s3tables",
        "warehouse": "{warehouse}",
        "credential_mode": "policy-scoped-s3-credentials",
        "status": "reference-only",
        "compatibility_stage": "reference-only-plus-rustfs-alias",
        "catalog_uri_shape": "{endpoint}/_iceberg",
        "warehouse_shape": "{warehouse}",
        "namespace_model": "single-level",
        "pagination_model": "vendor-specific",
        "not_claimed": ["full MinIO AIStor private extension parity"],
    },
    "cloudflare-r2-data-catalog": {
        "catalog_uri": "{catalog_uri}",
        "rest_path": "/catalog",
        "rest_signing_name": "s3",
        "warehouse": "{warehouse_name}",
        "credential_mode": "catalog-vended",
        "status": "reference-only",
        "compatibility_stage": "reference-only",
        "catalog_uri_shape": "{catalog_uri}",
        "warehouse_shape": "{warehouse_name}",
        "namespace_model": "provider-defined",
        "pagination_model": "vendor-specific",
        "not_claimed": ["live RustFS interoperability", "Cloudflare R2 catalog API parity"],
    },
    "oss-tables": {
        "catalog_uri": "{endpoint}/iceberg",
        "rest_path": "/iceberg",
        "rest_signing_name": "s3",
        "warehouse": "{warehouse}",
        "credential_mode": "sigv4-s3fileio-credentials",
        "status": "reference-only",
        "compatibility_stage": "reference-only",
        "catalog_uri_shape": "{endpoint}/iceberg",
        "warehouse_shape": "{warehouse}",
        "namespace_model": "provider-defined",
        "pagination_model": "vendor-specific",
        "not_claimed": ["live RustFS interoperability", "Alibaba OSS Tables API parity"],
    },
}

CLIENT_MATRIX: list[dict[str, str]] = [
    {
        "client": "PyIceberg",
        "status": "automated",
        "coverage": "create namespace, create table, append, reload, scan, metadata-location, refs, views, maintenance, diagnostics, optional catalog-vended table credentials with exact-prefix data-plane scope probe",
        "entrypoint": "scripts/table-catalog/pyiceberg_smoke.py",
    },
    {
        "client": "Spark Iceberg REST catalog",
        "status": "manual-ready",
        "coverage": "create/load/append/reload to be verified against a running RustFS endpoint",
        "entrypoint": "scripts/table-catalog/README.md",
    },
    {
        "client": "Trino Iceberg REST catalog",
        "status": "manual-live-read-probe",
        "coverage": "generated catalog properties and read-only SELECT probe for a table created by PyIceberg or Spark; write compatibility is not claimed",
        "entrypoint": "scripts/table-catalog/engine_compatibility.py --print-live-conformance",
    },
    {
        "client": "DuckDB Iceberg",
        "status": "manual-live-read-probe",
        "coverage": "generated httpfs/iceberg SQL using an operator-supplied current metadata location; write/commit is not claimed",
        "entrypoint": "scripts/table-catalog/engine_compatibility.py --print-live-conformance",
    },
    {
        "client": "Databend",
        "status": "manual-live-s3-stage-probe",
        "coverage": "generated S3 stage read probe for table data files; Iceberg REST catalog integration is not claimed",
        "entrypoint": "scripts/table-catalog/engine_compatibility.py --print-live-conformance",
    },
    {
        "client": "Snowflake Open Catalog / Iceberg integrations",
        "status": "manual-reference-probe",
        "coverage": "operator-adapted external volume/catalog integration SQL template; no RustFS live interoperability claim",
        "entrypoint": "scripts/table-catalog/engine_compatibility.py --print-live-conformance",
    },
]

UNSUPPORTED_INVENTORY: list[dict[str, str]] = [
    {
        "capability": "credential-vending",
        "status": "automated-after-table-bootstrap",
        "roadmap_area": "credential-vending",
        "catalog_endpoint": "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
        "expected_behavior": "the rustfs-vended-credentials profile requires server-side temporary credential vending, verifies the returned prefix exactly matches the table warehouse location, verifies scoped data-plane access, and switches append, reload, and scan operations to the returned table-scoped credentials after table creation",
    },
    {
        "capability": "background-maintenance-worker",
        "status": "controlled-run-once-supported",
        "roadmap_area": "maintenance-worker",
        "catalog_endpoint": "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/maintenance/scheduler",
        "expected_behavior": "background-enabled maintenance can be driven by the worker run endpoint and inspected through the scheduler status endpoint with disabled/paused state, current-job backpressure, retry deferral, quarantine boundary, audit timeline, per-job audit events, lease expiry recovery, heartbeat updates, and operator quarantine inspect/release/retry/abandon actions; built-in periodic scheduling is not claimed",
    },
    {
        "capability": "manifest-data-reachability-cleanup",
        "status": "conservative-cleanup-supported",
        "roadmap_area": "reachability-cleanup",
        "expected_behavior": "metadata maintenance reads manifest-list and manifest Avro references, reports retained manifest/data/delete objects, and deletes only unreferenced table objects that pass the safety window",
    },
    {
        "capability": "snapshot-expiration",
        "status": "manual-maintenance-supported",
        "roadmap_area": "snapshot-maintenance",
        "expected_behavior": "metadata maintenance can report snapshot expiration plans and manually commit safe snapshot expiration through the catalog; background scheduling and manifest/data cleanup are not claimed",
    },
    {
        "capability": "compaction-rewrite",
        "status": "controlled-run-once-supported",
        "roadmap_area": "snapshot-maintenance",
        "expected_behavior": "metadata maintenance can plan binpack candidates and commit a safe partition-local and sort-order-preserving Parquet rewrite through the catalog; manifests with position or equality delete files produce machine-readable row-level planning and fail closed before rewrite; built-in periodic scheduling, delete-file rewrite, and row-level compaction execution are not claimed",
    },
    {
        "capability": "row-level-delete-update-merge",
        "status": "catalog-conflict-validation-supported",
        "roadmap_area": "row-level-conflict-detection",
        "expected_behavior": "standard catalog commit validates append, overwrite, delete, and replace snapshot manifests for table-warehouse scope, referenced object existence, current-live-file deletes, and stale add/delete conflicts; end-to-end SQL DML client coverage remains a compatibility validation item",
    },
    {
        "capability": "external-catalog-bridge",
        "status": "operator-sync-supported",
        "roadmap_area": "external-catalog",
        "expected_behavior": "catalog import/register and operator-supplied external metadata pointer sync are supported for Polaris, Glue, DLF, and Hive identity boundaries; online vendor SDK polling and policy mirroring are not claimed",
    },
    {
        "capability": "multi-table-transactions",
        "status": "not-planned-short-term",
        "roadmap_area": "transaction-scope",
        "expected_behavior": "RustFS S3 Tables only claims single-table commit atomicity",
    },
]

PRODUCTION_READINESS_INVENTORY: list[dict[str, str]] = [
    {
        "capability": "strong-catalog-backing",
        "status": "migration-contract-supported",
        "validation": "catalog export and diagnostics expose an object-backed manifest, recoverable commit-log WAL state, strong-kv-wal migration target, required migration steps, and recovery blockers before cutover",
    },
    {
        "capability": "single-active-writer-ha",
        "status": "policy-published",
        "validation": "diagnostics publish single-active-writer-region semantics, linearizable leader-read requirements for commits, read-only replica limits, and explicit no active-active support",
    },
    {
        "capability": "scale-validation-matrix",
        "status": "matrix-published",
        "validation": "required validation scenarios are machine-readable: concurrent commit CAS, commit-log recovery replay, migration snapshot replay, stale replica read guards, and long-running client conformance",
    },
]


@dataclass(frozen=True)
class RuntimeDeps:
    boto3: Any
    botocore_client_error: Any
    botocore_config: Any
    botocore_credentials: Any
    botocore_auth: Any
    botocore_awsrequest: Any
    pyarrow: Any
    load_catalog: Any


@dataclass(frozen=True)
class StorageCredential:
    prefix: str
    config: dict[str, str]


class RestRequestError(RuntimeError):
    def __init__(self, method: str, path: str, status_code: int, response_body: str) -> None:
        super().__init__(f"{method} {path} failed with HTTP {status_code}: {response_body}")
        self.method = method
        self.path = path
        self.status_code = status_code
        self.response_body = response_body


def env_or_none(key: str) -> str | None:
    value = os.getenv(key)
    if value is None or value == "":
        return None
    return value


def vendor_profiles() -> dict[str, dict[str, Any]]:
    return {name: values.copy() for name, values in PROFILE_DEFAULTS.items()}


def selected_vendor_profile(args: argparse.Namespace) -> dict[str, Any]:
    defaults = PROFILE_DEFAULTS[args.profile]
    not_claimed = defaults.get("not_claimed", [])
    if not isinstance(not_claimed, list):
        raise ValueError("profile not_claimed field must be a list")
    return {
        "name": args.profile,
        "status": defaults["status"],
        "compatibility_stage": defaults["compatibility_stage"],
        "catalog_uri": profile_catalog_uri(args),
        "catalog_uri_shape": defaults["catalog_uri_shape"],
        "warehouse": profile_warehouse(args),
        "warehouse_shape": defaults["warehouse_shape"],
        "rest_path": args.rest_path,
        "rest_signing_name": args.rest_signing_name,
        "credential_mode": defaults["credential_mode"],
        "namespace_model": defaults["namespace_model"],
        "pagination_model": defaults["pagination_model"],
        "not_claimed": not_claimed.copy(),
    }


def client_matrix() -> list[dict[str, str]]:
    return [entry.copy() for entry in CLIENT_MATRIX]


def unsupported_inventory() -> list[dict[str, str]]:
    return [entry.copy() for entry in UNSUPPORTED_INVENTORY]


def production_readiness_inventory() -> list[dict[str, str]]:
    return [entry.copy() for entry in PRODUCTION_READINESS_INVENTORY]


def normalized_rest_path(rest_path: str) -> str:
    stripped = rest_path.strip()
    if not stripped:
        raise ValueError("REST catalog path cannot be empty")
    if not stripped.startswith("/"):
        stripped = f"/{stripped}"
    return stripped.rstrip("/")


def apply_profile_defaults(args: argparse.Namespace) -> argparse.Namespace:
    defaults = PROFILE_DEFAULTS[args.profile]
    if args.rest_path is None:
        args.rest_path = defaults["rest_path"]
    args.rest_path = normalized_rest_path(args.rest_path)
    if args.rest_signing_name is None:
        args.rest_signing_name = defaults["rest_signing_name"]
    if args.require_vended_credentials is None:
        args.require_vended_credentials = args.profile in VENDED_CREDENTIAL_PROFILES
    return args


def profile_template_context(args: argparse.Namespace) -> dict[str, str]:
    endpoint = normalized_endpoint(args.endpoint)
    warehouse = args.warehouse or args.bucket
    table_bucket = args.table_bucket or args.bucket
    warehouse_name = args.warehouse_name or warehouse
    catalog_uri = args.catalog_uri or f"{endpoint}{args.rest_path}"
    return {
        "account_id": args.account_id,
        "bucket": args.bucket,
        "catalog_uri": catalog_uri,
        "endpoint": endpoint,
        "region": args.region,
        "table_bucket": table_bucket,
        "warehouse": warehouse,
        "warehouse_name": warehouse_name,
    }


def formatted_profile_value(args: argparse.Namespace, key: str) -> str:
    template = PROFILE_DEFAULTS[args.profile][key]
    if not isinstance(template, str):
        raise ValueError(f"profile field {key} is not a string")
    return template.format(**profile_template_context(args))


def profile_catalog_uri(args: argparse.Namespace) -> str:
    if args.catalog_uri:
        return args.catalog_uri.rstrip("/")
    return formatted_profile_value(args, "catalog_uri").rstrip("/")


def profile_warehouse(args: argparse.Namespace) -> str:
    if args.warehouse:
        return args.warehouse
    return formatted_profile_value(args, "warehouse")


def parse_args() -> argparse.Namespace:
    run_id = str(int(time.time()))
    parser = argparse.ArgumentParser(description="Run a PyIceberg smoke test against RustFS table catalog APIs.")
    parser.add_argument("--profile", choices=sorted(PROFILE_DEFAULTS), default=env_or_none("RUSTFS_TABLE_PROFILE") or DEFAULT_PROFILE)
    parser.add_argument("--endpoint", default=os.getenv("RUSTFS_ENDPOINT", "http://127.0.0.1:9000"))
    parser.add_argument("--access-key", default=os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"))
    parser.add_argument("--secret-key", default=os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin"))
    parser.add_argument("--region", default=os.getenv("RUSTFS_REGION", os.getenv("AWS_REGION", "us-east-1")))
    parser.add_argument("--bucket", default=os.getenv("RUSTFS_TABLE_BUCKET", "rustfs-s3table-smoke"))
    parser.add_argument("--warehouse", default=env_or_none("RUSTFS_TABLE_WAREHOUSE"))
    parser.add_argument("--table-bucket", default=env_or_none("RUSTFS_TABLE_BUCKET_NAME"))
    parser.add_argument("--account-id", default=os.getenv("RUSTFS_TABLE_ACCOUNT_ID", "000000000000"))
    parser.add_argument("--warehouse-name", default=env_or_none("RUSTFS_TABLE_WAREHOUSE_NAME"))
    parser.add_argument("--catalog-uri", default=env_or_none("RUSTFS_TABLE_CATALOG_URI"))
    parser.add_argument("--namespace", default=os.getenv("RUSTFS_TABLE_NAMESPACE", f"smoke{run_id}"))
    parser.add_argument("--table", default=os.getenv("RUSTFS_TABLE_NAME", f"events{run_id}"))
    parser.add_argument("--catalog-name", default=os.getenv("RUSTFS_TABLE_CATALOG_NAME", "rustfs"))
    parser.add_argument("--rest-path", default=env_or_none("RUSTFS_TABLE_REST_PATH"))
    parser.add_argument("--rest-signing-name", default=env_or_none("RUSTFS_TABLE_REST_SIGNING_NAME"))
    parser.add_argument(
        "--require-vended-credentials",
        action="store_true",
        default=None,
        help="Require table-scoped credentials from the REST credentials endpoint before data-plane operations.",
    )
    parser.add_argument("--timeout", type=float, default=float(os.getenv("RUSTFS_TABLE_SMOKE_TIMEOUT", "20")))
    parser.add_argument("--cleanup", action="store_true", help="Drop the smoke table and namespace before exiting.")
    parser.add_argument("--replace", action="store_true", help="Drop an existing table with the same identifier first.")
    parser.add_argument(
        "--skip-catalog-api-probes",
        action="store_true",
        help="Skip direct REST probes for metadata-location, refs, views, maintenance, and diagnostics endpoints.",
    )
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification for HTTPS endpoints.")
    parser.add_argument("--print-client-matrix", action="store_true", help="Print the current client conformance matrix as JSON and exit.")
    parser.add_argument("--print-engine-compatibility", action="store_true", help="Print the current Iceberg engine compatibility matrix as JSON and exit.")
    parser.add_argument(
        "--print-production-failure-coverage",
        action="store_true",
        help="Print the current production failure coverage matrix as JSON and exit.",
    )
    parser.add_argument("--print-vendor-profiles", action="store_true", help="Print vendor connection profile references as JSON and exit.")
    parser.add_argument("--print-unsupported-inventory", action="store_true", help="Print unsupported capability inventory as JSON and exit.")
    parser.add_argument(
        "--print-production-readiness",
        action="store_true",
        help="Print catalog backing, HA, and scale-readiness inventory as JSON and exit.",
    )
    return apply_profile_defaults(parser.parse_args())


def load_runtime_deps() -> RuntimeDeps:
    missing: list[str] = []
    try:
        import boto3
    except ModuleNotFoundError:
        boto3 = None
        missing.append("boto3")
    try:
        import pyarrow as pyarrow
    except ModuleNotFoundError:
        pyarrow = None
        missing.append("pyarrow")
    try:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.exceptions import ClientError
        from botocore.config import Config
        from botocore.credentials import Credentials
    except ModuleNotFoundError:
        ClientError = Config = Credentials = SigV4Auth = AWSRequest = None
        missing.append("botocore")
    try:
        from pyiceberg.catalog import load_catalog
    except ModuleNotFoundError:
        load_catalog = None
        missing.append("pyiceberg")

    if missing:
        unique_missing = ", ".join(sorted(set(missing)))
        raise RuntimeError(
            f"missing Python dependencies: {unique_missing}\n"
            "Install them with: python -m pip install 'pyiceberg[pyarrow]' boto3"
        )

    return RuntimeDeps(
        boto3=boto3,
        botocore_client_error=ClientError,
        botocore_config=Config,
        botocore_credentials=Credentials,
        botocore_auth=SigV4Auth,
        botocore_awsrequest=AWSRequest,
        pyarrow=pyarrow,
        load_catalog=load_catalog,
    )


def normalized_endpoint(endpoint: str) -> str:
    return endpoint.rstrip("/")


def ensure_local_proxy_bypass(endpoint: str) -> None:
    host = urllib.parse.urlparse(endpoint).hostname
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return
    existing = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
    entries = {entry.strip() for entry in existing.split(",") if entry.strip()}
    entries.update({"127.0.0.1", "localhost", "::1"})
    os.environ["NO_PROXY"] = ",".join(sorted(entries))


def ensure_aws_env(access_key: str, secret_key: str, region: str) -> None:
    os.environ.setdefault("AWS_ACCESS_KEY_ID", access_key)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", secret_key)
    os.environ.setdefault("AWS_REGION", region)
    os.environ.setdefault("AWS_DEFAULT_REGION", region)


def unsigned_ssl_context(insecure: bool) -> ssl.SSLContext | None:
    if not insecure:
        return None
    return ssl._create_unverified_context()


def signed_rest_request(args: argparse.Namespace, deps: RuntimeDeps, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    endpoint = normalized_endpoint(args.endpoint)
    url = f"{endpoint}{path}"
    payload = b"" if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {
        "host": urllib.parse.urlparse(url).netloc,
        "x-amz-content-sha256": hashlib.sha256(payload).hexdigest(),
    }
    if body is not None:
        headers["content-type"] = "application/json"

    aws_request = deps.botocore_awsrequest(method=method, url=url, data=payload, headers=headers)
    credentials = deps.botocore_credentials(args.access_key, args.secret_key)
    deps.botocore_auth(credentials, args.rest_signing_name, args.region).add_auth(aws_request)
    prepared = aws_request.prepare()

    request = urllib.request.Request(
        prepared.url,
        data=payload if method not in {"GET", "HEAD"} else None,
        headers=dict(prepared.headers.items()),
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout, context=unsigned_ssl_context(args.insecure)) as response:
            response_data = response.read()
            if not response_data:
                return {}
            return json.loads(response_data.decode("utf-8"))
    except urllib.error.HTTPError as error:
        response_body = error.read().decode("utf-8", errors="replace")
        raise RestRequestError(method, path, error.code, response_body) from error


def signed_rest_request_expect_error(
    args: argparse.Namespace,
    deps: RuntimeDeps,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    expected_statuses: set[int] | None = None,
) -> RestRequestError:
    try:
        signed_rest_request(args, deps, method, path, body)
    except RestRequestError as error:
        if expected_statuses is not None and error.status_code not in expected_statuses:
            expected = ", ".join(str(status) for status in sorted(expected_statuses))
            raise RuntimeError(f"{method} {path} failed with HTTP {error.status_code}, expected one of: {expected}") from error
        return error
    raise RuntimeError(f"{method} {path} unexpectedly succeeded")


def ensure_bucket(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    client = configured_s3_client(args, deps)
    try:
        client.head_bucket(Bucket=args.bucket)
        return
    except deps.botocore_client_error as error:
        status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        error_code = error.response.get("Error", {}).get("Code")
        if status_code != 404 and error_code not in {"404", "NoSuchBucket", "NotFound"}:
            raise

    if args.region == "us-east-1":
        client.create_bucket(Bucket=args.bucket)
    else:
        client.create_bucket(Bucket=args.bucket, CreateBucketConfiguration={"LocationConstraint": args.region})


def configured_s3_client(args: argparse.Namespace, deps: RuntimeDeps) -> Any:
    return deps.boto3.client(
        "s3",
        endpoint_url=normalized_endpoint(args.endpoint),
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        verify=not args.insecure,
        config=deps.botocore_config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def enable_table_bucket(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    encoded_bucket = urllib.parse.quote(args.bucket, safe="")
    signed_rest_request(args, deps, "PUT", f"{args.rest_path}/v1/buckets/{encoded_bucket}")


def credentials_endpoint_path(args: argparse.Namespace) -> str:
    encoded_bucket = urllib.parse.quote(args.bucket, safe="")
    encoded_namespace = urllib.parse.quote(args.namespace, safe="")
    encoded_table = urllib.parse.quote(args.table, safe="")
    return f"{args.rest_path}/v1/{encoded_bucket}/namespaces/{encoded_namespace}/tables/{encoded_table}/credentials"


def table_endpoint_path(args: argparse.Namespace, suffix: str = "") -> str:
    encoded_bucket = urllib.parse.quote(args.bucket, safe="")
    encoded_namespace = urllib.parse.quote(args.namespace, safe="")
    encoded_table = urllib.parse.quote(args.table, safe="")
    return f"{args.rest_path}/v1/{encoded_bucket}/namespaces/{encoded_namespace}/tables/{encoded_table}{suffix}"


def table_ref_endpoint_path(args: argparse.Namespace, ref_name: str | None = None) -> str:
    path = table_endpoint_path(args, "/refs")
    if ref_name is None:
        return path
    return f"{path}/{urllib.parse.quote(ref_name, safe='')}"


def view_endpoint_path(args: argparse.Namespace, view_name: str | None = None) -> str:
    encoded_bucket = urllib.parse.quote(args.bucket, safe="")
    encoded_namespace = urllib.parse.quote(args.namespace, safe="")
    path = f"{args.rest_path}/v1/{encoded_bucket}/namespaces/{encoded_namespace}/views"
    if view_name is None:
        return path
    return f"{path}/{urllib.parse.quote(view_name, safe='')}"


def default_maintenance_config() -> dict[str, Any]:
    return {
        "version": TABLE_MAINTENANCE_CONFIG_VERSION,
        "retain-recent-metadata-files": 2,
        "delete-enabled": False,
        "background-enabled": False,
        "worker-paused": True,
        "worker-lease-timeout-seconds": 60,
        "max-retry-attempts": 0,
        "retry-initial-backoff-seconds": 5,
        "retry-max-backoff-seconds": 300,
        "quarantine-enabled": False,
        "quarantine-retention-seconds": 0,
    }


def storage_credential_from_response(response: dict[str, Any]) -> StorageCredential:
    credentials = response.get("storage-credentials")
    if not isinstance(credentials, list) or not credentials:
        raise RuntimeError("no storage credentials were returned by the table credentials endpoint")

    for credential in credentials:
        if not isinstance(credential, dict):
            continue
        config = credential.get("config")
        if not isinstance(config, dict):
            continue
        missing_keys = [key for key in REQUIRED_STORAGE_CREDENTIAL_KEYS if not config.get(key)]
        if missing_keys:
            continue
        prefix = credential.get("prefix")
        if not isinstance(prefix, str):
            prefix = ""
        return StorageCredential(prefix=prefix, config={str(key): str(value) for key, value in config.items()})

    required = ", ".join(REQUIRED_STORAGE_CREDENTIAL_KEYS)
    raise RuntimeError(f"no storage credentials contained the required keys: {required}")


def load_table_storage_credential(args: argparse.Namespace, deps: RuntimeDeps) -> StorageCredential:
    response = signed_rest_request(args, deps, "GET", credentials_endpoint_path(args))
    return storage_credential_from_response(response)


def s3_scope_from_uri(s3_uri: str, description: str) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(s3_uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise RuntimeError(f"{description} must be an s3 URI")
    bucket = urllib.parse.unquote(parsed.netloc)
    object_prefix = urllib.parse.unquote(parsed.path.lstrip("/")).rstrip("/")
    if not object_prefix:
        raise RuntimeError(f"{description} must include an object prefix")
    if "\\" in object_prefix:
        raise RuntimeError(f"{description} contains an invalid path separator")
    if any(segment in {"", ".", ".."} for segment in object_prefix.split("/")):
        raise RuntimeError(f"{description} contains an invalid path segment")
    return bucket, f"{object_prefix}/"


def s3_scope_from_credential(storage_credential: StorageCredential) -> tuple[str, str]:
    return s3_scope_from_uri(storage_credential.prefix, "storage credential prefix")


def string_value(value: Any) -> str | None:
    if callable(value):
        value = value()
    if isinstance(value, str) and value:
        return value
    return None


def table_warehouse_location(table: Any) -> str:
    for source in (getattr(table, "metadata", None), table):
        if callable(source):
            source = source()
        if source is None:
            continue
        if isinstance(source, dict):
            location = string_value(source.get("location"))
        else:
            location = string_value(getattr(source, "location", None))
        if location is not None:
            return location
    raise RuntimeError("created table did not expose a warehouse location")


def safe_probe_segment(namespace: str, table: str) -> str:
    source = f"{namespace}-{table}"
    return "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in source)


def safe_ref_segment(namespace: str, table: str) -> str:
    source = f"smoke-{namespace}-{table}".lower()
    segment = "".join(
        char if char.isascii() and (char.isalnum() or char in {"-", "_"}) else "-"
        for char in source
    )
    segment = segment.strip("-_")
    if not segment:
        return "smoke"
    return segment[:IDENTIFIER_SEGMENT_MAX_LEN].rstrip("-_") or "smoke"


def scope_probe_keys(object_prefix: str, namespace: str, table: str) -> tuple[str, str]:
    segment = safe_probe_segment(namespace, table)
    suffix = f"{segment}-{int(time.time())}.txt"
    return (
        f"{object_prefix}.rustfs-vended-scope-probe/{suffix}",
        f"__rustfs-vended-scope-denied/{suffix}",
    )


def vended_s3_client(args: argparse.Namespace, deps: RuntimeDeps, storage_credential: StorageCredential) -> Any:
    return deps.boto3.client(
        "s3",
        endpoint_url=normalized_endpoint(args.endpoint),
        aws_access_key_id=storage_credential.config["s3.access-key-id"],
        aws_secret_access_key=storage_credential.config["s3.secret-access-key"],
        aws_session_token=storage_credential.config["s3.session-token"],
        region_name=args.region,
        verify=not args.insecure,
        config=deps.botocore_config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def is_access_denied_error(error: BaseException) -> bool:
    response = getattr(error, "response", {})
    status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    error_code = response.get("Error", {}).get("Code")
    return status_code == 403 or error_code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}


def verify_vended_credential_data_plane_scope(
    args: argparse.Namespace,
    deps: RuntimeDeps,
    storage_credential: StorageCredential,
    table_location: str,
) -> None:
    bucket, object_prefix = s3_scope_from_credential(storage_credential)
    table_bucket, table_object_prefix = s3_scope_from_uri(table_location, "table warehouse location")
    if bucket != table_bucket or object_prefix != table_object_prefix:
        raise RuntimeError("storage credential prefix does not match table warehouse location")
    if table_bucket != args.bucket:
        raise RuntimeError(f"table warehouse bucket {table_bucket} does not match expected table bucket {args.bucket}")

    inside_key, denied_key = scope_probe_keys(object_prefix, args.namespace, args.table)
    client = vended_s3_client(args, deps, storage_credential)
    put_inside = False
    try:
        client.put_object(Bucket=bucket, Key=inside_key, Body=b"rustfs table credential scope probe\n")
        put_inside = True
        client.head_object(Bucket=bucket, Key=inside_key)
        client.get_object(Bucket=bucket, Key=inside_key)
    finally:
        if put_inside:
            client.delete_object(Bucket=bucket, Key=inside_key)

    put_denied = False
    try:
        client.put_object(Bucket=bucket, Key=denied_key, Body=b"rustfs denied table credential scope probe\n")
    except deps.botocore_client_error as error:
        if is_access_denied_error(error):
            put_denied = True
        else:
            raise RuntimeError(f"scope-denied write probe failed with unexpected S3 error: {error}") from error
    if not put_denied:
        try:
            configured_s3_client(args, deps).delete_object(Bucket=bucket, Key=denied_key)
        except Exception as cleanup_error:
            print(f"warning: failed to clean unexpected denied-scope probe object {denied_key}: {cleanup_error}", file=sys.stderr)
        raise RuntimeError("vended table credentials unexpectedly wrote outside the table warehouse prefix")

    admin_client = configured_s3_client(args, deps)
    seeded_denied_key = False
    try:
        admin_client.put_object(Bucket=bucket, Key=denied_key, Body=b"rustfs denied read table credential scope probe\n")
        seeded_denied_key = True
        try:
            client.get_object(Bucket=bucket, Key=denied_key)
        except deps.botocore_client_error as error:
            if is_access_denied_error(error):
                return
            raise RuntimeError(f"scope-denied read probe failed with unexpected S3 error: {error}") from error
        raise RuntimeError("vended table credentials unexpectedly read outside the table warehouse prefix")
    finally:
        if seeded_denied_key:
            try:
                admin_client.delete_object(Bucket=bucket, Key=denied_key)
            except Exception as cleanup_error:
                print(f"warning: failed to clean denied-scope read probe object {denied_key}: {cleanup_error}", file=sys.stderr)


def catalog_properties(args: argparse.Namespace, storage_credential: StorageCredential | None = None) -> dict[str, str]:
    endpoint = normalized_endpoint(args.endpoint)
    warehouse = profile_warehouse(args)
    properties = {
        "type": "rest",
        "uri": profile_catalog_uri(args),
        "warehouse": warehouse,
        "prefix": warehouse,
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": endpoint,
        "s3.access-key-id": args.access_key,
        "s3.secret-access-key": args.secret_key,
        "s3.region": args.region,
        "s3.path-style-access": "true",
        "rest.sigv4-enabled": "true",
        "rest.signing-region": args.region,
        "rest.signing-name": args.rest_signing_name,
    }
    if storage_credential is not None:
        properties.update(storage_credential.config)
    if args.insecure:
        properties["s3.verify-ssl"] = "false"
    return properties


def print_json_document(document: Any) -> None:
    print(json.dumps(document, indent=2, sort_keys=True))


def printed_metadata(args: argparse.Namespace) -> bool:
    printed = False
    if args.print_client_matrix:
        print_json_document({"client_matrix": client_matrix()})
        printed = True
    if args.print_engine_compatibility:
        print_json_document({"engine_compatibility": engine_compatibility.engine_compatibility_matrix()})
        printed = True
    if args.print_production_failure_coverage:
        print_json_document({"production_failure_coverage": failure_coverage.production_failure_matrix()})
        printed = True
    if args.print_vendor_profiles:
        print_json_document(
            {
                "selected_vendor_profile": selected_vendor_profile(args),
                "vendor_profiles": vendor_profiles(),
            }
        )
        printed = True
    if args.print_unsupported_inventory:
        print_json_document({"unsupported_inventory": unsupported_inventory()})
        printed = True
    if args.print_production_readiness:
        print_json_document({"production_readiness": production_readiness_inventory()})
        printed = True
    return printed


def install_rustfs_rest_sigv4_adapter(catalog: Any, args: argparse.Namespace, deps: RuntimeDeps) -> None:
    from urllib import parse

    from requests.adapters import HTTPAdapter

    class RustfsSigV4Adapter(HTTPAdapter):
        def add_headers(self, request: Any, **kwargs: Any) -> None:
            body = request.body or b""
            if isinstance(body, str):
                body = body.encode("utf-8")
            request.headers["x-amz-content-sha256"] = hashlib.sha256(body).hexdigest()

            if "connection" in request.headers:
                del request.headers["connection"]

            url = str(request.url).split("?")[0]
            query = str(parse.urlsplit(request.url).query)
            params = dict(parse.parse_qsl(query))
            credentials = deps.botocore_credentials(args.access_key, args.secret_key)
            aws_request = deps.botocore_awsrequest(
                method=request.method,
                url=url,
                params=params,
                data=body,
                headers=dict(request.headers),
            )
            deps.botocore_auth(credentials, args.rest_signing_name, args.region).add_auth(aws_request)
            request.headers.update(aws_request.headers)

    catalog._session.mount(catalog.uri, RustfsSigV4Adapter())


def table_identifier(args: argparse.Namespace) -> tuple[str, str]:
    return (args.namespace, args.table)


def table_exists(catalog: Any, identifier: tuple[str, str]) -> bool:
    try:
        return bool(catalog.table_exists(identifier))
    except AttributeError:
        try:
            catalog.load_table(identifier)
            return True
        except Exception:
            return False


def ensure_namespace(catalog: Any, namespace: str) -> None:
    try:
        catalog.create_namespace_if_not_exists(namespace)
        return
    except AttributeError:
        pass
    except Exception as error:
        if "already exists" in str(error).lower():
            return
        raise

    try:
        catalog.create_namespace(namespace)
    except Exception as error:
        if "already exists" not in str(error).lower():
            raise


def drop_table_if_present(catalog: Any, identifier: tuple[str, str]) -> None:
    if table_exists(catalog, identifier):
        catalog.drop_table(identifier)


def cleanup_catalog(catalog: Any, identifier: tuple[str, str]) -> None:
    drop_table_if_present(catalog, identifier)
    try:
        catalog.drop_namespace(identifier[0])
    except Exception as error:
        message = str(error).lower()
        if "not found" not in message and "does not exist" not in message:
            print(f"warning: failed to drop namespace {identifier[0]}: {error}", file=sys.stderr)


def current_snapshot_id_from_table_response(response: dict[str, Any]) -> int:
    metadata = response.get("metadata")
    if not isinstance(metadata, dict):
        raise RuntimeError("REST loadTable response did not include metadata")
    snapshot_id = metadata.get("current-snapshot-id")
    if not isinstance(snapshot_id, int):
        raise RuntimeError("REST loadTable response did not include a current snapshot id")
    return snapshot_id


def run_metadata_location_probe(args: argparse.Namespace, deps: RuntimeDeps, table_response: dict[str, Any]) -> None:
    metadata_location = table_response.get("metadata-location")
    if not isinstance(metadata_location, str) or not metadata_location:
        raise RuntimeError("REST loadTable response did not include metadata-location")
    location_response = signed_rest_request(args, deps, "GET", table_endpoint_path(args, "/metadata-location"))
    if location_response.get("metadata-location") != metadata_location:
        raise RuntimeError("metadata-location endpoint does not match loadTable metadata-location")
    if not location_response.get("version-token"):
        raise RuntimeError("metadata-location endpoint did not include version-token")


def run_table_ref_probe(args: argparse.Namespace, deps: RuntimeDeps, snapshot_id: int) -> None:
    ref_name = safe_ref_segment(args.namespace, args.table)
    ref_body = {
        "snapshot-id": snapshot_id,
        "type": "tag",
        "max-ref-age-ms": 86_400_000,
    }
    force_deleted = False
    signed_rest_request(args, deps, "PUT", table_ref_endpoint_path(args, ref_name), ref_body)
    try:
        refs = signed_rest_request(args, deps, "GET", table_ref_endpoint_path(args))
        if refs.get("refs", {}).get(ref_name, {}).get("snapshot-id") != snapshot_id:
            raise RuntimeError("table refs endpoint did not return the smoke tag")
        unforced_delete = signed_rest_request_expect_error(
            args,
            deps,
            "DELETE",
            table_ref_endpoint_path(args, ref_name),
            {},
            expected_statuses={400},
        )
        if "force is required" not in unforced_delete.response_body.lower():
            raise RuntimeError("unforced table ref delete did not report the retention force requirement")
        signed_rest_request(args, deps, "DELETE", table_ref_endpoint_path(args, ref_name), {"force": True})
        force_deleted = True
        refs = signed_rest_request(args, deps, "GET", table_ref_endpoint_path(args))
        if ref_name in refs.get("refs", {}):
            raise RuntimeError("table refs endpoint still returned the deleted smoke tag")
    finally:
        if not force_deleted:
            try:
                signed_rest_request(args, deps, "DELETE", table_ref_endpoint_path(args, ref_name), {"force": True})
            except RestRequestError as error:
                if error.status_code != 404:
                    raise


def smoke_view_request(args: argparse.Namespace, view_name: str, version_id: int, sql: str) -> dict[str, Any]:
    return {
        "name": view_name,
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                {"id": 1, "name": "id", "required": True, "type": "long"},
                {"id": 2, "name": "payload", "required": False, "type": "string"},
            ],
        },
        "view-version": {
            "version-id": version_id,
            "schema-id": 0,
            "timestamp-ms": int(time.time() * 1000),
            "summary": {"operation": "replace"},
            "representations": [
                {
                    "type": "sql",
                    "sql": sql,
                    "dialect": "spark",
                }
            ],
        },
        "properties": {"rustfs.smoke": "true", "rustfs.smoke.table": args.table},
    }


def run_view_probe(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    view_name = f"{args.table}_smoke_view"
    create_body = smoke_view_request(args, view_name, 1, f"SELECT id, payload FROM {args.namespace}.{args.table}")
    signed_rest_request(args, deps, "POST", view_endpoint_path(args), create_body)
    try:
        views = signed_rest_request(args, deps, "GET", view_endpoint_path(args))
        identifiers = views.get("identifiers", [])
        if not any(identifier.get("name") == view_name for identifier in identifiers if isinstance(identifier, dict)):
            raise RuntimeError("listViews response did not include the smoke view")
        loaded = signed_rest_request(args, deps, "GET", view_endpoint_path(args, view_name))
        metadata_location = loaded.get("metadata-location")
        if not isinstance(metadata_location, str) or not metadata_location:
            raise RuntimeError("loadView response did not include metadata-location")
    finally:
        try:
            signed_rest_request(args, deps, "DELETE", view_endpoint_path(args, view_name))
        except RestRequestError as error:
            if error.status_code != 404:
                raise


def run_maintenance_probe(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    config_path = table_endpoint_path(args, "/maintenance/config")
    scheduler_path = table_endpoint_path(args, "/maintenance/scheduler")
    signed_rest_request(args, deps, "PUT", config_path, default_maintenance_config())
    config = signed_rest_request(args, deps, "GET", config_path)
    if config.get("version") != TABLE_MAINTENANCE_CONFIG_VERSION:
        raise RuntimeError("maintenance config endpoint did not persist the smoke config")

    report = signed_rest_request(
        args,
        deps,
        "POST",
        table_endpoint_path(args, "/maintenance/metadata"),
        {"retain-recent-metadata-files": 1, "delete": False},
    )
    job = report.get("job")
    if not isinstance(job, dict) or not job.get("job-id"):
        raise RuntimeError("maintenance metadata endpoint did not return a job id")
    audit_events = report.get("audit-events")
    if not isinstance(audit_events, list) or not audit_events:
        raise RuntimeError("maintenance metadata endpoint did not return audit events")
    if not any(isinstance(event, dict) and event.get("action") == "PLANNED" for event in audit_events):
        raise RuntimeError("maintenance metadata endpoint did not report a planning audit event")
    job_report = signed_rest_request(args, deps, "GET", table_endpoint_path(args, f"/maintenance/jobs/{job['job-id']}"))
    if not isinstance(job_report.get("audit-events"), list):
        raise RuntimeError("maintenance job endpoint did not return audit events")
    quarantine = signed_rest_request(
        args,
        deps,
        "POST",
        table_endpoint_path(args, f"/maintenance/jobs/{job['job-id']}/quarantine"),
        {"action": "INSPECT"},
    )
    if quarantine.get("action") != "INSPECT" or not isinstance(quarantine.get("report"), dict):
        raise RuntimeError("maintenance quarantine endpoint did not return an inspection report")
    scheduler = signed_rest_request(args, deps, "GET", scheduler_path)
    expected_scheduler_statuses = {"READY", "DISABLED", "PAUSED", "BACKPRESSURED", "RETRY_DEFERRED", "QUARANTINED"}
    if scheduler.get("status") not in expected_scheduler_statuses:
        raise RuntimeError("maintenance scheduler endpoint did not return a stable scheduler status")
    scheduler_timeline = scheduler.get("audit_timeline")
    if not isinstance(scheduler_timeline, list):
        raise RuntimeError("maintenance scheduler endpoint did not return an audit timeline")
    if scheduler_timeline and not isinstance(scheduler_timeline[0].get("audit-events"), list):
        raise RuntimeError("maintenance scheduler audit timeline did not include job audit events")

    worker = signed_rest_request(args, deps, "POST", table_endpoint_path(args, "/maintenance/worker/run"), {})
    worker_job = worker.get("job")
    expected_statuses = {"DISABLED", "PAUSED", "FAILED", "SUCCESSFUL", "RUNNING"}
    if not isinstance(worker_job, dict) or worker_job.get("status") not in expected_statuses:
        raise RuntimeError("maintenance worker endpoint did not return a stable job status")
    if not isinstance(worker.get("audit-events"), list):
        raise RuntimeError("maintenance worker endpoint did not return audit events")


def run_catalog_api_probes(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    table_response = signed_rest_request(args, deps, "GET", table_endpoint_path(args))
    snapshot_id = current_snapshot_id_from_table_response(table_response)
    run_metadata_location_probe(args, deps, table_response)
    run_table_ref_probe(args, deps, snapshot_id)
    run_view_probe(args, deps)
    run_maintenance_probe(args, deps)
    diagnostics = signed_rest_request(args, deps, "GET", table_endpoint_path(args, "/catalog/diagnostics"))
    if not isinstance(diagnostics, dict) or not diagnostics:
        raise RuntimeError("catalog diagnostics endpoint returned an empty response")


def run_smoke(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    endpoint = normalized_endpoint(args.endpoint)
    ensure_local_proxy_bypass(endpoint)
    ensure_aws_env(args.access_key, args.secret_key, args.region)

    print(f"[1/10] ensuring S3 bucket {args.bucket}")
    ensure_bucket(args, deps)

    print(f"[2/10] enabling RustFS table bucket {args.bucket} through {args.rest_path}/v1")
    enable_table_bucket(args, deps)

    print(f"[3/10] loading PyIceberg REST catalog at {endpoint}{args.rest_path}")
    catalog = deps.load_catalog(args.catalog_name, **catalog_properties(args))
    install_rustfs_rest_sigv4_adapter(catalog, args, deps)
    identifier = table_identifier(args)

    if args.replace:
        print(f"[4/10] replacing existing table {'.'.join(identifier)} if present")
        drop_table_if_present(catalog, identifier)
    else:
        print(f"[4/10] table {'.'.join(identifier)} is available")

    print(f"[5/10] creating namespace and table {'.'.join(identifier)}")
    ensure_namespace(catalog, args.namespace)
    arrow_schema = deps.pyarrow.schema(
        [
            deps.pyarrow.field("id", deps.pyarrow.int64(), nullable=False),
            deps.pyarrow.field("payload", deps.pyarrow.string(), nullable=False),
        ]
    )
    created_table = catalog.create_table(identifier, schema=arrow_schema)

    storage_credential = None
    if args.require_vended_credentials:
        print(f"[6/10] loading table-scoped storage credentials")
        storage_credential = load_table_storage_credential(args, deps)
        print(f"credential scope: {storage_credential.prefix or 'not reported'}")
        print(f"[7/10] verifying table-scoped data-plane access")
        try:
            expected_table_location = table_warehouse_location(created_table)
        except RuntimeError:
            expected_table_location = table_warehouse_location(catalog.load_table(identifier))
        verify_vended_credential_data_plane_scope(args, deps, storage_credential, expected_table_location)
        catalog = deps.load_catalog(args.catalog_name, **catalog_properties(args, storage_credential=storage_credential))
        install_rustfs_rest_sigv4_adapter(catalog, args, deps)
    else:
        print(f"[6/10] using configured S3 credentials for data-plane operations")
        print(f"[7/10] skipping vended credential data-plane scope probe")

    print(f"[8/10] appending rows through PyIceberg")
    table = catalog.load_table(identifier)
    rows = deps.pyarrow.Table.from_pylist(
        [
            {"id": 1, "payload": "alpha"},
            {"id": 2, "payload": "beta"},
        ],
        schema=arrow_schema,
    )
    table.append(rows)

    print(f"[9/10] reloading and scanning table")
    loaded = catalog.load_table(identifier)
    scanned = loaded.scan().to_arrow()
    if scanned.num_rows != 2:
        raise RuntimeError(f"expected 2 rows after append, got {scanned.num_rows}")

    if args.skip_catalog_api_probes:
        print("[10/10] skipping direct REST catalog API probes")
    else:
        print("[10/10] probing direct REST catalog APIs")
        run_catalog_api_probes(args, deps)

    if args.cleanup:
        print(f"cleanup: dropping table and namespace {'.'.join(identifier)}")
        cleanup_catalog(catalog, identifier)

    print(f"PASS: PyIceberg smoke test completed for {'.'.join(identifier)}")


def main() -> int:
    args = parse_args()
    try:
        if printed_metadata(args):
            return 0
        deps = load_runtime_deps()
        run_smoke(args, deps)
        return 0
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
