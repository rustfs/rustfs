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

DEFAULT_PROFILE = "rustfs"

PROFILE_DEFAULTS: dict[str, dict[str, str]] = {
    "rustfs": {
        "catalog_uri": "{endpoint}/iceberg",
        "rest_path": "/iceberg",
        "rest_signing_name": "s3",
        "warehouse": "{bucket}",
        "credential_mode": "static-s3-credentials",
        "status": "automated-smoke-target",
    },
    "rustfs-compat": {
        "catalog_uri": "{endpoint}/_iceberg",
        "rest_path": "/_iceberg",
        "rest_signing_name": "s3tables",
        "warehouse": "{bucket}",
        "credential_mode": "static-s3-credentials",
        "status": "compatibility-smoke-target",
    },
    "aws-s3tables": {
        "catalog_uri": "https://s3tables.{region}.amazonaws.com/iceberg",
        "rest_path": "/iceberg",
        "rest_signing_name": "s3tables",
        "warehouse": "arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}",
        "credential_mode": "aws-iam-or-session-credentials",
        "status": "reference-only",
    },
    "minio-aistor": {
        "catalog_uri": "{endpoint}/_iceberg",
        "rest_path": "/_iceberg",
        "rest_signing_name": "s3tables",
        "warehouse": "{warehouse}",
        "credential_mode": "policy-scoped-s3-credentials",
        "status": "reference-only",
    },
    "cloudflare-r2-data-catalog": {
        "catalog_uri": "{catalog_uri}",
        "rest_path": "/catalog",
        "rest_signing_name": "s3",
        "warehouse": "{warehouse_name}",
        "credential_mode": "catalog-vended",
        "status": "reference-only",
    },
    "oss-tables": {
        "catalog_uri": "{endpoint}/iceberg",
        "rest_path": "/iceberg",
        "rest_signing_name": "s3",
        "warehouse": "{warehouse}",
        "credential_mode": "sigv4-s3fileio-credentials",
        "status": "reference-only",
    },
}

CLIENT_MATRIX: list[dict[str, str]] = [
    {
        "client": "PyIceberg",
        "status": "automated",
        "coverage": "create namespace, create table, append, reload, scan",
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
        "status": "documented-not-automated",
        "coverage": "configuration reference only until a repeatable container harness is added",
        "entrypoint": "scripts/table-catalog/README.md",
    },
    {
        "client": "DuckDB Iceberg",
        "status": "documented-not-automated",
        "coverage": "read path reference only; write/commit is not claimed",
        "entrypoint": "scripts/table-catalog/README.md",
    },
    {
        "client": "Databend",
        "status": "documented-not-automated",
        "coverage": "S3 data-plane reference only; Iceberg REST catalog integration is not claimed",
        "entrypoint": "scripts/table-catalog/README.md",
    },
    {
        "client": "Snowflake Open Catalog / Iceberg integrations",
        "status": "documented-not-automated",
        "coverage": "reference only; no RustFS automated smoke claim",
        "entrypoint": "scripts/table-catalog/README.md",
    },
]

UNSUPPORTED_INVENTORY: list[dict[str, str]] = [
    {
        "capability": "credential-vending",
        "status": "scope-preview-no-temporary-credentials",
        "roadmap_area": "credential-boundary",
        "catalog_endpoint": "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
        "expected_behavior": "load table advertises the table credential scope; the credentials endpoint returns an empty storage-credentials list and no long-lived credentials",
    },
    {
        "capability": "background-maintenance-worker",
        "status": "unsupported",
        "roadmap_area": "maintenance-worker",
        "expected_behavior": "manual maintenance APIs may exist, but background-enabled maintenance is rejected",
    },
    {
        "capability": "manifest-data-reachability-cleanup",
        "status": "unsupported",
        "roadmap_area": "reachability-cleanup",
        "expected_behavior": "metadata-only cleanup must not delete manifest, data, or delete files",
    },
    {
        "capability": "snapshot-expiration-and-compaction",
        "status": "unsupported",
        "roadmap_area": "snapshot-maintenance",
        "expected_behavior": "no automatic snapshot expiration or data rewrite is claimed",
    },
    {
        "capability": "iceberg-views",
        "status": "unsupported",
        "roadmap_area": "view-api",
        "expected_behavior": "view routes should return a stable unsupported response until implemented",
    },
    {
        "capability": "multi-table-transactions",
        "status": "not-planned-short-term",
        "roadmap_area": "transaction-scope",
        "expected_behavior": "RustFS S3 Tables only claims single-table commit atomicity",
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


def env_or_none(key: str) -> str | None:
    value = os.getenv(key)
    if value is None or value == "":
        return None
    return value


def vendor_profiles() -> dict[str, dict[str, str]]:
    return {name: values.copy() for name, values in PROFILE_DEFAULTS.items()}


def client_matrix() -> list[dict[str, str]]:
    return [entry.copy() for entry in CLIENT_MATRIX]


def unsupported_inventory() -> list[dict[str, str]]:
    return [entry.copy() for entry in UNSUPPORTED_INVENTORY]


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
    return args


def parse_args() -> argparse.Namespace:
    run_id = str(int(time.time()))
    parser = argparse.ArgumentParser(description="Run a PyIceberg smoke test against RustFS table catalog APIs.")
    parser.add_argument("--profile", choices=sorted(PROFILE_DEFAULTS), default=env_or_none("RUSTFS_TABLE_PROFILE") or DEFAULT_PROFILE)
    parser.add_argument("--endpoint", default=os.getenv("RUSTFS_ENDPOINT", "http://127.0.0.1:9000"))
    parser.add_argument("--access-key", default=os.getenv("RUSTFS_ACCESS_KEY", "rustfsadmin"))
    parser.add_argument("--secret-key", default=os.getenv("RUSTFS_SECRET_KEY", "rustfsadmin"))
    parser.add_argument("--region", default=os.getenv("RUSTFS_REGION", os.getenv("AWS_REGION", "us-east-1")))
    parser.add_argument("--bucket", default=os.getenv("RUSTFS_TABLE_BUCKET", "rustfs-s3table-smoke"))
    parser.add_argument("--namespace", default=os.getenv("RUSTFS_TABLE_NAMESPACE", f"smoke{run_id}"))
    parser.add_argument("--table", default=os.getenv("RUSTFS_TABLE_NAME", f"events{run_id}"))
    parser.add_argument("--catalog-name", default=os.getenv("RUSTFS_TABLE_CATALOG_NAME", "rustfs"))
    parser.add_argument("--rest-path", default=env_or_none("RUSTFS_TABLE_REST_PATH"))
    parser.add_argument("--rest-signing-name", default=env_or_none("RUSTFS_TABLE_REST_SIGNING_NAME"))
    parser.add_argument("--timeout", type=float, default=float(os.getenv("RUSTFS_TABLE_SMOKE_TIMEOUT", "20")))
    parser.add_argument("--cleanup", action="store_true", help="Drop the smoke table and namespace before exiting.")
    parser.add_argument("--replace", action="store_true", help="Drop an existing table with the same identifier first.")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification for HTTPS endpoints.")
    parser.add_argument("--print-client-matrix", action="store_true", help="Print the current client conformance matrix as JSON and exit.")
    parser.add_argument("--print-vendor-profiles", action="store_true", help="Print vendor connection profile references as JSON and exit.")
    parser.add_argument("--print-unsupported-inventory", action="store_true", help="Print unsupported capability inventory as JSON and exit.")
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
        raise RuntimeError(f"{method} {path} failed with HTTP {error.code}: {response_body}") from error


def ensure_bucket(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    client = deps.boto3.client(
        "s3",
        endpoint_url=normalized_endpoint(args.endpoint),
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name=args.region,
        verify=not args.insecure,
        config=deps.botocore_config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
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


def enable_table_bucket(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    encoded_bucket = urllib.parse.quote(args.bucket, safe="")
    signed_rest_request(args, deps, "PUT", f"{args.rest_path}/v1/buckets/{encoded_bucket}")


def catalog_properties(args: argparse.Namespace) -> dict[str, str]:
    endpoint = normalized_endpoint(args.endpoint)
    properties = {
        "type": "rest",
        "uri": f"{endpoint}{args.rest_path}",
        "warehouse": args.bucket,
        "prefix": args.bucket,
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
    if args.print_vendor_profiles:
        print_json_document({"vendor_profiles": vendor_profiles()})
        printed = True
    if args.print_unsupported_inventory:
        print_json_document({"unsupported_inventory": unsupported_inventory()})
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


def run_smoke(args: argparse.Namespace, deps: RuntimeDeps) -> None:
    endpoint = normalized_endpoint(args.endpoint)
    ensure_local_proxy_bypass(endpoint)
    ensure_aws_env(args.access_key, args.secret_key, args.region)

    print(f"[1/7] ensuring S3 bucket {args.bucket}")
    ensure_bucket(args, deps)

    print(f"[2/7] enabling RustFS table bucket {args.bucket} through {args.rest_path}/v1")
    enable_table_bucket(args, deps)

    print(f"[3/7] loading PyIceberg REST catalog at {endpoint}{args.rest_path}")
    catalog = deps.load_catalog(args.catalog_name, **catalog_properties(args))
    install_rustfs_rest_sigv4_adapter(catalog, args, deps)
    identifier = table_identifier(args)

    if args.replace:
        print(f"[4/7] replacing existing table {'.'.join(identifier)} if present")
        drop_table_if_present(catalog, identifier)
    else:
        print(f"[4/7] table {'.'.join(identifier)} is available")

    print(f"[5/7] creating namespace and table {'.'.join(identifier)}")
    ensure_namespace(catalog, args.namespace)
    arrow_schema = deps.pyarrow.schema(
        [
            deps.pyarrow.field("id", deps.pyarrow.int64(), nullable=False),
            deps.pyarrow.field("payload", deps.pyarrow.string(), nullable=False),
        ]
    )
    table = catalog.create_table(identifier, schema=arrow_schema)

    print(f"[6/7] appending rows through PyIceberg")
    rows = deps.pyarrow.Table.from_pylist(
        [
            {"id": 1, "payload": "alpha"},
            {"id": 2, "payload": "beta"},
        ],
        schema=arrow_schema,
    )
    table.append(rows)

    print(f"[7/7] reloading and scanning table")
    loaded = catalog.load_table(identifier)
    scanned = loaded.scan().to_arrow()
    if scanned.num_rows != 2:
        raise RuntimeError(f"expected 2 rows after append, got {scanned.num_rows}")

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
