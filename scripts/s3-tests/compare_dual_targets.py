#!/usr/bin/env python3
"""Send one S3 request to two endpoints and summarize response differences."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import hmac
import http.client
import json
import os
import pathlib
import ssl
import sys
import urllib.parse
from dataclasses import dataclass
from difflib import unified_diff
from typing import Iterable


DEFAULT_IGNORE_HEADERS = {
    "date",
    "server",
    "x-amz-id-2",
    "x-amz-request-id",
    "x-rustfs-deployment-id",
}


@dataclass
class Endpoint:
    label: str
    url: str


@dataclass
class SignedRequest:
    method: str
    endpoint: Endpoint
    path_and_query: str
    headers: list[tuple[str, str]]
    body: bytes


@dataclass
class ResponseSnapshot:
    label: str
    url: str
    status: int
    reason: str
    headers: list[tuple[str, str]]
    body: bytes

    def normalized_headers(self, ignored: set[str]) -> dict[str, list[str]]:
        values: dict[str, list[str]] = {}
        for name, value in self.headers:
            key = name.lower()
            if key in ignored:
                continue
            values.setdefault(key, []).append(value)
        for key in values:
            values[key].sort()
        return dict(sorted(values.items()))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left-url", required=True, help="Reference endpoint base URL, for example http://127.0.0.1:9000")
    parser.add_argument("--right-url", required=True, help="Candidate endpoint base URL, for example http://127.0.0.1:9001")
    parser.add_argument("--left-label", default="reference", help="Label used in reports for the left endpoint")
    parser.add_argument("--right-label", default="candidate", help="Label used in reports for the right endpoint")
    parser.add_argument("--method", default="GET", help="HTTP method")
    parser.add_argument("--path", required=True, help="Absolute request path with optional query, for example /bucket/object?versionId=1")
    parser.add_argument("--header", action="append", default=[], help="Extra header in 'Name: Value' form")
    parser.add_argument("--body-file", help="Read request body from file")
    parser.add_argument("--body", default="", help="Inline request body string")
    parser.add_argument("--content-type", help="Convenience setter for Content-Type")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"), help="Signing region")
    parser.add_argument("--service", default="s3", help="Signing service name")
    parser.add_argument("--access-key", default=os.getenv("AWS_ACCESS_KEY_ID", ""), help="Access key for SigV4 signing")
    parser.add_argument("--secret-key", default=os.getenv("AWS_SECRET_ACCESS_KEY", ""), help="Secret key for SigV4 signing")
    parser.add_argument("--session-token", default=os.getenv("AWS_SESSION_TOKEN", ""), help="Optional session token")
    parser.add_argument("--unsigned", action="store_true", help="Send request without SigV4 signing")
    parser.add_argument("--timeout", type=float, default=30.0, help="Per-request timeout in seconds")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS certificate verification")
    parser.add_argument(
        "--ignore-header",
        action="append",
        default=[],
        help="Response header name to ignore during comparison. Can be provided multiple times.",
    )
    parser.add_argument("--output-dir", default="artifacts/s3-compare/latest", help="Directory for snapshots and summary")
    return parser.parse_args()


def parse_header(raw: str) -> tuple[str, str]:
    name, sep, value = raw.partition(":")
    if not sep:
        raise ValueError(f"invalid header {raw!r}, expected 'Name: Value'")
    name = name.strip()
    value = value.strip()
    if not name:
        raise ValueError(f"invalid header {raw!r}, empty name")
    return name, value


def load_body(args: argparse.Namespace) -> bytes:
    if args.body_file:
        return pathlib.Path(args.body_file).read_bytes()
    return args.body.encode()


def payload_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def sign(key: bytes, value: str) -> bytes:
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


def derive_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    key_date = sign(("AWS4" + secret_key).encode(), date_stamp)
    key_region = sign(key_date, region)
    key_service = sign(key_region, service)
    return sign(key_service, "aws4_request")


def canonical_query(query: str) -> str:
    pairs = urllib.parse.parse_qsl(query, keep_blank_values=True)
    encoded = [
        (
            urllib.parse.quote(key, safe="-_.~"),
            urllib.parse.quote(value, safe="-_.~"),
        )
        for key, value in pairs
    ]
    encoded.sort()
    return "&".join(f"{key}={value}" for key, value in encoded)


def canonical_uri(path: str) -> str:
    segments = path.split("/")
    return "/".join(urllib.parse.quote(segment, safe="-_.~/") for segment in segments) or "/"


def build_signed_request(
    endpoint: Endpoint,
    method: str,
    raw_path: str,
    headers: list[tuple[str, str]],
    body: bytes,
    args: argparse.Namespace,
) -> SignedRequest:
    parsed = urllib.parse.urlsplit(endpoint.url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"invalid endpoint URL: {endpoint.url}")
    if not raw_path.startswith("/"):
        raise ValueError("--path must start with '/'")

    path, _, query = raw_path.partition("?")
    amz_date = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]

    request_headers: list[tuple[str, str]] = []
    for name, value in headers:
        request_headers.append((name, value))

    if not any(name.lower() == "host" for name, _ in request_headers):
        request_headers.append(("Host", parsed.netloc))
    if not any(name.lower() == "x-amz-content-sha256" for name, _ in request_headers):
        request_headers.append(("x-amz-content-sha256", payload_hash(body)))
    if not any(name.lower() == "x-amz-date" for name, _ in request_headers):
        request_headers.append(("x-amz-date", amz_date))
    if args.session_token and not any(name.lower() == "x-amz-security-token" for name, _ in request_headers):
        request_headers.append(("x-amz-security-token", args.session_token))

    if args.unsigned:
        return SignedRequest(method=method, endpoint=endpoint, path_and_query=raw_path, headers=request_headers, body=body)

    if not args.access_key or not args.secret_key:
        raise ValueError("SigV4 signing requires --access-key and --secret-key, or set --unsigned")

    normalized = [(name.lower().strip(), " ".join(value.strip().split())) for name, value in request_headers]
    normalized.sort()
    canonical_headers = "".join(f"{name}:{value}\n" for name, value in normalized)
    signed_headers = ";".join(name for name, _ in normalized)

    canonical_request = "\n".join(
        [
            method,
            canonical_uri(path),
            canonical_query(query),
            canonical_headers,
            signed_headers,
            payload_hash(body),
        ]
    )
    scope = f"{date_stamp}/{args.region}/{args.service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signature = hmac.new(
        derive_signing_key(args.secret_key, date_stamp, args.region, args.service),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={args.access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )
    request_headers.append(("Authorization", authorization))

    return SignedRequest(method=method, endpoint=endpoint, path_and_query=raw_path, headers=request_headers, body=body)


def send_request(request: SignedRequest, timeout: float, insecure: bool) -> ResponseSnapshot:
    parsed = urllib.parse.urlsplit(request.endpoint.url)
    if parsed.scheme == "https":
        context = ssl.create_default_context()
        if insecure:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        conn: http.client.HTTPConnection = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=timeout, context=context)
    elif parsed.scheme == "http":
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=timeout)
    else:
        raise ValueError(f"unsupported URL scheme: {parsed.scheme}")

    conn.request(request.method, request.path_and_query, body=request.body, headers=dict(request.headers))
    response = conn.getresponse()
    body = response.read()
    headers = response.getheaders()
    conn.close()
    return ResponseSnapshot(
        label=request.endpoint.label,
        url=request.endpoint.url,
        status=response.status,
        reason=response.reason,
        headers=headers,
        body=body,
    )


def body_as_text(body: bytes) -> str | None:
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        return None


def summarize_diff(left: ResponseSnapshot, right: ResponseSnapshot, ignored_headers: set[str]) -> dict:
    header_diff = {
        "left_only": {},
        "right_only": {},
        "different": {},
    }
    left_headers = left.normalized_headers(ignored_headers)
    right_headers = right.normalized_headers(ignored_headers)
    left_keys = set(left_headers)
    right_keys = set(right_headers)

    for key in sorted(left_keys - right_keys):
        header_diff["left_only"][key] = left_headers[key]
    for key in sorted(right_keys - left_keys):
        header_diff["right_only"][key] = right_headers[key]
    for key in sorted(left_keys & right_keys):
        if left_headers[key] != right_headers[key]:
            header_diff["different"][key] = {
                left.label: left_headers[key],
                right.label: right_headers[key],
            }

    left_text = body_as_text(left.body)
    right_text = body_as_text(right.body)
    if left_text is not None and right_text is not None:
        body_diff = {
            "kind": "text",
            "equal": left_text == right_text,
            "unified_diff": list(
                unified_diff(
                    left_text.splitlines(),
                    right_text.splitlines(),
                    fromfile=left.label,
                    tofile=right.label,
                    lineterm="",
                )
            ),
        }
    else:
        body_diff = {
            "kind": "binary",
            "equal": left.body == right.body,
            left.label: {
                "size": len(left.body),
                "sha256": hashlib.sha256(left.body).hexdigest(),
            },
            right.label: {
                "size": len(right.body),
                "sha256": hashlib.sha256(right.body).hexdigest(),
            },
        }

    return {
        "status_equal": left.status == right.status,
        "status": {
            left.label: {"code": left.status, "reason": left.reason},
            right.label: {"code": right.status, "reason": right.reason},
        },
        "headers_equal": not any(header_diff.values()),
        "header_diff": header_diff,
        "body_equal": body_diff["equal"],
        "body_diff": body_diff,
    }


def write_snapshot(output_dir: pathlib.Path, response: ResponseSnapshot) -> dict:
    endpoint_dir = output_dir / response.label
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    body_file = endpoint_dir / "body.bin"
    body_file.write_bytes(response.body)
    headers_file = endpoint_dir / "headers.json"
    headers_file.write_text(json.dumps(response.headers, indent=2, ensure_ascii=False) + "\n")
    summary = {
        "label": response.label,
        "url": response.url,
        "status": response.status,
        "reason": response.reason,
        "headers_file": str(headers_file),
        "body_file": str(body_file),
        "body_sha256": hashlib.sha256(response.body).hexdigest(),
        "body_base64_preview": base64.b64encode(response.body[:96]).decode(),
    }
    (endpoint_dir / "response.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
    return summary


def print_report(diff: dict, left: ResponseSnapshot, right: ResponseSnapshot) -> None:
    print(f"Compared {left.label} <-> {right.label}")
    print(f"Status: {left.status} vs {right.status}")
    print(f"Headers equal: {diff['headers_equal']}")
    print(f"Body equal: {diff['body_equal']}")

    header_diff = diff["header_diff"]
    if header_diff["left_only"] or header_diff["right_only"] or header_diff["different"]:
        print("Header differences detected:")
        if header_diff["left_only"]:
            print(f"  left only: {sorted(header_diff['left_only'])}")
        if header_diff["right_only"]:
            print(f"  right only: {sorted(header_diff['right_only'])}")
        if header_diff["different"]:
            print(f"  changed: {sorted(header_diff['different'])}")

    body_diff = diff["body_diff"]
    if body_diff["kind"] == "text" and not body_diff["equal"]:
        preview = body_diff["unified_diff"][:40]
        if preview:
            print("Body diff preview:")
            for line in preview:
                print(line)


def main() -> int:
    args = parse_args()
    try:
        extra_headers = [parse_header(raw) for raw in args.header]
        if args.content_type and not any(name.lower() == "content-type" for name, _ in extra_headers):
            extra_headers.append(("Content-Type", args.content_type))

        body = load_body(args)
        output_dir = pathlib.Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        left_endpoint = Endpoint(label=args.left_label, url=args.left_url.rstrip("/"))
        right_endpoint = Endpoint(label=args.right_label, url=args.right_url.rstrip("/"))
        method = args.method.upper()

        requests = [
            build_signed_request(left_endpoint, method, args.path, extra_headers, body, args),
            build_signed_request(right_endpoint, method, args.path, extra_headers, body, args),
        ]
        responses = [send_request(request, args.timeout, args.insecure) for request in requests]
        left, right = responses

        ignored_headers = {name.lower() for name in DEFAULT_IGNORE_HEADERS}
        ignored_headers.update(name.lower() for name in args.ignore_header)

        report = {
            "request": {
                "method": method,
                "path": args.path,
                "headers": requests[0].headers,
                "body_sha256": payload_hash(body),
                "body_size": len(body),
                "signed": not args.unsigned,
            },
            "responses": [write_snapshot(output_dir, response) for response in responses],
        }
        report["diff"] = summarize_diff(left, right, ignored_headers)
        (output_dir / "summary.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        print_report(report["diff"], left, right)
        return 0 if report["diff"]["status_equal"] and report["diff"]["headers_equal"] and report["diff"]["body_equal"] else 1
    except Exception as exc:  # noqa: BLE001
        print(f"compare_dual_targets.py failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
