#!/usr/bin/env python3
"""Verify RustFS dynamic EC block hints against a disposable S3 endpoint.

Requires boto3 and AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY. See
docs/testing/dynamic-block-size.md for rollout and restart probes.
"""

import argparse
import hashlib
import json
import os
import re
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

MIB = 1024 * 1024
CANDIDATES = (64 * 1024, 256 * 1024, MIB, 4 * MIB)
HINT = "rustfs-ec-block-size-hint"
ACTUAL = "x-rustfs-effective-ec-block-size"
STATUS = "x-rustfs-layout-hint-status"
REASON = "x-rustfs-layout-hint-reason"
SIZE = 4 * MIB + 123
PART_SIZES = (5 * MIB + 123, MIB + 77)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def headers(response):
    return response["ResponseMetadata"]["HTTPHeaders"]


def check_layout(response, expected, label, status=None, reason=None):
    values = headers(response)
    require(
        values.get(ACTUAL) == str(expected),
        f"{label}: actual B {values.get(ACTUAL)!r}, expected {expected}",
    )
    require(
        values.get(STATUS) == status,
        f"{label}: hint status {values.get(STATUS)!r}, expected {status!r}",
    )
    require(
        values.get(REASON) == reason,
        f"{label}: hint reason {values.get(REASON)!r}, expected {reason!r}",
    )
    require(
        not any("internal-ec-read-quantum" in key for key in values),
        f"{label}: internal q leaked over S3",
    )


def payload(key, size):
    return hashlib.shake_256(key.encode()).digest(size)


def sha256_argument(value):
    if re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
        raise argparse.ArgumentTypeError(
            "SHA-256 must contain exactly 64 hexadecimal characters"
        )
    return value.lower()


def read_body(response):
    try:
        return response["Body"].read()
    finally:
        response["Body"].close()


def verify_object(client, bucket, key, expected, block_size, part_boundary=None):
    head = client.head_object(Bucket=bucket, Key=key)
    check_layout(head, block_size, f"HEAD {key}")
    require(head["ContentLength"] == len(expected), f"HEAD {key}: size mismatch")
    full = client.get_object(Bucket=bucket, Key=key)
    check_layout(full, block_size, f"GET {key}")
    require(read_body(full) == expected, f"GET {key}: payload mismatch")
    ranges = []
    if expected:
        ranges.extend(
            [
                (0, min(122, len(expected) - 1)),
                (max(0, len(expected) - 123), len(expected) - 1),
            ]
        )
    if len(expected) > block_size:
        ranges.append((block_size - 37, min(block_size + 85, len(expected) - 1)))
    if part_boundary is not None:
        ranges.append((part_boundary - 37, min(part_boundary + 85, len(expected) - 1)))
    for start, end in ranges:
        response = client.get_object(
            Bucket=bucket, Key=key, Range=f"bytes={start}-{end}"
        )
        check_layout(response, block_size, f"Range {key} {start}-{end}")
        require(
            response["ResponseMetadata"]["HTTPStatusCode"] == 206,
            f"Range {key}: expected HTTP 206",
        )
        require(
            response["ContentRange"] == f"bytes {start}-{end}/{len(expected)}",
            f"Range {key}: content-range mismatch",
        )
        require(
            read_body(response) == expected[start : end + 1],
            f"Range {key}: payload mismatch",
        )
    print(
        json.dumps(
            {
                "key": key,
                "actual_b": block_size,
                "size": len(expected),
                "range_checks": len(ranges),
                "result": "passed",
            }
        )
    )


def ensure_absent(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
            return
        raise
    raise RuntimeError(f"refusing to overwrite existing object {bucket}/{key}")


def put_multipart(client, bucket, key, block_size, status, reason):
    initiated = client.create_multipart_upload(
        Bucket=bucket, Key=key, Metadata={HINT: str(block_size)}
    )
    upload_id = initiated["UploadId"]
    parts = []
    committed = False
    try:
        check_layout(
            initiated,
            block_size if status == "applied" else MIB,
            f"MPU initiate {key}",
            status,
            reason,
        )
        for number, size in enumerate(PART_SIZES, 1):
            response = client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=number,
                Body=payload(f"{key}:{number}", size),
            )
            parts.append({"PartNumber": number, "ETag": response["ETag"]})
        completed = client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
        committed = True
        check_layout(
            completed, block_size if status == "applied" else MIB, f"MPU complete {key}"
        )
    except BaseException:
        if not committed:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


def cases(policy):
    status = "applied" if policy == "applied" else "ignored"
    reason = None if status == "applied" else policy
    for block_size in CANDIDATES:
        actual = block_size if status == "applied" else MIB
        yield f"put-{block_size}", block_size, actual, status, reason, False
        yield f"mpu-{block_size}", block_size, actual, status, reason, True
    yield "put-default", None, MIB, None, None, False
    yield (
        "put-unsupported",
        131072,
        MIB,
        "ignored",
        "unsupported-size" if policy == "applied" else policy,
        False,
    )


def verify_external(client, args):
    require(
        args.external_block_size is not None,
        "--external-prefix requires --external-block-size",
    )
    found = 0
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=args.bucket, Prefix=args.external_prefix
    ):
        for item in page.get("Contents", []):
            key = item["Key"]
            response = client.get_object(Bucket=args.bucket, Key=key)
            expected = read_body(response)
            require(
                len(expected) == item["Size"],
                f"external {key}: full GET length mismatch",
            )
            if args.external_size is not None:
                require(
                    len(expected) == args.external_size,
                    f"external {key}: intended payload size mismatch",
                )
            if args.external_sha256 is not None:
                require(
                    hashlib.sha256(expected).hexdigest() == args.external_sha256,
                    f"external {key}: intended payload checksum mismatch",
                )
            verify_object(client, args.bucket, key, expected, args.external_block_size)
            found += 1
    require(found > 0, f"no external objects found under {args.external_prefix!r}")
    print(json.dumps({"external_objects": found, "result": "passed"}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--bucket", help="Disposable bucket; generated by default")
    parser.add_argument("--prefix", default="dynamic-block-e2e/")
    parser.add_argument(
        "--write-policy",
        choices=("applied", "disabled", "fleet-unconfirmed"),
        default="applied",
        help="Policy during object creation, including when verifying after restart",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep this script's bucket and objects for restart verification",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Read previously written objects; never delete them",
    )
    parser.add_argument(
        "--external-prefix",
        help="Only verify objects written by a client such as BrewFS",
    )
    parser.add_argument("--external-block-size", type=int, choices=CANDIDATES)
    parser.add_argument(
        "--external-size",
        type=int,
        help="Expected size in bytes of every external object",
    )
    parser.add_argument(
        "--external-sha256",
        type=sha256_argument,
        help="Expected SHA-256 of every external object",
    )
    args = parser.parse_args()
    if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get(
        "AWS_SECRET_ACCESS_KEY"
    ):
        parser.error(
            "set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for the disposable endpoint"
        )
    if (args.verify_only or args.external_prefix is not None) and not args.bucket:
        parser.error("--verify-only and --external-prefix require --bucket")
    if args.external_prefix is not None and not args.external_prefix:
        parser.error("--external-prefix must identify a nonempty task prefix")
    if args.external_size is not None and args.external_size < 0:
        parser.error("--external-size must be nonnegative")
    if (
        args.external_size is not None or args.external_sha256 is not None
    ) and args.external_prefix is None:
        parser.error("--external-size and --external-sha256 require --external-prefix")
    args.bucket = args.bucket or "dynamic-block-" + uuid.uuid4().hex[:20]
    client = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            proxies={},
            connect_timeout=5,
            read_timeout=120,
            retries={"total_max_attempts": 1},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    created = []
    created_bucket = False
    try:
        if args.external_prefix is not None:
            verify_external(client, args)
            return
        if not args.verify_only:
            try:
                client.head_bucket(Bucket=args.bucket)
            except ClientError as error:
                require(
                    error.response["ResponseMetadata"]["HTTPStatusCode"] == 404,
                    "test bucket lookup failed",
                )
            else:
                raise RuntimeError(f"refusing to reuse existing bucket {args.bucket}")
            client.create_bucket(Bucket=args.bucket)
            created_bucket = True
        for name, hint, actual, status, reason, multipart in cases(args.write_policy):
            key = args.prefix + name
            if multipart:
                expected = b"".join(
                    payload(f"{key}:{number}", size)
                    for number, size in enumerate(PART_SIZES, 1)
                )
            else:
                expected = payload(key, SIZE)
            if not args.verify_only:
                ensure_absent(client, args.bucket, key)
                created.append(key)
                if multipart:
                    put_multipart(client, args.bucket, key, hint, status, reason)
                else:
                    response = client.put_object(
                        Bucket=args.bucket,
                        Key=key,
                        Body=expected,
                        Metadata={} if hint is None else {HINT: str(hint)},
                    )
                    check_layout(response, actual, f"PUT {key}", status, reason)
            verify_object(
                client,
                args.bucket,
                key,
                expected,
                actual,
                PART_SIZES[0] if multipart else None,
            )
        if not args.verify_only and args.write_policy == "applied":
            key = args.prefix + "invalid-hint"
            ensure_absent(client, args.bucket, key)
            try:
                client.put_object(
                    Bucket=args.bucket,
                    Key=key,
                    Body=b"invalid-hint-probe",
                    Metadata={HINT: "+65536"},
                )
            except ClientError as error:
                require(
                    error.response["Error"]["Code"] == "InvalidArgument",
                    "malformed hint must fail with InvalidArgument",
                )
            else:
                created.append(key)
                raise AssertionError("malformed hint unexpectedly succeeded")
        print(
            json.dumps(
                {
                    "bucket": args.bucket,
                    "prefix": args.prefix,
                    "write_policy": args.write_policy,
                    "verify_only": args.verify_only,
                    "objects": 10,
                    "result": "passed",
                }
            )
        )
    finally:
        if not args.keep and not args.verify_only and args.external_prefix is None:
            for key in created:
                client.delete_object(Bucket=args.bucket, Key=key)
            if created_bucket:
                client.delete_bucket(Bucket=args.bucket)
        client.close()


if __name__ == "__main__":
    main()
