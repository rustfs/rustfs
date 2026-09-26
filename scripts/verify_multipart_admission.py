#!/usr/bin/env python3
"""Verify concurrent multipart uploads, per-attempt errors, and full readback.

Install boto3 and provide AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY for a disposable
test endpoint. Each invocation creates and removes its own uniquely named bucket.
Example: python3 scripts/verify_multipart_admission.py --endpoint http://localhost:9000 --uploads 24 --workers 16 --size-mib 400 --out /tmp/multipart-rung4
The issue #7385 reproduction used boto3/botocore 1.43.66 and urllib3 2.7.0.
By default each part gets one attempt, so throttling cannot be hidden by retries.
"""

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import platform
import threading
import time
import uuid
from collections import Counter
from pathlib import Path

import boto3
import botocore
import urllib3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

p = argparse.ArgumentParser()
p.add_argument("--endpoint", required=True)
p.add_argument("--uploads", type=int, default=16)
p.add_argument("--workers", type=int, default=16)
p.add_argument("--size-mib", type=int, default=400)
p.add_argument("--part-mib", type=int, default=8)
p.add_argument("--connect-timeout", type=int, default=30)
p.add_argument("--attempts", type=int, default=1)
p.add_argument("--out", required=True)
args = p.parse_args()
if any(
    getattr(args, key) <= 0
    for key in [
        "uploads",
        "workers",
        "size_mib",
        "part_mib",
        "connect_timeout",
        "attempts",
    ]
):
    p.error("counts, sizes, timeouts, and attempts must be positive")
if args.part_mib < 5 and args.size_mib > args.part_mib:
    p.error("non-final multipart parts must be at least 5 MiB")
if not os.environ.get("AWS_ACCESS_KEY_ID") or not os.environ.get(
    "AWS_SECRET_ACCESS_KEY"
):
    p.error("set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for the test endpoint")
out = Path(args.out)
out.mkdir(parents=True, exist_ok=False)
os.environ["NO_PROXY"] = "*"
config = Config(
    connect_timeout=args.connect_timeout,
    read_timeout=120,
    retries={"total_max_attempts": args.attempts, "mode": "standard"},
    max_pool_connections=args.uploads * args.workers,
    s3={"addressing_style": "path"},
    proxies={},
)
client = boto3.client(
    "s3",
    endpoint_url=args.endpoint,
    region_name="us-east-1",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
    config=config,
)
bucket = "multipart-admission-" + uuid.uuid4().hex[:20]
size = args.size_mib * 1024 * 1024
part_size = args.part_mib * 1024 * 1024
payload = os.urandom(part_size)
part_count = (size + part_size - 1) // part_size
expected = hashlib.sha256()
for part_index in range(part_count):
    expected.update(payload[: min(part_size, size - part_index * part_size)])
expected_digest = expected.hexdigest()
uploads = []
records, attempts = [], []
lock = threading.Lock()
start = time.monotonic()


def retry_event(**kw):
    response = kw.get("response")
    parsed = response[1] if response else {}
    request = kw.get("request_dict") or {}
    with lock:
        attempts.append(
            {
                "t": time.monotonic() - start,
                "attempt": kw.get("attempts"),
                "query": request.get("query_string"),
                "status": parsed.get("ResponseMetadata", {}).get("HTTPStatusCode"),
                "error": parsed.get("Error"),
                "exception": repr(kw.get("caught_exception")),
            }
        )


client.meta.events.register("needs-retry.s3.UploadPart", retry_event)
barrier = threading.Barrier(args.uploads)


def upload_part(key, upload_id, part_number):
    began = time.monotonic()
    record = {"key": key, "part": part_number, "start": began - start}
    try:
        body = payload[: min(part_size, size - (part_number - 1) * part_size)]
        response = client.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
            ContentLength=len(body),
        )
        record.update(
            ok=True, etag=response["ETag"], metadata=response["ResponseMetadata"]
        )
    except (BotoCoreError, ClientError, OSError) as exc:
        record.update(
            ok=False,
            error_type=type(exc).__name__,
            error=str(exc),
            response=getattr(exc, "response", None),
            inner=repr(getattr(exc, "kwargs", {}).get("error")),
        )
    record["duration"] = time.monotonic() - began
    with lock:
        records.append(record)
    return record


def upload_object(item):
    key, upload_id = item
    barrier.wait()
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        parts = list(
            pool.map(
                lambda part_number: upload_part(key, upload_id, part_number),
                range(1, part_count + 1),
            )
        )
    result = {"key": key, "ok": all(r["ok"] for r in parts)}
    if result["ok"]:
        try:
            response = client.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={
                    "Parts": [
                        {"PartNumber": r["part"], "ETag": r["etag"]} for r in parts
                    ]
                },
            )
            result["metadata"] = response["ResponseMetadata"]
        except (BotoCoreError, ClientError, OSError) as exc:
            result.update(ok=False, error_type=type(exc).__name__, error=str(exc))
    result["finished"] = time.monotonic() - start
    print(json.dumps(result), flush=True)
    return result


bucket_created = False
summary = None
cleanup_errors = []
try:
    client.create_bucket(Bucket=bucket)
    bucket_created = True
    for i in range(args.uploads):
        key = f"object-{i:02}"
        upload_id = client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        uploads.append((key, upload_id))
    started_at_unix = time.time()
    start = time.monotonic()
    with cf.ThreadPoolExecutor(max_workers=args.uploads) as pool:
        objects = list(pool.map(upload_object, uploads))
    elapsed = time.monotonic() - start
    upload_finished_at_unix = time.time()
    for result in objects:
        if result["ok"]:
            try:
                response = client.get_object(Bucket=bucket, Key=result["key"])
                digest, received = hashlib.sha256(), 0
                stream = response["Body"]
                try:
                    for chunk in stream.iter_chunks(1024 * 1024):
                        digest.update(chunk)
                        received += len(chunk)
                finally:
                    stream.close()
                result["readback_bytes"] = received
                result["readback_sha256"] = digest.hexdigest()
                result["verified"] = (
                    received == size and result["readback_sha256"] == expected_digest
                )
                if not result["verified"]:
                    result["verification_error"] = "body length or SHA-256 mismatch"
            except (BotoCoreError, ClientError, OSError) as exc:
                result.update(verified=False, verification_error=str(exc))
    summary = {
        "config": vars(args),
        "boto3": boto3.__version__,
        "botocore": botocore.__version__,
        "urllib3": urllib3.__version__,
        "platform": platform.platform(),
        "effective_retries": client.meta.config.retries,
        "elapsed": elapsed,
        "started_at_unix": started_at_unix,
        "upload_finished_at_unix": upload_finished_at_unix,
        "expected_sha256": expected_digest,
        "objects_passed": sum(r["ok"] for r in objects),
        "objects_verified": sum(r.get("verified", False) for r in objects),
        "parts_passed": sum(r["ok"] for r in records),
        "parts_total": len(records),
        "errors": dict(
            Counter(
                (r.get("response") or {})
                .get("Error", {})
                .get("Code", r.get("error_type"))
                for r in records
                if not r["ok"]
            )
        ),
        "attempts": len(attempts),
        "objects": objects,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2), flush=True)
finally:
    (out / "parts.json").write_text(json.dumps(records, indent=2))
    (out / "attempts.json").write_text(json.dumps(attempts, indent=2))
    for key, upload_id in uploads:
        try:
            client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "NoSuchUpload":
                cleanup_errors.append(
                    {"key": key, "operation": "abort", "error": str(exc)}
                )
        except (BotoCoreError, OSError) as exc:
            cleanup_errors.append({"key": key, "operation": "abort", "error": str(exc)})
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except (BotoCoreError, ClientError, OSError) as exc:
            cleanup_errors.append(
                {"key": key, "operation": "delete", "error": str(exc)}
            )
    if bucket_created:
        try:
            client.delete_bucket(Bucket=bucket)
        except (BotoCoreError, ClientError, OSError) as exc:
            cleanup_errors.append({"bucket": bucket, "error": str(exc)})
    if cleanup_errors:
        (out / "cleanup-errors.json").write_text(json.dumps(cleanup_errors, indent=2))
    client.close()
raise SystemExit(
    0
    if summary and summary["objects_verified"] == args.uploads and not cleanup_errors
    else 1
)
