#!/usr/bin/env python3
"""Read-only, version-aware content oracle for isolated scanner benchmarks."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def identity(row):
    return row["bucket"], row["key"], row["version_id"]


def validate_inventory(inventory):
    require(type(inventory) is dict and set(inventory) == {"buckets", "versions"}, "invalid inventory fields")
    buckets, rows = inventory["buckets"], inventory["versions"]
    require(type(buckets) is list and buckets and all(isinstance(b, str) and re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", b)
            for b in buckets) and len(set(buckets)) == len(buckets), "invalid bucket scope")
    require(type(rows) is list, "invalid version inventory")
    physical, logical, latest = set(), set(), {}
    for row in rows:
        require(type(row) is dict and set(row) == {"bucket", "key", "generation", "version_id", "is_latest",
                "delete_marker", "bytes", "sha256"}, "invalid version fields")
        require(row["bucket"] in buckets and isinstance(row["key"], str) and row["key"], "invalid object identity")
        require(isinstance(row["version_id"], str) and row["version_id"], "missing server version ID")
        require(isinstance(row["generation"], str) and re.fullmatch(r"[A-Za-z0-9._-]+", row["generation"]), "invalid logical generation")
        require(type(row["is_latest"]) is bool and type(row["delete_marker"]) is bool, "invalid version flags")
        require(type(row["bytes"]) is int and row["bytes"] >= 0, "invalid object length")
        if row["delete_marker"]:
            require(row["bytes"] == 0 and row["sha256"] is None, "delete marker has content")
        else:
            require(isinstance(row["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", row["sha256"]), "invalid content digest")
        key = row["bucket"], row["key"]
        generation = (*key, row["generation"])
        require(identity(row) not in physical and generation not in logical, "duplicate version identity")
        physical.add(identity(row))
        logical.add(generation)
        latest[key] = latest.get(key, 0) + row["is_latest"]
    require(all(count == 1 for count in latest.values()), "each object needs exactly one latest version")


def summary(rows):
    ordered = sorted(rows, key=lambda row: (row["bucket"], row["key"], row["generation"]))
    # Server-generated IDs differ between isolated legs. Bind them separately;
    # the comparable digest retains each independently planned logical generation.
    canonical = [{k: v for k, v in row.items() if k != "version_id"} for row in ordered]
    return {"objects": sum(row["is_latest"] and not row["delete_marker"] for row in rows),
            "versions": len(rows), "bytes": sum(row["bytes"] for row in rows), "sha256": digest(canonical)}


def list_versions(client, buckets, page_size, limit):
    found = {}
    for bucket in buckets:
        cursor, seen = {}, set()
        while True:
            previous_count = len(found)
            page = client.list_object_versions(Bucket=bucket, MaxKeys=page_size, **cursor)
            require(type(page.get("IsTruncated")) is bool, "missing version pagination state")
            require(not page.get("CommonPrefixes"), "unexpected grouped version listing")
            for field, deleted in (("Versions", False), ("DeleteMarkers", True)):
                require(type(page.get(field, [])) is list, "invalid version page")
                for item in page.get(field, []):
                    require(isinstance(item.get("Key"), str) and item["Key"]
                            and isinstance(item.get("VersionId"), str) and item["VersionId"]
                            and type(item.get("IsLatest")) is bool, "invalid listed version")
                    size = 0 if deleted else item.get("Size")
                    require(type(size) is int and size >= 0, "invalid listed size")
                    row = {"bucket": bucket, "key": item["Key"], "version_id": item["VersionId"],
                           "is_latest": item["IsLatest"], "delete_marker": deleted, "bytes": size}
                    require(identity(row) not in found, "duplicate listed version")
                    found[identity(row)] = row
                    require(len(found) <= limit, "version namespace exceeds inventory")
            if not page["IsTruncated"]:
                break
            require(len(found) > previous_count, "truncated version page made no progress")
            marker = page.get("NextKeyMarker"), page.get("NextVersionIdMarker")
            require(all(isinstance(value, str) and value for value in marker) and marker not in seen,
                    "missing or repeated version marker")
            seen.add(marker)
            cursor = dict(zip(("KeyMarker", "VersionIdMarker"), marker))
    return found


def list_current(client, buckets, page_size, limit):
    found = {}
    for bucket in buckets:
        cursor, seen = {}, set()
        while True:
            previous_count = len(found)
            page = client.list_objects_v2(Bucket=bucket, MaxKeys=page_size, **cursor)
            require(type(page.get("IsTruncated")) is bool and not page.get("CommonPrefixes"), "invalid current listing")
            for item in page.get("Contents", []):
                key, size = item.get("Key"), item.get("Size")
                require(isinstance(key, str) and key and type(size) is int and size >= 0, "invalid current object")
                require((bucket, key) not in found, "duplicate current object")
                found[bucket, key] = size
                require(len(found) <= limit, "current namespace exceeds inventory")
            if not page["IsTruncated"]:
                break
            require(len(found) > previous_count, "truncated current page made no progress")
            token = page.get("NextContinuationToken")
            require(isinstance(token, str) and token and token not in seen, "missing or repeated current marker")
            seen.add(token)
            cursor = {"ContinuationToken": token}
    return found


def read_content(client, row, current=False):
    request = {"Bucket": row["bucket"], "Key": row["key"]}
    if not current:
        request["VersionId"] = row["version_id"]
    if row["delete_marker"]:
        try:
            response = client.get_object(**request)
        except Exception as error:
            response = getattr(error, "response", {})
            metadata = response.get("ResponseMetadata", {})
            headers = metadata.get("HTTPHeaders", {})
            require(metadata.get("HTTPStatusCode") == (404 if current else 405)
                    and headers.get("x-amz-delete-marker") == "true"
                    and headers.get("x-amz-version-id") == row["version_id"], "delete marker GET contract failed")
            return 0, None
        else:
            if "Body" in response:
                response["Body"].close()
            raise ValueError("delete marker unexpectedly returned content")
    response = client.get_object(**request)
    body = response["Body"]
    try:
        require(response.get("VersionId", "null") == row["version_id"] and not response.get("DeleteMarker"), "GET version mismatch")
        require(type(response.get("ContentLength")) is int and response["ContentLength"] == row["bytes"], "GET length mismatch")
        length, checksum = 0, hashlib.sha256()
        while chunk := body.read(1024 * 1024):
            length += len(chunk)
            require(length <= row["bytes"], "GET body exceeds expected length")
            checksum.update(chunk)
        actual_hash = checksum.hexdigest()
        require(length == row["bytes"] and actual_hash == row["sha256"], "GET content mismatch")
        return length, actual_hash
    finally:
        body.close()


def verify_endpoint(client, inventory, workers, page_size):
    expected = {identity(row): row for row in inventory["versions"]}
    before = list_versions(client, inventory["buckets"], page_size, len(expected))
    require(set(before) == set(expected), "version namespace mismatch")
    for key, row in before.items():
        require(all(expected[key][field] == value for field, value in row.items()), "version metadata mismatch")
    latest = [row for row in inventory["versions"] if row["is_latest"]]
    current = {(row["bucket"], row["key"]): row["bytes"] for row in latest if not row["delete_marker"]}
    require(list_current(client, inventory["buckets"], page_size, len(current)) == current, "current namespace mismatch")
    observed = []

    def read(row):
        length, checksum = read_content(client, row)
        return {**before[identity(row)], "generation": row["generation"], "bytes": length, "sha256": checksum}

    rows = inventory["versions"]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        # Batches bound queued requests as well as active workers.
        for offset in range(0, len(rows), workers):
            observed.extend(pool.map(read, rows[offset:offset + workers]))
        for offset in range(0, len(latest), workers):
            list(pool.map(lambda row: read_content(client, row, current=True), latest[offset:offset + workers]))
    require(list_versions(client, inventory["buckets"], page_size, len(expected)) == before, "version namespace changed during oracle")
    require(list_current(client, inventory["buckets"], page_size, len(current)) == current, "current namespace changed during oracle")
    actual = summary(observed)
    require(actual == summary(rows), "content/version summary mismatch")
    return {"actual": actual, "observed": observed,
            "version_bindings_sha256": digest(sorted(before.values(), key=identity)),
            "get_requests": len(rows) + len(latest)}


def verify(clients, inventory, workers=8, page_size=1000):
    validate_inventory(inventory)
    require(type(workers) is int and 1 <= workers <= 64, "invalid worker count")
    require(type(page_size) is int and 1 <= page_size <= 1000, "invalid page size")
    require(isinstance(clients, dict) and clients, "missing endpoint clients")
    results = {name: verify_endpoint(client, inventory, workers, page_size) for name, client in clients.items()}
    return {"complete": True, "errors": 0, "actual": next(iter(results.values()))["actual"], "endpoints": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--endpoint", action="append", required=True, help="unique NAME=http[s]://host:port; credentials use AWS environment")
    parser.add_argument("--output", type=Path, required=True, help="new evidence JSON file, including observed versions")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--page-size", type=int, default=1000)
    args = parser.parse_args()
    try:
        import boto3
        from botocore.config import Config

        clients = {}
        for endpoint in args.endpoint:
            name, url = endpoint.split("=", 1)
            parsed = urlsplit(url)
            require(re.fullmatch(r"[A-Za-z0-9_-]+", name) and name not in clients, "invalid endpoint name")
            require(parsed.scheme in ("http", "https") and parsed.hostname and not parsed.username
                    and not parsed.password and not parsed.query and not parsed.fragment and parsed.path in ("", "/"), "invalid endpoint URL")
            clients[name] = boto3.client("s3", endpoint_url=url, region_name="us-east-1", config=Config(
                signature_version="s3v4", s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=30,
                retries={"total_max_attempts": 1}))
        with args.output.open("x") as output:
            result = verify(clients, json.loads(args.inventory.read_text()), args.workers, args.page_size)
            json.dump(result, output, sort_keys=True)
            output.write("\n")
    except Exception:
        # SDK errors can include server-supplied content; do not emit raw errors.
        print("version oracle failed; no passing evidence produced", file=sys.stderr)
        return 1
    print(json.dumps({key: result[key] for key in ("complete", "errors", "actual")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
