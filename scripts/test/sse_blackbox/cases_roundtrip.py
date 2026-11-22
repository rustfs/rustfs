"""Round trips and response projections (RT) and bucket default encryption (BD)."""

from __future__ import annotations

import json

import harness as h
from framework import Ctx, case

KEY_A = h.derived_key("ssec a")
SIZE = 70 * h.KIB


@case("RT-01", "SSE-S3 under the local master key", ["e1"])
def rt_01(c: Ctx) -> None:
    bucket = c.bucket()
    c.roundtrip(bucket, "object", h.plain("rt-01", SIZE), h.sse_s3(), None, h.projection_s3(), "SSE-S3")


@case("RT-02", "SSE-S3 under the KMS default key", ["e2"])
def rt_02(c: Ctx) -> None:
    bucket = c.bucket()
    c.roundtrip(bucket, "object", h.plain("rt-02", SIZE), h.sse_s3(), None, h.projection_s3(), "SSE-S3")
    c.check(h.DEFAULT_KEY in h.key_list(c.site), f"the KMS key list no longer contains the default key {h.DEFAULT_KEY}")


@case("RT-03", "SSE-KMS without a key id resolves to the KMS default key", ["e2"])
def rt_03(c: Ctx) -> None:
    bucket = c.bucket()
    status, body = h.kms_status(c.site)
    c.check(status == 200 and h.DEFAULT_KEY in json.dumps(body),
            f"KMS status does not report the default key {h.DEFAULT_KEY}: HTTP {status} {str(body)[:200]}")
    c.roundtrip(bucket, "object", h.plain("rt-03", SIZE), h.sse_kms(), None, h.projection_kms(h.DEFAULT_KEY), "SSE-KMS")


@case("RT-04", "SSE-KMS with an explicit key that is not the default", ["e2"])
def rt_04(c: Ctx) -> None:
    bucket = c.bucket()
    # KEY_1 differs from the default key, so a response naming the wrong key cannot pass.
    c.roundtrip(bucket, "object", h.plain("rt-04", SIZE), h.sse_kms(h.KEY_1), None, h.projection_kms(h.KEY_1), "SSE-KMS")


@case("RT-06", "SSE-C needs no server-side key source", ["e0"])
def rt_06(c: Ctx) -> None:
    bucket = c.bucket()
    c.roundtrip(bucket, "object", h.plain("rt-06", SIZE), h.ssec(KEY_A), h.ssec(KEY_A), h.projection_c(KEY_A), "SSE-C")


def _schemes(c: Ctx) -> list[tuple[str, dict, dict | None, dict]]:
    """(name, write headers, read headers, projection) for the schemes this configuration can serve."""
    schemes = [("sse-s3", h.sse_s3(), None, h.projection_s3()), ("sse-c", h.ssec(KEY_A), h.ssec(KEY_A), h.projection_c(KEY_A))]
    if c.config == "e2":
        schemes = [("sse-kms", h.sse_kms(h.KEY_1), None, h.projection_kms(h.KEY_1))]
    return schemes


@case("RT-07", "Object sizes 0, 1 and 70 KiB under each scheme", ["e1", "e2"])
def rt_07(c: Ctx) -> None:
    bucket = c.bucket()
    expected = {}
    for name, write_headers, read_headers, want in _schemes(c):
        for size in (0, 1, SIZE):
            key = f"{name}-{size}"
            with c.row(key):
                data = h.plain(f"rt-07 {key}", size)
                c.roundtrip(bucket, key, data, write_headers, read_headers, want, key)
                expected[key] = size
    listed = h.list_objects(c.site, bucket)
    for key, size in expected.items():
        c.check(listed.get(key) == size, f"ListObjectsV2 reports size {listed.get(key)} for {key}, expected the plaintext size {size}")


@case("RT-08", "Range reads of an SSE-KMS object", ["e2"])
def rt_08(c: Ctx) -> None:
    bucket = c.bucket()
    data = h.plain("rt-08", SIZE)
    c.ok(h.put(c.site, bucket, "object", data, h.sse_kms(h.KEY_1)), "PUT")
    last = SIZE - 1
    ranges = {
        "first-byte": ("bytes=0-0", 0, 0),
        "last-byte": (f"bytes={last}-{last}", last, last),
        "suffix-100": ("bytes=-100", SIZE - 100, last),
        "across-8k": ("bytes=8000-8500", 8000, 8500),
    }
    for name, (spec, first, end) in ranges.items():
        with c.row(name):
            status, headers, body = h.get(c.site, bucket, "object", range_=spec)
            c.check(status == 206, f"Range {spec}: HTTP {status}, expected 206")
            c.check(headers.get("content-range") == f"bytes {first}-{end}/{SIZE}",
                    f"Range {spec}: Content-Range {headers.get('content-range')!r}, expected 'bytes {first}-{end}/{SIZE}'")
            c.check(body == data[first:end + 1],
                    f"Range {spec}: body sha256 {h.sha256_hex(body)[:16]} len {len(body)} is not the plaintext slice")
            c.projection(headers, h.projection_kms(h.KEY_1), f"Range {spec}")


@case("BD-01", "Bucket default AES256 encrypts a request that names no scheme", ["e1"])
def bd_01(c: Ctx) -> None:
    bucket = c.bucket()
    c.ok(h.put_bucket_encryption(c.site, bucket, "AES256"), "PutBucketEncryption")
    c.roundtrip(bucket, "object", h.plain("bd-01", SIZE), None, None, h.projection_s3(), "no-header PUT")


@case("BD-02", "Bucket default aws:kms without a key id pins the KMS default key", ["e2"])
def bd_02(c: Ctx) -> None:
    bucket = c.bucket()
    c.ok(h.put_bucket_encryption(c.site, bucket, "aws:kms"), "PutBucketEncryption")
    status, config, _ = h.get_bucket_encryption(c.site, bucket)
    c.check(status == 200 and config.get("algorithm") == "aws:kms" and config.get("kms_key_id") == h.DEFAULT_KEY,
            f"GetBucketEncryption: HTTP {status} {config}, expected aws:kms with KMSMasterKeyID {h.DEFAULT_KEY}")
    c.roundtrip(bucket, "object", h.plain("bd-02", SIZE), None, None, h.projection_kms(h.DEFAULT_KEY), "no-header PUT")


@case("BD-03", "Explicit AES256 wins over a bucket default of aws:kms", ["e2"])
def bd_03(c: Ctx) -> None:
    bucket = c.bucket()
    c.ok(h.put_bucket_encryption(c.site, bucket, "aws:kms", h.KEY_1), "PutBucketEncryption")
    # The projection has no key id: the request did not inherit the bucket's KMS key.
    c.roundtrip(bucket, "object", h.plain("bd-03", SIZE), h.sse_s3(), None, h.projection_s3(), "explicit AES256")


@case("BD-04", "SSE-C is accepted under a bucket default and stays SSE-C", ["e1", "e2"])
def bd_04(c: Ctx) -> None:
    bucket = c.bucket()
    algorithm = "AES256" if c.config == "e1" else "aws:kms"
    c.ok(h.put_bucket_encryption(c.site, bucket, algorithm), "PutBucketEncryption")
    data = h.plain("bd-04", SIZE)
    c.roundtrip(bucket, "object", data, h.ssec(KEY_A), h.ssec(KEY_A), h.projection_c(KEY_A), "SSE-C")
    c.refused(h.get(c.site, bucket, "object"), 400, "InvalidRequest", "GET without the customer key")


@case("BD-06", "Removing the bucket default stops encrypting new objects and keeps old ones readable", ["e1"])
def bd_06(c: Ctx) -> None:
    bucket = c.bucket()
    before, after = h.plain("bd-06 before", SIZE), h.plain("bd-06 after", SIZE)
    c.ok(h.put_bucket_encryption(c.site, bucket, "AES256"), "PutBucketEncryption")
    c.roundtrip(bucket, "before", before, None, None, h.projection_s3(), "object written under the default")
    c.ok(h.delete_bucket_encryption(c.site, bucket), "DeleteBucketEncryption", want=204)
    c.roundtrip(bucket, "after", after, None, None, {}, "object written after the default was removed")
    c.read_back(bucket, "before", before, None, h.projection_s3(), "object written under the default, read afterwards")
