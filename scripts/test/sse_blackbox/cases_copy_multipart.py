"""CopyObject (CP) and multipart uploads (MP)."""

from __future__ import annotations

import harness as h
from framework import Ctx, case

KEY_A = h.derived_key("ssec a")
KEY_B = h.derived_key("ssec b")
SIZE = 70 * h.KIB
PART = 5 * h.MIB


@case("CP-01", "CopyObject across schemes: the destination scheme alone decides the result", ["e2"])
def cp_01(c: Ctx) -> None:
    bucket = c.bucket()
    # name: (source write headers, copy-source headers, destination headers, destination read headers, projection)
    combos = {
        "plain-to-kms": (None, {}, h.sse_kms(h.KEY_2), None, h.projection_kms(h.KEY_2)),
        "kms-to-ssec": (h.sse_kms(h.KEY_1), {}, h.ssec(KEY_B), h.ssec(KEY_B), h.projection_c(KEY_B)),
        "ssec-to-s3": (h.ssec(KEY_A), h.ssec(KEY_A, h.COPY_SSEC), h.sse_s3(), None, h.projection_s3()),
        "ssec-to-ssec": (h.ssec(KEY_A), h.ssec(KEY_A, h.COPY_SSEC), h.ssec(KEY_B), h.ssec(KEY_B), h.projection_c(KEY_B)),
        "s3-to-none": (h.sse_s3(), {}, {}, None, {}),
        "kms-to-kms": (h.sse_kms(h.KEY_1), {}, h.sse_kms(h.KEY_2), None, h.projection_kms(h.KEY_2)),
    }
    for name, (source_headers, copy_source, destination, read_headers, want) in combos.items():
        with c.row(name):
            data = h.plain(f"cp-01 {name}", SIZE)
            c.ok(h.put(c.site, bucket, f"{name}-src", data, source_headers), "source PUT")
            c.ok(h.copy(c.site, bucket, f"{name}-dst", bucket, f"{name}-src", {**copy_source, **destination}), "CopyObject")
            c.read_back(bucket, f"{name}-dst", data, read_headers, want, "destination")
            status, head_headers = h.head(c.site, bucket, f"{name}-dst", read_headers)
            if c.check(status == 200, f"destination HEAD: HTTP {status}, expected 200"):
                c.projection(head_headers, want, "destination HEAD")
            c.stored(bucket, f"{name}-dst")
            if name == "ssec-to-ssec":
                # The data was re-encrypted, not relabelled: the source key no longer opens the destination.
                c.refused(h.get(c.site, bucket, f"{name}-dst", h.ssec(KEY_A)), 400, "InvalidRequest",
                          "destination GET with the source key")


@case("CP-02", "Copying an SSE-C source requires its key", ["e0"])
def cp_02(c: Ctx) -> None:
    bucket = c.bucket()
    c.ok(h.put(c.site, bucket, "src", h.plain("cp-02", SIZE), h.ssec(KEY_A)), "source PUT")
    attempts = {"no-source-key": {}, "wrong-source-key": h.ssec(KEY_B, h.COPY_SSEC)}
    for name, copy_source in attempts.items():
        with c.row(name):
            status, _, body = h.copy(c.site, bucket, f"dst-{name}", bucket, "src", copy_source)
            code, message = h.s3_error(body)
            c.check(status == 400 and code != "", f"CopyObject: HTTP {status} {code!r} {message[:160]!r}, expected an explicit 400")
            c.absent(bucket, f"dst-{name}", "CopyObject")


def _upload(c: Ctx, bucket: str, key: str, parts: list[bytes], create_headers, part_headers, complete_headers):
    """A full multipart upload; returns (create headers, complete headers)."""
    status, create_response, upload_id, body = h.create_multipart(c.site, bucket, key, create_headers)
    c.need(status == 200 and upload_id, f"CreateMultipartUpload: HTTP {status} {h.s3_error(body)}")
    etags = []
    for number, data in enumerate(parts, start=1):
        part_response, _ = c.ok(h.upload_part(c.site, bucket, key, upload_id, number, data, part_headers), f"UploadPart {number}")
        etags.append((number, part_response["etag"]))
    complete_response, _ = c.ok(h.complete_multipart(c.site, bucket, key, upload_id, etags, complete_headers),
                                "CompleteMultipartUpload")
    return create_response, complete_response


@case("MP-01", "SSE-KMS multipart upload of three parts", ["e2"])
def mp_01(c: Ctx) -> None:
    bucket = c.bucket()
    parts = [h.plain("mp-01 part 1", PART), h.plain("mp-01 part 2", PART), h.plain("mp-01 part 3", h.MIB + 17)]
    data = b"".join(parts)
    want = h.projection_kms(h.KEY_1)
    create_response, complete_response = _upload(c, bucket, "object", parts, h.sse_kms(h.KEY_1), None, None)
    c.projection(create_response, want, "CreateMultipartUpload")
    c.projection(complete_response, want, "CompleteMultipartUpload")
    c.read_back(bucket, "object", data, None, want, "completed object")
    status, head_headers = h.head(c.site, bucket, "object")
    if c.check(status == 200, f"HEAD: HTTP {status}, expected 200"):
        c.check(head_headers.get("content-length") == str(len(data)),
                f"HEAD: Content-Length {head_headers.get('content-length')}, expected {len(data)}")
        etag = head_headers.get("etag", "").strip('"')
        c.check(len(etag) == 34 and etag.endswith("-3"), f"HEAD: ETag {etag!r}, expected 32 hex digits followed by -3")
    c.stored(bucket, "object")


@case("MP-03", "SSE-C multipart upload", ["e0"])
def mp_03(c: Ctx) -> None:
    bucket = c.bucket()
    parts = [h.plain("mp-03 part 1", PART), h.plain("mp-03 part 2", h.MIB + 17)]
    data = b"".join(parts)
    triple, want = h.ssec(KEY_A), h.projection_c(KEY_A)
    create_response, complete_response = _upload(c, bucket, "object", parts, triple, triple, triple)
    for name, response in (("CreateMultipartUpload", create_response), ("CompleteMultipartUpload", complete_response)):
        got = h.projection(response)
        c.check(want.items() <= got.items(), f"{name}: SSE headers {got} lack the SSE-C projection {want}")
        # Both multipart responses also carry `x-amz-server-side-encryption: AES256`, which belongs to SSE-S3.
        c.known_deviation(got == want, f"{name} answers an SSE-C upload with exactly {sorted(want)} (today: {sorted(got)})")
    c.read_back(bucket, "object", data, triple, want, "completed object")
    c.refused(h.get(c.site, bucket, "object"), 400, "InvalidRequest", "GET without the customer key")
    c.stored(bucket, "object")


@case("MP-06", "A part whose SSE-C key does not match its upload is refused and does not poison the upload", ["e0"])
def mp_06(c: Ctx) -> None:
    bucket = c.bucket()
    triple = h.ssec(KEY_A)
    part_1, part_2 = h.plain("mp-06 part 1", PART), h.plain("mp-06 part 2", h.KIB)

    status, _, upload_id, body = h.create_multipart(c.site, bucket, "ssec", triple)
    c.need(status == 200 and upload_id, f"CreateMultipartUpload (SSE-C): HTTP {status} {h.s3_error(body)}")
    with c.row("part-without-key"):
        c.refused(h.upload_part(c.site, bucket, "ssec", upload_id, 1, part_1), 400, "InvalidRequest", "UploadPart")
    with c.row("part-with-another-key"):
        c.refused(h.upload_part(c.site, bucket, "ssec", upload_id, 1, part_1, h.ssec(KEY_B)), 400, "InvalidRequest", "UploadPart")

    status, _, plain_upload, body = h.create_multipart(c.site, bucket, "plain")
    c.need(status == 200 and plain_upload, f"CreateMultipartUpload (plaintext): HTTP {status} {h.s3_error(body)}")
    with c.row("key-on-plaintext-upload"):
        c.refused(h.upload_part(c.site, bucket, "plain", plain_upload, 1, part_1, triple), 400, "InvalidRequest", "UploadPart")

    with c.row("upload-still-usable"):
        etags = []
        for number, data in ((1, part_1), (2, part_2)):
            response, _ = c.ok(h.upload_part(c.site, bucket, "ssec", upload_id, number, data, triple), f"UploadPart {number}")
            etags.append((number, response["etag"]))
        c.ok(h.complete_multipart(c.site, bucket, "ssec", upload_id, etags, triple), "CompleteMultipartUpload")
        c.read_back(bucket, "ssec", part_1 + part_2, triple, h.projection_c(KEY_A), "completed object")
