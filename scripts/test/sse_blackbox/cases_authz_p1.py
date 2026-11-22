"""Per-key authorization beyond the core matrix (AZ): which key a request resolves to, and every way to reach it.

The three identities are those of the core matrix: U1 may use KEY_1 only, U2 has full S3 access and no KMS grant,
U3 may use every key except KEY_1. A refusal is always 403 `AccessDenied`, and next to every refusal the same
request by a caller who holds the grant succeeds, so a refusal cannot be mistaken for an entry point that is broken.
"""

from __future__ import annotations

import json
import time
import urllib.parse
import xml.etree.ElementTree as ET

import audit_sink
import framework as fw
import harness as h
import observe
import scan
from cases_negative_authz import _users
from framework import Ctx, case

SIZE = 70 * h.KIB
PART = 5 * h.MIB
KEY_A = h.derived_key("ssec a")
ANONYMOUS_DENIAL_EVENT = "sse_kms_anonymous_key_authorization_denied"
AUDIT_CONFIG = "e2e-audit"
AUDIT_ENDPOINT_ENV = "RUSTFS_AUDIT_WEBHOOK_ENDPOINT_BLACKBOX"
# The scanner runs every second and a lifecycle day lasts two, so a rule in days is due within seconds.
FAST_LIFECYCLE_ENV = {"RUSTFS_SCANNER_CYCLE": "1", "RUSTFS_ILM_PROCESS_TIME": "1", "RUSTFS_ILM_DEBUG_DAY_SECS": "2"}
EXPIRY_DEADLINE = 90.0
# The fields an audit entry carries about the KMS work of its request.
KMS_AUDIT_TAGS = ("sseType", "kmsKeyId", "kmsOutcome", "kmsErrorClass")
CSV = b"id,name\n1,alpha\n2,beta\n3,gamma\n"
SELECT_ALL = (
    b'<SelectObjectContentRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
    b"<Expression>select * from s3object</Expression><ExpressionType>SQL</ExpressionType>"
    b"<InputSerialization><CSV><FileHeaderInfo>NONE</FileHeaderInfo></CSV></InputSerialization>"
    b"<OutputSerialization><CSV/></OutputSerialization></SelectObjectContentRequest>"
)


# ---------------------------------------------------------------------------
# Requests the core library does not have
# ---------------------------------------------------------------------------

def anonymous(s: h.Site, method: str, path: str, query: str = "", body: bytes | None = None,
              headers: dict[str, str] | None = None, label: str | None = None) -> tuple[int, dict[str, str], bytes]:
    """A request without credentials, recorded and archived like a signed one."""
    request_headers = {k.lower(): v for k, v in (headers or {}).items()}
    url = f"{s.endpoint}{urllib.parse.quote(path, safe='/-._~')}" + (f"?{query}" if query else "")
    status, response_headers, response_body = h._http(url, method, body, request_headers, 120.0)
    observe.exchange(label, method, path, query, status, response_headers, response_body)
    for hook in h.EXCHANGE_HOOKS:
        hook({"site": s.name, "label": label or method, "method": method, "path": path, "query": query,
              "request_headers": request_headers, "request_body": body, "status": status,
              "response_headers": response_headers, "response_body": response_body})
    if status == 0 and observe.in_case():
        raise h.EnvError(f"{method} {path}: no HTTP response ({response_body[:160].decode(errors='replace')})")
    return status, response_headers, response_body


def select_object(s: h.Site, bucket: str, key: str, headers=None, creds=None):
    """SelectObjectContent returning every record of a CSV object."""
    return h.signed_request(s, "POST", f"/{bucket}/{key}", query="select=&select-type=2", body=SELECT_ALL,
                            headers={"content-type": "application/xml", **(headers or {})}, creds=creds,
                            label="SelectObjectContent")


def object_attributes(s: h.Site, bucket: str, key: str, headers=None, creds=None):
    return h.signed_request(s, "GET", f"/{bucket}/{key}", query="attributes=",
                            headers={"x-amz-object-attributes": "ETag,ObjectSize,StorageClass", **(headers or {})},
                            creds=creds, label="GetObjectAttributes")


def upload_part_copy(s: h.Site, bucket: str, key: str, upload_id: str, part_no: int, src_bucket: str, src_key: str,
                     headers=None, creds=None):
    query = f"partNumber={part_no}&uploadId={urllib.parse.quote(upload_id)}"
    return h.signed_request(s, "PUT", f"/{bucket}/{key}", query=query,
                            headers={"x-amz-copy-source": f"/{src_bucket}/{urllib.parse.quote(src_key)}", **(headers or {})},
                            creds=creds, label="UploadPartCopy")


def abort_multipart(s: h.Site, bucket: str, key: str, upload_id: str, creds=None):
    return h.signed_request(s, "DELETE", f"/{bucket}/{key}", query=f"uploadId={urllib.parse.quote(upload_id)}",
                            creds=creds, label="AbortMultipartUpload")


def allow_anonymous(s: h.Site, bucket: str):
    """A bucket policy that lets a caller without credentials read and write objects."""
    policy = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": ["*"]}, "Action": ["s3:GetObject", "s3:PutObject"],
        "Resource": [f"arn:aws:s3:::{bucket}/*"]}]}
    return h.signed_request(s, "PUT", f"/{bucket}", query="policy=", body=json.dumps(policy).encode(),
                            headers={"content-type": "application/json"}, label="PutBucketPolicy")


def listed_parts(c: Ctx, bucket: str, key: str, upload_id: str) -> list[int]:
    _, body = c.ok(h.list_parts(c.site, bucket, key, upload_id), "ListParts")
    return [int(part.findtext("{*}PartNumber")) for part in ET.fromstring(body).findall("{*}Part")]


def denied(c: Ctx, response, what: str, data: bytes | None = None) -> None:
    """The per-key refusal: 403 `AccessDenied`, and not one byte of the object."""
    c.refused(response, 403, "AccessDenied", what, message_has="Access Denied")
    if data is not None:
        c.check(data[:64] not in response[2], f"{what}: the refused request returned object bytes")


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

@case("AZ-04", "Authorization follows the key a request resolves to, not the key its headers name", ["e2e"], level="p1")
def az_04(c: Ctx) -> None:
    bucket = c.bucket()
    users = _users(c)
    c.ok(h.put_bucket_encryption(c.site, bucket, "aws:kms", h.KEY_2), "PutBucketEncryption")
    want_default, want_explicit = h.projection_kms(h.KEY_2), h.projection_kms(h.KEY_1)
    admin_data = h.plain("az-04 admin", SIZE)
    put_headers, _ = c.ok(h.put(c.site, bucket, "admin-default", admin_data), "admin PUT without SSE headers")
    # Without this the refusals below could come from a default that never took effect.
    c.projection(put_headers, want_default, "admin PUT without SSE headers")

    with c.row("u1-default"):
        # U1 names no key; the bucket default resolves to KEY_2, which U1 may not use.
        denied(c, h.put(c.site, bucket, "u1-default", h.plain("az-04 u1 default", SIZE), creds=users["u1"]),
               "PUT without SSE headers")
        c.absent(bucket, "u1-default", "PUT without SSE headers")
        denied(c, h.get(c.site, bucket, "admin-default", creds=users["u1"]), "GET of the object under the default key",
               admin_data)
    with c.row("u1-explicit"):
        data = h.plain("az-04 u1 explicit", SIZE)
        put_headers, _ = c.ok(h.put(c.site, bucket, "u1-explicit", data, h.sse_kms(h.KEY_1), creds=users["u1"]),
                              "PUT naming KEY_1")
        c.projection(put_headers, want_explicit, "PUT naming KEY_1")
        get_headers, body = c.ok(h.get(c.site, bucket, "u1-explicit", creds=users["u1"]), "GET")
        c.projection(get_headers, want_explicit, "GET")
        c.bytes_equal(get_headers, body, data, "GET")
    with c.row("u3-default"):
        # The mirror image: U3 holds the grant on the resolved key and none on the one U1 had to name.
        data = h.plain("az-04 u3 default", SIZE)
        put_headers, _ = c.ok(h.put(c.site, bucket, "u3-default", data, creds=users["u3"]), "PUT without SSE headers")
        c.projection(put_headers, want_default, "PUT without SSE headers")
        get_headers, body = c.ok(h.get(c.site, bucket, "u3-default", creds=users["u3"]), "GET")
        c.bytes_equal(get_headers, body, data, "GET")
    with c.row("u3-explicit"):
        denied(c, h.put(c.site, bucket, "u3-explicit", h.plain("az-04 u3 explicit", SIZE), h.sse_kms(h.KEY_1),
                        creds=users["u3"]), "PUT naming KEY_1")
        c.absent(bucket, "u3-explicit", "PUT naming KEY_1")


@case("AZ-05", "CopyObject needs a grant on the source key and on the destination key", ["e2e"], level="p1")
def az_05(c: Ctx) -> None:
    bucket = c.bucket()
    creds = _users(c)["u1"]
    sources = {"k1": h.sse_kms(h.KEY_1), "k2": h.sse_kms(h.KEY_2), "plain": None}
    data = {name: h.plain(f"az-05 {name}", SIZE) for name in sources}
    for name, headers in sources.items():
        c.ok(h.put(c.site, bucket, f"src-{name}", data[name], headers), f"admin PUT src-{name}")
    # name: (source, destination headers, allowed, destination projection)
    rows = {
        "k2-to-k1": ("k2", h.sse_kms(h.KEY_1), False, None),
        "k1-to-k2": ("k1", h.sse_kms(h.KEY_2), False, None),
        "k1-to-k1": ("k1", h.sse_kms(h.KEY_1), True, h.projection_kms(h.KEY_1)),
        # One end needs no key at all, so the refusal can only come from the other end.
        "k2-to-plain": ("k2", {}, False, None),
        "plain-to-k2": ("plain", h.sse_kms(h.KEY_2), False, None),
        "plain-to-k1": ("plain", h.sse_kms(h.KEY_1), True, h.projection_kms(h.KEY_1)),
        "k1-to-plain": ("k1", {}, True, {}),
    }
    for name, (source, destination, allowed, want) in rows.items():
        with c.row(name):
            response = h.copy(c.site, bucket, f"dst-{name}", bucket, f"src-{source}", destination, creds=creds)
            if allowed:
                copy_headers, _ = c.ok(response, "CopyObject")
                c.projection(copy_headers, want, "CopyObject")
                get_headers, body = c.ok(h.get(c.site, bucket, f"dst-{name}", creds=creds), "destination GET")
                c.projection(get_headers, want, "destination GET")
                c.bytes_equal(get_headers, body, data[source], "destination GET")
            else:
                denied(c, response, "CopyObject", data[source])
                c.absent(bucket, f"dst-{name}", "CopyObject")


@case("AZ-06", "An upload id does not pass its creator's KMS grant to another identity", ["e2e"], level="p1")
def az_06(c: Ctx) -> None:
    bucket = c.bucket()
    users = _users(c)
    owner, other = users["u1"], users["u2"]
    parts = [h.plain("az-06 part 1", PART), h.plain("az-06 part 2", h.MIB + 17)]
    want = h.projection_kms(h.KEY_1)
    status, create_headers, upload_id, body = h.create_multipart(c.site, bucket, "object", h.sse_kms(h.KEY_1), creds=owner)
    c.need(status == 200 and upload_id, f"CreateMultipartUpload by the owner: HTTP {status} {h.s3_error(body)}")
    c.projection(create_headers, want, "CreateMultipartUpload")
    first, _ = c.ok(h.upload_part(c.site, bucket, "object", upload_id, 1, parts[0], creds=owner), "UploadPart 1 by the owner")

    # U2 may write to the bucket and knows the upload id; neither gives it KEY_1.
    with c.row("other-upload-part"):
        denied(c, h.upload_part(c.site, bucket, "object", upload_id, 2, parts[1], creds=other),
               "UploadPart by another identity")
        c.check(listed_parts(c, bucket, "object", upload_id) == [1],
                "the refused UploadPart left a part behind: the upload does not list exactly part 1")
    with c.row("other-complete"):
        denied(c, h.complete_multipart(c.site, bucket, "object", upload_id, [(1, first["etag"])], creds=other),
               "CompleteMultipartUpload by another identity")
        c.absent(bucket, "object", "CompleteMultipartUpload by another identity")
        c.check(listed_parts(c, bucket, "object", upload_id) == [1],
                "the refused CompleteMultipartUpload changed the upload: it does not list exactly part 1")
    with c.row("owner-completes"):
        second, _ = c.ok(h.upload_part(c.site, bucket, "object", upload_id, 2, parts[1], creds=owner),
                         "UploadPart 2 by the owner")
        complete_headers, _ = c.ok(h.complete_multipart(c.site, bucket, "object", upload_id,
                                                        [(1, first["etag"]), (2, second["etag"])], creds=owner),
                                   "CompleteMultipartUpload by the owner")
        c.projection(complete_headers, want, "CompleteMultipartUpload")
        get_headers, body = c.ok(h.get(c.site, bucket, "object", creds=owner), "GET by the owner")
        c.projection(get_headers, want, "GET by the owner")
        c.bytes_equal(get_headers, body, b"".join(parts), "GET by the owner")


@case("AZ-07", "UploadPartCopy needs a grant on the key of its source", ["e2e"], level="p1")
def az_07(c: Ctx) -> None:
    bucket = c.bucket()
    creds = _users(c)["u1"]
    data = {key: h.plain(f"az-07 {key}", SIZE) for key in (h.KEY_1, h.KEY_2)}
    for key, content in data.items():
        c.ok(h.put(c.site, bucket, f"src-{key}", content, h.sse_kms(key)), f"admin PUT under {key}")
    want = h.projection_kms(h.KEY_1)
    status, _, upload_id, body = h.create_multipart(c.site, bucket, "object", h.sse_kms(h.KEY_1), creds=creds)
    c.need(status == 200 and upload_id, f"CreateMultipartUpload: HTTP {status} {h.s3_error(body)}")

    with c.row("source-without-grant"):
        denied(c, upload_part_copy(c.site, bucket, "object", upload_id, 1, bucket, f"src-{h.KEY_2}", creds=creds),
               "UploadPartCopy from an object under KEY_2", data[h.KEY_2])
        c.check(listed_parts(c, bucket, "object", upload_id) == [],
                "the refused UploadPartCopy left a part behind: the upload lists parts")
    with c.row("source-with-grant"):
        _, body = c.ok(upload_part_copy(c.site, bucket, "object", upload_id, 1, bucket, f"src-{h.KEY_1}", creds=creds),
                       "UploadPartCopy from an object under KEY_1")
        etag = ET.fromstring(body).findtext("{*}ETag") or ""
        c.need(etag != "", "UploadPartCopy answered 200 without an ETag")
        complete_headers, _ = c.ok(h.complete_multipart(c.site, bucket, "object", upload_id, [(1, etag)], creds=creds),
                                   "CompleteMultipartUpload")
        c.projection(complete_headers, want, "CompleteMultipartUpload")
        get_headers, body = c.ok(h.get(c.site, bucket, "object", creds=creds), "GET")
        c.bytes_equal(get_headers, body, data[h.KEY_1], "GET")


def _read_entry_points(c: Ctx, bucket: str, etag: str, creds) -> dict[str, tuple]:
    """Every way to read the object or learn about it, as one identity."""
    status, head_headers = h.head(c.site, bucket, "object", creds=creds)
    return {
        "get": h.get(c.site, bucket, "object", creds=creds),
        "range-get": h.get(c.site, bucket, "object", creds=creds, range_="bytes=0-99"),
        # Preconditions that fail: the answer needs no body, and the key must still be authorized first.
        "get-if-match-other": h.get(c.site, bucket, "object", {"if-match": '"00000000000000000000000000000000"'},
                                    creds=creds, label="GetObject(if-match)"),
        "get-if-none-match-same": h.get(c.site, bucket, "object", {"if-none-match": etag}, creds=creds,
                                        label="GetObject(if-none-match)"),
        "select": select_object(c.site, bucket, "records.csv", creds=creds),
        "head": (status, head_headers, b""),
        "attributes": object_attributes(c.site, bucket, "object", creds=creds),
    }


@case("AZ-08", "Which read entry points require the grant on the object's key", ["e2e"], level="p1")
def az_08(c: Ctx) -> None:
    bucket = c.bucket()
    users = _users(c)
    data = h.plain("az-08", SIZE)
    want = h.projection_kms(h.KEY_1)
    put_headers, _ = c.ok(h.put(c.site, bucket, "object", data, h.sse_kms(h.KEY_1)), "admin PUT")
    c.ok(h.put(c.site, bucket, "records.csv", CSV, h.sse_kms(h.KEY_1)), "admin PUT of the CSV object")
    etag = put_headers.get("etag", "")
    c.need(etag != "", "PUT answered without an ETag")

    # U1 holds the grant: every entry point works, so a refusal below is about the grant and nothing else.
    granted = _read_entry_points(c, bucket, etag, users["u1"])
    with c.row("granted"):
        get_headers, body = c.ok(granted["get"], "GET")
        c.bytes_equal(get_headers, body, data, "GET")
        range_headers, body = c.ok(granted["range-get"], "Range GET", want=206)
        c.check(body == data[:100], "Range GET: the body is not the first 100 bytes of the object")
        c.check(range_headers.get("content-range") == f"bytes 0-99/{len(data)}",
                f"Range GET: Content-Range {range_headers.get('content-range')!r}, expected 'bytes 0-99/{len(data)}'")
        c.check(granted["get-if-match-other"][0] == 412,
                f"GET with a failing If-Match: HTTP {granted['get-if-match-other'][0]}, expected 412")
        c.check(granted["get-if-none-match-same"][0] == 304,
                f"GET with a failing If-None-Match: HTTP {granted['get-if-none-match-same'][0]}, expected 304")
        _, body = c.ok(granted["select"], "SelectObjectContent")
        c.check(CSV in body, "SelectObjectContent: the result does not carry the records")
        head_headers, _ = c.ok(granted["head"], "HEAD")
        c.projection(head_headers, want, "HEAD")
        _, body = c.ok(granted["attributes"], "GetObjectAttributes")
        c.check(ET.fromstring(body).findtext("{*}ObjectSize") == str(len(data)),
                "GetObjectAttributes: ObjectSize is not the object's size")

    refused = _read_entry_points(c, bucket, etag, users["u2"])
    for name in ("get", "range-get", "get-if-match-other", "get-if-none-match-same", "select"):
        with c.row(f"without-grant-{name}"):
            denied(c, refused[name], name, CSV if name == "select" else data)
    # HEAD and GetObjectAttributes read no object bytes and are not subject to the per-key grant today. HEAD also
    # names the key. Pinned as it is, so that a change in either direction is a decision and not an accident.
    with c.row("without-grant-head"):
        head_headers, _ = c.ok(refused["head"], "HEAD")
        c.projection(head_headers, want, "HEAD")
        c.check(head_headers.get("content-length") == str(len(data)),
                f"HEAD: Content-Length {head_headers.get('content-length')}, expected {len(data)}")
    with c.row("without-grant-attributes"):
        _, body = c.ok(refused["attributes"], "GetObjectAttributes")
        c.check(ET.fromstring(body).findtext("{*}ObjectSize") == str(len(data)),
                "GetObjectAttributes: ObjectSize is not the object's size")
        c.check(data[:64] not in body, "GetObjectAttributes returned object bytes")


def _anonymous_denial_events(c: Ctx, at_least: int, timeout: float) -> int:
    """How often the process logged that it refuses anonymous callers; waits for the log to be written."""
    deadline = time.monotonic() + timeout
    while True:
        count = len(h.log_events(c.site, ANONYMOUS_DENIAL_EVENT))
        if count >= at_least or time.monotonic() > deadline:
            observe.note("log-event", ANONYMOUS_DENIAL_EVENT, count)
            return count
        time.sleep(0.25)


@case("AZ-09", "Anonymous access to SSE-KMS objects in a public bucket", ["e2", "e2e"], level="p1")
def az_09(c: Ctx) -> None:
    bucket = c.bucket()
    enforced = c.config != "e2"
    kms_data, plain_data, new_data = h.plain("az-09 kms", SIZE), h.plain("az-09 plain", SIZE), h.plain("az-09 new", SIZE)
    want = h.projection_kms(h.KEY_1)
    c.ok(h.put(c.site, bucket, "kms", kms_data, h.sse_kms(h.KEY_1)), "admin PUT of the SSE-KMS object")
    c.ok(h.put(c.site, bucket, "plain", plain_data), "admin PUT of the plaintext object")
    c.ok(allow_anonymous(c.site, bucket), "PutBucketPolicy", want=204)

    # The policy does let a caller without credentials in: whatever is refused below is refused for the key.
    with c.row("policy-admits-anonymous"):
        get_headers, body = c.ok(anonymous(c.site, "GET", f"/{bucket}/plain", label="GetObject(anonymous)"),
                                 "anonymous GET of the plaintext object")
        c.bytes_equal(get_headers, body, plain_data, "anonymous GET of the plaintext object")
        c.ok(anonymous(c.site, "PUT", f"/{bucket}/anonymous-plain", body=new_data, label="PutObject(anonymous)"),
             "anonymous PUT without SSE headers")
    read = anonymous(c.site, "GET", f"/{bucket}/kms", label="GetObject(anonymous)")
    written = anonymous(c.site, "PUT", f"/{bucket}/anonymous-kms", body=new_data, headers=h.sse_kms(h.KEY_1),
                        label="PutObject(anonymous)")
    read_again = anonymous(c.site, "GET", f"/{bucket}/kms", label="GetObject(anonymous)")
    if enforced:
        with c.row("anonymous-get"):
            denied(c, read, "anonymous GET", kms_data)
            denied(c, read_again, "second anonymous GET", kms_data)
        with c.row("anonymous-put"):
            denied(c, written, "anonymous PUT")
            c.absent(bucket, "anonymous-kms", "anonymous PUT")
        with c.row("one-warning-per-process"), observe.quiet():
            # Unauthenticated traffic must not be able to flood the log: three refusals, one warning.
            count = _anonymous_denial_events(c, at_least=1, timeout=10.0)
            c.check(count == 1,
                    f"{count} {ANONYMOUS_DENIAL_EVENT} events in the log after three refused requests, expected exactly 1")
    else:
        with c.row("anonymous-get"):
            get_headers, body = c.ok(read, "anonymous GET")
            c.projection(get_headers, want, "anonymous GET")
            c.bytes_equal(get_headers, body, kms_data, "anonymous GET")
            c.ok(read_again, "second anonymous GET")
        with c.row("anonymous-put"):
            put_headers, _ = c.ok(written, "anonymous PUT")
            c.projection(put_headers, want, "anonymous PUT")
            c.read_back(bucket, "anonymous-kms", new_data, None, want, "object written anonymously")
        with c.row("no-warning"), observe.quiet():
            count = _anonymous_denial_events(c, at_least=1, timeout=3.0)
            c.check(count == 0,
                    f"{count} {ANONYMOUS_DENIAL_EVENT} events in the log of a process that does not enforce, expected 0")


@case("AZ-10", "Without enforcement, the default, no caller needs a KMS grant", ["e2"], level="p1")
def az_10(c: Ctx) -> None:
    # The requests of the enforced matrix, one for one; here every one of them succeeds. An upgrade must not turn
    # per-key authorization on by itself.
    bucket = c.bucket()
    users = _users(c)
    stored = {key: h.plain(f"az-01 admin {key}", SIZE) for key in (h.KEY_1, h.KEY_2)}
    for key, data in stored.items():
        c.ok(h.put(c.site, bucket, f"admin-{key}", data, h.sse_kms(key)), f"admin PUT under {key}")
    for name in ("u1", "u2", "u3"):
        for key in (h.KEY_1, h.KEY_2):
            with c.row(f"{name}-{key}"):
                data = h.plain(f"az-01 {name} {key}", SIZE)
                written = h.put(c.site, bucket, f"{name}-{key}", data, h.sse_kms(key), creds=users[name])
                read = h.get(c.site, bucket, f"admin-{key}", creds=users[name])
                put_headers, _ = c.ok(written, "PUT")
                c.projection(put_headers, h.projection_kms(key), "PUT")
                get_headers, body = c.ok(read, "GET")
                c.bytes_equal(get_headers, body, stored[key], "GET")
                c.read_back(bucket, f"{name}-{key}", data, None, h.projection_kms(key), "object written by the identity")


def _expire_rule(prefix: str) -> bytes:
    return ('<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>expire</ID>'
            f"<Status>Enabled</Status><Filter><Prefix>{prefix}</Prefix></Filter><Expiration><Days>1</Days></Expiration>"
            "</Rule></LifecycleConfiguration>").encode()


@case("AZ-11", "Enforcement does not stop the lifecycle scanner from expiring SSE-KMS objects", ["e2e"], level="p1",
      isolated=True)
def az_11(c: Ctx) -> None:
    # A day lasts two seconds and the scanner runs every second, so a one-day rule is due right after the write.
    c.restart(env={**fw.CONFIGS["e2e"](c.site), **FAST_LIFECYCLE_ENV})
    h.wait_kms_healthy(c.site)
    bucket = c.bucket()
    creds = _users(c)["u1"]
    want = h.projection_kms(h.KEY_1)
    c.ok(h.put_bucket_encryption(c.site, bucket, "aws:kms", h.KEY_1), "PutBucketEncryption")
    data = {key: h.plain(f"az-11 {key}", SIZE) for key in ("expire/object", "keep/object")}
    for key, content in data.items():
        put_headers, _ = c.ok(h.put(c.site, bucket, key, content, creds=creds), f"PUT {key}")
        c.projection(put_headers, want, f"PUT {key}")
    c.ok(h.signed_request(c.site, "PUT", f"/{bucket}", query="lifecycle=", body=_expire_rule("expire/"),
                          headers={"content-type": "application/xml"}, label="PutBucketLifecycleConfiguration"),
         "PutBucketLifecycleConfiguration")

    # The scanner acts for no caller, so it holds no grant; it must not be refused like one who holds none.
    deadline = time.monotonic() + EXPIRY_DEADLINE
    with observe.quiet():
        while h.head(c.site, bucket, "expire/object")[0] != 404 and time.monotonic() < deadline:
            time.sleep(1.0)
    status, _ = h.head(c.site, bucket, "expire/object", label="HeadObject(expired)")
    c.check(status == 404,
            f"the object under the rule is still there {EXPIRY_DEADLINE:.0f}s after it was due: HEAD is {status}, expected 404")
    # Same bucket, same key, outside the rule: the scanner removed what the rule names and nothing else.
    get_headers, body = c.ok(h.get(c.site, bucket, "keep/object", creds=creds), "GET of the object outside the rule")
    c.projection(get_headers, want, "GET of the object outside the rule")
    c.bytes_equal(get_headers, body, data["keep/object"], "GET of the object outside the rule")


# ---------------------------------------------------------------------------
# Audit: a process configuration that delivers audit entries to a receiver in the driver
# ---------------------------------------------------------------------------

def _audit_env(site: h.Site) -> dict[str, str]:
    sink = audit_sink.SINK
    return {
        **fw.CONFIGS["e2e"](site),
        "RUSTFS_AUDIT_ENABLE": "true",
        "RUSTFS_AUDIT_WEBHOOK_ENABLE_BLACKBOX": "on",
        AUDIT_ENDPOINT_ENV: sink.endpoint,
        "RUSTFS_AUDIT_WEBHOOK_QUEUE_DIR_BLACKBOX": str(site.root / "audit-queue"),
        # The product refuses loopback webhook targets unless the origin is listed.
        "RUSTFS_OUTBOUND_ALLOW_ORIGINS": sink.origin,
    }


def _audit_prepare(site: h.Site) -> None:
    fw.CONFIG_PREPARE["e2e"](site)
    (site.root / "audit-queue").mkdir(parents=True, exist_ok=True)
    audit_sink.SINK.start()


def _audit_teardown() -> None:
    sink = audit_sink.SINK
    sink.stop()
    if h.RUN_DIR is not None and sink.payloads:
        sink.write(h.RUN_DIR / "archive" / "audit-payloads.jsonl")


def _release_receiver(_site: h.Site, env: dict[str, str]) -> None:
    """Free the receiver's port as soon as a site starts that does not deliver to it; what it received is kept."""
    if AUDIT_ENDPOINT_ENV not in env:
        audit_sink.SINK.stop()


fw.register_config(AUDIT_CONFIG, _audit_env, prepare=_audit_prepare, provision=fw.CONFIG_PROVISION["e2e"],
                   teardown=_audit_teardown)
if _release_receiver not in h.SITE_START_HOOKS:
    h.SITE_START_HOOKS.append(_release_receiver)


def audit_entry(c: Ctx, response, what: str) -> dict | None:
    """The audit entry of the request that got `response`, once delivered. Waiting is timing, so it is not recorded."""
    # S3 responses name their request in x-amz-request-id, admin responses in x-request-id.
    request_id = response[1].get("x-amz-request-id") or response[1].get("x-request-id", "")
    c.need(request_id != "", f"{what}: the response carries no request id to find its audit entry by")
    with observe.quiet():
        entry = audit_sink.SINK.wait_for(request_id)
    c.check(entry is not None, f"{what}: no audit entry was delivered within {audit_sink.DELIVERY_TIMEOUT:.0f}s")
    return entry


def kms_tags(entry: dict) -> dict:
    tags = entry.get("tags") if isinstance(entry.get("tags"), dict) else {}
    return {name: tags[name] for name in KMS_AUDIT_TAGS if name in tags}


@case("AZ-13", "Audit entries state what a request did with KMS, and nothing about requests that did not use it",
      [AUDIT_CONFIG], level="p1")
def az_13(c: Ctx) -> None:
    bucket = c.bucket()
    users = _users(c)
    data = h.plain("az-13", SIZE)
    c.ok(h.put(c.site, bucket, "admin-kms", data, h.sse_kms(h.KEY_1)), "admin PUT under KEY_1")
    c.ok(h.put(c.site, bucket, "ssec", data, h.ssec(KEY_A)), "SSE-C PUT")
    c.ok(h.put(c.site, bucket, "plain", data), "plaintext PUT")
    success = {"sseType": "SSE-KMS", "kmsKeyId": h.KEY_1, "kmsOutcome": "success"}
    failure = {"sseType": "SSE-KMS", "kmsKeyId": h.KEY_1, "kmsOutcome": "failure", "kmsErrorClass": "access_denied"}
    # name: (response, expected HTTP status, expected KMS fields)
    requests = {
        "kms-put-allowed": (h.put(c.site, bucket, "u1-kms", data, h.sse_kms(h.KEY_1), creds=users["u1"]), 200, success),
        "kms-get-allowed": (h.get(c.site, bucket, "admin-kms", creds=users["u1"]), 200, success),
        "kms-put-denied": (h.put(c.site, bucket, "u2-kms", data, h.sse_kms(h.KEY_1), creds=users["u2"]), 403, failure),
        "kms-get-denied": (h.get(c.site, bucket, "admin-kms", creds=users["u2"]), 403, failure),
        "ssec-get": (h.get(c.site, bucket, "ssec", h.ssec(KEY_A)), 200, {}),
        "plain-get": (h.get(c.site, bucket, "plain"), 200, {}),
    }
    for name, (response, status, want) in requests.items():
        with c.row(name):
            c.need(response[0] == status, f"request: HTTP {response[0]} {h.s3_error(response[2])}, expected {status}")
            entry = audit_entry(c, response, name)
            if entry is None:
                continue
            api = entry.get("api") if isinstance(entry.get("api"), dict) else {}
            got = kms_tags(entry)
            observe.note("audit-entry", name, {"api": api.get("name"), "status_code": api.get("status_code"), "kms": got})
            # An entry that calls a refused request a success is worse than no entry.
            c.check(api.get("status_code") == status, f"audit entry: status_code {api.get('status_code')!r}, expected {status}")
            c.check(got == want, f"audit entry: KMS fields {got}, expected exactly {want}")
            leaked = scan.REGISTRY.scan(json.dumps(entry).encode())
            c.check(not leaked, "audit entry carries key material: " + "; ".join(hit.describe() for hit in leaked))
