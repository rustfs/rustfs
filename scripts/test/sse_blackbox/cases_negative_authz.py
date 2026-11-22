"""Malformed requests (NG), what SSE-S3 must not reveal (SEC-04), and per-key authorization (AZ)."""

from __future__ import annotations

import base64
import json

import harness as h
import observe
from framework import Ctx, case

KEY_A = h.derived_key("ssec a")
KEY_B = h.derived_key("ssec b")
SIZE = 70 * h.KIB


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


@case("NG-01", "PutObject refuses malformed combinations of SSE headers", ["e2"])
def ng_01(c: Ctx) -> None:
    bucket = c.bucket()
    data = h.plain("ng-01", h.KIB)
    rows = {
        "algorithm-aes128": {h.SSE: "AES128"},
        "algorithm-lowercase": {h.SSE: "aes256"},
        "ssec-with-aes256": {**h.ssec(KEY_A), h.SSE: "AES256"},
        "ssec-with-key-id": {**h.ssec(KEY_A), h.SSE_KEY_ID: h.KEY_1},
        "key-id-without-algorithm": {h.SSE_KEY_ID: h.KEY_1},
        "aes256-with-key-id": {h.SSE: "AES256", h.SSE_KEY_ID: h.KEY_1},
        "context-without-algorithm": {h.SSE_CONTEXT: _b64('{"dept":"a"}')},
        "context-not-base64": {h.SSE: "aws:kms", h.SSE_CONTEXT: "!!! not base64 !!!"},
        "context-json-array": {h.SSE: "aws:kms", h.SSE_CONTEXT: _b64('["dept","a"]')},
        "context-key-equals-bucket": {h.SSE: "aws:kms", h.SSE_CONTEXT: _b64(json.dumps({bucket: "elsewhere"}))},
    }
    for name, headers in rows.items():
        with c.row(name):
            c.refused(h.put(c.site, bucket, name, data, headers), 400, "InvalidArgument", "PUT")
            c.absent(bucket, name, "PUT")


@case("NG-02", "PutObject refuses an incomplete or inconsistent SSE-C triple", ["e0"])
def ng_02(c: Ctx) -> None:
    bucket = c.bucket()
    data = h.plain("ng-02", h.KIB)
    triple = h.ssec(KEY_A)
    algorithm, key, md5 = f"{h.SSEC}algorithm", f"{h.SSEC}key", f"{h.SSEC}key-md5"
    short = h.derived_key("short", 16)
    rows = {
        "algorithm-only": {algorithm: "AES256"},
        "key-only": {key: triple[key]},
        "md5-missing": {algorithm: "AES256", key: triple[key]},
        "key-not-base64": {**triple, key: "!!! not base64 !!!"},
        "key-16-bytes": h.ssec(short),
        "md5-of-another-key": {**triple, md5: h.key_md5(KEY_B)},
        "algorithm-aes128": {**triple, algorithm: "AES128"},
    }
    for name, headers in rows.items():
        with c.row(name):
            # A partial triple that is treated as "no SSE-C" would store the object in plaintext.
            c.refused(h.put(c.site, bucket, name, data, headers), 400, "InvalidRequest", "PUT")
            c.absent(bucket, name, "PUT")


@case("NG-03", "GetObject refuses read headers that do not match how the object was stored", ["e2"])
def ng_03(c: Ctx) -> None:
    bucket = c.bucket()
    objects = {"ssec": h.ssec(KEY_A), "kms": h.sse_kms(h.KEY_1), "plain": None}
    data = {name: h.plain(f"ng-03 {name}", SIZE) for name in objects}
    for name, headers in objects.items():
        c.ok(h.put(c.site, bucket, name, data[name], headers), f"{name} PUT")
    md5_only_matches = {**h.ssec(KEY_B), f"{h.SSEC}key-md5": h.key_md5(KEY_A)}
    rows = {
        "ssec-without-key": ("ssec", None, "InvalidRequest"),
        "ssec-with-another-key": ("ssec", h.ssec(KEY_B), "InvalidRequest"),
        "ssec-right-md5-wrong-key": ("ssec", md5_only_matches, "InvalidRequest"),
        "kms-with-ssec": ("kms", h.ssec(KEY_A), "InvalidArgument"),
        "plain-with-ssec": ("plain", h.ssec(KEY_A), "InvalidRequest"),
        "plain-with-aes256": ("plain", h.sse_s3(), "InvalidArgument"),
        # No customer key came with the request, and that is what the refusal reports; the aws:kms header is not reached.
        "ssec-with-aws-kms": ("ssec", h.sse_kms(), "InvalidRequest"),
    }
    for name, (target, headers, code) in rows.items():
        with c.row(name):
            response = h.get(c.site, bucket, target, headers)
            c.refused(response, 400, code, "GET")
            c.check(data[target][:64] not in response[2], "the refused GET returned object bytes")


@case("NG-04", "SSE-KMS with a key that does not exist", ["e2"])
def ng_04(c: Ctx) -> None:
    bucket = c.bucket()
    c.refused(h.put(c.site, bucket, "object", h.plain("ng-04", h.KIB), h.sse_kms(h.MISSING_KEY)), 400,
              "KMS.NotFoundException", "PUT")
    c.absent(bucket, "object", "PUT")


@case("SEC-04", "No SSE-S3 response names the key that wraps the data key", ["e2"])
def sec_04(c: Ctx) -> None:
    bucket = c.bucket()
    data = h.plain("sec-04", SIZE)
    part = h.plain("sec-04 part", 5 * h.MIB)
    responses = {}
    responses["PutObject"] = h.put(c.site, bucket, "object", data, h.sse_s3())
    responses["GetObject"] = h.get(c.site, bucket, "object")
    status, head_headers = h.head(c.site, bucket, "object")
    responses["HeadObject"] = (status, head_headers, b"")
    responses["CopyObject"] = h.copy(c.site, bucket, "copy", bucket, "object", h.sse_s3())
    status, create_headers, upload_id, body = h.create_multipart(c.site, bucket, "multipart", h.sse_s3())
    responses["CreateMultipartUpload"] = (status, create_headers, body)
    c.need(status == 200 and upload_id, f"CreateMultipartUpload: HTTP {status} {h.s3_error(body)}")
    responses["UploadPart"] = h.upload_part(c.site, bucket, "multipart", upload_id, 1, part)
    responses["ListParts"] = h.list_parts(c.site, bucket, "multipart", upload_id)
    etag = responses["UploadPart"][1].get("etag", "")
    responses["CompleteMultipartUpload"] = h.complete_multipart(c.site, bucket, "multipart", upload_id, [(1, etag)])
    for name, (status, headers, body) in responses.items():
        c.check(status == 200, f"{name}: HTTP {status}, expected 200")
        c.check(h.SSE_KEY_ID not in headers, f"{name}: the response carries {h.SSE_KEY_ID}: {headers.get(h.SSE_KEY_ID)!r}")
        if name != "GetObject":
            c.check(h.DEFAULT_KEY.encode() not in body, f"{name}: the response body names the default key {h.DEFAULT_KEY}")
        for header, value in headers.items():
            c.check(h.DEFAULT_KEY not in value, f"{name}: header {header} names the default key {h.DEFAULT_KEY}")


# ---------------------------------------------------------------------------
# Per-key authorization: three identities with full S3 access and different KMS grants
# ---------------------------------------------------------------------------

USER_SECRET = "sse-blackbox-user-secret"
KMS_ARN_FORMS = ("arn:rustfs:kms:::key/", "arn:aws:kms:::key/")
KMS_ALL_KEYS = "arn:aws:kms:::*"
_identities: dict[str, dict] = {}


def _policy(arn: str, name: str) -> dict:
    s3 = {"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::*"]}
    use = ["kms:GenerateDataKey", "kms:Decrypt"]
    extra = {
        # U1: may use KEY_1 only.
        "u1": [{"Effect": "Allow", "Action": use, "Resource": [f"{arn}{h.KEY_1}"]}],
        # U2: full S3 access and no KMS permission at all.
        "u2": [],
        # U3: may use every key, with an explicit deny on KEY_1.
        "u3": [{"Effect": "Allow", "Action": ["kms:*"], "Resource": [KMS_ALL_KEYS]},
               {"Effect": "Deny", "Action": use, "Resource": [f"{arn}{h.KEY_1}"]}],
    }[name]
    return {"Version": "2012-10-17", "Statement": [s3, *extra]}


def _users(c: Ctx) -> dict[str, tuple[str, str]]:
    """Create the three identities once per site. Provisioning is setup, so it stays out of the observations."""
    cache_key = str(c.site.root)
    if cache_key in _identities:
        return _identities[cache_key]
    users = {}
    with observe.quiet():
        for name in ("u1", "u2", "u3"):
            user, policy_name = f"bb-{name}", f"bb-policy-{name}"
            last = None
            for arn in KMS_ARN_FORMS:
                last = h.iam_add_policy(c.site, policy_name, _policy(arn, name))
                if last[0] == 200:
                    break
            if last[0] != 200:
                raise h.EnvError(f"add policy {policy_name}: HTTP {last[0]} {str(last[1])[:200]}")
            for step, (status, body) in (("add user", h.iam_add_user(c.site, user, USER_SECRET)),
                                         ("attach policy", h.iam_attach_policy(c.site, user, policy_name))):
                if status != 200:
                    raise h.EnvError(f"{step} {user}: HTTP {status} {str(body)[:200]}")
            users[name] = (user, USER_SECRET)
    _identities[cache_key] = users
    return users


@case("AZ-01", "Under enforcement a caller needs a KMS grant on the key the request resolves to", ["e2e"])
def az_01(c: Ctx) -> None:
    bucket = c.bucket()
    users = _users(c)
    stored = {key: h.plain(f"az-01 admin {key}", SIZE) for key in (h.KEY_1, h.KEY_2)}
    for key, data in stored.items():
        c.ok(h.put(c.site, bucket, f"admin-{key}", data, h.sse_kms(key)), f"admin PUT under {key}")
    # identity -> key -> allowed
    matrix = {
        "u1": {h.KEY_1: True, h.KEY_2: False},
        "u2": {h.KEY_1: False, h.KEY_2: False},
        "u3": {h.KEY_1: False, h.KEY_2: True},
    }
    for name, grants in matrix.items():
        for key, allowed in grants.items():
            with c.row(f"{name}-{key}"):
                data = h.plain(f"az-01 {name} {key}", SIZE)
                written = h.put(c.site, bucket, f"{name}-{key}", data, h.sse_kms(key), creds=users[name])
                read = h.get(c.site, bucket, f"admin-{key}", creds=users[name])
                if allowed:
                    put_headers, _ = c.ok(written, "PUT")
                    c.projection(put_headers, h.projection_kms(key), "PUT")
                    get_headers, body = c.ok(read, "GET")
                    c.bytes_equal(get_headers, body, stored[key], "GET")
                else:
                    c.refused(written, 403, "AccessDenied", "PUT")
                    c.absent(bucket, f"{name}-{key}", "PUT")
                    c.refused(read, 403, "AccessDenied", "GET")
                    c.check(stored[key][:64] not in read[2], "the refused GET returned object bytes")


@case("AZ-02", "Enforcement leaves schemes that name no KMS key alone", ["e2e"])
def az_02(c: Ctx) -> None:
    bucket = c.bucket()
    creds = _users(c)["u2"]
    for name, write_headers, read_headers, want in (("sse-s3", h.sse_s3(), None, h.projection_s3()),
                                                    ("sse-c", h.ssec(KEY_A), h.ssec(KEY_A), h.projection_c(KEY_A))):
        with c.row(name):
            data = h.plain(f"az-02 {name}", SIZE)
            put_headers, _ = c.ok(h.put(c.site, bucket, name, data, write_headers, creds=creds), "PUT")
            c.projection(put_headers, want, "PUT")
            get_headers, body = c.ok(h.get(c.site, bucket, name, read_headers, creds=creds), "GET")
            c.projection(get_headers, want, "GET")
            c.bytes_equal(get_headers, body, data, "GET")


@case("AZ-03", "A denied caller cannot tell an existing key from a missing one", ["e2e"])
def az_03(c: Ctx) -> None:
    bucket = c.bucket()
    creds = _users(c)["u2"]
    data = h.plain("az-03", h.KIB)
    existing = h.put(c.site, bucket, "denied-existing", data, h.sse_kms(h.KEY_1), creds=creds)
    missing = h.put(c.site, bucket, "denied-missing", data, h.sse_kms(h.MISSING_KEY), creds=creds)
    c.refused(existing, 403, "AccessDenied", "denied PUT under an existing key")
    c.refused(missing, 403, "AccessDenied", "denied PUT under a missing key")
    c.check(h.s3_error(existing[2]) == h.s3_error(missing[2]),
            f"the two denials differ: {h.s3_error(existing[2])} != {h.s3_error(missing[2])}")
    c.absent(bucket, "denied-missing", "denied PUT under a missing key")
    # The administrator, who is authorized, does learn that the key is missing.
    c.refused(h.put(c.site, bucket, "admin-missing", data, h.sse_kms(h.MISSING_KEY)), 400, "KMS.NotFoundException",
              "authorized PUT under a missing key")
