"""Key sources across restarts and state changes, on configurations that need no external service.

Every case states the same rule from another side: a write follows the state the process is in now, a read
follows what the object recorded when it was written.
"""

from __future__ import annotations

import base64
import hashlib
import xml.etree.ElementTree as ET

import backends as b
import framework as fw
import harness as h
import observe
import stored
from framework import Ctx, case

KEY_A = h.derived_key("ssec a")
SIZE = 70 * h.KIB
NO_KMS_MESSAGE = "SSE-KMS requires a configured and running KMS service"
GENERIC_INTERNAL_MESSAGE = "We encountered an internal error, please try again."
MASTER_KEY_VARIABLE = "RUSTFS_SSE_S3_MASTER_KEY"
# Where an object records its wrapped data key and the id of the key that wrapped it.
WRAPPED_KEY_NAME, KEY_ID_NAME = "x-rustfs-encryption-key", "x-rustfs-encryption-key-id"
# The two formats of a wrapped data key, as the stored-form reader names them.
KMS_ENVELOPE, LOCAL_ENVELOPE = "kms-envelope", "local-envelope"
# The key id an object records when the local master key sealed its data key.
LOCAL_MASTER_KEY_ID = "default"


# ---------------------------------------------------------------------------
# Shared steps
# ---------------------------------------------------------------------------


def unavailable(c: Ctx, response, what: str, data: bytes | None = None) -> None:
    """The KMS that has to serve this request is not there: an explicit, retryable 503 and no object bytes."""
    c.refused(response, 503, "ServiceUnavailable", what)
    if data is not None:
        no_object_bytes(c, response, data, what)


def no_object_bytes(c: Ctx, response, data: bytes, what: str) -> None:
    c.check(data[:64] not in response[2], f"{what}: the refused read returned object bytes")


def head_projection(c: Ctx, bucket: str, key: str, want: dict[str, str], what: str) -> dict[str, str]:
    """HEAD needs no data key, so it answers whatever state the key source is in."""
    status, headers = h.head(c.site, bucket, key)
    if c.check(status == 200, f"{what}: HEAD is {status}, expected 200"):
        c.projection(headers, want, f"{what} HEAD")
    return headers


def identity(c: Ctx, bucket: str, key: str) -> tuple[str | None, str | None, str]:
    """What changes when an object is rewritten: ETag, Last-Modified, and the bytes of its metadata file on every drive."""
    with observe.quiet():
        _, headers = h.head(c.site, bucket, key)
    digest = hashlib.sha256()
    for drive in c.site.drives():
        xl = drive / bucket / key / "xl.meta"
        digest.update(xl.read_bytes() if xl.exists() else b"<absent>")
    return headers.get("etag"), headers.get("last-modified"), digest.hexdigest()


def wrapped_by(c: Ctx, bucket: str, key: str, kind: str, key_id: str, what: str) -> None:
    """Stored form: which key source wrapped the object's data key, and under which key id."""
    metadata = stored.read_object(c.site, bucket, key).metadata
    got = stored.envelope_kind(metadata.get(WRAPPED_KEY_NAME, "")), metadata.get(KEY_ID_NAME)
    observe.note("wrapped-by", f"{bucket}/{key}", list(got))
    if c.build != "default":
        return  # another build records the data key under other names; the note is kept for the comparison only
    c.check(got == (kind, key_id), f"{what}: the object records {got} as (wrapping, key id), expected {(kind, key_id)}")


def stop_kms(c: Ctx) -> None:
    response = h.kms_stop(c.site)
    c.need(b.succeeded(response), f"KMS stop: HTTP {response[0]} {str(response[1])[:200]}")


def start_kms(c: Ctx) -> None:
    response = h.kms_start(c.site)
    c.need(b.succeeded(response), f"KMS start: HTTP {response[0]} {str(response[1])[:200]}")
    problem = b.kms_not_running(c.site)
    c.need(not problem, problem)


def restart_with(c: Ctx, env: dict[str, str], kms: bool) -> None:
    """Restart on the same data directory with exactly `env`; with `kms`, the service must come up by itself."""
    c.restart(env=env)
    if kms:
        problem = b.kms_not_running(c.site)
        c.need(not problem, f"after the restart {problem}")


# ---------------------------------------------------------------------------
# Local master key
# ---------------------------------------------------------------------------


@case("KS-05", "The local master key across restarts: the same key reads, another key fails without bytes", ["e1"],
      level="p1", isolated=True)
def ks_05(c: Ctx) -> None:
    bucket = c.bucket()
    data, other_data = h.plain("ks-05 a", SIZE), h.plain("ks-05 b", SIZE)
    original = {MASTER_KEY_VARIABLE: fw.master_key()}
    another = {MASTER_KEY_VARIABLE: base64.b64encode(h.derived_key("another local master key")).decode()}
    c.ok(h.put(c.site, bucket, "a", data, h.sse_s3()), "SSE-S3 PUT")
    written = identity(c, bucket, "a")

    restart_with(c, original, kms=False)
    with c.row("same-key"):
        c.read_back(bucket, "a", data, None, h.projection_s3(), "object after a restart with the same master key")

    restart_with(c, another, kms=False)
    with c.row("another-key"):
        response = h.get(c.site, bucket, "a")
        # The data key does not unwrap. The server cannot tell a wrong master key from a damaged object, so it
        # reports the generic internal error; what matters is that no bytes come back.
        c.refused(response, 500, "InternalError", "GET under another master key")
        c.check(h.s3_error(response[2])[1] == GENERIC_INTERNAL_MESSAGE,
                f"GET under another master key: message {h.s3_error(response[2])[1][:200]!r}, expected the generic text")
        no_object_bytes(c, response, data, "GET under another master key")
        for value in (original[MASTER_KEY_VARIABLE], another[MASTER_KEY_VARIABLE]):
            c.check(value.encode() not in response[2], "the refusal repeats a master key value")
        head_projection(c, bucket, "a", h.projection_s3(), "object under another master key")
        c.roundtrip(bucket, "b", other_data, h.sse_s3(), None, h.projection_s3(), "new SSE-S3 object under the other key")

    restart_with(c, original, kms=False)
    with c.row("original-key-again"):
        c.read_back(bucket, "a", data, None, h.projection_s3(), "object after returning to its master key")
        c.check(identity(c, bucket, "a") == written, "the failed read under another master key rewrote the object")
        response = h.get(c.site, bucket, "b")
        c.refused(response, 500, "InternalError", "GET of the object written under the other key")
        no_object_bytes(c, response, other_data, "GET of the object written under the other key")


# ---------------------------------------------------------------------------
# KMS and the local master key side by side
# ---------------------------------------------------------------------------


@case("KS-07", "While KMS is stopped SSE-S3 falls back to the local master key; each object reads by what it recorded",
      ["e2m"], level="p1", isolated=True)
def ks_07(c: Ctx) -> None:
    bucket = c.bucket()
    data = {name: h.plain(f"ks-07 {name}", SIZE) for name in ("a", "b", "c", "kms")}
    want = h.projection_s3()
    c.ok(h.put(c.site, bucket, "a", data["a"], h.sse_s3()), "SSE-S3 PUT while KMS runs")

    stop_kms(c)
    try:
        with c.row("while-stopped"):
            headers, _ = c.ok(h.put(c.site, bucket, "b", data["b"], h.sse_s3()), "SSE-S3 PUT while KMS is stopped")
            c.projection(headers, want, "SSE-S3 PUT while KMS is stopped")
            c.read_back(bucket, "b", data["b"], None, want, "object written while KMS is stopped")
            # The object was wrapped by KMS; the local master key must not be tried on it.
            unavailable(c, h.get(c.site, bucket, "a"), "GET of the KMS-wrapped object while KMS is stopped", data["a"])
            head_projection(c, bucket, "a", want, "KMS-wrapped object while KMS is stopped")
            # The fallback covers SSE-S3 only: an aws:kms object sealed by the local master key would be a downgrade.
            unavailable(c, h.put(c.site, bucket, "kms", data["kms"], h.sse_kms()), "SSE-KMS PUT while KMS is stopped")
            c.absent(bucket, "kms", "SSE-KMS PUT while KMS is stopped")
    finally:
        start_kms(c)

    with c.row("after-start"):
        c.read_back(bucket, "a", data["a"], None, want, "KMS-wrapped object after KMS came back")
        c.read_back(bucket, "b", data["b"], None, want, "object sealed by the local master key after KMS came back")
        c.roundtrip(bucket, "c", data["c"], h.sse_s3(), None, want, "new SSE-S3 object after KMS came back")
    with c.row("recorded-wrapping"):
        wrapped_by(c, bucket, "a", KMS_ENVELOPE, h.DEFAULT_KEY, "object written while KMS ran")
        wrapped_by(c, bucket, "b", LOCAL_ENVELOPE, LOCAL_MASTER_KEY_ID, "object written while KMS was stopped")
        wrapped_by(c, bucket, "c", KMS_ENVELOPE, h.DEFAULT_KEY, "object written after KMS came back")


@case("KS-08", "A deployment moves from the local master key to KMS, then drops the master key", ["e1"],
      level="p1", isolated=True)
def ks_08(c: Ctx) -> None:
    bucket = c.bucket()
    data_a, data_b = h.plain("ks-08 a", SIZE), h.plain("ks-08 b", SIZE)
    want = h.projection_s3()
    c.ok(h.put(c.site, bucket, "a", data_a, h.sse_s3()), "SSE-S3 PUT with the master key only")

    # The configuration's own start creates the Local KMS default key, which this data directory does not have yet.
    c.restart(config="e2m")
    with c.row("kms-and-master-key"):
        c.read_back(bucket, "a", data_a, None, want, "master-key object once KMS runs")
        c.roundtrip(bucket, "b", data_b, h.sse_s3(), None, want, "new SSE-S3 object once KMS runs")
        wrapped_by(c, bucket, "a", LOCAL_ENVELOPE, LOCAL_MASTER_KEY_ID, "object written before KMS")
        wrapped_by(c, bucket, "b", KMS_ENVELOPE, h.DEFAULT_KEY, "object written once KMS runs")

    # Same KMS and the same key files, without the master key. Starting as a configuration again would replace
    # the default key file, so the environment is given directly.
    restart_with(c, b.local_kms_env(c.site), kms=True)
    with c.row("kms-without-master-key"):
        c.read_back(bucket, "b", data_b, None, want, "KMS-wrapped object without the master key")
        response = h.get(c.site, bucket, "a")
        # The refusal names the missing master key: KMS was never asked to unwrap a key it did not wrap.
        c.refused(response, 400, "InvalidRequest", "GET of the master-key object without the master key",
                  message_has=MASTER_KEY_VARIABLE)
        no_object_bytes(c, response, data_a, "GET of the master-key object without the master key")
        head_projection(c, bucket, "a", want, "master-key object without the master key")


@case("KS-09", "KMS configured and started through the admin API at run time, and restored after a restart", ["e0"],
      level="p1", isolated=True)
def ks_09(c: Ctx) -> None:
    bucket = c.bucket()
    data, later = h.plain("ks-09 first", SIZE), h.plain("ks-09 later", SIZE)
    want = h.projection_kms(h.DEFAULT_KEY)

    with c.row("before-configure"):
        response = h.kms_start(c.site)
        c.check(response[0] == 200 and isinstance(response[1], dict) and response[1].get("success") is False,
                f"KMS start without a configuration: HTTP {response[0]} {str(response[1])[:200]}, expected success=false")
        # A refused start configured nothing, so the write is still the request of a deployment without KMS:
        # 400, as before the attempt. Today the attempt leaves the service in an error state, and from then on
        # the same write is answered as an outage, which clients retry without end.
        response = h.put(c.site, bucket, "early", data, h.sse_kms())
        intended = "an SSE-KMS PUT after a refused KMS start is 400 InvalidRequest, as it was before the attempt"
        if (response[0], h.s3_error(response[2])[0]) == (503, "ServiceUnavailable"):
            c.known_deviation(False, f"{intended}; today it is 503 ServiceUnavailable")
        elif c.refused(response, 400, "InvalidRequest", "SSE-KMS PUT after a refused KMS start", message_has=NO_KMS_MESSAGE):
            c.known_deviation(True, intended)
        c.absent(bucket, "early", "SSE-KMS PUT after a refused KMS start")

    h.write_local_key_file(c.site.key_dir, h.DEFAULT_KEY)
    payload = {"backend_type": "Local", "key_dir": str(c.site.key_dir), "default_key_id": h.DEFAULT_KEY,
               "allow_insecure_dev_defaults": True}
    response = b.kms_configure(c.site, payload)
    c.need(b.succeeded(response), f"KMS configure: HTTP {response[0]} {str(response[1])[:200]}")
    with c.row("configured-not-started"):
        # Configured but not running is an outage of a KMS that exists, hence retryable; before, there was none.
        unavailable(c, h.put(c.site, bucket, "early", data, h.sse_kms()), "SSE-KMS PUT before KMS is started")
        c.absent(bucket, "early", "SSE-KMS PUT before KMS is started")

    start_kms(c)
    with c.row("running"):
        # The process started without KMS; the choice must not have been frozen then.
        c.roundtrip(bucket, "first", data, h.sse_kms(), None, want, "SSE-KMS object after the run-time configuration")

    c.restart(env={})
    problem = b.kms_not_running(c.site)
    c.need(not problem, f"after a restart without any KMS variable {problem}: the admin configuration was not restored")
    with c.row("after-restart"):
        c.read_back(bucket, "first", data, None, want, "SSE-KMS object after the restart")
        c.roundtrip(bucket, "later", later, h.sse_kms(), None, want, "new SSE-KMS object after the restart")


@case("KS-12", "KMS stops in the middle of an SSE-KMS multipart upload", ["e2", "e2m"], level="p1",
      isolated=True)
def ks_12(c: Ctx) -> None:
    # With a master key loaded there is something a part could wrongly be sealed by while the upload still says
    # aws:kms; without one that mistake could not succeed, so the case runs on both.
    bucket = c.bucket()
    parts = [h.plain("ks-12 part 1", 5 * h.MIB), h.plain("ks-12 part 2", 5 * h.MIB), h.plain("ks-12 part 3", 1000)]
    want = h.projection_kms(h.KEY_1)
    status, headers, upload_id, body = h.create_multipart(c.site, bucket, "mp", h.sse_kms(h.KEY_1))
    c.need(status == 200 and bool(upload_id), f"CreateMultipartUpload: HTTP {status} {h.s3_error(body)}")
    c.projection(headers, want, "CreateMultipartUpload")
    etags = {}
    part_headers, _ = c.ok(h.upload_part(c.site, bucket, "mp", upload_id, 1, parts[0]), "UploadPart 1")
    etags[1] = part_headers.get("etag", "")

    stop_kms(c)
    try:
        with c.row("while-stopped"):
            unavailable(c, h.upload_part(c.site, bucket, "mp", upload_id, 2, parts[1]), "UploadPart 2 while KMS is stopped")
            # The refused part must not exist in any form: stored in plaintext or under another key it would
            # still be listed.
            status, _, body = h.list_parts(c.site, bucket, "mp", upload_id)
            if c.check(status == 200, f"ListParts while KMS is stopped: HTTP {status} {h.s3_error(body)}"):
                listed = [part.findtext("{*}PartNumber") for part in ET.fromstring(body).findall("{*}Part")]
                c.check(listed == ["1"], f"ListParts while KMS is stopped lists parts {listed}, expected only part 1")
    finally:
        start_kms(c)

    with c.row("resumed"):
        for number in (2, 3):
            part_headers, _ = c.ok(h.upload_part(c.site, bucket, "mp", upload_id, number, parts[number - 1]),
                                   f"UploadPart {number} after KMS came back")
            etags[number] = part_headers.get("etag", "")
        headers, _ = c.ok(h.complete_multipart(c.site, bucket, "mp", upload_id, sorted(etags.items())),
                          "CompleteMultipartUpload")
        c.projection(headers, want, "CompleteMultipartUpload")
        c.read_back(bucket, "mp", b"".join(parts), None, want, "completed object")
        c.stored(bucket, "mp")


# ---------------------------------------------------------------------------
# Key state
# ---------------------------------------------------------------------------

KS_13_TITLE = "A KMS key is disabled and enabled again"


def ks_13(c: Ctx) -> None:
    b.require(c.config, "disable")
    key_id = f"bb-ks-13-{c.config}"
    h.ensure_key(c.site, key_id)
    bucket = c.bucket(c.config)
    before, after = h.plain("ks-13 before", SIZE), h.plain("ks-13 after", SIZE)
    want = h.projection_kms(key_id)
    c.ok(h.put(c.site, bucket, "before", before, h.sse_kms(key_id)), "SSE-KMS PUT")

    response = b.key_disable(c.site, key_id)
    c.need(b.key_state(response) == "Disabled", f"disable key: HTTP {response[0]} {str(response[1])[:200]}")
    try:
        with c.row("while-disabled"):
            # A key in the wrong state is the caller's to fix: 400, never a 500 that clients retry.
            c.refused(h.put(c.site, bucket, "refused", after, h.sse_kms(key_id)), 400, "InvalidRequest",
                      "SSE-KMS PUT under the disabled key", message_has="is disabled")
            c.absent(bucket, "refused", "SSE-KMS PUT under the disabled key")
            if b.can(c.config, "read_while_disabled"):
                c.read_back(bucket, "before", before, None, want, "existing object while its key is disabled")
            else:
                response = h.get(c.site, bucket, "before")
                c.refused(response, 400, "InvalidRequest", "GET while the key is disabled")
                no_object_bytes(c, response, before, "GET while the key is disabled")
    finally:
        response = b.key_enable(c.site, key_id)
    c.need(b.key_state(response) == "Enabled", f"enable key: HTTP {response[0]} {str(response[1])[:200]}")
    with c.row("enabled-again"):
        c.read_back(bucket, "before", before, None, want, "existing object after the key is enabled again")
        c.roundtrip(bucket, "after", after, h.sse_kms(key_id), None, want, "new SSE-KMS object after the key is enabled again")


case("KS-13", KS_13_TITLE, ["e2"], level="p1", last=True)(ks_13)


@case("KS-14", "A destroyed KMS key: reads and writes under it report that the key is gone", ["e2"],
      level="p1", isolated=True)
def ks_14(c: Ctx) -> None:
    b.require(c.config, "destroy")
    # Destroying a key ahead of its waiting window is refused unless the server was started to allow it.
    restart_with(c, {**b.local_kms_env(c.site), "RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION": "true"}, kms=True)
    key_id = "bb-ks-14"
    h.ensure_key(c.site, key_id)
    bucket = c.bucket()
    data = h.plain("ks-14", SIZE)
    want = h.projection_kms(key_id)
    not_found = f"KMS key not found: {key_id}"
    c.ok(h.put(c.site, bucket, "object", data, h.sse_kms(key_id)), "SSE-KMS PUT")

    with c.row("pending-deletion"):
        response = b.key_schedule_deletion(c.site, key_id)
        c.need(b.succeeded(response), f"schedule key deletion: HTTP {response[0]} {str(response[1])[:200]}")
        c.refused(h.put(c.site, bucket, "pending", data, h.sse_kms(key_id)), 400, "InvalidRequest",
                  "SSE-KMS PUT under the key pending deletion", message_has="is pending deletion")
        c.absent(bucket, "pending", "SSE-KMS PUT under the key pending deletion")
        # The waiting window is a safety net only while the data is still there to be missed.
        c.read_back(bucket, "object", data, None, want, "existing object while its key is pending deletion")

    with c.row("destroyed"):
        response = b.key_destroy(c.site, key_id)
        c.need(b.succeeded(response), f"destroy key: HTTP {response[0]} {str(response[1])[:200]}")
        response = h.get(c.site, bucket, "object")
        c.refused(response, 400, "KMS.NotFoundException", "GET under the destroyed key")
        # The message names the key the caller already knows and nothing about where keys are kept.
        c.check(h.s3_error(response[2])[1] == not_found,
                f"GET under the destroyed key: message {h.s3_error(response[2])[1][:200]!r}, expected {not_found!r}")
        no_object_bytes(c, response, data, "GET under the destroyed key")
        response = h.put(c.site, bucket, "after", data, h.sse_kms(key_id))
        c.refused(response, 400, "KMS.NotFoundException", "SSE-KMS PUT under the destroyed key")
        c.check(h.s3_error(response[2])[1] == not_found,
                f"SSE-KMS PUT under the destroyed key: message {h.s3_error(response[2])[1][:200]!r}, expected {not_found!r}")
        c.absent(bucket, "after", "SSE-KMS PUT under the destroyed key")
        head_projection(c, bucket, "object", want, "object under the destroyed key")


@case("KS-19", "KMS runs without a default key: a write has to name its key", ["e2"], level="p1", isolated=True)
def ks_19(c: Ctx) -> None:
    restart_with(c, b.local_kms_env(c.site, RUSTFS_KMS_DEFAULT_KEY_ID=None), kms=True)
    bucket = c.bucket()
    data = h.plain("ks-19", SIZE)
    with c.row("sse-kms-without-key-id"):
        # Not an outage: the service runs, the request is incomplete. Hence 400 and not 503.
        c.refused(h.put(c.site, bucket, "bare", data, h.sse_kms()), 400, "InvalidRequest", "SSE-KMS PUT without a key id",
                  message_has="the KMS service has no default key")
        c.absent(bucket, "bare", "SSE-KMS PUT without a key id")
    with c.row("sse-kms-with-key-id"):
        c.roundtrip(bucket, "named", data, h.sse_kms(h.KEY_1), None, h.projection_kms(h.KEY_1), "SSE-KMS with an explicit key")
    with c.row("sse-s3"):
        # SSE-S3 cannot name a key. Without a service default the server asks KMS for a key called "default",
        # and that lookup is what the refusal reports.
        c.refused(h.put(c.site, bucket, "sse-s3", data, h.sse_s3()), 400, "KMS.NotFoundException",
                  "SSE-S3 PUT without a default key")
        c.absent(bucket, "sse-s3", "SSE-S3 PUT without a default key")
