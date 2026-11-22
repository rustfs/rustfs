"""Key sources and run-time state (KS): a write follows the state now, a read follows what the object recorded."""

from __future__ import annotations

import harness as h
from framework import CONFIGS, Ctx, case

KEY_A = h.derived_key("ssec a")
SIZE = 70 * h.KIB
MASTER_KEY_EVENT = "sse_local_master_key"
POLICY_MODE_EVENT = "sse_kms_key_policy_mode"
NO_KMS_MESSAGE = "SSE-KMS requires a configured and running KMS service"


def _master_key_state(c: Ctx, want: str, level: str | None = None) -> None:
    """Exactly one startup event states how the local master key was found."""
    records = c.startup_event(MASTER_KEY_EVENT)
    if not c.master_key_event:
        return  # recorded for the comparison; this binary predates the event
    if c.check(len(records) == 1, f"{len(records)} {MASTER_KEY_EVENT} events in the startup log, expected exactly 1"):
        c.check(records[0].get("state") == want, f"{MASTER_KEY_EVENT} state {records[0].get('state')!r}, expected {want!r}")
        if level:
            got = str(records[0].get("level", "")).lower()
            c.check(got == level, f"{MASTER_KEY_EVENT} level {got!r}, expected {level!r}")


def _ssec_roundtrip(c: Ctx, bucket: str) -> None:
    c.roundtrip(bucket, "ssec", h.plain(f"{c.case_id} ssec", SIZE), h.ssec(KEY_A), h.ssec(KEY_A), h.projection_c(KEY_A), "SSE-C")


@case("KS-01", "No KMS and no local master key: managed writes are refused, SSE-C works", ["e0"])
def ks_01(c: Ctx) -> None:
    bucket = c.bucket()
    _master_key_state(c, "not_set")
    data = h.plain("ks-01", SIZE)
    c.refused(h.put(c.site, bucket, "sse-s3", data, h.sse_s3()), 400, "InvalidRequest", "SSE-S3 PUT",
              message_has="RUSTFS_SSE_S3_MASTER_KEY")
    c.absent(bucket, "sse-s3", "SSE-S3 PUT")
    c.refused(h.put(c.site, bucket, "sse-kms", data, h.sse_kms()), 400, "InvalidRequest", "SSE-KMS PUT",
              message_has=NO_KMS_MESSAGE)
    c.absent(bucket, "sse-kms", "SSE-KMS PUT")
    _ssec_roundtrip(c, bucket)


@case("KS-02", "Local master key loaded: SSE-S3 works, SSE-KMS is refused and never downgraded", ["e1"])
def ks_02(c: Ctx) -> None:
    bucket = c.bucket()
    _master_key_state(c, "loaded")
    c.roundtrip(bucket, "sse-s3", h.plain("ks-02 s3", SIZE), h.sse_s3(), None, h.projection_s3(), "SSE-S3")
    # An object that says aws:kms while only the local master key protects it would be a silent downgrade.
    c.refused(h.put(c.site, bucket, "sse-kms", h.plain("ks-02 kms", SIZE), h.sse_kms()), 400, "InvalidRequest", "SSE-KMS PUT")
    c.absent(bucket, "sse-kms", "SSE-KMS PUT")


@case("KS-03", "An invalid local master key neither stops the process nor leaks its value", ["e1x-text", "e1x-short"])
def ks_03(c: Ctx) -> None:
    bucket = c.bucket()
    _master_key_state(c, "invalid", level="error")
    configured = CONFIGS[c.config](c.site)["RUSTFS_SSE_S3_MASTER_KEY"]
    fragments = {configured, configured[:12], configured[-12:]}
    response = h.put(c.site, bucket, "sse-s3", h.plain("ks-03", SIZE), h.sse_s3())
    c.refused(response, 400, "InvalidRequest", "SSE-S3 PUT")
    c.absent(bucket, "sse-s3", "SSE-S3 PUT")
    for fragment in fragments:
        c.check(fragment.encode() not in response[2], "the SSE-S3 refusal repeats part of the configured master key value")
        c.check(fragment not in h.log_text(c.site), "the server log repeats part of the configured master key value")
    _ssec_roundtrip(c, bucket)


@case("KS-04", "The startup log states whether per-key authorization is enforced", ["e2", "e2e"])
def ks_04(c: Ctx) -> None:
    records = c.startup_event(POLICY_MODE_EVENT)
    want_enforced, want_level = ("true", "info") if c.config == "e2e" else ("false", "warn")
    if c.check(len(records) == 1, f"{len(records)} {POLICY_MODE_EVENT} events in the startup log, expected exactly 1"):
        enforced = str(records[0].get("enforced")).lower()
        level = str(records[0].get("level", "")).lower()
        c.check(enforced == want_enforced, f"{POLICY_MODE_EVENT} enforced={enforced}, expected {want_enforced}")
        c.check(level == want_level, f"{POLICY_MODE_EVENT} level {level!r}, expected {want_level!r}")


@case("KS-06", "Stopping and starting KMS at run time", ["e2"], last=True)
def ks_06(c: Ctx) -> None:
    bucket = c.bucket()
    kms_data, ssec_data, plain_data = h.plain("ks-06 kms", SIZE), h.plain("ks-06 ssec", SIZE), h.plain("ks-06 plain", SIZE)
    want = h.projection_kms(h.KEY_1)
    c.ok(h.put(c.site, bucket, "kms", kms_data, h.sse_kms(h.KEY_1)), "SSE-KMS PUT")
    c.ok(h.put(c.site, bucket, "ssec", ssec_data, h.ssec(KEY_A)), "SSE-C PUT")
    c.ok(h.put(c.site, bucket, "plain", plain_data), "plaintext PUT")

    status, body = h.kms_stop(c.site)
    c.need(status == 200, f"KMS stop: HTTP {status} {str(body)[:200]}")
    try:
        with c.row("while-stopped"):
            status, _, body = h.put(c.site, bucket, "kms-while-stopped", kms_data, h.sse_kms(h.KEY_1))
            c.check(status == 503 and h.s3_error(body)[0] != "",
                    f"SSE-KMS PUT while KMS is stopped: HTTP {status} {h.s3_error(body)}, expected an explicit 503")
            c.absent(bucket, "kms-while-stopped", "SSE-KMS PUT while KMS is stopped")
            status, _, body = h.get(c.site, bucket, "kms")
            c.check(status == 503 and h.s3_error(body)[0] != "",
                    f"GET of the SSE-KMS object while KMS is stopped: HTTP {status} {h.s3_error(body)}, expected an explicit 503")
            c.check(kms_data[:64] not in body, "the refused GET returned object bytes")
            status, head_headers = h.head(c.site, bucket, "kms")
            if c.check(status == 200, f"HEAD of the SSE-KMS object while KMS is stopped: HTTP {status}, expected 200"):
                c.projection(head_headers, want, "HEAD while KMS is stopped")
            c.read_back(bucket, "ssec", ssec_data, h.ssec(KEY_A), h.projection_c(KEY_A), "SSE-C object while KMS is stopped")
            c.read_back(bucket, "plain", plain_data, None, {}, "plaintext object while KMS is stopped")
    finally:
        status, body = h.kms_start(c.site)
    c.need(status == 200, f"KMS start: HTTP {status} {str(body)[:200]}")
    h.wait_kms_healthy(c.site)
    with c.row("after-restart"):
        # The process was not restarted: the running server must pick the service up again.
        c.read_back(bucket, "kms", kms_data, None, want, "SSE-KMS object after KMS came back")
        c.roundtrip(bucket, "kms-after", h.plain("ks-06 after", SIZE), h.sse_kms(h.KEY_1), None, want, "new SSE-KMS object")
