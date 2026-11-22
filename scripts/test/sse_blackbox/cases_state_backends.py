"""Key sources and state changes on KMS backends other than the Local one: Vault Transit, Vault KV2, and Static."""

from __future__ import annotations

import base64
import time

import backends as b
import harness as h
import observe
from cases_state_p1 import (
    KEY_A,
    KMS_ENVELOPE,
    KS_13_TITLE,
    SIZE,
    head_projection,
    identity,
    ks_13,
    no_object_bytes,
    restart_with,
    unavailable,
    wrapped_by,
)
from framework import Ctx, case

# What a read reports when the configured backend holds other key material under the object's key id.
OTHER_MATERIAL_MESSAGE = "could not be unwrapped by the configured KMS backend"
# After a backend outage the server refuses KMS calls for 30 seconds before it tries the backend again; one
# health probe later the service is back. The bound leaves room for both and for a loaded machine.
RECOVERY_BOUND = 75.0


def _settled(c: Ctx, response, what: str) -> None:
    """A service-control call succeeded and left KMS running."""
    c.need(b.succeeded(response), f"{what}: HTTP {response[0]} {str(response[1])[:200]}")
    problem = b.kms_not_running(c.site)
    c.need(not problem, f"after {what} {problem}")


# ---------------------------------------------------------------------------
# Service control on a running backend
# ---------------------------------------------------------------------------


@case("KS-10", "Forced KMS restart, reconfiguration with the same settings, and reload leave objects and the default key alone",
      ["e3"], level="p1", isolated=True)
def ks_10(c: Ctx) -> None:
    backend = b.backend_of(c.config)
    bucket = c.bucket()
    kms_data, s3_data = h.plain("ks-10 kms", SIZE), h.plain("ks-10 s3", SIZE)
    c.ok(h.put(c.site, bucket, "kms", kms_data, h.sse_kms(h.KEY_1)), "SSE-KMS PUT")
    c.ok(h.put(c.site, bucket, "s3", s3_data, h.sse_s3()), "SSE-S3 PUT")

    def unchanged(row: str) -> None:
        c.read_back(bucket, "kms", kms_data, None, h.projection_kms(h.KEY_1), f"SSE-KMS object after {row}")
        c.read_back(bucket, "s3", s3_data, None, h.projection_s3(), f"SSE-S3 object after {row}")
        # A write without a key id shows which default key the service ended up with.
        c.roundtrip(bucket, f"kms-after-{row}", h.plain(f"ks-10 {row} kms", SIZE), h.sse_kms(), None,
                    h.projection_kms(h.DEFAULT_KEY), f"new SSE-KMS object after {row}")
        c.roundtrip(bucket, f"s3-after-{row}", h.plain(f"ks-10 {row} s3", SIZE), h.sse_s3(), None, h.projection_s3(),
                    f"new SSE-S3 object after {row}")
        wrapped_by(c, bucket, f"s3-after-{row}", KMS_ENVELOPE, h.DEFAULT_KEY, f"new SSE-S3 object after {row}")

    with c.row("forced-restart"):
        response = h.kms_start(c.site)
        c.check(response[0] == 200 and isinstance(response[1], dict) and response[1].get("success") is False,
                f"KMS start while running: HTTP {response[0]} {str(response[1])[:200]}, expected success=false")
        _settled(c, b.kms_restart(c.site), "the forced restart")
        unchanged("forced-restart")
    with c.row("reconfigure"):
        # The empty token asks the server to keep the credentials it has.
        _settled(c, b.kms_reconfigure(c.site, b.vault_configure_payload(backend)), "the reconfiguration")
        unchanged("reconfigure")
    with c.row("reload"):
        _settled(c, b.kms_reload(c.site), "the reload")
        unchanged("reload")


@case("KS-18", "Changing the KMS default key moves new writes only; a bucket default keeps the key it pinned", ["e3"],
      level="p1", isolated=True)
def ks_18(c: Ctx) -> None:
    backend = b.backend_of(c.config)
    pinned, free = c.bucket("pinned"), c.bucket("free")
    data = {name: h.plain(f"ks-18 {name}", SIZE) for name in ("before-kms", "before-s3", "pinned", "free", "s3")}
    old, new = h.projection_kms(h.DEFAULT_KEY), h.projection_kms(h.KEY_2)
    c.ok(h.put_bucket_encryption(c.site, pinned, "aws:kms"), "PutBucketEncryption aws:kms without a key id")
    status, config, _ = h.get_bucket_encryption(c.site, pinned)
    c.need(status == 200 and config.get("kms_key_id") == h.DEFAULT_KEY,
           f"GetBucketEncryption: HTTP {status} {config}, expected the default key {h.DEFAULT_KEY} to be pinned")
    c.ok(h.put(c.site, free, "before-kms", data["before-kms"], h.sse_kms()), "SSE-KMS PUT before the change")
    c.ok(h.put(c.site, free, "before-s3", data["before-s3"], h.sse_s3()), "SSE-S3 PUT before the change")

    response = b.kms_reconfigure(c.site, b.vault_configure_payload(backend, default_key_id=h.KEY_2))
    _settled(c, response, "the change of the default key")

    with c.row("bucket-default-keeps-its-key"):
        status, config, _ = h.get_bucket_encryption(c.site, pinned)
        c.check(status == 200 and config.get("kms_key_id") == h.DEFAULT_KEY,
                f"GetBucketEncryption after the change: HTTP {status} {config}, expected still {h.DEFAULT_KEY}")
        c.roundtrip(pinned, "after", data["pinned"], None, None, old, "PUT without SSE headers into the bucket")
    with c.row("new-writes-use-the-new-default"):
        c.roundtrip(free, "after", data["free"], h.sse_kms(), None, new, "SSE-KMS PUT without a key id")
        c.roundtrip(free, "after-s3", data["s3"], h.sse_s3(), None, h.projection_s3(), "SSE-S3 PUT")
        wrapped_by(c, free, "after-s3", KMS_ENVELOPE, h.KEY_2, "SSE-S3 object written after the change")
    with c.row("old-objects-keep-their-key"):
        c.read_back(free, "before-kms", data["before-kms"], None, old, "SSE-KMS object written before the change")
        c.read_back(free, "before-s3", data["before-s3"], None, h.projection_s3(), "SSE-S3 object written before the change")
        wrapped_by(c, free, "before-s3", KMS_ENVELOPE, h.DEFAULT_KEY, "SSE-S3 object written before the change")


# ---------------------------------------------------------------------------
# Changing the backend
# ---------------------------------------------------------------------------


@case("KS-11", "Changing the KMS backend through a restart: the other backend's objects fail explicitly and come back",
      ["e4"], level="p1", isolated=True)
def ks_11(c: Ctx) -> None:
    bucket = c.bucket()
    vault_only, local_only = "bb-ks-11-vault-only", "bb-ks-11-local-only"
    names = ("shared", "only", "s3")
    vault_data = {name: h.plain(f"ks-11 vault {name}", SIZE) for name in names}
    local_data = {name: h.plain(f"ks-11 local {name}", SIZE) for name in names}

    def write(prefix: str, data: dict, own_key: str) -> dict:
        """Three objects: under the default key id both backends have, under a key only this one has, and SSE-S3."""
        h.ensure_key(c.site, own_key)
        headers = {"shared": h.sse_kms(), "only": h.sse_kms(own_key), "s3": h.sse_s3()}
        for name in names:
            c.ok(h.put(c.site, bucket, f"{prefix}-{name}", data[name], headers[name]), f"{prefix}-{name} PUT")
        return {"shared": h.projection_kms(h.DEFAULT_KEY), "only": h.projection_kms(own_key), "s3": h.projection_s3()}

    def readable(prefix: str, data: dict, want: dict, where: str) -> None:
        for name in names:
            c.read_back(bucket, f"{prefix}-{name}", data[name], None, want[name], f"{prefix}-{name} {where}")

    def foreign(prefix: str, data: dict, want: dict, own_key: str, where: str) -> None:
        """Objects of the other backend: never bytes, never the generic internal error."""
        response = h.get(c.site, bucket, f"{prefix}-only")
        c.refused(response, 400, "KMS.NotFoundException", f"{prefix}-only {where}", message_has=own_key)
        no_object_bytes(c, response, data["only"], f"{prefix}-only {where}")
        for name in ("shared", "s3"):
            # The key id exists here too, with other material. That is an integrity fault and stays 500, with a
            # message that names this cause instead of the generic text.
            response = h.get(c.site, bucket, f"{prefix}-{name}")
            c.refused(response, 500, "InternalError", f"{prefix}-{name} {where}", message_has=OTHER_MATERIAL_MESSAGE)
            no_object_bytes(c, response, data[name], f"{prefix}-{name} {where}")
        head_projection(c, bucket, f"{prefix}-only", want["only"], f"{prefix}-only {where}")

    vault_want = write("vault", vault_data, vault_only)

    # First start of the Local backend on this data directory: the configuration creates its default key.
    c.restart(config="e2")
    local_env = dict(c.site.env)
    with c.row("vault-objects-on-local"):
        foreign("vault", vault_data, vault_want, vault_only, "on the Local backend")
    local_want = write("local", local_data, local_only)

    c.restart(config=c.config)
    with c.row("back-on-vault"):
        readable("vault", vault_data, vault_want, "back on Vault")
        foreign("local", local_data, local_want, local_only, "on Vault")

    # The Local key files exist now; starting as the configuration again would replace the default key.
    restart_with(c, local_env, kms=True)
    with c.row("back-on-local"):
        readable("local", local_data, local_want, "back on the Local backend")


# ---------------------------------------------------------------------------
# Key state
# ---------------------------------------------------------------------------

case("KS-13", KS_13_TITLE, ["e3", "e4"], level="p1", last=True)(ks_13)


@case("KS-15", "Key rotation: objects of the old version stay readable and are not rewritten", ["e3", "e4"], level="p1",
      last=True)
def ks_15(c: Ctx) -> None:
    b.require(c.config, "rotate")
    key_id = f"bb-ks-15-{c.config}"
    h.ensure_key(c.site, key_id)
    bucket = c.bucket(c.config)
    old, new = h.plain("ks-15 old", SIZE), h.plain("ks-15 new", SIZE)
    want = h.projection_kms(key_id)
    c.ok(h.put(c.site, bucket, "old", old, h.sse_kms(key_id)), "SSE-KMS PUT before the rotation")
    written = identity(c, bucket, "old")

    response = b.key_rotate(c.site, key_id)
    c.need(b.succeeded(response), f"rotate key: HTTP {response[0]} {str(response[1])[:200]}")
    with c.row("after-rotation"):
        c.read_back(bucket, "old", old, None, want, "object wrapped under the previous key version")
        c.roundtrip(bucket, "new", new, h.sse_kms(key_id), None, want, "new SSE-KMS object after the rotation")
        c.read_back(bucket, "old", old, None, want, "object wrapped under the previous key version, read again")
        c.check(identity(c, bucket, "old") == written, "the rotation or the reads after it rewrote the existing object")


# ---------------------------------------------------------------------------
# Backend outage
# ---------------------------------------------------------------------------


@case("KS-16", "Vault is sealed and unsealed: 503 meanwhile, and the running server recovers by itself", ["e3"],
      level="p1", isolated=True)
def ks_16(c: Ctx) -> None:
    bucket = c.bucket()
    data = {name: h.plain(f"ks-16 {name}", SIZE) for name in ("kms", "s3", "ssec", "plain", "refused", "after")}
    want = h.projection_kms(h.KEY_1)
    c.ok(h.put(c.site, bucket, "kms", data["kms"], h.sse_kms(h.KEY_1)), "SSE-KMS PUT")
    c.ok(h.put(c.site, bucket, "s3", data["s3"], h.sse_s3()), "SSE-S3 PUT")
    c.ok(h.put(c.site, bucket, "ssec", data["ssec"], h.ssec(KEY_A)), "SSE-C PUT")
    c.ok(h.put(c.site, bucket, "plain", data["plain"]), "plaintext PUT")

    b.seal_vault()
    try:
        with c.row("while-sealed"):
            unavailable(c, h.get(c.site, bucket, "kms"), "GET of the SSE-KMS object while Vault is sealed", data["kms"])
            unavailable(c, h.get(c.site, bucket, "s3"), "GET of the SSE-S3 object while Vault is sealed", data["s3"])
            unavailable(c, h.put(c.site, bucket, "refused-kms", data["refused"], h.sse_kms(h.KEY_1)),
                        "SSE-KMS PUT while Vault is sealed")
            c.absent(bucket, "refused-kms", "SSE-KMS PUT while Vault is sealed")
            unavailable(c, h.put(c.site, bucket, "refused-s3", data["refused"], h.sse_s3()), "SSE-S3 PUT while Vault is sealed")
            c.absent(bucket, "refused-s3", "SSE-S3 PUT while Vault is sealed")
            head_projection(c, bucket, "kms", want, "SSE-KMS object while Vault is sealed")
            c.read_back(bucket, "ssec", data["ssec"], h.ssec(KEY_A), h.projection_c(KEY_A), "SSE-C object while Vault is sealed")
            c.read_back(bucket, "plain", data["plain"], None, {}, "plaintext object while Vault is sealed")
    finally:
        b.unseal_vault()

    # The server is not restarted and nobody calls the admin API: it has to notice by itself.
    started = time.monotonic()
    with observe.quiet():
        while h.get(c.site, bucket, "kms")[0] != 200 and time.monotonic() - started < RECOVERY_BOUND:
            time.sleep(0.5)
    with c.row("recovered"):
        c.read_back(bucket, "kms", data["kms"], None, want,
                    f"SSE-KMS object {time.monotonic() - started:.0f}s after Vault was unsealed (bound {RECOVERY_BOUND:.0f}s)")
        c.read_back(bucket, "s3", data["s3"], None, h.projection_s3(), "SSE-S3 object after Vault was unsealed")
        c.roundtrip(bucket, "after", data["after"], h.sse_kms(h.KEY_1), None, want, "new SSE-KMS object after Vault was unsealed")


# ---------------------------------------------------------------------------
# Static backend
# ---------------------------------------------------------------------------


@case("KS-20", "Static backend: the data path works, key management is not implemented, admin configuration is refused",
      ["e8"], level="p1")
def ks_20(c: Ctx) -> None:
    bucket = c.bucket()
    want = h.projection_kms(b.STATIC_KEY)
    with c.row("data-path"):
        c.roundtrip(bucket, "s3", h.plain("ks-20 s3", SIZE), h.sse_s3(), None, h.projection_s3(), "SSE-S3")
        c.roundtrip(bucket, "kms", h.plain("ks-20 kms", SIZE), h.sse_kms(), None, want, "SSE-KMS without a key id")
        c.roundtrip(bucket, "kms-named", h.plain("ks-20 named", SIZE), h.sse_kms(b.STATIC_KEY), None, want,
                    "SSE-KMS naming the configured key")
        c.refused(h.put(c.site, bucket, "other", h.plain("ks-20 other", SIZE), h.sse_kms(h.KEY_1)), 400,
                  "KMS.NotFoundException", "SSE-KMS naming a key the backend does not have")
        c.absent(bucket, "other", "SSE-KMS naming a key the backend does not have")
    with c.row("key-management"):
        # A backend that cannot do something says 501; a 500 would read as a fault, a 404 as a missing key.
        attempts = {
            "create a key": h.key_create(c.site, h.KEY_1),
            "disable the key": b.key_disable(c.site, b.STATIC_KEY),
            "rotate the key": b.key_rotate(c.site, b.STATIC_KEY),
            "schedule the key's deletion": b.key_schedule_deletion(c.site, b.STATIC_KEY),
        }
        for what, (status, body) in attempts.items():
            c.check(status == 501 and isinstance(body, dict) and body.get("success") is False,
                    f"{what}: HTTP {status} {str(body)[:200]}, expected 501 with success=false")
        c.check(h.key_list(c.site) == [b.STATIC_KEY], f"the key list is {h.key_list(c.site)}, expected only {b.STATIC_KEY}")
        c.check(b.key_state(b.key_describe(c.site, b.STATIC_KEY)) == "Enabled", "the configured key is no longer Enabled")
    with c.row("admin-configure"):
        # The single key is server configuration and is never stored with the cluster configuration.
        payload = {"backend_type": "Static", "key_id": "bb-static-other",
                   "secret_key": base64.b64encode(h.derived_key("static key in a refused request")).decode()}
        for what, (status, body) in {"configure": b.kms_configure(c.site, payload),
                                     "reconfigure": b.kms_reconfigure(c.site, payload)}.items():
            c.check(status == 400 and "RUSTFS_KMS_STATIC_SECRET_KEY" in str(body),
                    f"{what} as Static through the admin API: HTTP {status} {str(body)[:200]}, expected 400 naming the variable")
        status, body = b.kms_service_status(c.site)
        state = {k: body.get(k) for k in ("status", "backend_type")} if isinstance(body, dict) else body
        c.check(status == 200 and state == {"status": "Running", "backend_type": "Static"},
                f"service status after the refused configuration: HTTP {status} {state}")
        c.read_back(bucket, "kms", h.plain("ks-20 kms", SIZE), None, want, "SSE-KMS object after the refused configuration")
