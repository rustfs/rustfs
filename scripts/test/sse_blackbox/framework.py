"""Case registry, process configurations, and the context a case uses to state what it expects.

A case never asserts with a bare `assert`: `Ctx.check` collects every expectation that does not hold, so one run
reports all of them, and the message always names the evidence that decided it.
"""

from __future__ import annotations

import base64
import contextlib
from dataclasses import dataclass, field

import harness as h
import observe

PASS, FAIL, ENV_ERROR, SKIP = "pass", "fail", "env-error", "skip"
# A case whose only unmet expectations are known deviations, and one whose known deviation no longer reproduces.
KNOWN, FIXED = "known-deviation", "deviation-fixed"


# ---------------------------------------------------------------------------
# Process configurations: where the key that wraps data keys comes from
# ---------------------------------------------------------------------------

def _local_kms(site: h.Site) -> dict[str, str]:
    return {
        "RUSTFS_KMS_ENABLE": "true",
        "RUSTFS_KMS_BACKEND": "local",
        "RUSTFS_KMS_KEY_DIR": str(site.key_dir),
        "RUSTFS_KMS_DEFAULT_KEY_ID": h.DEFAULT_KEY,
        "RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS": "true",
        "RUSTFS_KMS_PROBE_INTERVAL_SECS": str(h.KMS_PROBE_INTERVAL),
    }


def master_key() -> str:
    return base64.b64encode(h.derived_key("local master key")).decode()


CONFIGS = {
    # No KMS and no local master key.
    "e0": lambda site: {},
    # No KMS, local master key loaded.
    "e1": lambda site: {"RUSTFS_SSE_S3_MASTER_KEY": master_key()},
    # No KMS, local master key set to a value that is not base64.
    "e1x-text": lambda site: {"RUSTFS_SSE_S3_MASTER_KEY": "zz#sse-blackbox-invalid-value#zz"},
    # No KMS, local master key that decodes to 16 bytes instead of 32.
    "e1x-short": lambda site: {"RUSTFS_SSE_S3_MASTER_KEY": base64.b64encode(h.derived_key("short", 16)).decode()},
    # Local KMS backend, per-key authorization off (the default).
    "e2": _local_kms,
    # Local KMS backend, per-key authorization enforced.
    "e2e": lambda site: {**_local_kms(site), "RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY": "true"},
}
KMS_CONFIGS = ("e2", "e2e")
CONFIG_ORDER = ("e0", "e1x-text", "e1x-short", "e1", "e2", "e2e")


@dataclass
class Case:
    case_id: str
    title: str
    level: str
    layer: str
    configs: tuple[str, ...]
    fn: object
    last: bool = False


CASES: list[Case] = []


def case(case_id: str, title: str, configs, level: str = "p0", layer: str = "L1", last: bool = False):
    """Register a case. `last` moves it to the end of its configuration, for cases that change process state."""
    def register(fn):
        CASES.append(Case(case_id, title, level, layer, tuple(configs), fn, last))
        return fn
    return register


# ---------------------------------------------------------------------------
# Case context
# ---------------------------------------------------------------------------

@dataclass
class Ctx:
    site: h.Site
    config: str
    case_id: str
    # False for a binary older than the `sse_local_master_key` startup event; the event is then recorded, not asserted.
    master_key_event: bool = True
    failures: list[str] = field(default_factory=list)
    known: list[str] = field(default_factory=list)
    fixed: list[str] = field(default_factory=list)
    rows: list[dict] = field(default_factory=list)
    _row: str = ""

    # -- expectations ------------------------------------------------------

    def known_deviation(self, holds: bool, expectation: str) -> None:
        """An expectation the product is known not to meet today.

        It stays written down as the intended behavior. While it does not hold the case reports a known deviation
        instead of a failure; once it holds the case says so, and the marker must then become a plain `check`.
        """
        (self.fixed if holds else self.known).append(f"[{self._row}] {expectation}" if self._row else expectation)

    def check(self, condition: bool, message: str) -> bool:
        if not condition:
            self.failures.append(f"[{self._row}] {message}" if self._row else message)
        return bool(condition)

    def need(self, condition: bool, message: str) -> None:
        if not condition:
            raise h.CheckFailed(message)

    @contextlib.contextmanager
    def row(self, name: str):
        """One input row of a table-driven case; its verdict is reported on its own."""
        before = len(self.failures)
        self._row = name
        observe.set_row(name)
        try:
            yield
        except h.CheckFailed as err:
            self.failures.append(f"[{name}] {err}")
        finally:
            self.rows.append({"row": name, "verdict": FAIL if len(self.failures) > before else PASS,
                              "reasons": self.failures[before:]})
            self._row = ""
            observe.set_row("")

    def ok(self, response, what: str, want: int = 200):
        """The exchange answered `want`; anything else ends the case, since later steps depend on it."""
        status, headers, body = response
        if status != want:
            code, message = h.s3_error(body)
            raise h.CheckFailed(f"{what}: HTTP {status} {code} {message[:200]!r}, expected {want}")
        return headers, body

    def refused(self, response, status: int, code: str, what: str, message_has: str | None = None) -> bool:
        """An explicit refusal: the exact status, an S3 error document, and the exact code."""
        got_status, _, body = response
        got_code, got_message = h.s3_error(body)
        held = self.check(got_status == status and got_code == code,
                          f"{what}: HTTP {got_status} {got_code!r} {got_message[:200]!r}, expected {status} {code!r}")
        if message_has is not None:
            held &= self.check(message_has in got_message,
                               f"{what}: message {got_message[:240]!r} does not contain {message_has!r}")
        return held

    def projection(self, headers: dict[str, str], want: dict[str, str], what: str) -> None:
        """The SSE headers present are exactly `want`: nothing missing and nothing extra."""
        got = h.projection(headers)
        self.check(got == want, f"{what}: SSE headers {got}, expected exactly {want}")

    def no_sse_headers(self, headers: dict[str, str], what: str) -> None:
        self.projection(headers, {}, what)

    def bytes_equal(self, headers: dict[str, str], body: bytes, data: bytes, what: str) -> None:
        self.check(h.TRUNCATED not in headers,
                   f"{what}: the server ended the body after {headers.get(h.TRUNCATED)} of {headers.get('content-length')} bytes")
        self.check(h.sha256_hex(body) == h.sha256_hex(data),
                   f"{what}: body sha256 {h.sha256_hex(body)[:16]} len {len(body)}, expected {h.sha256_hex(data)[:16]} len {len(data)}")
        self.check(headers.get("content-length") == str(len(data)),
                   f"{what}: Content-Length {headers.get('content-length')}, expected {len(data)}")

    def absent(self, bucket: str, key: str, what: str, headers=None) -> None:
        """A refused write left nothing behind."""
        status, _ = h.head(self.site, bucket, key, headers=headers, label="HeadObject(residue)")
        self.check(status == 404, f"{what}: HEAD after the refused write is {status}, expected 404 (nothing may be stored)")

    # -- common flows ------------------------------------------------------

    def bucket(self, suffix: str = "") -> str:
        name = f"bb-{self.case_id.lower()}" + (f"-{suffix}" if suffix else "")
        h.make_bucket(self.site, name)
        return name

    def stored(self, bucket: str, key: str) -> list[str] | None:
        """Record which encryption metadata keys the object carries on disk."""
        keys = h.stored_encryption_keys(self.site, bucket, key)
        observe.note("stored-keys", f"{bucket}/{key}", keys)
        return keys

    def roundtrip(self, bucket: str, key: str, data: bytes, write_headers: dict | None, read_headers: dict | None,
                  want: dict[str, str], what: str) -> None:
        """PUT, GET and HEAD one object: the projection holds on all three and the bytes come back unchanged."""
        put_headers, _ = self.ok(h.put(self.site, bucket, key, data, write_headers), f"{what} PUT")
        self.projection(put_headers, want, f"{what} PUT")
        get_headers, body = self.ok(h.get(self.site, bucket, key, read_headers), f"{what} GET")
        self.projection(get_headers, want, f"{what} GET")
        self.bytes_equal(get_headers, body, data, f"{what} GET")
        status, head_headers = h.head(self.site, bucket, key, read_headers)
        if self.check(status == 200, f"{what} HEAD: HTTP {status}, expected 200"):
            self.projection(head_headers, want, f"{what} HEAD")
            self.check(head_headers.get("content-length") == str(len(data)),
                       f"{what} HEAD: Content-Length {head_headers.get('content-length')}, expected {len(data)}")
        self.stored(bucket, key)

    def read_back(self, bucket: str, key: str, data: bytes, read_headers: dict | None, want: dict[str, str],
                  what: str) -> None:
        get_headers, body = self.ok(h.get(self.site, bucket, key, read_headers), f"{what} GET")
        self.projection(get_headers, want, f"{what} GET")
        self.bytes_equal(get_headers, body, data, f"{what} GET")

    def startup_event(self, event: str) -> list[dict]:
        records = h.log_events(self.site, event)
        fields = [{k: r.get(k) for k in ("level", "state", "enforced") if k in r} for r in records]
        observe.note("startup-event", event, fields)
        return records
