"""Observation vectors: everything a client can see in one exchange, normalized so two runs compare field by field.

Each exchange inside a case becomes one record keyed by (config, case, seq). Two binaries driven by the same case
issue the same request sequence, so equal keys denote the same request and any field that differs is a behavior
difference. Values that are random by design (version ids, upload ids, ETags of encrypted objects) are reduced to
their shape.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import re
import xml.etree.ElementTree as ET

_records: list[dict] = []
_context: dict | None = None
_quiet = 0

# Response headers whose values are deterministic for a deterministic request and therefore compared verbatim.
_VERBATIM = ("content-range", "content-type", "x-amz-mp-parts-count", "x-amz-storage-class", "x-amz-delete-marker")
_HEX32 = re.compile(r'^"?[0-9a-f]{32}"?$')
_HEX32_PARTS = re.compile(r'^"?[0-9a-f]{32}-(\d+)"?$')
_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_LONG_TOKEN = re.compile(r"[A-Za-z0-9+/_=-]{40,}")
_RANDOM_QUERY = ("uploadId", "versionId")
# Headers that leak storage internals if they ever reach a client.
_INTERNAL_PREFIXES = ("x-rustfs-encryption-", "x-rustfs-internal-", "x-minio-internal-")
_INTERNAL_EXACT = ("x-amz-server-side-encryption-customer-key",)


def begin_case(config: str, case: str) -> None:
    global _context
    _context = {"config": config, "case": case, "seq": 0, "row": ""}


def end_case() -> None:
    global _context
    _context = None


def in_case() -> bool:
    return _context is not None and _quiet == 0


def set_row(row: str) -> None:
    if _context is not None:
        _context["row"] = row


@contextlib.contextmanager
def quiet():
    """Suspend recording around polling loops, whose iteration count is timing and not behavior."""
    global _quiet
    _quiet += 1
    try:
        yield
    finally:
        _quiet -= 1


def drain() -> list[dict]:
    out = list(_records)
    _records.clear()
    return out


def _etag_shape(value: str | None) -> str | None:
    if value is None:
        return None
    if _HEX32.match(value):
        return "hex32"
    parts = _HEX32_PARTS.match(value)
    if parts:
        return f"hex32-{parts.group(1)}"
    return "other"


def normalize_message(message: str) -> str:
    """Strip the parts of an error message that are random per request, keeping the wording."""
    return _LONG_TOKEN.sub("<token>", _UUID.sub("<uuid>", message))


def _query_shape(query: str) -> str:
    parts = []
    for pair in query.split("&") if query else []:
        name = pair.partition("=")[0]
        parts.append(f"{name}=<id>" if name in _RANDOM_QUERY else pair)
    return "&".join(parts)


def _body_shape(status: int, method: str, headers: dict[str, str], body: bytes) -> dict:
    """What the body says, without its random parts."""
    if not body:
        return {"kind": "empty"}
    # An object body is opaque even when its first byte happens to look like a document.
    is_object = method == "GET" and headers.get("content-type", "").startswith("application/octet-stream")
    text_start = b"" if is_object else body[:64].lstrip()
    if text_start.startswith(b"<"):
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return {"kind": "xml-unparsed", "length": len(body)}
        tag = root.tag.rpartition("}")[2]
        if tag == "Error":
            code = root.findtext("{*}Code") or root.findtext("Code") or ""
            message = root.findtext("{*}Message") or root.findtext("Message") or ""
            return {"kind": "s3-error", "code": code, "message": normalize_message(message)}
        return {"kind": "xml", "root": tag, "elements": sorted({el.tag.rpartition("}")[2] for el in root.iter()})}
    if text_start.startswith((b"{", b"[")):
        try:
            parsed = json.loads(body)
        except ValueError:  # not JSON, or not text at all
            parsed = None
        if isinstance(parsed, dict):
            shape = {"kind": "json", "keys": sorted(parsed)}
            for name in ("success", "status", "code", "error", "message", "backend_type", "backend_status"):
                if isinstance(parsed.get(name), (str, bool, int)) or parsed.get(name) is None and name in parsed:
                    value = parsed.get(name)
                    shape[name] = normalize_message(value) if isinstance(value, str) else value
            return shape
        if parsed is not None:
            return {"kind": "json", "type": type(parsed).__name__}
    # Object bodies: the plaintext is seed-determined, so the hash must match across runs.
    return {"kind": "bytes", "length": len(body), "sha256": hashlib.sha256(body).hexdigest()}


def exchange(label: str | None, method: str, path: str, query: str, status: int, headers: dict[str, str],
             body: bytes) -> None:
    if not in_case():
        return
    _context["seq"] += 1
    sse = {k: v for k, v in headers.items() if k.startswith("x-amz-server-side-encryption")}
    internal = sorted(k for k in headers if k.startswith(_INTERNAL_PREFIXES) or k in _INTERNAL_EXACT)
    record = {
        "config": _context["config"],
        "case": _context["case"],
        "seq": _context["seq"],
        "row": _context["row"],
        "kind": "exchange",
        "op": label or method,
        "request": f"{method} {path}" + (f"?{_query_shape(query)}" if query else ""),
        "status": status,
        "sse": dict(sorted(sse.items())),
        "headers": {name: headers[name] for name in _VERBATIM if name in headers},
        "header_names": sorted(headers),
        "etag": _etag_shape(headers.get("etag")),
        "version_id": "present" if "x-amz-version-id" in headers else None,
        "internal_headers": internal,
        "body": _body_shape(status, method, headers, body),
    }
    # A HEAD response states the object length without sending a body, and object lengths are deterministic.
    # Documents (errors, listings) carry request ids and timestamps, so their length is not recorded.
    if method == "HEAD" or (method == "GET" and status in (200, 206) and record["body"]["kind"] in ("bytes", "empty")):
        record["content_length"] = headers.get("content-length")
    if "x-harness-body-truncated" in headers:
        record["truncated_after"] = headers["x-harness-body-truncated"]
        record["header_names"].remove("x-harness-body-truncated")
    _records.append(record)


def note(kind: str, name: str, value) -> None:
    """Record a non-HTTP observation: a startup event, a stored key set, an admin fact."""
    if _context is None:
        return
    _records.append({"config": _context["config"], "case": _context["case"], "seq": 0, "row": _context["row"],
                     "kind": kind, "name": name, "value": value})
