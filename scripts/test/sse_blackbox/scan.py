"""Canaries and leak scanners: every secret a run uses is registered here, and nothing the product says may repeat it.

A canary is a secret by its raw bytes. It is searched in three text spellings (standard base64, URL-safe base64,
hex) and as the raw bytes, and each spelling is also searched by its first characters only, so a partial echo is
reported too. The registry lives in memory: the harness never writes a canary to disk, and a report names a canary
by its kind and a short digest, never by its value.

The one exemption is closed: the plaintext data key inside the response of the data-key generation endpoint that
returned it.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

import harness as h

# The admin endpoint whose response legitimately carries a plaintext data key.
DATA_KEY_ENDPOINT = "/kms/generate-data-key"
# How many leading characters of a spelling count as a partial leak.
FRAGMENT = 12
# A text secret shorter than this would match by accident; it is recorded as unscannable instead.
MIN_TEXT_SECRET = 12
MIN_KEY_BYTES = 16
# Characters a leaked token may continue with; an excerpt masks them together with the part that matched.
_TOKEN_TAIL = re.compile(rb"[A-Za-z0-9+/_=-]*")
_CHUNK = 4 * 1024 * 1024

KIND_SSEC = "sse-c-key"
KIND_MASTER_KEY = "local-master-key"
KIND_INVALID_MASTER_KEY = "invalid-master-key"
KIND_KEY_FILE = "kms-key-file"
KIND_DATA_KEY = "data-key"
KIND_ENV_SECRET = "env-secret"


@dataclass
class Canary:
    kind: str
    label: str
    digest: str
    # spelling name -> the full form searched for
    forms: dict[str, bytes]
    # (spelling name, bytes to search for, whether it is only a fragment of the full form)
    probes: list[tuple[str, bytes, bool]] = field(default_factory=list)
    # The archived exchange whose response legitimately carries this value (a generated data key).
    origin: int | None = None

    def name(self) -> str:
        return f"{self.kind} canary {self.digest} ({self.label})"


@dataclass
class Hit:
    canary: Canary
    spelling: str
    partial: bool
    offset: int

    def describe(self) -> str:
        how = f"first {FRAGMENT} characters of its {self.spelling} spelling" if self.partial else f"{self.spelling} spelling"
        return f"{self.canary.name()}, {how}"


def _unpadded(text: bytes) -> bytes:
    return text.rstrip(b"=")


def decode_base64(text: str) -> bytes | None:
    """The bytes `text` stands for when it is standard or URL-safe base64, else None."""
    cleaned = text.strip()
    if not cleaned or len(cleaned) % 4 == 1:
        return None
    padded = cleaned + "=" * (-len(cleaned) % 4)
    for altchars in (None, b"-_"):
        try:
            return base64.b64decode(padded, altchars=altchars, validate=True)
        except (binascii.Error, ValueError):
            continue
    return None


class Registry:
    """The secrets of one run."""

    def __init__(self) -> None:
        self._by_value: dict[bytes, Canary] = {}
        # Secrets that could not be registered, by kind and reason: a report must be able to say what it did not scan for.
        self.unscannable: list[str] = []

    def __len__(self) -> int:
        return len(self._by_value)

    def canaries(self) -> list[Canary]:
        return list(self._by_value.values())

    def kinds(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for canary in self._by_value.values():
            out[canary.kind] = out.get(canary.kind, 0) + 1
        return dict(sorted(out.items()))

    def _add(self, kind: str, label: str, value: bytes, forms: dict[str, bytes], fragments: tuple[int, ...],
             origin: int | None) -> Canary:
        known = self._by_value.get(value)
        if known is not None:
            return known
        canary = Canary(kind, label, hashlib.sha256(value).hexdigest()[:8], forms, origin=origin)
        for spelling, form in forms.items():
            for size in fragments:
                probe = form[:size] if size > 0 else form[size:]
                if len(probe) >= min(8, len(form)) and (spelling, probe) not in [(s, p) for s, p, _ in canary.probes]:
                    canary.probes.append((spelling, probe, len(probe) < len(form)))
        self._by_value[value] = canary
        return canary

    def register_bytes(self, kind: str, label: str, raw: bytes, origin: int | None = None) -> Canary | None:
        """Register key material by its raw bytes."""
        if len(raw) < MIN_KEY_BYTES:
            self.unscannable.append(f"{kind} ({label}): {len(raw)} bytes are too few to search for")
            return None
        forms = {
            "base64": _unpadded(base64.b64encode(raw)),
            "base64url": _unpadded(base64.urlsafe_b64encode(raw)),
            "hex": raw.hex().encode(),
            "hex (upper case)": raw.hex().upper().encode(),
            "raw bytes": raw,
        }
        if forms["base64url"] == forms["base64"]:
            # Without `+` or `/` the two alphabets spell the key the same way; one finding is one leak.
            del forms["base64url"]
        return self._add(kind, label, raw, forms, (FRAGMENT,), origin)

    def register_text(self, kind: str, label: str, text: str, origin: int | None = None,
                      fragments: tuple[int, ...] = (FRAGMENT,)) -> list[Canary]:
        """Register a secret given as text: the text itself and, when it is base64 of key material, those bytes.

        `fragments` lists the pieces of the text that count as a partial leak: a positive number is that many leading
        characters, a negative one that many trailing characters.
        """
        out = []
        literal = text.strip().encode()
        raw = decode_base64(text)
        if raw is not None and len(raw) >= MIN_KEY_BYTES:
            canary = self.register_bytes(kind, label, raw, origin)
            if canary is not None:
                out.append(canary)
        if len(literal) < MIN_TEXT_SECRET:
            if not out:
                self.unscannable.append(f"{kind} ({label}): a {len(literal)}-character value would match by accident")
            return out
        if raw is None or len(raw) < MIN_KEY_BYTES or fragments != (FRAGMENT,):
            out.append(self._add(kind, label, literal, {"literal": literal}, fragments, origin))
        return out

    def register_key_files(self, key_dir: Path) -> int:
        """Register the key material of every Local-backend key file below `key_dir`; returns how many files it read."""
        count = 0
        for path in sorted(key_dir.glob("*.key")) if key_dir.is_dir() else []:
            try:
                stored = json.loads(path.read_text())
            except (OSError, ValueError):
                self.unscannable.append(f"{KIND_KEY_FILE} ({path.name}): the file is not readable JSON")
                continue
            material = stored.get("encrypted_key_material") if isinstance(stored, dict) else None
            if isinstance(material, str) and material:
                self.register_text(KIND_KEY_FILE, f"{key_dir.parent.name}/{path.name}", material)
                count += 1
        return count

    # -- scanning ----------------------------------------------------------

    def scan(self, data: bytes) -> list[Hit]:
        """The first occurrence of each canary spelling in `data`.

        A spelling found in full hides the fragments of the same canary: two spellings often share their first
        characters, and one leak must not read as two.
        """
        hits = []
        for canary in self._by_value.values():
            found: dict[str, Hit] = {}
            for spelling, probe, is_fragment in canary.probes:
                offset = data.find(probe)
                if offset < 0:
                    continue
                form = canary.forms[spelling]
                start = offset if form.startswith(probe) else offset - (len(form) - len(probe))
                full = start >= 0 and data[start:start + len(form)] == form
                partial = is_fragment and not full
                previous = found.get(spelling)
                if previous is None or (previous.partial and not partial):
                    found[spelling] = Hit(canary, spelling, partial, offset)
            if any(not hit.partial for hit in found.values()):
                found = {spelling: hit for spelling, hit in found.items() if not hit.partial}
            hits.extend(found.values())
        return hits

    def mask(self, data: bytes) -> bytes:
        """`data` with every canary occurrence, and whatever token it continues into, replaced by a marker."""
        for canary in self._by_value.values():
            marker = f"<canary {canary.kind} {canary.digest}>".encode()
            for _, probe, _ in sorted(canary.probes, key=lambda item: -len(item[1])):
                start = data.find(probe)
                while start >= 0:
                    end = start + len(probe)
                    end += len(_TOKEN_TAIL.match(data, end).group(0))
                    data = data[:start] + marker + data[end:]
                    start = data.find(probe, start + len(marker))
        return data

    def mask_text(self, text: str) -> str:
        return self.mask(text.encode("latin-1", errors="replace")).decode("latin-1")

    def scan_file(self, path: Path) -> list[tuple[int, Hit, str]]:
        """(line number, hit, masked excerpt of the line) for every line of `path` that carries a canary."""
        out = []
        line_base = 1
        with path.open("rb") as handle:
            while True:
                lines = handle.readlines(_CHUNK)
                if not lines:
                    break
                # A block of whole lines is searched at once; only a block with a hit is searched line by line.
                if self.scan(b"".join(lines)):
                    for number, line in enumerate(lines):
                        for hit in self.scan(line):
                            excerpt = self.mask(line.rstrip(b"\r\n"))[:300].decode(errors="replace")
                            out.append((line_base + number, hit, excerpt))
                line_base += len(lines)
        return out


REGISTRY = Registry()

# Response headers that expose how an object is stored, carry encryption state between replication peers, or hand
# a customer key back.
INTERNAL_PREFIXES = ("x-rustfs-encryption-", "x-rustfs-internal-", "x-minio-internal-", "x-rustfs-replication-",
                     "x-minio-replication-")
INTERNAL_EXACT = ("x-amz-server-side-encryption-customer-key", "x-amz-copy-source-server-side-encryption-customer-key")
# Everything a 500 may say. The first two are the generic text in its two spellings; the third is the one diagnosis
# an operator needs and names nothing of the deployment.
INTERNAL_ERROR_MESSAGES = frozenset({
    "We encountered an internal error, please try again.",
    "We encountered an internal error. Please try again.",
    ("The object's data key envelope could not be unwrapped by the configured KMS backend: the key material under "
     "this key id differs from the one that wrapped it, or the envelope is damaged"),
})


def internal_headers(names) -> list[str]:
    """The response header names that expose how an object is stored."""
    return sorted(name for name in names if name.startswith(INTERNAL_PREFIXES) or name in INTERNAL_EXACT)


def error_message(body: bytes) -> str | None:
    """The message of an error response, S3 document or admin JSON; None when the body is neither."""
    code, message = h.s3_error(body)
    if code or message:
        return message
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    if isinstance(parsed, dict):
        for name in ("message", "error", "Message"):
            if isinstance(parsed.get(name), str):
                return parsed[name]
    return None


def internal_error_problem(body: bytes) -> str | None:
    """Why the body of a 500 says more than it may, or None when it is one of the fixed texts."""
    message = error_message(body)
    if message is None:
        return "the body is not an error document"
    if message not in INTERNAL_ERROR_MESSAGES:
        return f"message {message[:200]!r} is not one of the fixed texts"
    return None


def is_exempt(hit: Hit, exchange_number: int, path: str, where: str) -> bool:
    """The only accepted occurrence: a generated data key in the body of the response that returned it."""
    return (hit.canary.kind == KIND_DATA_KEY and hit.canary.origin == exchange_number and where == "body"
            and path.endswith(DATA_KEY_ENDPOINT))


def self_check(work_dir: Path) -> str | None:
    """Prove the scanner can report: plant a throwaway canary in a file and find it. Returns what went wrong, or None.

    Without this a scanner that stopped matching would read as a clean run.
    """
    registry = Registry()
    # A value whose standard and URL-safe base64 spellings differ, so each of the three is planted and found.
    secret = next(value for value in (hashlib.sha256(f"scanner self-check {i}".encode()).digest() for i in range(200))
                  if base64.b64encode(value) != base64.urlsafe_b64encode(value))
    canary = registry.register_bytes("self-check", "planted", secret)
    planted = work_dir / "scanner-self-check.tmp"
    try:
        work_dir.mkdir(parents=True, exist_ok=True)
        for spelling in ("base64", "base64url", "hex"):
            planted.write_bytes(b"first line\nvalue=" + canary.forms[spelling] + b" trailing\n")
            found = registry.scan_file(planted)
            reported = [(line, hit.partial) for line, hit, _ in found if hit.spelling == spelling]
            if reported != [(2, False)]:
                return f"a planted canary in its {spelling} spelling was not reported on its line"
            if any(canary.forms[spelling].decode() in excerpt for _, _, excerpt in found):
                return "the excerpt of a finding repeats the canary"
        planted.write_bytes(b"first line\nnothing to see\n")
        if registry.scan_file(planted):
            return "a file without a canary was reported"
    finally:
        planted.unlink(missing_ok=True)
    return None
