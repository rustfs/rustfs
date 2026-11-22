"""Stored form (layer 2): what an object looks like on the drives.

A case of the business layer sees only responses. This module reads the drives of the site under test instead:
`xl.meta` through the repository's format-aware dump tool, and the files next to it as raw bytes. It answers
three questions.

  - What is the stored form of an object? `form` reduces the metadata to something two binaries can be compared
    on: the key names, the exact value of every key whose value is deterministic, and the shape of every value
    that is random by design. `note_form` records it as a `stored-form` observation.
  - Is that the form this build is held to? `check_form` compares an object with an entry of the gold manifest
    of its build (`gold/<build>.json`) and names every key that differs.
  - Is something on the drives that must not be? `find_on_drives` searches every file for given bytes, and
    `unwrap_local_data_key` / `unseal_object_key` recover the keys an object was encrypted with from a key the
    harness itself configured, so a case can search for them too.

Key names are compared with their exact spelling: readers of other products do not fold case.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import diffing
import harness as h
import observe

GOLD_DIR = h.HERE / "gold"
DUMP_TOOL_ENV = "SSE_BB_DUMP_FILEINFO"
DUMP_TOOL_BUILD = "cargo build -p rustfs-filemeta --example dump_fileinfo"
DATA_DIR = "<data-dir>"

_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_HEX = re.compile(r"^(?:[0-9a-f]{2})+$")
_ETAG = re.compile(r"^[0-9a-f]{32}(?:-(\d+))?$")
# The zoned timestamp the KMS envelope carries; the zone name follows the machine, so it is not part of the shape.
_ZONED_TIME = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)\[[^\]]+\]$")


# ---------------------------------------------------------------------------
# Reading xl.meta through the dump tool
# ---------------------------------------------------------------------------

def dump_tool() -> Path:
    """The xl.meta reader: the path in `SSE_BB_DUMP_FILEINFO`, else the example built in this checkout."""
    override = os.environ.get(DUMP_TOOL_ENV)
    path = Path(override) if override else h.REPO_ROOT / "target" / "debug" / "examples" / "dump_fileinfo"
    if not path.is_file():
        raise h.EnvError(f"xl.meta dump tool missing at {path}; build it with `{DUMP_TOOL_BUILD}` "
                         f"or point {DUMP_TOOL_ENV} at an existing one")
    return path


_PART_LINE = re.compile(r"^part#(\d+): number=(\d+) size=(-?\d+) actual_size=(-?\d+) etag=(.*)$")
_META_LINE = re.compile(r"^meta\[(.+?)\]=(.*)$")
# Lines of the dump that carry nothing the stored form uses.
_IGNORED_LINE = re.compile(r"^(path:|etag:|parts:|part#\d+\.index|transition_|transitioned_obj:)")


def parse_dump(text: str) -> dict:
    """{'size', 'parts': [[number, stored size, logical size]], 'metadata': {key: value}} from the tool's output.

    A line the parser does not know is an error, not something to skip: a value that spans lines, or a new
    section of the dump, would otherwise drop out of the stored form without a trace.
    """
    info: dict = {"size": None, "parts": [], "metadata": {}}
    announced = None
    for line in text.splitlines():
        if line.startswith("size: "):
            info["size"] = int(line[6:])
        elif line.startswith("metadata entries: "):
            announced = int(line[18:])
        elif match := _PART_LINE.match(line):
            info["parts"].append([int(match.group(2)), int(match.group(3)), int(match.group(4))])
        elif match := _META_LINE.match(line):
            if match.group(1) in info["metadata"]:
                raise h.EnvError(f"xl.meta dump lists the metadata key {match.group(1)!r} twice")
            info["metadata"][match.group(1)] = match.group(2)
        elif not _IGNORED_LINE.match(line):
            raise h.EnvError(f"xl.meta dump has a line this reader does not know: {line[:120]!r}")
    if info["size"] is None or announced is None or announced != len(info["metadata"]):
        raise h.EnvError(f"xl.meta dump is incomplete: size {info['size']}, {announced} metadata entries announced, "
                         f"{len(info['metadata'])} read")
    return info


def dump(xl_meta: Path) -> dict:
    tool = dump_tool()
    try:
        done = subprocess.run([str(tool), str(xl_meta)], capture_output=True, text=True, timeout=60, check=False)
    except (OSError, subprocess.SubprocessError) as err:
        raise h.EnvError(f"xl.meta dump tool could not run: {err}") from err
    if done.returncode != 0:
        raise h.EnvError(f"xl.meta dump tool failed on {xl_meta.name} (exit {done.returncode}): {done.stderr.strip()[-300:]}")
    return parse_dump(done.stdout)


@dataclass
class DriveCopy:
    """One drive's copy of an object: its decoded xl.meta and the files next to it."""
    drive: str
    size: int
    parts: list
    metadata: dict
    # Relative file name -> size in bytes, with the data directory's random name replaced.
    files: dict = field(default_factory=dict)


@dataclass
class StoredObject:
    bucket: str
    key: str
    copies: list

    @property
    def first(self) -> DriveCopy:
        return self.copies[0]

    @property
    def metadata(self) -> dict:
        return self.first.metadata


def object_files(drive: Path, bucket: str, key: str) -> dict:
    """Files of one drive's copy. Hidden bookkeeping files of the upload are not part of the object."""
    root = drive / bucket / key
    files = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        parts = list(path.relative_to(root).parts)
        if len(parts) > 1 and _UUID.match(parts[0]):
            parts[0] = DATA_DIR
        files["/".join(parts)] = path.stat().st_size
    return files


def read_object(site: h.Site, bucket: str, key: str) -> StoredObject:
    """Every drive's copy of an object. An object no drive holds is a failed prerequisite of the case."""
    copies = []
    for drive in site.drives():
        xl_meta = drive / bucket / key / "xl.meta"
        if not xl_meta.is_file():
            continue
        info = dump(xl_meta)
        copies.append(DriveCopy(drive.name, info["size"], info["parts"], info["metadata"], object_files(drive, bucket, key)))
    if not copies:
        raise h.CheckFailed(f"{bucket}/{key}: no drive holds an xl.meta for it")
    return StoredObject(bucket, key, copies)


def drive_disagreements(obj: StoredObject, drives: int = 4) -> list[str]:
    """Where the drives' copies of one object differ. Every copy must describe the same object the same way.

    This compares raw values, random ones included: a nonce or a wrapped key that differs between two drives
    decrypts on one and fails on the other.
    """
    out = []
    if len(obj.copies) != drives:
        out.append(f"{len(obj.copies)} of {drives} drives hold an xl.meta ({', '.join(c.drive for c in obj.copies)})")
    first = obj.first
    for other in obj.copies[1:]:
        for name in sorted(set(first.metadata) | set(other.metadata)):
            if first.metadata.get(name) != other.metadata.get(name):
                out.append(f"metadata key {name} differs between {first.drive} and {other.drive}")
        if (first.size, first.parts) != (other.size, other.parts):
            out.append(f"sizes differ between {first.drive} and {other.drive}: {first.size} {first.parts} != {other.size} {other.parts}")
        if _stable_files(first.files) != _stable_files(other.files):
            out.append(f"files differ between {first.drive} and {other.drive}: {_stable_files(first.files)} != {_stable_files(other.files)}")
    return out


def _stable_files(files: dict) -> dict:
    """File names with the sizes that follow from the object alone; xl.meta's own length varies with random values."""
    return {name: (size if re.search(r"(^|/)part\.\d+$", name) else None) for name, size in files.items()}


# ---------------------------------------------------------------------------
# Shapes of random values
# ---------------------------------------------------------------------------

def _decode_base64(value: str) -> bytes | None:
    """Standard alphabet with padding, nothing else: a different variant must show up as a different shape."""
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None


def shape_bytes(value: str) -> str:
    """How a random byte string is written: `base64[n]`, `base64url[n]` or `hex[n]` with n decoded bytes.

    Hex is tried first, so base64 that happens to consist of hex digits alone would be misread. For the twelve
    bytes of a nonce that is one value in several billion, and padding rules it out for most other lengths.
    """
    if _HEX.match(value):
        return f"hex[{len(value) // 2}]"
    decoded = _decode_base64(value)
    if decoded is not None:
        return f"base64[{len(decoded)}]"
    try:
        return f"base64url[{len(base64.b64decode(value, altchars=b'-_', validate=True))}]"
    except (binascii.Error, ValueError):
        return f"text[{len(value)}]"


def shape_etag(value: str) -> str:
    match = _ETAG.match(value)
    if not match:
        return "other"
    return f"hex32-{match.group(1)}" if match.group(1) else "hex32"


def shape_uuid(value: str) -> str:
    return "uuid" if _UUID.match(value) else f"text[{len(value)}]"


def shape_hex(value: str) -> str:
    """Hex whose length follows the bucket and object names, so only the alphabet is the shape."""
    return "hex" if _HEX.match(value) else f"text[{len(value)}]"


def shape_base64(value: str) -> str:
    """Base64 whose decoded length is not part of the shape."""
    return "base64" if _decode_base64(value) is not None else f"text[{len(value)}]"


def _byte_array(value) -> str:
    if isinstance(value, list) and all(isinstance(b, int) and 0 <= b <= 255 for b in value):
        return f"bytes[{len(value)}]"
    return f"other:{type(value).__name__}"


def _text_shape(shaper):
    return lambda value: shaper(value) if isinstance(value, str) else f"other:{type(value).__name__}"


# Fields of a wrapped data key whose value is random or a point in time, and what remains of each as shape. A field
# not listed here is deterministic and kept verbatim: the key the data key is wrapped under, the key spec, the
# format version, the encryption context.
ENVELOPE_RANDOM_FIELDS = {
    "key_id": _text_shape(shape_uuid),        # identifier of this data key, generated per object
    "encrypted_key": _byte_array,             # the data key, wrapped
    "nonce": lambda v: _byte_array(v) if isinstance(v, list) else _text_shape(shape_bytes)(v),
    "ciphertext": _text_shape(shape_bytes),   # the data key, wrapped by the local master key
    "created_at": _text_shape(lambda v: "zoned-time" if _ZONED_TIME.match(v) else f"text[{len(v)}]"),
}


def envelope_of(value: str) -> dict | None:
    """The JSON object a wrapped data key decodes to, with its fields in stored order; None when it is not one."""
    decoded = _decode_base64(value)
    if decoded is None:
        return None
    try:
        envelope = json.loads(decoded)
    except ValueError:
        return None
    return envelope if isinstance(envelope, dict) else None


def envelope_kind(value: str) -> str:
    """Which provider's format a wrapped data key is in, judged by its fields alone."""
    envelope = envelope_of(value)
    if envelope is None:
        decoded = _decode_base64(value)
        return f"raw[{len(decoded)}]" if decoded is not None else "unreadable"
    fields = set(envelope)
    if fields == {"version", "nonce", "ciphertext"}:
        return "local-envelope"
    if {"key_id", "master_key_id", "key_spec", "encrypted_key", "nonce", "encryption_context", "created_at"} <= fields:
        return "kms-envelope"
    return "unknown-envelope"


def shape_wrapped_key(value: str) -> dict | str:
    """A wrapped data key: the envelope's field names in stored order, fixed fields verbatim, random ones as shape."""
    envelope = envelope_of(value)
    if envelope is None:
        return shape_bytes(value)
    fields = {name: {"shape": ENVELOPE_RANDOM_FIELDS[name](field_value)} if name in ENVELOPE_RANDOM_FIELDS
              else {"value": field_value} for name, field_value in envelope.items()}
    return {"encoding": "base64-json", "kind": envelope_kind(value), "order": list(envelope), "fields": fields}


def shape_sealed_key(value: str) -> dict | str:
    """A sealed object key: its length and the four header bytes that state version, cipher and payload size."""
    decoded = _decode_base64(value)
    if decoded is None or len(decoded) < 16:
        return shape_bytes(value)
    return {"encoding": "base64", "bytes": len(decoded), "header": decoded[:4].hex()}


def _both_prefixes(suffix: str, shaper) -> dict:
    return {f"x-rustfs-internal-{suffix}": shaper, f"x-minio-internal-{suffix}": shaper}


# Metadata keys whose value is random by design, and what remains of each as shape. This table is the one place
# that says "random": a key not listed here is recorded verbatim, so a new random value shows up as a difference
# between two runs instead of disappearing into a shape.
RANDOM_KEYS = {
    # MD5 of the stored bytes; for an encrypted object those are ciphertext.
    "etag": shape_etag,
    # Base nonce of the object, or of the multipart session.
    "x-rustfs-encryption-iv": shape_bytes,
    # The same nonce in a build that wraps the data key; the IV the object key is sealed under in a build that seals.
    "X-Minio-Internal-Server-Side-Encryption-Iv": shape_bytes,
    # Wrapped data key, in the envelope of the provider that issued it.
    "x-rustfs-encryption-key": shape_wrapped_key,
    "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": shape_wrapped_key,
    # Object key sealed under the customer key or under the data key.
    "X-Minio-Internal-Server-Side-Encryption-Sealed-Key": shape_sealed_key,
    "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": shape_sealed_key,
    "X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key": shape_sealed_key,
    # Bookkeeping of a completed multipart upload: the data directory, the upload id, the publication record.
    **_both_prefixes("encrypted-part-layout-quorum-v1", shape_uuid),
    **_both_prefixes("multipart-upload-id", shape_base64),
    **_both_prefixes("multipart-source-publication", shape_hex),
}


def shape_of(key: str, value: str):
    return RANDOM_KEYS[key](value)


# ---------------------------------------------------------------------------
# The stored form as an observation
# ---------------------------------------------------------------------------

def form(obj: StoredObject) -> dict:
    """What two binaries that store the same form agree on, byte for byte."""
    first = obj.first
    return {
        "drives": len(obj.copies),
        "drives_agree": not drive_disagreements(obj, len(obj.copies)),
        "files": _stable_files(first.files),
        "size": first.size,
        "parts": first.parts,
        "keys": sorted(first.metadata),
        "fixed": {k: v for k, v in sorted(first.metadata.items()) if k not in RANDOM_KEYS},
        "random": {k: shape_of(k, v) for k, v in sorted(first.metadata.items()) if k in RANDOM_KEYS},
    }


def note_form(bucket: str, key: str, value: dict) -> None:
    observe.note("stored-form", f"{bucket}/{key}", value)


def observe_form(site: h.Site, bucket: str, key: str) -> StoredObject:
    """Read an object from the drives and record its stored form."""
    obj = read_object(site, bucket, key)
    note_form(bucket, key, form(obj))
    return obj


def forms_on_drives(site: h.Site, bucket: str, keys: list[str]) -> dict[str, dict]:
    """The stored form of each object, for a driver outside a case. There an object no drive holds is the
    environment's fault: the step that should have stored it is setup, not what is being judged."""
    try:
        return {key: form(read_object(site, bucket, key)) for key in keys}
    except h.CheckFailed as err:
        raise h.EnvError(str(err)) from err


# ---------------------------------------------------------------------------
# Gold manifests
# ---------------------------------------------------------------------------

# Deterministic values that follow from the request rather than from the build. The manifest states them as a
# placeholder, and the case supplies what the placeholder stands for.
PARAMETER_KEYS = {
    "x-rustfs-internal-actual-size": "{size}",
    "x-minio-internal-actual-size": "{size}",
    "x-rustfs-encryption-original-size": "{size}",
    "x-amz-server-side-encryption-customer-original-size": "{size}",
    "x-amz-server-side-encryption-customer-key-md5": "{customer_key_md5}",
}


def object_params(bucket: str, key: str, parts: list[bytes], customer_key: bytes | None = None) -> dict:
    """What the placeholders of a manifest stand for, for one written object."""
    data = b"".join(parts)
    params = {"bucket": bucket, "key": key, "size": str(len(data)), "body_md5": hashlib.md5(data).hexdigest(),
              "plain_parts": [len(part) for part in parts]}
    if customer_key is not None:
        params["customer_key_md5"] = h.key_md5(customer_key)
    return params


def _fill(template, params: dict):
    """Replace placeholders in every string of a manifest value, keys of nested objects included."""
    if isinstance(template, str):
        for name, value in params.items():
            if isinstance(value, str):
                template = template.replace("{" + name + "}", value)
        return template
    if isinstance(template, list):
        return [_fill(item, params) for item in template]
    if isinstance(template, dict):
        return {_fill(name, params): _fill(value, params) for name, value in template.items()}
    return template


def _unfill(value, params: dict):
    """The inverse of `_fill` for the object's own names, used when proposing a manifest entry."""
    if isinstance(value, str):
        return value.replace(f"{params['bucket']}/{params['key']}", "{bucket}/{key}").replace(params["bucket"], "{bucket}")
    if isinstance(value, list):
        return [_unfill(item, params) for item in value]
    if isinstance(value, dict):
        return {_unfill(name, params): _unfill(item, params) for name, item in value.items()}
    return value


def propose_entry(obj: StoredObject, params: dict) -> dict:
    """The manifest entry that describes exactly this object. A proposal is reviewed by a person, never trusted."""
    keys = {}
    for name, value in sorted(obj.metadata.items()):
        if name == "etag" and value == params["body_md5"]:
            keys[name] = {"value": "{body_md5}"}
        elif name in RANDOM_KEYS:
            keys[name] = {"shape": _unfill(shape_of(name, value), params)}
        elif name in PARAMETER_KEYS and value == _fill(PARAMETER_KEYS[name], params):
            keys[name] = {"value": PARAMETER_KEYS[name]}
        else:
            keys[name] = {"value": _unfill(value, params)}
    return {"plain_parts": params["plain_parts"], "stored_size": obj.first.size, "stored_parts": obj.first.parts,
            "files": sorted(obj.first.files), "keys": keys}


def manifest_path(build: str) -> Path:
    return GOLD_DIR / f"{build}.json"


def load_manifest(build: str) -> dict:
    """The gold manifest of a build. A build without one cannot be checked, which is an environment error."""
    path = manifest_path(build)
    if not path.is_file():
        raise h.EnvError(f"no gold manifest for build {build!r} at {path.name}")
    manifest = json.loads(path.read_text())
    problems = lint_manifest(manifest)
    if problems:
        raise h.EnvError(f"gold manifest {path.name} is not well formed: {'; '.join(problems[:4])}")
    return manifest


def lint_manifest(manifest: dict) -> list[str]:
    """A manifest may call a value random only where `RANDOM_KEYS` does, so the two cannot drift apart."""
    problems = []
    for scheme, forms in manifest.get("forms", {}).items():
        for form_name, entry in forms.items():
            where = f"{scheme}/{form_name}"
            for required in ("plain_parts", "stored_size", "stored_parts", "files", "keys"):
                if required not in entry:
                    problems.append(f"{where}: no {required!r}")
            for name, expected in entry.get("keys", {}).items():
                if set(expected) not in ({"value"}, {"shape"}):
                    problems.append(f"{where}: {name} must state exactly one of 'value' and 'shape'")
                elif "shape" in expected and name not in RANDOM_KEYS:
                    problems.append(f"{where}: {name} is given a shape but is not a random key")
                elif "value" in expected and name in RANDOM_KEYS and name != "etag":
                    problems.append(f"{where}: {name} is a random key but is given an exact value")
    if not manifest.get("forms"):
        problems.append("no forms")
    return problems


def manifest_entry(manifest: dict, scheme: str, form_name: str) -> dict:
    try:
        return manifest["forms"][scheme][form_name]
    except KeyError:
        raise h.CheckFailed(f"the gold manifest of build {manifest.get('build')!r} has no entry for {scheme}/{form_name}") from None


def check_form(obj: StoredObject, entry: dict, params: dict, sizes: bool = True) -> list[str]:
    """Every way a stored object differs from a manifest entry; an empty list means it has exactly that form.

    Key names must match exactly: an extra key is reported as well as a missing one. `sizes=False` leaves out the
    stored sizes, for an object whose plaintext is not the one the entry was written from.
    """
    out = []
    metadata, expected_keys = obj.metadata, entry["keys"]
    for name in sorted(set(expected_keys) - set(metadata)):
        out.append(f"key missing: {name}")
    for name in sorted(set(metadata) - set(expected_keys)):
        out.append(f"key not in the manifest: {name}")
    for name in sorted(set(metadata) & set(expected_keys)):
        expected, value = expected_keys[name], metadata[name]
        if "value" in expected:
            want = _fill(expected["value"], params)
            if value != want:
                out.append(f"value of {name} is {value!r}, the manifest says {want!r}")
        else:
            want, got = _fill(expected["shape"], params), shape_of(name, value)
            if got != want:
                out.append(f"shape of {name} is {json.dumps(got, sort_keys=True)}, the manifest says {json.dumps(want, sort_keys=True)}")
    if sorted(obj.first.files) != entry["files"]:
        out.append(f"files are {sorted(obj.first.files)}, the manifest says {entry['files']}")
    if sizes:
        if params["plain_parts"] != entry["plain_parts"]:
            out.append(f"the manifest entry describes plaintext parts {entry['plain_parts']}, the object was written from {params['plain_parts']}")
        elif (obj.first.size, obj.first.parts) != (entry["stored_size"], entry["stored_parts"]):
            out.append(f"stored size {obj.first.size} with parts {obj.first.parts}, the manifest says "
                       f"{entry['stored_size']} with parts {entry['stored_parts']}")
    return out


# ---------------------------------------------------------------------------
# Searching the drives
# ---------------------------------------------------------------------------

def encodings(secret: bytes) -> dict[str, bytes]:
    """The ways a secret could have been written down: raw, base64 in both alphabets, and hex in both cases."""
    return {
        "raw": secret,
        "base64": base64.b64encode(secret),
        "base64url": base64.urlsafe_b64encode(secret),
        "hex": secret.hex().encode(),
        "HEX": secret.hex().upper().encode(),
    }


def find_on_drives(site: h.Site, needles: dict[str, bytes], under: str = "") -> dict[str, list[str]]:
    """{needle name: files that contain it}, over every file below `under` on every drive of the site."""
    hits: dict[str, list[str]] = {name: [] for name in needles}
    for drive in site.drives():
        # `os.walk` passes over a directory the server removes meanwhile; its trash and staging areas change under us.
        for directory, _, names in sorted(os.walk(drive / under if under else drive)):
            for path in (Path(directory) / name for name in sorted(names)):
                try:
                    data = path.read_bytes()
                except OSError:
                    continue  # a temporary file that is already gone
                for name, needle in needles.items():
                    if needle in data:
                        hits[name].append(str(path.relative_to(site.volume_dir)))
    return hits


def stored_bytes(site: h.Site, bucket: str, key: str) -> int:
    """Bytes the drives hold for an object, metadata included."""
    return sum(p.stat().st_size for drive in site.drives() for p in (drive / bucket / key).rglob("*") if p.is_file())


SAMPLE = 32


def plaintext_samples(parts: list[bytes]) -> dict[str, bytes]:
    """Short windows of the plaintext, named `part N <region> @<offset>`: the start, the inside and the end of every part.

    Erasure coding spreads a block over the drives, so a window that crosses from one drive's share into the
    next is in no file even when the object is stored as plaintext. Each region therefore has several windows,
    and `regions_without_hit` says which regions a search could not see at all.
    """
    samples = {}
    for number, part in enumerate(parts, start=1):
        size = len(part)
        regions = {"start": (0, 64), "inside": (size // 3, size // 3 + 4096),
                   "end": (size - SAMPLE, size - SAMPLE - 64, size - SAMPLE - 4096)}
        for region, offsets in regions.items():
            for offset in offsets:
                if 0 <= offset <= size - SAMPLE:
                    samples[f"part {number} {region} @{offset}"] = part[offset:offset + SAMPLE]
    return samples


def regions_without_hit(hits: dict[str, list[str]]) -> list[str]:
    """Regions of `plaintext_samples` in which no window was found."""
    found: dict[str, bool] = {}
    for name, files in hits.items():
        region = name.partition(" @")[0]
        found[region] = found.get(region, False) or bool(files)
    return sorted(region for region, hit in found.items() if not hit)


# ---------------------------------------------------------------------------
# Recovering keys with a key the harness configured
# ---------------------------------------------------------------------------
# AES-256-GCM decryption in the standard library's terms, for 48-byte payloads only. It exists so a case can
# open the wrapped key it finds on a drive and then prove that the key inside is not stored anywhere in the clear.

def _xtime(value: int) -> int:
    return ((value << 1) ^ (0x1B if value & 0x80 else 0)) & 0xFF


def _aes_sbox() -> list[int]:
    # The powers of 3 enumerate the field, which gives every inverse; the affine map turns inverses into the S-box.
    exp, log, value = [0] * 255, [0] * 256, 1
    for i in range(255):
        exp[i], log[value] = value, i
        value ^= _xtime(value)
    sbox = [0x63] * 256
    for byte in range(1, 256):
        inverse = exp[(255 - log[byte]) % 255]
        result = inverse
        for shift in range(1, 5):
            result ^= ((inverse << shift) | (inverse >> (8 - shift))) & 0xFF
        sbox[byte] = result ^ 0x63
    return sbox


_SBOX = _aes_sbox()


def _aes256_round_keys(key: bytes) -> list[bytes]:
    words = [key[i:i + 4] for i in range(0, 32, 4)]
    rcon = 1
    for i in range(8, 60):
        word = words[i - 1]
        if i % 8 == 0:
            word = bytes(_SBOX[b] for b in word[1:] + word[:1])
            word = bytes([word[0] ^ rcon]) + word[1:]
            rcon = _xtime(rcon)
        elif i % 8 == 4:
            word = bytes(_SBOX[b] for b in word)
        words.append(bytes(a ^ b for a, b in zip(words[i - 8], word, strict=True)))
    return [b"".join(words[i:i + 4]) for i in range(0, 60, 4)]


def _aes256_encrypt_block(round_keys: list[bytes], block: bytes) -> bytes:
    state = [a ^ b for a, b in zip(block, round_keys[0], strict=True)]
    for round_number in range(1, 15):
        state = [_SBOX[b] for b in state]
        state = [state[(i + 4 * (i % 4)) % 16] for i in range(16)]  # ShiftRows on column-major state
        if round_number != 14:
            mixed = []
            for column in range(0, 16, 4):
                a = state[column:column + 4]
                total = a[0] ^ a[1] ^ a[2] ^ a[3]
                mixed.extend(a[i] ^ total ^ _xtime(a[i] ^ a[(i + 1) % 4]) for i in range(4))
            state = mixed
        state = [a ^ b for a, b in zip(state, round_keys[round_number], strict=True)]
    return bytes(state)


def _ghash_multiply(x: int, y: int) -> int:
    product = 0
    for bit in range(127, -1, -1):
        if (y >> bit) & 1:
            product ^= x
        x = (x >> 1) ^ (0xE1 << 120 if x & 1 else 0)
    return product


def aes256_gcm_open(key: bytes, nonce: bytes, sealed: bytes, aad: bytes = b"") -> bytes | None:
    """Decrypt `ciphertext || 16-byte tag`. None when the tag does not verify, so a wrong key is never mistaken for data."""
    if len(key) != 32 or len(nonce) != 12 or len(sealed) < 16:
        return None
    round_keys = _aes256_round_keys(key)
    ciphertext, tag = sealed[:-16], sealed[-16:]
    hash_key = int.from_bytes(_aes256_encrypt_block(round_keys, bytes(16)), "big")

    def padded(data: bytes) -> bytes:
        return data + bytes(-len(data) % 16)

    digest = 0
    authenticated = padded(aad) + padded(ciphertext) + (len(aad) * 8).to_bytes(8, "big") + (len(ciphertext) * 8).to_bytes(8, "big")
    for offset in range(0, len(authenticated), 16):
        digest = _ghash_multiply(digest ^ int.from_bytes(authenticated[offset:offset + 16], "big"), hash_key)
    mask = _aes256_encrypt_block(round_keys, nonce + (1).to_bytes(4, "big"))
    if not hmac.compare_digest(bytes(a ^ b for a, b in zip(digest.to_bytes(16, "big"), mask, strict=True)), tag):
        return None
    plaintext = bytearray()
    for index in range(0, len(ciphertext), 16):
        stream = _aes256_encrypt_block(round_keys, nonce + (2 + index // 16).to_bytes(4, "big"))
        plaintext.extend(a ^ b for a, b in zip(ciphertext[index:index + 16], stream, strict=False))
    return bytes(plaintext)


def unwrap_local_data_key(wrapped: str, master_key: bytes) -> bytes | None:
    """The data key inside a wrapped key written under the local master key; None unless it authenticates."""
    envelope = envelope_of(wrapped)
    if envelope is None or envelope_kind(wrapped) != "local-envelope":
        return None
    nonce, ciphertext = _decode_base64(str(envelope["nonce"])), _decode_base64(str(envelope["ciphertext"]))
    if nonce is None or ciphertext is None:
        return None
    return aes256_gcm_open(master_key, nonce, ciphertext)


SEAL_ALGORITHM = "DAREv2-HMAC-SHA256"


def unseal_object_key(sealed_key: str, seal_iv: str, external_key: bytes, domain: str, bucket: str, key: str) -> bytes | None:
    """The object key inside a sealed key, opened with the key it was sealed under; None unless it authenticates.

    `domain` is `SSE-C`, `SSE-S3` or `SSE-KMS`; the sealing key is bound to it and to the object's path.
    """
    sealed, iv = _decode_base64(sealed_key), _decode_base64(seal_iv)
    if sealed is None or iv is None or len(sealed) != 64 or len(iv) != 32:
        return None
    sealing_key = hmac.new(external_key, iv + domain.encode() + SEAL_ALGORITHM.encode() + f"{bucket}/{key}".encode(),
                           hashlib.sha256).digest()
    return aes256_gcm_open(sealing_key, sealed[4:16], sealed[16:], aad=sealed[:4])


# ---------------------------------------------------------------------------
# Assembling a manifest from the proposals a run wrote
# ---------------------------------------------------------------------------

PROPOSAL_DIR = "gold-proposal"


def write_proposal(run_dir: Path | None, config: str, scheme: str, form_name: str, entry: dict) -> None:
    """Keep the entry that would describe what this run stored, next to the run's other files."""
    if run_dir is None:
        return
    path = run_dir / PROPOSAL_DIR / f"{config}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    proposals = json.loads(path.read_text()) if path.exists() else {}
    proposals.setdefault(scheme, {})[form_name] = entry
    path.write_text(json.dumps(proposals, indent=1, sort_keys=True) + "\n")


def assemble_manifest(run_dir: Path) -> tuple[dict, list[str]]:
    """(manifest, conflicts) from a run's proposals. Two configurations that store one scheme differently conflict."""
    meta = json.loads((run_dir / "run.json").read_text())
    binary = meta["binary"]
    forms: dict = {}
    conflicts = []
    for path in sorted((run_dir / PROPOSAL_DIR).glob("*.json")):
        for scheme, entries in json.loads(path.read_text()).items():
            for form_name, entry in entries.items():
                known = forms.setdefault(scheme, {}).setdefault(form_name, entry)
                if known != entry:
                    conflicts.append(f"{scheme}/{form_name}: configuration {path.stem} stores another form than an earlier one")
    manifest = {"build": binary["build"], "written_from": {"binary": binary["name"], "commit": binary["commit"]},
                "forms": forms}
    return manifest, conflicts


# ---------------------------------------------------------------------------
# Offline checks of this module's own judgment
# ---------------------------------------------------------------------------

def _random(seed: str, label: str, size: int) -> bytes:
    return hashlib.sha512(f"stored selftest {seed} {label}".encode()).digest()[:size]


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def _sample_metadata(build: str, seed: str) -> dict:
    """Metadata of an inlined SSE-S3 object under the local master key, as each build stores it.

    `seed` drives every random value and nothing else, so two seeds give two objects of one stored form.
    """
    envelope = _b64(json.dumps({"version": 1, "nonce": _b64(_random(seed, "wrap nonce", 12)),
                                "ciphertext": _b64(_random(seed, "wrapped", 48))}).encode())
    common = {
        "content-type": "application/octet-stream", "etag": _random(seed, "etag", 16).hex(),
        "x-amz-server-side-encryption": "AES256", "x-rustfs-encryption-key-id": "default",
        "x-rustfs-encryption-original-size": "4096",
        **_both_prefixes("actual-size", "4096"), **_both_prefixes("inline-data", "true"),
        **_both_prefixes("server-side-encryption-opaque-etag", "v1"),
    }
    if build == "default":
        return {**common, "x-rustfs-encryption-algorithm": "AES256", "x-rustfs-encryption-key": envelope,
                "x-rustfs-encryption-iv": _b64(_random(seed, "nonce", 12))}
    sealed = bytes.fromhex("20001f00") + _random(seed, "sealed", 60)
    return {**common, "X-Minio-Internal-Server-Side-Encryption-Seal-Algorithm": SEAL_ALGORITHM,
            "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Key-Id": "default",
            "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key": envelope,
            "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key": _b64(sealed),
            "X-Minio-Internal-Server-Side-Encryption-Iv": _b64(_random(seed, "seal iv", 32))}


def _sample_object(metadata: dict, drives: int = 4) -> StoredObject:
    copies = [DriveCopy(f"test{n}", 4234, [[1, 4234, 4096]], dict(metadata), {"xl.meta": 900 + n}) for n in range(1, drives + 1)]
    return StoredObject("bucket", "key", copies)


_SAMPLE_PARAMS = {"bucket": "bucket", "key": "key", "size": "4096", "body_md5": "0" * 32, "plain_parts": [4096]}
_SAMPLE_DUMP = """path: /drive/bucket/key/xl.meta
size: 4234
etag: Some("00")
parts: 1
part#0: number=1 size=4234 actual_size=4096 etag=00
part#0.index: none
metadata entries: 2
meta[content-type]=application/octet-stream
meta[x-rustfs-encryption-iv]=AAAAAAAAAAAAAAAA
"""


def _rejects(text: str) -> bool:
    try:
        parse_dump(text)
    except h.EnvError:
        return True
    return False


def _selftest_reading() -> list[str]:
    failures = []
    parsed = parse_dump(_SAMPLE_DUMP)
    if parsed != {"size": 4234, "parts": [[1, 4234, 4096]],
                  "metadata": {"content-type": "application/octet-stream", "x-rustfs-encryption-iv": "AAAAAAAAAAAAAAAA"}}:
        failures.append(f"dump reader: a well-formed dump is read as {parsed}")
    if not _rejects(_SAMPLE_DUMP + "a line of an unknown section\n"):
        failures.append("dump reader: a line it does not know is skipped silently")
    if not _rejects(_SAMPLE_DUMP.replace("metadata entries: 2", "metadata entries: 3")):
        failures.append("dump reader: fewer metadata entries than announced are accepted")
    obj = _sample_object(_sample_metadata("default", "a"))
    if drive_disagreements(obj):
        failures.append(f"drive comparison: identical copies disagree: {drive_disagreements(obj)}")
    obj.copies[2].metadata["x-rustfs-encryption-iv"] = _b64(_random("b", "nonce", 12))
    if not any("x-rustfs-encryption-iv" in line for line in drive_disagreements(obj)):
        failures.append("drive comparison: a nonce that differs on one drive is not reported")
    if not drive_disagreements(_sample_object(_sample_metadata("default", "a"), drives=3)):
        failures.append("drive comparison: a drive without a copy is not reported")
    return failures


def _selftest_form() -> list[str]:
    failures = []
    for build in ("default", "rio-v2"):
        metadata = _sample_metadata(build, "a")
        base = form(_sample_object(metadata))
        if form(_sample_object(_sample_metadata(build, "b"))) != base:
            failures.append(f"stored form ({build}): other random values of the same form change the observation")
        leaked = [key for key in metadata if key in RANDOM_KEYS and metadata[key] in json.dumps(base)]
        if leaked or set(base["random"]) != set(metadata) & set(RANDOM_KEYS):
            failures.append(f"stored form ({build}): random values are recorded verbatim: {leaked}")
    metadata = _sample_metadata("default", "a")
    base = form(_sample_object(metadata))
    envelope = json.loads(base64.b64decode(metadata["x-rustfs-encryption-key"]))
    variants = {
        "a changed fixed value": {**metadata, "x-rustfs-encryption-algorithm": "AES128"},
        "a renamed key": {("x-rustfs-encryption-nonce" if k == "x-rustfs-encryption-iv" else k): v for k, v in metadata.items()},
        "a key name in another case": {k.upper() if k == "x-rustfs-encryption-key-id" else k: v for k, v in metadata.items()},
        "a nonce of another length": {**metadata, "x-rustfs-encryption-iv": _b64(_random("a", "nonce", 16))},
        "a nonce in the URL-safe alphabet": {**metadata, "x-rustfs-encryption-iv": base64.urlsafe_b64encode(b"\xfb\xff" * 6).decode()},
        "a wrapped key with its fields in another order": {**metadata, "x-rustfs-encryption-key": _b64(json.dumps(
            dict(reversed(envelope.items()))).encode())},
        "a wrapped key with another format version": {**metadata, "x-rustfs-encryption-key": _b64(json.dumps(
            {**envelope, "version": 2}).encode())},
        "a data key stored without a wrapper": {**metadata, "x-rustfs-encryption-key": _b64(_random("a", "raw", 32))},
    }
    for name, changed in variants.items():
        if form(_sample_object(changed)) == base:
            failures.append(f"stored form: {name} does not change the observation")
    return failures


def _selftest_manifest() -> list[str]:
    failures = []
    obj = _sample_object(_sample_metadata("default", "a"))
    entry = propose_entry(obj, _SAMPLE_PARAMS)
    other = _sample_object(_sample_metadata("default", "b"))
    if check_form(other, entry, _SAMPLE_PARAMS):
        failures.append(f"manifest check: another object of the same form is refused: {check_form(other, entry, _SAMPLE_PARAMS)}")
    fixed = json.loads(json.dumps(entry))
    fixed["keys"]["x-amz-server-side-encryption"] = {"value": "aws:kms"}
    if not any("value of x-amz-server-side-encryption" in line for line in check_form(obj, fixed, _SAMPLE_PARAMS)):
        failures.append("manifest check: a manifest with another fixed value is satisfied")
    renamed = json.loads(json.dumps(entry))
    renamed["keys"]["x-rustfs-encryption-nonce"] = renamed["keys"].pop("x-rustfs-encryption-iv")
    differences = check_form(obj, renamed, _SAMPLE_PARAMS)
    if "key missing: x-rustfs-encryption-nonce" not in differences or "key not in the manifest: x-rustfs-encryption-iv" not in differences:
        failures.append(f"manifest check: a manifest with another key name is satisfied or does not name both keys: {differences}")
    resized = {**entry, "stored_size": entry["stored_size"] + 16}
    if not check_form(obj, resized, _SAMPLE_PARAMS) or check_form(obj, resized, _SAMPLE_PARAMS, sizes=False):
        failures.append("manifest check: another stored size is accepted, or is reported when sizes are left out")
    if not lint_manifest({"build": "x", "forms": {"s": {"f": {**entry, "keys": {"content-type": {"shape": "text"}}}}}}):
        failures.append("manifest lint: a shape for a key that is not random is accepted")
    # The manifests in the repository: each accepts its own build's form and names what differs in the other's.
    slots = {"default": "x-rustfs-encryption-key", "rio-v2": "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key"}
    for build, other_build in (("default", "rio-v2"), ("rio-v2", "default")):
        try:
            gold = manifest_entry(load_manifest(build), "sse-s3@local", "inline")
        except (h.EnvError, h.CheckFailed) as err:
            failures.append(f"gold manifest of build {build}: {err}")
            continue
        own = check_form(_sample_object(_sample_metadata(build, "a")), gold, _SAMPLE_PARAMS, sizes=False)
        if own:
            failures.append(f"gold manifest of build {build}: refuses the form of its own build: {own}")
        cross = check_form(_sample_object(_sample_metadata(other_build, "a")), gold, _SAMPLE_PARAMS, sizes=False)
        if f"key missing: {slots[build]}" not in cross or f"key not in the manifest: {slots[other_build]}" not in cross:
            failures.append(f"gold manifest of build {build}: accepts the form of build {other_build} or does not name its keys: {cross}")
    return failures


def _selftest_search() -> list[str]:
    failures = []
    plaintext = [h.plain("stored selftest part 1", 9000), h.plain("stored selftest part 2", 20000)]
    samples = plaintext_samples(plaintext)
    with tempfile.TemporaryDirectory() as root:
        site = h.Site(name="selftest", root=Path(root))
        for number, drive in enumerate(site.drives()):
            (drive / "bucket" / "object").mkdir(parents=True)
            (drive / "bucket" / "object" / "part.1").write_bytes(_random("cipher", str(number), 64) * 200)
        if regions_without_hit(find_on_drives(site, samples)) != sorted({name.partition(" @")[0] for name in samples}):
            failures.append("plaintext search: samples are found in files that do not contain them")
        # Each drive holds one half of every part, as erasure coding lays a block out: the window that starts
        # in one half and ends in the other is in no file, and its region still counts as seen.
        for number, part in enumerate(plaintext, start=1):
            (site.drives()[0] / "bucket" / "object" / f"plain.{number}").write_bytes(part[:len(part) // 3 + 16])
            (site.drives()[1] / "bucket" / "object" / f"plain.{number}").write_bytes(part[len(part) // 3 + 16:])
        hits = find_on_drives(site, samples)
        if regions_without_hit(hits):
            failures.append(f"plaintext search: a plaintext object on the drives is not found in {regions_without_hit(hits)}")
        if all(hits.values()):
            failures.append("plaintext search: the window split between two drives was expected to be in no file")
        secret = bytes([0xFB, 0xFF]) * 16  # its two base64 spellings differ in every character group
        (site.drives()[3] / "bucket" / "object" / "xl.meta").write_bytes(b"\x00meta" + base64.urlsafe_b64encode(secret) + b"\x00")
        found = [name for name, files in find_on_drives(site, encodings(secret)).items() if files]
        if found != ["base64url"]:
            failures.append(f"secret search: a key written in the URL-safe alphabet is found as {found}")
    return failures


def _selftest_keys() -> list[str]:
    failures = []
    # Known answers for an all-zero key and nonce: an empty message, and one block of zeros.
    tag_only = bytes.fromhex("530f8afbc74536b9a963b4f1c4cb738b")
    one_block = bytes.fromhex("cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919")
    if aes256_gcm_open(bytes(32), bytes(12), tag_only) != b"" or aes256_gcm_open(bytes(32), bytes(12), one_block) != bytes(16):
        failures.append("AES-256-GCM: a known answer does not open")
    tampered = one_block[:-1] + bytes([one_block[-1] ^ 1])
    if aes256_gcm_open(bytes(32), bytes(12), tampered) is not None or aes256_gcm_open(bytes(32), bytes(12), one_block, b"x") is not None:
        failures.append("AES-256-GCM: a wrong tag or other associated data opens")
    if aes256_gcm_open(bytes([1]) + bytes(31), bytes(12), one_block) is not None:
        failures.append("AES-256-GCM: another key opens")
    return failures


def _selftest_diff() -> list[str]:
    def run(value: dict) -> dict:
        note = {"config": "e1", "case": "SF-02", "seq": 0, "row": "", "kind": "stored-form", "name": "bucket/key", "value": value}
        result = {"config": "e1", "case": "SF-02", "row": "", "verdict": "pass"}
        return {"dir": None, "meta": {"binary": {}}, "results": [result], "observations": [note]}

    failures = []
    base = form(_sample_object(_sample_metadata("default", "a")))
    if diffing.compare(run(base), run(form(_sample_object(_sample_metadata("default", "b")))))["blocking"]:
        failures.append("diff: two objects of one stored form are reported as different")
    changed = form(_sample_object({**_sample_metadata("default", "a"), "x-rustfs-encryption-algorithm": "AES128"}))
    blocking = diffing.compare(run(base), run(changed))["blocking"]
    if len(blocking) != 1 or "value.fixed.x-rustfs-encryption-algorithm" not in blocking[0]:
        failures.append(f"diff: a changed fixed value in a stored form is not reported by its key: {blocking}")
    other_build = diffing.compare(run(base), run(form(_sample_object(_sample_metadata("rio-v2", "a")))))["blocking"]
    if len(other_build) != 1 or "value.keys" not in other_build[0]:
        failures.append("diff: the stored forms of two builds are not reported as different")
    return failures


def selftest() -> list[str]:
    return [*_selftest_reading(), *_selftest_form(), *_selftest_manifest(), *_selftest_search(), *_selftest_keys(),
            *_selftest_diff()]


def render_manifest(manifest: dict) -> str:
    """A manifest as JSON with one metadata key per line, so a change to it reads as a change to those keys."""
    def compact(value) -> str:
        return json.dumps(value, sort_keys=True)

    lines = ["{", f' "build": {compact(manifest["build"])},', f' "written_from": {compact(manifest["written_from"])},',
             ' "forms": {']
    schemes = sorted(manifest["forms"])
    for scheme in schemes:
        lines.append(f"  {compact(scheme)}: {{")
        forms = manifest["forms"][scheme]
        for form_name in forms:
            entry = forms[form_name]
            lines.append(f"   {compact(form_name)}: {{")
            lines.extend(f"    {compact(name)}: {compact(entry[name])}," for name in ("plain_parts", "stored_size", "stored_parts", "files"))
            lines.append('    "keys": {')
            keys = sorted(entry["keys"])
            lines.extend(f"     {compact(name)}: {compact(entry['keys'][name])}{',' if name != keys[-1] else ''}" for name in keys)
            lines.append("    }")
            lines.append("   }" + ("," if form_name != list(forms)[-1] else ""))
        lines.append("  }" + ("," if scheme != schemes[-1] else ""))
    lines.extend([" }", "}"])
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    """`stored.py propose <run dir>` prints the manifest that describes what a finished run stored."""
    if len(argv) != 3 or argv[1] != "propose":
        print(main.__doc__)
        return 2
    manifest, conflicts = assemble_manifest(Path(argv[2]))
    for conflict in conflicts:
        print(f"conflict: {conflict}", file=sys.stderr)
    print(render_manifest(manifest), end="")
    return 1 if conflicts else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
