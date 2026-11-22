"""Stored form (SF): what an encrypted object looks like on the drives.

These cases read the drives of the site under test, so they belong to layer 2: a failure here means the stored
form changed, which a person has to confirm as intended before the gold manifest follows. They never read an
object back through the API; whether stored objects are readable is the business layer's question.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import harness as h
import stored
from framework import Ctx, case

KEY_A = h.derived_key("ssec a")
KEY_B = h.derived_key("ssec b")
# One pair on purpose: the product serializes the stored context from a hash map, so the order of two pairs would
# vary from one write to the next and the stored string would stop being a fixed value.
CONTEXT = {"department": "stored-form"}

# Plaintext part sizes of the forms the gold manifests describe: no data, data inlined into xl.meta, one part
# file spanning several erasure blocks, and a multipart upload.
FORMS = {
    "empty": [0],
    "inline": [32 * h.KIB],
    "single": [3 * h.MIB + 1],
    "multipart": [5 * h.MIB, h.MIB + 17],
}
# Further sizes on both sides of the inline threshold, each with the form it is stored in.
STANDARD_SIZES = ((1, "inline"), (70 * h.KIB, "inline"), (200_000, "inline"), (h.MIB, "single"))

# Metadata keys that hold a wrapped data key or a sealed object key, in the build that writes each.
WRAPPED_KEY_SLOT = {"default": "x-rustfs-encryption-key",
                    "rio-v2": "X-Minio-Internal-Server-Side-Encryption-S3-Kms-Sealed-Key"}
SEAL_IV = "X-Minio-Internal-Server-Side-Encryption-Iv"
SEALED_KEY_SLOT = {"sse-c": "X-Minio-Internal-Server-Side-Encryption-Sealed-Key",
                   "sse-s3": "X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key",
                   "sse-kms": "X-Minio-Internal-Server-Side-Encryption-Kms-Sealed-Key"}
MULTIPART_MARKER = "X-Minio-Internal-Encrypted-Multipart"
PUBLIC_KEY_ID = "x-amz-server-side-encryption-aws-kms-key-id"
STORED_CONTEXT = "x-rustfs-encryption-context"


@dataclass(frozen=True)
class Scheme:
    # Entry of the gold manifest: the scheme, and for SSE-S3 where the key that wraps the data key comes from.
    name: str
    headers: dict | None
    # SSE headers a response for the object carries.
    want: dict
    customer_key: bytes | None = None

    @property
    def family(self) -> str:
        return self.name.split("@")[0].split("+")[0]

    @property
    def read_headers(self) -> dict | None:
        return self.headers if self.customer_key else None


PLAIN = Scheme("none", None, {})
SSE_C = Scheme("sse-c", h.ssec(KEY_A), h.projection_c(KEY_A), KEY_A)
SSE_S3_LOCAL = Scheme("sse-s3@local", h.sse_s3(), h.projection_s3())
SSE_S3_KMS = Scheme("sse-s3@kms", h.sse_s3(), h.projection_s3())
SSE_KMS = Scheme("sse-kms", h.sse_kms(h.KEY_1), h.projection_kms(h.KEY_1))
SSE_KMS_CONTEXT = Scheme("sse-kms+context", h.sse_kms(h.KEY_1, CONTEXT), h.projection_kms(h.KEY_1))
# What each configuration can encrypt with. SSE-C needs no key source, so it is everywhere.
SCHEMES = {
    "e0": (SSE_C,),
    "e1": (SSE_C, SSE_S3_LOCAL),
    "e2": (SSE_C, SSE_S3_KMS, SSE_KMS),
}
# The key source a configuration wraps data keys with, as the wrapped key's own fields show it.
ENVELOPE = {"e1": "local-envelope", "e2": "kms-envelope"}


def _parts(case_id: str, name: str, sizes: list[int]) -> list[bytes]:
    return [h.plain(f"{case_id} {name} part {number}", size) for number, size in enumerate(sizes, start=1)]


def _store(c: Ctx, bucket: str, key: str, parts: list[bytes], scheme: Scheme) -> None:
    """Store one object: a single part through PutObject, several through a multipart upload."""
    if len(parts) == 1:
        c.ok(h.put(c.site, bucket, key, parts[0], scheme.headers), f"{key} PUT")
        return
    status, _, upload_id, body = h.create_multipart(c.site, bucket, key, scheme.headers)
    c.need(status == 200 and upload_id, f"{key} CreateMultipartUpload: HTTP {status} {h.s3_error(body)}")
    etags = []
    for number, data in enumerate(parts, start=1):
        response, _ = c.ok(h.upload_part(c.site, bucket, key, upload_id, number, data, scheme.read_headers),
                           f"{key} UploadPart {number}")
        etags.append((number, response["etag"]))
    c.ok(h.complete_multipart(c.site, bucket, key, upload_id, etags, scheme.read_headers), f"{key} CompleteMultipartUpload")


def _params(bucket: str, key: str, parts: list[bytes], scheme: Scheme) -> dict:
    return stored.object_params(bucket, key, parts, scheme.customer_key)


def _agree(c: Ctx, obj: stored.StoredObject, what: str) -> None:
    disagreements = stored.drive_disagreements(obj)
    c.check(not disagreements, f"{what}: the drives do not hold the same copy: {'; '.join(disagreements[:6])}")


def _no_plaintext_on_drives(c: Ctx, parts: list[bytes]) -> None:
    """No file on any drive contains a sample of this plaintext."""
    found = {name: files for name, files in stored.find_on_drives(c.site, stored.plaintext_samples(parts)).items() if files}
    c.check(not found, "plaintext is on the drives: " + "; ".join(f"{name} in {files[:2]}" for name, files in found.items()))


def _absent_from_drives(c: Ctx, secret: bytes, what: str) -> None:
    """No file on any drive contains `secret` in any of the ways it could have been written down."""
    hits = {name: files for name, files in stored.find_on_drives(c.site, stored.encodings(secret)).items() if files}
    c.check(not hits, f"{what} is on the drives: " + "; ".join(f"as {name} in {files[:2]}" for name, files in hits.items()))


# ---------------------------------------------------------------------------
# SF-01: the bytes on the drives are ciphertext
# ---------------------------------------------------------------------------

@case("SF-01", "No drive holds the plaintext of an encrypted object", ["e1", "e2"], level="p0", layer="L2")
def sf_01(c: Ctx) -> None:
    bucket = c.bucket()
    for form_name in ("inline", "single", "multipart"):
        sizes = FORMS[form_name]
        # The same search has to find a plaintext object of the same sizes. Without that, "not found" could mean
        # that the search looks in the wrong place or that erasure coding splits every sample.
        control = _parts(c.case_id, f"control {form_name}", sizes)
        searchable = False
        with c.row(f"control-{form_name}"):
            _store(c, bucket, f"plain-{form_name}", control, PLAIN)
            missed = stored.regions_without_hit(stored.find_on_drives(c.site, stored.plaintext_samples(control)))
            searchable = c.check(not missed, f"the search finds no sample of a plaintext object in {missed}; "
                                 "it cannot show that an encrypted object is ciphertext")
        for scheme in SCHEMES[c.config]:
            with c.row(f"{scheme.family}-{form_name}"):
                key = f"{scheme.family}-{form_name}"
                parts = _parts(c.case_id, key, sizes)
                _store(c, bucket, key, parts, scheme)
                status, head_headers = h.head(c.site, bucket, key, scheme.read_headers)
                # The object answers as encrypted, and what is stored is at least as large as what was sent ...
                if c.check(status == 200, f"HEAD: HTTP {status}, expected 200"):
                    c.projection(head_headers, scheme.want, "HEAD")
                on_drives = stored.stored_bytes(c.site, bucket, key)
                c.check(on_drives >= sum(sizes), f"the drives hold {on_drives} bytes for an object of {sum(sizes)}")
                # ... yet no sample of the plaintext is anywhere on the drives, in the object's files or outside.
                c.need(searchable, "the plaintext control of this form was not found, so this row proves nothing")
                _no_plaintext_on_drives(c, parts)


# ---------------------------------------------------------------------------
# SF-02: key names, fixed values and shapes against the gold manifest
# ---------------------------------------------------------------------------

def _manifest(c: Ctx) -> tuple[dict | None, h.EnvError | None]:
    """The gold manifest of the build under test, or the reason there is none.

    A case goes on without one so that it still records what the build stores and writes the proposal a person
    turns into the manifest; it then ends as an environment error, never as a pass.
    """
    try:
        return stored.load_manifest(c.build), None
    except h.EnvError as err:
        return None, err


def _held_to_manifest(c: Ctx, manifest: dict | None, obj: stored.StoredObject, scheme: Scheme, form_name: str,
                      params: dict, sizes: bool = True) -> None:
    if manifest is None:
        return
    differences = stored.check_form(obj, stored.manifest_entry(manifest, scheme.name, form_name), params, sizes=sizes)
    c.check(not differences, f"stored form is not the gold form {scheme.name}/{form_name} of build {c.build}: "
            + "; ".join(differences))


@case("SF-02", "Every scheme stores exactly the form the gold manifest of the build describes", ["e0", "e1", "e2"],
      level="p1", layer="L2")
def sf_02(c: Ctx) -> None:
    bucket = c.bucket()
    manifest, missing = _manifest(c)
    schemes = (PLAIN, *SCHEMES[c.config], *((SSE_KMS_CONTEXT,) if c.config == "e2" else ()))
    plain_inline = None
    for scheme in schemes:
        for form_name, sizes in FORMS.items():
            with c.row(f"{scheme.name}-{form_name}"):
                key = f"{scheme.name.replace('@', '-').replace('+', '-')}-{form_name}"
                parts = _parts(c.case_id, key, sizes)
                _store(c, bucket, key, parts, scheme)
                obj = stored.observe_form(c.site, bucket, key)
                params = _params(bucket, key, parts, scheme)
                stored.write_proposal(h.RUN_DIR, c.config, scheme.name, form_name, stored.propose_entry(obj, params))
                _agree(c, obj, key)
                _held_to_manifest(c, manifest, obj, scheme, form_name, params)
                if scheme is PLAIN and form_name == "inline":
                    plain_inline = (obj, params)
    with c.row("plaintext-is-not-an-encrypted-form"):
        # The comparison must be able to say no: an unencrypted object is not the SSE-C form.
        c.need(plain_inline is not None, "the plaintext object was not stored")
        if manifest is not None:
            plain_obj, plain_params = plain_inline
            differences = stored.check_form(plain_obj, stored.manifest_entry(manifest, "sse-c", "inline"), plain_params)
            c.check("key missing: x-amz-server-side-encryption" in differences,
                    f"a plaintext object compared with the SSE-C form is not reported as lacking the scheme: {differences[:4]}")
    if missing is not None:
        raise h.EnvError(f"{missing}; this run wrote what the build stores to {stored.PROPOSAL_DIR}/{c.config}.json "
                         "in its run directory, see `stored.py propose`")


# ---------------------------------------------------------------------------
# SF-03: no key in the clear
# ---------------------------------------------------------------------------

@case("SF-03", "Neither the customer key nor a data key or object key is stored in the clear", ["e0", "e1"],
      level="p1", layer="L2")
def sf_03(c: Ctx) -> None:
    bucket = c.bucket()
    sealing = c.build == "rio-v2"
    for form_name in ("inline", "multipart"):
        with c.row(f"sse-c-{form_name}"):
            key = f"sse-c-{form_name}"
            _store(c, bucket, key, _parts(c.case_id, key, FORMS[form_name]), SSE_C)
            obj = stored.read_object(c.site, bucket, key)
            md5 = h.key_md5(KEY_A)
            c.check(obj.metadata.get("x-amz-server-side-encryption-customer-key-md5") == md5,
                    "the stored metadata does not carry the MD5 of the customer key")
            # The search reads the object's metadata: it finds the key's MD5 on every drive ...
            found = stored.find_on_drives(c.site, {"md5": md5.encode()}, under=f"{bucket}/{key}")["md5"]
            in_metadata = [name for name in found if name.endswith("/xl.meta")]
            c.need(len(in_metadata) == len(c.site.drives()), f"the search finds the key MD5 in {len(in_metadata)} xl.meta "
                   "files, expected one per drive; it cannot show that the key itself is absent")
            # ... and the key itself on none.
            _absent_from_drives(c, KEY_A, "the customer key")
            slot = SEALED_KEY_SLOT["sse-c"]
            c.check((slot in obj.metadata) == sealing,
                    f"build {c.build}: sealed-key slot {'missing' if sealing else 'present'} on an SSE-C object")
            if sealing and slot in obj.metadata:
                seal = (obj.metadata[slot], obj.metadata.get(SEAL_IV, ""))
                object_key = stored.unseal_object_key(*seal, KEY_A, "SSE-C", bucket, key)
                if c.check(object_key is not None, "the sealed key does not open with the customer key, the SSE-C domain "
                           "and the object's path"):
                    _absent_from_drives(c, object_key, "the object key")
                c.check(stored.unseal_object_key(*seal, KEY_B, "SSE-C", bucket, key) is None,
                        "the sealed key opens with another customer key")
                c.check(stored.unseal_object_key(*seal, KEY_A, "SSE-C", bucket, f"{key}-elsewhere") is None,
                        "the sealed key opens for another object's path")
    if c.config != "e1":
        return
    # The local master key is one the harness configured, so the wrapped data key can be opened here and the
    # key inside searched for. A KMS backend keeps its keys to itself; its data keys cannot be opened from outside.
    master_key = h.derived_key("local master key")
    for form_name in ("inline", "multipart"):
        with c.row(f"sse-s3-{form_name}"):
            key = f"sse-s3-{form_name}"
            _store(c, bucket, key, _parts(c.case_id, key, FORMS[form_name]), SSE_S3_LOCAL)
            obj = stored.read_object(c.site, bucket, key)
            wrapped = obj.metadata.get(WRAPPED_KEY_SLOT[c.build])
            c.need(wrapped is not None, f"no wrapped data key under {WRAPPED_KEY_SLOT[c.build]}")
            data_key = stored.unwrap_local_data_key(wrapped, master_key)
            c.need(data_key is not None and len(data_key) == 32,
                   "the wrapped data key does not open with the configured master key")
            c.check(stored.unwrap_local_data_key(wrapped, KEY_B) is None, "the wrapped data key opens with another key")
            _absent_from_drives(c, data_key, "the data key")
            _absent_from_drives(c, master_key, "the local master key")
            if sealing:
                seal = (obj.metadata.get(SEALED_KEY_SLOT["sse-s3"], ""), obj.metadata.get(SEAL_IV, ""))
                object_key = stored.unseal_object_key(*seal, data_key, "SSE-S3", bucket, key)
                if c.check(object_key is not None, "the sealed key does not open with the data key, the SSE-S3 domain "
                           "and the object's path"):
                    _absent_from_drives(c, object_key, "the object key")
                c.check(stored.unseal_object_key(*seal, data_key, "SSE-KMS", bucket, key) is None,
                        "the sealed key opens under another scheme's domain")


# ---------------------------------------------------------------------------
# SF-04, SF-05, SF-07, SF-09: single properties of the stored form
# ---------------------------------------------------------------------------

@case("SF-04", "The wrapped data key is in the format of the key source that issued it", ["e1", "e2"], level="p1", layer="L2")
def sf_04(c: Ctx) -> None:
    bucket = c.bucket()
    slot = WRAPPED_KEY_SLOT[c.build]
    for scheme in SCHEMES[c.config][1:]:
        for form_name in ("inline", "multipart"):
            with c.row(f"{scheme.family}-{form_name}"):
                key = f"{scheme.family}-{form_name}"
                _store(c, bucket, key, _parts(c.case_id, key, FORMS[form_name]), scheme)
                obj = stored.read_object(c.site, bucket, key)
                # Every key that can hold a wrapped data key, whichever build is expected to write it.
                wrapped = {name: stored.envelope_kind(obj.metadata[name]) for name in WRAPPED_KEY_SLOT.values()
                           if name in obj.metadata}
                c.check(wrapped == {slot: ENVELOPE[c.config]},
                        f"wrapped data keys by format: {wrapped}, expected exactly {{{slot!r}: {ENVELOPE[c.config]!r}}}")


@case("SF-05", "The stored record names the KMS key and the context only where the request did", ["e2"], level="p1", layer="L2")
def sf_05(c: Ctx) -> None:
    bucket = c.bucket()
    data = h.plain("sf-05", FORMS["inline"][0])
    rows = {"sse-kms": (SSE_KMS, h.KEY_1, None), "sse-kms-context": (SSE_KMS_CONTEXT, h.KEY_1, CONTEXT),
            "sse-s3": (SSE_S3_KMS, None, None)}
    for name, (scheme, key_id, context) in rows.items():
        with c.row(name):
            put_headers, _ = c.ok(h.put(c.site, bucket, name, data, scheme.headers), "PUT")
            obj = stored.read_object(c.site, bucket, name)
            if key_id is None:
                c.check(PUBLIC_KEY_ID not in obj.metadata,
                        f"an SSE-S3 object stores the public key id {obj.metadata.get(PUBLIC_KEY_ID)!r}")
            else:
                c.check(obj.metadata.get(PUBLIC_KEY_ID) == put_headers.get(h.SSE_KEY_ID) == key_id,
                        f"stored key id {obj.metadata.get(PUBLIC_KEY_ID)!r}, response key id {put_headers.get(h.SSE_KEY_ID)!r}, "
                        f"expected both to be {key_id!r}")
            recorded = obj.metadata.get(STORED_CONTEXT)
            if context is None:
                c.check(recorded is None, f"a request without a context stored one: {recorded!r}")
            else:
                c.check(recorded is not None and json.loads(recorded) == context,
                        f"stored context {recorded!r}, expected the request's {context}")


@case("SF-07", "A multipart object records its parts, and the multipart marker where the build needs it", ["e0", "e2"],
      level="p1", layer="L2")
def sf_07(c: Ctx) -> None:
    bucket = c.bucket()
    sizes = FORMS["multipart"]
    for scheme in SCHEMES[c.config]:
        with c.row(scheme.family):
            key = scheme.family
            _store(c, bucket, key, _parts(c.case_id, key, sizes), scheme)
            obj = stored.read_object(c.site, bucket, key)
            first = obj.first
            c.check([p[0] for p in first.parts] == [1, 2] and [p[2] for p in first.parts] == sizes,
                    f"recorded parts {first.parts} as [number, stored size, logical size], expected numbers 1 and 2 "
                    f"with logical sizes {sizes}")
            c.check(sum(p[1] for p in first.parts) == first.size and all(p[1] > p[2] for p in first.parts),
                    f"stored sizes {[p[1] for p in first.parts]} do not add up to the object's {first.size} or do not "
                    "exceed the plaintext, as ciphertext does")
            c.check(first.metadata.get("x-rustfs-encryption-original-size") == str(sum(sizes)),
                    f"recorded plaintext size {first.metadata.get('x-rustfs-encryption-original-size')!r}, expected {sum(sizes)}")
            # A build that seals an object key reads a multipart object part by part only when it is marked.
            marked = MULTIPART_MARKER in first.metadata
            c.check(marked == (c.build == "rio-v2"),
                    f"build {c.build}: the multipart marker is {'present' if marked else 'missing'}")
    with c.row("single-part-is-not-marked"):
        _store(c, bucket, "single", _parts(c.case_id, "single", FORMS["inline"]), SCHEMES[c.config][0])
        single = stored.read_object(c.site, bucket, "single")
        c.check(MULTIPART_MARKER not in single.metadata and len(single.first.parts) == 1,
                f"an object written by PutObject carries the multipart marker or {len(single.first.parts)} parts")


@case("SF-08", "The standard object set: every size stores the gold form of its layout", ["e0", "e1", "e2"],
      level="p1", layer="L2")
def sf_08(c: Ctx) -> None:
    """Records the stored form of each object, which is what a comparison between two binaries is made on."""
    bucket = c.bucket()
    manifest, missing = _manifest(c)
    for scheme in SCHEMES[c.config]:
        for size, form_name in STANDARD_SIZES:
            with c.row(f"{scheme.family}-{size}"):
                key = f"{scheme.family}-{size}"
                parts = _parts(c.case_id, key, [size])
                _store(c, bucket, key, parts, scheme)
                obj = stored.observe_form(c.site, bucket, key)
                _agree(c, obj, key)
                c.check(obj.first.parts and obj.first.parts[0][2] == size,
                        f"recorded parts {obj.first.parts}, expected one part of logical size {size}")
                # The sizes of the gold entry belong to another plaintext; the key names, values and files do not.
                _held_to_manifest(c, manifest, obj, scheme, form_name, _params(bucket, key, parts, scheme), sizes=False)
    if missing is not None:
        raise h.EnvError(str(missing))


@case("SF-09", "Every drive holds the same encryption metadata for an object", ["e1", "e2"], level="p1", layer="L2")
def sf_09(c: Ctx) -> None:
    bucket = c.bucket()
    previous = None
    for scheme in SCHEMES[c.config]:
        for form_name in ("inline", "multipart"):
            with c.row(f"{scheme.family}-{form_name}"):
                key = f"{scheme.family}-{form_name}"
                _store(c, bucket, key, _parts(c.case_id, key, FORMS[form_name]), scheme)
                obj = stored.read_object(c.site, bucket, key)
                _agree(c, obj, key)
                if previous is not None:
                    # The comparison sees a difference when there is one: another object's copy is not this one's.
                    mixed = stored.StoredObject(bucket, key, [obj.first, previous.first])
                    c.check(stored.drive_disagreements(mixed, 2), "two different objects compare as the same copy")
                previous = obj
