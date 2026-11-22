"""What must never leave the server (SEC): scans over everything a run saw, and refusals that must not differ.

Four checks judge the run as a whole and are reported as cases of the configuration `run`: no response repeats key
material (SEC-01), no server log does (SEC-02), no response carries a storage-internal header (SEC-03), and every
500 says nothing beyond a fixed text (SEC-06). The other cases compare what specific callers are told.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import archive
import audit_sink
import framework as fw
import harness as h
import observe
import scan
from cases_authz_p1 import AUDIT_CONFIG, CSV, audit_entry, denied, select_object
from cases_negative_authz import _users
from framework import Ctx, case

SIZE = 70 * h.KIB
KEY_A = h.derived_key("ssec a")
KEY_B = h.derived_key("ssec b")
RUN_CONFIG = "run"
MAX_REASONS = 40
IMMEDIATE_DELETION_ENV = "RUSTFS_KMS_ALLOW_IMMEDIATE_DELETION"
KEY_DISABLED = "bb-sec-disabled"
KEY_DESTROYED = "bb-sec-destroyed"
STATE_WORDING = {"usable": "the key is usable", "disabled": "the key is disabled", "destroyed": "the key was destroyed",
                 "kms-stopped": "KMS is stopped"}
# What a caller without the grant is told today when it reads an object, where it should hear 403: one answer per
# state, nothing wider.
READ_WITHOUT_GRANT_TODAY = {"destroyed": (400, "KMS.NotFoundException"), "kms-stopped": (503, "ServiceUnavailable")}
_REQUEST_ID = re.compile(rb"<RequestId>[^<]*</RequestId>")


# ---------------------------------------------------------------------------
# Checks over the whole run
# ---------------------------------------------------------------------------

def _capped(reasons: list[str]) -> list[str]:
    if len(reasons) <= MAX_REASONS:
        return reasons
    return [*reasons[:MAX_REASONS], f"... and {len(reasons) - MAX_REASONS} more"]


def _result(case_id: str, level: str, title: str, verdict: str, reasons: list[str], started: float) -> dict:
    return {"config": RUN_CONFIG, "case": case_id, "level": level, "layer": "L1", "row": "", "title": title,
            "verdict": verdict, "reasons": _capped(reasons), "seconds": round(time.monotonic() - started, 2)}


def _judged(case_id: str, level: str, title: str, problems: list[str], scanned: str, nothing: str | None,
            started: float) -> dict:
    """Pass with a statement of what was scanned, fail with where, or skip when there was nothing to judge."""
    if problems:
        return _result(case_id, level, title, fw.FAIL, problems, started)
    if nothing:
        # A scan over nothing must not read as a clean run.
        return _result(case_id, level, title, fw.SKIP, [nothing], started)
    return _result(case_id, level, title, fw.PASS, [scanned], started)


def scan_responses(records: list[dict]) -> tuple[list[str], int]:
    """(where a response repeats a canary, how many exempt occurrences there were), from finalized archive records."""
    problems, exempt = [], 0
    for record in records:
        exempt += record.get("exempt", 0)
        for finding in record["findings"]:
            problems.append(f"{archive.locate(record)}, response {finding['where']}: {finding['what']}")
    return problems, exempt


def scan_logs(run_dir: Path, registry: scan.Registry) -> tuple[list[str], int, int]:
    """(where a server log repeats a canary, number of files, number of bytes)."""
    problems, size = [], 0
    files = sorted(p for p in (run_dir / "sites").glob("*/logs/**/*") if p.is_file())
    for path in files:
        size += path.stat().st_size
        for line, hit, excerpt in registry.scan_file(path):
            problems.append(f"{path.relative_to(run_dir)} line {line}: {hit.describe()}; the line reads: {excerpt}")
    return problems, len(files), size


@fw.post_run
def scan_run(run_dir: Path, results: list[dict], observations: list[dict]) -> list[dict]:
    del results, observations  # the scans read the archive and the logs, not the verdicts
    started = time.monotonic()
    registry = scan.REGISTRY
    titles = {
        "SEC-01": "No response of the run contains key material",
        "SEC-02": "No server log of the run contains key material",
        "SEC-03": "No response of the run carries a storage-internal header",
        "SEC-06": "Every 500 of the run says nothing beyond a fixed text",
    }
    broken = scan.self_check(run_dir / archive.ARCHIVE_DIR)
    archive.harvest_key_files(registry, run_dir)
    records = archive.finalize(run_dir, registry)
    kinds = ", ".join(f"{count} {kind}" for kind, count in registry.kinds().items())
    no_canary = None if len(registry) else "no secret was registered in this run, so there was nothing to search for"
    out = []

    if broken:
        # A scanner that cannot find a planted canary proves nothing about the run.
        out.append(_result("SEC-01", "p0", titles["SEC-01"], fw.ENV_ERROR, [f"scanner self-check: {broken}"], started))
        out.append(_result("SEC-02", "p0", titles["SEC-02"], fw.ENV_ERROR, [f"scanner self-check: {broken}"], started))
    else:
        problems, exempt = scan_responses(records)
        scanned = (f"searched {len(records)} responses for {len(registry)} canaries ({kinds}); {exempt} accepted "
                   "occurrence(s) of a generated data key in the response that returned it")
        nothing = no_canary or (None if records else "the run archived no exchange")
        out.append(_judged("SEC-01", "p0", titles["SEC-01"], problems, scanned, nothing, started))

        problems, files, size = scan_logs(run_dir, registry)
        scanned = f"searched {files} log files ({size} bytes) for {len(registry)} canaries ({kinds})"
        nothing = no_canary or (None if files else "the run left no server log")
        out.append(_judged("SEC-02", "p0", titles["SEC-02"], problems, scanned, nothing, started))

    problems = [f"{archive.locate(record)}: response headers {', '.join(scan.internal_headers(record['response_headers']))}"
                for record in records if scan.internal_headers(record["response_headers"])]
    out.append(_judged("SEC-03", "p0", titles["SEC-03"], problems, f"checked the header names of {len(records)} responses",
                       None if records else "the run archived no exchange", started))

    failures = [record for record in records if record["status"] == 500]
    problems = []
    for record in failures:
        stored = record["response_body"]
        # An error document is archived in full; one that is not cannot be judged and is a problem by itself.
        problem = (scan.internal_error_problem(stored["text"].encode("latin-1")) if "text" in stored
                   else "the body is not a document")
        if problem:
            problems.append(f"{archive.locate(record)}: {registry.mask_text(problem)}")
    out.append(_judged("SEC-06", "p1", titles["SEC-06"], problems,
                       f"{len(failures)} of {len(records)} responses have status 500"
                       + ("; each carries one of the fixed texts" if failures else ""),
                       None if records else "the run archived no exchange", started))
    return out


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def _key_disable(c: Ctx, key_id: str) -> None:
    status, body = h.admin(c.site, "POST", "kms/keys/disable", payload={"key_id": key_id}, label="KmsDisableKey")
    c.need(status == 200, f"disable {key_id}: HTTP {status} {str(body)[:200]}")


def _key_destroy(c: Ctx, key_id: str) -> None:
    """Destroy the key material now: the product wants the deletion scheduled first, then confirmed."""
    steps = {"schedule the deletion of": {"key_id": key_id, "pending_window_in_days": 7},
             "destroy": {"key_id": key_id, "force_immediate": True, "confirm_key_id": key_id}}
    for step, payload in steps.items():
        status, body = h.admin(c.site, "DELETE", "kms/keys/delete", payload=payload, label="KmsDeleteKey")
        c.need(status == 200, f"{step} {key_id}: HTTP {status} {str(body)[:200]}")


def _refusal_shape(response) -> tuple:
    """What a caller can tell two refusals apart by: status, document without its request id, header names."""
    status, headers, body = response
    return status, _REQUEST_ID.sub(b"<RequestId/>", body), tuple(sorted(headers)), headers.get("content-length")


@case("SEC-05", "A caller without the grant is told the same whatever the state of the key or of KMS", ["e2e"],
      level="p1", isolated=True)
def sec_05(c: Ctx) -> None:
    # Destroying a key at once is off unless the process allows it, and it must not happen to a site other cases use.
    c.restart(env={**fw.CONFIGS["e2e"](c.site), IMMEDIATE_DELETION_ENV: "true"})
    h.wait_kms_healthy(c.site)
    bucket = c.bucket()
    creds = _users(c)["u2"]
    keys = {"usable": h.KEY_1, "disabled": KEY_DISABLED, "destroyed": KEY_DESTROYED}
    data = {state: h.plain(f"sec-05 {state}", SIZE) for state in keys}
    new = h.plain("sec-05 new", h.KIB)
    for state, key_id in keys.items():
        if key_id != h.KEY_1:
            status, body = h.key_create(c.site, key_id)
            c.need(status == 200, f"create {key_id}: HTTP {status} {str(body)[:200]}")
        c.ok(h.put(c.site, bucket, state, data[state], h.sse_kms(key_id)), f"admin PUT under the {state} key")
    _key_disable(c, KEY_DISABLED)
    _key_destroy(c, KEY_DESTROYED)
    status, body = h.admin(c.site, "POST", "kms/clear-cache", label="KmsClearCache")
    c.need(status == 200, f"clear the KMS cache: HTTP {status} {str(body)[:200]}")

    reads, writes = {}, {}
    deviating: set[str] = set()

    def without_grant(state: str, read_key: str, write_key_id: str) -> None:
        """One read and one write by the caller without a grant; both must be the plain refusal."""
        reads[state] = h.get(c.site, bucket, read_key, creds=creds)
        today = READ_WITHOUT_GRANT_TODAY.get(state)
        intended = f"GET by a caller without the grant is refused with 403 AccessDenied when {STATE_WORDING[state]}"
        if today is not None and (reads[state][0], h.s3_error(reads[state][2])[0]) == today:
            # The object layer unwraps the data key before the caller's grant is checked, so the state reaches a
            # caller who may not use the key. Only this one answer is tolerated.
            deviating.add(state)
            c.known_deviation(False, f"{intended}; today it answers {today[0]} {today[1]}")
            c.check(data[read_key][:64] not in reads[state][2], "GET: the refused request returned object bytes")
        else:
            denied(c, reads[state], "GET", data[read_key])
            if today is not None:
                c.known_deviation(True, intended)
        writes[state] = h.put(c.site, bucket, f"denied-{state}", new, h.sse_kms(write_key_id), creds=creds)
        denied(c, writes[state], "PUT")
        c.absent(bucket, f"denied-{state}", "PUT")

    # The administrator holds every grant and is told the state of each key. That difference is what the caller
    # without a grant must not see.
    with c.row("authorized-caller-sees-the-key-state"):
        c.ok(h.put(c.site, bucket, "admin-new-usable", new, h.sse_kms(h.KEY_1)), "admin PUT under the usable key")
        c.refused(h.put(c.site, bucket, "admin-new-disabled", new, h.sse_kms(KEY_DISABLED)), 400, "InvalidRequest",
                  "admin PUT under the disabled key")
        c.absent(bucket, "admin-new-disabled", "admin PUT under the disabled key")
        c.refused(h.put(c.site, bucket, "admin-new-destroyed", new, h.sse_kms(KEY_DESTROYED)), 400,
                  "KMS.NotFoundException", "admin PUT under the destroyed key")
        c.absent(bucket, "admin-new-destroyed", "admin PUT under the destroyed key")
        c.read_back(bucket, "usable", data["usable"], None, h.projection_kms(h.KEY_1), "admin GET under the usable key")
        c.refused(h.get(c.site, bucket, "destroyed"), 400, "KMS.NotFoundException", "admin GET under the destroyed key")
    for state, key_id in keys.items():
        with c.row(f"without-grant-{state}"):
            without_grant(state, state, key_id)

    # The last state is the service's, not a key's: KMS is stopped. Nothing follows it on this site.
    status, body = h.kms_stop(c.site)
    c.need(status == 200, f"KMS stop: HTTP {status} {str(body)[:200]}")
    with c.row("authorized-caller-sees-kms-stopped"):
        for what, response in (("PUT", h.put(c.site, bucket, "admin-new-stopped", new, h.sse_kms(h.KEY_1))),
                               ("GET", h.get(c.site, bucket, "usable"))):
            c.check(response[0] == 503 and h.s3_error(response[2])[0] != "",
                    f"admin {what} while KMS is stopped: HTTP {response[0]} {h.s3_error(response[2])}, expected an explicit 503")
        c.absent(bucket, "admin-new-stopped", "admin PUT while KMS is stopped")
    with c.row("without-grant-kms-stopped"):
        without_grant("kms-stopped", "usable", h.KEY_1)

    with c.row("refusals-are-identical"):
        for what, responses in (("GET", reads), ("PUT", writes)):
            shapes = {state: _refusal_shape(response) for state, response in responses.items()}
            for state in ("disabled", "destroyed", "kms-stopped"):
                if what == "GET" and state in deviating:
                    continue  # recorded above as a known deviation; every other pair must still agree
                c.check(shapes[state] == shapes["usable"],
                        f"the refused {what} when {STATE_WORDING[state]} differs from the one when "
                        f"{STATE_WORDING['usable']}: HTTP {shapes[state][0]} {h.s3_error(responses[state][2])} headers "
                        f"{list(shapes[state][2])} against HTTP {shapes['usable'][0]} "
                        f"{h.s3_error(responses['usable'][2])} headers {list(shapes['usable'][2])}")


@case("SEC-08", "The encryption context of an object is not echoed to its readers", ["e2"], level="p1")
def sec_08(c: Ctx) -> None:
    bucket = c.bucket()
    # One pair only: the server stores the context as a map and writes it back in no particular order.
    context = {"department": "finance"}
    write_headers = h.sse_kms(h.KEY_1, context)
    encoded = write_headers[h.SSE_CONTEXT]
    want = h.projection_kms(h.KEY_1)
    put_headers, _ = c.ok(h.put(c.site, bucket, "records.csv", CSV, write_headers), "PUT with an encryption context")
    get_headers, body = c.ok(h.get(c.site, bucket, "records.csv"), "GET")
    c.bytes_equal(get_headers, body, CSV, "GET")
    status, head_headers = h.head(c.site, bucket, "records.csv")
    c.need(status == 200, f"HEAD: HTTP {status}, expected 200")
    for what, headers in (("PUT", put_headers), ("GET", get_headers), ("HEAD", head_headers)):
        with c.row(what.lower()):
            c.projection(headers, want, what)
            for name, value in headers.items():
                c.check(encoded not in value and "finance" not in value, f"{what}: header {name} repeats the encryption context")
    with c.row("select"):
        select_headers, body = c.ok(select_object(c.site, bucket, "records.csv"), "SelectObjectContent")
        c.check(CSV in body, "SelectObjectContent: the result does not carry the records")
        # SelectObjectContent does echo the stored context today, unlike GET and HEAD. Pinned as it is: whether the
        # three should agree is an open product question, and a change must be a decision.
        c.projection(select_headers, {**want, h.SSE_CONTEXT: encoded}, "SelectObjectContent")


def _listed_etags(c: Ctx, bucket: str, query: str, entry: str, label: str) -> dict[str, str]:
    """{key: ETag without quotes} from a listing."""
    _, body = c.ok(h.signed_request(c.site, "GET", f"/{bucket}", query=query, label=label), label)
    return {item.findtext("{*}Key"): (item.findtext("{*}ETag") or "").strip('"')
            for item in ET.fromstring(body).findall(f"{{*}}{entry}")}


@case("SEC-09", "A listing never shows the MD5 of an encrypted object's plaintext", ["e2"], level="p1")
def sec_09(c: Ctx) -> None:
    bucket = c.bucket()
    schemes = {"plain": None, "sse-s3": h.sse_s3(), "sse-kms": h.sse_kms(h.KEY_1), "sse-c": h.ssec(KEY_A)}
    md5 = {}
    for name, headers in schemes.items():
        data = h.plain(f"sec-09 {name}", SIZE)
        md5[name] = hashlib.md5(data).hexdigest()
        c.ok(h.put(c.site, bucket, name, data, headers), f"{name} PUT")
    listings = {
        "list-objects-v2": _listed_etags(c, bucket, "list-type=2", "Contents", "ListObjectsV2"),
        "list-objects-v1": _listed_etags(c, bucket, "", "Contents", "ListObjects"),
        "list-object-versions": _listed_etags(c, bucket, "versions=", "Version", "ListObjectVersions"),
    }
    for listing, etags in listings.items():
        with c.row(listing):
            c.check(sorted(etags) == sorted(schemes), f"the listing names {sorted(etags)}, expected {sorted(schemes)}")
            # The plaintext object is the control: its ETag is its MD5, which is what a leak would look like.
            c.check(etags.get("plain") == md5["plain"],
                    f"plaintext object: listed ETag {etags.get('plain')!r} is not its MD5, so the comparison below "
                    "proves nothing")
            for name in ("sse-s3", "sse-kms", "sse-c"):
                c.check(etags.get(name) not in (None, "", md5[name]),
                        f"{name} object: the listed ETag {etags.get(name)!r} is missing or is the MD5 of the plaintext")


@case("SEC-10", "Nothing delivered to the audit receiver contains key material", [AUDIT_CONFIG], level="p1", last=True)
def sec_10(c: Ctx) -> None:
    bucket = c.bucket()
    data = h.plain("sec-10", SIZE)
    # Requests whose headers carry a customer key, accepted and refused, and the admin call that returns a data key.
    requests = {
        "ssec-put": (h.put(c.site, bucket, "ssec", data, h.ssec(KEY_A)), 200),
        "ssec-get": (h.get(c.site, bucket, "ssec", h.ssec(KEY_A)), 200),
        "ssec-get-other-key": (h.get(c.site, bucket, "ssec", h.ssec(KEY_B)), 400),
        "ssec-copy": (h.copy(c.site, bucket, "ssec-copy", bucket, "ssec", {**h.ssec(KEY_A, h.COPY_SSEC), **h.ssec(KEY_B)}), 200),
    }
    for name, (response, status) in requests.items():
        with c.row(name):
            c.need(response[0] == status, f"request: HTTP {response[0]} {h.s3_error(response[2])}, expected {status}")
            audit_entry(c, response, name)
    with c.row("data-key"):
        response = h.signed_request(c.site, "POST", f"{h.ADMIN_PREFIX}{scan.DATA_KEY_ENDPOINT}",
                                    body=json.dumps({"key_id": h.KEY_1, "key_spec": "Aes256"}).encode(),
                                    headers={"content-type": "application/json"}, label="KmsGenerateDataKey")
        c.need(response[0] == 200 and b"plaintext_key" in response[2],
               f"generate a data key: HTTP {response[0]}, expected 200 with a plaintext key")
        c.check(any(canary.kind == scan.KIND_DATA_KEY for canary in scan.REGISTRY.canaries()),
                "the returned plaintext data key was not registered as a canary")
        # The key was handed to the caller; the audit entry of that call must not repeat it.
        audit_entry(c, response, "data-key")
    with c.row("everything-delivered"):
        sink = audit_sink.SINK
        with observe.quiet():
            time.sleep(1.0)  # let deliveries that follow the last awaited entry arrive; what is late is not judged
        payloads = list(sink.payloads)
        c.check(len(payloads) >= len(requests),
                f"only {len(payloads)} audit deliveries arrived for {len(requests)} awaited requests")
        for number, payload in enumerate(payloads, start=1):
            for hit in scan.REGISTRY.scan(payload):
                c.check(False, f"audit delivery {number} of {len(payloads)}: {hit.describe()}")


@case("SEC-11", "Neither a response nor the log repeats an invalid master key value, in whole or in part",
      ["e1x-text", "e1x-short"], level="p1")
def sec_11(c: Ctx) -> None:
    bucket = c.bucket()
    canaries = [canary for canary in scan.REGISTRY.canaries()
                if canary.kind == scan.KIND_INVALID_MASTER_KEY and canary.label.endswith(f"site {c.site.name}")]
    c.need(canaries, "the configured master key value is not registered as a canary, so nothing would be found")
    c.check(any(is_fragment for canary in canaries for _, _, is_fragment in canary.probes),
            "no fragment of the configured value is searched for")
    data = h.plain("sec-11", SIZE)
    c.ok(h.put(c.site, bucket, "source", data), "plaintext PUT")
    status, create_headers, _, create_body = h.create_multipart(c.site, bucket, "multipart", h.sse_s3())
    # Every way to ask for SSE-S3 meets the unusable key; each refusal is a place where its value could be repeated.
    responses = {
        "PutObject": h.put(c.site, bucket, "object", data, h.sse_s3()),
        "CreateMultipartUpload": (status, create_headers, create_body),
        "CopyObject": h.copy(c.site, bucket, "copy", bucket, "source", h.sse_s3()),
    }
    for name, response in responses.items():
        with c.row(name):
            c.refused(response, 400, "InvalidRequest", name)
            head = "\n".join(f"{k}: {v}" for k, v in response[1].items()).encode("latin-1", errors="replace")
            blob = head + b"\n" + response[2]
            for hit in scan.REGISTRY.scan(blob):
                c.check(hit.canary not in canaries, f"{name}: the response repeats the {hit.describe()}")
    for name in ("object", "copy"):
        c.absent(bucket, name, f"refused SSE-S3 write of {name}")
    with c.row("log"):
        for hit in scan.REGISTRY.scan(h.log_text(c.site).encode(errors="replace")):
            c.check(hit.canary not in canaries, f"the server log repeats the {hit.describe()}")
