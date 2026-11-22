"""Offline checks of the suite's own judgment: what it must tell apart, and what it must not accept.

They need no server. Each one states a pair the suite has to distinguish or a difference it has to ignore, so a
change that makes the suite blind in one of these ways fails here.
"""

from __future__ import annotations

import diffing
import framework as fw
import harness as h
import observe
import stored

LIST_PARTS = (
    '<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket><Key>k</Key>'
    "<UploadId>{upload}</UploadId>{parts}</ListPartsResult>"
)
PART = '<Part><PartNumber>{number}</PartNumber><LastModified>{modified}</LastModified><ETag>"{etag}"</ETag><Size>{size}</Size></Part>'


def _observe_xml(body: str) -> dict:
    observe.begin_case("selftest", "XML")
    try:
        observe.exchange("ListParts", "GET", "/b/k", "uploadId=x", 200, {"content-type": "application/xml"}, body.encode())
    finally:
        observe.end_case()
    return observe.drain()[0]["body"]


def _listing(upload: str, modified: str, etag: str, sizes: list[int]) -> dict:
    parts = "".join(PART.format(number=i, modified=modified, etag=etag, size=size) for i, size in enumerate(sizes, start=1))
    return _observe_xml(LIST_PARTS.format(upload=upload, parts=parts))


def _xml_checks() -> list[str]:
    failures = []
    base = _listing("upload-a", "2026-01-01T00:00:00Z", "a" * 32, [5242880, 17])
    if _listing("upload-b", "2026-02-02T00:00:00Z", "b" * 32, [5242880, 17]) != base:
        failures.append("XML observation: a different upload id, timestamp or ETag value changes the observation")
    if _listing("upload-a", "2026-01-01T00:00:00Z", "a" * 32, [1, 17]) == base:
        failures.append("XML observation: a different part Size does not change the observation")
    if _listing("upload-a", "2026-01-01T00:00:00Z", "a" * 32, [5242880]) == base:
        failures.append("XML observation: a missing Part does not change the observation")
    if _listing("upload-a", "2026-01-01T00:00:00Z", "a" * 32, [17, 5242880]) == base:
        failures.append("XML observation: reordered parts do not change the observation")
    return failures


def _verdict(headers: dict[str, str]) -> tuple[int, int, int]:
    """(failures, known deviations, fixed deviations) for an SSE-C multipart response with these SSE headers."""
    key = h.derived_key("selftest")
    ctx = fw.Ctx(site=None, config="selftest", case_id="SELFTEST")
    ctx.projection_with_known_extra(headers, h.projection_c(key), {h.SSE: "AES256"}, "response")
    return len(ctx.failures), len(ctx.known), len(ctx.fixed)


def _deviation_checks() -> list[str]:
    failures = []
    key = h.derived_key("selftest")
    want = h.projection_c(key)
    triple = h.ssec(key)
    cases = {
        "the intended headers": (want, (0, 0, 1)),
        "the documented extra AES256 header": ({**want, h.SSE: "AES256"}, (0, 1, 0)),
        "the raw customer key echoed back": ({**want, h.SSE: "AES256", f"{h.SSEC}key": triple[f"{h.SSEC}key"]}, (1, 0, 0)),
        "an extra aws:kms header": ({**want, h.SSE: "aws:kms"}, (1, 0, 0)),
        "a missing key MD5": ({f"{h.SSEC}algorithm": "AES256", h.SSE: "AES256"}, (1, 0, 0)),
    }
    for name, (headers, expected) in cases.items():
        got = _verdict(headers)
        if got != expected:
            failures.append(f"known deviation: {name} gives (failures, known, fixed) = {got}, expected {expected}")
    return failures


def _run(binary: dict, value) -> dict:
    result = {"config": "e1", "case": "KS-02", "row": "", "verdict": fw.PASS}
    note = {"config": "e1", "case": "KS-02", "seq": 0, "row": "", "kind": "startup-event", "name": "sse_local_master_key",
            "value": value}
    return {"dir": None, "meta": {"binary": binary}, "results": [result], "observations": [note]}


def _diff_checks() -> list[str]:
    failures = []
    old, new = {"master_key_event": False}, {"master_key_event": True}
    loaded, not_set = [{"level": "INFO", "state": "loaded"}], [{"level": "INFO", "state": "not_set"}]
    outcome = diffing.compare(_run(old, []), _run(new, loaded))
    if outcome["blocking"] or len(outcome["allowed"]) != 1:
        failures.append("diff: the event missing on a binary that predates it is not an accepted difference")
    if not diffing.compare(_run(new, loaded), _run(new, not_set))["blocking"]:
        failures.append("diff: two binaries that both have the event may disagree on its state")
    if not diffing.compare(_run(new, []), _run(new, loaded))["blocking"]:
        failures.append("diff: a binary that should have the event may lack it")
    changed = _run(new, loaded)
    changed["results"][0]["verdict"] = fw.FAIL
    if not diffing.compare(_run(new, loaded), changed)["blocking"]:
        failures.append("diff: a changed verdict is not reported")
    return failures


def _leak_scan_checks() -> list[str]:
    """The leak scans report a canary in every spelling and accept only the one exempt occurrence."""
    import archive  # imported here: the module installs its hooks on import, and only a run needs them

    return archive.selftest()


def run() -> list[str]:
    return [*_xml_checks(), *_deviation_checks(), *_diff_checks(), *stored.selftest(), *_leak_scan_checks()]
