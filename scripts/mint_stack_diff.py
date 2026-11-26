#!/usr/bin/env python3
# Copyright 2026 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Compare Mint NDJSON logs from legacy (reference) and gateway (candidate) runs.

Each positional argument may be either a Mint `log.json` file or a leg artifact
directory containing `mint-log/log.json` (and optionally `target.json`).

The comparator normalizes per-run noise embedded in Mint function strings
(`awscli` random bucket IDs/endpoint URLs and `versioning` source file:line
prefixes), aggregates repeated `(sdk, test)` entries by exact `(PASS, FAIL, NA)`
counts so additional failures or dropped subcases cannot be masked, renders a
Markdown `(sdk, test, legacy, gateway)` diff table, and fails closed on:

  - exit 2: missing, unreadable, empty, zero-test, or malformed Mint logs, or
    missing/malformed required provenance metadata (`--require-meta`);
  - exit 1: any non-empty diff table entry (`legacy != gateway`) or provenance
    mismatch (such as comparing identical binary SHA-256 digests);
  - exit 0: both logs parsed with >= 1 test, provenance verified (when enabled),
    and the diff table is empty.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys

VALID_STATUSES = ("PASS", "FAIL", "NA")
STATUS_RANK = {"FAIL": 2, "PASS": 1, "NA": 0}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
AWSCLI_ENDPOINT_RE = re.compile(r"(--endpoint-url\s+)https?://\S+")
AWSCLI_BUCKET_RE = re.compile(r"awscli-mint-test-bucket-\d+")
VERSIONING_LOC_RE = re.compile(r"^/mint/\S+:\d+:\s*(?:main\.)?")


class InputError(Exception):
    """Unreadable, missing, empty, or malformed input; maps to exit code 2."""


class ProvenanceError(Exception):
    """Server identity mismatch between legs; maps to exit code 1."""


def resolve_inputs(
    target: Path,
    explicit_meta: Path | None,
    require_meta: bool,
    leg: str,
) -> tuple[Path, Path | None]:
    """Resolve (log_path, meta_path) for one leg."""
    if target.is_dir():
        log_path = target / "mint-log" / "log.json"
        default_meta = target / "target.json"
        meta_path = explicit_meta if explicit_meta is not None else (
            default_meta if (require_meta or default_meta.is_file()) else None
        )
    elif target.is_file():
        log_path = target
        meta_path = explicit_meta
    else:
        raise InputError(f"{leg}: path does not exist: {target}")

    if not log_path.is_file():
        raise InputError(f"{leg}: missing Mint log file: {log_path}")

    if require_meta and meta_path is None:
        raise InputError(f"{leg}: provenance metadata required (--{leg}-meta or target.json)")
    if meta_path is not None and not meta_path.is_file():
        raise InputError(f"{leg}: missing provenance metadata file: {meta_path}")

    return log_path, meta_path


def normalize_function(sdk: str, raw_function: str) -> str:
    """Normalize per-run ephemeral strings while preserving test identity."""
    text = re.sub(r"[\r\n]+", " ", raw_function).strip()
    if sdk == "awscli":
        text = AWSCLI_ENDPOINT_RE.sub(r"\1<endpoint>", text)
        text = AWSCLI_BUCKET_RE.sub("awscli-mint-test-bucket-<id>", text)
    elif sdk == "versioning":
        text = VERSIONING_LOC_RE.sub("", text)
    return text


def normalize_case_key(entry: dict, source: Path, line_no: int) -> tuple[str, str]:
    """Extract a canonical (sdk, test) tuple from one Mint JSON entry."""
    raw_name = entry.get("name")
    if not isinstance(raw_name, str) or not raw_name.strip():
        raise InputError(f"{source}:{line_no}: missing or non-string 'name'")
    raw_name = raw_name.strip()

    raw_function = entry.get("function")
    if raw_function is None:
        fn_text = ""
    elif isinstance(raw_function, str):
        fn_text = raw_function
    else:
        raise InputError(f"{source}:{line_no}: 'function' must be a string or null")

    if ":" in raw_name:
        sdk, subtest = (part.strip() for part in raw_name.split(":", 1))
        if not sdk:
            raise InputError(f"{source}:{line_no}: empty SDK prefix in 'name': {raw_name!r}")
        test = subtest or normalize_function(sdk, fn_text) or "(default)"
        return sdk, test

    sdk = raw_name
    test = normalize_function(sdk, fn_text) or "(default)"
    return sdk, test


def parse_mint_log(path: Path, leg: str) -> tuple[dict[tuple[str, str], dict[str, int]], int]:
    """Parse Mint NDJSON log into {(sdk, test): {'PASS': p, 'FAIL': f, 'NA': n}}."""
    try:
        raw_text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise InputError(f"{leg}: cannot read {path}: {exc}") from exc

    cases: dict[tuple[str, str], dict[str, int]] = {}
    entry_count = 0

    for line_no, raw_line in enumerate(raw_text.splitlines(), 1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("["):
            raise InputError(f"{path}:{line_no}: expected JSON object entry, got array")
        if not stripped.startswith("{"):
            # Mint writes plain progress lines (e.g. "Dependency validation complete").
            continue
        try:
            entry = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise InputError(f"{path}:{line_no}: malformed JSON entry: {exc}") from exc
        if not isinstance(entry, dict):
            raise InputError(f"{path}:{line_no}: expected JSON object entry")

        raw_status = entry.get("status")
        if not isinstance(raw_status, str):
            raise InputError(f"{path}:{line_no}: missing or non-string 'status'")
        status = raw_status.strip().upper()
        if status not in STATUS_RANK:
            raise InputError(f"{path}:{line_no}: unknown status {raw_status!r}")

        key = normalize_case_key(entry, path, line_no)
        bucket = cases.setdefault(key, {s: 0 for s in VALID_STATUSES})
        bucket[status] += 1
        entry_count += 1

    if entry_count == 0:
        raise InputError(f"{leg}: Mint log contains zero test entries: {path}")

    return cases, entry_count


def load_meta(path: Path, expected_leg: str) -> dict[str, str]:
    """Load and structurally validate one leg's target.json provenance file."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InputError(f"{expected_leg}: invalid provenance file {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise InputError(f"{expected_leg}: provenance file {path} must be a JSON object")

    for field in ("leg", "binary_sha256", "version"):
        value = data.get(field)
        if not isinstance(value, str) or not value.strip():
            raise InputError(f"{expected_leg}: provenance {path} missing non-empty '{field}'")

    sha = data["binary_sha256"].strip().lower()
    if not SHA256_RE.fullmatch(sha):
        raise InputError(f"{expected_leg}: provenance {path} has invalid binary_sha256: {sha!r}")

    result = {
        "leg": data["leg"].strip(),
        "binary_sha256": sha,
        "version": data["version"].strip(),
    }
    if "expected_sha256" in data and data["expected_sha256"] is not None:
        expected_sha = str(data["expected_sha256"]).strip().lower()
        if not SHA256_RE.fullmatch(expected_sha):
            raise InputError(
                f"{expected_leg}: provenance {path} has invalid expected_sha256: {expected_sha!r}"
            )
        result["expected_sha256"] = expected_sha
    if "expected_version" in data and data["expected_version"] is not None:
        result["expected_version"] = str(data["expected_version"]).strip()
    return result


def verify_provenance(legacy_meta: dict[str, str] | None, gateway_meta: dict[str, str] | None) -> list[str]:
    """Return provenance violations (empty list means valid)."""
    errors: list[str] = []
    for expected_leg, meta in (("legacy", legacy_meta), ("gateway", gateway_meta)):
        if meta is None:
            continue
        if meta["leg"] != expected_leg:
            errors.append(
                f"{expected_leg} provenance declares leg={meta['leg']!r}, expected {expected_leg!r}"
            )
        expected_sha = meta.get("expected_sha256")
        if expected_sha and meta["binary_sha256"] != expected_sha:
            errors.append(
                f"{expected_leg} binary_sha256 ({meta['binary_sha256']}) != expected_sha256 ({expected_sha})"
            )
    if legacy_meta and gateway_meta:
        if legacy_meta["binary_sha256"] == gateway_meta["binary_sha256"]:
            errors.append(
                "legacy and gateway ran the same binary_sha256 "
                f"({legacy_meta['binary_sha256']}): refusing self-comparison"
            )
    return errors


def overall_status(counts: dict[str, int]) -> str:
    """Return worst-rank status across repeated entries for one (sdk, test)."""
    present = [s for s in VALID_STATUSES if counts.get(s, 0) > 0]
    return max(present, key=STATUS_RANK.__getitem__)


def format_outcome(counts: dict[str, int] | None) -> str:
    """Format a case's outcome for the diff table."""
    if counts is None:
        return "MISSING"
    total = sum(counts.values())
    status = overall_status(counts)
    if total == 1:
        return status
    nonzero = [f"{counts[s]} {s}" for s in VALID_STATUSES if counts[s] > 0]
    if len(nonzero) == 1:
        return f"{status} ({total})"
    return f"{status} ({', '.join(nonzero)})"


def summarize_cases(cases: dict[tuple[str, str], dict[str, int]], entry_count: int) -> dict[str, int]:
    """Aggregate case-level status counts and total entry count for one leg."""
    totals = Counter(overall_status(counts) for counts in cases.values())
    return {
        "cases": len(cases),
        "entries": entry_count,
        "PASS": totals.get("PASS", 0),
        "FAIL": totals.get("FAIL", 0),
        "NA": totals.get("NA", 0),
    }


def per_sdk_summary(
    legacy_cases: dict[tuple[str, str], dict[str, int]],
    gateway_cases: dict[tuple[str, str], dict[str, int]],
    diffs: list[dict],
) -> list[dict]:
    """Compute per-SDK case summaries and diff counts."""
    sdks = sorted({sdk for sdk, _ in legacy_cases} | {sdk for sdk, _ in gateway_cases})
    diff_counts = Counter(row["sdk"] for row in diffs)
    rows = []
    for sdk in sdks:
        leg_counts = Counter(
            overall_status(counts) for (s, _), counts in legacy_cases.items() if s == sdk
        )
        gw_counts = Counter(
            overall_status(counts) for (s, _), counts in gateway_cases.items() if s == sdk
        )
        rows.append(
            {
                "sdk": sdk,
                "legacy": {s: leg_counts.get(s, 0) for s in VALID_STATUSES},
                "gateway": {s: gw_counts.get(s, 0) for s in VALID_STATUSES},
                "diffs": diff_counts.get(sdk, 0),
            }
        )
    return rows


def md_code(value: str) -> str:
    """Safely render text inside a Markdown table code cell."""
    sanitized = value.replace("\r", " ").replace("\n", " ").replace("|", "\\|").replace("`", "'")
    return f"`{sanitized}`"


def render_markdown(report: dict) -> str:
    """Render the comparison report and (sdk, test, legacy, gateway) diff table."""
    verdict = report["verdict"]
    diffs = report["differences"]
    errors = report["errors"]

    lines = ["# Mint Dual-Stack Comparison", ""]
    if verdict == "pass":
        lines.append("**Verdict**: PASS — diff table is empty (`0` differences).")
    elif verdict == "fail":
        if errors:
            lines.append(f"**Verdict**: FAIL — {len(errors)} provenance error(s) and {len(diffs)} difference(s).")
        else:
            lines.append(f"**Verdict**: FAIL — diff table is non-empty (`{len(diffs)}` difference(s)).")
    else:
        lines.append("**Verdict**: ERROR — input validation failed (fail-closed).")
    lines.append("")

    lines.extend(
        [
            "| Leg | Version | Binary SHA-256 | Cases | PASS | FAIL | NA |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for leg in ("legacy", "gateway"):
        info = report[leg]
        meta = info.get("meta") or {}
        summary = info.get("summary") or {}
        version = meta.get("version") or "n/a"
        sha = (meta.get("binary_sha256") or "n/a")[:16]
        lines.append(
            f"| `{leg}` | {md_code(version)} | {md_code(sha)} | "
            f"{summary.get('cases', 0)} | {summary.get('PASS', 0)} | "
            f"{summary.get('FAIL', 0)} | {summary.get('NA', 0)} |"
        )
    lines.append("")

    if errors:
        lines.append("## Errors")
        lines.append("")
        for err in errors:
            lines.append(f"- {err}")
        lines.append("")

    sdk_rows = report.get("per_sdk") or []
    if sdk_rows:
        lines.extend(
            [
                "## Per-SDK Summary",
                "",
                "| sdk | legacy (PASS / FAIL / NA) | gateway (PASS / FAIL / NA) | diffs |",
                "| --- | --- | --- | ---: |",
            ]
        )
        for row in sdk_rows:
            lc = row["legacy"]
            gc = row["gateway"]
            lines.append(
                f"| {md_code(row['sdk'])} | {lc['PASS']} / {lc['FAIL']} / {lc['NA']} | "
                f"{gc['PASS']} / {gc['FAIL']} / {gc['NA']} | {row['diffs']} |"
            )
        lines.append("")

    lines.append(f"## Diff Table ({len(diffs)})")
    lines.append("")
    if diffs:
        lines.extend(
            [
                "| sdk | test | legacy | gateway |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in diffs:
            lines.append(
                f"| {md_code(row['sdk'])} | {md_code(row['test'])} | "
                f"{md_code(row['legacy'])} | {md_code(row['gateway'])} |"
            )
    elif verdict == "pass":
        lines.append("_Empty: every `(sdk, test)` outcome matched between legacy and gateway._")
    else:
        lines.append("_No comparable diff rows (see Errors above)._")
    lines.append("")
    return "\n".join(lines)


def compare_legs(
    legacy_target: Path,
    gateway_target: Path,
    *,
    legacy_meta_path: Path | None = None,
    gateway_meta_path: Path | None = None,
    require_meta: bool = False,
) -> tuple[dict, int]:
    """Compare legacy and gateway Mint legs and return (report_dict, exit_code)."""
    report: dict = {
        "schema": 1,
        "verdict": "error",
        "exit_code": 2,
        "legacy": {"target": str(legacy_target), "log": None, "meta": None, "summary": None},
        "gateway": {"target": str(gateway_target), "log": None, "meta": None, "summary": None},
        "per_sdk": [],
        "differences": [],
        "errors": [],
    }

    input_errors: list[str] = []
    parsed_cases: dict[str, dict[tuple[str, str], dict[str, int]]] = {}
    parsed_meta: dict[str, dict[str, str] | None] = {"legacy": None, "gateway": None}

    for leg, target, explicit_meta in (
        ("legacy", legacy_target, legacy_meta_path),
        ("gateway", gateway_target, gateway_meta_path),
    ):
        try:
            log_path, meta_path = resolve_inputs(target, explicit_meta, require_meta, leg)
            report[leg]["log"] = str(log_path)
            if meta_path is not None:
                meta = load_meta(meta_path, leg)
                parsed_meta[leg] = meta
                report[leg]["meta"] = meta
            cases, entry_count = parse_mint_log(log_path, leg)
            parsed_cases[leg] = cases
            report[leg]["summary"] = summarize_cases(cases, entry_count)
        except InputError as exc:
            input_errors.append(str(exc))

    if input_errors:
        report["errors"] = input_errors
        report["verdict"] = "error"
        report["exit_code"] = 2
        return report, 2

    provenance_errors = verify_provenance(parsed_meta["legacy"], parsed_meta["gateway"])
    legacy_cases = parsed_cases["legacy"]
    gateway_cases = parsed_cases["gateway"]

    diffs: list[dict] = []
    for sdk, test in sorted(set(legacy_cases) | set(gateway_cases)):
        leg_counts = legacy_cases.get((sdk, test))
        gw_counts = gateway_cases.get((sdk, test))
        if leg_counts != gw_counts:
            diffs.append(
                {
                    "sdk": sdk,
                    "test": test,
                    "legacy": format_outcome(leg_counts),
                    "gateway": format_outcome(gw_counts),
                    "legacy_counts": leg_counts,
                    "gateway_counts": gw_counts,
                }
            )

    report["per_sdk"] = per_sdk_summary(legacy_cases, gateway_cases, diffs)
    report["differences"] = diffs
    report["errors"] = provenance_errors

    if provenance_errors or diffs:
        report["verdict"] = "fail"
        report["exit_code"] = 1
        return report, 1

    report["verdict"] = "pass"
    report["exit_code"] = 0
    return report, 0


def write_output(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare legacy (reference) and gateway (candidate) Mint log.json results."
    )
    parser.add_argument("legacy", type=Path, help="Legacy Mint log.json file or leg artifact directory")
    parser.add_argument("gateway", type=Path, help="Gateway Mint log.json file or leg artifact directory")
    parser.add_argument("--legacy-meta", type=Path, default=None, help="Path to legacy target.json metadata")
    parser.add_argument("--gateway-meta", type=Path, default=None, help="Path to gateway target.json metadata")
    parser.add_argument(
        "--require-meta",
        action="store_true",
        help="Fail closed if target.json provenance metadata is missing for either leg",
    )
    parser.add_argument("--markdown", type=Path, default=None, help="Write Markdown summary report to this path")
    parser.add_argument("--json", type=Path, default=None, dest="json_out", help="Write JSON diff report to this path")
    args = parser.parse_args(argv)

    report, exit_code = compare_legs(
        args.legacy,
        args.gateway,
        legacy_meta_path=args.legacy_meta,
        gateway_meta_path=args.gateway_meta,
        require_meta=args.require_meta,
    )
    markdown = render_markdown(report)

    if args.markdown is not None:
        write_output(args.markdown, markdown)
    if args.json_out is not None:
        write_output(args.json_out, json.dumps(report, indent=2, sort_keys=True) + "\n")

    print(markdown, end="")
    if exit_code == 2:
        for err in report["errors"]:
            print(f"error: {err}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
