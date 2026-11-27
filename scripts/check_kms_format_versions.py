#!/usr/bin/env python3
# Copyright 2026 RustFS Team
# SPDX-License-Identifier: Apache-2.0
"""Verify KMS/SSE format version constants and deprecation guards against policy docs.

Checks:
1. Every row in `## Version Anchors` of `docs/architecture/kms-format-version-policy.md`
   matches the exact constant value in the cited Rust source file.
2. Every numeric `*FORMAT_VERSION` and `CONTEXT_BINDING_*` constant declared under
   `crates/kms/src/` or `crates/sse/src/kms/` is registered in `## Version Anchors`.
3. Every row in `### Active Deprecation Guards` names a test function present in its
   cited Rust source file.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
POLICY_REL_PATH = Path("docs/architecture/kms-format-version-policy.md")

VERSION_SECTION_HEADING = "## Version Anchors"
DEPRECATION_SECTION_HEADING = "### Active Deprecation Guards"

# Matches Rust integer constants such as:
#   pub const FORMAT_VERSION: u32 = 1;
#   pub(crate) const LOCAL_SSE_DEK_FORMAT_VERSION: u8 = 1;
#   const FRAME_TYPE_SEGMENT_NONCE: u8 = 0x03;
CONST_DECL_RE = re.compile(
    r"^\s*(?:pub(?:\s*\([^)]*\))?\s+)?const\s+([A-Z][A-Z0-9_]*)\s*:\s*"
    r"(?:u8|u16|u32|u64|usize|i8|i16|i32|i64)\s*=\s*([0-9A-Fa-fxX_]+)\s*;",
    re.MULTILINE,
)

# Matches an `impl [Type]` block header for associated constants (e.g. BackupManifest::FORMAT_VERSION).
IMPL_HEADER_RE = re.compile(r"^\s*impl(?:<[^>]*>)?\s+([A-Za-z_][A-Za-z0-9_]*)\b")

# Tracks constants under crates/kms/src/ and crates/sse/src/kms/ that must stay documented.
TRACKED_CONST_NAME_RE = re.compile(r"^(?:[A-Z0-9_]*FORMAT_VERSION|CONTEXT_BINDING_[A-Z0-9_]+)$")
TRACKED_SCAN_DIRS = (Path("crates/kms/src"), Path("crates/sse/src/kms"))

TEST_FN_RE_TEMPLATE = r"^\s*(?:pub(?:\s*\([^)]*\))?\s+)?(?:async\s+)?fn\s+{name}\s*\("


@dataclass(frozen=True)
class VersionAnchor:
    fmt: str
    rel_file: str
    symbol: str
    value_raw: str


@dataclass(frozen=True)
class DeprecationGuard:
    surface: str
    field: str
    rel_file: str
    test_fn: str


def strip_code_ticks(cell: str) -> str:
    text = cell.strip()
    if len(text) >= 2 and text.startswith("`") and text.endswith("`"):
        return text[1:-1].strip()
    return text


def parse_markdown_table(text: str, heading: str) -> list[list[str]]:
    lines = text.splitlines()
    in_section = False
    heading_level = len(heading) - len(heading.lstrip("#"))
    rows: list[list[str]] = []
    saw_header = False
    saw_separator = False

    for line in lines:
        stripped = line.strip()
        if not in_section:
            if stripped == heading:
                in_section = True
            continue

        if stripped.startswith("#"):
            level = len(stripped) - len(stripped.lstrip("#"))
            if level <= heading_level:
                break

        if not stripped.startswith("|"):
            if saw_separator:
                break
            continue

        cells = [part.strip() for part in stripped.strip("|").split("|")]
        if not saw_header:
            saw_header = True
            continue
        if not saw_separator:
            if all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells):
                saw_separator = True
                continue
            break
        rows.append(cells)

    return rows


def parse_version_anchors(policy_text: str) -> tuple[list[VersionAnchor], list[str]]:
    rows = parse_markdown_table(policy_text, VERSION_SECTION_HEADING)
    errors: list[str] = []
    anchors: list[VersionAnchor] = []
    if not rows:
        errors.append(f"{POLICY_REL_PATH}: missing or empty '{VERSION_SECTION_HEADING}' table")
        return anchors, errors

    seen: set[tuple[str, str]] = set()
    for index, cells in enumerate(rows, 1):
        if len(cells) < 4:
            errors.append(f"{POLICY_REL_PATH}: '{VERSION_SECTION_HEADING}' row {index} has fewer than 4 columns")
            continue
        fmt = cells[0].strip()
        rel_file = strip_code_ticks(cells[1])
        symbol = strip_code_ticks(cells[2])
        value_raw = strip_code_ticks(cells[3])
        if not rel_file or not symbol or not value_raw:
            errors.append(f"{POLICY_REL_PATH}: '{VERSION_SECTION_HEADING}' row {index} has empty required fields")
            continue
        key = (rel_file, symbol)
        if key in seen:
            errors.append(f"{POLICY_REL_PATH}: duplicate Version Anchor entry for {rel_file}::{symbol}")
            continue
        seen.add(key)
        anchors.append(VersionAnchor(fmt=fmt, rel_file=rel_file, symbol=symbol, value_raw=value_raw))

    return anchors, errors


def parse_deprecation_guards(policy_text: str) -> tuple[list[DeprecationGuard], list[str]]:
    rows = parse_markdown_table(policy_text, DEPRECATION_SECTION_HEADING)
    errors: list[str] = []
    guards: list[DeprecationGuard] = []
    if not rows:
        errors.append(f"{POLICY_REL_PATH}: missing or empty '{DEPRECATION_SECTION_HEADING}' table")
        return guards, errors

    for index, cells in enumerate(rows, 1):
        if len(cells) < 4:
            errors.append(f"{POLICY_REL_PATH}: '{DEPRECATION_SECTION_HEADING}' row {index} has fewer than 4 columns")
            continue
        surface = strip_code_ticks(cells[0])
        field = strip_code_ticks(cells[1])
        rel_file = strip_code_ticks(cells[2])
        test_fn = strip_code_ticks(cells[3])
        if not rel_file or not test_fn:
            errors.append(f"{POLICY_REL_PATH}: '{DEPRECATION_SECTION_HEADING}' row {index} has empty file or test name")
            continue
        guards.append(DeprecationGuard(surface=surface, field=field, rel_file=rel_file, test_fn=test_fn))

    return guards, errors


def parse_rust_int(raw: str) -> int:
    return int(raw.replace("_", ""), 0)


def extract_constants(rust_source: str) -> dict[str, list[str]]:
    """Return mapping of symbol name (both bare `CONST` and `Type::CONST` inside impl) -> raw values."""
    found: dict[str, list[str]] = {}
    current_impl: str | None = None
    impl_brace_depth = 0
    brace_depth = 0

    for line in rust_source.splitlines():
        code_part = line.split("//", 1)[0]
        if current_impl is None:
            impl_match = IMPL_HEADER_RE.match(code_part)
            if impl_match:
                current_impl = impl_match.group(1)
                impl_brace_depth = brace_depth

        const_match = CONST_DECL_RE.match(code_part)
        if const_match:
            name, value = const_match.group(1), const_match.group(2)
            found.setdefault(name, []).append(value)
            if current_impl is not None and brace_depth > impl_brace_depth:
                found.setdefault(f"{current_impl}::{name}", []).append(value)

        brace_depth += code_part.count("{") - code_part.count("}")
        if current_impl is not None and brace_depth <= impl_brace_depth and "{" in code_part:
            # Single-line impl block ended on the same line.
            current_impl = None
        elif current_impl is not None and brace_depth <= impl_brace_depth and "}" in code_part:
            current_impl = None

    return found


def check_repo(root: Path) -> list[str]:
    failures: list[str] = []
    policy_path = root / POLICY_REL_PATH
    if not policy_path.is_file():
        return [f"missing KMS format version policy document: {POLICY_REL_PATH}"]

    policy_text = policy_path.read_text(encoding="utf-8")
    anchors, anchor_errors = parse_version_anchors(policy_text)
    guards, guard_errors = parse_deprecation_guards(policy_text)
    failures.extend(anchor_errors)
    failures.extend(guard_errors)

    file_cache: dict[str, str] = {}
    const_cache: dict[str, dict[str, list[str]]] = {}
    documented_const_keys: set[tuple[str, str]] = set()

    for anchor in anchors:
        src_path = root / anchor.rel_file
        if not src_path.is_file():
            failures.append(f"Version Anchor {anchor.symbol} cites missing file: {anchor.rel_file}")
            continue
        if anchor.rel_file not in file_cache:
            file_cache[anchor.rel_file] = src_path.read_text(encoding="utf-8")
            const_cache[anchor.rel_file] = extract_constants(file_cache[anchor.rel_file])

        try:
            expected_int = parse_rust_int(anchor.value_raw)
        except ValueError:
            failures.append(
                f"Version Anchor {anchor.rel_file}::{anchor.symbol} has non-integer documented value {anchor.value_raw!r}"
            )
            continue

        matches = const_cache[anchor.rel_file].get(anchor.symbol, [])
        if not matches:
            failures.append(f"Version Anchor constant {anchor.symbol} not found in {anchor.rel_file}")
            continue
        if len(matches) > 1:
            failures.append(
                f"Version Anchor constant {anchor.symbol} is declared {len(matches)} times in {anchor.rel_file}"
            )
            continue

        actual_raw = matches[0]
        actual_int = parse_rust_int(actual_raw)
        if actual_int != expected_int:
            failures.append(
                f"Version Anchor mismatch for {anchor.rel_file}::{anchor.symbol}: "
                f"policy documents {anchor.value_raw} ({expected_int}), source defines {actual_raw} ({actual_int})"
            )

        bare_name = anchor.symbol.split("::")[-1]
        documented_const_keys.add((anchor.rel_file, anchor.symbol))
        documented_const_keys.add((anchor.rel_file, bare_name))

    # Reverse check: any tracked KMS/SSE format version constant in source must be documented.
    for scan_dir_rel in TRACKED_SCAN_DIRS:
        scan_dir = root / scan_dir_rel
        if not scan_dir.is_dir():
            continue
        for rs_file in sorted(scan_dir.rglob("*.rs")):
            rel_posix = rs_file.relative_to(root).as_posix()
            if rel_posix not in file_cache:
                file_cache[rel_posix] = rs_file.read_text(encoding="utf-8")
                const_cache[rel_posix] = extract_constants(file_cache[rel_posix])
            for symbol in sorted(const_cache[rel_posix]):
                if "::" in symbol:
                    continue
                if TRACKED_CONST_NAME_RE.match(symbol) and (rel_posix, symbol) not in documented_const_keys:
                    failures.append(
                        f"undocumented KMS/SSE format version constant {symbol} in {rel_posix}; "
                        f"register it in {POLICY_REL_PATH} under '{VERSION_SECTION_HEADING}'"
                    )

    # Check every active deprecation guard test exists in source.
    for guard in guards:
        src_path = root / guard.rel_file
        if not src_path.is_file():
            failures.append(f"Deprecation guard {guard.test_fn} cites missing file: {guard.rel_file}")
            continue
        if guard.rel_file not in file_cache:
            file_cache[guard.rel_file] = src_path.read_text(encoding="utf-8")
        pattern = re.compile(TEST_FN_RE_TEMPLATE.format(name=re.escape(guard.test_fn)), re.MULTILINE)
        if not pattern.search(file_cache[guard.rel_file]):
            failures.append(
                f"Deprecation guard test '{guard.test_fn}' for field '{guard.field}' not found in {guard.rel_file}"
            )

    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Repository root directory (defaults to this script's repository root).",
    )
    args = parser.parse_args(argv)

    failures = check_repo(args.root)
    if failures:
        for failure in failures:
            print(f"KMS format version policy check failed: {failure}", file=sys.stderr)
        return 1

    print("KMS format version policy check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
