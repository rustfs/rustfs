#!/usr/bin/env python3
"""Move s3s error-type references onto rustfs-s3-types, one crate closure at a time.

Task T1.2 of rustfs/backlog#2734 (spec rustfs/backlog#2743). rustfs-s3-types
offers `S3Error`, `S3ErrorCode`, `S3Result` and the `s3_error!` macro with the
same call syntax as s3s, so the switch is a path rewrite and never a call-site
rewrite:

  * `use s3s::{S3ErrorCode, dto::X};` becomes `use rustfs_s3_types::S3ErrorCode;`
    plus `use s3s::dto::X;` (attributes and visibility are kept on both);
  * a qualified path in code (`s3s::S3ErrorCode::Foo`, `s3s::s3_error!(...)`)
    gets the `rustfs_s3_types::` prefix;
  * the manifest of a crate the run rewrote gains `rustfs-s3-types = { workspace
    = true }` in [dependencies] when absent there, and loses its `s3s`
    dependency only when no source file of the crate names s3s any more and no
    feature mentions it. A crate the run did not rewrite keeps its manifest.

Comments and string or character literals are never rewritten; references left
there are counted in the census. A use tree that holds a comment or a literal
is not rewritten either: it is reported and the run exits 1.

Usage:
  scripts/codemods/s3_error_to_s3_types.py [--hold FILE]... [--no-fmt] CRATE_DIR...
  scripts/codemods/s3_error_to_s3_types.py --self-test

CRATE_DIR is a repository-relative package directory, or a directory inside
one (for example rustfs/src/admin): only the Rust files below it are rewritten,
while the manifest decisions read the whole package. Files of nested packages
are left to their own package.

The run is idempotent: a second run over the same arguments changes nothing
and prints a census of zero changed files.

Files matched by .config/s3s-edge-allowlist.txt (the s3s edge that keeps s3s
until the flip) are refused: never rewritten, and listed in the census. A
missing allowlist is an error, never an empty list.

--hold FILE leaves one file untouched for this run and lists it in the census
with its remaining references. It is for a file whose s3s error type is fixed
by a crate outside this closure (for example a field of another crate's public
struct that the file compares against); the hold is visible in the census so it
cannot be forgotten.

Unless --no-fmt is given, rustfmt formats each rewritten file afterwards, so the
split imports land where rustfmt sorts them.

Exit status: 0 when every non-refused, non-held file is free of s3s error-type
references in code; 1 when references remain that the codemod could not
rewrite (listed); 2 on a usage error or a broken input.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path

NAMES = ("S3ErrorCode", "S3Error", "S3Result", "s3_error")
OLD_CRATE = "s3s"
NEW_CRATE = "rustfs_s3_types"
NEW_DEP_KEY = "rustfs-s3-types"
NEW_DEP_LINE = "rustfs-s3-types = { workspace = true }"
ALLOWLIST = ".config/s3s-edge-allowlist.txt"

CODE, COMMENT, LITERAL = 0, 1, 2

IDENT_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
CHAR_LITERAL = re.compile(r"'(?:\\(?:[nrt\\0'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\})|[^\\'\n])'")
RAW_STRING_START = re.compile(r'r(#*)"')
USE_START = re.compile(
    r"(?m)^(?P<indent>[ \t]*)(?P<attrs>(?:#\[[^\]\n]*\][ \t]*)*)(?P<vis>(?:pub(?:[ \t]*\([^)\n]*\))?[ \t]+)?)"
    r"use[ \t]+(?P<lead>::)?" + OLD_CRATE + r"::"
)
NAME_PATTERN = "|".join(sorted(NAMES, key=len, reverse=True))
QUALIFIED = re.compile(r"(?<![A-Za-z0-9_])" + OLD_CRATE + r"::(?P<name>" + NAME_PATTERN + r")(?![A-Za-z0-9_])")
OLD_CRATE_PATH = re.compile(r"(?<![A-Za-z0-9_])" + OLD_CRATE + r"(?:::|[ \t]*;|[ \t]+as[ \t])")
NEW_CRATE_PATH = re.compile(r"(?<![A-Za-z0-9_])" + NEW_CRATE + r"::")
ATTR_LINE = re.compile(r"^[ \t]*#\[[^\n]*\][ \t]*$")
TOML_KEY = re.compile(r"^(?P<key>[A-Za-z0-9_-]+)(?:\.[A-Za-z0-9_.-]+)?[ \t]*=")


class UsageError(Exception):
    """A broken argument or input: exit 2."""


def classify(text: str) -> bytearray:
    """Return one kind byte per character: CODE, COMMENT or LITERAL."""
    kinds = bytearray(len(text))
    i, n = 0, len(text)

    def mark(start: int, end: int, kind: int) -> None:
        kinds[start:end] = bytes([kind]) * (end - start)

    while i < n:
        ch = text[i]
        if text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end < 0 else end
            mark(i, end, COMMENT)
            i = end
        elif text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            mark(i, j, COMMENT)
            i = j
        elif ch == "r" and (i == 0 or text[i - 1] not in IDENT_CHARS or _is_byte_prefix(text, i - 1)):
            raw = RAW_STRING_START.match(text, i)
            if raw is None:
                i += 1
                continue
            closing = '"' + raw.group(1)
            end = text.find(closing, raw.end())
            end = n if end < 0 else end + len(closing)
            mark(i, end, LITERAL)
            i = end
        elif ch == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            mark(i, min(j + 1, n), LITERAL)
            i = j + 1
        elif ch == "'":
            literal = CHAR_LITERAL.match(text, i)
            if literal is None:  # a lifetime or a loop label
                i += 1
                continue
            mark(i, literal.end(), LITERAL)
            i = literal.end()
        else:
            i += 1
    return kinds


def _is_byte_prefix(text: str, index: int) -> bool:
    """True when text[index] is the `b` of a `br"..."` raw byte string."""
    return text[index] == "b" and (index == 0 or text[index - 1] not in IDENT_CHARS)


def split_top_level(inner: str) -> list[str]:
    items, depth, start = [], 0, 0
    for i, ch in enumerate(inner):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        elif ch == "," and depth == 0:
            items.append(inner[start:i])
            start = i + 1
    items.append(inner[start:])
    return [re.sub(r"\s+", " ", item).strip() for item in items if item.strip()]


def tree_items(tree: str) -> list[str]:
    tree = tree.strip()
    if tree.startswith("{") and tree.endswith("}") and _closing_brace(tree, 0) == len(tree) - 1:
        return split_top_level(tree[1:-1])
    return [re.sub(r"\s+", " ", tree)]


def _closing_brace(text: str, open_index: int) -> int:
    depth = 0
    for i in range(open_index, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return i
    return -1


def item_head(item: str) -> str:
    match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", item)
    return match.group(0) if match else ""


def render_tree(items: list[str]) -> str:
    return items[0] if len(items) == 1 else "{" + ", ".join(items) + "}"


@dataclass
class FileResult:
    text: str
    use_statements: int = 0
    qualified_paths: int = 0
    in_comments: int = 0
    in_literals: int = 0
    unsupported: list[int] = field(default_factory=list)


def rewrite(text: str) -> FileResult:
    """Rewrite one Rust source; pure, so a second pass over its output is a no-op."""
    kinds = classify(text)
    out: list[str] = []
    cursor = 0
    result = FileResult(text)
    skipped_spans: list[tuple[int, int]] = []

    for match in USE_START.finditer(text):
        start = match.start("indent")
        use_keyword = match.start("vis") + len(match.group("vis"))
        if start < cursor or kinds[use_keyword] != CODE:
            continue
        tree_start = match.end()
        end, depth = tree_start, 0
        while end < len(text):
            if kinds[end] == CODE:
                if text[end] == "{":
                    depth += 1
                elif text[end] == "}":
                    depth -= 1
                elif text[end] == ";" and depth == 0:
                    break
            end += 1
        if end >= len(text):
            continue
        items = tree_items(code_only(text, kinds, tree_start, end))
        moved = [item for item in items if item_head(item) in NAMES]
        if not moved:
            continue
        if any(kind != CODE for kind in kinds[tree_start:end]):
            result.unsupported.append(text.count("\n", 0, start) + 1)
            skipped_spans.append((start, end))
            continue
        kept = [item for item in items if item_head(item) not in NAMES]
        indent, vis, lead = match.group("indent"), match.group("vis"), match.group("lead") or ""
        attrs = match.group("attrs")
        preceding = _preceding_attribute_lines(text, start)
        statements = [f"{indent}{attrs}{vis}use {lead}{NEW_CRATE}::{render_tree(moved)};"]
        if kept:
            statements.append(f"{preceding}{indent}{attrs}{vis}use {lead}{OLD_CRATE}::{render_tree(kept)};")
        out.append(text[cursor:start])
        out.append("\n".join(statements))
        cursor = end + 1
        result.use_statements += 1
    out.append(text[cursor:])
    text = "".join(out)

    kinds = classify(text)
    skipped_spans = [(a, b) for a, b in _respan(result.text, text, skipped_spans)]
    pieces: list[str] = []
    cursor = 0
    for match in QUALIFIED.finditer(text):
        start = match.start()
        if _is_nested_module_path(text, start) or any(a <= start < b for a, b in skipped_spans):
            continue
        if kinds[start] == COMMENT:
            result.in_comments += 1
            continue
        if kinds[start] == LITERAL:
            result.in_literals += 1
            continue
        pieces.append(text[cursor:start])
        pieces.append(f"{NEW_CRATE}::{match.group('name')}")
        cursor = match.end()
        result.qualified_paths += 1
    pieces.append(text[cursor:])
    result.text = "".join(pieces)
    return result


def code_only(text: str, kinds: bytearray, start: int, end: int) -> str:
    """text[start:end] with every comment and literal character blanked out."""
    return "".join(ch if kinds[i] == CODE else " " for i, ch in enumerate(text[start:end], start))


def _respan(before: str, after: str, spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Re-locate skipped use statements after earlier statements changed length."""
    located = []
    for start, end in spans:
        snippet = before[start:end]
        index = after.find(snippet)
        if index >= 0:
            located.append((index, index + len(snippet)))
    return located


def _preceding_attribute_lines(text: str, start: int) -> str:
    """The contiguous `#[...]` lines right above `start`, to repeat on a split."""
    lines: list[str] = []
    cursor = start
    while cursor > 0:
        line_start = text.rfind("\n", 0, cursor - 1) + 1
        line = text[line_start : cursor - 1] if text[cursor - 1] == "\n" else ""
        if not line or not ATTR_LINE.match(line):
            break
        lines.insert(0, line + "\n")
        cursor = line_start
    return "".join(lines)


def _is_nested_module_path(text: str, start: int) -> bool:
    """`foo::s3s::S3Error` names a module called s3s, not the crate."""
    if start >= 2 and text[start - 2 : start] == "::":
        before = start - 2
        return before > 0 and text[before - 1] in IDENT_CHARS
    return False


def remaining_lines(text: str) -> list[int]:
    """Lines whose code still names one of NAMES through the s3s crate."""
    kinds = classify(text)
    lines = set()
    for match in QUALIFIED.finditer(text):
        if kinds[match.start()] == CODE and not _is_nested_module_path(text, match.start()):
            lines.add(text.count("\n", 0, match.start()) + 1)
    for match in USE_START.finditer(text):
        if kinds[match.end() - 1] != CODE:
            continue
        end = match.end()
        depth = 0
        while end < len(text) and not (kinds[end] == CODE and text[end] == ";" and depth == 0):
            if kinds[end] == CODE:
                depth += {"{": 1, "}": -1}.get(text[end], 0)
            end += 1
        if any(item_head(item) in NAMES for item in tree_items(code_only(text, kinds, match.end(), end))):
            lines.add(text.count("\n", 0, match.start()) + 1)
    return sorted(lines)


def names_crate(text: str, pattern: re.Pattern[str]) -> bool:
    kinds = classify(text)
    return any(kinds[match.start()] == CODE for match in pattern.finditer(text))


# ---------------------------------------------------------------------------
# Repository inputs


def git_files(root: Path, *pathspecs: str) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--", *pathspecs],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise UsageError(f"git ls-files {' '.join(pathspecs)} failed: {result.stderr.decode().strip()}")
    return sorted(path for path in result.stdout.decode().split("\0") if path)


def allowlisted_files(root: Path) -> set[str]:
    path = root / ALLOWLIST
    if not path.is_file():
        raise UsageError(f"allowlist '{ALLOWLIST}' is missing; refusing to run without it")
    files: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        entry = raw.strip()
        if not entry or entry.startswith("#"):
            continue
        files.update(git_files(root, f":(glob){entry}"))
    return files


def package_of(root: Path, directory: str) -> str:
    """The nearest directory at or above `directory` whose Cargo.toml has [package]."""
    path = Path(directory)
    while True:
        manifest = root / path / "Cargo.toml"
        if manifest.is_file() and re.search(r"(?m)^\[package\]", manifest.read_text(encoding="utf-8")):
            return path.as_posix()
        if path == Path("."):
            raise UsageError(f"'{directory}' is not a directory inside a package (no [package] manifest above it)")
        path = path.parent


def package_sources(root: Path, package: str, under: str) -> list[str]:
    """Tracked Rust files below `under` that belong to `package`, not to a nested package."""
    prefix = "" if package == "." else package + "/"
    files = git_files(root, f":(glob){under}/**/*.rs")
    nested = {
        str(Path(manifest).parent)
        for manifest in git_files(root, f":(glob){prefix}**/Cargo.toml")
        if str(Path(manifest).parent) != package
    }
    return [path for path in files if not any(path.startswith(directory + "/") for directory in nested)]


# ---------------------------------------------------------------------------
# Manifest edits


def _dependency_entries(lines: list[str]) -> tuple[int, list[tuple[str, int, int]]]:
    """Return the [dependencies] header index and (key, first, last) line spans."""
    try:
        header = next(i for i, line in enumerate(lines) if line.strip() == "[dependencies]")
    except StopIteration:
        return -1, []
    entries = []
    i = header + 1
    while i < len(lines) and not lines[i].lstrip().startswith("["):
        key = TOML_KEY.match(lines[i])
        if key is None:
            i += 1
            continue
        first, balance = i, 0
        while True:
            balance += _bracket_balance(lines[i])
            if balance <= 0 or i + 1 >= len(lines):
                break
            i += 1
        entries.append((key.group("key"), first, i))
        i += 1
    return header, entries


def _bracket_balance(line: str) -> int:
    balance, in_string = 0, False
    for ch in line.split("#", 1)[0] if '"' not in line else line:
        if ch == '"':
            in_string = not in_string
        elif not in_string and ch in "{[":
            balance += 1
        elif not in_string and ch in "}]":
            balance -= 1
    return balance


def edit_manifest(text: str, needs_new: bool, may_drop_old: bool) -> tuple[str, list[str]]:
    lines = text.splitlines(keepends=True)
    notes: list[str] = []
    header, entries = _dependency_entries(lines)
    if header < 0:
        if needs_new:
            raise UsageError("manifest has no [dependencies] table to add rustfs-s3-types to")
        return text, notes
    keys = {key for key, _, _ in entries}
    if may_drop_old and OLD_CRATE in keys:
        if re.search(r"(?m)^\s*[^#\n]*\b" + OLD_CRATE + r"\b", _features_section(text)):
            notes.append("kept s3s: a [features] entry mentions it")
        else:
            _, first, last = next(entry for entry in entries if entry[0] == OLD_CRATE)
            del lines[first : last + 1]
            notes.append("removed s3s")
            header, entries = _dependency_entries(lines)
    if needs_new and NEW_DEP_KEY not in keys:
        rustfs = [entry for entry in entries if entry[0].startswith("rustfs-")]
        before = [entry for entry in rustfs if entry[0] < NEW_DEP_KEY]
        if before:
            position = before[-1][2] + 1
        elif rustfs:
            position = rustfs[0][1]
        else:
            position = header + 1
        lines.insert(position, NEW_DEP_LINE + "\n")
        notes.append("added rustfs-s3-types")
    return "".join(lines), notes


def _features_section(text: str) -> str:
    match = re.search(r"(?ms)^\[features\]\s*$(.*?)(?=^\[|\Z)", text)
    return match.group(1) if match else ""


# ---------------------------------------------------------------------------
# Driver


@dataclass
class CrateCensus:
    crate: str
    changed: list[str] = field(default_factory=list)
    held: list[tuple[str, int]] = field(default_factory=list)
    refused: list[tuple[str, int]] = field(default_factory=list)
    use_statements: int = 0
    qualified_paths: int = 0
    in_comments: int = 0
    in_literals: int = 0
    remaining: list[str] = field(default_factory=list)
    manifest: list[str] = field(default_factory=list)


def run(root: Path, crates: list[str], holds: list[str], fmt: bool = True, out=sys.stdout) -> int:
    crates = [crate.rstrip("/") for crate in crates]
    packages: dict[str, str] = {}
    for crate in crates:
        if Path(crate).is_absolute() or ".." in Path(crate).parts:
            raise UsageError(f"crate directory '{crate}' must be repository-relative")
        if not (root / crate).is_dir():
            raise UsageError(f"'{crate}' is not a directory inside a package (no such directory)")
        packages[crate] = package_of(root, crate)
    refused_files = allowlisted_files(root)
    sources = {crate: package_sources(root, packages[crate], crate) for crate in crates}
    hold_set = set(holds)
    for hold in hold_set:
        if not any(hold in files for files in sources.values()):
            raise UsageError(f"--hold '{hold}' is not a tracked Rust file of the given crates")

    censuses: list[CrateCensus] = []
    rewritten: list[Path] = []
    for crate in crates:
        census = CrateCensus(crate)
        for relative in sources[crate]:
            path = root / relative
            text = path.read_text(encoding="utf-8")
            if relative in refused_files or relative in hold_set:
                bucket = census.refused if relative in refused_files else census.held
                left = remaining_lines(text)
                if left:
                    bucket.append((relative, len(left)))
                continue
            result = rewrite(text)
            census.use_statements += result.use_statements
            census.qualified_paths += result.qualified_paths
            census.in_comments += result.in_comments
            census.in_literals += result.in_literals
            census.remaining += [f"{relative}:{line}" for line in remaining_lines(result.text)]
            if result.text != text:
                path.write_text(result.text, encoding="utf-8")
                census.changed.append(relative)
                rewritten.append(path)
        # A crate this run did not rewrite keeps its manifest: a dev-only use of
        # rustfs-s3-types or an s3s entry kept for other reasons is not ours to edit.
        # Whether the package still names either crate is read from all of its
        # sources, not only from the directory this run was given.
        if census.changed:
            package = packages[crate]
            texts = [(root / f).read_text(encoding="utf-8") for f in package_sources(root, package, package)]
            crate_names_old = any(names_crate(text, OLD_CRATE_PATH) for text in texts)
            crate_names_new = any(names_crate(text, NEW_CRATE_PATH) for text in texts)
            manifest_path = root / package / "Cargo.toml"
            manifest = manifest_path.read_text(encoding="utf-8")
            edited, census.manifest = edit_manifest(manifest, crate_names_new, not crate_names_old)
            if edited != manifest:
                manifest_path.write_text(edited, encoding="utf-8")
        censuses.append(census)

    if fmt and rewritten:
        edition = _workspace_edition(root)
        result = subprocess.run(["rustfmt", "--edition", edition, *map(str, rewritten)], check=False)
        if result.returncode != 0:
            raise UsageError("rustfmt failed on the rewritten files")

    return _print_census(censuses, out)


def _workspace_edition(root: Path) -> str:
    match = re.search(r'(?m)^edition\s*=\s*"(\d{4})"', (root / "Cargo.toml").read_text(encoding="utf-8"))
    return match.group(1) if match else "2024"


def _print_census(censuses: list[CrateCensus], out) -> int:
    total = 0
    remaining = 0
    for census in censuses:
        total += len(census.changed)
        remaining += len(census.remaining)
        manifest = ", ".join(census.manifest) or "unchanged"
        print(
            f"{census.crate}: {len(census.changed)} files changed "
            f"({census.use_statements} use statements, {census.qualified_paths} qualified paths); "
            f"manifest {manifest}; {len(census.held)} held, {len(census.refused)} refused by the allowlist; "
            f"left in comments {census.in_comments}, in literals {census.in_literals}",
            file=out,
        )
        for path in census.changed:
            print(f"  changed  {path}", file=out)
        for path, count in census.held:
            print(f"  held     {path} ({count} lines still name s3s error types)", file=out)
        for path, count in census.refused:
            print(f"  refused  {path} ({count} lines still name s3s error types)", file=out)
        for location in census.remaining:
            print(f"  REMAINS  {location}", file=out)
    print(f"total: {total} files changed in {len(censuses)} crates", file=out)
    if remaining:
        print(f"error: {remaining} s3s error-type references could not be rewritten (REMAINS above)", file=out)
        return 1
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("crates", nargs="+", metavar="CRATE_DIR")
    parser.add_argument("--hold", action="append", default=[], metavar="FILE")
    parser.add_argument("--no-fmt", action="store_true")
    args = parser.parse_args(argv)
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False)
    if top.returncode != 0:
        print("error: not inside a git checkout", file=sys.stderr)
        return 2
    try:
        return run(Path(top.stdout.strip()), args.crates, args.hold, fmt=not args.no_fmt)
    except UsageError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


# ---------------------------------------------------------------------------
# Self-test


class SelfTest(unittest.TestCase):
    def rewritten(self, source: str) -> str:
        return rewrite(source).text

    # -- positive cases --

    def test_single_import_moves(self):
        self.assertEqual(self.rewritten("use s3s::S3ErrorCode;\n"), "use rustfs_s3_types::S3ErrorCode;\n")

    def test_mixed_brace_import_splits_and_keeps_the_rest_on_s3s(self):
        source = "use s3s::{\n    S3ErrorCode,\n    dto::{A, B},\n};\nfn f() {}\n"
        self.assertEqual(
            self.rewritten(source),
            "use rustfs_s3_types::S3ErrorCode;\nuse s3s::dto::{A, B};\nfn f() {}\n",
        )

    def test_every_name_moves_together_with_aliases_and_variant_paths(self):
        source = "pub(crate) use s3s::{Body, S3Error as E, S3ErrorCode::InvalidRequest, S3Result, s3_error};\n"
        self.assertEqual(
            self.rewritten(source),
            "pub(crate) use rustfs_s3_types::{S3Error as E, S3ErrorCode::InvalidRequest, S3Result, s3_error};\n"
            "pub(crate) use s3s::Body;\n",
        )

    def test_qualified_paths_in_code_move(self):
        source = "fn f() -> s3s::S3Result<()> { Err(s3s::s3_error!(NoSuchKey, \"x\")) }\nlet c = s3s::S3ErrorCode::Foo;\n"
        self.assertEqual(
            self.rewritten(source),
            "fn f() -> rustfs_s3_types::S3Result<()> { Err(rustfs_s3_types::s3_error!(NoSuchKey, \"x\")) }\n"
            "let c = rustfs_s3_types::S3ErrorCode::Foo;\n",
        )

    def test_attributes_are_repeated_on_both_halves_of_a_split(self):
        source = "#[cfg(test)]\nuse s3s::{S3Error, dto::X};\n#[cfg(test)] use s3s::{S3Result, Body};\n"
        self.assertEqual(
            self.rewritten(source),
            "#[cfg(test)]\nuse rustfs_s3_types::S3Error;\n#[cfg(test)]\nuse s3s::dto::X;\n"
            "#[cfg(test)] use rustfs_s3_types::S3Result;\n#[cfg(test)] use s3s::Body;\n",
        )

    def test_second_pass_is_a_no_op(self):
        source = "use s3s::{S3ErrorCode, dto::X};\nfn f() { let _ = s3s::S3ErrorCode::A; }\n"
        once = self.rewritten(source)
        self.assertNotEqual(once, source)
        twice = rewrite(once)
        self.assertEqual(twice.text, once)
        self.assertEqual((twice.use_statements, twice.qualified_paths), (0, 0))

    # -- negative cases --

    def test_comments_are_counted_not_rewritten(self):
        source = "// s3s::S3Error\n/* nested /* s3s::S3ErrorCode */ s3s::s3_error */\n/// [`s3s::S3Result`]\n"
        result = rewrite(source)
        self.assertEqual(result.text, source)
        self.assertEqual(result.in_comments, 4)

    def test_string_and_char_literals_are_counted_not_rewritten(self):
        source = (
            'const A: &str = "use s3s::S3Error;";\n'
            'const B: &str = r#"s3s::S3ErrorCode "quoted" "#;\n'
            "const C: &[u8] = br\"s3s::S3Result\";\n"
            'const D: &str = "esc \\" s3s::s3_error";\n'
        )
        result = rewrite(source)
        self.assertEqual(result.text, source)
        self.assertEqual(result.in_literals, 4)

    def test_lifetimes_do_not_open_a_literal(self):
        source = "fn f<'a>(x: &'a str) -> &'a s3s::S3Error { let c = '\\''; let d = 's'; s3s::s3_error!(A) }\n"
        self.assertEqual(
            self.rewritten(source),
            "fn f<'a>(x: &'a str) -> &'a rustfs_s3_types::S3Error { let c = '\\''; let d = 's'; rustfs_s3_types::s3_error!(A) }\n",
        )

    def test_other_s3s_items_are_untouched(self):
        source = "use s3s::dto::X;\nuse s3s::{Body, S3Request};\nuse s3s::S3ErrorCodeExt;\nuse s3s::header::S3Error;\n"
        self.assertEqual(self.rewritten(source), source)

    def test_paths_that_are_not_the_s3s_crate_are_untouched(self):
        source = "let a = foo::s3s::S3Error::new();\nlet b = my_s3s::S3Error;\nlet c = s3s::S3Errors;\nlet d = xs3s::S3ErrorCode::A;\n"
        self.assertEqual(self.rewritten(source), source)

    def test_a_use_tree_holding_a_comment_is_reported_not_rewritten(self):
        source = "use s3s::{\n    // why\n    S3Error,\n    Body,\n};\n"
        result = rewrite(source)
        self.assertEqual(result.text, source)
        self.assertEqual(result.unsupported, [1])
        self.assertEqual(remaining_lines(result.text), [1])

    def test_remaining_lines_ignores_comments_literals_and_moved_code(self):
        source = '// s3s::S3Error\nconst A: &str = "s3s::S3Error";\nuse rustfs_s3_types::S3Error;\nuse s3s::dto::X;\n'
        self.assertEqual(remaining_lines(source), [])

    def test_manifest_keeps_s3s_when_a_feature_mentions_it(self):
        manifest = '[dependencies]\ns3s = { workspace = true }\n\n[features]\nminio = ["s3s/minio"]\n'
        edited, notes = edit_manifest(manifest, needs_new=False, may_drop_old=True)
        self.assertEqual(edited, manifest)
        self.assertEqual(notes, ["kept s3s: a [features] entry mentions it"])

    def test_manifest_without_a_new_reference_gains_nothing(self):
        manifest = "[dependencies]\nrustfs-utils.workspace = true\ns3s = { workspace = true }\n"
        self.assertEqual(edit_manifest(manifest, needs_new=False, may_drop_old=False), (manifest, []))

    def test_manifest_with_the_dependency_already_present_is_unchanged(self):
        manifest = "[dependencies]\nrustfs-s3-types.workspace = true\n"
        self.assertEqual(edit_manifest(manifest, needs_new=True, may_drop_old=False), (manifest, []))

    def test_manifest_inserts_in_rustfs_order_and_drops_a_multi_line_s3s(self):
        manifest = (
            "[dependencies]\nrustfs-config.workspace = true\nrustfs-utils = { workspace = true }\n"
            's3s = { workspace = true, features = [\n    "minio",\n] }\ntokio.workspace = true\n\n[dev-dependencies]\ns3s.workspace = true\n'
        )
        edited, notes = edit_manifest(manifest, needs_new=True, may_drop_old=True)
        self.assertEqual(
            edited,
            "[dependencies]\nrustfs-config.workspace = true\nrustfs-s3-types = { workspace = true }\n"
            "rustfs-utils = { workspace = true }\ntokio.workspace = true\n\n[dev-dependencies]\ns3s.workspace = true\n",
        )
        self.assertEqual(notes, ["removed s3s", "added rustfs-s3-types"])

    # -- the driver against a scratch repository --

    def scratch_repo(self, files: dict[str, str]) -> Path:
        tmp = Path(tempfile.mkdtemp(prefix="s3-error-codemod-"))
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        for relative, text in files.items():
            (tmp / relative).parent.mkdir(parents=True, exist_ok=True)
            (tmp / relative).write_text(text, encoding="utf-8")
        subprocess.run(["git", "init", "-q", str(tmp)], check=True)
        subprocess.run(["git", "-C", str(tmp), "add", "-A"], check=True)
        return tmp

    def base_files(self) -> dict[str, str]:
        return {
            ALLOWLIST: "# edge\ncrates/edge/src/edge.rs\n",
            "crates/a/Cargo.toml": "[package]\nname = \"a\"\n\n[dependencies]\ns3s.workspace = true\n",
            "crates/a/src/lib.rs": "use s3s::S3Error;\npub fn f() -> S3Error { todo!() }\n",
            "crates/a/src/held.rs": "use s3s::S3ErrorCode;\n",
            "crates/a/nested/Cargo.toml": "[package]\nname = \"nested\"\n",
            "crates/a/nested/src/lib.rs": "use s3s::S3Error;\n",
            "crates/b/Cargo.toml": "[package]\nname = \"b\"\n\n[dependencies]\ns3s.workspace = true\n",
            "crates/b/src/lib.rs": "use s3s::{S3Result, dto::X};\n",
            "crates/edge/Cargo.toml": "[package]\nname = \"edge\"\n\n[dependencies]\ns3s.workspace = true\n",
            "crates/edge/src/edge.rs": "use s3s::S3Error;\n",
            "Cargo.toml": "[workspace]\nmembers = [\"crates/*\"]\n",
            "docs/README.md": "docs\n",
        }

    def test_driver_rewrites_counts_and_is_idempotent(self):
        root = self.scratch_repo(self.base_files())
        first = _Capture()
        status = run(root, ["crates/a", "crates/b", "crates/edge"], ["crates/a/src/held.rs"], fmt=False, out=first)
        self.assertEqual(status, 0, first.text)
        self.assertIn("crates/a: 1 files changed (1 use statements, 0 qualified paths); manifest added rustfs-s3-types; 1 held", first.text)
        self.assertIn("crates/b: 1 files changed", first.text)
        self.assertIn("refused  crates/edge/src/edge.rs (1 lines", first.text)
        self.assertIn("total: 2 files changed in 3 crates", first.text)
        self.assertEqual((root / "crates/a/src/held.rs").read_text(), "use s3s::S3ErrorCode;\n")
        self.assertEqual((root / "crates/a/nested/src/lib.rs").read_text(), "use s3s::S3Error;\n")
        self.assertEqual((root / "crates/edge/src/edge.rs").read_text(), "use s3s::S3Error;\n")
        self.assertIn("s3s.workspace = true", (root / "crates/a/Cargo.toml").read_text())
        self.assertIn("s3s.workspace = true", (root / "crates/b/Cargo.toml").read_text())
        second = _Capture()
        self.assertEqual(run(root, ["crates/a", "crates/b", "crates/edge"], ["crates/a/src/held.rs"], fmt=False, out=second), 0)
        self.assertIn("total: 0 files changed in 3 crates", second.text)
        self.assertNotIn("manifest added", second.text)

    def test_driver_drops_s3s_once_the_crate_is_free_of_it(self):
        files = self.base_files()
        files["crates/a/src/held.rs"] = "pub struct H;\n"
        root = self.scratch_repo(files)
        out = _Capture()
        self.assertEqual(run(root, ["crates/a"], [], fmt=False, out=out), 0, out.text)
        self.assertIn("manifest removed s3s, added rustfs-s3-types", out.text)
        self.assertNotIn("s3s", (root / "crates/a/Cargo.toml").read_text())

    def test_driver_leaves_the_manifest_of_a_crate_it_did_not_rewrite(self):
        files = self.base_files()
        manifest = "[package]\nname = \"c\"\n\n[dependencies]\ns3s.workspace = true\n\n[dev-dependencies]\nrustfs-s3-types.workspace = true\n"
        files["crates/c/Cargo.toml"] = manifest
        files["crates/c/src/lib.rs"] = "#[cfg(test)]\nmod tests {\n    use rustfs_s3_types::EventName;\n}\n"
        root = self.scratch_repo(files)
        out = _Capture()
        self.assertEqual(run(root, ["crates/c"], [], fmt=False, out=out), 0, out.text)
        self.assertIn("crates/c: 0 files changed (0 use statements, 0 qualified paths); manifest unchanged", out.text)
        self.assertEqual((root / "crates/c/Cargo.toml").read_text(), manifest)

    def test_driver_rewrites_only_below_a_subdirectory_and_reads_the_whole_package(self):
        files = self.base_files()
        files["crates/a/src/admin/x.rs"] = "use s3s::S3Result;\n"
        files["crates/a/src/held.rs"] = "pub struct H;\n"
        root = self.scratch_repo(files)
        out = _Capture()
        self.assertEqual(run(root, ["crates/a/src/admin"], [], fmt=False, out=out), 0, out.text)
        self.assertIn("crates/a/src/admin: 1 files changed", out.text)
        self.assertIn("manifest added rustfs-s3-types;", out.text)
        self.assertEqual((root / "crates/a/src/admin/x.rs").read_text(), "use rustfs_s3_types::S3Result;\n")
        self.assertEqual((root / "crates/a/src/lib.rs").read_text(), files["crates/a/src/lib.rs"])
        self.assertIn("s3s.workspace = true", (root / "crates/a/Cargo.toml").read_text())

    def test_driver_exits_1_on_a_reference_it_cannot_rewrite(self):
        files = self.base_files()
        files["crates/b/src/lib.rs"] = "use s3s::{\n    // keep\n    S3Result,\n};\n"
        root = self.scratch_repo(files)
        out = _Capture()
        self.assertEqual(run(root, ["crates/b"], [], fmt=False, out=out), 1)
        self.assertIn("REMAINS  crates/b/src/lib.rs:1", out.text)

    def test_driver_refuses_a_missing_allowlist(self):
        files = self.base_files()
        del files[ALLOWLIST]
        root = self.scratch_repo(files)
        with self.assertRaisesRegex(UsageError, "allowlist"):
            run(root, ["crates/a"], [], fmt=False, out=_Capture())
        self.assertEqual((root / "crates/a/src/lib.rs").read_text(), self.base_files()["crates/a/src/lib.rs"])

    def test_driver_rejects_a_bad_crate_or_hold(self):
        root = self.scratch_repo(self.base_files())
        for crates, holds, message in (
            (["crates/missing"], [], "not a directory inside a package"),
            (["docs"], [], "not a directory inside a package"),
            (["../crates/a"], [], "repository-relative"),
            (["crates/a"], ["crates/b/src/lib.rs"], "not a tracked Rust file"),
        ):
            with self.subTest(crates=crates, holds=holds), self.assertRaisesRegex(UsageError, message):
                run(root, crates, holds, fmt=False, out=_Capture())
        self.assertEqual((root / "crates/a/src/lib.rs").read_text(), self.base_files()["crates/a/src/lib.rs"])


class _Capture:
    def __init__(self) -> None:
        self.text = ""

    def write(self, chunk: str) -> int:
        self.text += chunk
        return len(chunk)

    def flush(self) -> None:
        pass


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
