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
  * a child module file that calls `s3_error!` through a file-level
    `use super::*;` gets its own `use rustfs_s3_types::s3_error;` once the
    file declaring it imports that macro by name (listed as `glob`); when that
    parent does not, the child is left alone and listed as `GLOB`;
  * the manifest of a crate the run rewrote gains `rustfs-s3-types = { workspace
    = true }` in [dependencies] when absent there, and loses its `s3s`
    dependency only when no source file of the crate names s3s any more and no
    feature mentions it. A crate the run did not rewrite keeps its manifest.

Comments and string or character literals are never rewritten; references left
there are counted in the census. A use tree that holds a comment or a literal
is not rewritten either: it is reported and the run exits 1.

Usage:
  scripts/codemods/s3_error_to_s3_types.py [--hold FILE]... [--facade PATH]... [--no-fmt] CRATE_DIR...
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

--facade PATH names a crate-local module that re-exports s3s's error items
(for example crate::storage_api::site_replication::s3, kept in an allowlisted
file). Imports and qualified paths of the four names through it are rewritten
as if they named s3s, so code that reaches s3s only through that facade moves
too; its other items stay on the facade. The path is matched as written, so a
relative spelling (super::...) is only meaningful for the directory it is
valid in. Each facade is listed in the census.

Unless --no-fmt is given, rustfmt formats each rewritten file afterwards, so the
split imports land where rustfmt sorts them.

Exit status: 0 when every non-refused, non-held file is free of s3s error-type
references in code; 1 when references remain that the codemod could not
rewrite (listed); 2 on a usage error or a broken input.
"""

from __future__ import annotations

import argparse
import functools
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
NAME_PATTERN = "|".join(sorted(NAMES, key=len, reverse=True))
FACADE_PATH = re.compile(r"(?:crate|super|self)(?:::[A-Za-z_][A-Za-z0-9_]*)+")
OLD_CRATE_PATH = re.compile(r"(?<![A-Za-z0-9_])" + OLD_CRATE + r"(?:::|[ \t]*;|[ \t]+as[ \t])")
NEW_CRATE_PATH = re.compile(r"(?<![A-Za-z0-9_])" + NEW_CRATE + r"::")
ATTR_LINE = re.compile(r"^[ \t]*#\[[^\n]*\][ \t]*$")
TOML_KEY = re.compile(r"^(?P<key>[A-Za-z0-9_-]+)(?:\.[A-Za-z0-9_.-]+)?[ \t]*=")
_VIS = r"(?:pub(?:[ \t]*\([^)\n]*\))?[ \t]+)?"
_ATTRS = r"(?:#\[[^\]\n]*\][ \t]*)*"
MOD_BLOCK = re.compile(r"(?m)^[ \t]*" + _ATTRS + _VIS + r"(?P<mod>mod)[ \t]+[A-Za-z_][A-Za-z0-9_]*[ \t]*\{")
ANY_USE = re.compile(r"(?m)^[ \t]*" + _ATTRS + _VIS + r"(?P<use>use)[ \t]+")
GLOB_SUPER = re.compile(r"(?m)^(?P<indent>[ \t]*)(?P<use>use)[ \t]+super::\*[ \t]*;")
MACRO_CALL = re.compile(r"(?<![A-Za-z0-9_])s3_error[ \t]*!")
MACRO_NAME = re.compile(r"(?<![A-Za-z0-9_])s3_error(?![A-Za-z0-9_])")
MACRO_DEFINITION = re.compile(r"macro_rules![ \t]*s3_error(?![A-Za-z0-9_])")
# The import spellings scripts/check_s3s_footprint.sh accepts as clearing a file.
RUSTFS_MACRO_IMPORT = re.compile(
    r"(?m)^[ \t]*(?:pub(?:\([^)]*\))?[ \t]+)?(?P<use>use)[ \t]+(?:::)?"
    + NEW_CRATE
    + r"::(?:s3_error[ \t]*;|\{[^;]*\bs3_error\s*[,}][^;]*;)"
)


class UsageError(Exception):
    """A broken argument or input: exit 2."""


@functools.lru_cache(maxsize=None)
def _patterns(facades: tuple[str, ...]) -> tuple[re.Pattern[str], re.Pattern[str]]:
    """The use-statement and qualified-path patterns for s3s plus the given facades.

    A facade is a crate-local module path that re-exports s3s's error items
    (for example `crate::storage_api::site_replication::s3`); its paths are
    treated exactly like `s3s::` paths for the four names and nothing else.
    """
    roots = "|".join(re.escape(root) for root in sorted((OLD_CRATE, *facades), key=len, reverse=True))
    use_start = re.compile(
        r"(?m)^(?P<indent>[ \t]*)(?P<attrs>(?:#\[[^\]\n]*\][ \t]*)*)(?P<vis>(?:pub(?:[ \t]*\([^)\n]*\))?[ \t]+)?)"
        r"use[ \t]+(?P<lead>::)?(?P<root>" + roots + r")::"
    )
    qualified = re.compile(
        r"(?<![A-Za-z0-9_])(?P<root>" + roots + r")::(?P<name>" + NAME_PATTERN + r")(?![A-Za-z0-9_])"
    )
    return use_start, qualified


def check_facades(facades: list[str]) -> tuple[str, ...]:
    for facade in facades:
        if not FACADE_PATH.fullmatch(facade):
            raise UsageError(f"facade '{facade}' is not a crate-relative module path such as crate::a::s3")
    return tuple(facades)


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


def rewrite(text: str, facades: tuple[str, ...] = ()) -> FileResult:
    """Rewrite one Rust source; pure, so a second pass over its output is a no-op."""
    use_start, qualified = _patterns(facades)
    kinds = classify(text)
    out: list[str] = []
    cursor = 0
    result = FileResult(text)
    skipped_spans: list[tuple[int, int]] = []

    for match in use_start.finditer(text):
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
        indent, vis, lead, root = match.group("indent"), match.group("vis"), match.group("lead") or "", match.group("root")
        attrs = match.group("attrs")
        preceding = _preceding_attribute_lines(text, start)
        statements = [f"{indent}{attrs}{vis}use {lead}{NEW_CRATE}::{render_tree(moved)};"]
        if kept:
            statements.append(f"{preceding}{indent}{attrs}{vis}use {lead}{root}::{render_tree(kept)};")
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
    for match in qualified.finditer(text):
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


def remaining_lines(text: str, facades: tuple[str, ...] = ()) -> list[int]:
    """Lines whose code still names one of NAMES through the s3s crate or a given facade."""
    use_start, qualified = _patterns(facades)
    kinds = classify(text)
    lines = set()
    for match in qualified.finditer(text):
        if kinds[match.start()] == CODE and not _is_nested_module_path(text, match.start()):
            lines.add(text.count("\n", 0, match.start()) + 1)
    for match in use_start.finditer(text):
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
# Children that reach the macro through `use super::*`
#
# A file whose module is declared in a parent file and that opens with
# `use super::*;` takes the parent's private imports, the `s3_error` macro
# among them. Once the parent imports the macro from rustfs_s3_types, the
# child's calls resolve there too, but nothing in the child says so: the s3s
# footprint counter keeps counting those lines (fail closed), and a later edit
# to the parent's imports would silently move the child to whichever macro the
# parent names next. So the child gets its own `use rustfs_s3_types::s3_error;`,
# right after the glob that used to supply the macro. An explicit import
# shadows the glob, so the call sites resolve exactly as before.


@dataclass
class GlobChildResult:
    text: str
    inserted: int = 0
    dependent_calls: int = 0


def _module_blocks(text: str, kinds: bytearray) -> list[tuple[int, int]]:
    """(open brace, closing brace) of every inline `mod name { ... }` in code."""
    blocks = []
    for match in MOD_BLOCK.finditer(text):
        if kinds[match.start("mod")] != CODE:
            continue
        depth = 0
        for i in range(match.end() - 1, len(text)):
            if kinds[i] != CODE:
                continue
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    blocks.append((match.end() - 1, i))
                    break
    return blocks


def _outermost_block(position: int, blocks: list[tuple[int, int]]) -> tuple[int, int] | None:
    """The outermost inline module containing `position`; None at file level."""
    containing = [block for block in blocks if block[0] < position < block[1]]
    return min(containing) if containing else None


def _use_trees(text: str, kinds: bytearray) -> list[tuple[int, str]]:
    """(start, code-only tree text) of every `use` declaration in code."""
    trees = []
    for match in ANY_USE.finditer(text):
        if kinds[match.start("use")] != CODE:
            continue
        end = match.end()
        while end < len(text) and not (kinds[end] == CODE and text[end] == ";"):
            end += 1
        trees.append((match.start(), code_only(text, kinds, match.end(), end)))
    return trees


def imports_rustfs_macro_at_file_level(text: str) -> bool:
    """True when the file imports rustfs_s3_types's macro by name outside any inline module.

    The spellings are the ones the s3s footprint counter accepts as clearing a
    file, so a child given the import here is also cleared there.
    """
    kinds = classify(text)
    blocks = _module_blocks(text, kinds)
    return any(
        kinds[match.start("use")] == CODE and _outermost_block(match.start(), blocks) is None
        for match in RUSTFS_MACRO_IMPORT.finditer(text)
    )


def glob_child_import(child: str, parent: str) -> GlobChildResult:
    """Give `child` its own rustfs_s3_types macro import when it relies on `parent`'s through `use super::*`.

    Pure, so a second pass over its output is a no-op: the inserted import names
    the macro, and a file that names it is left alone. `dependent_calls` counts
    the unqualified `s3_error!` calls in code that can only resolve through the
    file-level glob; it is non-zero with no insertion when the parent does not
    import rustfs_s3_types's macro by name, which the driver reports.
    """
    result = GlobChildResult(child)
    kinds = classify(child)
    globs = [match for match in GLOB_SUPER.finditer(child) if kinds[match.start("use")] == CODE]
    blocks = _module_blocks(child, kinds)
    file_glob = next((match for match in globs if _outermost_block(match.start(), blocks) is None), None)
    if file_glob is None:
        return result
    if any(MACRO_NAME.search(tree) for _, tree in _use_trees(child, kinds)):
        return result
    if any(kinds[match.start()] == CODE for match in MACRO_DEFINITION.finditer(child)):
        return result
    calls = [
        match.start()
        for match in MACRO_CALL.finditer(child)
        if kinds[match.start()] == CODE and child[max(0, match.start() - 2) : match.start()] != "::"
    ]
    if not calls:
        return result
    # A call inside an inline module reaches the file-level glob only through
    # that module's own `use super::*`; without one it never resolved through
    # the parent, so it is not ours to anchor.
    module_globs = {}
    for match in globs:
        containing = [block for block in blocks if block[0] < match.start() < block[1]]
        if len(containing) == 1:
            module_globs[containing[0]] = match
    file_level_calls = [call for call in calls if _outermost_block(call, blocks) is None]
    module_calls: dict[tuple[int, int], int] = {}
    for call in calls:
        block = _outermost_block(call, blocks)
        if block is not None and block in module_globs:
            module_calls[block] = module_calls.get(block, 0) + 1
    result.dependent_calls = len(file_level_calls) + sum(module_calls.values())
    if not result.dependent_calls or not imports_rustfs_macro_at_file_level(parent):
        return result
    # Anchor at file level when file-level code calls the macro; otherwise in
    # each inline module that does, so a test-only module does not leave an
    # unused import in the non-test build.
    anchors = [file_glob] if file_level_calls else [module_globs[block] for block in sorted(module_calls)]
    pieces, cursor = [], 0
    for anchor in anchors:
        indent = anchor.group("indent")
        attrs = _preceding_attribute_lines(child, anchor.start())
        line_end = child.find("\n", anchor.end())
        line_end = len(child) if line_end < 0 else line_end
        pieces.append(child[cursor:line_end])
        pieces.append(f"\n{attrs}{indent}use {NEW_CRATE}::s3_error;")
        cursor = line_end
        result.inserted += 1
    pieces.append(child[cursor:])
    result.text = "".join(pieces)
    return result


def parent_module_file(root: Path, relative: str) -> str | None:
    """The tracked file that declares the module `relative` holds, or None.

    `a/b/c.rs` is declared in `a/b/mod.rs`, `a/b.rs`, `a/b/lib.rs` or
    `a/b/main.rs`; `a/b/mod.rs` one level up. The candidate must declare
    `mod <name>` in code, so a crate root, a binary and a `#[path]` module
    resolve to None rather than to a guess.
    """
    path = Path(relative)
    if path.name in ("lib.rs", "main.rs"):
        return None
    if path.name == "mod.rs":
        name, directory = path.parent.name, path.parent.parent
    else:
        name, directory = path.stem, path.parent
    declares = re.compile(r"(?m)^[ \t]*(?:#\[[^\]\n]*\][ \t]*)*(?:pub(?:[ \t]*\([^)\n]*\))?[ \t]+)?mod[ \t]+" + name + r"[ \t]*;")
    for candidate in (directory / "mod.rs", directory.with_suffix(".rs"), directory / "lib.rs", directory / "main.rs"):
        file = root / candidate
        if not file.is_file():
            continue
        text = file.read_text(encoding="utf-8")
        kinds = classify(text)
        if any(kinds[match.end() - 1] == CODE for match in declares.finditer(text)):
            return candidate.as_posix()
    return None


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
    glob_children: list[tuple[str, int]] = field(default_factory=list)
    glob_unresolved: list[tuple[str, int, str]] = field(default_factory=list)


def run(
    root: Path, crates: list[str], holds: list[str], fmt: bool = True, out=sys.stdout, facades: list[str] | None = None
) -> int:
    facade_paths = check_facades(facades or [])
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
                left = remaining_lines(text, facade_paths)
                if left:
                    bucket.append((relative, len(left)))
                continue
            result = rewrite(text, facade_paths)
            census.use_statements += result.use_statements
            census.qualified_paths += result.qualified_paths
            census.in_comments += result.in_comments
            census.in_literals += result.in_literals
            census.remaining += [f"{relative}:{line}" for line in remaining_lines(result.text, facade_paths)]
            if result.text != text:
                path.write_text(result.text, encoding="utf-8")
                census.changed.append(relative)
                rewritten.append(path)
        censuses.append(census)

    # Second pass, once every parent of this run is rewritten: a child is judged
    # against its parent's text as it now stands, whichever run rewrote it.
    for census in censuses:
        for relative in sources[census.crate]:
            if relative in refused_files or relative in hold_set:
                continue
            path = root / relative
            text = path.read_text(encoding="utf-8")
            parent = parent_module_file(root, relative)
            if parent is None:
                continue
            result = glob_child_import(text, (root / parent).read_text(encoding="utf-8"))
            if result.inserted:
                path.write_text(result.text, encoding="utf-8")
                if relative not in census.changed:
                    census.changed.append(relative)
                    rewritten.append(path)
                census.glob_children.append((relative, result.dependent_calls))
            elif result.dependent_calls:
                census.glob_unresolved.append((relative, result.dependent_calls, parent))

    for census in censuses:
        # A crate this run did not rewrite keeps its manifest: a dev-only use of
        # rustfs-s3-types or an s3s entry kept for other reasons is not ours to edit.
        # Whether the package still names either crate is read from all of its
        # sources, not only from the directory this run was given.
        if census.changed:
            crate = census.crate
            package = packages[crate]
            texts = [(root / f).read_text(encoding="utf-8") for f in package_sources(root, package, package)]
            crate_names_old = any(names_crate(text, OLD_CRATE_PATH) for text in texts)
            crate_names_new = any(names_crate(text, NEW_CRATE_PATH) for text in texts)
            manifest_path = root / package / "Cargo.toml"
            manifest = manifest_path.read_text(encoding="utf-8")
            edited, census.manifest = edit_manifest(manifest, crate_names_new, not crate_names_old)
            if edited != manifest:
                manifest_path.write_text(edited, encoding="utf-8")

    if fmt and rewritten:
        edition = _workspace_edition(root)
        result = subprocess.run(["rustfmt", "--edition", edition, *map(str, rewritten)], check=False)
        if result.returncode != 0:
            raise UsageError("rustfmt failed on the rewritten files")

    for facade in facade_paths:
        print(f"facade   {facade}: its S3Error, S3ErrorCode, S3Result and s3_error paths are treated as s3s's", file=out)
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
        for path, calls in census.glob_children:
            print(f"  glob     {path}: use {NEW_CRATE}::s3_error; added ({calls} calls)", file=out)
        for path, calls, parent in census.glob_unresolved:
            print(
                f"  GLOB     {path} ({calls} s3_error! calls reach the macro through `use super::*` from {parent}, "
                f"which does not import it from {NEW_CRATE})",
                file=out,
            )
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
    parser.add_argument("--facade", action="append", default=[], metavar="PATH")
    parser.add_argument("--no-fmt", action="store_true")
    args = parser.parse_args(argv)
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False)
    if top.returncode != 0:
        print("error: not inside a git checkout", file=sys.stderr)
        return 2
    try:
        return run(Path(top.stdout.strip()), args.crates, args.hold, fmt=not args.no_fmt, facades=args.facade)
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

    # -- children that reach the macro through `use super::*` --

    RUSTFS_PARENT = "use rustfs_s3_types::{S3Error, s3_error};\nmod child;\n"

    def glob_child(self, child: str, parent: str | None = None) -> GlobChildResult:
        return glob_child_import(child, self.RUSTFS_PARENT if parent is None else parent)

    def test_glob_child_with_a_top_level_call_gets_the_import_after_its_glob(self):
        child = "use super::*;\nuse crate::x::Y;\n\nfn f() -> S3Error {\n    s3_error!(NoSuchKey)\n}\n"
        result = self.glob_child(child)
        self.assertEqual(
            result.text,
            "use super::*;\nuse rustfs_s3_types::s3_error;\nuse crate::x::Y;\n\nfn f() -> S3Error {\n    s3_error!(NoSuchKey)\n}\n",
        )
        self.assertEqual((result.inserted, result.dependent_calls), (1, 1))
        self.assertEqual(glob_child_import(result.text, self.RUSTFS_PARENT).text, result.text)

    def test_glob_child_with_calls_only_in_a_test_module_gets_the_import_there(self):
        child = (
            "use super::*;\n\npub fn f() {}\n\n#[cfg(test)]\nmod tests {\n    use super::*;\n"
            "    #[test]\n    fn t() {\n        let _ = s3_error!(InternalError, \"x\");\n    }\n}\n"
        )
        result = self.glob_child(child)
        self.assertEqual(
            result.text,
            child.replace("    use super::*;\n", "    use super::*;\n    use rustfs_s3_types::s3_error;\n"),
        )
        self.assertEqual((result.inserted, result.dependent_calls), (1, 1))

    def test_glob_child_attribute_on_the_glob_is_repeated_on_the_import(self):
        child = "#[allow(unused_imports)]\nuse super::*;\nfn f() { s3_error!(A); s3_error!(B, \"m\"); }\n"
        result = self.glob_child(child)
        self.assertEqual(
            result.text,
            "#[allow(unused_imports)]\nuse super::*;\n#[allow(unused_imports)]\nuse rustfs_s3_types::s3_error;\n"
            "fn f() { s3_error!(A); s3_error!(B, \"m\"); }\n",
        )
        self.assertEqual(result.dependent_calls, 2)

    def test_glob_child_that_already_names_the_macro_is_untouched(self):
        for use_line in (
            "use rustfs_s3_types::s3_error;",
            "use rustfs_s3_types::{S3Error, s3_error as se};",
            "use crate::facade::s3_error;",
            "use s3s::s3_error;",
        ):
            child = f"use super::*;\n{use_line}\nfn f() {{ s3_error!(A); }}\n"
            with self.subTest(use_line=use_line):
                self.assertEqual(self.glob_child(child), GlobChildResult(child))

    def test_glob_child_with_only_qualified_commented_or_quoted_calls_is_untouched(self):
        child = (
            "use super::*;\nfn f() { rustfs_s3_types::s3_error!(A); ::rustfs_s3_types::s3_error!(B); }\n"
            "// s3_error!(C)\n/* s3_error!(D) */\nconst M: &str = \"s3_error!(E)\";\n"
        )
        self.assertEqual(self.glob_child(child), GlobChildResult(child))

    def test_child_without_a_top_level_glob_is_untouched(self):
        for child in (
            "fn f() { s3_error!(A); }\n",
            "mod inner {\n    use super::*;\n    fn f() { s3_error!(A); }\n}\n",
            "// use super::*;\nfn f() { s3_error!(A); }\n",
            "use super::{S3Error, f};\nfn g() { s3_error!(A); }\n",
        ):
            with self.subTest(child=child):
                self.assertEqual(self.glob_child(child), GlobChildResult(child))

    def test_glob_child_whose_parent_does_not_import_rustfs_s3_types_macro_is_reported_not_rewritten(self):
        child = "use super::*;\nfn f() { s3_error!(A); s3_error!(B); }\n"
        for parent in (
            "use s3s::{S3Error, s3_error};\n",
            "use crate::storage_api::s3::{S3Error, s3_error};\n",
            "use rustfs_s3_types::S3Error;\n",
            "use rustfs_s3_types::s3_error as se;\n",
            "// use rustfs_s3_types::s3_error;\n",
            "/*\nuse rustfs_s3_types::s3_error;\n*/\n",
            "mod nested {\n    use rustfs_s3_types::s3_error;\n}\n",
        ):
            with self.subTest(parent=parent):
                self.assertEqual(self.glob_child(child, parent), GlobChildResult(child, dependent_calls=2))

    def test_calls_in_an_inline_module_without_its_own_glob_do_not_depend_on_the_parent(self):
        for child in (
            "use super::*;\nfn f() {}\nmod helpers {\n    use crate::facade::*;\n    fn g() { s3_error!(A); }\n}\n",
            "use super::*;\nmod tests {\n    mod inner {\n        use super::*;\n        fn g() { s3_error!(A); }\n    }\n}\n",
        ):
            with self.subTest(child=child):
                self.assertEqual(self.glob_child(child), GlobChildResult(child))

    def test_glob_child_that_defines_its_own_macro_is_untouched(self):
        child = "use super::*;\nmacro_rules! s3_error { ($c:ident) => { () }; }\nfn f() { s3_error!(A); }\n"
        self.assertEqual(self.glob_child(child), GlobChildResult(child))

    def test_driver_gives_glob_children_the_import_and_reports_the_rest(self):
        files = self.base_files()
        files.update(
            {
                "crates/a/src/held.rs": "pub struct H;\n",
                "crates/a/src/object/mod.rs": "use s3s::{S3Error, S3Request, s3_error};\nmod get;\nmod put;\n",
                "crates/a/src/object/get.rs": "use super::*;\nfn f() -> S3Error { s3_error!(NoSuchKey) }\n",
                "crates/a/src/object/put.rs": "use super::*;\nfn f() -> S3Error { rustfs_s3_types::s3_error!(A) }\n",
                "crates/a/src/site.rs": "use crate::facade::s3_error;\nmod hooks;\n",
                "crates/a/src/site/hooks.rs": "use super::*;\nfn f() { s3_error!(A); }\n",
                "crates/a/src/edge.rs": "use rustfs_s3_types::s3_error;\nmod inner;\n",
                "crates/a/src/edge/inner.rs": "use super::*;\nfn f() { s3_error!(A); }\n",
                ALLOWLIST: "# edge\ncrates/edge/src/edge.rs\ncrates/a/src/edge/inner.rs\n",
            }
        )
        root = self.scratch_repo(files)
        first = _Capture()
        self.assertEqual(run(root, ["crates/a"], [], fmt=False, out=first), 0, first.text)
        self.assertEqual(
            (root / "crates/a/src/object/get.rs").read_text(),
            "use super::*;\nuse rustfs_s3_types::s3_error;\nfn f() -> S3Error { s3_error!(NoSuchKey) }\n",
        )
        for untouched in ("crates/a/src/object/put.rs", "crates/a/src/site/hooks.rs", "crates/a/src/edge/inner.rs"):
            self.assertEqual((root / untouched).read_text(), files[untouched], untouched)
        self.assertIn("  changed  crates/a/src/object/get.rs", first.text)
        self.assertIn("  glob     crates/a/src/object/get.rs: use rustfs_s3_types::s3_error; added (1 calls)", first.text)
        self.assertIn(
            "  GLOB     crates/a/src/site/hooks.rs (1 s3_error! calls reach the macro through `use super::*`"
            " from crates/a/src/site.rs, which does not import it from rustfs_s3_types)",
            first.text,
        )
        self.assertNotIn("edge/inner.rs: use", first.text)
        second = _Capture()
        self.assertEqual(run(root, ["crates/a"], [], fmt=False, out=second), 0, second.text)
        self.assertIn("total: 0 files changed in 1 crates", second.text)
        self.assertNotIn("  glob     ", second.text)

    def test_driver_skips_a_child_whose_parent_file_does_not_declare_it(self):
        files = self.base_files()
        files.update(
            {
                "crates/a/src/held.rs": "pub struct H;\n",
                "crates/a/src/object/mod.rs": "use rustfs_s3_types::s3_error;\nmod get;\n",
                "crates/a/src/object/stray.rs": "use super::*;\nfn f() { s3_error!(A); }\n",
                "crates/a/src/bin/tool.rs": "use super::*;\nfn f() { s3_error!(A); }\n",
            }
        )
        root = self.scratch_repo(files)
        out = _Capture()
        self.assertEqual(run(root, ["crates/a"], [], fmt=False, out=out), 0, out.text)
        for untouched in ("crates/a/src/object/stray.rs", "crates/a/src/bin/tool.rs"):
            self.assertEqual((root / untouched).read_text(), files[untouched], untouched)
        self.assertNotIn("glob", out.text.lower())

    # -- facades that re-export the s3s error items --

    FACADE = "crate::storage_api::site::s3"

    def test_facade_import_moves_the_error_items_and_keeps_the_rest_on_the_facade(self):
        source = "use crate::storage_api::site::s3::{\n    Body, S3Error, S3ErrorCode as Code, S3Result, s3_error,\n};\nfn f() {}\n"
        self.assertEqual(
            rewrite(source, (self.FACADE,)).text,
            "use rustfs_s3_types::{S3Error, S3ErrorCode as Code, S3Result, s3_error};\nuse crate::storage_api::site::s3::Body;\nfn f() {}\n",
        )
        self.assertEqual(remaining_lines(rewrite(source, (self.FACADE,)).text, (self.FACADE,)), [])

    def test_facade_qualified_paths_move(self):
        source = "fn f() -> crate::storage_api::site::s3::S3Result<()> { Err(crate::storage_api::site::s3::s3_error!(A)) }\n"
        result = rewrite(source, (self.FACADE,))
        self.assertEqual(result.text, "fn f() -> rustfs_s3_types::S3Result<()> { Err(rustfs_s3_types::s3_error!(A)) }\n")
        self.assertEqual(result.qualified_paths, 2)

    def test_facade_paths_are_untouched_without_the_option_and_reported_with_it(self):
        source = "use crate::storage_api::site::s3::{S3Error, s3_error};\nlet c = crate::storage_api::site::s3::S3ErrorCode::A;\n"
        self.assertEqual(rewrite(source).text, source)
        self.assertEqual(remaining_lines(source), [])
        self.assertEqual(remaining_lines(source, (self.FACADE,)), [1, 2])

    def test_paths_that_are_not_the_facade_are_untouched(self):
        for source in (
            "use crate::storage_api::site::s3x::S3Error;\n",
            "use crate::storage_api::other::s3::{S3Error, Body};\n",
            "let a = my::crate::storage_api::site::s3::S3Error::new();\n",
            "use crate::storage_api::site::s3::dto::S3Error;\n",
            "use crate::storage_api::site::s3::{Body, S3Request};\n",
            "// use crate::storage_api::site::s3::S3Error;\nconst A: &str = \"crate::storage_api::site::s3::S3Error\";\n",
        ):
            with self.subTest(source=source):
                self.assertEqual(rewrite(source, (self.FACADE,)).text, source)

    def test_driver_rejects_a_facade_that_is_not_a_crate_relative_module_path(self):
        root = self.scratch_repo(self.base_files())
        for facade in ("s3s", "crate", "crate::", "crate::f::s3::", "crate::f s3", "::crate::f", "s3s::dto"):
            with self.subTest(facade=facade), self.assertRaisesRegex(UsageError, "facade"):
                run(root, ["crates/a"], [], fmt=False, out=_Capture(), facades=[facade])
        self.assertEqual((root / "crates/a/src/lib.rs").read_text(), self.base_files()["crates/a/src/lib.rs"])

    def test_driver_rewrites_a_facade_parent_and_anchors_its_glob_children(self):
        files = self.base_files()
        files.update(
            {
                "crates/a/src/held.rs": "pub struct H;\n",
                "crates/a/src/site/mod.rs": f"use {self.FACADE}::{{Body, S3Error, s3_error}};\nmod hooks;\n",
                "crates/a/src/site/hooks.rs": "use super::*;\nfn f() -> S3Error { s3_error!(A) }\n",
            }
        )
        root = self.scratch_repo(files)
        out = _Capture()
        self.assertEqual(run(root, ["crates/a/src/site"], [], fmt=False, out=out, facades=[self.FACADE]), 0, out.text)
        self.assertIn(f"facade   {self.FACADE}", out.text)
        self.assertEqual(
            (root / "crates/a/src/site/mod.rs").read_text(),
            f"use rustfs_s3_types::{{S3Error, s3_error}};\nuse {self.FACADE}::Body;\nmod hooks;\n",
        )
        self.assertEqual(
            (root / "crates/a/src/site/hooks.rs").read_text(),
            "use super::*;\nuse rustfs_s3_types::s3_error;\nfn f() -> S3Error { s3_error!(A) }\n",
        )
        second = _Capture()
        self.assertEqual(run(root, ["crates/a/src/site"], [], fmt=False, out=second, facades=[self.FACADE]), 0)
        self.assertIn("total: 0 files changed in 1 crates", second.text)

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
