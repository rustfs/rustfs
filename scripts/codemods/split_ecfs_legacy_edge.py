#!/usr/bin/env python3
"""Move the `impl S3 for FS` method bodies out of ecfs.rs into one file per migration batch.

Preparatory task P1 of the T2.6 to T2.10 batch plan (rustfs/backlog#2765). The
s3s-to-gateway migration converts the S3 operations in nine batches, and every
legacy handler body lives in rustfs/src/storage/ecfs.rs, so every batch would
edit that one file. This codemod moves the bodies into
rustfs/src/storage/legacy_edge/<batch>.rs, one file per batch. It is a move,
not a rewrite:

  * `impl S3 for FS` stays in ecfs.rs with every signature unchanged; each
    method body becomes one call to an inherent `FS::<method>_edge` in the
    file of the method's batch (OWNER below). A method without an assignment
    fails the run; KEEP_INLINE names the one method that stays inline, and why.
  * A body moves with its doc comments and attributes. The only edits are
    mechanical: the fn gains `_edge` and `pub(in crate::storage::ecfs)`,
    `#[instrument(` gains `name = "<method>"` so the span keeps its name,
    `super::` paths become absolute (the code sits one module deeper), and
    `s3s::dto::X` becomes `X`: a new file reaches s3s only through
    `use crate::storage::ecfs::*;` (scripts/check_s3s_footprint.sh).
  * A private helper of ecfs.rs whose every consumer moved to one batch moves
    with it, repeated until nothing changes. A helper that several batches,
    the code staying in ecfs.rs, or another file use stays in ecfs.rs.
  * The new files are child modules of ecfs (`#[path = "legacy_edge/mod.rs"]`),
    so moved code still reaches the private items of ecfs.rs. Each file
    imports by name what it uses from outside ecfs.rs, and ecfs.rs keeps only
    the imports its remaining code uses, so a batch that rewrites its file
    never has to touch the import block of ecfs.rs.
  * A test in ecfs.rs or ecfs_test.rs that calls an S3 method on an `FS` moves
    to legacy_edge/tests/<batch>.rs with the fixtures only it uses; fixtures
    several batches' tests share move to legacy_edge/tests/mod.rs. A test that
    reaches two batches, or a fixture a moved and a staying test share, fails
    the run.

Usage:
  scripts/codemods/split_ecfs_legacy_edge.py [--root DIR] [--no-fmt]
  scripts/codemods/split_ecfs_legacy_edge.py --self-test

The input is the unsplit tree; a tree that already has legacy_edge/ or a
delegating impl is refused. To reproduce a committed split, restore the
pre-split ecfs.rs and ecfs_test.rs, delete legacy_edge/, run this script, and
compare. Unless --no-fmt is given, rustfmt formats every written file.

Exit status: 0 on success; 1 when --self-test fails; 2 on a usage error or an
input whose shape this codemod does not expect (the message says what).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from dataclasses import dataclass, field
from pathlib import Path

ECFS = "rustfs/src/storage/ecfs.rs"
ECFS_TEST = "rustfs/src/storage/ecfs_test.rs"
EDGE_DIR = "rustfs/src/storage/legacy_edge"
EDGE_VISIBILITY = "pub(in crate::storage::ecfs)"
ECFS_GLOB = "use crate::storage::ecfs::*;"

LICENSE_HEADER = """\
// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
"""

# File stem -> (plan batch, what the file holds, trait methods it owns).
BATCHES: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "bucket_configs": (
        "A1",
        "the bucket CORS, tagging, public access block and encryption configurations",
        (
            "delete_bucket_cors",
            "get_bucket_cors",
            "put_bucket_cors",
            "delete_bucket_tagging",
            "get_bucket_tagging",
            "put_bucket_tagging",
            "delete_public_access_block",
            "get_public_access_block",
            "put_public_access_block",
            "delete_bucket_encryption",
            "get_bucket_encryption",
            "put_bucket_encryption",
        ),
    ),
    "bucket_access": (
        "A2",
        "bucket policy, policy status, versioning, notification and ACL",
        (
            "delete_bucket_policy",
            "get_bucket_policy",
            "put_bucket_policy",
            "get_bucket_policy_status",
            "get_bucket_versioning",
            "put_bucket_versioning",
            "get_bucket_notification_configuration",
            "put_bucket_notification_configuration",
            "get_bucket_acl",
            "put_bucket_acl",
        ),
    ),
    "bucket_replication_lifecycle": (
        "A3",
        "the bucket lifecycle, replication and Object Lock configurations",
        (
            "delete_bucket_lifecycle",
            "get_bucket_lifecycle_configuration",
            "put_bucket_lifecycle_configuration",
            "delete_bucket_replication",
            "get_bucket_replication",
            "put_bucket_replication",
            "get_object_lock_configuration",
            "put_object_lock_configuration",
        ),
    ),
    "bucket_core": (
        "A4",
        "bucket create, delete, head and location, and the website, accelerate, logging and request-payment "
        "configurations",
        (
            "create_bucket",
            "delete_bucket",
            "head_bucket",
            "get_bucket_location",
            "delete_bucket_website",
            "get_bucket_website",
            "put_bucket_website",
            "get_bucket_accelerate_configuration",
            "put_bucket_accelerate_configuration",
            "get_bucket_logging",
            "put_bucket_logging",
            "get_bucket_request_payment",
            "put_bucket_request_payment",
        ),
    ),
    "object_meta": (
        "B1",
        "object ACL, tagging, legal hold, retention and torrent",
        (
            "get_object_acl",
            "put_object_acl",
            "get_object_tagging",
            "put_object_tagging",
            "delete_object_tagging",
            "get_object_legal_hold",
            "put_object_legal_hold",
            "get_object_retention",
            "put_object_retention",
            "get_object_torrent",
        ),
    ),
    "listing_multipart": (
        "B2",
        "the object and multipart-upload listings, CreateMultipartUpload and AbortMultipartUpload",
        (
            "list_objects",
            "list_objects_v2",
            "list_object_versions",
            "list_multipart_uploads",
            "list_parts",
            "create_multipart_upload",
            "abort_multipart_upload",
        ),
    ),
    "object_read": (
        "C",
        "GetObject, HeadObject and GetObjectAttributes",
        ("get_object", "head_object", "get_object_attributes"),
    ),
    "object_write": (
        "D",
        "PutObject and UploadPart (PostObject has no trait method of its own and reaches PutObject)",
        ("put_object", "upload_part"),
    ),
    "object_misc": (
        "E",
        "CopyObject, UploadPartCopy, CompleteMultipartUpload, DeleteObject, DeleteObjects, RestoreObject and "
        "SelectObjectContent",
        (
            "copy_object",
            "upload_part_copy",
            "complete_multipart_upload",
            "delete_object",
            "delete_objects",
            "restore_object",
            "select_object_content",
        ),
    ),
}

# Methods whose body stays in ecfs.rs. ListBuckets belongs to the gateway-stack
# skeleton (task T2.5), which edits this body in ecfs.rs itself; it is in none
# of the nine batches.
KEEP_INLINE: dict[str, str] = {"list_buckets": "owned by the T2.5 gateway-stack skeleton, not by a batch"}

OWNER: dict[str, str] = {}


def refresh_owner() -> None:
    OWNER.clear()
    for stem, (_, _, methods) in BATCHES.items():
        for method in methods:
            if method in OWNER:
                raise ShapeError(f"method {method} is assigned to {OWNER[method]} and {stem}")
            OWNER[method] = stem


# Trait imports are used through method calls, which an identifier scan cannot
# see; every trait the inputs import is listed with its method names.
TRAIT_METHODS: dict[str, frozenset[str]] = {
    "BucketOperations": frozenset({"make_bucket", "get_bucket_info", "list_bucket", "delete_bucket"}),
    "ObjectOperations": frozenset(
        {
            "add_partial",
            "copy_object",
            "delete_object",
            "delete_object_tags",
            "delete_object_version",
            "delete_objects",
            "delete_objects_with_accounting",
            "get_object_info",
            "get_object_info_for_delete",
            "get_object_tags",
            "put_object_metadata",
            "put_object_tags",
            "restore_transitioned_object",
            "transition_object",
            "verify_object_integrity",
        }
    ),
    "StorageVersioningConfigExt": frozenset({"enabled"}),
}

MACROS = frozenset({"debug", "error", "warn", "info", "trace", "instrument", "counter", "histogram", "gauge", "s3_error"})
# Names ecfs.rs imports from the s3s crate root that moved code may spell `s3s::X`.
S3S_ROOT_NAMES = ("S3ErrorCode", "S3Error", "S3Request", "S3Response", "S3Result")

# The footprint guard's pattern: no new file may match it, comments included.
S3S_PATH = re.compile(r'(^|[^"A-Za-z0-9_])s3s::', re.M)
IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
IDENT_RE = re.compile(IDENT)
CHAR_LITERAL = re.compile(r"'(?:\\(?:[nrt\\0'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\})|[^\\'\n])'")
RAW_STRING_START = re.compile(r'(?<![A-Za-z0-9_])b?r(#*)"')
FORMAT_CAPTURE = re.compile(r"(?<!\{)\{(" + IDENT + r")(?=[}:])")
TEST_ATTR = re.compile(r"#\[\s*(?:tokio::)?test\b|#\[\s*test_case\b")
CFG_TEST = re.compile(r"#\[\s*cfg\s*\(\s*test\s*\)\s*\]")
TRAIT_CALLS = (re.compile(r"\bfs\s*\.\s*(" + IDENT + r")\s*\("), re.compile(r"\bS3::(" + IDENT + r")\s*\(\s*&fs\b"))
S3S_DTO_PATH = re.compile(r"(?<![A-Za-z0-9_:])s3s::dto::(" + IDENT + r")")
S3S_ROOT_PATH = re.compile(r"(?<![A-Za-z0-9_:])s3s::(" + "|".join(S3S_ROOT_NAMES) + r")\b")
FN_LOCAL_S3S_USE = re.compile(r"(?m)^[ \t]*use[ \t]+s3s::[^;]*;[ \t]*\n")
NAMED_ITEM_KINDS = ("fn", "const", "static", "struct", "enum", "type", "trait")


class ShapeError(Exception):
    """The input does not have the shape this codemod expects: exit 2."""


# ---------------------------------------------------------------------------
# Lexing


def mask_source(text: str) -> tuple[str, list[tuple[int, int]]]:
    """Blank comments and string/char literals (newlines kept); also return the literal spans."""
    out = list(text)
    strings: list[tuple[int, int]] = []
    n = len(text)

    def blank(start: int, end: int) -> None:
        for k in range(start, end):
            if out[k] != "\n":
                out[k] = " "

    def blank_literal(start: int, end: int) -> None:
        # An inline format argument (`"{name}"`) uses `name`; keep it visible to the usage scans.
        blank(start, end)
        for m in FORMAT_CAPTURE.finditer(text, start, end):
            out[m.start(1) : m.end(1)] = m.group(1)

    i = 0
    while i < n:
        if text.startswith("//", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
            continue
        if text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            if depth:
                raise ShapeError("unterminated block comment")
            blank(i, j)
            i = j
            continue
        raw = RAW_STRING_START.match(text, i)
        if raw:
            close = '"' + raw.group(1)
            j = text.find(close, raw.end())
            if j < 0:
                raise ShapeError("unterminated raw string literal")
            j += len(close)
            strings.append((i, j))
            blank_literal(i, j)
            i = j
            continue
        c = text[i]
        if c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            if j >= n:
                raise ShapeError("unterminated string literal")
            j += 1
            strings.append((i, j))
            blank_literal(i, j)
            i = j
            continue
        if c == "'":
            literal = CHAR_LITERAL.match(text, i)
            if literal:
                blank(i, literal.end())
                i = literal.end()
                continue
        i += 1
    return "".join(out), strings


@dataclass
class Source:
    """A Rust source text plus a copy with comments and literals blanked out."""

    text: str
    mask: str = ""
    strings: list[tuple[int, int]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.mask, self.strings = mask_source(self.text)


def masked(text: str) -> str:
    return mask_source(text)[0]


OPEN, CLOSE = "([{", ")]}"


def matching_close(mask: str, open_at: int) -> int:
    depth = 0
    for k in range(open_at, len(mask)):
        if mask[k] in OPEN:
            depth += 1
        elif mask[k] in CLOSE:
            depth -= 1
            if depth == 0:
                return k
    raise ShapeError(f"unbalanced bracket at offset {open_at}")


@dataclass
class Item:
    """One item of a module or impl body, its leading comments and attributes included."""

    start: int
    end: int
    head: int  # first character after the attributes
    kind: str
    name: str
    visibility: str


ITEM_HEAD = re.compile(
    r"(?P<vis>pub(?:\s*\([^)]*\))?\s+)?(?P<kw>(?:async\s+)?fn|use|const|static|struct|enum|impl|mod|type|trait)\b"
    r"\s*(?P<name>" + IDENT + r")?"
)


def skip_attributes(mask: str, i: int) -> int:
    while True:
        while i < len(mask) and mask[i].isspace():
            i += 1
        if mask.startswith("#[", i):
            i = matching_close(mask, i + 1) + 1
            continue
        return i


def segment(src: Source, start: int, end: int) -> list[Item]:
    """The items src.text[start:end] holds at nesting depth zero."""
    mask, text = src.mask, src.text
    items: list[Item] = []
    i = start
    while True:
        k = i
        while k < end and mask[k].isspace():
            k += 1
        if k >= end:
            if text[i:end].strip():
                raise ShapeError(f"a comment follows the last item near offset {i}")
            return items
        item_start = i
        while text[item_start].isspace():
            item_start += 1
        head_at = skip_attributes(mask, k)
        head = ITEM_HEAD.match(mask, head_at)
        if not head:
            snippet = text[head_at : head_at + 60].splitlines()[0] if head_at < end else ""
            raise ShapeError(f"unrecognised item at offset {head_at}: {snippet!r}")
        kind = "fn" if head.group("kw").endswith("fn") else head.group("kw")
        name = head.group("name") or ""
        if kind == "impl":
            name = re.sub(r"\s+", " ", mask[head_at : mask.index("{", head.end())]).strip()
        j, depth = head.end(), 0
        while True:
            if j >= end:
                raise ShapeError(f"item {name!r} runs past its enclosing block")
            c = mask[j]
            if c in OPEN:
                depth += 1
            elif c in CLOSE:
                depth -= 1
                if depth == 0 and c == "}":
                    after = j + 1
                    while after < end and mask[after] in " \t\n":
                        after += 1
                    j = after + 1 if after < end and mask[after] == ";" else j + 1
                    break
            elif c == ";" and depth == 0:
                j += 1
                break
            j += 1
        line_end = text.find("\n", j)
        line_end = end if line_end < 0 or line_end > end else line_end
        if text[j:line_end].strip():
            if mask[j:line_end].strip():
                raise ShapeError(f"code follows item {name!r} on its closing line")
            j = line_end  # a trailing comment belongs to the item it follows
        items.append(Item(item_start, j, head_at, kind, name, (head.group("vis") or "").strip()))
        i = j


def body_bounds(src: Source, item: Item) -> tuple[int, int]:
    """Offsets just inside the braces of an impl, mod or fn item."""
    depth, k = 0, item.head
    while True:
        c = src.mask[k]
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
        elif c == "{" and depth == 0:
            return k + 1, matching_close(src.mask, k)
        k += 1


def line_start(text: str, offset: int) -> int:
    return text.rfind("\n", 0, offset) + 1


# ---------------------------------------------------------------------------
# Use trees


@dataclass
class UseNode:
    """`segment`, `segment::child`, `segment::{children}`, `*` or `self`; leaves may carry an alias."""

    segment: str
    children: list[UseNode] | None = None
    alias: str | None = None


@dataclass(frozen=True)
class Leaf:
    path: tuple[str, ...]
    binding: str
    alias: str | None


def parse_use(code: str) -> UseNode:
    """Parse the masked text of one `use` item; a visibility or anything unexpected is refused."""
    m = re.fullmatch(r"\s*use\s+(?P<tree>[^;]*?)\s*;\s*", code)
    if not m:
        raise ShapeError(f"unsupported use item: {' '.join(code.split())!r}")
    source = m.group("tree")
    tokens = re.findall(r"::|[{},*]|" + IDENT, source)
    if "".join(tokens) != re.sub(r"\s+", "", source):
        raise ShapeError(f"unsupported use tree: {' '.join(source.split())!r}")
    pos = 0

    def take() -> str:
        nonlocal pos
        if pos >= len(tokens):
            raise ShapeError(f"truncated use tree: {source!r}")
        pos += 1
        return tokens[pos - 1]

    def peek() -> str | None:
        return tokens[pos] if pos < len(tokens) else None

    def group() -> list[UseNode]:
        children: list[UseNode] = []
        while peek() != "}":
            children.append(tree())
            if peek() == ",":
                take()
        take()
        return children

    def tree() -> UseNode:
        tok = take()
        if tok == "{":
            return UseNode("", children=group())
        node = UseNode(tok)
        if tok == "*":
            return node
        if peek() == "::":
            take()
            if peek() == "{":
                take()
                node.children = group()
            else:
                node.children = [tree()]
        elif peek() == "as":
            take()
            node.alias = take()
        return node

    root = tree()
    if pos != len(tokens):
        raise ShapeError(f"unsupported use tree: {source!r}")
    return root


def leaves(node: UseNode, prefix: tuple[str, ...] = ()) -> list[Leaf]:
    if node.children is None:
        if node.segment == "*":
            return [Leaf(prefix + ("*",), "*", None)]
        if node.segment == "self":
            return [Leaf(prefix, node.alias or prefix[-1], node.alias)]
        return [Leaf(prefix + (node.segment,), node.alias or node.segment, node.alias)]
    path = prefix + ((node.segment,) if node.segment else ())
    return [leaf for child in node.children for leaf in leaves(child, path)]


def filter_use(node: UseNode, keep, prefix: tuple[str, ...] = ()) -> UseNode | None:
    """The tree with only the leaves `keep` accepts, or None when none remain."""
    if node.children is None:
        return node if all(keep(leaf) for leaf in leaves(node, prefix)) else None
    path = prefix + ((node.segment,) if node.segment else ())
    children = [c for c in (filter_use(child, keep, path) for child in node.children) if c is not None]
    return UseNode(node.segment, children=children) if children else None


def render_use(node: UseNode) -> str:
    if node.children is None:
        return node.segment + (f" as {node.alias}" if node.alias else "")
    if len(node.children) == 1 and node.segment:
        only = node.children[0]
        if only.children is None and only.segment == "self":
            return node.segment + (f" as {only.alias}" if only.alias else "")
        return f"{node.segment}::{render_use(only)}"
    inner = ", ".join(render_use(c) for c in node.children)
    return f"{node.segment}::{{{inner}}}" if node.segment else f"{{{inner}}}"


def rebase_use(node: UseNode, old: str, new: tuple[str, ...]) -> UseNode:
    """The tree with a leading `old` segment replaced by the segments `new`."""
    if node.segment != old:
        return node
    rebased = node.children
    for segment_name in reversed(new[1:]):
        rebased = [UseNode(segment_name, children=rebased)]
    return UseNode(new[0], children=rebased)


# ---------------------------------------------------------------------------
# Usage scans, all over masked text


def free_identifiers(code: str) -> set[str]:
    """Identifiers not reached through `.` or `::`: the names an import can supply."""
    found = set()
    for m in IDENT_RE.finditer(code):
        if m.start() and (code[m.start() - 1].isalnum() or code[m.start() - 1] == "_"):
            continue
        before = code[: m.start()].rstrip(" \t\n")
        if before.endswith("::") or (before.endswith(".") and not before.endswith("..")):
            continue
        found.add(m.group())
    return found


def all_identifiers(code: str) -> set[str]:
    return set(IDENT_RE.findall(code))


def uses_macro(code: str, name: str) -> bool:
    return bool(re.search(r"(?<![A-Za-z0-9_:])" + name + r"\s*!", code) or re.search(r"#\[\s*" + name + r"\b", code))


def calls_method(code: str, names: frozenset[str]) -> bool:
    return any(m.group(1) in names for m in re.finditer(r"\.\s*(" + IDENT + r")\s*(?:::\s*<[^>]*>\s*)?\(", code))


def leaf_used(leaf: Leaf, code: str) -> bool:
    if leaf.binding == "*":
        raise ShapeError(f"glob import {'::'.join(leaf.path)} is only supported from s3s")
    name = leaf.path[-1]
    if leaf.binding == "_":
        if name not in TRAIT_METHODS:
            raise ShapeError(f"anonymous trait import {'::'.join(leaf.path)} has no method list in TRAIT_METHODS")
        return calls_method(code, TRAIT_METHODS[name])
    if leaf.binding in MACROS:
        return uses_macro(code, leaf.binding)
    if leaf.binding == "S3":
        return "S3" in free_identifiers(code) or calls_method(code, frozenset(OWNER) | frozenset(KEEP_INLINE))
    if name in TRAIT_METHODS and calls_method(code, TRAIT_METHODS[name]):
        return True
    return leaf.binding in free_identifiers(code)


def require_some_user(tree: UseNode, codes: list[str], where: str) -> None:
    """Fail when no code uses an import: the input compiles warning-free, so the scan missed a use.

    The usual cause is a trait used only through method calls that TRAIT_METHODS does not list;
    dropping such an import silently would only surface as a compile error.
    """
    for leaf in leaves(tree):
        if leaf.binding != "*" and not any(leaf_used(leaf, code) for code in codes):
            raise ShapeError(
                f"import {'::'.join(leaf.path)} in {where} has no use the scan can see; "
                "if it is a trait, list its methods in TRAIT_METHODS"
            )


def is_trait(leaf: Leaf) -> bool:
    return leaf.binding == "_" or leaf.path[-1] in TRAIT_METHODS or leaf.binding == "S3"


LOCAL_USE = re.compile(r"(?<![A-Za-z0-9_])use\s+[^;]*;")


def scoped(code: str) -> str:
    """Masked item code without its fn-local `use` items and the names they bind.

    A name a fn imports for itself is not a use of the module-level import of that name.
    """
    bindings: set[str] = set()
    for m in reversed(list(LOCAL_USE.finditer(code))):
        bindings |= {leaf.binding for leaf in leaves(parse_use(m.group())) if leaf.binding not in ("*", "_")}
        code = code[: m.start()] + " " * (m.end() - m.start()) + code[m.end() :]
    for name in bindings:
        code = re.sub(r"(?<![A-Za-z0-9_])" + re.escape(name) + r"(?![A-Za-z0-9_])", " " * len(name), code)
    return code


# ---------------------------------------------------------------------------
# Text edits


def dedent(src: Source, start: int, end: int, width: int) -> str:
    """The lines holding src.text[start:end], with `width` leading spaces removed from each."""
    first = line_start(src.text, start)
    out, offset = [], first
    for line in src.text[first:end].splitlines(keepends=True):
        if any(s < offset < e for s, e in src.strings):
            raise ShapeError(f"a string literal spans lines near offset {offset}; it cannot be re-indented")
        if line.strip() and not line.startswith(" " * width):
            raise ShapeError(f"line is indented less than {width} spaces: {line.rstrip()!r}")
        out.append(line[width:] if line.strip() else line.lstrip(" \t"))
        offset += len(line)
    return "".join(out)


def rewrite_code(text: str, rules: list[tuple[re.Pattern[str], str]]) -> str:
    """`text` with each rule applied to its code, never inside a comment or literal."""
    for pattern, replacement in rules:
        mask = masked(text)
        pieces, last = [], 0
        for m in pattern.finditer(mask):
            pieces += [text[last : m.start()], m.expand(replacement)]
            last = m.end()
        text = "".join(pieces) + text[last:]
    return text


def moved_code_rules(super_path: str) -> list[tuple[re.Pattern[str], str]]:
    return [
        (re.compile(r"(?<![A-Za-z0-9_:])super::"), super_path + "::"),
        (S3S_DTO_PATH, r"\1"),
        (S3S_ROOT_PATH, r"\1"),
    ]


def s3s_names_in(text: str) -> set[str]:
    """Names that moved code spells through an s3s path or a fn-local s3s import."""
    mask = masked(text)
    names = {m.group(1) for m in S3S_DTO_PATH.finditer(mask)} | {m.group(1) for m in S3S_ROOT_PATH.finditer(mask)}
    for m in FN_LOCAL_S3S_USE.finditer(mask):
        names |= {leaf.binding for leaf in leaves(parse_use(m.group().strip()))}
    return names


def strip_fn_local_s3s_uses(text: str) -> str:
    """Drop fn-local `use s3s::...;` lines; the file's ecfs glob supplies those names."""
    mask = masked(text)
    pieces, last = [], 0
    for m in FN_LOCAL_S3S_USE.finditer(mask):
        pieces.append(text[last : m.start()])
        last = m.end()
    return "".join(pieces) + text[last:]


def replace_items(text: str, items: list[Item], replacements: list[str | None]) -> str:
    """`text` with item k replaced by replacements[k]; a None drops the item, keeping the wider gap."""
    if not items:
        return text
    out = [text[: items[0].start]]
    gap: str | None = None
    emitted = False
    for k, item in enumerate(items):
        here = text[items[k - 1].end : item.start] if k else ""
        if gap is None or here.count("\n") > gap.count("\n"):
            gap = here
        if replacements[k] is None:
            continue
        if emitted:
            out.append(gap)
        out.append(replacements[k])
        gap, emitted = None, True
    out.append(text[items[-1].end :])
    return "".join(out)


# ---------------------------------------------------------------------------
# The split


@dataclass
class BatchOutput:
    free: list[str] = field(default_factory=list)
    helper_blocks: list[tuple[str, list[str]]] = field(default_factory=list)
    edges: list[str] = field(default_factory=list)
    code: str = ""  # masked code of everything above, for import decisions
    uses: list[str] = field(default_factory=list)
    tests: list[str] = field(default_factory=list)
    test_code: str = ""
    test_uses: list[str] = field(default_factory=list)


@dataclass
class TestModule:
    origin: str  # "ecfs" or "ecfs_test"
    src: Source
    index: int  # index among the top-level items of its file
    item: Item
    inner: list[Item]
    dest: dict[int, str | None]  # inner index -> batch stem, "mod" (shared fixture) or None (stays)


def called_batch(code: str, where: str) -> str | None:
    called = {m.group(1) for pattern in TRAIT_CALLS for m in pattern.finditer(code)}
    owners = {OWNER[name] for name in called if name in OWNER}
    if len(owners) > 1:
        raise ShapeError(f"{where} calls S3 methods of several batches: {sorted(owners)}")
    return owners.pop() if owners else None


def edge_and_delegate(src: Source, method: Item) -> tuple[str, str]:
    """The moved `_edge` fn and the one-line trait method that replaces it."""
    open_at, close_at = body_bounds(src, method)
    leading = src.text[method.start : method.head]
    signature = src.text[method.head : open_at - 1]
    body = src.text[open_at : close_at + 1]
    head = re.match(r"async\s+fn\s+" + re.escape(method.name) + r"\b", signature)
    if not head:
        raise ShapeError(f"impl S3 method {method.name} is not an `async fn`")
    param = re.search(r",\s*(" + IDENT + r")\s*:\s*S3Request<", signature)
    if not param:
        raise ShapeError(f"impl S3 method {method.name} takes no S3Request parameter")
    if re.search(r"_edge\s*\(", src.mask[open_at:close_at]):
        raise ShapeError(f"impl S3 method {method.name} already delegates to an edge fn: the tree is already split")
    leading = re.sub(
        r"#\[instrument\((\n[ \t]*)?",
        lambda m: f'#[instrument({m.group(1)}name = "{method.name}",{m.group(1)}'
        if m.group(1)
        else f'#[instrument(name = "{method.name}", ',
        leading,
    )
    edge = f"    {leading}{EDGE_VISIBILITY} async fn {method.name}_edge{signature[head.end():]}{{{body}"
    delegate = f"{signature}{{\n        self.{method.name}_edge({param.group(1)}).await\n    }}"
    return rewrite_code(edge, moved_code_rules("crate::storage")), delegate


def split(root: Path) -> dict[str, str]:
    refresh_owner()
    ecfs_path, test_path = root / ECFS, root / ECFS_TEST
    for path in (ecfs_path, test_path):
        if not path.is_file():
            raise ShapeError(f"{path} is missing")
    if (root / EDGE_DIR).exists():
        raise ShapeError(f"{EDGE_DIR} already exists: restore the pre-split ecfs.rs and ecfs_test.rs and remove it")
    ecfs = Source(ecfs_path.read_text(encoding="utf-8"))
    ecfs_test = Source(test_path.read_text(encoding="utf-8"))
    for name, src in ((ECFS, ecfs), (ECFS_TEST, ecfs_test)):
        if not src.text.startswith(LICENSE_HEADER):
            raise ShapeError(f"{name} does not start with the repository license header")

    top = segment(ecfs, len(LICENSE_HEADER), len(ecfs.text))
    impl_indexes = [k for k, it in enumerate(top) if it.kind == "impl" and it.name == "impl S3 for FS"]
    if len(impl_indexes) != 1:
        raise ShapeError(f"expected one `impl S3 for FS` in {ECFS}, found {len(impl_indexes)}")
    impl_index = impl_indexes[0]
    s3_start, s3_end = body_bounds(ecfs, top[impl_index])
    methods = segment(ecfs, s3_start, s3_end)
    names = [m.name for m in methods]
    if any(m.kind != "fn" for m in methods) or len(set(names)) != len(names):
        raise ShapeError("impl S3 for FS must hold only methods, each defined once")
    unassigned = [n for n in names if n not in OWNER and n not in KEEP_INLINE]
    if unassigned:
        raise ShapeError(f"impl S3 methods without a batch assignment: {unassigned}")
    absent = sorted((set(OWNER) | set(KEEP_INLINE)) - set(names))
    if absent:
        raise ShapeError(f"assigned methods absent from impl S3 for FS: {absent}")

    out = {stem: BatchOutput() for stem in BATCHES}
    delegates: dict[int, str] = {}
    for k, method in enumerate(methods):
        if method.name in KEEP_INLINE:
            continue
        edge, delegate = edge_and_delegate(ecfs, method)
        stem = OWNER[method.name]
        out[stem].edges.append(edge)
        out[stem].code += "\n" + scoped(masked(edge))
        delegates[k] = delegate

    # Tests: which move, and where.
    modules: list[TestModule] = []
    test_top = segment(ecfs_test, len(LICENSE_HEADER), len(ecfs_test.text))
    candidates = [("ecfs", ecfs, k, it) for k, it in enumerate(top) if it.kind == "mod"]
    for k, it in enumerate(test_top):
        if it.kind != "mod":
            raise ShapeError(f"{ECFS_TEST} holds a top-level {it.kind} {it.name!r} outside a test module")
        candidates.append(("ecfs_test", ecfs_test, k, it))
    for origin, src, k, it in candidates:
        if not CFG_TEST.search(src.mask[it.start : it.head]):
            raise ShapeError(f"{origin} module {it.name} is not #[cfg(test)]")
        inner_start, inner_end = body_bounds(src, it)
        inner = segment(src, inner_start, inner_end)
        dest: dict[int, str | None] = {}
        tests = set()
        for j, item in enumerate(inner):
            if item.kind == "fn" and TEST_ATTR.search(src.mask[item.start : item.head]):
                tests.add(j)
                dest[j] = called_batch(src.mask[item.start : item.end], f"{origin}::{it.name}::{item.name}")
        for j, item in enumerate(inner):
            if j in tests or item.kind not in NAMED_ITEM_KINDS or not item.name:
                continue
            users = {dest[t] for t in tests if item.name in all_identifiers(src.mask[inner[t].start : inner[t].end])}
            others = [
                o
                for o, other in enumerate(inner)
                if o != j and o not in tests and other.kind != "use" and item.name in all_identifiers(src.mask[other.start : other.end])
            ]
            moving = users - {None}
            if not moving:
                dest[j] = None
                continue
            if None in users or others:
                raise ShapeError(f"fixture {origin}::{it.name}::{item.name} is shared by moving and staying code")
            dest[j] = moving.pop() if len(moving) == 1 else "mod"
        modules.append(TestModule(origin, src, k, it, inner, dest))
    for module in modules:
        for j, stem in module.dest.items():
            if stem in BATCHES:
                out[stem].test_code += "\n" + module.src.mask[module.inner[j].start : module.inner[j].end]

    # Helpers of ecfs.rs follow their consumers until nothing changes.
    impl_fs = {k: segment(ecfs, *body_bounds(ecfs, it)) for k, it in enumerate(top) if it.kind == "impl" and it.name == "impl FS"}
    slots: dict[tuple[int, int | None], Item] = {}
    for k, it in enumerate(top):
        if k in impl_fs:
            slots.update({(k, j): inner for j, inner in enumerate(impl_fs[k])})
        elif it.kind in NAMED_ITEM_KINDS and it.name:
            slots[(k, None)] = it
    fixed_ecfs_code = [ecfs.mask[m.start : m.end] for k, m in enumerate(methods) if k not in delegates]
    for k, it in enumerate(top):
        if k != impl_index and k not in impl_fs and (k, None) not in slots and it.kind not in ("use", "mod"):
            fixed_ecfs_code.append(ecfs.mask[it.start : it.end])
    for module in modules:
        for j, inner in enumerate(module.inner):
            if module.dest.get(j) is None and inner.kind != "use":
                fixed_ecfs_code.append(module.src.mask[inner.start : inner.end])
    helper_owner: dict[tuple[int, int | None], str] = {}
    changed = True
    while changed:
        changed = False
        for key, item in slots.items():
            if key in helper_owner or item.visibility:
                continue
            consumers = set()
            for other_key, other in slots.items():
                if other_key != key and item.name in all_identifiers(ecfs.mask[other.start : other.end]):
                    consumers.add(helper_owner.get(other_key, "ecfs"))
            if any(item.name in all_identifiers(code) for code in fixed_ecfs_code):
                consumers.add("ecfs")
            for stem, batch in out.items():
                if item.name in all_identifiers(batch.code) or item.name in all_identifiers(batch.test_code):
                    consumers.add(stem)
            if len(consumers) == 1 and next(iter(consumers)) in BATCHES:
                stem = consumers.pop()
                helper_owner[key] = stem
                out[stem].code += "\n" + scoped(ecfs.mask[item.start : item.end])
                changed = True
    moved_names = {slots[key].name: stem for key, stem in helper_owner.items()}

    def helper_text(key: tuple[int, int | None]) -> str:
        item = slots[key]
        text = ecfs.text[item.start : item.end]
        if item.name in all_identifiers(out[helper_owner[key]].test_code):
            cut = item.head - item.start
            text = text[:cut] + "pub(super) " + text[cut:]  # the tests module is a sibling
        indent = "    " if key[1] is not None else ""
        return rewrite_code(indent + text, moved_code_rules("crate::storage"))

    for key in sorted(k for k in helper_owner if k[1] is None):
        out[helper_owner[key]].free.append(helper_text(key))
    for k, inner in impl_fs.items():
        doc = ecfs.text[top[k].start : top[k].head]
        per_stem: dict[str, list[str]] = {}
        for j in range(len(inner)):
            if (k, j) in helper_owner:
                per_stem.setdefault(helper_owner[(k, j)], []).append(helper_text((k, j)))
        for stem, fns in per_stem.items():
            out[stem].helper_blocks.append((doc, fns))

    # Imports of ecfs.rs: keep what the remaining code uses; hand each batch what it uses.
    residual = [ecfs.mask[top[impl_index].start : s3_start]]
    residual += [masked(delegates[k]) if k in delegates else scoped(ecfs.mask[m.start : m.end]) for k, m in enumerate(methods)]
    for k, it in enumerate(top):
        if k == impl_index or it.kind in ("use", "mod"):
            continue
        if k in impl_fs:
            residual.append(ecfs.mask[it.start : body_bounds(ecfs, it)[0]])
            residual += [scoped(ecfs.mask[i.start : i.end]) for j, i in enumerate(impl_fs[k]) if (k, j) not in helper_owner]
        elif (k, None) not in helper_owner:
            residual.append(scoped(ecfs.mask[it.start : it.end]))
    residual_code = "\n".join(residual)
    moved_code = "\n".join(b.code + b.test_code for b in out.values())
    explicit_bindings: set[str] = set(moved_names) | {it.name for it in top if it.name}
    replacement: dict[int, str | None] = {}
    for k, it in enumerate(top):
        if it.kind != "use":
            continue
        if it.start != it.head or it.visibility:
            raise ShapeError(f"use item with attributes, comments or visibility: {ecfs.text[it.start:it.end]!r}")
        tree = parse_use(ecfs.mask[it.start : it.end])
        from_s3s = tree.segment == "s3s"
        explicit_bindings |= {leaf.binding for leaf in leaves(tree) if not from_s3s and leaf.binding not in ("*", "_")}

        def keep(leaf: Leaf, from_s3s: bool = from_s3s) -> bool:
            if from_s3s:  # the legacy_edge files reach s3s through the ecfs glob
                return leaf.binding == "*" or leaf_used(leaf, residual_code) or leaf_used(leaf, moved_code)
            return leaf_used(leaf, residual_code)

        kept = filter_use(tree, keep)
        if kept is None:
            replacement[k] = None
        elif len(leaves(kept)) != len(leaves(tree)):
            replacement[k] = f"use {render_use(kept)};"
        require_some_user(tree, [residual_code, moved_code], ECFS)
        if from_s3s:
            continue
        for stem, batch in out.items():
            # A trait ecfs.rs still imports also reaches the file through the ecfs glob, and rustc
            # credits that one; a second, explicit import of it would be reported unused.
            used = filter_use(
                tree,
                lambda leaf, code=batch.code: leaf_used(leaf, code)
                and not (is_trait(leaf) and kept is not None and leaf in leaves(kept)),
            )
            if used is not None:
                batch.uses.append(f"use {render_use(rebase_use(used, 'super', ('crate', 'storage')))};")

    # Assemble ecfs.rs.
    for key, stem in helper_owner.items():
        if key[1] is None:
            replacement[key[0]] = None
    for k, inner in impl_fs.items():
        drops = [(k, j) in helper_owner for j in range(len(inner))]
        if all(drops):
            replacement[k] = None
        elif any(drops):
            start, end = body_bounds(ecfs, top[k])
            texts = [None if d else ecfs.text[i.start : i.end] for d, i in zip(drops, inner)]
            replacement[k] = ecfs.text[top[k].start : start] + replace_items(
                ecfs.text[start:end], [shift(i, -start) for i in inner], texts
            ) + ecfs.text[end : top[k].end]
    impl_item = top[impl_index]
    pieces, last = [], impl_item.start
    for k, method in enumerate(methods):
        if k in delegates:
            pieces += [ecfs.text[last : method.start], delegates[k]]
            last = method.end
    pieces.append(ecfs.text[last : impl_item.end])
    replacement[impl_index] = (
        "// The bodies of the methods below, one file per migration batch (rustfs/backlog#2765).\n"
        '#[path = "legacy_edge/mod.rs"]\nmod legacy_edge;\n\n' + "".join(pieces)
    )

    # Tests: move, and prune the imports of the modules they leave.
    shared: list[str] = []
    shared_code = ""
    shared_uses: list[str] = []
    test_replacement: dict[str, dict[int, str]] = {"ecfs": {}, "ecfs_test": {}}
    for module in modules:
        src, inner = module.src, module.inner
        super_path = "crate::storage::ecfs" if module.origin == "ecfs" else "crate::storage::ecfs_test"
        moved_here: dict[str, str] = {}
        texts: list[str | None] = []
        for j, item in enumerate(inner):
            dest = module.dest.get(j)
            if dest is None:
                texts.append(src.text[item.start : item.end])
                continue
            text = dedent(src, item.start, item.end, 4)
            for name in s3s_names_in(text):
                if name in explicit_bindings:
                    raise ShapeError(f"{item.name} names s3s's {name}, which another import shadows in ecfs.rs")
            text = strip_fn_local_s3s_uses(rewrite_code(text, moved_code_rules(super_path)))
            texts.append(None)
            moved_here[dest] = moved_here.get(dest, "") + "\n" + scoped(masked(text))
            if dest == "mod":
                shared.append(text)
                shared_code += "\n" + masked(text)
            else:
                out[dest].tests.append(text)
        if not moved_here:
            continue
        remaining = "\n".join(scoped(masked(t)) for j, t in enumerate(texts) if t is not None and inner[j].kind != "use")
        for j, item in enumerate(inner):
            if item.kind != "use":
                continue
            tree = parse_use(src.mask[item.start : item.end])
            kept = filter_use(tree, lambda leaf: leaf_used(leaf, remaining))
            require_some_user(tree, [remaining, *moved_here.values()], f"{module.origin}::{module.item.name}")
            if kept is None:
                texts[j] = None
            elif len(leaves(kept)) != len(leaves(tree)):
                texts[j] = f"use {render_use(kept)};"
            for dest, code in moved_here.items():
                lines = test_use_lines(tree, code, module.origin, moved_names)
                (shared_uses if dest == "mod" else out[dest].test_uses).extend(lines)
        start, end = body_bounds(src, module.item)
        body = replace_items(src.text[start:end], [shift(i, -start) for i in inner], texts)
        test_replacement[module.origin][module.index] = src.text[module.item.start : start] + body + src.text[end : module.item.end]
    for k, text in test_replacement["ecfs"].items():
        replacement[k] = text

    files = {
        ECFS: replace_items(ecfs.text, top, [replacement.get(k, ecfs.text[it.start : it.end]) for k, it in enumerate(top)]),
        ECFS_TEST: replace_items(
            ecfs_test.text,
            test_top,
            [test_replacement["ecfs_test"].get(k, ecfs_test.text[it.start : it.end]) for k, it in enumerate(test_top)],
        ),
    }
    shared_names = {m.inner[j].name for m in modules for j, d in m.dest.items() if d == "mod"}
    for stem, (batch, what, _) in BATCHES.items():
        files[f"{EDGE_DIR}/{stem}.rs"] = batch_file(batch, what, out[stem])
        if out[stem].tests:
            files[f"{EDGE_DIR}/tests/{stem}.rs"] = test_file(stem, batch, out[stem], shared_names)
    test_stems = [stem for stem in BATCHES if out[stem].tests]
    files[f"{EDGE_DIR}/mod.rs"] = mod_file(bool(test_stems))
    if test_stems:
        files[f"{EDGE_DIR}/tests/mod.rs"] = tests_mod_file(shared, shared_uses, test_stems)
    for path, text in files.items():
        if path.startswith(EDGE_DIR):
            hit = S3S_PATH.search(text)
            if hit:
                line = text[line_start(text, hit.start()) :].splitlines()[0]
                raise ShapeError(f"{path} would name an s3s path, which the footprint guard forbids: {line.strip()!r}")
    return files


def shift(item: Item, delta: int) -> Item:
    return Item(item.start + delta, item.end + delta, item.head + delta, item.kind, item.name, item.visibility)


def test_use_lines(tree: UseNode, code: str, origin: str, moved_names: dict[str, str]) -> list[str]:
    """The `use` items a moved test needs out of one `use` item of the module it left."""
    lines, plain, moved = [], [], {}
    for leaf in leaves(tree):
        if leaf.binding == "*" or leaf.path[0] == "s3s" or not leaf_used(leaf, code):
            continue  # s3s names come through the ecfs glob
        path = leaf.path
        if origin == "ecfs" and path[0] == "super":
            path = ("crate", "storage", "ecfs") + path[1:]
        if path[:3] == ("crate", "storage", "ecfs") and len(path) == 4:
            if path[3] in moved_names:
                moved.setdefault(moved_names[path[3]], []).append(path[3])
            continue  # the rest of ecfs.rs comes through the ecfs glob
        plain.append(leaf)
    for stem, names in moved.items():
        inner = names[0] if len(names) == 1 else "{" + ", ".join(names) + "}"
        lines.append(f"use super::super::{stem}::{inner};")
    if plain:
        kept = filter_use(tree, lambda leaf: leaf in plain)
        if kept is not None:
            if origin == "ecfs":
                kept = rebase_use(kept, "super", ("crate", "storage", "ecfs"))
            lines.insert(0, f"use {render_use(kept)};")
    return lines


def doc_comment(paragraph: str) -> str:
    return "".join(f"//! {line}\n" for line in textwrap.wrap(paragraph, width=96, break_on_hyphens=False))


def dedupe(lines: list[str]) -> list[str]:
    return list(dict.fromkeys(lines))


def batch_file(batch: str, what: str, out: BatchOutput) -> str:
    parts = [
        LICENSE_HEADER,
        "\n",
        doc_comment(f"Legacy s3s edge of {what}: batch {batch} of the gateway migration (rustfs/backlog#2765)."),
        "//!\n",
        doc_comment(
            "`impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the "
            "former trait method bodies, unchanged; the batch that moves these operations onto the gateway owns "
            "this file and is the only one that edits it."
        ),
        "\n",
        ECFS_GLOB + "\n",
    ]
    parts += [line + "\n" for line in dedupe(out.uses)]
    for text in out.free:
        parts += ["\n", text, "\n"]
    for doc, fns in out.helper_blocks:
        parts += ["\n", doc, "impl FS {\n", "\n\n".join(fns), "\n}\n"]
    parts += ["\n", "impl FS {\n", "\n\n".join(out.edges), "\n}\n"]
    return "".join(parts)


def test_file(stem: str, batch: str, out: BatchOutput, shared: set[str]) -> str:
    code = masked("\n".join(out.tests))
    fixtures = sorted(name for name in shared if name in all_identifiers(code))
    parts = [
        LICENSE_HEADER,
        "\n",
        doc_comment(f"Tests of the `{stem}` legacy-edge handlers (batch {batch}), moved unchanged from `ecfs.rs` and `ecfs_test.rs`."),
        "\n",
        ECFS_GLOB + "\n",
    ]
    if fixtures:
        inner = fixtures[0] if len(fixtures) == 1 else "{" + ", ".join(fixtures) + "}"
        parts.append(f"use super::{inner};\n")
    parts += [line + "\n" for line in dedupe(out.test_uses)]
    for text in out.tests:
        parts += ["\n", text.rstrip("\n"), "\n"]
    return "".join(parts)


def tests_mod_file(fixtures: list[str], uses: list[str], stems: list[str]) -> str:
    parts = [
        LICENSE_HEADER,
        "\n",
        "//! Tests of the legacy-edge handlers, one module per migration batch, and the fixtures they\n",
        "//! share.\n",
        "\n",
        ECFS_GLOB + "\n",
    ]
    parts += [line + "\n" for line in dedupe(uses)]
    for text in fixtures:
        parts += ["\n", text.rstrip("\n"), "\n"]
    parts.append("\n")
    parts += [f"mod {stem};\n" for stem in sorted(stems)]
    return "".join(parts)


def mod_file(with_tests: bool) -> str:
    parts = [
        LICENSE_HEADER,
        "\n",
        "//! The bodies of `impl S3 for FS`, one file per batch of the s3s-to-gateway migration\n",
        "//! (rustfs/backlog#2765), so that parallel batches edit disjoint files. `ecfs.rs` keeps the\n",
        "//! trait impl, which delegates every method here, and the helpers several batches share.\n",
        "//! `scripts/codemods/split_ecfs_legacy_edge.py` produced this split from the unsplit file.\n",
        "\n",
    ]
    parts += [f"mod {stem};\n" for stem in sorted(BATCHES)]
    if with_tests:
        parts += ["\n", "#[cfg(test)]\n", "mod tests;\n"]
    return "".join(parts)


def write(root: Path, files: dict[str, str], fmt: bool) -> list[Path]:
    written = []
    for rel, text in sorted(files.items()):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written.append(path)
    if fmt:
        proc = subprocess.run(["rustfmt", "--edition", "2024", *map(str, written)], check=False)
        if proc.returncode != 0:
            raise ShapeError("rustfmt failed on the written files")
    return written


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", type=Path, help="repository root (default: the git top level)")
    parser.add_argument("--no-fmt", action="store_true", help="do not run rustfmt on the written files")
    parser.add_argument("--self-test", action="store_true", help="run this codemod's own tests")
    args = parser.parse_args(argv)
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    root = args.root
    if root is None:
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False)
        if top.returncode != 0:
            print("error: not inside a git checkout; pass --root", file=sys.stderr)
            return 2
        root = Path(top.stdout.strip())
    before = {rel: (root / rel).read_text(encoding="utf-8").count("\n") for rel in (ECFS, ECFS_TEST) if (root / rel).is_file()}
    try:
        files = split(root)
        write(root, files, not args.no_fmt)
    except ShapeError as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    for rel in sorted(files):
        after = (root / rel).read_text(encoding="utf-8").count("\n")
        print(f"{rel}: {before[rel]} -> {after} lines" if rel in before else f"{rel}: {after} lines (new)")
    return 0


# ---------------------------------------------------------------------------
# Self-test

FIXTURE_ECFS = (
    LICENSE_HEADER
    + """
use super::helper_dep;
use s3s::{S3, S3Request, S3Response, S3Result, dto::*};
use tracing::{debug, instrument};

const SHARED: &str = "x";

fn only_meta(value: &str) -> &str {
    helper_dep(value)
}

pub struct FS {}

#[async_trait::async_trait]
impl S3 for FS {
    #[instrument(level = "debug", skip(self, req))]
    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        let _ = only_meta(SHARED);
        Ok(S3Response::new(s3s::dto::GetObjectAclOutput::default()))
    }

    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        debug!("{SHARED}");
        Ok(S3Response::new(ListBucketsOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::FS;
    use http::Method;

    fn fixture() -> FS {
        FS {}
    }

    #[tokio::test]
    async fn moves() {
        let fs = fixture();
        let _ = fs.get_object_acl(request(Method::GET)).await;
    }

    #[test]
    fn stays() {
        let _ = Method::PUT;
    }
}
"""
)

FIXTURE_TEST = LICENSE_HEADER + "\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn stays() {}\n}\n"


class SelfTest(unittest.TestCase):
    def setUp(self) -> None:
        self.saved = (dict(BATCHES), dict(KEEP_INLINE))
        BATCHES.clear()
        BATCHES["object_meta"] = ("B1", "object metadata", ("get_object_acl",))

    def tearDown(self) -> None:
        BATCHES.clear()
        BATCHES.update(self.saved[0])
        KEEP_INLINE.clear()
        KEEP_INLINE.update(self.saved[1])
        refresh_owner()

    def tree(self, ecfs: str = FIXTURE_ECFS, test: str = FIXTURE_TEST) -> Path:
        root = Path(tempfile.mkdtemp())
        (root / ECFS).parent.mkdir(parents=True)
        (root / ECFS).write_text(ecfs, encoding="utf-8")
        (root / ECFS_TEST).write_text(test, encoding="utf-8")
        return root

    def test_mask_blanks_comments_and_literals(self) -> None:
        mask, strings = mask_source('a // b\nc "d;}" \'{\' r#"x"# /* e */ f')
        for hidden in ("b", ";", "{", "x", "e"):
            self.assertNotIn(hidden, mask)
        self.assertIn("f", mask)
        self.assertEqual(len(strings), 2)

    def test_inline_format_arguments_count_as_uses(self) -> None:
        code = masked('debug!("{SHARED} {err:?} {{literal}}")')
        self.assertIn("SHARED", free_identifiers(code))
        self.assertIn("err", free_identifiers(code))
        self.assertNotIn("literal", free_identifiers(code))

    def test_use_tree_round_trip_filter_and_rebase(self) -> None:
        tree = parse_use("use super::a::{self, b::{C, D as E}, f as _};")
        self.assertEqual(render_use(tree), "super::a::{self, b::{C, D as E}, f as _}")
        kept = filter_use(tree, lambda leaf: leaf.binding in {"a", "E"})
        self.assertEqual(render_use(rebase_use(kept, "super", ("crate", "storage"))), "crate::storage::a::{self, b::D as E}")
        self.assertEqual(render_use(filter_use(tree, lambda leaf: leaf.binding == "a")), "super::a")
        self.assertIsNone(filter_use(tree, lambda leaf: False))
        with self.assertRaises(ShapeError):
            parse_use("pub use a::b;")

    def test_usage_ignores_paths_fields_and_variables_named_like_macros(self) -> None:
        code = masked("x.acl; crate::s3_api::f(); let error = 1; Foo::new(); // Bar\n")
        self.assertNotIn("acl", free_identifiers(code))
        self.assertNotIn("s3_api", free_identifiers(code))
        self.assertFalse(leaf_used(Leaf(("tracing", "error"), "error", None), code))
        self.assertTrue(leaf_used(Leaf(("tracing", "error"), "error", None), masked("error!(x)")))
        self.assertTrue(leaf_used(Leaf(("m", "Foo"), "Foo", None), code))
        self.assertFalse(leaf_used(Leaf(("m", "Bar"), "Bar", None), code))
        self.assertTrue(leaf_used(Leaf(("m", "ObjectOperations"), "_", None), masked("store\n  .get_object_info(a)")))
        self.assertFalse(leaf_used(Leaf(("m", "ObjectOperations"), "_", None), masked("self.copy_object_edge(a)")))

    def test_fn_local_import_is_not_a_use_of_the_module_import(self) -> None:
        method = Leaf(("http", "Method"), "Method", None)
        self.assertFalse(leaf_used(method, scoped(masked("fn f() { use http::Method; let _ = Method::GET; }"))))
        self.assertTrue(leaf_used(method, scoped(masked("fn f() { let _ = Method::GET; }"))))

    def test_trait_ecfs_keeps_is_not_imported_again_by_the_batch_file(self) -> None:
        ecfs = FIXTURE_ECFS.replace(
            "use tracing::{debug, instrument};\n",
            "use tracing::{debug, instrument};\nuse super::contract::{BucketOperations as _, ObjectOperations as _};\n",
        ).replace(
            "pub struct FS {}\n",
            "pub struct FS {}\n\npub fn tags(store: &S) {\n    store.get_object_tags();\n}\n",
        ).replace(
            "let _ = only_meta(SHARED);",
            "let _ = only_meta(SHARED);\n        store.get_object_info();\n        store.get_bucket_info();",
        )
        files = split(self.tree(ecfs=ecfs))
        edge = files[f"{EDGE_DIR}/object_meta.rs"]
        self.assertNotIn("ObjectOperations", edge, "the ecfs glob already supplies the trait")
        self.assertIn("BucketOperations as _", edge, "ecfs.rs no longer imports this trait")
        self.assertIn("ObjectOperations as _", files[ECFS])
        self.assertNotIn("BucketOperations", files[ECFS])

    def test_import_without_a_visible_use_is_refused(self) -> None:
        ecfs = FIXTURE_ECFS.replace("use tracing::{debug, instrument};\n", "use tracing::{debug, instrument};\nuse m::UnlistedTrait;\n")
        with self.assertRaisesRegex(ShapeError, "UnlistedTrait in .* has no use the scan can see"):
            split(self.tree(ecfs=ecfs))

    def test_anonymous_trait_without_method_list_is_refused(self) -> None:
        with self.assertRaisesRegex(ShapeError, "no method list"):
            leaf_used(Leaf(("m", "Unknown"), "_", None), "x.y()")

    def test_split_moves_body_helper_imports_and_test(self) -> None:
        files = split(self.tree())
        ecfs, edge = files[ECFS], files[f"{EDGE_DIR}/object_meta.rs"]
        moved_test = files[f"{EDGE_DIR}/tests/object_meta.rs"]
        self.assertIn("self.get_object_acl_edge(req).await", ecfs)
        self.assertIn('#[instrument(name = "get_object_acl", level = "debug"', edge)
        self.assertIn("pub(in crate::storage::ecfs) async fn get_object_acl_edge(", edge)
        self.assertIn("fn only_meta", edge)
        self.assertNotIn("fn only_meta", ecfs)
        self.assertIn("const SHARED", ecfs, "a helper the inline method uses stays")
        self.assertIn("use crate::storage::helper_dep;", edge)
        self.assertNotIn("helper_dep", ecfs)
        self.assertNotIn("instrument", ecfs.split("impl S3 for FS")[0])
        self.assertIn('debug!("{SHARED}")', ecfs, "the inline method keeps its body")
        self.assertIn("GetObjectAclOutput::default()", edge)
        self.assertIsNone(S3S_PATH.search(edge))
        self.assertIn("async fn moves()", moved_test)
        self.assertIn("fn fixture()", moved_test, "a fixture only the moved test uses moves with it")
        self.assertIn("use http::Method;", moved_test)
        self.assertIn("fn stays()", ecfs)
        self.assertNotIn("fixture", ecfs)
        self.assertIn("use http::Method;", ecfs, "the staying test still uses Method")
        self.assertNotIn("use super::FS;", ecfs, "no staying test uses FS")

    def test_unassigned_method_is_refused(self) -> None:
        KEEP_INLINE.clear()
        with self.assertRaisesRegex(ShapeError, r"without a batch assignment: \['list_buckets'\]"):
            split(self.tree())

    def test_assigned_method_missing_from_the_impl_is_refused(self) -> None:
        BATCHES["object_meta"] = ("B1", "object metadata", ("get_object_acl", "put_object_acl"))
        with self.assertRaisesRegex(ShapeError, "absent from impl S3 for FS"):
            split(self.tree())

    def test_split_tree_is_refused(self) -> None:
        root = self.tree()
        (root / EDGE_DIR).mkdir()
        with self.assertRaisesRegex(ShapeError, "already exists"):
            split(root)
        delegating = FIXTURE_ECFS.replace("let _ = only_meta(SHARED);", "self.get_object_acl_edge(req).await;")
        with self.assertRaisesRegex(ShapeError, "already split"):
            split(self.tree(ecfs=delegating))

    def test_test_reaching_two_batches_is_refused(self) -> None:
        KEEP_INLINE.clear()
        BATCHES["other"] = ("X", "other", ("list_buckets",))
        both = FIXTURE_ECFS.replace("let _ = fs.get_object_acl(", "let _ = fs.list_buckets(r); let _ = fs.get_object_acl(")
        with self.assertRaisesRegex(ShapeError, "several batches"):
            split(self.tree(ecfs=both))

    def test_fixture_shared_by_moving_and_staying_tests_is_refused(self) -> None:
        shared = FIXTURE_ECFS.replace("let _ = Method::PUT;", "let _ = fixture();")
        with self.assertRaisesRegex(ShapeError, "shared by moving and staying"):
            split(self.tree(ecfs=shared))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
