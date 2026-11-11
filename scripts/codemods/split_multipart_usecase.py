#!/usr/bin/env python3
"""Split rustfs/src/app/multipart_usecase.rs into one file per operation.

Preparatory refactor P3 of the s3s to rustfs-gateway migration. A pure code
move: every top-level item, every method of `impl DefaultMultipartUsecase` and
every test is assigned to a target file by the explicit tables below, and is
moved verbatim with its attributes and doc comments. Nothing is renamed and no
body changes, with these exceptions, each mechanical and listed here so a
reviewer knows where the output differs from the input:

  * the three test fixtures (`s3_op_total`, `build_request`, `make_usecase`)
    gain `pub(super)` because they move to `test_support.rs`;
  * the test module's `use` items become `pub(super) use` re-exports in
    `test_support.rs` (names the parent already imports are not repeated), so
    every per-operation test module reaches them with one glob;
  * code-position `s3s::` paths in the new files become
    `crate::app::storage_api::s3::`, the sanctioned s3s funnel, so the number
    of files naming s3s (scripts/check_s3s_footprint.sh) does not grow. The
    funnel gains two re-exports, `dto` and `xml`, in rustfs/src/app/storage_api.rs;
  * the nextest filter that names the one test it pins follows the test to its
    new module path.

Layout produced (parent keeps the struct, its constructors and accessors, the
shared helpers and the test fixtures):

  multipart_usecase.rs                   shared state and helpers
  multipart_usecase/test_support.rs      fixtures and test-only imports
  multipart_usecase/{abort,create,list,upload_part,complete,upload_part_copy}.rs
                                         one operation: its helpers, its
                                         `impl DefaultMultipartUsecase` block,
                                         its tests
  multipart_usecase/upload_part/tests/body_read_tests.rs
  multipart_usecase/upload_part_copy/tests/copy_admission_tests.rs
                                         the two former test files, unchanged

Usage:
  scripts/codemods/split_multipart_usecase.py [--no-fmt] [--report]
  scripts/codemods/split_multipart_usecase.py --self-test

The input is the unsplit tree. A tree that is already split is refused, as is
any top-level item, method or test without an assignment, any table entry that
matches nothing, and any private item that a file other than its own refers
to. Re-running therefore means restoring the pre-split files first:

  git checkout <pre-split-commit> -- rustfs/src/app/multipart_usecase.rs \\
      rustfs/src/app/multipart_usecase rustfs/src/app/storage_api.rs .config/nextest.toml
  git clean -fd rustfs/src/app/multipart_usecase

and then running this script. rustfmt formats the written files unless
--no-fmt is given. --report prints the item to file assignment and the
cross-file reference check and writes nothing.

Exit status: 0 on success, 1 when the input cannot be split as asked (the
message says why), 2 for a usage error.
"""

import argparse
import re
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
APP = "rustfs/src/app"
SOURCE = f"{APP}/multipart_usecase.rs"
OUT_DIR = f"{APP}/multipart_usecase"
STORAGE_API = f"{APP}/storage_api.rs"
NEXTEST = ".config/nextest.toml"
STRUCT = "DefaultMultipartUsecase"
FUNNEL = "crate::app::storage_api::s3::"

PARENT = "parent"
SUPPORT = "test_support"
OPS = ("abort", "create", "list", "upload_part", "complete", "upload_part_copy")
OP_DOC = {
    "abort": "AbortMultipartUpload.",
    "create": "CreateMultipartUpload.",
    "list": "ListMultipartUploads and ListParts.",
    "upload_part": "UploadPart.",
    "complete": "CompleteMultipartUpload.",
    "upload_part_copy": "UploadPartCopy.",
}

# Test files that already existed, moved unchanged: (old path, operation).
OLD_TEST_FILES = {
    "body_read_tests": (f"{OUT_DIR}/tests/body_read_tests.rs", "upload_part"),
    "copy_admission_tests": (f"{OUT_DIR}/tests/copy_admission_tests.rs", "upload_part_copy"),
}

# ---------------------------------------------------------------------------
# Assignment tables: item name -> target file. Every name must be listed; the
# script fails on a missing or an unused entry.
# ---------------------------------------------------------------------------

def assigned(target, names):
    return {name: target for name in names.split()}


# Top-level functions (everything that is not an import, the struct, its impl
# or the test module). A helper stays in the parent when more than one
# operation, or a cross-operation test, uses it.
FUNCTIONS = {
    **assigned(
        PARENT,
        """
        merge_part_encryption_metadata multipart_plaintext_size multipart_part_numbers
        multipart_max_total_object_size validate_table_catalog_object_mutation
        internal_object_info_lookup_opts multipart_upload_api_error
        """,
    ),
    **assigned(
        "upload_part_copy",
        "normalize_copy_source_version_id format_copy_source_version_id validate_copy_source_range_not_exceeds",
    ),
    **assigned(
        "complete",
        """
        validate_complete_multipart_parts normalize_complete_multipart_parts complete_part_from_s3
        has_complete_multipart_object_lock_headers validate_complete_multipart_checksum_type
        quota_accounting_object_size encode_s3_path extract_request_scheme extract_request_host
        build_complete_multipart_location
        """,
    ),
    **assigned("create", "create_multipart_upload_metadata should_advertise_session_compression"),
    **assigned(
        "upload_part",
        """
        resolve_upload_part_size require_upload_part_size upload_part_body_read_timeout
        passthrough_part_actual_size normalize_presigned_part_checksums
        """,
    ),
}

# Methods of `impl DefaultMultipartUsecase`.
METHODS = {
    **assigned(
        PARENT,
        "without_context from_global with_context with_context_and_concurrency_manager"
        " bucket_metadata_sys object_store object_data_cache concurrency_manager",
    ),
    **assigned("abort", "execute_abort_multipart_upload"),
    **assigned("complete", "execute_complete_multipart_upload execute_complete_multipart_upload_owned"),
    **assigned("create", "execute_create_multipart_upload"),
    **assigned("upload_part", "execute_upload_part"),
    **assigned("list", "execute_list_multipart_uploads execute_list_parts"),
    **assigned("upload_part_copy", "execute_upload_part_copy"),
}

# Items of `mod tests`. A test that drives more than one operation, and the
# tests of the shared helpers, stay in the parent's own `mod tests`.
TESTS = {
    **assigned(SUPPORT, "s3_op_total build_request make_usecase"),
    **assigned(
        PARENT,
        """
        internal_object_info_lookup_opts_drops_http_preconditions
        merge_part_encryption_metadata_keeps_source_metadata_unchanged
        managed_multipart_roundtrip_preserves_session_nonce_between_parts
        multipart_upload_rejects_reserved_directory_markers
        multipart_max_total_object_size_reads_compatible_internal_metadata
        multipart_upload_api_error_reports_malformed_upload_id_as_no_such_upload
        multipart_handlers_reject_another_callers_denied_kms_upload
        """,
    ),
    **assigned("abort", "execute_abort_multipart_upload_returns_internal_error_when_store_uninitialized"),
    **assigned(
        "create",
        """
        session_compression_is_advertised_only_for_non_passthrough_compressible_uploads
        execute_create_multipart_upload_rejects_invalid_storage_class
        create_multipart_upload_metadata_persists_dto_storage_class_without_raw_header
        create_multipart_upload_metadata_keeps_user_and_system_namespaces_separate
        create_multipart_rejects_ciphertext_replication_before_parts_are_staged
        """,
    ),
    **assigned(
        "list",
        """
        execute_list_multipart_uploads_returns_internal_error_when_store_uninitialized
        execute_list_multipart_uploads_rejects_invalid_key_marker_before_store_lookup
        execute_list_multipart_uploads_rejects_invalid_max_uploads_before_store_lookup
        execute_list_parts_returns_internal_error_when_store_uninitialized
        execute_list_parts_rejects_negative_part_number_marker_before_store_lookup
        execute_list_parts_rejects_invalid_max_parts_before_store_lookup
        """,
    ),
    **assigned(
        "upload_part",
        """
        presigned_checksum_request
        normalize_presigned_part_checksums_validates_body
        normalize_presigned_part_checksums_accepts_sdk_crc32_default
        normalize_presigned_part_checksums_rejects_ambiguous_values
        normalize_presigned_part_checksums_preserves_headers_and_rejects_invalid_values
        normalize_presigned_part_checksums_requires_verified_presign
        resolve_upload_part_size_uses_normalized_logical_length
        upload_part_length_contract_rejects_unknown_and_all_negative_lengths
        upload_part_timeout_policy_preserves_disabled_and_capped_floor
        execute_upload_part_rejects_missing_body
        execute_upload_part_rejects_oversize_declared_part_before_reading_the_body
        execute_upload_part_rejects_invalid_part_number_before_body_lookup
        execute_upload_part_rejects_unknown_length_before_admission_or_body_polling
        execute_upload_part_progress_timeout_releases_admission_and_leaves_no_part
        execute_upload_part_rejects_when_foreground_write_admission_is_full
        """,
    ),
    **assigned(
        "upload_part_copy",
        """
        execute_upload_part_copy_returns_internal_error_when_store_uninitialized
        execute_upload_part_copy_rejects_invalid_part_number_before_store_lookup
        test_validate_copy_source_range_not_exceeds_returns_invalid_range_when_range_exceeds
        test_validate_copy_source_range_not_exceeds_ok_when_range_valid
        test_validate_copy_source_range_not_exceeds_ok_for_suffix_range
        upload_part_copy_normalizes_and_formats_source_version_id
        """,
    ),
    **assigned(
        "complete",
        """
        upload_metadata_with_checksum_type headers_with_checksum_type
        complete_multipart_checksum_type_matching_the_upload_is_accepted
        complete_multipart_checksum_type_contradicting_the_upload_is_rejected
        complete_multipart_checksum_type_without_both_sides_is_accepted
        quota_accounting_uses_logical_size_when_available
        quota_accounting_fails_closed_only_when_quota_is_configured
        test_build_complete_multipart_location_uses_forwarded_proto_and_encodes_key
        test_build_complete_multipart_location_falls_back_to_uri_authority_and_scheme
        test_build_complete_multipart_location_returns_path_without_host
        execute_complete_multipart_upload_rejects_missing_parts_payload
        execute_complete_multipart_upload_rejects_missing_parts_list
        compressed_complete_records_logical_quota_usage_and_overwrite_delta
        authorized_replica_only_multipart_complete_preserves_persisted_anti_cascade_state
        authorized_replica_only_multipart_complete_preserves_persisted_anti_cascade_state_inner
        ssec_completion_and_replay_require_key_before_disclosing_plaintext_checksums
        assert_multipart_etag_listings
        kms_multipart_completion_and_replay_preserve_etag_in_versioned_listings
        conditional_multipart_upload_if_none_match_returns_precondition_failed_when_object_exists
        conditional_multipart_upload_if_none_match_returns_precondition_failed_when_object_exists_inner
        concurrent_completions_share_durable_bucket_quota_reservations
        multipart_completion_commits_after_quota_fleet_proof_loss_before_rename
        data_movement_multipart_completion_has_zero_quota_growth
        cancelled_multipart_completion_emits_once_and_replay_does_not_emit
        rejected_empty_parts_preserve_existing_object_and_staging
        execute_complete_multipart_upload_allows_duplicate_part_numbers_by_using_last_occurrence
        execute_complete_multipart_upload_rejects_out_of_order_parts
        validate_complete_multipart_parts_enforces_part_number_range
        normalize_complete_multipart_parts_keeps_last_duplicate_part
        execute_complete_multipart_upload_rejects_object_lock_headers
        """,
    ),
}

# Test module items that are not code to assign.
TEST_MODS = ("body_read_tests", "copy_admission_tests")

CODE, COMMENT, LITERAL = 0, 1, 2
IDENT_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
CHAR_LITERAL = re.compile(r"'(?:\\(?:[nrt\\0'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\})|[^\\'\n])'")
RAW_STRING_START = re.compile(r'r(#*)"')
# A relative path in code moved one module down: `super::object::..` named a sibling of
# multipart_usecase, and from an operation file that is `super::super::object::..`.
SUPER_PATH = re.compile(r"(?<![A-Za-z0-9_:])super::(?!\*)")
S3S_PATH = re.compile(r"(?<![\"A-Za-z0-9_])s3s::")
VIS = r"(?:pub(?:\([^)]*\))?\s+)?"
FN_HEAD = re.compile(r"^" + VIS + r"(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)")
STRUCT_HEAD = re.compile(r"^" + VIS + r"struct\s+([A-Za-z_][A-Za-z0-9_]*)")
IMPL_HEAD = re.compile(r"^impl\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
MOD_BLOCK_HEAD = re.compile(r"^" + VIS + r"mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
MOD_DECL_HEAD = re.compile(r"^" + VIS + r"mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;")
USE_HEAD = re.compile(r"^" + VIS + r"use\s")


class SplitError(Exception):
    """The input cannot be split as asked: exit 1."""


# ---------------------------------------------------------------------------
# Lexing: blank comments and literals so structure can be read from the rest.
# ---------------------------------------------------------------------------


def classify(text):
    """Return one kind byte per character: CODE, COMMENT or LITERAL."""
    kinds = bytearray(len(text))
    i, n = 0, len(text)

    def mark(start, end, kind):
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
        elif ch == "r" and (i == 0 or text[i - 1] not in IDENT_CHARS or (text[i - 1] == "b" and (i == 1 or text[i - 2] not in IDENT_CHARS))):
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


def code_only(text):
    """`text` with comments and literals replaced by spaces, newlines kept."""
    kinds = classify(text)
    return "".join(ch if kinds[i] == CODE or ch == "\n" else " " for i, ch in enumerate(text))


# ---------------------------------------------------------------------------
# Item splitting. The input is rustfmt output, so an item is the run of lines
# from its first attribute or comment to the line on which brackets balance
# and the line ends in `}` or `;`.
# ---------------------------------------------------------------------------


class Item:
    def __init__(self, lines, code_lines):
        self.lines = lines  # original lines, leading comments and attributes included
        self.code_lines = code_lines  # the same lines with comments and literals blanked
        self.head = next((line.strip() for line in code_lines if line.strip() and not line.strip().startswith("#[")), "")
        self.cfg_test = any(line.strip() == "#[cfg(test)]" for line in lines)

    @property
    def text(self):
        return "\n".join(self.lines)

    @property
    def code(self):
        return "\n".join(self.code_lines)

    def name(self):
        for pattern in (FN_HEAD, STRUCT_HEAD, IMPL_HEAD, MOD_BLOCK_HEAD, MOD_DECL_HEAD):
            match = pattern.match(self.head)
            if match:
                return match.group(1)
        return None

    def is_use(self):
        return bool(USE_HEAD.match(self.head))


def split_items(lines, code_lines, what):
    """Cut `lines` into items; blank lines separate them."""
    items, i, n = [], 0, len(lines)
    while i < n:
        if not lines[i].strip():
            i += 1
            continue
        start, balance, j = i, 0, i
        while j < n:
            stripped = code_lines[j].rstrip()
            balance += sum(stripped.count(c) for c in "([{") - sum(stripped.count(c) for c in ")]}")
            if balance == 0 and stripped.endswith(("}", ";")):
                break
            j += 1
        if j >= n:
            raise SplitError(f"{what}: an item starting at line {start + 1} never closes")
        items.append(Item(lines[start : j + 1], code_lines[start : j + 1]))
        i = j + 1
    return items


def canonical(items, separator):
    return separator.join(item.text for item in items)


# ---------------------------------------------------------------------------
# Use trees.
# ---------------------------------------------------------------------------


def split_top(inner):
    parts, depth, current = [], 0, []
    for ch in inner:
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return [p.strip() for p in parts if p.strip()]


def flatten_use(tree, prefix=""):
    """[(full path, bound name)] for a use tree; a glob binds `*`, `as _` binds `_`."""
    tree = tree.strip()
    brace = tree.find("{")
    if brace >= 0:
        head = tree[:brace].strip()
        inner = tree[brace + 1 : tree.rindex("}")]
        base = prefix + head
        found = []
        for part in split_top(inner):
            found.extend(flatten_use(part, base))
        return found
    alias = None
    if " as " in tree:
        tree, alias = (side.strip() for side in tree.split(" as ", 1))
    path = prefix + tree
    last = path.split("::")[-1]
    if last == "self":
        path = path[: -len("::self")]
        last = path.split("::")[-1]
    return [(path, alias if alias else last)]


def use_paths(item):
    body = re.sub(r"^\s*(?:#\[[^\n]*\]\s*)*", "", item.code, flags=re.S)
    body = re.sub(r"^" + VIS + r"use\s+", "", body.strip())
    body = body.rstrip().rstrip(";")
    return flatten_use(body)


# ---------------------------------------------------------------------------
# The parsed input.
# ---------------------------------------------------------------------------


class Parsed:
    pass


def parse(text):
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    code_lines = code_only("\n".join(lines)).split("\n")

    # Leading licence and `//!` lines are the file preamble.
    first = 0
    while first < len(lines) and (lines[first].startswith("//") or not lines[first].strip()):
        first += 1
    preamble = lines[:first]
    items = split_items(lines[first:], code_lines[first:], "multipart_usecase.rs")

    parsed = Parsed()
    parsed.preamble = preamble
    parsed.imports, parsed.functions, parsed.body = [], [], []
    parsed.struct = parsed.impl = parsed.tests = None
    for item in items:
        if not item.is_use():
            parsed.body.append(item)
        name = item.name()
        if item.is_use():
            if parsed.functions or parsed.struct or parsed.impl:
                raise SplitError("a `use` item follows code: imports are expected first")
            parsed.imports.append(item)
        elif STRUCT_HEAD.match(item.head) and name == STRUCT:
            parsed.struct = item
        elif IMPL_HEAD.match(item.head) and name == STRUCT:
            if parsed.impl is not None:
                raise SplitError(f"more than one `impl {STRUCT}` block")
            parsed.impl = item
        elif MOD_BLOCK_HEAD.match(item.head) and name == "tests":
            parsed.tests = item
        elif FN_HEAD.match(item.head):
            item.after_impl = parsed.impl is not None
            parsed.functions.append(item)
        elif MOD_DECL_HEAD.match(item.head):
            raise SplitError(f"{SOURCE} already declares {item.head!r}: it is split already; restore the pre-split files first")
        else:
            raise SplitError(f"unrecognised top-level item: {item.head!r}")
    for what, value in (("struct", parsed.struct), ("impl", parsed.impl), ("tests module", parsed.tests)):
        if value is None:
            raise SplitError(f"the {what} was not found; is the input already split?")

    # Spacing must be the canonical one, or the output would not be a move.
    rebuilt = "\n".join(preamble) + "\n" + "\n".join(i.text for i in parsed.imports) + "\n\n"
    rebuilt += canonical(parsed.body, "\n\n")
    if "\n".join(lines) != "\n".join(rebuilt.split("\n")):
        raise SplitError("the item layout is not the one this script reassembles (spacing differs from rustfmt's)")

    parsed.impl_methods = body_items(parsed.impl, "impl")
    parsed.test_items = body_items(parsed.tests, "mod tests")
    return parsed


def body_items(block, what):
    """Items between the opening line and the closing brace of `block`."""
    opening = next(i for i, line in enumerate(block.code_lines) if line.strip() and not line.strip().startswith("#["))
    inner, inner_code = block.lines[opening + 1 : -1], block.code_lines[opening + 1 : -1]
    items = split_items(inner, inner_code, what)
    return items


# ---------------------------------------------------------------------------
# Assignment and checks.
# ---------------------------------------------------------------------------


def assign(items, table, what):
    placed = {}
    seen = set()
    for item in items:
        name = item.name()
        if name is None:
            raise SplitError(f"{what}: cannot name the item {item.head!r}")
        if name in seen:
            raise SplitError(f"{what}: two items are named {name}")
        seen.add(name)
        target = table.get(name)
        if target is None:
            raise SplitError(f"{what}: no assignment for {name} ({item.head[:60]!r})")
        placed[name] = (target, item)
    unused = sorted(set(table) - seen)
    if unused:
        raise SplitError(f"{what}: assignments that match nothing: {', '.join(unused)}")
    return placed


def check_targets(table, what, allowed):
    for name, target in table.items():
        if target not in allowed:
            raise SplitError(f"{what}: {name} is assigned to unknown file {target!r}")


def word(name):
    return re.compile(r"(?<![A-Za-z0-9_])" + re.escape(name) + r"(?![A-Za-z0-9_])")


def referenced_from(name, texts, own):
    """Files other than `own` whose code mentions `name`."""
    pattern = word(name)
    return sorted(f for f, code in texts.items() if f != own and pattern.search(code))


def plan(parsed):
    """Decide the content of every output file; returns (pieces, report)."""
    check_targets(FUNCTIONS, "functions", {PARENT, *OPS})
    check_targets(METHODS, "methods", {PARENT, *OPS})
    check_targets(TESTS, "tests", {PARENT, SUPPORT, *OPS})
    functions = assign(parsed.functions, FUNCTIONS, "functions")
    methods = assign(parsed.impl_methods, METHODS, "methods")

    test_code_items = []
    test_uses, test_mods, test_glob = [], [], None
    for item in parsed.test_items:
        if item.is_use():
            if item.head.replace(" ", "") == "usesuper::*;":
                test_glob = item
            else:
                test_uses.append(item)
        elif MOD_DECL_HEAD.match(item.head):
            if item.name() not in TEST_MODS:
                raise SplitError(f"mod tests: unexpected module declaration {item.head!r}")
            test_mods.append(item)
        else:
            test_code_items.append(item)
    if test_glob is None:
        raise SplitError("mod tests does not start with `use super::*;`")
    if sorted(m.name() for m in test_mods) != sorted(TEST_MODS):
        raise SplitError("mod tests: the declared test modules differ from TEST_MODS")
    tests = assign(test_code_items, TESTS, "tests")

    for placed in (functions, methods):
        for target, item in placed.values():
            if target != PARENT:
                push_down(item)

    # Piece lists per target file.
    pieces = {f: {"functions": [], "methods": [], "tests": []} for f in (PARENT, SUPPORT, *OPS)}
    for _, (target, item) in functions.items():
        pieces[target]["functions"].append(item)
    for _, (target, item) in methods.items():
        pieces[target]["methods"].append(item)
    for _, (target, item) in tests.items():
        pieces[target]["tests"].append(item)
    if pieces[SUPPORT]["functions"] or pieces[SUPPORT]["methods"]:
        raise SplitError("test_support holds tests-module fixtures only")
    pieces["_uses"] = test_uses
    pieces["_mods"] = test_mods
    return pieces, (functions, methods, tests)


def per_file_code(parsed, pieces, extra):
    """Code of each output file for the reference check (parent keeps imports)."""
    texts = {}
    for target in (PARENT, SUPPORT, *OPS):
        parts = []
        if target == PARENT:
            parts += [i.code for i in parsed.imports] + [parsed.struct.code]
        parts += [i.code for i in pieces[target]["functions"]]
        parts += [i.code for i in pieces[target]["methods"]]
        parts += [i.code for i in pieces[target]["tests"]]
        texts[target] = "\n".join(parts)
    for target, code in extra.items():
        texts[target] += "\n" + code
    return texts


def reference_check(pieces, texts):
    """A private item moved out of the parent must stay inside its own file.

    Items kept in the parent and fixtures in test_support are visible to every
    child through the globs, so only items placed in an operation file are
    checked. A name that is also a local binding elsewhere would show up here as
    a false alarm; the message names the files so the table can be adjusted.
    """
    problems = []
    for group in ("functions", "methods", "tests"):
        for target in OPS:
            for item in pieces[target][group]:
                name = item.name()
                if name is None or not FN_HEAD.match(item.head):
                    continue
                if re.match(r"^pub(\(|\s)", item.head) and group == "methods":
                    continue  # a public method is reached through the type
                users = referenced_from(name, texts, target)
                if users:
                    problems.append(f"{name} is placed in {target} but also mentioned in {', '.join(users)}")
    return problems


# ---------------------------------------------------------------------------
# Rendering.
# ---------------------------------------------------------------------------


def rewrite_code(text, pattern, replacement):
    """Replace `pattern` matches that sit in code, never in comments or literals."""
    kinds = classify(text)
    out, last = [], 0
    for match in pattern.finditer(text):
        if kinds[match.start()] != CODE:
            continue
        out.append(text[last : match.start()])
        out.append(replacement)
        last = match.end()
    out.append(text[last:])
    return "".join(out)


def rewrite_s3s(text):
    """Send code-position `s3s::` paths through the sanctioned funnel."""
    return rewrite_code(text, S3S_PATH, FUNNEL)


def push_down(item):
    """Make the relative paths of an item that leaves the parent module point where they did."""
    text = rewrite_code(item.text, SUPER_PATH, "super::super::")
    item.lines = text.split("\n")
    item.code_lines = code_only(text).split("\n")


def licence(preamble):
    block = []
    for line in preamble:
        if not line.startswith("//") or line.startswith("//!"):
            break
        block.append(line)
    return "\n".join(block)


def doc_line(preamble):
    return next(line for line in preamble if line.startswith("//!"))


def join_items(items):
    return "\n\n".join(item.text for item in items)


def render_operation(op, pieces, header, tests_extra_mods):
    out = [header, f"//! {OP_DOC[op]}", "", "use super::*;", ""]
    body = []
    if pieces[op]["functions"]:
        body.append(join_items(pieces[op]["functions"]))
    if pieces[op]["methods"]:
        body.append(f"impl {STRUCT} {{\n{join_items(pieces[op]['methods'])}\n}}")
    if pieces[op]["tests"] or tests_extra_mods:
        inner = [join_items(pieces[op]["tests"])] if pieces[op]["tests"] else []
        inner += [f"    mod {name};" for name in tests_extra_mods]
        body.append(
            "#[cfg(test)]\nmod tests {\n    use super::super::test_support::*;\n    use super::*;\n\n"
            + "\n\n".join(inner)
            + "\n}"
        )
    out.append("\n\n".join(body))
    return rewrite_s3s("\n".join(out) + "\n")


def render_test_support(parsed, pieces, header, parent_bound):
    out = [header, "//! Fixtures and test-only imports shared by the multipart use-case tests.", "", "use super::*;"]
    reexports, notes = [], []
    for item in pieces["_uses"]:
        for path, name in use_paths(item):
            if name in parent_bound:
                notes.append((name, parent_bound[name], path))
                continue
            reexports.append(f"pub(super) use {path};")
    out.extend(reexports)
    out.append("")
    fixtures = []
    for item in pieces[SUPPORT]["tests"]:
        lines = list(item.lines)
        for index, line in enumerate(lines):
            if FN_HEAD.match(line.strip()) and not line.startswith("    #"):
                lines[index] = re.sub(r"^(\s*)", r"\1pub(super) ", line, count=1)
                break
        fixtures.append("\n".join(line[4:] if line.startswith("    ") else line for line in lines))
    out.append("\n\n".join(fixtures))
    return rewrite_s3s("\n".join(out) + "\n"), notes


def render_parent(parsed, pieces, header):
    out = [header, "\n".join(line for line in parsed.preamble if line.startswith("//!")), ""]
    out.append("\n".join(i.text for i in parsed.imports))
    out.append("")
    mods = [f"mod {op};" for op in sorted(OPS + (SUPPORT,))]
    mods = [("#[cfg(test)]\n" + m) if SUPPORT in m else m for m in mods]
    out.append("\n".join(mods))
    out.append("")
    before = [i for i in pieces[PARENT]["functions"] if not i.after_impl]
    after = [i for i in pieces[PARENT]["functions"] if i.after_impl]
    chunks = [join_items(before)] if before else []
    chunks.append(
        f"{parsed.struct.text}\n\n{parsed.impl.lines[0]}\n{join_items(pieces[PARENT]['methods'])}\n}}"
    )
    if after:
        chunks.append(join_items(after))
    if pieces[PARENT]["tests"]:
        chunks.append(
            rewrite_s3s(
                "#[cfg(test)]\nmod tests {\n    use super::test_support::*;\n    use super::*;\n\n"
                + join_items(pieces[PARENT]["tests"])
                + "\n}"
            )
        )
    out.append("\n\n".join(chunks))
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Driver.
# ---------------------------------------------------------------------------


def bound_names(imports):
    bound = {}
    for item in imports:
        for path, name in use_paths(item):
            if name not in ("*", "_"):
                bound[name] = path
    return bound


def build(root):
    source_path = root / SOURCE
    if not source_path.is_file():
        raise SplitError(f"{SOURCE} is missing")
    text = source_path.read_text(encoding="utf-8")
    parsed = parse(text)
    pieces, (functions, methods, tests) = plan(parsed)

    extra = {}
    old_texts = {}
    for name, (path, op) in OLD_TEST_FILES.items():
        file = root / path
        if not file.is_file():
            raise SplitError(f"{path} is missing")
        old_texts[name] = file.read_text(encoding="utf-8")
        extra[op] = extra.get(op, "") + "\n" + code_only(old_texts[name])
    texts = per_file_code(parsed, pieces, extra)
    problems = reference_check(pieces, texts)
    if problems:
        raise SplitError("cross-file references:\n  " + "\n  ".join(problems))

    header = licence(parsed.preamble) + "\n"
    files = {}
    parent_bound = bound_names(parsed.imports)
    support, notes = render_test_support(parsed, pieces, header, parent_bound)
    files[f"{OUT_DIR}/test_support.rs"] = support
    for op in OPS:
        mods = [name for name, (_, owner) in OLD_TEST_FILES.items() if owner == op]
        files[f"{OUT_DIR}/{op}.rs"] = render_operation(op, pieces, header, mods)
    for name, (path, op) in OLD_TEST_FILES.items():
        files[f"{OUT_DIR}/{op}/tests/{name}.rs"] = old_texts[name]
    files[SOURCE] = render_parent(parsed, pieces, header)
    return parsed, pieces, files, notes, (functions, methods, tests)


def check_footprint(files):
    for path, text in files.items():
        if path == SOURCE:
            continue
        for number, line in enumerate(text.split("\n"), 1):
            if S3S_PATH.search(line):
                raise SplitError(f"{path}:{number} still names s3s directly: {line.strip()}")
    return None


FUNNEL_NAMES = {"dto", "xml", "S3", "Body", "S3Request", "S3ErrorCode", "S3Response", "S3Error", "S3Result"}


def check_funnel_names(parsed_sources, files):
    seen = set()
    original = "\n".join(parsed_sources)
    for match in S3S_PATH.finditer(original):
        tail = re.match(r"[A-Za-z_][A-Za-z0-9_]*", original[match.end() :])
        if tail:
            seen.add(tail.group(0))
    unknown = sorted(seen - FUNNEL_NAMES)
    if unknown:
        raise SplitError(f"s3s paths with no funnel re-export: {', '.join(unknown)}")


def edit_storage_api(text):
    if "pub(crate) use s3s::dto;" in text:
        raise SplitError(f"{STORAGE_API} already re-exports s3s::dto")
    anchor = "    #[cfg(test)]\n    pub(crate) use s3s::dto::{GetObjectInput, HeadObjectInput, ListObjectsV2Input, ListObjectsV2Output};\n"
    if text.count(anchor) != 1:
        raise SplitError(f"{STORAGE_API}: the s3 module's dto re-export anchor was not found exactly once")
    text = text.replace(anchor, "    pub(crate) use s3s::dto;\n" + anchor)
    anchor = "    #[cfg(test)]\n    pub(crate) use s3s::xml::{Serialize as XmlSerialize, Serializer as XmlSerializer};\n"
    if text.count(anchor) != 1:
        raise SplitError(f"{STORAGE_API}: the s3 module's xml re-export anchor was not found exactly once")
    return text.replace(anchor, "    #[cfg(test)]\n    pub(crate) use s3s::xml;\n" + anchor)


def edit_nextest(text, tests):
    """Point the filter that pins one test at the module the test now lives in."""
    replaced = 0
    for name, (target, _) in tests.items():
        needle = f"app::multipart_usecase::tests::{name}"
        if needle in text:
            moved = "app::multipart_usecase::tests" if target == PARENT else f"app::multipart_usecase::{target}::tests"
            replaced += text.count(needle)
            text = text.replace(needle, f"{moved}::{name}")
    if replaced == 0:
        raise SplitError(f"{NEXTEST} names no multipart_usecase test: has the filter moved?")
    return text


def run(root, fmt, report):
    parsed, pieces, files, notes, (functions, methods, tests) = build(root)
    check_funnel_names([i.text for i in parsed.functions + parsed.impl_methods + parsed.test_items], files)
    check_footprint(files)
    if report:
        print_report(parsed, pieces, files, notes, (functions, methods, tests))
        return 0

    storage = edit_storage_api((root / STORAGE_API).read_text(encoding="utf-8"))
    nextest = edit_nextest((root / NEXTEST).read_text(encoding="utf-8"), tests)

    for path, text in files.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    for name, (path, op) in OLD_TEST_FILES.items():
        (root / path).unlink()
    old_dir = root / OUT_DIR / "tests"
    if old_dir.is_dir() and not any(old_dir.iterdir()):
        old_dir.rmdir()
    (root / STORAGE_API).write_text(storage, encoding="utf-8")
    (root / NEXTEST).write_text(nextest, encoding="utf-8")

    if fmt:
        written = [str(root / p) for p in files if p != SOURCE] + [str(root / SOURCE), str(root / STORAGE_API)]
        result = subprocess.run(["rustfmt", "--edition", "2024", *written], check=False, cwd=root)
        if result.returncode != 0:
            raise SplitError("rustfmt failed on the written files")
    summary(files, pieces)
    return 0


def print_report(parsed, pieces, files, notes, assigned):
    for target in (PARENT, SUPPORT, *OPS):
        counts = {k: len(pieces[target][k]) for k in ("functions", "methods", "tests")}
        print(f"{target}: {counts}")
    for name, parent_path, test_path in notes:
        print(f"note: test import {name} is already bound in the parent ({parent_path}); test path {test_path}")
    for path, text in sorted(files.items()):
        print(f"{path}: {len(text.splitlines())} lines")


def summary(files, pieces):
    for path, text in sorted(files.items()):
        print(f"wrote {path}: {len(text.splitlines())} lines")
    for target in (PARENT, SUPPORT, *OPS):
        fns = len(pieces[target]["functions"]) + len(pieces[target]["methods"])
        tests = sum(1 for i in pieces[target]["tests"] if re.search(r"#\[(tokio::)?test", i.text))
        print(f"{target}: {fns} production fns, {tests} tests")


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--no-fmt", action="store_true", help="do not run rustfmt on the written files")
    parser.add_argument("--report", action="store_true", help="print the assignment and write nothing")
    parser.add_argument("--self-test", action="store_true", help="run the script's own tests")
    args = parser.parse_args(argv)
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    try:
        return run(ROOT, not args.no_fmt, args.report)
    except SplitError as error:
        print(f"split_multipart_usecase: {error}", file=sys.stderr)
        return 1


class SelfTest(unittest.TestCase):
    SAMPLE = "\n".join(
        [
            "// licence",
            "",
            "//! doc",
            "",
            "use a::b;",
            "",
            "/// doc",
            "fn one(",
            "    x: u8,",
            ") -> u8 {",
            '    let s = "}";',
            "    x",
            "}",
            "",
            "#[cfg(test)]",
            "mod tests {",
            "    use super::*;",
            "",
            "    #[test]",
            "    fn t() {}",
            "}",
        ]
    )

    def test_items_split_on_balance_not_on_braces_in_strings(self):
        lines = self.SAMPLE.split("\n")[6:]
        items = split_items(lines, code_only("\n".join(lines)).split("\n"), "sample")
        self.assertEqual([i.name() for i in items], ["one", "tests"])
        self.assertTrue(items[1].cfg_test)

    def test_use_tree_flattening(self):
        paths = flatten_use("a::{b, c::{d as e, f}, g::*}")
        self.assertEqual(paths, [("a::b", "b"), ("a::c::d", "e"), ("a::c::f", "f"), ("a::g::*", "*")])

    def test_s3s_rewrite_skips_strings_and_identifiers(self):
        text = 'let a = s3s::dto::X; let b = "s3s::dto"; let c = rs3s::y;'
        self.assertEqual(rewrite_s3s(text), f'let a = {FUNNEL}dto::X; let b = "s3s::dto"; let c = rs3s::y;')

    def test_unassigned_item_fails(self):
        lines = self.SAMPLE.split("\n")[6:13]
        items = split_items(lines, code_only("\n".join(lines)).split("\n"), "sample")
        with self.assertRaises(SplitError):
            assign(items, {}, "functions")
        with self.assertRaises(SplitError):
            assign(items, {"one": PARENT, "ghost": PARENT}, "functions")

    def test_unclosed_item_fails(self):
        with self.assertRaises(SplitError):
            split_items(["fn a() {"], ["fn a() {"], "sample")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
