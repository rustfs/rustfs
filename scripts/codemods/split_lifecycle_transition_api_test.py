#!/usr/bin/env python3
"""Split rustfs/src/app/lifecycle_transition_api_test.rs into one file per operation owner.

Preparatory refactor P4 for the s3s-to-gateway migration. Nine batches each
migrate one family of S3 operations and change that family's use-case
signatures. While every lifecycle integration test lives in one 3,700-line
file, two batches edit the same file. This codemod moves every top-level item
of that file, verbatim with its attributes and comments, into one of the
module files below, chosen by the explicit ASSIGNMENT table:

  lifecycle_transition_api_test.rs      module root: license header, the
                                        original `use` block, module list
  lifecycle_transition_api_test/
    common.rs        shared fixtures (no owner; frozen during batches)
    transition.rs    ILM transition-engine tests that call no S3 use-case
    lifecycle_config.rs  PutBucketLifecycleConfiguration (batch A3)
    object_lock.rs   PutObjectRetention / PutObjectLegalHold (batch B1)
    put.rs           PutObject (batch D)
    copy.rs          CopyObject, including tests that mix put and copy (batch E)
    delete.rs        DeleteObject / DeleteObjects (batch E)
    restore.rs       RestoreObject (batch E)
    multipart.rs     CompleteMultipartUpload (batch E)

Items are moved verbatim. The only rewrites, applied to moved items and
nowhere else, are the ones a move out of the parent module requires:

  * `super::` becomes `super::super::` (the items now live one module deeper);
  * `s3s::S3ErrorCode` becomes `S3ErrorCode`, imported once in the module root
    from `crate::app::storage_api::s3`, and a function-local `use s3s::S3;`
    becomes `use crate::app::storage_api::s3::S3;`, so no new file names s3s
    (scripts/check_s3s_footprint.sh counts files);
  * items placed in common.rs that a sibling file uses gain `pub(super)`;
  * a call to a test seam's use-case entry point (`execute_get_object`,
    `execute_head_object`, `execute_list_objects_v2`, `execute_put_object`) in
    a file that does not own that operation becomes a call to the seam in
    `crate::app::test_seams` with the same arguments (scripts/check_test_seams.py).

Every non-blank line of the input lands in exactly one output file. The run
fails (exit 1) and writes nothing when an item is missing from ASSIGNMENT, an
ASSIGNMENT entry names no item, an item would be moved twice, a moved file
still names s3s, or a cross-owner seam call cannot be rewritten.

Usage:
  scripts/codemods/split_lifecycle_transition_api_test.py [--no-fmt]
  scripts/codemods/split_lifecycle_transition_api_test.py --self-test

The input must be the unsplit file. A second run over the split tree finds the
module root instead and exits 1 without writing; to regenerate, restore the
unsplit file (for example `git show <rev>:rustfs/src/app/lifecycle_transition_api_test.rs`),
re-run, and the output is byte-identical to the committed split. Unless
--no-fmt is given, `rustfmt --edition 2024` formats the written files.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

SOURCE = Path("rustfs/src/app/lifecycle_transition_api_test.rs")
TARGET_DIR = Path("rustfs/src/app/lifecycle_transition_api_test")

# Module files in the order the root declares them, with a one-line owner note.
MODULES = {
    "common": "Shared fixtures for the lifecycle integration tests; owned by no operation batch.",
    "copy": "CopyObject tests, including tests that also PUT (batch E owns CopyObject).",
    "delete": "DeleteObject and DeleteObjects tests (batch E owns both).",
    "lifecycle_config": "PutBucketLifecycleConfiguration tests (batch A3 owns the lifecycle operations).",
    "multipart": "CompleteMultipartUpload tests (batch E owns CompleteMultipartUpload).",
    "object_lock": "PutObjectRetention and PutObjectLegalHold tests (batch B1 owns both).",
    "put": "PutObject tests (batch D owns PutObject).",
    "restore": "RestoreObject tests (batch E owns RestoreObject).",
    "transition": "ILM transition-engine tests that call no S3 use-case; owned by no operation batch.",
}

# Every top-level item that is not a `use` declaration, by name.
ASSIGNMENT = {
    # Fixtures. A fixture used by one owner's tests moves with them; common.rs keeps the ones several owners share.
    "GLOBAL_ENV": "common",
    "TRANSITION_WAIT_TIMEOUT": "common",
    "RESTORE_SUSPENDED_WAIT_TIMEOUT": "restore",
    "ENV_GET_CODEC_STREAMING_ENABLE": "transition",
    "ENV_GET_CODEC_STREAMING_ROLLOUT": "transition",
    "ENV_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED": "transition",
    "ENV_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED": "transition",
    "ENV_GET_CODEC_STREAMING_MIN_SIZE": "transition",
    "RUSTFS_EXPECTED_CURRENT_VERSION_ID": "common",
    "init_tracing": "common",
    "setup_test_env": "common",
    "create_test_bucket": "common",
    "suspend_test_bucket": "restore",
    "upload_test_object": "common",
    "transition_uploaded_object_directly": "restore",
    "set_bucket_lifecycle_transition_with_tier": "common",
    "set_bucket_object_lock_enabled": "common",
    "expiration_lifecycle_configuration": "lifecycle_config",
    "expired_object_delete_marker_lifecycle_configuration": "lifecycle_config",
    "del_marker_expiration_lifecycle_configuration": "lifecycle_config",
    "register_mock_tier": "common",
    "wait_for_transition": "common",
    "wait_for_restore_completion": "restore",
    "with_forced_immediate_enqueue_timeout": "common",
    "with_get_codec_streaming_remote_probe_env": "transition",
    "wait_for_object_absence": "delete",
    "wait_for_delete_marker": "common",
    "build_request": "common",
    "streaming_blob_from_bytes": "common",
    "read_object_bytes": "common",
    "live_object_version_count": "common",
    "install_malformed_versioning_config": "delete",
    "install_malformed_replication_config": "delete",
    "install_delete_replication_config": "common",
    # ILM transition engine; no S3 use-case entry point.
    "transition_wait_timeout_reports_metadata_state_and_read_failure": "transition",
    "get_transitioned_object_uses_remote_codec_fallback_path": "transition",
    "duplicate_transition_task_skips_already_transitioned_version": "transition",
    "transition_rejects_stale_etag_before_remote_upload": "transition",
    "transition_commit_rejects_replaced_source": "transition",
    "cancelled_transition_waiting_for_prepared_reader_cleans_remote": "transition",
    "lifecycle_transition_marks_dirty_disks_for_capacity_manager": "transition",
    "immediate_transition_timeout_eventually_completes_via_compensation": "transition",
    # PutBucketLifecycleConfiguration (A3).
    "compensation_driven_delete_marker_still_honors_lifecycle_cleanup": "lifecycle_config",
    "put_bucket_lifecycle_configuration_expires_existing_objects": "lifecycle_config",
    "put_bucket_lifecycle_configuration_allows_expired_delete_marker_on_object_lock_bucket": "lifecycle_config",
    "put_bucket_lifecycle_configuration_rejects_del_marker_expiration_on_object_lock_bucket": "lifecycle_config",
    "put_bucket_lifecycle_configuration_rejects_zero_day_del_marker_expiration_on_object_lock_bucket": "lifecycle_config",
    "put_bucket_lifecycle_configuration_rejects_zero_day_expiration": "lifecycle_config",
    # PutObjectRetention / PutObjectLegalHold (B1).
    "object_lock_handlers_schedule_replication": "object_lock",
    # PutObject (D).
    "put_object_if_none_match_existing_object_returns_precondition_failed": "put",
    "put_object_computes_replication_decision_exactly_once": "put",
    "put_object_extract_computes_replication_decision_per_entry": "put",
    # CopyObject (E); mixed put+copy tests go here.
    "copy_object_if_none_match_existing_destination_returns_precondition_failed": "copy",
    "undo_copy_requires_the_observed_current_version": "copy",
    "undo_copy_serializes_a_concurrent_put_until_after_commit": "copy",
    "copy_object_allows_new_version_for_locked_destination_but_blocks_explicit_overwrite": "copy",
    "concurrent_reverse_copy_object_does_not_deadlock_with_reader_locks": "copy",
    "put_and_copy_object_transition_immediately_via_usecases": "copy",
    "compensation_driven_copy_still_completes_transition": "copy",
    "copy_object_computes_replication_decision_and_strips_stale_status": "copy",
    # CompleteMultipartUpload (E).
    "complete_multipart_upload_transitions_immediately_via_usecase": "multipart",
    "compensation_driven_complete_multipart_upload_still_transitions": "multipart",
    "compensation_driven_complete_multipart_upload_still_transitions_inner": "multipart",
    # DeleteObject / DeleteObjects (E).
    "undo_delete_only_removes_the_current_matching_delete_marker": "delete",
    "delete_transitioned_object_removes_remote_tier_copy_via_usecase": "delete",
    "compensation_driven_transition_still_cleans_remote_tier_on_delete": "delete",
    "compensation_driven_versioned_delete_still_creates_delete_marker": "delete",
    "delete_objects_resolves_bucket_versioning_once_per_request": "delete",
    "delete_objects_treats_empty_directory_version_as_absent": "delete",
    "delete_objects_preserves_explicit_null_directory_version": "delete",
    "delete_object_versioning_config_failure_leaves_latest_object_intact": "delete",
    "delete_object_replication_config_failure_leaves_latest_object_intact": "delete",
    "delete_object_uses_one_replication_config_generation": "delete",
    "replica_delete_uses_authorized_source_version_and_its_tags": "delete",
    "force_delete_replication_config_failure_leaves_prefix_intact": "delete",
    "replica_delete_replication_config_failure_leaves_object_intact": "delete",
    "delete_objects_replication_config_failure_does_not_call_storage_delete": "delete",
    "nonexistent_bucket_fails_before_versioning_lookup_on_get_head_delete": "delete",
    # RestoreObject (E).
    "restore_object_usecase_reports_ongoing_conflict": "restore",
    "restore_object_usecase_recovers_cancelled_post_commit_generation": "restore",
    "restore_object_usecase_recovers_cancelled_post_commit_generation_inner": "restore",
    "restore_object_usecase_completes_suspended_null_version_in_place": "restore",
    "restore_object_usecase_completes_suspended_null_version_in_place_inner": "restore",
    "restore_object_usecase_accepts_exactly_one_of_two_concurrent_restores": "restore",
}

# Seam entry points: method name -> (seam function, module that owns the operation here).
# A call in any other module is rewritten to `test_seams::<seam>(&receiver, args...)`.
SEAMS = {
    "execute_get_object": ("get_object", None),
    "execute_head_object": ("head_object", None),
    "execute_list_objects_v2": ("list_objects_v2", None),
    "execute_put_object": ("put_object", "put"),
}

ROOT_EXTRA_USE = "use crate::app::storage_api::s3::S3ErrorCode;\n"
S3S_PATH = re.compile(r'(^|[^"A-Za-z0-9_])s3s::')
SUPER_PATH = re.compile(r"(?<![A-Za-z0-9_:])super::")
ITEM_NAME = re.compile(
    r"^(?:pub(?:\([^)]*\))?\s+)?(?:(?:async|unsafe|const|extern)\s+)*"
    r"(?P<kind>fn|static|const|struct|enum|type|trait|mod|impl|macro_rules!|use)\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)?"
)
VISIBILITY_TARGET = re.compile(r"^(?P<indent>[ \t]*)(?P<head>(?:async\s+|unsafe\s+)*(?:fn|static|const)\s)", re.M)


class SplitError(Exception):
    """The input cannot be split exactly as the tables describe: exit 1."""


# ---------------------------------------------------------------------------
# Lexing: positions of code characters, skipping comments and literals.


def code_mask(text: str) -> bytearray:
    """1 for every character that is Rust code, 0 inside comments and literals."""
    mask = bytearray(len(text))
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
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
            i = j
            continue
        raw = re.compile(r'b?r(#*)"').match(text, i)
        if raw and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            end = text.find('"' + raw.group(1), raw.end())
            if end < 0:
                raise SplitError("unterminated raw string")
            i = end + 1 + len(raw.group(1))
            continue
        if c == '"' or (c == "b" and text.startswith('b"', i) and (i == 0 or not text[i - 1].isalnum())):
            j = i + (2 if c == "b" else 1)
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            i = j + 1
            continue
        if c == "'":
            char = re.compile(r"'(?:\\(?:[nrt\\0'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\})|[^\\'\n])'").match(text, i)
            if char:
                i = char.end()
                continue
        mask[i] = 1
        i += 1
    return mask


@dataclass
class Item:
    name: str
    kind: str
    text: str  # verbatim, from its first comment/attribute line through its last line, with newline


def split_items(text: str) -> tuple[str, list[Item]]:
    """Return (license header, top-level items) and prove every non-blank line is covered once."""
    mask = code_mask(text)
    ends: list[int] = []
    depth = 0
    for i, ch in enumerate(text):
        if not mask[i]:
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth < 0:
                raise SplitError(f"unbalanced closing '{ch}' at offset {i}")
            if depth == 0 and ch == "}":
                nxt = next((k for k in range(i + 1, len(text)) if mask[k] and not text[k].isspace()), None)
                if nxt is None or text[nxt] != ";":
                    ends.append(i)
        elif ch == ";" and depth == 0:
            ends.append(i)
    if depth:
        raise SplitError("unbalanced braces at end of file")
    first_code = next(i for i in range(len(text)) if mask[i] and not text[i].isspace())
    header_end = text.rfind("\n", 0, first_code) + 1
    header = text[:header_end]
    # The header is the leading comment block; anything else before the first item belongs to that item.
    header_lines = header.splitlines(keepends=True)
    while header_lines and header_lines[-1].strip():
        header_lines.pop()
    header = "".join(header_lines)
    if not header.strip():
        raise SplitError("the file does not start with a license header")
    items: list[Item] = []
    start = len(header)
    for end in ends:
        line_end = text.find("\n", end)
        line_end = len(text) if line_end < 0 else line_end + 1
        chunk = text[start:line_end]
        stripped = chunk.lstrip("\n")
        if not stripped.strip():
            raise SplitError(f"empty item before offset {end}")
        items.append(classify(stripped))
        start = line_end
    if text[start:].strip():
        raise SplitError("text after the last item")
    covered = [line for item in items for line in item.text.splitlines() if line.strip()]
    covered = [line for line in header.splitlines() if line.strip()] + covered
    expected = [line for line in text.splitlines() if line.strip()]
    if covered != expected:
        raise SplitError("item split does not cover every non-blank input line exactly once")
    return header, items


def classify(chunk: str) -> Item:
    mask = code_mask(chunk)
    depth = 0
    for line_start, line in _lines_with_offsets(chunk):
        body = "".join(ch if mask[line_start + k] else " " for k, ch in enumerate(line)).strip()
        if not body:
            continue
        if depth == 0 and not body.startswith("#"):
            match = ITEM_NAME.match(body)
            if not match:
                raise SplitError(f"unrecognised item head: {body[:80]!r}")
            kind = match.group("kind")
            return Item(name=match.group("name") or "", kind=kind, text=chunk)
        depth += body.count("[") - body.count("]")
    raise SplitError(f"item without a head: {chunk[:80]!r}")


def _lines_with_offsets(text: str):
    offset = 0
    for line in text.splitlines(keepends=True):
        yield offset, line.rstrip("\n")
        offset += len(line)


# ---------------------------------------------------------------------------
# Rewrites applied to moved items.


def rewrite_paths(text: str) -> str:
    mask = code_mask(text)
    out = []
    last = 0
    for match in SUPER_PATH.finditer(text):
        if mask[match.start()]:
            out.append(text[last : match.start()])
            out.append("super::super::")
            last = match.end()
    out.append(text[last:])
    text = "".join(out)
    text = text.replace("use s3s::S3;", "use crate::app::storage_api::s3::S3;")
    text = re.sub(r"(?<![A-Za-z0-9_:])s3s::S3ErrorCode\b", "S3ErrorCode", text)
    return text


def make_shared(text: str) -> str:
    """Give the item head `pub(super)`; attributes and comments before it are untouched."""
    mask = code_mask(text)
    for match in VISIBILITY_TARGET.finditer(text):
        if mask[match.start("head")] and _depth_at(text, mask, match.start("head")) == 0:
            pos = match.start("head")
            return text[:pos] + "pub(super) " + text[pos:]
    raise SplitError(f"cannot find the head of shared item {text[:80]!r}")


def _depth_at(text: str, mask: bytearray, pos: int) -> int:
    depth = 0
    for i in range(pos):
        if mask[i]:
            depth += text[i] in "([{"
            depth -= text[i] in ")]}"
    return depth


def rewrite_seam_calls(text: str, module: str) -> tuple[str, int]:
    """Rewrite `<receiver>.execute_<op>(args)` to `test_seams::<seam>(&<receiver>, args)` outside the owner."""
    count = 0
    for method, (seam, owner) in SEAMS.items():
        if owner == module:
            continue
        while True:
            mask = code_mask(text)
            match = next((m for m in re.finditer(r"\.\s*" + method + r"\(", text) if mask[m.start()]), None)
            if match is None:
                break
            recv_start = _receiver_start(text, match.start())
            receiver = text[recv_start : match.start()].rstrip()
            if not receiver:
                raise SplitError(f"{module}: call to {method} without a receiver")
            args_start = match.end()
            args_end = _matching(text, mask, match.end() - 1)
            args = text[args_start:args_end]
            text = text[:recv_start] + f"test_seams::{seam}(&{receiver}, {args})" + text[args_end + 1 :]
            count += 1
    return text, count


def _receiver_start(text: str, dot: int) -> int:
    i = dot
    while i > 0 and text[i - 1].isspace():
        i -= 1
    end = i
    if end and text[end - 1] == ")":
        depth = 0
        while i > 0:
            i -= 1
            depth += text[i] == ")"
            depth -= text[i] == "("
            if depth == 0:
                break
    while i > 0 and (text[i - 1].isalnum() or text[i - 1] in "_:"):
        i -= 1
    if i == end or (i > 0 and text[i - 1] == "."):
        raise SplitError(f"unsupported receiver before offset {dot}: only a name or a path call is rewritten")
    return i


def _matching(text: str, mask: bytearray, open_pos: int) -> int:
    depth = 0
    for i in range(open_pos, len(text)):
        if not mask[i]:
            continue
        depth += text[i] in "([{"
        depth -= text[i] in ")]}"
        if depth == 0:
            return i
    raise SplitError(f"unbalanced call at offset {open_pos}")


# ---------------------------------------------------------------------------
# Assembly.


def build(text: str) -> dict[Path, str]:
    """Return the full content of every output file, keyed by repository-relative path."""
    header, items = split_items(text)
    license_block = header.rstrip("\n") + "\n"
    uses = [item for item in items if item.kind == "use"]
    moved = [item for item in items if item.kind != "use"]
    if items[: len(uses)] != uses:
        raise SplitError("a `use` declaration follows another item; the root keeps only the leading use block")
    names = [item.name for item in moved]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise SplitError(f"items defined twice: {', '.join(duplicates)}")
    unassigned = [name for name in names if name not in ASSIGNMENT]
    stale = sorted(set(ASSIGNMENT) - set(names))
    if unassigned or stale:
        raise SplitError(
            "ASSIGNMENT does not match the input:"
            + "".join(f"\n  unassigned item: {name}" for name in unassigned)
            + "".join(f"\n  no such item: {name}" for name in stale)
        )
    unknown = sorted({module for module in ASSIGNMENT.values() if module not in MODULES})
    if unknown:
        raise SplitError(f"ASSIGNMENT names undeclared modules: {', '.join(unknown)}")

    bodies: dict[str, list[str]] = {module: [] for module in MODULES}
    for item in moved:
        module = ASSIGNMENT[item.name]
        body, _ = rewrite_seam_calls(rewrite_paths(item.text), module)
        bodies[module].append(body)

    # A common item gains `pub(super)` only when a sibling uses it; the rest stay private to common.rs.
    sibling_code = _code_only("\n".join(body for module, items in bodies.items() if module != "common" for body in items))
    common_items = [item for item in moved if ASSIGNMENT[item.name] == "common"]
    common_names = {item.name for item in common_items if re.search(rf"\b{re.escape(item.name)}\b", sibling_code)}
    bodies["common"] = [
        make_shared(body) if item.name in common_names else body for item, body in zip(common_items, bodies["common"])
    ]
    outputs: dict[Path, str] = {}
    root_uses = "".join(item.text if item.text.endswith("\n") else item.text + "\n" for item in uses)
    root = (
        license_block
        + "\n"
        + "//! Lifecycle, transition and object-version integration tests, split by the batch that owns each operation.\n"
        + "//!\n"
        + "//! Generated by scripts/codemods/split_lifecycle_transition_api_test.py; each module file names its owner.\n"
        + "\n"
        + root_uses
        + ROOT_EXTRA_USE
        + "\n"
        + "".join(f"mod {module};\n" for module in MODULES)
    )
    outputs[SOURCE] = root
    for module, doc in MODULES.items():
        content = bodies[module]
        if not content:
            raise SplitError(f"module {module} receives no item")
        joined = "\n".join(content)
        preamble = ["use super::*;\n"]
        if module != "common" and any(re.search(rf"\b{re.escape(name)}\b", joined) for name in common_names):
            preamble.append("use super::common::*;\n")
        if "test_seams::" in joined:
            preamble.append("use crate::app::test_seams;\n")
        text_out = license_block + "\n" + f"//! {doc}\n" + "\n" + "".join(preamble) + "\n" + joined
        leftover = [line for line in text_out.splitlines() if S3S_PATH.search(line)]
        if leftover:
            raise SplitError(f"{module}.rs still names s3s: {leftover[0].strip()}")
        for method, (_, owner) in SEAMS.items():
            if owner != module and re.search(r"\." + method + r"\(", _code_only(text_out)):
                raise SplitError(f"{module}.rs still calls {method} directly")
        outputs[TARGET_DIR / f"{module}.rs"] = text_out
    return outputs


def _code_only(text: str) -> str:
    mask = code_mask(text)
    return "".join(ch if mask[i] or ch == "\n" else " " for i, ch in enumerate(text))


def run(root: Path, fmt: bool) -> int:
    source = root / SOURCE
    try:
        text = source.read_text()
    except OSError as error:
        print(f"error: cannot read {SOURCE}: {error}", file=sys.stderr)
        return 1
    if re.search(r"(?m)^mod common;$", text):
        print(f"error: {SOURCE} is already split; restore the unsplit file first", file=sys.stderr)
        return 1
    try:
        outputs = build(text)
    except SplitError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    stray = sorted(p.name for p in (root / TARGET_DIR).glob("*.rs") if TARGET_DIR / p.name not in outputs) if (
        root / TARGET_DIR
    ).is_dir() else []
    if stray:
        print(f"error: {TARGET_DIR} holds files this codemod does not write: {', '.join(stray)}", file=sys.stderr)
        return 1
    (root / TARGET_DIR).mkdir(parents=True, exist_ok=True)
    for path, content in outputs.items():
        (root / path).write_text(content)
    if fmt:
        paths = [str(root / path) for path in outputs]
        result = subprocess.run(["rustfmt", "--edition", "2024", *paths], cwd=root, check=False)
        if result.returncode != 0:
            print("error: rustfmt failed on the written files", file=sys.stderr)
            return 1
    for path in outputs:
        print(f"wrote {path} ({len((root / path).read_text().splitlines())} lines)")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    if argv not in ([], ["--no-fmt"]):
        print(__doc__.split("Usage:", 1)[1].split("\n\n", 1)[0], file=sys.stderr)
        return 2
    top = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False)
    if top.returncode != 0:
        print("error: not inside a git checkout", file=sys.stderr)
        return 2
    return run(Path(top.stdout.strip()), fmt=argv != ["--no-fmt"])


# ---------------------------------------------------------------------------
# Self-test


class SelfTest(unittest.TestCase):
    HEADER = "// Copyright\n// License\n\n"

    def items(self, body: str) -> list[Item]:
        return split_items(self.HEADER + body)[1]

    def test_items_keep_attributes_comments_and_braces_in_literals(self):
        body = 'use a::{b, c};\n\n// note\n#[test]\nfn one() {\n    let s = "}";\n    let c = \'}\';\n}\n\nstatic X: u8 = 1;\n'
        items = self.items(body)
        self.assertEqual([(i.kind, i.name) for i in items], [("use", "a"), ("fn", "one"), ("static", "X")])
        self.assertTrue(items[1].text.startswith("// note\n#[test]\n"))

    def test_raw_strings_and_lifetimes_do_not_confuse_depth(self):
        items = self.items("fn a<'x>(v: &'x str) {\n    let r = r#\"{\"#;\n}\nfn b() {}\n")
        self.assertEqual([i.name for i in items], ["a", "b"])

    def test_unbalanced_input_is_rejected(self):
        with self.assertRaises(SplitError):
            split_items(self.HEADER + "fn a() {\n")

    def test_unassigned_item_is_rejected(self):
        with self.assertRaisesRegex(SplitError, "unassigned item: not_in_table"):
            build(self.HEADER + "use x::y;\nfn not_in_table() {}\n")

    def test_super_paths_move_one_level_down_but_not_in_strings(self):
        source = 'fn a() {\n    use super::x::Y;\n    let s = "super::z";\n}\n'
        self.assertEqual(rewrite_paths(source), 'fn a() {\n    use super::super::x::Y;\n    let s = "super::z";\n}\n')

    def test_s3s_error_code_and_trait_paths_are_rewritten(self):
        source = "fn a() {\n    use s3s::S3;\n    f(&s3s::S3ErrorCode::NoSuchKey);\n}\n"
        self.assertEqual(
            rewrite_paths(source),
            "fn a() {\n    use crate::app::storage_api::s3::S3;\n    f(&S3ErrorCode::NoSuchKey);\n}\n",
        )

    def test_shared_items_gain_pub_super_after_attributes(self):
        self.assertEqual(make_shared("#[allow(x)]\nasync fn a() {}\n"), "#[allow(x)]\npub(super) async fn a() {}\n")
        self.assertEqual(make_shared("static A: u8 = 1;\n"), "pub(super) static A: u8 = 1;\n")

    def test_cross_owner_calls_use_the_seam(self):
        source = "fn t() {\n    Box::pin(usecase\n        .execute_put_object(&fs, req(x)))\n}\n"
        rewritten, count = rewrite_seam_calls(source, "copy")
        self.assertEqual(count, 1)
        self.assertEqual(rewritten, "fn t() {\n    Box::pin(test_seams::put_object(&usecase, &fs, req(x)))\n}\n")

    def test_owner_calls_stay_direct(self):
        source = "fn t() {\n    usecase.execute_put_object(&fs, req)\n}\n"
        self.assertEqual(rewrite_seam_calls(source, "put"), (source, 0))

    def test_call_expression_receivers_are_kept_whole(self):
        source = "fn t() {\n    Usecase::with(Some(a)).execute_get_object(r)\n}\n"
        rewritten, _ = rewrite_seam_calls(source, "delete")
        self.assertEqual(rewritten, "fn t() {\n    test_seams::get_object(&Usecase::with(Some(a)), r)\n}\n")

    def test_already_split_input_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / SOURCE
            path.parent.mkdir(parents=True)
            path.write_text(self.HEADER + "mod common;\n")
            self.assertEqual(run(Path(tmp), fmt=False), 1)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
