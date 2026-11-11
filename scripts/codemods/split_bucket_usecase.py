#!/usr/bin/env python3
"""Split rustfs/src/app/bucket_usecase.rs into one file per configuration family.

Preparatory refactor P2 of the gateway migration (rustfs/backlog#2765). The
single 6,000-line use-case file is edited by every bucket-configuration batch;
this codemod moves each family into its own file under
rustfs/src/app/bucket_usecase/ so that later batches edit disjoint files. It
is a pure move: no signature, body or test is rewritten.

What it does, in order:

  * parses the unsplit file into its top-level items, the methods of the one
    `impl DefaultBucketUsecase` block, and the items of the `tests` module;
  * assigns every one of them to a target file through the explicit tables
    TOP_LEVEL, METHODS and TESTS below. An item without an assignment, an
    assignment naming no item, or a name that occurs twice is an error: the
    run stops with exit 1 and writes nothing, so nothing is dropped or left
    behind silently;
  * moves each item verbatim with its attributes, doc comments and comments
    directly above it. Methods stay in `impl DefaultBucketUsecase` blocks of
    the same indentation, tests in a `mod tests` of their file;
  * writes `mod` declarations into bucket_usecase.rs, a `use super::*;` and the
    minimal sibling imports into each new file, and bumps a private item or
    method to `pub(super)` only when another file refers to it;
  * keeps s3s out of every new file: the new files reach it through
    `use super::*;`, and the s3s imports the moved tests need sit in the
    parent as one `#[cfg(test)]` import, so the footprint count does not move;
  * formats the written files with rustfmt (skip with --no-fmt).

Usage:
  scripts/codemods/split_bucket_usecase.py [--root REPO] [--no-fmt] [--dump]
  scripts/codemods/split_bucket_usecase.py --self-test

The run starts from the unsplit file and refuses to run on a split tree
(restore the unsplit file first: git checkout <base> -- rustfs/src/app/bucket_usecase.rs
and remove rustfs/src/app/bucket_usecase/). Re-running it on the restored file
reproduces the committed tree byte for byte.

--dump prints the item inventory (kind, name, line range, assignment) and
writes nothing.

Exit status: 0 on success; 1 when the tables and the file disagree; 2 on a
usage error or an input the parser cannot read.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import unittest
from unittest import mock
from dataclasses import dataclass, field
from pathlib import Path

SOURCE = "rustfs/src/app/bucket_usecase.rs"
SPLIT_DIR = "rustfs/src/app/bucket_usecase"

CODE, COMMENT, LITERAL = 0, 1, 2
IDENT_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
CHAR_LITERAL = re.compile(r"'(?:\\(?:[nrt\\0'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\})|[^\\'\n])'")
RAW_STRING_START = re.compile(r'r(#*)"')
IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
FORMAT_CAPTURE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)(?::[^{}]*)?\}")


class SplitError(Exception):
    """The tables and the file disagree: exit 1."""


class UsageError(Exception):
    """A broken argument or an input the parser cannot read: exit 2."""


# ---------------------------------------------------------------------------
# Lexing and item splitting
# ---------------------------------------------------------------------------


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
        elif ch == "r" and (i == 0 or text[i - 1] not in IDENT_CHARS or (text[i - 1] == "b" and (i < 2 or text[i - 2] not in IDENT_CHARS))):
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


def mask(text: str, keep_literals: bool = False) -> str:
    """The text with comments (and, unless kept, literals) blanked, newlines preserved."""
    kinds = classify(text)
    out = []
    for ch, kind in zip(text, kinds):
        if kind == COMMENT or (kind == LITERAL and not keep_literals):
            out.append("\n" if ch == "\n" else " ")
        else:
            out.append(ch)
    return "".join(out)


@dataclass
class Item:
    """One item (or method, or test) cut out of the source."""

    kind: str  # fn use struct enum impl const static type mod trait macro
    name: str  # the assignment key
    start: int  # offset of its first attached comment/attribute line
    end: int  # offset just past its last character
    text: str = ""
    line: int = 0  # 1-based line of `start`
    vis: str = ""  # "", "pub", "pub(crate)", ...
    cfg_test: bool = False


HEADER = re.compile(
    r"""\A\s*
    (?:\#\[[^\]]*(?:\[[^\]]*\][^\]]*)*\]\s*)*          # attributes, one nesting level of []
    (?P<vis>pub(?:\s*\([^)]*\))?\s+)?
    (?P<quals>(?:(?:async|unsafe|extern(?:\s+"[^"]*")?|default)\s+)*)
    (?:(?P<const_fn>const)\s+(?=(?:async|unsafe|extern|fn)\b))?
    (?P<kw>fn|struct|enum|impl|const|static|type|use|mod|trait|macro_rules!|union)\b
    """,
    re.X,
)


def split_items(text: str, start: int, end: int) -> list[Item]:
    """Cut text[start:end] into the items at its own nesting level.

    Everything between two items (comments, attributes, doc comments) belongs
    to the item that follows; blank lines separate. Text after the last item
    that is not blank is an error, so no comment can be dropped.
    """
    masked = mask(text)
    items: list[Item] = []
    i = start
    while i < end:
        while i < end and text[i] in " \t\r\n":
            i += 1
        if i >= end:
            break
        where = text.count("\n", 0, i) + 1
        item_start = max(text.rfind("\n", 0, i) + 1, items[-1].end if items else start)
        match = HEADER.match(masked[i:end])
        if match is None:
            # Also the failure for a comment or attribute with no item after it.
            raise SplitError(f"cannot read an item header at line {where}: {text[i:i + 60]!r}")
        kw = match.group("kw")
        only_semicolon = kw in ("const", "static", "type", "use") and not match.group("const_fn")
        depth, j, closed = 0, i, False
        while j < end and not closed:
            ch = masked[j]
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth -= 1
                closed = depth == 0 and ch == "}" and not only_semicolon
            elif ch == ";" and depth == 0:
                closed = True
            j += 1
        if not closed:
            raise SplitError(f"item at line {where} is not terminated")
        item = Item(kind=kw, name=item_name(masked[i:j], match), start=item_start, end=j)
        item.text = text[item_start:j]
        item.line = text.count("\n", 0, item_start) + 1
        item.vis = (match.group("vis") or "").strip()
        item.cfg_test = bool(re.search(r"#\[cfg\(test\)\]", masked[i : i + match.end()]))
        items.append(item)
        i = j
    return items


def item_name(masked_item: str, match: re.Match[str]) -> str:
    """The assignment key of an item: its identifier, or its normalised header for an impl."""
    kw = match.group("kw")
    rest = masked_item[match.end():]
    if kw == "impl":
        header = rest.split("{", 1)[0]
        return "impl " + " ".join(header.split())
    if kw == "use":
        return "use " + " ".join(rest.split(";", 1)[0].split())
    ident = IDENT.search(rest)
    if ident is None:
        raise SplitError(f"no identifier after `{kw}` in {masked_item[:60]!r}")
    return ident.group(0)


@dataclass
class Parsed:
    prelude: str  # license header and `//!` docs, verbatim
    top: list[Item]  # every top-level item except the impl and the tests module
    impl: Item  # `impl DefaultBucketUsecase`
    methods: list[Item]  # its methods
    tests: Item  # `#[cfg(test)] mod tests`
    test_items: list[Item]  # its items: `use` lines, helpers, tests
    text: str = ""


def parse_source(text: str) -> Parsed:
    """Parse the unsplit file into prelude, top-level items, methods and test items."""
    lines = text.splitlines(keepends=True)
    prelude_end = 0
    for line in lines:
        if line.strip() == "" or line.startswith("//"):
            prelude_end += len(line)
        else:
            break
    prelude = text[:prelude_end]
    items = split_items(text, prelude_end, len(text))
    impls = [item for item in items if item.name == "impl DefaultBucketUsecase"]
    tests = [item for item in items if item.kind == "mod" and item.name == "tests"]
    if len(impls) != 1 or len(tests) != 1:
        raise UsageError("expected exactly one `impl DefaultBucketUsecase` block and one `mod tests`")
    impl, tests_mod = impls[0], tests[0]
    if not tests_mod.cfg_test:
        raise UsageError("`mod tests` is not #[cfg(test)]")
    top = [item for item in items if item is not impl and item is not tests_mod]
    methods = split_items(text, text.index("{", _header_end(text, impl)) + 1, impl.end - 1)
    test_items = split_items(text, text.index("{", _header_end(text, tests_mod)) + 1, tests_mod.end - 1)
    return Parsed(prelude, top, impl, methods, tests_mod, test_items, text)


def _header_end(text: str, item: Item) -> int:
    """Offset of the `impl`/`mod` keyword of an item, past its attributes."""
    masked = mask(text)
    match = HEADER.match(masked[item.start : item.end])
    assert match is not None
    return item.start + match.end()


def dump(parsed: Parsed) -> None:
    """Print one line per item: region, target file, kind, source line, name."""
    for tag, items, table, skip_use in (
        ("top", parsed.top, TOP_LEVEL, True),
        ("method", parsed.methods, METHODS, False),
        ("test", parsed.test_items, TESTS, True),
    ):
        owner = assign(items, table, tag, skip_use)
        for index, item in enumerate(items):
            target = owner.get(index, PARENT if tag == "top" else "(imports)")
            print(f"{tag}\t{target}\t{item.kind}\t{item.vis or '-'}\t{item.line}\t{item.name}")


# ---------------------------------------------------------------------------
# The assignment tables. Every item name appears exactly once.
# ---------------------------------------------------------------------------

PARENT = "bucket_usecase"  # the file that keeps the struct, admission and shared helpers
FLAT = ("test_support", "guards")  # files of plain test items, gated `#[cfg(test)]` in the parent
FAMILIES = (
    "cors",
    "tagging",
    "public_access_block",
    "encryption",
    "policy",
    "versioning",
    "notification",
    "lifecycle",
    "replication",
    "core",
    "listing",
)
STUBS = ("acl", "object_lock", "website", "accelerate", "logging", "request_payment")

DOCS = {
    "cors": "Bucket CORS configuration use-case operations.",
    "tagging": "Bucket tagging use-case operations.",
    "public_access_block": "Bucket public access block use-case operations.",
    "encryption": "Bucket default-encryption use-case operations.",
    "policy": "Bucket policy and policy status use-case operations.",
    "versioning": "Bucket versioning use-case operations.",
    "notification": "Bucket notification configuration use-case operations.",
    "lifecycle": "Bucket lifecycle configuration use-case operations.",
    "replication": "Bucket replication configuration use-case operations.",
    "core": "Bucket create, delete, head and location use-case operations.",
    "listing": "Object and version listing use-case operations, including the metadata listing extension.",
    "test_support": "Request fixtures shared by the bucket use-case tests.",
    "guards": "Source-text guards over the bucket use-case sources.",
    "acl": "Bucket ACL use-case operations (reserved for the next batch).",
    "object_lock": "Bucket object-lock configuration use-case operations (reserved for the next batch).",
    "website": "Bucket website configuration use-case operations (reserved for the next batch).",
    "accelerate": "Bucket accelerate configuration use-case operations (reserved for the next batch).",
    "logging": "Bucket logging configuration use-case operations (reserved for the next batch).",
    "request_payment": "Bucket request-payment configuration use-case operations (reserved for the next batch).",
}

# Top-level items other than `use` items (which all stay in the parent) and the two
# blocks handled separately (`impl DefaultBucketUsecase`, `mod tests`).
TOP_LEVEL: dict[str, list[str]] = {
    PARENT: [
        "LOG_COMPONENT_APP",
        "LOG_SUBSYSTEM_BUCKET",
        "BUCKET_OPERATION_CONCURRENCY",
        "BUCKET_OPERATION_QUEUE_CAPACITY",
        "BUCKET_OPERATION_QUEUE_TIMEOUT",
        "BUCKET_OPERATION_ADMISSION",
        "BucketOperationAdmission",
        "impl BucketOperationAdmission",
        "bucket_admission_closed",
        "bucket_admission_slow_down",
        "serialize_config",
        "update_bucket_config_for_incarnation",
        "delete_bucket_config_for_incarnation",
        "to_internal_error",
        "sr_bucket_meta_item",
        "notify_bucket_metadata_reload",
        "notify_table_bucket_metadata_reload",
        "notify_bucket_metadata_reload_with_context",
        "record_local_scanner_maintenance_reload",
        "notify_bucket_metadata_delete",
        "DefaultBucketUsecase",
        "await_bucket_usecase_on_fresh_task",
        "await_bucket_usecase_on_fresh_task_with_admission",
    ],
    "notification": [
        "is_valid_notification_filter_value",
        "invalid_filter_value_message",
        "invalid_filter_name_message",
        "validate_notification_filter_rules",
        "validate_notification_configuration_filters",
        "parse_notification_target_id",
        "NotificationEventRule",
        "build_notification_event_rules",
        "resolve_notification_region",
    ],
    "replication": [
        "validate_replication_config_targets",
        "validate_replication_config_capabilities",
        "validate_bucket_replication_update",
        "merge_user_replication_config_update",
        "split_replication_config_for_user_delete",
        "replication_targets_without_arns",
        "remove_replication_targets_from_config_targets",
        "write_replication_targets_after_config_delete",
        "restore_replication_config_after_target_cleanup_failure",
    ],
    "versioning": [
        "versioning_configuration_has_object_lock_incompatible_settings",
        "validate_bucket_versioning_update",
    ],
    "lifecycle": [
        "ERR_LIFECYCLE_RULE_STATUS",
        "assign_lifecycle_rule_ids",
        "validate_lifecycle_rule_status",
        "lifecycle_validation_error",
        "lifecycle_has_transition_rules",
        "lifecycle_has_expiry_rules",
        "lifecycle_rules_have_expiry",
        "lifecycle_has_abort_multipart_rules",
    ],
    "encryption": ["validate_bucket_encryption_configuration"],
    "core": ["create_bucket_exists_response"],
    "listing": [
        "ListObjectVersionsInfo",
        "ListObjectsV2Info",
        "XMLNS_S3",
        "ObjectInternalInfo",
        "ObjectMetadataExtension",
        "ListObjectVersionMetadataEntry",
        "ListObjectVersionsMetadataOutput",
        "ObjectMetadataEntry",
        "ListObjectsV2MetadataOutput",
        "impl xml::Serialize for ListObjectVersionsMetadataOutput",
        "impl SerializeContent for ListObjectVersionsMetadataOutput",
        "impl xml::Serialize for ListObjectsV2MetadataOutput",
        "impl SerializeContent for ListObjectsV2MetadataOutput",
        "VersionMetadataContent",
        "impl <T: SerializeContent> SerializeContent for VersionMetadataContent<'_, T>",
        "impl SerializeContent for ObjectMetadataEntry",
        "is_valid_xml_name",
        "sanitize_xml_text",
        "impl SerializeContent for ObjectMetadataExtension",
        "impl SerializeContent for ObjectInternalInfo",
        "ObjectMetadataPermissions",
        "encode_list_versions_value",
        "encode_list_objects_v2_value",
        "build_metadata_extension_user_metadata",
        "metadata_count_to_i32",
        "is_list_objects_metadata_action_allowed",
        "collect_list_objects_metadata_permissions",
        "build_list_object_versions_metadata_output",
        "build_list_objects_v2_metadata_output",
    ],
}

# The methods of `impl DefaultBucketUsecase`.
METHODS: dict[str, list[str]] = {
    PARENT: [
        "without_context",
        "from_global",
        "with_context",
        "global_region",
        "object_store",
        "execute_list_buckets",
    ],
    "core": [
        "create_bucket_exists_result",
        "execute_create_bucket",
        "execute_create_bucket_inner",
        "execute_delete_bucket",
        "execute_delete_bucket_inner",
        "execute_head_bucket",
        "execute_get_bucket_location",
    ],
    "encryption": ["execute_delete_bucket_encryption", "execute_get_bucket_encryption", "execute_put_bucket_encryption"],
    "cors": ["execute_delete_bucket_cors", "execute_get_bucket_cors", "execute_put_bucket_cors"],
    "lifecycle": [
        "execute_delete_bucket_lifecycle",
        "execute_get_bucket_lifecycle_configuration",
        "execute_put_bucket_lifecycle_configuration",
    ],
    "policy": [
        "execute_delete_bucket_policy",
        "execute_get_bucket_policy",
        "execute_get_bucket_policy_status",
        "execute_put_bucket_policy",
    ],
    "replication": [
        "execute_delete_bucket_replication",
        "execute_get_bucket_replication",
        "execute_put_bucket_replication",
    ],
    "tagging": ["execute_delete_bucket_tagging", "execute_get_bucket_tagging", "execute_put_bucket_tagging"],
    "public_access_block": [
        "execute_delete_public_access_block",
        "execute_get_public_access_block",
        "execute_put_public_access_block",
    ],
    "versioning": ["execute_get_bucket_versioning", "execute_put_bucket_versioning"],
    "notification": [
        "execute_get_bucket_notification_configuration",
        "execute_put_bucket_notification_configuration",
    ],
    "listing": [
        "execute_list_objects_v2",
        "execute_list_objects_v2_inner",
        "execute_list_objects_v2m",
        "execute_list_object_versions",
        "execute_list_object_versions_m",
        "execute_list_objects",
    ],
}

# The non-`use` items of `mod tests`: tests and the helper functions they share. A helper
# listed under a family file must be used by that file alone; a helper used by several
# files belongs in `test_support`. The tests' `use` lines are not assigned: every file gets
# the ones its own code names.
TESTS: dict[str, list[str]] = {
    PARENT: [
        "diagnostic_bucket_usecase_task_keeps_entry_and_permits_after_waiter_drop",
        "bucket_usecase_task_preserves_http_operation_context",
        "bucket_usecase_task_finishes_post_commit_hooks_after_parent_cancellation",
        "bucket_usecase_admission_releases_execution_during_slow_post_commit_work",
        "bucket_usecase_admission_bounds_post_commit_work_after_request_cancellation",
        "bucket_usecase_admission_times_out_without_starting_work",
        "bucket_usecase_admission_queues_bursts_with_bounded_execution",
        "bucket_usecase_admission_bounds_queue_and_cancels_waiters",
        "bucket_usecase_admission_maps_closed_semaphore_to_internal_error",
        "bucket_usecase_task_maps_panics_to_internal_errors",
        "scanner_maintenance_reload_marks_only_scanner_owned_config_changes",
        "build_request_with_req_info_preserves_owner_state",
        "execute_list_buckets_returns_internal_error_when_store_uninitialized",
    ],
    "test_support": ["build_request", "build_request_with_req_info", "s3_op_total"],
    "guards": [
        "bucket_usecase_sources",
        "usecase_method_source",
        "bucket_create_and_delete_use_admitted_fresh_tasks",
        "bucket_metadata_config_changes_notify_peer_metadata_reload",
    ],
    "cors": [
        "execute_delete_bucket_cors_returns_internal_error_when_store_uninitialized",
        "execute_get_bucket_cors_returns_internal_error_when_store_uninitialized",
        "execute_put_bucket_cors_returns_internal_error_when_store_uninitialized",
    ],
    "tagging": [
        "execute_get_bucket_tagging_returns_internal_error_when_store_uninitialized",
        "put_bucket_tagging_rejects_invalid_shapes_before_storage_mutation",
        "execute_put_bucket_tagging_returns_internal_error_when_store_uninitialized",
    ],
    "public_access_block": [
        "execute_delete_public_access_block_returns_internal_error_when_store_uninitialized",
        "execute_get_public_access_block_returns_internal_error_when_store_uninitialized",
        "execute_put_public_access_block_returns_internal_error_when_store_uninitialized",
    ],
    "encryption": [
        "sse_config",
        "sse_rule",
        "put_bucket_encryption_refuses_configurations_the_write_path_cannot_honour",
        "execute_get_bucket_encryption_returns_internal_error_when_store_uninitialized",
        "execute_put_bucket_encryption_returns_internal_error_when_store_uninitialized",
    ],
    "policy": [
        "execute_delete_bucket_policy_records_s3_operation_before_store_lookup",
        "execute_get_bucket_policy_returns_internal_error_when_store_uninitialized",
        "execute_put_bucket_policy_returns_internal_error_when_store_uninitialized",
    ],
    "versioning": [
        "validate_bucket_versioning_update_rejects_invalid_status_before_metadata_lookup",
        "versioning_configuration_has_object_lock_incompatible_settings_rejects_suspended",
        "versioning_configuration_has_object_lock_incompatible_settings_rejects_exclude_folders",
        "versioning_configuration_has_object_lock_incompatible_settings_rejects_excluded_prefixes",
        "execute_get_bucket_versioning_returns_internal_error_when_store_uninitialized",
    ],
    "notification": [
        "resolve_notification_region_prefers_global_region",
        "resolve_notification_region_falls_back_to_request_region",
        "resolve_notification_region_defaults_value",
        "validate_notification_configuration_filters_rejects_invalid_filter_name",
        "validate_notification_configuration_filters_accepts_case_insensitive_filter_names",
        "validate_notification_configuration_filters_rejects_duplicate_prefix_rules",
        "validate_notification_configuration_filters_rejects_invalid_filter_value",
        "validate_notification_configuration_filters_rejects_missing_filter_name",
        "validate_notification_configuration_filters_rejects_missing_filter_value",
        "validate_notification_configuration_filters_rejects_duplicate_suffix_rules",
        "execute_put_bucket_notification_configuration_rejects_invalid_filter_before_store_lookup",
    ],
    "lifecycle": [
        "normalize_lifecycle_rules_generate_rule_ids_for_missing_values",
        "put_bucket_lifecycle_validation_errors_keep_their_s3_code",
        "validate_lifecycle_rule_status_rejects_invalid_status",
        "lifecycle_has_transition_rules_ignores_disabled_rules",
        "lifecycle_has_transition_rules_accepts_enabled_noncurrent_transitions",
        "lifecycle_has_abort_multipart_rules_accepts_enabled_abort_rule",
        "execute_put_bucket_lifecycle_configuration_rejects_missing_configuration",
    ],
    "replication": [
        "replication_rule_for_target",
        "replication_target_arns_use_role_when_present",
        "replication_target_arns_use_rule_destinations_without_role",
        "replication_rule_with_id",
        "site_peers",
        "put_replication_merge_preserves_site_replication_rules",
        "put_then_delete_replication_without_site_replication_treats_site_repl_id_as_user_rule",
        "delete_replication_split_keeps_only_reconciler_derived_rules",
        "put_replication_merge_returns_incoming_verbatim_without_site_rules",
        "delete_replication_split_keeps_site_rules_and_their_targets",
        "delete_replication_split_protects_targets_shared_with_site_rules",
        "delete_replication_split_removes_everything_without_site_rules",
        "replication_targets_with_arn",
        "validate_replication_config_targets_accepts_matching_destination_arns",
        "validate_replication_config_targets_rejects_stale_destination_arns",
        "validate_replication_config_targets_accepts_matching_role_arn",
        "validate_replication_config_targets_rejects_role_with_multiple_destinations",
        "validate_replication_config_targets_trims_destination_arns",
        "validate_replication_config_targets_ignores_disabled_rules",
        "validate_replication_config_capabilities_names_unsupported_field",
        "validate_replication_config_capabilities_rejects_structural_defects_before_write",
        "validate_replication_config_capabilities_rejects_invalid_status_before_write",
        "remove_replication_targets_from_config_targets_only_removes_referenced_replication_targets",
        "execute_delete_bucket_replication_returns_internal_error_when_store_uninitialized",
        "execute_get_bucket_replication_returns_internal_error_when_store_uninitialized",
        "execute_put_bucket_replication_returns_internal_error_when_store_uninitialized",
        "execute_put_bucket_replication_rejects_unsupported_fields_before_store_or_metadata_write",
    ],
    "core": [
        "create_bucket_exists_response_returns_ok_in_default_region",
        "create_bucket_exists_response_returns_owned_by_you_outside_default_region",
        "execute_create_bucket_returns_internal_error_when_store_uninitialized",
        "execute_create_bucket_on_existing_bucket_does_not_wait_for_bucket_lifecycle_lock",
        "execute_create_bucket_creates_missing_bucket",
        "execute_delete_bucket_returns_internal_error_when_store_uninitialized",
        "execute_head_bucket_returns_internal_error_when_store_uninitialized",
        "execute_get_bucket_location_returns_internal_error_when_store_uninitialized",
    ],
    "listing": [
        "serialize_s3s_response",
        "execute_list_object_versions_returns_internal_error_when_store_uninitialized",
        "execute_list_object_versions_m_returns_internal_error_when_store_uninitialized",
        "keyless_object_and_version_listings_do_not_expose_legacy_encrypted_fingerprints",
        "metadata_listings_hide_ssec_user_metadata_even_with_read_permission",
        "build_list_object_versions_metadata_output_maps_metadata_and_preserves_entry_order",
        "build_list_object_versions_metadata_output_uses_params_and_hides_metadata_without_permissions",
        "execute_list_objects_returns_internal_error_when_store_uninitialized",
        "execute_list_objects_v2m_returns_internal_error_when_store_uninitialized",
        "build_list_objects_v2_metadata_output_maps_metadata_and_key_count",
        "list_objects_v2_metadata_output_serializes_minio_extension_xml",
        "is_valid_xml_name_rejects_non_name_keys",
        "sanitize_xml_text_strips_illegal_control_chars",
        "list_objects_v2_metadata_output_survives_poison_metadata_key",
        "build_list_objects_v2_metadata_output_uses_params_and_hides_owner_without_fetch_owner",
        "execute_list_objects_v2_rejects_negative_max_keys",
        "execute_list_objects_v2_rejects_invalid_continuation_token_before_store_lookup",
        "execute_list_objects_v2m_rejects_negative_max_keys",
        "execute_list_objects_v2m_rejects_invalid_continuation_token_before_store_lookup",
        "execute_list_object_versions_rejects_negative_max_keys_before_store_lookup",
        "execute_list_object_versions_m_rejects_negative_max_keys_before_store_lookup",
    ],
}


# Imports that bring a trait in for its methods only. rustc credits an import as used when a
# name is resolved through it, also through a child's `use super::*;`, but it does not credit
# a trait import for method calls made behind a glob: such an import would warn as unused
# once every caller has moved out of the parent. So each of them moves, with its callers,
# into the files that call its methods (or stays where the parent still calls them). A
# trait import of that kind that this table does not list stops the run.
TRAIT_IMPORTS: dict[str, tuple[str, ...]] = {
    "super::storage_api::bucket_usecase::bucket::ObjectLockConfigExt": ("versioning",),
    "super::storage_api::bucket_usecase::bucket::VersioningConfigExt": ("versioning",),
    "super::storage_api::bucket_usecase::contract::list::ListOperations": ("listing",),
    "super::storage_api::bucket_usecase::contract::bucket::BucketOperations": (PARENT,),  # execute_list_buckets
    "rustfs_policy::policy::Validator": ("policy",),
    "tracing::Instrument": (PARENT,),  # await_bucket_usecase_on_fresh_task_with_admission
}


def invert(table: dict[str, list[str]], what: str) -> dict[str, str]:
    """name -> file for a table; a name listed twice is an error."""
    owner: dict[str, str] = {}
    for file, names in table.items():
        for name in names:
            if name in owner:
                raise SplitError(f"{what} table lists `{name}` twice ({owner[name]} and {file})")
            owner[name] = file
    return owner


def assign(items: list[Item], table: dict[str, list[str]], what: str, skip_use: bool) -> dict[int, str]:
    """index in `items` -> target file; refuses an unassigned item, an unknown name or a duplicate item name."""
    owner = invert(table, what)
    seen: dict[str, int] = {}
    result: dict[int, str] = {}
    for index, item in enumerate(items):
        if item.kind == "use" and skip_use:
            continue
        if item.name in seen:
            raise SplitError(f"{what}: `{item.name}` occurs twice in the source (lines {items[seen[item.name]].line} and {item.line})")
        seen[item.name] = index
        if item.name not in owner:
            raise SplitError(f"{what}: no assignment for `{item.name}` (source line {item.line}); add it to the {what} table")
        result[index] = owner[item.name]
    unknown = sorted(set(owner) - set(seen))
    if unknown:
        raise SplitError(f"{what} table names items the source does not have: {', '.join(unknown)}")
    return result


# ---------------------------------------------------------------------------
# Identifier use, imports and visibility
# ---------------------------------------------------------------------------


def identifiers(text: str, bare: bool = False) -> set[str]:
    """Every identifier the code of `text` names, including inline format captures like `{name}`.

    Comments and string contents are ignored, except that `{name}` inside a string
    counts as a use of `name` (a format string can capture a constant). With `bare`,
    only identifiers that are not a later path segment (`a::NAME`) or a field or
    method (`.name`) count: those are the names an import has to provide.
    """
    kinds = classify(text)
    code = "".join(" " if kind != CODE and ch != "\n" else ch for ch, kind in zip(text, kinds))
    if bare:
        found = {m.group(0) for m in IDENT.finditer(code) if not re.search(r"(?:::|\.)\s*\Z", code[: m.start()])}
    else:
        found = set(IDENT.findall(code))
    literal = "".join(ch if kind == LITERAL else " " for ch, kind in zip(text, kinds))
    found.update(match.group(1) for match in FORMAT_CAPTURE.finditer(literal))
    return found


@dataclass(frozen=True)
class Leaf:
    """One name a `use` item brings in: its full path, and `as` alias when present."""

    path: tuple[str, ...]
    alias: str | None = None
    glob: bool = False

    @property
    def bound(self) -> str:
        return self.alias or self.path[-1]


USE_TOKEN = re.compile(r"::|\{|\}|,|\*|[A-Za-z_][A-Za-z0-9_]*")


def flatten_use(item_text: str) -> list[Leaf]:
    """The leaves of a `use` item: `use a::{b, c as d};` gives a::b and a::c as d."""
    masked = mask(item_text)
    match = re.search(r"\buse\s+(.*?);\s*\Z", masked, re.S)
    if match is None:
        raise SplitError(f"cannot read use item: {item_text[:80]!r}")
    tokens = USE_TOKEN.findall(match.group(1))
    leaves: list[Leaf] = []
    pos = 0

    def branch(prefix: tuple[str, ...]) -> None:
        """Parse one comma-separated element of a use tree below `prefix`."""
        nonlocal pos
        path = list(prefix)
        while pos < len(tokens):
            tok = tokens[pos]
            if tok == "{":
                pos += 1
                while pos < len(tokens) and tokens[pos] != "}":
                    branch(tuple(path))
                    if pos < len(tokens) and tokens[pos] == ",":
                        pos += 1
                if pos >= len(tokens):
                    raise SplitError(f"unbalanced use tree: {item_text[:80]!r}")
                pos += 1
                return
            if tok == "*":
                pos += 1
                leaves.append(Leaf(tuple(path), None, True))
                return
            if tok == "::":
                pos += 1
                continue
            if tok in ("}", ","):
                raise SplitError(f"unexpected `{tok}` in use tree: {item_text[:80]!r}")
            path.append(tok)
            pos += 1
            if pos < len(tokens) and tokens[pos] == "as":
                alias = tokens[pos + 1]
                pos += 2
                leaves.append(Leaf(tuple(path), alias))
                return
            if pos < len(tokens) and tokens[pos] == "::":
                continue
            if path[-1] == "self":
                path.pop()
            leaves.append(Leaf(tuple(path)))
            return

    branch(())
    if pos != len(tokens):
        raise SplitError(f"trailing tokens in use item: {item_text[:80]!r}")
    return leaves


def bump_visibility(item: Item) -> None:
    """Widen a private item or method to `pub(super)`; an item that is already pub(..) is left alone."""
    if item.vis:
        return
    masked = mask(item.text)
    match = HEADER.match(masked)
    assert match is not None
    at = match.start("quals") if match.group("quals") else match.start("kw")
    if match.group("const_fn"):
        at = match.start("const_fn")
    item.text = item.text[:at] + "pub(super) " + item.text[at:]
    item.vis = "pub(super)"


def rewrite_super(item: Item) -> int:
    """Rewrite the `super::` paths of an item that moves one module down; returns how many."""
    kinds = classify(item.text)
    out, count, i = [], 0, 0
    text = item.text
    while i < len(text):
        if (
            text.startswith("super::", i)
            and kinds[i] == CODE
            and (i == 0 or (text[i - 1] not in IDENT_CHARS and text[i - 1] != ":"))
        ):
            out.append("crate::app::")
            count += 1
            i += len("super::")
        else:
            out.append(text[i])
            i += 1
    item.text = "".join(out)
    return count


S3S_ALIAS = "s3s_crate"


def rewrite_s3s(item: Item) -> int:
    """Reach s3s through the parent's `s3s_crate` alias: `s3s::dto::Tag` becomes `s3s_crate::dto::Tag`."""
    kinds = classify(item.text)
    text, out, count, i = item.text, [], 0, 0
    while i < len(text):
        if text.startswith("s3s::", i) and kinds[i] == CODE and (i == 0 or (text[i - 1] not in IDENT_CHARS and text[i - 1] != ":")):
            out.append(S3S_ALIAS + "::")
            count += 1
            i += len("s3s::")
        else:
            out.append(text[i])
            i += 1
    item.text = "".join(out)
    return count


def remove_leaves(item: Item, gone: list[Leaf]) -> str:
    """The text of a `use` item without the given leaves (and the comma that went with each)."""
    text = item.text
    before = flatten_use(text)
    for leaf in gone:
        name = re.escape(leaf.path[-1])
        masked = mask(text)
        match = re.search(rf"(?<![A-Za-z0-9_:]){name}(?:\s+as\s+_)?(?=\s*[,}}])", masked)
        if match is None:
            continue
        start, end = match.start(), match.end()
        after = re.match(r"\s*,", masked[end:])
        if after is not None:
            end += after.end()
        else:
            preceding = re.search(r",\s*\Z", masked[:start])
            if preceding is not None:
                start = preceding.start()
        text = text[:start] + text[end:]
    rest = Item(kind="use", name=item.name, start=0, end=len(text), text=text)
    expected = [leaf for leaf in before if leaf not in gone]
    if flatten_use(rest.text) != expected:
        raise SplitError(f"could not remove {[l.path[-1] for l in gone]} from `use` at line {item.line} cleanly")
    return text


def has_super_path(item: Item) -> bool:
    kinds = classify(item.text)
    return any(
        item.text.startswith("super::", i) and kinds[i] == CODE and (i == 0 or item.text[i - 1] not in IDENT_CHARS)
        for i in (m.start() for m in re.finditer("super::", item.text))
    )


# ---------------------------------------------------------------------------
# Building the split
# ---------------------------------------------------------------------------

PARENT_MODULE = "crate::app::bucket_usecase"
S3S_PATH = re.compile(r'(^|[^"A-Za-z0-9_])s3s::')


@dataclass
class Plan:
    """What each target file receives."""

    prod: dict[str, list[Item]] = field(default_factory=dict)
    methods: dict[str, list[Item]] = field(default_factory=dict)
    tests: dict[str, list[Item]] = field(default_factory=dict)
    imports: dict[str, set[tuple[str, str]]] = field(default_factory=dict)  # file -> (module, name), production code
    test_imports: dict[str, set[tuple[str, str]]] = field(default_factory=dict)  # file -> (module, name), test code
    bumped: list[str] = field(default_factory=list)
    rewritten: int = 0
    s3s_prod: int = 0  # `s3s::` paths rewritten in production code
    s3s_test: int = 0  # ... in test code
    glob_users: dict[str, list[str]] = field(default_factory=dict)  # why a file keeps `use super::*;`
    trait_uses: dict[str, list[Leaf]] = field(default_factory=dict)  # file -> trait imports it now carries
    removed_from_parent: list[Leaf] = field(default_factory=list)
    parent_use_text: dict[str, str] = field(default_factory=dict)  # `use` item text as written to the parent, by original text


def split_plan(parsed: Parsed) -> Plan:
    """Assign every item, then work out the cross-file imports and the visibility bumps."""
    plan = Plan()
    top_owner = assign(parsed.top, TOP_LEVEL, "TOP_LEVEL", skip_use=True)
    method_owner = assign(parsed.methods, METHODS, "METHODS", skip_use=False)
    test_owner = assign(parsed.test_items, TESTS, "TESTS", skip_use=True)
    for file in (*FAMILIES, PARENT):
        plan.prod[file], plan.methods[file], plan.tests[file] = [], [], []
    for file in FLAT:
        plan.tests[file] = []
    for index, file in top_owner.items():
        plan.prod[file].append(parsed.top[index])
    for index, file in method_owner.items():
        plan.methods[file].append(parsed.methods[index])
    for index, file in test_owner.items():
        plan.tests[file].append(parsed.test_items[index])

    # Defs by name: production (top-level items and methods) and test helpers.
    prod_def: dict[str, tuple[str, Item, bool]] = {}
    test_def: dict[str, tuple[str, Item]] = {}
    for file in (*FAMILIES, PARENT):
        for item in plan.prod[file]:
            if not item.name.startswith(("impl", "use")):
                prod_def[item.name] = (file, item, False)
        for item in plan.methods[file]:
            if item.name in prod_def:
                raise SplitError(f"method `{item.name}` shares its name with a top-level item")
            prod_def[item.name] = (file, item, True)
    for file, items in plan.tests.items():
        for item in items:
            if item.kind == "fn":
                test_def[item.name] = (file, item)

    prod_ids = {f: set().union(*(identifiers(i.text) for i in plan.prod[f] + plan.methods[f]), set()) for f in (*FAMILIES, PARENT)}
    test_ids = {f: set().union(*(identifiers(i.text) for i in plan.tests[f]), set()) for f in plan.tests}
    prod_bare = {f: set().union(*(identifiers(i.text, bare=True) for i in plan.prod[f] + plan.methods[f]), set()) for f in prod_ids}
    test_bare = {f: set().union(*(identifiers(i.text, bare=True) for i in plan.tests[f]), set()) for f in plan.tests}
    for file in (*FAMILIES, *FLAT, PARENT):
        plan.imports[file], plan.test_imports[file] = set(), set()

    to_bump: dict[int, Item] = {}
    for file in (*FAMILIES, PARENT):
        for name in sorted(prod_ids[file]):
            if name not in prod_def:
                continue
            owner, item, is_method = prod_def[name]
            if owner == file or owner == PARENT:
                continue
            if file == PARENT:
                raise SplitError(
                    f"the parent keeps code that names `{name}`, which the tables move to {owner}; "
                    f"keep `{name}` in {PARENT} or move its user"
                )
            if not is_method and name in prod_bare[file]:
                plan.imports[file].add((owner, name))
            to_bump[id(item)] = item
            plan.bumped.append(f"{owner}::{name} (used by {file})")
    for file in (*FAMILIES, *FLAT, PARENT):
        for name in sorted(test_ids.get(file, set())):
            if name in prod_def:
                owner, item, is_method = prod_def[name]
                if owner in (file, PARENT):
                    continue
                if not is_method and name in test_bare[file]:
                    plan.test_imports[file].add((owner, name))
                to_bump[id(item)] = item
                plan.bumped.append(f"{owner}::{name} (used by {file} tests)")
            if name in test_def:
                owner, item = test_def[name]
                if owner == file:
                    continue
                if owner not in FLAT:
                    raise SplitError(
                        f"test helper `{name}` lives in {owner} but {file} uses it; move it to test_support in TESTS"
                    )
                plan.test_imports[file].add((owner, name))
                to_bump[id(item)] = item
                plan.bumped.append(f"{owner}::{name} (test helper used by {file})")
    for item in to_bump.values():
        bump_visibility(item)

    # Trait imports used only for their methods (see TRAIT_IMPORTS).
    code_ids = identifiers("\n".join(i.text for i in (*(t for t in parsed.top if t.kind != "use"), parsed.impl, parsed.tests)))
    listed: set[str] = set()
    for use_item in (t for t in parsed.top if t.kind == "use"):
        for leaf in flatten_use(use_item.text):
            key = "::".join(leaf.path)
            if leaf.glob:
                continue
            by_name = leaf.alias != "_" and leaf.bound in code_ids
            if by_name and key not in TRAIT_IMPORTS:
                continue
            if key not in TRAIT_IMPORTS:
                raise SplitError(
                    f"the parent imports `{key}` without naming it anywhere: it brings a trait in for method calls, "
                    "which rustc does not credit through a child's `use super::*;`; list it in TRAIT_IMPORTS"
                )
            if by_name:
                raise SplitError(f"TRAIT_IMPORTS lists `{key}`, but the source names it directly; remove the entry")
            listed.add(key)
            for file in TRAIT_IMPORTS[key]:
                if file != PARENT:
                    plan.trait_uses.setdefault(file, []).append(leaf)
            if PARENT not in TRAIT_IMPORTS[key]:
                plan.removed_from_parent.append(leaf)
    missing = sorted(set(TRAIT_IMPORTS) - listed)
    if missing:
        raise SplitError(f"TRAIT_IMPORTS names imports the source does not have: {', '.join(missing)}")

    # Production code moving one module down: `super::` paths shift.
    for file in FAMILIES:
        for item in plan.prod[file] + plan.methods[file]:
            plan.rewritten += rewrite_super(item)
    for file in FAMILIES:
        for item in plan.prod[file] + plan.methods[file]:
            plan.s3s_prod += rewrite_s3s(item)
    for file in (*FAMILIES, *FLAT):
        for item in plan.tests[file]:
            plan.s3s_test += rewrite_s3s(item)
            if has_super_path(item):
                raise SplitError(f"test `{item.name}` has a `super::` path and would change meaning when moved to {file}")
    return plan


def license_header(parsed: Parsed) -> str:
    """The `//` licence block that opens the source, exactly as the neighbouring files have it."""
    lines = []
    for line in parsed.prelude.splitlines(keepends=True):
        if not line.startswith("//") or line.startswith("//!"):
            break
        lines.append(line)
    if not lines or "Apache License" not in "".join(lines):
        raise UsageError("the source does not open with the Apache-2.0 licence header")
    return "".join(lines)


def dedent4(text: str) -> str:
    """Remove four spaces of indentation from every line that does not start inside a literal."""
    kinds = classify(text)
    out, offset = [], 0
    for line in text.splitlines(keepends=True):
        if line.startswith("    ") and kinds[offset] != LITERAL:
            line = line[4:]
        out.append(line)
        offset += len(text[offset:].split("\n", 1)[0]) + 1
    return "".join(out)


def group_use(leaves: list[Leaf], cfg: str = "") -> list[str]:
    """`use` lines for the given leaves, one line per shared module path."""
    by_prefix: dict[tuple[str, ...], list[str]] = {}
    for leaf in sorted(leaves, key=lambda l: (l.path, l.alias or "")):
        name = leaf.path[-1] + (f" as {leaf.alias}" if leaf.alias else "")
        by_prefix.setdefault(leaf.path[:-1], []).append(name)
    lines = []
    for prefix, names in by_prefix.items():
        module = "::".join(prefix)
        target = names[0] if len(names) == 1 else "{" + ", ".join(names) + "}"
        lines.append(f"{cfg}use {module}::{target};")
    return lines


def absolute(leaf: Leaf) -> Leaf:
    """The leaf with a leading `super::` (relative to `app`) spelled `crate::app::`."""
    if leaf.path[0] == "super":
        return Leaf(("crate", "app", *leaf.path[1:]), leaf.alias, leaf.glob)
    return leaf


def grouped_sibling_uses(pairs: set[tuple[str, str]]) -> list[str]:
    """`use crate::app::bucket_usecase::<file>::{names};` lines, one per source file."""
    by_file: dict[str, list[str]] = {}
    for module, name in sorted(pairs):
        by_file.setdefault(module, []).append(name)
    return [
        f"use {PARENT_MODULE}::{module}::" + (names[0] if len(names) == 1 else "{" + ", ".join(names) + "}") + ";"
        for module, names in by_file.items()
    ]


def render(parsed: Parsed, plan: Plan) -> dict[str, str]:
    """The text of every file the split writes, keyed by repository-relative path."""
    licence = license_header(parsed)
    top_uses = [item for item in parsed.top if item.kind == "use"]
    parent_leaves: dict[str, Leaf] = {}
    for item in top_uses:
        for leaf in flatten_use(item.text):
            if not leaf.glob and leaf.alias != "_":
                parent_leaves[leaf.bound] = leaf
    pool: list[Leaf] = []
    for item in parsed.test_items:
        if item.kind != "use":
            continue
        if "#[" in mask(item.text).split("use", 1)[0]:
            raise SplitError(f"test `use` at line {item.line} carries an attribute; the codemod does not move those")
        for leaf in flatten_use(item.text):
            if leaf.glob:
                if leaf.path != ("super",):
                    raise SplitError(f"test glob import `{'::'.join(leaf.path)}::*` is not supported")
                continue
            if leaf.alias == "_":
                raise SplitError(f"test import `{'::'.join(leaf.path)} as _` cannot be placed by usage")
            pool.append(leaf)

    s3s_for_parent: dict[str, Leaf] = {}

    def test_use_lines(file: str) -> list[str]:
        ids = set().union(*(identifiers(i.text, bare=True) for i in plan.tests[file]), set())
        chosen: list[Leaf] = []
        for leaf in pool:
            if leaf.bound not in ids:
                continue
            known = parent_leaves.get(leaf.bound)
            if known is not None and known.path == leaf.path and known.alias == leaf.alias:
                continue  # the parent already brings the same item in; `use super::*` carries it down
            if leaf.path[0] == "s3s":
                clash = s3s_for_parent.get(leaf.bound)
                if known is not None or (clash is not None and clash != leaf):
                    raise SplitError(f"test import of `{'::'.join(leaf.path)}` clashes with a different binding of `{leaf.bound}`")
                s3s_for_parent[leaf.bound] = leaf
                continue
            chosen.append(leaf)
        lines = group_use(chosen)
        lines += grouped_sibling_uses(plan.test_imports[file])
        return sorted(set(lines))

    def prod_use_lines(file: str) -> list[str]:
        return grouped_sibling_uses(plan.imports[file])

    # The names a file can take from the parent through `use super::*;`.
    mod_names = set(FAMILIES) | set(STUBS) | set(FLAT)
    parent_names = set(parent_leaves) | mod_names
    for item in plan.prod[PARENT]:
        if not item.name.startswith(("impl", "use")):
            parent_names.add(item.name)

    def wants_glob(file: str, code_ids: set[str]) -> bool:
        used = sorted(code_ids & parent_names)
        plan.glob_users[file] = used
        return bool(used)

    out: dict[str, str] = {}

    def tests_block(file: str) -> str:
        items = plan.tests[file]
        if not items:
            return ""
        ids = set().union(*(identifiers(i.text, bare=True) for i in items))
        visible = parent_names | {i.name for i in plan.prod[file]}
        lines = (["use super::*;"] if ids & visible else []) + test_use_lines(file)
        head = "".join(f"    {line}\n" for line in lines)
        return "#[cfg(test)]\nmod tests {\n" + head + "\n" + "\n\n".join(i.text for i in items) + "\n}\n"

    for file in FAMILIES:
        prod_ids = set().union(*(identifiers(i.text, bare=True) for i in plan.prod[file] + plan.methods[file]), set())
        parts = [licence, f"//! {DOCS[file]}\n"]
        header = []
        if wants_glob(file, prod_ids):
            header.append("use super::*;")
        header += prod_use_lines(file)
        header += group_use([absolute(leaf) for leaf in plan.trait_uses.get(file, [])])
        if header:
            parts.append("\n".join(header) + "\n")
        if plan.prod[file]:
            parts.append("\n\n".join(i.text for i in plan.prod[file]) + "\n")
        if plan.methods[file]:
            parts.append("impl DefaultBucketUsecase {\n" + "\n\n".join(i.text for i in plan.methods[file]) + "\n}\n")
        block = tests_block(file)
        if block:
            parts.append(block)
        out[f"{SPLIT_DIR}/{file}.rs"] = "\n".join(parts)

    for file in FLAT:
        ids = set().union(*(identifiers(i.text, bare=True) for i in plan.tests[file]), set())
        parts = [licence, f"//! {DOCS[file]}\n"]
        header = []
        if wants_glob(file, ids):
            header.append("use super::*;")
        header += test_use_lines(file)
        if header:
            parts.append("\n".join(header) + "\n")
        parts.append("\n\n".join(dedent4(i.text) for i in plan.tests[file]) + "\n")
        out[f"{SPLIT_DIR}/{file}.rs"] = "\n".join(parts)

    for file in STUBS:
        out[f"{SPLIT_DIR}/{file}.rs"] = f"{licence}\n//! {DOCS[file]}\n"

    # The parent: the source with the moved items cut out, plus the module list.
    text = parsed.text
    edits: list[tuple[int, int, str]] = []

    def cut(item: Item, replacement: str = "") -> None:
        end = item.end
        while end < len(text) and text[end] in " \t\r\n":
            end += 1
        edits.append((item.start, end, replacement + ("\n\n" if replacement else "")))

    for use_item in (t for t in parsed.top if t.kind == "use"):
        gone = [leaf for leaf in flatten_use(use_item.text) if leaf in plan.removed_from_parent]
        if gone:
            plan.parent_use_text[use_item.text] = remove_leaves(use_item, gone)
            edits.append((use_item.start, use_item.end, plan.parent_use_text[use_item.text]))
    owner_of_top = assign(parsed.top, TOP_LEVEL, "TOP_LEVEL", skip_use=True)
    for index, file in owner_of_top.items():
        if file != PARENT:
            cut(parsed.top[index])
    impl_head = text[parsed.impl.start : text.index("{", _header_end(text, parsed.impl)) + 1]
    kept = plan.methods[PARENT]
    cut(parsed.impl, impl_head + "\n" + "\n\n".join(i.text for i in kept) + "\n}")
    tests_head = text[parsed.tests.start : text.index("{", _header_end(text, parsed.tests)) + 1]
    parent_tests_items = plan.tests[PARENT]
    test_lines = ["use super::*;"] + test_use_lines(PARENT)
    parent_tests = tests_head + "\n" + "".join(f"    {line}\n" for line in test_lines) + "\n"
    parent_tests += "\n\n".join(i.text for i in parent_tests_items) + "\n}"
    cut(parsed.tests, parent_tests)

    cfg_uses = []
    by_prefix: dict[tuple[str, ...], list[Leaf]] = {}
    for leaf in s3s_for_parent.values():
        by_prefix.setdefault(leaf.path[:-1], []).append(leaf)
    for leaves in by_prefix.values():
        cfg_uses += group_use(leaves, cfg="#[cfg(test)]\n")
    if plan.s3s_prod or plan.s3s_test:
        cfg_uses.insert(0, ("" if plan.s3s_prod else "#[cfg(test)]\n") + f"use s3s as {S3S_ALIAS};")
    clash = (set(FAMILIES) | set(STUBS) | set(FLAT)) & (set(parent_leaves) | {i.name for i in plan.prod[PARENT]})
    if clash:
        raise SplitError(f"module name(s) {sorted(clash)} are already bound in the parent")
    mods = []
    for name in sorted(set(FAMILIES) | set(STUBS) | set(FLAT)):
        mods.append(("#[cfg(test)]\n" if name in FLAT else "") + f"mod {name};")
    first_non_use = next(item for item in parsed.top if item.kind != "use")
    alias_note = (
        "// The files in `bucket_usecase/` reach the s3 wire crate through this alias (`use super::*;`),\n"
        "// which keeps the direct s3s footprint on this file alone.\n"
    )
    insertion = "\n".join(mods) + "\n\n" + (alias_note + "\n".join(cfg_uses) + "\n\n" if cfg_uses else "")
    edits.append((first_non_use.start, first_non_use.start, insertion))

    doc_end = text.index("\n", text.rindex("//!", 0, len(parsed.prelude))) + 1
    edits.append(
        (
            doc_end,
            doc_end,
            "//!\n//! The struct, admission control and helpers shared by every configuration family stay here. Each family\n"
            "//! keeps its operations and their tests in its own file under `bucket_usecase/`.\n",
        )
    )
    pieces, cursor = [], 0
    for start, end, replacement in sorted(edits, key=lambda e: (e[0], e[1] != e[0])):
        pieces.append(text[cursor:start])
        pieces.append(replacement)
        cursor = max(cursor, end)
    pieces.append(text[cursor:])
    parent = "".join(pieces)
    out[SOURCE] = parent.rstrip("\n") + "\n"

    for path, body in out.items():
        hit = S3S_PATH.search(body) if path != SOURCE else None
        if hit is not None:
            line = body.count("\n", 0, hit.start()) + 1
            raise SplitError(
                f"{path}:{line} names s3s directly ({body.splitlines()[line - 1].strip()!r}); "
                "new files must reach it through `use super::*;` (footprint rule)"
            )
    return out


# ---------------------------------------------------------------------------
# Verification, writing, command line
# ---------------------------------------------------------------------------

TEST_ATTR = re.compile(r"(?m)^[ \t]*#\[((tokio::)?test([\]\(])|(test_case::)?test_case\()")


def squash(text: str) -> str:
    """Whitespace-free text without trailing commas, for comparing code across a reformat."""
    return re.sub(r",([)\]}])", r"\1", re.sub(r"\s+", "", mask(text, keep_literals=True)))


def verify(parsed: Parsed, plan: Plan, written: dict[str, str]) -> None:
    """Every moved item is present in its target, and no test was lost: fail loudly otherwise."""
    flat = {path: squash(body) for path, body in written.items()}
    place = {**{f: f"{SPLIT_DIR}/{f}.rs" for f in (*FAMILIES, *FLAT)}, PARENT: SOURCE}
    for file in (*FAMILIES, *FLAT, PARENT):
        for item in plan.prod.get(file, []) + plan.methods.get(file, []) + plan.tests.get(file, []):
            if squash(item.text) not in flat[place[file]]:
                raise SplitError(f"verification: `{item.name}` is not intact in {place[file]}")
    for item in parsed.top:
        if item.kind == "use" and squash(plan.parent_use_text.get(item.text, item.text)) not in flat[SOURCE]:
            raise SplitError(f"verification: the parent lost `{item.name}`")
    before = len(TEST_ATTR.findall(parsed.text))
    after = sum(len(TEST_ATTR.findall(body)) for body in written.values())
    if before != after:
        raise SplitError(f"verification: {before} test attributes before the split, {after} after")


def census(parsed: Parsed, plan: Plan, written: dict[str, str]) -> str:
    lines = [f"{'file':<22}{'lines':>7}{'prod fns/items':>16}{'methods':>9}{'tests':>7}"]
    original = parsed.text.count("\n")
    lines.append(f"{'(before) parent':<22}{original:>7}")
    place = {**{f: f"{SPLIT_DIR}/{f}.rs" for f in (*FAMILIES, *FLAT, *STUBS)}, PARENT: SOURCE}
    for file in (PARENT, *FAMILIES, *FLAT, *STUBS):
        body = written[place[file]]
        n_tests = len(TEST_ATTR.findall(body))
        lines.append(
            f"{file:<22}{body.count(chr(10)):>7}{len(plan.prod.get(file, [])):>16}{len(plan.methods.get(file, [])):>9}{n_tests:>7}"
        )
    lines.append(f"trait imports moved out of the parent: {', '.join(leaf.path[-1] for leaf in plan.removed_from_parent)}")
    lines.append(f"visibility widened to pub(super): {len(plan.bumped)}")
    for entry in sorted(set(plan.bumped)):
        lines.append(f"  {entry}")
    lines.append(f"`super::` paths rewritten to `crate::app::`: {plan.rewritten}")
    lines.append(f"`s3s::` paths rewritten to `{S3S_ALIAS}::`: {plan.s3s_prod} in production code, {plan.s3s_test} in tests")
    return "\n".join(lines)


def run_rustfmt(root: Path, paths: list[str]) -> None:
    rustfmt = shutil.which("rustfmt")
    if rustfmt is None:
        raise UsageError("rustfmt not found; install it or pass --no-fmt")
    done = subprocess.run([rustfmt, "--edition", "2024", *paths], cwd=root, capture_output=True, text=True)
    if done.returncode != 0:
        raise UsageError(f"rustfmt failed:\n{done.stderr}{done.stdout}")


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", default=".", help="repository root (default: current directory)")
    parser.add_argument("--no-fmt", action="store_true", help="do not run rustfmt on the written files")
    parser.add_argument("--dump", action="store_true", help="print the item inventory and the plan; write nothing")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()
    source = root / SOURCE
    if not source.is_file():
        raise UsageError(f"{SOURCE} not found under {root}")
    if (root / SPLIT_DIR).exists():
        raise UsageError(
            f"{SPLIT_DIR} already exists: restore the unsplit {SOURCE} and remove the directory before re-running"
        )
    text = source.read_text()
    if re.search(r"(?m)^mod (cors|tagging|listing);", text):
        raise UsageError(f"{SOURCE} already declares the split modules: restore the unsplit file first")
    parsed = parse_source(text)
    if args.dump:
        dump(parsed)
        return 0
    plan = split_plan(parsed)
    written = render(parsed, plan)
    verify(parsed, plan, written)
    (root / SPLIT_DIR).mkdir()
    for path, body in written.items():
        (root / path).write_text(body)
    if not args.no_fmt:
        run_rustfmt(root, sorted(written))
        reread = {path: (root / path).read_text() for path in written}
        verify(parsed, plan, reread)
        written = reread
    print(census(parsed, plan, written))
    return 0


class SelfTest(unittest.TestCase):
    def test_split_items_keeps_comments_with_the_next_item(self) -> None:
        text = "// one\nfn a() {}\n\n/// doc\n\n#[x]\nfn b() { let s = \"}\"; }\nconst C: u8 = 1;\n"
        items = split_items(text, 0, len(text))
        self.assertEqual([i.name for i in items], ["a", "b", "C"])
        self.assertTrue(items[0].text.startswith("// one"))
        self.assertTrue(items[1].text.startswith("/// doc"))
        self.assertEqual(items[2].text, "const C: u8 = 1;")

    def test_trailing_comment_is_an_error(self) -> None:
        text = "fn a() {}\n// orphan\n"
        with self.assertRaises(SplitError):
            split_items(text, 0, len(text))

    def test_flatten_use(self) -> None:
        leaves = flatten_use("use a::{b, c::{d as e, self}, f::*};")
        self.assertEqual(
            [(leaf.path, leaf.alias, leaf.glob) for leaf in leaves],
            [(("a", "b"), None, False), (("a", "c", "d"), "e", False), (("a", "c"), None, False), (("a", "f"), None, True)],
        )

    def test_identifiers_ignore_comments_and_strings_but_not_format_captures(self) -> None:
        found = identifiers('fn f() { /* hidden */ let x = format!("{CAP} literal"); // gone\n y(x) }')
        self.assertTrue({"f", "x", "y", "CAP"} <= found)
        self.assertNotIn("hidden", found)
        self.assertNotIn("literal", found)
        self.assertNotIn("gone", found)

    def test_unassigned_item_fails_loudly(self) -> None:
        item = Item(kind="fn", name="mystery", start=0, end=1, text="fn mystery() {}", line=7)
        with self.assertRaises(SplitError) as ctx:
            assign([item], {"cors": []}, "TOP_LEVEL", skip_use=True)
        self.assertIn("no assignment for `mystery`", str(ctx.exception))

    def test_table_name_without_item_fails_loudly(self) -> None:
        with self.assertRaises(SplitError) as ctx:
            assign([], {"cors": ["ghost"]}, "TOP_LEVEL", skip_use=True)
        self.assertIn("ghost", str(ctx.exception))

    def test_duplicate_table_entry_fails_loudly(self) -> None:
        with self.assertRaises(SplitError):
            invert({"a": ["x"], "b": ["x"]}, "TOP_LEVEL")

    def test_bump_visibility(self) -> None:
        for before, after in (
            ("fn a() {}", "pub(super) fn a() {}"),
            ("#[x]\n    async fn a() {}", "#[x]\n    pub(super) async fn a() {}"),
            ("pub(crate) fn a() {}", "pub(crate) fn a() {}"),
        ):
            item = split_items(before, 0, len(before))[0]
            bump_visibility(item)
            self.assertEqual(item.text, after)

    def test_remove_leaves_takes_the_comma_along(self) -> None:
        text = "use a::{\n    X as _,\n    Y,\n    b::{P, Q, R},\n};"
        item = Item(kind="use", name="use", start=0, end=len(text), text=text)
        for gone, expected in (("X", ["Y", "P", "Q", "R"]), ("R", ["X", "Y", "P", "Q"])):
            leaves = [leaf for leaf in flatten_use(text) if leaf.path[-1] == gone]
            rewritten = remove_leaves(item, leaves)
            self.assertEqual([leaf.path[-1] for leaf in flatten_use(rewritten)], expected)

    def test_trait_import_without_a_name_use_needs_an_entry(self) -> None:
        parsed = parse_source(
            "// Copyright\n// Apache License\n\n//! doc\n\nuse x::T as _;\n"
            "#[derive(Clone)]\npub struct DefaultBucketUsecase;\n"
            "impl DefaultBucketUsecase {}\n#[cfg(test)]\nmod tests {}\n"
        )
        with (
            mock.patch.dict(TOP_LEVEL, {PARENT: ["DefaultBucketUsecase"]}, clear=True),
            mock.patch.dict(METHODS, {}, clear=True),
            mock.patch.dict(TESTS, {}, clear=True),
            mock.patch.dict(TRAIT_IMPORTS, {}, clear=True),
            self.assertRaises(SplitError) as ctx,
        ):
            split_plan(parsed)
        self.assertIn("TRAIT_IMPORTS", str(ctx.exception))

    def test_rewrite_super_leaves_strings_alone(self) -> None:
        item = Item(kind="fn", name="f", start=0, end=0, text='fn f() { super::x::y(); "super::z"; a::super::b; }')
        self.assertEqual(rewrite_super(item), 1)
        self.assertEqual(item.text, 'fn f() { crate::app::x::y(); "super::z"; a::super::b; }')


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except UsageError as err:
        print(f"error: {err}", file=sys.stderr)
        sys.exit(2)
    except SplitError as err:
        print(f"error: {err}", file=sys.stderr)
        sys.exit(1)
