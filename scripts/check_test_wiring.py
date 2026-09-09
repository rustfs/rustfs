#!/usr/bin/env python3
"""Fail when committed tests silently fall out of their execution wiring."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from unittest import mock
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from scanner_abba import (
    MAX_JSON_BYTES,
    RELEASE_PROFILE_ARTIFACTS,
    SCENARIOS,
    digest,
    number,
    read_json,
    require,
    sha,
    write_json,
)


ROOT = Path(__file__).resolve().parents[1]
SCANNER_HEAL_REGISTRY_SCHEMA_MAX = 2
SCANNER_HEAL_RELEASE_REQUIRED_GATES = (
    "G01", "G02", "G03", "G04", "G05", "G06", "G07", "G08", "G09", "G10", "G11", "G12", "G13", "G14",
    "P1", "P2", "P3", "P4", "R-E", "R-D", "R-L",
)
SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS = {
    "G07": (
        "mrf_responsibility_oracle",
        "commit_boundary_crash_matrix",
    ),
    "G08": (
        "mrf_capacity_evidence",
        "disk_full_matrix",
        "replica_loss_matrix",
    ),
    "G03": (
        "durable_root_publication_proof",
        "scoped_ack_request_identity",
        "participating_peer_capability_snapshot",
        "mixed_peer_ack_fallback_oracle",
    ),
    "G09": (
        "mixed_version_reader_evidence",
        "mixed_version_writer_evidence",
        "rollback_payload_evidence",
    ),
    "G11": (
        "maintenance_producer_matrix",
        "complete_producer_inventory",
        "segment_activation_preflight",
    ),
    "G14": (
        "same_window_field_evidence",
        "ec8_4_evidence",
        "multi_set_evidence",
        "multi_pool_evidence",
        "distributed_segment_invalidation_evidence",
    ),
    "P4": (
        "mrf_scale_measurement",
        "mrf_replay_cost_measurement",
        "retained_responsibility_evidence",
        "mrf_cleanup_gc_soak_evidence",
    ),
    "P2": (
        "post_stop_convergence_measurement",
        "cold_segment_reuse_measurement",
    ),
}
SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS = {
    "G01": ("root_authority_evidence", "quota_authority_evidence"),
    "G02": ("bounded_checkpoint_oracle", "independent_version_inventory"),
    "G03": SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS["G03"],
    "G04": ("cache_boundary_crash_evidence", "root_floor_intent_crash_evidence"),
    "G05": ("per_object_outcome_oracle", "terminal_retention_bounds"),
    "G06": ("concurrent_status_evidence", "legacy_client_compatibility", "truncation_behavior"),
    "G07": ("mrf_responsibility_oracle", "commit_boundary_crash_matrix"),
    "G08": ("mrf_capacity_evidence", "disk_full_matrix", "replica_loss_matrix"),
    "G09": SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS["G09"],
    "G10": ("scheduler_bound_evidence", "pressure_recovery_evidence"),
    "G11": SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS["G11"],
    "G12": ("reset_quota_path_evidence", "settlement_quota_path_evidence"),
    "G13": ("quorum_minus_one_matrix", "unknown_disk_remount_matrix", "object_lock_dry_run_grace_evidence"),
    "G14": SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS["G14"],
    "P1": ("cold_walk_share_measurement", "foreground_latency_throughput_measurement", "profile_evidence"),
    "P2": SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS["P2"],
    "P3": ("two_hour_pressure_measurement", "heal_capacity_measurement", "recovery_window_measurement"),
    "P4": SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS["P4"],
    "R-E": ("fixed_budget_restart_evidence", "enumeration_evidence", "classification_evidence"),
    "R-D": ("manager_disposition_evidence", "event_disposition_evidence", "ledger_disposition_evidence", "grace_handling"),
    "R-L": ("legacy_source_conflict_evidence", "migration_gap_evidence", "crash_safe_source_retirement_evidence"),
}
SCANNER_HEAL_RELEASE_MIXED_VERSION_ROLES = {
    ("G03", "durable_root_publication_proof"): "durable-root-publication",
    ("G03", "scoped_ack_request_identity"): "scoped-ack-request",
    ("G03", "participating_peer_capability_snapshot"): "peer-capability-snapshot",
    ("G03", "mixed_peer_ack_fallback_oracle"): "mixed-peer-ack-fallback",
    ("G09", "mixed_version_reader_evidence"): "mixed-version-reader",
    ("G09", "mixed_version_writer_evidence"): "mixed-version-writer",
    ("G09", "rollback_payload_evidence"): "rollback-payload",
    ("R-L", "legacy_source_conflict_evidence"): "legacy-source-conflict",
    ("R-L", "migration_gap_evidence"): "migration-gap",
    ("R-L", "crash_safe_source_retirement_evidence"): "crash-safe-source-retirement",
}
SCANNER_HEAL_RELEASE_MRF_DURABLE_REPLAY_FIELDS = {
    ("G07", "mrf_responsibility_oracle"),
    ("G07", "commit_boundary_crash_matrix"),
    ("P4", "mrf_replay_cost_measurement"),
    ("P4", "retained_responsibility_evidence"),
    ("P4", "mrf_cleanup_gc_soak_evidence"),
}
SCANNER_HEAL_RELEASE_MRF_ARTIFACT_KINDS = {
    ("G07", "mrf_responsibility_oracle"): "mrf-durable-responsibility-oracle",
    ("G07", "commit_boundary_crash_matrix"): "mrf-commit-boundary-crash-matrix",
    ("G08", "mrf_capacity_evidence"): "mrf-capacity-boundary",
    ("G08", "disk_full_matrix"): "mrf-disk-full-enospc-matrix",
    ("G08", "replica_loss_matrix"): "mrf-replica-loss-matrix",
    ("P4", "mrf_scale_measurement"): "mrf-scale-measurement",
    ("P4", "mrf_replay_cost_measurement"): "mrf-replay-cost-measurement",
    ("P4", "retained_responsibility_evidence"): "mrf-retained-responsibility-soak",
    ("P4", "mrf_cleanup_gc_soak_evidence"): "mrf-cleanup-gc-soak",
}
SCANNER_HEAL_SEGMENT_ACTIVATION_FAIL_CLOSED_CHECKS = (
    "missing_producer_identity",
    "restart_gap",
    "generation_gap",
    "overflow",
    "missing_cold_zero_walk_oracle",
    "distributed_without_peer_invalidation",
)
SCANNER_HEAL_SEGMENT_ACTIVATION_PROOF_INPUTS = (
    "source",
    "bucket_incarnation",
    "key_format",
    "baseline_scan_plan_digest",
    "process_epoch",
    "generation_window",
    "producer_identities",
)
SCANNER_HEAL_RELEASE_G07_REQUIRED_CASES = {
    "mrf_responsibility_oracle": (
        "legacy-journal-replay",
        "scoped-journal-replay",
        "committed-checkpoint-replay",
    ),
    "commit_boundary_crash_matrix": (
        "before-committed-payload",
        "after-payload-before-manifest",
        "after-manifest-before-cleanup",
        "restart-replay-before-successor",
    ),
}
SCANNER_HEAL_RELEASE_G08_REQUIRED_CASES = {
    "mrf_capacity_evidence": (
        "queue-count-limit",
        "journal-byte-limit",
        "committed-payload-byte-limit",
    ),
    "disk_full_matrix": (
        "payload-write-enospc",
        "manifest-write-enospc",
        "journal-write-enospc",
        "cleanup-delete-enospc",
    ),
    "replica_loss_matrix": (
        "single-replica-loss",
        "quorum-minus-one",
        "all-replicas-unavailable",
    ),
}
SCANNER_HEAL_RELEASE_MRF_CLEANUP_GC_SOAK_CASES = (
    "retained-anchor-survives-restart",
    "verified-successor-allows-idle-gc",
    "stale-legacy-journal-cleanup",
    "repeated-replay-no-resurrection",
)
SCANNER_HEAL_RELEASE_CRASH_BOUNDARY_FIELDS = {
    ("G04", "root_floor_intent_crash_evidence"): (
        "persist-failure-no-202",
        "same-key-retry-reuses-intent",
        "different-params-conflict",
        "process-restart-replay",
    ),
}
SCANNER_HEAL_RELEASE_SCOPED_ACK_CASES = {
    "durable_root_publication_proof": (
        "root-cas-success",
        "root-readback-success",
        "dirty-fallback-on-missing-proof",
    ),
    "scoped_ack_request_identity": (
        "bucket-incarnation",
        "exact-generation",
        "scanner-instance",
        "participating-peer-set",
    ),
    "participating_peer_capability_snapshot": (
        "supports-scoped-ack",
        "probe-only-fallback",
        "missing-capability-dirty",
    ),
    "mixed_peer_ack_fallback_oracle": (
        "legacy-peer-fallback",
        "truncated-token-reject",
        "restarted-peer-reject",
    ),
}
SCANNER_HEAL_RELEASE_MIXED_VERSION_CASES = {
    "mixed_version_reader_evidence": (
        "old-writer-new-reader",
        "new-writer-old-reader",
    ),
    "mixed_version_writer_evidence": (
        "old-reader-new-writer",
        "new-reader-old-writer",
    ),
    "rollback_payload_evidence": (
        "rollback-to-old",
        "rollback-to-new",
        "unknown-field-retained",
    ),
}
SCANNER_HEAL_RELEASE_P4_RETAINED_RESPONSIBILITY_CASES = (
    "retain-pending-replay-anchor",
    "verified-proof-discharges-anchor",
    "idle-cleanup-reclaims-runtime-checkpoint",
    "idle-cleanup-reclaims-replay-source",
)
SCANNER_HEAL_RELEASE_SCHEDULER_BOUNDS = (
    "admission-retry-idempotency",
    "deadline-budget",
    "lock-hold-bound",
    "minimum-progress",
)
SCANNER_HEAL_RELEASE_PRESSURE_METRICS = (
    "foreground_p95_ms",
    "foreground_p99_ms",
    "throughput_ops",
    "error_rate",
    "heal_lock_wait_p99_ms",
    "attempt_cost_samples",
)
SCHEDULED_ALERT_WORKFLOWS = tuple(
    item["workflow"]
    for item in json.loads((ROOT / ".github/scheduled-validations.json").read_text())
)


def words(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def rust_code_only(source: str) -> str:
    """Blank Rust comments and literals while preserving byte positions."""
    def quoted_end(quote_index: int, delimiter: str) -> int:
        end = quote_index + 1
        while end < len(source):
            if source[end] == "\\":
                end += 2
            elif source[end] == delimiter:
                return end + 1
            else:
                end += 1
        return end

    code = list(source)
    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = len(source) if end < 0 else end
        elif source.startswith("/*", index):
            depth = 1
            end = index + 2
            while end < len(source) and depth:
                if source.startswith("/*", end):
                    depth += 1
                    end += 2
                elif source.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
        else:
            raw = re.match(r'(?:br|cr|r)(?P<hashes>#{0,})"', source[index:])
            if raw:
                marker = '"' + raw.group("hashes")
                end = source.find(marker, index + raw.end())
                end = len(source) if end < 0 else end + len(marker)
            elif source[index] == '"' or source.startswith(('b"', 'c"'), index):
                start_quote = index if source[index] == '"' else index + 1
                end = quoted_end(start_quote, '"')
            elif source[index] == "'" and index + 2 < len(source) and (
                source[index + 1] == "\\" or source[index + 2] == "'"
            ):
                end = quoted_end(index, "'")
            elif source.startswith("b'", index):
                end = quoted_end(index + 1, "'")
            else:
                index += 1
                continue

        for offset in range(index, end):
            if code[offset] != "\n":
                code[offset] = " "
        index = end
    return "".join(code)


def declared(parent: Path, module: str) -> bool:
    source = parent.read_text()
    code = rust_code_only(source)
    pattern = re.compile(rf"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+{re.escape(module)}\s*;", re.MULTILINE)
    allowed_preambles = {
        "#[cfg(test)]": "#[cfg(test)]",
        "#[cfg(all(test,target_os=))]": '#[cfg(all(test,target_os="linux"))]',
    }
    for match in pattern.finditer(code):
        prefix = code[: match.start()]
        depths = {"(": 0, "[": 0, "{": 0}
        pairs = {")": "(", "]": "[", "}": "{"}
        for char in prefix:
            if char in depths:
                depths[char] += 1
            elif char in pairs:
                depths[pairs[char]] -= 1
        if any(depths.values()):
            continue

        boundary = max(prefix.rfind(";"), prefix.rfind("{"), prefix.rfind("}"))
        code_preamble = re.sub(r"\s+", "", prefix[boundary + 1 :])
        if not code_preamble:
            return True
        if code_preamble in allowed_preambles:
            attr_start = prefix.rfind("#[cfg", boundary + 1)
            if attr_start >= 0 and re.sub(r"\s+", "", source[attr_start : match.start()]) == allowed_preambles[code_preamble]:
                return True
    return False


def module_source(src: Path, directory: Path) -> Path | None:
    if not directory.parts:
        return src / "lib.rs"

    mod_file = src / directory / "mod.rs"
    if mod_file.is_file():
        return mod_file

    sibling = src.joinpath(*directory.parts[:-1], f"{directory.name}.rs")
    return sibling if sibling.is_file() else None


def check_e2e_modules(root: Path) -> list[str]:
    src = root / "crates/e2e_test/src"
    errors: list[str] = []
    for test_file in sorted(src.rglob("*_test.rs")):
        relative = test_file.relative_to(root).as_posix()
        directory = test_file.relative_to(src).parent
        parent = module_source(src, directory)
        if parent is None:
            errors.append(f"{relative}: no canonical parent module")
            continue

        if not declared(parent, test_file.stem):
            errors.append(f"{relative}: not declared by {parent.relative_to(root).as_posix()}")
        while directory.parts:
            module = directory.name
            directory = directory.parent
            parent = module_source(src, directory)
            if parent is None:
                errors.append(f"{relative}: module {module} has no canonical parent")
                break
            if not declared(parent, module):
                errors.append(f"{relative}: module {module} not declared by {parent.relative_to(root).as_posix()}")
    return errors


def check_vault_test_groups(root: Path) -> list[str]:
    src = root / "crates/e2e_test/src"
    vault_modules = {
        path.relative_to(src).with_suffix("").as_posix().replace("/", "::")
        for path in src.rglob("*_test.rs")
        if "VaultTestEnvironment" in rust_code_only(path.read_text())
    }
    config = tomllib.loads((root / ".config/nextest.toml").read_text())
    errors: list[str] = []
    for profile in ("default", "e2e-full"):
        overrides = config.get("profile", {}).get(profile, {}).get("overrides", [])
        filters = "\n".join(
            override.get("filter", "")
            for override in overrides
            if override.get("test-group") == "e2e-vault"
        )
        for module in sorted(vault_modules):
            if f"{module}::" not in filters:
                errors.append(f".config/nextest.toml: profile.{profile} does not serialize Vault module {module}")
    return errors


def check_ilm_build_budget(root: Path) -> list[str]:
    workflow_path = root / ".github/workflows/ci.yml"
    if not workflow_path.is_file():
        return [".github/workflows/ci.yml: missing workflow"]
    workflow = workflow_path.read_text()
    job = re.search(
        r"^  test-ilm-integration-serial:\n(?P<body>.*?)(?=^  [a-z0-9-]+:\n)",
        workflow,
        re.MULTILINE | re.DOTALL,
    )
    if job is None:
        return [".github/workflows/ci.yml: missing test-ilm-integration-serial job"]

    step = re.search(
        r"^      - name: Run ignored ILM integration tests serially\n(?P<body>.*?)(?=^      - name:|\Z)",
        job.group("body"),
        re.MULTILINE | re.DOTALL,
    )
    if step is None:
        return [".github/workflows/ci.yml: missing ignored ILM integration step"]

    budget = (
        "CARGO_BUILD_JOBS: ${{ (github.event_name == 'push' || "
        "github.event_name == 'workflow_dispatch') && '3' || '2' }}"
    )
    if budget not in step.group("body"):
        return [".github/workflows/ci.yml: ILM integration must keep the measured 3/2 Cargo build budget"]
    return []


def check_fuzz_targets(root: Path) -> list[str]:
    manifest = tomllib.loads((root / "fuzz/Cargo.toml").read_text())
    expected = {item["name"] for item in manifest.get("bin", []) if "name" in item}
    errors: list[str] = []
    if not expected:
        return ["fuzz/Cargo.toml: no [[bin]] fuzz targets found"]

    runner = (root / "scripts/fuzz/run.sh").read_text()
    match = re.search(r'^targets="([^"]+)"', runner, re.MULTILINE)
    runner_targets = set(match.group(1).split()) if match else set()
    if runner_targets != expected:
        errors.append(f"scripts/fuzz/run.sh targets {sorted(runner_targets)} != manifest {sorted(expected)}")

    workflow = (root / ".github/workflows/fuzz.yml").read_text()
    matrices = [words(value) for value in re.findall(r"^\s*target:\s*\[([^]]+)]", workflow, re.MULTILINE)]
    if len(matrices) != 2:
        errors.append(f".github/workflows/fuzz.yml: expected smoke and nightly target matrices, found {len(matrices)}")
    for index, matrix in enumerate(matrices, start=1):
        if matrix != expected:
            errors.append(f".github/workflows/fuzz.yml matrix {index} {sorted(matrix)} != manifest {sorted(expected)}")

    runtime_targets = re.findall(r"^\s*FUZZ_TARGET:\s*(\S.*?)\s*$", workflow, re.MULTILINE)
    if runtime_targets != ["${{ matrix.target }}", "${{ matrix.target }}"]:
        errors.append(".github/workflows/fuzz.yml: smoke and nightly jobs must pass matrix.target to FUZZ_TARGET")

    dependency_paths = {
        f"{path.removeprefix('../')}/**"
        for dependency in manifest.get("dependencies", {}).values()
        if isinstance(dependency, dict)
        and isinstance(path := dependency.get("path"), str)
        and path.startswith("../crates/")
    }
    missing_paths = sorted(path for path in dependency_paths if f'"{path}"' not in workflow)
    if missing_paths:
        errors.append(f".github/workflows/fuzz.yml missing direct dependency paths: {', '.join(missing_paths)}")

    staged_matches = re.findall(r"^\s*for target in ([^;]+); do", workflow, re.MULTILINE)
    staged = set(staged_matches[0].split()) if staged_matches else set()
    if staged != expected:
        errors.append(f".github/workflows/fuzz.yml staged binaries {sorted(staged)} != manifest {sorted(expected)}")

    return errors


def check_runner_selection(root: Path) -> list[str]:
    runner = (root / "scripts/run_e2e_tests.sh").read_text()
    errors: list[str] = []
    if "--include-ignored" not in runner:
        errors.append("scripts/run_e2e_tests.sh: runner must include default and ignored tests")
    if "--test-threads=1" not in runner:
        errors.append("scripts/run_e2e_tests.sh: runner must serialize fixed-port protocol tests")
    if re.search(r"(?<!include-)--ignored\b", runner):
        errors.append("scripts/run_e2e_tests.sh: bare --ignored silently excludes default tests")
    if "--exact" in runner:
        errors.append("scripts/run_e2e_tests.sh: --test is documented as a pattern and must not force exact matching")
    if 'eval "$test_cmd"' in runner:
        errors.append("scripts/run_e2e_tests.sh: command construction must not use eval")
    start = re.search(r"start_rustfs\(\) \{(?P<body>.*?)\n\}\n\n# Function to run tests", runner, re.DOTALL)
    start_body = start.group("body") if start else ""
    if '"http://localhost:9000/health/ready"' not in start_body or not re.search(
        r"curl [^\n]*(?:--fail|-f(?:\s|$))", start_body
    ):
        errors.append("scripts/run_e2e_tests.sh: startup must require the ready endpoint to return HTTP success")
    if start_body.count("return 0") != 1 or "nc -z" in start_body:
        errors.append("scripts/run_e2e_tests.sh: startup readiness must not fall back to process or port liveness")
    failed_start = re.search(r"if ! start_rustfs; then(?P<body>.*?)\n\s*fi", runner, re.DOTALL)
    failed_start_commands = (
        [line.strip() for line in failed_start.group("body").splitlines() if line.strip()] if failed_start else []
    )
    if failed_start_commands != ['print_error "Failed to start RustFS properly"', "exit 1"]:
        errors.append("scripts/run_e2e_tests.sh: failed startup must not continue into tests")
    return errors


def check_s3_tests_runner(root: Path) -> list[str]:
    runner = (root / "scripts/s3-tests/run.sh").read_text()
    errors: list[str] = []
    if "--showlocals" in runner:
        errors.append("scripts/s3-tests/run.sh: pytest failure diagnostics must not dump local values")
    readiness = re.search(
        r"test_s3_api_ready\(\) \{(?P<body>.*?)\n\}\n\n# First, wait",
        runner,
        re.DOTALL,
    )
    readiness_body = readiness.group("body") if readiness else ""
    if (
        '"http://${S3_HOST}:${S3_PORT}/health/ready"' not in readiness_body
        or re.search(r"/health(?=[\"'\s])", readiness_body)
        or '[ "${READY_CODE}" != "200" ]' not in readiness_body
    ):
        errors.append("scripts/s3-tests/run.sh: startup must require the ready endpoint to return HTTP 200")
    signed_probe = re.search(
        r"if command -v awscurl\b[^\n]*; then(?P<body>.*?)\n\s*fi\s*"
        r"(?:#[^\n]*\n\s*)*return 0\s*$",
        readiness_body,
        re.DOTALL,
    )
    signed_body = signed_probe.group("body") if signed_probe else ""
    readiness_code = "\n".join(line.split("#", 1)[0] for line in readiness_body.splitlines())
    signed_code = "\n".join(line.split("#", 1)[0] for line in signed_body.splitlines())
    signed_commands = [
        command
        for line in signed_body.splitlines()
        if (command := line.split("#", 1)[0].strip())
    ]
    signed_success = re.search(
        r'if echo "\$\{RESPONSE\}" \| grep -q "<ListAllMyBucketsResult"; then\s*'
        r"(?:#[^\n]*\n\s*)*return 0\s*\n\s*fi",
        signed_body,
    )
    response_capture = re.search(
        r"^\s*RESPONSE=\$\((?P<command>.*?)\)\s*$",
        signed_code,
        re.DOTALL | re.MULTILINE,
    )
    response_command = response_capture.group("command") if response_capture else ""
    response_command = response_command.replace("\\\n", " ").strip()
    response_operators = re.search(
        r";|\|\||&&|(?<![>|])\|(?!\|)|(?<![>&])&(?![>&0-9])|\$\(|[<>]\(|`",
        response_command,
    )
    if (
        len(re.findall(r"\breturn\s+0\b", readiness_code)) != 2
        or len(re.findall(r"\breturn\s+0\b", signed_code)) != 1
        or len(re.findall(r"(?<![A-Za-z0-9_])RESPONSE=", signed_code)) != 1
        or not response_command.startswith("awscurl ")
        or "\n" in response_command
        or response_operators
        or not signed_success
        or not signed_commands
        or signed_commands[-1] != "return 1"
    ):
        errors.append("scripts/s3-tests/run.sh: readiness must bind success to the signed probe")
    return errors


def check_workflow_readiness(root: Path) -> list[str]:
    errors: list[str] = []
    for relative in (".github/workflows/e2e-s3tests.yml", ".github/workflows/mint.yml"):
        path = root / relative
        try:
            lines = path.read_text().splitlines()
        except FileNotFoundError:
            errors.append(f"{relative}: missing workflow")
            continue
        start = next(
            (index for index, line in enumerate(lines) if line.strip() == "- name: Wait for RustFS ready"),
            None,
        )
        if start is None:
            errors.append(f"{relative}: missing RustFS readiness step")
            continue
        indent = len(lines[start]) - len(lines[start].lstrip())
        end = next(
            (
                index
                for index in range(start + 1, len(lines))
                if lines[index].strip().startswith("- name:")
                and len(lines[index]) - len(lines[index].lstrip()) <= indent
            ),
            len(lines),
        )
        readiness_step = "\n".join(lines[start:end])
        ready_branch = re.search(
            r"if curl [^\n]*/health/ready[^\n]*; then(?P<body>.*?)\n\s*fi",
            readiness_step,
            re.DOTALL,
        )
        ready_body = ready_branch.group("body") if ready_branch else ""
        ready_condition = ready_branch.group(0).splitlines()[0].rsplit("; then", 1)[0] if ready_branch else ""
        step_commands = [
            command
            for line in readiness_step.splitlines()
            if (command := line.split("#", 1)[0].strip())
        ]
        step_code = "\n".join(step_commands)
        ready_code = "\n".join(line.split("#", 1)[0] for line in ready_body.splitlines())
        if (
            "/health/ready" not in readiness_step
            or re.search(r"/health(?=[\"'\s])", readiness_step)
            or not re.search(r"curl [^\n]*(?:-sf|-fs|--fail)", readiness_step)
            or not ready_branch
            or re.search(r"\|\||&&|(?<![>|])\|(?!\|)|;|(?<![>&])&(?![>&])", ready_condition)
            or len(re.findall(r"\bexit\s+0\b", step_code)) != 1
            or not re.search(r"\bexit\s+0\b", ready_code)
            or not step_commands
            or step_commands[-1] != "exit 1"
        ):
            errors.append(f"{relative}: RustFS readiness step must fail closed on /health/ready")
    return errors


def profile_selection_entries(root: Path, profile: str) -> tuple[Path, list[str], dict[str, str]]:
    if not re.fullmatch(r"e2e-[a-z0-9-]+", profile):
        raise ValueError(f"invalid e2e profile name: {profile}")
    path = root / f".config/{profile}-selection.txt"
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    values = dict(line.split("=", 1) for line in lines if "=" in line)
    if len(values) != len(lines) or any(not re.fullmatch(r"sha256(?:-[a-z0-9]+)?", key) for key in values):
        raise ValueError(f"{path.relative_to(root).as_posix()}: invalid sha256 entry")
    return path, lines, values


def profile_selection(root: Path, profile: str) -> str:
    path, _, values = profile_selection_entries(root, profile)
    key = f"sha256-{sys.platform}"
    digest = values.get(key, values.get("sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"{path.relative_to(root).as_posix()}: missing sha256 for {sys.platform}")
    return digest


def profile_listing_digest(listing: Path) -> tuple[int, str]:
    data = json.loads(listing.read_text())
    selected = sorted(
        f"{suite_id}::{test_name}"
        for suite_id, suite in data["rust-suites"].items()
        for test_name, testcase in suite["testcases"].items()
        if testcase.get("filter-match", {}).get("status") == "matches"
    )
    return len(selected), hashlib.sha256(("\n".join(selected) + "\n").encode()).hexdigest()


def update_profile_selection(root: Path, profile: str, listing: Path, platform: str) -> tuple[int, str, str]:
    if not re.fullmatch(r"[a-z0-9]+", platform):
        raise ValueError(f"invalid platform name: {platform}")
    path, lines, values = profile_selection_entries(root, profile)
    key = "sha256" if "sha256" in values else f"sha256-{platform}"
    if key not in values:
        raise ValueError(f"{path.relative_to(root).as_posix()}: missing {key} entry")
    count, digest = profile_listing_digest(listing)
    path.write_text("\n".join(f"{key}={digest}" if line.startswith(f"{key}=") else line for line in lines) + "\n")
    return count, digest, key


def check_profile_definitions(root: Path) -> list[str]:
    config = tomllib.loads((root / ".config/nextest.toml").read_text())
    profiles = {
        profile
        for profile in config.get("profile", {})
        if profile.startswith("e2e-")
    }
    selection_profiles = {
        path.name.removesuffix("-selection.txt") for path in (root / ".config").glob("e2e-*-selection.txt")
    }
    errors: list[str] = []
    for profile in sorted(profiles | selection_profiles):
        if profile not in profiles:
            errors.append(f".config/nextest.toml: missing profile.{profile}")
        if profile not in selection_profiles:
            errors.append(f".config/{profile}-selection.txt: missing expected profile selection")
            continue
        try:
            profile_selection(root, profile)
        except (FileNotFoundError, ValueError) as error:
            errors.append(str(error))
    return errors


def yaml_block(lines: list[str], key: str, indent: int) -> list[str] | None:
    try:
        start = lines.index(f"{' ' * indent}{key}:") + 1
    except ValueError:
        return None
    end = next(
        (
            index
            for index in range(start, len(lines))
            if lines[index].strip()
            and not lines[index].lstrip().startswith("#")
            and len(lines[index]) - len(lines[index].lstrip()) <= indent
        ),
        len(lines),
    )
    return lines[start:end]


def workflow_step_block(
    job_lines: list[str], value: str, key: str = "uses", indent: int = 6
) -> tuple[int, list[str]] | None:
    uses_index = next(
        (
            index
            for index, line in enumerate(job_lines)
            if (
                line.split("#", 1)[0].strip() == f"- {key}: {value}"
                and len(line) - len(line.lstrip()) == indent
            )
            or (
                line.split("#", 1)[0].strip() == f"{key}: {value}"
                and len(line) - len(line.lstrip()) == indent + 2
            )
        ),
        None,
    )
    if uses_index is None:
        return None
    start = next(
        (
            index
            for index in range(uses_index, -1, -1)
            if job_lines[index].lstrip().startswith("- ")
        ),
        uses_index,
    )
    indent = len(job_lines[start]) - len(job_lines[start].lstrip())
    end = next(
        (
            index
            for index in range(start + 1, len(job_lines))
            if len(job_lines[index]) - len(job_lines[index].lstrip()) == indent
            and job_lines[index].lstrip().startswith("- ")
        ),
        len(job_lines),
    )
    return start, job_lines[start:end]


def yaml_scalar_continues(lines: list[str], index: int, indent: int) -> bool:
    following = next(
        (line for line in lines[index + 1:] if line.strip() and not line.lstrip().startswith("#")), None
    )
    return following is not None and len(following) - len(following.lstrip()) > indent


def check_quick_checks(root: Path) -> list[str]:
    errors: list[str] = []
    bypass_key = r'''(?:if|continue-on-error|needs|"if"|"continue-on-error"|"needs"|'if'|'continue-on-error'|'needs')\s*:'''
    for name in ("ci.yml",):
        relative = f".github/workflows/{name}"
        path = root / relative
        job = yaml_block(path.read_text().splitlines(), "quick-checks", 2) if path.is_file() else None
        if job is None:
            errors.append(f"{relative}: missing Quick Checks job")
            continue
        conditions = [index for index, line in enumerate(job) if re.match(rf"^    {bypass_key}", line)]
        expected = ["if: github.event_name != 'pull_request' || github.event.action != 'closed'"] if name == "ci.yml" else []
        if [job[index].strip() for index in conditions] != expected or any(
            yaml_scalar_continues(job, index, 4) for index in conditions
        ):
            errors.append(f"{relative}: Quick Checks job must not add dependencies, bypass failures, or change its event condition")
        checkout = workflow_step_block(job, "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0")
        action = workflow_step_block(job, "./.github/actions/quick-checks")
        if checkout is None or action is None:
            errors.append(f"{relative}: Quick Checks requires checkout and the shared quick-checks action")
            continue
        if checkout[0] >= action[0]:
            errors.append(f"{relative}: checkout must run before shared Quick Checks")
        if "          persist-credentials: false" not in checkout[1]:
            errors.append(f"{relative}: Quick Checks checkout must disable persisted credentials")
        for step in (checkout, action):
            if any(re.match(rf"^\s+(?:- )?{bypass_key}", line) for line in step[1]):
                errors.append(f"{relative}: Quick Checks checkout and shared action must run without bypasses")

    relative = ".github/actions/quick-checks/action.yml"
    path = root / relative
    runs = yaml_block(path.read_text().splitlines(), "runs", 0) if path.is_file() else None
    if runs is None or "  using: composite" not in runs:
        errors.append(f"{relative}: missing composite action")
        return errors
    steps = yaml_block(runs, "steps", 2) or []
    for command in ("shellcheck --version && actionlint", "./scripts/check_error_other_format_ratchet.sh", "make script-tests"):
        step = workflow_step_block(steps, command, key="run", indent=4)
        if step is None:
            errors.append(f"{relative}: missing direct execution of {command}")
            continue
        if "      shell: bash" not in step[1] or any(
            re.match(rf"^\s+(?:- )?{bypass_key}", line) for line in step[1]
        ):
            errors.append(f"{relative}: {command} must use bash without a condition or continue-on-error")
        run_index = next(
            index for index, line in enumerate(step[1])
            if line.split("#", 1)[0].rstrip() in (f"      run: {command}", f"    - run: {command}")
        )
        if yaml_scalar_continues(step[1], run_index, 6):
            errors.append(f"{relative}: {command} must remain a single-line run scalar")
    return errors


def alert_step_errors(
    job_lines: list[str],
    expected_action_if: str | None,
    required_permissions: tuple[str, ...],
    required_action_tokens: tuple[str, ...],
) -> list[str]:
    checkout = workflow_step_block(job_lines, "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0")
    action = workflow_step_block(job_lines, "./.github/actions/schedule-failure-issue")
    errors: list[str] = []
    permissions = yaml_block(job_lines, "permissions", 4)
    permission_text = "\n".join(line.split("#", 1)[0] for line in permissions or [])
    missing_permissions = [token for token in required_permissions if token not in permission_text]
    if missing_permissions:
        errors.append("alert job permissions missing " + ", ".join(missing_permissions))
    if checkout is None:
        errors.append("checkout step is missing")
    if action is None:
        errors.append("local alert action step is missing")
    if checkout is None or action is None:
        return errors

    if checkout[0] >= action[0]:
        errors.append("checkout must run before the local alert action")
    checkout_ifs = [line.strip() for line in checkout[1] if line.strip().startswith("if:")]
    if checkout_ifs:
        errors.append("checkout step must not be conditional")
    action_ifs = [line.strip() for line in action[1] if line.strip().startswith("if:")]
    expected_ifs = [] if expected_action_if is None else [expected_action_if]
    if action_ifs != expected_ifs:
        errors.append("alert action has an invalid step condition")
    action_text = "\n".join(line.split("#", 1)[0] for line in action[1])
    missing_action_tokens = [token for token in required_action_tokens if token not in action_text]
    if missing_action_tokens:
        errors.append("alert action inputs missing " + ", ".join(missing_action_tokens))
    return errors


def schedule_utc_slots(hour: int, minute: int, timezone_name: str | None) -> set[tuple[int, int]]:
    if timezone_name is None:
        return {(hour, minute)}
    zone = ZoneInfo(timezone_name)
    return {
        (utc.hour, utc.minute)
        for year in (2025, 2026)
        for month in range(1, 13)
        for utc in [datetime(year, month, 1, hour, minute, tzinfo=zone).astimezone(timezone.utc)]
    }


def check_scheduled_alerts(root: Path) -> list[str]:
    errors: list[str] = []
    schedule_slots: dict[tuple[int, int], list[str]] = {}
    for relative in SCHEDULED_ALERT_WORKFLOWS:
        path = root / relative
        try:
            lines = path.read_text().splitlines()
        except FileNotFoundError:
            errors.append(f"{relative}: missing scheduled validation workflow")
            continue

        on_block = yaml_block(lines, "on", 0)
        schedule_block = yaml_block(on_block or [], "schedule", 2)
        schedule_lines = schedule_block or []
        cron_indices = [index for index, line in enumerate(schedule_lines) if re.match(r"^\s*-\s+cron:", line)]
        if not cron_indices:
            errors.append(f"{relative}: missing simple numeric schedule")
        else:
            for position, cron_index in enumerate(cron_indices):
                cron_line = schedule_lines[cron_index]
                schedule = re.match(r"^\s*-\s+cron:\s*[\"']?(\d+)\s+(\d+)\s+", cron_line)
                if not schedule:
                    errors.append(f"{relative}: missing simple numeric schedule")
                    continue
                minute, hour = map(int, schedule.groups())
                if minute == 0:
                    errors.append(f"{relative}: scheduled validation must avoid minute zero")
                entry_end = cron_indices[position + 1] if position + 1 < len(cron_indices) else len(schedule_lines)
                entry = "\n".join(schedule_lines[cron_index + 1 : entry_end])
                timezone_match = re.search(r"^\s*timezone:\s*[\"']?([^\"'\s]+)", entry, re.MULTILINE)
                timezone_name = timezone_match.group(1) if timezone_match else None
                try:
                    utc_slots = schedule_utc_slots(hour, minute, timezone_name)
                except ZoneInfoNotFoundError:
                    errors.append(f"{relative}: unknown schedule timezone {timezone_name}")
                    continue
                for slot in utc_slots:
                    schedule_slots.setdefault(slot, []).append(relative)

        job_lines = yaml_block(lines, "alert-on-failure", 2)
        if job_lines is None:
            errors.append(f"{relative}: missing alert-on-failure job")
            continue
        job = "\n".join(line.split("#", 1)[0] for line in job_lines)
        required = (
            "always()",
            "github.event_name == 'schedule'",
            "contains(needs.*.result, 'failure')",
            "issues: write",
            "uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
            "uses: ./.github/actions/schedule-failure-issue",
            "github-token: ${{ secrets.GITHUB_TOKEN }}",
        )
        missing = [token for token in required if token not in job]
        if missing:
            errors.append(f"{relative}: alert-on-failure missing {', '.join(missing)}")
        else:
            errors.extend(
                f"{relative}: {error}"
                for error in alert_step_errors(job_lines, None, ("issues: write",), ("github-token: ${{ secrets.GITHUB_TOKEN }}",))
            )

    for (hour, minute), workflows in schedule_slots.items():
        if len(workflows) > 1:
            errors.append(
                f"scheduled validations share {hour:02d}:{minute:02d} UTC: {', '.join(workflows)}"
            )

    watchdog_path = root / ".github/workflows/scheduled-validation-watchdog.yml"
    try:
        watchdog_lines = watchdog_path.read_text().splitlines()
    except FileNotFoundError:
        errors.append(".github/workflows/scheduled-validation-watchdog.yml: missing completion watchdog")
        return errors
    watchdog_on = yaml_block(watchdog_lines, "on", 0)
    watchdog_run = yaml_block(watchdog_on or [], "workflow_run", 2)
    watchdog_workflows = yaml_block(watchdog_run or [], "workflows", 4)
    if watchdog_workflows is None:
        errors.append(".github/workflows/scheduled-validation-watchdog.yml: missing workflow_run workflows")
        return errors
    watchdog_sources = "\n".join(line.split("#", 1)[0] for line in watchdog_workflows)
    for relative in SCHEDULED_ALERT_WORKFLOWS:
        path = root / relative
        if not path.is_file():
            continue
        source = path.read_text()
        match = re.search(r"^name:\s*[\"']?([^\"'\n]+)", source, re.MULTILINE)
        if not match:
            errors.append(f"{relative}: missing workflow name")
        elif f'- "{match.group(1).strip()}"' not in watchdog_sources:
            errors.append(f"{relative}: missing from scheduled completion watchdog")
    watchdog_job_lines = yaml_block(watchdog_lines, "alert-on-incomplete-run", 2)
    if watchdog_job_lines is None:
        errors.append(".github/workflows/scheduled-validation-watchdog.yml: missing alert-on-incomplete-run job")
        return errors
    watchdog_job = "\n".join(line.split("#", 1)[0] for line in watchdog_job_lines)
    required = (
        "github.event.workflow_run.event == 'schedule'",
        "github.event.workflow_run.conclusion != 'success'",
        "github.event.workflow_run.conclusion != 'failure'",
        "actions: read",
        "issues: write",
        "uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "uses: ./.github/actions/schedule-failure-issue",
        "github-token: ${{ secrets.GITHUB_TOKEN }}",
        "workflow-name: ${{ github.event.workflow_run.name }}",
        "source-run-id: ${{ github.event.workflow_run.id }}",
        "source-run-attempt: ${{ github.event.workflow_run.run_attempt }}",
        "source-event: ${{ github.event.workflow_run.event }}",
        "source-ref-name: ${{ github.event.workflow_run.head_branch }}",
        "source-sha: ${{ github.event.workflow_run.head_sha }}",
    )
    missing = [token for token in required if token not in watchdog_job]
    if missing:
        errors.append(
            ".github/workflows/scheduled-validation-watchdog.yml: missing " + ", ".join(missing)
        )
    else:
        errors.extend(
            ".github/workflows/scheduled-validation-watchdog.yml: " + error
            for error in alert_step_errors(
                watchdog_job_lines,
                None,
                ("actions: read", "issues: write"),
                (
                    "github-token: ${{ secrets.GITHUB_TOKEN }}",
                    "workflow-name: ${{ github.event.workflow_run.name }}",
                    "source-run-id: ${{ github.event.workflow_run.id }}",
                    "source-run-attempt: ${{ github.event.workflow_run.run_attempt }}",
                    "source-event: ${{ github.event.workflow_run.event }}",
                    "source-ref-name: ${{ github.event.workflow_run.head_branch }}",
                    "source-sha: ${{ github.event.workflow_run.head_sha }}",
                ),
            )
        )

    freshness_path = root / ".github/workflows/scheduled-validation-freshness.yml"
    try:
        freshness_lines = freshness_path.read_text().splitlines()
    except FileNotFoundError:
        errors.append(".github/workflows/scheduled-validation-freshness.yml: missing freshness check")
        return errors
    freshness_job_lines = yaml_block(freshness_lines, "check-freshness", 2)
    if freshness_job_lines is None:
        errors.append(".github/workflows/scheduled-validation-freshness.yml: missing check-freshness job")
        return errors
    freshness_job = "\n".join(line.split("#", 1)[0] for line in freshness_job_lines)
    required = (
        "python3 scripts/check_scheduled_validation_freshness.py",
        "actions: read",
        "issues: write",
        "if: failure()",
        "uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "uses: ./.github/actions/schedule-failure-issue",
        "github-token: ${{ secrets.GITHUB_TOKEN }}",
        "details-file: ${{ runner.temp }}/scheduled-validation-freshness.md",
    )
    missing = [token for token in required if token not in freshness_job]
    if missing:
        errors.append(
            ".github/workflows/scheduled-validation-freshness.yml: missing " + ", ".join(missing)
        )
    else:
        errors.extend(
            ".github/workflows/scheduled-validation-freshness.yml: " + error
            for error in alert_step_errors(
                freshness_job_lines,
                "if: failure()",
                ("actions: read", "issues: write"),
                (
                    "github-token: ${{ secrets.GITHUB_TOKEN }}",
                    "details-file: ${{ runner.temp }}/scheduled-validation-freshness.md",
                ),
            )
        )
    if not (root / "scripts/check_scheduled_validation_freshness.py").is_file():
        errors.append("scripts/check_scheduled_validation_freshness.py: missing freshness checker")
    return errors


def check_profile_listing(root: Path, profile: str, listing: Path) -> list[str]:
    try:
        expected_digest = profile_selection(root, profile)
        count, digest = profile_listing_digest(listing)
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        return [f"cannot read {profile} nextest listing: {error}"]
    if digest != expected_digest:
        return [
            f"{profile} selection changed: count={count} sha256={digest}; "
            f"expected sha256={expected_digest}"
        ]
    print(f"{profile} selection OK: {count} tests, sha256={digest}")
    return []


def core_requirements(root: Path) -> dict:
    data = json.loads((root / ".config/ecstore-required-tests.json").read_text())
    if not data["tests"] or not data["fixtures"]:
        raise ValueError("core test and fixture requirements must not be empty")
    identities = [(test["suite"], test["name"]) for test in data["tests"]]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate core test requirement")
    return data


def check_core_fixtures(root: Path) -> list[str]:
    try:
        fixtures = core_requirements(root)["fixtures"]
        errors = []
        for fixture in fixtures:
            path = (root / fixture["path"]).resolve()
            if not path.is_relative_to(root.resolve()):
                raise ValueError("core fixture path escapes repository")
            if not path.is_file():
                errors.append(f"{fixture['path']}: required core fixture missing")
            elif hashlib.sha256(path.read_bytes()).hexdigest() != fixture["sha256"]:
                errors.append(f"{fixture['path']}: core fixture sha256 mismatch")
        return errors
    except (OSError, KeyError, TypeError, ValueError) as error:
        return [f"cannot validate core fixtures: {error}"]


def check_core_listing(root: Path, listing: Path) -> list[str]:
    """Check the existing CI run's selection, not a second filtered test run."""
    try:
        required = core_requirements(root)["tests"]
        suites = json.loads(listing.read_text())["rust-suites"]
        if not isinstance(suites, dict):
            raise ValueError("rust-suites must be an object")
        errors = check_core_fixtures(root)
        for test in required:
            testcase = suites.get(test["suite"], {}).get("testcases", {}).get(test["name"], {})
            if testcase.get("ignored") is not False or testcase.get("filter-match", {}).get("status") != "matches":
                errors.append(f"{test['invariant']}: required test not selected: {test['suite']}::{test['name']}")
        return errors
    except (OSError, KeyError, TypeError, ValueError) as error:
        return [f"cannot read core nextest listing: {error}"]


def evidence_integer(value: object, name: str, minimum: int, maximum: int) -> int:
    require(type(value) is int and minimum <= value <= maximum, f"invalid integer {name}")
    return value


def evidence_string(value: object, name: str, pattern: str | None = None) -> str:
    require(isinstance(value, str) and value.strip(), f"invalid string {name}")
    if pattern is not None:
        require(re.fullmatch(pattern, value) is not None, f"invalid string {name}")
    return value


def evidence_timestamp(value: object, name: str) -> datetime:
    text = evidence_string(value, name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"invalid timestamp {name}") from error
    require(parsed.tzinfo is not None, f"{name} must include timezone")
    return parsed


def evidence_string_list(value: object, name: str) -> list[str]:
    require(
        isinstance(value, list) and value and all(isinstance(item, str) and item.strip() for item in value),
        f"{name} must be a non-empty string list",
    )
    return value


def evidence_exact_strings(value: object, expected: tuple[str, ...], name: str) -> list[str]:
    strings = evidence_string_list(value, name)
    observed = set(strings)
    require(len(observed) == len(strings), f"duplicate {name}")
    missing = sorted(set(expected) - observed)
    require(not missing, f"missing {name}: {', '.join(missing)}")
    unknown = sorted(observed - set(expected))
    require(not unknown, f"unknown {name}: {', '.join(unknown)}")
    return strings


def scanner_heal_registry_schema(registry: dict[str, object]) -> int:
    return evidence_integer(registry.get("schema"), "registry schema", 1, SCANNER_HEAL_REGISTRY_SCHEMA_MAX)


def scanner_heal_oracle_names(root: Path) -> tuple[str, ...]:
    registry = read_json(root / ".config/scanner-heal-required-tests.json")
    scanner_heal_registry_schema(registry)
    cases = registry.get("cases")
    require(isinstance(cases, dict) and cases, "invalid scanner/heal registry")
    names = set()
    for case_id, requirement in cases.items():
        require(isinstance(case_id, str) and case_id, "invalid scanner/heal case identity")
        lane = requirement.get("lane")
        require(isinstance(lane, str) and re.fullmatch(r"[a-z0-9-]+", lane) is not None,
                f"invalid nextest profile lane for {case_id}")
        oracle = requirement.get("oracle")
        require(isinstance(oracle, str) and oracle.endswith(".json"), f"invalid oracle for {case_id}")
        path = Path(oracle)
        require(not path.is_absolute() and ".." not in path.parts, f"oracle path escapes run directory for {case_id}")
        require(requirement.get("evidence") in ("process-restart", "process-crash-restart"),
                f"invalid evidence for {case_id}")
        require(type(requirement.get("unclean_shutdown_marker")) is bool,
                f"invalid unclean-shutdown marker expectation for {case_id}")
        if "erasure" in requirement:
            erasure = requirement["erasure"]
            require(isinstance(erasure, dict), f"invalid erasure expectation for {case_id}")
            data_blocks = evidence_integer(erasure.get("data_blocks"), f"{case_id} data_blocks", 1, 16)
            parity_blocks = evidence_integer(erasure.get("parity_blocks"), f"{case_id} parity_blocks", 1, 16)
            require(data_blocks >= parity_blocks, f"invalid erasure geometry for {case_id}")
            require(data_blocks + parity_blocks == requirement["topology"]["nodes"] * requirement["topology"]["drives_per_node"],
                    f"erasure geometry differs from topology for {case_id}")
        if "erasure_set_drive_count" in requirement:
            erasure_set_drive_count = evidence_integer(requirement.get("erasure_set_drive_count"),
                                                       f"{case_id} erasure_set_drive_count", 1, 64)
            require(erasure_set_drive_count == requirement["topology"]["nodes"] * requirement["topology"]["drives_per_node"],
                    f"erasure set drive count differs from topology for {case_id}")
        names.add(oracle)
    return tuple(sorted(names))


def scanner_heal_release_requirements(registry: dict[str, object]) -> tuple[dict[str, dict[str, object]], bool, list[str]]:
    schema = scanner_heal_registry_schema(registry)
    if schema == 1:
        pending = registry.get("release_pending")
        require(isinstance(pending, dict), "invalid scanner/heal release requirements")
        requirements = {}
        for gate, reason in pending.items():
            require(isinstance(gate, str) and re.fullmatch(r"[A-Z][A-Z0-9-]*", gate) is not None,
                    "invalid scanner/heal release gate")
            require(isinstance(reason, str) and reason.strip(), f"missing release requirement for {gate}")
            requirements[gate] = {"gate": gate, "status": "pending", "lane": "schema-1-pending",
                                  "description": reason, "requires": [reason]}
        return requirements, False, ["schema-1-pending"] if requirements else []

    lanes = registry.get("release_lanes")
    cases = registry.get("cases")
    require(isinstance(cases, dict) and cases, "invalid scanner/heal registry")
    require(isinstance(lanes, dict) and lanes, "invalid scanner/heal release lanes")
    lane_statuses = {}
    lane_gates = {}
    for lane_id, lane in lanes.items():
        require(isinstance(lane_id, str) and re.fullmatch(r"[a-z0-9-]+", lane_id) is not None,
                "invalid scanner/heal release lane")
        require(isinstance(lane, dict), f"invalid release lane {lane_id}")
        status = lane.get("status")
        require(status in ("implemented", "pending"), f"invalid release lane status for {lane_id}")
        lane_statuses[lane_id] = status
        if status == "implemented":
            lane_cases = lane.get("cases")
            require(isinstance(lane_cases, list) and lane_cases and all(isinstance(case, str) and case for case in lane_cases),
                    f"implemented release lane {lane_id} has no cases")
            require(all(case in cases for case in lane_cases), f"implemented release lane {lane_id} has unknown cases")
        else:
            gates = lane.get("gates")
            require(isinstance(gates, list) and gates and all(isinstance(gate, str) and gate for gate in gates),
                    f"pending release lane {lane_id} has no gates")
            lane_gates[lane_id] = set(gates)

    raw_requirements = registry.get("release_requirements")
    require(isinstance(raw_requirements, list) and raw_requirements, "invalid scanner/heal release requirements")
    requirements: dict[str, dict[str, object]] = {}
    for item in raw_requirements:
        require(isinstance(item, dict), "invalid scanner/heal release requirement")
        gate = item.get("gate")
        require(isinstance(gate, str) and re.fullmatch(r"[A-Z][A-Z0-9-]*", gate) is not None,
                "invalid scanner/heal release gate")
        require(gate not in requirements, f"duplicate scanner/heal release gate {gate}")
        status = item.get("status")
        require(status == "pending", f"release gate {gate} must stay pending until real evidence is registered")
        lane = item.get("lane")
        require(isinstance(lane, str) and lane in lane_statuses, f"unknown release lane for {gate}")
        require(lane_statuses[lane] == "pending", f"pending release gate {gate} mapped to non-pending lane {lane}")
        description = item.get("description")
        require(isinstance(description, str) and description.strip(), f"missing release requirement for {gate}")
        requires = item.get("requires")
        require(isinstance(requires, list) and requires and
                all(isinstance(requirement, str) and requirement.strip() for requirement in requires),
                f"missing concrete evidence requirements for {gate}")
        evidence_fields = item.get("evidence_fields", [])
        require(isinstance(evidence_fields, list) and
                all(isinstance(field, str) and re.fullmatch(r"[a-z0-9][a-z0-9_]*", field) is not None
                    for field in evidence_fields),
                f"invalid evidence fields for release gate {gate}")
        required_fields = set(SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS.get(gate, ()))
        missing_fields = sorted(required_fields - set(evidence_fields))
        require(not missing_fields,
                f"release gate {gate} missing required evidence fields: {', '.join(missing_fields)}")
        requirements[gate] = item

    missing = sorted(set(SCANNER_HEAL_RELEASE_REQUIRED_GATES) - set(requirements))
    require(not missing, f"missing scanner/heal release requirements: {', '.join(missing)}")
    for lane, gates in lane_gates.items():
        unknown = sorted(gates - set(requirements))
        require(not unknown, f"pending release lane {lane} has unknown gates: {', '.join(unknown)}")
        mapped = {gate for gate, requirement in requirements.items() if requirement["lane"] == lane}
        require(gates == mapped, f"pending release lane {lane} gates do not match release requirements")
    pending_lanes = sorted(lane for lane, status in lane_statuses.items() if status == "pending")
    return requirements, True, pending_lanes


def begin_scanner_heal_receipt(root: Path, directory: Path, binary: Path, test_binary: Path) -> None:
    """Record an existing build; this command never builds or runs a test."""
    require(not directory.exists(), "scanner/heal run directory must be new")
    require(not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip(),
            "commit tracked source changes before creating evidence")
    builds = {}
    for label, path in (("binary", binary), ("test_binary", test_binary)):
        path = path.resolve(strict=True)
        require(path.is_file() and os.access(path, os.X_OK), f"missing executable {label}")
        builds[label] = {"path": str(path), "sha256": digest(path)}
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    require(re.fullmatch(r"[0-9a-f]{40}", revision), "invalid source revision")
    version = subprocess.check_output([builds["binary"]["path"], "--version"], text=True, timeout=30)
    embedded_revision = re.search(r"^git commit\s*:\s*([0-9a-f]{40})\s*$", version, re.MULTILINE)
    embedded_status = re.search(r"^git status\s*:\s*(.*)\Z", version, re.MULTILINE | re.DOTALL)
    require(embedded_revision is not None and embedded_revision[1] == revision, "server binary source revision mismatch")
    require(embedded_status is not None and not embedded_status[1].strip(), "server binary was built from dirty/unknown source")
    lock_blob = subprocess.check_output(["git", "hash-object", "Cargo.lock"], cwd=root, text=True).strip()
    require(re.fullmatch(r"[0-9a-f]{40}", lock_blob), "invalid Cargo.lock identity")
    features = os.environ.get("RUSTFS_E2E_EXPECTED_FEATURES")
    require(features is not None, "set RUSTFS_E2E_EXPECTED_FEATURES to the compiled e2e crate feature set")
    features = ",".join(sorted(set(filter(None, (feature.strip() for feature in features.split(","))))))
    require(all(re.fullmatch(r"[a-z0-9-]+", feature) for feature in features.split(",") if feature), "invalid expected features")
    directory.mkdir(parents=True)
    write_json(directory / "run.json", {"schema": 1, "run_id": uuid.uuid4().hex,
                                       "source_revision": revision,
                                       "binary_source_revision": embedded_revision[1],
                                       "test_build": {"source_revision": revision, "dirty": False,
                                                      "lock_blob": lock_blob, "features": features},
                                       "started_at": datetime.now(timezone.utc).timestamp(), **builds})


def finish_scanner_heal_receipt(directory: Path, exit_code: int, root: Path = ROOT) -> None:
    require(type(exit_code) is int and 0 <= exit_code <= 255, "invalid test exit code")
    require(not (directory / "execution.json").exists(), "execution receipt already exists")
    run = read_json(directory / "run.json")
    artifacts = {}
    oracle_names = scanner_heal_oracle_names(root)
    for name in ("listing.json", "junit.xml"):
        path = directory / name
        if exit_code != 0 and not path.exists():
            continue
        require(path.is_file() and 0 < path.stat().st_size <= MAX_JSON_BYTES, f"missing/oversized {name}")
        require(path.stat().st_mtime >= run["started_at"], f"stale {name}")
        artifacts[name] = digest(path)
    for name in oracle_names:
        path = directory / name
        if not path.exists():
            continue
        require(path.is_file() and 0 < path.stat().st_size <= MAX_JSON_BYTES, f"missing/oversized {name}")
        require(path.stat().st_mtime >= run["started_at"], f"stale {name}")
        artifacts[name] = digest(path)
    if exit_code == 0:
        require(any(name in artifacts for name in oracle_names), "missing scanner/heal case oracle")
    write_json(directory / "execution.json", {"run_id": run["run_id"], "exit_code": exit_code,
                                             "finished_at": datetime.now(timezone.utc).timestamp(),
                                             "artifacts": artifacts})


def check_scanner_heal_evidence(root: Path, directory: Path, case_id: str) -> list[str]:
    """Validate one actual case, or fail the release while required lanes are pending."""
    try:
        registry = read_json(root / ".config/scanner-heal-required-tests.json")
        scanner_heal_registry_schema(registry)
        require(registry.get("cases"), "invalid scanner/heal registry")
        selected = registry["cases"] if case_id == "release" else {case_id: registry["cases"][case_id]}
        run = read_json(directory / "run.json")
        execution = read_json(directory / "execution.json")
        evidence_integer(run.get("schema"), "run schema", 1, 1)
        require(re.fullmatch(r"[0-9a-f]{32}", run["run_id"]), "invalid run identity")
        require(re.fullmatch(r"[0-9a-f]{40}", run["source_revision"]), "invalid source revision")
        require(run.get("binary_source_revision") == run["source_revision"], "server source provenance missing")
        expected_build = run["test_build"]
        require(expected_build["source_revision"] == run["source_revision"] and expected_build["dirty"] is False,
                "test source provenance missing")
        require(re.fullmatch(r"[0-9a-f]{40}", expected_build["lock_blob"]), "invalid test lockfile identity")
        require(isinstance(expected_build["features"], str), "missing test features")
        require(execution.get("run_id") == run["run_id"], "execution belongs to another run")
        require(type(execution.get("exit_code")) is int and execution["exit_code"] == 0, "test command failed or did not run")
        number(run["started_at"], "started_at", 1)
        number(execution["finished_at"], "finished_at", run["started_at"])
        for label in ("binary", "test_binary"):
            require(sha(run[label]["sha256"]) and digest(Path(run[label]["path"])) == run[label]["sha256"],
                    f"{label} changed or missing")
        for name in ("listing.json", "junit.xml"):
            path = directory / name
            require(0 < path.stat().st_size <= MAX_JSON_BYTES, f"missing/oversized {name}")
            require(run["started_at"] <= path.stat().st_mtime <= execution["finished_at"], f"{name} outside run window")
            require(digest(path) == execution["artifacts"][name], f"{name} hash mismatch")
        suites = read_json(directory / "listing.json")["rust-suites"]
        xml = (directory / "junit.xml").read_bytes()
        require(b"<!DOCTYPE" not in xml and b"<!ENTITY" not in xml, "JUnit entities are forbidden")
        junit = ET.fromstring(xml)
        cases = list(junit.iter("testcase"))
        require(bool(cases), "JUnit has zero testcases")
        for case in cases:
            require(not any(child.tag in ("failure", "error", "skipped", "rerunFailure", "rerunError", "flakyFailure", "flakyError")
                            for child in case), "JUnit contains failed, skipped or retried tests")
        errors = []
        for name, requirement in selected.items():
            suite, test = requirement["suite"], requirement["name"]
            for key in ("nodes", "drives_per_node"):
                evidence_integer(requirement["topology"][key], f"required {key}", 1, 16)
            listing_suite = suites[suite]
            require(listing_suite["binary-id"] == suite, "nextest suite binary identity mismatch")
            listed_binary = Path(listing_suite["binary-path"]).resolve(strict=True)
            require(listed_binary == Path(run["test_binary"]["path"]).resolve(strict=True) and
                    digest(listed_binary) == run["test_binary"]["sha256"], "nextest selected another test binary")
            require(listing_suite["package-name"] == "e2e_test" and listing_suite["build-platform"] in ("host", "target"),
                    "unexpected nextest suite metadata")
            listed = listing_suite.get("testcases", {}).get(test, {})
            require(listed.get("ignored") is False and listed.get("filter-match", {}).get("status") == "matches",
                    f"required test not selected: {suite}::{test}")
            matches = [case for case in cases if case.get("name") == test and case.get("classname") == suite]
            require(len(matches) == 1, f"missing/duplicate JUnit case: {suite}::{test}")
            started = datetime.fromisoformat(matches[0].attrib["timestamp"].replace("Z", "+00:00"))
            require(started.tzinfo is not None, "JUnit timestamp must include timezone")
            # quick-junit truncates timestamps to milliseconds.
            require(run["started_at"] - 0.001 <= started.timestamp() <= execution["finished_at"],
                    "JUnit testcase executed outside this run")
            path = directory / requirement["oracle"]
            require(path.resolve().is_relative_to(directory.resolve()), "oracle path escapes run directory")
            require(run["started_at"] <= path.stat().st_mtime <= execution["finished_at"], "oracle outside run window")
            require(digest(path) == execution["artifacts"][requirement["oracle"]], "oracle hash mismatch")
            oracle = read_json(path)
            evidence_integer(oracle.get("schema"), "oracle schema", 1, 1)
            require(oracle.get("evidence") == requirement["evidence"], f"not real {requirement['evidence']} evidence")
            require(oracle.get("case") == name and oracle.get("run_id") == run["run_id"], "oracle belongs to another case/run")
            require(oracle.get("source_revision") == run["source_revision"], "oracle source mismatch")
            require(oracle.get("unclean_shutdown_marker") is requirement["unclean_shutdown_marker"],
                    "unclean-shutdown marker evidence mismatch")
            built = oracle["test_build"]
            for key in ("source_revision", "dirty", "lock_blob", "features"):
                require(built[key] == expected_build[key], f"compiled test {key} mismatch")
            require(built["dirty"] is False, "test binary was compiled from dirty source")
            require(all(isinstance(built[key], str) and built[key] and built[key] != "unknown" for key in ("target", "profile")),
                    "missing compiled target/profile")
            require(isinstance(built["rustflags_hex"], str) and re.fullmatch(r"(?:[0-9a-f]{2})*", built["rustflags_hex"]) is not None,
                    "invalid compiled rustflags")
            for label in ("binary", "test_binary"):
                require(oracle.get(f"{label}_sha256") == run[label]["sha256"], f"oracle {label} mismatch")
            require(oracle.get("topology") == requirement["topology"], "oracle topology mismatch")
            for key in ("nodes", "drives_per_node"):
                evidence_integer(oracle["topology"][key], f"observed {key}", 1, 16)
            expected_erasure = requirement.get("erasure")
            if expected_erasure is not None:
                require(isinstance(expected_erasure, dict), "invalid erasure expectation")
                expected_data_blocks = evidence_integer(expected_erasure.get("data_blocks"), "expected EC data blocks", 1, 16)
                expected_parity_blocks = evidence_integer(expected_erasure.get("parity_blocks"), "expected EC parity blocks", 1, 16)
                require(
                    expected_data_blocks + expected_parity_blocks
                    == oracle["topology"]["nodes"] * oracle["topology"]["drives_per_node"],
                    "expected EC geometry differs from topology",
                )
            else:
                expected_data_blocks = None
                expected_parity_blocks = None
            evidence_integer(oracle.get("pid_before"), "pid_before", 1, 2**32 - 1)
            evidence_integer(oracle.get("pid_after"), "pid_after", 1, 2**32 - 1)
            require(oracle["pid_before"] != oracle["pid_after"], "no process restart witnessed")
            objects = oracle["objects"]
            require(isinstance(objects, list) and requirement["min_objects"] <= len(objects) <= requirement["max_objects"],
                    "incomplete/oversized object oracle")
            require(len({obj["key"] for obj in objects}) == len(objects), "duplicate object identity")
            require(sum(obj["expected_physical"] is None for obj in objects) == 1,
                    "only the outage object may lack a pre-fault target manifest")
            for obj in objects:
                require(isinstance(obj["key"], str) and 0 < len(obj["key"].encode()) <= 1024, "invalid object identity")
                require(obj["version_id"] is None, "this case only covers unversioned objects")
                require(type(obj["expected_bytes"]) is int and obj["expected_bytes"] > 0, "missing expected bytes")
                require(type(obj["actual_bytes"]) is int and obj["actual_bytes"] == obj["expected_bytes"], "S3 body length mismatch")
                require(sha(obj["expected_sha256"]) and obj["actual_sha256"] == obj["expected_sha256"], "S3 body digest mismatch")
                physical = obj["physical"]
                if obj["expected_physical"] is not None:
                    require(physical == obj["expected_physical"], "target shard differs from pre-fault manifest")
                for geometry in [physical] + ([obj["expected_physical"]] if obj["expected_physical"] is not None else []):
                    data = evidence_integer(geometry["data_blocks"], "EC data blocks", 1, 16)
                    parity = evidence_integer(geometry["parity_blocks"], "EC parity blocks", 1, 16)
                    require(data + parity == oracle["topology"]["nodes"] * oracle["topology"]["drives_per_node"],
                            "EC geometry differs from this case's single set")
                    if expected_data_blocks is not None:
                        require(data == expected_data_blocks and parity == expected_parity_blocks,
                                "EC data/parity geometry differs from the required case")
                    evidence_integer(geometry["erasure_index"], "target erasure index", 1, data + parity)
                require(physical["has_xl_meta"] is True and physical["version_id"] is None, "missing target metadata")
                parts = physical["expected_part_numbers"]
                require(isinstance(parts, list) and 0 < len(parts) <= 10000, "no physical part coverage")
                require(all(type(part) is int and part > 0 for part in parts) and len(set(parts)) == len(parts),
                        "invalid physical part identity")
                require({str(part) for part in parts} == set(physical["present_part_fingerprints"]), "target shard parts missing")
                for part in physical["present_part_fingerprints"].values():
                    require(type(part["size"]) is int and part["size"] > 0 and sha(part["sha256"]), "invalid target shard fingerprint")
            node_listings = oracle["node_listings"]
            require(isinstance(node_listings, list) and len(node_listings) == requirement["topology"]["nodes"],
                    "missing per-node S3 listing")
            require(all(keys == sorted(obj["key"] for obj in objects) for keys in node_listings),
                    "S3 listing differs from object oracle")
        if case_id == "release":
            release_requirements, _, _ = scanner_heal_release_requirements(registry)
            errors.extend(
                f"pending {gate}: {requirement['description']}" for gate, requirement in release_requirements.items()
            )
        return errors
    except (OSError, KeyError, TypeError, ValueError, ET.ParseError) as error:
        return [f"scanner/heal evidence rejected: {error}"]


def scanner_heal_release_status(root: Path, directory: Path) -> dict[str, object]:
    """Return a compact release decision without weakening case validation."""
    registry = read_json(root / ".config/scanner-heal-required-tests.json")
    scanner_heal_registry_schema(registry)
    cases = registry.get("cases")
    require(isinstance(cases, dict) and cases, "invalid scanner/heal registry")
    release_requirements, release_schema_capable, pending_lanes = scanner_heal_release_requirements(registry)

    verified_cases = []
    rejected_cases = []
    for case_id in sorted(cases):
        if check_scanner_heal_evidence(root, directory, case_id):
            rejected_cases.append(case_id)
        else:
            verified_cases.append(case_id)

    return {
        "schema": 1,
        "decision": "blocked",
        "release_approved": False,
        "release_schema_capable": release_schema_capable,
        "verified_cases": verified_cases,
        "rejected_cases": rejected_cases,
        "pending_gates": sorted(release_requirements),
        "pending_lanes": pending_lanes,
    }


def release_bundle_artifact_path(bundle_path: Path, raw_path: object, gate: str, field: str) -> Path:
    require(isinstance(raw_path, str) and raw_path.strip(), f"{gate}.{field} missing artifact")
    path = Path(raw_path)
    require(not path.is_absolute() and ".." not in path.parts, f"{gate}.{field} artifact path escapes bundle directory")
    resolved = (bundle_path.parent / path).resolve()
    require(resolved.is_relative_to(bundle_path.parent.resolve()), f"{gate}.{field} artifact path escapes bundle directory")
    require(resolved.is_file(), f"{gate}.{field} artifact is missing")
    require(resolved.stat().st_size > 0, f"{gate}.{field} artifact is empty")
    return resolved


def is_json_artifact_format(value: str) -> bool:
    normalized = value.lower()
    return normalized == "json" or normalized.endswith("+json")


def release_bundle_json_artifact_mirrored_fields(gate: str, field: str) -> tuple[str, ...]:
    fields: list[str] = []
    if gate in ("G03", "G09", "R-L"):
        fields.extend(("versions", "mixed_version_role"))
    if gate in ("G04", "G07", "R-E", "R-L"):
        fields.append("crash_points")
    if gate == "G03":
        fields.append("scoped_ack_cases")
        if field == "durable_root_publication_proof":
            fields.extend(("root_cas_observed", "root_readback_observed"))
        if field == "scoped_ack_request_identity":
            fields.append("whole_cycle_fallback_observed")
    if gate == "G04" and field == "root_floor_intent_crash_evidence":
        fields.extend(("durable_intent_cases", "persist_failure_blocks_acceptance"))
    if gate == "G07":
        fields.append({
            "mrf_responsibility_oracle": "mrf_responsibility_cases",
            "commit_boundary_crash_matrix": "commit_crash_cases",
        }[field])
    if gate == "G08":
        fields.append({
            "mrf_capacity_evidence": "capacity_cases",
            "disk_full_matrix": "disk_full_cases",
            "replica_loss_matrix": "replica_loss_cases",
        }[field])
    if gate == "G09":
        fields.append("mixed_version_cases")
        if field == "rollback_payload_evidence":
            fields.append("rollback_payload_replayed")
    if (gate, field) in SCANNER_HEAL_RELEASE_MRF_DURABLE_REPLAY_FIELDS:
        fields.extend(("replayed_records", "responsibility_anchor_retained", "successor_snapshot_published"))
    if gate == "P4" and field == "retained_responsibility_evidence":
        fields.extend((
            "duration_seconds",
            "retained_responsibility_cases",
            "retention_window_seconds",
            "idle_cleanup_observed",
            "verified_proof_discharge_observed",
        ))
    if gate == "P4" and field == "mrf_cleanup_gc_soak_evidence":
        fields.extend((
            "duration_seconds",
            "cleanup_gc_cases",
            "verified_idle_gc_observed",
            "pending_responsibilities_after_gc",
            "stale_journals_after_gc",
        ))
    return tuple(dict.fromkeys(fields))


def validate_release_bundle_json_artifact_payload(path: Path, source_revision: str, gate: str, field: str,
                                                 run_id: str, window_id: str,
                                                 artifact_kind: str | None = None) -> None:
    payload = read_json(path)
    prefix = f"{gate}.{field}"
    for marker in ("fixture", "fixture_only", "dry_run", "synthetic"):
        require(payload.get(marker) is not True, f"{prefix} JSON artifact is {marker}")
    require(payload.get("evidence_type") == "measured", f"{prefix} JSON artifact must be measured")
    require(payload.get("source_revision") == source_revision, f"{prefix} JSON artifact source revision mismatch")
    require(payload.get("run_id") == run_id, f"{prefix} JSON artifact run_id mismatch")
    require(payload.get("measurement_window_id") == window_id, f"{prefix} JSON artifact measurement window mismatch")
    require(payload.get("gate") == gate, f"{prefix} JSON artifact gate mismatch")
    require(payload.get("field") == field, f"{prefix} JSON artifact field mismatch")
    if artifact_kind is not None:
        require(payload.get("artifact_kind") == artifact_kind, f"{prefix} JSON artifact kind mismatch")
    mrf_artifact_kind = SCANNER_HEAL_RELEASE_MRF_ARTIFACT_KINDS.get((gate, field))
    if mrf_artifact_kind is not None:
        require(payload.get("artifact_kind") == mrf_artifact_kind,
                f"{prefix} JSON artifact kind must be {mrf_artifact_kind}")
    mirror_fields = release_bundle_json_artifact_mirrored_fields(gate, field)
    for mirror_field in mirror_fields:
        require(mirror_field in payload, f"{prefix} JSON artifact missing {mirror_field}")
    if not mirror_fields:
        return
    validate_release_bundle_domain_evidence(gate, field, payload)
    if gate in ("G03", "G09", "R-L"):
        versions = payload.get("versions")
        require(isinstance(versions, list) and
                len(set(versions)) >= 2 and
                all(isinstance(version, str) and re.fullmatch(r"[0-9a-f]{40}", version) is not None
                    for version in versions),
                f"{prefix} JSON artifact requires mixed-version evidence")
        require(source_revision in versions, f"{prefix} JSON artifact versions omit tested source revision")
        expected_role = SCANNER_HEAL_RELEASE_MIXED_VERSION_ROLES[(gate, field)]
        require(payload.get("mixed_version_role") == expected_role,
                f"{prefix} JSON artifact mixed-version role must be {expected_role}")
    if gate in ("G04", "G07", "R-E", "R-L"):
        crash_points = payload.get("crash_points")
        require(isinstance(crash_points, list) and crash_points,
                f"{prefix} JSON artifact requires crash-boundary evidence")
    if (gate, field) in SCANNER_HEAL_RELEASE_MRF_DURABLE_REPLAY_FIELDS:
        evidence_integer(payload.get("replayed_records"), f"{prefix} JSON artifact replayed_records", 1, 2**63 - 1)
        require(payload.get("responsibility_anchor_retained") is True,
                f"{prefix} JSON artifact requires retained MRF responsibility anchors")
        require(payload.get("successor_snapshot_published") is True,
                f"{prefix} JSON artifact requires successor snapshot publication evidence")
    if gate == "P4" and field == "mrf_cleanup_gc_soak_evidence":
        release_bundle_exact_strings(
            payload.get("cleanup_gc_cases"),
            SCANNER_HEAL_RELEASE_MRF_CLEANUP_GC_SOAK_CASES,
            f"{prefix} JSON artifact cleanup_gc_cases",
        )
        require(payload.get("verified_idle_gc_observed") is True,
                f"{prefix} JSON artifact requires verified idle GC evidence")
        require(payload.get("pending_responsibilities_after_gc") == 0,
                f"{prefix} JSON artifact requires zero pending responsibilities after GC")
        require(payload.get("stale_journals_after_gc") == 0,
                f"{prefix} JSON artifact requires zero stale journals after GC")
    if gate == "G07":
        case_field = {
            "mrf_responsibility_oracle": "mrf_responsibility_cases",
            "commit_boundary_crash_matrix": "commit_crash_cases",
        }[field]
        cases = evidence_string_list(payload.get(case_field), f"{prefix} JSON artifact {case_field}")
        missing_cases = sorted(set(SCANNER_HEAL_RELEASE_G07_REQUIRED_CASES[field]) - set(cases))
        require(not missing_cases, f"{prefix} JSON artifact missing cases: {', '.join(missing_cases)}")
    if gate == "G08":
        case_field = {
            "mrf_capacity_evidence": "capacity_cases",
            "disk_full_matrix": "disk_full_cases",
            "replica_loss_matrix": "replica_loss_cases",
        }[field]
        cases = evidence_string_list(payload.get(case_field), f"{prefix} JSON artifact {case_field}")
        missing_cases = sorted(set(SCANNER_HEAL_RELEASE_G08_REQUIRED_CASES[field]) - set(cases))
        require(not missing_cases, f"{prefix} JSON artifact missing cases: {', '.join(missing_cases)}")


def release_bundle_bool_true(value: object, name: str) -> None:
    require(value is True, f"{name} must be true")


def release_bundle_exact_strings(value: object, expected: tuple[str, ...], name: str) -> None:
    require(isinstance(value, list) and all(isinstance(item, str) and item.strip() for item in value),
            f"{name} must list measured cases")
    observed = set(value)
    require(len(observed) == len(value), f"{name} has duplicate cases")
    missing = sorted(set(expected) - observed)
    require(not missing, f"{name} missing cases: {', '.join(missing)}")
    unknown = sorted(observed - set(expected))
    require(not unknown, f"{name} has unknown cases: {', '.join(unknown)}")


def release_bundle_number(value: object, name: str, minimum: int | float = 0) -> float:
    return number(value, name, minimum)


def validate_release_bundle_domain_evidence(gate: str, field: str, evidence: dict[str, object]) -> None:
    if gate == "G03":
        release_bundle_exact_strings(
            evidence.get("scoped_ack_cases"),
            SCANNER_HEAL_RELEASE_SCOPED_ACK_CASES[field],
            f"{gate}.{field}.scoped_ack_cases",
        )
        if field == "durable_root_publication_proof":
            release_bundle_bool_true(evidence.get("root_cas_observed"), f"{gate}.{field}.root_cas_observed")
            release_bundle_bool_true(evidence.get("root_readback_observed"), f"{gate}.{field}.root_readback_observed")
        if field == "scoped_ack_request_identity":
            release_bundle_bool_true(evidence.get("whole_cycle_fallback_observed"),
                                     f"{gate}.{field}.whole_cycle_fallback_observed")

    if gate == "G04" and field == "root_floor_intent_crash_evidence":
        release_bundle_exact_strings(
            evidence.get("durable_intent_cases"),
            SCANNER_HEAL_RELEASE_CRASH_BOUNDARY_FIELDS[(gate, field)],
            f"{gate}.{field}.durable_intent_cases",
        )
        release_bundle_bool_true(evidence.get("persist_failure_blocks_acceptance"),
                                 f"{gate}.{field}.persist_failure_blocks_acceptance")

    if gate == "G09":
        release_bundle_exact_strings(
            evidence.get("mixed_version_cases"),
            SCANNER_HEAL_RELEASE_MIXED_VERSION_CASES[field],
            f"{gate}.{field}.mixed_version_cases",
        )
        if field == "rollback_payload_evidence":
            release_bundle_bool_true(evidence.get("rollback_payload_replayed"),
                                     f"{gate}.{field}.rollback_payload_replayed")

    if gate == "G10":
        if field == "scheduler_bound_evidence":
            release_bundle_exact_strings(
                evidence.get("scheduler_bounds"),
                SCANNER_HEAL_RELEASE_SCHEDULER_BOUNDS,
                f"{gate}.{field}.scheduler_bounds",
            )
            release_bundle_bool_true(evidence.get("duplicate_task_bound_observed"),
                                     f"{gate}.{field}.duplicate_task_bound_observed")
        if field == "pressure_recovery_evidence":
            metrics = evidence.get("pressure_metrics")
            require(isinstance(metrics, dict), f"{gate}.{field} missing pressure metrics")
            for metric in SCANNER_HEAL_RELEASE_PRESSURE_METRICS:
                release_bundle_number(metrics.get(metric), f"{gate}.{field}.{metric}")
            evidence_integer(metrics.get("foreground_pressure_samples"),
                             f"{gate}.{field}.foreground_pressure_samples", 1, 2**63 - 1)
            evidence_integer(metrics.get("foreground_pressure_high_samples"),
                             f"{gate}.{field}.foreground_pressure_high_samples", 1, 2**63 - 1)

    if gate == "P1":
        if field == "cold_walk_share_measurement":
            share = release_bundle_number(evidence.get("cold_walk_share"), f"{gate}.{field}.cold_walk_share")
            require(share <= 1, f"{gate}.{field}.cold_walk_share exceeds one")
            evidence_integer(evidence.get("walk_objects"), f"{gate}.{field}.walk_objects", 1, 2**63 - 1)
            evidence_integer(evidence.get("cold_walk_objects"), f"{gate}.{field}.cold_walk_objects", 0, 2**63 - 1)
        if field == "foreground_latency_throughput_measurement":
            for metric in ("foreground_p95_ms", "foreground_p99_ms", "throughput_ops", "error_rate"):
                release_bundle_number(evidence.get(metric), f"{gate}.{field}.{metric}")
        if field == "profile_evidence":
            for metric in ("allocation_bytes", "rss_peak_bytes", "save_operations", "saved_bytes"):
                evidence_integer(evidence.get(metric), f"{gate}.{field}.{metric}", 1, 2**63 - 1)

    if gate == "P3":
        if field == "two_hour_pressure_measurement":
            release_bundle_exact_strings(evidence.get("abba_legs"), ("A1", "B1", "B2", "A2"),
                                         f"{gate}.{field}.abba_legs")
            release_bundle_exact_strings(evidence.get("scenarios"), SCENARIOS, f"{gate}.{field}.scenarios")
            for metric in ("foreground_p95_ms", "foreground_p99_ms", "throughput_ops"):
                release_bundle_number(evidence.get(metric), f"{gate}.{field}.{metric}", 1)
        if field == "heal_capacity_measurement":
            capacity = evidence.get("heal_capacity")
            require(isinstance(capacity, dict), f"{gate}.{field} missing heal capacity")
            for metric in ("objects", "versions", "bytes", "completed_objects"):
                evidence_integer(capacity.get(metric), f"{gate}.{field}.{metric}", 1, 2**63 - 1)
        if field == "recovery_window_measurement":
            release_bundle_exact_strings(
                evidence.get("fault_modes"),
                ("process-restart", "process-crash-restart"),
                f"{gate}.{field}.fault_modes",
            )
            for metric in ("recovery_p95_ms", "recovery_p99_ms"):
                release_bundle_number(evidence.get(metric), f"{gate}.{field}.{metric}", 1)

    if gate == "P4" and field == "retained_responsibility_evidence":
        release_bundle_exact_strings(
            evidence.get("retained_responsibility_cases"),
            SCANNER_HEAL_RELEASE_P4_RETAINED_RESPONSIBILITY_CASES,
            f"{gate}.{field}.retained_responsibility_cases",
        )
        retention_window = evidence_integer(
            evidence.get("retention_window_seconds"),
            f"{gate}.{field}.retention_window_seconds",
            7200,
            86400,
        )
        duration = evidence_integer(evidence.get("duration_seconds"), f"{gate}.{field}.duration_seconds", 1, 86400)
        require(duration >= retention_window, f"{gate}.{field} duration must cover retention window")
        release_bundle_bool_true(evidence.get("idle_cleanup_observed"), f"{gate}.{field}.idle_cleanup_observed")
        release_bundle_bool_true(
            evidence.get("verified_proof_discharge_observed"),
            f"{gate}.{field}.verified_proof_discharge_observed",
        )


def validate_release_bundle_artifact(bundle_path: Path, source_revision: str, gate: str, field: str,
                                     evidence: dict[str, object]) -> str:
    require(evidence.get("evidence_type") == "measured", f"{gate}.{field} must be measured evidence")
    require(evidence.get("source_revision") == source_revision, f"{gate}.{field} source revision mismatch")
    run_id = evidence_string(evidence.get("run_id"), f"{gate}.{field}.run_id", r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
    window_id = evidence_string(evidence.get("measurement_window_id"), f"{gate}.{field}.measurement_window_id",
                                r"[A-Za-z0-9][A-Za-z0-9._:-]{7,127}")
    require(window_id != run_id, f"{gate}.{field} must separate run and measurement-window identities")
    started = evidence_timestamp(evidence.get("started_at"), f"{gate}.{field}.started_at")
    finished = evidence_timestamp(evidence.get("finished_at"), f"{gate}.{field}.finished_at")
    require(started <= finished, f"{gate}.{field} evidence timestamps are inverted")
    command = evidence.get("command")
    require(isinstance(command, list) and command and
            all(isinstance(part, str) and part.strip() for part in command),
            f"{gate}.{field} missing command provenance")
    artifact_format = evidence_string(evidence.get("artifact_format"), f"{gate}.{field}.artifact_format",
                                      r"[A-Za-z0-9][A-Za-z0-9._+:-]{1,63}")
    artifact = release_bundle_artifact_path(bundle_path, evidence.get("artifact"), gate, field)
    require(sha(evidence.get("sha256")) and digest(artifact) == evidence["sha256"], f"{gate}.{field} artifact hash mismatch")
    if is_json_artifact_format(artifact_format):
        validate_release_bundle_json_artifact_payload(artifact, source_revision, gate, field, run_id, window_id)
    summary = evidence.get("summary")
    require(isinstance(summary, str) and summary.strip(), f"{gate}.{field} missing human summary")
    if gate.startswith("P"):
        duration = evidence_integer(evidence.get("duration_seconds"), f"{gate}.{field}.duration_seconds", 1, 86400)
        require((finished - started).total_seconds() + 1 >= duration, f"{gate}.{field} duration exceeds run window")
        require(duration >= 900, f"{gate}.{field} requires at least 900 seconds")
        if gate == "P3" and field == "two_hour_pressure_measurement":
            require(duration >= 7200, f"{gate}.{field} requires at least two hours")
        if gate == "P4" and field == "mrf_cleanup_gc_soak_evidence":
            require(duration >= 7200, f"{gate}.{field} requires at least two hours")
    elif "duration_seconds" in evidence:
        evidence_integer(evidence.get("duration_seconds"), f"{gate}.{field}.duration_seconds", 1, 86400)
    if gate == "G10":
        if field == "scheduler_bound_evidence":
            evidence_integer(evidence.get("max_deferred_items"), f"{gate}.{field}.max_deferred_items", 1, 2**31 - 1)
            evidence_integer(evidence.get("max_deferred_bytes"), f"{gate}.{field}.max_deferred_bytes", 1, 2**63 - 1)
            evidence_integer(evidence.get("max_retry_age_seconds"), f"{gate}.{field}.max_retry_age_seconds", 1, 86400)
            evidence_integer(evidence.get("duplicate_task_count"), f"{gate}.{field}.duplicate_task_count", 0, 0)
        if field == "pressure_recovery_evidence":
            require(evidence.get("pressure_pacing_engaged") is True,
                    f"{gate}.{field} requires pressure pacing engagement")
            evidence_integer(evidence.get("recovery_window_seconds"), f"{gate}.{field}.recovery_window_seconds", 1, 86400)
            evidence_integer(evidence.get("lock_hold_p95_ms"), f"{gate}.{field}.lock_hold_p95_ms", 0, 2**31 - 1)
            evidence_integer(evidence.get("foreground_latency_p95_ms"),
                             f"{gate}.{field}.foreground_latency_p95_ms", 1, 2**31 - 1)
    if gate == "P1" and field == "foreground_latency_throughput_measurement":
        evidence_integer(evidence.get("foreground_latency_p95_ms"),
                         f"{gate}.{field}.foreground_latency_p95_ms", 1, 2**31 - 1)
        evidence_integer(evidence.get("foreground_latency_p99_ms"),
                         f"{gate}.{field}.foreground_latency_p99_ms", 1, 2**31 - 1)
        evidence_integer(evidence.get("throughput_ops_per_second"),
                         f"{gate}.{field}.throughput_ops_per_second", 1, 2**31 - 1)
        evidence_integer(evidence.get("error_count"), f"{gate}.{field}.error_count", 0, 0)
    if gate == "P3":
        if field == "two_hour_pressure_measurement":
            require(evidence.get("fixed_offered_load") is True, f"{gate}.{field} requires fixed offered load")
            evidence_integer(evidence.get("foreground_latency_p99_ms"),
                             f"{gate}.{field}.foreground_latency_p99_ms", 1, 2**31 - 1)
            evidence_integer(evidence.get("attempt_cost_samples"),
                             f"{gate}.{field}.attempt_cost_samples", 1, 2**63 - 1)
        if field == "heal_capacity_measurement":
            evidence_integer(evidence.get("completed_heal_objects"),
                             f"{gate}.{field}.completed_heal_objects", 1, 2**63 - 1)
            evidence_integer(evidence.get("duplicate_task_count"), f"{gate}.{field}.duplicate_task_count", 0, 0)
        if field == "recovery_window_measurement":
            evidence_integer(evidence.get("pressure_recovery_window_seconds"),
                             f"{gate}.{field}.pressure_recovery_window_seconds", 1, 86400)
            evidence_integer(evidence.get("lock_hold_p95_ms"), f"{gate}.{field}.lock_hold_p95_ms", 0, 2**31 - 1)
    if gate in ("G03", "G09", "R-L"):
        versions = evidence.get("versions")
        require(isinstance(versions, list) and
                len(set(versions)) >= 2 and
                all(isinstance(version, str) and re.fullmatch(r"[0-9a-f]{40}", version) is not None
                    for version in versions),
                f"{gate}.{field} requires mixed-version evidence")
        require(source_revision in versions, f"{gate}.{field} versions omit tested source revision")
        expected_role = SCANNER_HEAL_RELEASE_MIXED_VERSION_ROLES[(gate, field)]
        require(evidence.get("mixed_version_role") == expected_role,
                f"{gate}.{field} mixed-version role must be {expected_role}")
    if gate in ("G04", "G07", "R-E", "R-L"):
        crash_points = evidence.get("crash_points")
        require(isinstance(crash_points, list) and crash_points,
                f"{gate}.{field} requires crash-boundary evidence")
    if (gate, field) in SCANNER_HEAL_RELEASE_MRF_DURABLE_REPLAY_FIELDS:
        evidence_integer(evidence.get("replayed_records"), f"{gate}.{field}.replayed_records", 1, 2**63 - 1)
        require(evidence.get("responsibility_anchor_retained") is True,
                f"{gate}.{field} requires retained MRF responsibility anchors")
        require(evidence.get("successor_snapshot_published") is True,
                f"{gate}.{field} requires successor snapshot publication evidence")
    if gate == "P4" and field == "mrf_cleanup_gc_soak_evidence":
        release_bundle_exact_strings(
            evidence.get("cleanup_gc_cases"),
            SCANNER_HEAL_RELEASE_MRF_CLEANUP_GC_SOAK_CASES,
            f"{gate}.{field}.cleanup_gc_cases",
        )
        require(evidence.get("verified_idle_gc_observed") is True,
                f"{gate}.{field} requires verified idle GC evidence")
        require(evidence.get("pending_responsibilities_after_gc") == 0,
                f"{gate}.{field} requires zero pending responsibilities after GC")
        require(evidence.get("stale_journals_after_gc") == 0,
                f"{gate}.{field} requires zero stale journals after GC")
    if field == "segment_activation_preflight":
        require(evidence.get("production_activation") is False,
                f"{gate}.{field} must keep production activation disabled")
        require(evidence.get("scanner_segment_reuse_activated") is False,
                f"{gate}.{field} must prove the runtime activation gate is disabled")
        evidence_exact_strings(evidence.get("proof_inputs"),
                               SCANNER_HEAL_SEGMENT_ACTIVATION_PROOF_INPUTS,
                               f"{gate}.{field}.proof_inputs")
        evidence_exact_strings(evidence.get("fail_closed_checks"),
                               SCANNER_HEAL_SEGMENT_ACTIVATION_FAIL_CLOSED_CHECKS,
                               f"{gate}.{field}.fail_closed_checks")
    if field == "cold_segment_reuse_measurement":
        evidence_integer(evidence.get("hot_walked_segments"), f"{gate}.{field}.hot_walked_segments", 1, 2**63 - 1)
        evidence_integer(evidence.get("cold_walked_segments"), f"{gate}.{field}.cold_walked_segments", 0, 0)
        require(evidence.get("full_walk_oracle_equivalent") is True,
                f"{gate}.{field} requires full-walk oracle equivalence")
        require(evidence.get("published_root_equivalent") is True,
                f"{gate}.{field} requires published-root equivalence")
    if gate == "G07":
        case_field = {
            "mrf_responsibility_oracle": "mrf_responsibility_cases",
            "commit_boundary_crash_matrix": "commit_crash_cases",
        }[field]
        cases = evidence_string_list(evidence.get(case_field), f"{gate}.{field}.{case_field}")
        missing_cases = sorted(set(SCANNER_HEAL_RELEASE_G07_REQUIRED_CASES[field]) - set(cases))
        require(not missing_cases, f"{gate}.{field} missing cases: {', '.join(missing_cases)}")
    if gate == "G08":
        case_field = {
            "mrf_capacity_evidence": "capacity_cases",
            "disk_full_matrix": "disk_full_cases",
            "replica_loss_matrix": "replica_loss_cases",
        }[field]
        cases = evidence_string_list(evidence.get(case_field), f"{gate}.{field}.{case_field}")
        missing_cases = sorted(set(SCANNER_HEAL_RELEASE_G08_REQUIRED_CASES[field]) - set(cases))
        require(not missing_cases, f"{gate}.{field} missing cases: {', '.join(missing_cases)}")
    if gate == "G14":
        if field == "same_window_field_evidence":
            required_fields = set(SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS["G14"]) - {field}
            same_window_fields = evidence.get("same_window_fields")
            require(isinstance(same_window_fields, list) and
                    len(set(same_window_fields)) == len(same_window_fields) and
                    all(isinstance(item, str) and item in required_fields for item in same_window_fields),
                    "G14.same_window_field_evidence requires named G14 field coverage")
            missing_same_window_fields = sorted(required_fields - set(same_window_fields))
            require(not missing_same_window_fields,
                    "G14.same_window_field_evidence missing fields: " + ", ".join(missing_same_window_fields))
        if field == "ec8_4_evidence":
            topology = evidence.get("topology")
            require(isinstance(topology, dict), "G14.ec8_4_evidence missing topology")
            require(topology.get("erasure") == "EC8+4", "G14.ec8_4_evidence must record EC8+4")
            require(evidence_integer(topology.get("nodes"), "G14 topology nodes", 3, 64) >= 3,
                    "G14.ec8_4_evidence requires at least three nodes")
            require(evidence_integer(topology.get("drives_per_node"), "G14 topology drives", 4, 64) >= 4,
                    "G14.ec8_4_evidence requires at least four drives per node")
        if field == "multi_set_evidence":
            evidence_integer(evidence.get("sets"), "G14 multi_set_evidence.sets", 2, 1024)
        if field == "multi_pool_evidence":
            evidence_integer(evidence.get("pools"), "G14 multi_pool_evidence.pools", 2, 1024)
        if field == "distributed_segment_invalidation_evidence":
            require(evidence.get("invalidation_domain") == "distributed-ec",
                    "G14.distributed_segment_invalidation_evidence requires distributed EC invalidation")
            require(evidence.get("distributed_ec_invalidation") is True,
                    "G14.distributed_segment_invalidation_evidence requires peer invalidation proof")
            evidence_integer(evidence.get("peer_count"), "G14 distributed_segment_invalidation_evidence.peer_count", 3, 64)
            require(evidence.get("same_window_remote_proof") is True,
                    "G14.distributed_segment_invalidation_evidence requires same-window remote proof")
            require(evidence.get("all_peers_bound_to_generation_window") is True,
                    "G14.distributed_segment_invalidation_evidence requires peer generation-window binding")
    if field == "profile_evidence":
        evidence_integer(evidence.get("resolved_samples"), f"{gate}.{field}.resolved_samples", 1, 2**63 - 1)
        profile_artifacts = evidence.get("profile_artifacts")
        require(isinstance(profile_artifacts, dict), f"{gate}.{field} missing profile artifacts")
        missing_artifacts = sorted(set(RELEASE_PROFILE_ARTIFACTS) - set(profile_artifacts))
        require(not missing_artifacts,
                f"{gate}.{field} missing profile artifacts: {', '.join(missing_artifacts)}")
        unknown_artifacts = sorted(set(profile_artifacts) - set(RELEASE_PROFILE_ARTIFACTS))
        require(not unknown_artifacts,
                f"{gate}.{field} unknown profile artifacts: {', '.join(unknown_artifacts)}")
        for artifact_kind in RELEASE_PROFILE_ARTIFACTS:
            item = profile_artifacts[artifact_kind]
            require(isinstance(item, dict), f"{gate}.{field}.{artifact_kind} must be an object")
            artifact_field = f"{field}.{artifact_kind}"
            artifact_path = release_bundle_artifact_path(bundle_path, item.get("artifact"), gate, artifact_field)
            require(sha(item.get("sha256")) and digest(artifact_path) == item["sha256"],
                    f"{gate}.{artifact_field} artifact hash mismatch")
            artifact_format = evidence_string(item.get("artifact_format"), f"{gate}.{artifact_field}.artifact_format",
                                              r"[A-Za-z0-9][A-Za-z0-9._+:-]{1,63}")
            require(item.get("source_revision") == source_revision,
                    f"{gate}.{artifact_field} source revision mismatch")
            require(item.get("run_id") == run_id, f"{gate}.{artifact_field} run_id mismatch")
            require(item.get("measurement_window_id") == window_id,
                    f"{gate}.{artifact_field} measurement window mismatch")
            if is_json_artifact_format(artifact_format):
                validate_release_bundle_json_artifact_payload(
                    artifact_path,
                    source_revision,
                    gate,
                    field,
                    run_id,
                    window_id,
                    artifact_kind,
                )
            if "resolved_samples" in item:
                evidence_integer(item.get("resolved_samples"), f"{gate}.{artifact_field}.resolved_samples",
                                 0, 2**63 - 1)
    validate_release_bundle_domain_evidence(gate, field, evidence)
    return window_id


def validate_release_bundle_gate_windows(gate: str, field_windows: dict[str, str]) -> None:
    if gate == "G14" or gate.startswith("P"):
        windows = sorted(set(field_windows.values()))
        require(len(windows) == 1, f"{gate} evidence fields must share one measurement window")


def validate_scanner_heal_release_bundle_gate(bundle_path: Path, source_revision: str,
                                              requirements: dict[str, dict[str, object]],
                                              raw_gates: dict[str, object], gate: str) -> list[str]:
    gate_errors: list[str] = []
    requirement = requirements[gate]
    gate_evidence = raw_gates.get(gate)
    if not isinstance(gate_evidence, dict):
        return ["missing gate evidence"]
    if gate_evidence.get("status") != "pass":
        gate_errors.append("gate status must be pass")
    if gate_evidence.get("lane") != requirement["lane"]:
        gate_errors.append("gate lane mismatch")
    if gate_evidence.get("evidence_type") != "measured":
        gate_errors.append("gate evidence type must be measured")
    fields = gate_evidence.get("evidence_fields")
    if not isinstance(fields, dict):
        gate_errors.append("missing gate evidence fields")
        fields = {}
    required_fields = tuple(SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS[gate])
    missing_fields = [field for field in required_fields if field not in fields]
    if missing_fields:
        gate_errors.append(f"missing required fields: {', '.join(missing_fields)}")
    field_windows: dict[str, str] = {}
    for field in required_fields:
        if field not in fields:
            continue
        evidence = fields[field]
        if not isinstance(evidence, dict):
            gate_errors.append(f"{field} must be an object")
            continue
        for marker in ("fixture_only", "dry_run", "synthetic"):
            if evidence.get(marker) is True:
                gate_errors.append(f"{field} is {marker}")
                continue
        try:
            field_windows[field] = validate_release_bundle_artifact(bundle_path, source_revision, gate, field, evidence)
        except (OSError, KeyError, TypeError, ValueError) as error:
            gate_errors.append(str(error))
    if not gate_errors:
        try:
            validate_release_bundle_gate_windows(gate, field_windows)
        except ValueError as error:
            gate_errors.append(str(error))
    return gate_errors


def scanner_heal_release_bundle_status(root: Path, bundle_path: Path) -> dict[str, object]:
    """Validate a complete hard-gate evidence bundle without accepting synthetic claims."""
    registry = read_json(root / ".config/scanner-heal-required-tests.json")
    requirements, release_schema_capable, pending_lanes = scanner_heal_release_requirements(registry)
    require(release_schema_capable, "scanner/heal release bundle requires schema 2 registry")
    bundle_path = bundle_path.resolve()
    bundle = read_json(bundle_path)
    for marker in ("fixture_only", "dry_run", "synthetic"):
        require(bundle.get(marker) is not True, f"scanner/heal release evidence bundle is {marker}")
    require(bundle.get("schema") == 1, "unsupported scanner/heal release evidence bundle schema")
    require(bundle.get("evidence") == "measured", "scanner/heal release evidence bundle must be measured")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    source_revision = bundle.get("source_revision")
    require(isinstance(source_revision, str) and re.fullmatch(r"[0-9a-f]{40}", source_revision) is not None and
            source_revision == revision,
            "scanner/heal release evidence source revision mismatch")
    raw_gates = bundle.get("gates")
    require(isinstance(raw_gates, dict), "scanner/heal release evidence bundle missing gates")

    verified: list[str] = []
    rejected: dict[str, list[str]] = {}
    for gate in requirements:
        gate_errors = validate_scanner_heal_release_bundle_gate(bundle_path, source_revision, requirements, raw_gates, gate)
        if gate_errors:
            rejected[gate] = gate_errors
        else:
            verified.append(gate)

    unknown = sorted(set(raw_gates) - set(requirements))
    if unknown:
        rejected["unknown"] = [f"unknown gates: {', '.join(unknown)}"]
    approved = not rejected and sorted(verified) == sorted(requirements)
    return {
        "schema": 1,
        "decision": "approved" if approved else "blocked",
        "release_approved": approved,
        "release_schema_capable": release_schema_capable,
        "verified_gates": sorted(verified),
        "rejected_gates": rejected,
        "pending_gates": [] if approved else sorted(gate for gate in requirements if gate not in verified),
        "pending_lanes": [] if approved else pending_lanes,
    }


def scanner_heal_release_bundle_gate_status(root: Path, bundle_path: Path, gate: str) -> dict[str, object]:
    """Validate one measured release-bundle gate without approving the full release."""
    registry = read_json(root / ".config/scanner-heal-required-tests.json")
    requirements, release_schema_capable, pending_lanes = scanner_heal_release_requirements(registry)
    require(release_schema_capable, "scanner/heal release bundle gate requires schema 2 registry")
    require(gate in requirements, f"unknown scanner/heal release gate: {gate}")
    bundle_path = bundle_path.resolve()
    bundle = read_json(bundle_path)
    for marker in ("fixture_only", "dry_run", "synthetic"):
        require(bundle.get(marker) is not True, f"scanner/heal release evidence bundle is {marker}")
    require(bundle.get("schema") == 1, "unsupported scanner/heal release evidence bundle schema")
    require(bundle.get("evidence") == "measured", "scanner/heal release evidence bundle must be measured")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    source_revision = bundle.get("source_revision")
    require(isinstance(source_revision, str) and re.fullmatch(r"[0-9a-f]{40}", source_revision) is not None and
            source_revision == revision,
            "scanner/heal release evidence source revision mismatch")
    raw_gates = bundle.get("gates")
    require(isinstance(raw_gates, dict), "scanner/heal release evidence bundle missing gates")
    unknown = sorted(set(raw_gates) - set(requirements))
    require(not unknown, f"scanner/heal release evidence bundle has unknown gates: {', '.join(unknown)}")

    gate_errors = validate_scanner_heal_release_bundle_gate(bundle_path, source_revision, requirements, raw_gates, gate)
    verified = not gate_errors
    return {
        "schema": 1,
        "decision": "verified" if verified else "blocked",
        "release_approved": False,
        "release_schema_capable": release_schema_capable,
        "verified_gate": gate if verified else None,
        "rejected_gate": None if verified else gate,
        "rejected_errors": gate_errors,
        "pending_gates": sorted(item for item in requirements if item != gate),
        "pending_lanes": pending_lanes,
    }


def release_bundle_descriptor_path(descriptor_path: Path, raw_path: object, gate: str, field: str) -> Path:
    require(isinstance(raw_path, str) and raw_path.strip(), f"{gate}.{field} missing source artifact")
    path = Path(raw_path)
    resolved = path if path.is_absolute() else descriptor_path.parent / path
    resolved = resolved.resolve(strict=True)
    require(resolved.is_file(), f"{gate}.{field} source artifact is missing")
    require(resolved.stat().st_size > 0, f"{gate}.{field} source artifact is empty")
    return resolved


def release_bundle_artifact_suffix(path: Path) -> str:
    suffix = path.suffix
    if re.fullmatch(r"\.[A-Za-z0-9._+-]{1,16}", suffix or "") is not None:
        return suffix
    return ".artifact"


def reject_release_bundle_markers(item: dict[str, object], label: str) -> None:
    for marker in ("fixture_only", "dry_run", "synthetic"):
        require(item.get(marker) is not True, f"{label} is {marker}")


def copy_release_bundle_artifact(descriptor_path: Path, bundle_dir: Path, evidence: dict[str, object],
                                 gate: str, field: str, label: str) -> None:
    source = release_bundle_descriptor_path(descriptor_path, evidence.get("artifact"), gate, field)
    source_sha = digest(source)
    if "sha256" in evidence:
        require(sha(evidence.get("sha256")) and evidence["sha256"] == source_sha,
                f"{gate}.{field} descriptor artifact hash mismatch")
    artifact_dir = bundle_dir / "artifacts"
    safe_label = re.sub(r"[^A-Za-z0-9._-]", "-", label)
    target = artifact_dir / f"{safe_label}{release_bundle_artifact_suffix(source)}"
    require(not target.exists(), f"{gate}.{field} duplicate assembled artifact path")
    shutil.copyfile(source, target)
    require(digest(target) == source_sha, f"{gate}.{field} assembled artifact hash mismatch")
    evidence["artifact"] = target.relative_to(bundle_dir).as_posix()
    evidence["sha256"] = source_sha


def assemble_scanner_heal_release_bundle(root: Path, descriptor_path: Path, directory: Path) -> tuple[Path, dict[str, object]]:
    """Copy measured Scanner/Heal evidence into one validated release bundle."""
    require(not directory.exists(), "scanner/heal release bundle directory must be new")
    registry = read_json(root / ".config/scanner-heal-required-tests.json")
    requirements, release_schema_capable, _ = scanner_heal_release_requirements(registry)
    require(release_schema_capable, "scanner/heal release bundle assembly requires schema 2 registry")
    descriptor_path = descriptor_path.resolve(strict=True)
    descriptor = read_json(descriptor_path)
    reject_release_bundle_markers(descriptor, "scanner/heal release evidence descriptor")
    require(descriptor.get("schema") == 1, "unsupported scanner/heal release evidence descriptor schema")
    require(descriptor.get("evidence") == "measured", "scanner/heal release evidence descriptor must be measured")
    source_revision = descriptor.get("source_revision")
    require(isinstance(source_revision, str) and re.fullmatch(r"[0-9a-f]{40}", source_revision) is not None,
            "scanner/heal release evidence descriptor source revision is invalid")
    raw_gates = descriptor.get("gates")
    require(isinstance(raw_gates, dict), "scanner/heal release evidence descriptor missing gates")
    unknown_gates = sorted(set(raw_gates) - set(requirements))
    require(not unknown_gates, f"scanner/heal release evidence descriptor has unknown gates: {', '.join(unknown_gates)}")

    bundle_dir = directory.resolve()
    artifact_dir = bundle_dir / "artifacts"
    artifact_dir.mkdir(parents=True)
    assembled_gates = {}
    for gate in sorted(raw_gates):
        raw_gate = raw_gates[gate]
        require(isinstance(raw_gate, dict), f"{gate} descriptor gate must be an object")
        reject_release_bundle_markers(raw_gate, f"{gate} descriptor gate")
        raw_fields = raw_gate.get("evidence_fields", raw_gate)
        require(isinstance(raw_fields, dict), f"{gate} descriptor gate missing evidence fields")
        required_fields = set(SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS[gate])
        unknown_fields = sorted(set(raw_fields) - required_fields)
        require(not unknown_fields, f"{gate} descriptor has unknown fields: {', '.join(unknown_fields)}")
        fields = {}
        for field in sorted(raw_fields):
            raw_evidence = raw_fields[field]
            require(isinstance(raw_evidence, dict), f"{gate}.{field} descriptor evidence must be an object")
            evidence = json.loads(json.dumps(raw_evidence))
            reject_release_bundle_markers(evidence, f"{gate}.{field} descriptor evidence")
            copy_release_bundle_artifact(descriptor_path, bundle_dir, evidence, gate, field, f"{gate}-{field}")
            profile_artifacts = evidence.get("profile_artifacts")
            if profile_artifacts is not None:
                require(isinstance(profile_artifacts, dict), f"{gate}.{field} profile artifacts must be an object")
                for artifact_kind, item in sorted(profile_artifacts.items()):
                    require(isinstance(item, dict), f"{gate}.{field}.{artifact_kind} descriptor profile artifact must be an object")
                    reject_release_bundle_markers(item, f"{gate}.{field}.{artifact_kind} descriptor profile artifact")
                    copy_release_bundle_artifact(
                        descriptor_path,
                        bundle_dir,
                        item,
                        gate,
                        f"{field}.{artifact_kind}",
                        f"{gate}-{field}-{artifact_kind}",
                    )
            fields[field] = evidence
        assembled_gates[gate] = {
            "status": "pass",
            "lane": requirements[gate]["lane"],
            "evidence_type": "measured",
            "evidence_fields": fields,
        }

    bundle = bundle_dir / "release-evidence.json"
    write_json(bundle, {
        "schema": 1,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": assembled_gates,
    })
    return bundle, scanner_heal_release_bundle_status(root, bundle)


def write_scanner_heal_release_bundle_fixture(root: Path, directory: Path) -> Path:
    """Write a non-approvable release bundle shape fixture for dry-run validation."""
    bundle_dir = directory.resolve()
    bundle_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir = bundle_dir / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    registry = read_json(root / ".config/scanner-heal-required-tests.json")
    requirements, release_schema_capable, _ = scanner_heal_release_requirements(registry)
    require(release_schema_capable, "scanner/heal release bundle fixture requires schema 2 registry")
    source_revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    baseline_revision = "0" * 39 + "1"
    require(baseline_revision != source_revision, "scanner/heal release bundle fixture needs a distinct baseline")
    started = datetime.now(timezone.utc).replace(microsecond=0)
    gates = {}
    for gate, requirement in requirements.items():
        fields = {}
        for field in SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS[gate]:
            artifact = artifact_dir / f"{gate}-{field}.json"
            write_json(artifact, {
                "schema": 1,
                "fixture_only": True,
                "gate": gate,
                "field": field,
                "source_revision": source_revision,
            })
            duration = 60
            evidence = {
                "fixture_only": True,
                "artifact": artifact.relative_to(bundle_dir).as_posix(),
                "sha256": digest(artifact),
                "evidence_type": "measured",
                "source_revision": source_revision,
                "run_id": f"{gate.lower()}-{field.replace('_', '-')}-fixture-run",
                "measurement_window_id": f"{gate.lower()}-fixture-window",
                "started_at": started.isoformat().replace("+00:00", "Z"),
                "command": ["fixture-only", "scanner-heal-release-bundle", gate, field],
                "artifact_format": "json",
                "summary": f"fixture-only parser sample for {gate}.{field}; not release evidence",
            }
            if gate.startswith("P"):
                duration = 900
                evidence["duration_seconds"] = duration
            if gate == "P3" and field == "two_hour_pressure_measurement":
                duration = 7200
                evidence["duration_seconds"] = duration
            evidence["finished_at"] = (started + timedelta(seconds=duration)).isoformat().replace("+00:00", "Z")
            if gate == "G10" and field == "scheduler_bound_evidence":
                evidence.update({
                    "max_deferred_items": 256,
                    "max_deferred_bytes": 262144,
                    "max_retry_age_seconds": 30,
                    "duplicate_task_count": 0,
                })
            if gate == "G10" and field == "pressure_recovery_evidence":
                evidence.update({
                    "pressure_pacing_engaged": True,
                    "recovery_window_seconds": 5,
                    "lock_hold_p95_ms": 0,
                    "foreground_latency_p95_ms": 10,
                })
            if gate == "P1" and field == "foreground_latency_throughput_measurement":
                evidence.update({
                    "foreground_latency_p95_ms": 10,
                    "foreground_latency_p99_ms": 20,
                    "throughput_ops_per_second": 100,
                    "error_count": 0,
                })
            if gate == "P3" and field == "two_hour_pressure_measurement":
                evidence.update({
                    "fixed_offered_load": True,
                    "foreground_latency_p99_ms": 20,
                    "attempt_cost_samples": 1,
                })
            if gate == "P3" and field == "heal_capacity_measurement":
                evidence.update({"completed_heal_objects": 1, "duplicate_task_count": 0})
            if gate == "P3" and field == "recovery_window_measurement":
                evidence.update({"pressure_recovery_window_seconds": 5, "lock_hold_p95_ms": 0})
            if gate in ("G03", "G09", "R-L"):
                evidence["versions"] = [baseline_revision, source_revision]
                evidence["mixed_version_role"] = SCANNER_HEAL_RELEASE_MIXED_VERSION_ROLES[(gate, field)]
            if gate in ("G04", "G07", "R-E", "R-L"):
                evidence["crash_points"] = ["fixture-before-commit"]
            if (gate, field) in SCANNER_HEAL_RELEASE_MRF_DURABLE_REPLAY_FIELDS:
                evidence["replayed_records"] = 1
                evidence["responsibility_anchor_retained"] = True
                evidence["successor_snapshot_published"] = True
            if gate == "G14" and field == "ec8_4_evidence":
                evidence["topology"] = {"erasure": "EC8+4", "nodes": 3, "drives_per_node": 4}
            if gate == "G14" and field == "same_window_field_evidence":
                evidence["same_window_fields"] = [
                    item
                    for item in SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS["G14"]
                    if item != "same_window_field_evidence"
                ]
            if gate == "G14" and field == "multi_set_evidence":
                evidence["sets"] = 2
            if gate == "G14" and field == "multi_pool_evidence":
                evidence["pools"] = 2
            if field == "profile_evidence":
                evidence["resolved_samples"] = 1
                artifacts = {}
                for artifact_kind in RELEASE_PROFILE_ARTIFACTS:
                    profile_artifact = artifact_dir / f"{gate}-{field}-{artifact_kind}.json"
                    write_json(profile_artifact, {
                        "schema": 1,
                        "fixture_only": True,
                        "gate": gate,
                        "field": field,
                        "artifact": artifact_kind,
                        "source_revision": source_revision,
                    })
                    artifacts[artifact_kind] = {
                        "artifact": profile_artifact.relative_to(bundle_dir).as_posix(),
                        "sha256": digest(profile_artifact),
                        "artifact_format": "json",
                        "source_revision": source_revision,
                        "run_id": evidence["run_id"],
                        "measurement_window_id": evidence["measurement_window_id"],
                    }
                evidence["profile_artifacts"] = artifacts
            fields[field] = evidence
        gates[gate] = {
            "status": "pass",
            "lane": requirement["lane"],
            "evidence_type": "measured",
            "evidence_fields": fields,
        }
    bundle = bundle_dir / "release-evidence.fixture.json"
    write_json(bundle, {
        "schema": 1,
        "fixture_only": True,
        "evidence": "measured",
        "source_revision": source_revision,
        "gates": gates,
    })
    return bundle


def validate(root: Path) -> list[str]:
    errors: list[str] = []
    errors.extend(check_core_fixtures(root))
    errors.extend(check_e2e_modules(root))
    errors.extend(check_vault_test_groups(root))
    errors.extend(check_ilm_build_budget(root))
    errors.extend(check_fuzz_targets(root))
    errors.extend(check_runner_selection(root))
    errors.extend(check_s3_tests_runner(root))
    errors.extend(check_workflow_readiness(root))
    errors.extend(check_profile_definitions(root))
    errors.extend(check_scheduled_alerts(root))
    errors.extend(check_quick_checks(root))
    return errors


class SelfTests(unittest.TestCase):
    def test_quick_checks_rejects_caller_and_execution_bypasses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            caller = (
                "jobs:\n  quick-checks:\n    steps:\n"
                "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                "        with:\n          persist-credentials: false\n"
                "      - uses: ./.github/actions/quick-checks\n"
            )
            action = (
                "runs:\n  using: composite\n  steps:\n"
                "    - uses: taiki-e/install-action@pinned\n"
                "      with:\n        tool: actionlint@1.7.12\n"
                "    - name: Lint workflows\n      shell: bash\n      run: shellcheck --version && actionlint\n"
                "    - name: Error format ratchet\n      shell: bash\n"
                "      run: ./scripts/check_error_other_format_ratchet.sh\n"
                "    - name: Script tests\n      shell: bash\n      run: make script-tests\n"
            )
            sources = {
                ".github/workflows/ci.yml": caller.replace(
                    "    steps:", "    if: github.event_name != 'pull_request' || github.event.action != 'closed'\n    steps:"
                ),
                ".github/actions/quick-checks/action.yml": action,
            }
            for relative, source in sources.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source)
            self.assertEqual(check_quick_checks(root), [])
            for relative in (".github/workflows/ci.yml",):
                source = sources[relative]
                mutations = {
                    "different action": source.replace("./.github/actions/quick-checks", "./.github/actions/other"),
                    "conditional call": source + "        if: false\n",
                    "ignored call failure": source + "        continue-on-error: true\n",
                    "conditional checkout": source.replace("        with:", "        if: false\n        with:"),
                    "ignored job failure": source.replace("    steps:", "    continue-on-error: true\n    steps:"),
                    "changed job condition": (
                        source.replace("github.event_name != 'pull_request' || github.event.action != 'closed'", "false")
                        if relative.endswith("/ci.yml") else source.replace("    steps:", "    if: false\n    steps:")
                    ),
                    "persisted credentials": source.replace("persist-credentials: false", "persist-credentials: true"),
                    "late checkout": source.replace("      - uses: ./.github/actions/quick-checks\n", "").replace(
                        "    steps:\n", "    steps:\n      - uses: ./.github/actions/quick-checks\n"
                    ),
                    "missing job": source.replace("  quick-checks:", "  other-checks:"),
                }
                for key in ("'if' : false", '"if": false', "'continue-on-error': true", '"continue-on-error" : true'):
                    mutations[f"quoted call {key}"] = source + f"        {key}\n"
                    mutations[f"quoted checkout {key}"] = source.replace("        with:", f"        {key}\n        with:")
                    job_source = source.replace(
                        "    if: github.event_name != 'pull_request' || github.event.action != 'closed'\n", ""
                    ) if "if" in key else source
                    mutations[f"quoted job {key}"] = job_source.replace("    steps:", f"    {key}\n    steps:")
                for dependency in ("needs: prerequisite", "needs: [prerequisite]", "needs:\n      - prerequisite", "'needs' : [prerequisite]", '"needs": [prerequisite]'):
                    for condition in ("false", "true"):
                        prerequisite = f"\n  prerequisite:\n    if: {condition}\n    runs-on: ubuntu-latest\n    steps:\n      - run: exit 1\n"
                        mutations[f"job dependency {dependency} if {condition}"] = source.replace("    steps:", f"    {dependency}\n    steps:") + prerequisite
                if relative.endswith("/ci.yml"):
                    for separator in ("", "\n", "      # continued condition\n"):
                        mutations[f"continued job condition {separator!r}"] = source.replace(
                            "    steps:", f"{separator}      && false\n    steps:"
                        )
                for case, mutated in mutations.items():
                    with self.subTest(path=relative, case=case):
                        (root / relative).write_text(mutated)
                        self.assertTrue(check_quick_checks(root))
                (root / relative).write_text(source)
            relative = ".github/actions/quick-checks/action.yml"
            mutations = {
                "not composite": action.replace("using: composite", "using: node24"),
                "only installed actionlint": action.replace("run: shellcheck --version && actionlint", "run: echo actionlint"),
                "missing shellcheck preflight": action.replace("shellcheck --version && ", ""),
                "missing ratchet": action.replace("run: ./scripts/check_error_other_format_ratchet.sh", "run: echo skipped"),
                "missing script tests": action.replace("run: make script-tests", "run: echo skipped"),
                "swallowed script failure": action.replace("run: make script-tests", "run: make script-tests || true"),
                "swallowed lint failure": action.replace("&& actionlint", "&& actionlint || true"),
                "swallowed ratchet failure": action.replace("ratchet.sh", "ratchet.sh || true"),
                "conditional lint": action.replace("run: shellcheck", "if: false\n      run: shellcheck"),
                "ignored ratchet failure": action.replace("run: ./scripts/", "continue-on-error: true\n      run: ./scripts/"),
                "non-failing shell": action.replace("shell: bash", "shell: bash {0}"),
                "run text in step name": action.replace(
                    "name: Lint workflows", "name: |\n        run: shellcheck --version && actionlint"
                ).replace("\n      run: shellcheck --version && actionlint\n", "\n      run: shellcheck --version && actionlint\n        || true\n"),
            }
            for command in ("shellcheck --version && actionlint", "./scripts/check_error_other_format_ratchet.sh", "make script-tests"):
                for key in ("'if' : false", '"if": false', "'continue-on-error': true", '"continue-on-error" : true'):
                    mutations[f"quoted {command} {key}"] = action.replace(f"run: {command}", f"{key}\n      run: {command}")
                for separator in ("", "\n", "        # continued command\n"):
                    mutations[f"continued {command} {separator!r}"] = action.replace(
                        f"run: {command}\n", f"run: {command}\n{separator}        || true\n"
                    )
            for case, mutated in mutations.items():
                with self.subTest(case=case):
                    (root / relative).write_text(mutated)
                    self.assertTrue(check_quick_checks(root))
            (root / relative).unlink()
            self.assertTrue(check_quick_checks(root))

    def test_quick_checks_commands_propagate_failure(self) -> None:
        runs = yaml_block((ROOT / ".github/actions/quick-checks/action.yml").read_text().splitlines(), "runs", 0)
        steps = yaml_block(runs or [], "steps", 2) or []
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            commands = ("shellcheck", "actionlint", "./scripts/check_error_other_format_ratchet.sh")
            (root / "Makefile").write_text(".PHONY: script-tests\nscript-tests:\n\texit 17\n")
            for failing in (*commands, "make script-tests"):
                with self.subTest(command=failing):
                    run = "shellcheck --version && actionlint" if failing in ("shellcheck", "actionlint") else failing
                    step = workflow_step_block(steps, run, key="run", indent=4)
                    self.assertIsNotNone(step)
                    run_index = next(index for index, line in enumerate(step[1]) if line.startswith("      run:"))
                    self.assertFalse(yaml_scalar_continues(step[1], run_index, 6))
                    body = step[1][run_index].removeprefix("      run: ")
                    for command in commands:
                        shim = root / command
                        shim.write_text(f"#!/bin/sh\nexit {17 if command == failing else 0}\n")
                        shim.chmod(0o755)
                    result = subprocess.run(
                        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", body],
                        cwd=root, env=dict(os.environ, PATH=f"{root}{os.pathsep}{os.environ['PATH']}"),
                        capture_output=True, text=True,
                    )
                    self.assertEqual(result.returncode, 2 if failing == "make script-tests" else 17, result.stderr)

    def test_validate_includes_quick_checks(self) -> None:
        error = "Quick Checks wiring regression"
        with mock.patch(__name__ + ".check_quick_checks", return_value=[error]):
            self.assertIn(error, validate(ROOT))

    def scanner_heal_fixture(self, directory: Path) -> tuple[Path, Path]:
        """Parser fixtures only; these files are never runtime evidence."""
        root, run_dir = directory / "repo", directory / "run"
        (root / ".config").mkdir(parents=True)
        run_dir.mkdir()
        registry = read_json(ROOT / ".config/scanner-heal-required-tests.json")
        write_json(root / ".config/scanner-heal-required-tests.json", registry)
        requirements = registry["cases"]
        binary = directory / "fake-binary"
        binary.write_bytes(b"parser fixture, not a real build")
        binary.chmod(0o700)
        build = {"path": str(binary), "sha256": digest(binary)}
        write_json(run_dir / "run.json", {"schema": 1, "run_id": "a" * 32, "source_revision": "b" * 40,
                                         "binary_source_revision": "b" * 40,
                                         "test_build": {"source_revision": "b" * 40, "dirty": False,
                                                        "lock_blob": "c" * 40, "features": "default"},
                                         "started_at": datetime.now(timezone.utc).timestamp() - 1,
                                         "binary": build, "test_binary": build})
        suite = "e2e_test"
        write_json(run_dir / "listing.json", {"rust-suites": {suite: {
            "binary-id": suite, "binary-path": str(binary), "package-name": "e2e_test", "build-platform": "target",
            "testcases": {
                requirement["name"]: {"ignored": False, "filter-match": {"status": "matches"}}
                for requirement in requirements.values()
            }
        }}})
        (run_dir / "junit.xml").write_text(
            "<testsuites><testsuite>"
            + "".join(
                f'<testcase name="{requirement["name"]}" classname="{requirement["suite"]}" '
                f'timestamp="{datetime.now(timezone.utc).isoformat(timespec="milliseconds")}"/>'
                for requirement in requirements.values()
            )
            + "</testsuite></testsuites>"
        )
        def oracle_objects(requirement: dict[str, object]) -> list[dict[str, object]]:
            topology = requirement["topology"]
            total_blocks = topology["nodes"] * topology["drives_per_node"]
            erasure = requirement.get("erasure")
            if erasure is None:
                parity_blocks = 4 if total_blocks == 12 else total_blocks // 2
                data_blocks = total_blocks - parity_blocks
            else:
                data_blocks = erasure["data_blocks"]
                parity_blocks = erasure["parity_blocks"]
            physical = {"has_xl_meta": True, "version_id": None, "data_dir": "data-generation",
                        "erasure_index": 1, "data_blocks": data_blocks, "parity_blocks": parity_blocks,
                        "expected_part_numbers": [1],
                        "present_part_fingerprints": {"1": {"size": 12, "sha256": "c" * 64}},
                        "inline_data_fingerprint": None}
            obj = {"key": "object", "version_id": None, "expected_bytes": 16, "actual_bytes": 16,
                   "expected_sha256": "d" * 64, "actual_sha256": "d" * 64,
                   "expected_physical": physical, "physical": physical}
            count = requirement.get("min_objects", 9)
            objects = [dict(obj, key=f"object-{index}") for index in range(count)]
            objects[-1] = dict(objects[-1], expected_physical=None)
            return objects

        for case_id, requirement in requirements.items():
            objects = oracle_objects(requirement)
            write_json(run_dir / requirement["oracle"], {
                "schema": 1, "evidence": requirement["evidence"], "case": case_id,
                "run_id": "a" * 32, "source_revision": "b" * 40,
                "test_build": {"source_revision": "b" * 40, "dirty": False, "lock_blob": "c" * 40,
                               "features": "default", "target": "aarch64-apple-darwin", "profile": "debug", "rustflags_hex": ""},
                "binary_sha256": build["sha256"], "test_binary_sha256": build["sha256"],
                "topology": requirement["topology"], "pid_before": 10, "pid_after": 11,
                "unclean_shutdown_marker": requirement["unclean_shutdown_marker"],
                "objects": objects, "node_listings": [[item["key"] for item in objects]] * requirement["topology"]["nodes"],
            })
        finish_scanner_heal_receipt(run_dir, 0, root)
        return root, run_dir

    def test_scanner_heal_case_lane_is_required_for_runner_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, _ = self.scanner_heal_fixture(Path(tmp))
            registry_path = root / ".config/scanner-heal-required-tests.json"
            registry = read_json(registry_path)
            del registry["cases"]["ec84-target-drive-restart"]["lane"]
            write_json(registry_path, registry)
            with self.assertRaisesRegex(ValueError, "invalid nextest profile lane for ec84-target-drive-restart"):
                scanner_heal_oracle_names(root)

    def scanner_heal_release_bundle_fixture(self, directory: Path) -> tuple[Path, Path]:
        """Parser fixtures only; the bundle is not runtime evidence."""
        root, _ = self.scanner_heal_fixture(directory)
        bundle_dir = directory / "bundle"
        artifact_dir = bundle_dir / "artifacts"
        artifact_dir.mkdir(parents=True)
        registry = read_json(root / ".config/scanner-heal-required-tests.json")
        requirements, _, _ = scanner_heal_release_requirements(registry)
        gates = {}
        source_revision = "b" * 40
        started = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)
        for gate, requirement in requirements.items():
            fields = {}
            for field in SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS[gate]:
                artifact = artifact_dir / f"{gate}-{field}.json"
                run_id = f"{gate.lower()}-{field.replace('_', '-')}-run"
                window_id = f"{gate.lower()}-window"
                duration = 60
                evidence = {
                    "artifact": artifact.relative_to(bundle_dir).as_posix(),
                    "evidence_type": "measured",
                    "source_revision": source_revision,
                    "run_id": run_id,
                    "measurement_window_id": window_id,
                    "started_at": started.isoformat().replace("+00:00", "Z"),
                    "command": ["cargo", "nextest", "run", requirement["description"]],
                    "artifact_format": "json",
                    "summary": f"parser fixture for {gate}.{field}",
                }
                if gate.startswith("P"):
                    duration = 900
                    evidence["duration_seconds"] = duration
                if gate == "P3" and field == "two_hour_pressure_measurement":
                    duration = 7200
                    evidence["duration_seconds"] = duration
                if gate == "P4" and field == "mrf_cleanup_gc_soak_evidence":
                    duration = 7200
                    evidence["duration_seconds"] = duration
                evidence["finished_at"] = (started + timedelta(seconds=duration)).isoformat().replace("+00:00", "Z")
                if gate == "G10" and field == "scheduler_bound_evidence":
                    evidence.update({
                        "max_deferred_items": 256,
                        "max_deferred_bytes": 262144,
                        "max_retry_age_seconds": 30,
                        "duplicate_task_count": 0,
                    })
                if gate == "G10" and field == "pressure_recovery_evidence":
                    evidence.update({
                        "pressure_pacing_engaged": True,
                        "recovery_window_seconds": 5,
                        "lock_hold_p95_ms": 0,
                        "foreground_latency_p95_ms": 10,
                    })
                if gate == "P1" and field == "foreground_latency_throughput_measurement":
                    evidence.update({
                        "foreground_latency_p95_ms": 10,
                        "foreground_latency_p99_ms": 20,
                        "throughput_ops_per_second": 100,
                        "error_count": 0,
                    })
                if gate == "P3" and field == "two_hour_pressure_measurement":
                    evidence.update({
                        "fixed_offered_load": True,
                        "foreground_latency_p99_ms": 20,
                        "attempt_cost_samples": 1,
                    })
                if gate == "P3" and field == "heal_capacity_measurement":
                    evidence.update({"completed_heal_objects": 1, "duplicate_task_count": 0})
                if gate == "P3" and field == "recovery_window_measurement":
                    evidence.update({"pressure_recovery_window_seconds": 5, "lock_hold_p95_ms": 0})
                if gate in ("G03", "G09", "R-L"):
                    evidence["versions"] = ["a" * 40, source_revision]
                    evidence["mixed_version_role"] = SCANNER_HEAL_RELEASE_MIXED_VERSION_ROLES[(gate, field)]
                if gate in ("G04", "G07", "R-E", "R-L"):
                    evidence["crash_points"] = ["before-commit"]
                if gate == "G03":
                    evidence["scoped_ack_cases"] = list(SCANNER_HEAL_RELEASE_SCOPED_ACK_CASES[field])
                    if field == "durable_root_publication_proof":
                        evidence["root_cas_observed"] = True
                        evidence["root_readback_observed"] = True
                    if field == "scoped_ack_request_identity":
                        evidence["whole_cycle_fallback_observed"] = True
                if gate == "G04" and field == "root_floor_intent_crash_evidence":
                    evidence["durable_intent_cases"] = list(SCANNER_HEAL_RELEASE_CRASH_BOUNDARY_FIELDS[(gate, field)])
                    evidence["persist_failure_blocks_acceptance"] = True
                if gate == "G09":
                    evidence["mixed_version_cases"] = list(SCANNER_HEAL_RELEASE_MIXED_VERSION_CASES[field])
                    if field == "rollback_payload_evidence":
                        evidence["rollback_payload_replayed"] = True
                if (gate, field) in SCANNER_HEAL_RELEASE_MRF_DURABLE_REPLAY_FIELDS:
                    evidence["replayed_records"] = 2
                    evidence["responsibility_anchor_retained"] = True
                    evidence["successor_snapshot_published"] = True
                if gate == "P4" and field == "mrf_cleanup_gc_soak_evidence":
                    evidence["cleanup_gc_cases"] = list(SCANNER_HEAL_RELEASE_MRF_CLEANUP_GC_SOAK_CASES)
                    evidence["verified_idle_gc_observed"] = True
                    evidence["pending_responsibilities_after_gc"] = 0
                    evidence["stale_journals_after_gc"] = 0
                if gate == "G07":
                    case_field = {
                        "mrf_responsibility_oracle": "mrf_responsibility_cases",
                        "commit_boundary_crash_matrix": "commit_crash_cases",
                    }[field]
                    evidence[case_field] = list(SCANNER_HEAL_RELEASE_G07_REQUIRED_CASES[field])
                if gate == "G08":
                    case_field = {
                        "mrf_capacity_evidence": "capacity_cases",
                        "disk_full_matrix": "disk_full_cases",
                        "replica_loss_matrix": "replica_loss_cases",
                    }[field]
                    evidence[case_field] = list(SCANNER_HEAL_RELEASE_G08_REQUIRED_CASES[field])
                if gate == "G10" and field == "scheduler_bound_evidence":
                    evidence["scheduler_bounds"] = list(SCANNER_HEAL_RELEASE_SCHEDULER_BOUNDS)
                    evidence["duplicate_task_bound_observed"] = True
                if gate == "G10" and field == "pressure_recovery_evidence":
                    evidence["pressure_metrics"] = {
                        "foreground_p95_ms": 40.0,
                        "foreground_p99_ms": 90.0,
                        "throughput_ops": 250.0,
                        "error_rate": 0.0,
                        "heal_lock_wait_p99_ms": 25.0,
                        "attempt_cost_samples": 8.0,
                        "foreground_pressure_samples": 120,
                        "foreground_pressure_high_samples": 12,
                    }
                if gate == "G14" and field == "ec8_4_evidence":
                    evidence["topology"] = {"erasure": "EC8+4", "nodes": 3, "drives_per_node": 4}
                if gate == "G14" and field == "same_window_field_evidence":
                    evidence["same_window_fields"] = [
                        item
                        for item in SCANNER_HEAL_RELEASE_BUNDLE_REQUIRED_EVIDENCE_FIELDS["G14"]
                        if item != "same_window_field_evidence"
                    ]
                if gate == "G14" and field == "multi_set_evidence":
                    evidence["sets"] = 2
                if gate == "G14" and field == "multi_pool_evidence":
                    evidence["pools"] = 2
                if gate == "G14" and field == "distributed_segment_invalidation_evidence":
                    evidence["invalidation_domain"] = "distributed-ec"
                    evidence["distributed_ec_invalidation"] = True
                    evidence["peer_count"] = 3
                    evidence["same_window_remote_proof"] = True
                    evidence["all_peers_bound_to_generation_window"] = True
                if field == "segment_activation_preflight":
                    evidence["production_activation"] = False
                    evidence["scanner_segment_reuse_activated"] = False
                    evidence["proof_inputs"] = list(SCANNER_HEAL_SEGMENT_ACTIVATION_PROOF_INPUTS)
                    evidence["fail_closed_checks"] = list(SCANNER_HEAL_SEGMENT_ACTIVATION_FAIL_CLOSED_CHECKS)
                if field == "cold_segment_reuse_measurement":
                    evidence["hot_walked_segments"] = 2
                    evidence["cold_walked_segments"] = 0
                    evidence["full_walk_oracle_equivalent"] = True
                    evidence["published_root_equivalent"] = True
                if gate == "P4" and field == "retained_responsibility_evidence":
                    evidence["duration_seconds"] = 7200
                    evidence["finished_at"] = (started + timedelta(seconds=7200)).isoformat().replace("+00:00", "Z")
                    evidence["retained_responsibility_cases"] = list(
                        SCANNER_HEAL_RELEASE_P4_RETAINED_RESPONSIBILITY_CASES
                    )
                    evidence["retention_window_seconds"] = 7200
                    evidence["idle_cleanup_observed"] = True
                    evidence["verified_proof_discharge_observed"] = True
                if field == "profile_evidence":
                    evidence["resolved_samples"] = 1
                    evidence["allocation_bytes"] = 1024
                    evidence["rss_peak_bytes"] = 4096
                    evidence["save_operations"] = 2
                    evidence["saved_bytes"] = 2048
                    artifacts = {}
                    for artifact_kind in RELEASE_PROFILE_ARTIFACTS:
                        profile_artifact = artifact_dir / f"{gate}-{field}-{artifact_kind}.json"
                        write_json(profile_artifact, {
                            "schema": 1,
                            "evidence_type": "measured",
                            "source_revision": source_revision,
                            "run_id": run_id,
                            "measurement_window_id": window_id,
                            "gate": gate,
                            "field": field,
                            "artifact_kind": artifact_kind,
                        })
                        artifacts[artifact_kind] = {
                            "artifact": profile_artifact.relative_to(bundle_dir).as_posix(),
                            "sha256": digest(profile_artifact),
                            "artifact_format": "json",
                            "source_revision": source_revision,
                            "run_id": run_id,
                            "measurement_window_id": window_id,
                        }
                    evidence["profile_artifacts"] = artifacts
                if gate == "P1" and field == "cold_walk_share_measurement":
                    evidence["cold_walk_share"] = 0.5
                    evidence["walk_objects"] = 100
                    evidence["cold_walk_objects"] = 50
                if gate == "P1" and field == "foreground_latency_throughput_measurement":
                    evidence["foreground_p95_ms"] = 50.0
                    evidence["foreground_p99_ms"] = 100.0
                    evidence["throughput_ops"] = 200.0
                    evidence["error_rate"] = 0.0
                if gate == "P3" and field == "two_hour_pressure_measurement":
                    evidence["abba_legs"] = ["A1", "B1", "B2", "A2"]
                    evidence["scenarios"] = list(SCENARIOS)
                    evidence["foreground_p95_ms"] = 50.0
                    evidence["foreground_p99_ms"] = 100.0
                    evidence["throughput_ops"] = 200.0
                if gate == "P3" and field == "heal_capacity_measurement":
                    evidence["heal_capacity"] = {
                        "objects": 100,
                        "versions": 100,
                        "bytes": 1048576,
                        "completed_objects": 100,
                    }
                if gate == "P3" and field == "recovery_window_measurement":
                    evidence["fault_modes"] = ["process-restart", "process-crash-restart"]
                    evidence["recovery_p95_ms"] = 500.0
                    evidence["recovery_p99_ms"] = 1000.0
                artifact_payload = {
                    "schema": 1,
                    "evidence_type": "measured",
                    "source_revision": source_revision,
                    "run_id": run_id,
                    "measurement_window_id": window_id,
                    "gate": gate,
                    "field": field,
                }
                mrf_artifact_kind = SCANNER_HEAL_RELEASE_MRF_ARTIFACT_KINDS.get((gate, field))
                if mrf_artifact_kind is not None:
                    artifact_payload["artifact_kind"] = mrf_artifact_kind
                for mirror_field in release_bundle_json_artifact_mirrored_fields(gate, field):
                    artifact_payload[mirror_field] = evidence[mirror_field]
                write_json(artifact, artifact_payload)
                evidence["sha256"] = digest(artifact)
                fields[field] = evidence
            gates[gate] = {
                "status": "pass",
                "lane": requirement["lane"],
                "evidence_type": "measured",
                "evidence_fields": fields,
            }
        bundle = bundle_dir / "release-evidence.json"
        write_json(bundle, {"schema": 1, "evidence": "measured", "source_revision": source_revision, "gates": gates})
        return root, bundle

    def test_scanner_heal_release_bundle_accepts_complete_measured_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
            with mock.patch("subprocess.check_output", return_value="b" * 40):
                status = scanner_heal_release_bundle_status(root, bundle)
            self.assertEqual(status["decision"], "approved")
            self.assertTrue(status["release_approved"])
            self.assertEqual(len(status["verified_gates"]), len(SCANNER_HEAL_RELEASE_REQUIRED_GATES))
            self.assertEqual(status["pending_gates"], [])
            self.assertEqual(status["pending_lanes"], [])

    def test_scanner_heal_release_bundle_assembler_copies_measured_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, descriptor = self.scanner_heal_release_bundle_fixture(Path(tmp))
            with mock.patch("subprocess.check_output", return_value="b" * 40):
                bundle, status = assemble_scanner_heal_release_bundle(root, descriptor, Path(tmp) / "assembled")
            self.assertEqual(status["decision"], "approved")
            self.assertTrue(status["release_approved"])
            assembled = read_json(bundle)
            evidence = assembled["gates"]["G01"]["evidence_fields"]["root_authority_evidence"]
            self.assertEqual(evidence["artifact"], "artifacts/G01-root_authority_evidence.json")
            self.assertEqual(evidence["sha256"], digest(bundle.parent / evidence["artifact"]))
            self.assertTrue((bundle.parent / evidence["artifact"]).is_file())

    def test_scanner_heal_release_bundle_gate_accepts_single_measured_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
            data = read_json(bundle)
            data["gates"] = {"G09": data["gates"]["G09"]}
            write_json(bundle, data)
            with mock.patch("subprocess.check_output", return_value="b" * 40):
                status = scanner_heal_release_bundle_gate_status(root, bundle, "G09")
                full_status = scanner_heal_release_bundle_status(root, bundle)
            self.assertEqual(status["decision"], "verified")
            self.assertFalse(status["release_approved"])
            self.assertEqual(status["verified_gate"], "G09")
            self.assertIn("G07", status["pending_gates"])
            self.assertEqual(full_status["decision"], "blocked")
            self.assertFalse(full_status["release_approved"])

    def test_scanner_heal_release_bundle_gate_rejects_missing_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
            data = read_json(bundle)
            del data["gates"]["G09"]["evidence_fields"]["rollback_payload_evidence"]
            write_json(bundle, data)
            with mock.patch("subprocess.check_output", return_value="b" * 40):
                status = scanner_heal_release_bundle_gate_status(root, bundle, "G09")
            self.assertEqual(status["decision"], "blocked")
            self.assertFalse(status["release_approved"])
            self.assertIn("missing required fields: rollback_payload_evidence", status["rejected_errors"])

    def test_scanner_heal_release_bundle_gate_rejects_unknown_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
            data = read_json(bundle)
            data["gates"] = {"G09": data["gates"]["G09"], "Z99": {"status": "pass"}}
            write_json(bundle, data)
            with mock.patch("subprocess.check_output", return_value="b" * 40):
                with self.assertRaisesRegex(ValueError, "unknown gates: Z99"):
                    scanner_heal_release_bundle_gate_status(root, bundle, "G09")

    def test_scanner_heal_release_bundle_assembler_rejects_fixture_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, _ = self.scanner_heal_fixture(Path(tmp))
            with mock.patch("subprocess.check_output", return_value="b" * 40):
                descriptor = write_scanner_heal_release_bundle_fixture(root, Path(tmp) / "bundle")
                with self.assertRaisesRegex(ValueError, "fixture_only"):
                    assemble_scanner_heal_release_bundle(root, descriptor, Path(tmp) / "assembled")

    def test_scanner_heal_release_bundle_assembler_rejects_hash_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, descriptor = self.scanner_heal_release_bundle_fixture(Path(tmp))
            data = read_json(descriptor)
            data["gates"]["G01"]["evidence_fields"]["root_authority_evidence"]["sha256"] = "0" * 64
            write_json(descriptor, data)

            with self.assertRaisesRegex(ValueError, "descriptor artifact hash mismatch"):
                assemble_scanner_heal_release_bundle(root, descriptor, Path(tmp) / "assembled")

    def test_scanner_heal_release_bundle_fixture_writer_is_not_release_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, _ = self.scanner_heal_fixture(Path(tmp))
            with mock.patch("subprocess.check_output", return_value="b" * 40):
                bundle = write_scanner_heal_release_bundle_fixture(root, Path(tmp) / "bundle")
                with self.assertRaisesRegex(ValueError, "fixture_only"):
                    scanner_heal_release_bundle_status(root, bundle)
            data = read_json(bundle)
            self.assertTrue(data["fixture_only"])
            self.assertEqual(data["source_revision"], "b" * 40)

    def test_scanner_heal_release_bundle_rejects_synthetic_or_missing_fields(self) -> None:
        for fault in ("synthetic", "missing-field", "hash", "empty-artifact"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
                data = read_json(bundle)
                if fault == "synthetic":
                    data["evidence"] = "synthetic"
                elif fault == "missing-field":
                    del data["gates"]["G09"]["evidence_fields"]["rollback_payload_evidence"]
                elif fault == "hash":
                    artifact = bundle.parent / data["gates"]["G01"]["evidence_fields"]["root_authority_evidence"]["artifact"]
                    artifact.write_text(artifact.read_text(encoding="utf-8") + "\n", encoding="utf-8")
                else:
                    artifact = bundle.parent / data["gates"]["G01"]["evidence_fields"]["root_authority_evidence"]["artifact"]
                    artifact.write_text("", encoding="utf-8")
                write_json(bundle, data)

                with mock.patch("subprocess.check_output", return_value="b" * 40):
                    if fault == "synthetic":
                        with self.assertRaisesRegex(ValueError, "must be measured"):
                            scanner_heal_release_bundle_status(root, bundle)
                    else:
                        status = scanner_heal_release_bundle_status(root, bundle)
                if fault != "synthetic":
                    self.assertEqual(status["decision"], "blocked")
                    self.assertFalse(status["release_approved"])
                    self.assertTrue(status["rejected_gates"])

    def test_scanner_heal_release_bundle_enforces_topology_duration_profile_and_versions(self) -> None:
        for fault, gate, field, mutation, expected in (
            ("topology", "G14", "ec8_4_evidence", lambda item: item.update({"topology": {"erasure": "EC4+2", "nodes": 2, "drives_per_node": 3}}), "EC8+4"),
            ("missing-duration", "P1", "cold_walk_share_measurement", lambda item: item.pop("duration_seconds"), "duration_seconds"),
            ("duration", "P3", "two_hour_pressure_measurement", lambda item: item.update({"duration_seconds": 7199}), "two hours"),
            ("mrf-cleanup-soak-duration", "P4", "mrf_cleanup_gc_soak_evidence", lambda item: item.update({"duration_seconds": 7199}), "two hours"),
            ("profile", "P1", "profile_evidence", lambda item: item.pop("resolved_samples"), "resolved_samples"),
            (
                "profile-artifact",
                "P1",
                "profile_evidence",
                lambda item: item["profile_artifacts"].pop("flamegraph"),
                "missing profile artifacts",
            ),
            (
                "profile-artifact-hash",
                "P1",
                "profile_evidence",
                lambda item: item["profile_artifacts"]["rss-samples"].update({"sha256": "0" * 64}),
                "artifact hash mismatch",
            ),
            (
                "profile-artifact-source",
                "P1",
                "profile_evidence",
                lambda item: item["profile_artifacts"]["allocation-profile"].update({"source_revision": "c" * 40}),
                "source revision mismatch",
            ),
            (
                "profile-artifact-run",
                "P1",
                "profile_evidence",
                lambda item: item["profile_artifacts"]["flamegraph"].update({"run_id": "p1-different-profile-run"}),
                "run_id mismatch",
            ),
            (
                "profile-artifact-window",
                "P1",
                "profile_evidence",
                lambda item: item["profile_artifacts"]["save-frequency"].update(
                    {"measurement_window_id": "p1-different-window"}
                ),
                "measurement window mismatch",
            ),
            ("versions", "G09", "mixed_version_reader_evidence", lambda item: item.update({"versions": [1, 2]}), "mixed-version"),
            ("stale-versions", "G09", "mixed_version_writer_evidence", lambda item: item.update({"versions": ["a" * 40, "c" * 40]}), "tested source revision"),
            ("g08-capacity-cases", "G08", "mrf_capacity_evidence", lambda item: item.update({"capacity_cases": ["queue-count-limit"]}), "missing cases"),
            (
                "g08-disk-full-cases",
                "G08",
                "disk_full_matrix",
                lambda item: item["disk_full_cases"].remove("cleanup-delete-enospc"),
                "missing cases",
            ),
            ("g08-replica-loss-cases", "G08", "replica_loss_matrix", lambda item: item.update({"replica_loss_cases": ["single-replica-loss"]}), "missing cases"),
            ("g07-responsibility-cases", "G07", "mrf_responsibility_oracle", lambda item: item.update({"mrf_responsibility_cases": ["legacy-journal-replay"]}), "missing cases"),
            ("g07-crash-cases", "G07", "commit_boundary_crash_matrix", lambda item: item.pop("commit_crash_cases"), "non-empty string list"),
            ("mrf-records", "G07", "mrf_responsibility_oracle", lambda item: item.pop("replayed_records"), "replayed_records"),
            ("mrf-anchor", "G07", "commit_boundary_crash_matrix", lambda item: item.update({"responsibility_anchor_retained": False}), "retained MRF responsibility anchors"),
            ("mrf-successor", "P4", "retained_responsibility_evidence", lambda item: item.pop("successor_snapshot_published"), "successor snapshot"),
            (
                "mrf-cleanup-gc-cases",
                "P4",
                "mrf_cleanup_gc_soak_evidence",
                lambda item: item["cleanup_gc_cases"].remove("repeated-replay-no-resurrection"),
                "cleanup_gc_cases missing cases",
            ),
            (
                "mrf-cleanup-gc-idle",
                "P4",
                "mrf_cleanup_gc_soak_evidence",
                lambda item: item.update({"verified_idle_gc_observed": False}),
                "verified idle GC",
            ),
            (
                "mrf-cleanup-gc-pending",
                "P4",
                "mrf_cleanup_gc_soak_evidence",
                lambda item: item.update({"pending_responsibilities_after_gc": 1}),
                "zero pending responsibilities",
            ),
            (
                "mrf-cleanup-gc-stale",
                "P4",
                "mrf_cleanup_gc_soak_evidence",
                lambda item: item.update({"stale_journals_after_gc": 1}),
                "zero stale journals",
            ),
            ("same-window-fields", "G14", "same_window_field_evidence", lambda item: item.update({"same_window_fields": ["ec8_4_evidence", "multi_set_evidence"]}), "missing fields"),
            ("activation-enabled", "G11", "segment_activation_preflight", lambda item: item.update({"production_activation": True}), "production activation disabled"),
            (
                "activation-missing-fail-closed",
                "G11",
                "segment_activation_preflight",
                lambda item: item["fail_closed_checks"].remove("overflow"),
                "missing G11.segment_activation_preflight.fail_closed_checks",
            ),
            (
                "activation-missing-proof-input",
                "G11",
                "segment_activation_preflight",
                lambda item: item["proof_inputs"].remove("process_epoch"),
                "missing G11.segment_activation_preflight.proof_inputs",
            ),
            (
                "distributed-invalidation",
                "G14",
                "distributed_segment_invalidation_evidence",
                lambda item: item.update({"distributed_ec_invalidation": False}),
                "peer invalidation proof",
            ),
            (
                "distributed-peer-window",
                "G14",
                "distributed_segment_invalidation_evidence",
                lambda item: item.update({"all_peers_bound_to_generation_window": False}),
                "peer generation-window binding",
            ),
            (
                "cold-segment-walk",
                "P2",
                "cold_segment_reuse_measurement",
                lambda item: item.update({"cold_walked_segments": 1}),
                "cold_walked_segments",
            ),
            (
                "cold-segment-oracle",
                "P2",
                "cold_segment_reuse_measurement",
                lambda item: item.update({"full_walk_oracle_equivalent": False}),
                "full-walk oracle equivalence",
            ),
            (
                "p4-retained-responsibility-cases",
                "P4",
                "retained_responsibility_evidence",
                lambda item: item["retained_responsibility_cases"].remove("idle-cleanup-reclaims-replay-source"),
                "retained_responsibility_cases missing cases",
            ),
            (
                "p4-retention-window",
                "P4",
                "retained_responsibility_evidence",
                lambda item: item.update({"retention_window_seconds": 7199}),
                "retention_window_seconds",
            ),
            (
                "p4-idle-cleanup",
                "P4",
                "retained_responsibility_evidence",
                lambda item: item.update({"idle_cleanup_observed": False}),
                "idle_cleanup_observed",
            ),
            (
                "p4-proof-discharge",
                "P4",
                "retained_responsibility_evidence",
                lambda item: item.pop("verified_proof_discharge_observed"),
                "verified_proof_discharge_observed",
            ),
            ("scheduler-duplicates", "G10", "scheduler_bound_evidence", lambda item: item.pop("duplicate_task_count"), "duplicate_task_count"),
            ("p1-throughput", "P1", "foreground_latency_throughput_measurement", lambda item: item.pop("throughput_ops_per_second"), "throughput_ops_per_second"),
            ("p3-fixed-load", "P3", "two_hour_pressure_measurement", lambda item: item.update({"fixed_offered_load": False}), "fixed offered load"),
        ):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
                data = read_json(bundle)
                mutation(data["gates"][gate]["evidence_fields"][field])
                write_json(bundle, data)

                with mock.patch("subprocess.check_output", return_value="b" * 40):
                    status = scanner_heal_release_bundle_status(root, bundle)
                self.assertEqual(status["decision"], "blocked")
                self.assertFalse(status["release_approved"])
                self.assertTrue(any(expected in error for error in status["rejected_gates"][gate]))

    def test_scanner_heal_release_bundle_requires_mixed_version_field_roles(self) -> None:
        for gate, field, wrong_role in (
            ("G03", "mixed_peer_ack_fallback_oracle", "mixed-version-reader"),
            ("G09", "mixed_version_reader_evidence", "mixed-version-writer"),
            ("G09", "mixed_version_writer_evidence", "mixed-version-reader"),
            ("G09", "rollback_payload_evidence", "mixed-version-reader"),
            ("R-L", "crash_safe_source_retirement_evidence", "migration-gap"),
        ):
            with self.subTest(gate=gate, field=field), tempfile.TemporaryDirectory() as tmp:
                root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
                data = read_json(bundle)
                data["gates"][gate]["evidence_fields"][field]["mixed_version_role"] = wrong_role
                write_json(bundle, data)

                with mock.patch("subprocess.check_output", return_value="b" * 40):
                    status = scanner_heal_release_bundle_status(root, bundle)
                self.assertEqual(status["decision"], "blocked")
                self.assertFalse(status["release_approved"])
                self.assertTrue(any("mixed-version role" in error for error in status["rejected_gates"][gate]))

    def test_scanner_heal_release_bundle_requires_field_provenance(self) -> None:
        for fault, mutation, expected in (
            ("source", lambda item: item.update({"source_revision": "c" * 40}), "source revision mismatch"),
            ("run-id", lambda item: item.pop("run_id"), "run_id"),
            ("window-id", lambda item: item.update({"measurement_window_id": item["run_id"]}), "separate run"),
            ("started-at", lambda item: item.update({"started_at": "not-a-time"}), "timestamp"),
            ("finished-at", lambda item: item.update({"finished_at": "2026-09-07T00:00:00Z"}), "timestamps are inverted"),
            ("command", lambda item: item.update({"command": []}), "command provenance"),
            ("artifact-format", lambda item: item.pop("artifact_format"), "artifact_format"),
        ):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
                data = read_json(bundle)
                mutation(data["gates"]["G01"]["evidence_fields"]["root_authority_evidence"])
                write_json(bundle, data)

                with mock.patch("subprocess.check_output", return_value="b" * 40):
                    status = scanner_heal_release_bundle_status(root, bundle)
                self.assertEqual(status["decision"], "blocked")
                self.assertFalse(status["release_approved"])
                self.assertTrue(any(expected in error for error in status["rejected_gates"]["G01"]), fault)

    def test_scanner_heal_release_bundle_requires_json_artifact_provenance(self) -> None:
        for fault, mutation, evidence_path, expected in (
            (
                "summary-window",
                lambda payload: payload.update({"measurement_window_id": "p1-stale-window"}),
                ("P1", "profile_evidence"),
                "JSON artifact measurement window mismatch",
            ),
            (
                "summary-source",
                lambda payload: payload.update({"source_revision": "c" * 40}),
                ("G09", "mixed_version_reader_evidence"),
                "JSON artifact source revision mismatch",
            ),
            (
                "profile-kind",
                lambda payload: payload.update({"artifact_kind": "flamegraph"}),
                ("P1", "profile_evidence", "allocation-profile"),
                "JSON artifact kind mismatch",
            ),
            (
                "profile-run",
                lambda payload: payload.update({"run_id": "p1-different-profile-run"}),
                ("P1", "profile_evidence", "rss-samples"),
                "JSON artifact run_id mismatch",
            ),
            (
                "fixture-marker",
                lambda payload: payload.update({"fixture": True}),
                ("G08", "disk_full_matrix"),
                "JSON artifact is fixture",
            ),
            (
                "mrf-artifact-kind",
                lambda payload: payload.update({"artifact_kind": "generic-json"}),
                ("G08", "disk_full_matrix"),
                "JSON artifact kind must be mrf-disk-full-enospc-matrix",
            ),
            (
                "g08-case-mirror",
                lambda payload: payload["disk_full_cases"].remove("manifest-write-enospc"),
                ("G08", "disk_full_matrix"),
                "JSON artifact missing cases",
            ),
            (
                "p4-gc-mirror",
                lambda payload: payload.update({"pending_responsibilities_after_gc": 1}),
                ("P4", "mrf_cleanup_gc_soak_evidence"),
                "JSON artifact requires zero pending responsibilities",
            ),
        ):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
                data = read_json(bundle)
                gate, field, *artifact_kind = evidence_path
                evidence = data["gates"][gate]["evidence_fields"][field]
                if artifact_kind:
                    item = evidence["profile_artifacts"][artifact_kind[0]]
                else:
                    item = evidence
                artifact = bundle.parent / item["artifact"]
                payload = read_json(artifact)
                mutation(payload)
                write_json(artifact, payload)
                item["sha256"] = digest(artifact)
                write_json(bundle, data)

                with mock.patch("subprocess.check_output", return_value="b" * 40):
                    status = scanner_heal_release_bundle_status(root, bundle)
                self.assertEqual(status["decision"], "blocked")
                self.assertFalse(status["release_approved"])
                self.assertTrue(any(expected in error for error in status["rejected_gates"][gate]), fault)

    def test_scanner_heal_release_bundle_requires_same_gate_measurement_window(self) -> None:
        for gate, field in (("G14", "multi_pool_evidence"), ("P1", "profile_evidence"), ("P3", "heal_capacity_measurement")):
            with self.subTest(gate=gate), tempfile.TemporaryDirectory() as tmp:
                root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
                data = read_json(bundle)
                evidence = data["gates"][gate]["evidence_fields"][field]
                evidence["measurement_window_id"] = f"{gate.lower()}-different-window"
                artifacts = [evidence]
                if "profile_artifacts" in evidence:
                    artifacts.extend(evidence["profile_artifacts"].values())
                for item in artifacts:
                    artifact = bundle.parent / item["artifact"]
                    payload = read_json(artifact)
                    payload["measurement_window_id"] = evidence["measurement_window_id"]
                    write_json(artifact, payload)
                    if "measurement_window_id" in item:
                        item["measurement_window_id"] = evidence["measurement_window_id"]
                    item["sha256"] = digest(artifact)
                write_json(bundle, data)

                with mock.patch("subprocess.check_output", return_value="b" * 40):
                    status = scanner_heal_release_bundle_status(root, bundle)
                self.assertEqual(status["decision"], "blocked")
                self.assertFalse(status["release_approved"])
                self.assertTrue(any("must share one measurement window" in error
                                    for error in status["rejected_gates"][gate]))

    def test_scanner_heal_release_bundle_requires_domain_evidence(self) -> None:
        for fault, gate, field, mutation, expected in (
            (
                "scoped-ack-cases",
                "G03",
                "scoped_ack_request_identity",
                lambda item: item["scoped_ack_cases"].remove("exact-generation"),
                "missing cases",
            ),
            (
                "scoped-ack-fallback",
                "G03",
                "scoped_ack_request_identity",
                lambda item: item.update({"whole_cycle_fallback_observed": False}),
                "whole_cycle_fallback_observed",
            ),
            (
                "durable-intent-cases",
                "G04",
                "root_floor_intent_crash_evidence",
                lambda item: item["durable_intent_cases"].remove("persist-failure-no-202"),
                "durable_intent_cases missing cases",
            ),
            (
                "durable-intent-persist",
                "G04",
                "root_floor_intent_crash_evidence",
                lambda item: item.pop("persist_failure_blocks_acceptance"),
                "persist_failure_blocks_acceptance",
            ),
            (
                "mixed-version-cases",
                "G09",
                "mixed_version_writer_evidence",
                lambda item: item["mixed_version_cases"].remove("old-reader-new-writer"),
                "mixed_version_cases missing cases",
            ),
            (
                "rollback-payload",
                "G09",
                "rollback_payload_evidence",
                lambda item: item.update({"rollback_payload_replayed": False}),
                "rollback_payload_replayed",
            ),
            (
                "scheduler-bounds",
                "G10",
                "scheduler_bound_evidence",
                lambda item: item["scheduler_bounds"].remove("lock-hold-bound"),
                "scheduler_bounds missing cases",
            ),
            (
                "pressure-metric",
                "G10",
                "pressure_recovery_evidence",
                lambda item: item["pressure_metrics"].pop("heal_lock_wait_p99_ms"),
                "heal_lock_wait_p99_ms",
            ),
            (
                "cold-walk-share",
                "P1",
                "cold_walk_share_measurement",
                lambda item: item.update({"cold_walk_share": 1.2}),
                "cold_walk_share exceeds one",
            ),
            (
                "foreground-throughput",
                "P1",
                "foreground_latency_throughput_measurement",
                lambda item: item.pop("throughput_ops"),
                "throughput_ops",
            ),
            (
                "profile-cost",
                "P1",
                "profile_evidence",
                lambda item: item.pop("save_operations"),
                "save_operations",
            ),
            (
                "abba-leg",
                "P3",
                "two_hour_pressure_measurement",
                lambda item: item["abba_legs"].remove("B2"),
                "abba_legs missing cases",
            ),
            (
                "pressure-scenario",
                "P3",
                "two_hour_pressure_measurement",
                lambda item: item["scenarios"].remove("running-heal"),
                "scenarios missing cases",
            ),
            (
                "heal-capacity",
                "P3",
                "heal_capacity_measurement",
                lambda item: item["heal_capacity"].pop("completed_objects"),
                "completed_objects",
            ),
            (
                "recovery-window",
                "P3",
                "recovery_window_measurement",
                lambda item: item["fault_modes"].remove("process-crash-restart"),
                "fault_modes missing cases",
            ),
        ):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root, bundle = self.scanner_heal_release_bundle_fixture(Path(tmp))
                data = read_json(bundle)
                mutation(data["gates"][gate]["evidence_fields"][field])
                write_json(bundle, data)

                with mock.patch("subprocess.check_output", return_value="b" * 40):
                    status = scanner_heal_release_bundle_status(root, bundle)
                self.assertEqual(status["decision"], "blocked")
                self.assertFalse(status["release_approved"])
                self.assertTrue(any(expected in error for error in status["rejected_gates"][gate]), fault)

    def test_scanner_heal_case_does_not_approve_pending_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            self.assertEqual(check_scanner_heal_evidence(root, run_dir, "background-target-restart"), [])
            errors = check_scanner_heal_evidence(root, run_dir, "release")
            self.assertEqual(len(errors), 21)
            self.assertTrue(any(error.startswith("pending R-E:") for error in errors))
            self.assertTrue(any(error.startswith("pending R-D:") for error in errors))
            self.assertTrue(any(error.startswith("pending R-L:") for error in errors))

            status = scanner_heal_release_status(root, run_dir)
            self.assertEqual(status["decision"], "blocked")
            self.assertFalse(status["release_approved"])
            self.assertTrue(status["release_schema_capable"])
            self.assertEqual(status["rejected_cases"], [])
            self.assertEqual(len(status["pending_gates"]), 21)
            self.assertIn("mixed-version-rollback", status["pending_lanes"])
            self.assertIn("ec8-4-multiset", status["pending_lanes"])
            self.assertIn("scheduler-pressure", status["pending_lanes"])

    def test_scanner_heal_case_only_schema_cannot_approve_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            registry["schema"] = 1
            registry["release_pending"] = {}
            registry.pop("release_lanes")
            registry.pop("release_requirements")
            write_json(root / ".config/scanner-heal-required-tests.json", registry)

            status = scanner_heal_release_status(root, run_dir)
            self.assertEqual(status["decision"], "blocked")
            self.assertFalse(status["release_approved"])
            self.assertFalse(status["release_schema_capable"])
            self.assertEqual(status["rejected_cases"], [])
            self.assertEqual(status["pending_gates"], [])

    def test_scanner_heal_release_requirements_cannot_be_cleared(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            registry["release_requirements"] = []
            write_json(root / ".config/scanner-heal-required-tests.json", registry)

            with self.assertRaisesRegex(ValueError, "invalid scanner/heal release requirements"):
                scanner_heal_release_status(root, run_dir)

    def test_scanner_heal_release_matrix_lanes_remain_pending_without_real_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))

            errors = check_scanner_heal_evidence(root, run_dir, "release")
            for gate, text in (
                ("G03", "Exact scoped ACK"),
                ("G09", "mixed-version reader/writer"),
                ("G14", "3x4 EC8+4"),
                ("P3", "two-hour pressure/heal capacity"),
                ("R-L", "Legacy source conflicts"),
            ):
                self.assertTrue(any(error.startswith(f"pending {gate}:") and text in error for error in errors), gate)

            status = scanner_heal_release_status(root, run_dir)
            self.assertFalse(status["release_approved"])
            self.assertIn("mixed-version-rollback", status["pending_lanes"])
            self.assertIn("ec8-4-multiset", status["pending_lanes"])
            self.assertIn("scheduler-pressure", status["pending_lanes"])
            requirements, _, _ = scanner_heal_release_requirements(read_json(root / ".config/scanner-heal-required-tests.json"))
            self.assertIn("durable_root_publication_proof", requirements["G03"]["evidence_fields"])
            self.assertIn("disk_full_matrix", requirements["G08"]["evidence_fields"])
            self.assertIn("mixed_version_writer_evidence", requirements["G09"]["evidence_fields"])
            self.assertIn("segment_activation_preflight", requirements["G11"]["evidence_fields"])
            self.assertIn("distributed_segment_invalidation_evidence", requirements["G14"]["evidence_fields"])
            self.assertIn("cold_segment_reuse_measurement", requirements["P2"]["evidence_fields"])
            self.assertEqual(
                tuple(requirements["P4"]["evidence_fields"]),
                SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS["P4"],
            )

    def test_scanner_heal_required_evidence_fields_cannot_be_removed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            for gate in ("G03", "G07", "G08", "G09", "G11", "G14", "P2", "P4"):
                for requirement in registry["release_requirements"]:
                    if requirement["gate"] == gate:
                        requirement["evidence_fields"] = []
                        break
                write_json(root / ".config/scanner-heal-required-tests.json", registry)

                with self.subTest(gate=gate):
                    with self.assertRaisesRegex(ValueError, f"release gate {gate} missing required evidence fields"):
                        scanner_heal_release_status(root, run_dir)

                registry = read_json(root / ".config/scanner-heal-required-tests.json")
                for requirement in registry["release_requirements"]:
                    if requirement["gate"] == gate:
                        requirement["evidence_fields"] = list(SCANNER_HEAL_RELEASE_REQUIRED_EVIDENCE_FIELDS[gate])
                        break

    def test_scanner_heal_ec84_case_rejects_wrong_erasure_geometry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            self.assertEqual(check_scanner_heal_evidence(root, run_dir, "background-target-crash-ec8-4"), [])

            path = run_dir / "background-target-crash-ec8-4.json"
            oracle = read_json(path)
            oracle["objects"][0]["physical"]["data_blocks"] = 10
            oracle["objects"][0]["physical"]["parity_blocks"] = 2
            oracle["objects"][0]["expected_physical"]["data_blocks"] = 10
            oracle["objects"][0]["expected_physical"]["parity_blocks"] = 2
            write_json(path, oracle)
            (run_dir / "execution.json").unlink()
            finish_scanner_heal_receipt(run_dir, 0, root)

            errors = check_scanner_heal_evidence(root, run_dir, "background-target-crash-ec8-4")
            self.assertTrue(
                any("EC data/parity geometry differs from the required case" in error for error in errors),
                errors,
            )

    def test_scanner_heal_pending_gate_cannot_map_to_implemented_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            registry["release_requirements"][0]["lane"] = "single-set-restart"
            write_json(root / ".config/scanner-heal-required-tests.json", registry)

            with self.assertRaisesRegex(ValueError, "mapped to non-pending lane"):
                scanner_heal_release_status(root, run_dir)

    def test_scanner_heal_release_status_rejects_synthetic_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            registry["schema"] = 1
            registry["release_pending"] = {}
            registry.pop("release_lanes")
            registry.pop("release_requirements")
            write_json(root / ".config/scanner-heal-required-tests.json", registry)
            path = run_dir / "background-target-crash.json"
            oracle = read_json(path)
            oracle["evidence"] = "synthetic"
            write_json(path, oracle)
            (run_dir / "execution.json").unlink()
            finish_scanner_heal_receipt(run_dir, 0, root)

            status = scanner_heal_release_status(root, run_dir)
            self.assertEqual(status["decision"], "blocked")
            self.assertFalse(status["release_approved"])
            self.assertFalse(status["release_schema_capable"])
            self.assertEqual(status["rejected_cases"], ["background-target-crash"])
            self.assertEqual(status["pending_gates"], [])

    def test_scanner_heal_release_status_rejects_focused_case_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            registry["schema"] = 1
            registry["release_pending"] = {}
            registry.pop("release_lanes")
            registry.pop("release_requirements")
            write_json(root / ".config/scanner-heal-required-tests.json", registry)
            for case_id, requirement in registry["cases"].items():
                if case_id != "background-target-restart":
                    (run_dir / requirement["oracle"]).unlink()
            (run_dir / "execution.json").unlink()
            finish_scanner_heal_receipt(run_dir, 0, root)

            status = scanner_heal_release_status(root, run_dir)
            self.assertEqual(status["decision"], "blocked")
            self.assertFalse(status["release_approved"])
            self.assertEqual(status["verified_cases"], ["background-target-restart"])
            self.assertEqual(status["rejected_cases"], sorted(set(registry["cases"]) - {"background-target-restart"}))

    def test_scanner_heal_finish_collects_oracles_from_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            alternate = dict(registry["cases"]["background-target-restart"])
            alternate["oracle"] = "alternate-target-restart.json"
            registry["cases"]["alternate-target-restart"] = alternate
            write_json(root / ".config/scanner-heal-required-tests.json", registry)
            oracle = read_json(run_dir / "background-target-restart.json")
            oracle["case"] = "alternate-target-restart"
            write_json(run_dir / "alternate-target-restart.json", oracle)
            (run_dir / "background-target-restart.json").unlink()
            (run_dir / "execution.json").unlink()

            finish_scanner_heal_receipt(run_dir, 0, root)

            self.assertIn("alternate-target-restart.json", read_json(run_dir / "execution.json")["artifacts"])
            self.assertEqual(check_scanner_heal_evidence(root, run_dir, "alternate-target-restart"), [])

    def test_scanner_heal_release_consumes_multiple_registry_oracles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            registry = read_json(root / ".config/scanner-heal-required-tests.json")
            alternate = dict(registry["cases"]["background-target-restart"])
            alternate["oracle"] = "alternate-target-restart.json"
            registry["cases"]["alternate-target-restart"] = alternate
            write_json(root / ".config/scanner-heal-required-tests.json", registry)
            oracle = read_json(run_dir / "background-target-restart.json")
            oracle["case"] = "alternate-target-restart"
            write_json(run_dir / "alternate-target-restart.json", oracle)
            (run_dir / "execution.json").unlink()

            finish_scanner_heal_receipt(run_dir, 0, root)

            artifacts = read_json(run_dir / "execution.json")["artifacts"]
            self.assertIn("background-target-restart.json", artifacts)
            self.assertIn("alternate-target-restart.json", artifacts)
            self.assertEqual(check_scanner_heal_evidence(root, run_dir, "alternate-target-restart"), [])
            errors = check_scanner_heal_evidence(root, run_dir, "release")
            self.assertEqual(len(errors), 21)
            self.assertTrue(all(error.startswith("pending ") for error in errors))

    def test_scanner_heal_crash_case_rejects_restart_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            path = run_dir / "background-target-crash.json"
            oracle = read_json(path)
            oracle["evidence"] = "process-restart"
            oracle["unclean_shutdown_marker"] = False
            write_json(path, oracle)
            (run_dir / "execution.json").unlink()
            finish_scanner_heal_receipt(run_dir, 0, root)

            self.assertTrue(check_scanner_heal_evidence(root, run_dir, "background-target-crash"))

    def test_scanner_heal_rejects_broken_execution_and_artifacts(self) -> None:
        for fault in ("exit", "missing", "zero", "skipped", "failed", "retry", "filtered", "ignored", "stale",
                      "hash", "binary", "synthetic", "wrong-run", "same-pid", "body", "parts", "listing", "topology"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root, run_dir = self.scanner_heal_fixture(Path(tmp))
                path = run_dir / "background-target-restart.json"
                oracle = read_json(path)
                if fault == "exit":
                    receipt = read_json(run_dir / "execution.json")
                    receipt["exit_code"] = 42
                    write_json(run_dir / "execution.json", receipt)
                elif fault == "missing":
                    path.unlink()
                elif fault == "zero":
                    (run_dir / "junit.xml").write_text("<testsuites/>")
                elif fault in ("skipped", "failed", "retry"):
                    junit = run_dir / "junit.xml"
                    tag = {"skipped": "skipped", "failed": "failure", "retry": "rerunFailure"}[fault]
                    junit.write_text(junit.read_text().replace("/></testsuite>", f"><{tag}/></testcase></testsuite>"))
                elif fault in ("filtered", "ignored"):
                    listing = read_json(run_dir / "listing.json")
                    case = next(iter(listing["rust-suites"]["e2e_test"]["testcases"].values()))
                    case["ignored"] = fault == "ignored"
                    case["filter-match"]["status"] = "mismatch" if fault == "filtered" else "matches"
                    write_json(run_dir / "listing.json", listing)
                elif fault == "stale":
                    os.utime(path, (1, 1))
                elif fault == "hash":
                    path.write_text(path.read_text() + " ")
                elif fault == "binary":
                    Path(read_json(run_dir / "run.json")["binary"]["path"]).write_bytes(b"another build")
                else:
                    if fault == "synthetic":
                        oracle["evidence"] = "synthetic"
                    elif fault == "wrong-run":
                        oracle["run_id"] = "f" * 32
                    elif fault == "same-pid":
                        oracle["pid_after"] = oracle["pid_before"]
                    elif fault == "body":
                        oracle["objects"][0]["actual_sha256"] = "e" * 64
                    elif fault == "parts":
                        oracle["objects"][0]["physical"]["present_part_fingerprints"] = {}
                    elif fault == "listing":
                        oracle["node_listings"][0] = []
                    elif fault == "topology":
                        oracle["topology"] = {"nodes": 3, "drives_per_node": 4}
                    write_json(path, oracle)
                if fault not in ("exit", "missing", "stale", "hash", "binary"):
                    (run_dir / "execution.json").unlink()
                    finish_scanner_heal_receipt(run_dir, 0)
                self.assertTrue(check_scanner_heal_evidence(root, run_dir, "background-target-restart"), fault)

    def test_scanner_heal_receipts_reject_reuse_and_missing_builds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            with self.assertRaisesRegex(ValueError, "already exists"):
                finish_scanner_heal_receipt(run_dir, 0)
            with self.assertRaisesRegex(ValueError, "must be new"):
                begin_scanner_heal_receipt(root, run_dir, Path("missing"), Path("missing"))
            with mock.patch("subprocess.check_output", side_effect=["", "b" * 40]):
                with self.assertRaises(FileNotFoundError):
                    begin_scanner_heal_receipt(root, Path(tmp) / "new-run", Path(tmp) / "missing", Path(tmp) / "missing")

    def test_scanner_heal_begin_requires_embedded_source_provenance(self) -> None:
        for kind in ("current", "stale", "dirty", "unknown"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                root, _ = self.scanner_heal_fixture(Path(tmp))
                sources = root / "crates/e2e_test/src"
                sources.mkdir(parents=True)
                (sources / "heal_erasure_disk_rebuild_test.rs").write_bytes(b"oracle source")
                (sources / "chaos.rs").write_bytes(b"census source")
                revision = "c" * 40 if kind == "stale" else "b" * 40
                version = f"rustfs\ngit commit   : {revision}\ngit status   :\n"
                if kind == "dirty":
                    version += "modified source\n"
                if kind == "unknown":
                    version = "rustfs without build provenance"
                with mock.patch("subprocess.check_output", side_effect=["", "b" * 40, version, "c" * 40]), \
                     mock.patch.dict(os.environ, {"RUSTFS_E2E_EXPECTED_FEATURES": "default"}):
                    directory = Path(tmp) / "fresh"
                    binary = Path(tmp) / "fake-binary"
                    if kind == "current":
                        begin_scanner_heal_receipt(root, directory, binary, binary)
                        self.assertEqual(read_json(directory / "run.json")["binary_source_revision"], "b" * 40)
                    else:
                        with self.assertRaisesRegex(ValueError, "server binary"):
                            begin_scanner_heal_receipt(root, directory, binary, binary)
                        self.assertFalse(directory.exists())

    def test_scanner_heal_rejects_copied_junit_and_wrong_suite_build(self) -> None:
        for fault in ("old-junit", "missing-time", "wrong-binary", "common-source", "lockfile", "features", "dirty-build"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp:
                root, run_dir = self.scanner_heal_fixture(Path(tmp))
                if fault in ("old-junit", "missing-time"):
                    path = run_dir / "junit.xml"
                    xml = ET.fromstring(path.read_bytes())
                    testcase = next(xml.iter("testcase"))
                    if fault == "old-junit":
                        testcase.set("timestamp", "2000-01-01T00:00:00.000Z")
                    else:
                        del testcase.attrib["timestamp"]
                    # Rewriting/copying gives an old execution a fresh mtime.
                    path.write_bytes(ET.tostring(xml))
                elif fault == "wrong-binary":
                    path = run_dir / "listing.json"
                    listing = read_json(path)
                    another = Path(tmp) / "another-binary"
                    another.write_bytes(Path(tmp, "fake-binary").read_bytes())
                    listing["rust-suites"]["e2e_test"]["binary-path"] = str(another)
                    write_json(path, listing)
                else:
                    path = run_dir / "background-target-restart.json"
                    oracle = read_json(path)
                    key, value = {"common-source": ("source_revision", "f" * 40), "lockfile": ("lock_blob", "f" * 40),
                                  "features": ("features", "default,sftp"), "dirty-build": ("dirty", True)}[fault]
                    oracle["test_build"][key] = value
                    write_json(path, oracle)
                (run_dir / "execution.json").unlink()
                finish_scanner_heal_receipt(run_dir, 0)
                self.assertTrue(check_scanner_heal_evidence(root, run_dir, "background-target-restart"), fault)

    def test_scanner_heal_rejects_boolean_fractional_and_out_of_geometry_integers(self) -> None:
        valid = {"schema": 1, "nodes": 4, "drives_per_node": 1, "pid_before": 10, "pid_after": 11,
                 "erasure_index": 1, "data_blocks": 2, "parity_blocks": 2}
        cases = [(field, value) for field, correct in valid.items() for value in (True, float(correct))]
        cases += [("erasure_index", 5), ("erasure_index", 0), ("pid_after", -1)]
        for field, value in cases:
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as tmp:
                root, run_dir = self.scanner_heal_fixture(Path(tmp))
                path = run_dir / "background-target-restart.json"
                oracle = read_json(path)
                if field in ("nodes", "drives_per_node"):
                    oracle["topology"][field] = value
                elif field in ("erasure_index", "data_blocks", "parity_blocks"):
                    oracle["objects"][-1]["physical"][field] = value
                else:
                    oracle[field] = value
                write_json(path, oracle)
                (run_dir / "execution.json").unlink()
                finish_scanner_heal_receipt(run_dir, 0)
                self.assertTrue(check_scanner_heal_evidence(root, run_dir, "background-target-restart"))
        for filename in ("run.json", ".config/scanner-heal-required-tests.json"):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as tmp:
                root, run_dir = self.scanner_heal_fixture(Path(tmp))
                path = (root if filename.startswith(".config") else run_dir) / filename
                content = read_json(path)
                content["schema"] = True
                write_json(path, content)
                self.assertTrue(check_scanner_heal_evidence(root, run_dir, "background-target-restart"))

    def test_core_gate_rejects_missing_ignored_filtered_and_corrupt_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".config").mkdir()
            fixture = root / "fixture.hex"
            fixture.write_text("4142")
            requirements = {
                "tests": [{"invariant": "commit", "suite": "store", "name": "commit_test"}],
                "fixtures": [{"path": "fixture.hex", "sha256": hashlib.sha256(fixture.read_bytes()).hexdigest()}],
            }
            (root / ".config/ecstore-required-tests.json").write_text(json.dumps(requirements))
            listing = root / "listing.json"
            good = {"ignored": False, "filter-match": {"status": "matches"}}
            for case, testcase in (("selected", good), ("missing", {}), ("ignored", dict(good, ignored=True)),
                                   ("filtered", dict(good, **{"filter-match": {"status": "mismatch"}}))):
                with self.subTest(case=case):
                    listing.write_text(json.dumps({"rust-suites": {"store": {"testcases": {"commit_test": testcase}}}}))
                    self.assertEqual(bool(check_core_listing(root, listing)), case != "selected")
            listing.write_text(json.dumps({"rust-suites": {"store": {"testcases": {"commit_test": good}}}}))
            fixture.write_text("4143")
            self.assertIn("sha256 mismatch", check_core_listing(root, listing)[0])
            fixture.unlink()
            self.assertIn("fixture missing", check_core_listing(root, listing)[0])
            listing.write_text("not json")
            self.assertIn("cannot read", check_core_listing(root, listing)[0])

    def test_ilm_lane_keeps_the_measured_cargo_build_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workflow = root / ".github/workflows/ci.yml"
            workflow.parent.mkdir(parents=True)
            valid = (
                "jobs:\n"
                "  test-ilm-integration-serial:\n"
                "    steps:\n"
                "      - name: Run ignored ILM integration tests serially\n"
                "        env:\n"
                "          CARGO_BUILD_JOBS: ${{ (github.event_name == 'push' || github.event_name == 'workflow_dispatch') && '3' || '2' }}\n"
                "        run: cargo nextest run\n"
                "  next-job:\n"
                "    steps: []\n"
            )
            workflow.write_text(valid)
            self.assertEqual(check_ilm_build_budget(root), [])

            workflow.write_text(valid.replace("          CARGO_BUILD_JOBS:", "          BUILD_JOBS:"))
            self.assertEqual(len(check_ilm_build_budget(root)), 1)

    def test_vault_tests_require_cross_process_serialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "crates/e2e_test/src/kms"
            src.mkdir(parents=True)
            (src / "vault_test.rs").write_text("fn test(env: VaultTestEnvironment) {}\n")
            config = root / ".config/nextest.toml"
            config.parent.mkdir()
            config.write_text(
                "[profile.default]\n"
                "[[profile.default.overrides]]\n"
                "filter = 'test(/^kms::vault_test::/)'\n"
                "test-group = 'e2e-vault'\n"
                "[profile.e2e-full]\n"
            )
            self.assertEqual(len(check_vault_test_groups(root)), 1)
            config.write_text(
                config.read_text()
                + "[[profile.e2e-full.overrides]]\n"
                + "filter = 'test(/^kms::vault_test::/)'\n"
                + "test-group = 'e2e-vault'\n"
            )
            self.assertEqual(check_vault_test_groups(root), [])

    def test_runner_readiness_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            script = root / "scripts/run_e2e_tests.sh"
            script.parent.mkdir(parents=True)
            valid_runner = (
                "start_rustfs() {\n"
                '  curl --fail "http://localhost:9000/health/ready" && return 0\n'
                "  return 1\n"
                "}\n\n# Function to run tests\n"
                "--include-ignored --test-threads=1\n"
                "if ! start_rustfs; then\n"
                '  print_error "Failed to start RustFS properly"\n'
                "  exit 1\n"
                "fi\n"
            )
            script.write_text(valid_runner)
            self.assertEqual(check_runner_selection(root), [])

            script.write_text(valid_runner.replace("/health/ready", "/health"))
            self.assertEqual(len(check_runner_selection(root)), 1)

            script.write_text(valid_runner.replace("return 1", "nc -z localhost 9000 && return 0"))
            self.assertEqual(len(check_runner_selection(root)), 1)

            script.write_text(
                valid_runner.replace(
                    '  print_error "Failed to start RustFS properly"\n  exit 1',
                    '  if [ -n "$RUSTFS_PID" ]; then\n    echo continuing\n  else\n    exit 1\n  fi',
                )
            )
            self.assertEqual(len(check_runner_selection(root)), 1)

    def test_e2e_requires_registration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "crates/e2e_test/src"
            src.mkdir(parents=True)
            (src / "lib.rs").write_text("")
            test_file = src / "boundary_test.rs"
            test_file.write_text("#[test]\nfn boundary() {}\n")
            self.assertEqual(len(check_e2e_modules(root)), 1)
            (src / "lib.rs").write_text("mod boundary_test;\n")
            self.assertEqual(check_e2e_modules(root), [])

            nested = src / "protocols"
            nested.mkdir()
            (nested / "mod.rs").write_text("mod fixed_port_test;\n")
            (nested / "fixed_port_test.rs").write_text("#[test]\nfn fixed_port() {}\n")
            self.assertEqual(len(check_e2e_modules(root)), 1)
            (src / "lib.rs").write_text("mod boundary_test;\nmod protocols;\n")
            self.assertEqual(check_e2e_modules(root), [])

            (src / "lib.rs").write_text("#[cfg(any())]\nmod boundary_test;\nmod protocols;\n")
            self.assertEqual(len(check_e2e_modules(root)), 1)

            (src / "lib.rs").write_text(
                "#[cfg(any())]\n/// hidden module\nmod boundary_test;\n#[cfg_attr(test, cfg(any()))]\nmod protocols;\n"
            )
            self.assertEqual(len(check_e2e_modules(root)), 2)

            (src / "lib.rs").write_text(
                'const PHANTOM: &str = r#"{\nmod boundary_test;\n"#;\ndiscard! { mod protocols; }\n'
            )
            self.assertEqual(len(check_e2e_modules(root)), 2)

            (src / "lib.rs").write_text(
                '#[cfg(all(test, target_os = r"windows" /* target_os = "linux" */))]\n'
                "mod boundary_test;\nmod protocols;\n"
            )
            self.assertEqual(len(check_e2e_modules(root)), 1)

            (src / "lib.rs").write_text(
                '#[cfg(all(test, target_os = r"windows"))] // #[cfg(all(test, target_os = "linux"))]\n'
                "mod boundary_test;\nmod protocols;\n"
            )
            self.assertEqual(len(check_e2e_modules(root)), 1)

    def test_fuzz_runtime_uses_matrix_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "fuzz").mkdir()
            (root / "scripts/fuzz").mkdir(parents=True)
            (root / ".github/workflows").mkdir(parents=True)
            (root / "fuzz/Cargo.toml").write_text(
                'dep = { path = "../crates/dep" }\n[[bin]]\nname = "one"\n'
            )
            (root / "scripts/fuzz/run.sh").write_text('targets="one"\n')
            (root / ".github/workflows/fuzz.yml").write_text(
                'paths:\n  - "crates/dep/**"\n'
                "target: [one]\nFUZZ_TARGET: fixed\n"
                "target: [one]\nFUZZ_TARGET: ${{ matrix.target }}\n"
                "for target in one; do\n"
                "  fuzz/prebuilt/${{ env.CARGO_BUILD_TARGET }}/release/one\n"
            )
            self.assertEqual(len(check_fuzz_targets(root)), 1)

    def test_s3_runner_rejects_unbounded_failure_locals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = root / "scripts/s3-tests/run.sh"
            runner.parent.mkdir(parents=True)
            valid_runner = (
                "test_s3_api_ready() {\n"
                '    READY_CODE=$(curl "http://${S3_HOST}:${S3_PORT}/health/ready")\n'
                '    if [ "${READY_CODE}" != "200" ]; then\n'
                "        return 1\n"
                "    fi\n"
                "    if command -v awscurl; then\n"
                "        RESPONSE=$(awscurl --service s3)\n"
                '        if echo "${RESPONSE}" | grep -q "<ListAllMyBucketsResult"; then\n'
                "            return 0\n"
                "        fi\n"
                "        return 1\n"
                "    fi\n"
                "    return 0\n"
                "}\n\n# First, wait\n"
                "tox -- -vv -ra --tb=long\n"
            )
            runner.write_text(valid_runner)
            self.assertEqual(check_s3_tests_runner(root), [])
            runner.write_text(valid_runner.replace("--tb=long", "--showlocals --tb=long"))
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            runner.write_text(valid_runner.replace("/health/ready", "/health"))
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            runner.write_text(valid_runner.replace("    return 0\n}\n", "    return 0\n    return 0\n}\n"))
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            runner.write_text(
                valid_runner.replace(
                    "        return 1\n    fi\n    return 0\n",
                    "        return 0\n    fi\n    return 1\n",
                )
            )
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            runner.write_text(valid_runner.replace("        return 1\n    fi", "        false || return 0\n        return 1\n    fi"))
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            runner.write_text(valid_runner.replace('echo "${RESPONSE}"', 'echo "<ListAllMyBucketsResult"'))
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            runner.write_text(
                valid_runner.replace(
                    "RESPONSE=$(awscurl --service s3)",
                    'RESPONSE=$(awscurl --service s3 || echo "<ListAllMyBucketsResult")',
                )
            )
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            runner.write_text(
                valid_runner.replace(
                    "RESPONSE=$(awscurl --service s3)",
                    'RESPONSE=$(awscurl --service s3; echo "<ListAllMyBucketsResult")',
                )
            )
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            with (
                mock.patch(__name__ + ".check_e2e_modules", return_value=[]),
                mock.patch(__name__ + ".check_vault_test_groups", return_value=[]),
                mock.patch(__name__ + ".check_fuzz_targets", return_value=[]),
                mock.patch(__name__ + ".check_core_fixtures", return_value=[]),
                mock.patch(__name__ + ".check_runner_selection", return_value=[]),
                mock.patch(__name__ + ".check_workflow_readiness", return_value=[]),
                mock.patch(__name__ + ".check_profile_definitions", return_value=[]),
                mock.patch(__name__ + ".check_ilm_build_budget", return_value=[]),
                mock.patch(__name__ + ".check_scheduled_alerts", return_value=[]),
                mock.patch(__name__ + ".check_quick_checks", return_value=[]),
            ):
                self.assertEqual(len(validate(root)), 1)

    def test_workflow_readiness_requires_dependency_probe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workflows = root / ".github/workflows"
            workflows.mkdir(parents=True)
            valid_workflow = (
                "jobs:\n"
                "  test:\n"
                "    steps:\n"
                "      - name: Wait for RustFS ready\n"
                "        run: |\n"
                "          for _ in {1..60}; do\n"
                "            if curl -sf http://127.0.0.1:9000/health/ready; then\n"
                "              exit 0\n"
                "            fi\n"
                "          done\n"
                "          exit 1\n"
                "      - name: Run tests\n"
                "        run: true\n"
            )
            for name in ("e2e-s3tests.yml", "mint.yml"):
                (workflows / name).write_text(valid_workflow)
            self.assertEqual(check_workflow_readiness(root), [])

            (workflows / "mint.yml").write_text(valid_workflow.replace("/health/ready", "/health"))
            self.assertEqual(len(check_workflow_readiness(root)), 1)
            (workflows / "mint.yml").write_text(
                valid_workflow.replace(
                    "if curl -sf http://127.0.0.1:9000/health/ready; then",
                    "if curl -sf http://127.0.0.1:9000/health/ready || true; then",
                )
            )
            self.assertEqual(len(check_workflow_readiness(root)), 1)
            (workflows / "mint.yml").write_text(
                valid_workflow.replace(
                    "if curl -sf http://127.0.0.1:9000/health/ready; then",
                    "if curl -sf http://127.0.0.1:9000/health/ready || :; then",
                )
            )
            self.assertEqual(len(check_workflow_readiness(root)), 1)
            (workflows / "mint.yml").write_text(
                valid_workflow.replace(
                    "if curl -sf http://127.0.0.1:9000/health/ready; then\n              exit 0\n            fi",
                    "curl -sf http://127.0.0.1:9000/health/ready || true\n            exit 0",
                )
            )
            self.assertEqual(len(check_workflow_readiness(root)), 1)

    def test_profile_listing_enforces_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".config").mkdir()
            digest = hashlib.sha256(b"suite::two\n").hexdigest()
            (root / ".config/e2e-smoke-selection.txt").write_text(f"sha256={digest}\n")
            listing = root / "listing.json"
            listing.write_text(
                json.dumps(
                    {
                        "rust-suites": {
                            "suite": {
                                "testcases": {
                                    "one": {"filter-match": {"status": "matches"}},
                                    "two": {"filter-match": {"status": "mismatch"}},
                                }
                            }
                        }
                    }
                )
            )
            self.assertEqual(len(check_profile_listing(root, "e2e-smoke", listing)), 1)

    def test_profile_listing_binds_platform_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".config").mkdir()
            darwin_digest = hashlib.sha256(b"suite::darwin\n").hexdigest()
            linux_digest = hashlib.sha256(b"suite::linux\n").hexdigest()
            (root / ".config/e2e-full-selection.txt").write_text(
                f"sha256-darwin={darwin_digest}\nsha256-linux={linux_digest}\n"
            )
            listing = root / "listing.json"
            listing.write_text(
                json.dumps(
                    {
                        "rust-suites": {
                            "suite": {
                                "testcases": {"darwin": {"filter-match": {"status": "matches"}}}
                            }
                        }
                    }
                )
            )
            with mock.patch.object(sys, "platform", "linux"):
                self.assertEqual(len(check_profile_listing(root, "e2e-full", listing)), 1)

    def test_update_profile_selection_changes_only_requested_platform(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".config").mkdir()
            darwin_digest = "a" * 64
            (root / ".config/e2e-full-selection.txt").write_text(
                f"sha256-darwin={darwin_digest}\nsha256-linux={'b' * 64}\n"
            )
            listing = root / "listing.json"
            listing.write_text(
                json.dumps(
                    {
                        "rust-suites": {
                            "suite": {
                                "testcases": {"linux": {"filter-match": {"status": "matches"}}}
                            }
                        }
                    }
                )
            )

            count, digest, key = update_profile_selection(root, "e2e-full", listing, "linux")

            self.assertEqual(count, 1)
            self.assertEqual(key, "sha256-linux")
            self.assertEqual(
                (root / ".config/e2e-full-selection.txt").read_text(),
                f"sha256-darwin={darwin_digest}\nsha256-linux={digest}\n",
            )
            with mock.patch.object(sys, "platform", "linux"):
                self.assertEqual(check_profile_listing(root, "e2e-full", listing), [])
            selection = root / ".config/e2e-smoke-selection.txt"
            selection.write_text(f"sha256={'a' * 64}\n")

            count, digest, key = update_profile_selection(root, "e2e-smoke", listing, "linux")

            self.assertEqual((count, key), (1, "sha256"))
            self.assertEqual(selection.read_text(), f"sha256={digest}\n")

    def test_scheduled_alerts_require_completion_watchdog(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alert = (
                "  alert-on-failure:\n"
                "    if: always() && github.event_name == 'schedule' && "
                "contains(needs.*.result, 'failure')\n"
                "    permissions:\n"
                "      issues: write\n"
                "    steps:\n"
                "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                "      - uses: ./.github/actions/schedule-failure-issue\n"
                "        with:\n"
                "          github-token: ${{ secrets.GITHUB_TOKEN }}\n"
            )
            names: list[str] = []
            for index, relative in enumerate(SCHEDULED_ALERT_WORKFLOWS, start=1):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                names.append(path.stem)
                path.write_text(
                    f'name: "{path.stem}"\n'
                    f'on:\n  schedule:\n    - cron: "{index} {index} * * *"\n'
                    f'jobs:\n{alert}'
                )
            watchdog = root / ".github/workflows/scheduled-validation-watchdog.yml"
            watchdog.write_text(
                "on:\n  workflow_run:\n    workflows:\n"
                + "\n".join(f'      - "{name}"' for name in names)
                + "\njobs:\n"
                + "  alert-on-incomplete-run:\n"
                + "    github.event.workflow_run.event == 'schedule'\n"
                + "    github.event.workflow_run.conclusion != 'success'\n"
                + "    github.event.workflow_run.conclusion != 'failure'\n"
                + "    permissions:\n"
                + "      actions: read\n"
                + "      issues: write\n"
                + "    steps:\n"
                + "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                + "      - uses: ./.github/actions/schedule-failure-issue\n"
                + "        with:\n"
                + "          github-token: ${{ secrets.GITHUB_TOKEN }}\n"
                + "          workflow-name: ${{ github.event.workflow_run.name }}\n"
                + "          source-run-id: ${{ github.event.workflow_run.id }}\n"
                + "          source-run-attempt: ${{ github.event.workflow_run.run_attempt }}\n"
                + "          source-event: ${{ github.event.workflow_run.event }}\n"
                + "          source-ref-name: ${{ github.event.workflow_run.head_branch }}\n"
                + "          source-sha: ${{ github.event.workflow_run.head_sha }}\n"
            )
            freshness = root / ".github/workflows/scheduled-validation-freshness.yml"
            freshness.write_text(
                "jobs:\n"
                "  check-freshness:\n"
                "    permissions:\n"
                "      actions: read\n"
                "      issues: write\n"
                "    steps:\n"
                "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                "      - run: python3 scripts/check_scheduled_validation_freshness.py\n"
                "      - uses: ./.github/actions/schedule-failure-issue\n"
                "        if: failure()\n"
                "        with:\n"
                "          github-token: ${{ secrets.GITHUB_TOKEN }}\n"
                "          details-file: ${{ runner.temp }}/scheduled-validation-freshness.md\n"
            )
            checker = root / "scripts/check_scheduled_validation_freshness.py"
            checker.parent.mkdir()
            checker.write_text("")
            self.assertEqual(check_scheduled_alerts(root), [])

            first = root / SCHEDULED_ALERT_WORKFLOWS[0]
            mutations = (
                ("contains(needs.*.result, 'failure')", "false"),
                ("issues: write", "issues: read"),
                (
                    "uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
                    "uses: actions/checkout@missing",
                ),
                (
                    "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n",
                    "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                    "        if: github.event_name == 'workflow_dispatch'\n",
                ),
                (
                    "      - uses: ./.github/actions/schedule-failure-issue\n",
                    "      - uses: ./.github/actions/schedule-failure-issue\n"
                    "        if: github.event_name == 'workflow_dispatch'\n",
                ),
                ("uses: ./.github/actions/schedule-failure-issue", "uses: actions/checkout@v7"),
                ("github-token: ${{ secrets.GITHUB_TOKEN }}", "github-token: missing"),
            )
            for required, replacement in mutations:
                original = first.read_text()
                first.write_text(original.replace(required, replacement))
                self.assertEqual(len(check_scheduled_alerts(root)), 1)
                first.write_text(original)

            first_original = first.read_text()
            real_steps = (
                "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                "      - uses: ./.github/actions/schedule-failure-issue\n"
                "        with:\n"
                "          github-token: ${{ secrets.GITHUB_TOKEN }}\n"
            )
            first.write_text(
                first_original.replace(
                    real_steps,
                    "      - run: |\n"
                    "          : <<'MARKER'\n"
                    "          uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                    "          MARKER\n"
                    "      - run: |\n"
                    "          : <<'MARKER'\n"
                    "          uses: ./.github/actions/schedule-failure-issue\n"
                    "          github-token: ${{ secrets.GITHUB_TOKEN }}\n"
                    "          MARKER\n",
                )
            )
            self.assertTrue(check_scheduled_alerts(root))
            first.write_text(
                first_original.replace(
                    real_steps,
                    "      - uses: ./.github/actions/schedule-failure-issue\n"
                    "        with:\n"
                    "          github-token: ${{ secrets.GITHUB_TOKEN }}\n"
                    "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n",
                )
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            first.write_text(first_original)

            watchdog_mutations = (
                ("actions: read", "actions: none"),
                ("issues: write", "issues: read"),
                (
                    "uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
                    "uses: actions/checkout@missing",
                ),
                (
                    "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n",
                    "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                    "        if: github.event_name == 'workflow_dispatch'\n",
                ),
                (
                    "      - uses: ./.github/actions/schedule-failure-issue\n",
                    "      - uses: ./.github/actions/schedule-failure-issue\n"
                    "        if: github.event_name == 'workflow_dispatch'\n",
                ),
                ("uses: ./.github/actions/schedule-failure-issue", "uses: actions/checkout@v7"),
                ("github-token: ${{ secrets.GITHUB_TOKEN }}", "github-token: missing"),
                ("source-event: ${{ github.event.workflow_run.event }}", "source-event: watchdog"),
                (
                    "source-ref-name: ${{ github.event.workflow_run.head_branch }}",
                    "source-ref-name: main",
                ),
                ("source-sha: ${{ github.event.workflow_run.head_sha }}", "source-sha: missing"),
            )
            for required, replacement in watchdog_mutations:
                original = watchdog.read_text()
                watchdog.write_text(original.replace(required, replacement))
                self.assertEqual(len(check_scheduled_alerts(root)), 1)
                watchdog.write_text(original)

            watchdog_original = watchdog.read_text()
            watchdog.write_text(
                watchdog_original.replace("issues: write", "issues: read")
                + "  decoy:\n    permissions:\n      issues: write\n"
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            watchdog.write_text(watchdog_original)

            first_original = first.read_text()
            first.write_text(
                first_original.replace('  schedule:\n    - cron: "1 1 * * *"\n', "")
                + '  decoy:\n    strategy:\n      matrix:\n        cron:\n          - "1 1 * * *"\n'
                + '    runs-on: ubuntu-latest\n    steps:\n      - run: true\n'
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            first.write_text(first_original)

            first.write_text(
                first_original.replace(
                    '    - cron: "1 1 * * *"\n',
                    '    - cron: "1 1 * * *"\n    - cron: "0 5 * * *"\n',
                )
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            first.write_text(
                first_original.replace(
                    '    - cron: "1 1 * * *"\n',
                    '    - cron: "1 1 * * *"\n    - cron: "2 2 * * *"\n',
                )
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            first.write_text(first_original)

            watchdog.write_text(
                watchdog_original.replace(f'      - "{names[0]}"\n', "")
                + f'  decoy:\n    strategy:\n      matrix:\n        workflow:\n          - "{names[0]}"\n'
                + '    runs-on: ubuntu-latest\n    steps:\n      - run: true\n'
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            watchdog.write_text(watchdog_original)

            watchdog.write_text(watchdog_original.replace(f'      - "{names[0]}"\n', ""))
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            watchdog.write_text(watchdog_original)
            original = first.read_text()
            first.write_text(re.sub(r'- cron: "\d+ \d+', '- cron: "0 0', original, count=1))
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            first.write_text(original)

            second = root / SCHEDULED_ALERT_WORKFLOWS[1]
            second_original = second.read_text()
            second.write_text(re.sub(r'- cron: "\d+ \d+', '- cron: "1 1', second_original, count=1))
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            second.write_text(second_original)

            first.write_text(
                first_original.replace(
                    '    - cron: "1 1 * * *"\n',
                    '    - cron: "7 0 * * *"\n      timezone: "Asia/Shanghai"\n',
                )
            )
            second.write_text(
                second_original.replace(
                    '    - cron: "2 2 * * *"\n',
                    '    - cron: "2 2 * * *"\n    - cron: "7 16 * * *"\n',
                )
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            first.write_text(first_original)
            second.write_text(second_original)

            freshness_original = freshness.read_text()
            freshness.write_text(freshness_original.replace("details-file:", "report-file:"))
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            freshness.write_text(
                freshness_original.replace("github-token: ${{ secrets.GITHUB_TOKEN }}", "github-token: missing")
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            freshness.write_text(
                freshness_original.replace(
                    "uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
                    "uses: actions/checkout@missing",
                )
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            freshness.write_text(
                freshness_original.replace(
                    "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n",
                    "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
                    "        if: github.event_name == 'workflow_dispatch'\n",
                )
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            freshness.write_text(
                freshness_original.replace("if: failure()", "if: github.event_name == 'workflow_dispatch'")
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)
            freshness.write_text(
                freshness_original.replace("issues: write", "issues: read")
                + "  decoy:\n    permissions:\n      issues: write\n"
            )
            self.assertEqual(len(check_scheduled_alerts(root)), 1)

def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if sys.argv[1:2] in (["--begin-scanner-heal"], ["--finish-scanner-heal"], ["--check-scanner-heal"],
                         ["--check-scanner-heal-release"], ["--check-scanner-heal-release-bundle"],
                         ["--check-scanner-heal-release-bundle-gate"],
                         ["--assemble-scanner-heal-release-bundle"],
                         ["--write-scanner-heal-release-bundle-fixture"]):
        try:
            if len(sys.argv) == 5 and sys.argv[1] == "--begin-scanner-heal":
                begin_scanner_heal_receipt(ROOT, Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
                return 0
            if len(sys.argv) == 4 and sys.argv[1] == "--finish-scanner-heal":
                finish_scanner_heal_receipt(Path(sys.argv[2]), int(sys.argv[3]))
                return 0
            if len(sys.argv) == 4 and sys.argv[1] == "--check-scanner-heal":
                errors = check_scanner_heal_evidence(ROOT, Path(sys.argv[2]), sys.argv[3])
                for error in errors:
                    print(f"ERROR: {error}", file=sys.stderr)
                if not errors:
                    print(f"Case evidence verified: {sys.argv[3]}; this does not approve release")
                return 1 if errors else 0
            if len(sys.argv) == 3 and sys.argv[1] == "--check-scanner-heal-release":
                try:
                    status = scanner_heal_release_status(ROOT, Path(sys.argv[2]))
                except (OSError, KeyError, TypeError, ValueError, ET.ParseError) as error:
                    print(json.dumps({"schema": 1, "decision": "invalid", "release_approved": False,
                                      "error": str(error)}, sort_keys=True, separators=(",", ":")))
                    return 2
                print(json.dumps(status, sort_keys=True, separators=(",", ":")))
                return 0 if status["release_approved"] else 1
            if len(sys.argv) == 3 and sys.argv[1] == "--check-scanner-heal-release-bundle":
                try:
                    status = scanner_heal_release_bundle_status(ROOT, Path(sys.argv[2]))
                except (OSError, KeyError, TypeError, ValueError, ET.ParseError) as error:
                    print(json.dumps({"schema": 1, "decision": "invalid", "release_approved": False,
                                      "error": str(error)}, sort_keys=True, separators=(",", ":")))
                    return 2
                print(json.dumps(status, sort_keys=True, separators=(",", ":")))
                return 0 if status["release_approved"] else 1
            if len(sys.argv) == 4 and sys.argv[1] == "--check-scanner-heal-release-bundle-gate":
                try:
                    status = scanner_heal_release_bundle_gate_status(ROOT, Path(sys.argv[2]), sys.argv[3])
                except (OSError, KeyError, TypeError, ValueError, ET.ParseError) as error:
                    print(json.dumps({"schema": 1, "decision": "invalid", "release_approved": False,
                                      "error": str(error)}, sort_keys=True, separators=(",", ":")))
                    return 2
                print(json.dumps(status, sort_keys=True, separators=(",", ":")))
                return 0 if status["decision"] == "verified" else 1
            if len(sys.argv) == 4 and sys.argv[1] == "--assemble-scanner-heal-release-bundle":
                try:
                    bundle, status = assemble_scanner_heal_release_bundle(ROOT, Path(sys.argv[2]), Path(sys.argv[3]))
                except (OSError, KeyError, TypeError, ValueError, ET.ParseError) as error:
                    print(json.dumps({"schema": 1, "decision": "invalid", "release_approved": False,
                                      "error": str(error)}, sort_keys=True, separators=(",", ":")))
                    return 2
                print(bundle)
                print(json.dumps(status, sort_keys=True, separators=(",", ":")))
                return 0 if status["release_approved"] else 1
            if len(sys.argv) == 3 and sys.argv[1] == "--write-scanner-heal-release-bundle-fixture":
                bundle = write_scanner_heal_release_bundle_fixture(ROOT, Path(sys.argv[2]))
                print(bundle)
                return 0
            raise ValueError("expected --begin-scanner-heal DIR BINARY TEST_BINARY, --finish-scanner-heal DIR EXIT, --check-scanner-heal DIR CASE|release, --check-scanner-heal-release DIR, --check-scanner-heal-release-bundle FILE, --check-scanner-heal-release-bundle-gate FILE GATE, --assemble-scanner-heal-release-bundle DESCRIPTOR DIR, or --write-scanner-heal-release-bundle-fixture DIR")
        except (OSError, KeyError, TypeError, ValueError, subprocess.SubprocessError) as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1
    if len(sys.argv) == 3 and sys.argv[1] == "--check-core":
        errors = check_core_listing(ROOT, Path(sys.argv[2]))
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1 if errors else 0
    if len(sys.argv) == 4 and sys.argv[1] == "--check-profile":
        errors = check_profile_listing(ROOT, sys.argv[2], Path(sys.argv[3]))
        if errors:
            for error in errors:
                print(f"ERROR: {error}", file=sys.stderr)
            return 1
        return 0
    if len(sys.argv) == 5 and sys.argv[1] == "--update-profile":
        try:
            count, digest, key = update_profile_selection(ROOT, sys.argv[2], Path(sys.argv[3]), sys.argv[4])
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            print(f"ERROR: cannot update {sys.argv[2]} selection: {error}", file=sys.stderr)
            return 1
        print(f"Updated .config/{sys.argv[2]}-selection.txt: count={count} {key}={digest}")
        return 0
    if sys.argv[1:]:
        print(
            "usage: check_test_wiring.py [--self-test | --check-core LISTING | --check-profile PROFILE LISTING | "
            "--update-profile PROFILE LISTING PLATFORM | --check-scanner-heal-release-bundle FILE | "
            "--check-scanner-heal-release-bundle-gate FILE GATE | "
            "--assemble-scanner-heal-release-bundle DESCRIPTOR DIR | --write-scanner-heal-release-bundle-fixture DIR]",
            file=sys.stderr,
        )
        return 2

    errors = validate(ROOT)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("OK: e2e modules, runner selection, fuzz matrices, profiles, scheduled alerts, and Quick Checks are wired")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
