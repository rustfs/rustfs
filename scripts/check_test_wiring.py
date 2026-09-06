#!/usr/bin/env python3
"""Fail when committed tests silently fall out of their execution wiring."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from unittest import mock
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from scanner_abba import MAX_JSON_BYTES, digest, number, read_json, require, sha, write_json


ROOT = Path(__file__).resolve().parents[1]
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
    for name in ("ci.yml", "ci-docs-only.yml"):
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


def scanner_heal_oracle_names(root: Path) -> tuple[str, ...]:
    registry = read_json(root / ".config/scanner-heal-required-tests.json")
    evidence_integer(registry.get("schema"), "registry schema", 1, 1)
    cases = registry.get("cases")
    require(isinstance(cases, dict) and cases, "invalid scanner/heal registry")
    names = set()
    for case_id, requirement in cases.items():
        require(isinstance(case_id, str) and case_id, "invalid scanner/heal case identity")
        oracle = requirement.get("oracle")
        require(isinstance(oracle, str) and oracle.endswith(".json"), f"invalid oracle for {case_id}")
        path = Path(oracle)
        require(not path.is_absolute() and ".." not in path.parts, f"oracle path escapes run directory for {case_id}")
        names.add(oracle)
    return tuple(sorted(names))


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
        evidence_integer(registry.get("schema"), "registry schema", 1, 1)
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
            require(oracle.get("evidence") == "process-restart", "not real process-restart evidence")
            require(oracle.get("case") == name and oracle.get("run_id") == run["run_id"], "oracle belongs to another case/run")
            require(oracle.get("source_revision") == run["source_revision"], "oracle source mismatch")
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
            errors.extend(f"pending {gate}: {reason}" for gate, reason in registry["release_pending"].items())
        return errors
    except (OSError, KeyError, TypeError, ValueError, ET.ParseError) as error:
        return [f"scanner/heal evidence rejected: {error}"]


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
                ".github/workflows/ci-docs-only.yml": caller,
                ".github/actions/quick-checks/action.yml": action,
            }
            for relative, source in sources.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source)
            self.assertEqual(check_quick_checks(root), [])
            for relative in (".github/workflows/ci.yml", ".github/workflows/ci-docs-only.yml"):
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
        requirement = registry["cases"]["background-target-restart"]
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
        write_json(run_dir / "listing.json", {"rust-suites": {requirement["suite"]: {
            "binary-id": requirement["suite"], "binary-path": str(binary), "package-name": "e2e_test", "build-platform": "target",
            "testcases": {
            requirement["name"]: {"ignored": False, "filter-match": {"status": "matches"}}
        }}}})
        (run_dir / "junit.xml").write_text(
            f'<testsuites><testsuite><testcase name="{requirement["name"]}" classname="{requirement["suite"]}" '
            f'timestamp="{datetime.now(timezone.utc).isoformat(timespec="milliseconds")}"/></testsuite></testsuites>')
        physical = {"has_xl_meta": True, "version_id": None, "data_dir": "data-generation",
                    "erasure_index": 1, "data_blocks": 2, "parity_blocks": 2, "expected_part_numbers": [1],
                    "present_part_fingerprints": {"1": {"size": 12, "sha256": "c" * 64}},
                    "inline_data_fingerprint": None}
        obj = {"key": "object", "version_id": None, "expected_bytes": 16, "actual_bytes": 16,
               "expected_sha256": "d" * 64, "actual_sha256": "d" * 64,
               "expected_physical": physical, "physical": physical}
        objects = [dict(obj, key=f"object-{index}") for index in range(9)]
        objects[-1] = dict(objects[-1], expected_physical=None)
        write_json(run_dir / "background-target-restart.json", {
            "schema": 1, "evidence": "process-restart", "case": "background-target-restart",
            "run_id": "a" * 32, "source_revision": "b" * 40,
            "test_build": {"source_revision": "b" * 40, "dirty": False, "lock_blob": "c" * 40,
                           "features": "default", "target": "aarch64-apple-darwin", "profile": "debug", "rustflags_hex": ""},
            "binary_sha256": build["sha256"], "test_binary_sha256": build["sha256"],
            "topology": {"nodes": 4, "drives_per_node": 1}, "pid_before": 10, "pid_after": 11,
            "objects": objects, "node_listings": [[item["key"] for item in objects]] * 4,
        })
        finish_scanner_heal_receipt(run_dir, 0, root)
        return root, run_dir

    def test_scanner_heal_case_does_not_approve_pending_release(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root, run_dir = self.scanner_heal_fixture(Path(tmp))
            self.assertEqual(check_scanner_heal_evidence(root, run_dir, "background-target-restart"), [])
            errors = check_scanner_heal_evidence(root, run_dir, "release")
            self.assertEqual(len(errors), 21)
            self.assertTrue(any(error.startswith("pending R-E:") for error in errors))
            self.assertTrue(any(error.startswith("pending R-D:") for error in errors))
            self.assertTrue(any(error.startswith("pending R-L:") for error in errors))

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
    if sys.argv[1:2] in (["--begin-scanner-heal"], ["--finish-scanner-heal"], ["--check-scanner-heal"]):
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
            raise ValueError("expected --begin-scanner-heal DIR BINARY TEST_BINARY, --finish-scanner-heal DIR EXIT, or --check-scanner-heal DIR CASE|release")
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
            "--update-profile PROFILE LISTING PLATFORM]",
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
