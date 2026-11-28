#!/usr/bin/env python3
# Copyright 2024 RustFS Team
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

"""Verify fixed security rejection-branch mutants in isolated workspaces.

For each case in `.config/security-rejection-mutants.toml`:
1. Verifies the original snippet digest and exact single occurrence in source.
2. Runs the selected test(s) on the unmutated baseline and requires all to pass.
3. Materializes an isolated temporary workspace with ONLY the single mutation
   applied (never writing to the caller's checkout).
4. Compiles the mutated crate (`--no-run`), verifies exact test selection via
   `cargo nextest list`, runs `cargo nextest run --profile ci --retries 0`, and
   requires an explicit JUnit `<failure>` on the selected test(s) with exit 100.
5. Treats compile failure, zero/missing tests, timeouts, harness errors, and
   baseline failures as invalid verification (never counted as killed).
6. Re-verifies the source checkout file hash is completely unchanged.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import tomllib
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 1
DEFAULT_CASES_PATH = ".config/security-rejection-mutants.toml"
SKIP_TOP_LEVEL_ENTRIES = {".git", "target", "artifacts", "__pycache__", ".mypy_cache"}
MAX_LOG_SUMMARY_CHARS = 1200


@dataclass(frozen=True)
class MutantCase:
    case_id: str
    source_file: str
    symbol: str
    original_snippet_sha256: str
    original_snippet: str
    mutated_snippet: str
    mutation_description: str
    package: str
    target_kind: str
    selected_tests: tuple[str, ...]
    expected_rejection: str
    timeout_seconds: int
    compile_timeout_seconds: int = 900


@dataclass
class StepOutcome:
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def git_head_sha(root: Path) -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, stderr=subprocess.PIPE)
        return out.decode("utf-8").strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def compute_harness_sha(script_path: Path, cases_path: Path) -> str:
    h = hashlib.sha256()
    h.update(script_path.read_bytes())
    h.update(b"\0")
    h.update(cases_path.read_bytes())
    return h.hexdigest()


def sanitize_log_summary(text: str) -> str:
    cleaned = re.sub(r"(?i)(authorization|bearer|secret|token)\s*[:=]\s*\S+", r"\1=[redacted]", text)
    lines = [line.rstrip() for line in cleaned.splitlines() if line.strip()]
    joined = "\n".join(lines[-20:])
    if len(joined) > MAX_LOG_SUMMARY_CHARS:
        return joined[-MAX_LOG_SUMMARY_CHARS:]
    return joined


def load_cases(path: Path) -> list[MutantCase]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"{path}: unsupported schema_version {data.get('schema_version')!r}")
    raw_cases = data.get("cases")
    if not isinstance(raw_cases, list) or not raw_cases:
        raise ValueError(f"{path}: cases must be a non-empty list")

    seen_ids: set[str] = set()
    cases: list[MutantCase] = []
    for entry in raw_cases:
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: every case must be a table")
        case_id = entry.get("id")
        if not isinstance(case_id, str) or not re.fullmatch(r"[a-z0-9_-]+", case_id):
            raise ValueError(f"{path}: invalid case id {case_id!r}")
        if case_id in seen_ids:
            raise ValueError(f"{path}: duplicate case id {case_id!r}")
        seen_ids.add(case_id)

        source_file = entry.get("source_file")
        symbol = entry.get("symbol")
        orig_sha = entry.get("original_snippet_sha256")
        orig_snip = entry.get("original_snippet")
        mut_snip = entry.get("mutated_snippet")
        desc = entry.get("mutation_description")
        package = entry.get("package")
        target_kind = entry.get("target_kind", "lib")
        selected = entry.get("selected_tests")
        expected = entry.get("expected_rejection")
        timeout = entry.get("timeout_seconds", 120)
        compile_timeout = entry.get("compile_timeout_seconds", 900)

        for field_name, val in (
            ("source_file", source_file),
            ("symbol", symbol),
            ("original_snippet", orig_snip),
            ("mutated_snippet", mut_snip),
            ("mutation_description", desc),
            ("package", package),
            ("expected_rejection", expected),
        ):
            if not isinstance(val, str) or not val:
                raise ValueError(f"{path}: case {case_id} missing non-empty string {field_name}")
        if not isinstance(orig_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", orig_sha):
            raise ValueError(f"{path}: case {case_id} invalid original_snippet_sha256")
        if sha256_bytes(orig_snip.encode("utf-8")) != orig_sha:
            raise ValueError(
                f"{path}: case {case_id} original_snippet_sha256 does not match original_snippet "
                f"(expected {orig_sha}, actual {sha256_bytes(orig_snip.encode('utf-8'))})"
            )
        if orig_snip == mut_snip:
            raise ValueError(f"{path}: case {case_id} mutated_snippet must differ from original_snippet")
        if target_kind not in ("lib", "bins", "tests"):
            raise ValueError(f"{path}: case {case_id} unsupported target_kind {target_kind!r}")
        if (
            not isinstance(selected, list)
            or not selected
            or not all(isinstance(name, str) and name for name in selected)
        ):
            raise ValueError(f"{path}: case {case_id} selected_tests must be a non-empty list of strings")
        if type(timeout) is not int or timeout <= 0:
            raise ValueError(f"{path}: case {case_id} timeout_seconds must be a positive int")
        if type(compile_timeout) is not int or compile_timeout <= 0:
            raise ValueError(f"{path}: case {case_id} compile_timeout_seconds must be a positive int")

        cases.append(
            MutantCase(
                case_id=case_id,
                source_file=source_file,
                symbol=symbol,
                original_snippet_sha256=orig_sha,
                original_snippet=orig_snip,
                mutated_snippet=mut_snip,
                mutation_description=desc,
                package=package,
                target_kind=target_kind,
                selected_tests=tuple(selected),
                expected_rejection=expected,
                timeout_seconds=timeout,
                compile_timeout_seconds=compile_timeout,
            )
        )
    return cases


def build_filter_expression(selected_tests: tuple[str, ...]) -> str:
    return " | ".join(f"test(={name})" for name in selected_tests)


def target_kind_flag(target_kind: str) -> str:
    return {"lib": "--lib", "bins": "--bins", "tests": "--tests"}[target_kind]


def find_crate_rel_dir(repo_root: Path, source_file: str) -> str | None:
    """Return the repo-relative crate directory owning `source_file`, if inside a workspace."""
    cur = (repo_root / source_file).parent
    root_resolved = repo_root.resolve()
    while True:
        if (cur / "Cargo.toml").is_file() and cur.resolve() != root_resolved:
            return cur.resolve().relative_to(root_resolved).as_posix()
        if cur.resolve() == root_resolved or cur.parent == cur:
            return None
        cur = cur.parent


def rewrite_workspace_cargo_toml(cargo_toml_text: str, repo_root: Path, mutated_crate_rel: str) -> str:
    """Pin all untouched workspace path deps to `repo_root` so Cargo reuses compiled artifacts."""
    normalized_mutated = Path(mutated_crate_rel).as_posix().lstrip("./")
    root_posix = repo_root.resolve().as_posix()

    # Replace `[workspace] members = [...]` with only the mutated crate so Cargo does not
    # require symlinking or re-fingerprinting the other workspace members.
    rewritten = re.sub(
        r"(?ms)^(\s*members\s*=\s*)\[.*?\]",
        rf'\1["{normalized_mutated}"]',
        cargo_toml_text,
        count=1,
    )
    rewritten = re.sub(
        r"(?ms)^(\s*exclude\s*=\s*)\[.*?\]",
        r"\1[]",
        rewritten,
        count=1,
    )

    def replace_path_attr(match: re.Match[str]) -> str:
        prefix, raw_rel, suffix = match.group(1), match.group(2), match.group(3)
        norm = Path(raw_rel).as_posix().lstrip("./")
        if Path(raw_rel).is_absolute() or norm == normalized_mutated:
            return match.group(0)
        abs_target = (repo_root / norm).resolve()
        if abs_target.exists():
            return f"{prefix}{root_posix}/{norm}{suffix}"
        return match.group(0)

    rewritten = re.sub(r'(\bpath\s*=\s*")([^"]+)(")', replace_path_attr, rewritten)
    return rewritten


def materialize_isolated_workspace(repo_root: Path, temp_root: Path, source_file: str, mutated_text: str) -> Path:
    """Create or update an isolated overlay of `repo_root` with only `source_file` replaced.

    For Cargo workspaces, untouched path dependencies are pinned to their canonical
    `repo_root` paths so Cargo reuses compiled artifacts from `CARGO_TARGET_DIR` and
    recompiles only the single mutated crate.
    """
    rel = Path(source_file)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"source_file must be a safe repo-relative path: {source_file}")

    parts = rel.parts

    def populate_tree(src_dir: Path, dst_dir: Path, rel_parts: tuple[str, ...], skip_root_names: set[str]) -> None:
        dst_dir.mkdir(parents=True, exist_ok=True)
        target_part = rel_parts[0]
        is_leaf_dir = len(rel_parts) == 1
        for entry in src_dir.iterdir():
            name = entry.name
            if src_dir == repo_root and name in skip_root_names:
                continue
            dst_entry = dst_dir / name
            if name == target_part:
                if is_leaf_dir:
                    if dst_entry.is_symlink() or dst_entry.is_file():
                        dst_entry.unlink()
                    dst_entry.write_text(mutated_text, encoding="utf-8")
                else:
                    if dst_entry.is_symlink():
                        dst_entry.unlink()
                    populate_tree(entry, dst_entry, rel_parts[1:], skip_root_names)
            else:
                if not dst_entry.exists() and not dst_entry.is_symlink():
                    os.symlink(entry, dst_entry)

    root_manifest = repo_root / "Cargo.toml"
    crate_rel = find_crate_rel_dir(repo_root, source_file)
    if root_manifest.is_file() and crate_rel is not None and "[workspace]" in root_manifest.read_text(encoding="utf-8"):
        # Symlink untouched repo directories/files so relative `include_str!("../../...")`
        # paths resolve, while pinning workspace manifest/lock/nextest config separately.
        populate_tree(
            repo_root,
            temp_root,
            parts,
            SKIP_TOP_LEVEL_ENTRIES | {"Cargo.toml", "Cargo.lock", ".config"},
        )
        rewritten = rewrite_workspace_cargo_toml(root_manifest.read_text(encoding="utf-8"), repo_root, crate_rel)
        (temp_root / "Cargo.toml").write_text(rewritten, encoding="utf-8")

        lockfile = repo_root / "Cargo.lock"
        dst_lock = temp_root / "Cargo.lock"
        if lockfile.is_file() and not dst_lock.exists():
            shutil.copy2(lockfile, dst_lock)

        # Write a minimal single-crate nextest config so profile filters referencing
        # other workspace members (e.g. e2e_test) do not fail nextest config validation.
        nextest_cfg = temp_root / ".config" / "nextest.toml"
        nextest_cfg.parent.mkdir(parents=True, exist_ok=True)
        nextest_cfg.write_text(
            '[profile.ci]\nretries = 0\nfail-fast = false\n\n[profile.ci.junit]\npath = "junit.xml"\n',
            encoding="utf-8",
        )
        return temp_root

    populate_tree(repo_root, temp_root, parts, SKIP_TOP_LEVEL_ENTRIES)
    return temp_root


def restore_isolated_file(repo_root: Path, temp_root: Path, source_file: str) -> None:
    """Restore a single mutated overlay file back to a read-only symlink to `repo_root`."""
    dst_entry = temp_root / source_file
    if dst_entry.exists() or dst_entry.is_symlink():
        dst_entry.unlink()
    os.symlink(repo_root / source_file, dst_entry)


def parse_nextest_listing(stdout: str) -> set[str]:
    data = json.loads(stdout)
    matched: set[str] = set()
    for suite in data.get("rust-suites", {}).values():
        for test_name, testcase in suite.get("testcases", {}).items():
            if testcase.get("filter-match", {}).get("status") == "matches":
                matched.add(test_name)
    return matched


def parse_junit_results(junit_path: Path) -> dict[str, str]:
    """Return mapping of testcase name -> 'passed' | 'failed' | 'error' | 'skipped'."""
    tree = ET.parse(junit_path)
    root = tree.getroot()
    results: dict[str, str] = {}
    for tc in root.iter("testcase"):
        name = tc.attrib.get("name", "")
        if not name:
            continue
        if tc.find("skipped") is not None:
            results[name] = "skipped"
        elif tc.find("error") is not None:
            results[name] = "error"
        elif tc.find("failure") is not None:
            results[name] = "failed"
        else:
            results[name] = "passed"
    return results


def default_command_runner(cmd: list[str], cwd: Path, env: dict[str, str], timeout: int) -> StepOutcome:
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        return StepOutcome(returncode=proc.returncode, stdout=stdout, stderr=stderr, timed_out=False)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        stdout, stderr = proc.communicate()
        return StepOutcome(returncode=-1, stdout=stdout or "", stderr=stderr or "", timed_out=True)


def run_selected_suite(
    *,
    case: MutantCase,
    workspace: Path,
    target_dir: Path,
    env_base: dict[str, str],
    timeout_seconds: int,
    compile_timeout_seconds: int,
    runner=default_command_runner,
) -> tuple[str, str, str]:
    """Compile, verify selection, execute, and classify result using JUnit evidence.

    Returns `(status, reason, log_summary)` where `status` is one of:
    `passed`, `assertion_failed`, `compile_failed`, `missing_selected_tests`,
    `timeout`, `harness_error`.
    """
    env = dict(env_base)
    env["CARGO_TARGET_DIR"] = str(target_dir)
    kind_flag = target_kind_flag(case.target_kind)
    filter_expr = build_filter_expression(case.selected_tests)
    junit_candidates = (
        workspace / "target" / "nextest" / "ci" / "junit.xml",
        target_dir / "nextest" / "ci" / "junit.xml",
    )

    # Step 1: compile only, so build failures are never misclassified as test assertion kills.
    build_cmd = [
        "cargo",
        "nextest",
        "run",
        "--no-run",
        "--profile",
        "ci",
        "-p",
        case.package,
        kind_flag,
    ]
    build_out = runner(build_cmd, workspace, env, compile_timeout_seconds)
    if build_out.timed_out:
        return "timeout", "compile_timeout", sanitize_log_summary(build_out.stderr or build_out.stdout)
    if build_out.returncode != 0:
        return "compile_failed", f"compile_exit_{build_out.returncode}", sanitize_log_summary(build_out.stderr or build_out.stdout)

    # Step 2: verify exact selected tests exist and match.
    list_cmd = [
        "cargo",
        "nextest",
        "list",
        "--profile",
        "ci",
        "-p",
        case.package,
        kind_flag,
        "-E",
        filter_expr,
        "--message-format",
        "json",
    ]
    list_out = runner(list_cmd, workspace, env, compile_timeout_seconds)
    if list_out.timed_out:
        return "timeout", "list_timeout", sanitize_log_summary(list_out.stderr or list_out.stdout)
    if list_out.returncode != 0:
        return "harness_error", f"list_exit_{list_out.returncode}", sanitize_log_summary(list_out.stderr or list_out.stdout)
    try:
        matched = parse_nextest_listing(list_out.stdout)
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        return "harness_error", f"invalid_list_json:{exc}", sanitize_log_summary(list_out.stdout)
    expected_set = set(case.selected_tests)
    if matched != expected_set:
        return (
            "missing_selected_tests",
            f"matched={sorted(matched)} expected={sorted(expected_set)}",
            sanitize_log_summary(list_out.stderr or list_out.stdout),
        )

    # Step 3: remove any stale JUnit artifact and run tests with zero retries.
    for candidate in junit_candidates:
        if candidate.exists():
            candidate.unlink()

    run_cmd = [
        "cargo",
        "nextest",
        "run",
        "--profile",
        "ci",
        "-p",
        case.package,
        kind_flag,
        "--no-tests",
        "fail",
        "--retries",
        "0",
        "-E",
        filter_expr,
    ]
    run_out = runner(run_cmd, workspace, env, timeout_seconds)
    log_summary = sanitize_log_summary((run_out.stdout + "\n" + run_out.stderr).strip())
    if run_out.timed_out:
        return "timeout", "run_timeout", log_summary

    junit_path = next((p for p in junit_candidates if p.is_file()), None)
    if junit_path is None:
        return "harness_error", f"missing_junit_exit_{run_out.returncode}", log_summary

    try:
        junit_results = parse_junit_results(junit_path)
    except ET.ParseError as exc:
        return "harness_error", f"invalid_junit:{exc}", log_summary
    finally:
        # Avoid leaving an empty worktree-local target/nextest dir when CARGO_TARGET_DIR is external.
        ws_nextest = workspace / "target" / "nextest"
        if ws_nextest.exists() and target_dir.resolve() != (workspace / "target").resolve():
            shutil.rmtree(ws_nextest, ignore_errors=True)
            ws_target = workspace / "target"
            if ws_target.is_dir() and not any(ws_target.iterdir()):
                ws_target.rmdir()

    missing_in_junit = sorted(expected_set - set(junit_results))
    if missing_in_junit:
        return "harness_error", f"junit_missing_tests:{missing_in_junit}", log_summary

    selected_statuses = {name: junit_results[name] for name in case.selected_tests}
    if any(status == "skipped" for status in selected_statuses.values()):
        return "harness_error", f"skipped_selected_test:{selected_statuses}", log_summary
    if any(status == "error" for status in selected_statuses.values()):
        return "harness_error", f"error_selected_test:{selected_statuses}", log_summary

    if run_out.returncode == 0:
        if all(status == "passed" for status in selected_statuses.values()):
            return "passed", "all_selected_passed", log_summary
        return "harness_error", f"zero_exit_with_non_passing_junit:{selected_statuses}", log_summary

    if run_out.returncode == 100 and any(status == "failed" for status in selected_statuses.values()):
        return "assertion_failed", "junit_assertion_failure", log_summary

    return "harness_error", f"unexpected_exit_{run_out.returncode}:{selected_statuses}", log_summary


def verify_case(
    *,
    case: MutantCase,
    repo_root: Path,
    source_sha: str,
    harness_sha: str,
    target_dir: Path,
    env_base: dict[str, str],
    timeout_scale: float = 1.0,
    compile_timeout_override: int | None = None,
    shared_overlay: Path | None = None,
    baseline_cache: dict[tuple[str, str, tuple[str, ...]], tuple[str, str, str]] | None = None,
    runner=default_command_runner,
) -> dict:
    source_path = repo_root / case.source_file
    mutation_sha256 = sha256_bytes(
        (case.source_file + "\0" + case.original_snippet + "\0" + case.mutated_snippet).encode("utf-8")
    )
    base_record = {
        "schema_version": SCHEMA_VERSION,
        "source_sha": source_sha,
        "harness_sha": harness_sha,
        "case_id": case.case_id,
        "source_file": case.source_file,
        "symbol": case.symbol,
        "mutation_description": case.mutation_description,
        "mutation_sha256": mutation_sha256,
        "selected_tests": list(case.selected_tests),
        "expected_rejection": case.expected_rejection,
    }

    if not source_path.is_file():
        return {
            **base_record,
            "baseline_result": "invalid",
            "mutant_result": "not_run",
            "killed": False,
            "reason": "source_file_missing",
            "log_summary": f"missing source file: {case.source_file}",
        }

    source_text = source_path.read_text(encoding="utf-8")
    source_pre_sha = sha256_bytes(source_text.encode("utf-8"))
    occurrences = source_text.count(case.original_snippet)
    if occurrences != 1:
        return {
            **base_record,
            "baseline_result": "invalid",
            "mutant_result": "not_run",
            "killed": False,
            "reason": f"snippet_drift_occurrences_{occurrences}",
            "log_summary": f"{case.source_file} matched original_snippet {occurrences} time(s); expected 1",
        }

    timeout_seconds = max(1, int(case.timeout_seconds * timeout_scale))
    raw_compile_timeout = compile_timeout_override if compile_timeout_override is not None else case.compile_timeout_seconds
    compile_timeout_seconds = max(timeout_seconds, int(raw_compile_timeout * timeout_scale))

    # 1. Baseline verification on unmutated repository (cached across identical suites in one run).
    baseline_key = (case.package, case.target_kind, case.selected_tests)
    if baseline_cache is not None and baseline_key in baseline_cache:
        base_status, base_reason, base_log = baseline_cache[baseline_key]
    else:
        base_status, base_reason, base_log = run_selected_suite(
            case=case,
            workspace=repo_root,
            target_dir=target_dir,
            env_base=env_base,
            timeout_seconds=timeout_seconds,
            compile_timeout_seconds=compile_timeout_seconds,
            runner=runner,
        )
        if baseline_cache is not None:
            baseline_cache[baseline_key] = (base_status, base_reason, base_log)
    if base_status != "passed":
        return {
            **base_record,
            "baseline_result": base_status,
            "mutant_result": "not_run",
            "killed": False,
            "reason": f"baseline_{base_status}:{base_reason}",
            "log_summary": base_log,
        }

    # 2. Isolated mutant verification in a temp overlay workspace (never touching repo_root).
    mutated_text = source_text.replace(case.original_snippet, case.mutated_snippet, 1)
    if shared_overlay is not None:
        crate_dir = find_crate_rel_dir(repo_root, case.source_file)
        overlay_subdir = shared_overlay / (crate_dir.replace("/", "__") if crate_dir else "root")
        isolated = materialize_isolated_workspace(repo_root, overlay_subdir, case.source_file, mutated_text)
        try:
            mut_status, mut_reason, mut_log = run_selected_suite(
                case=case,
                workspace=isolated,
                target_dir=target_dir,
                env_base=env_base,
                timeout_seconds=timeout_seconds,
                compile_timeout_seconds=compile_timeout_seconds,
                runner=runner,
            )
        finally:
            restore_isolated_file(repo_root, overlay_subdir, case.source_file)
    else:
        with tempfile.TemporaryDirectory(prefix=f"rustfs-mutant-{case.case_id}-") as tmp_dir:
            isolated = materialize_isolated_workspace(repo_root, Path(tmp_dir), case.source_file, mutated_text)
            mut_status, mut_reason, mut_log = run_selected_suite(
                case=case,
                workspace=isolated,
                target_dir=target_dir,
                env_base=env_base,
                timeout_seconds=timeout_seconds,
                compile_timeout_seconds=compile_timeout_seconds,
                runner=runner,
            )

    # 3. Verify original checkout was never modified.
    source_post_sha = sha256_file(source_path)
    if source_post_sha != source_pre_sha:
        return {
            **base_record,
            "baseline_result": "passed",
            "mutant_result": "harness_error",
            "killed": False,
            "reason": "source_checkout_mutated",
            "log_summary": "source file hash changed during isolated mutation run",
        }

    killed = mut_status == "assertion_failed"
    if killed:
        reason = "killed_by_assertion"
    elif mut_status == "passed":
        reason = "mutant_survived"
    else:
        reason = f"invalid_{mut_status}:{mut_reason}"

    return {
        **base_record,
        "baseline_result": "passed",
        "mutant_result": mut_status,
        "killed": killed,
        "reason": reason,
        "log_summary": mut_log,
    }


def _write_fake_junit(target_dir: Path, outcomes: dict[str, str]) -> None:
    junit = target_dir / "nextest" / "ci" / "junit.xml"
    junit.parent.mkdir(parents=True, exist_ok=True)
    cases_xml = []
    for name, status in outcomes.items():
        if status == "passed":
            cases_xml.append(f'<testcase name="{name}"/>')
        elif status == "failed":
            cases_xml.append(f'<testcase name="{name}"><failure message="assertion failed">diff</failure></testcase>')
        elif status == "skipped":
            cases_xml.append(f'<testcase name="{name}"><skipped/></testcase>')
        elif status == "error":
            cases_xml.append(f'<testcase name="{name}"><error message="harness crash"/></testcase>')
    junit.write_text(f'<testsuites><testsuite name="test">{"".join(cases_xml)}</testsuite></testsuites>', encoding="utf-8")


class RejectionMutantSelfTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "repo"
        self.target = Path(self.tmp.name) / "target"
        (self.root / "src").mkdir(parents=True)
        self.original_code = "fn reject(x: bool) -> bool {\n    if x { return false; }\n    true\n}\n"
        (self.root / "src/lib.rs").write_text(self.original_code, encoding="utf-8")
        snip = "    if x { return false; }"
        self.case = MutantCase(
            case_id="demo_reject",
            source_file="src/lib.rs",
            symbol="reject",
            original_snippet_sha256=sha256_bytes(snip.encode("utf-8")),
            original_snippet=snip,
            mutated_snippet="    let _ = x;",
            mutation_description="bypass reject branch",
            package="demo",
            target_kind="lib",
            selected_tests=("tests::rejects_invalid",),
            expected_rejection="must reject invalid input",
            timeout_seconds=30,
        )

    def _make_runner(self, *, base_rc=0, base_junit="passed", mut_build_rc=0, mut_list=None, mut_rc=100, mut_junit="failed", mut_timeout=False, write_mut_junit=True):
        seen_workspaces: list[Path] = []

        def runner(cmd: list[str], cwd: Path, env: dict[str, str], timeout: int) -> StepOutcome:
            is_mutant = cwd != self.root
            if is_mutant:
                seen_workspaces.append(cwd)
                self.assertIn("let _ = x;", (cwd / "src/lib.rs").read_text(encoding="utf-8"))
                self.assertIn("if x { return false; }", (self.root / "src/lib.rs").read_text(encoding="utf-8"))
            target_dir = Path(env["CARGO_TARGET_DIR"])
            if "--no-run" in cmd:
                if is_mutant and mut_build_rc != 0:
                    return StepOutcome(mut_build_rc, "", "error[E0308]: mismatched types")
                return StepOutcome(0, "", "")
            if "list" in cmd:
                names = mut_list if (is_mutant and mut_list is not None) else list(self.case.selected_tests)
                payload = {
                    "rust-suites": {
                        "demo": {
                            "testcases": {
                                name: {"filter-match": {"status": "matches"}} for name in names
                            }
                        }
                    }
                }
                return StepOutcome(0, json.dumps(payload), "")
            if is_mutant:
                if mut_timeout:
                    return StepOutcome(-1, "", "timed out", timed_out=True)
                if write_mut_junit:
                    _write_fake_junit(target_dir, {self.case.selected_tests[0]: mut_junit})
                return StepOutcome(mut_rc, "", "assertion failed: AccessDenied")
            _write_fake_junit(target_dir, {self.case.selected_tests[0]: base_junit})
            return StepOutcome(base_rc, "", "")

        return runner, seen_workspaces

    def test_valid_kill_with_baseline_green_and_assertion_red(self) -> None:
        runner, workspaces = self._make_runner()
        res = verify_case(
            case=self.case,
            repo_root=self.root,
            source_sha="a" * 40,
            harness_sha="b" * 64,
            target_dir=self.target,
            env_base={},
            runner=runner,
        )
        self.assertEqual(res["baseline_result"], "passed")
        self.assertEqual(res["mutant_result"], "assertion_failed")
        self.assertTrue(res["killed"])
        self.assertEqual(res["reason"], "killed_by_assertion")
        self.assertTrue(workspaces)
        self.assertEqual((self.root / "src/lib.rs").read_text(encoding="utf-8"), self.original_code)

    def test_mutant_survived_fails(self) -> None:
        runner, _ = self._make_runner(mut_rc=0, mut_junit="passed")
        res = verify_case(
            case=self.case,
            repo_root=self.root,
            source_sha="a" * 40,
            harness_sha="b" * 64,
            target_dir=self.target,
            env_base={},
            runner=runner,
        )
        self.assertFalse(res["killed"])
        self.assertEqual(res["reason"], "mutant_survived")

    def test_baseline_failure_stops_and_never_counts_as_killed(self) -> None:
        runner, workspaces = self._make_runner(base_rc=100, base_junit="failed")
        res = verify_case(
            case=self.case,
            repo_root=self.root,
            source_sha="a" * 40,
            harness_sha="b" * 64,
            target_dir=self.target,
            env_base={},
            runner=runner,
        )
        self.assertFalse(res["killed"])
        self.assertEqual(res["mutant_result"], "not_run")
        self.assertEqual(workspaces, [])

    def test_compile_failure_zero_tests_timeout_and_harness_error_are_invalid(self) -> None:
        for label, kwargs, expected_mutant_result in (
            ("compile_failed", {"mut_build_rc": 101}, "compile_failed"),
            ("zero_tests", {"mut_list": []}, "missing_selected_tests"),
            ("timeout", {"mut_timeout": True}, "timeout"),
            ("missing_junit", {"mut_rc": 100, "write_mut_junit": False}, "harness_error"),
            ("skipped_test", {"mut_rc": 100, "mut_junit": "skipped"}, "harness_error"),
            ("non_100_exit", {"mut_rc": 2, "mut_junit": "failed"}, "harness_error"),
        ):
            with self.subTest(label=label):
                runner, _ = self._make_runner(**kwargs)
                res = verify_case(
                    case=self.case,
                    repo_root=self.root,
                    source_sha="a" * 40,
                    harness_sha="b" * 64,
                    target_dir=self.target,
                    env_base={},
                    runner=runner,
                )
                self.assertFalse(res["killed"])
                self.assertEqual(res["mutant_result"], expected_mutant_result)

    def test_snippet_drift_fails_closed(self) -> None:
        (self.root / "src/lib.rs").write_text("fn reject(_x: bool) -> bool { true }\n", encoding="utf-8")
        runner, _ = self._make_runner()
        res = verify_case(
            case=self.case,
            repo_root=self.root,
            source_sha="a" * 40,
            harness_sha="b" * 64,
            target_dir=self.target,
            env_base={},
            runner=runner,
        )
        self.assertFalse(res["killed"])
        self.assertEqual(res["reason"], "snippet_drift_occurrences_0")

    def test_committed_registry_parses_and_matches_live_repo_snippets(self) -> None:
        cases = load_cases(ROOT / DEFAULT_CASES_PATH)
        self.assertGreaterEqual(len(cases), 3)
        for case in cases:
            text = (ROOT / case.source_file).read_text(encoding="utf-8")
            self.assertEqual(
                text.count(case.original_snippet),
                1,
                f"case {case.case_id} snippet must match exactly once in {case.source_file}",
            )

    def test_workspace_overlay_pins_sibling_paths_to_repo_root(self) -> None:
        ws = Path(self.tmp.name) / "ws"
        (ws / "crates" / "target_crate" / "src").mkdir(parents=True)
        (ws / "crates" / "dep_crate" / "src").mkdir(parents=True)
        (ws / "Cargo.toml").write_text(
            '[workspace]\nmembers = [\n  "crates/target_crate",\n  "crates/dep_crate",\n]\n'
            'exclude = ["vendor/hyper"]\n'
            '[workspace.dependencies]\n'
            'target-crate = { path = "crates/target_crate", version = "1.0.0" }\n'
            'dep-crate = { path = "crates/dep_crate", version = "1.0.0" }\n',
            encoding="utf-8",
        )
        (ws / "crates" / "target_crate" / "Cargo.toml").write_text('[package]\nname = "target-crate"\n', encoding="utf-8")
        (ws / "crates" / "dep_crate" / "Cargo.toml").write_text('[package]\nname = "dep-crate"\n', encoding="utf-8")
        (ws / "crates" / "target_crate" / "src" / "lib.rs").write_text("orig\n", encoding="utf-8")
        (ws / "crates" / "dep_crate" / "src" / "lib.rs").write_text("dep\n", encoding="utf-8")

        overlay = Path(self.tmp.name) / "ws-overlay"
        materialize_isolated_workspace(ws, overlay, "crates/target_crate/src/lib.rs", "mutated\n")
        rewritten = (overlay / "Cargo.toml").read_text(encoding="utf-8")
        self.assertIn('members = ["crates/target_crate"]', rewritten)
        self.assertIn(f'dep-crate = {{ path = "{ws.resolve().as_posix()}/crates/dep_crate"', rewritten)
        self.assertIn('target-crate = { path = "crates/target_crate"', rewritten)
        self.assertEqual((overlay / "crates/target_crate/src/lib.rs").read_text(encoding="utf-8"), "mutated\n")
        self.assertEqual((ws / "crates/target_crate/src/lib.rs").read_text(encoding="utf-8"), "orig\n")

    def test_baseline_cache_deduplicates_identical_suite(self) -> None:
        baseline_runs = 0
        base_runner, _ = self._make_runner()

        def counting_runner(cmd: list[str], cwd: Path, env: dict[str, str], timeout: int) -> StepOutcome:
            nonlocal baseline_runs
            if cwd == self.root and "--no-run" in cmd:
                baseline_runs += 1
            return base_runner(cmd, cwd, env, timeout)

        cache: dict[tuple[str, str, tuple[str, ...]], tuple[str, str, str]] = {}
        for _ in range(2):
            res = verify_case(
                case=self.case,
                repo_root=self.root,
                source_sha="a" * 40,
                harness_sha="b" * 64,
                target_dir=self.target,
                env_base={},
                baseline_cache=cache,
                runner=counting_runner,
            )
            self.assertTrue(res["killed"])
        self.assertEqual(baseline_runs, 1)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", default=DEFAULT_CASES_PATH, help="Path to rejection-mutant TOML registry")
    parser.add_argument("--case", action="append", default=[], help="Optional subset of registered case IDs")
    parser.add_argument("--output", help="Optional path to write JSON report")
    parser.add_argument("--timeout-scale", type=float, default=1.0)
    parser.add_argument("--compile-timeout", type=int, default=None, help="Override per-step compile timeout seconds")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)

    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(RejectionMutantSelfTests)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1

    cases_path = Path(args.cases)
    if not cases_path.is_absolute():
        cases_path = ROOT / cases_path

    try:
        all_cases = load_cases(cases_path)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        print(f"ERROR: failed to load mutant registry {cases_path}: {exc}", file=sys.stderr)
        return 1

    if args.case:
        by_id = {c.case_id: c for c in all_cases}
        unknown = [cid for cid in args.case if cid not in by_id]
        if unknown:
            print(f"ERROR: unknown rejection-mutant case id(s): {unknown}", file=sys.stderr)
            return 1
        selected_cases = [by_id[cid] for cid in args.case]
    else:
        selected_cases = all_cases

    source_sha = git_head_sha(ROOT)
    harness_sha = compute_harness_sha(Path(__file__).resolve(), cases_path)

    env_target = os.environ.get("CARGO_TARGET_DIR", "").strip()
    temp_target: tempfile.TemporaryDirectory[str] | None = None
    if env_target:
        target_dir = Path(env_target)
    else:
        temp_target = tempfile.TemporaryDirectory(prefix="rustfs-mutant-target-")
        target_dir = Path(temp_target.name)

    try:
        results = []
        baseline_cache: dict[tuple[str, str, tuple[str, ...]], tuple[str, str, str]] = {}
        with tempfile.TemporaryDirectory(prefix="rustfs-mutant-overlay-") as overlay_dir:
            overlay_path = Path(overlay_dir)
            for idx, case in enumerate(selected_cases, start=1):
                print(
                    f"==> [{idx}/{len(selected_cases)}] verifying {case.case_id} ({case.source_file}::{case.symbol})",
                    flush=True,
                )
                res = verify_case(
                    case=case,
                    repo_root=ROOT,
                    source_sha=source_sha,
                    harness_sha=harness_sha,
                    target_dir=target_dir,
                    env_base=dict(os.environ),
                    timeout_scale=args.timeout_scale,
                    compile_timeout_override=args.compile_timeout,
                    shared_overlay=overlay_path,
                    baseline_cache=baseline_cache,
                )
                results.append(res)
                status = "KILLED" if res["killed"] else "FAILED"
                print(
                    f"[{status}] {res['case_id']}: baseline={res['baseline_result']} "
                    f"mutant={res['mutant_result']} reason={res['reason']}",
                    flush=True,
                )
    finally:
        if temp_target is not None:
            temp_target.cleanup()

    all_killed = all(r["baseline_result"] == "passed" and r["killed"] is True for r in results)
    report = {
        "schema_version": SCHEMA_VERSION,
        "source_sha": source_sha,
        "harness_sha": harness_sha,
        "all_killed": all_killed,
        "cases": results,
    }

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    for r in results:
        status = "KILLED" if r["killed"] else "FAILED"
        print(
            f"[{status}] {r['case_id']} ({r['source_file']}::{r['symbol']}): "
            f"baseline={r['baseline_result']} mutant={r['mutant_result']} reason={r['reason']}"
        )

    return 0 if all_killed else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
