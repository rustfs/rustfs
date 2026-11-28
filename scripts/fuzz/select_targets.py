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

"""Trusted target selection for fuzz smoke/nightly and rejection-mutant gates.

Maps changed files against registered fuzz targets in `fuzz/Cargo.toml` and
fails closed to the full target + mutant set whenever the event requires full
validation, git history is unavailable, or inputs are malformed.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[2]
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
ALLOWED_MODES = ("smoke", "nightly")
MAX_BUDGET_SECONDS = 3600

# Canonical registered fuzz targets (must match fuzz/Cargo.toml [[bin]] names).
ALL_TARGETS = (
    "archive_extract",
    "bucket_validation",
    "local_metadata",
    "notify_xml",
    "path_containment",
    "policy_ingress",
    "sigv4_request",
)

# Shared harness / workspace dependency changes trigger the full set + mutants.
FULL_TRIGGER_PATTERNS = (
    "fuzz/**",
    "scripts/fuzz/**",
    ".github/workflows/fuzz.yml",
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    ".config/security-rejection-mutants.toml",
    "scripts/security/check_rejection_mutants.py",
)

# Per-crate / per-surface path triggers. Every direct `../crates/*` dependency
# of `fuzz/Cargo.toml` must remain represented here.
TARGET_PATH_RULES: dict[str, tuple[str, ...]] = {
    "archive_extract": ("crates/utils/**",),
    "bucket_validation": ("crates/ecstore/**", "crates/utils/**"),
    "local_metadata": ("crates/filemeta/**", "crates/utils/**"),
    "notify_xml": ("crates/notify/**",),
    "path_containment": ("crates/ecstore/**", "crates/utils/**"),
    "policy_ingress": ("crates/policy/**", "crates/security-governance/**"),
    "sigv4_request": ("crates/signer/**", "crates/utils/**", "rustfs/src/auth.rs"),
}

MUTANT_TRIGGER_PATTERNS = (
    "rustfs/src/auth.rs",
    "crates/signer/**",
    "crates/notify/**",
    "fuzz/**",
    "scripts/fuzz/**",
    ".github/workflows/fuzz.yml",
    "Cargo.toml",
    "Cargo.lock",
    ".config/security-rejection-mutants.toml",
    "scripts/security/check_rejection_mutants.py",
)


def _match_glob(path: str, pattern: str) -> bool:
    if pattern.endswith("/**"):
        prefix = pattern[:-3]
        return path == prefix or path.startswith(prefix + "/")
    return fnmatch.fnmatchcase(path, pattern)


def is_documentation_path(path: str) -> bool:
    parts = PurePosixPath(path).parts
    if not parts or path.startswith("/") or any(part in (".", "..") for part in parts):
        return False
    if parts[-1] == "AGENTS.md":
        return True
    if len(parts) == 1 and (path.endswith(".md") or path == "LICENSE" or path.startswith("LICENSE-")):
        return True
    if path.startswith(("docs/", ".agents/skills/")) and path.endswith(".md"):
        return True
    return path.startswith("docs/") and path.endswith((".png", ".jpg", ".svg"))


def load_manifest_targets(root: Path) -> list[str]:
    manifest = tomllib.loads((root / "fuzz/Cargo.toml").read_text(encoding="utf-8"))
    bins = sorted(item["name"] for item in manifest.get("bin", []) if isinstance(item.get("name"), str))
    if not bins:
        raise ValueError("fuzz/Cargo.toml contains no [[bin]] targets")
    if tuple(bins) != ALL_TARGETS:
        raise ValueError(f"registered targets mismatch: manifest={bins} selector={list(ALL_TARGETS)}")
    return bins


def parse_requested_targets(raw: str, registered: list[str]) -> list[str]:
    text = raw.strip()
    if not text or text == "auto":
        return []
    if text == "all":
        return list(registered)
    registered_set = set(registered)
    selected: list[str] = []
    for token in text.split(","):
        name = token.strip()
        if not name or not re.fullmatch(r"[a-z0-9_]+", name) or name not in registered_set:
            raise ValueError(f"unregistered or invalid fuzz target: {name!r}")
        if name not in selected:
            selected.append(name)
    return sorted(selected)


def validate_budget(raw: str) -> int:
    text = raw.strip()
    if not re.fullmatch(r"[1-9][0-9]*", text):
        raise ValueError(f"max_total_time must be a positive integer: {raw!r}")
    value = int(text)
    if value > MAX_BUDGET_SECONDS:
        raise ValueError(f"max_total_time exceeds {MAX_BUDGET_SECONDS}s cap: {value}")
    return value


def changed_files_from_git(root: Path, base_sha: str, head_sha: str) -> list[str] | None:
    if not SHA_RE.fullmatch(base_sha) or not SHA_RE.fullmatch(head_sha):
        return None
    if base_sha == "0" * 40:
        return None
    try:
        output = subprocess.check_output(
            ["git", "diff", "--name-only", "-z", f"{base_sha}..{head_sha}"],
            cwd=root,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return sorted({entry for entry in output.decode("utf-8", errors="replace").split("\0") if entry})


def select_from_changed_files(
    *,
    event_name: str,
    mode: str,
    max_total_time: int,
    registered: list[str],
    changed_files: list[str] | None,
    requested_targets: str = "auto",
    ci_mode: str = "",
    base_sha: str = "",
    head_sha: str = "",
) -> dict:
    if mode not in ALLOWED_MODES:
        raise ValueError(f"unsupported fuzz mode: {mode!r}")

    explicit_targets = parse_requested_targets(requested_targets, registered)
    if explicit_targets:
        return {
            "schema_version": 1,
            "applicable": True,
            "reason": "explicit_targets",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": explicit_targets,
            "run_mutants": event_name != "schedule",
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": len(changed_files) if changed_files is not None else None,
        }

    if event_name == "push" and ci_mode == "proven":
        return {
            "schema_version": 1,
            "applicable": False,
            "reason": "na_proven_merge_group_push",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": [],
            "run_mutants": False,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": len(changed_files) if changed_files is not None else None,
        }

    if event_name in ("merge_group", "schedule", "workflow_dispatch", "push"):
        return {
            "schema_version": 1,
            "applicable": True,
            "reason": f"{event_name}_full_scope",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": list(registered),
            "run_mutants": event_name != "schedule",
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": len(changed_files) if changed_files is not None else None,
        }

    if event_name != "pull_request":
        return {
            "schema_version": 1,
            "applicable": True,
            "reason": "fallback_unknown_event",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": list(registered),
            "run_mutants": True,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": None,
        }

    # pull_request: fail closed to full set if diff is unavailable or empty.
    if not changed_files:
        return {
            "schema_version": 1,
            "applicable": True,
            "reason": "fallback_unknown_diff",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": list(registered),
            "run_mutants": True,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": 0 if changed_files == [] else None,
        }

    if all(is_documentation_path(path) for path in changed_files):
        return {
            "schema_version": 1,
            "applicable": False,
            "reason": "na_docs_only",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": [],
            "run_mutants": False,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": len(changed_files),
        }

    if any(_match_glob(path, pattern) for path in changed_files for pattern in FULL_TRIGGER_PATTERNS):
        return {
            "schema_version": 1,
            "applicable": True,
            "reason": "shared_security_input_surface",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": list(registered),
            "run_mutants": True,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": len(changed_files),
        }

    matched_targets = sorted(
        target
        for target in registered
        if any(_match_glob(path, pattern) for path in changed_files for pattern in TARGET_PATH_RULES.get(target, ()))
    )
    run_mutants = any(_match_glob(path, pattern) for path in changed_files for pattern in MUTANT_TRIGGER_PATTERNS)

    if not matched_targets and not run_mutants:
        return {
            "schema_version": 1,
            "applicable": False,
            "reason": "na_unrelated_paths",
            "event_name": event_name,
            "mode": mode,
            "max_total_time": max_total_time,
            "targets": [],
            "run_mutants": False,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "changed_files_count": len(changed_files),
        }

    return {
        "schema_version": 1,
        "applicable": True,
        "reason": "relevant_paths",
        "event_name": event_name,
        "mode": mode,
        "max_total_time": max_total_time,
        "targets": matched_targets,
        "run_mutants": run_mutants,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "changed_files_count": len(changed_files),
    }


def write_github_outputs(path: Path, selection: dict) -> None:
    lines = [
        f"applicable={'true' if selection['applicable'] else 'false'}",
        f"reason={selection['reason']}",
        f"mode={selection['mode']}",
        f"max_total_time={selection['max_total_time']}",
        f"targets_json={json.dumps(selection['targets'])}",
        f"targets_csv={','.join(selection['targets'])}",
        f"has_targets={'true' if selection['targets'] else 'false'}",
        f"run_mutants={'true' if selection['run_mutants'] else 'false'}",
    ]
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


class SelectTargetsTests(unittest.TestCase):
    def test_signer_notify_and_auth_paths_select_expected_targets_and_mutants(self) -> None:
        registered = list(ALL_TARGETS)
        for path, expected_targets in (
            ("crates/signer/src/request_signature_v4.rs", ["sigv4_request"]),
            ("crates/notify/src/rules/xml_config.rs", ["notify_xml"]),
            ("rustfs/src/auth.rs", ["sigv4_request"]),
        ):
            with self.subTest(path=path):
                result = select_from_changed_files(
                    event_name="pull_request",
                    mode="smoke",
                    max_total_time=60,
                    registered=registered,
                    changed_files=[path],
                )
                self.assertTrue(result["applicable"])
                self.assertEqual(result["targets"], expected_targets)
                self.assertTrue(result["run_mutants"])

    def test_docs_only_and_unrelated_paths_record_explicit_na(self) -> None:
        registered = list(ALL_TARGETS)
        docs = select_from_changed_files(
            event_name="pull_request",
            mode="smoke",
            max_total_time=60,
            registered=registered,
            changed_files=["README.md", "docs/testing/ci-gates.md"],
        )
        self.assertFalse(docs["applicable"])
        self.assertEqual(docs["reason"], "na_docs_only")
        self.assertEqual(docs["targets"], [])
        self.assertFalse(docs["run_mutants"])

        unrelated = select_from_changed_files(
            event_name="pull_request",
            mode="smoke",
            max_total_time=60,
            registered=registered,
            changed_files=["crates/obs/src/telemetry/otel.rs"],
        )
        self.assertFalse(unrelated["applicable"])
        self.assertEqual(unrelated["reason"], "na_unrelated_paths")
        self.assertEqual(unrelated["targets"], [])
        self.assertFalse(unrelated["run_mutants"])

    def test_merge_group_and_missing_diff_fall_back_to_full_set(self) -> None:
        registered = list(ALL_TARGETS)
        mg = select_from_changed_files(
            event_name="merge_group",
            mode="smoke",
            max_total_time=60,
            registered=registered,
            changed_files=["README.md"],
        )
        self.assertTrue(mg["applicable"])
        self.assertEqual(mg["targets"], registered)
        self.assertTrue(mg["run_mutants"])

        unknown = select_from_changed_files(
            event_name="pull_request",
            mode="smoke",
            max_total_time=60,
            registered=registered,
            changed_files=None,
        )
        self.assertTrue(unknown["applicable"])
        self.assertEqual(unknown["reason"], "fallback_unknown_diff")
        self.assertEqual(unknown["targets"], registered)
        self.assertTrue(unknown["run_mutants"])

        sched = select_from_changed_files(
            event_name="schedule",
            mode="nightly",
            max_total_time=300,
            registered=registered,
            changed_files=[],
            requested_targets="all",
        )
        self.assertTrue(sched["applicable"])
        self.assertEqual(sched["targets"], registered)
        self.assertFalse(sched["run_mutants"])

    def test_rejects_unregistered_targets_shell_injection_and_invalid_budgets(self) -> None:
        registered = list(ALL_TARGETS)
        for bad in ("unknown_target", "sigv4_request;rm -rf /", "$(id)", ","):
            with self.assertRaises(ValueError):
                parse_requested_targets(bad, registered)
        for bad_budget in ("0", "-1", "abc", "99999"):
            with self.assertRaises(ValueError):
                validate_budget(bad_budget)

    def test_manifest_matches_selector_and_direct_deps_are_covered(self) -> None:
        registered = load_manifest_targets(ROOT)
        self.assertEqual(len(registered), 7)
        manifest = tomllib.loads((ROOT / "fuzz/Cargo.toml").read_text(encoding="utf-8"))
        all_rule_paths = {p for rules in TARGET_PATH_RULES.values() for p in rules}
        for dep in manifest.get("dependencies", {}).values():
            if isinstance(dep, dict) and isinstance(dep.get("path"), str) and dep["path"].startswith("../crates/"):
                expected = f"{dep['path'].removeprefix('../')}/**"
                self.assertIn(expected, all_rule_paths)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME", "pull_request"))
    parser.add_argument("--mode", default="smoke")
    parser.add_argument("--max-total-time", default="60")
    parser.add_argument("--targets", default="auto")
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--head-sha", default="")
    parser.add_argument("--ci-mode", default="")
    parser.add_argument("--output", help="Write JSON selection manifest to this file")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)

    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelectTargetsTests)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1

    try:
        registered = load_manifest_targets(ROOT)
        budget = validate_budget(args.max_total_time)
        changed = changed_files_from_git(ROOT, args.base_sha, args.head_sha)
        selection = select_from_changed_files(
            event_name=args.event_name,
            mode=args.mode,
            max_total_time=budget,
            registered=registered,
            changed_files=changed,
            requested_targets=args.targets,
            ci_mode=args.ci_mode,
            base_sha=args.base_sha,
            head_sha=args.head_sha,
        )
        selection["manifest_sha256"] = hashlib.sha256(
            json.dumps(selection, sort_keys=True).encode("utf-8")
        ).hexdigest()
    except ValueError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.github_output:
        write_github_outputs(Path(args.github_output), selection)

    print(json.dumps(selection, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
