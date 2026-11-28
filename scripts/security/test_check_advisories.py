#!/usr/bin/env python3
# Copyright 2026 RustFS Team
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
"""Contract and negative tests for scripts/security/check_advisories.py (rustfs/backlog#2668)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_advisories as gate  # noqa: E402


def make_diagnostic(
    adv_id: str,
    crate: str,
    version: str,
    *,
    code: str = "vulnerability",
    parent_crate: str = "rustfs-iam",
) -> dict:
    return {
        "type": "diagnostic",
        "fields": {
            "advisory": {
                "id": adv_id,
                "package": crate,
                "title": f"Advisory {adv_id} on {crate}",
                "url": f"https://rustsec.org/advisories/{adv_id}",
                "informational": None if code == "vulnerability" else "unmaintained",
                "cvss": "CVSS:3.1/AV:N/AC:H/PR:N/UI:N/S:U/C:H/I:N/A:N" if code == "vulnerability" else None,
                "aliases": [],
            },
            "code": code,
            "severity": "error",
            "message": f"Advisory {adv_id} detected",
            "graphs": [
                {
                    "Krate": {"name": crate, "version": version},
                    "parents": [
                        {
                            "Krate": {"name": parent_crate, "version": "1.0.5"},
                            "parents": [{"Krate": {"name": "rustfs", "version": "1.0.5"}}],
                        }
                    ],
                }
            ],
            "labels": [{"span": f"{crate} {version} registry+https://github.com/rust-lang/crates.io-index"}],
        },
    }


def make_summary(errors: int, notes: int = 0) -> dict:
    return {"type": "summary", "fields": {"advisories": {"errors": errors, "helps": 0, "notes": notes, "warnings": 0}}}


class FixtureHarness:
    """Isolated git repo + fake advisory DB + mock cargo-deny binary."""

    def __init__(self, tmp_dir: str) -> None:
        self.root = Path(tmp_dir)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.db_dir = self.root / "advisory-db"
        self.db_dir.mkdir()
        self.mock_bin = self.root / "mock-cargo-deny"
        self.out_json = self.root / "advisories-gate.json"
        self.raw_log = self.root / "cargo-deny.log"
        self.mock_state = self.root / "mock-state.json"

        self._git(self.repo, "init", "-q")
        self._git(self.repo, "config", "user.name", "Test")
        self._git(self.repo, "config", "user.email", "test@example.invalid")
        self.write("Cargo.toml", '[workspace]\nmembers = []\n')
        self.write("Cargo.lock", 'version = 4\n\n[[package]]\nname = "rsa"\nversion = "0.9.10"\n')

        self._git(self.db_dir, "init", "-q")
        self._git(self.db_dir, "config", "user.name", "DB")
        self._git(self.db_dir, "config", "user.email", "db@example.invalid")
        self._git(self.db_dir, "remote", "add", "origin", gate.DEFAULT_ADVISORY_DB_URL)
        (self.db_dir / "README.md").write_text("rustsec fixture\n", encoding="utf-8")
        self._git(self.db_dir, "add", "-A")
        self._git(self.db_dir, "commit", "-qm", "db snapshot")
        self.db_rev = self._git(self.db_dir, "rev-parse", "HEAD")

        self.set_mock()
        self._install_mock_cargo_deny()

    def _git(self, cwd: Path, *args: str) -> str:
        return subprocess.check_output(
            ["git", "-c", "core.hooksPath=/dev/null", *args],
            cwd=cwd,
            text=True,
            stderr=subprocess.PIPE,
        ).strip()

    def write(self, rel: str, content: str) -> Path:
        path = self.repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def commit(self, msg: str = "fixture") -> str:
        self._git(self.repo, "add", "-A")
        self._git(self.repo, "commit", "-qm", msg)
        return self._git(self.repo, "rev-parse", "HEAD")

    def set_fetch_head_age(self, now: datetime, age_seconds: int) -> None:
        fetch_head = self.db_dir / ".git" / "FETCH_HEAD"
        fetch_head.write_text(f"{self.db_rev}\t\tbranch 'main' of {gate.DEFAULT_ADVISORY_DB_URL}\n", encoding="utf-8")
        ts = (now - timedelta(seconds=age_seconds)).timestamp()
        os.utime(fetch_head, (ts, ts))

    def set_mock(
        self,
        *,
        version: str = gate.EXPECTED_CARGO_DENY_VERSION,
        fetch_exit: int = 0,
        fetch_stderr: str = "",
        hits: list[dict] | None = None,
        crash_check: bool = False,
        malformed_output: bool = False,
    ) -> None:
        self.mock_state.write_text(
            json.dumps({
                "version": version,
                "fetch_exit": fetch_exit,
                "fetch_stderr": fetch_stderr,
                "hits": hits if hits is not None else [],
                "crash_check": crash_check,
                "malformed_output": malformed_output,
            }),
            encoding="utf-8",
        )

    def _install_mock_cargo_deny(self) -> None:
        script = f"""#!{sys.executable}
import json, sys, tomllib
from pathlib import Path

state = json.loads(Path({str(self.mock_state)!r}).read_text(encoding="utf-8"))
args = sys.argv[1:]
if args == ["--version"]:
    print(f"cargo-deny {{state['version']}}")
    raise SystemExit(0)
if "fetch" in args:
    if state["fetch_stderr"]:
        print(state["fetch_stderr"], file=sys.stderr)
    raise SystemExit(state["fetch_exit"])
if "check" in args:
    if state["crash_check"]:
        print("fatal metadata error", file=sys.stderr)
        raise SystemExit(2)
    if state["malformed_output"]:
        print("NOT-JSON-OUTPUT")
        raise SystemExit(1)
    cfg_path = Path(args[args.index("--config") + 1])
    cfg = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
    ignored = set()
    for item in cfg.get("advisories", {{}}).get("ignore", []):
        if isinstance(item, str):
            ignored.add(item)
        elif isinstance(item, dict) and "id" in item:
            ignored.add(item["id"])
    emitted = 0
    for hit in state["hits"]:
        adv_id = (hit.get("fields", {{}}).get("advisory") or {{}}).get("id")
        if adv_id and adv_id in ignored:
            continue
        print(json.dumps(hit))
        emitted += 1
    print(json.dumps({{"type": "summary", "fields": {{"advisories": {{"errors": emitted, "helps": 0, "notes": len(ignored), "warnings": 0}}}}}}))
    raise SystemExit(1 if emitted else 0)
raise SystemExit(2)
"""
        self.mock_bin.write_text(script, encoding="utf-8")
        self.mock_bin.chmod(self.mock_bin.stat().st_mode | stat.S_IXUSR)

    def write_valid_pair(
        self,
        *,
        adv_id: str = "RUSTSEC-2023-0071",
        crate: str = "rsa",
        affected_versions: str = "=0.9.10",
        approved_at: str = "2026-10-01T00:00:00Z",
        expires_at: str = "2026-12-15T00:00:00Z",
        approval_ref: str = "https://github.com/rustfs/rustfs/pull/9492",
    ) -> None:
        self.write(
            "deny.toml",
            f"""[graph]
all-features = true
no-default-features = false

[advisories]
version = 2
yanked = "deny"
ignore = [
    {{ id = "{adv_id}", reason = "tracked temporary exception" }},
]
""",
        )
        self.write(
            ".config/advisory-exceptions.toml",
            f"""schema_version = 1

[[exceptions]]
advisory_id = "{adv_id}"
crate = "{crate}"
affected_versions = "{affected_versions}"
owner = "rustfs-maintainers"
tracking_issue = "https://github.com/rustfs/backlog/issues/2668"
reason = "Transitive dependency waiting on upstream release."
mitigation = "No vulnerable decryption oracle reachable from server handlers."
approved_at = "{approved_at}"
expires_at = "{expires_at}"
approval_reference = "{approval_ref}"
""",
        )

    def run(
        self,
        *,
        now: str = "2026-11-26T00:00:00Z",
        base_ref: str = "",
        trusted_approvals: list[dict] | None = None,
        pr_author: str | None = None,
        offline_only: bool = False,
        env_override: dict[str, str] | None = None,
    ) -> tuple[dict, int]:
        argv = [
            "--repo-root",
            str(self.repo),
            "--output",
            str(self.out_json),
            "--raw-log",
            str(self.raw_log),
            "--advisory-db-dir",
            str(self.db_dir),
            "--cargo-deny-bin",
            str(self.mock_bin),
            "--skip-cargo-metadata",
        ]
        if now is not None:
            argv += ["--now", now]
        if base_ref:
            argv += ["--base-ref", base_ref]
        if offline_only:
            argv.append("--offline-only")
        if pr_author:
            argv += ["--pr-author", pr_author]
        if trusted_approvals is not None:
            trusted_path = self.root / "trusted-approvals.json"
            trusted_path.write_text(json.dumps(trusted_approvals), encoding="utf-8")
            argv += ["--trusted-approvals-json", str(trusted_path)]
        args = gate.build_parser().parse_args(argv)
        env = dict(os.environ)
        env.pop("GITHUB_ACTIONS", None)
        env.pop("CI_BASE_SHA", None)
        if env_override:
            env.update(env_override)
        with patch.dict(os.environ, env, clear=True):
            return gate.run_gate(args)


class AdvisoryGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.h = FixtureHarness(self.tmp.name)

    def test_valid_unexpired_exception_passes_and_emits_structured_report(self) -> None:
        self.h.write_valid_pair()
        self.h.commit("base")
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])

        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_OK, report["errors"])
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["advisory_db"]["revision"], self.h.db_rev)
        self.assertEqual(report["advisory_db"]["freshness_source"], "live_fetch")
        self.assertFalse(report["advisory_db"]["used_cached_db"])
        self.assertEqual(report["tool"]["actual_version"], gate.EXPECTED_CARGO_DENY_VERSION)
        self.assertEqual(len(report["advisory_hits"]), 1)
        self.assertEqual(report["advisory_hits"][0]["disposition"], "exempted")
        self.assertIn("rsa 0.9.10 <- rustfs-iam 1.0.5 <- rustfs 1.0.5", report["advisory_hits"][0]["chains"])
        self.assertEqual(report["exceptions"][0]["status"], "valid")
        self.assertEqual(report["exceptions"][0]["matched_versions"], ["0.9.10"])
        self.assertTrue(self.h.out_json.is_file())
        self.assertTrue(self.h.raw_log.is_file())

    def test_clean_tree_with_zero_advisories_and_zero_exceptions_passes(self) -> None:
        self.h.write(
            "deny.toml",
            '[graph]\nall-features = true\nno-default-features = false\n[advisories]\nversion = 2\nyanked = "deny"\nignore = []\n',
        )
        self.h.write(".config/advisory-exceptions.toml", "schema_version = 1\nexceptions = []\n")
        self.h.set_mock(hits=[])
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_OK, report["errors"])
        self.assertEqual(report["advisory_hits"], [])
        self.assertEqual(report["exceptions"], [])

    def test_unexempted_live_advisory_fails_with_advisory_exit_code(self) -> None:
        self.h.write(
            "deny.toml",
            '[graph]\nall-features = true\nno-default-features = false\n[advisories]\nversion = 2\nyanked = "deny"\nignore = []\n',
        )
        self.h.write(".config/advisory-exceptions.toml", "schema_version = 1\nexceptions = []\n")
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2026-9999", "foo", "1.2.3")])

        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_ADVISORY_FOUND)
        self.assertIn("unexempted_advisory", report["reason_codes"])
        self.assertEqual(report["advisory_hits"][0]["disposition"], "unexempted")

    def test_exact_boundary_and_past_expiry_fail_with_expiry_exit_code(self) -> None:
        self.h.write_valid_pair(approved_at="2026-10-01T00:00:00Z", expires_at="2026-11-26T00:00:00Z")
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])

        # Exact boundary: now == expires_at MUST fail.
        report_exact, code_exact = self.h.run(now="2026-11-26T00:00:00Z")
        self.assertEqual(code_exact, gate.EXIT_EXCEPTION_EXPIRED)
        self.assertIn("exception_expired", report_exact["reason_codes"])
        self.assertEqual(report_exact["exceptions"][0]["status"], "expired")

        # One second before expiry passes.
        report_before, code_before = self.h.run(now="2026-11-25T23:59:59Z")
        self.assertEqual(code_before, gate.EXIT_OK, report_before["errors"])

    def test_untracked_deny_ignore_and_bare_string_ignore_fail_closed(self) -> None:
        self.h.write(
            "deny.toml",
            '[graph]\nall-features = true\nno-default-features = false\n[advisories]\nversion = 2\nyanked = "deny"\nignore = ["RUSTSEC-2023-0071"]\n',
        )
        self.h.write(".config/advisory-exceptions.toml", "schema_version = 1\nexceptions = []\n")
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])

        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("untracked_deny_ignore", report["reason_codes"])
        self.assertIn("bare_deny_ignore", report["reason_codes"])

    def test_schema_validation_rejects_unknown_fields_duplicates_bad_dates_and_over_90d(self) -> None:
        self.h.write_valid_pair()
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])

        # Unknown schema version
        self.h.write(
            ".config/advisory-exceptions.toml",
            (self.h.repo / ".config/advisory-exceptions.toml").read_text().replace("schema_version = 1", "schema_version = 2"),
        )
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("invalid_exception_schema", report["reason_codes"])

        # Unknown field
        self.h.write_valid_pair()
        self.h.write(
            ".config/advisory-exceptions.toml",
            (self.h.repo / ".config/advisory-exceptions.toml").read_text() + 'extra_field = "nope"\n',
        )
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("invalid_exception_schema", report["reason_codes"])

        # Duplicate advisory ID
        self.h.write_valid_pair()
        text = (self.h.repo / ".config/advisory-exceptions.toml").read_text()
        entry_block = text.split("[[exceptions]]", 1)[1]
        self.h.write(".config/advisory-exceptions.toml", text + "\n[[exceptions]]" + entry_block)
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("duplicate_exception", report["reason_codes"])

        # Validity window > 90 days (91 days)
        self.h.write_valid_pair(approved_at="2026-10-01T00:00:00Z", expires_at="2026-12-31T00:00:00Z")
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("validity_window_exceeded", report["reason_codes"])

        # Non-UTC timestamp
        self.h.write_valid_pair(approved_at="2026-10-01T00:00:00+08:00", expires_at="2026-12-01T00:00:00Z")
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("invalid_timestamp", report["reason_codes"])

        # Missing owner / placeholder owner / wildcard version
        self.h.write_valid_pair(affected_versions="*")
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("invalid_version_range", report["reason_codes"])

        # Malformed TOML
        self.h.write(".config/advisory-exceptions.toml", "schema_version = [unterminated\n")
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("invalid_exception_schema", report["reason_codes"])

    def test_scope_mismatch_and_stale_unused_exception_fail_closed(self) -> None:
        # Locked version 0.9.10 outside =0.9.9
        self.h.write_valid_pair(affected_versions="=0.9.9")
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("exception_scope_mismatch", report["reason_codes"])

        # Stale unused exception when crate no longer hits advisory
        self.h.write_valid_pair()
        self.h.set_mock(hits=[])
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("stale_unused_exception", report["reason_codes"])

    def test_weakening_deny_graph_or_yanked_fails_closed(self) -> None:
        self.h.write_valid_pair()
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])
        deny_text = (self.h.repo / "deny.toml").read_text()
        for bad in (
            deny_text.replace("all-features = true", "all-features = false"),
            deny_text.replace("no-default-features = false", "no-default-features = true"),
            deny_text.replace('yanked = "deny"', 'yanked = "warn"'),
            deny_text + 'vulnerability = "allow"\n',
        ):
            self.h.write("deny.toml", bad)
            report, code = self.h.run()
            self.assertEqual(code, gate.EXIT_POLICY_ERROR)
            self.assertIn("config_weakened", report["reason_codes"])

    def test_pr_widening_or_self_written_approval_reference_is_rejected(self) -> None:
        self.h.write_valid_pair(
            affected_versions="=0.9.10",
            approved_at="2026-10-01T00:00:00Z",
            expires_at="2026-12-15T00:00:00Z",
        )
        base = self.h.commit("base with narrow exception")
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])

        # Unchanged exception against base passes without needing new approval.
        report, code = self.h.run(base_ref=base, trusted_approvals=[])
        self.assertEqual(code, gate.EXIT_OK, report["errors"])

        # Widening affected_versions without trusted approval fails.
        self.h.write_valid_pair(
            affected_versions=">=0.9.0, <0.10.0",
            approved_at="2026-10-01T00:00:00Z",
            expires_at="2026-12-15T00:00:00Z",
            approval_ref="https://github.com/rustfs/rustfs/pull/99999",
        )
        report, code = self.h.run(base_ref=base, trusted_approvals=[], pr_author="contributor")
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("widened_exception_scope", report["reason_codes"])

        # Self-approval by PR author is rejected even if present in approval records.
        self_approval = [{
            "approval_reference": "https://github.com/rustfs/rustfs/pull/99999",
            "advisory_id": "RUSTSEC-2023-0071",
            "crate": "rsa",
            "affected_versions": ">=0.9.0, <0.10.0",
            "expires_at": "2026-12-15T00:00:00Z",
            "approver": "contributor",
            "approver_association": "MEMBER",
            "state": "APPROVED",
        }]
        report, code = self.h.run(base_ref=base, trusted_approvals=self_approval, pr_author="contributor")
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("widened_exception_scope", report["reason_codes"])

        # Independent maintainer approval covering the widened scope passes.
        maintainer_approval = [{
            **self_approval[0],
            "approver": "overtrue",
        }]
        report, code = self.h.run(base_ref=base, trusted_approvals=maintainer_approval, pr_author="contributor")
        self.assertEqual(code, gate.EXIT_OK, report["errors"])

    def test_advisory_db_freshness_cache_fallback_and_stale_rejection(self) -> None:
        self.h.write_valid_pair()
        self.h.set_mock(fetch_exit=1, fetch_stderr="network unreachable", hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])
        now = datetime(2026, 11, 26, 0, 0, 0, tzinfo=timezone.utc)

        # No FETCH_HEAD -> arbitrary checkout rejected even when present.
        report, code = self.h.run(now="2026-11-26T00:00:00Z")
        self.assertEqual(code, gate.EXIT_INFRA_ERROR)
        self.assertIn("stale_or_untrusted_advisory_db", report["reason_codes"])

        # Fresh 2-hour cache passes and marks used_cached_db=True.
        self.h.set_fetch_head_age(now, age_seconds=2 * 3600)
        report, code = self.h.run(now="2026-11-26T00:00:00Z")
        self.assertEqual(code, gate.EXIT_OK, report["errors"])
        self.assertTrue(report["advisory_db"]["used_cached_db"])
        self.assertEqual(report["advisory_db"]["freshness_source"], "cached_fallback")
        self.assertEqual(report["advisory_db"]["cache_age_seconds"], 2 * 3600)

        # Stale 25-hour cache fails closed.
        self.h.set_fetch_head_age(now, age_seconds=25 * 3600)
        report, code = self.h.run(now="2026-11-26T00:00:00Z")
        self.assertEqual(code, gate.EXIT_INFRA_ERROR)
        self.assertIn("stale_or_untrusted_advisory_db", report["reason_codes"])

    def test_tool_version_mismatch_crash_and_ci_clock_override_fail_closed(self) -> None:
        self.h.write_valid_pair()
        self.h.set_mock(version="0.18.0", hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_INFRA_ERROR)
        self.assertIn("tool_version_mismatch", report["reason_codes"])

        self.h.set_mock(crash_check=True)
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_INFRA_ERROR)
        self.assertIn("cargo_deny_failed", report["reason_codes"])

        self.h.set_mock(malformed_output=True)
        report, code = self.h.run()
        self.assertEqual(code, gate.EXIT_INFRA_ERROR)
        self.assertIn("scan_output_unparseable", report["reason_codes"])

        # Clock override via --now is rejected under GITHUB_ACTIONS=true.
        self.h.set_mock(hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")])
        report, code = self.h.run(now="2026-11-26T00:00:00Z", env_override={"GITHUB_ACTIONS": "true"})
        self.assertEqual(code, gate.EXIT_POLICY_ERROR)
        self.assertIn("untrusted_clock_override", report["reason_codes"])

    def test_github_approval_reference_verification_rejects_recycled_or_untrusted_records(self) -> None:
        exc = {
            "advisory_id": "RUSTSEC-2023-0071",
            "crate": "rsa",
            "affected_versions": "=0.9.10",
            "expires_at": "2026-12-15T00:00:00Z",
            "approval_reference": "https://github.com/rustfs/rustfs/pull/9999#pullrequestreview-12345",
        }
        good_body = "Approved RUSTSEC-2023-0071 for rsa =0.9.10 until 2026-12-15."

        def fake_gh(payload: dict, returncode: int = 0):
            return subprocess.CompletedProcess(["gh"], returncode, stdout=json.dumps(payload), stderr="")

        with patch.dict(os.environ, {"GH_TOKEN": "tok"}, clear=False):
            # Trusted maintainer APPROVED review passes.
            with patch.object(
                gate.subprocess,
                "run",
                return_value=fake_gh({"state": "APPROVED", "author_association": "MEMBER", "user": {"login": "overtrue"}, "body": good_body}),
            ):
                ok, _ = gate.verify_github_approval(exc, "contributor")
                self.assertTrue(ok)

            # Self-approval by PR author is rejected.
            with patch.object(
                gate.subprocess,
                "run",
                return_value=fake_gh({"state": "APPROVED", "author_association": "MEMBER", "user": {"login": "contributor"}, "body": good_body}),
            ):
                ok, _ = gate.verify_github_approval(exc, "contributor")
                self.assertFalse(ok)

            # Non-maintainer association is rejected.
            with patch.object(
                gate.subprocess,
                "run",
                return_value=fake_gh({"state": "APPROVED", "author_association": "CONTRIBUTOR", "user": {"login": "someone"}, "body": good_body}),
            ):
                ok, _ = gate.verify_github_approval(exc, "contributor")
                self.assertFalse(ok)

            # Unrelated merged PR recycled without naming advisory/scope/expiry is rejected.
            recycled_exc = {**exc, "approval_reference": "https://github.com/rustfs/rustfs/pull/1"}
            with patch.object(
                gate.subprocess,
                "run",
                return_value=fake_gh({"merged": True, "merged_by": {"login": "overtrue"}, "user": {"login": "other"}, "body": "unrelated PR"}),
            ):
                ok, _ = gate.verify_github_approval(recycled_exc, "contributor")
                self.assertFalse(ok)

            # Bare issue URL without maintainer comment fragment is rejected.
            issue_exc = {**exc, "approval_reference": "https://github.com/rustfs/rustfs/issues/2668"}
            ok, _ = gate.verify_github_approval(issue_exc, "contributor")
            self.assertFalse(ok)

            # Maintainer issue comment with matching issue_url naming advisory/scope/expiry passes.
            comment_exc = {**exc, "approval_reference": "https://github.com/rustfs/rustfs/issues/2668#issuecomment-555"}
            with patch.object(
                gate.subprocess,
                "run",
                return_value=fake_gh({
                    "issue_url": "https://api.github.com/repos/rustfs/rustfs/issues/2668",
                    "author_association": "OWNER",
                    "user": {"login": "overtrue"},
                    "body": good_body,
                }),
            ):
                ok, _ = gate.verify_github_approval(comment_exc, "contributor")
                self.assertTrue(ok)

            # Comment belonging to a different issue/PR number is rejected.
            with patch.object(
                gate.subprocess,
                "run",
                return_value=fake_gh({
                    "issue_url": "https://api.github.com/repos/rustfs/rustfs/issues/9999",
                    "author_association": "OWNER",
                    "user": {"login": "overtrue"},
                    "body": good_body,
                }),
            ):
                ok, _ = gate.verify_github_approval(comment_exc, "contributor")
                self.assertFalse(ok)

            # Malformed or non-dict JSON from gh api fails closed without raising.
            for raw_stdout in ("not-json", "[]", "null"):
                with patch.object(
                    gate.subprocess,
                    "run",
                    return_value=subprocess.CompletedProcess(["gh"], 0, stdout=raw_stdout, stderr=""),
                ):
                    ok, _ = gate.verify_github_approval(exc, "contributor")
                    self.assertFalse(ok)
                    ok, _ = gate.verify_github_approval(comment_exc, "contributor")
                    self.assertFalse(ok)
                    ok, _ = gate.verify_github_approval(recycled_exc, "contributor")
                    self.assertFalse(ok)

    def test_strip_advisory_ignores_toml_preserves_table_lists_such_as_graph_targets(self) -> None:
        import tomllib

        sample = """[graph]
all-features = true
no-default-features = false
targets = [
    { triple = "x86_64-unknown-linux-gnu" },
    { triple = "aarch64-unknown-linux-musl", features = ["fips"] },
]

[advisories]
version = 2
yanked = "deny"
ignore = [
    { id = "RUSTSEC-2023-0071", reason = "tracked" },
]
"""
        stripped = gate.strip_advisory_ignores_toml(sample)
        parsed = tomllib.loads(stripped)
        self.assertEqual(parsed["graph"]["targets"], [
            {"triple": "x86_64-unknown-linux-gnu"},
            {"triple": "aarch64-unknown-linux-musl", "features": ["fips"]},
        ])
        self.assertEqual(parsed["advisories"]["ignore"], [])
        self.assertEqual(parsed["advisories"]["yanked"], "deny")

    def test_secret_redaction_strips_tokens_from_raw_log(self) -> None:
        self.h.write_valid_pair()
        self.h.set_mock(
            fetch_exit=1,
            fetch_stderr="Authorization: Bearer ghp_1234567890abcdefghij1234567890 failed AKIAIOSFODNN7EXAMPLE",
            hits=[make_diagnostic("RUSTSEC-2023-0071", "rsa", "0.9.10")],
        )
        now = datetime(2026, 11, 26, 0, 0, 0, tzinfo=timezone.utc)
        self.h.set_fetch_head_age(now, age_seconds=60)
        report, code = self.h.run(now="2026-11-26T00:00:00Z")
        self.assertEqual(code, gate.EXIT_OK, report["errors"])
        log_text = self.h.raw_log.read_text(encoding="utf-8")
        self.assertNotIn("ghp_1234567890abcdefghij1234567890", log_text)
        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", log_text)
        self.assertIn("<REDACTED>", log_text)
        self.assertNotIn("ghp_1234567890abcdefghij1234567890", report["advisory_db"]["fetch_error"])


if __name__ == "__main__":
    unittest.main()
