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
"""Fail-closed RustSec advisories and time-bounded exception gate (rustfs/backlog#2668)."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DENY_CONFIG = "deny.toml"
DEFAULT_EXCEPTIONS_CONFIG = ".config/advisory-exceptions.toml"
DEFAULT_CARGO_LOCK = "Cargo.lock"
DEFAULT_CARGO_MANIFEST = "Cargo.toml"
DEFAULT_ADVISORY_DB_URL = "https://github.com/RustSec/advisory-db"

EXPECTED_CARGO_DENY_VERSION = "0.20.2"
SCHEMA_VERSION = 1
MAX_VALIDITY_DAYS = 90
MAX_CACHE_AGE_SECONDS = 24 * 60 * 60
MAX_FUTURE_CLOCK_SKEW_SECONDS = 60
MAX_CHAINS_PER_ADVISORY = 32

EXIT_OK = 0
EXIT_ADVISORY_FOUND = 10
EXIT_POLICY_ERROR = 11
EXIT_EXCEPTION_EXPIRED = 12
EXIT_INFRA_ERROR = 13

REQUIRED_EXCEPTION_FIELDS = frozenset({
    "advisory_id",
    "crate",
    "affected_versions",
    "owner",
    "tracking_issue",
    "reason",
    "mitigation",
    "approved_at",
    "expires_at",
    "approval_reference",
})

ADVISORY_ID_RE = re.compile(
    r"^(?:RUSTSEC-\d{4}-\d{4}|GHSA-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}|CVE-\d{4}-\d{4,})$"
)
CRATE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")
OWNER_RE = re.compile(r"^@?[A-Za-z0-9](?:[A-Za-z0-9_/-]{0,62}[A-Za-z0-9])?$")
PLACEHOLDER_VALUES = frozenset({"tbd", "todo", "none", "n/a", "na", "unknown", "pending"})
TRACKING_ISSUE_RE = re.compile(r"^https://github\.com/rustfs/[A-Za-z0-9_.-]+/issues/[1-9][0-9]*$")
APPROVAL_REF_RE = re.compile(
    r"^https://github\.com/rustfs/(?P<repo>[A-Za-z0-9_.-]+)/(?P<kind>pull|issues)/(?P<number>[1-9][0-9]*)"
    r"(?:(?P<fragment>#(?:pullrequestreview|issuecomment)-[1-9][0-9]*))?$"
)
UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
SEMVER_RE = re.compile(
    r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
    r"(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$"
)
CLAUSE_RE = re.compile(r"^(<=|>=|=|<|>)\s*(\S+)$")
FORBIDDEN_BLANKET_LEVELS = ("vulnerability", "unmaintained", "unsound", "notice")
TRUSTED_APPROVER_ASSOCIATIONS = frozenset({"OWNER", "MEMBER"})

SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization:\s*(?:bearer|token|basic)\s+)\S+"),
    re.compile(r"(?i)(x-access-token:)[^\s@]+"),
    re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
)


def sanitize_text(text: str) -> str:
    cleaned = text
    for pattern in SECRET_PATTERNS:
        cleaned = pattern.sub(lambda m: (m.group(1) if m.lastindex else "") + "<REDACTED>", cleaned)
    return cleaned


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_utc(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not UTC_TIMESTAMP_RE.fullmatch(value):
        raise ValueError(f"{field} must be a strict UTC RFC 3339 timestamp (YYYY-MM-DDTHH:MM:SSZ)")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid calendar UTC timestamp: {value!r}") from exc


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, order=True)
class SemVer:
    major: int
    minor: int
    patch: int
    prerelease: tuple[tuple[int, int | str], ...] = ()

    @classmethod
    def parse(cls, raw: str) -> "SemVer":
        match = SEMVER_RE.fullmatch(raw.strip())
        if not match:
            raise ValueError(f"invalid SemVer version: {raw!r}")
        major, minor, patch = int(match.group(1)), int(match.group(2)), int(match.group(3))
        pre_raw = match.group(4)
        if pre_raw is None:
            # Release versions sort after any pre-release of the same core triple.
            pre: tuple[tuple[int, int | str], ...] = ((2, ""),)
        else:
            parts = []
            for ident in pre_raw.split("."):
                if not ident:
                    raise ValueError(f"invalid SemVer prerelease identifier in {raw!r}")
                if ident.isdigit():
                    if len(ident) > 1 and ident.startswith("0"):
                        raise ValueError(f"leading zero in numeric SemVer prerelease in {raw!r}")
                    parts.append((0, int(ident)))
                else:
                    parts.append((1, ident))
            pre = tuple(parts)
        return cls(major=major, minor=minor, patch=patch, prerelease=pre)


@dataclass(frozen=True)
class VersionInterval:
    lower: SemVer | None
    lower_inclusive: bool
    upper: SemVer
    upper_inclusive: bool
    raw: str

    @classmethod
    def parse(cls, raw: Any) -> "VersionInterval":
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError("affected_versions must be a non-empty version constraint string")
        if "*" in raw:
            raise ValueError("affected_versions must not contain wildcards ('*')")
        clauses = [part.strip() for part in raw.split(",")]
        if not clauses or any(not clause for clause in clauses):
            raise ValueError(f"malformed affected_versions constraint: {raw!r}")
        lower: SemVer | None = None
        lower_inc = True
        upper: SemVer | None = None
        upper_inc = False
        for clause in clauses:
            match = CLAUSE_RE.fullmatch(clause)
            if not match:
                raise ValueError(
                    f"affected_versions clause {clause!r} must use explicit operator (=, <, <=, >, >=) and exact SemVer"
                )
            op, ver_str = match.group(1), match.group(2)
            ver = SemVer.parse(ver_str)
            if op == "=":
                if lower is None or ver > lower or (ver == lower and not lower_inc):
                    lower, lower_inc = ver, True
                if upper is None or ver < upper or (ver == upper and not upper_inc):
                    upper, upper_inc = ver, True
            elif op == ">=":
                if lower is None or ver > lower:
                    lower, lower_inc = ver, True
            elif op == ">":
                if lower is None or ver > lower or (ver == lower and lower_inc):
                    lower, lower_inc = ver, False
            elif op == "<=":
                if upper is None or ver < upper:
                    upper, upper_inc = ver, True
            elif op == "<":
                if upper is None or ver < upper or (ver == upper and upper_inc):
                    upper, upper_inc = ver, False
        if upper is None:
            raise ValueError(f"affected_versions must specify a finite upper bound (=, <, or <=): {raw!r}")
        if lower is not None:
            if lower > upper or (lower == upper and not (lower_inc and upper_inc)):
                raise ValueError(f"affected_versions defines an empty version range: {raw!r}")
        return cls(lower=lower, lower_inclusive=lower_inc, upper=upper, upper_inclusive=upper_inc, raw=raw.strip())

    def contains(self, version_str: str) -> bool:
        ver = SemVer.parse(version_str)
        if self.lower is not None:
            if ver < self.lower or (ver == self.lower and not self.lower_inclusive):
                return False
        if ver > self.upper or (ver == self.upper and not self.upper_inclusive):
            return False
        return True

    def is_subset_of(self, other: "VersionInterval") -> bool:
        if other.lower is not None:
            if self.lower is None:
                return False
            if self.lower < other.lower:
                return False
            if self.lower == other.lower and self.lower_inclusive and not other.lower_inclusive:
                return False
        if self.upper > other.upper:
            return False
        if self.upper == other.upper and self.upper_inclusive and not other.upper_inclusive:
            return False
        return True


def parse_deny_config(text: str) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, dict[str, Any]]]:
    try:
        doc = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"invalid deny.toml TOML: {exc}") from exc
    if not isinstance(doc, dict):
        raise ValueError("deny.toml root must be a table")
    errors: list[dict[str, Any]] = []
    graph = doc.get("graph")
    if not isinstance(graph, dict):
        errors.append({"code": "config_weakened", "message": "deny.toml must define [graph] table"})
        graph = {}
    if graph.get("all-features") is not True:
        errors.append({"code": "config_weakened", "message": "deny.toml [graph].all-features must remain true"})
    if graph.get("no-default-features") is not False:
        errors.append({"code": "config_weakened", "message": "deny.toml [graph].no-default-features must remain false"})

    advisories = doc.get("advisories")
    if not isinstance(advisories, dict):
        errors.append({"code": "config_weakened", "message": "deny.toml must define [advisories] table"})
        advisories = {}
    if advisories.get("version") != 2:
        errors.append({"code": "config_weakened", "message": "deny.toml [advisories].version must be 2"})
    if advisories.get("yanked") != "deny":
        errors.append({"code": "config_weakened", "message": "deny.toml [advisories].yanked must remain 'deny'"})
    for key in FORBIDDEN_BLANKET_LEVELS:
        if key in advisories and advisories[key] != "deny":
            errors.append({
                "code": "config_weakened",
                "message": f"deny.toml [advisories].{key} cannot be weakened to {advisories[key]!r}",
            })

    raw_ignores = advisories.get("ignore", [])
    ignores: dict[str, dict[str, Any]] = {}
    if not isinstance(raw_ignores, list):
        errors.append({"code": "config_invalid", "message": "deny.toml [advisories].ignore must be a list"})
        raw_ignores = []
    for index, item in enumerate(raw_ignores):
        if isinstance(item, str):
            errors.append({
                "code": "bare_deny_ignore",
                "message": f"deny.toml [advisories].ignore[{index}] uses bare string {item!r}; structured {{ id, reason }} entry required",
                "advisory_id": item,
            })
            if item in ignores:
                errors.append({
                    "code": "duplicate_deny_ignore",
                    "message": f"duplicate advisory ignore {item!r} in deny.toml",
                    "advisory_id": item,
                })
            else:
                ignores[item] = {"id": item, "reason": ""}
            continue
        if not isinstance(item, dict):
            errors.append({
                "code": "config_invalid",
                "message": f"deny.toml [advisories].ignore[{index}] must be a table",
            })
            continue
        adv_id = item.get("id")
        reason = item.get("reason")
        if not isinstance(adv_id, str) or not ADVISORY_ID_RE.fullmatch(adv_id):
            errors.append({
                "code": "config_invalid",
                "message": f"deny.toml [advisories].ignore[{index}] has invalid advisory id: {adv_id!r}",
            })
            continue
        if not isinstance(reason, str) or not reason.strip():
            errors.append({
                "code": "bare_deny_ignore",
                "message": f"deny.toml [advisories].ignore entry for {adv_id} must include a non-empty reason",
                "advisory_id": adv_id,
            })
        if adv_id in ignores:
            errors.append({
                "code": "duplicate_deny_ignore",
                "message": f"duplicate advisory ignore {adv_id} in deny.toml",
                "advisory_id": adv_id,
            })
            continue
        ignores[adv_id] = dict(item)
    return doc, errors, ignores


def format_toml_key(key: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]+", key):
        return key
    return json.dumps(key)


def format_toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(format_toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        items = ", ".join(f"{format_toml_key(k)} = {format_toml_value(v)}" for k, v in value.items())
        return "{ " + items + " }"
    raise ValueError(f"unsupported TOML value type in deny.toml: {type(value).__name__}")


def strip_advisory_ignores_toml(text: str) -> str:
    """Create a temporary deny.toml copy with [advisories].ignore emptied, preserving [graph] and [advisories]."""
    doc = tomllib.loads(text)
    lines: list[str] = []
    graph = doc.get("graph", {})
    lines.append("[graph]")
    for key, val in graph.items():
        lines.append(f"{format_toml_key(key)} = {format_toml_value(val)}")
    advisories = doc.get("advisories", {})
    lines.append("")
    lines.append("[advisories]")
    for key, val in advisories.items():
        if key == "ignore":
            continue
        lines.append(f"{format_toml_key(key)} = {format_toml_value(val)}")
    lines.append("ignore = []")
    return "\n".join(lines) + "\n"


def parse_exceptions_registry(text: str) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    try:
        doc = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        return {}, [{"code": "invalid_exception_schema", "message": f"invalid advisory-exceptions.toml TOML: {exc}"}]
    if not isinstance(doc, dict):
        return {}, [{"code": "invalid_exception_schema", "message": "advisory-exceptions.toml root must be a table"}]
    errors: list[dict[str, Any]] = []
    unknown_top = sorted(set(doc.keys()) - {"schema_version", "exceptions"})
    if unknown_top:
        errors.append({
            "code": "invalid_exception_schema",
            "message": f"unknown top-level keys in advisory-exceptions.toml: {unknown_top}",
        })
    if type(doc.get("schema_version")) is not int or doc.get("schema_version") != SCHEMA_VERSION:
        errors.append({
            "code": "invalid_exception_schema",
            "message": f"advisory-exceptions.toml schema_version must be integer {SCHEMA_VERSION}",
        })
    raw_entries = doc.get("exceptions")
    if raw_entries is None:
        raw_entries = []
    if not isinstance(raw_entries, list):
        errors.append({
            "code": "invalid_exception_schema",
            "message": "advisory-exceptions.toml `exceptions` must be a list of tables",
        })
        return {}, errors

    entries: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(raw_entries):
        if not isinstance(raw, dict):
            errors.append({
                "code": "invalid_exception_schema",
                "message": f"exceptions[{index}] must be a TOML table",
            })
            continue
        keys = set(raw.keys())
        missing = sorted(REQUIRED_EXCEPTION_FIELDS - keys)
        unknown = sorted(keys - REQUIRED_EXCEPTION_FIELDS)
        adv_id = raw.get("advisory_id") if isinstance(raw.get("advisory_id"), str) else f"index:{index}"
        if missing:
            errors.append({
                "code": "invalid_exception_schema",
                "advisory_id": adv_id,
                "message": f"exception {adv_id} missing required fields: {missing}",
            })
        if unknown:
            errors.append({
                "code": "invalid_exception_schema",
                "advisory_id": adv_id,
                "message": f"exception {adv_id} has unknown fields: {unknown}",
            })

        entry_errors: list[dict[str, Any]] = []
        if not isinstance(raw.get("advisory_id"), str) or not ADVISORY_ID_RE.fullmatch(raw["advisory_id"]):
            entry_errors.append({
                "code": "invalid_exception_field",
                "advisory_id": adv_id,
                "message": f"invalid advisory_id: {raw.get('advisory_id')!r}",
            })
        crate = raw.get("crate")
        if not isinstance(crate, str) or not CRATE_NAME_RE.fullmatch(crate):
            entry_errors.append({
                "code": "invalid_exception_field",
                "advisory_id": adv_id,
                "message": f"invalid crate name: {crate!r}",
            })
        interval: VersionInterval | None = None
        try:
            interval = VersionInterval.parse(raw.get("affected_versions"))
        except ValueError as exc:
            entry_errors.append({
                "code": "invalid_version_range",
                "advisory_id": adv_id,
                "message": str(exc),
            })
        owner = raw.get("owner")
        if not isinstance(owner, str) or not OWNER_RE.fullmatch(owner.strip()) or owner.strip().lower() in PLACEHOLDER_VALUES:
            entry_errors.append({
                "code": "invalid_exception_field",
                "advisory_id": adv_id,
                "message": f"invalid owner: {owner!r}",
            })
        tracking_issue = raw.get("tracking_issue")
        if not isinstance(tracking_issue, str) or not TRACKING_ISSUE_RE.fullmatch(tracking_issue.strip()):
            entry_errors.append({
                "code": "invalid_exception_field",
                "advisory_id": adv_id,
                "message": f"tracking_issue must be a rustfs GitHub issue URL: {tracking_issue!r}",
            })
        for text_field in ("reason", "mitigation"):
            text_val = raw.get(text_field)
            if (
                not isinstance(text_val, str)
                or len(text_val.strip()) < 16
                or text_val.strip().lower() in PLACEHOLDER_VALUES
            ):
                entry_errors.append({
                    "code": "invalid_exception_field",
                    "advisory_id": adv_id,
                    "message": f"{text_field} must be a non-placeholder explanation of at least 16 characters",
                })
        approval_ref = raw.get("approval_reference")
        if not isinstance(approval_ref, str) or not APPROVAL_REF_RE.fullmatch(approval_ref.strip()):
            entry_errors.append({
                "code": "invalid_exception_field",
                "advisory_id": adv_id,
                "message": f"approval_reference must be a rustfs GitHub PR or review/comment URL: {approval_ref!r}",
            })

        approved_dt: datetime | None = None
        expires_dt: datetime | None = None
        try:
            approved_dt = parse_utc(raw.get("approved_at"), "approved_at")
        except ValueError as exc:
            entry_errors.append({"code": "invalid_timestamp", "advisory_id": adv_id, "message": str(exc)})
        try:
            expires_dt = parse_utc(raw.get("expires_at"), "expires_at")
        except ValueError as exc:
            entry_errors.append({"code": "invalid_timestamp", "advisory_id": adv_id, "message": str(exc)})
        if approved_dt is not None and expires_dt is not None:
            if expires_dt <= approved_dt:
                entry_errors.append({
                    "code": "invalid_validity_window",
                    "advisory_id": adv_id,
                    "message": f"expires_at ({raw['expires_at']}) must be after approved_at ({raw['approved_at']})",
                })
            elif expires_dt - approved_dt > timedelta(days=MAX_VALIDITY_DAYS):
                entry_errors.append({
                    "code": "validity_window_exceeded",
                    "advisory_id": adv_id,
                    "message": (
                        f"exception {adv_id} validity window exceeds {MAX_VALIDITY_DAYS} days "
                        f"({raw['approved_at']} -> {raw['expires_at']})"
                    ),
                })

        errors.extend(entry_errors)
        if isinstance(raw.get("advisory_id"), str) and ADVISORY_ID_RE.fullmatch(raw["advisory_id"]):
            key = raw["advisory_id"]
            if key in entries:
                errors.append({
                    "code": "duplicate_exception",
                    "advisory_id": key,
                    "message": f"duplicate exception entry for {key} in advisory-exceptions.toml",
                })
            else:
                entries[key] = {
                    **raw,
                    "_interval": interval,
                    "_approved_dt": approved_dt,
                    "_expires_dt": expires_dt,
                    "_errors": list(entry_errors),
                }
    return entries, errors


def run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def git_head_sha(repo_root: Path) -> str | None:
    proc = run_git(repo_root, "rev-parse", "HEAD")
    if proc.returncode == 0:
        sha = proc.stdout.strip()
        if SHA40_RE.fullmatch(sha):
            return sha
    return None


def git_show_optional(repo_root: Path, revision: str, rel_path: str) -> tuple[bool, str]:
    proc = run_git(repo_root, "show", f"{revision}:{rel_path}")
    if proc.returncode == 0:
        return True, proc.stdout
    return False, proc.stderr.strip()


def normalize_git_url(url: str) -> str:
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith(".git"):
        cleaned = cleaned[:-4]
    return cleaned.lower()


def find_advisory_db_dir(
    repo_root: Path,
    deny_doc: dict[str, Any],
    explicit_dir: Path | None,
    expected_url: str,
) -> Path | None:
    if explicit_dir is not None:
        return explicit_dir
    advisories = deny_doc.get("advisories", {}) if isinstance(deny_doc, dict) else {}
    db_path = advisories.get("db-path")
    if isinstance(db_path, str) and db_path.strip():
        candidate = Path(db_path).expanduser()
        if not candidate.is_absolute():
            candidate = (repo_root / candidate).resolve()
        return candidate
    cargo_home = Path(os.environ.get("CARGO_HOME") or (Path.home() / ".cargo"))
    dbs_root = cargo_home / "advisory-dbs"
    if not dbs_root.is_dir():
        return None
    expected_norm = normalize_git_url(expected_url)
    matches: list[tuple[float, Path]] = []
    for child in sorted(dbs_root.iterdir()):
        if not child.is_dir() or not (child / ".git").exists():
            continue
        remote = run_git(child, "remote", "get-url", "origin")
        if remote.returncode == 0 and normalize_git_url(remote.stdout) == expected_norm:
            fetch_head = child / ".git" / "FETCH_HEAD"
            mtime = fetch_head.stat().st_mtime if fetch_head.is_file() else child.stat().st_mtime
            matches.append((mtime, child))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0], reverse=True)
    return matches[0][1]


def inspect_advisory_db(
    db_dir: Path | None,
    expected_url: str,
    now: datetime,
    *,
    fresh_fetch: bool,
    fetch_error: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    info: dict[str, Any] = {
        "url": expected_url,
        "path_label": db_dir.name if db_dir is not None else None,
        "revision": None,
        "commit_timestamp": None,
        "fetched_at": None,
        "freshness_source": "live_fetch" if fresh_fetch else "cached_fallback",
        "used_cached_db": not fresh_fetch,
        "cache_age_seconds": None,
        "max_cache_age_seconds": MAX_CACHE_AGE_SECONDS,
    }
    if fetch_error:
        info["fetch_error"] = sanitize_text(fetch_error)
    errors: list[dict[str, Any]] = []
    if db_dir is None or not db_dir.is_dir() or not (db_dir / ".git").exists():
        errors.append({
            "code": "advisory_db_unavailable",
            "message": "advisory database git checkout not found; cannot verify revision or freshness",
        })
        return info, errors
    remote = run_git(db_dir, "remote", "get-url", "origin")
    if remote.returncode != 0 or normalize_git_url(remote.stdout) != normalize_git_url(expected_url):
        errors.append({
            "code": "untrusted_advisory_db",
            "message": (
                f"advisory database remote {remote.stdout.strip()!r} does not match expected {expected_url!r}"
            ),
        })
        return info, errors
    head_proc = run_git(db_dir, "rev-parse", "HEAD")
    rev = head_proc.stdout.strip() if head_proc.returncode == 0 else ""
    if not SHA40_RE.fullmatch(rev):
        errors.append({
            "code": "advisory_db_unavailable",
            "message": "unable to resolve 40-character advisory database revision SHA",
        })
        return info, errors
    info["revision"] = rev
    ts_proc = run_git(db_dir, "show", "-s", "--format=%cI", "HEAD")
    if ts_proc.returncode == 0 and ts_proc.stdout.strip():
        info["commit_timestamp"] = ts_proc.stdout.strip()

    if fresh_fetch:
        info["fetched_at"] = format_utc(now)
        info["cache_age_seconds"] = 0
        return info, errors

    fetch_head = db_dir / ".git" / "FETCH_HEAD"
    if not fetch_head.is_file() or fetch_head.stat().st_size == 0:
        errors.append({
            "code": "stale_or_untrusted_advisory_db",
            "message": "advisory database fetch failed and local cache has no verifiable FETCH_HEAD timestamp",
        })
        return info, errors
    fetched_dt = datetime.fromtimestamp(fetch_head.stat().st_mtime, tz=timezone.utc)
    info["fetched_at"] = format_utc(fetched_dt)
    age_seconds = int((now - fetched_dt).total_seconds())
    info["cache_age_seconds"] = age_seconds
    if age_seconds < -MAX_FUTURE_CLOCK_SKEW_SECONDS:
        errors.append({
            "code": "stale_or_untrusted_advisory_db",
            "message": f"cached advisory database FETCH_HEAD timestamp {info['fetched_at']} is in the future",
        })
    elif age_seconds > MAX_CACHE_AGE_SECONDS:
        errors.append({
            "code": "stale_or_untrusted_advisory_db",
            "message": (
                f"advisory database fetch failed and cached DB is stale ({age_seconds}s > {MAX_CACHE_AGE_SECONDS}s)"
            ),
        })
    return info, errors


def check_cargo_deny_version(cargo_deny_bin: str, expected_version: str) -> tuple[str | None, list[dict[str, Any]]]:
    cmd = [cargo_deny_bin, "--version"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    except OSError as exc:
        return None, [{"code": "tool_unavailable", "message": f"failed to execute {cargo_deny_bin}: {exc}"}]
    if proc.returncode != 0:
        return None, [{
            "code": "tool_unavailable",
            "message": f"{cargo_deny_bin} --version failed (exit {proc.returncode}): {sanitize_text(proc.stderr.strip())}",
        }]
    output = (proc.stdout or proc.stderr).strip()
    match = re.search(r"cargo-deny\s+([0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?)", output)
    if not match:
        return output, [{"code": "tool_version_mismatch", "message": f"unparseable cargo-deny version: {output!r}"}]
    actual = match.group(1)
    if actual != expected_version:
        return actual, [{
            "code": "tool_version_mismatch",
            "message": f"cargo-deny version {actual} does not match pinned version {expected_version}",
        }]
    return actual, []


def extract_chains(graphs: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    versions: set[str] = set()
    chains: list[str] = []

    def walk(node: dict[str, Any], path: list[str]) -> None:
        if len(chains) >= MAX_CHAINS_PER_ADVISORY:
            return
        krate = node.get("Krate")
        if not isinstance(krate, dict):
            return
        name = str(krate.get("name", ""))
        ver = str(krate.get("version", ""))
        kind = krate.get("kind")
        label = f"{name} {ver}" + (f" ({kind})" if kind else "")
        next_path = [*path, label]
        parents = node.get("parents")
        if not parents:
            chains.append(" <- ".join(next_path))
            return
        for parent in parents:
            if isinstance(parent, dict):
                walk(parent, next_path)
                if len(chains) >= MAX_CHAINS_PER_ADVISORY:
                    return

    for root in graphs:
        if not isinstance(root, dict):
            continue
        krate = root.get("Krate")
        if isinstance(krate, dict) and isinstance(krate.get("version"), str):
            versions.add(krate["version"])
        walk(root, [])
    return sorted(versions), chains


def parse_cargo_deny_jsonl(output: str) -> tuple[list[dict[str, Any]], dict[str, Any] | None, list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    summary: dict[str, Any] | None = None
    parse_errors: list[dict[str, Any]] = []
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            parse_errors.append({
                "code": "scan_output_unparseable",
                "message": f"non-JSON line in cargo-deny output: {sanitize_text(line[:180])}",
            })
            continue
        if not isinstance(obj, dict):
            parse_errors.append({"code": "scan_output_unparseable", "message": "non-object JSON record in cargo-deny output"})
            continue
        kind = obj.get("type")
        fields = obj.get("fields", {})
        if kind == "summary" and isinstance(fields, dict):
            summary = fields
        elif kind == "diagnostic" and isinstance(fields, dict):
            diagnostics.append(fields)
    return diagnostics, summary, parse_errors


def hit_from_diagnostic(diag: dict[str, Any]) -> dict[str, Any]:
    adv = diag.get("advisory")
    code = str(diag.get("code", "unknown"))
    graphs = diag.get("graphs") if isinstance(diag.get("graphs"), list) else []
    versions, chains = extract_chains(graphs)
    crate_name = ""
    if isinstance(adv, dict) and isinstance(adv.get("package"), str):
        crate_name = adv["package"]
    if not crate_name or not versions:
        for label in diag.get("labels", []):
            if isinstance(label, dict) and isinstance(label.get("span"), str):
                parts = label["span"].split()
                if len(parts) >= 2:
                    if not crate_name:
                        crate_name = parts[0]
                    if not versions:
                        versions = [parts[1]]
    if isinstance(adv, dict) and isinstance(adv.get("id"), str):
        return {
            "advisory_id": adv["id"],
            "code": code,
            "severity": diag.get("severity", "error"),
            "crate": crate_name,
            "versions": versions,
            "title": adv.get("title") or diag.get("message", ""),
            "url": adv.get("url"),
            "informational": adv.get("informational"),
            "cvss": adv.get("cvss"),
            "aliases": adv.get("aliases", []),
            "chains": chains,
        }
    return {
        "advisory_id": None,
        "code": code,
        "severity": diag.get("severity", "error"),
        "crate": crate_name,
        "versions": versions,
        "title": diag.get("message", ""),
        "url": None,
        "informational": None,
        "cvss": None,
        "aliases": [],
        "chains": chains,
    }


def parse_github_json_object(stdout: str, endpoint: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        data = json.loads(stdout)
    except (ValueError, TypeError):
        return None, f"malformed JSON from GitHub API for {endpoint}"
    if not isinstance(data, dict):
        return None, f"unexpected non-object GitHub API payload for {endpoint}"
    return data, None


def verify_github_approval(
    exc: dict[str, Any],
    pr_author: str | None,
) -> tuple[bool, str]:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        return False, "no GH_TOKEN available to verify approval_reference against GitHub"
    match = APPROVAL_REF_RE.fullmatch(exc["approval_reference"].strip())
    if not match:
        return False, "approval_reference is not a valid rustfs GitHub URL"
    repo = match.group("repo")
    kind = match.group("kind")
    number = match.group("number")
    fragment = match.group("fragment") or ""
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "GH_TOKEN": token,
    }
    if fragment.startswith("#pullrequestreview-"):
        if kind != "pull":
            return False, "pullrequestreview fragment requires a pull request URL"
        review_id = fragment.split("-", 1)[1]
        endpoint = f"repos/rustfs/{repo}/pulls/{number}/reviews/{review_id}"
        proc = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True, env=env, timeout=30, check=False)
        if proc.returncode != 0:
            return False, f"GitHub review lookup failed for {endpoint}"
        data, parse_err = parse_github_json_object(proc.stdout, endpoint)
        if parse_err is not None or data is None:
            return False, parse_err or f"invalid GitHub review payload for {endpoint}"
        user = data.get("user")
        author = user.get("login") if isinstance(user, dict) else None
        assoc = data.get("author_association", "")
        state = data.get("state", "")
        body = data.get("body") or ""
        if state != "APPROVED":
            return False, f"review {review_id} state is {state!r}, not APPROVED"
        if assoc not in TRUSTED_APPROVER_ASSOCIATIONS:
            return False, f"reviewer {author!r} association {assoc!r} is not maintainer/owner"
        if pr_author and author == pr_author:
            return False, f"PR author {pr_author!r} cannot self-approve exception"
        if (
            exc["advisory_id"] not in body
            or exc["expires_at"][:10] not in body
            or exc["affected_versions"] not in body
        ):
            return False, "review body must explicitly reference advisory_id, affected_versions, and expires_at date"
        return True, f"verified APPROVED review {review_id} by {author} ({assoc})"

    if fragment.startswith("#issuecomment-"):
        comment_id = fragment.split("-", 1)[1]
        endpoint = f"repos/rustfs/{repo}/issues/comments/{comment_id}"
        proc = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True, env=env, timeout=30, check=False)
        if proc.returncode != 0:
            return False, f"GitHub comment lookup failed for {endpoint}"
        data, parse_err = parse_github_json_object(proc.stdout, endpoint)
        if parse_err is not None or data is None:
            return False, parse_err or f"invalid GitHub comment payload for {endpoint}"
        issue_url = data.get("issue_url")
        expected_suffix = f"/repos/rustfs/{repo.lower()}/issues/{number}"
        if not isinstance(issue_url, str) or not issue_url.rstrip("/").lower().endswith(expected_suffix):
            return False, f"comment {comment_id} does not belong to rustfs/{repo}#{number}"
        user = data.get("user")
        author = user.get("login") if isinstance(user, dict) else None
        assoc = data.get("author_association", "")
        body = data.get("body") or ""
        if assoc not in TRUSTED_APPROVER_ASSOCIATIONS:
            return False, f"comment author {author!r} association {assoc!r} is not maintainer/owner"
        if pr_author and author == pr_author:
            return False, f"PR author {pr_author!r} cannot self-approve exception"
        if (
            exc["advisory_id"] not in body
            or exc["expires_at"][:10] not in body
            or exc["affected_versions"] not in body
        ):
            return False, "approval comment must explicitly reference advisory_id, affected_versions, and expires_at date"
        return True, f"verified maintainer comment {comment_id} by {author} ({assoc})"

    if kind == "pull":
        endpoint = f"repos/rustfs/{repo}/pulls/{number}"
        proc = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True, env=env, timeout=30, check=False)
        if proc.returncode != 0:
            return False, f"GitHub PR lookup failed for {endpoint}"
        data, parse_err = parse_github_json_object(proc.stdout, endpoint)
        if parse_err is not None or data is None:
            return False, parse_err or f"invalid GitHub PR payload for {endpoint}"
        if not data.get("merged"):
            return False, f"PR #{number} is not merged; open PR URLs require an explicit #pullrequestreview-<id> approval"
        merged_by_obj = data.get("merged_by")
        merged_by = merged_by_obj.get("login") if isinstance(merged_by_obj, dict) else None
        if not merged_by:
            return False, f"merged PR #{number} lacks merged_by maintainer attribution"
        pr_user = data.get("user")
        pr_user_login = pr_user.get("login") if isinstance(pr_user, dict) else None
        if pr_author and merged_by == pr_author and pr_user_login == pr_author:
            return False, f"PR author {pr_author!r} cannot self-approve via self-merged PR"
        body = data.get("body") or ""
        if (
            exc["advisory_id"] not in body
            or exc["expires_at"][:10] not in body
            or exc["affected_versions"] not in body
        ):
            return (
                False,
                f"merged PR #{number} body does not explicitly name advisory_id, affected_versions, and expires_at date; "
                "cite a specific maintainer #pullrequestreview-<id> or #issuecomment-<id>",
            )
        return True, f"verified merged PR #{number} merged by {merged_by}"

    return False, "bare issue URL is not an approval record; cite a maintainer #issuecomment-<id> or #pullrequestreview-<id>"


def verify_trusted_approval(
    exc: dict[str, Any],
    trusted_records: list[dict[str, Any]] | None,
    pr_author: str | None,
) -> tuple[bool, str]:
    if trusted_records is not None:
        head_interval: VersionInterval | None = exc.get("_interval")
        head_expires: datetime | None = exc.get("_expires_dt")
        for record in trusted_records:
            if not isinstance(record, dict):
                continue
            if (
                record.get("approval_reference") != exc.get("approval_reference")
                or record.get("advisory_id") != exc.get("advisory_id")
                or record.get("crate") != exc.get("crate")
            ):
                continue
            if record.get("approver_association") not in TRUSTED_APPROVER_ASSOCIATIONS:
                return False, f"approver association {record.get('approver_association')!r} is not trusted"
            if pr_author and record.get("approver") == pr_author:
                return False, f"PR author {pr_author!r} cannot self-approve exception"
            if record.get("state") not in {"APPROVED", "MERGED"}:
                return False, f"approval state {record.get('state')!r} is not APPROVED/MERGED"
            try:
                rec_interval = VersionInterval.parse(record.get("affected_versions"))
                rec_expires = parse_utc(record.get("expires_at"), "trusted_approval.expires_at")
            except ValueError as exc_err:
                return False, f"invalid trusted approval record: {exc_err}"
            if head_interval is None or not head_interval.is_subset_of(rec_interval):
                return False, "exception affected_versions widens beyond trusted approval record"
            if head_expires is None or head_expires > rec_expires:
                return False, "exception expires_at exceeds trusted approval record"
            return True, f"verified trusted approval by {record.get('approver')}"
        return False, "no matching trusted approval record found"
    return verify_github_approval(exc, pr_author)


def check_base_diff_approvals(
    repo_root: Path,
    base_ref: str,
    head_exceptions: dict[str, dict[str, Any]],
    deny_rel_path: str,
    exceptions_rel_path: str,
    trusted_records: list[dict[str, Any]] | None,
    pr_author: str | None,
) -> tuple[dict[str, list[str]], list[dict[str, Any]]]:
    """Compare head exceptions against base_ref and require trusted approval for new/widened/extended items."""
    per_id_reasons: dict[str, list[str]] = {}
    errors: list[dict[str, Any]] = []

    verify_ref = run_git(repo_root, "rev-parse", "--verify", f"{base_ref}^{{commit}}")
    if verify_ref.returncode != 0:
        errors.append({
            "code": "base_ref_unavailable",
            "message": f"base commit {base_ref!r} is unavailable for PR exception diff verification",
        })
        return per_id_reasons, errors

    has_base_exc, base_exc_text = git_show_optional(repo_root, base_ref, exceptions_rel_path)
    has_base_deny, base_deny_text = git_show_optional(repo_root, base_ref, deny_rel_path)

    base_exceptions: dict[str, dict[str, Any]] = {}
    base_deny_ignores: dict[str, dict[str, Any]] = {}
    if has_base_exc:
        base_exceptions, base_exc_errors = parse_exceptions_registry(base_exc_text)
        if base_exc_errors:
            # Corrupt base cannot authorize head changes; treat all head exceptions as requiring approval.
            base_exceptions = {}
    if has_base_deny:
        try:
            _, _, base_deny_ignores = parse_deny_config(base_deny_text)
        except ValueError:
            base_deny_ignores = {}

    for adv_id, exc in head_exceptions.items():
        changes: list[str] = []
        if has_base_exc:
            base_exc = base_exceptions.get(adv_id)
            if base_exc is None:
                changes.append("added")
            else:
                if exc.get("crate") != base_exc.get("crate"):
                    changes.append("crate_changed")
                head_iv, base_iv = exc.get("_interval"), base_exc.get("_interval")
                if head_iv is not None and base_iv is not None and not head_iv.is_subset_of(base_iv):
                    changes.append("scope_widened")
                head_exp, base_exp = exc.get("_expires_dt"), base_exc.get("_expires_dt")
                if head_exp is not None and base_exp is not None and head_exp > base_exp:
                    changes.append("expiry_extended")
                if exc.get("approved_at") != base_exc.get("approved_at") or exc.get("approval_reference") != base_exc.get("approval_reference"):
                    changes.append("approval_changed")
        else:
            # Initial migration PR: any advisory ignore not already in base deny.toml is a newly added exception.
            if adv_id not in base_deny_ignores:
                changes.append("added")

        if not changes:
            continue

        ok, detail = verify_trusted_approval(exc, trusted_records, pr_author)
        if not ok:
            code = "widened_exception_scope" if "scope_widened" in changes else "unapproved_exception_change"
            per_id_reasons.setdefault(adv_id, []).append(code)
            errors.append({
                "code": code,
                "advisory_id": adv_id,
                "crate": exc.get("crate"),
                "message": f"exception {adv_id} changed ({','.join(changes)}) without trusted approval: {detail}",
            })
    return per_id_reasons, errors


def read_pr_author_from_event() -> str | None:
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    if not event_path:
        return None
    try:
        payload = json.loads(Path(event_path).read_text(encoding="utf-8"))
        return (payload.get("pull_request") or {}).get("user", {}).get("login")
    except (OSError, ValueError, AttributeError):
        return None


def run_gate(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    repo_root = Path(args.repo_root).resolve()
    deny_path = (repo_root / args.deny_config).resolve()
    exceptions_path = (repo_root / args.exceptions_config).resolve()
    lock_path = (repo_root / args.cargo_lock).resolve()
    manifest_path = (repo_root / args.manifest_path).resolve()

    in_github_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    policy_errors: list[dict[str, Any]] = []
    infra_errors: list[dict[str, Any]] = []
    advisory_errors: list[dict[str, Any]] = []
    expiry_errors: list[dict[str, Any]] = []

    if args.now is not None:
        if in_github_actions and not args.allow_ci_clock_override_for_test:
            policy_errors.append({
                "code": "untrusted_clock_override",
                "message": "--now clock override is forbidden under GITHUB_ACTIONS",
            })
            now = datetime.now(timezone.utc)
            clock_source = "rejected_override"
        else:
            try:
                now = parse_utc(args.now, "--now")
                clock_source = "injected_test_clock"
            except ValueError as exc:
                policy_errors.append({"code": "invalid_timestamp", "message": str(exc)})
                now = datetime.now(timezone.utc)
                clock_source = "system_utc_fallback"
    else:
        now = datetime.now(timezone.utc)
        clock_source = "system_utc"

    digests: dict[str, str | None] = {
        "cargo_lock_sha256": None,
        "deny_toml_sha256": None,
        "exceptions_toml_sha256": None,
    }
    for key, path in (
        ("cargo_lock_sha256", lock_path),
        ("deny_toml_sha256", deny_path),
        ("exceptions_toml_sha256", exceptions_path),
    ):
        if not path.is_file():
            infra_errors.append({"code": "missing_input_file", "message": f"required file missing: {path.name}"})
        else:
            digests[key] = sha256_file(path)

    deny_doc: dict[str, Any] = {}
    deny_ignores: dict[str, dict[str, Any]] = {}
    deny_text = ""
    if deny_path.is_file():
        deny_text = deny_path.read_text(encoding="utf-8")
        try:
            deny_doc, deny_cfg_errors, deny_ignores = parse_deny_config(deny_text)
            policy_errors.extend(deny_cfg_errors)
        except ValueError as exc:
            policy_errors.append({"code": "config_invalid", "message": str(exc)})

    exceptions: dict[str, dict[str, Any]] = {}
    if exceptions_path.is_file():
        exc_text = exceptions_path.read_text(encoding="utf-8")
        exceptions, exc_parse_errors = parse_exceptions_registry(exc_text)
        policy_errors.extend(exc_parse_errors)

    # Bidirectional consistency between deny.toml [advisories].ignore and .config/advisory-exceptions.toml.
    deny_ids = set(deny_ignores.keys())
    exc_ids = set(exceptions.keys())
    for adv_id in sorted(deny_ids - exc_ids):
        policy_errors.append({
            "code": "untracked_deny_ignore",
            "advisory_id": adv_id,
            "message": f"advisory {adv_id} is ignored in deny.toml without a matching record in {args.exceptions_config}",
        })
    for adv_id in sorted(exc_ids - deny_ids):
        policy_errors.append({
            "code": "missing_deny_ignore",
            "advisory_id": adv_id,
            "message": f"exception {adv_id} is registered in {args.exceptions_config} but missing from deny.toml [advisories].ignore",
        })

    # Clock-based approval & expiry validation on every registered exception.
    expired_ids: set[str] = set()
    invalid_time_ids: set[str] = set()
    for adv_id, exc in sorted(exceptions.items()):
        approved_dt = exc.get("_approved_dt")
        expires_dt = exc.get("_expires_dt")
        if approved_dt is not None and now < approved_dt:
            invalid_time_ids.add(adv_id)
            policy_errors.append({
                "code": "invalid_approval_timestamp",
                "advisory_id": adv_id,
                "crate": exc.get("crate"),
                "message": f"exception {adv_id} approved_at ({exc['approved_at']}) is after current UTC time ({format_utc(now)})",
            })
        if expires_dt is not None and now >= expires_dt:
            expired_ids.add(adv_id)
            expiry_errors.append({
                "code": "exception_expired",
                "advisory_id": adv_id,
                "crate": exc.get("crate"),
                "message": (
                    f"exception {adv_id} for {exc.get('crate')} ({exc.get('affected_versions')}) "
                    f"expired at {exc['expires_at']} (evaluated at {format_utc(now)})"
                ),
            })

    # Optional PR base-commit comparison for newly added / widened / extended exceptions.
    base_ref = (args.base_ref or os.environ.get("CI_BASE_SHA") or "").strip()
    trusted_records: list[dict[str, Any]] | None = None
    if args.trusted_approvals_json:
        try:
            loaded = json.loads(Path(args.trusted_approvals_json).read_text(encoding="utf-8"))
            if not isinstance(loaded, list):
                raise ValueError("trusted-approvals-json must contain a JSON array")
            trusted_records = loaded
        except (OSError, ValueError) as exc_err:
            policy_errors.append({"code": "invalid_trusted_approvals", "message": str(exc_err)})
    pr_author = args.pr_author or read_pr_author_from_event()
    unapproved_by_id: dict[str, list[str]] = {}
    if base_ref:
        unapproved_by_id, base_diff_errors = check_base_diff_approvals(
            repo_root,
            base_ref,
            exceptions,
            args.deny_config,
            args.exceptions_config,
            trusted_records,
            pr_author,
        )
        policy_errors.extend(base_diff_errors)

    # Check pinned cargo-deny version.
    actual_version, version_errors = check_cargo_deny_version(args.cargo_deny_bin, args.expected_cargo_deny_version)
    infra_errors.extend(version_errors)

    expected_db_url = args.advisory_db_url
    advisory_db_info: dict[str, Any] = {
        "url": expected_db_url,
        "path_label": None,
        "revision": None,
        "commit_timestamp": None,
        "fetched_at": None,
        "freshness_source": "not_run",
        "used_cached_db": False,
        "cache_age_seconds": None,
        "max_cache_age_seconds": MAX_CACHE_AGE_SECONDS,
    }
    raw_log_chunks: list[str] = []
    authoritative_exit: int | None = None
    probe_exit: int | None = None
    advisory_hits: list[dict[str, Any]] = []
    unignored_hit_ids: set[str] = set()

    if not version_errors and deny_path.is_file() and lock_path.is_file() and manifest_path.is_file():
        # Step 1: Advisory database update (or verified <=24h cache fallback).
        fresh_fetch = False
        fetch_error: str | None = None
        if args.offline_only:
            fetch_error = "offline-only mode requested; skipped live fetch"
        else:
            fetch_cmd = [
                args.cargo_deny_bin,
                "--locked",
                "--config",
                str(deny_path),
                "--manifest-path",
                str(manifest_path),
                "fetch",
                "db",
            ]
            try:
                fetch_proc = subprocess.run(fetch_cmd, cwd=repo_root, capture_output=True, text=True, timeout=180, check=False)
                raw_log_chunks.append(
                    f"=== fetch db (exit={fetch_proc.returncode}) ===\n{fetch_proc.stdout}{fetch_proc.stderr}\n"
                )
                if fetch_proc.returncode == 0:
                    fresh_fetch = True
                else:
                    fetch_error = (fetch_proc.stderr or fetch_proc.stdout).strip() or f"exit {fetch_proc.returncode}"
            except (OSError, subprocess.TimeoutExpired) as exc_err:
                fetch_error = str(exc_err)
                raw_log_chunks.append(f"=== fetch db error ===\n{fetch_error}\n")

        db_dir = find_advisory_db_dir(
            repo_root,
            deny_doc,
            Path(args.advisory_db_dir).resolve() if args.advisory_db_dir else None,
            expected_db_url,
        )
        advisory_db_info, db_errors = inspect_advisory_db(
            db_dir,
            expected_db_url,
            now,
            fresh_fetch=fresh_fetch,
            fetch_error=fetch_error,
        )
        infra_errors.extend(db_errors)

        # Step 2: Run cargo-deny check advisories (only if DB freshness verified).
        if not db_errors:
            with tempfile.TemporaryDirectory(prefix="rustfs-advisories-") as tmpdir:
                tmp_root = Path(tmpdir)
                metadata_arg: list[str] = []
                metadata_failed = False
                if args.metadata_path:
                    meta_path = Path(args.metadata_path).resolve()
                    if not meta_path.is_file():
                        infra_errors.append({"code": "metadata_failed", "message": f"metadata file not found: {meta_path}"})
                        metadata_failed = True
                    else:
                        metadata_arg = ["--metadata-path", str(meta_path)]
                elif not args.skip_cargo_metadata:
                    meta_cmd = [
                        args.cargo_bin,
                        "metadata",
                        "--format-version",
                        "1",
                        "--locked",
                        "--manifest-path",
                        str(manifest_path),
                        "--all-features",
                    ]
                    if args.offline_only or not fresh_fetch:
                        meta_cmd.append("--offline")
                    try:
                        meta_proc = subprocess.run(meta_cmd, cwd=repo_root, capture_output=True, text=True, timeout=300, check=False)
                        if meta_proc.returncode != 0:
                            raw_log_chunks.append(
                                f"=== cargo metadata failed (exit={meta_proc.returncode}) ===\n{meta_proc.stderr}\n"
                            )
                            infra_errors.append({
                                "code": "metadata_failed",
                                "message": f"cargo metadata failed (exit {meta_proc.returncode}): {sanitize_text(meta_proc.stderr.strip()[:300])}",
                            })
                            metadata_failed = True
                        else:
                            meta_file = tmp_root / "cargo-metadata.json"
                            meta_file.write_text(meta_proc.stdout, encoding="utf-8")
                            metadata_arg = ["--metadata-path", str(meta_file)]
                    except (OSError, subprocess.TimeoutExpired) as exc_err:
                        infra_errors.append({"code": "metadata_failed", "message": f"cargo metadata error: {exc_err}"})
                        metadata_failed = True

                if not metadata_failed:
                    # 2a. Authoritative run against real deny.toml
                    auth_cmd = [
                        args.cargo_deny_bin,
                        "--locked",
                        "--offline",
                        *metadata_arg,
                        "--config",
                        str(deny_path),
                        "--manifest-path",
                        str(manifest_path),
                        "--format",
                        "json",
                        "check",
                        "advisories",
                    ]
                    try:
                        auth_proc = subprocess.run(auth_cmd, cwd=repo_root, capture_output=True, text=True, timeout=180, check=False)
                        authoritative_exit = auth_proc.returncode
                        combined_auth = (auth_proc.stdout or "") + ("\n" + auth_proc.stderr if auth_proc.stderr else "")
                        raw_log_chunks.append(f"=== authoritative check (exit={authoritative_exit}) ===\n{combined_auth}\n")
                        auth_diags, auth_summary, auth_parse_errors = parse_cargo_deny_jsonl(combined_auth)
                        if auth_parse_errors:
                            infra_errors.extend(auth_parse_errors)
                        if auth_summary is None and authoritative_exit != 0 and not auth_diags:
                            infra_errors.append({
                                "code": "cargo_deny_failed",
                                "message": f"cargo-deny failed without advisory diagnostics (exit {authoritative_exit})",
                            })
                        for diag in auth_diags:
                            if diag.get("severity") == "error":
                                hit = hit_from_diagnostic(diag)
                                if hit["advisory_id"]:
                                    unignored_hit_ids.add(hit["advisory_id"])
                                advisory_errors.append({
                                    "code": "yanked_crate" if hit["code"] == "yanked" else "unexempted_advisory",
                                    "advisory_id": hit["advisory_id"],
                                    "crate": hit["crate"],
                                    "message": (
                                        f"unexempted {hit['code']} on {hit['crate']} {','.join(hit['versions'])}: "
                                        f"{hit['advisory_id'] or hit['title']}"
                                    ),
                                })
                        if authoritative_exit != 0 and not advisory_errors and not infra_errors:
                            infra_errors.append({
                                "code": "cargo_deny_failed",
                                "message": f"cargo-deny returned non-zero exit code {authoritative_exit}",
                            })
                    except (OSError, subprocess.TimeoutExpired) as exc_err:
                        infra_errors.append({"code": "cargo_deny_failed", "message": f"cargo-deny execution error: {exc_err}"})

                    # 2b. Unfiltered diagnostic probe with ignore=[] to capture full hit chains and verify exception scopes.
                    if not infra_errors and deny_text:
                        try:
                            probe_cfg = tmp_root / "deny-unfiltered.toml"
                            probe_cfg.write_text(strip_advisory_ignores_toml(deny_text), encoding="utf-8")
                            probe_cmd = [
                                args.cargo_deny_bin,
                                "--locked",
                                "--offline",
                                *metadata_arg,
                                "--config",
                                str(probe_cfg),
                                "--manifest-path",
                                str(manifest_path),
                                "--format",
                                "json",
                                "check",
                                "advisories",
                            ]
                            probe_proc = subprocess.run(probe_cmd, cwd=repo_root, capture_output=True, text=True, timeout=180, check=False)
                            probe_exit = probe_proc.returncode
                            combined_probe = (probe_proc.stdout or "") + ("\n" + probe_proc.stderr if probe_proc.stderr else "")
                            raw_log_chunks.append(f"=== unfiltered probe (exit={probe_exit}) ===\n{combined_probe}\n")
                            probe_diags, probe_summary, probe_parse_errors = parse_cargo_deny_jsonl(combined_probe)
                            if probe_parse_errors:
                                infra_errors.extend(probe_parse_errors)
                            elif probe_summary is None and probe_exit != 0 and not probe_diags:
                                infra_errors.append({
                                    "code": "cargo_deny_probe_failed",
                                    "message": f"unfiltered cargo-deny probe failed without diagnostics (exit {probe_exit})",
                                })
                            else:
                                for diag in probe_diags:
                                    if diag.get("severity") == "error":
                                        advisory_hits.append(hit_from_diagnostic(diag))
                        except (OSError, ValueError, subprocess.TimeoutExpired) as exc_err:
                            infra_errors.append({"code": "cargo_deny_probe_failed", "message": f"unfiltered probe error: {exc_err}"})

    # Reconcile unfiltered hits with registered exceptions (crate name, exact affected_versions, stale exceptions).
    hits_by_id: dict[str, list[dict[str, Any]]] = {}
    for hit in advisory_hits:
        adv_id = hit.get("advisory_id")
        if adv_id:
            hits_by_id.setdefault(adv_id, []).append(hit)

    exception_verdicts: list[dict[str, Any]] = []
    for adv_id, exc in sorted(exceptions.items()):
        matched_hits = hits_by_id.get(adv_id, [])
        matched_versions = sorted({v for h in matched_hits for v in h.get("versions", [])})
        matched_crates = sorted({h.get("crate", "") for h in matched_hits if h.get("crate")})
        entry_errs = list(exc.get("_errors", []))
        status = "valid"

        if adv_id in unapproved_by_id:
            status = "unapproved_change"
            for code in unapproved_by_id[adv_id]:
                entry_errs.append({"code": code, "message": f"PR modification requires trusted approval ({code})"})

        if adv_id not in deny_ids:
            status = "invalid"
            entry_errs.append({"code": "missing_deny_ignore", "message": "not present in deny.toml [advisories].ignore"})

        if not infra_errors and probe_exit is not None:
            if not matched_hits:
                status = "stale_unused"
                err = {
                    "code": "stale_unused_exception",
                    "advisory_id": adv_id,
                    "crate": exc.get("crate"),
                    "message": f"exception {adv_id} ({exc.get('crate')}) did not match any active advisory in Cargo.lock; remove stale ignore",
                }
                entry_errs.append(err)
                policy_errors.append(err)
            else:
                if matched_crates != [exc.get("crate")]:
                    status = "scope_mismatch"
                    err = {
                        "code": "exception_scope_mismatch",
                        "advisory_id": adv_id,
                        "crate": exc.get("crate"),
                        "message": f"exception {adv_id} declares crate {exc.get('crate')!r}, but scan matched {matched_crates}",
                    }
                    entry_errs.append(err)
                    policy_errors.append(err)
                interval: VersionInterval | None = exc.get("_interval")
                if interval is not None:
                    out_of_scope = [v for v in matched_versions if not interval.contains(v)]
                    if out_of_scope:
                        status = "scope_mismatch"
                        err = {
                            "code": "exception_scope_mismatch",
                            "advisory_id": adv_id,
                            "crate": exc.get("crate"),
                            "message": (
                                f"exception {adv_id} affected_versions {exc.get('affected_versions')!r} "
                                f"does not cover matched version(s): {out_of_scope}"
                            ),
                        }
                        entry_errs.append(err)
                        policy_errors.append(err)

        if adv_id in expired_ids:
            status = "expired"
            entry_errs.append({"code": "exception_expired", "message": f"expired at {exc.get('expires_at')}"})
        elif adv_id in invalid_time_ids or exc.get("_errors"):
            status = "invalid"

        exception_verdicts.append({
            "advisory_id": adv_id,
            "crate": exc.get("crate"),
            "affected_versions": exc.get("affected_versions"),
            "owner": exc.get("owner"),
            "tracking_issue": exc.get("tracking_issue"),
            "approved_at": exc.get("approved_at"),
            "expires_at": exc.get("expires_at"),
            "approval_reference": exc.get("approval_reference"),
            "matched_versions": matched_versions,
            "status": status,
            "errors": [{"code": e["code"], "message": e["message"]} for e in entry_errs],
        })

    verdict_by_id = {v["advisory_id"]: v["status"] for v in exception_verdicts}
    for hit in advisory_hits:
        adv_id = hit.get("advisory_id")
        if not adv_id or adv_id in unignored_hit_ids:
            hit["disposition"] = "unexempted"
        elif adv_id in verdict_by_id:
            status_map = {
                "valid": "exempted",
                "expired": "expired_exception",
                "unapproved_change": "unapproved_exception",
                "scope_mismatch": "scope_mismatch",
                "invalid": "invalid_exception",
                "stale_unused": "stale_unused",
            }
            hit["disposition"] = status_map.get(verdict_by_id[adv_id], "invalid_exception")
        else:
            hit["disposition"] = "untracked_ignore"

    all_errors = [*infra_errors, *advisory_errors, *policy_errors, *expiry_errors]
    reason_codes = sorted({e["code"] for e in all_errors})
    if infra_errors:
        exit_code = EXIT_INFRA_ERROR
    elif advisory_errors:
        exit_code = EXIT_ADVISORY_FOUND
    elif policy_errors:
        exit_code = EXIT_POLICY_ERROR
    elif expiry_errors:
        exit_code = EXIT_EXCEPTION_EXPIRED
    else:
        exit_code = EXIT_OK

    raw_log_written: str | None = None
    if args.raw_log:
        raw_log_path = Path(args.raw_log)
        raw_log_path.parent.mkdir(parents=True, exist_ok=True)
        raw_log_path.write_text(sanitize_text("\n".join(raw_log_chunks)), encoding="utf-8")
        raw_log_written = str(args.raw_log)

    graph_cfg = deny_doc.get("graph", {}) if isinstance(deny_doc, dict) else {}
    adv_cfg = deny_doc.get("advisories", {}) if isinstance(deny_doc, dict) else {}
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "pass" if exit_code == EXIT_OK else "fail",
        "exit_code": exit_code,
        "reason_codes": reason_codes,
        "errors": all_errors,
        "evaluated_at": format_utc(now),
        "clock_source": clock_source,
        "source_sha": git_head_sha(repo_root),
        "base_sha": base_ref or None,
        "run_identity": {
            "repository": os.environ.get("GITHUB_REPOSITORY"),
            "event_name": os.environ.get("GITHUB_EVENT_NAME"),
            "ref": os.environ.get("GITHUB_REF"),
            "run_id": os.environ.get("GITHUB_RUN_ID"),
            "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
            "job": os.environ.get("GITHUB_JOB"),
        },
        "digests": digests,
        "tool": {
            "name": "cargo-deny",
            "expected_version": args.expected_cargo_deny_version,
            "actual_version": actual_version,
            "authoritative_exit_code": authoritative_exit,
            "probe_exit_code": probe_exit,
        },
        "graph_policy": {
            "all_features": graph_cfg.get("all-features"),
            "no_default_features": graph_cfg.get("no-default-features"),
            "yanked": adv_cfg.get("yanked"),
            "locked": True,
        },
        "advisory_db": advisory_db_info,
        "advisory_hits": advisory_hits,
        "exceptions": exception_verdicts,
        "raw_log_path": raw_log_written,
    }

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=False) + "\n", encoding="utf-8")

    return report, exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="Run the unit test suite for this checker")
    parser.add_argument("--repo-root", default=str(ROOT), help="Repository root directory")
    parser.add_argument("--deny-config", default=DEFAULT_DENY_CONFIG, help="Path to deny.toml relative to repo root")
    parser.add_argument(
        "--exceptions-config",
        default=DEFAULT_EXCEPTIONS_CONFIG,
        help="Path to advisory-exceptions.toml relative to repo root",
    )
    parser.add_argument("--cargo-lock", default=DEFAULT_CARGO_LOCK, help="Path to Cargo.lock relative to repo root")
    parser.add_argument("--manifest-path", default=DEFAULT_CARGO_MANIFEST, help="Path to Cargo.toml relative to repo root")
    parser.add_argument("--output", help="Write structured advisories-gate.json report to this path")
    parser.add_argument("--raw-log", help="Write raw cargo-deny stdout/stderr log to this path")
    parser.add_argument("--base-ref", default="", help="Base commit SHA for PR exception diff checks (defaults to CI_BASE_SHA)")
    parser.add_argument("--trusted-approvals-json", help="Optional JSON array of verified maintainer approval records (for tests)")
    parser.add_argument("--pr-author", help="Optional PR author login (defaults to GitHub Actions event payload)")
    parser.add_argument("--now", help="Injected UTC RFC 3339 clock (forbidden under GITHUB_ACTIONS)")
    parser.add_argument("--offline-only", action="store_true", help="Skip live DB fetch and evaluate cached DB freshness only")
    parser.add_argument("--advisory-db-dir", help="Explicit advisory DB git checkout path (for tests)")
    parser.add_argument("--advisory-db-url", default=DEFAULT_ADVISORY_DB_URL, help="Expected advisory DB remote URL")
    parser.add_argument("--cargo-deny-bin", default="cargo-deny", help="cargo-deny executable path")
    parser.add_argument("--cargo-bin", default="cargo", help="cargo executable path")
    parser.add_argument("--expected-cargo-deny-version", default=EXPECTED_CARGO_DENY_VERSION, help="Pinned cargo-deny version")
    parser.add_argument("--metadata-path", help="Precomputed cargo metadata JSON file")
    parser.add_argument("--skip-cargo-metadata", action="store_true", help="Do not pre-run cargo metadata (for mocked tests)")
    parser.add_argument("--allow-ci-clock-override-for-test", action="store_true", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.self_test:
        suite = unittest.defaultTestLoader.discover(str(Path(__file__).resolve().parent), pattern="test_check_advisories.py")
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1

    report, exit_code = run_gate(args)
    summary_line = (
        f"advisories-gate: status={report['status']} exit_code={exit_code} "
        f"hits={len(report['advisory_hits'])} exceptions={len(report['exceptions'])} "
        f"reasons={','.join(report['reason_codes']) or 'none'}"
    )
    print(summary_line)
    for err in report["errors"]:
        print(f"ERROR [{err['code']}]: {err['message']}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
