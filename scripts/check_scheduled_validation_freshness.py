#!/usr/bin/env python3
"""Fail when a critical scheduled validation has not started recently."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest import mock
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


def load_validations(path: Path) -> list[tuple[str, int]]:
    data = json.loads(path.read_text())
    if not isinstance(data, list) or not data:
        raise ValueError("scheduled validation config must be a non-empty list")

    validations: list[tuple[str, int]] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("scheduled validation entries must be objects")
        workflow = item.get("workflow")
        max_age_hours = item.get("max_age_hours")
        if not isinstance(workflow, str) or not re.fullmatch(
            r"\.github/workflows/[a-z0-9-]+\.yml", workflow
        ):
            raise ValueError(f"invalid scheduled validation workflow: {workflow!r}")
        if workflow in seen:
            raise ValueError(f"duplicate scheduled validation workflow: {workflow}")
        if (
            not isinstance(max_age_hours, int)
            or isinstance(max_age_hours, bool)
            or max_age_hours <= 0
        ):
            raise ValueError(f"invalid max_age_hours for {workflow}: {max_age_hours!r}")
        seen.add(workflow)
        validations.append((workflow, max_age_hours))
    return validations


def parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"invalid run timestamp: {value!r}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"run timestamp has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def stale_reason(
    run: dict[str, object] | None, now: datetime, max_age_hours: int
) -> str | None:
    if run is None:
        return "no scheduled run has been recorded"
    created_at = parse_timestamp(run.get("created_at"))
    age = now - created_at
    if age > timedelta(hours=max_age_hours):
        return f"last scheduled run is {age.total_seconds() / 3600:.1f}h old"
    return None


def fetch_latest_scheduled_run(
    repository: str, workflow: str, token: str, api_url: str
) -> dict[str, object] | None:
    owner, repo = repository.split("/", 1)
    workflow_name = Path(workflow).name
    endpoint = (
        f"{api_url.rstrip('/')}/repos/{quote(owner, safe='')}/{quote(repo, safe='')}"
        f"/actions/workflows/{quote(workflow_name, safe='')}/runs?"
        + urlencode({"event": "schedule", "per_page": 1})
    )
    request = Request(
        endpoint,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    runs = payload.get("workflow_runs")
    if not isinstance(runs, list):
        raise ValueError(f"GitHub returned no workflow_runs list for {workflow}")
    if not runs:
        return None
    if not isinstance(runs[0], dict):
        raise ValueError(f"GitHub returned an invalid workflow run for {workflow}")
    return runs[0]


def write_report(path: Path, failures: list[tuple[str, int, str, str]]) -> None:
    lines = ["## Scheduled validation freshness"]
    if not failures:
        lines.append("")
        lines.append("All critical scheduled validations have a recent scheduled run.")
    else:
        lines.extend(
            [
                "",
                "The following critical validations are stale or could not be inspected:",
                "",
                "| Workflow | Limit | Result | Last run |",
                "| --- | ---: | --- | --- |",
            ]
        )
        for workflow, max_age_hours, reason, run_url in failures:
            link = f"[open]({run_url})" if run_url else "—"
            lines.append(f"| `{workflow}` | {max_age_hours}h | {reason} | {link} |")
    path.write_text("\n".join(lines) + "\n")


def check_freshness(
    config: Path, report: Path, repository: str, token: str, api_url: str
) -> int:
    now = datetime.now(timezone.utc)
    failures: list[tuple[str, int, str, str]] = []
    for workflow, max_age_hours in load_validations(config):
        try:
            run = fetch_latest_scheduled_run(repository, workflow, token, api_url)
            reason = stale_reason(run, now, max_age_hours)
            if reason is not None:
                run_url = str(run.get("html_url", "")) if run else ""
                failures.append((workflow, max_age_hours, reason, run_url))
        except Exception as error:
            failures.append(
                (workflow, max_age_hours, f"inspection failed: {error}", "")
            )
    write_report(report, failures)
    return 1 if failures else 0


class SelfTests(unittest.TestCase):
    NOW = datetime(2026, 8, 22, 12, tzinfo=timezone.utc)

    def test_freshness_boundaries(self) -> None:
        at_limit = {"created_at": "2026-08-21T00:00:00Z"}
        past_limit = {"created_at": "2026-08-20T23:59:59Z"}
        self.assertIsNone(stale_reason(at_limit, self.NOW, 36))
        self.assertIsNotNone(stale_reason(past_limit, self.NOW, 36))
        self.assertIsNotNone(stale_reason(None, self.NOW, 36))

    def test_config_rejects_duplicate_and_invalid_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "validations.json"
            path.write_text(
                json.dumps(
                    [
                        {"workflow": ".github/workflows/ci.yml", "max_age_hours": 36},
                        {"workflow": ".github/workflows/ci.yml", "max_age_hours": 0},
                    ]
                )
            )
            with self.assertRaises(ValueError):
                load_validations(path)
            path.write_text(
                json.dumps(
                    [{"workflow": ".github/workflows/ci.yml", "max_age_hours": 0}]
                )
            )
            with self.assertRaises(ValueError):
                load_validations(path)

    def test_check_reports_missing_runs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "validations.json"
            report = root / "report.md"
            config.write_text(
                json.dumps(
                    [
                        {"workflow": ".github/workflows/ci.yml", "max_age_hours": 36},
                        {"workflow": ".github/workflows/fuzz.yml", "max_age_hours": 36},
                        {"workflow": ".github/workflows/mint.yml", "max_age_hours": 36},
                    ]
                )
            )
            with mock.patch(
                __name__ + ".fetch_latest_scheduled_run",
                side_effect=[
                    {"created_at": "2999-01-01T00:00:00Z"},
                    None,
                    RuntimeError("API unavailable"),
                ],
            ):
                self.assertEqual(
                    check_freshness(
                        config,
                        report,
                        "rustfs/rustfs",
                        "token",
                        "https://api.github.test",
                    ),
                    1,
                )
            contents = report.read_text()
            self.assertIn(".github/workflows/fuzz.yml", contents)
            self.assertIn("inspection failed: API unavailable", contents)
            self.assertNotIn(".github/workflows/ci.yml`", contents)

            config.write_text(
                json.dumps(
                    [{"workflow": ".github/workflows/ci.yml", "max_age_hours": 36}]
                )
            )
            with mock.patch(
                __name__ + ".fetch_latest_scheduled_run",
                return_value={"created_at": "2999-01-01T00:00:00Z"},
            ):
                self.assertEqual(
                    check_freshness(
                        config,
                        report,
                        "rustfs/rustfs",
                        "token",
                        "https://api.github.test",
                    ),
                    0,
                )
            self.assertIn("All critical scheduled validations", report.read_text())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, default=ROOT / ".github/scheduled-validations.json"
    )
    parser.add_argument("--report", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        load_validations(args.config)
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)
        return (
            0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
        )
    if args.report is None:
        parser.error("--report is required unless --self-test is used")

    repository = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_TOKEN", "")
    api_url = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", repository):
        parser.error("GITHUB_REPOSITORY must be owner/repository")
    if not token:
        parser.error("GH_TOKEN is required")
    return check_freshness(args.config, args.report, repository, token, api_url)


if __name__ == "__main__":
    raise SystemExit(main())
