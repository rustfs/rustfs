#!/usr/bin/env python3
"""Require recent scheduled attempts and completed successes on the default branch."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest import mock
from urllib.parse import parse_qs, quote, urlencode, urlsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


def load_validations(path: Path) -> list[tuple[str, int, datetime | None]]:
    data = json.loads(path.read_text())
    if not isinstance(data, list) or not data:
        raise ValueError("scheduled validation config must be a non-empty list")

    validations: list[tuple[str, int, datetime | None]] = []
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
        grace_raw = item.get("never_ran_grace_until")
        grace: datetime | None = None
        if grace_raw is not None:
            if not isinstance(grace_raw, str):
                raise ValueError(
                    f"invalid never_ran_grace_until for {workflow}: {grace_raw!r}"
                )
            try:
                grace = parse_timestamp(grace_raw)
            except ValueError as error:
                raise ValueError(
                    f"invalid never_ran_grace_until for {workflow}: {error}"
                ) from error
        seen.add(workflow)
        validations.append((workflow, max_age_hours, grace))
    return validations


def parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"invalid run timestamp: {value!r}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"run timestamp has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def stale_reason(
    run: dict[str, object] | None,
    now: datetime,
    max_age_hours: int,
) -> str | None:
    if run is None:
        return "no scheduled run has been recorded"
    created_at = parse_timestamp(run.get("created_at"))
    age = now - created_at
    if age > timedelta(hours=max_age_hours):
        return f"last scheduled run is {age.total_seconds() / 3600:.1f}h old"
    return None


def fetch_latest_scheduled_run(
    repository: str,
    workflow: str,
    token: str,
    api_url: str,
    default_branch: str,
    successful: bool = False,
) -> dict[str, object] | None:
    owner, repo = repository.split("/", 1)
    workflow_name = Path(workflow).name
    query = {"event": "schedule", "branch": default_branch, "per_page": 1}
    if successful:
        # Filter on the server: the last success may be beyond a page of failures.
        query["status"] = "success"
    endpoint = (
        f"{api_url.rstrip('/')}/repos/{quote(owner, safe='')}/{quote(repo, safe='')}"
        f"/actions/workflows/{quote(workflow_name, safe='')}/runs?"
        + urlencode(query)
    )
    request = Request(
        endpoint,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    # Two requests per manifest entry must fit the watchdog's ten-minute job.
    with urlopen(request, timeout=15) as response:
        payload = json.load(response)
    runs = payload.get("workflow_runs") if isinstance(payload, dict) else None
    if not isinstance(runs, list):
        raise ValueError(f"GitHub returned no workflow_runs list for {workflow}")
    total_count = payload.get("total_count")
    if not isinstance(total_count, int) or isinstance(total_count, bool) or total_count < len(runs):
        raise ValueError(f"GitHub returned an invalid run count for {workflow}")
    if not runs:
        if total_count:
            raise ValueError(f"GitHub returned an empty first page with recorded runs for {workflow}")
        return None
    run = runs[0]
    if not isinstance(run, dict):
        raise ValueError(f"GitHub returned an invalid workflow run for {workflow}")
    if run.get("event") != "schedule" or run.get("head_branch") != default_branch:
        raise ValueError(f"GitHub returned a run outside the scheduled default-branch query for {workflow}")
    if not isinstance(run.get("status"), str) or not run["status"]:
        raise ValueError(f"GitHub returned no run status for {workflow}")
    conclusion = run.get("conclusion")
    if (conclusion is not None and not isinstance(conclusion, str)) or (
        run["status"] == "completed" and not conclusion
    ):
        raise ValueError(f"GitHub returned an invalid run conclusion for {workflow}")
    if successful and (run["status"] != "completed" or conclusion != "success"):
        raise ValueError(f"GitHub returned a run without a completed success for {workflow}")
    parse_timestamp(run.get("created_at"))
    if not isinstance(run.get("html_url"), str) or not run["html_url"]:
        raise ValueError(f"GitHub returned no run URL for {workflow}")
    return run


def describe_run(run: dict[str, object] | None) -> str:
    if run is None:
        return "No recorded run"
    outcome = run["status"]
    if run.get("conclusion"):
        outcome = f"{outcome}/{run['conclusion']}"
    return f"[{outcome}]({run['html_url']}) — created {run['created_at']}"


def write_report(path: Path, rows: list[tuple[str, int, str, str, str]], default_branch: str) -> None:
    lines = [
        "## Scheduled validation freshness",
        "",
        f"Default branch: `{default_branch}`. Ages use scheduled-run creation time; rerunning an old commit does not refresh its evidence.",
        "Attempt outcomes are shown independently of successful-run freshness.",
        "Success is the GitHub workflow run conclusion; suite completeness remains the responsibility of each workflow.",
        "",
        "| Workflow | Limit | Freshness | Last attempt | Last completed success |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for workflow, max_age_hours, result, attempt, success in rows:
        cells = [f"`{workflow}`", f"{max_age_hours}h", result, attempt, success]
        lines.append("| " + " | ".join(cell.replace("|", "\\|").replace("\n", " ") for cell in cells) + " |")
    path.write_text("\n".join(lines) + "\n")


def check_freshness(
    config: Path, report: Path, repository: str, token: str, api_url: str, default_branch: str
) -> int:
    now = datetime.now(timezone.utc)
    rows: list[tuple[str, int, str, str, str]] = []
    failed = False
    for workflow, max_age_hours, never_ran_grace_until in load_validations(config):
        runs: dict[str, dict[str, object] | None] = {}
        reasons: list[str] = []
        for label, successful in (("Last attempt", False), ("Last completed success", True)):
            try:
                runs[label] = fetch_latest_scheduled_run(
                    repository, workflow, token, api_url, default_branch, successful
                )
            except Exception as error:
                reasons.append(f"{label}: inspection failed: {error}")
        # A failed inspection or any recorded attempt ends first-run grace.
        initial_grace = (
            len(runs) == 2
            and all(run is None for run in runs.values())
            and never_ran_grace_until is not None
            and now <= never_ran_grace_until
        )
        if not initial_grace:
            for label, run in runs.items():
                reason = stale_reason(run, now, max_age_hours)
                if reason is not None:
                    reasons.append(f"{label}: {reason}")
        failed |= bool(reasons)
        result = "; ".join(reasons) if reasons else "Fresh"
        if initial_grace:
            result = f"Initial grace until {never_ran_grace_until.isoformat()}"
        evidence = [
            describe_run(runs[label]) if label in runs else "Inspection failed"
            for label in ("Last attempt", "Last completed success")
        ]
        rows.append((workflow, max_age_hours, result, *evidence))
    write_report(report, rows, default_branch)
    return 1 if failed else 0


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
            for bad_grace in (36, "not-a-timestamp", "2026-09-01T00:00:00"):
                path.write_text(
                    json.dumps(
                        [
                            {
                                "workflow": ".github/workflows/ci.yml",
                                "max_age_hours": 36,
                                "never_ran_grace_until": bad_grace,
                            }
                        ]
                    )
                )
                with self.assertRaises(ValueError):
                    load_validations(path)
            path.write_text(
                json.dumps(
                    [
                        {
                            "workflow": ".github/workflows/ci.yml",
                            "max_age_hours": 36,
                            "never_ran_grace_until": "2026-09-02T06:37:00Z",
                        }
                    ]
                )
            )
            self.assertEqual(
                load_validations(path),
                [
                    (
                        ".github/workflows/ci.yml",
                        36,
                        datetime(2026, 9, 2, 6, 37, tzinfo=timezone.utc),
                    )
                ],
            )

    @staticmethod
    def run_fixture(**overrides: object) -> dict[str, object]:
        return {
            "status": "completed",
            "conclusion": "success",
            "event": "schedule",
            "head_branch": "release/current",
            "created_at": "2026-08-22T00:00:00Z",
            "html_url": "https://github.test/rustfs/rustfs/actions/runs/1",
            **overrides,
        }

    def check_payloads(
        self, payloads: list[object], *, grace: str | None = None, workflows: int = 1
    ) -> tuple[int, str, list]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "validations.json"
            report = root / "report.md"
            entries = [
                {"workflow": f".github/workflows/check-{index}.yml", "max_age_hours": 36}
                for index in range(workflows)
            ]
            if grace is not None:
                entries[0]["never_ran_grace_until"] = grace
            config.write_text(json.dumps(entries))
            responses = []
            for payload in payloads:
                if isinstance(payload, dict) and isinstance(payload.get("workflow_runs"), list):
                    payload = {"total_count": len(payload["workflow_runs"]), **payload}
                responses.append(payload if isinstance(payload, Exception) else io.StringIO(json.dumps(payload)))
            with (
                mock.patch(__name__ + ".urlopen", side_effect=responses) as request,
                mock.patch(__name__ + ".datetime", wraps=datetime) as clock,
            ):
                clock.now.return_value = self.NOW
                status = check_freshness(
                    config, report, "rustfs/rustfs", "test-token",
                    "https://api.github.test", "release/current",
                )
            return status, report.read_text(), request.call_args_list

    def test_requests_filter_schedule_default_branch_and_success_on_server(self) -> None:
        attempt = self.run_fixture(status="in_progress", conclusion=None)
        success = self.run_fixture(html_url="https://github.test/rustfs/rustfs/actions/runs/2")
        status, report, calls = self.check_payloads([
            {"workflow_runs": [attempt], "total_count": 1001},
            {"workflow_runs": [success], "total_count": 1},
        ])
        self.assertEqual(status, 0)
        self.assertEqual(len(calls), 2)
        for call, successful in zip(calls, (False, True)):
            request = call.args[0]
            url = urlsplit(request.full_url)
            self.assertEqual(url.path, "/repos/rustfs/rustfs/actions/workflows/check-0.yml/runs")
            expected = {"event": ["schedule"], "branch": ["release/current"], "per_page": ["1"]}
            if successful:
                expected["status"] = ["success"]
            self.assertEqual(parse_qs(url.query), expected)
            self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
            self.assertEqual(call.kwargs, {"timeout": 15})
        self.assertIn("[in_progress]", report)
        self.assertIn(str(attempt["html_url"]), report)
        self.assertIn(str(success["html_url"]), report)

    def test_cancelled_attempt_cannot_refresh_expired_success(self) -> None:
        attempt = self.run_fixture(conclusion="cancelled")
        success = self.run_fixture(
            created_at="2026-08-20T23:59:59Z", updated_at="2026-08-22T11:59:59Z",
            html_url="https://github.test/rustfs/rustfs/actions/runs/2",
        )
        status, report, _ = self.check_payloads([
            {"workflow_runs": [attempt]}, {"workflow_runs": [success]},
        ])
        self.assertEqual(status, 1)
        self.assertIn("Last completed success: last scheduled run is", report)
        self.assertIn("[completed/cancelled]", report)
        for run in (attempt, success):
            self.assertIn(str(run["html_url"]), report)
            self.assertIn(str(run["created_at"]), report)

    def test_attempt_outcome_does_not_replace_recent_success(self) -> None:
        success = self.run_fixture(created_at="2026-08-21T00:00:00Z")
        for state, conclusion in (
            ("completed", "failure"), ("completed", "cancelled"),
            ("completed", "timed_out"), ("completed", "success"),
            ("queued", None), ("in_progress", None),
        ):
            with self.subTest(state=state, conclusion=conclusion):
                status, report, _ = self.check_payloads([
                    {"workflow_runs": [self.run_fixture(status=state, conclusion=conclusion)]},
                    {"workflow_runs": [success]},
                ])
                self.assertEqual(status, 0)
                self.assertIn(f"[{state}" + (f"/{conclusion}" if conclusion else "") + "]", report)
                self.assertIn("Fresh", report)
                self.assertNotIn("All critical scheduled validations", report)

    def test_grace_requires_two_successful_queries_with_no_history(self) -> None:
        for attempt, success, grace, expected in (
            (None, None, "2026-08-22T12:00:00Z", 0),
            (None, None, "2026-08-22T11:59:59Z", 1),
            (self.run_fixture(conclusion="failure"), None, "2026-08-23T00:00:00Z", 1),
            (self.run_fixture(status="queued", conclusion=None), None, "2026-08-23T00:00:00Z", 1),
            (None, self.run_fixture(), "2026-08-23T00:00:00Z", 1),
        ):
            with self.subTest(attempt=attempt, success=success, grace=grace):
                status, report, _ = self.check_payloads([
                    {"workflow_runs": [] if attempt is None else [attempt]},
                    {"workflow_runs": [] if success is None else [success]},
                ], grace=grace)
                self.assertEqual(status, expected)
                self.assertEqual("Initial grace until" in report, expected == 0)

    def test_api_failures_preserve_other_evidence_and_never_enter_grace(self) -> None:
        good = {"workflow_runs": [self.run_fixture()]}
        for first, second in (
            (RuntimeError("API unavailable"), good),
            (good, RuntimeError("API unavailable")),
            (RuntimeError("API unavailable"), {"workflow_runs": []}),
        ):
            with self.subTest(first=first, second=second):
                status, report, calls = self.check_payloads(
                    [first, second], grace="2026-08-23T00:00:00Z"
                )
                self.assertEqual(status, 1)
                self.assertEqual(len(calls), 2)
                self.assertIn("inspection failed: API unavailable", report)
                self.assertNotIn("Initial grace until", report)
                if first is good or second is good:
                    self.assertIn(str(self.run_fixture()["html_url"]), report)

    def test_invalid_api_evidence_fails_closed(self) -> None:
        malformed = [
            [], {}, {"workflow_runs": {}}, {"workflow_runs": [None]},
            {"workflow_runs": [], "total_count": 1},
            {"workflow_runs": [], "total_count": -1},
            {"workflow_runs": [], "total_count": None},
            {"workflow_runs": [], "total_count": True},
            *({"workflow_runs": [self.run_fixture(**override)]} for override in (
                {"event": "workflow_dispatch"}, {"head_branch": "other"},
                {"created_at": "invalid"}, {"created_at": "2026-08-22T00:00:00"},
                {"status": None}, {"conclusion": None}, {"conclusion": 1},
                {"html_url": ""},
            )),
        ]
        for payload in malformed:
            for index, label in enumerate(("Last attempt", "Last completed success")):
                with self.subTest(payload=payload, label=label):
                    payloads = [{"workflow_runs": [self.run_fixture()]} for _ in range(2)]
                    payloads[index] = payload
                    status, report, _ = self.check_payloads(payloads, grace="2026-08-23T00:00:00Z")
                    self.assertEqual(status, 1)
                    self.assertIn(f"{label}: inspection failed", report)
                    self.assertNotIn("Initial grace until", report)
                    self.assertIn(str(self.run_fixture()["html_url"]), report)
        for state, conclusion in (("in_progress", "success"), ("completed", "failure"), ("completed", "skipped")):
            with self.subTest(state=state, conclusion=conclusion):
                status, report, _ = self.check_payloads([
                    {"workflow_runs": [self.run_fixture()]},
                    {"workflow_runs": [self.run_fixture(status=state, conclusion=conclusion)]},
                ])
                self.assertEqual(status, 1)
                self.assertIn("without a completed success", report)

    def test_report_retains_every_workflow(self) -> None:
        status, report, calls = self.check_payloads([
            {"workflow_runs": [self.run_fixture()]}, {"workflow_runs": [self.run_fixture()]},
            {"workflow_runs": []}, {"workflow_runs": []},
            RuntimeError("API unavailable"), {"workflow_runs": [self.run_fixture()]},
        ], workflows=3)
        self.assertEqual(status, 1)
        self.assertEqual(len(calls), 6)
        for index in range(3):
            self.assertEqual(report.count(f"`.github/workflows/check-{index}.yml`"), 1)
        self.assertIn("No recorded run", report)
        self.assertIn("Inspection failed", report)

    def test_cli_requires_the_repository_default_branch(self) -> None:
        from check_test_wiring import yaml_block

        workflow = (ROOT / ".github/workflows/scheduled-validation-freshness.yml").read_text().splitlines()
        job = yaml_block(workflow, "check-freshness", 2)
        self.assertIsNotNone(job)
        start = job.index("      - name: Check latest scheduled runs")
        end = next((index for index in range(start + 1, len(job)) if job[index].startswith("      - ")), len(job))
        environment = yaml_block(job[start:end], "env", 8)
        self.assertIsNotNone(environment)
        self.assertIn("          RUSTFS_DEFAULT_BRANCH: ${{ github.event.repository.default_branch }}", environment)

        with (
            mock.patch.dict(os.environ, {"GITHUB_REPOSITORY": "rustfs/rustfs", "GH_TOKEN": "test-token"}, clear=True),
            mock.patch.object(sys, "argv", ["checker", "--report", "unused.md"]),
            mock.patch("sys.stderr", new=io.StringIO()) as stderr,
            self.assertRaises(SystemExit) as error,
        ):
            main()
        self.assertEqual(error.exception.code, 2)
        self.assertIn("RUSTFS_DEFAULT_BRANCH", stderr.getvalue())


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
    default_branch = os.environ.get("RUSTFS_DEFAULT_BRANCH", "")
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", repository):
        parser.error("GITHUB_REPOSITORY must be owner/repository")
    if not token:
        parser.error("GH_TOKEN is required")
    if not default_branch or any(character.isspace() for character in default_branch):
        parser.error("RUSTFS_DEFAULT_BRANCH is required and must name the repository default branch")
    return check_freshness(args.config, args.report, repository, token, api_url, default_branch)


if __name__ == "__main__":
    raise SystemExit(main())
