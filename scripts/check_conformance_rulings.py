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
"""Judge the conformance run against .config/conformance-rulings.toml.

Usage:
  check_conformance_rulings.py --run-dir <dir> [--case-dir <pinned cases>] [--rulings <toml>]
  check_conformance_rulings.py --self-test

<dir> holds what the lane observed: the server record acceptance_lane.py reads,
`runner-exit.txt` (the conformance runner's exit code), `alive-after-conformance.txt`
(`alive` when the server still ran after the corpus) and `report.json` (the
runner's `--json` report). The verdict comes from those observations, never
from the exit status of an earlier step.

Red: a failed case without a ruling; a ruled case that fails
differently from its ruling's `failure`; a ruling whose case no longer fails;
and a skip whose reason is the server's own answer (the endpoint not answering,
or a fixture it refused). An unimplemented connection control also blocks acceptance and is
reported as a runner limitation.

Exit codes: 0 judged clean; 1 otherwise; 2 a malformed ledger, report or observation. The Markdown summary
goes to stdout.
"""

import argparse
from collections import Counter
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import re
import sys
import tempfile
import tomllib
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from acceptance_lane import InputError, read_exit, read_text, served, write_served  # noqa: E402  (sibling script)

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / ".config/conformance-rulings.toml"
VERDICTS = ("passed", "failed", "skipped")
# rustfs/backlog#2684 also links the upstream decisions on response heads and HTTP/1 connection closure.
LEDGER_ID = re.compile(r"(R-\d{3}|K-\d+|rustfs/gateway#(?:1120|1159))")
RULING_KEYS = {"case", "ledger", "reason", "failure", "expires"}
NOT_SERVED = "observed 501"
# External skips that block acceptance, including transport controls the runner cannot drive.
EXTERNAL_SKIP = re.compile(r"environment: external (endpoint|fixture .* returned status)")
RUNNER_CONTROL = re.compile(
    r"environment: external endpoint `[a-z_]+` control is not implemented with concurrent response observation"
)


def load_rulings(path):
    try:
        rows = tomllib.loads(Path(path).read_text(encoding="utf-8")).get("ruling", [])
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise InputError(f"{path}: {error}") from error
    rulings = {}
    today = datetime.now(UTC).date()
    for row in rows:
        if not isinstance(row, dict) or set(row) != RULING_KEYS:
            raise InputError(f"{path}: a ruling has exactly the keys {sorted(RULING_KEYS)}: {row}")
        if not LEDGER_ID.fullmatch(str(row["ledger"])):
            raise InputError(f"{path}: unknown ledger id in {row}")
        for key in ("reason", "failure"):
            if not isinstance(row[key], str) or not row[key].strip() or "\n" in row[key]:
                raise InputError(f"{path}: the {key} of {row['case']} must be one non-empty line")
        expiry = row["expires"]
        if type(expiry) is not date or not today < expiry <= today + timedelta(days=90):
            raise InputError(f"{path}: {row['case']} needs a future expiry within 90 days")
        if row["case"] in rulings:
            raise InputError(f"{path}: {row['case']} is ruled twice")
        rulings[row["case"]] = row
    return rulings


def read_report(run_dir):
    """Return the runner's report from run_dir, or None when it wrote none."""
    path = Path(run_dir) / "report.json"
    if not path.is_file():
        return None
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
        cases = report["cases"]
        if not isinstance(cases, list) or any(not {"id", "verdict"} <= set(case) for case in cases):
            raise KeyError("cases")
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise InputError(f"{path}: not a conformance report: {error}") from error
    if any(not isinstance(case["id"], str) or not case["id"] for case in cases):
        raise InputError(f"{path}: each case needs a non-empty ID")
    duplicates = sorted(case for case, count in Counter(row["id"] for row in cases).items() if count != 1)
    if duplicates:
        raise InputError(f"{path}: repeated case IDs: {', '.join(duplicates)}")
    unknown = sorted({case["verdict"] for case in cases} - set(VERDICTS))
    if unknown:
        raise InputError(f"{path}: unknown verdicts {unknown}")
    return report


def first_failure(case):
    return (case.get("failures") or ["no failure recorded"])[0]


def judge(rulings, run_dir, case_dir=None):
    """Return (summary lines, problems)."""
    server, problem = served(run_dir)
    lines = ["### Conformance", "", server]
    if problem:
        return lines, [f"conformance: {problem}; no case ran"]
    runner_exit, report = read_exit(Path(run_dir) / "runner-exit.txt"), read_report(run_dir)
    alive = read_text(Path(run_dir) / "alive-after-conformance.txt")
    lines += [f"- runner exit: {runner_exit if runner_exit is not None else 'not recorded'}",
              f"- server after the corpus: {alive or 'not recorded'}"]
    if alive != "alive":
        return lines, [f"conformance: the server did not stay up through the corpus"]
    if report is None or runner_exit not in (0, 1):
        lines.append("- report: none" if report is None else "- report: written by a run that did not finish")
        return lines, [f"conformance: the runner did not complete a run (exit {runner_exit})"]
    cases = {case["id"]: case for case in report["cases"]}
    if not cases:
        return lines, [f"conformance: the report holds no case"]
    if case_dir is not None:
        # The pinned corpus requires the file stem to equal its declared case ID.
        expected = Counter(path.stem for path in Path(case_dir).rglob("*.toml"))
        if not expected or any(count != 1 for count in expected.values()):
            raise InputError(f"{case_dir}: empty or ambiguous pinned case inventory")
        missing = sorted(expected.keys() - cases.keys())
        extra = sorted(cases.keys() - expected.keys())
        if missing or extra:
            return lines, [f"conformance: report differs from pinned cases: missing {missing}, unexpected {extra}"]
        if report.get("profile") != "rustfs":
            return lines, ["conformance: expected the rustfs profile"]
    by_verdict = {verdict: sorted(i for i, c in cases.items() if c["verdict"] == verdict) for verdict in VERDICTS}
    lines.append(f"- target_build: {report.get('target_build') or 'not reported by the endpoint'}, profile `{report.get('profile')}`")
    skip_reasons = {}
    for case in by_verdict["skipped"]:
        # Grouped with the case-specific names (`c-acl-0002`, a bucket) elided.
        reason = re.sub(r"`[^`]*`", "`…`", cases[case].get("reason") or "no reason given")
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
    table = ["", "| skipped | reason |", "| ---: | --- |"] + [
        f"| {count} | {reason.replace('|', '/')} |" for reason, count in sorted(skip_reasons.items(), key=lambda item: -item[1])]
    executed = len(by_verdict["passed"]) + len(by_verdict["failed"])
    failed = by_verdict["failed"]
    unruled = [case for case in failed if case not in rulings]
    different = [case for case in failed if case in rulings
                 and cases[case].get("failures") != [rulings[case]["failure"]]]
    stale = sorted(case for case in rulings if cases.get(case, {}).get("verdict") != "failed")
    server_skips, runner_controls = [], []
    for case in by_verdict["skipped"]:
        reason = cases[case].get("reason") or ""
        if RUNNER_CONTROL.fullmatch(reason):
            runner_controls.append(case)
        elif EXTERNAL_SKIP.match(reason):
            server_skips.append(case)
    not_served = [case for case in failed if any(NOT_SERVED in str(f) for f in cases[case].get("failures", []))]
    lines.append(
        f"- cases: {len(cases)}; ran {executed}, passed {len(by_verdict['passed'])}, failed {len(failed)} "
        f"(ruled {len(failed) - len(unruled) - len(different)}, unruled {len(unruled)}, failing differently {len(different)}), "
        f"answered 501 {len(not_served)}, skipped {len(by_verdict['skipped'])} (on the server's answer {len(server_skips)})")
    sections = [
        ("Failed without a ruling", unruled, lambda case: first_failure(cases[case])),
        ("Failing differently from the ruling", different, lambda case: first_failure(cases[case])),
        ("Skipped for unavailable runner controls", runner_controls, lambda case: cases[case].get("reason")),
        ("Skipped on the server's answer", server_skips, lambda case: cases[case].get("reason")),
        ("Stale rulings: the case no longer fails; remove the ruling", stale,
         lambda case: cases[case]["verdict"] if case in cases else "not in the report"),
    ]
    for title, members, detail in sections:
        if members:
            lines += ["", f"#### {title} ({len(members)})", ""] + [f"- `{case}`: {detail(case)}" for case in members]
    problems = [f"conformance: {case} failed without a ruling" for case in unruled]
    problems += [f"conformance: {case} fails differently from its ruling" for case in different]
    problems += [f"conformance: {case} requires an unavailable runner control" for case in runner_controls]
    problems += [f"conformance: {case} was skipped on the server's answer" for case in server_skips]
    problems += [f"conformance: the ruling on {case} is stale" for case in stale]
    return lines + (table if skip_reasons else []), problems


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--case-dir", type=Path, help="the cases directory of the pinned gateway corpus")
    parser.add_argument("--rulings", default=str(LEDGER))
    args = parser.parse_args(argv)
    try:
        lines, problems = judge(load_rulings(args.rulings), args.run_dir, args.case_dir)
    except InputError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print("\n".join(lines))
    for problem in problems:
        print(f"::error::{problem}", file=sys.stderr)
    return 1 if problems else 0


class SelfTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.dir = Path(temporary.name)

    def write_run(self, cases, startup="ready", runner_exit="1", alive="alive"):
        write_served(self.dir, startup)
        if runner_exit is not None:
            (self.dir / "runner-exit.txt").write_text(runner_exit + "\n")
        if alive is not None:
            (self.dir / "alive-after-conformance.txt").write_text(alive + "\n")
        if cases is not None:
            report = {"profile": "rustfs", "target_build": None, "cases": [
                {"id": i, "verdict": v, "reason": f if v == "skipped" else "", "failures": [f] if f and v == "failed" else []}
                for i, v, f in cases]}
            (self.dir / "report.json").write_text(json.dumps(report))

    def ledger(self, *cases, failure="[expect/status]"):
        path = self.dir / "rulings.toml"
        expiry = datetime.now(UTC).date() + timedelta(days=7)
        path.write_text("".join(
            f'[[ruling]]\ncase = "{c}"\nledger = "R-001"\nreason = "why"\nfailure = "{failure}"\nexpires = {expiry}\n' for c in cases))
        return load_rulings(path)

    def judged(self, rulings):
        return judge(rulings, self.dir)

    def test_every_failure_ruled_and_every_ruling_failing_is_clean(self):
        self.write_run([("c-a-0001", "passed", ""), ("c-a-0002", "failed", "[expect/status] at /expect/status: observed 403")])
        lines, problems = self.judged(self.ledger("c-a-0002", failure="[expect/status] at /expect/status: observed 403"))
        self.assertEqual(problems, [])
        self.assertIn("ran 2, passed 1, failed 1 (ruled 1, unruled 0, failing differently 0)", "\n".join(lines))
        self.assertIn("- server: ready", lines)
        self.assertIn("- target_build: not reported by the endpoint, profile `rustfs`", lines)

    def test_an_unruled_failure_is_red(self):
        self.write_run([("c-a-0001", "failed", "[expect/status]: x")])
        self.assertEqual(self.judged({})[1], ["conformance: c-a-0001 failed without a ruling"])

    def test_a_ruled_case_failing_differently_is_red(self):
        self.write_run([("c-a-0001", "failed", "[expect/status] at /expect/status: expected status 200, observed 500")])
        self.assertEqual(self.judged(self.ledger("c-a-0001", failure="[expect/error.code]"))[1],
                         ["conformance: c-a-0001 fails differently from its ruling"])

    def test_a_second_failure_or_changed_detail_is_not_ruled(self):
        self.write_run([("c-a-0001", "failed", "missing region")])
        report_path = self.dir / "report.json"
        report = json.loads(report_path.read_text())
        ruling = self.ledger("c-a-0001", failure="missing region")
        for failures in (["missing region", "wrong status"], ["missing region on another exchange"]):
            report["cases"][0]["failures"] = failures
            report_path.write_text(json.dumps(report))
            with self.subTest(failures=failures):
                self.assertEqual(self.judged(ruling)[1], ["conformance: c-a-0001 fails differently from its ruling"])

    def test_a_skip_on_the_endpoint_not_answering_is_red(self):
        self.write_run([("c-a-0001", "passed", ""),
                        ("c-a-0002", "skipped", "environment: external endpoint 127.0.0.1:9000 could not connect: refused")])
        self.assertEqual(self.judged({})[1], ["conformance: c-a-0002 was skipped on the server's answer"])

    def test_unimplemented_runner_controls_stay_red_and_are_attributed_to_the_runner(self):
        for action in ("close", "half_close", "stall"):
            with self.subTest(action=action):
                reason = f"environment: external endpoint `{action}` control is not implemented with concurrent response observation"
                self.write_run([("c-a-0001", "skipped", reason)])
                lines, problems = self.judged({})
                self.assertEqual(problems, ["conformance: c-a-0001 requires an unavailable runner control"])
                self.assertIn("Skipped for unavailable runner controls (1)", "\n".join(lines))
                self.assertIn("on the server's answer 0", "\n".join(lines))
                self.assertIn(reason, "\n".join(lines))

    def test_a_skip_on_a_refused_fixture_is_red(self):
        self.write_run([("c-a-0001", "skipped", "environment: external fixture PUT for object `b/k` returned status 400; the failed create is not owned")])
        self.assertEqual(self.judged({})[1], ["conformance: c-a-0001 was skipped on the server's answer"])

    def test_a_corpus_limit_skip_is_counted_not_red(self):
        self.write_run([("c-a-0001", "skipped", "environment: `clock.fixed` pins the instant the target observes")])
        lines, problems = self.judged({})
        self.assertEqual(problems, [])
        self.assertIn("skipped 1 (on the server's answer 0)", "\n".join(lines))

    def test_a_server_that_died_during_the_corpus_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="0", alive="exited")
        self.assertEqual(self.judged({})[1], ["conformance: the server did not stay up through the corpus"])

    def test_a_missing_liveness_record_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="0", alive=None)
        self.assertEqual(self.judged({})[1], ["conformance: the server did not stay up through the corpus"])

    def test_a_ruling_on_a_passing_case_is_stale(self):
        self.write_run([("c-a-0001", "passed", "")])
        self.assertEqual(self.judged(self.ledger("c-a-0001"))[1], ["conformance: the ruling on c-a-0001 is stale"])

    def test_a_ruling_on_a_skipped_or_absent_case_is_stale(self):
        self.write_run([("c-a-0001", "skipped", "case.applies_to.profiles is [minio]")])
        self.assertEqual(self.judged(self.ledger("c-a-0001", "c-gone-0001"))[1],
                         ["conformance: the ruling on c-a-0001 is stale", "conformance: the ruling on c-gone-0001 is stale"])

    def test_a_server_that_did_not_start_is_red_and_says_why(self):
        self.write_run(None, startup="did not start: refused", runner_exit=None, alive=None)
        lines, problems = self.judged({})
        self.assertIn("- server: did not start: refused", lines)
        self.assertEqual(problems, ["conformance: the server did not serve; no case ran"])

    def test_a_missing_startup_record_is_red(self):
        self.assertEqual(self.judged({})[1], ["conformance: the server did not serve; no case ran"])

    def test_a_missing_report_is_red_even_when_the_exit_was_swallowed(self):
        self.write_run(None, runner_exit="0")
        self.assertEqual(self.judged({})[1], ["conformance: the runner did not complete a run (exit 0)"])

    def test_a_swallowed_exit_cannot_hide_a_failed_case(self):
        self.write_run([("c-a-0001", "failed", "x")], runner_exit="0")
        self.assertEqual(self.judged({})[1], ["conformance: c-a-0001 failed without a ruling"])

    def test_an_environment_exit_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="3")
        self.assertEqual(self.judged({})[1], ["conformance: the runner did not complete a run (exit 3)"])

    def test_an_empty_report_is_red(self):
        self.write_run([], runner_exit="0")
        self.assertEqual(self.judged({})[1], ["conformance: the report holds no case"])

    def test_answers_of_501_are_counted(self):
        self.write_run([("c-a-0001", "failed", "[expect/status]: expected status 200, observed 501")])
        self.assertIn("answered 501 1", "\n".join(self.judged({})[0]))

    def test_pinned_case_inventory_requires_every_id_exactly_once(self):
        pinned = self.dir / "cases" / "bkt"
        pinned.mkdir(parents=True)
        for name in ("c-bkt-0001", "c-bkt-0002"):
            (pinned / f"{name}.toml").touch()
        a = ("c-bkt-0001", "passed", "")
        b = ("c-bkt-0002", "skipped", "unsupported fixture")
        self.write_run([a, b])
        self.assertEqual(judge({}, self.dir, pinned.parent)[1], [])
        for rows in ([a], [a, b, ("c-bkt-0003", "passed", "")]):
            self.write_run(rows)
            self.assertTrue(judge({}, self.dir, pinned.parent)[1])
        self.write_run([a, a, b])
        with self.assertRaisesRegex(InputError, "repeated case IDs"):
            judge({}, self.dir, pinned.parent)

    def test_an_unknown_verdict_is_refused(self):
        self.write_run([("c-a-0001", "validated", "")])
        with self.assertRaises(InputError):
            read_report(self.dir)

    def test_malformed_ledgers_are_refused(self):
        path = self.dir / "rulings.toml"
        base = 'case = "c"\nledger = "R-001"\nreason = "r"\nfailure = "[expect/status]"\n'
        today = datetime.now(UTC).date()
        dated = base + f"expires = {today + timedelta(days=1)}\n"
        for body in (
            base,
            base + f"expires = {today}\n",
            base + f"expires = {today + timedelta(days=91)}\n",
            'case = "c"\nledger = "R-001"\nreason = "r"\n',
            base + 'expires = "x"\n',
            dated + 'stack = "gateway"\n',
            dated.replace('"R-001"', '"2684"'),
            dated.replace('reason = "r"', 'reason = ""'),
            dated.replace('failure = "[expect/status]"', 'failure = ""'),
        ):
            path.write_text("[[ruling]]\n" + body)
            with self.subTest(body=body), self.assertRaises(InputError):
                load_rulings(path)
        path.write_text(("[[ruling]]\n" + dated) * 2)
        with self.assertRaises(InputError):
            load_rulings(path)

    def test_a_malformed_report_or_exit_record_is_refused(self):
        (self.dir / "report.json").write_text('{"cases": [{"id": "c"}]}')
        with self.assertRaises(InputError):
            read_report(self.dir)
        (self.dir / "runner-exit.txt").write_text("zero\n")
        with self.assertRaises(InputError):
            read_exit(self.dir / "runner-exit.txt")

    def test_the_committed_ledger_is_well_formed(self):
        load_rulings(LEDGER)


if __name__ == "__main__":
    sys.exit(main())
