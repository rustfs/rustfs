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
"""Judge the gateway stack's conformance run against .config/conformance-rulings.toml.

Usage:
  check_conformance_rulings.py --stack <legacy|gateway> --run-dir <dir> [--rulings <toml>]
  check_conformance_rulings.py --diff <legacy dir> <gateway dir>
  check_conformance_rulings.py --self-test

<dir> holds what the lane observed: the server record acceptance_lane.py reads,
`runner-exit.txt` (the conformance runner's exit code), `alive-after-conformance.txt`
(`alive` when the server still ran after the corpus) and `report.json` (the
runner's `--json` report). The verdict comes from those observations, never
from the exit status of an earlier step.

The `rustfs` profile describes the gateway edge, so only the gateway stack is
judged. Red there: a failed case without a ruling; a ruled case that fails
differently from its ruling's `failure`; a ruling whose case no longer fails;
and a skip whose reason is the server's own answer (the endpoint not answering,
or a fixture it refused), which is a failure the runner records as a skip. The
legacy run is a reference: it must serve, stay up and complete, and its cases
are counted, never judged. `--diff` prints, as information, every case whose
verdict differs between the two reports.

Exit codes: 0 judged clean (legacy: the run completed; `--diff`: always); 1
otherwise; 2 a malformed ledger, report or observation. The Markdown summary
goes to stdout.
"""

import argparse
import json
from pathlib import Path
import re
import sys
import tempfile
import tomllib
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from acceptance_lane import InputError, read_exit, read_text, served, write_served  # noqa: E402  (sibling script)
import junit_diff  # noqa: E402  (sibling script, not a package)

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / ".config/conformance-rulings.toml"
STACKS = ("legacy", "gateway")
VERDICTS = ("passed", "failed", "skipped")
# rustfs/backlog#2684 names its entries R-0xx (gateway ledger rows) and K-n (kept behaviour).
LEDGER_ID = re.compile(r"(R-\d{3}|K-\d+)")
RULING_KEYS = {"case", "ledger", "reason", "failure"}
NOT_SERVED = "observed 501"
# Skip reasons of the pinned runner that are the server's answer, not a corpus limit.
SERVER_SKIP = re.compile(r"environment: external (endpoint|fixture .* returned status)")


def load_rulings(path):
    try:
        rows = tomllib.loads(Path(path).read_text(encoding="utf-8")).get("ruling", [])
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise InputError(f"{path}: {error}") from error
    rulings = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != RULING_KEYS:
            raise InputError(f"{path}: a ruling has exactly the keys {sorted(RULING_KEYS)}: {row}")
        if not LEDGER_ID.fullmatch(str(row["ledger"])):
            raise InputError(f"{path}: unknown ledger id in {row}")
        for key in ("reason", "failure"):
            if not isinstance(row[key], str) or not row[key].strip() or "\n" in row[key]:
                raise InputError(f"{path}: the {key} of {row['case']} must be one non-empty line")
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
    unknown = sorted({case["verdict"] for case in cases} - set(VERDICTS))
    if unknown:
        raise InputError(f"{path}: unknown verdicts {unknown}")
    return report


def first_failure(case):
    return (case.get("failures") or ["no failure recorded"])[0]


def judge(stack, rulings, run_dir):
    """Return (summary lines, problems). Rulings apply to the gateway stack only."""
    server, problem = served(run_dir, stack)
    lines = [f"### Conformance, `{stack}` stack", "", server]
    if problem:
        return lines, [problem + "; no case ran"]
    runner_exit, report = read_exit(Path(run_dir) / "runner-exit.txt"), read_report(run_dir)
    alive = read_text(Path(run_dir) / "alive-after-conformance.txt")
    lines += [f"- runner exit: {runner_exit if runner_exit is not None else 'not recorded'}",
              f"- server after the corpus: {alive or 'not recorded'}"]
    if alive != "alive":
        return lines, [f"{stack}: the server did not stay up through the corpus"]
    if report is None or runner_exit not in (0, 1):
        lines.append("- report: none" if report is None else "- report: written by a run that did not finish")
        return lines, [f"{stack}: the runner did not complete a run (exit {runner_exit})"]
    cases = {case["id"]: case for case in report["cases"]}
    if not cases:
        return lines, [f"{stack}: the report holds no case"]
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
    if stack == "legacy":
        lines += [f"- cases: {len(cases)}; executed {executed}, skipped {len(by_verdict['skipped'])}",
                  "- not judged: reference only (the `rustfs` profile describes the gateway edge)"]
        return lines + (table if skip_reasons else []), []
    failed = by_verdict["failed"]
    unruled = [case for case in failed if case not in rulings]
    different = [case for case in failed if case in rulings
                 and not any(str(f).startswith(rulings[case]["failure"]) for f in cases[case].get("failures", []))]
    stale = sorted(case for case in rulings if cases.get(case, {}).get("verdict") != "failed")
    server_skips = [case for case in by_verdict["skipped"] if SERVER_SKIP.match(cases[case].get("reason") or "")]
    not_served = [case for case in failed if any(NOT_SERVED in str(f) for f in cases[case].get("failures", []))]
    lines.append(
        f"- cases: {len(cases)}; ran {executed}, passed {len(by_verdict['passed'])}, failed {len(failed)} "
        f"(ruled {len(failed) - len(unruled) - len(different)}, unruled {len(unruled)}, failing differently {len(different)}), "
        f"answered 501 {len(not_served)}, skipped {len(by_verdict['skipped'])} (on the server's answer {len(server_skips)})")
    sections = [
        ("Failed without a ruling", unruled, lambda case: first_failure(cases[case])),
        ("Failing differently from the ruling", different, lambda case: first_failure(cases[case])),
        ("Skipped on the server's answer", server_skips, lambda case: cases[case].get("reason")),
        ("Stale rulings: the case no longer fails; remove the ruling", stale,
         lambda case: cases[case]["verdict"] if case in cases else "not in the report"),
    ]
    for title, members, detail in sections:
        if members:
            lines += ["", f"#### {title} ({len(members)})", ""] + [f"- `{case}`: {detail(case)}" for case in members]
    problems = [f"{stack}: {case} failed without a ruling" for case in unruled]
    problems += [f"{stack}: {case} fails differently from its ruling" for case in different]
    problems += [f"{stack}: {case} was skipped on the server's answer" for case in server_skips]
    problems += [f"{stack}: the ruling on {case} is stale" for case in stale]
    return lines + (table if skip_reasons else []), problems


def diff(legacy, gateway):
    """Return the summary lines comparing two reports; information only."""
    lines = ["### Conformance: legacy vs gateway (information, not judged)", ""]
    if legacy is None or gateway is None:
        missing = [name for name, report in (("legacy", legacy), ("gateway", gateway)) if report is None]
        return lines + [f"- no report from {', '.join(missing)}; nothing to compare"]
    verdicts = lambda report: {case["id"]: case["verdict"] for case in report["cases"]}
    differing, _ = junit_diff.compare(verdicts(legacy), verdicts(gateway), set())
    if not differing:
        return lines + ["- no case differs"]
    lines += [f"{len(differing)} cases differ.", "", "| case | legacy | gateway |", "| --- | --- | --- |"]
    return lines + [f"| {d['case']} | {d['a']} | {d['b']} |" for d in differing]


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--stack", choices=STACKS)
    parser.add_argument("--run-dir")
    parser.add_argument("--rulings", default=str(LEDGER))
    parser.add_argument("--diff", nargs=2, metavar=("LEGACY", "GATEWAY"))
    args = parser.parse_args(argv)
    if bool(args.diff) == bool(args.stack and args.run_dir):
        parser.error("give either --stack and --run-dir, or --diff")
    try:
        if args.diff:
            lines, problems = diff(read_report(args.diff[0]), read_report(args.diff[1])), []
        else:
            lines, problems = judge(args.stack, load_rulings(args.rulings), args.run_dir)
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

    def write_run(self, cases, stack="gateway", startup="ready", runner_exit="1", alive="alive", observed=None):
        write_served(self.dir, stack, startup, observed)
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
        path.write_text("".join(
            f'[[ruling]]\ncase = "{c}"\nledger = "R-001"\nreason = "why"\nfailure = "{failure}"\n' for c in cases))
        return load_rulings(path)

    def judged(self, stack, rulings):
        return judge(stack, rulings, self.dir)

    def test_every_failure_ruled_and_every_ruling_failing_is_clean(self):
        self.write_run([("c-a-0001", "passed", ""), ("c-a-0002", "failed", "[expect/status] at /expect/status: observed 403")])
        lines, problems = self.judged("gateway", self.ledger("c-a-0002"))
        self.assertEqual(problems, [])
        self.assertIn("ran 2, passed 1, failed 1 (ruled 1, unruled 0, failing differently 0)", "\n".join(lines))
        self.assertIn("- server: ready; stack observed `gateway` (`server_starting` log event)", lines)
        self.assertIn("- target_build: not reported by the endpoint, profile `rustfs`", lines)

    def test_an_unruled_failure_is_red(self):
        self.write_run([("c-a-0001", "failed", "[expect/status]: x")])
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: c-a-0001 failed without a ruling"])

    def test_a_ruled_case_failing_differently_is_red(self):
        self.write_run([("c-a-0001", "failed", "[expect/status] at /expect/status: expected status 200, observed 500")])
        self.assertEqual(self.judged("gateway", self.ledger("c-a-0001", failure="[expect/error.code]"))[1],
                         ["gateway: c-a-0001 fails differently from its ruling"])

    def test_a_skip_on_the_endpoint_not_answering_is_red(self):
        self.write_run([("c-a-0001", "passed", ""),
                        ("c-a-0002", "skipped", "environment: external endpoint 127.0.0.1:9000 could not connect: refused")])
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: c-a-0002 was skipped on the server's answer"])

    def test_a_skip_on_a_refused_fixture_is_red(self):
        self.write_run([("c-a-0001", "skipped", "environment: external fixture PUT for object `b/k` returned status 400; the failed create is not owned")])
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: c-a-0001 was skipped on the server's answer"])

    def test_a_corpus_limit_skip_is_counted_not_red(self):
        self.write_run([("c-a-0001", "skipped", "environment: `clock.fixed` pins the instant the target observes")])
        lines, problems = self.judged("gateway", {})
        self.assertEqual(problems, [])
        self.assertIn("skipped 1 (on the server's answer 0)", "\n".join(lines))

    def test_the_legacy_reference_is_counted_never_judged(self):
        self.write_run([("c-a-0001", "failed", "x"), ("c-a-0002", "passed", ""),
                        ("c-a-0003", "skipped", "environment: external endpoint did not answer")], stack="legacy")
        lines, problems = self.judged("legacy", self.ledger("c-a-0002"))
        self.assertEqual(problems, [])
        self.assertIn("- cases: 3; executed 2, skipped 1", lines)
        self.assertIn("- not judged: reference only (the `rustfs` profile describes the gateway edge)", lines)
        self.assertNotIn("passed", "\n".join(lines))

    def test_a_legacy_reference_that_did_not_complete_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], stack="legacy", runner_exit="3")
        self.assertEqual(self.judged("legacy", {})[1], ["legacy: the runner did not complete a run (exit 3)"])

    def test_a_server_that_died_during_the_corpus_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="0", alive="exited")
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: the server did not stay up through the corpus"])

    def test_a_missing_liveness_record_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], stack="legacy", runner_exit="0", alive=None)
        self.assertEqual(self.judged("legacy", {})[1], ["legacy: the server did not stay up through the corpus"])

    def test_a_ruling_on_a_passing_case_is_stale(self):
        self.write_run([("c-a-0001", "passed", "")])
        self.assertEqual(self.judged("gateway", self.ledger("c-a-0001"))[1], ["gateway: the ruling on c-a-0001 is stale"])

    def test_a_ruling_on_a_skipped_or_absent_case_is_stale(self):
        self.write_run([("c-a-0001", "skipped", "case.applies_to.profiles is [minio]")])
        self.assertEqual(self.judged("gateway", self.ledger("c-a-0001", "c-gone-0001"))[1],
                         ["gateway: the ruling on c-a-0001 is stale", "gateway: the ruling on c-gone-0001 is stale"])

    def test_a_server_that_did_not_start_is_red_and_says_why(self):
        self.write_run(None, startup="did not start: gateway stack is not assembled yet", runner_exit=None, alive=None)
        lines, problems = self.judged("gateway", {})
        self.assertIn("- server: did not start: gateway stack is not assembled yet", lines)
        self.assertEqual(problems, ["gateway: the server did not serve; no case ran"])

    def test_a_missing_startup_record_is_red(self):
        self.assertEqual(self.judged("legacy", {})[1], ["legacy: the server did not serve; no case ran"])

    def test_a_server_of_the_other_stack_is_red_and_the_observed_stack_is_printed(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="0", observed="legacy")
        lines, problems = self.judged("gateway", {})
        self.assertIn("- server: ready, but the stack was not observed as `gateway` (observed: legacy)", lines)
        self.assertEqual(problems, ["gateway: the stack that served was not observed; no case ran"])

    def test_ready_without_stack_evidence_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="0")
        (self.dir / "stack.json").unlink()
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: the stack that served was not observed; no case ran"])

    def test_a_missing_report_is_red_even_when_the_exit_was_swallowed(self):
        self.write_run(None, runner_exit="0")
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: the runner did not complete a run (exit 0)"])

    def test_a_swallowed_exit_cannot_hide_a_failed_case(self):
        self.write_run([("c-a-0001", "failed", "x")], runner_exit="0")
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: c-a-0001 failed without a ruling"])

    def test_an_environment_exit_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="3")
        self.assertEqual(self.judged("gateway", {})[1], ["gateway: the runner did not complete a run (exit 3)"])

    def test_an_empty_report_is_red(self):
        self.write_run([], stack="legacy", runner_exit="0")
        self.assertEqual(self.judged("legacy", {})[1], ["legacy: the report holds no case"])

    def test_answers_of_501_are_counted(self):
        self.write_run([("c-a-0001", "failed", "[expect/status]: expected status 200, observed 501")])
        self.assertIn("answered 501 1", "\n".join(self.judged("gateway", {})[0]))

    def test_an_unknown_verdict_is_refused(self):
        self.write_run([("c-a-0001", "validated", "")])
        with self.assertRaises(InputError):
            read_report(self.dir)

    def test_malformed_ledgers_are_refused(self):
        path = self.dir / "rulings.toml"
        base = 'case = "c"\nledger = "R-001"\nreason = "r"\nfailure = "[expect/status]"\n'
        for body in (
            'case = "c"\nledger = "R-001"\nreason = "r"\n',
            base + 'expires = "x"\n',
            base + 'stack = "gateway"\n',
            base.replace('"R-001"', '"2684"'),
            base.replace('reason = "r"', 'reason = ""'),
            base.replace('failure = "[expect/status]"', 'failure = ""'),
        ):
            path.write_text("[[ruling]]\n" + body)
            with self.subTest(body=body), self.assertRaises(InputError):
                load_rulings(path)
        path.write_text(("[[ruling]]\n" + base) * 2)
        with self.assertRaises(InputError):
            load_rulings(path)

    def test_a_malformed_report_or_exit_record_is_refused(self):
        (self.dir / "report.json").write_text('{"cases": [{"id": "c"}]}')
        with self.assertRaises(InputError):
            read_report(self.dir)
        (self.dir / "runner-exit.txt").write_text("zero\n")
        with self.assertRaises(InputError):
            read_exit(self.dir / "runner-exit.txt")

    def test_diff_lists_changed_and_absent_cases_only(self):
        legacy = {"cases": [{"id": "a", "verdict": "passed"}, {"id": "b", "verdict": "passed"}, {"id": "c", "verdict": "failed"}]}
        gateway = {"cases": [{"id": "a", "verdict": "passed"}, {"id": "b", "verdict": "failed"}]}
        lines = diff(legacy, gateway)
        self.assertIn("| b | passed | failed |", lines)
        self.assertIn("| c | failed | missing |", lines)
        self.assertNotIn("| a |", "\n".join(lines))

    def test_diff_without_both_reports_compares_nothing(self):
        self.assertIn("- no report from gateway; nothing to compare", diff({"cases": []}, None))

    def test_the_committed_ledger_is_well_formed(self):
        load_rulings(LEDGER)


if __name__ == "__main__":
    sys.exit(main())
