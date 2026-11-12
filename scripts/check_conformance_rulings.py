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

<dir> holds what the lane observed: `startup.txt` (`ready`, or the reason the
server did not start), `runner-exit.txt` (the conformance runner's exit code)
and `report.json` (the runner's `--json` report). The verdict comes from those
observations, never from the exit status of an earlier step.

The `rustfs` profile describes the gateway edge, so only the gateway stack is
judged: a failed case without a ruling and a ruling whose case no longer fails
are both red. The legacy run is a reference: it must start and complete, and
its cases are reported, never judged. `--diff` prints every case whose verdict
differs between the two reports, as information.

Exit codes: 0 judged clean (legacy: the run completed; `--diff`: always); 1 an
unruled failure, a stale ruling, or a run that did not complete; 2 a malformed
ledger or report. The Markdown summary goes to stdout.
"""

import argparse
import json
from pathlib import Path
import re
import sys
import tomllib
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import junit_diff  # noqa: E402  (sibling script, not a package)

ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / ".config/conformance-rulings.toml"
STACKS = ("legacy", "gateway")
# rustfs/backlog#2684 names its entries R-0xx (gateway ledger rows) and K-n (kept behaviour).
LEDGER_ID = re.compile(r"(R-\d{3}|K-\d+)")
RULING_KEYS = {"case", "ledger", "reason"}
NOT_SERVED = "observed 501"


class InputError(Exception):
    """A ledger or report that cannot be trusted; exit code 2."""


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
        reason = row["reason"]
        if not isinstance(reason, str) or not reason.strip() or "\n" in reason:
            raise InputError(f"{path}: the reason of {row['case']} must be one non-empty line")
        if row["case"] in rulings:
            raise InputError(f"{path}: {row['case']} is ruled twice")
        rulings[row["case"]] = row
    return rulings


def read_run(run_dir):
    """Return (startup, runner exit or None, report or None) as observed in run_dir."""
    run_dir = Path(run_dir)
    startup_file = run_dir / "startup.txt"
    startup = startup_file.read_text(encoding="utf-8").strip() if startup_file.is_file() else ""
    exit_file = run_dir / "runner-exit.txt"
    runner_exit = None
    if exit_file.is_file():
        text = exit_file.read_text(encoding="utf-8").strip()
        if not text.isdigit():
            raise InputError(f"{exit_file}: not an exit code: {text!r}")
        runner_exit = int(text)
    report_file = run_dir / "report.json"
    report = None
    if report_file.is_file():
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
            cases = report["cases"]
            if not isinstance(cases, list) or any(not {"id", "verdict"} <= set(case) for case in cases):
                raise KeyError("cases")
        except (json.JSONDecodeError, KeyError, TypeError) as error:
            raise InputError(f"{report_file}: not a conformance report: {error}") from error
    return startup, runner_exit, report


def judge(stack, rulings, startup, runner_exit, report):
    """Return (summary lines, problems). Rulings apply to the gateway stack only."""
    lines = [f"### Conformance, `{stack}` stack", ""]
    if startup != "ready":
        observed = startup or "no startup record"
        lines.append(f"- server: {observed if startup.startswith('did not start') else 'did not start: ' + observed}")
        return lines, [f"{stack}: the server did not serve; no case ran"]
    lines.append("- server: ready")
    lines.append(f"- runner exit: {runner_exit if runner_exit is not None else 'not recorded'}")
    if report is None or runner_exit not in (0, 1):
        lines.append("- report: none" if report is None else "- report: written by a run that did not finish")
        return lines, [f"{stack}: the runner did not complete a run (exit {runner_exit})"]
    cases = {case["id"]: case for case in report["cases"]}
    by_verdict = {verdict: sorted(i for i, c in cases.items() if c["verdict"] == verdict) for verdict in ("passed", "failed", "skipped")}
    failed = by_verdict["failed"]
    reference = stack == "legacy"
    unruled = [] if reference else [case for case in failed if case not in rulings]
    stale = [] if reference else sorted(case for case in rulings if cases.get(case, {}).get("verdict") != "failed")
    not_served = [case for case in failed if any(NOT_SERVED in str(f) for f in cases[case].get("failures", []))]
    lines += [
        f"- target_build: `{report.get('target_build')}`, profile `{report.get('profile')}`",
        f"- cases: {len(cases)}; ran {len(failed) + len(by_verdict['passed'])}, passed {len(by_verdict['passed'])}, "
        f"failed {len(failed)}" + ("" if reference else f" (ruled {len(failed) - len(unruled)}, unruled {len(unruled)})")
        + f", answered 501 {len(not_served)}, skipped {len(by_verdict['skipped'])}",
    ]
    if reference:
        lines.append("- not judged: reference only (the `rustfs` profile describes the gateway edge)")
    if not cases:
        return lines, [f"{stack}: the report holds no case"]
    skip_reasons = {}
    for case in by_verdict["skipped"]:
        # Grouped with the case-specific names (`c-acl-0002`, a bucket) elided.
        reason = re.sub(r"`[^`]*`", "`…`", cases[case].get("reason") or "no reason given")
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
    if skip_reasons:
        lines += ["", "| skipped | reason |", "| ---: | --- |"]
        lines += [f"| {count} | {reason.replace('|', '/')} |" for reason, count in sorted(skip_reasons.items(), key=lambda item: -item[1])]
    if unruled:
        lines += ["", f"#### Failed without a ruling ({len(unruled)})", ""]
        lines += [f"- `{case}`: {(cases[case].get('failures') or ['no failure recorded'])[0]}" for case in unruled]
    if stale:
        lines += ["", f"#### Stale rulings ({len(stale)}): the case no longer fails; remove the ruling", ""]
        lines += [f"- `{case}`: {cases[case]['verdict'] if case in cases else 'not in the report'}" for case in stale]
    problems = [f"{stack}: {case} failed without a ruling" for case in unruled]
    problems += [f"{stack}: the ruling on {case} is stale" for case in stale]
    return lines, problems


def diff(legacy, gateway):
    """Return (summary lines, differing cases) between two reports; None when one is missing."""
    lines = ["### Conformance: legacy vs gateway (information, not judged)", ""]
    if legacy is None or gateway is None:
        missing = [name for name, report in (("legacy", legacy), ("gateway", gateway)) if report is None]
        return lines + [f"- no report from {', '.join(missing)}; nothing to compare"], None
    verdicts = lambda report: {case["id"]: case["verdict"] for case in report["cases"]}
    differing, _ = junit_diff.compare(verdicts(legacy), verdicts(gateway), set())
    if not differing:
        return lines + ["- no case differs"], differing
    lines += [f"{len(differing)} cases differ.", "", "| case | legacy | gateway |", "| --- | --- | --- |"]
    lines += [f"| {d['case']} | {d['a']} | {d['b']} |" for d in differing]
    return lines, differing


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
            lines, problems = diff(read_run(args.diff[0])[2], read_run(args.diff[1])[2])[0], []
        else:
            lines, problems = judge(args.stack, load_rulings(args.rulings), *read_run(args.run_dir))
    except InputError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print("\n".join(lines))
    for problem in problems:
        print(f"::error::{problem}", file=sys.stderr)
    return 1 if problems else 0


class SelfTest(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.dir = Path(tempfile.mkdtemp())

    def write_run(self, cases, startup="ready", runner_exit="1"):
        (self.dir / "startup.txt").write_text(startup + "\n")
        if runner_exit is not None:
            (self.dir / "runner-exit.txt").write_text(runner_exit + "\n")
        if cases is not None:
            report = {"profile": "rustfs", "target_build": "RustFS", "cases": [
                {"id": i, "verdict": v, "reason": "", "failures": [f] if f else []} for i, v, f in cases]}
            (self.dir / "report.json").write_text(json.dumps(report))

    def ledger(self, *cases):
        path = self.dir / "rulings.toml"
        path.write_text("".join(f'[[ruling]]\ncase = "{c}"\nledger = "R-001"\nreason = "why"\n' for c in cases))
        return load_rulings(path)

    def run_judge(self, stack, rulings):
        return judge(stack, rulings, *read_run(self.dir))

    def test_every_failure_ruled_and_every_ruling_failing_is_clean(self):
        self.write_run([("c-a-0001", "passed", ""), ("c-a-0002", "failed", "[expect/status]: expected status 200, observed 403")])
        lines, problems = self.run_judge("gateway", self.ledger("c-a-0002"))
        self.assertEqual(problems, [])
        self.assertIn("ran 2, passed 1, failed 1 (ruled 1, unruled 0), answered 501 0", "\n".join(lines))

    def test_an_unruled_failure_is_red(self):
        self.write_run([("c-a-0001", "failed", "[expect/status]: x")])
        self.assertEqual(self.run_judge("gateway", {})[1], ["gateway: c-a-0001 failed without a ruling"])

    def test_the_legacy_reference_is_reported_never_judged(self):
        self.write_run([("c-a-0001", "failed", "x"), ("c-a-0002", "passed", "")])
        lines, problems = self.run_judge("legacy", self.ledger("c-a-0002"))
        self.assertEqual(problems, [])
        self.assertIn("- not judged: reference only (the `rustfs` profile describes the gateway edge)", lines)
        self.assertNotIn("unruled", "\n".join(lines))

    def test_a_legacy_reference_that_did_not_complete_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="3")
        self.assertEqual(self.run_judge("legacy", {})[1], ["legacy: the runner did not complete a run (exit 3)"])

    def test_a_ruling_on_a_passing_case_is_stale(self):
        self.write_run([("c-a-0001", "passed", "")])
        self.assertEqual(self.run_judge("gateway", self.ledger("c-a-0001"))[1],
                         ["gateway: the ruling on c-a-0001 is stale"])

    def test_a_ruling_on_a_skipped_or_absent_case_is_stale(self):
        self.write_run([("c-a-0001", "skipped", "")])
        problems = self.run_judge("gateway", self.ledger("c-a-0001", "c-gone-0001"))[1]
        self.assertEqual(problems, ["gateway: the ruling on c-a-0001 is stale", "gateway: the ruling on c-gone-0001 is stale"])

    def test_a_server_that_did_not_start_is_red_and_says_why(self):
        self.write_run(None, startup="did not start: gateway stack is not assembled yet", runner_exit=None)
        lines, problems = self.run_judge("gateway", {})
        self.assertIn("- server: did not start: gateway stack is not assembled yet", lines)
        self.assertEqual(problems, ["gateway: the server did not serve; no case ran"])

    def test_a_missing_startup_record_is_red(self):
        self.assertEqual(self.run_judge("legacy", {})[1], ["legacy: the server did not serve; no case ran"])

    def test_a_missing_report_is_red_even_when_the_exit_was_swallowed(self):
        self.write_run(None, runner_exit="0")
        self.assertEqual(self.run_judge("gateway", {})[1], ["gateway: the runner did not complete a run (exit 0)"])

    def test_a_swallowed_exit_cannot_hide_a_failed_case(self):
        self.write_run([("c-a-0001", "failed", "x")], runner_exit="0")
        self.assertEqual(self.run_judge("gateway", {})[1], ["gateway: c-a-0001 failed without a ruling"])

    def test_an_environment_exit_is_red(self):
        self.write_run([("c-a-0001", "passed", "")], runner_exit="3")
        self.assertEqual(self.run_judge("gateway", {})[1], ["gateway: the runner did not complete a run (exit 3)"])

    def test_an_empty_report_is_red(self):
        self.write_run([], runner_exit="0")
        self.assertEqual(self.run_judge("legacy", {})[1], ["legacy: the report holds no case"])

    def test_answers_of_501_are_counted(self):
        self.write_run([("c-a-0001", "failed", "[expect/status]: expected status 200, observed 501")])
        self.assertIn("answered 501 1", "\n".join(self.run_judge("gateway", {})[0]))

    def test_malformed_ledgers_are_refused(self):
        path = self.dir / "rulings.toml"
        for body in (
            '[[ruling]]\ncase = "c"\nledger = "R-001"\n',
            '[[ruling]]\ncase = "c"\nledger = "R-001"\nreason = "r"\nexpires = "x"\n',
            '[[ruling]]\nstack = "gateway"\ncase = "c"\nledger = "R-001"\nreason = "r"\n',
            '[[ruling]]\ncase = "c"\nledger = "2684"\nreason = "r"\n',
            '[[ruling]]\ncase = "c"\nledger = "K-1"\nreason = ""\n',
            '[[ruling]]\ncase = "c"\nledger = "K-1"\nreason = "r"\n' * 2,
        ):
            path.write_text(body)
            with self.subTest(body=body), self.assertRaises(InputError):
                load_rulings(path)

    def test_a_malformed_report_is_refused(self):
        (self.dir / "report.json").write_text('{"cases": [{"id": "c"}]}')
        with self.assertRaises(InputError):
            read_run(self.dir)

    def test_diff_lists_changed_and_absent_cases_only(self):
        legacy = {"cases": [{"id": "a", "verdict": "passed"}, {"id": "b", "verdict": "passed"}, {"id": "c", "verdict": "failed"}]}
        gateway = {"cases": [{"id": "a", "verdict": "passed"}, {"id": "b", "verdict": "failed"}]}
        self.assertEqual([(d["case"], d["a"], d["b"]) for d in diff(legacy, gateway)[1]],
                         [("b", "passed", "failed"), ("c", "failed", "missing")])

    def test_diff_without_both_reports_compares_nothing(self):
        lines, differing = diff({"cases": []}, None)
        self.assertIsNone(differing)
        self.assertIn("- no report from gateway; nothing to compare", lines)

    def test_the_committed_ledger_is_well_formed(self):
        load_rulings(LEDGER)


if __name__ == "__main__":
    sys.exit(main())
