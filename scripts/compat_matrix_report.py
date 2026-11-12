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
"""Summarize and judge the gateway client matrix run against one RustFS stack.

Usage:
  compat_matrix_report.py check --stack <legacy|gateway> --run-dir <dir>
  compat_matrix_report.py diff --legacy <dir> --gateway <dir>
  compat_matrix_report.py --self-test

<dir> holds what the lane observed: `startup.txt` (`ready`, or why the server did
not start), `skipped-clients.txt` (`<client>: <reason>` per client that could not
be installed), `matrix-exit.txt` (the exit code of the gateway's
ci/compat/run_matrix.sh) and `compat/matrix.json` (its manifest).

`check` is clean only when the server served, the matrix finished, every client
ran, and no cell is `fail` or `sut-unregistered` (rustfs/backlog#2776). A
`client-unsupported` cell is the client's gap, never a pass, and is counted.
`diff` lists every (client, scenario) cell whose status differs between stacks.

Exit codes: 0 clean, or no difference; 1 otherwise; 2 malformed input.
"""

import argparse
import json
from pathlib import Path
import sys
import unittest

STATUSES = ("pass", "fail", "client-unsupported", "sut-unregistered")


class InputError(Exception):
    """A manifest that cannot be trusted; exit code 2."""


def read_run(run_dir):
    """Return (startup, skipped clients, matrix exit or None, {(client, scenario): cell} or None)."""
    run_dir = Path(run_dir)
    read = lambda name: (run_dir / name).read_text(encoding="utf-8").strip() if (run_dir / name).is_file() else ""
    skipped = [line for line in read("skipped-clients.txt").splitlines() if line.strip()]
    exit_text = read("matrix-exit.txt")
    if exit_text and not exit_text.isdigit():
        raise InputError(f"{run_dir}/matrix-exit.txt: not an exit code: {exit_text!r}")
    manifest = run_dir / "compat/matrix.json"
    cells = None
    if manifest.is_file():
        try:
            cells = {(client["name"], scenario["id"]): scenario
                     for client in json.loads(manifest.read_text(encoding="utf-8"))["clients"]
                     for scenario in client["scenarios"]}
        except (json.JSONDecodeError, KeyError, TypeError) as error:
            raise InputError(f"{manifest}: not a client matrix manifest: {error}") from error
        if any(cell.get("status") not in STATUSES for cell in cells.values()):
            raise InputError(f"{manifest}: unknown cell status")
    return read("startup.txt"), skipped, int(exit_text) if exit_text else None, cells


def check(stack, startup, skipped, matrix_exit, cells):
    """Return (summary lines, problems)."""
    lines = [f"### Client matrix, `{stack}` stack", ""]
    if startup != "ready":
        observed = startup or "no startup record"
        lines.append(f"- server: {observed if startup.startswith('did not start') else 'did not start: ' + observed}")
        return lines, [f"{stack}: the server did not serve; no client ran"]
    lines += ["- server: ready", f"- run_matrix exit: {matrix_exit if matrix_exit is not None else 'not recorded'}"]
    lines += [f"- {line}" if ": skipped: " in line else f"- {line}: skipped" for line in skipped]
    problems = [f"{stack}: client {line.split(':', 1)[0]} did not run" for line in skipped]
    if cells is None or matrix_exit not in (0, 1) or not cells:
        lines.append("- manifest: none" if cells is None else "- manifest: written by a run that did not finish")
        return lines, problems + [f"{stack}: the matrix did not complete (exit {matrix_exit})"]
    counts = {status: sorted(cell for cell, value in cells.items() if value["status"] == status) for status in STATUSES}
    clients = {client for client, _ in cells}
    lines.append(f"- {len(clients)} clients x {len({scenario for _, scenario in cells})} scenarios: "
                 + ", ".join(f"{status} {len(counts[status])}" for status in STATUSES))
    for status in ("fail", "sut-unregistered"):
        for client, scenario in counts[status]:
            lines.append(f"- {status}: `{client}` / `{scenario}`: {cells[(client, scenario)].get('detail') or ''}")
            problems.append(f"{stack}: {client}/{scenario} is {status}")
    return lines, problems


def diff(legacy, gateway):
    """Return (summary lines, differing cells) between two cell maps; a missing side is `absent`."""
    lines = ["### Client matrix: legacy vs gateway", ""]
    if legacy is None or gateway is None:
        missing = [name for name, cells in (("legacy", legacy), ("gateway", gateway)) if cells is None]
        return lines + [f"- no manifest from {', '.join(missing)}; nothing to compare"], None
    status = lambda cells, key: cells[key]["status"] if key in cells else "absent"
    differing = [(key, status(legacy, key), status(gateway, key))
                 for key in sorted(set(legacy) | set(gateway)) if status(legacy, key) != status(gateway, key)]
    if not differing:
        return lines + ["- no cell differs"], differing
    lines += [f"{len(differing)} cells differ.", "", "| client | scenario | legacy | gateway |", "| --- | --- | --- | --- |"]
    lines += [f"| {client} | {scenario} | {a} | {b} |" for (client, scenario), a, b in differing]
    return lines, differing


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    one = commands.add_parser("check")
    one.add_argument("--stack", required=True, choices=("legacy", "gateway"))
    one.add_argument("--run-dir", required=True)
    two = commands.add_parser("diff")
    two.add_argument("--legacy", required=True)
    two.add_argument("--gateway", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            lines, problems = check(args.stack, *read_run(args.run_dir))
        else:
            lines, differing = diff(read_run(args.legacy)[3], read_run(args.gateway)[3])
            problems = ["the stacks were not both measured"] if differing is None else [
                f"{client}/{scenario}: legacy {a}, gateway {b}" for (client, scenario), a, b in differing]
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

    def write_run(self, directory, cells, startup="ready", matrix_exit="0", skipped=""):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "startup.txt").write_text(startup + "\n")
        (directory / "skipped-clients.txt").write_text(skipped)
        if matrix_exit is not None:
            (directory / "matrix-exit.txt").write_text(matrix_exit + "\n")
        if cells is not None:
            clients = {}
            for (client, scenario), status in cells.items():
                clients.setdefault(client, []).append({"id": scenario, "status": status, "detail": "d"})
            (directory / "compat").mkdir(exist_ok=True)
            (directory / "compat/matrix.json").write_text(json.dumps(
                {"clients": [{"name": name, "scenarios": scenarios} for name, scenarios in clients.items()]}))
        return directory

    def checked(self, **kwargs):
        return check("legacy", *read_run(self.write_run(self.dir, **kwargs)))

    def test_pass_and_client_unsupported_cells_are_clean(self):
        lines, problems = self.checked(cells={("boto3", "a"): "pass", ("restic", "a"): "client-unsupported"})
        self.assertEqual(problems, [])
        self.assertIn("- 2 clients x 1 scenarios: pass 1, fail 0, client-unsupported 1, sut-unregistered 0", lines)

    def test_a_failed_cell_is_red(self):
        self.assertEqual(self.checked(cells={("boto3", "a"): "fail"}, matrix_exit="1")[1], ["legacy: boto3/a is fail"])

    def test_an_unregistered_cell_is_red(self):
        self.assertEqual(self.checked(cells={("mc", "a"): "sut-unregistered"})[1], ["legacy: mc/a is sut-unregistered"])

    def test_a_client_that_could_not_be_installed_is_skipped_never_passed(self):
        lines, problems = self.checked(cells={("boto3", "a"): "pass"}, skipped="restic: skipped: go is not on PATH\n")
        self.assertIn("- restic: skipped: go is not on PATH", lines)
        self.assertEqual(problems, ["legacy: client restic did not run"])

    def test_a_server_that_did_not_start_is_red_and_says_why(self):
        lines, problems = self.checked(cells=None, matrix_exit=None, startup="did not start: refused")
        self.assertIn("- server: did not start: refused", lines)
        self.assertEqual(problems, ["legacy: the server did not serve; no client ran"])

    def test_an_environment_exit_is_red_even_with_a_manifest(self):
        self.assertEqual(self.checked(cells={("boto3", "a"): "pass"}, matrix_exit="3")[1],
                         ["legacy: the matrix did not complete (exit 3)"])

    def test_a_missing_manifest_is_red_even_when_the_exit_was_swallowed(self):
        self.assertEqual(self.checked(cells=None)[1], ["legacy: the matrix did not complete (exit 0)"])

    def test_a_swallowed_exit_cannot_hide_a_failed_cell(self):
        self.assertEqual(self.checked(cells={("boto3", "a"): "fail"}, matrix_exit="0")[1], ["legacy: boto3/a is fail"])

    def test_an_unknown_status_is_refused(self):
        with self.assertRaises(InputError):
            self.checked(cells={("boto3", "a"): "passed"})

    def test_diff_lists_changed_and_absent_cells_only(self):
        legacy = read_run(self.write_run(self.dir / "l", {("boto3", "a"): "pass", ("boto3", "b"): "pass", ("mc", "a"): "pass"}))[3]
        gateway = read_run(self.write_run(self.dir / "g", {("boto3", "a"): "pass", ("boto3", "b"): "sut-unregistered"}))[3]
        self.assertEqual(diff(legacy, gateway)[1], [(("boto3", "b"), "pass", "sut-unregistered"), (("mc", "a"), "pass", "absent")])

    def test_diff_without_both_manifests_compares_nothing(self):
        lines, differing = diff({("boto3", "a"): {"status": "pass"}}, None)
        self.assertIsNone(differing)
        self.assertIn("- no manifest from gateway; nothing to compare", lines)


if __name__ == "__main__":
    sys.exit(main())
