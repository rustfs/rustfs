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
"""Plan and judge the s3-tests reference parity lane.

Subcommands:
  targets    validate the target list and print it as a JSON matrix
  export-env validate one target's extra environment and print KEY=VALUE lines
  failed     print the failed test names of one run, in implemented_tests.txt form
  compare    compare every candidate against the reference, per pytest node id

`compare` reads <reports>/<target>/selected-nodeids.txt, junit.xml and the
optional rerun-junit.xml of every target. Each node id is reduced to pass /
fail / skip / missing; a case that failed and then passed its single rerun is a
pass and is listed as flaky. A tolerated entry must match a real difference.

Exit codes: 0 no untolerated difference and no stale entry; 1 a difference, a
stale tolerated entry or an incomplete run; 2 missing or malformed input.
"""

import argparse
import json
from pathlib import Path
import re
import sys
import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import junit_diff  # noqa: E402  (sibling script, not a package)

PASS, FAIL, SKIP, MISSING = junit_diff.PASS, junit_diff.FAIL, junit_diff.SKIP, junit_diff.MISSING
NAME = re.compile(r"[a-z][a-z0-9-]{0,39}")
ENV_KEY = re.compile(r"RUSTFS_[A-Z0-9_]+")
ENV_VALUE = re.compile(r"[A-Za-z0-9._,:/=+-]*")
ISSUE = re.compile(r"https://github\.com/rustfs/[A-Za-z0-9_.-]+/issues/[1-9][0-9]*")
NODEID = re.compile(r"[A-Za-z0-9_/]+\.py(::[A-Za-z0-9_]+)+(\[[^\]\s]*\])?")
TEST_FILE_PREFIX = "s3tests/functional/test_s3.py::"
BINARIES = ("reference", "candidate")


class InputError(Exception):
    """Input that cannot be trusted; maps to exit code 2."""


# --- target list -----------------------------------------------------------

def load_targets(path):
    try:
        data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise InputError(f"{path}: target list is missing") from exc
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise InputError(f"{path}: cannot read target list: {exc}") from exc
    if data.get("schema") != 1 or type(data.get("schema")) is not int or set(data) != {"schema", "targets"}:
        raise InputError(f"{path}: unsupported target list schema")
    return validate_targets(data["targets"], str(path))


def validate_targets(targets, where="targets"):
    if not isinstance(targets, list) or len(targets) < 2:
        raise InputError(f"{where}: need one reference and at least one candidate")
    result, names = [], set()
    for item in targets:
        if not isinstance(item, dict) or not set(item) <= {"name", "binary", "env"}:
            raise InputError(f"{where}: invalid target entry {item!r}")
        name, binary, env = item.get("name"), item.get("binary"), item.get("env", {})
        if not isinstance(name, str) or not NAME.fullmatch(name) or name in names:
            raise InputError(f"{where}: invalid or duplicate target name {name!r}")
        if binary not in BINARIES:
            raise InputError(f"{where}: target {name} has unknown binary {binary!r}")
        validate_env(env, f"{where}: target {name}")
        names.add(name)
        result.append({"name": name, "binary": binary, "env": dict(env)})
    if [t["binary"] for t in result].count("reference") != 1:
        raise InputError(f"{where}: exactly one target must use the reference binary")
    return result


def validate_env(env, where):
    if not isinstance(env, dict):
        raise InputError(f"{where}: env must be a table")
    for key, value in env.items():
        if not isinstance(key, str) or not ENV_KEY.fullmatch(key):
            raise InputError(f"{where}: env key {key!r} must match RUSTFS_[A-Z0-9_]+")
        if not isinstance(value, str) or not ENV_VALUE.fullmatch(value):
            raise InputError(f"{where}: env value for {key} has unsupported characters")
    return env


# --- node ids and reports --------------------------------------------------

def nodeid_to_case(nodeid):
    """Map a pytest node id to the classname::name id that junit_diff uses."""
    if not NODEID.fullmatch(nodeid):
        raise InputError(f"invalid pytest node id {nodeid!r}")
    path, *parts = nodeid.split("::")
    module = path[: -len(".py")].replace("/", ".")
    classname = ".".join([module, *parts[:-1]])
    return junit_diff.case_id(classname, parts[-1])


def read_selected(path):
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as exc:
        raise InputError(f"{path}: selected node id list is missing") from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise InputError(f"{path}: cannot read selected node ids: {exc}") from exc
    nodeids = [line.strip() for line in lines if line.strip()]
    if not nodeids:
        raise InputError(f"{path}: selected node id list is empty")
    if len(set(nodeids)) != len(nodeids):
        raise InputError(f"{path}: duplicate selected node id")
    by_case = {}
    for nodeid in nodeids:
        case = nodeid_to_case(nodeid)
        if case in by_case:
            raise InputError(f"{path}: node ids {by_case[case]} and {nodeid} collide")
        by_case[case] = nodeid
    return nodeids, by_case


def target_states(directory):
    """Return (nodeids, {nodeid: state}, [flaky nodeids]) for one target run."""
    directory = Path(directory)
    if not directory.is_dir():
        raise InputError(f"{directory}: report directory is missing")
    nodeids, by_case = read_selected(directory / "selected-nodeids.txt")
    try:
        first, _ = junit_diff.parse_report(str(directory / "junit.xml"))
    except junit_diff.InputError as exc:
        raise InputError(str(exc)) from exc
    unexpected = sorted(set(first) - set(by_case))
    if unexpected:
        raise InputError(f"{directory}: junit.xml has unselected cases: {', '.join(unexpected[:5])}")
    states = {by_case[case]: state for case, state in first.items()}
    flaky = []
    rerun_path = directory / "rerun-junit.xml"
    if rerun_path.exists():
        try:
            rerun, _ = junit_diff.parse_report(str(rerun_path))
        except junit_diff.InputError as exc:
            raise InputError(str(exc)) from exc
        for case, state in sorted(rerun.items()):
            nodeid = by_case.get(case)
            if nodeid is None or states.get(nodeid) != FAIL:
                raise InputError(f"{rerun_path}: {case} was rerun without failing first")
            if state == PASS:
                flaky.append(nodeid)
            states[nodeid] = PASS if state == PASS else FAIL
    for nodeid in nodeids:
        states.setdefault(nodeid, MISSING)
    return nodeids, states, flaky


def failed_names(directory):
    _, states, _ = target_states(directory)
    names = []
    for nodeid, state in states.items():
        if state == FAIL:
            if not nodeid.startswith(TEST_FILE_PREFIX):
                raise InputError(f"cannot express {nodeid} as an implemented_tests.txt entry")
            names.append(nodeid[len(TEST_FILE_PREFIX):])
    return sorted(names)


# --- tolerated differences ---------------------------------------------------

def read_tolerated(path, candidates):
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError as exc:
        raise InputError(f"{path}: tolerated-difference file is missing") from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise InputError(f"{path}: cannot read tolerated differences: {exc}") from exc
    entries = {}
    for number, line in enumerate(lines, 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        fields = [part.strip() for part in line.split(" | ")]
        if len(fields) != 4 or not all(fields):
            raise InputError(f"{path}:{number}: expected '<target> | <node id> | <issue URL> | <reason>'")
        target, nodeid, issue, _reason = fields
        if target not in candidates:
            raise InputError(f"{path}:{number}: {target!r} is not a candidate target")
        nodeid_to_case(nodeid)
        if not ISSUE.fullmatch(issue):
            raise InputError(f"{path}:{number}: tracking issue must be a https://github.com/rustfs/<repo>/issues/<n> URL")
        if (target, nodeid) in entries:
            raise InputError(f"{path}:{number}: duplicate entry for {target} {nodeid}")
        entries[(target, nodeid)] = issue
    return entries


# --- comparison ----------------------------------------------------------------

def count(states):
    result = {PASS: 0, FAIL: 0, SKIP: 0, MISSING: 0}
    for state in states.values():
        result[state] += 1
    return result


def compare(targets, reports, tolerated_path):
    reference = next(t["name"] for t in targets if t["binary"] == "reference")
    candidates = [t["name"] for t in targets if t["binary"] == "candidate"]
    tolerated = read_tolerated(tolerated_path, set(candidates))
    runs = {t["name"]: target_states(Path(reports) / t["name"]) for t in targets}
    selected = runs[reference][0]
    for name, (nodeids, _, _) in runs.items():
        if nodeids != selected:
            raise InputError(f"{name}: selected node ids differ from the {reference} run")
    ref_states = runs[reference][1]
    differences = []
    for name in candidates:
        states = runs[name][1]
        for nodeid in selected:
            if ref_states[nodeid] != states[nodeid]:
                issue = tolerated.get((name, nodeid))
                differences.append({"target": name, "nodeid": nodeid, "reference": ref_states[nodeid],
                                    "candidate": states[nodeid], "tolerated": issue})
    seen = {(d["target"], d["nodeid"]) for d in differences}
    stale = [{"target": t, "nodeid": n, "issue": i} for (t, n), i in sorted(tolerated.items()) if (t, n) not in seen]
    incomplete = {name: [n for n in selected if run[1][n] == MISSING] for name, run in runs.items()}
    return {
        "reference": reference,
        "candidates": candidates,
        "selected": len(selected),
        "counts": {name: count(run[1]) for name, run in runs.items()},
        "flaky": {name: run[2] for name, run in runs.items()},
        "differences": differences,
        "stale_tolerated": stale,
        "incomplete": {name: ids for name, ids in incomplete.items() if ids},
    }


def failed(summary):
    return bool(summary["stale_tolerated"] or summary["incomplete"]
                or any(not d["tolerated"] for d in summary["differences"]))


def render(summary):
    out = ["## s3-tests reference parity", ""]
    out.append(f"Reference target `{summary['reference']}`; {summary['selected']} selected node ids per target.")
    out += ["", "| Target | pass | fail | skip | missing | flaky (passed on rerun) |", "|---|---:|---:|---:|---:|---:|"]
    for name, c in summary["counts"].items():
        out.append(f"| `{name}` | {c[PASS]} | {c[FAIL]} | {c[SKIP]} | {c[MISSING]} | {len(summary['flaky'][name])} |")
    diffs = summary["differences"]
    out += ["", f"### Differences ({len(diffs)})", ""]
    if diffs:
        out += ["| Target | Node id | reference | candidate | Tolerated |", "|---|---|---|---|---|"]
        for d in diffs:
            out.append(f"| `{d['target']}` | `{d['nodeid']}` | {d['reference']} | {d['candidate']} | {d['tolerated'] or 'no'} |")
    else:
        out.append("No differences.")
    if summary["stale_tolerated"]:
        out += ["", "### Stale tolerated entries (no matching difference; remove them)", ""]
        out += [f"- `{s['target']}` `{s['nodeid']}` ({s['issue']})" for s in summary["stale_tolerated"]]
    if summary["incomplete"]:
        out += ["", "### Incomplete runs (selected node ids without a result)", ""]
        out += [f"- `{name}`: {len(ids)} missing, first `{ids[0]}`" for name, ids in summary["incomplete"].items()]
    flaky = [(name, ids) for name, ids in summary["flaky"].items() if ids]
    if flaky:
        out += ["", "### Flaky (failed, then passed the single rerun; not a difference)", ""]
        out += [f"- `{name}`: " + ", ".join(f"`{i}`" for i in ids) for name, ids in flaky]
    out += ["", "Result: **" + ("FAIL" if failed(summary) else "PASS") + "**", ""]
    return "\n".join(out)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    tgt = sub.add_parser("targets")
    tgt.add_argument("--config", required=True)
    env = sub.add_parser("export-env")
    env.add_argument("--target-json", required=True)
    fail = sub.add_parser("failed")
    fail.add_argument("--report", required=True, help="one target's report directory")
    cmp_ = sub.add_parser("compare")
    cmp_.add_argument("--targets-json", required=True)
    cmp_.add_argument("--reports", required=True)
    cmp_.add_argument("--tolerated", required=True)
    cmp_.add_argument("--markdown", help="write the summary table here (appended)")
    cmp_.add_argument("--json", help="write the machine-readable summary here")
    args = parser.parse_args(argv)
    try:
        if args.mode == "targets":
            print(json.dumps(load_targets(args.config), separators=(",", ":")))
        elif args.mode == "export-env":
            target = json.loads(args.target_json)
            if not isinstance(target, dict):
                raise InputError("target must be a JSON object")
            for key, value in sorted(validate_env(target.get("env", {}), "target").items()):
                print(f"{key}={value}")
        elif args.mode == "failed":
            for name in failed_names(args.report):
                print(name)
        else:
            targets = validate_targets(json.loads(args.targets_json))
            summary = compare(targets, args.reports, args.tolerated)
            text = render(summary)
            print(text)
            if args.markdown:
                with open(args.markdown, "a", encoding="utf-8") as handle:
                    handle.write(text + "\n")
            if args.json:
                with open(args.json, "w", encoding="utf-8") as handle:
                    json.dump(summary, handle, indent=2, sort_keys=True)
                    handle.write("\n")
            return 1 if failed(summary) else 0
    except (InputError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
