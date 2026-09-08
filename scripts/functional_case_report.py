#!/usr/bin/env python3
"""Preserve every functional case execution and its suite context in reports."""

from __future__ import annotations

import argparse
from pathlib import Path
import re


def generate_report(log_file: Path, case_file: Path, matrix_file: Path | None = None) -> bool:
    ansi = re.compile(r"\x1b\[[0-9;]*m")
    start_re = re.compile(r"^---\s+([A-Z][A-Z0-9]*-[0-9]+)\s+(.+?)\s+---$")
    done_re = re.compile(r"^\[(PASS|FAIL|UNSUPPORTED)\]\s+([A-Z][A-Z0-9]*-[0-9]+)\b")
    context_re = re.compile(r"^(?:\[INFO\]\s+)?==\s+((?:topology|suite):.+?)\s+==$")
    topo_re = re.compile(r"^\[UPG-TOPO\]\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+PASS=(\d+)\s+FAIL=(\d+)\s*$")
    rows = []
    pending = {}
    topo_rows = []
    context = "context not recorded"
    complete = True
    try:
        with log_file.open(encoding="utf-8", errors="replace") as log:
            for raw in log:
                line = ansi.sub("", raw).strip()
                if match := context_re.match(line):
                    context = match[1]
                    pending.clear()
                elif match := topo_re.match(line):
                    topo_rows.append(match.groups())
                elif match := start_re.match(line):
                    case_id, name = match.groups()
                    pending[case_id] = len(rows)
                    rows.append([case_id, f"{name} ({context})", "RUNNING"])
                elif match := done_re.match(line):
                    status, case_id = match.groups()
                    index = pending.pop(case_id, None)
                    if index is None:
                        complete = False
                        rows.append([case_id, f"{case_id} ({context}; start not recorded)", status])
                    else:
                        rows[index][2] = status
    except FileNotFoundError:
        pass

    counts = {status: sum(row[2] == status for row in rows) for status in ("PASS", "FAIL", "UNSUPPORTED", "RUNNING")}
    with case_file.open("w", encoding="utf-8") as out:
        out.write(f"## Case Summary\n\n- Total: {len(rows)}\n")
        for status, count in counts.items():
            out.write(f"- {status}: {count}\n")
        out.write("\n| Case | Name | Status |\n| --- | --- | --- |\n")
        for row in rows:
            out.write("| " + " | ".join(value.replace("|", "&#124;") for value in row) + " |\n")
        if not rows:
            out.write("\nNo case execution was recorded; the log is missing, empty, or stopped before the cases.\n")

    valid = complete and bool(rows) and not counts["FAIL"] and not counts["RUNNING"]
    if matrix_file is not None:
        with matrix_file.open("w", encoding="utf-8") as out:
            out.write("## Upgrade Matrix\n\n| Topology | KMS Backend | From Version | To Version | Result |\n")
            out.write("| --- | --- | --- | --- | --- |\n")
            for topo, backend, old_v, new_v, npass, nfail in topo_rows:
                result = "PASS" if nfail == "0" else "FAIL"
                out.write(f"| {topo} | {backend} | {old_v} | {new_v} | {result} (PASS={npass} FAIL={nfail}) |\n")
            if not topo_rows:
                out.write("| - | - | - | - | NOT RUN (suite failed before upgrade) |\n")
        valid = valid and bool(topo_rows) and all(row[-1] == "0" for row in topo_rows)
    return valid


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_file", type=Path)
    parser.add_argument("case_file", type=Path)
    parser.add_argument("matrix_file", type=Path, nargs="?")
    args = parser.parse_args()
    raise SystemExit(0 if generate_report(args.log_file, args.case_file, args.matrix_file) else 1)
