#!/usr/bin/env python3
# Copyright 2024 RustFS Team
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

"""Aggregate a cargo-llvm-cov JSON export into a per-crate line-coverage table.

Usage:
    coverage_per_crate.py <coverage.json> [repo-root]

Reads the JSON export written by ``cargo llvm-cov report --json`` and prints a
GitHub-flavoured markdown table with one row per workspace crate directory
(``crates/<name>`` or ``rustfs``), sorted worst-first, followed by the
workspace TOTAL row. Consumed by ``.github/workflows/coverage.yml`` (job
summary) and ``make coverage`` (backlog#1153 infra-5). Stdlib only.
"""

import json
import os
import sys


def crate_label(filename: str, root: str) -> str:
    """Map an absolute source path to its workspace crate directory."""
    rel = os.path.relpath(filename, root)
    parts = rel.split(os.sep)
    if parts[0] == "..":
        return "(external)"
    if parts[0] == "crates" and len(parts) > 1:
        return f"crates/{parts[1]}"
    # Top-level crate directories (the `rustfs` binary crate) and anything
    # else that sneaks into the export (e.g. generated code at the root).
    return parts[0]


def fmt_pct(covered: int, count: int) -> str:
    return f"{100.0 * covered / count:.2f}%" if count else "—"


def main() -> int:
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    path = sys.argv[1]
    root = os.path.abspath(sys.argv[2] if len(sys.argv) == 3 else os.getcwd())

    with open(path, encoding="utf-8") as fh:
        export = json.load(fh)

    try:
        data = export["data"][0]
        files = data["files"]
        totals = data["totals"]["lines"]
    except (KeyError, IndexError) as exc:
        print(f"error: unexpected llvm-cov JSON shape ({exc})", file=sys.stderr)
        return 1

    crates: dict[str, list[int]] = {}
    for f in files:
        lines = f["summary"]["lines"]
        acc = crates.setdefault(crate_label(f["filename"], root), [0, 0])
        acc[0] += lines["covered"]
        acc[1] += lines["count"]

    rows = sorted(
        crates.items(),
        key=lambda kv: (100.0 * kv[1][0] / kv[1][1]) if kv[1][1] else 101.0,
    )

    print("## Per-crate line coverage")
    print()
    print("| Crate | Line coverage | Lines covered / total |")
    print("|---|---:|---:|")
    for name, (covered, count) in rows:
        print(f"| `{name}` | {fmt_pct(covered, count)} | {covered} / {count} |")
    print(
        f"| **TOTAL** | **{fmt_pct(totals['covered'], totals['count'])}** "
        f"| {totals['covered']} / {totals['count']} |"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
