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
    coverage_per_crate.py --inspect-file <relpath> <coverage.json> [repo-root]
    coverage_per_crate.py --self-test

Reads the JSON export written by ``cargo llvm-cov report --json`` (or
``llvm-cov export``) and prints a GitHub-flavoured markdown table with one row
per workspace crate directory (``crates/<name>`` or ``rustfs``), sorted
worst-first, followed by the workspace TOTAL row.

Canonicalizes source file paths before per-crate aggregation so files compiled
both via normal module paths and via ``#[path = "../src/..."]`` in integration
tests (such as ``rustfs/tests/connect_logs.rs`` ->
``rustfs/src/connect/diagnostics/logs.rs``) are merged onto their single
canonical source file rather than double-counted or misattributed as 0%.
Consumed by ``.github/workflows/coverage.yml`` and ``make coverage``
(backlog#1153 infra-5, backlog#2679 T07). Stdlib only.
"""

from __future__ import annotations

import argparse
from itertools import groupby
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest


def canonical_relpath(filename: str, root: str) -> str:
    """Resolve `filename` to a normalized POSIX path relative to `root`."""
    norm_root = os.path.normpath(os.path.abspath(root))
    real_root = os.path.realpath(norm_root)
    norm_file = os.path.normpath(filename if os.path.isabs(filename) else os.path.join(norm_root, filename))
    if os.path.exists(norm_file):
        real_file = os.path.realpath(norm_file)
        try:
            rel = os.path.relpath(real_file, real_root)
        except ValueError:
            rel = os.path.relpath(norm_file, norm_root)
    else:
        rel = os.path.relpath(norm_file, norm_root)
    return rel.replace(os.sep, "/")


def crate_label(filename: str, root: str) -> str:
    """Map a source path (canonicalized) to its workspace crate directory."""
    rel = canonical_relpath(filename, root)
    parts = rel.split("/")
    if parts[0] == "..":
        return "(external)"
    if parts[0] == "crates" and len(parts) > 1:
        return f"crates/{parts[1]}"
    # Top-level crate directories (the `rustfs` binary crate) and anything
    # else that sneaks into the export (e.g. generated code at the root).
    return parts[0]


def fmt_pct(covered: int, count: int) -> str:
    return f"{100.0 * covered / count:.2f}%" if count else "—"


def _line_counts(lines: dict[str, int], source: str) -> tuple[int, int]:
    covered = lines["covered"]
    count = lines["count"]
    if type(covered) is not int or type(count) is not int or covered < 0 or count < 0 or covered > count:
        raise ValueError(f"invalid line coverage for {source}: {covered}/{count}")
    return covered, count


def _merge_segment_line_counts(entries: list[dict]) -> tuple[int, int] | None:
    """Union mapped lines across aliases when the segments agree with LLVM's summaries."""
    mapped: set[int] = set()
    covered: set[int] = set()
    for entry in entries:
        segments = entry.get("segments")
        if not isinstance(segments, list) or not segments or any(
            not isinstance(seg, list) or len(seg) < 6 or type(seg[0]) is not int or seg[0] < 1
            or type(seg[2]) is not int or seg[2] < 0 for seg in segments
        ):
            return None
        expected_covered, expected_count = _line_counts(entry["summary"]["lines"], entry["filename"])
        file_mapped: set[int] = set()
        file_covered: set[int] = set()
        previous_line = 0
        wrapped = None
        for line, group in groupby(segments, key=lambda seg: seg[0]):
            if line <= previous_line:
                return None
            if wrapped is not None:
                if line - previous_line - 1 > expected_count:
                    return None
                between = range(previous_line + 1, line)
                file_mapped.update(between)
                if wrapped:
                    file_covered.update(between)
            on_line = list(group)
            starts = [seg for seg in on_line if seg[3] and seg[4]]
            starts_skipped = not on_line[0][3] and on_line[0][4]
            if starts or (wrapped is not None and not starts_skipped):
                file_mapped.add(line)
                if wrapped or any(seg[2] and not seg[5] for seg in starts):
                    file_covered.add(line)
            last = on_line[-1]
            wrapped = last[2] if last[3] else None
            previous_line = line
        if (len(file_covered), len(file_mapped)) != (expected_covered, expected_count):
            return None
        mapped.update(file_mapped)
        covered.update(file_covered)
    return len(covered), len(mapped)


def load_file_attribution(path: str, root: str) -> tuple[dict[str, dict], dict[str, int]]:
    """Load per-canonical-file coverage and detect `#[path]` alias merges."""
    with open(path, encoding="utf-8") as fh:
        export = json.load(fh)

    data = export["data"][0]
    files = data["files"]
    total_covered, total_count = _line_counts(data["totals"]["lines"], "totals")

    grouped: dict[str, list[dict]] = {}
    for f in files:
        _line_counts(f["summary"]["lines"], f["filename"])
        canon = canonical_relpath(f["filename"], root)
        grouped.setdefault(canon, []).append(f)

    attributed: dict[str, dict] = {}
    for canon, entries in grouped.items():
        raw_entries = []
        for item in entries:
            cov, cnt = _line_counts(item["summary"]["lines"], item["filename"])
            raw_entries.append({"filename": item["filename"], "covered": cov, "count": cnt})

        if len(entries) == 1:
            merged_covered = raw_entries[0]["covered"]
            merged_count = raw_entries[0]["count"]
            merge_method = "single"
        else:
            seg_merged = _merge_segment_line_counts(entries)
            if seg_merged is not None:
                merged_covered, merged_count = seg_merged
                merge_method = "segments_union"
            else:
                merged_covered = max(item["covered"] for item in raw_entries)
                merged_count = max(item["count"] for item in raw_entries)
                merge_method = "max_dedup"
            merged_covered = min(merged_covered, merged_count)

        attributed[canon] = {
            "canonical_path": canon,
            "crate": crate_label(canon, root),
            "covered": merged_covered,
            "count": merged_count,
            "raw_entries": raw_entries,
            "has_path_alias": len(entries) > 1 or any(".." in Path(e["filename"]).parts or os.path.normpath(e["filename"]) != e["filename"] for e in raw_entries),
            "merge_method": merge_method,
        }

    return attributed, {"covered": total_covered, "count": total_count}


def load_coverage(path: str, root: str) -> tuple[dict[str, list[int]], dict[str, int]]:
    attributed, totals = load_file_attribution(path, root)
    crates: dict[str, list[int]] = {}
    for record in attributed.values():
        acc = crates.setdefault(record["crate"], [0, 0])
        acc[0] += record["covered"]
        acc[1] += record["count"]
    return crates, totals


class CoveragePerCrateTests(unittest.TestCase):
    def test_path_alias_entries_merge_onto_single_canonical_source_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "rustfs/src/connect/diagnostics/logs.rs"
            target.parent.mkdir(parents=True)
            (root / "rustfs/tests").mkdir(parents=True)
            target.write_text("// logs\n", encoding="utf-8")

            export = {
                "data": [
                    {
                        "files": [
                            {
                                "filename": str(target),
                                "summary": {"lines": {"covered": 0, "count": 400}},
                            },
                            {
                                "filename": str(root / "rustfs/tests/../src/connect/diagnostics/logs.rs"),
                                "summary": {"lines": {"covered": 320, "count": 400}},
                            },
                        ],
                        "totals": {"lines": {"covered": 320, "count": 400}},
                    }
                ]
            }
            cov_json = root / "coverage.json"
            cov_json.write_text(json.dumps(export), encoding="utf-8")

            attributed, _ = load_file_attribution(str(cov_json), str(root))
            key = "rustfs/src/connect/diagnostics/logs.rs"
            self.assertIn(key, attributed)
            self.assertEqual(attributed[key]["crate"], "rustfs")
            self.assertEqual((attributed[key]["covered"], attributed[key]["count"]), (320, 400))
            self.assertEqual(len(attributed[key]["raw_entries"]), 2)
            self.assertTrue(attributed[key]["has_path_alias"])

            crates, _ = load_coverage(str(cov_json), str(root))
            # Must NOT double-count 400 + 400 = 800 lines in the denominator.
            self.assertEqual(crates["rustfs"], [320, 400])

    def test_segment_union_merges_disjoint_covered_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "rustfs/src/connect/diagnostics/logs.rs"
            target.parent.mkdir(parents=True)
            target.write_text("// logs\n", encoding="utf-8")
            export = {
                "data": [
                    {
                        "files": [
                            {
                                "filename": str(target),
                                "summary": {"lines": {"covered": 1, "count": 3}},
                                "segments": [[10, 1, 1, True, True, False], [10, 2, 0, False, False, False],
                                             [20, 1, 0, True, True, False], [20, 2, 0, False, False, False],
                                             [30, 1, 0, True, True, False]],
                            },
                            {
                                "filename": str(root / "rustfs/tests/../src/connect/diagnostics/logs.rs"),
                                "summary": {"lines": {"covered": 1, "count": 3}},
                                "segments": [[10, 1, 0, True, True, False], [10, 2, 0, False, False, False],
                                             [20, 1, 4, True, True, False], [20, 2, 0, False, False, False],
                                             [30, 1, 0, True, True, False]],
                            },
                        ],
                        "totals": {"lines": {"covered": 2, "count": 3}},
                    }
                ]
            }
            cov_json = root / "coverage.json"
            cov_json.write_text(json.dumps(export), encoding="utf-8")
            attributed, _ = load_file_attribution(str(cov_json), str(root))
            rec = attributed["rustfs/src/connect/diagnostics/logs.rs"]
            self.assertEqual(rec["merge_method"], "segments_union")
            self.assertEqual((rec["covered"], rec["count"]), (2, 3))

    def test_wrapped_regions_include_lines_without_segment_entries(self) -> None:
        entries = []
        for first, second in ((1, 0), (0, 1)):
            entries.append({
                "filename": "src/lib.rs",
                "summary": {"lines": {"covered": 3, "count": 6}},
                "segments": [[10, 1, first, True, True, False], [12, 2, 0, False, False, False],
                             [20, 1, second, True, True, False], [22, 2, 0, False, False, False]],
            })
        self.assertEqual(_merge_segment_line_counts(entries), (6, 6))
        entries[0]["summary"]["lines"]["count"] = 7
        self.assertIsNone(_merge_segment_line_counts(entries))

    def test_single_parent_traversal_entry_marks_has_path_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "rustfs/src/connect/diagnostics/logs.rs"
            target.parent.mkdir(parents=True)
            (root / "rustfs/tests").mkdir(parents=True)
            target.write_text("// logs\n", encoding="utf-8")
            export = {
                "data": [
                    {
                        "files": [
                            {
                                "filename": str(root / "rustfs/tests/../src/connect/diagnostics/logs.rs"),
                                "summary": {"lines": {"covered": 120, "count": 200}},
                            },
                        ],
                        "totals": {"lines": {"covered": 120, "count": 200}},
                    }
                ]
            }
            cov_json = root / "coverage.json"
            cov_json.write_text(json.dumps(export), encoding="utf-8")
            attributed, _ = load_file_attribution(str(cov_json), str(root))
            rec = attributed["rustfs/src/connect/diagnostics/logs.rs"]
            self.assertEqual(rec["canonical_path"], "rustfs/src/connect/diagnostics/logs.rs")
            self.assertTrue(rec["has_path_alias"])
            self.assertEqual((rec["covered"], rec["count"]), (120, 200))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage_json", nargs="?", help="Path to llvm-cov JSON export")
    parser.add_argument("repo_root", nargs="?", default=os.getcwd(), help="Repository root path")
    parser.add_argument("--inspect-file", help="Inspect canonical path attribution for one repo-relative file (JSON output)")
    parser.add_argument("--self-test", action="store_true", help="Run built-in path-attribution self-tests")
    args = parser.parse_args(argv)

    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(CoveragePerCrateTests)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1

    if not args.coverage_json:
        parser.print_usage(sys.stderr)
        return 2

    root = os.path.abspath(args.repo_root)

    try:
        if args.inspect_file:
            attributed, _ = load_file_attribution(args.coverage_json, root)
            canon = canonical_relpath(args.inspect_file, root)
            record = attributed.get(canon)
            if record is None:
                print(json.dumps({"canonical_path": canon, "found": False}, indent=2, sort_keys=True))
                return 1
            print(json.dumps({"found": True, **record}, indent=2, sort_keys=True))
            return 0
        crates, totals = load_coverage(args.coverage_json, root)
    except (KeyError, IndexError, ValueError) as exc:
        print(f"error: unexpected llvm-cov JSON shape ({exc})", file=sys.stderr)
        return 1

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
