#!/usr/bin/env python3
# Copyright 2024 RustFS Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare security-critical crate line coverage with the report-only baseline."""

import argparse
import json
import math
import os
import sys
import tempfile
import tomllib
from pathlib import Path

from coverage_per_crate import fmt_pct, load_coverage


SECURITY_CRATES = ("crates/iam", "crates/kms", "crates/policy", "crates/crypto")


def load_baselines(path: str) -> tuple[float, dict[str, tuple[int, int]]]:
    with open(path, "rb") as fh:
        config = tomllib.load(fh)

    if config.get("phase") != "report-only":
        raise ValueError("coverage baseline phase must be report-only")

    raw_allowed_drop = config["allowed_drop_percentage_points"]
    if isinstance(raw_allowed_drop, bool) or not isinstance(raw_allowed_drop, (int, float)):
        raise ValueError("allowed_drop_percentage_points must be a number")
    allowed_drop = float(raw_allowed_drop)
    if not math.isfinite(allowed_drop) or allowed_drop < 0:
        raise ValueError("allowed_drop_percentage_points must be finite and non-negative")

    baselines: dict[str, tuple[int, int]] = {}
    for crate, values in config["crates"].items():
        covered = values["covered"]
        count = values["count"]
        if type(covered) is not int or type(count) is not int:
            raise ValueError(f"invalid baseline for {crate}: covered and count must be integers")
        if covered < 0 or count <= 0 or covered > count:
            raise ValueError(f"invalid baseline for {crate}: {covered}/{count}")
        baselines[crate] = (covered, count)
    missing = [crate for crate in SECURITY_CRATES if crate not in baselines]
    unexpected = sorted(set(baselines).difference(SECURITY_CRATES))
    if missing or unexpected:
        raise ValueError(f"coverage baseline crate set mismatch: missing={missing}, unexpected={unexpected}")
    return allowed_drop, baselines


def compare(
    current: dict[str, list[int]],
    baselines: dict[str, tuple[int, int]],
    allowed_drop: float,
) -> list[tuple[str, int, int, int, int, float, bool]]:
    rows = []
    for crate, (baseline_covered, baseline_count) in baselines.items():
        if crate not in current:
            raise ValueError(f"coverage report is missing {crate}")
        covered, count = current[crate]
        if type(covered) is not int or type(count) is not int:
            raise ValueError(f"invalid coverage for {crate}: covered and count must be integers")
        if covered < 0 or count <= 0 or covered > count:
            raise ValueError(f"invalid coverage for {crate}: {covered}/{count}")
        current_pct = 100.0 * covered / count
        baseline_pct = 100.0 * baseline_covered / baseline_count
        delta = current_pct - baseline_pct
        rows.append((crate, covered, count, baseline_covered, baseline_count, delta, delta < -allowed_drop))
    return rows


def print_report(rows: list[tuple[str, int, int, int, int, float, bool]], allowed_drop: float) -> None:
    print("## Security-critical coverage ratchet (report-only)")
    print()
    print(f"Calibration threshold: a drop greater than {allowed_drop:.2f} percentage points is reported as a regression.")
    print()
    print("| Crate | Current | Baseline | Delta | Status |")
    print("|---|---:|---:|---:|---|")
    for crate, covered, count, baseline_covered, baseline_count, delta, regressed in rows:
        status = "REGRESSION (report-only)" if regressed else "OK"
        print(
            f"| `{crate}` | {fmt_pct(covered, count)} ({covered}/{count}) "
            f"| {fmt_pct(baseline_covered, baseline_count)} ({baseline_covered}/{baseline_count}) "
            f"| {delta:+.2f} pp | {status} |"
        )
    print()
    print("This calibration phase records regressions without failing the job; malformed or incomplete evidence still fails closed.")


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        coverage = root / "coverage.json"
        baseline = root / "baseline.toml"
        coverage_data = {
            "data": [
                {
                    "files": [
                        {
                            "filename": str(root / "crates/iam/src/lib.rs"),
                            "summary": {"lines": {"covered": 80, "count": 100}},
                        },
                        {
                            "filename": str(root / "crates/kms/src/lib.rs"),
                            "summary": {"lines": {"covered": 90, "count": 100}},
                        },
                        {
                            "filename": str(root / "crates/policy/src/lib.rs"),
                            "summary": {"lines": {"covered": 90, "count": 100}},
                        },
                        {
                            "filename": str(root / "crates/crypto/src/lib.rs"),
                            "summary": {"lines": {"covered": 90, "count": 100}},
                        },
                    ],
                    "totals": {"lines": {"covered": 350, "count": 400}},
                }
            ]
        }
        coverage.write_text(json.dumps(coverage_data), encoding="utf-8")
        baseline_text = """phase = "report-only"
allowed_drop_percentage_points = 1.0
[crates."crates/iam"]
covered = 90
count = 100
[crates."crates/kms"]
covered = 85
count = 100
[crates."crates/policy"]
covered = 90
count = 100
[crates."crates/crypto"]
covered = 90
count = 100
"""
        baseline.write_text(baseline_text, encoding="utf-8")
        current, _ = load_coverage(str(coverage), str(root))
        allowed_drop, baselines = load_baselines(str(baseline))
        rows = compare(current, baselines, allowed_drop)
        assert [row[-1] for row in rows] == [True, False, False, False]
        try:
            compare({"crates/iam": current["crates/iam"]}, baselines, allowed_drop)
        except ValueError as error:
            assert str(error) == "coverage report is missing crates/kms"
        else:
            raise AssertionError("missing crate must fail closed")
        try:
            compare({**current, "crates/iam": [101, 100]}, baselines, allowed_drop)
        except ValueError as error:
            assert str(error) == "invalid coverage for crates/iam: 101/100"
        else:
            raise AssertionError("invalid coverage must fail closed")
        for invalid_threshold in ("true", '"1.0"', "nan", "inf", "-inf"):
            baseline.write_text(
                baseline_text.replace("allowed_drop_percentage_points = 1.0", f"allowed_drop_percentage_points = {invalid_threshold}"),
                encoding="utf-8",
            )
            try:
                load_baselines(str(baseline))
            except ValueError:
                pass
            else:
                raise AssertionError(f"non-finite threshold {invalid_threshold} must fail closed")
        for field, invalid_values in (
            ("covered", ("true", '"90"', "90.0", "90.5")),
            ("count", ("true", '"100"', "100.0", "100.5")),
        ):
            for invalid_value in invalid_values:
                baseline.write_text(
                    baseline_text.replace(f"{field} = {90 if field == 'covered' else 100}", f"{field} = {invalid_value}", 1),
                    encoding="utf-8",
                )
                try:
                    load_baselines(str(baseline))
                except ValueError:
                    pass
                else:
                    raise AssertionError(f"non-integer baseline {field} {invalid_value} must fail closed")
        for covered, count in (
            (True, 100),
            (80, True),
            (80.0, 100),
            (80, 100.0),
            (float("nan"), 100),
            (80, float("inf")),
        ):
            try:
                compare({**current, "crates/iam": [covered, count]}, baselines, allowed_drop)
            except ValueError:
                pass
            else:
                raise AssertionError(f"invalid aggregate coverage {covered}/{count} must fail closed")
        lines = coverage_data["data"][0]["files"][0]["summary"]["lines"]
        for field, invalid_values in (
            ("covered", (True, "80", 80.0, 80.5, float("nan"), float("inf"), float("-inf"))),
            ("count", (True, "100", 100.0, 100.5, float("nan"), float("inf"), float("-inf"))),
        ):
            original = lines[field]
            for invalid_value in invalid_values:
                lines[field] = invalid_value
                coverage.write_text(json.dumps(coverage_data), encoding="utf-8")
                try:
                    load_coverage(str(coverage), str(root))
                except ValueError:
                    pass
                else:
                    raise AssertionError(f"invalid raw coverage {field} {invalid_value} must fail closed")
            lines[field] = original
        baseline.write_text(
            baseline_text.replace(
                '[crates."crates/crypto"]\ncovered = 90\ncount = 100\n',
                "",
            ),
            encoding="utf-8",
        )
        try:
            load_baselines(str(baseline))
        except ValueError:
            pass
        else:
            raise AssertionError("missing security-crate baseline must fail closed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("coverage_json", nargs="?")
    parser.add_argument("--baseline", default=".config/coverage-baselines.toml")
    parser.add_argument("--repo-root", default=os.getcwd())
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        print("security coverage self-test passed")
        return 0
    if not args.coverage_json:
        parser.error("coverage_json is required unless --self-test is used")

    try:
        current, _ = load_coverage(args.coverage_json, os.path.abspath(args.repo_root))
        allowed_drop, baselines = load_baselines(args.baseline)
        rows = compare(current, baselines, allowed_drop)
    except (OSError, ValueError, KeyError, IndexError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    print_report(rows, allowed_drop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
