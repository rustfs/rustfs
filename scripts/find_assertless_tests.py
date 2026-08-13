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
"""Census of assertion-less tests (rustfs/backlog#1836 PR3).

Flags `#[test]` / `#[tokio::test]` functions whose bodies contain no
verification signal: no assert!/assert_eq!/assert_ne!/panic! macro, no
`.expect(`/`.unwrap(`, no `?` operator, no `#[should_panic]`, and no
`insta` snapshot / proptest / matches! usage. Such a test is green no
matter what the code under test does.

This is a heuristic REVIEW QUEUE, not a lint: a hit still needs human
reading before it is fixed or deleted, because assertions may live in a
called helper. Known false-positive classes are excluded up front:

- `#[test_case(...)]`-driven functions (the values are the assertion's
  parameters; the assert lives in the shared body — still scanned, but a
  body that asserts is not flagged anyway; the exclusion covers wrappers
  that only delegate to a suite runner).
- Functions whose body calls a helper with `assert`, `verify`, `check`,
  `expect`, `run_` or `_case` in its name (suite-delegation pattern).

Usage:
    scripts/find_assertless_tests.py [path ...]     # default: crates rustfs/src

Exit code is always 0; the output is the queue.
"""

import re
import sys
from pathlib import Path

VERIFY_SIGNALS = re.compile(
    r"assert!|assert_eq!|assert_ne!|debug_assert|panic!\(|\.expect\(|\.unwrap\(|"
    r"unreachable!|matches!\(|insta::|proptest!|\.await\?|\)\?|\?;|should_panic"
)
DELEGATION = re.compile(r"\b[a-z0-9_]*(?:assert|verify|check|expect|run_case|_case|harness|round_trip|roundtrip)[a-z0-9_]*\s*\(")
TEST_ATTR = re.compile(r"#\[(?:tokio::)?test[\](]")
TEST_CASE_ATTR = re.compile(r"#\[test_case")
FN_LINE = re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s+([a-zA-Z0-9_]+)")


def scan_file(path: Path):
    try:
        lines = path.read_text(encoding="utf-8").split("\n")
    except (UnicodeDecodeError, OSError):
        return
    i = 0
    while i < len(lines):
        if not TEST_ATTR.search(lines[i]):
            i += 1
            continue
        # collect the whole attribute block (may include #[serial], #[test_case], ...)
        attrs = []
        j = i
        while j < len(lines) and (lines[j].strip().startswith("#[") or lines[j].strip().startswith("//")):
            attrs.append(lines[j])
            j += 1
        if j >= len(lines):
            break
        m = FN_LINE.match(lines[j])
        if not m:
            i = j + 1
            continue
        name = m.group(1)
        if any(TEST_CASE_ATTR.search(a) for a in attrs):
            i = j + 1
            continue
        # brace-match the body
        depth = 0
        begun = False
        body = []
        k = j
        while k < len(lines):
            for ch in lines[k]:
                if ch == "{":
                    depth += 1
                    begun = True
                elif ch == "}":
                    depth -= 1
            body.append(lines[k])
            if begun and depth <= 0:
                break
            k += 1
        text = "\n".join(body)
        if not VERIFY_SIGNALS.search(text) and not DELEGATION.search(text):
            print(f"{path}:{j + 1}: {name}")
        i = k + 1


def main():
    roots = [Path(p) for p in (sys.argv[1:] or ["crates", "rustfs/src"])]
    for root in roots:
        for path in sorted(root.rglob("*.rs")):
            if "target" in path.parts:
                continue
            scan_file(path)


if __name__ == "__main__":
    main()
