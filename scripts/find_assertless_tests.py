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
- Functions whose body calls a helper *named* like a shared check or suite
  runner: an `assert_`/`verify_`/`check_`/`expect_`/`ensure_`/`run_` prefix,
  or a `_case`/`_cases`/`_harness`/`_roundtrip` suffix. The name must carry
  the token as its own leading or trailing segment — matching it anywhere
  inside the identifier hid whole test bodies behind an unrelated domain
  call such as `record_get_object_bitrot_verify_duration(..)`.
- Functions whose body only defines an unused inner `fn _name(..)`: that is
  the compile-time shape check (exhaustive match, signature pin), where the
  type system is the assertion.

Usage:
    scripts/find_assertless_tests.py [path ...]     # default: crates rustfs/src

Exit code is always 0; the output is the queue.
"""

import re
import sys
from pathlib import Path

VERIFY_SIGNALS = re.compile(
    r"assert[a-z0-9_]*!|debug_assert|panic!\(|\.expect\(|\.unwrap\(|"
    r"unreachable!|matches!\(|insta::|proptest!|\.await\?|\)\?|\?;|should_panic"
)
DELEGATION = re.compile(
    r"\b(?:assert|verify|check|expect|ensure|run)_[a-z0-9_]*(?:::<[^>]*>)?\s*\(|"
    r"\b[a-z0-9_]+_(?:case|cases|harness|roundtrip|round_trip)(?:::<[^>]*>)?\s*\("
)

# A body whose whole content is one call delegates by construction, whatever the
# callee is named: `run(DurabilityMode::Strict).await` and
# `aborting_encode_drops_blocked_producer(EncodePipeline::Vec).await` both hand
# every assertion to a shared harness.
SINGLE_CALL_BODY = re.compile(
    r"\A\s*[a-zA-Z_][a-zA-Z0-9_:]*(?:::<[^>]*>)?\s*\([^;]*\)\s*(?:\.await\s*)?;?\s*\Z",
    re.S,
)

# A nested `fn` that is only bound and discarded is a signature guard: the type
# system is the assertion, exactly like the `fn _name()` form below.
SIGNATURE_GUARD = re.compile(r"\bfn\s+[a-zA-Z0-9_]+\s*(?:<[^>]*>)?\s*\([^;]*\)[^;]*\{", re.S)
DISCARDED_BINDING = re.compile(r"\blet\s+_\s*=\s*[a-zA-Z_][a-zA-Z0-9_]*\s*;")
# `let _ = Type::<T>::method;` — a path item referenced but never called can only
# be a signature guard; the call form (`let _ = x.foo();`) is excluded by the
# absence of parens before the semicolon.
DISCARDED_PATH_ITEM = re.compile(r"\blet\s+_\s*=\s*[a-zA-Z_][a-zA-Z0-9_]*(?:::(?:<[^>]*>|[a-zA-Z_][a-zA-Z0-9_]*))+\s*;")
COMPILE_TIME_CHECK = re.compile(r"\bfn\s+_[a-zA-Z0-9_]*\s*(?:<[^>]*>)?\s*\(")
TEST_ATTR = re.compile(r"#\[(?:tokio::)?test[\](]")
TEST_CASE_ATTR = re.compile(r"#\[test_case")
FN_LINE = re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s+([a-zA-Z0-9_]+)")



# A char literal is 'x' or '\n'; a lone `'` is a lifetime (`&'a str`), and
# consuming to the next quote on one would swallow the rest of the line.
CHAR_LITERAL = re.compile(r"'(?:[^'\\]|\\.)'")
RAW_STRING_OPEN = re.compile(r'r(#*)"')


class LiteralStripper:
    """Blanks out literals and comments so brace matching sees only code.

    Carries state across lines: Rust string literals — the JSON and `r#"..."#`
    fixtures these tests are full of — routinely span lines, and a per-line
    scanner falls out of phase on the first one. A `{` inside a string would
    otherwise unbalance the count and truncate a test body before its
    assertions.
    """

    def __init__(self) -> None:
        self.in_string = False
        self.raw_hashes = None  # None when the open string is not raw

    def feed(self, line: str) -> str:
        out = []
        i = 0
        n = len(line)
        while i < n:
            if self.in_string:
                if self.raw_hashes is not None:
                    close = '"' + "#" * self.raw_hashes
                    idx = line.find(close, i)
                    if idx == -1:
                        return "".join(out)
                    i = idx + len(close)
                    self.in_string = False
                    self.raw_hashes = None
                    continue
                if line[i] == "\\":
                    i += 2
                    continue
                if line[i] == '"':
                    self.in_string = False
                    i += 1
                    continue
                i += 1
                continue

            ch = line[i]
            if ch == "/" and i + 1 < n and line[i + 1] == "/":
                break
            m = RAW_STRING_OPEN.match(line, i)
            if m:
                self.in_string = True
                self.raw_hashes = len(m.group(1))
                i = m.end()
                continue
            if ch == '"':
                self.in_string = True
                self.raw_hashes = None
                i += 1
                continue
            if ch == "'":
                cm = CHAR_LITERAL.match(line, i)
                if cm:
                    i = cm.end()
                    continue
                out.append(ch)
                i += 1
                continue
            out.append(ch)
            i += 1
        return "".join(out)


def extract_body(text: str) -> str:
    """Return what is between the outermost braces of a scanned function."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end <= start:
        return text
    return text[start + 1 : end]


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
        stripper = LiteralStripper()
        while k < len(lines):
            for ch in stripper.feed(lines[k]):
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
        # The attribute block carries verification too: `#[should_panic(expected
        # = "...")]` makes the panic message the assertion.
        attr_text = "\n".join(attrs)
        inner = extract_body(text)
        delegates = (
            DELEGATION.search(text)
            or SINGLE_CALL_BODY.match(inner)
            or (SIGNATURE_GUARD.search(inner) and DISCARDED_BINDING.search(inner))
            or DISCARDED_PATH_ITEM.search(inner)
        )
        if not VERIFY_SIGNALS.search(text) and not VERIFY_SIGNALS.search(attr_text) and not delegates and not COMPILE_TIME_CHECK.search(text):
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
