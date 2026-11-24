#!/usr/bin/env python3
"""Restrict legacy crate names to reviewed, test-only S3 comparison code."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

from check_test_seams import CHAR_LITERAL, InputError

def oracle_files(config: Path) -> set[str]:
    paths = set()
    for line in config.read_text(encoding="utf-8").splitlines():
        if not line.startswith("# s3s-oracle"):
            continue
        match = re.fullmatch(r"# s3s-oracle: ((?:[A-Za-z0-9_-]+/)+[A-Za-z0-9_-]+\.rs)", line)
        if not match or match[1] in paths or "fake_s3_target" in Path(match[1]).with_suffix("").parts:
            raise InputError(f"{config}: invalid or repeated oracle entry: {line}")
        paths.add(match[1])
    if not paths:
        raise InputError(f"{config}: no reviewed S3 oracle sources")
    return paths


TEST_CFG = re.compile(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]")
FILE_TEST_CFG = re.compile(r"\s*#\s*!\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]")
ITEM = re.compile(
    r"\s*(?:pub(?:\s*\([^)]*\))?\s+)?(?:(?:async|unsafe|default|const(?=\s+(?:unsafe\s+)?fn\b))\s+)*"
    r"(mod|use|extern|type|const|static|fn|impl|struct|enum|trait|union|macro_rules)\b"
)
USE = re.compile(r"\b(?:use|extern\s+crate)\b[^;]*;", re.DOTALL)


# Keep offsets for cfg spans and diagnostics. Literal contents are separate so
# an executable logging target cannot disappear with a historical comment.
TOKEN = re.compile(r'//|/\*|(?<!\w)(?:br|cr|r)(?P<hashes>#*)"|[bc]?"|' + CHAR_LITERAL.pattern)
STRING_END = re.compile(r'(?:\\[\s\S]|[^"\\])*"')


def lexical_views(source: str) -> tuple[str, list[tuple[int, str]], list[tuple[int, int]]]:
    code = []
    strings = []
    comments = []
    cursor = 0
    while match := TOKEN.search(source, cursor):
        start, end = match.span()
        code.append(source[cursor:start])
        token = match[0]
        if token == "//":
            newline = source.find("\n", end)
            end = len(source) if newline < 0 else newline
            comments.append((start, end))
        elif token == "/*":
            depth = 1
            for delimiter in re.finditer(r"/\*|\*/", source[end:]):
                depth += 1 if delimiter[0] == "/*" else -1
                if not depth:
                    end += delimiter.end()
                    break
            if depth:
                raise InputError(f"unclosed block comment at offset {start}")
            comments.append((start, end))
        elif match["hashes"] is not None:
            delimiter = '"' + match["hashes"]
            close_at = source.find(delimiter, end)
            if close_at < 0:
                raise InputError(f"unclosed raw literal at offset {start}")
            strings.append((end, source[end:close_at]))
            end = close_at + len(delimiter)
        elif token.endswith('"'):
            close_at = STRING_END.match(source, end)
            if not close_at:
                raise InputError(f"unclosed string at offset {start}")
            strings.append((end, source[end:close_at.end() - 1]))
            end = close_at.end()
        code.append(re.sub(r"[^\n]", " ", source[start:end]))
        cursor = end
    code.append(source[cursor:])
    return "".join(code), strings, comments


def close(code: str, start: int) -> int:
    """Return the position just after a balanced group in masked Rust."""
    pairs = {"(": ")", "[": "]", "{": "}"}
    stack = [pairs[code[start]]]
    for index in range(start + 1, len(code)):
        token = code[index]
        if token in pairs:
            stack.append(pairs[token])
        elif token in ")]}":
            if stack.pop() != token:
                raise InputError(f"mismatched delimiter at byte {index}")
            if not stack:
                return index + 1
    raise InputError(f"unterminated group at byte {start}")


def test_regions(code: str) -> list[tuple[int, int]]:
    if FILE_TEST_CFG.match(code):
        return [(0, len(code))]
    regions = []
    for attr in TEST_CFG.finditer(code):
        cursor = attr.end()
        while extra := re.match(r"\s*#\s*\[", code[cursor:]):
            cursor = close(code, cursor + extra.end() - 1)
        item = ITEM.match(code, cursor)
        if not item:
            # A cfg on a field, statement, or macro argument cannot authorize the next item.
            continue
        cursor = item.end()
        if item[1] == "const" and not re.match(r"\s+(?:r#)?\w+\s*:", code[cursor:]):
            # Unknown const-qualified items may have a body and no semicolon.
            continue
        while cursor < len(code):
            char = code[cursor]
            if char in "([{":
                end = close(code, cursor)
                if char == "{" and item[1] not in {"use", "const", "static", "type"}:
                    regions.append((attr.start(), end))
                    break
                cursor = end
            elif char == ";":
                regions.append((attr.start(), cursor + 1))
                break
            elif char in ")]}":
                break
            else:
                cursor += 1
    return regions


def crate_names(packages: list[dict]) -> dict[Path, set[str]]:
    if not packages:
        raise InputError("Cargo metadata returned no workspace packages")
    return {
        Path(package["manifest_path"]).parent.resolve(): {
            (edge["rename"] or edge["name"]).replace("-", "_")
            for edge in package["dependencies"] if re.fullmatch(r"s3s(?:-.*)?", edge["name"])
        }
        for package in packages
    }


def check_file(path: str, source: str, names: set[str], oracles: set[str]) -> list[str]:
    pattern = re.compile(r"\b(?:" + "|".join(sorted(map(re.escape, names | {"s3s", "s3s_sigv4", "legacy_s3s"}))) + r")\b")
    if not pattern.search(source):
        return []
    # Have the toolchain parse the complete source, including literals. A broken
    # comment, string, or item cannot hide a reference from the lexical scope check.
    parsed = subprocess.run(
        ["rustfmt", "--edition", "2024", "--emit", "stdout", "--config", "skip_children=true"],
        input=source, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    if parsed.returncode:
        raise InputError(f"{path}: cannot parse Rust: {parsed.stderr.strip()}")
    code, literals, _ = lexical_views(source)
    uses = [match.span() for match in USE.finditer(code)]
    regions = test_regions(code) if path in oracles else []
    errors = []
    # EnvFilter directives can name a bare crate, a nested tracing target, or a
    # comma-separated target=level list. Preserve ordinary prose and wire data.
    target = re.compile(r"(?:^|,)\s*(?:" + pattern.pattern + r")(?:::[\w:]+)?(?:\s*=\s*(?:trace|debug|info|warn|error|off))?\s*(?=,|$)")
    for start, value in literals:
        if target.search(value) and not any(a <= start < b for a, b in regions):
            errors.append(f"{path}:{source.count(chr(10), 0, start) + 1}: legacy crate target in a runtime literal")
    for match in pattern.finditer(code):
        if not (code[match.end():].lstrip().startswith("::") or any(a <= match.start() < b for a, b in uses)):
            continue
        if not any(a <= match.start() < b for a, b in regions):
            errors.append(f"{path}:{source.count(chr(10), 0, match.start()) + 1}: {match[0]} outside a test-only S3 oracle")
    return errors


def manifest_errors(packages: list[dict], bans: list[dict]) -> list[str]:
    allowed = {ban["name"]: ban.get("wrappers", []) for ban in bans if isinstance(ban, dict) and "name" in ban}
    errors = []
    for package in packages:
        for edge in package["dependencies"]:
            name = edge["name"]
            if re.fullmatch(r"s3s(?:-.*)?", name) and (edge["kind"] != "dev" or package["name"] not in allowed.get(name, [])):
                errors.append(f"{package['manifest_path']}: {name} must be a dev dependency of its reviewed oracle package")
    return errors


def check(root: Path) -> list[str]:
    packages = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"], cwd=root,
    ))["packages"]
    aliases = crate_names(packages)
    config = root / "deny.toml"
    oracles = oracle_files(config)
    with config.open("rb") as handle:
        errors = manifest_errors(packages, tomllib.load(handle)["bans"]["deny"])
    names = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "*.rs"], cwd=root,
    ).decode().split("\0")
    files = sorted({name for name in names if name and (root / name).is_file()})
    if not files:
        raise InputError("git returned no Rust sources")
    for name in files:
        path = (root / name).resolve()
        owner = next((dir_ for dir_ in path.parents if dir_ in aliases), None)
        errors.extend(check_file(name, path.read_text(encoding="utf-8"), aliases.get(owner, set()), oracles))
    return errors


class OracleTests(unittest.TestCase):
    mixed = "rustfs/src/auth.rs"
    oracles = {mixed, "crates/s3-types/src/compat_s3s.rs"}

    def test_test_only_items_and_literals(self):
        source = r'''// s3s::S3
        const HISTORY: &str = r#"use legacy_s3s::Body;"#;
        #[cfg(test)] use s3s::{Body, S3};
        #[cfg(test)] #[allow(dead_code)] mod parity { use s3s_sigv4 as signing; }
        fn ordinary() {}'''
        self.assertEqual(check_file(self.mixed, source, set(), self.oracles), [])

    def test_mixed_file_and_aliases(self):
        aliases = crate_names([{"manifest_path": "/pkg/Cargo.toml", "dependencies": [
            {"name": "s3s-sigv4", "rename": "old-signer", "kind": "dev"},
        ]}])[Path("/pkg").resolve()]
        for source in (
            "#[cfg(test)] mod tests { use old_signer as old; }\nuse old_signer as other;",
            '#[cfg(any(test, feature="also"))] use {old_signer as dependency};',
            '#[cfg_attr(test, cfg(test))] extern crate old_signer;',
            "mod tests { #![cfg(test)] use old_signer::Old; }\nuse old_signer::Outside;",
            "struct Mixed { #[cfg(test)] test: u8, normal: old_signer::Old }",
            "#[cfg(test)] const fn test_only() {} use old_signer::Outside;",
        ):
            with self.subTest(source=source):
                self.assertTrue(check_file(self.mixed, source, aliases, self.oracles))

    def test_fixture_cannot_be_an_oracle(self):
        source = "#![cfg(test)]\nuse s3s::S3;"
        self.assertFalse(check_file("crates/s3-types/src/compat_s3s.rs", source, set(), self.oracles))
        self.assertTrue(check_file("crates/e2e_test/src/fake_s3_target/mod.rs", source, set(), self.oracles))
        self.assertTrue(check_file("rustfs/tests/new.rs", source, set(), self.oracles))

    def test_dev_manifest_edges_need_a_reviewed_oracle_owner(self):
        bans = [{"name": "s3s", "wrappers": ["rustfs"]}]
        for package, kind, refused in [("rustfs", "dev", False), ("other", "dev", True), ("rustfs", "build", True), ("rustfs", None, True)]:
            item = {"name": package, "manifest_path": "Cargo.toml", "dependencies": [{"name": "s3s", "rename": "old", "kind": kind}]}
            with self.subTest(package=package, kind=kind):
                self.assertEqual(bool(manifest_errors([item], bans)), refused)

    def test_fake_target_cannot_be_added_to_the_inventory(self):
        with tempfile.TemporaryDirectory() as temp:
            config = Path(temp, "deny.toml")
            for path in ("crates/e2e_test/src/fake_s3_target.rs", "crates/e2e_test/src/fake_s3_target/service.rs"):
                config.write_text(f"# s3s-oracle: {path}\n")
                with self.assertRaises(InputError):
                    oracle_files(config)

    def test_runtime_targets_and_comments(self):
        for value in ['"s3s"', 'r#"s3s::service"#', '"warn,s3s=trace,rustfs=info"']:
            with self.subTest(value=value):
                self.assertTrue(check_file("crates/obs/src/telemetry/filter.rs", f"const TARGET: &str = {value};", set(), set()))
        self.assertFalse(check_file(self.mixed, '// historical target: "s3s"\nfn present() {}', set(), set()))

    def test_unreadable_source_is_an_error(self):
        with self.assertRaises(InputError):
            check_file(self.mixed, '/* unterminated s3s::S3', set(), self.oracles)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        unittest.main(argv=[sys.argv[0]])
    try:
        errors = check(Path.cwd().resolve())
    except (InputError, OSError, UnicodeError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
        sys.exit(f"S3 oracle check failed: {exc}")
    if errors:
        sys.exit("\n".join(errors))
    print("s3s source oracle check passed")
