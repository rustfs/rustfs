#!/usr/bin/env python3
"""Guard: tests outside an operation's owning family call its use-case only through the test seam.

Preparatory refactor P4 of the s3s-to-gateway migration. Each migration batch
changes the use-case signatures of its own operations. Tests owned by another
family that call those entry points as an oracle would break and pull a second
batch into the same file, so they call the `#[cfg(test)]` seams in
rustfs/src/app/test_seams/ instead; the owning batch then edits one seam body.

For every entry point in SEAMS this guard fails when

  * a direct method call `.execute_<op>(` appears in test code (a module or
    item under `#[cfg(test)]`, a file whose module declaration or an ancestor's
    is under `#[cfg(test)]`, or any file under rustfs/tests/ or rustfs/benches/)
    in a file that is neither the seam nor one of the operation's owner files;
  * the seam file is missing, or no longer calls the entry point exactly once;
  * an owner file listed below does not exist (a stale table must not widen
    what passes);
  * a file holding a direct call cannot be placed in the module tree;
  * no Rust file is found at all.

Production calls (the legacy S3 trait, admin and metadata routes) are not test
code and are not judged here. Comments and string literals are ignored.

Usage:
  scripts/check_test_seams.py
  scripts/check_test_seams.py --self-test

Exit status: 0 clean, 1 violations or missing inputs, 2 usage error.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

SCAN_ROOT = "rustfs"


@dataclass(frozen=True)
class Seam:
    batch: str
    seam_file: str
    owners: tuple[str, ...]  # files that own the operation; each must exist
    owner_dirs: tuple[str, ...] = ()  # directories a split of an owner file may create


SEAMS = {
    "execute_get_object": Seam(
        batch="C",
        seam_file="rustfs/src/app/test_seams/get_object.rs",
        owners=(
            "rustfs/src/app/object/get.rs",
            "rustfs/src/app/object/head.rs",
            "rustfs/src/app/object/conditional_read_bench.rs",
            "rustfs/src/app/object/on_demand_migration_put.rs",
        ),
    ),
    "execute_head_object": Seam(
        batch="C",
        seam_file="rustfs/src/app/test_seams/head_object.rs",
        owners=(
            "rustfs/src/app/object/get.rs",
            "rustfs/src/app/object/head.rs",
        ),
    ),
    "execute_list_objects_v2": Seam(
        batch="B2",
        seam_file="rustfs/src/app/test_seams/list_objects_v2.rs",
        owners=("rustfs/src/app/bucket_usecase.rs",),
        # P2 splits bucket_usecase.rs into bucket_usecase/*.rs; narrow this to the listing file once it exists.
        owner_dirs=("rustfs/src/app/bucket_usecase/",),
    ),
    "execute_put_object": Seam(
        batch="D",
        seam_file="rustfs/src/app/test_seams/put_object.rs",
        owners=(
            "rustfs/src/app/object/put.rs",
            "rustfs/src/app/lifecycle_transition_api_test/put.rs",
        ),
    ),
}

CFG_TEST = re.compile(r"#!?\[\s*cfg\s*\(\s*(?:all\s*\(\s*)?test\b")
TEST_ATTR = re.compile(r"#\[\s*(?:tokio::)?test\b")
CALL = re.compile(r"\.\s*(execute_get_object|execute_head_object|execute_list_objects_v2|execute_put_object)\s*\(")
CHAR_LITERAL = re.compile(r"'(?:\\(?:[nrt\\0'\"]|x[0-9a-fA-F]{2}|u\{[0-9a-fA-F_]{1,6}\})|[^\\'\n])'")
RAW_STRING = re.compile(r'b?r(#*)"')


class InputError(Exception):
    """A required input is missing or cannot be interpreted."""


def code_mask(text: str) -> bytearray:
    """1 for every character that is Rust code, 0 inside comments and literals."""
    mask = bytearray(len(text))
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        if text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            i = j
            continue
        raw = RAW_STRING.match(text, i)
        if raw and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            end = text.find('"' + raw.group(1), raw.end())
            i = n if end < 0 else end + 1 + len(raw.group(1))
            continue
        if c == '"' or (text.startswith('b"', i) and (i == 0 or not text[i - 1].isalnum())):
            j = i + (2 if c == "b" else 1)
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            i = j + 1
            continue
        if c == "'":
            char = CHAR_LITERAL.match(text, i)
            if char:
                i = char.end()
                continue
        mask[i] = 1
        i += 1
    return mask


def test_regions(text: str, mask: bytearray) -> list[tuple[int, int]]:
    """Spans of items under `#[cfg(test)]` or `#[test]`/`#[tokio::test]` (the whole file for `#![cfg(test)]`)."""
    regions = []
    for match in [*CFG_TEST.finditer(text), *TEST_ATTR.finditer(text)]:
        if not mask[match.start()]:
            continue
        if text[match.start() + 1] == "!":
            return [(0, len(text))]
        regions.append((match.start(), _item_end(text, mask, match.start())))
    return regions


def _item_end(text: str, mask: bytearray, attr_start: int) -> int:
    """End offset of the item an attribute at attr_start applies to."""
    depth, i, n = 0, attr_start, len(text)
    in_attr = True
    while i < n:
        if mask[i]:
            ch = text[i]
            if ch in "([{":
                if ch == "{" and depth == 0 and not in_attr:
                    return _matching(text, mask, i) + 1
                depth += 1
            elif ch in ")]}":
                depth -= 1
                if depth == 0 and ch == "]":
                    in_attr = False
            elif ch == ";" and depth == 0 and not in_attr:
                return i + 1
            elif ch == "#" and depth == 0:
                in_attr = True
        i += 1
    raise InputError(f"unterminated item after attribute at offset {attr_start}")


def _matching(text: str, mask: bytearray, open_pos: int) -> int:
    depth = 0
    for i in range(open_pos, len(text)):
        if mask[i]:
            depth += text[i] in "([{"
            depth -= text[i] in ")]}"
            if depth == 0:
                return i
    raise InputError(f"unbalanced brace at offset {open_pos}")


def module_declaration(root: Path, path: str) -> tuple[list[str], str] | None:
    """(candidate parent files, module name) for a file below src/, or None for a crate root."""
    parts = Path(path)
    if parts.name in ("lib.rs", "main.rs") and parts.parent.name == "src":
        return None
    if parts.name == "mod.rs":
        name, directory = parts.parent.name, parts.parent.parent
    else:
        name, directory = parts.stem, parts.parent
    if directory.name == "bin" and parts.name != "mod.rs":
        return None
    candidates = [directory.with_suffix(".rs"), directory / "mod.rs"]
    if directory.name == "src":
        candidates = [directory / "lib.rs", directory / "main.rs"]
    return [str(parent) for parent in candidates if (root / parent).is_file()], name


def is_test_only_file(root: Path, path: str, cache: dict[str, bool]) -> bool:
    if path in cache:
        return cache[path]
    parts = Path(path).parts
    if len(parts) > 1 and parts[1] in ("tests", "benches"):
        cache[path] = True
        return True
    text = (root / path).read_text()
    mask = code_mask(text)
    if any(text[m.start() + 1] == "!" for m in CFG_TEST.finditer(text) if mask[m.start()]):
        cache[path] = True
        return True
    declaration = module_declaration(root, path)
    if declaration is None:
        if Path(path).name in ("lib.rs", "main.rs") or Path(path).parent.name == "bin":
            cache[path] = False
            return False
        raise InputError(f"{path}: cannot find the file that declares this module")
    parents, name = declaration
    declarations = []
    for parent in parents:
        parent_text = (root / parent).read_text()
        parent_mask = code_mask(parent_text)
        pattern = rf"(?m)^[ \t]*(?:pub(?:\([^)\n]*\))?[ \t]+)?mod[ \t]+{re.escape(name)}[ \t]*;"
        for m in re.finditer(pattern, parent_text):
            if parent_mask[m.start() + len(m.group()) - len(m.group().lstrip())]:
                declarations.append((parent, parent_text, parent_mask, m.start()))
    if len(declarations) != 1:
        raise InputError(f"{path}: expected one `mod {name};` in {' or '.join(parents) or 'a parent file'}, found {len(declarations)}")
    parent, parent_text, parent_mask, offset = declarations[0]
    cfg_test = any(start <= offset < end for start, end in test_regions(parent_text, parent_mask))
    result = cfg_test or is_test_only_file(root, parent, cache)
    cache[path] = result
    return result


def rust_files(root: Path) -> list[str]:
    listed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "--", f"{SCAN_ROOT}/*.rs"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if listed.returncode != 0:
        raise InputError(f"git ls-files failed: {listed.stderr.strip()}")
    files = sorted(line for line in listed.stdout.splitlines() if line and (root / line).is_file())
    if not files:
        raise InputError(f"no Rust files found under {SCAN_ROOT}/")
    return files


def check(root: Path, files: list[str] | None = None) -> list[str]:
    errors: list[str] = []
    for method, seam in SEAMS.items():
        seam_path = root / seam.seam_file
        if not seam_path.is_file():
            errors.append(f"{seam.seam_file}: seam for {method} is missing")
        else:
            text = seam_path.read_text()
            mask = code_mask(text)
            calls = [m for m in CALL.finditer(text) if mask[m.start()] and m.group(1) == method]
            if len(calls) != 1:
                errors.append(f"{seam.seam_file}: must call {method} exactly once, found {len(calls)}")
        for owner in seam.owners:
            if not (root / owner).is_file():
                errors.append(f"{owner}: owner file of {method} is missing; update SEAMS in {Path(__file__).name}")
    files = rust_files(root) if files is None else files
    cache: dict[str, bool] = {}
    for path in files:
        text = (root / path).read_text()
        mask = code_mask(text)
        calls = [m for m in CALL.finditer(text) if mask[m.start()]]
        if not calls:
            continue
        whole_file = is_test_only_file(root, path, cache)
        regions = test_regions(text, mask)
        for match in calls:
            method = match.group(1)
            seam = SEAMS[method]
            if path == seam.seam_file or path in seam.owners or path.startswith(seam.owner_dirs or ("\0",)):
                continue
            if whole_file or any(start <= match.start() < end for start, end in regions):
                line = text.count("\n", 0, match.start()) + 1
                errors.append(
                    f"{path}:{line}: test calls {method} directly; batch {seam.batch} owns it, "
                    f"call crate::app::test_seams::{method.removeprefix('execute_')} instead"
                )
    return errors


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    if argv:
        print(__doc__.split("Usage:", 1)[1].split("\n\n", 1)[0], file=sys.stderr)
        return 2
    root = Path(__file__).resolve().parent.parent
    try:
        errors = check(root)
    except InputError as error:
        print(f"test seam guard: {error}", file=sys.stderr)
        return 1
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        print(f"test seam guard failed: {len(errors)} finding(s)", file=sys.stderr)
        return 1
    print("test seam guard passed")
    return 0


# ---------------------------------------------------------------------------
# Self-test


SEAM_BODY = {
    "execute_get_object": "pub(crate) fn get_object(u: &U, r: R) -> F { u.execute_get_object(r) }\n",
    "execute_head_object": "pub(crate) fn head_object(u: &U, r: R) -> F { u.execute_head_object(r) }\n",
    "execute_list_objects_v2": "pub(crate) fn list_objects_v2(u: &U, r: R) -> F { u.execute_list_objects_v2(r) }\n",
    "execute_put_object": "pub(crate) fn put_object(u: &U, f: &Fs, r: R) -> F { u.execute_put_object(f, r) }\n",
}


class SelfTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.files: dict[str, str] = {}
        self.write("rustfs/src/main.rs", "mod app;\n")
        self.write(
            "rustfs/src/app/mod.rs",
            "mod object;\nmod bucket_usecase;\nmod lifecycle_transition_api_test;\nmod storage;\n"
            "#[cfg(test)]\nmod test_seams;\n#[cfg(test)]\nmod gating_test;\n",
        )
        self.write("rustfs/src/app/object/mod.rs", "mod get;\nmod head;\nmod put;\n#[cfg(test)]\nmod conditional_read_bench;\nmod on_demand_migration_put;\n")
        self.write("rustfs/src/app/test_seams/mod.rs", "mod get_object;\nmod head_object;\nmod list_objects_v2;\nmod put_object;\n")
        for method, seam in SEAMS.items():
            self.write(seam.seam_file, SEAM_BODY[method])
            for owner in seam.owners:
                if owner not in self.files:
                    self.write(owner, "fn owner() {}\n")
        self.write("rustfs/src/app/lifecycle_transition_api_test/mod.rs", "mod put;\nmod delete;\n")
        self.write("rustfs/src/app/storage.rs", "fn prod(u: &U) { u.execute_get_object(r); }\n")

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, path: str, text: str):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        self.files[path] = text

    def run_check(self) -> list[str]:
        return check(self.root, sorted(p for p in self.files if p.endswith(".rs")))

    # -- negative cases: each must fail --

    def test_direct_call_in_cfg_test_module_of_non_owner_fails(self):
        self.write("rustfs/src/app/storage.rs", "fn prod() {}\n#[cfg(test)]\nmod tests {\n    fn t() { u.execute_get_object(r); }\n}\n")
        self.assertEqual(len(self.run_check()), 1)

    def test_direct_call_split_across_lines_fails(self):
        self.write("rustfs/src/app/storage.rs", "#[cfg(test)]\nmod tests {\n    fn t() { usecase\n        .execute_head_object(r); }\n}\n")
        self.assertRegex(self.run_check()[0], r"storage\.rs:4: test calls execute_head_object directly")

    def test_direct_call_in_cfg_test_helper_fn_fails(self):
        self.write("rustfs/src/app/storage.rs", "#[cfg(test)]\n#[allow(dead_code)]\nfn helper() { u.execute_list_objects_v2(r); }\n")
        self.assertEqual(len(self.run_check()), 1)

    def test_file_declared_under_cfg_test_fails(self):
        self.write("rustfs/src/app/gating_test.rs", "fn t() { u.execute_put_object(&fs, r); }\n")
        self.assertEqual(len(self.run_check()), 1)

    def test_file_whose_ancestor_is_cfg_test_fails(self):
        self.write("rustfs/src/app/test_seams/extra.rs", "fn t() { u.execute_get_object(r); }\n")
        self.write("rustfs/src/app/test_seams/mod.rs", self.files["rustfs/src/app/test_seams/mod.rs"] + "mod extra;\n")
        self.assertEqual(len(self.run_check()), 1)

    def test_put_from_non_owner_lifecycle_file_fails(self):
        self.write("rustfs/src/app/lifecycle_transition_api_test/delete.rs", "#[test]\nfn t() { u.execute_put_object(&fs, r); }\n")
        self.write("rustfs/src/app/mod.rs", self.files["rustfs/src/app/mod.rs"].replace("mod lifecycle", "#[cfg(test)]\nmod lifecycle"))
        self.assertEqual(len(self.run_check()), 1)

    def test_direct_call_in_test_fn_without_cfg_test_fails(self):
        self.write("rustfs/src/app/storage.rs", "#[tokio::test]\nasync fn t() { u.execute_get_object(r).await; }\n")
        self.assertEqual(len(self.run_check()), 1)

    def test_integration_test_file_fails(self):
        self.write("rustfs/tests/oracle.rs", "fn t() { u.execute_get_object(r); }\n")
        self.assertEqual(len(self.run_check()), 1)

    def test_missing_seam_fails(self):
        (self.root / SEAMS["execute_get_object"].seam_file).unlink()
        del self.files[SEAMS["execute_get_object"].seam_file]
        self.assertRegex(" ".join(self.run_check()), "seam for execute_get_object is missing")

    def test_seam_that_stopped_wrapping_fails(self):
        self.write(SEAMS["execute_put_object"].seam_file, "pub(crate) fn put_object() { todo!() }\n")
        self.assertRegex(" ".join(self.run_check()), "must call execute_put_object exactly once, found 0")

    def test_missing_owner_file_fails(self):
        owner = SEAMS["execute_put_object"].owners[0]
        (self.root / owner).unlink()
        del self.files[owner]
        self.assertRegex(" ".join(self.run_check()), "owner file of execute_put_object is missing")

    def test_undeclared_file_with_direct_call_fails_loudly(self):
        self.write("rustfs/src/app/orphan.rs", "fn t() { u.execute_get_object(r); }\n")
        with self.assertRaisesRegex(InputError, "expected one `mod orphan;`"):
            self.run_check()

    def test_no_rust_files_fails(self):
        empty = Path(self.tmp.name) / "empty"
        empty.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=empty, check=True)
        with self.assertRaisesRegex(InputError, "no Rust files found"):
            rust_files(empty)

    # -- positive cases: each must pass --

    def test_seam_call_in_non_owner_test_passes(self):
        self.write("rustfs/src/app/storage.rs", "#[cfg(test)]\nmod tests {\n    fn t() { crate::app::test_seams::get_object(&u, r); }\n}\n")
        self.assertEqual(self.run_check(), [])

    def test_owner_test_and_production_calls_pass(self):
        self.write("rustfs/src/app/object/put.rs", "fn prod() {}\n#[cfg(test)]\nmod tests {\n    fn t() { u.execute_put_object(&fs, r); }\n}\n")
        self.assertEqual(self.run_check(), [])

    def test_calls_in_comments_and_strings_are_ignored(self):
        self.write("rustfs/src/app/storage.rs", '#[cfg(test)]\nmod tests {\n    // u.execute_get_object(r)\n    const S: &str = "u.execute_get_object(r)";\n}\n')
        self.assertEqual(self.run_check(), [])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
