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
    entry_method: str | None = None  # a raw-fields body shared by both production edges


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
        entry_method="execute_head_object_fields",
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
METHOD_SEAMS = {name: seam for method, seam in SEAMS.items() for name in {method, seam.entry_method or method}}
CALL = re.compile(r"\.\s*(" + "|".join(re.escape(name) for name in sorted(METHOD_SEAMS)) + r")\s*\(")
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


def module_attributes(code: str, offset: int) -> list[tuple[int, int]]:
    """Attribute spans immediately preceding a declaration, using masked code."""
    attributes = []
    end = offset
    while True:
        end = len(code[:end].rstrip())
        if end == 0 or code[end - 1] != "]":
            return attributes
        depth, start = 1, end - 2
        while start >= 0 and depth:
            depth += code[start] == "]"
            depth -= code[start] == "["
            start -= 1
        if depth:
            raise InputError("unbalanced module attribute")
        start = len(code[:start + 1].rstrip()) - 1
        if start < 0 or code[start] != "#":
            return attributes
        attributes.append((start, end))
        end = start


class ModuleDeclarations:
    """Resolve file-level external modules; inline and macro scopes are not inferred."""

    def __init__(self, root: Path, files: list[str]):
        self.root = root.resolve()
        self.sources: dict[str, tuple[str, bytearray]] = {}
        self.parents: dict[str, list[tuple[str, int]]] = {}
        pending: list[tuple[str, int, str, list[str]]] = []
        declaration = re.compile(r"\b(?:pub(?:\([^()\n]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z_0-9]*)\s*;")
        for parent in files:
            text = (root / parent).read_text()
            mask = code_mask(text)
            self.sources[parent] = (text, mask)
            code = "".join(ch if mask[i] or ch == "\n" else " " for i, ch in enumerate(text))
            depth, previous = 0, 0
            for match in declaration.finditer(code):
                for ch in code[previous:match.start()]:
                    depth += ch in "([{"
                    depth -= ch in ")]}"
                previous = match.start()
                if depth != 0:
                    continue
                paths = []
                for start, end in module_attributes(code, match.start()):
                    attribute = code[start:end]
                    if re.match(r"#\s*\[\s*path\b", attribute):
                        literal = re.fullmatch(r'#\s*\[\s*path\s*=\s*"([^"\\]*)"\s*\]', text[start:end])
                        if literal is None:
                            raise InputError(f"{parent}: unsupported module path attribute")
                        paths.append(literal.group(1))
                    elif re.search(r"\bpath\s*=", attribute):
                        raise InputError(f"{parent}: conditional module path attributes are unsupported")
                if len(paths) > 1:
                    raise InputError(f"{parent}: duplicate module path attributes")
                pending.append((parent, match.start(), match.group(1), paths))
        # rustc treats every #[path]-included file like mod.rs for its children,
        # regardless of the source filename or the declared module name.
        attributed_files = {(root / Path(parent).parent / paths[0]).resolve()
                            for parent, _, _, paths in pending if paths}
        for parent, offset, name, paths in pending:
            file = Path(parent)
            if paths:
                targets = [file.parent / paths[0]]
            else:
                crate_root = file.parent.name == "src" and file.name in ("lib.rs", "main.rs") or file.parent.name == "bin"
                mod_rs = file.name == "mod.rs" or crate_root or (root / file).resolve() in attributed_files
                directory = file.parent if mod_rs else file.with_suffix("")
                targets = [directory / f"{name}.rs", directory / name / "mod.rs"]
            if not paths and sum((root / target).is_file() for target in targets) > 1:
                raise InputError(f"{parent}: ambiguous module files for {name}")
            for target in targets:
                resolved = (root / target).resolve()
                if not resolved.is_relative_to(self.root):
                    raise InputError(f"{parent}: module path escapes the repository: {target}")
                if not resolved.is_file():
                    if paths:
                        raise InputError(f"{parent}: module path is missing: {target}")
                    continue
                relative = str(resolved.relative_to(self.root))
                self.parents.setdefault(relative, []).append((parent, offset))


def is_test_only_file(root: Path, path: str, cache: dict[str, bool], modules: ModuleDeclarations, visiting: set[str]) -> bool:
    if path in cache:
        return cache[path]
    if path in visiting:
        raise InputError(f"{path}: cycle in module declarations")
    visiting.add(path)
    try:
        parts = Path(path).parts
        if len(parts) > 1 and parts[1] in ("tests", "benches"):
            cache[path] = True
            return True
        text, mask = modules.sources[path]
        if any(text[m.start() + 1] == "!" for m in CFG_TEST.finditer(text) if mask[m.start()]):
            cache[path] = True
            return True
        file = Path(path)
        if file.name in ("lib.rs", "main.rs") and file.parent.name == "src" or file.parent.name == "bin":
            cache[path] = False
            return False
        declarations = modules.parents.get(path, [])
        if len(declarations) != 1:
            name = file.parent.name if file.name == "mod.rs" else file.stem
            raise InputError(f"{path}: expected one `mod {name};` declaration resolving to this file, found {len(declarations)}")
        parent, offset = declarations[0]
        parent_text, parent_mask = modules.sources[parent]
        cfg_test = any(start <= offset < end for start, end in test_regions(parent_text, parent_mask))
        # Validate the parent chain even when this declaration is test-only.
        parent_test = is_test_only_file(root, parent, cache, modules, visiting)
        cache[path] = cfg_test or parent_test
        return cache[path]
    finally:
        visiting.remove(path)


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
            entry = seam.entry_method or method
            calls = [m for m in CALL.finditer(text) if mask[m.start()] and m.group(1) == entry]
            if len(calls) != 1:
                errors.append(f"{seam.seam_file}: must call {entry} exactly once, found {len(calls)}")
            if entry != method and any(mask[m.start()] and m.group(1) == method for m in CALL.finditer(text)):
                errors.append(f"{seam.seam_file}: must not call {method}; call {entry} instead")
        for owner in seam.owners:
            if not (root / owner).is_file():
                errors.append(f"{owner}: owner file of {method} is missing; update SEAMS in {Path(__file__).name}")
    files = rust_files(root) if files is None else files
    cache: dict[str, bool] = {}
    modules = ModuleDeclarations(root, files)
    for path in files:
        text = (root / path).read_text()
        mask = code_mask(text)
        calls = [m for m in CALL.finditer(text) if mask[m.start()]]
        if not calls:
            continue
        whole_file = is_test_only_file(root, path, cache, modules, set())
        regions = test_regions(text, mask)
        for match in calls:
            method = match.group(1)
            seam = METHOD_SEAMS[method]
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
    "execute_head_object": "pub(crate) fn head_object(u: &U, r: R) -> F { u.execute_head_object_fields(r) }\n",
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

    def test_external_head_fields_call_fails(self):
        self.write("rustfs/src/app/storage.rs", "#[cfg(test)] fn t() { u.execute_head_object_fields(r); }")
        self.assertRegex(" ".join(self.run_check()), "test calls execute_head_object_fields directly")

    def test_head_fields_seam_missing_call_fails(self):
        self.write(SEAMS["execute_head_object"].seam_file, "fn head_object() {}")
        self.assertRegex(" ".join(self.run_check()), "must call execute_head_object_fields exactly once, found 0")

    def test_head_fields_seam_duplicate_call_fails(self):
        self.write(SEAMS["execute_head_object"].seam_file, "fn head_object() { u.execute_head_object_fields(r); u.execute_head_object_fields(r); }")
        self.assertRegex(" ".join(self.run_check()), "must call execute_head_object_fields exactly once, found 2")

    def test_head_seam_wrapper_instead_of_fields_fails(self):
        self.write(SEAMS["execute_head_object"].seam_file, "fn head_object() { u.execute_head_object(r); }")
        self.assertRegex(" ".join(self.run_check()), "must call execute_head_object_fields exactly once, found 0")

    def test_head_seam_both_entries_fails(self):
        self.write(SEAMS["execute_head_object"].seam_file, "fn head_object() { u.execute_head_object_fields(r); u.execute_head_object(r); }")
        self.assertRegex(" ".join(self.run_check()), "must not call execute_head_object")

    def test_commented_and_quoted_head_fields_calls_do_not_count(self):
        self.write(SEAMS["execute_head_object"].seam_file, 'fn head_object() { let s = "u.execute_head_object_fields(r)"; /* u.execute_head_object_fields(r); */ }')
        self.assertRegex(" ".join(self.run_check()), "must call execute_head_object_fields exactly once, found 0")

    def test_head_owners_can_call_both_entries(self):
        self.write("rustfs/src/app/object/head.rs", "#[cfg(test)] fn t() { u.execute_head_object(r); u.execute_head_object_fields(r); }")
        self.assertEqual(self.run_check(), [])

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

    def path_module(self, attributes: str = "", body: str = "fn prod() { u.execute_get_object(r); }\n"):
        self.write("rustfs/src/app/storage.rs", attributes + '#[path = "moved/mod.rs"]\nmod relocated;\n')
        self.write("rustfs/src/app/moved/mod.rs", body)

    def test_path_module_from_non_mod_file_passes(self):
        self.path_module()
        self.assertEqual(self.run_check(), [])

    def test_path_module_from_mod_file_passes(self):
        self.write("rustfs/src/app/mod.rs", self.files["rustfs/src/app/mod.rs"] + '#[path = "elsewhere.rs"]\nmod renamed;\n')
        self.write("rustfs/src/app/elsewhere.rs", "fn prod() { u.execute_get_object(r); }\n")
        self.assertEqual(self.run_check(), [])

    def test_path_module_child_uses_its_file_directory(self):
        self.path_module(body="mod child;\n")
        self.write("rustfs/src/app/moved/child.rs", "fn prod() { u.execute_get_object(r); }\n")
        self.assertEqual(self.run_check(), [])

    def test_non_mod_file_default_child_uses_module_directory(self):
        self.write("rustfs/src/app/storage.rs", "mod child;\n")
        self.write("rustfs/src/app/storage/child.rs", "fn prod() { u.execute_get_object(r); }\n")
        self.assertEqual(self.run_check(), [])

    def test_attributed_non_mod_file_child_is_a_sibling(self):
        self.write("rustfs/src/app/storage.rs", '#[path = "other.rs"]\nmod renamed;\n')
        self.write("rustfs/src/app/other.rs", "mod child;\n")
        self.write("rustfs/src/app/child.rs", "fn prod() { u.execute_get_object(r); }\n")
        self.assertEqual(self.run_check(), [])

    def test_attributed_non_mod_file_child_does_not_use_stem_directory(self):
        self.write("rustfs/src/app/storage.rs", '#[path = "other.rs"]\nmod renamed;\n')
        self.write("rustfs/src/app/other.rs", "mod child;\n")
        self.write("rustfs/src/app/other/child.rs", "fn prod() { u.execute_get_object(r); }\n")
        with self.assertRaisesRegex(InputError, "expected one `mod child;`.*found 0"):
            self.run_check()

    def test_attributed_non_mod_file_test_ancestor_is_preserved(self):
        self.write("rustfs/src/app/storage.rs", '#[cfg(test)]\n#[path = "other.rs"]\nmod renamed;\n')
        self.write("rustfs/src/app/other.rs", "mod child;\n")
        self.write("rustfs/src/app/child.rs", "fn t() { u.execute_get_object(r); }\n")
        self.assertRegex(self.run_check()[0], r"app/child.rs:1: test calls execute_get_object directly")

    def test_two_conventional_module_files_are_ambiguous(self):
        self.write("rustfs/src/app/storage.rs", "mod child;\n")
        self.write("rustfs/src/app/storage/child.rs", "fn prod() { u.execute_get_object(r); }\n")
        self.write("rustfs/src/app/storage/child/mod.rs", "fn prod() {}\n")
        with self.assertRaisesRegex(InputError, "ambiguous module files for child"):
            self.run_check()

    def test_path_module_cfg_test_still_rejects_direct_call(self):
        self.path_module("#[cfg(test)]\n")
        self.assertRegex(self.run_check()[0], r"moved/mod.rs:1: test calls execute_get_object directly")

    def test_path_module_test_ancestor_still_rejects_direct_call(self):
        self.path_module(body="mod child;\n")
        self.write("rustfs/src/app/mod.rs", self.files["rustfs/src/app/mod.rs"].replace("mod storage;", "#[cfg(test)]\nmod storage;"))
        self.write("rustfs/src/app/moved/child.rs", "fn t() { u.execute_get_object(r); }\n")
        self.assertRegex(self.run_check()[0], r"moved/child.rs:1: test calls execute_get_object directly")

    def test_path_module_test_using_seam_passes(self):
        self.path_module("#[cfg(test)]\n", "fn t() { crate::app::test_seams::get_object(&u, r); }\n")
        self.assertEqual(self.run_check(), [])

    def test_path_module_missing_target_fails(self):
        self.path_module()
        (self.root / "rustfs/src/app/moved/mod.rs").unlink()
        del self.files["rustfs/src/app/moved/mod.rs"]
        with self.assertRaisesRegex(InputError, "module path is missing"):
            self.run_check()

    def test_path_module_duplicate_declarations_fail(self):
        self.path_module()
        self.write("rustfs/src/app/storage.rs", self.files["rustfs/src/app/storage.rs"] + '#[path = "moved/mod.rs"]\nmod second;\n')
        with self.assertRaisesRegex(InputError, "found 2"):
            self.run_check()

    def test_conventional_and_explicit_owners_are_ambiguous(self):
        self.write("rustfs/src/app/storage.rs", "mod child;\n")
        self.write("rustfs/src/app/storage/child.rs", "fn prod() { u.execute_get_object(r); }\n")
        self.write("rustfs/src/app/mod.rs", self.files["rustfs/src/app/mod.rs"] + '#[path = "storage/child.rs"]\nmod alias;\n')
        with self.assertRaisesRegex(InputError, "expected one `mod child;`.*found 2"):
            self.run_check()

    def test_path_module_normalized_target_and_same_line_attributes(self):
        self.path_module()
        self.write("rustfs/src/app/storage.rs", '#[cfg(test)] #[path = "./moved/../moved/mod.rs"] pub(crate) mod renamed;\n')
        self.assertRegex(self.run_check()[0], r"moved/mod.rs:1: test calls execute_get_object directly")

    def test_path_module_does_not_declare_conventional_target(self):
        self.write("rustfs/src/app/storage.rs", '#[cfg(test)]\n#[path = "actual.rs"]\nmod nominal;\n')
        self.write("rustfs/src/app/actual.rs", "fn actual() {}\n")
        self.write("rustfs/src/app/storage/nominal.rs", "fn t() { u.execute_get_object(r); }\n")
        with self.assertRaisesRegex(InputError, "expected one `mod nominal;`.*found 0"):
            self.run_check()

    def test_path_module_wrong_existing_target_leaves_file_undeclared(self):
        self.path_module()
        self.write("rustfs/src/app/elsewhere.rs", "fn elsewhere() {}\n")
        self.write("rustfs/src/app/storage.rs", '#[path = "elsewhere.rs"]\nmod relocated;\n')
        with self.assertRaisesRegex(InputError, "expected one `mod moved;`.*found 0"):
            self.run_check()

    def test_path_module_fake_declarations_are_ignored(self):
        self.path_module()
        self.write("rustfs/src/app/storage.rs", self.files["rustfs/src/app/storage.rs"] +
                   '// #[path = "moved/mod.rs"] mod fake;\n'
                   '/* #[path = "moved/mod.rs"] mod fake; */\n'
                   'const S: &str = r#"#[path = "moved/mod.rs"] mod fake;"#;\n')
        self.assertEqual(self.run_check(), [])

    def test_path_module_inline_and_macro_scopes_are_not_inferred(self):
        for wrapper in ("mod inline {\n%s}\n", "some_macro! {\n%s}\n"):
            with self.subTest(wrapper=wrapper):
                self.path_module()
                self.write("rustfs/src/app/storage.rs", wrapper % self.files["rustfs/src/app/storage.rs"])
                with self.assertRaisesRegex(InputError, "found 0"):
                    self.run_check()

    def test_path_module_escape_is_rejected(self):
        self.write("rustfs/src/app/storage.rs", '#[path = "../../../../outside.rs"]\nmod escaped;\n')
        with self.assertRaisesRegex(InputError, "escapes the repository"):
            self.run_check()

    def test_path_module_unsupported_literal_is_rejected(self):
        self.write("rustfs/src/app/storage.rs", '#[path = "moved\\x2fmod.rs"]\nmod escaped;\n')
        with self.assertRaisesRegex(InputError, "unsupported module path"):
            self.run_check()

    def test_path_module_conditional_path_is_rejected(self):
        self.write("rustfs/src/app/storage.rs", '#[cfg_attr(test, path = "moved.rs")]\nmod conditional;\n')
        with self.assertRaisesRegex(InputError, "conditional module path"):
            self.run_check()

    def test_path_module_duplicate_attributes_are_rejected(self):
        self.path_module('#[path = "moved/mod.rs"]\n')
        with self.assertRaisesRegex(InputError, "duplicate module path attributes"):
            self.run_check()

    def test_path_module_parent_cycle_is_rejected(self):
        self.write("rustfs/src/app/a.rs", '#[path = "b.rs"]\nmod b;\nfn prod() { u.execute_get_object(r); }\n')
        self.write("rustfs/src/app/b.rs", '#[path = "a.rs"]\nmod a;\n')
        with self.assertRaisesRegex(InputError, "cycle in module declarations"):
            self.run_check()

    def test_calls_in_comments_and_strings_are_ignored(self):
        self.write("rustfs/src/app/storage.rs", '#[cfg(test)]\nmod tests {\n    // u.execute_get_object(r)\n    const S: &str = "u.execute_get_object(r)";\n}\n')
        self.assertEqual(self.run_check(), [])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
