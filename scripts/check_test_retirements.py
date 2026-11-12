#!/usr/bin/env python3
"""Guard: every test a migration slice retires exists and names a replacement that exists.

The operation slices of the s3s-to-gateway migration (rustfs/backlog#2765) port
legacy tests rather than delete them; when a legacy test is scheduled to die
with its subject, the slice records it in its own table under
.config/test-retirements/, one file per slice, so parallel slices never edit
the same file. Each entry line is

    <crate> | <module path>::<test fn> | <kind> | <replacement> | <reason>

The retired test must exist when the entry is added: `<crate>` is a workspace
package and a file under its src/ or tests/ declares `fn <test fn>` under a
test attribute. The task that deletes retired tests deletes their entries in
the same change. <kind> says what <replacement> names:

    test          <repo path>::<fn>; the file lies under src/ or tests/ of a
                  workspace package and declares `fn <fn>` under a test attribute
    differential  <Operation>/<case>; a recorder `step("<Operation>", "<case>"`
                  call exists under crates/e2e_test/src/dual_stack_differential/
    e2e           <fn>; a file under crates/e2e_test/src/ declares `fn <fn>`
                  under a test attribute

A test attribute is `#[test]`, `#[tokio::test…]` or `#[test_case…]` (the forms
this repository uses), possibly among other attributes directly above the fn.
Comments never count, and string literals do not count for a fn. Lines
starting with '#' and blank lines are ignored. The guard fails when a table
file is missing or unknown, an entry does not have five non-empty fields, its
kind is unknown, the same retired test is listed twice (in any table), or the
retired test or its replacement does not exist. Tying the per-crate test-count
floor to these entries is deliberately not done here.

Usage:
  scripts/check_test_retirements.py [--root DIR]
  scripts/check_test_retirements.py --self-test

Exit status: 0 clean, 1 violations or missing inputs, 2 usage error.
"""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path

TABLE_DIR = ".config/test-retirements"
STEMS = (
    "bucket_core",
    "bucket_configs",
    "bucket_access",
    "bucket_replication_lifecycle",
    "object_meta",
    "listing_multipart",
    "object_read",
    "object_write",
    "object_commit",
    "object_misc",
    "t211",
    "t212",
    "cutover",
)
DIFFERENTIAL_DIR = "crates/e2e_test/src/dual_stack_differential"
E2E_DIR = "crates/e2e_test/src"
IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
CRATE = re.compile(r"[A-Za-z0-9_-]+")
TEST_PATH = re.compile(rf"{IDENT}(::{IDENT})+")
TEST_ATTRIBUTE = re.compile(r"#\[\s*(test|tokio::test|test_case(::test_case)?)\s*[\](]")


RAW_STRING = re.compile(r'r(#*)"')
CHAR_LITERAL = re.compile(r"'(?:[^'\\\n]|\\[^\n]+?)'")


def blank(text: str, strings: bool) -> str:
    """`text` with comments (and, with `strings`, string literals) replaced by spaces."""
    out, i, n = list(text), 0, len(text)
    while i < n:
        literal = True
        if text.startswith("//", i):
            end, literal = text.find("\n", i) % (n + 1), False
        elif text.startswith("/*", i):
            depth, end, literal = 1, i + 2, False
            while end < n and depth:
                depth += {"/*": 1, "*/": -1}.get(text[end : end + 2], 0)
                end += 2 if text[end : end + 2] in ("/*", "*/") else 1
        elif (raw := RAW_STRING.match(text, i)) and (i == 0 or not text[i - 1].isalnum()):
            close = text.find('"' + raw.group(1), raw.end())
            end = n if close < 0 else close + 1 + len(raw.group(1))
        elif text[i] == '"':
            end = i + 1
            while end < n and text[end] != '"':
                end += 2 if text[end] == "\\" else 1
            end += 1
        elif char := CHAR_LITERAL.match(text, i):
            end = char.end()
        else:
            i += 1
            continue
        if strings or not literal:
            out[i:end] = [ch if ch == "\n" else " " for ch in text[i:end]]
        i = end
    return "".join(out)


def declares_test_fn(code: str, name: str) -> bool:
    """Whether `code` (comments and strings blanked) declares `fn <name>` under a test attribute."""
    for match in re.finditer(rf"\bfn\s+{re.escape(name)}\b", code):
        head = re.sub(r"(\s*\b(pub(\([^)]*\))?|async|const|unsafe))*\s*$", "", code[max(0, match.start() - 4000) : match.start()])  # attributes sit right above
        while head.endswith("]"):
            depth, k = 0, len(head) - 1
            while k >= 0:
                depth += {"]": 1, "[": -1}.get(head[k], 0)
                if depth == 0:
                    break
                k -= 1
            if k < 1 or head[k - 1] != "#":
                break
            if TEST_ATTRIBUTE.match(head[k - 1 :]):
                return True
            head = head[: k - 1].rstrip()
    return False


class Tree:
    """Cached reads of the repository: package directories and blanked Rust sources."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.cache: dict[tuple[Path, bool], str] = {}
        self.packages: dict[str, Path] = {}
        for manifest in [root / "rustfs" / "Cargo.toml", *sorted(root.glob("crates/*/Cargo.toml"))]:
            if manifest.is_file():
                name = tomllib.loads(manifest.read_text()).get("package", {}).get("name")
                if isinstance(name, str):
                    self.packages[name] = manifest.parent

    def code(self, path: Path, strings: bool = True) -> str:
        key = (path, strings)
        if key not in self.cache:
            files = sorted(path.rglob("*.rs")) if path.is_dir() else [path]
            self.cache[key] = "\n".join(blank(file.read_text(), strings) for file in files if file.is_file())
        return self.cache[key]

    def in_package_sources(self, path: Path) -> bool:
        return any(path.is_relative_to(d / "src") or path.is_relative_to(d / "tests") for d in self.packages.values())


def retired_error(tree: Tree, crate: str, test: str) -> str | None:
    directory = tree.packages.get(crate)
    if directory is None:
        return f"`{crate}` is not a workspace package"
    name = test.rpartition("::")[2]
    if any(declares_test_fn(tree.code(directory / sub), name) for sub in ("src", "tests")):
        return None
    return f"retired test `fn {name}` is not a test function under {directory.relative_to(tree.root)}/src or tests"


def replacement_error(tree: Tree, kind: str, replacement: str) -> str | None:
    """Why `replacement` of `kind` does not exist, or None when it does."""
    if kind == "test":
        path, sep, name = replacement.rpartition("::")
        if not sep or not re.fullmatch(IDENT, name) or Path(path).is_absolute() or ".." in Path(path).parts:
            return f"test replacement `{replacement}` is not <repo path>::<fn>"
        file = tree.root / path
        if not file.is_file():
            return f"test replacement file `{path}` does not exist"
        if not tree.in_package_sources(file):
            return f"test replacement file `{path}` is not under src/ or tests/ of a workspace package"
        return None if declares_test_fn(tree.code(file), name) else f"`{path}` declares no test `fn {name}`"
    if kind == "differential":
        operation, sep, case = replacement.partition("/")
        if not sep or not re.fullmatch(IDENT, operation) or not case:
            return f"differential replacement `{replacement}` is not <Operation>/<case>"
        step = rf'\bstep\(\s*"{re.escape(operation)}",\s*"{re.escape(case)}"'
        found = re.search(step, tree.code(tree.root / DIFFERENTIAL_DIR, strings=False))
        return None if found else f"no differential step `{operation}`/`{case}` under {DIFFERENTIAL_DIR}"
    if not re.fullmatch(IDENT, replacement):
        return f"e2e replacement `{replacement}` is not a function name"
    found = declares_test_fn(tree.code(tree.root / E2E_DIR), replacement)
    return None if found else f"no test `fn {replacement}` under {E2E_DIR}"


def check(root: Path) -> list[str]:
    errors: list[str] = []
    table = root / TABLE_DIR
    present = {path.name for path in table.iterdir()} if table.is_dir() else set()
    expected = {f"{stem}.txt" for stem in STEMS}
    errors += [f"{TABLE_DIR}/{name}: missing" for name in sorted(expected - present)]
    errors += [f"{TABLE_DIR}/{name}: not a retirement table (known: {', '.join(STEMS)})" for name in sorted(present - expected)]
    seen: dict[tuple[str, str], str] = {}
    tree = Tree(root)
    for name in sorted(expected & present):
        for number, line in enumerate((table / name).read_text().splitlines(), 1):
            where = f"{TABLE_DIR}/{name}:{number}"
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = [part.strip() for part in line.split("|")]
            if len(fields) != 5 or not all(fields):
                errors.append(f"{where}: expected 5 non-empty fields separated by '|', found {len(fields)}")
                continue
            crate, test, kind, replacement, _reason = fields
            if not CRATE.fullmatch(crate) or not TEST_PATH.fullmatch(test):
                errors.append(f"{where}: `{crate} | {test}` is not <crate> | <module path>::<test fn>")
                continue
            if kind not in ("test", "differential", "e2e"):
                errors.append(f"{where}: unknown kind `{kind}` (test, differential or e2e)")
                continue
            if (crate, test) in seen:
                errors.append(f"{where}: `{crate} {test}` is already retired at {seen[(crate, test)]}")
                continue
            seen[(crate, test)] = where
            for problem in (retired_error(tree, crate, test), replacement_error(tree, kind, replacement)):
                if problem:
                    errors.append(f"{where}: {problem}")
    return errors


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTest)
        return 0 if unittest.TextTestRunner(verbosity=1).run(suite).wasSuccessful() else 1
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]), help="repository root")
    args = parser.parse_args(argv)
    errors = check(Path(args.root))
    for error in errors:
        print(f"error: {error}", file=sys.stderr)
    if errors:
        return 1
    print(f"test retirements OK: {len(STEMS)} tables")
    return 0


class SelfTest(unittest.TestCase):
    SEAM = "rustfs/src/app/test_seams/get_object.rs"
    TEST = f"rustfs | app::object::get::tests::legacy_get | test | {SEAM}::get_object_ported | ported"

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        for stem in STEMS:
            self.write(f"{TABLE_DIR}/{stem}.txt", "# <crate> | <module path>::<test fn> | <kind> | <replacement> | <reason>\n")
        self.write("rustfs/Cargo.toml", '[package]\nname = "rustfs"\n')
        self.write("crates/e2e_test/Cargo.toml", '[package]\nname = "e2e_test"\n')
        self.write(
            "rustfs/src/app/object/get.rs",
            "#[cfg(test)]\nmod tests {\n    #[test]\n    fn legacy_get() {}\n    #[tokio::test]\n    #[serial_test::serial]\n"
            "    async fn legacy_get_normal() {}\n    #[test_case(1)]\n    fn legacy_get_range(_: u8) {}\n}\n",
        )
        self.write(self.SEAM, "#[test]\nfn get_object_ported() {}\n")
        self.write(f"{DIFFERENTIAL_DIR}/scenarios/mod.rs", 's.step(\n    "GetObject",\n    "normal",\n    c.get_object().send(),\n)\n')
        self.write(f"{E2E_DIR}/get_object_test.rs", '#[tokio::test(flavor = "multi_thread")]\nasync fn get_object_range_e2e() {}\n')

    def write(self, path: str, text: str) -> None:
        (self.root / path).parent.mkdir(parents=True, exist_ok=True)
        (self.root / path).write_text(text)

    def retire(self, *lines: str, stem: str = "object_read") -> list[str]:
        with (self.root / TABLE_DIR / f"{stem}.txt").open("a") as table:
            table.write("".join(line + "\n" for line in lines))
        return check(self.root)

    def assertFails(self, errors: list[str], pattern: str) -> None:
        self.assertRegex(" ".join(errors), pattern)

    # -- negative cases: each must fail --

    def test_missing_table_fails(self) -> None:
        (self.root / TABLE_DIR / "cutover.txt").unlink()
        self.assertEqual(check(self.root), [f"{TABLE_DIR}/cutover.txt: missing"])

    def test_missing_directory_fails(self) -> None:
        for path in (self.root / TABLE_DIR).iterdir():
            path.unlink()
        (self.root / TABLE_DIR).rmdir()
        self.assertEqual(len(check(self.root)), len(STEMS))

    def test_unknown_table_fails(self) -> None:
        self.write(f"{TABLE_DIR}/object_reads.txt", "")
        self.assertFails(check(self.root), "object_reads.txt: not a retirement table")

    def test_four_fields_fail(self) -> None:
        self.assertFails(self.retire(self.TEST.rsplit(" | ", 1)[0]), "expected 5 non-empty fields.*found 4")

    def test_six_fields_fail(self) -> None:
        self.assertFails(self.retire(self.TEST + " | extra"), "expected 5 non-empty fields.*found 6")

    def test_empty_field_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.rsplit("|", 1)[0] + "| "), "expected 5 non-empty fields")

    def test_test_path_without_module_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.replace("app::object::get::tests::legacy_get", "legacy_get")), "is not <crate>")

    def test_unknown_kind_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.replace("| test |", "| unit |")), "unknown kind `unit`")

    def test_duplicate_in_one_table_fails(self) -> None:
        self.assertFails(self.retire(self.TEST, self.TEST), r"object_read.txt:3: .* already retired at .*object_read.txt:2")

    def test_duplicate_across_tables_fails(self) -> None:
        self.retire(self.TEST, stem="object_meta")
        self.assertFails(self.retire(self.TEST), r"object_read.txt:2: .* already retired at .*object_meta.txt:2")

    def test_retired_test_that_never_existed_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.replace("::legacy_get |", "::legacy_gone |")), "retired test `fn legacy_gone` is not a test")

    def test_retired_test_in_unknown_crate_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.replace("rustfs |", "rustfs-gone |", 1)), "`rustfs-gone` is not a workspace package")

    def test_test_replacement_file_missing_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.replace("get_object.rs", "head_object.rs")), "file `.*head_object.rs` does not exist")

    def test_test_replacement_fn_missing_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.replace("::get_object_ported", "::get_object_renamed")), "no test `fn get_object_renamed`")

    def test_test_replacement_fn_in_a_comment_fails(self) -> None:
        self.write(self.SEAM, "// #[test]\n// fn get_object_ported() {}\n/* #[test] fn get_object_ported() {} */\n")
        self.assertFails(self.retire(self.TEST), "no test `fn get_object_ported`")

    def test_test_replacement_fn_in_a_string_fails(self) -> None:
        self.write(self.SEAM, 'const S: &str = "#[test]\nfn get_object_ported() {}";\n')
        self.assertFails(self.retire(self.TEST), "no test `fn get_object_ported`")

    def test_test_replacement_fn_without_test_attribute_fails(self) -> None:
        self.write(self.SEAM, "#[cfg(test)]\nfn get_object_ported() {}\n")
        self.assertFails(self.retire(self.TEST), "no test `fn get_object_ported`")

    def test_test_replacement_outside_package_sources_fails(self) -> None:
        self.write("scripts/fixtures/get_object.rs", "#[test]\nfn get_object_ported() {}\n")
        line = self.TEST.replace(self.SEAM, "scripts/fixtures/get_object.rs")
        self.assertFails(self.retire(line), "not under src/ or tests/ of a workspace package")

    def test_test_replacement_outside_repository_fails(self) -> None:
        self.assertFails(self.retire(self.TEST.replace(self.SEAM, "../outside.rs")), "is not <repo path>::<fn>")

    def test_differential_case_missing_fails(self) -> None:
        line = "rustfs | app::object::get::tests::legacy_get | differential | GetObject/negative | ported"
        self.assertFails(self.retire(line), "no differential step `GetObject`/`negative`")

    def test_differential_operation_missing_fails(self) -> None:
        line = "rustfs | app::object::get::tests::legacy_get | differential | HeadObject/normal | ported"
        self.assertFails(self.retire(line), "no differential step `HeadObject`/`normal`")

    def test_differential_step_in_a_comment_fails(self) -> None:
        self.write(f"{DIFFERENTIAL_DIR}/scenarios/extra/object_read.rs", '// s.step("HeadObject", "normal", c.head_object().send(), none)\n')
        line = "rustfs | app::object::get::tests::legacy_get | differential | HeadObject/normal | ported"
        self.assertFails(self.retire(line), "no differential step `HeadObject`/`normal`")

    def test_e2e_fn_missing_fails(self) -> None:
        line = "rustfs | app::object::get::tests::legacy_get | e2e | get_object_range | ported"
        self.assertFails(self.retire(line), "no test `fn get_object_range` under")

    def test_e2e_fn_without_test_attribute_fails(self) -> None:
        self.write(f"{E2E_DIR}/helpers.rs", "pub async fn get_object_helper() {}\n")
        line = "rustfs | app::object::get::tests::legacy_get | e2e | get_object_helper | ported"
        self.assertFails(self.retire(line), "no test `fn get_object_helper` under")

    # -- positive cases: each must pass --

    def test_empty_tables_pass(self) -> None:
        self.assertEqual(check(self.root), [])

    def test_one_entry_of_each_kind_passes(self) -> None:
        errors = self.retire(
            "",
            "# a comment",
            self.TEST,
            "rustfs | app::object::get::tests::legacy_get_normal | differential | GetObject/normal | covered by the differential",
            "rustfs | app::object::get::tests::legacy_get_range | e2e | get_object_range_e2e | covered end to end",
        )
        self.assertEqual(errors, [])


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
