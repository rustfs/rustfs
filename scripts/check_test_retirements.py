#!/usr/bin/env python3
"""Guard: every test a migration slice retires names a replacement that exists.

The operation slices of the s3s-to-gateway migration (rustfs/backlog#2765) port
legacy tests rather than delete them; when a legacy test is scheduled to die
with its subject, the slice records it in its own table under
.config/test-retirements/, one file per slice, so parallel slices never edit
the same file. Each entry line is

    <crate> | <module path>::<test fn> | <kind> | <replacement> | <reason>

and <kind> says what <replacement> names:

    test          <repo path>::<fn>; that file declares `fn <fn>`
    differential  <Operation>/<case>; a recorder step for that operation and
                  case exists under crates/e2e_test/src/dual_stack_differential/
    e2e           <fn>; a file under crates/e2e_test/src/ declares `fn <fn>`

Lines starting with '#' and blank lines are ignored. The guard fails when a
table file is missing or unknown, an entry does not have five non-empty fields,
its kind is unknown, the same retired test is listed twice (in any table), or
its replacement does not exist. An entry whose retired test still exists
passes: it is scheduled to die with its subject. Tying the per-crate test-count
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
TEST_PATH = re.compile(rf"{IDENT}(::{IDENT})*")


def rust_text(root: Path, directory: str) -> str:
    return "\n".join(path.read_text() for path in sorted((root / directory).rglob("*.rs")))


def declares_fn(text: str, name: str) -> bool:
    return re.search(rf"\bfn\s+{re.escape(name)}\b", text) is not None


def replacement_error(root: Path, kind: str, replacement: str, cache: dict[str, str]) -> str | None:
    """Why `replacement` of `kind` does not exist, or None when it does."""
    if kind == "test":
        path, sep, name = replacement.rpartition("::")
        if not sep or not re.fullmatch(IDENT, name) or Path(path).is_absolute() or ".." in Path(path).parts:
            return f"test replacement `{replacement}` is not <repo path>::<fn>"
        file = root / path
        if not file.is_file():
            return f"test replacement file `{path}` does not exist"
        return None if declares_fn(file.read_text(), name) else f"`{path}` declares no `fn {name}`"
    if kind == "differential":
        operation, sep, case = replacement.partition("/")
        if not sep or not re.fullmatch(IDENT, operation) or not case:
            return f"differential replacement `{replacement}` is not <Operation>/<case>"
        text = cache.setdefault(DIFFERENTIAL_DIR, rust_text(root, DIFFERENTIAL_DIR))
        step = rf'\bstep\(\s*"{re.escape(operation)}",\s*"{re.escape(case)}"'
        return None if re.search(step, text) else f"no differential step `{operation}`/`{case}` under {DIFFERENTIAL_DIR}"
    if not re.fullmatch(IDENT, replacement):
        return f"e2e replacement `{replacement}` is not a function name"
    text = cache.setdefault(E2E_DIR, rust_text(root, E2E_DIR))
    return None if declares_fn(text, replacement) else f"no `fn {replacement}` under {E2E_DIR}"


def check(root: Path) -> list[str]:
    errors: list[str] = []
    table = root / TABLE_DIR
    present = {path.name for path in table.iterdir()} if table.is_dir() else set()
    expected = {f"{stem}.txt" for stem in STEMS}
    errors += [f"{TABLE_DIR}/{name}: missing" for name in sorted(expected - present)]
    errors += [f"{TABLE_DIR}/{name}: not a retirement table (known: {', '.join(STEMS)})" for name in sorted(present - expected)]
    seen: dict[tuple[str, str], str] = {}
    cache: dict[str, str] = {}
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
            if not CRATE.fullmatch(crate) or not TEST_PATH.fullmatch(test) or "::" not in test:
                errors.append(f"{where}: `{crate} | {test}` is not <crate> | <module path>::<test fn>")
                continue
            if kind not in ("test", "differential", "e2e"):
                errors.append(f"{where}: unknown kind `{kind}` (test, differential or e2e)")
                continue
            if (crate, test) in seen:
                errors.append(f"{where}: `{crate} {test}` is already retired at {seen[(crate, test)]}")
                continue
            seen[(crate, test)] = where
            problem = replacement_error(root, kind, replacement, cache)
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
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        for stem in STEMS:
            self.write(f"{TABLE_DIR}/{stem}.txt", "# <crate> | <module path>::<test fn> | <kind> | <replacement> | <reason>\n")
        self.write("rustfs/src/app/test_seams/get_object.rs", "#[test]\nfn get_object_ported() {}\n")
        self.write(f"{DIFFERENTIAL_DIR}/scenarios/mod.rs", 's.step(\n    "GetObject",\n    "normal",\n    c.get_object().send(),\n)\n')
        self.write(f"{E2E_DIR}/get_object_test.rs", "#[tokio::test]\nasync fn get_object_range_e2e() {}\n")

    def write(self, path: str, text: str) -> None:
        (self.root / path).parent.mkdir(parents=True, exist_ok=True)
        (self.root / path).write_text(text)

    def retire(self, *lines: str, stem: str = "object_read") -> list[str]:
        with (self.root / TABLE_DIR / f"{stem}.txt").open("a") as table:
            table.write("".join(line + "\n" for line in lines))
        return check(self.root)

    TEST = "rustfs | app::object::get::tests::legacy_get | test | rustfs/src/app/test_seams/get_object.rs::get_object_ported | ported"

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
        self.assertRegex(" ".join(check(self.root)), "object_reads.txt: not a retirement table")

    def test_four_fields_fail(self) -> None:
        self.assertRegex(" ".join(self.retire(self.TEST.rsplit(" | ", 1)[0])), "expected 5 non-empty fields.*found 4")

    def test_six_fields_fail(self) -> None:
        self.assertRegex(" ".join(self.retire(self.TEST + " | extra")), "expected 5 non-empty fields.*found 6")

    def test_empty_field_fails(self) -> None:
        self.assertRegex(" ".join(self.retire(self.TEST.rsplit("|", 1)[0] + "| ")), "expected 5 non-empty fields")

    def test_test_path_without_module_fails(self) -> None:
        self.assertRegex(" ".join(self.retire(self.TEST.replace("app::object::get::tests::legacy_get", "legacy_get"))), "is not <crate>")

    def test_unknown_kind_fails(self) -> None:
        self.assertRegex(" ".join(self.retire(self.TEST.replace("| test |", "| unit |"))), "unknown kind `unit`")

    def test_duplicate_in_one_table_fails(self) -> None:
        self.assertRegex(" ".join(self.retire(self.TEST, self.TEST)), r"object_read.txt:3: .* already retired at .*object_read.txt:2")

    def test_duplicate_across_tables_fails(self) -> None:
        self.retire(self.TEST, stem="object_meta")
        self.assertRegex(" ".join(self.retire(self.TEST)), r"object_read.txt:2: .* already retired at .*object_meta.txt:2")

    def test_test_replacement_file_missing_fails(self) -> None:
        line = self.TEST.replace("get_object.rs", "head_object.rs")
        self.assertRegex(" ".join(self.retire(line)), "replacement file `rustfs/src/app/test_seams/head_object.rs` does not exist")

    def test_test_replacement_fn_missing_fails(self) -> None:
        line = self.TEST.replace("::get_object_ported", "::get_object_renamed")
        self.assertRegex(" ".join(self.retire(line)), "declares no `fn get_object_renamed`")

    def test_test_replacement_outside_repository_fails(self) -> None:
        line = self.TEST.replace("rustfs/src/app/test_seams/get_object.rs", "../outside.rs")
        self.assertRegex(" ".join(self.retire(line)), "is not <repo path>::<fn>")

    def test_differential_case_missing_fails(self) -> None:
        line = "rustfs | app::object::get::tests::legacy_get | differential | GetObject/negative | ported"
        self.assertRegex(" ".join(self.retire(line)), "no differential step `GetObject`/`negative`")

    def test_differential_operation_missing_fails(self) -> None:
        line = "rustfs | app::object::get::tests::legacy_get | differential | HeadObject/normal | ported"
        self.assertRegex(" ".join(self.retire(line)), "no differential step `HeadObject`/`normal`")

    def test_e2e_fn_missing_fails(self) -> None:
        line = "rustfs | app::object::get::tests::legacy_get | e2e | get_object_range | ported"
        self.assertRegex(" ".join(self.retire(line)), "no `fn get_object_range` under")

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
