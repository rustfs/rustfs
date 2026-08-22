#!/usr/bin/env python3
"""Fail when committed tests silently fall out of their execution wiring."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import tempfile
import tomllib
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEDULED_ALERT_WORKFLOWS = (
    ".github/workflows/audit.yml",
    ".github/workflows/build.yml",
    ".github/workflows/ci.yml",
    ".github/workflows/coverage.yml",
    ".github/workflows/e2e-replication-nightly.yml",
    ".github/workflows/e2e-s3tests.yml",
    ".github/workflows/fuzz.yml",
    ".github/workflows/mint.yml",
    ".github/workflows/minio-interop.yml",
    ".github/workflows/nightly-gnu.yml",
    ".github/workflows/performance-ab.yml",
    ".github/workflows/runner-hygiene.yml",
)


def words(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def rust_code_only(source: str) -> str:
    """Blank Rust comments and literals while preserving byte positions."""
    def quoted_end(quote_index: int, delimiter: str) -> int:
        end = quote_index + 1
        while end < len(source):
            if source[end] == "\\":
                end += 2
            elif source[end] == delimiter:
                return end + 1
            else:
                end += 1
        return end

    code = list(source)
    index = 0
    while index < len(source):
        if source.startswith("//", index):
            end = source.find("\n", index)
            end = len(source) if end < 0 else end
        elif source.startswith("/*", index):
            depth = 1
            end = index + 2
            while end < len(source) and depth:
                if source.startswith("/*", end):
                    depth += 1
                    end += 2
                elif source.startswith("*/", end):
                    depth -= 1
                    end += 2
                else:
                    end += 1
        else:
            raw = re.match(r'(?:br|cr|r)(?P<hashes>#{0,})"', source[index:])
            if raw:
                marker = '"' + raw.group("hashes")
                end = source.find(marker, index + raw.end())
                end = len(source) if end < 0 else end + len(marker)
            elif source[index] == '"' or source.startswith(('b"', 'c"'), index):
                start_quote = index if source[index] == '"' else index + 1
                end = quoted_end(start_quote, '"')
            elif source[index] == "'" and index + 2 < len(source) and (
                source[index + 1] == "\\" or source[index + 2] == "'"
            ):
                end = quoted_end(index, "'")
            elif source.startswith("b'", index):
                end = quoted_end(index + 1, "'")
            else:
                index += 1
                continue

        for offset in range(index, end):
            if code[offset] != "\n":
                code[offset] = " "
        index = end
    return "".join(code)


def declared(parent: Path, module: str) -> bool:
    source = parent.read_text()
    code = rust_code_only(source)
    pattern = re.compile(rf"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+{re.escape(module)}\s*;", re.MULTILINE)
    allowed_preambles = {
        "#[cfg(test)]": "#[cfg(test)]",
        "#[cfg(all(test,target_os=))]": '#[cfg(all(test,target_os="linux"))]',
    }
    for match in pattern.finditer(code):
        prefix = code[: match.start()]
        depths = {"(": 0, "[": 0, "{": 0}
        pairs = {")": "(", "]": "[", "}": "{"}
        for char in prefix:
            if char in depths:
                depths[char] += 1
            elif char in pairs:
                depths[pairs[char]] -= 1
        if any(depths.values()):
            continue

        boundary = max(prefix.rfind(";"), prefix.rfind("{"), prefix.rfind("}"))
        code_preamble = re.sub(r"\s+", "", prefix[boundary + 1 :])
        if not code_preamble:
            return True
        if code_preamble in allowed_preambles:
            attr_start = prefix.rfind("#[cfg", boundary + 1)
            if attr_start >= 0 and re.sub(r"\s+", "", source[attr_start : match.start()]) == allowed_preambles[code_preamble]:
                return True
    return False


def module_source(src: Path, directory: Path) -> Path | None:
    if not directory.parts:
        return src / "lib.rs"

    mod_file = src / directory / "mod.rs"
    if mod_file.is_file():
        return mod_file

    sibling = src.joinpath(*directory.parts[:-1], f"{directory.name}.rs")
    return sibling if sibling.is_file() else None


def check_e2e_modules(root: Path) -> list[str]:
    src = root / "crates/e2e_test/src"
    errors: list[str] = []
    for test_file in sorted(src.rglob("*_test.rs")):
        relative = test_file.relative_to(root).as_posix()
        directory = test_file.relative_to(src).parent
        parent = module_source(src, directory)
        if parent is None:
            errors.append(f"{relative}: no canonical parent module")
            continue

        if not declared(parent, test_file.stem):
            errors.append(f"{relative}: not declared by {parent.relative_to(root).as_posix()}")
        while directory.parts:
            module = directory.name
            directory = directory.parent
            parent = module_source(src, directory)
            if parent is None:
                errors.append(f"{relative}: module {module} has no canonical parent")
                break
            if not declared(parent, module):
                errors.append(f"{relative}: module {module} not declared by {parent.relative_to(root).as_posix()}")
    return errors


def check_fuzz_targets(root: Path) -> list[str]:
    manifest = tomllib.loads((root / "fuzz/Cargo.toml").read_text())
    expected = {item["name"] for item in manifest.get("bin", []) if "name" in item}
    errors: list[str] = []
    if not expected:
        return ["fuzz/Cargo.toml: no [[bin]] fuzz targets found"]

    runner = (root / "scripts/fuzz/run.sh").read_text()
    match = re.search(r'^targets="([^"]+)"', runner, re.MULTILINE)
    runner_targets = set(match.group(1).split()) if match else set()
    if runner_targets != expected:
        errors.append(f"scripts/fuzz/run.sh targets {sorted(runner_targets)} != manifest {sorted(expected)}")

    workflow = (root / ".github/workflows/fuzz.yml").read_text()
    matrices = [words(value) for value in re.findall(r"^\s*target:\s*\[([^]]+)]", workflow, re.MULTILINE)]
    if len(matrices) != 2:
        errors.append(f".github/workflows/fuzz.yml: expected smoke and nightly target matrices, found {len(matrices)}")
    for index, matrix in enumerate(matrices, start=1):
        if matrix != expected:
            errors.append(f".github/workflows/fuzz.yml matrix {index} {sorted(matrix)} != manifest {sorted(expected)}")

    runtime_targets = re.findall(r"^\s*FUZZ_TARGET:\s*(\S.*?)\s*$", workflow, re.MULTILINE)
    if runtime_targets != ["${{ matrix.target }}", "${{ matrix.target }}"]:
        errors.append(".github/workflows/fuzz.yml: smoke and nightly jobs must pass matrix.target to FUZZ_TARGET")

    dependency_paths = {
        f"{path.removeprefix('../')}/**"
        for dependency in manifest.get("dependencies", {}).values()
        if isinstance(dependency, dict)
        and isinstance(path := dependency.get("path"), str)
        and path.startswith("../crates/")
    }
    missing_paths = sorted(path for path in dependency_paths if f'"{path}"' not in workflow)
    if missing_paths:
        errors.append(f".github/workflows/fuzz.yml missing direct dependency paths: {', '.join(missing_paths)}")

    staged_matches = re.findall(r"^\s*for target in ([^;]+); do", workflow, re.MULTILINE)
    staged = set(staged_matches[0].split()) if staged_matches else set()
    if staged != expected:
        errors.append(f".github/workflows/fuzz.yml staged binaries {sorted(staged)} != manifest {sorted(expected)}")

    return errors


def check_runner_selection(root: Path) -> list[str]:
    runner = (root / "scripts/run_e2e_tests.sh").read_text()
    errors: list[str] = []
    if "--include-ignored" not in runner:
        errors.append("scripts/run_e2e_tests.sh: runner must include default and ignored tests")
    if "--test-threads=1" not in runner:
        errors.append("scripts/run_e2e_tests.sh: runner must serialize fixed-port protocol tests")
    if re.search(r"(?<!include-)--ignored\b", runner):
        errors.append("scripts/run_e2e_tests.sh: bare --ignored silently excludes default tests")
    if "--exact" in runner:
        errors.append("scripts/run_e2e_tests.sh: --test is documented as a pattern and must not force exact matching")
    if 'eval "$test_cmd"' in runner:
        errors.append("scripts/run_e2e_tests.sh: command construction must not use eval")
    return errors


def check_s3_tests_runner(root: Path) -> list[str]:
    runner = (root / "scripts/s3-tests/run.sh").read_text()
    if "--showlocals" in runner:
        return ["scripts/s3-tests/run.sh: pytest failure diagnostics must not dump local values"]
    return []


def profile_selection(root: Path, profile: str) -> str:
    if not re.fullmatch(r"e2e-[a-z0-9-]+", profile):
        raise ValueError(f"invalid e2e profile name: {profile}")
    path = root / f".config/{profile}-selection.txt"
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    values = dict(line.split("=", 1) for line in lines if "=" in line)
    if len(values) != len(lines) or any(not re.fullmatch(r"sha256(?:-[a-z0-9]+)?", key) for key in values):
        raise ValueError(f"{path.relative_to(root).as_posix()}: invalid sha256 entry")
    key = f"sha256-{sys.platform}"
    digest = values.get(key, values.get("sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"{path.relative_to(root).as_posix()}: missing sha256 for {sys.platform}")
    return digest


def check_profile_definitions(root: Path) -> list[str]:
    config = tomllib.loads((root / ".config/nextest.toml").read_text())
    profiles = {
        profile
        for profile in config.get("profile", {})
        if profile.startswith("e2e-")
    }
    selection_profiles = {
        path.name.removesuffix("-selection.txt") for path in (root / ".config").glob("e2e-*-selection.txt")
    }
    errors: list[str] = []
    for profile in sorted(profiles | selection_profiles):
        if profile not in profiles:
            errors.append(f".config/nextest.toml: missing profile.{profile}")
        if profile not in selection_profiles:
            errors.append(f".config/{profile}-selection.txt: missing expected profile selection")
            continue
        try:
            profile_selection(root, profile)
        except (FileNotFoundError, ValueError) as error:
            errors.append(str(error))
    return errors


def check_scheduled_alerts(root: Path) -> list[str]:
    errors: list[str] = []
    for relative in SCHEDULED_ALERT_WORKFLOWS:
        path = root / relative
        try:
            lines = path.read_text().splitlines()
        except FileNotFoundError:
            errors.append(f"{relative}: missing scheduled validation workflow")
            continue

        try:
            start = lines.index("  alert-on-failure:") + 1
        except ValueError:
            errors.append(f"{relative}: missing alert-on-failure job")
            continue
        end = next(
            (index for index in range(start, len(lines)) if re.fullmatch(r"  [A-Za-z0-9_-]+:", lines[index])),
            len(lines),
        )
        job = "\n".join(line.split("#", 1)[0] for line in lines[start:end])
        required = (
            "always()",
            "github.event_name == 'schedule'",
            "contains(needs.*.result, 'failure')",
            "issues: write",
            "uses: ./.github/actions/schedule-failure-issue",
            "github-token: ${{ secrets.GITHUB_TOKEN }}",
        )
        missing = [token for token in required if token not in job]
        if missing:
            errors.append(f"{relative}: alert-on-failure missing {', '.join(missing)}")

    watchdog_path = root / ".github/workflows/scheduled-validation-watchdog.yml"
    try:
        watchdog = "\n".join(
            line.split("#", 1)[0] for line in watchdog_path.read_text().splitlines()
        )
    except FileNotFoundError:
        errors.append(".github/workflows/scheduled-validation-watchdog.yml: missing completion watchdog")
        return errors
    for relative in SCHEDULED_ALERT_WORKFLOWS:
        path = root / relative
        if not path.is_file():
            continue
        source = path.read_text()
        match = re.search(r"^name:\s*[\"']?([^\"'\n]+)", source, re.MULTILINE)
        if not match:
            errors.append(f"{relative}: missing workflow name")
        elif f'- "{match.group(1).strip()}"' not in watchdog:
            errors.append(f"{relative}: missing from scheduled completion watchdog")
    required = (
        "github.event.workflow_run.event == 'schedule'",
        "github.event.workflow_run.conclusion != 'success'",
        "github.event.workflow_run.conclusion != 'failure'",
        "workflow-name: ${{ github.event.workflow_run.name }}",
        "source-run-id: ${{ github.event.workflow_run.id }}",
        "source-run-attempt: ${{ github.event.workflow_run.run_attempt }}",
    )
    missing = [token for token in required if token not in watchdog]
    if missing:
        errors.append(
            ".github/workflows/scheduled-validation-watchdog.yml: missing " + ", ".join(missing)
        )
    return errors


def check_profile_listing(root: Path, profile: str, listing: Path) -> list[str]:
    try:
        expected_digest = profile_selection(root, profile)
        data = json.loads(listing.read_text())
        selected = sorted(
            f"{suite_id}::{test_name}"
            for suite_id, suite in data["rust-suites"].items()
            for test_name, testcase in suite["testcases"].items()
            if testcase.get("filter-match", {}).get("status") == "matches"
        )
        digest = hashlib.sha256(("\n".join(selected) + "\n").encode()).hexdigest()
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        return [f"cannot read {profile} nextest listing: {error}"]
    if digest != expected_digest:
        return [
            f"{profile} selection changed: count={len(selected)} sha256={digest}; "
            f"expected sha256={expected_digest}"
        ]
    print(f"{profile} selection OK: {len(selected)} tests, sha256={digest}")
    return []


def validate(root: Path) -> list[str]:
    errors: list[str] = []
    errors.extend(check_e2e_modules(root))
    errors.extend(check_fuzz_targets(root))
    errors.extend(check_runner_selection(root))
    errors.extend(check_s3_tests_runner(root))
    errors.extend(check_profile_definitions(root))
    errors.extend(check_scheduled_alerts(root))
    return errors


class SelfTests(unittest.TestCase):
    def test_e2e_requires_registration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "crates/e2e_test/src"
            src.mkdir(parents=True)
            (src / "lib.rs").write_text("")
            test_file = src / "boundary_test.rs"
            test_file.write_text("#[test]\nfn boundary() {}\n")
            self.assertEqual(len(check_e2e_modules(root)), 1)
            (src / "lib.rs").write_text("mod boundary_test;\n")
            self.assertEqual(check_e2e_modules(root), [])

            nested = src / "protocols"
            nested.mkdir()
            (nested / "mod.rs").write_text("mod fixed_port_test;\n")
            (nested / "fixed_port_test.rs").write_text("#[test]\nfn fixed_port() {}\n")
            self.assertEqual(len(check_e2e_modules(root)), 1)
            (src / "lib.rs").write_text("mod boundary_test;\nmod protocols;\n")
            self.assertEqual(check_e2e_modules(root), [])

            (src / "lib.rs").write_text("#[cfg(any())]\nmod boundary_test;\nmod protocols;\n")
            self.assertEqual(len(check_e2e_modules(root)), 1)

            (src / "lib.rs").write_text(
                "#[cfg(any())]\n/// hidden module\nmod boundary_test;\n#[cfg_attr(test, cfg(any()))]\nmod protocols;\n"
            )
            self.assertEqual(len(check_e2e_modules(root)), 2)

            (src / "lib.rs").write_text(
                'const PHANTOM: &str = r#"{\nmod boundary_test;\n"#;\ndiscard! { mod protocols; }\n'
            )
            self.assertEqual(len(check_e2e_modules(root)), 2)

            (src / "lib.rs").write_text(
                '#[cfg(all(test, target_os = r"windows" /* target_os = "linux" */))]\n'
                "mod boundary_test;\nmod protocols;\n"
            )
            self.assertEqual(len(check_e2e_modules(root)), 1)

            (src / "lib.rs").write_text(
                '#[cfg(all(test, target_os = r"windows"))] // #[cfg(all(test, target_os = "linux"))]\n'
                "mod boundary_test;\nmod protocols;\n"
            )
            self.assertEqual(len(check_e2e_modules(root)), 1)

    def test_fuzz_runtime_uses_matrix_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "fuzz").mkdir()
            (root / "scripts/fuzz").mkdir(parents=True)
            (root / ".github/workflows").mkdir(parents=True)
            (root / "fuzz/Cargo.toml").write_text(
                'dep = { path = "../crates/dep" }\n[[bin]]\nname = "one"\n'
            )
            (root / "scripts/fuzz/run.sh").write_text('targets="one"\n')
            (root / ".github/workflows/fuzz.yml").write_text(
                'paths:\n  - "crates/dep/**"\n'
                "target: [one]\nFUZZ_TARGET: fixed\n"
                "target: [one]\nFUZZ_TARGET: ${{ matrix.target }}\n"
                "for target in one; do\n"
                "  fuzz/prebuilt/${{ env.CARGO_BUILD_TARGET }}/release/one\n"
            )
            self.assertEqual(len(check_fuzz_targets(root)), 1)

    def test_s3_runner_rejects_unbounded_failure_locals(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = root / "scripts/s3-tests/run.sh"
            runner.parent.mkdir(parents=True)
            runner.write_text("tox -- -vv -ra --tb=long\n")
            self.assertEqual(check_s3_tests_runner(root), [])
            runner.write_text("tox -- -vv -ra --showlocals --tb=long\n")
            self.assertEqual(len(check_s3_tests_runner(root)), 1)
            with (
                mock.patch(__name__ + ".check_e2e_modules", return_value=[]),
                mock.patch(__name__ + ".check_fuzz_targets", return_value=[]),
                mock.patch(__name__ + ".check_runner_selection", return_value=[]),
                mock.patch(__name__ + ".check_profile_definitions", return_value=[]),
                mock.patch(__name__ + ".check_scheduled_alerts", return_value=[]),
            ):
                self.assertEqual(len(validate(root)), 1)

    def test_profile_listing_enforces_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".config").mkdir()
            digest = hashlib.sha256(b"suite::two\n").hexdigest()
            (root / ".config/e2e-smoke-selection.txt").write_text(f"sha256={digest}\n")
            listing = root / "listing.json"
            listing.write_text(
                json.dumps(
                    {
                        "rust-suites": {
                            "suite": {
                                "testcases": {
                                    "one": {"filter-match": {"status": "matches"}},
                                    "two": {"filter-match": {"status": "mismatch"}},
                                }
                            }
                        }
                    }
                )
            )
            self.assertEqual(len(check_profile_listing(root, "e2e-smoke", listing)), 1)

    def test_profile_listing_binds_platform_digest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".config").mkdir()
            darwin_digest = hashlib.sha256(b"suite::darwin\n").hexdigest()
            linux_digest = hashlib.sha256(b"suite::linux\n").hexdigest()
            (root / ".config/e2e-full-selection.txt").write_text(
                f"sha256-darwin={darwin_digest}\nsha256-linux={linux_digest}\n"
            )
            listing = root / "listing.json"
            listing.write_text(
                json.dumps(
                    {
                        "rust-suites": {
                            "suite": {
                                "testcases": {"darwin": {"filter-match": {"status": "matches"}}}
                            }
                        }
                    }
                )
            )
            with mock.patch.object(sys, "platform", "linux"):
                self.assertEqual(len(check_profile_listing(root, "e2e-full", listing)), 1)

    def test_scheduled_alerts_require_completion_watchdog(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            alert = (
                "  alert-on-failure:\n"
                "    if: always() && github.event_name == 'schedule' && "
                "contains(needs.*.result, 'failure')\n"
                "    permissions:\n"
                "      issues: write\n"
                "    steps:\n"
                "      - uses: ./.github/actions/schedule-failure-issue\n"
                "        with:\n"
                "          github-token: ${{ secrets.GITHUB_TOKEN }}\n"
            )
            names: list[str] = []
            for relative in SCHEDULED_ALERT_WORKFLOWS:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                names.append(path.stem)
                path.write_text(f'name: "{path.stem}"\n{alert}')
            watchdog = root / ".github/workflows/scheduled-validation-watchdog.yml"
            watchdog.write_text(
                "\n".join(f'- "{name}"' for name in names)
                + "\ngithub.event.workflow_run.event == 'schedule'\n"
                + "github.event.workflow_run.conclusion != 'success'\n"
                + "github.event.workflow_run.conclusion != 'failure'\n"
                + "workflow-name: ${{ github.event.workflow_run.name }}\n"
                + "source-run-id: ${{ github.event.workflow_run.id }}\n"
                + "source-run-attempt: ${{ github.event.workflow_run.run_attempt }}\n"
            )
            self.assertEqual(check_scheduled_alerts(root), [])

            first = root / SCHEDULED_ALERT_WORKFLOWS[0]
            mutations = (
                ("contains(needs.*.result, 'failure')", "false"),
                ("issues: write", "issues: read"),
                ("uses: ./.github/actions/schedule-failure-issue", "uses: actions/checkout@v7"),
                ("github-token: ${{ secrets.GITHUB_TOKEN }}", "github-token: missing"),
            )
            for required, replacement in mutations:
                original = first.read_text()
                first.write_text(original.replace(required, replacement))
                self.assertEqual(len(check_scheduled_alerts(root)), 1)
                first.write_text(original)

            watchdog.write_text(watchdog.read_text().replace(f'- "{names[0]}"\n', ""))
            self.assertEqual(len(check_scheduled_alerts(root)), 1)

def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if len(sys.argv) == 4 and sys.argv[1] == "--check-profile":
        errors = check_profile_listing(ROOT, sys.argv[2], Path(sys.argv[3]))
        if errors:
            for error in errors:
                print(f"ERROR: {error}", file=sys.stderr)
            return 1
        return 0
    if sys.argv[1:]:
        print(
            "usage: check_test_wiring.py [--self-test | --check-profile PROFILE LISTING]",
            file=sys.stderr,
        )
        return 2

    errors = validate(ROOT)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("OK: e2e modules, runner selection, fuzz matrices, profiles, and scheduled alerts are wired")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
