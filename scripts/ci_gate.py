#!/usr/bin/env python3
"""Select safe documentation-only CI and verify the complete required job set."""
from __future__ import annotations

import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
ALWAYS_JOBS = ("classify-changes", "typos", "quick-checks")
CODE_JOBS = (
    "test-and-lint", "test-ilm-integration-serial", "test-and-lint-rio-v2",
    "offline-enrollment-root-boundary",
    "connect-short-credential-boundary", "test-and-lint-protocols",
    "build-rustfs-debug-binary", "uring-integration", "e2e-tests",
    "s3-implemented-tests", "s3-lifecycle-behavior-tests",
)
OPTIONAL_JOBS = ("build-rustfs-debug-binary-rio-v2", "e2e-tests-rio-v2", "e2e-full")
NON_VALIDATION_JOBS = {"required-checks", "cancel-closed-pr-runs", "alert-on-failure"}


def documentation_path(path: str) -> bool:
    parts = PurePosixPath(path).parts
    if not parts or path.startswith("/") or any(part in (".", "..") for part in parts) or any(ord(c) < 32 for c in path):
        return False
    if parts[-1] == "AGENTS.md":
        return True
    if len(parts) == 1 and (path.endswith(".md") or path == "LICENSE" or path.startswith("LICENSE-")):
        return True
    if path.startswith(("docs/", ".agents/skills/")) and path.endswith(".md"):
        return True
    return path.startswith("docs/") and path.endswith((".png", ".jpg", ".svg"))


def select_mode(event: str, base: str, head: str, root: Path) -> str:
    if event != "pull_request" or not all(re.fullmatch(r"[0-9a-f]{40}", sha) for sha in (base, head)):
        return "full"
    try:
        changed = subprocess.check_output(
            ["git", "diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--name-only", "-z", base, head, "--"],
            cwd=root, stderr=subprocess.PIPE,
        ).decode("utf-8")
    except (subprocess.CalledProcessError, UnicodeError):
        return "full"
    paths = changed.rstrip("\0").split("\0") if changed else []
    return "docs" if paths and all(documentation_path(path) for path in paths) else "full"


def expected_results(mode: str, event: str, ref: str) -> dict[str, str]:
    if event not in ("pull_request", "push", "merge_group", "schedule", "workflow_dispatch"):
        raise ValueError(f"unsupported CI event: {event!r}")
    if mode not in ("docs", "full") or (mode == "docs" and event != "pull_request"):
        raise ValueError(f"invalid CI selection: {mode!r} for {event!r}")
    expected = {job: "success" for job in ALWAYS_JOBS}
    expected.update({job: "success" if mode == "full" else "skipped" for job in CODE_JOBS})
    rio = mode == "full" and event in ("schedule", "workflow_dispatch")
    expected.update({job: "success" if rio else "skipped" for job in OPTIONAL_JOBS[:2]})
    full = mode == "full" and (event in ("merge_group", "workflow_dispatch") or (event == "push" and ref in ("refs/heads/main", "refs/heads/release")))
    expected["e2e-full"] = "success" if full else "skipped"
    return expected


def verify_results(needs: object, event: str, ref: str) -> list[str]:
    if not isinstance(needs, dict):
        return ["needs must be a job-result object"]
    selection = needs.get("classify-changes", {})
    outputs = selection.get("outputs", {}) if isinstance(selection, dict) else {}
    mode = outputs.get("mode") if isinstance(outputs, dict) else None
    try:
        expected = expected_results(mode, event, ref)
    except ValueError as error:
        return [str(error)]
    errors = []
    if set(needs) != set(expected):
        errors.append(f"job set differs: missing={sorted(set(expected) - set(needs))}, unexpected={sorted(set(needs) - set(expected))}")
    for job, required in expected.items():
        result = needs.get(job, {})
        actual = result.get("result") if isinstance(result, dict) else None
        if actual != required:
            errors.append(f"{job}: expected {required}, got {actual!r}")
    return errors


def check_workflow(root: Path) -> list[str]:
    # Reuse the repository's canonical-indentation checker; actionlint validates YAML syntax.
    from check_test_wiring import yaml_block, yaml_scalar_continues

    errors = []
    lines = (root / ".github/workflows/ci.yml").read_text().splitlines()
    jobs = yaml_block(lines, "jobs", 0) or []
    names = set()
    for index, line in enumerate(jobs):
        if not re.match(r"^  \S", line) or line.lstrip().startswith("#"):
            continue
        header = re.fullmatch(r'''  (["']?)([A-Za-z_][A-Za-z0-9_-]*)\1\s*:\s*(?:#.*)?''', line)
        if header is None:
            errors.append("CI job declarations must use single-line job IDs")
            continue
        name = header[2]
        if name in names:
            errors.append(f"duplicate CI job ID: {name}")
        names.add(name)
        jobs[index] = f"  {name}:"
    required = set(ALWAYS_JOBS + CODE_JOBS + OPTIONAL_JOBS)
    if names - NON_VALIDATION_JOBS != required:
        errors.append("CI verification jobs and the required gate contract differ")
    for job in required:
        block = yaml_block(jobs, job, 2) or []
        if any(re.match(r"\s+(?:- )?[\"']?continue-on-error[\"']?\s*:", line) for line in block):
            errors.append(f"{job} cannot convert a validation failure into success")
    gate = yaml_block(jobs, "required-checks", 2) or []
    def scalar(block, key, indent):
        prefix = " " * indent + key + ": "
        matches = [index for index, line in enumerate(block) if line.startswith(prefix)]
        if len(matches) != 1:
            return None
        index = matches[0]
        if yaml_scalar_continues(block, index, indent):
            return None
        return block[index][len(prefix):]

    display_names = {}
    for job in names:
        block = [re.sub(r'''^    (?:'name'|"name")\s*:\s*''', "    name: ", line)
                 for line in yaml_block(jobs, job, 2) or []]
        value = scalar(block, "name", 4)
        display = re.fullmatch(r'''(?:"([^"\\]*)"|'([^']*)'|([^'"#][^#]*?))(?:\s+#.*)?\s*''', (value or "").strip())
        if display is None or (display[3] is not None and display[3].startswith(tuple("|>*&!{[?"))):
            errors.append(f"{job} must use a verifiable single-line display name")
            continue
        name = next(value for value in display.groups() if value is not None)
        if "${{" in name and (job != "test-and-lint-protocols" or name != "Test and Lint (${{ matrix.features.name }})"):
            errors.append(f"{job} has an unverifiable dynamic display name")
        display_names[job] = name

    dependencies = yaml_block(gate, "needs", 4) or []
    declared = [line.strip().removeprefix("- ") for line in dependencies if line.strip()]
    if set(declared) != required or len(declared) != len(required):
        errors.append("required-checks must directly depend on every verification job exactly once")
    if display_names.get("required-checks") != "Test and Lint" or list(display_names.values()).count("Test and Lint") != 1:
        errors.append("Test and Lint must uniquely name the aggregate gate")
    if scalar(gate, "if", 4) != "always() && (github.event_name != 'pull_request' || github.event.action != 'closed')":
        errors.append("required-checks must run after failed or skipped dependencies")
    if scalar(gate, "shell", 8) != "bash" or scalar(gate, "run", 8) != "python3 scripts/ci_gate.py verify" or scalar(gate, "CI_NEEDS", 10) != "${{ toJSON(needs) }}":
        errors.append("required-checks must verify the actual needs results")
    if any(re.match(r'''\s+(?:- )?(?:["']?continue-on-error["']?\s*:|["']?if["']?\s*:)''', line) and not line.startswith("    if:") for line in gate):
        errors.append("required-checks cannot ignore failures")
    pr = yaml_block(lines, "pull_request", 2) or []
    if any(line.strip().startswith(("paths:", "paths-ignore:")) for line in pr):
        errors.append("all pull requests must enter the single CI workflow")
    if (root / ".github/workflows/ci-docs-only.yml").exists():
        errors.append("the duplicate required-status companion must be removed")
    return errors


class SelfTests(unittest.TestCase):
    def test_documentation_paths_do_not_hide_build_or_fixture_changes(self):
        for path in ("README.md", "AGENTS.md", "crates/utils/AGENTS.md", "docs/testing/README.md", "docs/diagram.svg", ".agents/skills/example/SKILL.md"):
            self.assertTrue(documentation_path(path), path)
        for path in ("", "src/lib.rs", "crates/foo/tests/fixtures/data.md", "Cargo.lock", "build.rs", "deploy/chart.yaml", ".github/workflows/ci.yml", "scripts/dev_build.sh", "assets/logo.png", "docs/test.rs", "README.md\n", "../README.md"):
            self.assertFalse(documentation_path(path), path)

    def test_git_range_includes_deleted_source_and_rename_origins(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", "-c", "core.hooksPath=/dev/null", "-c", "user.name=CI Test", "-c", "user.email=ci@example.invalid", *args], cwd=root, stderr=subprocess.PIPE).decode().strip()
            git("init", "-q")
            (root / "server.rs").write_text("fn main() {}\n")
            (root / "README.md").write_text("old\n")
            git("add", "."); git("commit", "-qm", "base")
            base = git("rev-parse", "HEAD")
            (root / "README.md").write_text("new\n")
            git("add", "."); git("commit", "-qm", "docs")
            docs = git("rev-parse", "HEAD")
            self.assertEqual(select_mode("pull_request", base, docs, root), "docs")
            (root / "server.rs").rename(root / "server.md")
            git("add", "-A"); git("commit", "-qm", "rename source")
            head = git("rev-parse", "HEAD")
            self.assertEqual(select_mode("pull_request", base, head, root), "full")
            self.assertEqual(select_mode("pull_request", docs, docs, root), "full")
            self.assertEqual(select_mode("pull_request", "0" * 40, head, root), "full")
            self.assertEqual(select_mode("pull_request", "--output=bad", head, root), "full")
            self.assertEqual(select_mode("merge_group", base, docs, root), "full")

    def test_event_contract_requires_complete_candidate_and_optional_lanes(self):
        ordinary = expected_results("full", "pull_request", "refs/pull/1/merge")
        self.assertEqual({job for job, state in ordinary.items() if state == "skipped"}, set(OPTIONAL_JOBS))
        docs = expected_results("docs", "pull_request", "refs/pull/1/merge")
        self.assertEqual({job for job, state in docs.items() if state == "success"}, set(ALWAYS_JOBS))
        for event in ("schedule", "workflow_dispatch", "merge_group", "push"):
            result = expected_results("full", event, "refs/heads/main")
            self.assertEqual(result["e2e-full"], "skipped" if event == "schedule" else "success")
            self.assertEqual(result["e2e-tests-rio-v2"], "success" if event in ("schedule", "workflow_dispatch") else "skipped")
            with self.assertRaises(ValueError):
                expected_results("docs", event, "refs/heads/main")

    def test_every_wrong_result_missing_job_or_selection_fails_closed(self):
        for mode, event in (("full", "pull_request"), ("docs", "pull_request"), ("full", "schedule"), ("full", "workflow_dispatch"), ("full", "merge_group")):
            good = {job: {"result": value} for job, value in expected_results(mode, event, "refs/heads/main").items()}
            good["classify-changes"]["outputs"] = {"mode": mode}
            self.assertEqual(verify_results(good, event, "refs/heads/main"), [])
            for job in good:
                for value in ("success", "skipped", "failure", "cancelled", "neutral", "", None):
                    if value == good[job]["result"]:
                        continue
                    with self.subTest(mode=mode, event=event, job=job, result=value):
                        bad = {**good, job: {**good[job], "result": value}}
                        self.assertTrue(verify_results(bad, event, "refs/heads/main"))
                self.assertTrue(verify_results({key: value for key, value in good.items() if key != job}, event, "refs/heads/main"))
                missing_result = {key: value for key, value in good[job].items() if key != "result"}
                self.assertTrue(verify_results({**good, job: missing_result}, event, "refs/heads/main"))
            self.assertTrue(verify_results({**good, "unknown-job": {"result": "success"}}, event, "refs/heads/main"))
            for selection in ({}, {"mode": ""}, {"mode": True}, []):
                bad = {**good, "classify-changes": {"result": "success", "outputs": selection}}
                self.assertTrue(verify_results(bad, event, "refs/heads/main"))

    def test_full_e2e_gate_preserves_workflow_branch_and_event_scope(self):
        for event, ref, required in (
            ("push", "refs/heads/main", "success"),
            ("push", "refs/heads/release", "success"),
            ("push", "refs/heads/feature", "skipped"),
            ("push", "refs/heads/release-candidate", "skipped"),
            ("push", "refs/tags/release", "skipped"),
            ("pull_request", "refs/pull/1/merge", "skipped"),
            ("schedule", "refs/heads/release", "skipped"),
            ("workflow_dispatch", "refs/heads/feature", "success"),
            ("merge_group", "refs/heads/gh-readonly-queue/release/pr-1", "success"),
        ):
            with self.subTest(event=event, ref=ref):
                expected = expected_results("full", event, ref)
                self.assertEqual(expected["e2e-full"], required)
                needs = {job: {"result": result} for job, result in expected.items()}
                needs["classify-changes"]["outputs"] = {"mode": "full"}
                for result in ("success", "skipped", "failure", "cancelled"):
                    needs["e2e-full"]["result"] = result
                    self.assertEqual(verify_results(needs, event, ref) == [], result == required)

    def test_repository_wiring_and_missing_dependency_regression(self):
        self.assertEqual(check_workflow(ROOT), [])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".github/workflows").mkdir(parents=True)
            source = (ROOT / ".github/workflows/ci.yml").read_text()
            path = root / ".github/workflows/ci.yml"
            for job in ALWAYS_JOBS + CODE_JOBS + OPTIONAL_JOBS:
                before, gate = source.split("  required-checks:\n", 1)
                path.write_text(before + "  required-checks:\n" + gate.replace(f"      - {job}\n", "", 1))
                self.assertTrue(check_workflow(root), job)
            for old, new in (
                ("run: python3 scripts/ci_gate.py verify", "run: python3 scripts/ci_gate.py verify || true"),
                ("run: python3 scripts/ci_gate.py verify", "run: python3 scripts/ci_gate.py verify\n          || true"),
                ("CI_NEEDS: ${{ toJSON(needs) }}", "CI_NEEDS: '{}'"),
                ("name: Test and Lint\n", "name: Unrequired result\n"),
                ("        shell: bash\n        run: python3 scripts/ci_gate.py verify", "        shell: echo {0}\n        run: python3 scripts/ci_gate.py verify"),
                ("        shell: bash\n        run: python3 scripts/ci_gate.py verify", "        run: python3 scripts/ci_gate.py verify"),
                ("        run: python3 scripts/ci_gate.py verify", '        "if": false\n        run: python3 scripts/ci_gate.py verify'),
            ):
                path.write_text(source.replace(old, new))
                self.assertTrue(check_workflow(root), new)
            for job in ALWAYS_JOBS + CODE_JOBS + OPTIONAL_JOBS:
                for field in ("continue-on-error", '"continue-on-error"', "'continue-on-error'"):
                    path.write_text(source.replace(f"  {job}:\n", f"  {job}:\n    {field}: true\n", 1))
                    self.assertTrue(check_workflow(root), (job, field))
                    before, block = source.split(f"  {job}:\n", 1)
                    block = block.replace("      - name:", f"      - {field}: true\n        name:", 1)
                    path.write_text(before + f"  {job}:\n" + block)
                    self.assertTrue(check_workflow(root), (job, field, "step"))
            path.write_text(source + "\n  cancel-after-test-and-lint-failure:\n    runs-on: ubuntu-latest\n")
            self.assertTrue(check_workflow(root))

    def test_job_ids_and_display_names_cannot_hide_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".github/workflows").mkdir(parents=True)
            source = (ROOT / ".github/workflows/ci.yml").read_text()
            path = root / ".github/workflows/ci.yml"
            for header in ("typos", "'typos'", '"typos"'):
                path.write_text(source.replace("  typos:\n", f"  {header}: # spelling\n"))
                self.assertEqual(check_workflow(root), [], header)
            for name in ("Test and Lint # required", "'Test and Lint'", '"Test and Lint" # required'):
                path.write_text(source.replace("    name: Test and Lint\n", f"    name: {name}\n"))
                self.assertEqual(check_workflow(root), [], name)
            for key in ("'name'", '"name"'):
                path.write_text(source.replace("    name: Typos\n", f"    {key}: Typos\n"))
                self.assertEqual(check_workflow(root), [], key)
            for header in ("new_test", "NewTest", "_new_test", "'new_test'", '"new_test"', '"new\\u005ftest"'):
                path.write_text(source + f"\n  {header}:\n    name: New test\n    runs-on: ubuntu-latest\n    steps:\n      - run: exit 1\n")
                self.assertTrue(check_workflow(root), header)
            path.write_text(source + "\n  'typos':\n    name: Duplicate\n    runs-on: ubuntu-latest\n    steps:\n      - run: exit 1\n")
            self.assertIn("duplicate CI job ID: typos", check_workflow(root))
            for name in (
                "Test and Lint", "Test and Lint # duplicate", "'Test and Lint'",
                '"Test and Lint" # duplicate', '"Test\\u0020and Lint"',
                ">-\n      Test and Lint", "|-\n      Test and Lint", "Test and\n      Lint",
                "*required_name", "&required_name Test and Lint", "!!str Test and Lint",
                "${{ 'Test and Lint' }}", '"${{ github.event.inputs.check_name }}"',
            ):
                path.write_text(source.replace("    name: Typos\n", f"    name: {name}\n"))
                self.assertTrue(check_workflow(root), name)
            path.write_text(source.replace("    name: Typos\n", ""))
            self.assertIn("typos must use a verifiable single-line display name", check_workflow(root))

    def test_verify_command_preserves_failures(self):
        good = {job: {"result": value} for job, value in expected_results("full", "pull_request", "refs/pull/1/merge").items()}
        good["classify-changes"]["outputs"] = {"mode": "full"}
        failed = {**good, "e2e-tests": {"result": "failure"}}
        for needs, code in ((json.dumps(good), 0), (json.dumps(failed), 1), ("{}", 1), ("{", 1)):
            with self.subTest(needs=needs):
                env = dict(os.environ, CI_NEEDS=needs, GITHUB_EVENT_NAME="pull_request", GITHUB_REF="refs/pull/1/merge")
                result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "verify"], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertIn("ERROR:" if code else "CI contract passed", result.stderr if code else result.stdout)

    def test_actual_selector_bootstrap_uses_base_policy_and_fails_closed(self):
        from check_test_wiring import yaml_block
        jobs = yaml_block((ROOT / ".github/workflows/ci.yml").read_text().splitlines(), "jobs", 0)
        selector = yaml_block(jobs, "classify-changes", 2)
        body = "\n".join(line[10:] for line in selector[selector.index("        run: |") + 1:])
        for event, changed, base_sha, available, broken, expected in (
            ("pull_request", "README.md", "b" * 40, True, False, "docs"),
            ("pull_request", "src/server.rs", "b" * 40, True, False, "full"),
            ("pull_request", "README.md", "b" * 40, False, False, "full"),
            ("merge_group", "README.md", "b" * 40, False, False, "full"),
            ("pull_request", "README.md", "b" * 40, True, True, None),
            ("pull_request", "README.md", "", True, True, "full"),
        ):
            with self.subTest(event=event, changed=changed, available=available, broken=broken), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "scripts").mkdir()
                (root / "scripts/ci_gate.py").write_text("raise SystemExit(71)\n")
                (root / "python3").symlink_to(sys.executable)
                base = root / "base-policy.py"
                base.write_text("raise SystemExit(29)\n" if broken else Path(__file__).read_text())
                git = root / "git"
                git.write_text('''#!/bin/sh
if [ "$1" = show ]; then
  [ "$2" = "$CI_BASE_SHA:scripts/ci_gate.py" ] || exit 19
  [ "$BASE_AVAILABLE" = yes ] || exit 128
  cat "$BASE_POLICY"
elif [ "$1" = diff ]; then
  printf '%s\\0' "$CHANGED_PATH"
else
  exit 20
fi
''')
                git.chmod(0o755)
                output = root / "output"
                output.touch()
                env = dict(os.environ, GITHUB_EVENT_NAME=event, CI_BASE_SHA=base_sha, GITHUB_SHA="c" * 40,
                           RUNNER_TEMP=str(root), GITHUB_OUTPUT=str(output), BASE_POLICY=str(base),
                           BASE_AVAILABLE="yes" if available else "no", CHANGED_PATH=changed,
                           PATH=f"{root}{os.pathsep}{os.environ['PATH']}")
                result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", body], cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 29 if expected is None else 0, result.stderr)
                self.assertEqual(output.read_text(), "" if expected is None else f"mode={expected}\n")


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        return not unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)).wasSuccessful()
    if sys.argv[1:] == ["select"]:
        mode = select_mode(os.environ.get("GITHUB_EVENT_NAME", ""), os.environ.get("CI_BASE_SHA", ""), os.environ.get("GITHUB_SHA", ""), Path.cwd())
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write(f"mode={mode}\n")
        print(f"CI selection: {mode}")
        return 0
    if sys.argv[1:] == ["verify"]:
        try:
            errors = verify_results(json.loads(os.environ["CI_NEEDS"]), os.environ.get("GITHUB_EVENT_NAME", ""), os.environ.get("GITHUB_REF", ""))
        except (KeyError, ValueError) as error:
            errors = [str(error)]
    elif sys.argv[1:] == ["--check-workflow"]:
        errors = check_workflow(ROOT)
    else:
        print("usage: ci_gate.py {select|verify|--check-workflow|--self-test}", file=sys.stderr)
        return 2
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if not errors:
        print("CI contract passed")
    return bool(errors)


if __name__ == "__main__":
    raise SystemExit(main())
