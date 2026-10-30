#!/usr/bin/env python3
"""Archive consumers require exact provenance and complete selected executions."""

import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
from string import Template
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

import ci_test_artifact as artifact


def listing(cases):
    suites = {}
    for binary, name, ignored in cases:
        suite = suites.setdefault(binary, {"status": "listed", "testcases": {}})
        suite["testcases"][name] = {"ignored": ignored,
                                   "filter-match": {"status": "mismatch" if ignored else "matches"}}
    return {"rust-suites": suites}


def junit(cases):
    root = ET.Element("testsuites", tests=str(len(cases)), failures="0", errors="0")
    suites = {}
    for binary, name, outcome in cases:
        if binary not in suites:
            suites[binary] = ET.SubElement(root, "testsuite", name=binary, failures="0", errors="0")
        case = ET.SubElement(suites[binary], "testcase", name=name, classname=binary)
        if outcome:
            ET.SubElement(case, outcome)
    return root


class ArtifactIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive = self.root / "tests.tar.zst"
        self.archive.write_bytes(b"controlled archive bytes")
        self.identity = {"source": {"head": "a" * 40, "sha256": "b" * 64}, "tree": "c" * 40,
                         "workspace_root": "/workspace/rustfs", "lock_sha256": "d" * 64,
                         "rustc": "rustc 1.92.0", "nextest": "cargo-nextest 0.9.138",
                         "configuration": "workspace", "profile": "test", "features": [],
                         "build_environment": {key: "" for key in artifact.BUILD_ENVIRONMENT},
                         "system": "Linux", "machine": "x86_64", "repository": "owner/repo",
                         "run_id": "123"}
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.size = self.archive.stat().st_size
        self.record = {"schema": 1, "identity": copy.deepcopy(self.identity),
                       "package_args": ["--workspace", "--exclude", "e2e_test"],
                       "sha256": self.digest, "size_bytes": self.size, "libc": ["glibc", "2.39"]}

    def verify(self, record=None, expected=None, digest=None, size=None, libc=("glibc", "2.39")):
        artifact.verify_record(record or self.record, expected or self.identity,
                               digest or self.digest, self.size if size is None else size, libc)

    def test_exact_record_accepts_same_or_newer_glibc(self):
        self.verify()
        self.verify(libc=("glibc", "2.40"))

    def test_every_provenance_dimension_rejects_mismatch(self):
        mutations = {"source": {"head": "e" * 40, "sha256": "f" * 64}, "tree": "e" * 40,
                     "workspace_root": "/different/checkout", "lock_sha256": "e" * 64,
                     "rustc": "rustc 1.93.0", "nextest": "cargo-nextest 0.9.140",
                     "configuration": "e2e", "profile": "release", "features": ["rio-v2"],
                     "build_environment": {**self.identity["build_environment"], "RUSTFLAGS": "-C opt-level=3"},
                     "system": "Darwin", "machine": "aarch64", "repository": "other/repo",
                     "run_id": "124"}
        for key, value in mutations.items():
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "identity mismatch"):
                self.verify(expected={**self.identity, key: value})

    def test_failed_job_rerun_reuses_the_archive_but_another_run_does_not(self):
        with patch.object(artifact, "source_identity", return_value=self.identity["source"]), \
                patch.object(artifact, "output", return_value="fixture"), \
                patch.object(artifact, "file_hash", return_value="d" * 64), \
                patch.dict(os.environ, {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1"}, clear=True):
            producer = artifact.identity("workspace")
            with patch.dict(os.environ, {"GITHUB_RUN_ATTEMPT": "2"}):
                self.assertEqual(artifact.identity("workspace"), producer)
            with patch.dict(os.environ, {"GITHUB_RUN_ID": "124"}):
                self.assertNotEqual(artifact.identity("workspace"), producer)

    def test_modified_truncated_and_empty_archives_are_rejected(self):
        for key, value in (("schema", 2), ("sha256", "f" * 64), ("size_bytes", self.size + 1)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.verify(record={**self.record, key: value})
        with self.assertRaises(ValueError):
            self.verify(size=0)
        with self.assertRaises(ValueError):
            self.verify(digest="0" * 64)

    def test_abi_refuses_older_or_different_or_unparseable_libc(self):
        for libc in (("glibc", "2.38"), ("musl", "1.2.5"), ("glibc", "unknown")):
            with self.subTest(libc=libc), self.assertRaises(ValueError):
                self.verify(libc=libc)
        for producer in ([], ["glibc"], ["glibc", "not-a-version"]):
            with self.subTest(producer=producer), self.assertRaises(ValueError):
                self.verify(record={**self.record, "libc": producer})
        musl = {**self.record, "libc": ["musl", "1.2.5"]}
        self.verify(record=musl, libc=("musl", "1.2.5"))
        with self.assertRaises(ValueError):
            self.verify(record=musl, libc=("musl", "1.2.6"))

    def test_verify_binds_actual_bytes_and_package_plan(self):
        artifact.manifest_path(self.archive).write_text(json.dumps(self.record))
        with patch.object(artifact, "identity", return_value=self.identity), \
                patch.object(artifact.platform, "libc_ver", return_value=("glibc", "2.39")), \
                patch.dict(os.environ, {}, clear=True):
            self.assertEqual(artifact.verify(self.archive, "workspace"), self.record)
            with patch.dict(os.environ, {"CI_PACKAGES": '["rustfs"]'}), self.assertRaisesRegex(ValueError, "package selection"):
                artifact.verify(self.archive, "workspace")
            self.archive.write_bytes(b"corrupt")
            with self.assertRaisesRegex(ValueError, "corrupt"):
                artifact.verify(self.archive, "workspace")

    def test_package_selection_rejects_invalid_and_keeps_e2e_separate(self):
        with patch.dict(os.environ, {"CI_PACKAGES": '["rustfs-utils", "rustfs", "rustfs"]'}):
            self.assertEqual(artifact.package_args("workspace"), ["-p", "rustfs", "-p", "rustfs-utils"])
            self.assertEqual(artifact.package_args("e2e"), ["-p", "e2e_test"])
        locked = [{"name": "zstd", "version": "0.13.3"},
                  {"name": "zstd", "version": "0.14.3", "source": "registry+https://github.com/rust-lang/crates.io-index"},
                  {"name": "rustfs", "version": "0.1.0"}]
        with patch.object(artifact, "locked_packages", return_value=locked), \
                patch.dict(os.environ, {"CI_PACKAGES": '["zstd", "rustfs"]'}):
            self.assertEqual(artifact.package_args("workspace"), ["-p", "rustfs", "-p", "zstd@0.13.3"])
            for clash in ([*locked[:2], {"name": "zstd", "version": "0.14.3"}], [locked[1], {**locked[1], "version": "0.15.0"}]):
                with self.subTest(clash=clash), patch.object(artifact, "locked_packages", return_value=clash), \
                        self.assertRaisesRegex(ValueError, "unambiguously"):
                    artifact.package_args("workspace")
        for value in ('[]', '{}', '[null]', '["--all-features"]', '["e2e_test"]', '[""]'):
            with self.subTest(value=value), patch.dict(os.environ, {"CI_PACKAGES": value}), self.assertRaises(ValueError):
                artifact.package_args("workspace")

    def test_source_drift_during_archive_build_cannot_write_receipt(self):
        after = {**self.identity, "source": {"head": "e" * 40, "sha256": "f" * 64}}
        with patch.object(artifact, "identity", side_effect=[self.identity, after]), \
                patch.object(artifact.subprocess, "run"), patch.dict(os.environ, {}, clear=True), \
                self.assertRaisesRegex(ValueError, "changed while compiling"):
            artifact.build(self.archive, "workspace")
        self.assertFalse(artifact.manifest_path(self.archive).exists())

    def test_codegen_overrides_are_bound_but_build_concurrency_is_not(self):
        overrides = {"CARGO_PROFILE_TEST_OVERFLOW_CHECKS": "false", "CARGO_PROFILE_TEST_DEBUG_ASSERTIONS": "false",
                     "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS": "-C opt-level=3",
                     "CARGO_BUILD_RUSTFLAGS": "-C debuginfo=0", "RUSTFS_BUILD_VERSION": "fixture",
                     "CFLAGS": "-O3", "CARGO_BUILD_JOBS": "2"}
        with patch.dict(os.environ, overrides, clear=True):
            bound = artifact.build_environment()
            for key, value in overrides.items():
                if key != "CARGO_BUILD_JOBS":
                    self.assertEqual(bound[key], value)
            with patch.dict(os.environ, {"CARGO_BUILD_JOBS": "4"}):
                self.assertEqual(artifact.build_environment(), bound)
            with patch.dict(os.environ, {"CARGO_PROFILE_TEST_OVERFLOW_CHECKS": "true"}):
                self.assertNotEqual(artifact.build_environment(), bound)

    def test_native_target_and_original_target_directory_are_required(self):
        for overrides in ({"CARGO_BUILD_TARGET": "aarch64-unknown-linux-gnu"},
                          {"CARGO_TARGET_DIR": str(self.root / "different-target")}):
            with self.subTest(overrides=overrides), patch.dict(os.environ, overrides, clear=True), self.assertRaises(ValueError):
                artifact.identity("workspace")

    def test_lint_command_keeps_all_targets_deny_warnings_and_failure(self):
        arguments = ["ci_test_artifact.py", "cargo", "lint", "--configuration", "workspace"]
        expected = ["cargo", "clippy", "--locked", "-p", "rustfs", "-p", "e2e_test", "--all-targets", "--", "-D", "warnings"]
        with patch.object(sys, "argv", arguments), patch.dict(os.environ, {"CI_PACKAGES": '["rustfs"]'}):
            with patch.object(artifact.subprocess, "run") as run:
                self.assertEqual(artifact.main(), 0)
                self.assertEqual(run.call_args.args[0], expected)
                self.assertTrue(run.call_args.kwargs["check"])
            with patch.object(artifact.subprocess, "run", side_effect=subprocess.CalledProcessError(1, expected)), \
                    patch.object(sys, "stderr", io.StringIO()):
                self.assertEqual(artifact.main(), 1)
        for environment in ({}, {"CI_PACKAGES": ""}):
            with self.subTest(environment=environment), patch.object(sys, "argv", arguments), \
                    patch.dict(os.environ, environment, clear=True), patch.object(artifact.subprocess, "run") as run:
                self.assertEqual(artifact.main(), 0)
                self.assertEqual(run.call_args.args[0], ["cargo", "clippy", "--locked", "--workspace", "--all-targets", "--", "-D", "warnings"])

    def test_doctests_are_an_independent_command(self):
        with patch.object(sys, "argv", ["ci_test_artifact.py", "cargo", "doc", "--configuration", "workspace"]), \
                patch.dict(os.environ, {}, clear=True), patch.object(artifact.subprocess, "run") as run:
            self.assertEqual(artifact.main(), 0)
        self.assertEqual(run.call_args.args[0], ["cargo", "test", "--locked", "--workspace", "--exclude", "e2e_test", "--doc"])


class ExecutionEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.listing_file = self.root / "listing.json"
        self.junit_file = self.root / "junit.xml"
        self.listing = listing([("crate-a", "one", False), ("crate-b", "two", False), ("crate-b", "ignored", True)])
        self.report = junit([("crate-a", "one", None), ("crate-b", "two", None)])

    def verify(self, listing_value=None, report=None, *, core=False):
        self.listing_file.write_text(json.dumps(self.listing if listing_value is None else listing_value))
        ET.ElementTree(self.report if report is None else report).write(self.junit_file)
        return artifact.verify_execution(self.listing_file, self.junit_file, core=core)

    def test_exact_selected_set_and_known_ignored_cases(self):
        self.assertEqual(self.verify(), 2)
        self.assertEqual(self.verify(report=junit([("crate-a", "one", None), ("crate-b", "two", None),
                                                   ("crate-b", "ignored", "skipped")])), 2)

    def test_missing_extra_or_misnamed_execution_is_rejected(self):
        cases = [("crate-a", "one", None), ("crate-b", "two", None)]
        for altered in (cases[:1], cases + [("crate-a", "extra", None)],
                        [("wrong-binary", "one", None), cases[1]]):
            with self.subTest(cases=altered), self.assertRaises(ValueError):
                self.verify(report=junit(altered))

    def test_selected_skip_failure_error_or_duplicate_is_rejected(self):
        for outcome in ("skipped", "failure", "error"):
            with self.subTest(outcome=outcome), self.assertRaises(ValueError):
                self.verify(report=junit([("crate-a", "one", outcome), ("crate-b", "two", None)]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.verify(report=junit([("crate-a", "one", None), ("crate-a", "one", None), ("crate-b", "two", None)]))
        self.report.find("testsuite/testcase").set("classname", "different")
        with self.assertRaisesRegex(ValueError, "misplaced"):
            self.verify()

    def test_extra_skipped_case_must_exist_in_listing(self):
        with self.assertRaises(ValueError):
            self.verify(report=junit([("crate-a", "one", None), ("crate-b", "two", None),
                                      ("unknown-binary", "unknown-case", "skipped")]))

    def test_setup_scripts_must_pass_without_counting_as_selected_tests(self):
        cases = [("crate-a", "one", None), ("crate-b", "two", None)]
        script = ("@setup-script:prepare", "prepare", None)
        self.assertEqual(self.verify(report=junit([*cases, script])), 2)
        for outcome in ("skipped", "failure", "error"):
            with self.subTest(outcome=outcome), self.assertRaises(ValueError):
                self.verify(report=junit([*cases, (*script[:2], outcome)]))
        with self.assertRaisesRegex(ValueError, "missing"):
            self.verify(report=junit([cases[0], script]))

    def test_malformed_or_duplicate_setup_script_evidence_is_rejected(self):
        cases = [("crate-a", "one", None), ("crate-b", "two", None)]
        script = ("@setup-script:prepare", "prepare", None)
        for extra in ([script, script], [("@setup-script:", "", None)],
                      [(script[0], "wrong-name", None)]):
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                self.verify(report=junit([*cases, *extra]))
        report = junit([*cases, script])
        report.append(copy.deepcopy(report[-1]))
        with self.assertRaises(ValueError):
            self.verify(report=report)
        report = junit(cases)
        ET.SubElement(report, "testsuite", name=script[0], failures="0", errors="0")
        with self.assertRaises(ValueError):
            self.verify(report=report)
        report = junit([*cases, script])
        report[-1].find("testcase").set("classname", "wrong-suite")
        with self.assertRaisesRegex(ValueError, "misplaced"):
            self.verify(report=report)

    def test_empty_unlisted_or_failed_top_level_report_is_rejected(self):
        with self.assertRaises(ValueError):
            self.verify(listing_value={"rust-suites": {}})
        with self.assertRaises(ValueError):
            self.verify(listing_value=listing([("crate-a", "ignored", True)]))
        self.listing["rust-suites"]["crate-a"]["status"] = "not-listed"
        with self.assertRaises(ValueError):
            self.verify()
        self.listing["rust-suites"]["crate-a"]["status"] = "listed"
        for attribute in ("failures", "errors"):
            report = copy.deepcopy(self.report)
            report.set(attribute, "1")
            with self.subTest(attribute=attribute), self.assertRaises(ValueError):
                self.verify(report=report)

    def test_core_requires_exactly_three_selected_proofs(self):
        names = sorted(artifact.CORE_TESTS)
        core_listing = listing([("e2e", name, False) for name in names])
        core_report = junit([("e2e", name, None) for name in names])
        self.assertEqual(self.verify(core_listing, core_report, core=True), 3)
        for cases in ([name for name in names if name != names[0]], names + ["unrelated"]):
            with self.subTest(cases=cases), self.assertRaises(ValueError):
                self.verify(listing([("e2e", name, False) for name in cases]),
                            junit([("e2e", name, None) for name in cases]), core=True)
        # Reusing one proof's name in another binary must not expand the suite.
        duplicate_name = listing([("e2e", name, False) for name in names] + [("other-binary", names[0], False)])
        duplicate_report = junit([("e2e", name, None) for name in names] + [("other-binary", names[0], None)])
        with self.assertRaises(ValueError):
            self.verify(duplicate_name, duplicate_report, core=True)


class NextestSetupScriptTests(unittest.TestCase):
    def test_workflow_consumers_reuse_extraction_and_validate_setup_script_reports(self):
        from ci_gate import workflow_document

        workflow = workflow_document((artifact.ROOT / ".github/workflows/ci.yml").read_text())

        def commands(job):
            result = []
            for step in workflow["jobs"][job]["steps"]:
                for line in step.get("run", "").replace("\\\n", " ").splitlines():
                    if "cargo nextest " not in line:
                        continue
                    words = shlex.split(line)
                    for start in range(len(words) - 1):
                        if words[start:start + 2] == ["cargo", "nextest"]:
                            end = next((index for index in range(start, len(words))
                                        if words[index] in ("|", ">", "2>&1")), len(words))
                            result.append(words[start:end])
                            break
            return result

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / ".config").mkdir()
            (root / "Cargo.toml").write_text(
                '[package]\nname = "rustfs"\nversion = "0.1.0"\nedition = "2021"\n'
                '[lib]\nname = "lifecycle_integration_test"\n[workspace]\n'
            )
            (root / "rust-toolchain.toml").write_text((artifact.ROOT / "rust-toolchain.toml").read_text())
            (root / "src/lib.rs").write_text(
                '#[test]\nfn setup_is_applied() {\n'
                '    assert_eq!(std::env::var("EVIDENCE_SETUP").unwrap(), "ready");\n}\n'
                'mod lifecycle_transition_api_test {\n'
                '    #[test]\n    #[ignore]\n    fn ignored_case() {}\n}\n'
            )
            config = root / ".config/nextest.toml"
            config.write_text(
                'experimental = ["setup-scripts"]\n'
                '[scripts.setup.prepare]\ncommand = ["sh", "prepare.sh"]\n'
                '[[profile.default.scripts]]\nfilter = "all()"\nsetup = "prepare"\n'
                + ''.join(f'[profile.{profile}.junit]\npath = "junit.xml"\n'
                          for profile in ("default", "ci", "e2e-smoke", "e2e-full", "core-contract"))
            )
            script = root / "prepare.sh"
            script.write_text('echo EVIDENCE_SETUP=ready >> "$NEXTEST_ENV"\n')
            environment = {**os.environ, "CARGO_TARGET_DIR": str(root / "target"), "CARGO_NET_OFFLINE": "true",
                           "GITHUB_WORKSPACE": str(root), "RUNNER_TEMP": str(root),
                           "NEXTEST_ARCHIVE": str(root / "e2e-tests.tar.zst")}

            def nextest(*arguments):
                return subprocess.run(["cargo", "nextest", *arguments], cwd=root, env=environment,
                                      capture_output=True, text=True, timeout=120)

            archive = root / "built-tests.tar.zst"
            archived = nextest("archive", "--archive-file", str(archive))
            self.assertEqual(archived.returncode, 0, archived.stderr)
            # Any accidental compilation after the archive build must fail.
            (root / "src/lib.rs").write_text('compile_error!("consumer attempted to rebuild");\n')
            listing_file = root / "listing.json"
            for job in ("workspace-tests", "core-contract", "e2e-tests", "e2e-full"):
                with self.subTest(job=job):
                    shutil.rmtree(root / "target")
                    consumer_archive = root / ("workspace-tests.tar.zst" if job == "workspace-tests" else "e2e-tests.tar.zst")
                    shutil.copyfile(archive, consumer_archive)
                    selected = [[Template(word).substitute(environment) for word in command] for command in commands(job)]
                    self.assertEqual(len(selected), 3 if job == "workspace-tests" else 2)
                    self.assertEqual(selected[0][:3], ["cargo", "nextest", "list"])
                    listed = nextest(*selected[0][2:])
                    self.assertEqual(listed.returncode, 0, listed.stderr)
                    listing_file.write_text(listed.stdout)
                    self.assertTrue((root / "target/nextest/binaries-metadata.json").is_file())
                    # The remaining commands cannot extract again, even for ignored tests.
                    consumer_archive.unlink()
                    for command in selected[1:]:
                        executed = nextest(*command[2:])
                        self.assertEqual(executed.returncode, 0, executed.stderr)
                        profile = command[command.index("--profile") + 1]
                        report = root / f"target/nextest/{profile}/junit.xml"
                        suites = ET.parse(report).getroot().findall("testsuite")
                        self.assertIn("@setup-script:prepare", [suite.get("name") for suite in suites])
                        if "--run-ignored" in command:
                            cases = [case.get("name") for suite in suites if not suite.get("name", "").startswith("@")
                                     for case in suite.findall("testcase") if case.find("skipped") is None]
                            self.assertEqual(cases, ["lifecycle_transition_api_test::ignored_case"])
                        else:
                            self.assertEqual(artifact.verify_execution(listing_file, report), 1)

            verified = subprocess.run(
                [sys.executable, str(artifact.ROOT / "scripts/ci_test_artifact.py"), "evidence",
                 str(listing_file), str(report)], capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(verified.returncode, 0, verified.stderr)
            self.assertIn("Verified 1 test executions", verified.stdout)

            for setup in ('echo EVIDENCE_SETUP=wrong >> "$NEXTEST_ENV"\n', 'exit 1\n'):
                report.unlink()
                script.write_text(setup)
                failed = nextest(*command[2:])
                self.assertNotEqual(failed.returncode, 0)
                with self.assertRaisesRegex(ValueError, "successful nextest run"):
                    artifact.verify_execution(listing_file, report)

            for name in ("cargo-metadata.json", "binaries-metadata.json"):
                metadata = root / "target/nextest" / name
                content = metadata.read_bytes()
                metadata.unlink()
                missing = nextest(*command[2:])
                self.assertNotEqual(missing.returncode, 0)
                self.assertIn(name, missing.stderr)
                metadata.write_bytes(content)


if __name__ == "__main__":
    unittest.main()
