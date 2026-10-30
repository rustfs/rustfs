#!/usr/bin/env python3
"""Exercise candidate substitution and complete-chain acceptance boundaries."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
import zipfile

import functional_chain_evidence as evidence
import resolve_functional_candidate as candidate
import initialize_validation_evidence as validation_storage


class ValidationStorageTests(unittest.TestCase):
    def fixture(self, root, *, tools_only=False):
        temporary = root / '_work/_temp'
        temporary.mkdir(parents=True)
        return dict(RUNNER_TEMP=str(temporary.resolve()), GITHUB_RUN_ID='77', GITHUB_RUN_ATTEMPT='1',
                    VALIDATION_TOOLS_ONLY=str(tools_only).lower(), GITHUB_ENV=str(root / 'env'))

    def usage(self, *, available=None, inodes=None):
        return mock.Mock(f_bavail=validation_storage.MINIMUM_FREE_BYTES if available is None else available,
                         f_frsize=1, f_favail=validation_storage.MINIMUM_FREE_INODES if inodes is None else inodes)

    def test_self_hosted_evidence_survives_temp_cleanup_and_refuses_reuse(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = self.fixture(root)
            with mock.patch.object(validation_storage.os, 'statvfs', return_value=self.usage()):
                evidence_root = validation_storage.initialize(env)
                with self.assertRaises(FileExistsError):
                    validation_storage.initialize(env)
            self.assertEqual(evidence_root, Path(env['RUNNER_TEMP']).parent /
                             'rustfs-validation-evidence/rustfs-validation-77-1')
            self.assertEqual(evidence_root.parent.stat().st_mode & 0o777, 0o700)
            self.assertEqual(evidence_root.stat().st_mode & 0o777, 0o700)
            Path(env['RUNNER_TEMP']).rmdir()
            self.assertTrue((evidence_root / 'runner-storage-preflight.json').is_file())
            self.assertEqual(Path(env['GITHUB_ENV']).read_text(), 'VALIDATION_ROOT=' + str(evidence_root) + chr(10))

    def test_each_filesystem_byte_and_inode_budget_fails_before_candidate_execution(self):
        for position in (0, 1):
            for kind in ('bytes', 'inodes'):
                with self.subTest(position=position, kind=kind), tempfile.TemporaryDirectory() as temp:
                    root = Path(temp)
                    env = self.fixture(root)
                    usage = [self.usage(), self.usage()]
                    usage[position] = self.usage(available=validation_storage.MINIMUM_FREE_BYTES - 1) if kind == 'bytes' \
                        else self.usage(inodes=validation_storage.MINIMUM_FREE_INODES - 1)
                    with mock.patch.object(validation_storage.os, 'statvfs', side_effect=usage):
                        with self.assertRaisesRegex(ValueError, 'at least 10 GiB'):
                            validation_storage.initialize(env)
                    evidence_root = Path(Path(env['GITHUB_ENV']).read_text().strip().split('=', 1)[1])
                    report = json.loads((evidence_root / 'runner-storage-preflight.json').read_text())
                    self.assertFalse(report['ready'])
                    self.assertFalse(report['automaticEvidenceDeletion'])
                    self.assertEqual(len(report['storage']), 2)

    def test_tools_resolver_keeps_its_disposable_hosted_location(self):
        with tempfile.TemporaryDirectory() as temp:
            env = self.fixture(Path(temp), tools_only=True)
            with mock.patch.object(validation_storage.os, 'statvfs', return_value=self.usage()):
                root = validation_storage.initialize(env)
            self.assertEqual(root.parent, Path(env['RUNNER_TEMP']))
            self.assertFalse(json.loads((root / 'runner-storage-preflight.json').read_text())
                             ['persistentOutsideRunnerTemp'])

    def test_rejects_unsafe_parent_and_untrusted_identities(self):
        for kind in ('symlink', 'public', 'run', 'attempt', 'selection'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                env = self.fixture(root)
                parent = Path(env['RUNNER_TEMP']).parent / 'rustfs-validation-evidence'
                if kind == 'symlink':
                    other = root / 'other'
                    other.mkdir(mode=0o700)
                    parent.symlink_to(other, target_is_directory=True)
                elif kind == 'public':
                    parent.mkdir(mode=0o755)
                    parent.chmod(0o755)
                else:
                    env[{'run': 'GITHUB_RUN_ID', 'attempt': 'GITHUB_RUN_ATTEMPT',
                         'selection': 'VALIDATION_TOOLS_ONLY'}[kind]] = '../foreign'
                with self.assertRaises(ValueError):
                    validation_storage.initialize(env)
                self.assertFalse(Path(env['GITHUB_ENV']).exists())
                self.assertFalse((parent / 'rustfs-validation-77-1').exists())

    def test_workflow_uploads_persistent_evidence_and_preflights_before_resolution(self):
        source = (candidate.ROOT / '.github/workflows/rustfs-functional-chain.yml').read_text()
        initialize = source.index('      - name: Initialize validation evidence')
        self.assertLess(initialize, source.index('      - name: Resolve immutable candidate'))
        self.assertIn('python3 -B scripts/initialize_validation_evidence.py', source)
        self.assertIn('VALIDATION_TOOLS_ONLY: ${{ inputs.validation_tools_only }}', source)
        upload = source.split('      - name: Upload validation evidence', 1)[1].split('  validation-tools:', 1)[0]
        self.assertIn("if: ${{ always() && env.VALIDATION_ROOT != '' }}", upload)
        self.assertIn('path: ${{ env.VALIDATION_ROOT }}', upload)


class RaceNativeToolsWorkflowTests(unittest.TestCase):
    def chain_shell_step(self, name):
        source = (candidate.ROOT / '.github/workflows/rustfs-functional-chain.yml').read_text()
        step = source.split('      - name: ' + name + '\n', 1)[1].split('\n      - name:', 1)[0]
        shell = step.split('        run: |\n', 1)[1]
        return '\n'.join(line[10:] for line in shell.splitlines()
                         if not line.strip() or line.startswith('          '))

    def test_race_actual_workflow_selection_preserves_legacy_and_propagates_failures(self):
        shell = self.chain_shell_step('Select exact-source race native evidence')
        g01 = 'heal.race-unversioned-overwrite'
        cases = [(False, 2, 0, 'false', 'a' * 40, g01), (True, 2, 0, 'false', 'a' * 40, g01),
                 (True, 0, 0, 'true', 'a' * 40, g01), (True, 1, 1, None, 'a' * 40, g01),
                 (True, 42, 42, None, 'a' * 40, g01)]
        reviewed = [('6afdec9fe1b0b88007f2e5b445c19bf73d51d3ca', g01)]
        reviewed += [('bd0671ba781d8ebf509f4f03533228dfb44496f1', case) for case in
                     (g01, 'heal.race-versioned-put', 'heal.race-version-delete')]
        reviewed += [('8380466f3a27c3c3ef4115084178a0ab99a78f32', case) for case in
                     (g01, 'heal.race-versioned-put', 'heal.race-version-delete', 'heal.race-markers-and-batch-delete')]
        reviewed += [('757b751482bb970bc321e92059690a7698554358', case) for case in
                     ('heal.admin-nolock-safety', 'heal.race-copy-and-metadata-update', 'heal.race-multipart-complete-abort')]
        for source, case in reviewed:
            cases += [(False, 2, 1, None, source, case), (True, 2, 1, None, source, case),
                      (True, 0, 0, 'true', source, case)]
        for helper, support, code, required, source, case in cases:
            with self.subTest(helper=helper, support=support, source=source, case=case), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                env = self.fixture(root, new_helper=helper)
                output = root / 'output'
                env.update(SUPPORT_STATUS=str(support), GITHUB_OUTPUT=str(output), CANDIDATE_SHA=source,
                           VALIDATION_CASE_ID=case)
                result = subprocess.run(['bash', '-c', shell], cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertEqual(output.read_text() if output.exists() else None,
                                 'required=' + required + '\n' if required else None)
                if helper:
                    calls = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()]
                    self.assertEqual(calls, [{'kind': 'race_native', 'args': ['supports', '--commit', source,
                                      '--case-id', case]}])

    def test_race_actual_admission_requires_native_and_runtime_before_exporting_directory(self):
        shell = self.chain_shell_step('Verify exact-source race native and runtime evidence')
        for case in ('heal.race-unversioned-overwrite', 'heal.race-versioned-put', 'heal.race-version-delete',
                     'heal.race-markers-and-batch-delete', 'heal.race-multipart-complete-abort',
                     'heal.admin-nolock-safety', 'heal.race-copy-and-metadata-update'):
            for native_exit, runtime in [(0, 0), (42, 0), (0, 42), (0, 'missing'), (0, 'collector-only')]:
                with self.subTest(case=case, native_exit=native_exit, runtime=runtime), tempfile.TemporaryDirectory() as name:
                    root = Path(name)
                    env = self.fixture(root, new_helper=True)
                    directory = root / 'auto-testing/scripts/validation_heal'
                    runtime_driver = 'nolock.py' if case == 'heal.admin-nolock-safety' else 'race.py'
                    (directory / 'race_native_evidence.py').write_text(
                        'import os,sys\nprint("{}")\nsys.exit(int(os.environ["VERIFY_EXIT"]))\n')
                    if runtime == 'collector-only':
                        (directory / runtime_driver).write_text(
                            'import argparse\np=argparse.ArgumentParser()\np.add_argument("--rc-bin", required=True)\np.parse_args()\n')
                    elif runtime != 'missing':
                        (directory / runtime_driver).write_text(
                            'import json,os,pathlib,sys\n'
                            'assert not pathlib.Path(os.environ["GITHUB_ENV"]).exists()\n'
                            'assert sys.argv[1] == "preflight"\n'
                            'assert sys.argv[-2:] == ["--case-id",os.environ["VALIDATION_CASE_ID"]]\n'
                            'assert pathlib.Path(os.environ["VALIDATION_HEAL_NATIVE_DIRECTORY"],"tool-run.json").is_file()\n'
                            'pathlib.Path("runtime-call.json").write_text(json.dumps(sys.argv[1:]))\n'
                            'sys.exit(int(os.environ["RUNTIME_EXIT"]))\n')
                    validation = root / 'validation'
                    validation.mkdir()
                    (validation / 'tool-run.json').write_text('{"id":123}')
                    native = root / 'rustfs-validation-native-77-1'
                    native.mkdir()
                    output = root / 'env'
                    env.update(RUNNER_TEMP=str(root), GITHUB_RUN_ID='77', GITHUB_RUN_ATTEMPT='1',
                               VALIDATION_ROOT=str(validation), GITHUB_ENV=str(output), VERIFY_EXIT=str(native_exit),
                               RUNTIME_EXIT=str(runtime), VALIDATION_CASE_ID=case)
                    result = subprocess.run(['bash', '-c', shell], cwd=root, env=env, capture_output=True, text=True)
                    expected = native_exit or (runtime if isinstance(runtime, int) else 2)
                    self.assertEqual(result.returncode, expected, result.stderr)
                    self.assertEqual((native / 'tool-run.json').read_bytes(), (validation / 'tool-run.json').read_bytes())
                    self.assertEqual(output.exists(), expected == 0)
                    self.assertEqual((root / 'runtime-call.json').exists(), native_exit == 0 and isinstance(runtime, int))
                    if expected == 0:
                        self.assertEqual(output.read_text(), 'VALIDATION_HEAL_NATIVE_DIRECTORY=' + str(native) + '\n')
        workflow = (candidate.ROOT / '.github/workflows/rustfs-functional-chain.yml').read_text()
        self.assertLess(workflow.index('Verify exact-source race native and runtime'),
                        workflow.index('Run selected canonical validation case'))
        self.assertIn("steps.race_native.outputs.required == 'true'", workflow)
        self.assertIn("inputs.validation_case == 'heal.race-markers-and-batch-delete'", workflow)
        self.assertIn("inputs.validation_case == 'heal.admin-nolock-safety'", workflow)
        self.assertIn("inputs.validation_case == 'heal.race-copy-and-metadata-update'", workflow)
        selection = workflow.split('      - name: Select exact-source race native evidence\n', 1)[1].split('\n        env:', 1)[0]
        self.assertIn("inputs.validation_case == 'heal.race-multipart-complete-abort'", selection)

    def test_i05_native_prerequisite_propagates_failure_and_exports_only_after_verification(self):
        shell = self.chain_shell_step('Verify I05 source-plan or queued-successor native prerequisites')
        for branch in ('source-plan', 'queued-successor'):
            for exit_code in (0, 42):
                with self.subTest(branch=branch, exit_code=exit_code), tempfile.TemporaryDirectory() as name:
                    root = Path(name)
                    env = self.fixture(root, new_helper=True)
                    script_name = 'lock_source.py' if branch == 'source-plan' else 'race_native_evidence.py'
                    script = root / 'auto-testing/scripts/validation_heal' / script_name
                    script.write_text('import os,sys\n'
                        'assert sys.argv[1] == "--directory"\n'
                        'assert "--candidate" in sys.argv\n'
                        'assert sys.argv[sys.argv.index("--testing-commit") + 1] == os.environ["AUTO_TESTING_SHA"]\n'
                        'if os.environ["I05_DIAGNOSTIC_BRANCH"] == "queued-successor":\n'
                        '    assert sys.argv[-2:] == ["--case-id", "heal.lock-timeout-and-caller-abort"]\n'
                        'else:\n'
                        '    assert "--case-id" not in sys.argv\n'
                        'sys.exit(int(os.environ["VERIFY_EXIT"]))\n')
                    validation = root / 'validation'; validation.mkdir()
                    (validation / 'tool-run.json').write_text('{"id":123}')
                    native = root / 'rustfs-validation-native-77-1'; native.mkdir()
                    output = root / 'env'
                    env.update(RUNNER_TEMP=str(root), GITHUB_RUN_ID='77', GITHUB_RUN_ATTEMPT='1',
                               VALIDATION_ROOT=str(validation), GITHUB_ENV=str(output), VERIFY_EXIT=str(exit_code),
                               I05_DIAGNOSTIC_BRANCH=branch)
                    result = subprocess.run(['bash', '-c', shell], cwd=root, env=env, capture_output=True, text=True)
                    self.assertEqual(result.returncode, exit_code, result.stderr)
                    self.assertEqual(output.exists(), exit_code == 0)
                    self.assertEqual((native / 'tool-run.json').read_bytes(), (validation / 'tool-run.json').read_bytes())
        workflow = (candidate.ROOT / '.github/workflows/rustfs-functional-chain.yml').read_text()
        self.assertLess(workflow.index('Verify I05 source-plan or queued-successor native prerequisites'),
                        workflow.index('Run selected canonical validation case'))
        download = workflow.split('      - name: Download pinned native storage evidence\n', 1)[1].split('        uses:', 1)[0]
        self.assertIn("inputs.i05_diagnostic_branch != 'timeout'", download)
        execution = workflow.split('      - name: Run selected canonical validation case\n', 1)[1].split('        run:', 1)[0]
        self.assertIn('I05_DIAGNOSTIC_BRANCH: ${{ inputs.i05_diagnostic_branch }}', execution)

    def test_f07_directory_fsync_artifact_is_candidate_scoped_and_bound(self):
        shell = self.chain_shell_step('Bind F07 exact-candidate directory fsync evidence')
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            validation = root / 'validation'; validation.mkdir()
            (validation / 'tool-run.json').write_text('{"id":123}')
            native = root / 'rustfs-validation-native-77-1'; native.mkdir()
            output = root / 'env'
            env = dict(os.environ, RUNNER_TEMP=str(root), GITHUB_RUN_ID='77', GITHUB_RUN_ATTEMPT='1',
                       VALIDATION_ROOT=str(validation), GITHUB_ENV=str(output))
            result = subprocess.run(['bash', '-c', shell], cwd=root, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((native / 'tool-run.json').read_bytes(), (validation / 'tool-run.json').read_bytes())
            self.assertEqual(output.read_text(), 'VALIDATION_HEAL_NATIVE_DIRECTORY=' + str(native) + '\n')
        workflow = (candidate.ROOT / '.github/workflows/rustfs-functional-chain.yml').read_text()
        binding = workflow.split('      - name: Bind F07 exact-candidate directory fsync evidence\n', 1)[1]
        guard = binding.split('        run:', 1)[0]
        self.assertIn("inputs.validation_case == 'heal.checkpoint-and-mrf-corruption'", guard)
        self.assertIn("steps.candidate.outputs.source_sha == '1aa9be87d18cf941e5020f19e8622c26ba4b10ac'", guard)
        self.assertLess(workflow.index('Bind F07 exact-candidate directory fsync evidence'),
                        workflow.index('Run selected canonical validation case'))

    def shell_step(self, name):
        from check_test_wiring import yaml_block
        source = (candidate.ROOT / '.github/workflows/rustfs-validation-tools.yml').read_text()
        job = yaml_block(source.splitlines(), 'native-heal-regressions', 2)
        start = job.index('      - name: ' + name)
        end = next((index for index in range(start + 1, len(job))
                    if job[index].startswith('      - name: ')), len(job))
        step = job[start:end]
        return '\n'.join(line[10:] for line in step[step.index('        run: |') + 1:]
                         if not line.strip() or line.startswith('          '))

    def fixture(self, root, *, new_helper):
        directory = root / 'auto-testing/scripts/validation_heal'
        directory.mkdir(parents=True)
        stub = '''import json, os, pathlib, sys
kind = pathlib.Path(__file__).stem
with pathlib.Path('calls.jsonl').open('a') as out:
    out.write(json.dumps({'kind': kind, 'args': sys.argv[1:]}) + '\\n')
if sys.argv[1] == 'supports': sys.exit(int(os.environ['SUPPORT_STATUS']))
sys.exit(int(os.environ.get(kind.upper() + '_FAIL', '0')))
'''
        (directory / 'native_regressions.py').write_text(stub)
        if new_helper: (directory / 'race_native.py').write_text(stub)
        bin_dir = root / 'bin'
        bin_dir.mkdir()
        git = bin_dir / 'git'
        git.write_text('#!/bin/sh\nprintf "%s\\n" "$FAKE_HEAD"\n')
        git.chmod(0o755)
        env = dict(os.environ, AUTO_TESTING_SHA='b' * 40, CANDIDATE_SHA='a' * 40,
                   FAKE_HEAD='b' * 40, SUPPORT_STATUS='2', PATH=str(bin_dir) + os.pathsep + os.environ['PATH'])
        env.pop('BASH_ENV', None)
        return env

    def test_actual_native_build_routes_old_new_unknown_and_failed_collectors(self):
        shell = self.shell_step('Build and enumerate the unmodified candidate test executable')
        for mode, helper, support, failure, head, expected_code, expected_calls in [
            ('old-harness', False, 2, None, 'b', 0, [('native_regressions', 'build')]),
            ('legacy', True, 2, None, 'b', 0, [('race_native', 'supports'), ('native_regressions', 'build')]),
            ('race', True, 0, None, 'b', 0, [('race_native', 'supports'), ('race_native', 'build')]),
            ('support-error', True, 42, None, 'b', 42, [('race_native', 'supports')]),
            ('invalid-support', True, 1, None, 'b', 1, [('race_native', 'supports')]),
            ('race-build-error', True, 0, 'RACE_NATIVE_FAIL', 'b', 42, [('race_native', 'supports'), ('race_native', 'build')]),
            ('legacy-build-error', True, 2, 'NATIVE_REGRESSIONS_FAIL', 'b', 42, [('race_native', 'supports'), ('native_regressions', 'build')]),
            ('wrong-helper', True, 0, None, 'c', 1, []),
        ]:
            with tempfile.TemporaryDirectory() as temp, self.subTest(mode=mode):
                root = Path(temp)
                env = self.fixture(root, new_helper=helper)
                env.update(SUPPORT_STATUS=str(support), FAKE_HEAD=head * 40)
                if failure: env[failure] = '42'
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', shell],
                                        cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, expected_code, result.stderr)
                calls = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()] if (root / 'calls.jsonl').exists() else []
                self.assertEqual([(row['kind'], row['args'][0]) for row in calls], expected_calls)
                for row in calls:
                    if row['args'][0] == 'supports':
                        self.assertEqual(row['args'], ['supports', '--commit', 'a' * 40])
                    else:
                        expected = ['build', '--source', 'candidate', '--commit', 'a' * 40]
                        if row['kind'] == 'race_native': expected += ['--testing-commit', 'b' * 40]
                        self.assertEqual(row['args'], expected + ['--out', 'native-tools'])

    def test_actual_native_run_rejects_ambiguous_formats_and_propagates_failures(self):
        shell = self.shell_step('Run exact native regressions separately from fleet validation')
        for mode, helper, manifests, failure, expected_code, expected_kind in [
            ('legacy', False, ['native-regressions.json'], None, 0, 'native_regressions'),
            ('race', True, ['race-native.json'], None, 0, 'race_native'),
            ('ambiguous', True, ['native-regressions.json', 'race-native.json'], None, 1, None),
            ('legacy-error', True, ['native-regressions.json'], 'NATIVE_REGRESSIONS_FAIL', 42, 'native_regressions'),
            ('race-error', True, ['race-native.json'], 'RACE_NATIVE_FAIL', 42, 'race_native'),
            ('missing-race-helper', False, ['race-native.json'], None, 2, None),
        ]:
            with tempfile.TemporaryDirectory() as temp, self.subTest(mode=mode):
                root = Path(temp)
                env = self.fixture(root, new_helper=helper)
                if failure: env[failure] = '42'
                (root / 'native-tools').mkdir()
                for manifest in manifests: (root / 'native-tools' / manifest).write_text('{}')
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', shell],
                                        cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, expected_code, result.stderr)
                calls = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()] if (root / 'calls.jsonl').exists() else []
                self.assertEqual(len(calls), int(expected_kind is not None))
                if expected_kind:
                    self.assertEqual(calls, [{'kind': expected_kind, 'args': ['run', '--directory', 'native-tools', '--out', 'native-evidence']}])

    def test_native_upload_retains_both_fixed_executable_kinds(self):
        source = (candidate.ROOT / '.github/workflows/rustfs-validation-tools.yml').read_text()
        self.assertIn('            native-tools/ecstore-native-tests\n', source)
        self.assertIn('            native-tools/ecstore-race-native-tests\n', source)


class PersistenceToolsWorkflowTests(unittest.TestCase):
    def test_supplement_preserves_old_pins_and_propagates_actual_collector_failure(self):
        from check_test_wiring import yaml_block
        source = (candidate.ROOT / '.github/workflows/rustfs-validation-tools.yml').read_text()
        job = yaml_block(source.splitlines(), 'native-heal-regressions', 2)
        start = job.index('      - name: Run existing F07 directory fsync failure regressions')
        end = job.index('      - name: Upload native executable identities and original diagnostics')
        step = job[start:end]
        shell = '\n'.join(line[10:] for line in step[step.index('        run: |') + 1:]
                          if not line.strip() or line.startswith('          '))
        for mode in ('old-harness', 'success', 'run-failure', 'verify-failure'):
            with tempfile.TemporaryDirectory() as temp, self.subTest(mode=mode):
                root = Path(temp)
                if mode != 'old-harness':
                    collector = root / 'auto-testing/scripts/validation_heal/persistence_native.py'
                    collector.parent.mkdir(parents=True)
                    collector.write_text('''import json, os, pathlib, sys
with pathlib.Path('calls.jsonl').open('a') as out: out.write(json.dumps(sys.argv[1:]) + '\\n')
sys.exit(42 if os.environ['COLLECTOR_MODE'] == sys.argv[1] + '-failure' else 0)
''')
                env = dict(os.environ, AUTO_TESTING_SHA='b' * 40, COLLECTOR_MODE=mode)
                env.pop('BASH_ENV', None)
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', shell],
                                        cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, mode in ('old-harness', 'success'), result.stderr)
                if mode == 'old-harness':
                    report = json.loads((root / 'native-persistence-evidence/not-available.json').read_text())
                    self.assertIs(report['complete'], False)
                    self.assertIs(report['requiredNativeCoverageComplete'], False)
                    self.assertIs(report['canonicalCasePassClaimed'], False)
                    self.assertFalse((root / 'calls.jsonl').exists())
                else:
                    calls = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()]
                    self.assertEqual([call[0] for call in calls], ['run'] if mode == 'run-failure' else ['run', 'verify'])
                    for call in calls: self.assertEqual(call[-2:], ['--testing-commit', 'b' * 40])


class InterruptedCleanupGuardTests(unittest.TestCase):
    def test_actual_workflow_guard_rejects_live_success_and_unrelated_runs(self):
        source = (candidate.ROOT / ".github/workflows/rustfs-functional-chain.yml").read_text()
        step = source[source.index('      - name: Audit and finalize retained validation environment'):]
        guard = step.split("          python3 - <<'PY'", 1)[1].split("          PY", 1)[0]
        guard = "\n".join(line[10:] for line in guard.splitlines())
        for state, conclusion, branch, workflow, accepted in (
            ("completed", "failure", "topic", "RustFS Functional Chain", True),
            ("completed", "cancelled", "topic", "RustFS Functional Chain", True),
            ("in_progress", "cancelled", "topic", "RustFS Functional Chain", False),
            ("completed", "success", "topic", "RustFS Functional Chain", False),
            ("completed", "cancelled", "other", "RustFS Functional Chain", False),
            ("completed", "cancelled", "topic", "Other Workflow", False),
        ):
            with self.subTest(state=state, conclusion=conclusion, branch=branch, workflow=workflow), tempfile.TemporaryDirectory() as temp:
                Path(temp, "failed-run.json").write_text(json.dumps({"status": state, "conclusion": conclusion,
                    "headBranch": branch, "workflowName": workflow}))
                with mock.patch.dict(os.environ, VALIDATION_ROOT=temp, GITHUB_REF_NAME="topic"):
                    if accepted:
                        exec(compile(guard, "workflow-cleanup-guard", "exec"), {})
                    else:
                        with self.assertRaises(ValueError):
                            exec(compile(guard, "workflow-cleanup-guard", "exec"), {})


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.run = {"id": 123, "run_attempt": 2, "head_sha": "a" * 40}
        self.manifest = {"schema": 2, "workflow_sha": "a" * 40, "source_sha": "b" * 40, "source_ref": "release",
                         "build_run_id": 123, "build_run_attempt": 2, "package_sha256": "c" * 64,
                         "package_url": "https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/123/2/" + "c" * 64 + "/rustfs.deb"}

    def archive(self, names=None):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for name in names or ["nightly-candidate-123-2.json"]:
                archive.writestr(name, json.dumps(self.manifest))
        payload = output.getvalue()
        artifact = {"id": 789, "name": "nightly-candidate-123-2", "expired": False, "size_in_bytes": len(payload),
                    "digest": "sha256:" + hashlib.sha256(payload).hexdigest(), "workflow_run": {"id": 123, "head_sha": "a" * 40}}
        return payload, artifact

    def test_distinct_build_source_preserves_both_identities(self):
        payload, artifact = self.archive()
        result = candidate.read_manifest(payload, artifact, self.run)
        self.assertEqual(result["source_sha"], "b" * 40)
        self.assertEqual(result["workflow_sha"], "a" * 40)
        self.assertEqual(result["source_ref"], "release")

    def test_legacy_requires_the_build_and_workflow_sha_to_agree(self):
        self.manifest["schema"] = 1
        del self.manifest["workflow_sha"], self.manifest["source_ref"]
        with self.assertRaisesRegex(ValueError, "legacy"):
            candidate.validate_manifest(self.manifest, self.run)
        self.manifest["source_sha"] = self.run["head_sha"]
        candidate.validate_manifest(self.manifest, self.run)

    def test_manifest_substitutions_fail(self):
        for key, value in (("workflow_sha", "d" * 40), ("build_run_id", 124), ("build_run_attempt", 1),
                           ("package_sha256", "d" * 64), ("package_url", "https://example.com/package.deb"),
                           ("schema", True), ("source_ref", "release\nFORGED=value")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                candidate.validate_manifest({**self.manifest, key: value}, self.run)

    def test_artifact_substitutions_and_archive_members_fail(self):
        payload, artifact = self.archive()
        for key, value in (("expired", True), ("name", "nightly-candidate-123-1"), ("size_in_bytes", 1),
                           ("digest", "sha256:" + "d" * 64), ("workflow_run", {"id": 124, "head_sha": "a" * 40})):
            with self.subTest(key=key), self.assertRaises(ValueError):
                candidate.read_manifest(payload, {**artifact, key: value}, self.run)
        for names in (["../nightly-candidate-123-2.json"], ["nightly-candidate-123-2.json", "extra.json"]):
            payload, artifact = self.archive(names)
            with self.assertRaises(ValueError):
                candidate.read_manifest(payload, artifact, self.run)

    def test_resolver_uses_attempt_metadata_and_does_not_resolve_moving_branch(self):
        payload, artifact = self.archive()
        run = {**self.run, "path": ".github/workflows/nightly-gnu.yml", "head_branch": "main", "head_repository": {"full_name": "rustfs/rustfs"},
               "event": "schedule", "status": "completed", "conclusion": "success", "run_started_at": "2026-09-12T00:00:00Z"}
        with mock.patch.object(candidate, "api", side_effect=[run, {"artifacts": [artifact]}, payload]) as api:
            result = candidate.resolve(123, 2)
        self.assertTrue(api.call_args_list[0].args[0].endswith("/attempts/2"))
        self.assertFalse(any("branches/" in call.args[0] for call in api.call_args_list))
        self.assertEqual(result["artifact_id"], 789)
        self.assertEqual(result["manifest"]["source_sha"], "b" * 40)

    def test_validation_manifest_binds_requested_source_and_private_channel(self):
        manifest = {**self.manifest, "schema": 3, "channel": "validation", "source_ref": self.manifest["source_sha"],
                    "package_url": self.manifest["package_url"].replace("/packages/nightly/", "/packages/validation/")}
        run = {**self.run, "path": ".github/workflows/rustfs-validation-package.yml"}
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr("validation-candidate-123-2.json", json.dumps(manifest))
        raw = payload.getvalue()
        artifact = {"id": 789, "name": "validation-candidate-123-2", "expired": False,
                    "size_in_bytes": len(raw), "digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
                    "workflow_run": {"id": 123, "head_sha": "a" * 40}}
        result = candidate.read_manifest(raw, artifact, run)
        self.assertEqual(result["channel"], "validation")
        self.assertIn("/packages/validation/runs/123/2/", result["package_url"])

    def test_validation_resolver_requires_manual_workflow_dispatch(self):
        payload, artifact = self.archive()
        run = {"id": 123, "run_attempt": 2, "head_sha": "a" * 40,
               "path": ".github/workflows/rustfs-validation-package.yml", "head_branch": "main",
               "head_repository": {"full_name": "rustfs/rustfs"}, "event": "schedule",
               "status": "completed", "conclusion": "success", "run_started_at": "2026-10-12T00:00:00Z"}
        with mock.patch.object(candidate, "api", side_effect=[run]):
            with self.assertRaisesRegex(ValueError, "manually dispatched"):
                candidate.resolve(123, 2, allow_validation=True)

    def test_validation_workflow_rejects_legacy_manifest_schema(self):
        manifest = {**self.manifest}
        payload_buffer = io.BytesIO()
        with zipfile.ZipFile(payload_buffer, "w") as archive:
            archive.writestr("validation-candidate-123-2.json", json.dumps(manifest))
        payload = payload_buffer.getvalue()
        artifact = {"id": 789, "name": "validation-candidate-123-2", "expired": False,
                    "size_in_bytes": len(payload), "digest": "sha256:" + hashlib.sha256(payload).hexdigest(),
                    "workflow_run": {"id": 123, "head_sha": "a" * 40}}
        run = {**self.run, "path": ".github/workflows/rustfs-validation-package.yml"}
        with self.assertRaisesRegex(ValueError, "schema-3"):
            candidate.read_manifest(payload, artifact, run)

    def test_private_candidate_resolution_is_explicit_and_attempt_bound(self):
        self.manifest.update(schema=3, channel="validation", source_ref=self.manifest["source_sha"],
                             package_url=self.manifest["package_url"].replace("/packages/nightly/", "/packages/validation/"))
        payload, artifact = self.archive(["validation-candidate-123-2.json"])
        artifact["name"] = "validation-candidate-123-2"
        base = {**self.run, "head_branch": "cxymds/validation-runner", "head_repository": {"full_name": "rustfs/rustfs"},
                "event": "workflow_dispatch", "status": "completed", "conclusion": "success",
                "run_started_at": "2026-10-12T00:00:00Z"}
        for path in candidate.VALIDATION_BUILD_WORKFLOWS:
            run = {**base, "path": path}
            with self.subTest(path=path), mock.patch.object(candidate, "api", side_effect=[run, {"artifacts": [artifact]}, payload]):
                resolved = candidate.resolve(123, 2, allow_validation=True)
                self.assertEqual(resolved["manifest"]["source_sha"], "b" * 40)
                self.assertEqual(resolved["workflow_sha"], "a" * 40)
            with mock.patch.object(candidate, "api", return_value=run), self.assertRaises(ValueError):
                candidate.resolve(123, 2)
        for field, value in (("id", 124), ("run_attempt", 1), ("event", "push"), ("status", "in_progress"),
                             ("conclusion", "failure"), ("path", ".github/workflows/other.yml"),
                             ("head_repository", {"full_name": "untrusted/rustfs"})):
            run = {**base, "path": ".github/workflows/rustfs-functional-chain.yml", field: value}
            with self.subTest(field=field), mock.patch.object(candidate, "api", return_value=run), self.assertRaises(ValueError):
                candidate.resolve(123, 2, allow_validation=True)

    def test_private_manifest_rejects_alias_channel_source_and_schema_substitution(self):
        valid = {**self.manifest, "schema": 3, "channel": "validation", "source_ref": self.manifest["source_sha"],
                 "package_url": self.manifest["package_url"].replace("/packages/nightly/", "/packages/validation/")}
        for key, value in (("channel", "nightly"), ("source_ref", "main"), ("source_sha", "e" * 40),
                           ("package_url", self.manifest["package_url"]), ("workflow_sha", "e" * 40),
                           ("build_run_attempt", 3), ("schema", True)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                candidate.validate_manifest({**valid, key: value}, self.run)

    def test_package_only_mode_rejects_conflicting_fleet_and_tool_requests(self):
        from check_test_wiring import yaml_block
        workflow = Path(__file__).resolve().parents[1] / ".github/workflows/rustfs-functional-chain.yml"
        job = yaml_block(workflow.read_text().splitlines(), "validation-package-request", 2)
        begin = job.index("        run: |") + 1
        script = "\n".join(line[10:] for line in job[begin:] if not line.strip() or line.startswith("          "))
        import os
        inputs = {"validation_package_sha": "a" * 40}
        env = {**os.environ, "PACKAGE_INPUTS": json.dumps(inputs)}
        result = subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        for field in ("build_run_id", "build_run_attempt", "validation_case", "auto_testing_sha", "validation_tools_only",
                      "validation_native_tools", "validation_shared_tools", "validation_manager_tools",
                      "validation_cleanup_run_id", "validation_tools_run_id"):
            with self.subTest(field=field):
                env["PACKAGE_INPUTS"] = json.dumps({**inputs, field: "not-empty"})
                result = subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
        for value in ("main", "a" * 39, "a" * 40 + "\nFORGED=value"):
            env["PACKAGE_INPUTS"] = json.dumps({"validation_package_sha": value})
            self.assertNotEqual(subprocess.run(["bash", "-e", "-c", script], env=env, capture_output=True).returncode, 0)


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.chain = {"run_id": 456, "attempt": 1, "candidate": {"source_sha": "a" * 40, "workflow_sha": "b" * 40}}
        self.needs = {suite: {"result": "success"} for suite in evidence.SUITES}
        for suite in evidence.SUITES:
            value = {"schema": 1, "suite": suite, "chain": self.chain, "valid": True, "report_sha256": "c" * 64,
                     "counts": {"PASS": 1, "FAIL": 0, "SKIP": 0, "UNSUPPORTED": 0, "RUNNING": 0}}
            (self.directory / (suite + ".json")).write_text(json.dumps(value))

    def test_complete_chain_retains_source_identity(self):
        result = evidence.aggregate(self.chain, self.directory, self.needs)
        self.assertTrue(result["complete"])
        self.assertEqual(result["chain"], self.chain)
        self.assertEqual([suite["suite"] for suite in result["suites"]], [
            "upgrade", "s3", "kms", "tier", "storage", "heal", "pool", "security", "replication",
            "fault-tolerance", "table", "performance"])

    def test_release_suites_cannot_be_missing_failed_or_have_missing_artifacts(self):
        for suite in ("fault-tolerance", "table"):
            with self.subTest(suite=suite):
                missing = {key: value for key, value in self.needs.items() if key != suite}
                with self.assertRaises(ValueError):
                    evidence.aggregate(self.chain, self.directory, missing)
                with self.assertRaises(ValueError):
                    evidence.aggregate(self.chain, self.directory, {**self.needs, suite: {"result": "failure"}})
                path = self.directory / (suite + ".json")
                original = path.read_text()
                path.unlink()
                with self.assertRaises(ValueError):
                    evidence.aggregate(self.chain, self.directory, self.needs)
                path.write_text(original)

    def test_failure_report_preserves_counts_without_weakening_success_gate(self):
        path = self.directory / "s3.json"
        record = json.loads(path.read_text())
        record.update(valid=False, counts={**record["counts"], "FAIL": 2}, error="failed cases")
        path.write_text(json.dumps(record))
        needs = {**self.needs, "s3": {"result": "failure"}, "performance": {"result": "cancelled"}}
        (self.directory / "performance.json").unlink()
        result = evidence.summarize(self.chain, self.directory, needs)
        self.assertFalse(result["complete"])
        self.assertEqual(result["lanes"][1]["evidence"]["counts"]["FAIL"], 2)
        self.assertEqual(result["lanes"][-1]["result"], "cancelled")
        self.assertIsNone(result["lanes"][-1]["evidence"])
        with self.assertRaises(ValueError):
            evidence.aggregate(self.chain, self.directory, needs)

    def test_preparation_failure_still_has_all_required_lanes(self):
        result = evidence.summarize(None, self.directory / "absent", {"prepare": {"result": "failure"}})
        self.assertFalse(result["complete"])
        self.assertEqual([lane["suite"] for lane in result["lanes"]], list(evidence.SUITES))
        self.assertIn("| performance | missing | missing |", evidence.render_summary(result))
        self.assertIn("Preparation: failure", evidence.render_summary(result))

    def test_malformed_or_cross_attempt_record_is_not_reused(self):
        for payload in ("broken json", "[]", json.dumps({"suite": "s3", "chain": {"attempt": 9}})):
            (self.directory / "s3.json").write_text(payload)
            result = evidence.summarize(self.chain, self.directory, self.needs)
            self.assertFalse(result["complete"])
            self.assertIsNone(result["lanes"][1]["evidence"])

    def test_missing_failed_cancelled_or_skipped_lane_never_passes(self):
        for state in ("failure", "cancelled", "skipped", "pending"):
            with self.subTest(state=state), self.assertRaises(ValueError):
                evidence.aggregate(self.chain, self.directory, {**self.needs, "s3": {"result": state}})
        del self.needs["s3"]
        with self.assertRaises(ValueError):
            evidence.aggregate(self.chain, self.directory, self.needs)

    def test_partial_rerun_missing_artifact_and_zero_test_fail(self):
        path = self.directory / "s3.json"
        original = json.loads(path.read_text())
        values = [{**original, "chain": {**self.chain, "attempt": 2}},
                  {**original, "counts": {**original["counts"], "PASS": 0}},
                  {**original, "counts": {**original["counts"], "FAIL": 1}},
                  {**original, "valid": False}, {**original, "report_sha256": ""}]
        for value in values:
            path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):
                evidence.aggregate(self.chain, self.directory, self.needs)
        path.unlink()
        with self.assertRaises(ValueError):
            evidence.aggregate(self.chain, self.directory, self.needs)

    def test_reports_count_case_status_not_cleanup_status(self):
        text = "| Topology | Case | Name | Status | Cleanup |\n| --- | --- | --- | --- | --- |\n| sns | TIER-1 | test | UNSUPPORTED | PASS |\n| sns | TIER-2 | test | FAIL | PASS |\n"
        counts = evidence.report_counts(text)
        self.assertEqual(counts["PASS"], 0)
        self.assertEqual(counts["FAIL"], 1)
        self.assertEqual(counts["UNSUPPORTED"], 1)
        self.assertEqual(evidence.report_counts("")["PASS"], 0)

    def test_performance_needs_real_complete_metrics(self):
        header = "method\tsize\tthroughput\tobj_per_s\treq_avg\treq_p50\treq_p90\treq_p99\n"
        row = "put\t1MiB\t100MiB/s\t100\t1ms\t1ms\t2ms\t3ms\n"
        self.assertEqual(evidence.report_counts(header + row, True)["PASS"], 1)
        self.assertEqual(evidence.report_counts(header, True)["PASS"], 0)
        with self.assertRaises(ValueError):
            evidence.report_counts(header + row + row, True)
        with self.assertRaises(ValueError):
            evidence.report_counts(header + "put\t1MiB\t\t\t\t\t\t\n", True)

    def test_fault_tolerance_requires_complete_known_verdicts(self):
        passing = "FT-CASE: A-read verdict=pass observed expected result\n"
        divergent = "FT-CASE: C2-read verdict=known-divergence lock majority unavailable\n"
        summary = "FT-SUMMARY: unexpected=0 known-divergence=1 strict=0\n"
        counts = evidence.fault_tolerance_counts(passing + divergent + summary)
        self.assertEqual(counts["PASS"], 1)
        self.assertEqual(counts["UNSUPPORTED"], 1)
        for invalid in (passing, passing + passing + summary, passing + divergent + summary.replace("strict=0", "strict=1"),
                        passing + divergent + summary.replace("known-divergence=1", "known-divergence=0"),
                        passing.replace("verdict=pass", "verdict=UNKNOWN") + summary):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                evidence.fault_tolerance_counts(invalid)


class EnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / ".config").mkdir()
        (self.root / ".config/functional-script-revision.txt").write_text("d" * 40)
        manifest = {"schema": 2, "workflow_sha": "a" * 40, "source_sha": "b" * 40, "source_ref": "release",
                    "build_run_id": 123, "build_run_attempt": 2, "package_sha256": "c" * 64,
                    "package_url": "https://dl.rustfs.com/artifacts/rustfs/packages/nightly/runs/123/2/" + "c" * 64 + "/rustfs.deb"}
        self.chain = {"schema": 1, "run_id": 456, "attempt": 3, "workflow_sha": "e" * 40, "testing_sha": "d" * 40,
                      "candidate": {"manifest": manifest, "artifact_id": 789, "artifact_digest": "sha256:" + "f" * 64,
                                    "workflow_sha": "a" * 40, "workflow_ref": "main", "build_started_at": "2026-09-12T00:00:00Z"}}
        self.env = {"CHAIN_MANIFEST": json.dumps(self.chain), "GITHUB_RUN_ID": "456", "GITHUB_RUN_ATTEMPT": "3", "GITHUB_SHA": "e" * 40,
                    "GITHUB_ENV": str(self.root / "env"), "GITHUB_OUTPUT": str(self.root / "output"),
                    "CHAIN_JOB_STATUS": "success", "CHAIN_TEST_OUTCOME": "success", "CHAIN_REPORT_OUTCOME": "success"}

    def test_consumer_exports_the_same_package_and_checksum_to_installers(self):
        with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, self.env), mock.patch.object(evidence.subprocess, "check_output", return_value="e" * 40):
            evidence.consume(evidence.current_chain())
        variables = dict(line.split("=", 1) for line in (self.root / "env").read_text().splitlines())
        self.assertEqual(variables["PACKAGE_SHA256"], "c" * 64)
        self.assertEqual(variables["TO_SHA256"], "c" * 64)
        self.assertEqual(variables["RUSTFS_NIGHTLY_PACKAGE_URL"], self.chain["candidate"]["manifest"]["package_url"])

    def test_partial_rerun_and_wrong_lane_checkout_fail_before_install(self):
        for changes, head in (({"GITHUB_RUN_ATTEMPT": "4"}, "e" * 40), ({}, "f" * 40), ({"GITHUB_RUN_ID": "457"}, "e" * 40)):
            with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, {**self.env, **changes}), mock.patch.object(evidence.subprocess, "check_output", return_value=head), self.assertRaises(ValueError):
                evidence.current_chain()
        self.assertFalse((self.root / "env").exists())

    def test_testing_sha_fallback_is_accepted_while_garbage_is_rejected(self):
        # prepare's >24h staleness fallback legitimately sets testing_sha to
        # auto-testing main HEAD, which differs from the committed pin; only
        # the sha format is an invariant now.
        for testing_sha, ok in (("d" * 40, True), ("1" * 40, True), ("xyz", False), ("", False)):
            chain = dict(self.chain, testing_sha=testing_sha)
            env = dict(self.env, CHAIN_MANIFEST=json.dumps(chain))
            if ok:
                with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, env), mock.patch.object(evidence.subprocess, "check_output", return_value="e" * 40):
                    evidence.consume(evidence.current_chain())
            else:
                with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, env), mock.patch.object(evidence.subprocess, "check_output", return_value="e" * 40), self.assertRaises(ValueError):
                    evidence.current_chain()

    def test_report_or_swallowed_test_failure_cannot_produce_valid_evidence(self):
        report = self.root / "cases.md"
        report.write_text("| Case | Name | Status |\n| --- | --- | --- |\n| KMS-1 | fixture | PASS |\n")
        for index, (key, status) in enumerate((("CHAIN_TEST_OUTCOME", "failure"), ("CHAIN_REPORT_OUTCOME", "failure"), ("CHAIN_JOB_STATUS", "cancelled"))):
            output = self.root / str(index) / "kms.json"
            Path(self.env["GITHUB_OUTPUT"]).unlink(missing_ok=True)
            with mock.patch.dict(evidence.os.environ, {**self.env, key: status}), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40), self.assertRaises(ValueError):
                evidence.record(self.chain, "kms", report, output)
            self.assertFalse(json.loads(output.read_text())["valid"])
            self.assertIn("error", json.loads(output.read_text()))
            self.assertEqual(Path(self.env["GITHUB_OUTPUT"]).read_text(), "written=true\n")
        output = self.root / "success" / "kms.json"
        with mock.patch.dict(evidence.os.environ, self.env), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40):
            evidence.record(self.chain, "kms", report, output)
        self.assertTrue(json.loads(output.read_text())["valid"])

    def test_collision_or_failed_write_never_authorizes_evidence_upload(self):
        report = self.root / "cases.md"
        report.write_text("| Case | Status |\n| --- | --- |\n| KMS-1 | PASS |\n")
        output = self.root / "stale" / "kms.json"
        output.parent.mkdir()
        output.write_text("OLD RUN EVIDENCE")
        with mock.patch.dict(evidence.os.environ, self.env), mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40):
            with self.assertRaises(FileExistsError):
                evidence.record(self.chain, "kms", report, output)
            self.assertEqual(output.read_text(), "OLD RUN EVIDENCE")
            self.assertFalse(Path(self.env["GITHUB_OUTPUT"]).exists())
            with mock.patch.object(Path, "write_text", side_effect=OSError("disk full")), self.assertRaises(OSError):
                evidence.record(self.chain, "kms", report, self.root / "new" / "kms.json")
            self.assertFalse(Path(self.env["GITHUB_OUTPUT"]).exists())

    def test_record_uses_workspace_testing_checkout_for_nested_lane(self):
        lane_checkout = self.root / "rustfs-repo"
        lane_checkout.mkdir()
        private_checkout = self.root / "auto-testing"
        private_checkout.mkdir()
        subprocess.run(["git", "init", "--quiet", str(private_checkout)], check=True)
        subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                        "-c", "commit.gpgSign=false", "commit", "--allow-empty", "-qm", "fixture"],
                       cwd=private_checkout, check=True)
        private_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=private_checkout, text=True).strip()
        chain = {**self.chain, "testing_sha": private_head}
        report = self.root / "security.md"
        report.write_text("| Case | Status |\n| --- | --- |\n| SEC-1 | PASS |\n")
        env = {**self.env, "GITHUB_WORKSPACE": str(self.root)}
        output = self.root / "nested-success" / "security.json"
        with mock.patch.object(evidence, "ROOT", lane_checkout), mock.patch.dict(evidence.os.environ, env):
            evidence.record(chain, "security", report, output)
            self.assertTrue(json.loads(output.read_text())["valid"])
            wrong_pin_output = self.root / "nested-wrong-pin" / "security.json"
            with self.assertRaisesRegex(ValueError, "suite used a different private script revision"):
                evidence.record({**chain, "testing_sha": "f" * 40}, "security", report, wrong_pin_output)
            self.assertFalse(json.loads(wrong_pin_output.read_text())["valid"])
        fallback_output = self.root / "local-success" / "security.json"
        with mock.patch.object(evidence, "ROOT", self.root), mock.patch.dict(evidence.os.environ, self.env):
            evidence.os.environ.pop("GITHUB_WORKSPACE", None)
            evidence.record(chain, "security", report, fallback_output)
            self.assertTrue(json.loads(fallback_output.read_text())["valid"])

    def test_unknown_status_cannot_hide_among_passing_cases(self):
        text = "| Case | Name | Status |\n| --- | --- | --- |\n| KMS-1 | fixture | PASS |\n| KMS-2 | fixture | NOT RUN |\n"
        with self.assertRaises(ValueError):
            evidence.report_counts(text)

    def test_release_suite_records_require_successful_execution_and_complete_reports(self):
        reports = {
            "table": "| Case | Name | Status |\n| --- | --- | --- |\n| TBL-101 | Iceberg smoke | PASS |\n",
            "fault-tolerance": "FT-CASE: A-read verdict=pass read succeeded\nFT-SUMMARY: unexpected=0 known-divergence=0 strict=0\n",
        }
        for suite, text in reports.items():
            report = self.root / (suite + ".txt")
            report.write_text(text)
            for status in ("success", "failure"):
                with self.subTest(suite=suite, status=status):
                    output = self.root / (suite + "-" + status) / (suite + ".json")
                    with mock.patch.dict(evidence.os.environ, {**self.env, "CHAIN_TEST_OUTCOME": status}), \
                            mock.patch.object(evidence.subprocess, "check_output", return_value="d" * 40):
                        if status == "success":
                            evidence.record(self.chain, suite, report, output)
                        else:
                            with self.assertRaises(ValueError):
                                evidence.record(self.chain, suite, report, output)
                    self.assertEqual(json.loads(output.read_text())["valid"], status == "success")

    def test_driver_passes_one_manifest_and_runs_every_lane_after_failure(self):
        from check_test_wiring import yaml_block
        lines = (candidate.ROOT / ".github/workflows/rustfs-functional-chain.yml").read_text().splitlines()
        previous = None
        for suite in evidence.SUITES:
            job = "\n".join(yaml_block(lines, suite, 2))
            self.assertIn("needs: [prepare" + (", " + previous if previous and suite != "performance" else "") + "]", job)
            selected = " && contains(fromJSON(needs.prepare.outputs.suites || '[]'), '" + suite + "')"
            if suite == "performance":
                # gated on the preflight probe: runs only when its fleet is online
                self.assertIn("if: ${{ always() && needs.prepare.result == 'success'" + selected + " && needs.prepare.outputs.performance_ready == 'online' }}", job)
            else:
                self.assertIn("if: ${{ always() && needs.prepare.result == 'success'" + selected + " }}", job)
            self.assertIn("chain_manifest: ${{ needs.prepare.outputs.manifest }}", job)
            previous = suite
        complete = "\n".join(yaml_block(lines, "complete-chain", 2))
        self.assertIn("needs: [prepare, " + ", ".join(evidence.SUITES) + "]", complete)
        self.assertIn("functional_chain_evidence.py aggregate", complete)
        self.assertIn("functional_chain_evidence.py summarize", complete)
        self.assertIn("functional-chain-report-", complete)
        self.assertIn("needs.prepare.result != 'skipped'", complete)
        prepare = "\n".join(yaml_block(lines, "prepare", 2))
        self.assertIn("Check shared functional fleet runner before scheduling suites", prepare)
        self.assertIn("check_functional_runners.py smoke-testing", prepare)
        self.assertIn("Probe performance fleet runner", prepare)
        self.assertIn("check_functional_runners.py pf-testing", prepare)
        self.assertIn("performance_ready: ${{ steps.perf_probe.outputs.performance_ready }}", prepare)

    def test_every_lane_retains_failed_evidence_and_identifies_its_own_attempt(self):
        paths = list((candidate.ROOT / ".github/workflows").glob("rustfs-*-test.yml"))
        lanes = [path for path in paths if "name: Upload chain evidence" in path.read_text()]
        self.assertEqual(len(lanes), len(evidence.SUITES))
        for path in lanes:
            with self.subTest(path=path.name):
                text = path.read_text()
                self.assertIn("if: ${{ always() && steps.chain_record.outputs.written == 'true' }}", text)
                self.assertIn("python3 auto-testing/scripts/issue_manager.py handle", text)
                self.assertIn('--run-id "${GITHUB_RUN_ID}"', text)
                self.assertIn('--attempt "${GITHUB_RUN_ATTEMPT}"', text)
                run_url = next(line for line in text.splitlines() if "--run-url" in line)
                self.assertIn('/attempts/${GITHUB_RUN_ATTEMPT}#summary"', run_url)

    def test_table_issue_manager_receives_outcome_and_case_evidence(self):
        text = (candidate.ROOT / ".github/workflows/rustfs-table-test.yml").read_text()
        self.assertIn("--outcome '${{ steps.test.outcome }}'", text)
        self.assertIn('--report "${FUNCTIONAL_ARTIFACTS_DIR}/cases.md"', text)

    def test_pool_topology_is_checked_before_cleanup_with_four_node_defaults(self):
        text = (candidate.ROOT / ".github/workflows/rustfs-pool-expand-test.yml").read_text()
        self.assertIn("http://rustfs-node4:9000", text)
        self.assertIn("vars.RUSTFS_NODES || 'vm000 vm001 vm002'", text)
        self.assertLess(text.index("name: Validate pool topology"), text.index("name: Cleanup environment (before)"))
        self.assertIn("steps.topology.outcome == 'success' && inputs.cleanup_after", text)


class RunnerTests(unittest.TestCase):
    def test_offline_or_missing_runner_fails_before_dispatch(self):
        import check_functional_runners as runners
        for inventory in ([], [{"status": "offline", "labels": [{"name": "pf-testing"}]}]):
            with mock.patch.object(runners, "api", return_value={"runners": inventory}), self.assertRaisesRegex(ValueError, "pf-testing"):
                runners.check(["pf-testing"])

    def test_busy_online_runner_is_available_and_inventory_is_paginated(self):
        import check_functional_runners as runners
        pages = [{"runners": [{"status": "online", "labels": []}] * 100},
                 {"runners": [{"status": "online", "busy": True, "labels": [{"name": "pf-testing"}]}]}]
        with mock.patch.object(runners, "api", side_effect=pages) as api:
            runners.check(["pf-testing"])
        self.assertIn("page=2", api.call_args.args[0])

    def test_unavailable_inventory_does_not_assume_online(self):
        import check_functional_runners as runners
        with mock.patch.object(runners, "api", side_effect=OSError("forbidden")), self.assertRaises(OSError):
            runners.check(["pf-testing"])

    def test_release_lanes_use_pinned_scripts_and_verified_package_before_recording(self):
        for suite, report in (("fault-tolerance", "suite.log"), ("table", "cases.md")):
            with self.subTest(suite=suite):
                workflow = (candidate.ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text()
                self.assertIn("  workflow_call:", workflow)
                self.assertIn("  workflow_dispatch:", workflow)
                self.assertIn("group: rustfs-shared-functional-tests-v2", workflow)
                expected_ref = (
                    "ref: ${{ steps.chain.outputs.testing_sha || inputs.auto_testing_ref || 'main' }}"
                    if suite == "fault-tolerance"
                    else "ref: ${{ steps.chain.outputs.testing_sha || 'main' }}"
                )
                self.assertIn(expected_ref, workflow)
                self.assertIn("steps.chain_package.outputs.package_url || inputs.package_url", workflow)
                self.assertLess(workflow.index("prepare_functional_package.py prepare"), workflow.index("id: test"))
                self.assertIn("prepare_functional_package.py cleanup", workflow)
                self.assertIn(f"functional_chain_evidence.py record --suite {suite}", workflow)
                self.assertIn('--report "${FUNCTIONAL_ARTIFACTS_DIR}/' + report + '"', workflow)
                self.assertIn("CHAIN_TEST_OUTCOME: ${{ steps.test.outcome }}", workflow)
                self.assertIn("CHAIN_REPORT_OUTCOME: ${{ steps.chain_report.outcome }}", workflow)


class WorkflowTimeoutTests(unittest.TestCase):
    def test_non_performance_suites_have_hard_and_step_deadlines(self):
        from check_test_wiring import yaml_block
        from test_security_workflow import FunctionalWorkflowTests, named_steps
        jobs = {**FunctionalWorkflowTests.JOBS, "security": "security-test"}
        jobs.pop("performance")
        self.assertEqual(len(jobs), 11)
        for suite, job_id in jobs.items():
            with self.subTest(suite=suite):
                source = (candidate.ROOT / f".github/workflows/rustfs-{suite}-test.yml").read_text()
                job = yaml_block(source.splitlines(), job_id, 2)
                # heal: a progressing heal outran the shared 45-minute step
                # cap (chain 40796474356), so it gets a 90-minute run budget
                # and a 120-minute job budget for setup, report and cleanup.
                job_timeout = 360 if suite == "pool-expand" else 120 if suite == "heal" else 60
                self.assertIn(f"    timeout-minutes: {job_timeout}", job)
                steps = named_steps(job)
                primary = [step for step in steps.values() if any(
                    line in ("        id: test", "        id: pool_test") for line in step)]
                self.assertEqual(len(primary), 1)
                if suite == "pool-expand":
                    self.assertIn("        timeout-minutes: ${{ fromJSON(inputs.pool_timeout_minutes || '240') }}", primary[0])
                    for event in ("workflow_call", "workflow_dispatch"):
                        event_block = yaml_block(source.splitlines(), event, 2)
                        timeout_input = yaml_block(event_block, "pool_timeout_minutes", 6)
                        self.assertIsNotNone(timeout_input, event)
                        self.assertIn("        default: '240'", timeout_input)
                        self.assertIn("        required: false", timeout_input)
                elif suite == "heal":
                    self.assertIn("        timeout-minutes: 90", primary[0])
                else:
                    self.assertIn("        timeout-minutes: 45", primary[0])
                cleanup = "Cleanup environment"
                for phase in ("before", "after"):
                    self.assertIn("        timeout-minutes: 5", steps[f"{cleanup} ({phase})"])
                self.assertTrue(any("always()" in line for line in steps[f"{cleanup} (after)"]))
                for name, step in steps.items():
                    if name.startswith(("Generate report", "Upload functional report", "File failure issue",
                                        "Upload report", "Upload test logs", "Manage backlog issues")):
                        self.assertIn("        timeout-minutes: 2", step, name)
                if suite in ("heal", "pool-expand"):
                    install = next(step for name, step in steps.items() if name.startswith("Install RustFS package"))
                    self.assertIn("        timeout-minutes: 5", install)
                    self.assertIn("        timeout-minutes: 5", steps["Preflight checks"])

    def test_performance_is_exempt_from_functional_timeouts(self):
        from check_test_wiring import yaml_block
        source = (candidate.ROOT / ".github/workflows/rustfs-performance-test.yml").read_text()
        job = "\n".join(yaml_block(source.splitlines(), "performance-test", 2))
        self.assertIn("    timeout-minutes: 900", job)
        benchmark = job.split("id: benchmark", 1)[1].split("      - name:", 1)[0]
        self.assertNotIn("        timeout-minutes:", benchmark)
        self.assertIn("RUSTFS_WARP_DURATION: ${{ inputs.warp_duration || '5m' }}", job)
        self.assertNotIn("RUSTFS_WARP_SLEEP:", job)
        self.assertIn("RUSTFS_WARP_METHODS: ${{ inputs.test_method }}", job)
        self.assertIn("RUSTFS_WARP_SIZES: ${{ inputs.object_size }}", job)
        self.assertIn("default: '5m'", source)


if __name__ == "__main__":
    unittest.main()
