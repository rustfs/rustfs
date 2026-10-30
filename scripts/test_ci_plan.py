#!/usr/bin/env python3
"""Regression cases for trusted planning and fail-closed aggregation."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import ci_gate as gate


class Repository:
    def __init__(self, root):
        self.root = root
        self.git('init', '-q')
        self.write('.gitignore', '/target/\n')
        self.write('Cargo.toml', '[workspace]\nmembers = ["rustfs", "core", "leaf", "consumer", "macro", "crates/e2e_test"]\n[workspace.dependencies]\nrenamed = { package="leaf", path="leaf" }\n')
        for name, directory in [('rustfs', 'rustfs'), ('core', 'core'), ('leaf', 'leaf'), ('consumer', 'consumer'), ('macro', 'macro'), ('e2e_test', 'crates/e2e_test')]:
            self.write(f'{directory}/Cargo.toml', f'[package]\nname = "{name}"\nversion = "0.1.0"\n')
            self.write(f'{directory}/src/lib.rs', '// source\n')
        self.write('consumer/Cargo.toml', '[package]\nname="consumer"\nversion="0.1.0"\n[target.\'cfg(unix)\'.dev-dependencies]\nrenamed = { workspace=true }\n[build-dependencies]\nmacro={path="../macro"}\n')
        self.write('macro/Cargo.toml', '[package]\nname="macro"\nversion="0.1.0"\n[lib]\nproc-macro=true\n')
        self.write('rustfs/Cargo.toml', '[package]\nname="rustfs"\nversion="0.1.0"\n[dependencies]\nconsumer={path="../consumer"}\n')
        self.policy = {'schema_version': 1, 'rollout': 'shadow', 'core_packages': ['core'], 'connect_packages': ['core'],
                       'full_paths': ['Cargo.toml', 'Cargo.lock', '*/Cargo.toml', '.config/*', '.github/*', 'scripts/*', 'rust-toolchain*']}
        self.write(gate.POLICY_PATH, json.dumps(self.policy))
        self.write('.config/ecstore-required-tests.json', json.dumps({'tests': [{'suite': 'core::integration', 'name': 'required'}]}))
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(['git', '-c', 'core.hooksPath=/dev/null', '-c', 'user.name=CI Test', '-c', 'user.email=ci@example.invalid', *args], cwd=self.root, stderr=subprocess.PIPE).decode().strip()

    def write(self, path, content):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)

    def commit(self):
        self.git('add', '-A')
        self.git('commit', '-qm', 'fixture')
        return self.git('rev-parse', 'HEAD')

    def plan(self, head=None, event='pull_request', base=None, action='opened'):
        return gate.select_plan(event, base or self.base, head or self.git('rev-parse', 'HEAD'), 'refs/heads/main', self.root, action)


class PlannerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Repository(Path(self.temp.name))

    def test_dependency_graph_includes_renamed_target_dev_build_and_proc_macro_edges(self):
        packages, graph = gate.cargo_graph(self.repo.root, self.repo.base)
        self.assertEqual(graph['consumer'], {'leaf', 'macro'})
        self.assertEqual(gate.closure({'leaf'}, graph, reverse=True), {'leaf', 'consumer', 'rustfs'})
        self.assertEqual(gate.closure({'macro'}, graph, reverse=True), {'macro', 'consumer', 'rustfs'})
        self.repo.write('leaf/src/lib.rs', '// changed\n')
        plan = self.repo.plan(self.repo.commit())
        self.assertEqual(plan['proposed']['packages'], ['consumer', 'core', 'leaf', 'rustfs'])
        self.assertFalse(plan['proposed']['connect'])
        self.assertTrue(plan['connect'])
        self.assertEqual(plan['packages'], sorted(set(packages) - {'e2e_test'}))
        self.assertEqual(plan['policy']['rollout'], 'shadow')

    def test_deleted_or_moved_source_keeps_original_dependents(self):
        (self.repo.root / 'leaf/src/lib.rs').rename(self.repo.root / 'core/src/moved.rs')
        plan = self.repo.plan(self.repo.commit())
        self.assertEqual(plan['proposed']['packages'], ['consumer', 'core', 'leaf', 'rustfs'])
        self.assertTrue(plan['proposed']['connect'])
        self.assertFalse(plan['proposed']['full_validation'])

    def test_shared_unknown_lock_and_policy_changes_are_full(self):
        for path in ('Cargo.lock', 'rust-toolchain.toml', 'shared/fixture.json', 'scripts/run.sh', '.config/ci-plan.json', 'crates/new/Cargo.toml'):
            with self.subTest(path=path):
                if path == gate.POLICY_PATH:
                    modified = dict(self.repo.policy, rollout='enforced', core_packages=[])
                    self.repo.write(path, json.dumps(modified))
                else:
                    self.repo.write(path, 'changed\n')
                plan = self.repo.plan(self.repo.commit())
                self.assertTrue(plan['proposed']['full_validation'])
                self.assertTrue(plan['connect'])
                self.assertEqual(plan['policy']['rollout'], 'shadow')

    def test_core_inventory_is_unioned_and_cannot_be_removed_by_candidate(self):
        self.repo.write('leaf/src/lib.rs', '// change\n')
        with patch.object(gate, 'read_policy', return_value=dict(self.repo.policy, rollout='enforced', core_packages=[])):
            plan = self.repo.plan(self.repo.commit())
            self.assertIn('core', plan['packages'])
        self.repo.write('.config/ecstore-required-tests.json', json.dumps({'tests': []}))
        plan = self.repo.plan(self.repo.commit())
        self.assertTrue(plan['full_validation'])
        self.assertTrue(plan['connect'])

    def test_enforced_selection_keeps_shared_package_inputs_full(self):
        self.repo.write(gate.POLICY_PATH, json.dumps(dict(self.repo.policy, rollout='enforced')))
        baseline = self.repo.commit()
        for path in ('leaf/build.rs', 'leaf/tests/fixtures/data.json', 'leaf/tests/fixtures/shared.rs',
                     'leaf/src/template.html', 'leaf/native/include.h'):
            with self.subTest(path=path):
                self.repo.write(path, 'changed\n')
                plan = self.repo.plan(self.repo.commit(), base=baseline)
                self.assertTrue(plan['full_validation'])
                self.assertTrue(plan['connect'])
                self.assertEqual(set(plan['packages']), {'rustfs', 'core', 'leaf', 'consumer', 'macro'})

    def test_enforced_unrelated_source_omits_connect_but_keeps_its_dependents_and_core(self):
        self.repo.write(gate.POLICY_PATH, json.dumps(dict(self.repo.policy, rollout='enforced')))
        baseline = self.repo.commit()
        self.repo.write('leaf/src/lib.rs', '// changed\n')
        plan = self.repo.plan(self.repo.commit(), base=baseline)
        self.assertEqual(plan['packages'], ['consumer', 'core', 'leaf', 'rustfs'])
        self.assertEqual(plan['jobs']['connect-feature-boundaries'], 'skipped')
        for job in ('test-and-lint', 'workspace-tests', 'core-contract', 'e2e-tests',
                    's3-implemented-tests', 's3-lifecycle-behavior-tests'):
            self.assertEqual(plan['jobs'][job], 'success')

    def test_enforced_plan_runs_a_real_failing_reverse_dependency_test(self):
        self.repo.write('rust-toolchain.toml', (gate.ROOT / 'rust-toolchain.toml').read_text())
        self.repo.write(gate.POLICY_PATH, json.dumps(dict(self.repo.policy, rollout='enforced')))
        self.repo.write('leaf/src/lib.rs', 'pub fn answer() -> u8 { 1 }\n')
        self.repo.write('core/src/lib.rs', '#[test] fn core_contract() { assert_eq!(1, 1); }\n')
        self.repo.write('consumer/src/lib.rs', '#[cfg(test)] mod tests { extern crate renamed; #[test] fn reverse_dependency_contract() { assert_eq!(renamed::answer(), 1); } }\n')
        baseline = self.repo.commit()
        self.repo.write('leaf/src/lib.rs', 'pub fn answer() -> u8 { 2 }\n')
        plan = self.repo.plan(self.repo.commit(), base=baseline)
        arguments = ['cargo', 'nextest', 'run', '--offline']
        for package in plan['packages']:
            arguments.extend(['-p', package])
        result = subprocess.run(arguments, cwd=self.repo.root, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('reverse_dependency_contract', result.stderr)
        self.assertIn('1 failed', result.stderr)
        self.assertIn('1 passed', result.stderr)

    def test_docs_and_e2e_shortcuts_do_not_hide_production_or_fixture_inputs(self):
        self.repo.write('README.md', '# docs\n')
        docs = self.repo.plan(self.repo.commit())
        self.assertEqual(docs['mode'], 'docs')
        self.assertEqual(docs['packages'], [])
        self.repo.write('crates/e2e_test/src/lib.rs', '// harness change\n')
        e2e = self.repo.plan(self.repo.commit())
        self.assertEqual(e2e['mode'], 'e2e')
        self.assertFalse(e2e['connect'])
        self.repo.write('leaf/tests/fixtures/README.md', 'fixture input\n')
        code = self.repo.plan(self.repo.commit())
        self.assertEqual(code['mode'], 'full')
        self.assertIn('leaf', code['proposed']['packages'])

    def test_shadow_retains_full_mode_for_harness_build_and_fixture_changes(self):
        for path in ('crates/e2e_test/build.rs', 'crates/e2e_test/src/fixture.json'):
            self.repo.write(path, 'changed\n')
            plan = self.repo.plan(self.repo.commit())
            self.assertEqual(plan['mode'], 'full')
            self.assertTrue(plan['connect'])

    def test_new_harness_dependency_disables_e2e_shortcut(self):
        self.repo.write('consumer/Cargo.toml', '[package]\nname="consumer"\nversion="0.1.0"\n[dependencies]\nharness={path="../crates/e2e_test"}\n')
        baseline = self.repo.commit()
        self.repo.write('crates/e2e_test/src/lib.rs', '// change\n')
        plan = self.repo.plan(self.repo.commit(), base=baseline)
        self.assertEqual(plan['mode'], 'full')
        self.assertIn('consumer', plan['proposed']['packages'])

    def test_non_pr_events_validate_complete_combined_candidate_and_closed_cancels(self):
        self.repo.write('leaf/src/lib.rs', '// PR one\n')
        self.repo.write('macro/src/lib.rs', '// PR two\n')
        head = self.repo.commit()
        for event in ('push', 'merge_group', 'schedule', 'workflow_dispatch'):
            plan = self.repo.plan(head, event=event)
            self.assertTrue(plan['full_validation'])
            self.assertEqual(plan['mode'], 'full')
            self.assertEqual(plan['head'], head)
            self.assertEqual(plan['tree'], self.repo.git('rev-parse', head + '^{tree}'))
            self.assertEqual(gate.validate_plan(plan, event, 'refs/heads/main', head, plan['tree']), plan['jobs'])
        closed = self.repo.plan(head, action='closed')
        self.assertEqual(set(closed['jobs'].values()), {'skipped'})

    def test_unavailable_diff_policy_or_base_falls_back_and_invalid_head_fails(self):
        self.repo.write('leaf/src/lib.rs', '// changed\n')
        head = self.repo.commit()
        for base in ('0' * 40, '', '--output=bad'):
            plan = gate.select_plan('pull_request', base, head, 'refs/heads/main', self.repo.root)
            self.assertTrue(plan['full_validation'])
            self.assertTrue(plan['connect'])
            self.assertTrue(plan['packages'])
        with patch.object(gate, 'read_policy', side_effect=ValueError('broken policy')):
            self.assertTrue(self.repo.plan(head)['full_validation'])
        with self.assertRaises(ValueError):
            self.repo.plan('--malicious')

    def test_actual_workflow_bootstrap_never_executes_pr_policy_and_upgrades_legacy_output(self):
        workflow = gate.workflow_document((Path(gate.__file__).resolve().parent.parent / '.github/workflows/ci.yml').read_text())
        body = next(step['run'] for step in workflow['jobs']['classify-changes']['steps'] if step.get('id') == 'scope')
        self.repo.write('scripts/ci_gate.py', "import os\nwith open(os.environ['GITHUB_OUTPUT'], 'a') as output: output.write('mode=docs\\npackages=[\"core\"]\\nplan={}\\n')\n")
        base = self.repo.commit()
        self.repo.write('scripts/ci_gate.py', 'raise SystemExit(71)\n')
        head = self.repo.commit()
        for revision in (base, '0' * 40, ''):
            output = self.repo.root / 'outputs'
            output.write_text('')
            env = dict(os.environ, GITHUB_EVENT_NAME='pull_request', GITHUB_REF='refs/heads/main', GITHUB_SHA=head,
                       CI_BASE_SHA=revision, RUNNER_TEMP=str(self.repo.root), GITHUB_OUTPUT=str(output))
            result = subprocess.run(['bash', '--noprofile', '--norc', '-e', '-o', 'pipefail', '-c', body],
                                    cwd=self.repo.root, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            values = dict(line.split('=', 1) for line in output.read_text().splitlines())
            self.assertEqual(values['mode'], 'full')
            self.assertEqual(values['connect'], 'true')
            self.assertEqual(values['packages'], '')
            self.assertEqual(values['plan'], '')
        self.repo.write('scripts/ci_gate.py', 'raise SystemExit(29)\n')
        broken = self.repo.commit()
        env['CI_BASE_SHA'] = broken
        result = subprocess.run(['bash', '--noprofile', '--norc', '-e', '-o', 'pipefail', '-c', body],
                                cwd=self.repo.root, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 29)

    def test_bootstrap_script_is_standalone_and_reads_policy_from_base_objects(self):
        self.repo.write('leaf/src/lib.rs', '// changed\n')
        self.repo.write(gate.POLICY_PATH, json.dumps(dict(self.repo.policy, rollout='enforced', core_packages=[])))
        head = self.repo.commit()
        script = self.repo.root / 'trusted.py'
        script.write_text(Path(gate.__file__).read_text())
        output = self.repo.root / 'outputs'
        env = dict(os.environ, GITHUB_EVENT_NAME='pull_request', GITHUB_REF='refs/heads/main', GITHUB_SHA=head,
                   CI_BASE_SHA=self.repo.base, GITHUB_OUTPUT=str(output))
        result = subprocess.run([sys.executable, '-I', str(script), 'select'], cwd=self.repo.root, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        values = dict(line.split('=', 1) for line in output.read_text().splitlines())
        self.assertEqual(set(values), {'mode', 'plan', 'connect', 'packages', 'full_validation', 'rehearse_shards'})
        plan = json.loads(values['plan'])
        self.assertEqual(plan['policy']['revision'], self.repo.base)
        self.assertEqual(plan['policy']['rollout'], 'shadow')
        self.assertEqual(values['connect'], 'true')


class GateTests(unittest.TestCase):
    def plan(self, event='pull_request', mode='full', connect=True):
        jobs = gate.expected_results(mode, event, 'refs/heads/main', connect)
        return {'schema_version': 1, 'event': event, 'ref': 'refs/heads/main', 'head': 'a' * 40, 'tree': 'b' * 40,
                'base': 'c' * 40, 'action': '', 'mode': mode, 'connect': connect, 'full_validation': mode == 'full', 'packages': ['rustfs'] if mode != 'docs' else [],
                'rehearse_shards': False,
                'policy': {'revision': 'c' * 40, 'rollout': 'enforced'}, 'jobs': jobs, 'configurations': gate.configurations(jobs),
                'reasons': ['fixture'], 'not_applicable': {job: 'fixture' for job, state in jobs.items() if state == 'skipped'}}

    def needs(self, plan):
        needs = {job: {'result': state} for job, state in plan['jobs'].items()}
        needs['classify-changes']['outputs'] = {'mode': plan['mode'], 'plan': json.dumps(plan)}
        return needs

    def test_failure_cancel_missing_extra_or_unexpected_skip_is_rejected(self):
        for event, mode in [('pull_request', 'docs'), ('pull_request', 'e2e'), ('pull_request', 'full'), ('push', 'full'), ('merge_group', 'full'), ('schedule', 'full'), ('workflow_dispatch', 'full')]:
            plan = self.plan(event, mode)
            good = self.needs(plan)
            self.assertEqual(gate.verify_results(good, event, plan['ref'], plan['head'], plan['tree']), [])
            for job, state in plan['jobs'].items():
                for bad in (None, 'failure', 'cancelled', 'skipped', 'success'):
                    if bad == state:
                        continue
                    needs = copy.deepcopy(good)
                    needs[job]['result'] = bad
                    self.assertTrue(gate.verify_results(needs, event, plan['ref']), (event, mode, job, bad))
                needs = copy.deepcopy(good)
                del needs[job]
                self.assertTrue(gate.verify_results(needs, event, plan['ref']))
            good['new-unwired-job'] = {'result': 'success'}
            self.assertTrue(gate.verify_results(good, event, plan['ref']))

    def test_invalid_plan_matrix_and_candidate_identity_fail_closed(self):
        plan = self.plan('merge_group')
        for field, value in [('schema_version', 2), ('event', 'push'), ('ref', 'wrong'), ('base', '../main'), ('packages', []), ('connect', 'false'), ('configurations', []), ('jobs', {}), ('not_applicable', {}), ('policy', {})]:
            bad = dict(plan, **{field: value})
            needs = self.needs(plan)
            needs['classify-changes']['outputs']['plan'] = json.dumps(bad)
            self.assertTrue(gate.verify_results(needs, 'merge_group', plan['ref']), field)
        good = self.needs(plan)
        self.assertTrue(gate.verify_results(good, plan['event'], plan['ref'], 'c' * 40, plan['tree']))
        self.assertTrue(gate.verify_results(good, plan['event'], plan['ref'], plan['head'], 'c' * 40))
        self.assertTrue(gate.verify_results(good, plan['event'], plan['ref'], base='d' * 40))
        self.assertTrue(gate.verify_results(good, plan['event'], plan['ref'], action='closed'))

    def test_legacy_base_cannot_authorize_skipped_new_jobs(self):
        expected = gate.expected_results('full', 'pull_request', 'refs/heads/main')
        needs = {job: {'result': result} for job, result in expected.items()}
        needs['classify-changes']['outputs'] = {'mode': 'full'}
        self.assertEqual(gate.verify_results(needs, 'pull_request', 'refs/heads/main'), [])
        needs['classify-changes']['outputs']['mode'] = 'docs'
        self.assertTrue(gate.verify_results(needs, 'pull_request', 'refs/heads/main'))
        needs['classify-changes']['outputs'] = {'mode': 'full', 'plan': '{'}
        self.assertTrue(gate.verify_results(needs, 'pull_request', 'refs/heads/main'))

    def workflow(self):
        jobs = {}
        for job in gate.ALWAYS_JOBS + gate.CODE_JOBS + gate.OPTIONAL_JOBS:
            # A complete plan lookup is represented by equivalent explicit expressions here.
            if job in gate.ALWAYS_JOBS:
                expression = "true"
            elif job in gate.EXTENDED_FEATURE_JOBS:
                expression = "needs.classify-changes.outputs.mode == 'full' && contains(fromJSON('[\"merge_group\",\"schedule\",\"workflow_dispatch\"]'), github.event_name)"
            elif job in gate.OPTIONAL_JOBS[:2]:
                expression = "contains(fromJSON('[\"schedule\",\"workflow_dispatch\"]'), github.event_name)"
            elif job == 'e2e-full':
                expression = "github.event_name == 'merge_group' || github.event_name == 'workflow_dispatch' || (github.event_name == 'push' && github.ref == 'refs/heads/main')"
            elif job in gate.SHARD_JOBS:
                expression = "needs.classify-changes.outputs.rehearse_shards == 'true'"
            elif job == 'connect-feature-boundaries':
                expression = "needs.classify-changes.outputs.mode == 'full' && needs.classify-changes.outputs.connect == 'true'"
            elif job in gate.E2E_JOBS:
                expression = "needs.classify-changes.outputs.mode != 'docs'"
            else:
                expression = "needs.classify-changes.outputs.mode == 'full'"
            expression = f"({expression}) && (github.event_name != 'pull_request' || github.event.action != 'closed')"
            jobs[job] = {'name': job, 'if': expression, 'steps': []}
        jobs['test-and-lint']['steps'] = [{'run': 'cargo clippy --all-targets -- -D warnings'}]
        jobs['test-and-lint-protocols']['strategy'] = {'matrix': {'features': [
            {'name': 'swift', 'flags': '--features swift'}, {'name': 'sftp', 'flags': '--features sftp'}, {'name': 'http3', 'flags': '--features http3,swift'}]}}
        jobs['required-checks'] = {'name': 'Test and Lint', 'if': "always() && (github.event_name != 'pull_request' || github.event.action != 'closed')",
                                   'needs': list(gate.ALWAYS_JOBS + gate.CODE_JOBS + gate.OPTIONAL_JOBS),
                                   'steps': [{'shell': 'bash', 'env': {'CI_NEEDS': '${{ toJSON(needs) }}'}, 'run': 'python3 scripts/ci_gate.py verify'}]}
        return {'on': {'pull_request': {}}, 'jobs': jobs}

    def check(self, workflow):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / '.github/workflows/ci.yml'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(workflow))
            return gate.check_workflow(root)

    def test_ignored_ilm_archive_execution_preserves_failure_and_core_report(self):
        workflow = gate.workflow_document((Path(gate.__file__).resolve().parent.parent / '.github/workflows/ci.yml').read_text())
        steps = workflow['jobs']['workspace-tests']['steps']
        bodies = [step['run'] for step in steps if '--run-ignored ignored-only' in step.get('run', '')]
        self.assertEqual(len(bodies), 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = root / 'bin'
            commands.mkdir()
            timeout = commands / 'timeout'
            timeout.write_text('#!/bin/sh\nshift 4\nexec "$@"\n')
            timeout.chmod(0o755)
            cargo = commands / 'cargo'
            cargo.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$ILM_ARGS"\nexit "$ILM_STATUS"\n')
            cargo.chmod(0o755)
            report = root / 'target/nextest/ci/junit.xml'
            report.parent.mkdir(parents=True)
            report.write_text('existing core report')
            args = root / 'args'
            for status in (0, 23, 124):
                env = dict(os.environ, PATH=f"{commands}{os.pathsep}{os.environ['PATH']}", ILM_ARGS=str(args),
                           ILM_STATUS=str(status), RUNNER_TEMP=str(root), GITHUB_WORKSPACE=str(root))
                result = subprocess.run(['bash', '-e', '-o', 'pipefail', '-c', bodies[0]], cwd=root, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertEqual(report.read_text(), 'existing core report')
                arguments = args.read_text().splitlines()
                self.assertEqual(arguments[:7], ['nextest', 'run', '--profile', 'default', '-j1', '--run-ignored', 'ignored-only'])
                self.assertNotIn('--archive-file', arguments)
                self.assertEqual(arguments[arguments.index('--cargo-metadata') + 1], 'target/nextest/cargo-metadata.json')
                self.assertEqual(arguments[arguments.index('--binaries-metadata') + 1], 'target/nextest/binaries-metadata.json')
                self.assertEqual(arguments[arguments.index('--target-dir-remap') + 1], str(root / 'target'))
                self.assertIn(f'exit_status={status}', (root / 'artifacts/ilm-integration/diagnostics.txt').read_text())

    def test_requested_shards_and_union_proof_are_required_and_cannot_be_skipped(self):
        plan = self.plan('workflow_dispatch')
        plan['rehearse_shards'] = True
        plan['jobs'] = gate.expected_results('full', 'workflow_dispatch', plan['ref'], rehearse_shards=True)
        plan['not_applicable'] = {job: 'not selected' for job, state in plan['jobs'].items() if state == 'skipped'}
        self.assertEqual(gate.verify_results(self.needs(plan), plan['event'], plan['ref']), [])
        for job in gate.SHARD_JOBS:
            for state in ('failure', 'cancelled', 'skipped', None):
                needs = self.needs(plan)
                needs[job]['result'] = state
                self.assertTrue(gate.verify_results(needs, plan['event'], plan['ref']))
        with self.assertRaises(ValueError):
            gate.expected_results('full', 'pull_request', plan['ref'], rehearse_shards=True)

    def test_semantic_yaml_equivalence_and_wiring_boundaries(self):
        workflow = self.workflow()
        self.assertEqual(self.check(workflow), [])
        workflow['jobs']['required-checks']['if'] = "${{ (github.event.action != 'closed' || github.event_name != 'pull_request') && always() }}"
        workflow['jobs']['required-checks']['needs'].reverse()
        for job in ('workspace-tests', 'core-contract', 'e2e-tests', 's3-implemented-tests', 's3-lifecycle-behavior-tests'):
            del workflow['jobs'][job]['if']
            workflow['jobs'][job]['needs'] = ['test-and-lint' if job == 'workspace-tests' else 'build-rustfs-debug-binary']
        self.assertEqual(self.check(workflow), [])
        for mutate in (
            lambda w: w['jobs']['required-checks']['needs'].pop(),
            lambda w: w['jobs']['required-checks'].update({'if': 'success()'}),
            lambda w: w['jobs']['required-checks'].update({'if': "'always()' == 'always()'"}),
            lambda w: w['jobs']['required-checks']['steps'][0].update({'run': 'python3 scripts/ci_gate.py verify || true'}),
            lambda w: w['jobs']['typos'].update({'continue-on-error': 'true'}),
            lambda w: w['jobs']['test-and-lint']['steps'][0].update({'if': 'false'}),
            lambda w: w['jobs']['test-and-lint-protocols']['strategy']['matrix']['features'].pop(),
            lambda w: w['jobs'].update({'unaccounted': {'name': 'New'}}),
            lambda w: w['on']['pull_request'].update({'paths': ['README.md']}),
            lambda w: w['jobs']['core-contract'].update({'if': 'false'}),
        ):
            changed = copy.deepcopy(workflow)
            mutate(changed)
            self.assertTrue(self.check(changed))
        with self.assertRaises(ValueError):
            gate.workflow_document('jobs: { test: {}, test: {} }')


if __name__ == '__main__':
    unittest.main()
