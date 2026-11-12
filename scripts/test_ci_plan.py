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

    def plan(self, head=None, event='pull_request', base=None, action='opened', proof=None):
        return gate.select_plan(event, base or self.base, head or self.git('rev-parse', 'HEAD'), 'refs/heads/main', self.root, action, proof=proof)


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
        for job in ('workspace-lint-and-build', 'workspace-tests', 'core-contract', 'e2e-tests',
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
        arguments = ['cargo', 'nextest', 'run', '--offline', '--color', 'never', '--no-fail-fast']
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

    def release_metadata_base(self):
        self.repo.write(gate.POLICY_PATH, json.dumps(dict(self.repo.policy, rollout='enforced')))
        files = {
            'flake.nix': '{ pkgs }: {\n  rustfs = pkgs.buildRustPackage {\n    pname = "rustfs";\n    version = "1.0.4";\n    src = ./.;\n  };\n  clientVersion = "0.1.36";\n}\n',
            'helm/rustfs/Chart.yaml': 'apiVersion: v2\nname: rustfs\nversion: "1.0.4"\nappVersion: "1.0.4"\n',
            'rustfs.spec': 'Name: rustfs\nVersion: 1.0.4\nRelease: 1\n%build\ncargo build --release\n%changelog\n* Mon Oct 26 2026 Maintainer <maintainer@example.invalid>\n- Update RPM package to RustFS 1.0.4\n',
        }
        for path, content in files.items():
            self.repo.write(path, content)
        return self.repo.commit(), files

    def test_stable_installation_metadata_keeps_quick_checks_without_rust_rebuild(self):
        baseline, files = self.release_metadata_base()
        for path, content in files.items():
            updated = content.replace('1.0.4', '1.0.5') if path != 'rustfs.spec' else content.replace('Version: 1.0.4', 'Version: 1.0.5')
            if path == 'rustfs.spec':
                updated = updated.replace('%changelog\n', '%changelog\n* Wed Nov 11 2026 Maintainer <maintainer@example.invalid>\n- Update RPM package to RustFS 1.0.5\n\n')
            self.repo.write(path, updated)
        self.repo.write('README.md', '# Install 1.0.5\n')
        head = self.repo.commit()
        plan = self.repo.plan(head, base=baseline)
        self.assertEqual(plan['mode'], 'docs')
        self.assertEqual(plan['packages'], [])
        for job in gate.ALWAYS_JOBS:
            self.assertEqual(plan['jobs'][job], 'success')
        self.assertTrue(all(plan['jobs'][job] == 'skipped' for job in gate.CODE_JOBS))
        self.assertEqual(gate.validate_plan(plan, 'pull_request', 'refs/heads/main', head, plan['tree']), plan['jobs'])
        for event in ('push', 'merge_group', 'schedule', 'workflow_dispatch'):
            self.assertTrue(self.repo.plan(head, event=event, base=baseline)['full_validation'])

    def test_push_skips_validation_only_for_the_tree_a_merge_group_run_proved(self):
        head, tree = self.repo.git('rev-parse', 'HEAD'), self.repo.git('rev-parse', 'HEAD^{tree}')
        proof = {'run_id': 7, 'url': 'https://github.com/rustfs/rustfs/actions/runs/7', 'tree': tree}
        plan = self.repo.plan(head, event='push', proof=proof)
        self.assertEqual((plan['mode'], plan['proven_by']), ('proven', proof))
        self.assertEqual({job for job, state in plan['jobs'].items() if state == 'success'}, set(gate.ALWAYS_JOBS))
        self.assertTrue(all(proof['url'] in plan['not_applicable'][job] for job in gate.CODE_JOBS))
        self.assertEqual(gate.validate_plan(plan, 'push', 'refs/heads/main', head, tree), plan['jobs'])
        # A direct push has no proving run; a proof of another tree, or on any other event, proves nothing.
        for event, candidate in [('push', None), ('push', dict(proof, tree='0' * 40)), ('merge_group', proof),
                                 ('schedule', proof), ('workflow_dispatch', proof), ('pull_request', proof)]:
            with self.subTest(event=event, proof=candidate):
                other = self.repo.plan(head, event=event, proof=candidate)
                self.assertNotEqual(other['mode'], 'proven')
                self.assertIsNone(other['proven_by'])
                if event != 'pull_request':
                    self.assertTrue(all(other['jobs'][job] == 'success' for job in (gate.LINT_JOB, gate.ARCHIVE_PRODUCER, 'workspace-tests')))

    def test_individual_stable_version_fields_without_new_rpm_history_are_metadata(self):
        baseline, files = self.release_metadata_base()
        for path, content in files.items():
            with self.subTest(path=path):
                self.repo.git('reset', '--hard', baseline)
                updated = content.replace('1.0.4', '1.0.5') if path != 'rustfs.spec' else content.replace('Version: 1.0.4', 'Version: 1.0.5')
                self.repo.write(path, updated)
                self.assertEqual(self.repo.plan(self.repo.commit(), base=baseline)['mode'], 'docs')

    def test_installation_metadata_does_not_hide_build_or_malformed_changes(self):
        baseline, files = self.release_metadata_base()
        cases = [
            ('flake.nix', files['flake.nix'].replace('src = ./.;', 'src = ./other;')),
            ('flake.nix', files['flake.nix'].replace('0.1.36', '0.1.37')),
            ('flake.nix', files['flake.nix'].replace('1.0.4', '${builtins.readFile ./version}')),
            ('helm/rustfs/Chart.yaml', files['helm/rustfs/Chart.yaml'] + 'dependencies: []\n'),
            ('helm/rustfs/Chart.yaml', files['helm/rustfs/Chart.yaml'].replace('appVersion: "1.0.4"', 'appVersion: "1.0.5"')),
            ('helm/rustfs/Chart.yaml', files['helm/rustfs/Chart.yaml'] + 'version: "1.0.5"\n'),
            ('rustfs.spec', files['rustfs.spec'].replace('cargo build --release', 'cargo build')),
            ('rustfs.spec', files['rustfs.spec'].replace('Release: 1', 'Release: 2')),
            ('rustfs.spec', files['rustfs.spec'].replace('%changelog\n', '%changelog\n%(echo injected)\n')),
            ('rustfs.spec', files['rustfs.spec'].replace('Update RPM package', 'Rewrite existing history')),
        ]
        cases.extend((path, content.replace('1.0.4', '1.0.5-rc.1')) for path, content in files.items())
        for path, content in cases:
            with self.subTest(path=path, content=content):
                self.repo.git('reset', '--hard', baseline)
                self.repo.write(path, content)
                self.assertTrue(self.repo.plan(self.repo.commit(), base=baseline)['full_validation'])

    def test_installation_metadata_rejects_normalization_marker_collisions(self):
        _, files = self.release_metadata_base()
        old = files['flake.nix'].replace('  clientVersion =', '  second = pkgs.buildRustPackage {\n    pname = "rustfs";\n    version = "<version>";\n    src = ./other;\n  };\n  clientVersion =')
        self.repo.write('flake.nix', old)
        baseline = self.repo.commit()
        new = old.replace('version = "1.0.4"', 'version = "<version>"').replace('version = "<version>";\n    src = ./other;', 'version = "1.0.5";\n    src = ./other;')
        self.repo.write('flake.nix', new)
        self.assertTrue(self.repo.plan(self.repo.commit(), base=baseline)['full_validation'])

    def test_installation_metadata_rejects_missing_files_symlinks_and_mode_changes(self):
        baseline, files = self.release_metadata_base()
        for path in files:
            for mutation in ('delete', 'symlink', 'executable'):
                with self.subTest(path=path, mutation=mutation):
                    self.repo.git('reset', '--hard', baseline)
                    target = self.repo.root / path
                    if mutation == 'executable':
                        target.chmod(0o755)
                    else:
                        target.unlink()
                        if mutation == 'symlink':
                            target.symlink_to('/tmp/release-metadata')
                    self.assertTrue(self.repo.plan(self.repo.commit(), base=baseline)['full_validation'])
        self.repo.git('reset', '--hard', baseline)
        self.assertTrue(self.repo.plan(baseline, base=self.repo.base)['full_validation'])

    def test_installation_metadata_cannot_override_code_or_explicit_full_policy(self):
        baseline, files = self.release_metadata_base()
        self.repo.write('flake.nix', files['flake.nix'].replace('1.0.4', '1.0.5'))
        self.repo.write('leaf/src/lib.rs', '// changed implementation\n')
        plan = self.repo.plan(self.repo.commit(), base=baseline)
        self.assertEqual(plan['mode'], 'full')
        self.assertEqual(plan['packages'], ['consumer', 'core', 'leaf', 'rustfs'])
        self.repo.git('reset', '--hard', baseline)
        self.repo.write('flake.nix', files['flake.nix'].replace('1.0.4', '1.0.5'))
        head = self.repo.commit()
        policy = dict(self.repo.policy, rollout='enforced', full_paths=['flake.nix'])
        with patch.object(gate, 'read_policy', return_value=policy):
            self.assertTrue(self.repo.plan(head, base=baseline)['full_validation'])
        with patch.object(gate, 'read_policy', return_value=self.repo.policy):
            self.assertTrue(self.repo.plan(head, base=baseline)['full_validation'])

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

    def gateway_base(self):
        """A base revision carrying the gateway pins and a lockfile."""
        manifest = (self.repo.root / 'Cargo.toml').read_text()
        self.repo.write('Cargo.toml', manifest + 'rustfs-gateway = { git = "https://example.invalid/gateway.git", rev = "aaaa", version = "0.1.0" }\n'
                        'unrelated = { version = "1.0.0" }\n')
        self.repo.write('Cargo.lock', 'version = 4\n\n[[package]]\nname = "rustfs-gateway"\nversion = "0.1.0"\nsource = "git+https://example.invalid/gateway.git?rev=aaaa#aaaa"\n\n'
                        '[[package]]\nname = "unrelated"\nversion = "1.0.0"\n')
        return self.repo.commit()

    def test_gateway_smoke_is_skipped_on_pull_requests_without_gateway_stack_inputs(self):
        base = self.gateway_base()
        # Every path here changes the product or CI, and none is a gateway-stack input.
        for path, content in (
                ('leaf/src/lib.rs', '// kms change\n'),
                ('rustfs/src/admin/handlers/site_replication.rs', '// admin\n'),
                ('rustfs/src/server/http.rs', '// shared listener\n'),
                ('rustfs/src/server/stackless.rs', '// not the stack module\n'),
                ('crates/gateway-adapterx/src/lib.rs', '// sibling name\n'),
                ('README.md', '# docs\n'),
                ('.config/e2e-full-selection.txt', 'other profile\n'),
                # The suite's contents are not gateway-stack inputs before T2.10:
                # an e2e-only pull request does not run the lane.
                ('crates/e2e_test/src/lib.rs', '// new smoke test\n'),
                ('crates/e2e_test/src/sts_query_compat_test.rs', '// new module\n'),
                ('crates/e2e_test/Cargo.toml', '[package]\nname = "e2e_test"\nversion = "0.1.1"\n'),
                ('.config/e2e-smoke-selection.txt', 'e2e_test::new_case\n'),
                ('.config/nextest.toml', '[profile.e2e-smoke]\n'),
                ('.github/workflows/build.yml', 'name: other\n'),
                ('scripts/run.sh', 'echo\n'),
                ('Cargo.toml', (self.repo.root / 'Cargo.toml').read_text().replace('unrelated = { version = "1.0.0" }', 'unrelated = { version = "1.0.1" }')),
                ('Cargo.lock', (self.repo.root / 'Cargo.lock').read_text().replace('name = "unrelated"\nversion = "1.0.0"', 'name = "unrelated"\nversion = "1.0.1"')),
        ):
            with self.subTest(path=path):
                self.repo.write(path, content)
                plan = self.repo.plan(self.repo.commit(), base=base)
                self.assertEqual(plan['gateway_smoke'], {'run': False, 'reason': 'no gateway-stack input changed'})
                base = plan['head']

    def test_gateway_smoke_runs_on_pull_requests_that_change_a_gateway_stack_input(self):
        base = self.gateway_base()
        for path in (
                'crates/gateway-adapter/src/lib.rs', 'crates/gateway-adapter/Cargo.toml',
                'rustfs/src/server/gateway_stack.rs', 'rustfs/src/server/gateway_stack/authorizer.rs',
                'rustfs/src/server/gateway_handlers/bucket/list_buckets.rs',
                'rustfs/src/server/stack.rs', 'rustfs/src/server/stack/website.rs',
                'rustfs/src/config/config_struct.rs', 'rustfs/Cargo.toml',
                '.github/workflows/ci.yml', '.github/actions/test-runtime/action.yml',
                'scripts/ci_gate.py', 'scripts/e2e_binary.py',
        ):
            with self.subTest(path=path):
                target = self.repo.root / path
                self.repo.write(path, (target.read_text() if target.exists() else '') + '\n# changed\n')
                plan = self.repo.plan(self.repo.commit(), base=base)
                self.assertEqual(plan['gateway_smoke'], {'run': True, 'reason': f'gateway-stack input changed: {path}'})
                base = plan['head']
        # One gateway input among unrelated paths is enough.
        self.repo.write('leaf/src/lib.rs', '// unrelated\n')
        self.repo.write('rustfs/src/server/stack/metadata.rs', '// stack\n')
        self.assertTrue(self.repo.plan(self.repo.commit(), base=base)['gateway_smoke']['run'])

    def test_gateway_smoke_follows_the_gateway_pins_inside_shared_manifests(self):
        self.gateway_base()
        manifest, lock = (self.repo.root / 'Cargo.toml').read_text(), (self.repo.root / 'Cargo.lock').read_text()
        for path, content in (
                ('Cargo.toml', manifest.replace('rev = "aaaa"', 'rev = "bbbb"')),
                ('Cargo.toml', manifest + 'rustfs-gateway-http = { git = "https://example.invalid/gateway.git", rev = "aaaa" }\n'),
                ('Cargo.toml', manifest.replace('rustfs-gateway = ', 'renamed-gw = { package = "rustfs-gateway-types", version = "1" }\nrustfs-gateway = ')),
                ('Cargo.toml', manifest + '[patch."https://example.invalid/gateway.git"]\nrustfs-gateway = { path = "leaf" }\n'),
                ('Cargo.lock', lock.replace('rev=aaaa#aaaa', 'rev=bbbb#bbbb')),
                ('Cargo.lock', lock + '\n[[package]]\nname = "rustfs-gateway-types"\nversion = "0.1.0"\n'),
                # A lockfile that cannot be parsed is not evidence of an unrelated change.
                ('Cargo.lock', 'not toml ['),
        ):
            with self.subTest(path=path, content=content[-60:]):
                self.repo.write('Cargo.toml', manifest)
                self.repo.write('Cargo.lock', lock)
                self.repo.write('leaf/src/lib.rs', f'// {len(content)}\n')
                start = self.repo.commit()
                self.repo.write(path, content)
                plan = self.repo.plan(self.repo.commit(), base=start)
                self.assertEqual(plan['gateway_smoke'], {'run': True, 'reason': f'gateway pin changed: {path}'})
        # Removing the lockfile leaves nothing to compare.
        self.repo.write('Cargo.toml', manifest)
        self.repo.write('Cargo.lock', lock)
        start = self.repo.commit()
        (self.repo.root / 'Cargo.lock').unlink()
        self.assertTrue(self.repo.plan(self.repo.commit(), base=start)['gateway_smoke']['run'])

    def test_gateway_smoke_runs_on_every_non_pr_event_and_when_the_change_set_is_unknown(self):
        self.repo.write('leaf/src/lib.rs', '// unrelated\n')
        head = self.repo.commit()
        self.assertFalse(self.repo.plan(head)['gateway_smoke']['run'])
        for event in ('push', 'merge_group', 'schedule', 'workflow_dispatch'):
            self.assertEqual(self.repo.plan(head, event=event)['gateway_smoke'],
                             {'run': True, 'reason': f'{event} validates the complete candidate'})
        for base in ('0' * 40, '', '--output=bad'):
            plan = gate.select_plan('pull_request', base, head, 'refs/heads/main', self.repo.root)
            self.assertEqual(plan['gateway_smoke'], {'run': True, 'reason': 'change set unavailable'})
        with patch.object(gate, 'read_policy', side_effect=ValueError('broken policy')):
            self.assertTrue(self.repo.plan(head)['gateway_smoke']['run'])

    def test_actual_workflow_bootstrap_never_executes_pr_policy_and_upgrades_legacy_output(self):
        workflow = gate.workflow_document((Path(gate.__file__).resolve().parent.parent / '.github/workflows/ci.yml').read_text())
        body = next(step['run'] for step in workflow['jobs']['classify-changes']['steps'] if step.get('id') == 'scope')
        self.repo.write('scripts/ci_gate.py', "import os\nwith open(os.environ['GITHUB_OUTPUT'], 'a') as output: output.write('mode=docs\\npackages=[\"core\"]\\nplan={}\\ngateway_smoke=false\\n')\n")
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
            # A refused base plan cannot keep the report-only lane off either.
            self.assertEqual(values['gateway_smoke'], 'true')
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
        self.assertEqual(set(values), {'mode', 'plan', 'connect', 'packages', 'full_validation', 'rehearse_shards', 'gateway_smoke', 'plan_contract'})
        self.assertEqual(values['gateway_smoke'], 'false')
        self.assertFalse(json.loads(values['plan'])['gateway_smoke']['run'])
        self.assertEqual(values['plan_contract'], gate.PLAN_CONTRACT)
        plan = json.loads(values['plan'])
        self.assertEqual(plan['policy']['revision'], self.repo.base)
        self.assertEqual(plan['policy']['rollout'], 'shadow')
        self.assertEqual(values['connect'], 'true')


class GateTests(unittest.TestCase):
    def test_fips_provider_is_required_at_nightly_and_skipped_on_pr(self):
        self.assertEqual(gate.expected_results('full', 'schedule', 'refs/heads/main')['fips-provider'], 'success')
        self.assertEqual(gate.expected_results('full', 'pull_request', 'refs/heads/main')['fips-provider'], 'skipped')
        plan = self.plan('schedule')
        good = self.needs(plan)
        self.assertEqual(gate.verify_results(good, 'schedule', plan['ref'], plan['head'], plan['tree']), [])
        good['fips-provider']['result'] = 'skipped'
        self.assertTrue(gate.verify_results(good, 'schedule', plan['ref']))

    def plan(self, event='pull_request', mode='full', connect=True):
        jobs = gate.expected_results(mode, event, 'refs/heads/main', connect)
        return {'schema_version': 1, 'event': event, 'ref': 'refs/heads/main', 'head': 'a' * 40, 'tree': 'b' * 40,
                'base': 'c' * 40, 'action': '', 'mode': mode, 'connect': connect, 'full_validation': mode == 'full', 'packages': ['rustfs'] if mode != 'docs' else [],
                'rehearse_shards': False,
                'policy': {'revision': 'c' * 40, 'rollout': 'enforced'}, 'jobs': jobs, 'configurations': gate.configurations(jobs),
                'reasons': ['fixture'], 'not_applicable': {job: 'fixture' for job, state in jobs.items() if state == 'skipped'},
                'proven_by': {'run_id': 7, 'url': 'https://github.com/rustfs/rustfs/actions/runs/7', 'tree': 'b' * 40} if mode == 'proven' else None}

    def needs(self, plan):
        needs = {job: {'result': state} for job, state in plan['jobs'].items()}
        needs['classify-changes']['outputs'] = {'mode': plan['mode'], 'plan': json.dumps(plan)}
        return needs

    def test_failure_cancel_missing_extra_or_unexpected_skip_is_rejected(self):
        for event, mode in [('pull_request', 'docs'), ('pull_request', 'e2e'), ('pull_request', 'full'), ('push', 'full'), ('push', 'proven'), ('merge_group', 'full'), ('schedule', 'full'), ('workflow_dispatch', 'full')]:
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

    def test_a_proven_push_needs_its_own_tree_proof_and_nothing_else_may_carry_one(self):
        proven, full = self.plan('push', 'proven'), self.plan('push', 'full')
        self.assertEqual(gate.verify_results(self.needs(proven), 'push', proven['ref'], proven['head'], proven['tree']), [])
        proof = proven['proven_by']
        bad = [dict(proven, proven_by=None), dict(proven, proven_by=dict(proof, tree='c' * 40)),
               dict(proven, proven_by=dict(proof, run_id='7')), dict(proven, proven_by=dict(proof, run_id=0)),
               dict(proven, proven_by=dict(proof, url='https://example.invalid/7')), dict(proven, proven_by={}),
               dict(full, proven_by=proof)]
        for plan in bad:
            with self.subTest(plan=plan['proven_by']), self.assertRaises(ValueError):
                gate.validate_plan(plan, 'push', plan['ref'])
        for event in ('pull_request', 'merge_group', 'schedule', 'workflow_dispatch'):
            with self.subTest(event=event), self.assertRaises(ValueError):
                gate.expected_results('proven', event, 'refs/heads/main')

    def test_the_queue_run_that_proves_a_push_validates_everything_that_push_would(self):
        queued = {job for job, result in gate.expected_results('full', 'merge_group', 'refs/heads/main').items() if result == 'success'}
        for ref in gate.INTEGRATION_PUSH_REFS:
            pushed = {job for job, result in gate.expected_results('full', 'push', ref).items() if result == 'success'}
            with self.subTest(ref=ref):
                self.assertLessEqual(pushed, queued)

    def test_legacy_base_cannot_authorize_skipped_new_jobs(self):
        expected = gate.expected_results('full', 'pull_request', 'refs/heads/main')
        needs = {job: {'result': result} for job, result in expected.items()}
        needs['classify-changes']['outputs'] = {'mode': 'full'}
        self.assertEqual(gate.verify_results(needs, 'pull_request', 'refs/heads/main'), [])
        needs['classify-changes']['outputs']['mode'] = 'docs'
        self.assertTrue(gate.verify_results(needs, 'pull_request', 'refs/heads/main'))
        needs['classify-changes']['outputs'] = {'mode': 'full', 'plan': '{'}
        self.assertTrue(gate.verify_results(needs, 'pull_request', 'refs/heads/main'))

    def test_plan_from_a_previous_contract_cannot_authorize_the_new_job_set(self):
        # A base planner predating the NIGHTLY_JOBS contract still emits
        # connect-feature-boundaries as required success on a pull request. The
        # current verifier must reject that plan instead of accepting the old
        # responsibility, so the workflow's contract-marker fallback is the only
        # path that can widen or narrow the required set.
        stale = self.plan()
        stale['jobs'] = dict(stale['jobs'], **{'connect-feature-boundaries': 'success',
                                              'uring-integration': 'success',
                                              'test-and-lint-rio-v2': 'success',
                                              'test-and-lint-protocols': 'success'})
        stale['not_applicable'] = {job: 'fixture' for job, state in stale['jobs'].items() if state == 'skipped'}
        expected = gate.expected_results('full', 'pull_request', 'refs/heads/main')
        self.assertEqual(expected['connect-feature-boundaries'], 'skipped')
        needs = {job: {'result': result} for job, result in expected.items()}
        needs['classify-changes']['outputs'] = {'mode': 'full', 'plan': json.dumps(stale, separators=(',', ':')),
                                                'connect': 'true', 'full_validation': 'true', 'rehearse_shards': 'false'}
        self.assertTrue(gate.verify_results(needs, 'pull_request', 'refs/heads/main'))

    def workflow(self):
        jobs = {}
        for job in gate.ALWAYS_JOBS + gate.CODE_JOBS + gate.OPTIONAL_JOBS:
            # A complete plan lookup is represented by equivalent explicit expressions here.
            if job in gate.ALWAYS_JOBS:
                expression = "true"
            elif job == 'connect-feature-boundaries':
                expression = "needs.classify-changes.outputs.mode == 'full' && needs.classify-changes.outputs.connect == 'true' && contains(fromJSON('[\"schedule\",\"workflow_dispatch\"]'), github.event_name)"
            elif job in gate.NIGHTLY_JOBS:
                expression = "needs.classify-changes.outputs.mode == 'full' && contains(fromJSON('[\"schedule\",\"workflow_dispatch\"]'), github.event_name)"
            elif job in gate.OPTIONAL_JOBS[:2]:
                expression = "contains(fromJSON('[\"schedule\",\"workflow_dispatch\"]'), github.event_name)"
            elif job == 'e2e-full':
                expression = "needs.classify-changes.outputs.mode == 'full' && (github.event_name == 'merge_group' || github.event_name == 'workflow_dispatch' || (github.event_name == 'push' && github.ref == 'refs/heads/main'))"
            elif job in gate.SHARD_JOBS:
                expression = "needs.classify-changes.outputs.rehearse_shards == 'true'"
            elif job in gate.E2E_JOBS:
                expression = "contains(fromJSON('[\"full\",\"e2e\"]'), needs.classify-changes.outputs.mode)"
            else:
                expression = "needs.classify-changes.outputs.mode == 'full'"
            expression = f"({expression}) && (github.event_name != 'pull_request' || github.event.action != 'closed')"
            jobs[job] = {'name': job, 'if': expression, 'steps': []}
        jobs['classify-changes']['outputs'] = {'mode': '${{ steps.scope.outputs.mode }}'}
        jobs[gate.LINT_JOB]['steps'] = [{'run': 'cargo clippy --all-targets -- -D warnings'}]
        jobs['workspace-tests']['needs'] = [gate.ARCHIVE_PRODUCER]
        jobs['classify-changes']['steps'] = [{'id': 'scope', 'run': f"grep -q '^plan_contract={gate.PLAN_CONTRACT}$' \"$GITHUB_OUTPUT\""}]
        # The gateway integration branch reruns e2e-tests' suite as a report-only lane.
        for job, (mirror, steps, env, selection) in gate.REPORT_ONLY_JOBS.items():
            key, _, value = env.partition(': ')
            jobs[mirror]['env'] = {key: 'legacy'}
            jobs[mirror]['steps'] = [{'name': step, 'run': f'echo {step}'} for step in steps]
            jobs[job] = copy.deepcopy(jobs[mirror])
            jobs[job].update({'name': f'{job} (report-only)', 'continue-on-error': True, 'env': {key: value},
                              'needs': ['classify-changes'],
                              'if': f"({jobs[mirror]['if']}) && (github.event_name != 'pull_request' || needs.classify-changes.outputs.{selection} != 'false')"})
            jobs['classify-changes']['outputs'][selection] = f"${{{{ steps.scope.outputs.{selection} || 'true' }}}}"
            jobs[job]['steps'].append({'name': 'Upload report', 'uses': 'actions/upload-artifact', 'with': {'name': job}})
        jobs['test-and-lint-protocols']['strategy'] = {'matrix': {'features': [
            {'name': 'swift', 'flags': '--features swift'}, {'name': 'sftp', 'flags': '--features sftp'}, {'name': 'http3', 'flags': '--features http3,swift'},
            {'name': 'internode-transport', 'flags': '--features rustfs/internode-transport'}]}}
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

    def test_full_e2e_gate_covers_main_and_integration_pushes_only(self):
        for event, ref, required in (
            ("push", "refs/heads/main", "success"),
            ("push", "refs/heads/gateway/integration", "success"),
            ("push", "refs/heads/gateway/integration-old", "skipped"),
            ("push", "refs/heads/release", "skipped"),
            ("push", "refs/heads/feature", "skipped"),
        ):
            self.assertEqual(gate.expected_results("full", event, ref)["e2e-full"], required, ref)

    def test_report_only_stack_lane_mirrors_e2e_smoke_outside_the_aggregate(self):
        workflow = self.workflow()
        self.assertEqual(self.check(workflow), [])
        job, (mirror, steps, env, _) = next(iter(gate.REPORT_ONLY_JOBS.items()))
        key = env.partition(': ')[0]
        run = steps.index('Run e2e smoke suite')
        for case, mutate in enumerate((
            # The lane can neither gate the aggregate nor vanish.
            lambda w: w['jobs']['required-checks']['needs'].append(job),
            lambda w: w['jobs'].pop(job),
            # It must select the gateway stack.
            lambda w: w['jobs'][job]['env'].update({key: 'legacy'}),
            lambda w: w['jobs'][job].pop('env'),
            # Its suite must not drift from the required lane in either direction.
            lambda w: w['jobs'][job]['steps'][run].update({'run': 'echo --run-ignored all'}),
            lambda w: w['jobs'][mirror]['steps'][run].update({'run': 'echo --run-ignored all'}),
            lambda w: w['jobs'][job]['steps'][run].update({'name': 'Run gateway smoke suite'}),
            lambda w: w['jobs'][job]['steps'].pop(0),
        )):
            changed = copy.deepcopy(workflow)
            mutate(changed)
            self.assertTrue(self.check(changed), case)
        changed = copy.deepcopy(workflow)
        changed['jobs'][job]['steps'][-1]['with']['name'] = 'renamed-report'
        self.assertEqual(self.check(changed), [], "only the mirrored steps are bound")

    def test_parallel_clippy_lane_still_gates_the_aggregate(self):
        lint = gate.LINT_JOB
        # The aggregate requires the lint wherever the archive producer runs.
        for event, mode, required in (('pull_request', 'full', 'success'), ('pull_request', 'e2e', 'success'),
                                      ('pull_request', 'docs', 'skipped'), ('merge_group', 'full', 'success'),
                                      ('push', 'full', 'success'), ('schedule', 'full', 'success'),
                                      ('workflow_dispatch', 'full', 'success')):
            plan = self.plan(event, mode)
            self.assertEqual(plan['jobs'][lint], required, (event, mode))
            self.assertEqual(plan['jobs'][lint], plan['jobs'][gate.ARCHIVE_PRODUCER], (event, mode))
            self.assertEqual(gate.verify_results(self.needs(plan), event, plan['ref']), [])
            for bad in ({'failure', 'cancelled', 'skipped', 'success', None} - {required}):
                needs = self.needs(plan)
                needs[lint]['result'] = bad
                errors = gate.verify_results(needs, event, plan['ref'])
                self.assertTrue(any(error.startswith(f'{lint}: expected {required}') for error in errors), (event, mode, bad, errors))
            needs = self.needs(plan)
            del needs[lint]
            self.assertTrue(any(lint in error and 'job set differs' in error for error in gate.verify_results(needs, event, plan['ref'])))
        self.assertIn(lint, [item['job'] for item in gate.configurations(self.plan()['jobs']) if item['id'] == 'workspace-lint'])
        # A base planner that predates the lint job cannot describe the job set.
        stale = self.plan()
        del stale['jobs'][lint]
        needs = self.needs(stale)
        self.assertTrue(gate.verify_results(needs, 'pull_request', stale['ref']))

    def test_clippy_lane_is_wired_beside_the_archive_producer(self):
        workflow = self.workflow()
        self.assertEqual(self.check(workflow), [])
        lint, producer = gate.LINT_JOB, gate.ARCHIVE_PRODUCER
        step = {'run': 'python3 scripts/ci_test_artifact.py cargo lint --configuration workspace'}
        for case, (mutate, message) in enumerate((
            # The aggregate must keep depending on the lint, and the lint must keep running.
            (lambda w: w['jobs']['required-checks']['needs'].remove(lint), 'required-checks must directly depend on every verification job exactly once'),
            (lambda w: w['jobs'].pop(lint), 'CI verification jobs and the required gate contract differ'),
            (lambda w: w['jobs'][lint].update({'steps': []}), 'workspace Clippy must run on every code event, pull requests included'),
            (lambda w: w['jobs'][lint]['steps'][0].update({'if': "github.event_name != 'merge_group'"}), 'workspace Clippy must run on every code event, pull requests included'),
            (lambda w: w['jobs'][lint]['steps'][0].update({'run': 'cargo clippy --all-targets'}), 'workspace Clippy must run on every code event, pull requests included'),
            (lambda w: w['jobs'][lint].update({'continue-on-error': 'true'}), f'{lint} cannot convert a validation failure into success'),
            (lambda w: w['jobs'][lint]['steps'][0].update({'continue-on-error': 'true'}), f'{lint} cannot convert a validation failure into success'),
            # Its scope is the archive producer's scope on every event.
            (lambda w: w['jobs'][lint].update({'if': "github.event_name == 'merge_group'"}), f'{lint} must run under the same condition as {producer}'),
            (lambda w: w['jobs'][lint].update({'if': 'false'}), f'{lint} must run under the same condition as {producer}'),
            # One lint, in one place, off the path the workspace tests wait on.
            (lambda w: w['jobs'][producer]['steps'].append(dict(step)), f'workspace Clippy belongs to {lint} alone'),
            (lambda w: w['jobs'][producer].update({'needs': [lint]}), f'{producer} must not wait for {lint}'),
            (lambda w: w['jobs']['workspace-tests']['needs'].append(lint), f'workspace-tests must not wait for {lint}'),
            (lambda w: w['jobs']['workspace-tests'].update({'needs': ['classify-changes']}), f'workspace-tests must consume the archive of {producer}'),
            # The workflow fallback must name the contract this planner emits.
            (lambda w: w['jobs']['classify-changes']['steps'][0].update({'run': "grep -q '^plan_contract=3$' \"$GITHUB_OUTPUT\""}), f'classify-changes must fall back to the full matrix unless the plan carries contract {gate.PLAN_CONTRACT}'),
            (lambda w: w['jobs']['classify-changes'].update({'steps': []}), f'classify-changes must fall back to the full matrix unless the plan carries contract {gate.PLAN_CONTRACT}'),
        )):
            changed = copy.deepcopy(workflow)
            mutate(changed)
            self.assertIn(message, self.check(changed), case)
        # The checked-in workflow satisfies the same contract.
        self.assertEqual(gate.check_workflow(Path(gate.__file__).resolve().parent.parent), [])

    def test_report_only_lane_runs_only_where_its_selection_says_and_always_outside_pull_requests(self):
        workflow = self.workflow()
        self.assertEqual(self.check(workflow), [])
        job, (mirror, _, _, selection) = next(iter(gate.REPORT_ONLY_JOBS.items()))
        output = f'needs.classify-changes.outputs.{selection}'
        scope = workflow['jobs'][mirror]['if']
        # Equivalent spellings stay green: the guard pins behaviour, not text.
        changed = copy.deepcopy(workflow)
        changed['jobs'][job]['if'] = f"${{{{ ({output} != 'false' || github.event_name != 'pull_request') && ({scope}) }}}}"
        self.assertEqual(self.check(changed), [])
        for case, (mutate, message) in enumerate((
            # Condition removed: the lane is back on every pull request.
            (lambda w: w['jobs'][job].pop('if'), 'runs on pull requests that change no gateway-stack input'),
            (lambda w: w['jobs'][job].update({'if': scope}), 'runs on pull requests that change no gateway-stack input'),
            (lambda w: w['jobs'][job].update({'if': f"({scope}) && ({output} != 'false' || true)"}), 'runs on pull requests that change no gateway-stack input'),
            # Condition that also skips pushes to main (and every other non-PR event).
            (lambda w: w['jobs'][job].update({'if': f"({scope}) && {output} != 'false'"}), 'is skipped on push'),
            (lambda w: w['jobs'][job].update({'if': f"({scope}) && github.event_name == 'pull_request' && {output} != 'false'"}), 'is skipped on push'),
            (lambda w: w['jobs'][job].update({'if': f"({scope}) && github.event_name != 'push' && (github.event_name != 'pull_request' || {output} != 'false')"}), 'is skipped on push'),
            (lambda w: w['jobs'][job].update({'if': f"({scope}) && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && {output} != 'false'))"}), 'is skipped on merge_group'),
            (lambda w: w['jobs'][job].update({'if': 'false'}), 'is skipped on push'),
            # A selected pull request must run it; an unknown selection is not a skip.
            (lambda w: w['jobs'][job].update({'if': f"({scope}) && github.event_name != 'pull_request'"}), 'is skipped on a pull request that changes a gateway-stack input'),
            (lambda w: w['jobs'][job].update({'if': f"({scope}) && (github.event_name != 'pull_request' || {output} == 'true')"}), 'is skipped on a pull request whose selection is unknown'),
            # The scope narrows the required lane's schedule; it never widens it.
            (lambda w: w['jobs'][job].update({'if': f"github.event_name != 'pull_request' || {output} != 'false'"}), f'runs where {mirror} does not'),
            # The expression is inert unless the output really reaches the job.
            (lambda w: w['jobs'][job].update({'needs': []}), 'must need classify-changes'),
            (lambda w: w['jobs']['classify-changes']['outputs'].pop(selection), f'classify-changes must publish {selection}'),
            (lambda w: w['jobs']['classify-changes']['outputs'].update({selection: 'false'}), f'classify-changes must publish {selection}'),
            # A status function would detach the lane from its build prerequisite.
            (lambda w: w['jobs'][job].update({'if': f"always() && ({w['jobs'][job]['if']})"}), 'must not override its prerequisites'),
            # Report-only means the job says so, and no step may turn a failed suite green.
            (lambda w: w['jobs'][job].pop('continue-on-error'), 'must stay report-only'),
            (lambda w: w['jobs'][job]['steps'][0].update({'continue-on-error': True}), 'cannot hide a failed step'),
        )):
            changed = copy.deepcopy(workflow)
            mutate(changed)
            errors = self.check(changed)
            self.assertTrue(any(message in error for error in errors), (case, message, errors))

    def test_actual_workflow_schedules_the_gateway_lane_by_event_and_selection(self):
        workflow = gate.workflow_document((gate.ROOT / '.github/workflows/ci.yml').read_text())
        block = workflow['jobs']['e2e-tests-gateway']
        self.assertEqual(block['continue-on-error'], 'true')
        self.assertIn('classify-changes', block['needs'])
        self.assertIn('build-rustfs-debug-binary', block['needs'])

        def runs(event, selected):
            return gate.condition(block['if'], {'github.event_name': event, 'needs.classify-changes.outputs.gateway_smoke': selected})

        self.assertFalse(runs('pull_request', 'false'), 'unrelated pull request')
        self.assertTrue(runs('pull_request', 'true'), 'gateway-stack pull request')
        self.assertTrue(runs('pull_request', ''), 'selection unavailable')
        for event in ('push', 'merge_group', 'schedule', 'workflow_dispatch'):
            for selected in ('true', 'false', ''):
                self.assertTrue(runs(event, selected), (event, selected))
        self.assertEqual(workflow['jobs']['classify-changes']['outputs']['gateway_smoke'], "${{ steps.scope.outputs.gateway_smoke || 'true' }}")

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
            workflow['jobs'][job]['needs'] = ['workspace-lint-and-build' if job == 'workspace-tests' else 'build-rustfs-debug-binary']
        self.assertEqual(self.check(workflow), [])
        for mutate in (
            lambda w: w['jobs']['required-checks']['needs'].pop(),
            lambda w: w['jobs']['required-checks'].update({'if': 'success()'}),
            lambda w: w['jobs']['required-checks'].update({'if': "'always()' == 'always()'"}),
            lambda w: w['jobs']['required-checks']['steps'][0].update({'run': 'python3 scripts/ci_gate.py verify || true'}),
            lambda w: w['jobs']['typos'].update({'continue-on-error': 'true'}),
            lambda w: w['jobs'][gate.LINT_JOB]['steps'][0].update({'if': 'false'}),
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
