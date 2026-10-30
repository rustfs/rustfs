#!/usr/bin/env python3
"""A bounded shard run must preserve resource groups and exact test membership."""

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

import ci_test_shards as shards
import check_migration_gate_evidence as migration
from test_ci_test_artifact import listing


class PartitionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.listing = listing([('a', 'one', False), ('b', 'two', False), ('c', 'three', False)])
        self.junit = self.root / 'baseline.xml'
        self.junit.write_text('<testsuites failures="0" errors="0">' + ''.join(
            f'<testsuite name="{binary}"><testcase name="{name}" classname="{binary}" time="{seconds}"/></testsuite>'
            for binary, name, seconds in [('a', 'one', 20), ('b', 'two', 10), ('c', 'three', 25)]) + '</testsuites>')

    def test_observed_durations_balance_whole_protected_component(self):
        result = shards.partition(self.listing, self.junit, {('a', 'one'), ('b', 'two')})
        self.assertEqual([item['binaries'] for item in result], [['a', 'b'], ['c']])
        self.assertEqual([item['observed_test_seconds'] for item in result], [30, 25])
        self.assertEqual(sum(item['tests'] for item in result), 3)
        self.assertEqual(len(shards.partition(self.listing, self.junit, set(), 1)), 1)
        self.assertEqual(len(shards.partition(self.listing, self.junit, {('a', 'one'), ('b', 'two'), ('c', 'three')})), 1)

    def test_missing_invalid_and_unknown_observations_fail(self):
        for duration in ('NaN', '-1', 'Infinity'):
            with self.subTest(duration=duration):
                original = self.junit.read_text()
                self.junit.write_text(original.replace('time="20"', f'time="{duration}"'))
                with self.assertRaises(ValueError):
                    shards.partition(self.listing, self.junit, set())
                self.junit.write_text(original)
        with self.assertRaises(ValueError):
            shards.partition(self.listing, self.junit, {('unknown', 'test')})
        for count in (0, 3, True):
            with self.assertRaises(ValueError):
                shards.partition(self.listing, self.junit, set(), count)
        self.junit.write_text('<testsuites/>')
        with self.assertRaisesRegex(ValueError, 'does not cover'):
            shards.partition(self.listing, self.junit, set())

    def test_inherited_thread_constraints_are_included_without_parsing_the_dsl(self):
        config = {'profile': {'ci': {'inherits': 'intermediate', 'overrides': [
            {'filter': 'test(=shared)', 'test-group': 'shared'}]}, 'intermediate': {'overrides': [
            {'filter': 'test(=thread_bound)', 'threads-required': 2}]}, 'default': {'overrides': [
            {'filter': 'test(=exclusive)', 'threads-required': 'num-test-threads'}]}}}
        expression = shards.protected_filter(config)
        self.assertIn('test(=shared)', expression)
        self.assertIn('test(=thread_bound)', expression)
        self.assertIn('test(=exclusive)', expression)
        config['profile']['intermediate']['inherits'] = 'ci'
        with self.assertRaisesRegex(ValueError, 'cyclic'):
            shards.protected_filter(config)


class RealNextestTests(unittest.TestCase):
    def test_archive_shards_execute_without_rebuilding_and_reject_incomplete_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'src').mkdir()
            (root / 'tests').mkdir()
            (root / '.config').mkdir()
            (root / 'rust-toolchain.toml').write_text((shards.ROOT / 'rust-toolchain.toml').read_text())
            (root / 'Cargo.toml').write_text('[package]\nname="probe"\nversion="0.1.0"\nedition="2021"\n')
            (root / 'src/lib.rs').write_text('#[test] fn first() { assert!(std::env::var_os("SHARD_FAIL").is_none()); }\n')
            (root / 'tests/second.rs').write_text('#[test] fn second() {}\n#[test] #[ignore] fn omitted() {}\n')
            (root / 'tests/third.rs').write_text('#[test] fn third() {}\n')
            (root / '.config/nextest.toml').write_text('''experimental = ["setup-scripts"]
[test-groups]
shared = { max-threads = 1 }
[scripts.setup.fixture]
command = ['sh', '-c', 'echo SHARD_SETUP=present >> "$NEXTEST_ENV"']
[profile.ci]
retries = 0
fail-fast = false
[profile.ci.junit]
path = "junit.xml"
[[profile.ci.overrides]]
filter = 'binary_id(=probe) or binary_id(=probe::second)'
test-group = 'shared'
[[profile.ci.scripts]]
filter = 'all()'
setup = ['fixture']
''')
            archive = root / 'tests.tar.zst'
            def cargo(*arguments):
                result = subprocess.run(['cargo', 'nextest', *arguments], cwd=root, text=True, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                return result
            cargo('archive', '--offline', '--archive-file', str(archive))
            full = shards.list_tests(root, ['--archive-file', str(archive), '--extract-to', str(root), '--extract-overwrite'])
            full_path = root / 'listing.json'
            shards.write_json(full_path, full)
            cargo('run', '--profile', 'ci', *shards.metadata_arguments(root))
            junit = root / 'baseline.xml'
            shutil.copyfile(root / 'target/nextest/ci/junit.xml', junit)
            with patch.object(shards, 'source_identity', return_value={'commit': 'a' * 40}):
                plan_dir = root / 'plan'
                plan = shards.prepare(root, full_path, junit, plan_dir, 2)
                self.assertEqual(len(plan['shards']), 2)
                reserved = next(item for item in plan['shards'] if 'probe' in item['binaries'])
                self.assertIn('probe::second', reserved['binaries'])
                (root / 'src/lib.rs').write_text('compile_error!("rebuilding is forbidden");')
                evidence = root / 'evidence'
                for shard in plan['shards']:
                    shards.execute(root, plan_dir, archive, shard['index'], evidence / str(shard['index']))
                self.assertEqual(shards.verify(plan_dir, evidence), 3)
                floor = root / 'migration-floor.txt'
                floor.write_text('1\n')
                # Older local nextest versions omit the test kind; all fixture
                # cases are Rust #[test] functions, as CI's listing records.
                migration_listing = copy.deepcopy(full)
                for case in migration_listing['rust-suites']['probe']['testcases'].values():
                    case.setdefault('kind', 'test')
                shards.write_json(root / 'migration-listing.json', migration_listing)
                with patch.object(migration, 'SUITE', 'probe'), patch.object(migration, 'NAME_PARTS', ('first',)):
                    self.assertEqual(migration.verify(root / 'migration-listing.json', evidence / 'combined.xml', floor), (1, 1))
                receipt = evidence / '1/receipt.json'
                original = receipt.read_text()
                for field, value in [('index', 2), ('exit_code', 1), ('plan_sha256', 'wrong')]:
                    shards.write_json(receipt, dict(json.loads(original), **{field: value}))
                    with self.assertRaises(ValueError):
                        shards.verify(plan_dir, evidence)
                receipt.write_text(original)
                receipt.rename(receipt.with_suffix('.absent'))
                with self.assertRaisesRegex(ValueError, 'missing'):
                    shards.verify(plan_dir, evidence)
                receipt.with_suffix('.absent').rename(receipt)
                report = evidence / '1/junit.xml'
                original_report = report.read_bytes()
                xml = ET.fromstring(original_report)
                suite = next(item for item in xml.findall('testsuite') if not item.get('name').startswith('@setup-script:'))
                suite.remove(suite.findall('testcase')[0])
                ET.ElementTree(xml).write(report)
                with self.assertRaises(ValueError):
                    shards.verify(plan_dir, evidence)
                report.write_bytes(original_report)
                bad = copy.deepcopy(plan)
                bad['shards'][0]['binaries'].append(bad['shards'][1]['binaries'][0])
                shards.write_json(plan_dir / 'plan.json', bad)
                with self.assertRaisesRegex(ValueError, 'overlapping'):
                    shards.load_plan(plan_dir)
                shards.write_json(plan_dir / 'plan.json', plan)
                with patch.dict(os.environ, {'SHARD_FAIL': '1'}), self.assertRaisesRegex(ValueError, 'execution failed'):
                    shards.execute(root, plan_dir, archive, reserved['index'], root / 'failed')


if __name__ == '__main__':
    unittest.main()
