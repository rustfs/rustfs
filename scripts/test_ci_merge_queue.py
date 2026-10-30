#!/usr/bin/env python3
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from ci_merge_queue import REQUIRED_CHECKS, compare, prepare
import ci_gate
from test_ci_plan import Repository


class QueueTests(unittest.TestCase):
    def snapshot(self):
        return {"id": 12, "name": "main", "target": "branch", "enforcement": "active", "bypass_actors": [],
                "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
                "rules": [{"type": "required_status_checks", "parameters": {
                    "required_status_checks": [{"context": name, "integration_id": 15368} for name in sorted(REQUIRED_CHECKS)]}},
                    {"type": "pull_request", "parameters": {"required_approving_review_count": 1, "allowed_merge_methods": ["squash"]}},
                    {"type": "required_linear_history"}]}

    def test_prepares_only_additive_queue_rule_and_preserves_rollback(self):
        snapshot = self.snapshot()
        original = copy.deepcopy(snapshot)
        record = prepare(snapshot)
        self.assertEqual(snapshot, original)
        self.assertEqual(record["after"]["rules"][:-1], record["before"]["rules"])
        compare(record, snapshot, "before")
        compare(record, dict(record["after"], id=snapshot["id"]), "after")

    def test_changed_or_missing_checks_cannot_be_hidden(self):
        for key, value in (("context", "Other"), ("integration_id", 42)):
            snapshot = self.snapshot()
            snapshot["rules"][0]["parameters"]["required_status_checks"][0][key] = value
            with self.assertRaises(ValueError):
                prepare(snapshot)

    def test_rule_drift_wrong_id_and_weakened_proposal_fail_readback(self):
        snapshot = self.snapshot()
        record = prepare(snapshot)
        for change in (dict(snapshot, id=99), dict(snapshot, bypass_actors=[{"actor_id": 42}])):
            with self.assertRaises(ValueError):
                compare(record, change, "before")
        record["after"]["rules"][-1]["parameters"]["grouping_strategy"] = "HEADGREEN"
        with self.assertRaises(ValueError):
            compare(record, snapshot, "before")

    def test_disabled_ruleset_missing_reviews_or_duplicate_activation_rejected(self):
        for update in ({"enforcement": "disabled"}, {"target": "tag"}, {"rules": self.snapshot()["rules"][:1]}):
            with self.assertRaises(ValueError):
                prepare(dict(self.snapshot(), **update))
        record = prepare(self.snapshot())
        with self.assertRaises(ValueError):
            prepare(dict(record["after"], id=12))

    def test_individually_passing_changes_fail_on_the_actual_combined_git_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Repository(Path(directory))
            repo.write('rust-toolchain.toml', (ci_gate.ROOT / 'rust-toolchain.toml').read_text())
            repo.write('core/src/lib.rs', 'mod a; mod b; #[test] fn combined_contract() { assert!(a::VALUE + b::VALUE <= 3); }\n')
            repo.write('core/src/a.rs', 'pub const VALUE: u8 = 1;\n')
            repo.write('core/src/b.rs', 'pub const VALUE: u8 = 1;\n')
            baseline = repo.commit()
            def check():
                return subprocess.run(['cargo', 'nextest', 'run', '--offline', '--color', 'never', '-p', 'core'],
                                      cwd=repo.root, capture_output=True, text=True)
            repo.git('checkout', '-b', 'first')
            repo.write('core/src/a.rs', 'pub const VALUE: u8 = 2;\n')
            first = repo.commit()
            self.assertEqual(check().returncode, 0)
            repo.git('checkout', '-b', 'second', baseline)
            repo.write('core/src/b.rs', 'pub const VALUE: u8 = 2;\n')
            second = repo.commit()
            self.assertEqual(check().returncode, 0)
            repo.git('checkout', '-b', 'combined', first)
            repo.git('merge', '--no-ff', '-m', 'combined candidate', second)
            head = repo.git('rev-parse', 'HEAD')
            plan = ci_gate.select_plan('merge_group', baseline, head, 'refs/heads/main', repo.root)
            self.assertEqual(plan['head'], head)
            self.assertEqual(plan['tree'], repo.git('rev-parse', head + '^{tree}'))
            self.assertTrue(plan['full_validation'])
            result = check()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('combined_contract', result.stderr)
            self.assertIn('1 failed', result.stderr)
            needs = {job: {'result': value} for job, value in plan['jobs'].items()}
            needs['classify-changes']['outputs'] = {'mode': 'full', 'plan': json.dumps(plan)}
            needs['workspace-tests']['result'] = 'failure'
            self.assertTrue(ci_gate.verify_results(needs, 'merge_group', plan['ref'], head, plan['tree']))


if __name__ == "__main__":
    unittest.main()
