#!/usr/bin/env python3
import copy
import unittest

from ci_merge_queue import REQUIRED_CHECKS, compare, prepare


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


if __name__ == "__main__":
    unittest.main()
