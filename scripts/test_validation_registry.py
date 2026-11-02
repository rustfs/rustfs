#!/usr/bin/env python3
"""Selection, ownership and environment failure boundaries for periodic suites."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import functional_environment as environment
import functional_chain_evidence as evidence
import validation_registry as registry


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.registry = registry.load_registry()

    def test_all_workflow_responsibilities_and_suite_exits_are_wired(self):
        self.assertTrue(registry.check_wiring(self.registry))
        self.assertEqual(len(self.registry["suites"]), 12)
        self.assertEqual(sum(item["origin"] == "baseline" for item in self.registry["workflows"]), 61)
        self.assertEqual(len(self.registry["workflows"]), len(list((registry.ROOT / ".github/workflows").glob("*.yml"))))

    def test_manual_and_periodic_plan_choose_identical_responsibilities(self):
        for cadence in ("nightly", "weekly", "monthly"):
            automatic = registry.select_plan(self.registry, cadence, event="schedule")
            manual = registry.select_plan(self.registry, cadence, event="workflow_dispatch")
            self.assertEqual(automatic, manual)
            self.assertEqual(automatic["timezone"], "UTC")
            self.assertTrue(all(registry.workflow_entry(path)["policy"] == "active"
                                for path in automatic["periodic_workflows"]))

    def test_single_suite_has_no_implicit_successor_or_optional_escape(self):
        for suite in registry.suite_ids():
            plan = registry.select_plan(self.registry, requested=suite)
            self.assertEqual(plan["suites"], [suite])
            self.assertEqual(plan["optional_suites"], [])
        for invalid in ("s3,", "s3,s3", "unknown", " s3", "s3, kms"):
            with self.assertRaises(ValueError):
                registry.select_plan(self.registry, requested=invalid)
        with self.assertRaises(ValueError):
            registry.select_plan(self.registry, requested="s3", event="schedule")

    def test_disabled_and_cyclic_responsibilities_cannot_be_scheduled(self):
        value = copy.deepcopy(self.registry)
        path = value["suites"][0]["workflow"]
        registry.workflow_entry(path, value)["policy"] = "disabled"
        with self.assertRaisesRegex(ValueError, "disabled"):
            registry.select_plan(value)
        value = copy.deepcopy(self.registry)
        value["suites"][0]["dependencies"] = ["s3"]
        value["suites"][1]["dependencies"] = ["upgrade"]
        with self.assertRaisesRegex(ValueError, "cyclic"):
            registry.select_plan(value)

    def test_watchdog_uses_registry_scope_source_and_alert_ownership(self):
        item = registry.workflow_entry(".github/workflows/audit.yml")
        event = {"repository": {"full_name": "rustfs/rustfs", "default_branch": "main"},
                 "workflow_run": {"path": item["workflow"], "name": item["name"], "event": "schedule",
                                  "head_branch": "main", "head_repository": {"full_name": "rustfs/rustfs"},
                                  "conclusion": "cancelled"}}
        self.assertTrue(registry.watchdog_event(event))
        for field, value in (("event", "workflow_dispatch"), ("head_branch", "other"),
                             ("path", ".github/workflows/cla.yml"), ("conclusion", "success"),
                             ("conclusion", "failure"), ("head_repository", {"full_name": "other/repo"})):
            changed = copy.deepcopy(event)
            changed["workflow_run"][field] = value
            self.assertFalse(registry.watchdog_event(changed))
        disabled = copy.deepcopy(self.registry)
        registry.workflow_entry(item["workflow"], disabled)["policy"] = "disabled"
        self.assertFalse(registry.watchdog_event(event, disabled))


class SelectionEvidenceTests(unittest.TestCase):
    def test_nonfirst_single_suite_is_verified_and_extraneous_results_are_rejected(self):
        chain = {"run_id": 1, "attempt": 2}
        plan = registry.select_plan(registry.load_registry(), requested="kms")
        needs = {name: {"result": "success" if name == "kms" else "skipped"} for name in evidence.SUITES}
        record = {"schema": 1, "suite": "kms", "chain": chain, "valid": True, "report_sha256": "a" * 64,
                  "counts": {"PASS": 1, "FAIL": 0, "RUNNING": 0, "UNSUPPORTED": 0, "SKIP": 0}}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "kms.json").write_text(json.dumps(record))
            result = evidence.aggregate(chain, root, needs, plan=plan)
            self.assertEqual([item["suite"] for item in result["suites"]], ["kms"])
            for status in ("failure", "cancelled", "skipped", "missing"):
                with self.assertRaises(ValueError):
                    evidence.aggregate(chain, root, {**needs, "kms": {"result": status}}, plan=plan)
            with self.assertRaises(ValueError):
                evidence.aggregate(chain, root, {**needs, "s3": {"result": "success"}}, plan=plan)
            with self.assertRaises(ValueError):
                evidence.aggregate(chain, root, needs, allow_skipped=["kms"], plan=plan)
            record["chain"]["attempt"] = 1
            (root / "kms.json").write_text(json.dumps(record))
            with self.assertRaises(ValueError):
                evidence.aggregate({"run_id": 1, "attempt": 2}, root, needs, plan=plan)


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1", "RUSTFS_NODES": "node1 node2",
                    "FUNCTIONAL_ENVIRONMENT_RECEIPT": str(self.root / "receipt.json")}

    def execute(self, node, user, mode, identity):
        import base64
        home = self.root / node
        home.mkdir(exist_ok=True)
        encoded = base64.b64encode(json.dumps(identity).encode()).decode()
        subprocess.run([sys.executable, "-c", environment.REMOTE, mode, encoded],
                       env={**os.environ, "HOME": str(home)}, check=True, capture_output=True, text=True)

    def test_failed_or_skipped_cleanup_quarantines_the_entire_next_attempt(self):
        environment.transition("enter", "s3", self.env, execute=self.execute)
        for outcome in ("failure", "cancelled", "skipped", ""):
            with self.assertRaises(ValueError):
                environment.transition("finish", "s3", self.env, outcome, self.execute)
            self.assertEqual(json.loads((self.root / "receipt.json").read_text())["state"], "quarantined")
        with self.assertRaises(subprocess.CalledProcessError):
            environment.transition("enter", "kms", {**self.env, "GITHUB_RUN_ID": "124"}, execute=self.execute)
        environment.transition("finish", "s3", self.env, "success", self.execute)
        receipt = environment.transition("enter", "kms", {**self.env, "GITHUB_RUN_ID": "124"}, execute=self.execute)
        self.assertEqual(receipt["state"], "owned")

    def test_foreign_attempt_cannot_release_and_partial_admission_is_not_reused(self):
        calls = []
        def interrupted(node, user, mode, identity):
            calls.append((node, mode))
            if node == "node2" and mode == "enter":
                raise OSError("node unavailable")
            self.execute(node, user, mode, identity)
        with self.assertRaises(OSError):
            environment.transition("enter", "s3", self.env, execute=interrupted)
        self.assertEqual(calls[:2], [("node1", "check"), ("node2", "check")])
        with self.assertRaises(subprocess.CalledProcessError):
            environment.transition("enter", "kms", self.env, execute=self.execute)
        with self.assertRaises(subprocess.CalledProcessError):
            environment.transition("finish", "s3", {**self.env, "GITHUB_RUN_ATTEMPT": "2"}, "success", self.execute)
        self.assertTrue((self.root / "node1/.rustfs-functional-environment/active.json").is_file())
        environment.transition("recover", "s3", self.env, "success", self.execute)
        environment.transition("enter", "kms", self.env, execute=self.execute)

    def test_node_identity_is_validated_before_ssh(self):
        for name in ("node;uname", "-oProxyCommand=anything", "node1 node1"):
            execute = mock.Mock()
            with self.assertRaises(ValueError):
                environment.transition("enter", "s3", {**self.env, "RUSTFS_NODES": name}, execute=execute)
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
