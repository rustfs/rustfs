#!/usr/bin/env python3
"""Historical successes must not hide incomplete evidence or newer attempts."""
import base64
import copy
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest import mock

import functional_chain_health as health
from functional_chain_evidence import SUITES


class HealthTests(unittest.TestCase):
    def setUp(self):
        now = datetime.now(timezone.utc)
        self.now = now.isoformat()
        self.run = {"id": 123, "run_attempt": 2, "head_sha": "a" * 40, "html_url": "https://github.com/rustfs/rustfs/actions/runs/123",
                    "run_started_at": (now - timedelta(hours=1)).isoformat()}
        self.candidate = {"manifest": {"build_run_id": 456, "build_run_attempt": 3, "source_ref": "release", "source_sha": "b" * 40},
                          "workflow_sha": "c" * 40, "workflow_ref": "main", "build_started_at": (now - timedelta(hours=2)).isoformat()}
        self.chain = {"run_id": 123, "attempt": 2, "workflow_sha": "a" * 40, "testing_sha": "d" * 40, "candidate": self.candidate}
        self.summary = {"schema": 1, "complete": True, "chain": self.chain, "completed_at": (now - timedelta(minutes=1)).isoformat(),
                        "suites": [{"schema": 1, "suite": suite, "chain": self.chain, "valid": True, "report_sha256": "f" * 64,
                                    "counts": {"PASS": 1, "FAIL": 0, "SKIP": 0, "UNSUPPORTED": 0, "RUNNING": 0}} for suite in SUITES]}
        self.config = {"content": base64.b64encode(("d" * 40 + "\n").encode()).decode()}

    def validate(self, summary=None, candidate=None, config=None):
        with mock.patch.object(health, "resolve", return_value=candidate or self.candidate), mock.patch.object(health, "api", return_value=config or self.config):
            return health.validate_summary(summary or self.summary, self.run)

    def test_release_success_preserves_both_sources(self):
        result = self.validate()
        self.assertEqual(result["source_ref"], "release")
        self.assertEqual(result["source_sha"], "b" * 40)
        self.assertEqual(result["workflow_sha"], "a" * 40)
        self.assertEqual(result["candidate"]["workflow_sha"], "c" * 40)

    def test_substituted_producer_pin_attempt_or_empty_suite_fails(self):
        with self.assertRaises(ValueError):
            self.validate(candidate={**self.candidate, "workflow_sha": "e" * 40})
        with self.assertRaises(ValueError):
            self.validate(config={"content": base64.b64encode(b"wrong pin").decode()})
        wrong = copy.deepcopy(self.summary)
        wrong["chain"]["attempt"] = 1
        with self.assertRaises(ValueError):
            self.validate(wrong)
        wrong = copy.deepcopy(self.summary)
        wrong["suites"][0]["counts"]["PASS"] = 0
        with self.assertRaises(ValueError):
            self.validate(wrong)

    def test_new_failure_retains_last_complete_success_without_becoming_healthy(self):
        complete = self.validate()
        previous = {"schema": 1, "observed_at": self.now, "last_complete_success": {"release": complete}}
        current = {"schema": 1, "observed_at": self.now, "last_complete_success": {}, "healthy": False,
                   "latest_attempt": {"conclusion": "failure"}}
        result = health.merge_history(current, previous)
        self.assertFalse(result["healthy"])
        self.assertEqual(result["latest_attempt"]["conclusion"], "failure")
        self.assertEqual(result["last_complete_success"]["release"]["source_sha"], "b" * 40)
        self.assertTrue(result["last_complete_success"]["release"]["retained_history"])

    def test_expired_history_is_not_fresh_and_null_or_stale_state_cannot_publish(self):
        complete = self.validate()
        complete["expires_at"] = "2000-01-01T00:00:00Z"
        previous = {"schema": 1, "observed_at": self.now, "last_complete_success": {"release": complete}}
        current = {"schema": 1, "observed_at": self.now, "last_complete_success": {}, "healthy": False}
        self.assertFalse(health.merge_history(current, previous)["last_complete_success"]["release"]["fresh"])
        with self.assertRaises(ValueError):
            health.merge_history(current, None)
        with self.assertRaises(ValueError):
            health.merge_history({**current, "observed_at": "2000-01-01T00:00:00Z"}, previous)
        existing = {"sha": "old-blob", "content": base64.b64encode(b"null").decode()}
        with mock.patch.object(health, "api", return_value=existing), mock.patch.object(health.subprocess, "run") as write:
            with self.assertRaises(ValueError):
                health.publish(current)
            write.assert_not_called()

    def test_collection_rejects_a_concurrent_rerun(self):
        run = {**self.run, "status": "completed", "conclusion": "failure"}
        responses = [{"state": "active"}, {"workflow_runs": [run]}, run,
                     {"workflow_runs": [{**run, "run_attempt": 3, "status": "queued"}]}]
        with mock.patch.object(health, "api", side_effect=responses):
            with self.assertRaisesRegex(ValueError, "changed during inspection"):
                health.collect()

    def test_publication_uses_the_read_blob_sha(self):
        current = {"schema": 1, "observed_at": self.now, "last_complete_success": {}, "healthy": False}
        existing = {"sha": "reviewed-blob", "content": base64.b64encode(json.dumps(current).encode()).decode()}
        with mock.patch.object(health, "api", return_value=existing), mock.patch.object(health.subprocess, "run") as write:
            health.publish(current)
        body = json.loads(write.call_args.kwargs["input"])
        self.assertEqual(body["sha"], "reviewed-blob")
        self.assertFalse(json.loads(base64.b64decode(body["content"]))["healthy"])


if __name__ == "__main__":
    unittest.main()
