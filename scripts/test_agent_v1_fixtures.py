from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verify_agent_v1_fixtures.py"
SCHEMAS = ROOT / "protocol" / "agent" / "v1"
FIXTURES = ROOT / "protocol" / "agent" / "v1" / "fixtures"
ALLOWLIST = ROOT / "protocol" / "agent" / "v1" / "schema-gap-allowlist.json"


class FixtureVerifierRegressionTests(unittest.TestCase):
    def run_cli(self, *args: object) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *map(str, args)],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_manifest_chain_passes(self):
        proc = self.run_cli("--fixtures", FIXTURES)
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)

    def test_health_accept_reject_vectors_use_base_schema_and_allow_semantics(self):
        root = FIXTURES / "diagnostic-scheduler"
        proc = self.run_cli(
            "--schemas", SCHEMAS,
            "--accept", root / "health-service-accept-vectors.json",
            "--reject", root / "health-service-reject-vectors.json",
            "--allow-schema-gaps", ALLOWLIST,
        )
        self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
        self.assertIn("EXPECTED-SEMANTIC-GAP health-service.job.wrong-target", proc.stdout)
        self.assertIn("EXPECTED-SEMANTIC-GAP health-service.job.revoked-consent", proc.stdout)

    def test_unapproved_schema_gap_fails_ci(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "example.schema.json").write_text(json.dumps({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
            }))
            (root / "accept.json").write_text(json.dumps({"vectors": [
                {"id": "base", "schema": "example.schema.json", "value": {"x": 1}},
            ]}))
            (root / "reject.json").write_text(json.dumps({"vectors": [
                {"id": "unapproved-gap", "base": "base", "path": "/x", "replacement": 2},
            ]}))
            proc = self.run_cli("--schemas", root, "--accept", root / "accept.json", "--reject", root / "reject.json")
            self.assertNotEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("unapproved GAP", proc.stdout)

    def test_unknown_base_is_failure_not_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "example.schema.json").write_text(json.dumps({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
            }))
            (root / "accept.json").write_text(json.dumps({"vectors": [
                {"id": "base", "schema": "example.schema.json", "value": {"x": 1}},
            ]}))
            (root / "reject.json").write_text(json.dumps({"vectors": [
                {"id": "unknown-base", "base": "missing", "path": "/x", "replacement": 2},
            ]}))
            proc = self.run_cli("--schemas", root, "--accept", root / "accept.json", "--reject", root / "reject.json")
            self.assertNotEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("unknown base", proc.stdout)

    def test_each_reject_uses_schema_of_its_own_base(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for schema_name, key in (("job.schema.json", "job"), ("health.schema.json", "health")):
                (root / schema_name).write_text(json.dumps({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "required": [key],
                }))
            (root / "accept.json").write_text(json.dumps({"vectors": [
                {"id": "job-base", "schema": "job.schema.json", "value": {"job": True}},
                {"id": "health-base", "schema": "health.schema.json", "value": {"health": True}},
            ]}))
            (root / "reject.json").write_text(json.dumps({"vectors": [
                {"id": "job-reject", "base": "job-base", "path": "/job", "remove": True},
                {"id": "health-reject", "base": "health-base", "path": "/health", "remove": True},
            ]}))
            proc = self.run_cli("--schemas", root, "--accept", root / "accept.json", "--reject", root / "reject.json")
            self.assertEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("PASS job-reject [job.schema.json]", proc.stdout)
            self.assertIn("PASS health-reject [health.schema.json]", proc.stdout)

    def test_unused_allowlist_entry_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "example.schema.json").write_text(json.dumps({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["x"],
            }))
            (root / "accept.json").write_text(json.dumps({"vectors": [
                {"id": "base", "schema": "example.schema.json", "value": {"x": 1}},
            ]}))
            (root / "reject.json").write_text(json.dumps({"vectors": [
                {"id": "reject", "base": "base", "path": "/x", "remove": True},
            ]}))
            (root / "allow.json").write_text(json.dumps({"allow_schema_gaps": [
                {"id": "never-seen", "reason": "must fail as stale"},
            ]}))
            proc = self.run_cli("--schemas", root, "--accept", root / "accept.json", "--reject", root / "reject.json", "--allow-schema-gaps", root / "allow.json")
            self.assertNotEqual(0, proc.returncode, proc.stdout + proc.stderr)
            self.assertIn("unused allowlist entry", proc.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
