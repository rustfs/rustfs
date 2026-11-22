from __future__ import annotations

import hashlib
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

sys.path.insert(0, str(ROOT / "scripts"))
import verify_agent_v1_fixtures as verifier  # noqa: E402


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


class FixtureManifestCoverageTests(unittest.TestCase):
    """The root manifest must list every fixture set exactly once."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def add_set(self, name: str, payload: str = "x") -> str:
        directory = self.root / name
        directory.mkdir(parents=True)
        (directory / "data.txt").write_text(payload, encoding="utf-8")
        digest = hashlib.sha256(payload.encode()).hexdigest()
        (directory / "MANIFEST.sha256").write_text(f"{digest}  data.txt\n", encoding="utf-8")
        return hashlib.sha256((directory / "MANIFEST.sha256").read_bytes()).hexdigest()

    def write_root(self, lines: list[str]) -> None:
        (self.root / "manifest.sha256").write_text("".join(lines), encoding="utf-8")

    def test_valid_chain_passes(self) -> None:
        digest = self.add_set("alpha")
        self.write_root([f"{digest}  alpha\n"])
        self.assertTrue(verifier.verify_manifests(self.root))

    def test_empty_root_manifest_fails(self) -> None:
        self.add_set("alpha")
        self.write_root([])
        self.assertFalse(verifier.verify_manifests(self.root))

    def test_unlisted_fixture_set_fails(self) -> None:
        digest = self.add_set("alpha")
        self.add_set("beta")
        self.write_root([f"{digest}  alpha\n"])
        self.assertFalse(verifier.verify_manifests(self.root))

    def test_duplicate_entry_fails(self) -> None:
        digest = self.add_set("alpha")
        self.write_root([f"{digest}  alpha\n", f"{digest}  alpha\n"])
        self.assertFalse(verifier.verify_manifests(self.root))


class JsonPointerArrayIndexTests(unittest.TestCase):
    """Array tokens that Python's int() would accept but RFC 6901 forbids."""

    def test_negative_index_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            verifier.apply_mutation({"items": [1, 2, 3]}, {"path": "/items/-1", "replacement": 9})

    def test_leading_zero_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            verifier.apply_mutation({"items": [1, 2, 3]}, {"path": "/items/01", "replacement": 9})

    def test_removal_rejects_negative_index(self) -> None:
        with self.assertRaises(ValueError):
            verifier.apply_mutation({"items": [1, 2, 3]}, {"path": "/items/-1", "remove": True})

    def test_valid_index_still_works(self) -> None:
        mutated = verifier.apply_mutation({"items": [1, 2, 3]}, {"path": "/items/1", "replacement": 9})
        self.assertEqual([1, 9, 3], mutated["items"])
